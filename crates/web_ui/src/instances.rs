use crate::{
    auth::admin,
    error::{Error, Result},
    model::{DatabaseDiscoveryInput, Instance, InstanceInput, Metadata},
    registry::{SinkRegistry, SourceRegistry},
    secrets,
    store::{Store, now},
};
use mysql_driver::{Conn, OptsBuilder, prelude::Queryable};
use rusqlite::{OptionalExtension, params};
use std::time::Duration;

fn instance_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Instance> {
    let json: Option<String> = r.get(9)?;
    let metadata = json
        .map(|json| {
            serde_json::from_str(&json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        })
        .transpose()?;
    let database: String = r.get(13)?;
    let databases = r
        .get::<_, String>(14)
        .ok()
        .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            if database.is_empty() {
                Vec::new()
            } else {
                vec![database.clone()]
            }
        });
    Ok(Instance {
        id: r.get(0)?,
        name: r.get(1)?,
        host: r.get(2)?,
        port: r.get(3)?,
        version: r.get(4)?,
        kind: r.get(12)?,
        database,
        databases,
        reader_username: r.get(5)?,
        writer_username: r.get(6)?,
        has_reader_password: r.get(7)?,
        has_writer_password: r.get(8)?,
        metadata,
        checked_at: r.get(10)?,
        probe_error: r.get(11)?,
    })
}
const FIELDS: &str = "id,name,host,port,version,reader_username,writer_username,reader_secret IS NOT NULL,writer_secret IS NOT NULL,metadata_json,checked_at,probe_error,kind,database_name,database_names_json";

#[derive(Default)]
struct ExistingCredentials {
    reader_username: String,
    reader_secret: Option<Vec<u8>>,
    writer_username: String,
    writer_secret: Option<Vec<u8>>,
}

fn validate(input: &InstanceInput) -> Result<()> {
    if input.name.trim().is_empty() || input.name.len() > 128 {
        return Err(Error::Invalid("实例名称需要 1–128 字节"));
    }
    let host = &input.host;
    if host.is_empty()
        || host.len() > 253
        || !(host.parse::<std::net::IpAddr>().is_ok()
            || host
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b".-".contains(&c)))
    {
        return Err(Error::Invalid("请输入有效的 IP 或主机名"));
    }
    let registered_source = SourceRegistry.find(&input.kind, &input.version).is_some();
    let registered_sink = SinkRegistry.find(&input.kind, &input.version).is_some();
    let valid_databases = requested_databases(input).is_ok();
    let valid_version = (registered_source || registered_sink) && valid_databases;
    if input.port == 0 || !valid_version {
        return Err(Error::Invalid(
            "数据库类型、版本、端口或连接数据库无效；当前支持 MySQL 5.7/8.0/8.4 和 PostgreSQL 15",
        ));
    }
    for (user, pass) in [
        (&input.reader_username, &input.reader_password),
        (&input.writer_username, &input.writer_password),
    ] {
        if user.len() > 96 || pass.as_ref().is_some_and(|p| p.len() > 1024) {
            return Err(Error::Invalid("数据库账号或密码过长"));
        }
    }
    Ok(())
}

fn requested_databases(input: &InstanceInput) -> Result<Vec<String>> {
    let values = if input.databases.is_empty() {
        if input.database.trim().is_empty() {
            Vec::new()
        } else {
            vec![input.database.trim().to_owned()]
        }
    } else {
        input.databases.clone()
    };
    if input.kind == "mysql" {
        return if values.is_empty() {
            Ok(Vec::new())
        } else {
            Err(Error::Invalid("MySQL 不需要配置连接数据库"))
        };
    }
    if values.is_empty() || values.len() > 128 {
        return Err(Error::Invalid("PostgreSQL 至少选择一个连接数据库"));
    }
    let mut unique = std::collections::BTreeSet::new();
    for value in &values {
        if value.is_empty()
            || value.len() > 63
            || value.chars().any(char::is_control)
            || !unique.insert(value.clone())
        {
            return Err(Error::Invalid("PostgreSQL 连接数据库名称无效或重复"));
        }
    }
    Ok(unique.into_iter().collect())
}
impl Store {
    pub(crate) fn discover_postgresql_databases(
        &self,
        actor: i64,
        input: DatabaseDiscoveryInput,
    ) -> Result<Vec<String>> {
        let conn = self.db()?;
        admin(&conn, actor)?;
        if input.kind != "postgresql" || input.version != "15" || input.port == 0 {
            return Err(Error::Invalid("仅支持探测 PostgreSQL 15 数据库"));
        }
        if input.host.is_empty()
            || input.host.len() > 253
            || !(input.host.parse::<std::net::IpAddr>().is_ok()
                || input
                    .host
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b".-".contains(&c)))
        {
            return Err(Error::Invalid("请输入有效的 PostgreSQL 地址"));
        }
        drop(conn);
        let mut password = input.reader_password;
        if password.is_empty()
            && let Some(id) = input.instance_id.as_deref()
        {
            let sealed: Option<Vec<u8>> = self
                .db()?
                .query_row(
                    "SELECT reader_secret FROM instances WHERE id=?1 AND reader_username=?2",
                    rusqlite::params![id, input.reader_username],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(sealed) = sealed {
                password = secrets::unseal(&self.cipher, &sealed, &format!("{id}:reader"))?;
            }
        }
        if input.reader_username.is_empty() || password.is_empty() {
            return Err(Error::Invalid("探测数据库需要读取账号和密码"));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::Internal)?;
        runtime
            .block_on(postgresql_15::databases(
                &input.host,
                input.port,
                &input.reader_username,
                &password,
            ))
            .map_err(|_| Error::Invalid("PostgreSQL 数据库探测失败，请检查地址、读取账号和密码"))
    }

    pub(crate) fn instances(&self) -> Result<Vec<Instance>> {
        let conn = self.db()?;
        Ok(conn
            .prepare(&format!("SELECT {FIELDS} FROM instances ORDER BY name"))?
            .query_map([], instance_row)?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub(crate) fn instance(&self, id: &str) -> Result<Instance> {
        self.db()?
            .query_row(
                &format!("SELECT {FIELDS} FROM instances WHERE id=?1"),
                [id],
                instance_row,
            )
            .optional()?
            .ok_or(Error::NotFound)
    }
    pub(crate) fn save_instance(
        &self,
        actor: i64,
        id: Option<String>,
        input: InstanceInput,
    ) -> Result<Instance> {
        validate(&input)?;
        let databases = requested_databases(&input)?;
        let database = databases.first().cloned().unwrap_or_default();
        let databases_json = serde_json::to_string(&databases).map_err(|_| Error::Internal)?;
        let conn = self.db()?;
        admin(&conn, actor)?;
        let new = id.is_none();
        let id = id.unwrap_or_else(secrets::random_token);
        let old: Option<ExistingCredentials> = if new {
            None
        } else {
            Some(conn.query_row("SELECT reader_username,reader_secret,writer_username,writer_secret FROM instances WHERE id=?1",[&id],|r|Ok(ExistingCredentials {
                reader_username: r.get(0)?,
                reader_secret: r.get(1)?,
                writer_username: r.get(2)?,
                writer_secret: r.get(3)?,
            })).optional()?.ok_or(Error::NotFound)?)
        };
        let password = |role: &str,
                        user: &str,
                        value: Option<&str>,
                        old_user: &str,
                        old_value: Option<Vec<u8>>|
         -> Result<Option<Vec<u8>>> {
            if user.is_empty() {
                return Ok(None);
            }
            if let Some(value) = value {
                return Ok(Some(secrets::seal(
                    &self.cipher,
                    value,
                    &format!("{id}:{role}"),
                )?));
            }
            if old_user != user || old_value.is_none() {
                return Err(Error::Invalid("新建或更换数据库账号时必须同时填写密码"));
            }
            Ok(old_value)
        };
        let old = old.unwrap_or_default();
        let reader = password(
            "reader",
            &input.reader_username,
            input.reader_password.as_deref(),
            &old.reader_username,
            old.reader_secret,
        )?;
        let writer = password(
            "writer",
            &input.writer_username,
            input.writer_password.as_deref(),
            &old.writer_username,
            old.writer_secret,
        )?;
        if new {
            conn.execute("INSERT INTO instances(id,name,host,port,version,reader_username,reader_secret,writer_username,writer_secret,kind,database_name,database_names_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![id,input.name.trim(),input.host,input.port,input.version,input.reader_username,reader,input.writer_username,writer,input.kind,database,databases_json])?;
        } else {
            conn.execute("UPDATE instances SET name=?2,host=?3,port=?4,version=?5,reader_username=?6,reader_secret=?7,writer_username=?8,writer_secret=?9,kind=?10,database_name=?11,database_names_json=?12,metadata_json=NULL,checked_at=NULL,probe_error=NULL,revision=revision+1 WHERE id=?1",params![id,input.name.trim(),input.host,input.port,input.version,input.reader_username,reader,input.writer_username,writer,input.kind,database,databases_json])?;
        }
        drop(conn);
        self.instance(&id)
    }
    pub(crate) fn delete_instance(&self, actor: i64, id: &str) -> Result<()> {
        let conn = self.db()?;
        admin(&conn, actor)?;
        if conn.execute("DELETE FROM instances WHERE id=?1", [id])? == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }
    pub(crate) fn probe_instance(&self, actor: i64, id: &str) -> Result<Instance> {
        let (d, password, revision) = {
            let conn = self.db()?;
            admin(&conn, actor)?;
            let d = conn
                .query_row(
                    &format!("SELECT {FIELDS} FROM instances WHERE id=?1"),
                    [id],
                    instance_row,
                )
                .optional()?
                .ok_or(Error::NotFound)?;
            let (value, revision): (Option<Vec<u8>>, i64) = conn.query_row(
                "SELECT reader_secret,revision FROM instances WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let value = value.ok_or(Error::Invalid("请先配置读取账号和密码"))?;
            let password = secrets::unseal(&self.cipher, &value, &format!("{id}:reader"))?;
            (d, password, revision)
        };
        let result = probe(&d, &password);
        let (metadata, error) = match result {
            Ok(value) => (
                Some(serde_json::to_string(&value).map_err(|_| Error::Internal)?),
                None,
            ),
            Err(message) => (None, Some(message)),
        };
        let conn = self.db()?;
        admin(&conn, actor)?;
        // A changed probe result is part of the instance identity used by task
        // plans.  Advance the revision only when the observed metadata or
        // probe error actually changes, so a repeated health check does not
        // unnecessarily invalidate every task.
        if conn.execute("UPDATE instances SET metadata_json=?1,probe_error=?2,checked_at=?3,revision=revision+CASE WHEN metadata_json IS NOT ?1 OR probe_error IS NOT ?2 THEN 1 ELSE 0 END WHERE id=?4 AND revision=?5",params![metadata,error,now(),id,revision])?==0 {return Err(Error::Conflict("探测期间实例已被修改，请重新探测"));}
        drop(conn);
        self.instance(id)
    }
}
fn probe(d: &Instance, password: &str) -> std::result::Result<Metadata, &'static str> {
    if d.kind == "postgresql" {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "无法启动数据库探测")?;
        return runtime
            .block_on(postgresql_15::metadata(
                &d.host,
                d.port,
                &d.database,
                &d.reader_username,
                password,
            ))
            .map(Metadata::Postgresql)
            .map_err(
                |_| "PostgreSQL 15 连接或元信息读取失败，请检查版本、地址、连接数据库及读取账号",
            );
    }
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(d.host.clone()))
        .tcp_port(d.port)
        .user(Some(d.reader_username.clone()))
        .pass(Some(password))
        .tcp_connect_timeout(Some(Duration::from_secs(3)))
        .read_timeout(Some(Duration::from_secs(3)))
        .write_timeout(Some(Duration::from_secs(3)));
    let mut conn = Conn::new(opts).map_err(|_| "连接失败，请检查地址、读取账号和密码")?;
    let (version,log_bin,format,row_image,gtid):(String,u8,String,String,String)=conn.query_first("SELECT @@version, @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.gtid_mode")
        .map_err(|_|"读取元信息失败，请检查读取账号权限")?.ok_or("未获得实例元信息")?;
    if !version.starts_with(&format!("{}.", d.version)) {
        return Err("实际 MySQL 版本与实例配置不符");
    }
    Ok(Metadata::Mysql {
        server_version: version,
        log_bin: log_bin != 0,
        binlog_format: format,
        binlog_row_image: row_image,
        gtid_mode: gtid,
    })
}
