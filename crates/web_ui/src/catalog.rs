//! Read-only database discovery for configuring a Replication Route.
use crate::{
    Store,
    auth::admin,
    error::{Error, Result},
    model::Metadata,
    registry::{SinkRegistry, SourceRegistry},
    secrets,
};
use mysql_driver::{Conn, OptsBuilder, prelude::Queryable};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sqlx::{
    Connection as _, PgConnection, Row as _,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{collections::BTreeMap, time::Duration};

fn postgres_block_on<F>(runtime: &tokio::runtime::Runtime, future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| runtime.block_on(future))
                .join()
                .expect("PostgreSQL catalog worker panicked")
        })
    } else {
        runtime.block_on(future)
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum EndpointRole {
    Source,
    Sink,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogQuery {
    pub role: EndpointRole,
    pub schema: Option<String>,
    pub database: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CatalogColumn {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub extra: String,
    pub collation: Option<String>,
    pub default_value: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CatalogTable {
    pub schema: String,
    pub name: String,
    pub engine: String,
    pub columns: Vec<CatalogColumn>,
    pub primary_key: Vec<String>,
}

impl CatalogTable {
    pub fn unavailable_reason(&self) -> Option<&'static str> {
        if !self.engine.eq_ignore_ascii_case("InnoDB")
            && !self.engine.eq_ignore_ascii_case("PostgreSQL")
        {
            return Some("仅支持 InnoDB 或 PostgreSQL 普通表");
        }
        if self.primary_key.is_empty() {
            return Some("需要主键");
        }
        if self.columns.is_empty() {
            return Some("无法读取列定义");
        }
        None
    }
}

#[derive(Serialize)]
pub(crate) struct Catalog {
    pub revision: i64,
    pub server_uuid: String,
    pub database: Option<String>,
    pub metadata: Metadata,
    pub schemas: Vec<String>,
    pub tables: Vec<CatalogTable>,
}

pub(crate) struct CatalogConnection {
    pub revision: i64,
    pub server_uuid: String,
    pub database: Option<String>,
    pub metadata: Metadata,
    conn: CatalogBackend,
}

enum CatalogBackend {
    Mysql(Conn),
    Postgresql {
        runtime: Option<tokio::runtime::Runtime>,
        conn: PgConnection,
    },
}
impl Drop for CatalogBackend {
    fn drop(&mut self) {
        if let CatalogBackend::Postgresql { runtime, .. } = self
            && let Some(runtime) = runtime.take()
            && tokio::runtime::Handle::try_current().is_ok()
        {
            let _ = std::thread::spawn(move || drop(runtime)).join();
        }
    }
}

impl Store {
    #[cfg(test)]
    pub(crate) fn catalog_connection(
        &self,
        actor: i64,
        id: &str,
        role: EndpointRole,
    ) -> Result<CatalogConnection> {
        self.catalog_connection_for_database(actor, id, role, None)
    }

    pub(crate) fn catalog_connection_for_database(
        &self,
        actor: i64,
        id: &str,
        role: EndpointRole,
        requested_database: Option<&str>,
    ) -> Result<CatalogConnection> {
        // Release the SQLite lock before doing network I/O.
        let (kind, host, port, version, database, username, password, revision) = {
            let conn = self.db()?;
            admin(&conn, actor)?;
            let kind: String = conn
                .query_row("SELECT kind FROM instances WHERE id=?1", [id], |r| r.get(0))
                .optional()?
                .ok_or(Error::NotFound)?;
            let field = if role == EndpointRole::Source {
                "reader"
            } else {
                "writer"
            };
            let (host, port, version, default_database, databases_json, username, sealed, revision): (String, u16, String, String, String, String, Option<Vec<u8>>, i64) = conn.query_row(
                &format!("SELECT host,port,version,database_name,database_names_json,{field}_username,{field}_secret,revision FROM instances WHERE id=?1"), [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)),
            ).optional()?.ok_or(Error::NotFound)?;
            let configured_databases: Vec<String> = serde_json::from_str(&databases_json)
                .unwrap_or_else(|_| {
                    if default_database.is_empty() {
                        Vec::new()
                    } else {
                        vec![default_database.clone()]
                    }
                });
            let database = if kind == "postgresql" {
                let selected = requested_database
                    .map(str::to_owned)
                    .or_else(|| configured_databases.first().cloned())
                    .ok_or(Error::Invalid("PostgreSQL 实例尚未配置连接数据库"))?;
                if !configured_databases.iter().any(|value| value == &selected) {
                    return Err(Error::Invalid("所选 PostgreSQL 连接数据库未在实例配置中"));
                }
                selected
            } else {
                String::new()
            };
            let sealed = sealed.ok_or(Error::Invalid(
                "请先为源实例配置读取账号，为目的实例配置写入账号",
            ))?;
            let password = secrets::unseal(&self.cipher, &sealed, &format!("{id}:{field}"))?;
            (
                kind, host, port, version, database, username, password, revision,
            )
        };
        let registered = match role {
            EndpointRole::Source => SourceRegistry.find(&kind, &version),
            EndpointRole::Sink => SinkRegistry.find(&kind, &version),
        }
        .ok_or(Error::Invalid("Web 未注册该数据库连接器"))?;
        if kind == "postgresql" {
            let database_for_connection = database.clone();
            let (runtime, conn, metadata) = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| Error::Internal)?;
                let metadata = match version.as_str() {
                    "15" => runtime.block_on(postgresql_15::metadata(
                        &host,
                        port,
                        &database_for_connection,
                        &username,
                        &password,
                    )),
                    "16" => runtime.block_on(postgresql_16::metadata(
                        &host,
                        port,
                        &database_for_connection,
                        &username,
                        &password,
                    )),
                    "17" => runtime.block_on(postgresql_17::metadata(
                        &host,
                        port,
                        &database_for_connection,
                        &username,
                        &password,
                    )),
                    _ => return Err(Error::Invalid("未注册的 PostgreSQL SourceAdapter")),
                }
                .map_err(|_| Error::Invalid("读取 PostgreSQL 配置失败，请检查账号权限"))?;
                let options = PgConnectOptions::new()
                    .host(&host)
                    .port(port)
                    .database(&database_for_connection)
                    .username(&username)
                    .password(&password)
                    .ssl_mode(PgSslMode::Prefer);
                let conn = runtime
                    .block_on(PgConnection::connect_with(&options))
                    .map_err(|_| Error::Invalid("数据库连接失败，请检查实例地址和对应账号权限"))?;
                Ok::<_, Error>((runtime, conn, metadata))
            })
            .join()
            .map_err(|_| Error::Internal)??;
            return Ok(CatalogConnection {
                revision,
                server_uuid: format!("postgresql:{id}"),
                database: Some(database),
                metadata: Metadata::Postgresql(metadata),
                conn: CatalogBackend::Postgresql {
                    runtime: Some(runtime),
                    conn,
                },
            });
        }
        if registered.identity.kind != "mysql" {
            return Err(Error::Invalid("Web 同步任务不支持该数据库类型"));
        }
        let mut conn = Conn::new(
            OptsBuilder::new()
                .ip_or_hostname(Some(host))
                .tcp_port(port)
                .user(Some(username))
                .pass(Some(password))
                .tcp_connect_timeout(Some(Duration::from_secs(3)))
                .read_timeout(Some(Duration::from_secs(5)))
                .write_timeout(Some(Duration::from_secs(5))),
        )
        .map_err(|_| Error::Invalid("数据库连接失败，请检查实例地址和对应账号权限"))?;
        let (actual_version, log_bin, format, row_image, gtid, server_uuid): (String,u8,String,String,String,String) = conn.query_first(
            "SELECT @@version,@@GLOBAL.log_bin,@@GLOBAL.binlog_format,@@GLOBAL.binlog_row_image,@@GLOBAL.gtid_mode,@@server_uuid")
            .map_err(|_| Error::Invalid("读取数据库配置失败，请检查账号权限"))?.ok_or(Error::Invalid("未获得数据库配置"))?;
        if !actual_version.starts_with(&format!("{version}.")) {
            return Err(Error::Invalid("实际 MySQL 版本与实例配置不符"));
        }
        Ok(CatalogConnection {
            revision,
            server_uuid,
            database: None,
            metadata: Metadata::Mysql {
                server_version: actual_version,
                log_bin: log_bin != 0,
                binlog_format: format,
                binlog_row_image: row_image,
                gtid_mode: gtid,
            },
            conn: CatalogBackend::Mysql(conn),
        })
    }

    pub(crate) fn catalog(&self, actor: i64, id: &str, query: CatalogQuery) -> Result<Catalog> {
        let mut db =
            self.catalog_connection_for_database(actor, id, query.role, query.database.as_deref())?;
        let schemas = db.schemas()?;
        let tables = if let Some(schema) = query.schema {
            if !schemas.contains(&schema) {
                return Err(Error::Invalid("数据库不存在或当前账号无权访问"));
            }
            db.tables(&schema)?
        } else {
            Vec::new()
        };
        Ok(Catalog {
            revision: db.revision,
            server_uuid: db.server_uuid,
            database: db.database,
            metadata: db.metadata,
            schemas,
            tables,
        })
    }
}

impl CatalogConnection {
    pub(crate) fn schemas(&mut self) -> Result<Vec<String>> {
        match &mut self.conn {
            CatalogBackend::Mysql(conn) => conn.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE LOWER(SCHEMA_NAME) NOT IN ('information_schema','performance_schema','mysql','sys','cdc') ORDER BY SCHEMA_NAME")
                .map_err(|_| Error::Invalid("读取数据库列表失败，请检查账号权限")),
            CatalogBackend::Postgresql { runtime, conn } => postgres_block_on(
                runtime.as_ref().expect("catalog runtime"),
                async {
                    sqlx::query_scalar::<_, String>(
                        "SELECT schema_name FROM information_schema.schemata
                         WHERE schema_name NOT IN ('information_schema','pg_catalog','cdc')
                           AND schema_name NOT LIKE 'pg_toast%' AND schema_name NOT LIKE 'pg_temp_%'
                         ORDER BY schema_name",
                    )
                    .fetch_all(conn)
                    .await
                },
            )
                .map_err(|_| Error::Invalid("读取 PostgreSQL schema 列表失败，请检查账号权限")),
        }
    }

    pub(crate) fn tables(&mut self, schema: &str) -> Result<Vec<CatalogTable>> {
        let CatalogBackend::Mysql(conn) = &mut self.conn else {
            return self.postgresql_tables(schema);
        };
        let rows: Vec<(String,String)> = conn.exec(
            "SELECT TABLE_NAME,COALESCE(ENGINE,'') FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (schema,))
            .map_err(|_| Error::Invalid("读取表列表失败，请检查账号权限"))?;
        let mut tables: BTreeMap<String, CatalogTable> = rows
            .into_iter()
            .map(|(name, engine)| {
                (
                    name.clone(),
                    CatalogTable {
                        schema: schema.into(),
                        name,
                        engine,
                        columns: vec![],
                        primary_key: vec![],
                    },
                )
            })
            .collect();
        type ColumnRow = (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
        );
        let columns: Vec<ColumnRow> = conn.exec(
            "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,EXTRA,COLLATION_NAME,COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME,ORDINAL_POSITION", (schema,))
            .map_err(|_| Error::Invalid("读取列定义失败，请检查账号权限"))?;
        for (table, name, column_type, nullable, extra, collation, default_value) in columns {
            if let Some(table) = tables.get_mut(&table) {
                table.columns.push(CatalogColumn {
                    name,
                    column_type,
                    nullable: nullable == "YES",
                    extra,
                    collation,
                    default_value,
                });
            }
        }
        let keys: Vec<(String,String)> = conn.exec(
            "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND INDEX_NAME='PRIMARY' ORDER BY TABLE_NAME,SEQ_IN_INDEX", (schema,))
            .map_err(|_| Error::Invalid("读取主键失败，请检查账号权限"))?;
        for (table, column) in keys {
            if let Some(table) = tables.get_mut(&table) {
                table.primary_key.push(column);
            }
        }
        Ok(tables.into_values().collect())
    }

    fn postgresql_tables(&mut self, schema: &str) -> Result<Vec<CatalogTable>> {
        let CatalogBackend::Postgresql { runtime, conn } = &mut self.conn else {
            unreachable!()
        };
        postgres_block_on(runtime.as_ref().expect("catalog runtime"), async {
            let names = sqlx::query_scalar::<_, String>(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                     WHERE n.nspname=$1 AND c.relkind='r' AND c.relpersistence='p'
                     ORDER BY c.relname",
            )
            .bind(schema)
            .fetch_all(&mut *conn)
            .await
            .map_err(|_| Error::Invalid("读取 PostgreSQL 表列表失败，请检查账号权限"))?;
            let mut tables: BTreeMap<String, CatalogTable> = names
                .into_iter()
                .map(|name| {
                    (
                        name.clone(),
                        CatalogTable {
                            schema: schema.into(),
                            name,
                            engine: "PostgreSQL".into(),
                            columns: vec![],
                            primary_key: vec![],
                        },
                    )
                })
                .collect();
            let columns = sqlx::query(
                "SELECT c.table_name,c.column_name,
                            format_type(a.atttypid,a.atttypmod) AS column_type,
                            c.is_nullable,c.is_generated,c.collation_name,c.column_default
                     FROM information_schema.columns c
                     JOIN pg_namespace n ON n.nspname=c.table_schema
                     JOIN pg_class cl ON cl.relnamespace=n.oid AND cl.relname=c.table_name
                     JOIN pg_attribute a ON a.attrelid=cl.oid AND a.attname=c.column_name
                     WHERE c.table_schema=$1 ORDER BY c.table_name,c.ordinal_position",
            )
            .bind(schema)
            .fetch_all(&mut *conn)
            .await
            .map_err(|_| Error::Invalid("读取 PostgreSQL 字段失败，请检查账号权限"))?;
            for row in columns {
                let table_name: String = row.try_get("table_name").map_err(|_| Error::Internal)?;
                if let Some(table) = tables.get_mut(&table_name) {
                    let generated: String =
                        row.try_get("is_generated").map_err(|_| Error::Internal)?;
                    table.columns.push(CatalogColumn {
                        name: row.try_get("column_name").map_err(|_| Error::Internal)?,
                        column_type: row.try_get("column_type").map_err(|_| Error::Internal)?,
                        nullable: row
                            .try_get::<String, _>("is_nullable")
                            .map_err(|_| Error::Internal)?
                            == "YES",
                        extra: if generated == "ALWAYS" {
                            "generated".into()
                        } else {
                            String::new()
                        },
                        collation: row.try_get("collation_name").map_err(|_| Error::Internal)?,
                        default_value: row
                            .try_get("column_default")
                            .map_err(|_| Error::Internal)?,
                    });
                }
            }
            let keys = sqlx::query(
                "SELECT c.relname AS table_name,a.attname AS column_name
                     FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid
                     JOIN pg_namespace n ON n.oid=c.relnamespace
                     CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS k(attnum,ord)
                     JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum
                     WHERE p.contype='p' AND n.nspname=$1 ORDER BY c.relname,k.ord",
            )
            .bind(schema)
            .fetch_all(&mut *conn)
            .await
            .map_err(|_| Error::Invalid("读取 PostgreSQL 主键失败，请检查账号权限"))?;
            for row in keys {
                let table_name: String = row.try_get("table_name").map_err(|_| Error::Internal)?;
                if let Some(table) = tables.get_mut(&table_name) {
                    table
                        .primary_key
                        .push(row.try_get("column_name").map_err(|_| Error::Internal)?);
                }
            }
            Ok(tables.into_values().collect())
        })
    }
}
