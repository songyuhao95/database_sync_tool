//! Read-only instance inspection; never creates publications, slots or tables.
use crate::{Result, type_mapping::SourceExtension};
use change_event::ServerBuildIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::{
    Connection, PgConnection, Row,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub server_version: String,
    /// Exact server-build evidence used by SourceTypeMapping qualification.
    #[serde(default)]
    pub server_build: Option<ServerBuildIdentity>,
    pub database: String,
    #[serde(default)]
    pub schemas: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<SourceExtension>,
    pub server_encoding: String,
    pub wal_level: String,
    pub max_replication_slots: i32,
    pub max_wal_senders: i32,
    pub replication_slots: i64,
    pub can_replicate: bool,
    pub in_recovery: bool,
    /// Digest of settings that can change PostgreSQL value semantics.
    #[serde(default)]
    pub environment_fingerprint: Option<String>,
}

pub async fn metadata(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<Metadata> {
    metadata_for_version(host, port, database, username, password, 15).await
}

pub async fn metadata_for_version(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    expected_major: u16,
) -> Result<Metadata> {
    // Bound the entire inspection, including connection, authentication and queries.
    tokio::time::timeout(Duration::from_secs(8), async {
        let options = PgConnectOptions::new().host(host).port(port).database(database)
            .username(username).password(password).ssl_mode(PgSslMode::Prefer);
        let mut conn = PgConnection::connect_with(&options).await?;
        let row = sqlx::query(
            "SELECT current_setting('server_version') AS version,
             current_setting('server_version_num')::integer AS version_num,
             version() AS build,
             current_database() AS database, current_setting('server_encoding') AS encoding,
             current_setting('wal_level') AS wal_level,
             current_setting('TimeZone') AS timezone,
             current_setting('DateStyle') AS datestyle,
             current_setting('bytea_output') AS bytea_output,
             current_setting('extra_float_digits') AS extra_float_digits,
             current_setting('row_security') AS row_security,
             current_setting('max_replication_slots')::integer AS max_slots,
             current_setting('max_wal_senders')::integer AS max_senders,
             (SELECT count(*) FROM pg_replication_slots WHERE database=current_database()) AS slots,
             (SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname=current_user) AS can_replicate,
             pg_is_in_recovery() AS in_recovery"
        ).fetch_one(&mut conn).await?;
        if row.try_get::<i32,_>("version_num")? / 10000 != i32::from(expected_major) {
            return Err(crate::invalid(format!(
                "expected PostgreSQL {expected_major}"
            )));
        }
        let server_version: String = row.try_get("version")?;
        let build: String = row.try_get("build")?;
        let schemas = sqlx::query_scalar::<_, String>(
            "SELECT n.nspname
               FROM pg_namespace n
              WHERE n.nspname NOT LIKE 'pg_toast%'
                AND has_schema_privilege(current_user, n.oid, 'USAGE')
              ORDER BY n.nspname",
        )
        .fetch_all(&mut conn)
        .await?;
        let extensions = sqlx::query(
            "SELECT e.extname,e.extversion,n.nspname AS schema
               FROM pg_extension e
               JOIN pg_namespace n ON n.oid=e.extnamespace
              ORDER BY e.extname",
        )
        .fetch_all(&mut conn)
        .await?
        .into_iter()
        .map(|extension| {
            Ok(SourceExtension {
                name: extension.try_get("extname")?,
                version: extension.try_get("extversion")?,
                schema: extension.try_get("schema")?,
                installed: true,
            })
        })
        .collect::<Result<Vec<_>>>()?;
        let environment_fingerprint = environment_fingerprint([
            row.try_get::<String, _>("database")?,
            row.try_get::<String, _>("encoding")?,
            row.try_get::<String, _>("wal_level")?,
            row.try_get::<String, _>("timezone")?,
            row.try_get::<String, _>("datestyle")?,
            row.try_get::<String, _>("bytea_output")?,
            row.try_get::<String, _>("extra_float_digits")?,
            row.try_get::<String, _>("row_security")?,
        ]);
        let metadata = Metadata {
            server_version: server_version.clone(),
            server_build: Some(ServerBuildIdentity::new(
                "postgresql",
                "community",
                server_version,
                build,
            )),
            database: row.try_get("database")?,
            schemas,
            extensions,
            server_encoding: row.try_get("encoding")?, wal_level: row.try_get("wal_level")?,
            max_replication_slots: row.try_get("max_slots")?, max_wal_senders: row.try_get("max_senders")?,
            replication_slots: row.try_get("slots")?, can_replicate: row.try_get("can_replicate")?,
            in_recovery: row.try_get("in_recovery")?,
            environment_fingerprint: Some(environment_fingerprint),
        };
        conn.close().await?;
        Ok(metadata)
    }).await?
}

fn environment_fingerprint(values: impl IntoIterator<Item = String>) -> String {
    let mut digest = Sha256::new();
    for value in values {
        digest.update(value.len().to_string().as_bytes());
        digest.update([0]);
        digest.update(value.as_bytes());
        digest.update([0xff]);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Discover databases that the configured user can connect to.
///
/// PostgreSQL connections are database-scoped, so discovery uses the standard
/// maintenance database and never requires the caller to type a database name.
pub async fn databases(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut last_error = None;
        for maintenance_database in ["postgres", "template1"] {
            let options = PgConnectOptions::new()
                .host(host)
                .port(port)
                .database(maintenance_database)
                .username(username)
                .password(password)
                .ssl_mode(PgSslMode::Prefer);
            let mut conn = match PgConnection::connect_with(&options).await {
                Ok(conn) => conn,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let result = sqlx::query_scalar::<_, String>(
                "SELECT datname
                 FROM pg_database
                 WHERE datallowconn
                   AND NOT datistemplate
                   AND has_database_privilege(current_user, datname, 'CONNECT')
                 ORDER BY datname",
            )
            .fetch_all(&mut conn)
            .await;
            conn.close().await?;
            return result.map_err(Into::into);
        }
        Err(last_error
            .map(Into::into)
            .unwrap_or_else(|| crate::invalid("无法连接 PostgreSQL 维护数据库")))
    })
    .await?
}
