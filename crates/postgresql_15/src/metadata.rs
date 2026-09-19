//! Read-only instance inspection; never creates publications, slots or tables.
use crate::Result;
use serde::{Deserialize, Serialize};
use sqlx::{
    Connection, PgConnection, Row,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub server_version: String,
    pub database: String,
    pub server_encoding: String,
    pub wal_level: String,
    pub max_replication_slots: i32,
    pub max_wal_senders: i32,
    pub replication_slots: i64,
    pub can_replicate: bool,
    pub in_recovery: bool,
}

pub async fn metadata(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<Metadata> {
    // Bound the entire inspection, including connection, authentication and queries.
    tokio::time::timeout(Duration::from_secs(8), async {
        let options = PgConnectOptions::new().host(host).port(port).database(database)
            .username(username).password(password).ssl_mode(PgSslMode::Prefer);
        let mut conn = PgConnection::connect_with(&options).await?;
        let row = sqlx::query(
            "SELECT current_setting('server_version') AS version,
             current_setting('server_version_num')::integer AS version_num,
             current_database() AS database, current_setting('server_encoding') AS encoding,
             current_setting('wal_level') AS wal_level,
             current_setting('max_replication_slots')::integer AS max_slots,
             current_setting('max_wal_senders')::integer AS max_senders,
             (SELECT count(*) FROM pg_replication_slots WHERE database=current_database()) AS slots,
             (SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname=current_user) AS can_replicate,
             pg_is_in_recovery() AS in_recovery"
        ).fetch_one(&mut conn).await?;
        if row.try_get::<i32,_>("version_num")? / 10000 != 15 {
            return Err(crate::invalid("expected PostgreSQL 15"));
        }
        let metadata = Metadata {
            server_version: row.try_get("version")?, database: row.try_get("database")?,
            server_encoding: row.try_get("encoding")?, wal_level: row.try_get("wal_level")?,
            max_replication_slots: row.try_get("max_slots")?, max_wal_senders: row.try_get("max_senders")?,
            replication_slots: row.try_get("slots")?, can_replicate: row.try_get("can_replicate")?,
            in_recovery: row.try_get("in_recovery")?,
        };
        conn.close().await?;
        Ok(metadata)
    }).await?
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
