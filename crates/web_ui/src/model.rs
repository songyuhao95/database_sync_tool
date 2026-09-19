use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize)]
pub struct User {
    pub id: i64,
    pub owner: String,
    pub username: String,
    pub role: String,
    pub theme: String,
    pub note: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewUser {
    pub owner: String,
    pub username: String,
    pub password: String,
    pub role: String,
    #[serde(default)]
    pub note: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserUpdate {
    pub owner: String,
    pub password: Option<String>,
    #[serde(default)]
    pub note: String,
}
fn mysql_kind() -> String {
    "mysql".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceInput {
    pub name: String,
    pub host: String,
    pub port: u16,
    #[serde(default = "mysql_kind")]
    pub kind: String,
    pub version: String,
    #[serde(default)]
    pub database: String,
    /// New API shape for PostgreSQL. `database` remains readable for older
    /// clients; storage normalizes both forms into one list.
    #[serde(default)]
    pub databases: Vec<String>,
    #[serde(default)]
    pub reader_username: String,
    pub reader_password: Option<String>,
    #[serde(default)]
    pub writer_username: String,
    pub writer_password: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseDiscoveryInput {
    #[serde(default)]
    pub instance_id: Option<String>,
    pub host: String,
    pub port: u16,
    pub kind: String,
    pub version: String,
    pub reader_username: String,
    pub reader_password: String,
}
/// Untagged keeps historical MySQL metadata JSON readable and its API shape unchanged.
#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Metadata {
    Mysql {
        server_version: String,
        log_bin: bool,
        binlog_format: String,
        binlog_row_image: String,
        gtid_mode: String,
    },
    Postgresql(postgresql_15::Metadata),
}
#[derive(Clone, Serialize)]
pub struct Instance {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub kind: String,
    pub version: String,
    pub database: String,
    pub databases: Vec<String>,
    pub reader_username: String,
    pub writer_username: String,
    pub has_reader_password: bool,
    pub has_writer_password: bool,
    pub metadata: Option<Metadata>,
    pub checked_at: Option<i64>,
    pub probe_error: Option<String>,
}
