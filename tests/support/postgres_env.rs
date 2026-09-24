use sqlx::postgres::{PgConnectOptions, PgSslMode};
use std::env;

pub fn env_name(version: &str, key: &str) -> String {
    if version == "15" {
        format!("PG_CDC_{key}")
    } else {
        format!("PG_CDC{version}_{key}")
    }
}

pub fn setting(version: &str, key: &str, default: &str) -> String {
    env::var(env_name(version, key)).unwrap_or_else(|_| default.into())
}

pub fn options(version: &str, user: &str, password: &str) -> PgConnectOptions {
    PgConnectOptions::new()
        .host(&setting(version, "HOST", "192.168.0.10"))
        .port(
            setting(version, "PORT", "54321")
                .parse()
                .expect("invalid PostgreSQL test port"),
        )
        .username(user)
        .password(password)
        .database("CDC_test")
        .ssl_mode(PgSslMode::Prefer)
}
