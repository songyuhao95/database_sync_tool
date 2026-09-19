use std::env;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, params};

fn main() -> Result<(), Box<dyn Error>> {
    let host = env::var("CDC_MYSQL_HOST").unwrap_or_else(|_| "192.168.0.10".to_owned());
    let port = env::var("CDC_MYSQL_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(33061);
    let user = env::var("CDC_MYSQL_WRITER_USER").unwrap_or_else(|_| "mysql_writer".to_owned());
    let password = env::var("CDC_MYSQL_WRITER_PASSWORD")
        .or_else(|_| env::var("CDC_MYSQL_PASSWORD"))
        .map_err(|_| "set CDC_MYSQL_WRITER_PASSWORD or CDC_MYSQL_PASSWORD")?;

    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password));
    let mut conn = Conn::new(opts)?;

    conn.query_drop(
        "CREATE DATABASE IF NOT EXISTS CDC_test
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
    )?;
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS CDC_test.binlog_reader_demo (
             id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
             message VARCHAR(255) NOT NULL,
             amount DECIMAL(12, 2) NOT NULL,
             metadata JSON NULL,
             changed_at DATETIME(6) NOT NULL
         ) ENGINE=InnoDB",
    )?;

    let token = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    conn.exec_drop(
        "INSERT INTO CDC_test.binlog_reader_demo
             (message, amount, metadata, changed_at)
         VALUES (:message, 12.34, JSON_OBJECT('stage', 'insert'), NOW(6))",
        params! { "message" => format!("binlog test {token}") },
    )?;
    let id = conn.last_insert_id();

    conn.exec_drop(
        "UPDATE CDC_test.binlog_reader_demo
            SET message = :message,
                amount = 56.78,
                metadata = JSON_OBJECT('stage', 'update'),
                changed_at = NOW(6)
          WHERE id = :id",
        params! {
            "id" => id,
            "message" => format!("binlog test {token} updated"),
        },
    )?;
    conn.exec_drop(
        "DELETE FROM CDC_test.binlog_reader_demo WHERE id = :id",
        params! { "id" => id },
    )?;

    println!("generated INSERT, UPDATE, DELETE in CDC_test.binlog_reader_demo (id={id})");
    Ok(())
}
