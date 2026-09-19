use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};
use std::env;

fn writer(port: u16, password: &str) -> Conn {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some("192.168.0.10"))
            .tcp_port(port)
            .user(Some("mysql_writer"))
            .pass(Some(password)),
    )
    .unwrap()
}

fn prepare(port: u16, password: &str) {
    let mut conn = writer(port, password);
    conn.query_drop(
        "CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
    )
    .unwrap();
    conn.query_drop("DROP TABLE IF EXISTS CDC_test.cdc_source_metadata_smoke")
        .unwrap();
    conn.query_drop(
        "CREATE TABLE CDC_test.cdc_source_metadata_smoke (
           id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
           payload VARCHAR(64) NOT NULL,
           payload_len INT GENERATED ALWAYS AS (CHAR_LENGTH(payload)) STORED
         ) ENGINE=InnoDB",
    )
    .unwrap();
}

#[test]
#[ignore = "requires the three configured MySQL test instances"]
fn captured_rows_include_primary_key_and_generated_column_roles() {
    let reader_password =
        env::var("CDC_MYSQL_READER_PASSWORD").expect("set CDC_MYSQL_READER_PASSWORD");
    let writer_password =
        env::var("CDC_MYSQL_WRITER_PASSWORD").expect("set CDC_MYSQL_WRITER_PASSWORD");

    macro_rules! verify {
        ($adapter:ident, $port:literal) => {{
            prepare($port, &writer_password);
            let mut config = $adapter::BinlogConfig::new(
                "192.168.0.10",
                $port,
                "mysql_reader",
                reader_password.clone(),
            );
            config.server_id = 2_500_000 + $port;
            let mut stream = $adapter::binlog(config).unwrap();

            writer($port, &writer_password)
                .query_drop(
                    "INSERT INTO CDC_test.cdc_source_metadata_smoke (id, payload)
                     VALUES (900002, 'metadata')",
                )
                .unwrap();

            let transaction = loop {
                let transaction = stream.next().expect("stream ended").expect("decode failed");
                if transaction
                    .changes
                    .iter()
                    .any(|change| change.table == "cdc_source_metadata_smoke")
                {
                    break transaction;
                }
            };
            let row = transaction
                .changes
                .iter()
                .find(|change| change.table == "cdc_source_metadata_smoke")
                .unwrap()
                .after
                .as_ref()
                .unwrap();
            assert_eq!(row[0].primary_key_ordinal, Some(0));
            assert!(!row[0].generated);
            assert_eq!(row[1].collation.as_deref(), Some("utf8mb4_unicode_ci"));
            assert!(row[2].generated);
            change_event::validate(transaction).unwrap();

            drop(stream);
            writer($port, &writer_password)
                .query_drop("DELETE FROM CDC_test.cdc_source_metadata_smoke WHERE id = 900002")
                .unwrap();
        }};
    }

    verify!(mysql_5_7, 33061);
    verify!(mysql_8_0, 33062);
    verify!(mysql_8_4, 33063);
}
