use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue, Operation,
    RowChange, Source, SourceCursor, validate,
};
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};
use std::env;

fn cursor(position: u32) -> SourceCursor {
    let file = "mysql-bin.000001";
    let mut raw = file.as_bytes().to_vec();
    raw.push(0);
    raw.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(raw),
        display: format!("{file}:{position}"),
    }
}

fn columns(message: &str) -> Vec<ColumnDatum> {
    vec![
        ColumnDatum {
            ordinal: 0,
            name: "id".into(),
            native_type: "bigint unsigned".into(),
            primary_key_ordinal: Some(0),
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::Integer {
                signed: false,
                bits: 64,
                value: "900001".into(),
            }),
        },
        ColumnDatum {
            ordinal: 1,
            name: "message".into(),
            native_type: "varchar(255)".into(),
            primary_key_ordinal: None,
            generated: false,
            collation: Some("utf8mb4_unicode_ci".into()),
            datum: Datum::Value(LogicalValue::Text {
                charset: "utf8mb4".into(),
                bytes_base64url: URL_SAFE_NO_PAD.encode(message.as_bytes()),
                text: Some(message.into()),
            }),
        },
        ColumnDatum {
            ordinal: 2,
            name: "amount".into(),
            native_type: "decimal(12,2)".into(),
            primary_key_ordinal: None,
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::Decimal {
                unscaled: "1234".into(),
                scale: 2,
            }),
        },
        ColumnDatum {
            ordinal: 3,
            name: "metadata".into(),
            native_type: "json".into(),
            primary_key_ordinal: None,
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::Json {
                value: JsonValue::Object(vec![
                    JsonEntry {
                        key: "stage".into(),
                        value: JsonValue::String(message.into()),
                    },
                    JsonEntry {
                        key: "exact".into(),
                        value: JsonValue::Decimal {
                            unscaled: "100".into(),
                            scale: 2,
                        },
                    },
                    JsonEntry {
                        key: "approximate".into(),
                        value: JsonValue::DoubleBits("3ff0000000000000".into()),
                    },
                ]),
            }),
        },
        ColumnDatum {
            ordinal: 4,
            name: "changed_at".into(),
            native_type: "datetime(6)".into(),
            primary_key_ordinal: None,
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 10,
                hour: 8,
                minute: 30,
                second: 0,
                microsecond: 123456,
            }),
        },
        ColumnDatum {
            ordinal: 5,
            name: "message_len".into(),
            native_type: "int".into(),
            primary_key_ordinal: None,
            generated: true,
            collation: None,
            datum: Datum::Value(LogicalValue::Integer {
                signed: true,
                bits: 32,
                value: message.len().to_string(),
            }),
        },
    ]
}

fn transaction(duplicate_insert: bool) -> change_event::ValidatedTransaction {
    let first = RowChange {
        database: None,
        operation: Operation::Insert,
        schema: "CDC_test".into(),
        table: "cdc_sink_smoke".into(),
        source_cursor: cursor(110),
        source_timestamp: 1_700_000_000,
        schema_basis: "test_fixture".into(),
        before: None,
        after: Some(columns("insert")),
    };
    let changes = if duplicate_insert {
        vec![
            first,
            RowChange {
                database: None,
                operation: Operation::Insert,
                schema: "CDC_test".into(),
                table: "cdc_sink_smoke".into(),
                source_cursor: cursor(120),
                source_timestamp: 1_700_000_000,
                schema_basis: "test_fixture".into(),
                before: None,
                after: Some(columns("duplicate")),
            },
        ]
    } else {
        vec![
            first,
            RowChange {
                database: None,
                operation: Operation::Update,
                schema: "CDC_test".into(),
                table: "cdc_sink_smoke".into(),
                source_cursor: cursor(120),
                source_timestamp: 1_700_000_000,
                schema_basis: "test_fixture".into(),
                before: Some(columns("insert")),
                after: Some(columns("updated")),
            },
            RowChange {
                database: None,
                operation: Operation::Delete,
                schema: "CDC_test".into(),
                table: "cdc_sink_smoke".into(),
                source_cursor: cursor(130),
                source_timestamp: 1_700_000_000,
                schema_basis: "test_fixture".into(),
                before: Some(columns("updated")),
                after: None,
            },
        ]
    };
    validate(ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44-log".into(),
            id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
        },
        id: "430c326c-ab91-11f1-a23b-0242ac160004:42".into(),
        begin_cursor: cursor(100),
        commit_cursor: cursor(200),
        changes,
    })
    .unwrap()
}

fn connection(port: u16, password: &str) -> Conn {
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
    let mut conn = connection(port, password);
    conn.query_drop(
        "CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
    )
    .unwrap();
    conn.query_drop("DROP TABLE IF EXISTS CDC_test.cdc_sink_smoke")
        .unwrap();
    conn.query_drop(
        "CREATE TABLE CDC_test.cdc_sink_smoke (
           id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
           message VARCHAR(255) NOT NULL,
           amount DECIMAL(12,2) NOT NULL,
           metadata JSON NOT NULL,
           changed_at DATETIME(6) NOT NULL,
           message_len INT GENERATED ALWAYS AS (CHAR_LENGTH(message)) STORED
         ) ENGINE=InnoDB",
    )
    .unwrap();
}

fn assert_empty(port: u16, password: &str) {
    let count: u64 = connection(port, password)
        .query_first("SELECT COUNT(*) FROM CDC_test.cdc_sink_smoke")
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
#[ignore = "requires the three configured MySQL test instances"]
fn writes_and_rolls_back_on_mysql_5_7_8_0_and_8_4() {
    let password = env::var("CDC_MYSQL_WRITER_PASSWORD").expect("set CDC_MYSQL_WRITER_PASSWORD");
    macro_rules! verify {
        ($adapter:ident, $port:literal) => {{
            prepare($port, &password);
            let config = $adapter::TargetConfig {
                host: "192.168.0.10".into(),
                port: $port,
                user: "mysql_writer".into(),
                password: password.clone(),
            };
            let plan = $adapter::sql(&transaction(false)).unwrap();
            let result = $adapter::execute(&config, &plan).unwrap();
            assert_eq!(result.statements_executed, 3);
            assert_empty($port, &password);

            let duplicate = $adapter::sql(&transaction(true)).unwrap();
            assert!($adapter::execute(&config, &duplicate).is_err());
            assert_empty($port, &password);
        }};
    }
    verify!(mysql_5_7, 33061);
    verify!(mysql_8_0, 33062);
    verify!(mysql_8_4, 33063);
}
