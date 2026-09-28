#![allow(dead_code)] // Shared by independently compiled integration-test binaries.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::*;
use mysql::{Conn, OptsBuilder, prelude::Queryable};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn setting(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}
pub fn password(role: &str) -> String {
    env::var(format!("CDC_MYSQL_{role}_PASSWORD"))
        .expect("set the MySQL test password environment variable")
}
pub fn port(key: &str, default: u16) -> u16 {
    env::var(key)
        .map(|p| p.parse().expect("invalid test port"))
        .unwrap_or(default)
}
pub fn connection(port: u16, reader: bool) -> Conn {
    let role = if reader { "READER" } else { "WRITER" };
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(setting("CDC_MYSQL_HOST", "192.168.0.10")))
            .tcp_port(port)
            .user(Some(setting(
                &format!("CDC_MYSQL_{role}_USER"),
                if reader {
                    "mysql_reader"
                } else {
                    "mysql_writer"
                },
            )))
            .pass(Some(password(role)))
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(15)))
            .write_timeout(Some(Duration::from_secs(15))),
    )
    .expect("test database connection")
}
pub struct TestTable {
    pub conn: Conn,
    pub name: String,
    cleaned: bool,
}
impl TestTable {
    pub fn create(port: u16) -> Self {
        let mut conn = connection(port, false);
        // Do not drop/reuse any pre-existing object, even one with a similar name.
        let tag = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("cdc_contract_{}_{tag}", std::process::id());
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci").unwrap();
        conn.query_drop(format!(
            "CREATE TABLE CDC_test.{name} (
            id BIGINT UNSIGNED NOT NULL,
            tenant INT UNSIGNED NOT NULL,
            message VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
            amount DECIMAL(30,6) NOT NULL,
            bytes VARBINARY(32) NOT NULL,
            note VARCHAR(255) NULL,
            changed_at DATETIME(6) NOT NULL,
            message_len INT GENERATED ALWAYS AS (CHAR_LENGTH(message)) STORED,
            PRIMARY KEY(tenant,id)
        ) ENGINE=InnoDB"
        ))
        .unwrap();
        println!("test object: CDC_test.{name}");
        Self {
            conn,
            name,
            cleaned: false,
        }
    }
    pub fn cleanup(&mut self) {
        self.conn
            .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
            .expect("clean up test table");
        self.cleaned = true;
    }
}
impl Drop for TestTable {
    fn drop(&mut self) {
        if !self.cleaned {
            // Best effort during a panic; normal cleanup failures fail the test.
            if let Err(e) = self
                .conn
                .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
            {
                eprintln!("cleanup failed for CDC_test.{}: {e}", self.name);
            }
        }
    }
}

pub struct GenericGeometryTable {
    pub conn: Conn,
    pub name: String,
    cleaned: bool,
}

impl GenericGeometryTable {
    pub fn create(port: u16) -> Self {
        let mut conn = connection(port, false);
        let tag = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("cdc_geometry_{}_{tag}", std::process::id());
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci").unwrap();
        conn.query_drop(format!(
            "CREATE TABLE CDC_test.{name} (
                id BIGINT NOT NULL PRIMARY KEY,
                shape GEOMETRY NOT NULL
            ) ENGINE=InnoDB"
        ))
        .unwrap();
        Self {
            conn,
            name,
            cleaned: false,
        }
    }

    pub fn cleanup(&mut self) {
        self.conn
            .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
            .expect("clean up generic geometry test table");
        self.cleaned = true;
    }
}

impl Drop for GenericGeometryTable {
    fn drop(&mut self) {
        if !self.cleaned
            && let Err(error) = self
                .conn
                .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
        {
            eprintln!("cleanup failed for CDC_test.{}: {error}", self.name);
        }
    }
}

pub struct AllMysqlTypesTable {
    pub conn: Conn,
    pub name: String,
    cleaned: bool,
}

impl AllMysqlTypesTable {
    pub fn create(port: u16) -> Self {
        let conn = connection(port, false);
        let tag = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("cdc_all_types_{}_{tag}", std::process::id());
        Self::create_named(conn, name)
    }

    pub fn create_named(mut conn: Conn, name: String) -> Self {
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            .unwrap();
        conn.query_drop(Self::create_statement(&name)).unwrap();
        Self {
            conn,
            name,
            cleaned: false,
        }
    }

    pub fn create_sink_named(mut conn: Conn, name: String) -> Self {
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            .unwrap();
        let statement = Self::create_statement(&name)
            .replace("DECIMAL(12,2) UNSIGNED", "DECIMAL(12,2)")
            // MySQL 5.7 may implicitly make the first TIMESTAMP NOT NULL;
            // keep target fixtures nullable so every source version can
            // write its full declared NULL domain.
            .replace(
                "timestamp_value TIMESTAMP(6)",
                "timestamp_value TIMESTAMP(6) NULL",
            )
            .replace("FLOAT(7,4)", "FLOAT")
            .replace("FLOAT(23)", "FLOAT")
            .replace("FLOAT(24)", "FLOAT")
            .replace("DOUBLE PRECISION(12,2)", "DOUBLE");
        conn.query_drop(statement).unwrap();
        Self {
            conn,
            name,
            cleaned: false,
        }
    }

    pub fn create_statement(name: &str) -> String {
        format!(
            "CREATE TABLE CDC_test.{name} (
                id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                tiny_signed TINYINT,
                tiny_unsigned TINYINT UNSIGNED,
                small_signed SMALLINT,
                small_unsigned SMALLINT UNSIGNED,
                medium_signed MEDIUMINT,
                medium_unsigned MEDIUMINT UNSIGNED,
                int_signed INT,
                int_unsigned INT UNSIGNED,
                big_signed BIGINT,
                big_unsigned BIGINT UNSIGNED,
                decimal_default DECIMAL,
                decimal_precision DECIMAL(10),
                decimal_scaled DECIMAL(30,6),
                decimal_unsigned DECIMAL(12,2) UNSIGNED,
                numeric_alias NUMERIC(12,3),
                float_plain FLOAT,
                float_precision FLOAT(23),
                float_precision_boundary FLOAT(24),
                float_scaled FLOAT(7,4),
                double_plain DOUBLE,
                double_precision DOUBLE PRECISION(12,2),
                bit_one BIT,
                bit_wide BIT(64),
                date_value DATE,
                datetime_value DATETIME(6),
                timestamp_value TIMESTAMP(6),
                time_value TIME(6),
                year_value YEAR,
                char_value CHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                varchar_value VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                tinytext_value TINYTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                text_value TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                mediumtext_value MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                longtext_value LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                binary_value BINARY(4),
                varbinary_value VARBINARY(32),
                tinyblob_value TINYBLOB,
                blob_value BLOB,
                mediumblob_value MEDIUMBLOB,
                longblob_value LONGBLOB,
                enum_value ENUM('alpha','beta','gamma') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                set_value SET('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                json_value JSON,
                geometry_value GEOMETRY,
                point_value POINT,
                linestring_value LINESTRING,
                polygon_value POLYGON,
                multipoint_value MULTIPOINT,
                multilinestring_value MULTILINESTRING,
                multipolygon_value MULTIPOLYGON,
                geometrycollection_value GEOMETRYCOLLECTION,
                null_marker VARCHAR(32) NULL,
                decimal_max_precision DECIMAL(65,30),
                float_negative_zero FLOAT,
                double_negative_zero DOUBLE,
                date_zero DATE,
                datetime_zero DATETIME(6),
                timestamp_zero TIMESTAMP(6) NULL DEFAULT NULL,
                latin1_value CHAR(2) CHARACTER SET latin1 COLLATE latin1_bin,
                binary_padding BINARY(4)
            ) ENGINE=InnoDB"
        )
    }

    pub fn populate(&mut self) {
        self.conn.query_drop("SET SESSION sql_mode = ''").unwrap();
        self.conn.query_drop(format!(
            "INSERT INTO CDC_test.{} VALUES (
                1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,
                1234567890, 1234567890, 12345678901234567890.123456, 123456789.12, 123456789.123,
                1.25, 1.5, 1.75, 12.3456, 1.125, 12.34,
                b'1', b'1010010110100101101001011010010110100101101001011010010110100101',
                '2024-02-29', '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.123456', '838:59:58.999999', 2024,
                'fixed', 'base', 'tiny text', 'text value', 'medium text', 'long text',
                X'01020304', X'00FF', X'01', X'0203', X'040506', X'070809',
                'beta', 'a,c', JSON_OBJECT('kind','mysql','value',1),
                ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'),
                ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'),
                ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
                ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'),
                ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))'),
                ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))'),
                NULL,
                99999999999999999999999999999999999.999999999999999999999999999999,
                -0e0, -0e0,
                '0000-00-00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00.000000',
                'é', X'01'
            )",
            self.name
        )).unwrap();
    }

    pub fn cleanup(&mut self) {
        self.conn
            .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
            .expect("clean up all-types test table");
        self.cleaned = true;
    }
}

impl Drop for AllMysqlTypesTable {
    fn drop(&mut self) {
        if !self.cleaned
            && let Err(error) = self
                .conn
                .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
        {
            eprintln!("cleanup failed for CDC_test.{}: {error}", self.name);
        }
    }
}

pub fn cursor(position: u32) -> SourceCursor {
    cursor_with_file("mysql-bin.000001", position)
}
pub fn cursor_with_file(file: &str, position: u32) -> SourceCursor {
    let mut raw = file.as_bytes().to_vec();
    raw.push(0);
    raw.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(raw),
        display: format!("{file}:{position}"),
    }
}
fn integer(value: &str, signed: bool, bits: u8) -> Datum {
    Datum::Value(LogicalValue::Integer {
        signed,
        bits,
        value: value.into(),
    })
}
pub fn columns(updated: bool) -> Vec<ColumnDatum> {
    // Independent fixed inputs shared by every target version.
    let message = if updated { "" } else { "中文'\\\n" };
    let values = [
        (
            "id",
            "bigint unsigned",
            Some(1),
            false,
            integer(
                if updated {
                    "42"
                } else {
                    "18446744073709551615"
                },
                false,
                64,
            ),
        ),
        (
            "tenant",
            "int unsigned",
            Some(0),
            false,
            integer("7", false, 32),
        ),
        (
            "message",
            "varchar(255)",
            None,
            false,
            Datum::Value(LogicalValue::Text {
                charset: "utf8mb4".into(),
                bytes_base64url: URL_SAFE_NO_PAD.encode(message),
                text: Some(message.into()),
            }),
        ),
        (
            "amount",
            "decimal(30,6)",
            None,
            false,
            Datum::Value(LogicalValue::Decimal {
                unscaled: if updated {
                    "-1"
                } else {
                    "12345678901234567890123456"
                }
                .into(),
                scale: 6,
            }),
        ),
        (
            "bytes",
            "varbinary(32)",
            None,
            false,
            Datum::Value(LogicalValue::Binary {
                bytes_base64url: "AP8".into(),
            }),
        ),
        ("note", "varchar(255)", None, false, Datum::Null),
        (
            "changed_at",
            "datetime(6)",
            None,
            false,
            Datum::Value(LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 14,
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 123456,
            }),
        ),
        (
            "message_len",
            "int",
            None,
            true,
            integer(if updated { "0" } else { "5" }, true, 32),
        ),
    ];
    values
        .into_iter()
        .enumerate()
        .map(
            |(ordinal, (name, native_type, primary_key_ordinal, generated, datum))| ColumnDatum {
                ordinal,
                name: name.into(),
                native_type: native_type.into(),
                primary_key_ordinal,
                generated,
                collation: if matches!(name, "message" | "note") {
                    Some("utf8mb4_unicode_ci".into())
                } else {
                    None
                },
                datum,
            },
        )
        .collect()
}
pub fn fixture(version: &str, table: &str) -> ChangeTransaction {
    let mut changes = Vec::new();
    for (i, operation) in [Operation::Insert, Operation::Update, Operation::Delete]
        .into_iter()
        .enumerate()
    {
        changes.push(RowChange {
            database: None,
            operation,
            schema: "CDC_test".into(),
            table: table.into(),
            source_cursor: cursor(110 + i as u32 * 10),
            source_timestamp: 1_700_000_000,
            schema_basis: "contract_fixture".into(),
            before: match operation {
                Operation::Insert => None,
                Operation::Update => Some(columns(false)),
                Operation::Delete => Some(columns(true)),
            },
            after: match operation {
                Operation::Insert => Some(columns(false)),
                Operation::Update => Some(columns(true)),
                Operation::Delete => None,
            },
        });
    }
    ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: version.into(),
            id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
        },
        id: "430c326c-ab91-11f1-a23b-0242ac160004:42".into(),
        begin_cursor: cursor(100),
        commit_cursor: cursor(200),
        changes,
    }
}

pub fn postgres_fixture(table: &str) -> ChangeTransaction {
    postgres_fixture_for_version(table, "15")
}

pub fn postgres_fixture_for_version(table: &str, version: &str) -> ChangeTransaction {
    let mut fixture = fixture(version, table);
    fixture.source = Source {
        kind: "postgresql".into(),
        version: version.into(),
        id: "550e8400-e29b-41d4-a716-446655440000".into(),
    };
    fixture.begin_cursor = postgres_cursor(100);
    fixture.commit_cursor = postgres_cursor(200);
    for (index, change) in fixture.changes.iter_mut().enumerate() {
        change.source_cursor = postgres_cursor(110 + index as u32 * 10);
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            for column in image {
                column.native_type = match column.native_type.as_str() {
                    "bigint unsigned" => "bigint",
                    "int unsigned" | "int" => "integer",
                    "varchar(255)" => "text",
                    "decimal(30,6)" => "numeric(30,6)",
                    "varbinary(32)" => "bytea",
                    "datetime(6)" => "timestamp(6)",
                    native => native,
                }
                .into();
            }
        }
    }
    fixture
}

fn postgres_cursor(lsn: u32) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: lsn.to_string(),
        display: format!("{lsn}/0"),
    }
}
pub fn roundtrip(tx: &ValidatedTransaction) -> ValidatedTransaction {
    let encoded = change_event::json(tx).unwrap();
    let mut reader = JsonReader::new(std::io::Cursor::new(&encoded));
    let decoded = reader
        .next_transaction()
        .unwrap()
        .expect("complete transaction");
    assert!(reader.next_transaction().unwrap().is_none());
    reader.finish().unwrap();
    assert_eq!(change_event::json(&decoded).unwrap(), encoded);
    decoded
}
