//! Issue #15's database-neutral local ChangeEvent × Sink matrix.
//!
//! The fixture is deliberately built at the public adapter seams: each source
//! validates a native-shaped transaction and every target plans that same
//! validated event. No source-target pair is selected by the test.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue, Operation,
    RowChange, Source, SourceCursor, TargetCapabilityFailure, ValidatedTransaction,
};
use std::error::Error;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
pub enum SourceVersion {
    Mysql57,
    Mysql80,
    Mysql84,
    Postgresql15,
}

impl SourceVersion {
    pub const ALL: [Self; 4] = [
        Self::Mysql57,
        Self::Mysql80,
        Self::Mysql84,
        Self::Postgresql15,
    ];

    pub fn version(self) -> &'static str {
        match self {
            Self::Mysql57 => "5.7.44",
            Self::Mysql80 => "8.0.46",
            Self::Mysql84 => "8.4.8",
            Self::Postgresql15 => "15.19",
        }
    }

    pub fn is_postgresql(self) -> bool {
        matches!(self, Self::Postgresql15)
    }
}

fn mysql_cursor(position: u32) -> SourceCursor {
    let mut value = b"mysql-bin.000001\0".to_vec();
    value.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(value),
        display: format!("mysql-bin.000001:{position}"),
    }
}

fn postgres_cursor(position: u32) -> SourceCursor {
    let value = format!("0/{position:X}");
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        display: value.clone(),
        value,
    }
}

fn cursor(source: SourceVersion, position: u32) -> SourceCursor {
    if source.is_postgresql() {
        postgres_cursor(position)
    } else {
        mysql_cursor(position)
    }
}

fn native_type(source: SourceVersion, name: &str) -> &'static str {
    if source.is_postgresql() {
        match name {
            "id" => "bigint",
            "tenant" => "integer",
            "message" | "note" => "text",
            "amount" => "numeric(30,6)",
            "ratio" => "double precision",
            "bytes" => "bytea",
            "birthday" => "date",
            "changed_at" => "timestamp(6) without time zone",
            "published_at" => "timestamp(6) with time zone",
            "payload" => "jsonb",
            _ => panic!("unknown matrix column {name}"),
        }
    } else {
        match name {
            "id" => "bigint",
            "tenant" => "int",
            "message" | "note" => "varchar(255)",
            "amount" => "decimal(30,6)",
            "ratio" => "double",
            "bytes" => "varbinary(32)",
            "birthday" => "date",
            "changed_at" => "datetime(6)",
            "published_at" => "timestamp(6)",
            "payload" => "json",
            _ => panic!("unknown matrix column {name}"),
        }
    }
}

fn text(source: SourceVersion, value: &str) -> Datum {
    Datum::Value(LogicalValue::Text {
        charset: if source.is_postgresql() {
            "UTF8"
        } else {
            "utf8mb4"
        }
        .into(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(value),
        text: Some(value.into()),
    })
}

fn value(source: SourceVersion, name: &str, updated: bool) -> Datum {
    match name {
        "id" => Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: if updated { "2" } else { "1" }.into(),
        }),
        "tenant" => Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 32,
            value: "7".into(),
        }),
        "message" => text(source, if updated { "updated" } else { "中文'\\" }),
        "amount" => Datum::Value(LogicalValue::Decimal {
            unscaled: if updated {
                "-123456"
            } else {
                "12345678901234567890123456"
            }
            .into(),
            scale: 6,
        }),
        "ratio" => Datum::Value(LogicalValue::Float {
            bits: 64,
            ieee754_hex: "3ff8000000000000".into(),
        }),
        "bytes" => Datum::Value(LogicalValue::Binary {
            bytes_base64url: "AP8".into(),
        }),
        "note" => Datum::Null,
        "birthday" => Datum::Value(LogicalValue::Date {
            year: 2026,
            month: 9,
            day: 14,
        }),
        "changed_at" => Datum::Value(LogicalValue::LocalDatetime {
            year: 2026,
            month: 9,
            day: 14,
            hour: 1,
            minute: 2,
            second: 3,
            microsecond: 123456,
        }),
        "published_at" => Datum::Value(LogicalValue::Instant {
            unix_seconds: "1700000000".into(),
            nanoseconds: 123456000,
        }),
        "payload" => Datum::Value(LogicalValue::Json {
            value: JsonValue::Object(vec![
                JsonEntry {
                    key: "amount".into(),
                    value: JsonValue::Decimal {
                        unscaled: "123456".into(),
                        scale: 2,
                    },
                },
                JsonEntry {
                    key: "ok".into(),
                    value: JsonValue::Boolean(true),
                },
            ]),
        }),
        _ => panic!("unknown matrix column {name}"),
    }
}

pub const COLUMN_NAMES: [&str; 11] = [
    "id",
    "tenant",
    "message",
    "amount",
    "ratio",
    "bytes",
    "note",
    "birthday",
    "changed_at",
    "published_at",
    "payload",
];

fn full_image(source: SourceVersion, updated: bool) -> Vec<ColumnDatum> {
    COLUMN_NAMES
        .into_iter()
        .enumerate()
        .map(|(ordinal, name)| ColumnDatum {
            ordinal,
            name: name.into(),
            native_type: native_type(source, name).into(),
            primary_key_ordinal: match name {
                "id" => Some(0),
                "tenant" => Some(1),
                _ => None,
            },
            generated: false,
            collation: None,
            datum: value(source, name, updated),
        })
        .collect()
}

fn postgresql_before(source: SourceVersion, updated: bool) -> Vec<ColumnDatum> {
    full_image(source, updated)
        .into_iter()
        .map(|mut column| {
            if column.primary_key_ordinal.is_none() {
                column.datum = Datum::Unavailable;
            }
            column
        })
        .collect()
}

fn postgresql_after(source: SourceVersion) -> Vec<ColumnDatum> {
    full_image(source, true)
        .into_iter()
        .map(|mut column| {
            if column.name != "id" && column.name != "tenant" && column.name != "message" {
                column.datum = Datum::Unchanged;
            }
            column
        })
        .collect()
}

pub fn transaction(source: SourceVersion, table: &str) -> ChangeTransaction {
    let source_identity = if source.is_postgresql() {
        "postgresql:123456:1:16384:Q0RDX3Rlc3Q"
    } else {
        "430c326c-ab91-11f1-a23b-0242ac160004"
    };
    let schema = if source.is_postgresql() {
        "public"
    } else {
        "CDC_test"
    };
    let mut changes = Vec::new();
    for (index, operation) in [Operation::Insert, Operation::Update, Operation::Delete]
        .into_iter()
        .enumerate()
    {
        let (before, after) = match (source.is_postgresql(), operation) {
            (_, Operation::Insert) => (None, Some(full_image(source, false))),
            (true, Operation::Update) => (
                Some(postgresql_before(source, false)),
                Some(postgresql_after(source)),
            ),
            (false, Operation::Update) => (
                Some(full_image(source, false)),
                Some(full_image(source, true)),
            ),
            (true, Operation::Delete) => (Some(postgresql_before(source, true)), None),
            (false, Operation::Delete) => (Some(full_image(source, true)), None),
        };
        changes.push(RowChange {
            database: source.is_postgresql().then(|| "CDC_test".into()),
            schema: schema.into(),
            table: table.into(),
            operation,
            source_cursor: cursor(
                source,
                if source.is_postgresql() {
                    200
                } else {
                    110 + index as u32 * 10
                },
            ),
            source_timestamp: 1_700_000_000 + index as u32,
            schema_basis: if source.is_postgresql() {
                "pgoutput+catalog:42".into()
            } else {
                "matrix_fixture".into()
            },
            before,
            after,
        });
    }
    ChangeTransaction {
        source: Source {
            kind: if source.is_postgresql() {
                "postgresql"
            } else {
                "mysql"
            }
            .into(),
            version: source.version().into(),
            id: source_identity.into(),
        },
        id: if source.is_postgresql() {
            "pg:42:0/C8".into()
        } else {
            format!("{source_identity}:42")
        },
        begin_cursor: cursor(source, 100),
        commit_cursor: cursor(source, 200),
        changes,
    }
}

pub fn validate_source(
    source: SourceVersion,
    transaction: ChangeTransaction,
) -> TestResult<ValidatedTransaction> {
    Ok(match source {
        SourceVersion::Mysql57 => mysql_5_7::validate_change_event(transaction)?,
        SourceVersion::Mysql80 => mysql_8_0::validate_change_event(transaction)?,
        SourceVersion::Mysql84 => mysql_8_4::validate_change_event(transaction)?,
        SourceVersion::Postgresql15 => postgresql_15::validate_change_event(transaction)?,
    })
}

pub fn roundtrip(transaction: &ValidatedTransaction) -> TestResult<ValidatedTransaction> {
    let encoded = change_event::json(transaction)?;
    let mut reader = change_event::JsonReader::new(std::io::Cursor::new(encoded));
    let decoded = reader
        .next_transaction()?
        .ok_or("matrix fixture did not round-trip")?;
    reader.finish()?;
    Ok(decoded)
}

pub fn assert_source_adapters_exist() {
    fn assert_source_adapter<T: change_event::SourceAdapter>() {}
    assert_source_adapter::<mysql_5_7::BinlogStream>();
    assert_source_adapter::<mysql_8_0::BinlogStream>();
    assert_source_adapter::<mysql_8_4::BinlogStream>();
    assert_source_adapter::<postgresql_15::Replication>();
}

pub fn assert_target_capability_failure(error: std::io::Error) {
    assert!(error.to_string().contains("Target Capability Failure"));
    assert!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<TargetCapabilityFailure>())
            .is_some()
    );
}
