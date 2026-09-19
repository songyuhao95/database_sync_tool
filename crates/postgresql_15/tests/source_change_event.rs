use change_event::{
    ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonReader, JsonValue, LogicalValue,
    Operation, RowChange, Source, SourceCursor,
};

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn column(
    ordinal: usize,
    name: &str,
    native_type: &str,
    key: Option<usize>,
    datum: Datum,
) -> ColumnDatum {
    ColumnDatum {
        ordinal,
        name: name.into(),
        native_type: native_type.into(),
        primary_key_ordinal: key,
        generated: false,
        collation: None,
        datum,
    }
}

fn transaction() -> ChangeTransaction {
    let before = vec![
        column(
            0,
            "id",
            "bigint",
            Some(0),
            Datum::Value(LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "7".into(),
            }),
        ),
        column(
            1,
            "message",
            "text",
            None,
            Datum::Value(LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "b2xk".into(),
                text: Some("old".into()),
            }),
        ),
    ];
    let mut after = before.clone();
    after[1].datum = Datum::Value(LogicalValue::Text {
        charset: "UTF8".into(),
        bytes_base64url: "bmV3".into(),
        text: Some("new".into()),
    });
    ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15.19".into(),
            id: "postgresql:123456:1:16384".into(),
        },
        id: "pg:42:0/20".into(),
        begin_cursor: cursor("0/10"),
        commit_cursor: cursor("0/20"),
        changes: vec![
            RowChange {
                database: Some("CDC_test".into()),
                operation: Operation::Insert,
                schema: "public".into(),
                table: "items".into(),
                source_cursor: cursor("0/20"),
                source_timestamp: 1_700_000_000,
                schema_basis: "pgoutput+catalog:16390".into(),
                before: None,
                after: Some(after.clone()),
            },
            RowChange {
                database: Some("CDC_test".into()),
                operation: Operation::Update,
                schema: "public".into(),
                table: "items".into(),
                source_cursor: cursor("0/20"),
                source_timestamp: 1_700_000_000,
                schema_basis: "pgoutput+catalog:16390".into(),
                before: Some(before),
                after: Some(after),
            },
            RowChange {
                database: Some("CDC_test".into()),
                operation: Operation::Delete,
                schema: "public".into(),
                table: "items".into(),
                source_cursor: cursor("0/20"),
                source_timestamp: 1_700_000_000,
                schema_basis: "pgoutput+catalog:16390".into(),
                before: Some(vec![column(
                    0,
                    "id",
                    "bigint",
                    Some(0),
                    Datum::Value(LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "7".into(),
                    }),
                )]),
                after: None,
            },
        ],
    }
}

#[test]
fn postgres_source_adapter_outputs_v03_and_replays() {
    let validated = postgresql_15::validate_change_event(transaction()).unwrap();
    let encoded = change_event::json(&validated).unwrap();
    assert!(
        encoded
            .lines()
            .all(|line| line.contains("cdc.change-event-json.v0.3"))
    );
    let mut reader = JsonReader::new(std::io::Cursor::new(encoded));
    let replayed = reader.next_transaction().unwrap().unwrap();
    assert_eq!(replayed.transaction().changes.len(), 3);
    reader.finish().unwrap();
}

#[test]
fn postgres_replication_implements_database_neutral_source_adapter() {
    fn assert_source_adapter<T: change_event::SourceAdapter>() {}
    assert_source_adapter::<postgresql_15::Replication>();
}

#[test]
fn postgres_source_adapter_rejects_native_and_boundary_violations() {
    let mut invalid = transaction();
    invalid.commit_cursor = cursor("0/0");
    assert!(postgresql_15::validate_change_event(invalid).is_err());

    let mut invalid = transaction();
    invalid.changes[0].after.as_mut().unwrap()[1].native_type = "jsonb".into();
    assert!(postgresql_15::validate_change_event(invalid).is_err());

    let mut invalid = transaction();
    invalid.changes[1].after.as_mut().unwrap()[1].datum = Datum::Unchanged;
    invalid.changes[1].before.as_mut().unwrap()[1].datum = Datum::Unavailable;
    assert!(postgresql_15::validate_change_event(invalid).is_ok());

    let mut invalid = transaction();
    invalid.changes[0].schema_basis = "catalog:16390".into();
    assert!(postgresql_15::validate_change_event(invalid).is_err());

    let mut invalid = transaction();
    invalid.changes[0].after.as_mut().unwrap()[1].generated = true;
    assert!(postgresql_15::validate_change_event(invalid).is_err());

    let mut invalid = transaction();
    invalid.source.id = "postgresql:123456:1:16384:V3Jvbmc".into();
    assert!(postgresql_15::validate_change_event(invalid).is_err());
}

#[test]
fn postgres_source_identity_binds_database_name_when_present() {
    let mut tx = transaction();
    tx.source.id = "postgresql:123456:1:16384:Q0RDX3Rlc3Q".into();
    postgresql_15::validate_change_event(tx).unwrap();
}

#[test]
fn postgres_source_contract_maps_supported_native_types() {
    let values = vec![
        ("boolean", LogicalValue::Boolean { value: true }),
        (
            "uuid",
            LogicalValue::Uuid {
                value: "12345678-1234-1234-1234-123456789abc".into(),
            },
        ),
        (
            "smallint",
            LogicalValue::Integer {
                signed: true,
                bits: 16,
                value: "1".into(),
            },
        ),
        (
            "integer",
            LogicalValue::Integer {
                signed: true,
                bits: 32,
                value: "2".into(),
            },
        ),
        (
            "bigint",
            LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "3".into(),
            },
        ),
        (
            "numeric(30,6)",
            LogicalValue::Decimal {
                unscaled: "123456".into(),
                scale: 6,
            },
        ),
        (
            "real",
            LogicalValue::Float {
                bits: 32,
                ieee754_hex: "3f800000".into(),
            },
        ),
        (
            "double precision",
            LogicalValue::Float {
                bits: 64,
                ieee754_hex: "3ff0000000000000".into(),
            },
        ),
        (
            "text",
            LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "dGV4dA".into(),
                text: Some("text".into()),
            },
        ),
        (
            "bytea",
            LogicalValue::Binary {
                bytes_base64url: "AP8".into(),
            },
        ),
        (
            "date",
            LogicalValue::Date {
                year: 2026,
                month: 9,
                day: 15,
            },
        ),
        (
            "timestamp(6) without time zone",
            LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 15,
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 4,
            },
        ),
        (
            "timestamp(6) with time zone",
            LogicalValue::Instant {
                unix_seconds: "1773882123".into(),
                nanoseconds: 4_000,
            },
        ),
        (
            "jsonb",
            LogicalValue::Json {
                value: JsonValue::Object(vec![JsonEntry {
                    key: "n".into(),
                    value: JsonValue::SignedInteger("1".into()),
                }]),
            },
        ),
    ];
    let mut tx = transaction();
    tx.changes[0].after = Some(
        values
            .into_iter()
            .enumerate()
            .map(|(ordinal, (native_type, value))| {
                column(
                    ordinal,
                    &format!("c{ordinal}"),
                    native_type,
                    (ordinal == 0).then_some(0),
                    Datum::Value(value),
                )
            })
            .collect(),
    );
    postgresql_15::validate_change_event(tx).unwrap();
}
