use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, LogicalValue, Operation, RowChange, Source,
    SourceCursor, validate,
};

pub fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: value.into(),
        display: value.into(),
    }
}

pub fn column(ordinal: usize, name: &str, key: Option<usize>, datum: Datum) -> ColumnDatum {
    ColumnDatum {
        ordinal,
        name: name.into(),
        native_type: if name == "id" { "bigint" } else { "text" }.into(),
        primary_key_ordinal: key,
        generated: false,
        collation: None,
        datum,
    }
}

fn id(value: &str) -> ColumnDatum {
    column(
        0,
        "id",
        Some(0),
        Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: value.into(),
        }),
    )
}

fn message(datum: Datum) -> ColumnDatum {
    column(1, "message", None, datum)
}

pub fn text(value: &str) -> Datum {
    Datum::Value(LogicalValue::Text {
        charset: "UTF8".into(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(value),
        text: Some(value.into()),
    })
}

pub fn fixture() -> change_event::ValidatedTransaction {
    validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15.14".into(),
            id: "postgresql:1:1:1".into(),
        },
        id: "pg:42:0/16B6C80".into(),
        begin_cursor: cursor("0/16B6C50"),
        commit_cursor: cursor("0/16B6C80"),
        changes: vec![
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Insert,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_000,
                schema_basis: "pgoutput+catalog:1".into(),
                before: None,
                after: Some(vec![id("1"), message(text("中文'\\"))]),
            },
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Update,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_001,
                schema_basis: "pgoutput+catalog:1".into(),
                before: Some(vec![id("1"), message(Datum::Unavailable)]),
                after: Some(vec![id("2"), message(Datum::Unchanged)]),
            },
            RowChange {
                database: Some("CDC_test".into()),
                schema: "public".into(),
                table: "cdc_contract".into(),
                operation: Operation::Delete,
                source_cursor: cursor("0/16B6C80"),
                source_timestamp: 1_700_000_002,
                schema_basis: "pgoutput+catalog:1".into(),
                before: Some(vec![id("2"), message(Datum::Unavailable)]),
                after: None,
            },
        ],
    })
    .unwrap()
}

pub fn mysql_cursor(position: u32) -> SourceCursor {
    let mut bytes = b"mysql-bin.000001\0".to_vec();
    bytes.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(bytes),
        display: format!("mysql-bin.000001:{position}"),
    }
}

pub fn mysql_fixture(version: &str) -> change_event::ValidatedTransaction {
    let mut transaction = fixture().transaction().clone();
    transaction.source = Source {
        kind: "mysql".into(),
        version: version.into(),
        id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
    };
    transaction.begin_cursor = mysql_cursor(100);
    transaction.commit_cursor = mysql_cursor(200);
    for (index, change) in transaction.changes.iter_mut().enumerate() {
        change.source_cursor = mysql_cursor(110 + index as u32 * 10);
        change.schema_basis = "mysql canonical fixture".into();
    }
    validate(transaction).unwrap()
}

pub fn postgres_fixture(version: &str) -> change_event::ValidatedTransaction {
    let mut transaction = fixture().transaction().clone();
    transaction.source.version = version.into();
    validate(transaction).unwrap()
}
