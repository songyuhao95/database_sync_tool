use change_event::{
    ChangeTransaction, ColumnDatum, Datum, LogicalValue, Operation, RowChange, Source,
    SourceCursor, validate,
};

fn cursor(display: &str) -> SourceCursor {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let (file, position) = display.rsplit_once(':').unwrap();
    let mut raw = file.as_bytes().to_vec();
    raw.push(0);
    raw.extend_from_slice(&position.parse::<u32>().unwrap().to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".to_owned(),
        value: URL_SAFE_NO_PAD.encode(raw),
        display: display.to_owned(),
    }
}

fn transaction() -> change_event::ValidatedTransaction {
    validate(ChangeTransaction {
        source: Source {
            kind: "mysql".to_owned(),
            version: "5.7.44-log".to_owned(),
            id: "430c326c-ab91-11f1-a23b-0242ac160004".to_owned(),
        },
        id: "430c326c-ab91-11f1-a23b-0242ac160004:1".to_owned(),
        begin_cursor: cursor("binlog.000001:100"),
        commit_cursor: cursor("binlog.000001:200"),
        changes: vec![RowChange {
            database: None,
            operation: Operation::Insert,
            schema: "CDC_test".to_owned(),
            table: "items".to_owned(),
            source_cursor: cursor("binlog.000001:150"),
            source_timestamp: 1_700_000_000,
            schema_basis: "test_fixture".into(),
            before: None,
            after: Some(vec![
                ColumnDatum {
                    ordinal: 0,
                    name: "id".to_owned(),
                    native_type: "bigint".to_owned(),
                    primary_key_ordinal: Some(0),
                    generated: false,
                    collation: None,
                    datum: Datum::Value(LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "7".to_owned(),
                    }),
                },
                ColumnDatum {
                    ordinal: 1,
                    name: "message".to_owned(),
                    native_type: "varchar(64)".to_owned(),
                    primary_key_ordinal: None,
                    generated: false,
                    collation: Some("utf8mb4_unicode_ci".into()),
                    datum: Datum::Value(LogicalValue::Text {
                        charset: "utf8mb4".to_owned(),
                        bytes_base64url: "aGVsbG8".to_owned(),
                        text: Some("hello".to_owned()),
                    }),
                },
            ]),
        }],
    })
    .expect("fixture should be valid")
}

#[test]
fn target_crates_render_strict_insert_with_version_identity() {
    let validated = transaction();
    let sql80 = mysql_8_0::sql(&validated).unwrap().script();
    let sql84 = mysql_8_4::sql(&validated).unwrap().script();
    let sql57 = mysql_5_7::sql(&validated).unwrap().script();
    for output in [&sql57, &sql80, &sql84] {
        assert!(output.contains("INSERT INTO `CDC_test`.`items`"));
        assert!(!output.contains("ON DUPLICATE KEY"));
    }
    assert!(sql57.contains("target=mysql-5.7"));
    assert!(sql80.contains("target=mysql-8.0"));
    assert!(sql84.contains("target=mysql-8.4"));
}
