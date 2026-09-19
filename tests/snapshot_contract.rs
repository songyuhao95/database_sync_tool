use change_event::{ColumnDatum, Datum, LogicalValue, SnapshotBatch, Source, validate_snapshot};
fn batch() -> SnapshotBatch {
    SnapshotBatch {
        source: Source {
            kind: "mysql".into(),
            version: "8.4.0".into(),
            id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
        },
        schema: "CDC_test".into(),
        table: "zero_key".into(),
        rows: vec![vec![ColumnDatum {
            ordinal: 0,
            name: "id".into(),
            native_type: "bigint unsigned".into(),
            primary_key_ordinal: Some(0),
            generated: false,
            collation: None,
            datum: Datum::Value(LogicalValue::Integer {
                signed: false,
                bits: 64,
                value: "18446744073709551615".into(),
            }),
        }]],
        last_in_table: true,
    }
}
#[test]
fn snapshot_validation_and_version_renderers_preserve_exact_values() {
    let checked = validate_snapshot(batch()).unwrap();
    for sql in [
        mysql_5_7::snapshot_sql(&checked)
            .unwrap()
            .statements()
            .collect::<Vec<_>>()
            .join("\n"),
        mysql_8_0::snapshot_sql(&checked)
            .unwrap()
            .statements()
            .collect::<Vec<_>>()
            .join("\n"),
        mysql_8_4::snapshot_sql(&checked)
            .unwrap()
            .statements()
            .collect::<Vec<_>>()
            .join("\n"),
    ] {
        assert_eq!(sql, "INSERT INTO `CDC_test`.`zero_key` (`id`) VALUES (?);");
    }
    let mut no_key = batch();
    no_key.rows[0][0].primary_key_ordinal = None;
    assert!(validate_snapshot(no_key).is_err());
    let mut overflow = batch();
    overflow.rows[0][0].datum = Datum::Value(LogicalValue::Integer {
        signed: false,
        bits: 64,
        value: "18446744073709551616".into(),
    });
    assert!(validate_snapshot(overflow).is_err());
    let mut empty = batch();
    empty.rows.clear();
    assert!(validate_snapshot(empty.clone()).is_ok());
    empty.last_in_table = false;
    assert!(validate_snapshot(empty).is_err());
}
