//! Live protocol + decoder tests. Nonblocking bounded replay, no snapshot/global locks.
#[path = "support/mysql_contract.rs"]
mod contract;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{Datum, LogicalValue, Operation, validate};
use contract::*;
use mysql::prelude::Queryable;

fn inspect(transactions: &[change_event::ValidatedTransaction], version: &str) {
    assert_eq!(
        transactions.len(),
        3,
        "only committed transactions should be captured"
    );
    let changes: Vec<_> = transactions
        .iter()
        .flat_map(|t| &t.transaction().changes)
        .collect();
    assert_eq!(
        changes.len(),
        4,
        "multi-row insert, update, delete; rollback must be absent"
    );
    assert!(
        transactions
            .iter()
            .all(|t| t.transaction().source.version.starts_with(version))
    );
    assert_eq!(
        transactions[0].transaction().changes.len(),
        2,
        "source transaction stays together"
    );
    assert!(matches!(changes[0].operation, Operation::Insert));
    assert!(matches!(changes[1].operation, Operation::Insert));
    assert!(matches!(changes[2].operation, Operation::Update));
    assert!(matches!(changes[3].operation, Operation::Delete));
    for (image, expected) in [
        (changes[0].after.as_ref().unwrap(), columns(false)),
        (changes[2].before.as_ref().unwrap(), columns(false)),
        (changes[2].after.as_ref().unwrap(), columns(true)),
        (changes[3].before.as_ref().unwrap(), columns(true)),
    ] {
        assert_eq!(image.len(), expected.len());
        for (actual, expected) in image.iter().zip(expected) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.primary_key_ordinal, expected.primary_key_ordinal);
            assert_eq!(actual.generated, expected.generated);
            match (&actual.datum, &expected.datum) {
                (
                    Datum::Value(LogicalValue::Decimal {
                        unscaled: a,
                        scale: sa,
                    }),
                    Datum::Value(LogicalValue::Decimal {
                        unscaled: b,
                        scale: sb,
                    }),
                ) => {
                    // Native MySQL decimal text may include leading zeroes. Compare exact integers, never floats.
                    assert_eq!(sa, sb);
                    assert_eq!(a.parse::<i128>().unwrap(), b.parse::<i128>().unwrap());
                }
                _ => assert_eq!(
                    serde_json::to_value(&actual.datum).unwrap(),
                    serde_json::to_value(&expected.datum).unwrap(),
                    "{}",
                    actual.name
                ),
            }
        }
    }
    assert!(matches!(&changes[1].after.as_ref().unwrap()[0].datum,
        Datum::Value(LogicalValue::Integer{value,..}) if value=="2"));
    for tx in transactions {
        roundtrip(tx);
    }
}

macro_rules! capture_tests {
    ($adapter:ident, $version:literal, $port_key:literal, $port:literal, $status:literal) => {
        mod $adapter {
            use super::*;
            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_capture() {
                let port = port($port_key,$port);
                let mut table = TestTable::create(port);
                let mut reader = connection(port,true);
                let (file,position,_,_,gtids):(String,u64,String,String,String)=reader.query_first($status).unwrap().unwrap();
                let gtid_mode:String=reader.query_first("SELECT @@GLOBAL.GTID_MODE").unwrap().unwrap();
                let gtid_enabled=matches!(gtid_mode.as_str(),"ON"|"ON_PERMISSIVE");
                let config = || {
                    let mut c = ::$adapter::BinlogConfig::new(setting("CDC_MYSQL_HOST","192.168.0.10"),port,
                        setting("CDC_MYSQL_READER_USER","mysql_reader"),password("READER"));
                    c.non_blocking=true;
                    c.max_events=Some(10000);
                    c.tables=vec![("CDC_test".into(),table.name.clone())];
                    c
                };
                let auto = ::$adapter::binlog(config()).unwrap();
                assert_eq!(auto.start_mode().label(),if gtid_enabled { "gtid" } else { "position" });
                drop(auto);
                let insert = format!("INSERT INTO CDC_test.{} (id,tenant,message,amount,bytes,note,changed_at) VALUES
                    (18446744073709551615,7,CONVERT(X'E4B8ADE69687275C0A' USING utf8mb4),12345678901234567890.123456,X'00FF',NULL,'2026-09-14 01:02:03.123456'),
                    (2,7,'second',0,X'',NULL,'2026-09-14 01:02:03.123456')",table.name);
                table.conn.query_drop("START TRANSACTION").unwrap();
                table.conn.query_drop(&insert).unwrap();
                table.conn.query_drop("ROLLBACK").unwrap();
                table.conn.query_drop("START TRANSACTION").unwrap();
                table.conn.query_drop(&insert).unwrap();
                table.conn.query_drop("COMMIT").unwrap();
                table.conn.query_drop(format!("UPDATE CDC_test.{} SET id=42,message='',amount=-0.000001 WHERE tenant=7 AND id=18446744073709551615",table.name)).unwrap();
                table.conn.query_drop(format!("DELETE FROM CDC_test.{} WHERE tenant=7 AND id=42",table.name)).unwrap();
                let artifact_dir=std::path::PathBuf::from(setting("CDC_TEST_ARTIFACT_DIR","target/connector-artifacts"));
                std::fs::create_dir_all(&artifact_dir).unwrap();
                let mut position_events=None;
                for mode in [::$adapter::BinlogStartMode::Position,::$adapter::BinlogStartMode::Gtid] {
                    let mut c=config(); c.start_mode=mode;
                    if matches!(mode,::$adapter::BinlogStartMode::Position) {
                        c.start=Some(::$adapter::BinlogPosition{file:file.clone(),position});
                    } else { c.gtid_set=Some(gtids.clone()); }
                    if !gtid_enabled && matches!(mode,::$adapter::BinlogStartMode::Gtid) {
                        assert!(::$adapter::binlog(c).is_err());
                        println!("GTID disabled: explicit GTID rejected, Auto fallback verified");
                        continue;
                    }
                    let log=artifact_dir.join(format!("{}-{}-{}.binlog.log",stringify!($adapter),table.name,mode.label()));
                    c.binlog_log_path=Some(log.clone());
                    let mut capture=::$adapter::binlog(c).unwrap();
                    let transactions:Vec<_>=capture.by_ref().filter_map(|t| {
                        let tx=t.expect("replication protocol / decode error");
                        (!tx.changes.is_empty()).then(||validate(tx).unwrap())
                    }).collect();
                    assert!(capture.protocol_events()>=6);
                    drop(capture);
                    let raw=std::fs::read_to_string(log).unwrap();
                    for event in ["FORMAT_DESCRIPTION_EVENT","TABLE_MAP_EVENT","WRITE_ROWS_EVENT","UPDATE_ROWS_EVENT","DELETE_ROWS_EVENT","XID_EVENT"] {
                        assert!(raw.contains(event),"missing protocol event: {event}");
                    }
                    inspect(&transactions,$version);
                    let json=transactions.iter().map(|t|change_event::json(t).unwrap()).collect::<String>();
                    std::fs::write(artifact_dir.join(format!("{}-{}-{}.change_event.jsonl",stringify!($adapter),table.name,mode.label())),&json).unwrap();
                    if let Some((expected,_))=&position_events {
                        assert_eq!(&json,expected,"GTID and file-position must decode identical changes");
                    } else { position_events=Some((json,transactions)); }
                }
                let (_,transactions)=position_events.unwrap();
                let (_,p)=transactions[0].transaction().commit_cursor.display.rsplit_once(':').unwrap();
                let mut resumed=config();
                resumed.start_mode=::$adapter::BinlogStartMode::Position;
                resumed.start=Some(::$adapter::BinlogPosition{file,position:p.parse().unwrap()});
                let replay:Vec<_>=::$adapter::binlog(resumed).unwrap().filter_map(|t| {
                    let tx=t.unwrap(); (!tx.changes.is_empty()).then(||validate(tx).unwrap())
                }).collect();
                assert_eq!(replay.len(),2,"resume must not replay the first committed transaction");
                for (actual,expected) in replay.iter().zip(&transactions[1..]) {
                    assert_eq!(change_event::json(actual).unwrap(),change_event::json(expected).unwrap());
                }
                println!("PASS {}: protocol events, INSERT/UPDATE/DELETE, transaction boundary, rollback exclusion, exact values, primary-key change, JSON roundtrip and resume",stringify!($adapter));
                table.cleanup();
            }

            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_generic_geometry_capture() {
                let port = port($port_key, $port);
                let mut table = GenericGeometryTable::create(port);
                let mut reader = connection(port, true);
                let (file, position, _, _, _): (String, u64, String, String, String) =
                    reader.query_first($status).unwrap().unwrap();

                table
                    .conn
                    .query_drop(format!(
                        "INSERT INTO CDC_test.{} (id, shape) VALUES (1, ST_GeomFromText('POINT(1 2)'))",
                        table.name
                    ))
                    .unwrap();
                table
                    .conn
                    .query_drop(format!(
                        "UPDATE CDC_test.{} SET shape=ST_GeomFromText('LINESTRING(0 0,1 1)') WHERE id=1",
                        table.name
                    ))
                    .unwrap();
                table
                    .conn
                    .query_drop(format!(
                        "DELETE FROM CDC_test.{} WHERE id=1",
                        table.name
                    ))
                    .unwrap();

                let mut config = ::$adapter::BinlogConfig::new(
                    setting("CDC_MYSQL_HOST", "192.168.0.10"),
                    port,
                    setting("CDC_MYSQL_READER_USER", "mysql_reader"),
                    password("READER"),
                );
                config.start_mode = ::$adapter::BinlogStartMode::Position;
                config.start = Some(::$adapter::BinlogPosition { file, position });
                config.non_blocking = true;
                config.max_events = Some(10_000);
                config.tables = vec![("CDC_test".into(), table.name.clone())];
                let mut capture = ::$adapter::binlog(config).unwrap();
                let mut transactions = Vec::new();
                while let Some(transaction) = capture.next_change_event().unwrap() {
                    transactions.push(transaction);
                }

                assert_eq!(transactions.len(), 3);
                let changes: Vec<_> = transactions
                    .iter()
                    .flat_map(|transaction| &transaction.transaction().changes)
                    .collect();
                assert_eq!(changes.len(), 3);
                assert!(matches!(changes[0].operation, Operation::Insert));
                assert!(matches!(changes[1].operation, Operation::Update));
                assert!(matches!(changes[2].operation, Operation::Delete));
                assert!(matches!(
                    &changes[0].after.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "point"
                ));
                assert!(matches!(
                    &changes[1].before.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "point"
                ));
                assert!(matches!(
                    &changes[1].after.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "linestring"
                ));
                assert!(matches!(
                    &changes[2].before.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "linestring"
                ));
                for transaction in &transactions {
                    assert_eq!(
                        change_event::json(&roundtrip(transaction)).unwrap(),
                        change_event::json(transaction).unwrap()
                    );
                }

                table.cleanup();
            }

            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_all_mysql_type_capture() {
                let port = port($port_key, $port);
                let mut table = AllMysqlTypesTable::create(port);
                let mut reader = connection(port, true);
                let (file, position, _, _, _): (String, u64, String, String, String) =
                    reader.query_first($status).unwrap().unwrap();

                table.conn.query_drop("SET SESSION sql_mode = ''").unwrap();
                let insert = format!(
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
                    table.name
                );
                table.conn.query_drop(insert).unwrap();
                table.conn.query_drop(format!(
                    "UPDATE CDC_test.{} SET varchar_value='updated', null_marker='now', float_precision_boundary=2.5 WHERE id=1",
                    table.name
                )).unwrap();
                table.conn.query_drop(format!(
                    "DELETE FROM CDC_test.{} WHERE id=1",
                    table.name
                )).unwrap();

                let mut config = ::$adapter::BinlogConfig::new(
                    setting("CDC_MYSQL_HOST", "192.168.0.10"),
                    port,
                    setting("CDC_MYSQL_READER_USER", "mysql_reader"),
                    password("READER"),
                );
                config.start_mode = ::$adapter::BinlogStartMode::Position;
                config.start = Some(::$adapter::BinlogPosition { file, position });
                config.non_blocking = true;
                config.max_events = Some(20_000);
                config.tables = vec![("CDC_test".into(), table.name.clone())];
                let mut capture = ::$adapter::binlog(config).unwrap();
                let mut transactions = Vec::new();
                while let Some(transaction) = capture.next_change_event().unwrap() {
                    transactions.push(transaction);
                }

                assert_eq!(transactions.len(), 3, "INSERT/UPDATE/DELETE commit boundaries");
                let changes: Vec<_> = transactions
                    .iter()
                    .flat_map(|transaction| &transaction.transaction().changes)
                    .collect();
                assert_eq!(changes.len(), 3);
                assert!(matches!(changes[0].operation, Operation::Insert));
                assert!(matches!(changes[1].operation, Operation::Update));
                assert!(matches!(changes[2].operation, Operation::Delete));

                let columns = [
                    ("id", "integer"),
                    ("tiny_signed", "integer"), ("tiny_unsigned", "integer"),
                    ("small_signed", "integer"), ("small_unsigned", "integer"),
                    ("medium_signed", "integer"), ("medium_unsigned", "integer"),
                    ("int_signed", "integer"), ("int_unsigned", "integer"),
                    ("big_signed", "integer"), ("big_unsigned", "integer"),
                    ("decimal_default", "decimal"), ("decimal_precision", "decimal"),
                    ("decimal_scaled", "decimal"), ("decimal_unsigned", "decimal"),
                    ("numeric_alias", "decimal"),
                    ("float_plain", "float"), ("float_precision", "float"),
                    ("float_precision_boundary", "float"), ("float_scaled", "float"),
                    ("double_plain", "float"), ("double_precision", "float"),
                    ("bit_one", "bit"), ("bit_wide", "bit"),
                    ("date_value", "date"), ("datetime_value", "local_datetime"),
                    ("timestamp_value", "instant"), ("time_value", "duration"),
                    ("year_value", "year"),
                    ("char_value", "text"), ("varchar_value", "text"),
                    ("tinytext_value", "text"), ("text_value", "text"),
                    ("mediumtext_value", "text"), ("longtext_value", "text"),
                    ("binary_value", "binary"), ("varbinary_value", "binary"),
                    ("tinyblob_value", "binary"), ("blob_value", "binary"),
                    ("mediumblob_value", "binary"), ("longblob_value", "binary"),
                    ("enum_value", "enum"), ("set_value", "set"), ("json_value", "json"),
                    ("geometry_value", "spatial"), ("point_value", "spatial"),
                    ("linestring_value", "spatial"), ("polygon_value", "spatial"),
                    ("multipoint_value", "spatial"), ("multilinestring_value", "spatial"),
                    ("multipolygon_value", "spatial"), ("geometrycollection_value", "spatial"),
                    ("null_marker", "null"),
                    ("decimal_max_precision", "decimal"),
                    ("float_negative_zero", "float"), ("double_negative_zero", "float"),
                    ("date_zero", "invalid_temporal"),
                    ("datetime_zero", "invalid_temporal"),
                    ("timestamp_zero", "invalid_temporal"),
                    ("latin1_value", "text"), ("binary_padding", "binary"),
                ];
                fn kind(value: &LogicalValue) -> &'static str {
                    match value {
                        LogicalValue::Integer { .. } => "integer",
                        LogicalValue::Decimal { .. } => "decimal",
                        LogicalValue::Float { .. } => "float",
                        LogicalValue::Text { .. } => "text",
                        LogicalValue::Binary { .. } => "binary",
                        LogicalValue::BitString { .. } => "bit",
                        LogicalValue::Date { .. } => "date",
                        LogicalValue::LocalDatetime { .. } => "local_datetime",
                        LogicalValue::Instant { .. } => "instant",
                        LogicalValue::Duration { .. } => "duration",
                        LogicalValue::Year { .. } => "year",
                        LogicalValue::Enum { .. } => "enum",
                        LogicalValue::Set { .. } => "set",
                        LogicalValue::Json { .. } => "json",
                        LogicalValue::Spatial { .. } => "spatial",
                        LogicalValue::InvalidTemporal { .. } => "invalid_temporal",
                        other => panic!("unexpected MySQL LogicalValue variant: {other:?}"),
                    }
                }
                let insert_image = changes[0].after.as_ref().unwrap();
                assert_eq!(insert_image.len(), columns.len());
                for ((column, (expected_name, expected_kind)), ordinal) in
                    insert_image.iter().zip(columns).zip(0_usize..)
                {
                    assert_eq!(column.ordinal, ordinal);
                    assert_eq!(column.name, expected_name);
                    assert_eq!(
                        column.primary_key_ordinal.is_some(),
                        expected_name == "id"
                    );
                    match (&column.datum, expected_kind) {
                        (Datum::Null, "null") => {}
                        (Datum::Value(value), kind_expected) => {
                            assert_eq!(kind(value), kind_expected, "{}", column.name)
                        }
                        (datum, kind_expected) => panic!(
                            "{} expected {kind_expected}, found {datum:?}",
                            column.name
                        ),
                    }
                }
                let boundary_bits = if insert_image[18]
                    .native_type
                    .to_ascii_lowercase()
                    .starts_with("double")
                {
                    64
                } else {
                    32
                };
                assert!(matches!(
                    &insert_image[18].datum,
                    Datum::Value(LogicalValue::Float { bits, .. }) if *bits == boundary_bits
                ), "FLOAT(24) catalog type {:?} decoded as {:?}", insert_image[18].native_type, insert_image[18].datum);
                for (index, expected) in [
                    (1, "-128"),
                    (3, "-32768"),
                    (5, "-8388608"),
                    (7, "-2147483648"),
                    (9, "-9223372036854775808"),
                    (2, "255"),
                    (4, "65535"),
                    (6, "16777215"),
                    (8, "4294967295"),
                    (10, "18446744073709551615"),
                ] {
                    assert!(matches!(
                        &insert_image[index].datum,
                        Datum::Value(LogicalValue::Integer { value, .. }) if value == expected
                    ), "{} expected integer boundary {expected}, found {:?}", insert_image[index].name, insert_image[index].datum);
                }
                assert!(matches!(
                    &insert_image[22].datum,
                    Datum::Value(LogicalValue::BitString {
                        bit_length: 1,
                        bit_order: change_event::BitOrder::LsbFirst,
                        bytes_base64url,
                        ..
                    }) if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [1])
                ), "BIT(1) value was not preserved: {:?}", insert_image[22].datum);
                assert!(matches!(
                    &insert_image[23].datum,
                    Datum::Value(LogicalValue::BitString {
                        bit_length: 64,
                        bit_order: change_event::BitOrder::LsbFirst,
                        bytes_base64url,
                        ..
                    }) if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [0xa5; 8])
                ), "BIT(64) bytes were not preserved: {:?}", insert_image[23].datum);
                assert!(matches!(
                    &insert_image[24].datum,
                    Datum::Value(LogicalValue::Date { year: 2024, month: 2, day: 29 })
                ));
                assert!(matches!(
                    &insert_image[25].datum,
                    Datum::Value(LogicalValue::LocalDatetime {
                        year: 2024,
                        month: 2,
                        day: 29,
                        hour: 12,
                        minute: 34,
                        second: 56,
                        microsecond: 123_456,
                    })
                ));
                assert!(matches!(
                    &insert_image[28].datum,
                    Datum::Value(LogicalValue::Year { value: 2024 })
                ));
                assert!(matches!(
                    &insert_image[41].datum,
                    Datum::Value(LogicalValue::Enum { label }) if label == "beta"
                ));
                assert!(matches!(
                    &insert_image[42].datum,
                    Datum::Value(LogicalValue::Set { members }) if members == &["a", "c"]
                ));
                assert!(matches!(
                    &insert_image[53].datum,
                    Datum::Value(LogicalValue::Decimal { unscaled, scale })
                        if unscaled == "99999999999999999999999999999999999999999999999999999999999999999" && *scale == 30
                ), "DECIMAL(65,30) boundary decoded as {:?}", insert_image[53].datum);
                assert!(matches!(
                    &insert_image[54].datum,
                    Datum::Value(LogicalValue::Float { bits: 32, ieee754_hex }) if ieee754_hex == "80000000"
                ), "FLOAT negative zero decoded as {:?}", insert_image[54].datum);
                assert!(matches!(
                    &insert_image[55].datum,
                    Datum::Value(LogicalValue::Float { bits: 64, ieee754_hex }) if ieee754_hex == "8000000000000000"
                ), "DOUBLE negative zero decoded as {:?}", insert_image[55].datum);
                for (index, kind) in [(56, "mysql.date"), (57, "mysql.datetime"), (58, "mysql.timestamp")] {
                    assert!(matches!(
                        &insert_image[index].datum,
                        Datum::Value(LogicalValue::InvalidTemporal { kind: actual, raw })
                            if actual == kind && raw.starts_with("0000-00-00")
                    ), "{} zero date decoded as {:?}", insert_image[index].name, insert_image[index].datum);
                }
                assert!(matches!(
                    &insert_image[59].datum,
                    Datum::Value(LogicalValue::Text { charset, bytes_base64url, .. })
                        if charset.eq_ignore_ascii_case("latin1")
                            && URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes.first() == Some(&0xe9))
                ), "latin1 CHAR bytes were not preserved: {:?}", insert_image[59].datum);
                assert!(matches!(
                    &insert_image[60].datum,
                    Datum::Value(LogicalValue::Binary { bytes_base64url })
                        if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [1, 0, 0, 0])
                ), "fixed BINARY zero padding was not preserved: {:?}", insert_image[60].datum);
                let update_before = changes[1].before.as_ref().unwrap();
                let update_after = changes[1].after.as_ref().unwrap();
                assert_eq!(update_before.len(), columns.len());
                assert_eq!(update_after.len(), columns.len());
                assert!(matches!(&update_before[30].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("base")), "update before varchar: {:?}", update_before[30]);
                assert!(matches!(&update_after[30].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("updated")));
                assert!(matches!(&update_before[52].datum, Datum::Null));
                assert!(matches!(&update_after[52].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("now")));
                assert_eq!(changes[2].before.as_ref().unwrap().len(), columns.len());

                for transaction in &transactions {
                    assert_eq!(
                        change_event::json(&roundtrip(transaction)).unwrap(),
                        change_event::json(transaction).unwrap()
                    );
                }
                println!(
                    "PASS {}: {} native columns; typed INSERT/UPDATE/DELETE, NULL, full before/after presence and JSON replay",
                    stringify!($adapter),
                    columns.len()
                );
                table.cleanup();
            }
        }
    };
}
capture_tests!(
    mysql_5_7,
    "5.7.",
    "CDC_MYSQL57_PORT",
    33061,
    "SHOW MASTER STATUS"
);
capture_tests!(
    mysql_8_0,
    "8.0.",
    "CDC_MYSQL80_PORT",
    33062,
    "SHOW MASTER STATUS"
);
capture_tests!(
    mysql_8_4,
    "8.4.",
    "CDC_MYSQL84_PORT",
    33063,
    "SHOW BINARY LOG STATUS"
);
