//! Live protocol + decoder tests. Nonblocking bounded replay, no snapshot/global locks.
#[path = "support/mysql_contract.rs"]
mod contract;
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
