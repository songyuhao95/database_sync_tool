#[path = "support/mysql_contract.rs"]
mod contract;
use change_event::{Datum, JsonReader};
use contract::*;

macro_rules! source_tests {
    ($adapter:ident, $version:literal) => {
        mod $adapter {
            use super::*;
            #[test]
            fn local_change_event() {
                let tx =
                    ::$adapter::validate_change_event(fixture($version, "cdc_contract")).unwrap();
                let decoded = roundtrip(&tx);
                assert_eq!(decoded.transaction().changes.len(), 3);
                let json = change_event::json(&tx).unwrap();
                let truncated = json.lines().take(4).collect::<Vec<_>>().join("\n") + "\n";
                let mut reader = JsonReader::new(std::io::Cursor::new(truncated));
                assert!(reader.next_transaction().unwrap().is_none());
                assert!(
                    reader.finish().is_err(),
                    "partial transaction must not be emitted"
                );
                for bad in [Datum::Unavailable, Datum::Unchanged] {
                    let mut invalid = tx.transaction().clone();
                    invalid.changes[0].after.as_mut().unwrap()[2].datum = bad;
                    assert!(
                        ::$adapter::validate_change_event(invalid).is_err(),
                        "MySQL FULL image must be complete"
                    );
                }
                let mut invalid = tx.transaction().clone();
                invalid.commit_cursor = cursor(105);
                assert!(
                    ::$adapter::validate_change_event(invalid).is_err(),
                    "row cannot be after commit"
                );
            }

            #[test]
            fn source_adapter_outputs_validated_v03_change_events() {
                let validated =
                    ::$adapter::validate_change_event(fixture($version, "cdc_contract"))
                        .expect("MySQL SourceAdapter should return a validated transaction");
                let encoded = change_event::json(&validated).unwrap();
                assert!(
                    encoded
                        .lines()
                        .all(|line| { line.contains("cdc.change-event-json.v0.3") })
                );
                let mut reader = JsonReader::new(std::io::Cursor::new(encoded));
                assert_eq!(
                    reader
                        .next_transaction()
                        .unwrap()
                        .unwrap()
                        .transaction()
                        .changes
                        .len(),
                    3
                );
                reader.finish().unwrap();
            }

            #[test]
            fn source_contract_rejects_mysql_native_boundary_violations() {
                let tx = fixture($version, "cdc_contract");

                let mut invalid = tx.clone();
                for column in invalid.changes[0].after.as_mut().unwrap() {
                    column.primary_key_ordinal = None;
                }
                assert!(::$adapter::validate_change_event(invalid).is_err());

                let mut invalid = tx.clone();
                invalid.changes[0].after.as_mut().unwrap()[0].native_type = "varchar(32)".into();
                assert!(::$adapter::validate_change_event(invalid).is_err());

                let mut invalid = tx.clone();
                invalid.id = "430c326c-ab91-11f1-a23b-0242ac160005:42".into();
                assert!(::$adapter::validate_change_event(invalid).is_err());

                let mut invalid = tx;
                invalid.begin_cursor = cursor_with_file("bin/log.000001", 100);
                invalid.commit_cursor = cursor_with_file("bin/log.000001", 200);
                invalid.changes[0].source_cursor = cursor_with_file("bin/log.000001", 150);
                assert!(::$adapter::validate_change_event(invalid).is_err());
            }
        }
    };
}
source_tests!(mysql_5_7, "5.7.44");
source_tests!(mysql_8_0, "8.0.46");
source_tests!(mysql_8_4, "8.4.8");

#[test]
fn mysql_57_and_80_reject_tagged_gtids() {
    for rejected in [
        ::mysql_5_7::validate_change_event,
        ::mysql_8_0::validate_change_event,
    ] {
        let mut tx = fixture("8.4.8", "tagged_gtid");
        tx.id = "430c326c-ab91-11f1-a23b-0242ac160004:domain_1:42".into();
        assert!(rejected(tx).is_err());
    }
}

#[test]
fn mysql_84_accepts_tagged_gtids() {
    let mut tx = fixture("8.4.8", "tagged_gtid");
    tx.id = "430c326c-ab91-11f1-a23b-0242ac160004:domain_1:42".into();
    ::mysql_8_4::validate_change_event(tx).unwrap();
}
