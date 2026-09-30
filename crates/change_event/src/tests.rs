use super::*;
use std::collections::BTreeMap;

const SOURCE_TYPE_DEFINITION_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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

fn postgresql_cursor() -> SourceCursor {
    SourceCursor {
        format: "pg.logical-replication.v1".into(),
        value: "MC8x".into(),
        display: "0/1".into(),
    }
}

fn insert_transaction() -> ChangeTransaction {
    ChangeTransaction {
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
            after: Some(vec![ColumnDatum {
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
            }]),
        }],
    }
}

#[test]
fn validation_accepts_a_complete_insert_transaction() {
    let validated = validate(insert_transaction()).expect("fixture should be valid");
    assert_eq!(validated.transaction().changes.len(), 1);
}

#[test]
fn validation_rejects_inconsistent_images_and_bad_values() {
    let mut transaction = insert_transaction();
    transaction.changes[0].before = Some(vec![]);
    assert!(validate(transaction).is_err());

    let mut transaction = insert_transaction();
    let Some(after) = transaction.changes[0].after.as_mut() else {
        unreachable!()
    };
    let Datum::Value(LogicalValue::Integer { value, .. }) = &mut after[0].datum else {
        unreachable!()
    };
    *value = "not-an-integer".to_owned();
    assert!(validate(transaction).is_err());
}

#[test]
fn json_emits_begin_row_and_commit_lines() {
    let validated = validate(insert_transaction()).expect("fixture should be valid");
    let output = json(&validated).expect("JSON rendering should succeed");
    assert!(
        output
            .lines()
            .all(|line| line.contains("cdc.change-event-json.v0.3"))
    );
    let lines: Vec<_> = output.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains("transaction_begin"));
    assert!(lines[1].contains("row_change"));
    assert!(lines[2].contains("transaction_commit"));
}

#[test]
fn json_reader_accepts_the_previous_v02_contract() {
    use std::io::Cursor;

    let validated = validate(insert_transaction()).expect("fixture should be valid");
    let output = json(&validated)
        .unwrap()
        .replace("cdc.change-event-json.v0.3", "cdc.change-event-json.v0.2");
    let mut reader = JsonReader::new(Cursor::new(output));
    assert!(reader.next_transaction().unwrap().is_some());
    reader.finish().unwrap();
}

#[test]
fn json_reader_keeps_v01_mysql_compatibility_source_scoped() {
    use std::io::Cursor;

    let validated = validate(insert_transaction()).unwrap();
    let output = json(&validated)
        .unwrap()
        .replace(FORMAT, LEGACY_FORMAT)
        .replace("\"kind\":\"mysql\"", "\"kind\":\"oracle\"");
    let mut reader = JsonReader::new(Cursor::new(output));
    assert!(reader.next_transaction().is_err());
}

#[test]
fn json_reader_keeps_historical_mysql_v0_compatibility() {
    use std::io::Cursor;

    let bytes = include_bytes!("../../../change-events.jsonl");
    let input = if bytes.starts_with(&[0xFF, 0xFE]) {
        let units = bytes[2..]
            .chunks(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        String::from_utf16(&units).unwrap()
    } else {
        String::from_utf8(bytes.to_vec()).unwrap()
    };
    let mut reader = JsonReader::new(Cursor::new(input));
    let mut transactions = 0;
    while reader.next_transaction().unwrap().is_some() {
        transactions += 1;
    }
    reader.finish().unwrap();
    assert_eq!(transactions, 3);
}

#[test]
fn generic_validation_accepts_an_unlisted_source_kind() {
    let mut transaction = insert_transaction();
    transaction.source = Source {
        kind: "oracle".into(),
        version: "23c".into(),
        id: "opaque-source-identity".into(),
    };
    assert!(validate(transaction).is_ok());
}

#[test]
fn generic_validation_preserves_keyless_rows_for_target_capability_checks() {
    let mut transaction = insert_transaction();
    transaction.changes[0].after.as_mut().unwrap()[0].primary_key_ordinal = None;
    assert!(validate(transaction).is_ok());
}

#[test]
fn json_reader_returns_only_a_complete_validated_transaction() {
    use std::io::{BufReader, Cursor};

    let validated = validate(insert_transaction()).expect("fixture should be valid");
    let output = json(&validated).expect("JSON rendering should succeed");
    let mut reader = JsonReader::new(BufReader::new(Cursor::new(output)));
    let decoded = reader
        .next_transaction()
        .expect("JSON should be readable")
        .expect("one transaction should be returned");
    assert_eq!(decoded.transaction().id, validated.transaction().id);
    assert!(reader.next_transaction().unwrap().is_none());
    reader.finish().expect("input ended on a complete commit");
}

#[test]
fn raw_value_carrier_requires_stable_source_evidence() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    let carrier = RawValueCarrier::new(
        "postgresql.hstore.v1",
        "hstore",
        "sha256:definition-1",
        "binary",
        URL_SAFE_NO_PAD.encode([0, 255, 1]),
        Some("\"a\"=>\"b\""),
    );
    carrier
        .validate()
        .expect("complete carrier should validate");
    assert!(
        carrier.reversible.is_none(),
        "legacy carrier must not imply value recovery"
    );
    let legacy_value = LogicalValue::Raw {
        carrier: carrier.clone(),
    };
    assert!(
        legacy_value.validate().is_ok(),
        "legacy JSON remains readable"
    );
    assert!(
        !LogicalType::raw(
            "postgresql.hstore.v1",
            "hstore",
            "sha256:definition-1",
            "binary",
        )
        .matches_value(&legacy_value),
        "legacy carrier without recoverability evidence cannot qualify as a complete value"
    );
    assert!(
        !LogicalType::Opaque {
            source_type: "hstore".into(),
            format: "binary".into(),
        }
        .matches_value(&legacy_value)
    );
    assert_eq!(carrier.raw_bytes().unwrap(), [0, 255, 1]);
    let encoded = serde_json::to_vec(&carrier).unwrap();
    let decoded: RawValueCarrier = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, carrier);

    let mut incomplete = carrier;
    incomplete.codec_identity.clear();
    let error = incomplete.validate().unwrap_err();
    assert_eq!(error.code(), "raw_value_carrier.missing_evidence");

    let evidenced = RawValueCarrier::new(
        "postgresql.hstore.v1",
        "hstore",
        SOURCE_TYPE_DEFINITION_DIGEST,
        "binary",
        URL_SAFE_NO_PAD.encode([0, 255, 1]),
        None::<String>,
    )
    .with_source_context(hstore_representation_context(postgresql_cursor()), false)
    .expect("payload evidence should be recorded");
    evidenced
        .validate()
        .expect("nonreversible evidence validates");
    assert_eq!(evidenced.reversible, Some(false));
    assert_eq!(evidenced.payload_length, Some(3));
    assert!(
        LogicalValue::Raw {
            carrier: evidenced.clone(),
        }
        .validate()
        .is_err()
    );
    let mut corrupted = evidenced;
    corrupted.payload_sha256 = Some(format!("sha256:{}", "b".repeat(64)));
    assert_eq!(
        corrupted.validate().unwrap_err().code(),
        "raw_value_carrier.payload_digest_mismatch"
    );

    let reversible = RawValueCarrier::new(
        "postgresql.hstore.v1",
        "hstore",
        SOURCE_TYPE_DEFINITION_DIGEST,
        "binary",
        URL_SAFE_NO_PAD.encode([0, 255, 1]),
        Some("\"a\"=>\"b\""),
    )
    .with_source_context(hstore_representation_context(postgresql_cursor()), true)
    .expect("recoverability evidence should be recorded");
    let reversible_value = LogicalValue::Raw {
        carrier: reversible,
    };
    assert!(reversible_value.validate().is_ok());
    assert!(
        LogicalType::raw(
            "postgresql.hstore.v1",
            "hstore",
            SOURCE_TYPE_DEFINITION_DIGEST,
            "binary",
        )
        .matches_value(&reversible_value)
    );
    assert!(
        LogicalType::Opaque {
            source_type: "hstore".into(),
            format: "binary".into(),
        }
        .matches_value(&reversible_value)
    );
    assert!(
        !LogicalType::Opaque {
            source_type: "jsonb".into(),
            format: "binary".into(),
        }
        .matches_value(&reversible_value)
    );
    assert!(
        !LogicalType::Opaque {
            source_type: "hstore".into(),
            format: "text".into(),
        }
        .matches_value(&reversible_value)
    );

    let mut mismatched_fingerprint = match &reversible_value {
        LogicalValue::Raw { carrier } => carrier.clone(),
        _ => unreachable!(),
    };
    mismatched_fingerprint.source_definition_digest = "sha256:bbbb".into();
    assert_eq!(
        mismatched_fingerprint.validate().unwrap_err().code(),
        "raw_value_carrier.source_definition_digest_mismatch"
    );
    assert_eq!(
        RawValueCarrier::new(
            "postgresql.hstore.v1",
            "hstore",
            "sha256:definition-1",
            "binary",
            URL_SAFE_NO_PAD.encode([0, 255, 1]),
            None::<String>,
        )
        .with_source_context(hstore_representation_context(postgresql_cursor()), true)
        .unwrap_err()
        .code(),
        "raw_value_carrier.source_definition_digest_mismatch"
    );
}

fn representation_context(cursor: SourceCursor) -> SourceRepresentationContext {
    SourceRepresentationContext {
        connector: ConnectorIdentity::new("postgresql", "17"),
        server_build: ServerBuildIdentity::new("PostgreSQL", "community", "17.0", "17.0-build-1"),
        source_type_identity: "pg_catalog.numeric/1700/typmod:-1".into(),
        source_type_definition_digest: SOURCE_TYPE_DEFINITION_DIGEST.into(),
        protocol: "pgoutput-v1".into(),
        format: SourceRepresentationFormat::Binary,
        type_metadata: BTreeMap::from([
            ("oid".into(), "1700".into()),
            ("typmod".into(), "-1".into()),
        ]),
        source_cursor: cursor,
    }
}

fn hstore_representation_context(cursor: SourceCursor) -> SourceRepresentationContext {
    let mut context = representation_context(cursor);
    context.source_type_identity = "pg_catalog.hstore".into();
    context.type_metadata = BTreeMap::from([("oid".into(), "12345".into())]);
    context
}

#[test]
fn exact_numeric_values_preserve_unbounded_precision_signed_scale_and_specials() {
    let maximum_postgres_integer_part = "9".repeat(131_072);
    let arbitrary = LogicalValue::Decimal {
        unscaled: maximum_postgres_integer_part,
        scale: 0,
    };
    arbitrary.validate().expect("large exact decimal is valid");
    assert!(LogicalType::decimal_unbounded().matches_value(&arbitrary));

    for (unscaled, scale) in [
        ("1234567890123456789012345678901234567890", 16_383),
        ("1200", -3),
        ("NaN", 0),
        ("Infinity", 0),
        ("-Infinity", 0),
    ] {
        let value = LogicalValue::Decimal {
            unscaled: unscaled.into(),
            scale,
        };
        value.validate().expect("exact numeric value validates");
        let decoded: LogicalValue =
            serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(decoded, value);
    }

    LogicalType::decimal(2, -3)
        .validate()
        .expect("negative numeric scale is a declared semantic");
    LogicalType::decimal(2, 3)
        .validate()
        .expect("scale greater than precision is representable");
    assert!(LogicalType::decimal(0, 0).validate().is_err());
    assert!(
        LogicalType::decimal(2, -3).matches_value(&LogicalValue::Decimal {
            unscaled: "99".into(),
            scale: -3,
        })
    );
    assert!(
        LogicalType::decimal(2, -3).matches_value(&LogicalValue::Decimal {
            unscaled: "99000".into(),
            scale: 0,
        })
    );
    assert!(
        !LogicalType::decimal(2, -3).matches_value(&LogicalValue::Decimal {
            unscaled: "100000".into(),
            scale: 0,
        })
    );
    assert!(
        !LogicalType::decimal(2, -3).matches_value(&LogicalValue::Decimal {
            unscaled: "12345".into(),
            scale: 0,
        })
    );
    assert!(
        !LogicalType::decimal(2, -3).matches_value(&LogicalValue::Decimal {
            unscaled: "1200".into(),
            scale: -3,
        })
    );
    assert!(
        validate_value(&LogicalValue::Decimal {
            unscaled: "NaN".into(),
            scale: 1,
        })
        .is_err()
    );
    assert!(
        !LogicalType::decimal(10, 2).matches_value(&LogicalValue::Decimal {
            unscaled: "Infinity".into(),
            scale: 0,
        })
    );
    assert!(
        LogicalType::decimal(10, 0).matches_value(&LogicalValue::Decimal {
            unscaled: "NaN".into(),
            scale: 0,
        })
    );
    for special in ["Infinity", "-Infinity"] {
        assert!(
            !LogicalType::decimal(10, 0).matches_value(&LogicalValue::Decimal {
                unscaled: special.into(),
                scale: 0,
            }),
            "bounded decimal cannot contain {special}"
        );
    }
}

#[test]
fn temporal_values_preserve_bce_years_infinity_offsets_and_calendar_interval_parts() {
    for value in [
        LogicalValue::Date {
            year: -4_712,
            month: 2,
            day: 29,
        },
        LogicalValue::LocalDatetime {
            year: 0,
            month: 2,
            day: 29,
            hour: 23,
            minute: 59,
            second: 59,
            microsecond: 999_999,
        },
        LogicalValue::Date {
            year: 294_276,
            month: 12,
            day: 31,
        },
        LogicalValue::Year { value: i32::MAX },
        LogicalValue::LocalTime {
            hour: 24,
            minute: 0,
            second: 0,
            microsecond: 0,
        },
        LogicalValue::OffsetTime {
            hour: 24,
            minute: 0,
            second: 0,
            microsecond: 0,
            offset_seconds: -57_599,
        },
        LogicalValue::CalendarInterval {
            months: i32::MIN,
            days: i32::MAX,
            microseconds: i64::MIN,
        },
    ] {
        value.validate().expect("temporal boundary value validates");
        let decoded: LogicalValue =
            serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(decoded, value);
    }

    for (kind, value) in [
        (TemporalInfinityKind::Date, LogicalType::date()),
        (
            TemporalInfinityKind::LocalDatetime,
            LogicalType::local_datetime(6),
        ),
        (TemporalInfinityKind::Instant, LogicalType::instant(6)),
        (
            TemporalInfinityKind::CalendarInterval,
            LogicalType::calendar_interval(6),
        ),
    ] {
        let infinity = LogicalValue::TemporalInfinity {
            kind,
            negative: true,
        };
        infinity.validate().unwrap();
        assert!(value.matches_value(&infinity));
    }
    assert!(
        LogicalType::offset_time(6).matches_value(&LogicalValue::OffsetTime {
            hour: 15,
            minute: 59,
            second: 0,
            microsecond: 1,
            offset_seconds: 57_540,
        })
    );
    assert!(
        LogicalType::calendar_interval(6).matches_value(&LogicalValue::CalendarInterval {
            months: 1,
            days: 2,
            microseconds: 3_000_000,
        })
    );
    assert!(
        LogicalType::offset_time(3).matches_value(&LogicalValue::OffsetTime {
            hour: 12,
            minute: 0,
            second: 0,
            microsecond: 123_000,
            offset_seconds: -57_599,
        })
    );
    assert!(
        !LogicalType::offset_time(3).matches_value(&LogicalValue::OffsetTime {
            hour: 12,
            minute: 0,
            second: 0,
            microsecond: 123_456,
            offset_seconds: -57_599,
        })
    );
    assert!(
        LogicalType::calendar_interval(3).matches_value(&LogicalValue::CalendarInterval {
            months: 1,
            days: -2,
            microseconds: -123_000,
        })
    );
    assert!(
        !LogicalType::calendar_interval(3).matches_value(&LogicalValue::CalendarInterval {
            months: 1,
            days: -2,
            microseconds: -123_456,
        })
    );
    assert!(
        !LogicalType::instant(6).matches_value(&LogicalValue::Instant {
            unix_seconds: "0".into(),
            nanoseconds: 123_456_789,
        })
    );

    assert!(
        validate_value(&LogicalValue::Date {
            year: 0,
            month: 2,
            day: 30,
        })
        .is_err()
    );
    assert!(
        validate_value(&LogicalValue::OffsetTime {
            hour: 10,
            minute: 0,
            second: 0,
            microsecond: 0,
            offset_seconds: 57_600,
        })
        .is_err()
    );
    assert!(
        validate_value(&LogicalValue::LocalTime {
            hour: 24,
            minute: 0,
            second: 0,
            microsecond: 1,
        })
        .is_err()
    );
}

#[test]
fn source_representation_envelope_is_integrity_checked_and_not_a_logical_value() {
    let mut transaction = insert_transaction();
    let change = &mut transaction.changes[0];
    let cursor = change.source_cursor.clone();
    let envelope = SourceRepresentationEnvelope::new(
        representation_context(cursor.clone()),
        "binary",
        &[0, 255, 1],
    );
    envelope.validate().expect("source evidence is complete");
    assert_eq!(envelope.raw_bytes().unwrap(), [0, 255, 1]);

    let mut invalid_length = envelope.clone();
    invalid_length.payload_length += 1;
    assert_eq!(
        invalid_length.validate().unwrap_err().code(),
        "source_representation.payload_length_mismatch"
    );
    let mut invalid_digest = envelope.clone();
    invalid_digest.payload_sha256 = format!("sha256:{}", "b".repeat(64));
    assert_eq!(
        invalid_digest.validate().unwrap_err().code(),
        "source_representation.payload_digest_mismatch"
    );

    let column = &mut change.after.as_mut().unwrap()[0];
    column.primary_key_ordinal = None;
    column.datum = Datum::SourceRepresentationEnvelope(envelope.clone());
    let validated = validate(transaction).expect("envelope is a separate datum payload");
    let encoded = json(&validated).expect("envelope serializes in v0.3");
    assert!(encoded.contains("source_representation_envelope"));
    assert!(encoded.contains("payload_sha256"));
    let mut reader = JsonReader::new(encoded.as_bytes());
    let replayed = reader.next_transaction().unwrap().unwrap();
    reader.finish().unwrap();
    assert_eq!(
        replayed.transaction().changes[0].after.as_ref().unwrap()[0].datum,
        Datum::SourceRepresentationEnvelope(envelope)
    );
    assert_eq!(
        replayed.transaction().content_digest(),
        validated.transaction().content_digest()
    );
}

#[test]
fn representation_envelope_must_match_event_cursor_and_cannot_be_a_key() {
    let mut transaction = insert_transaction();
    let mut context = representation_context(transaction.changes[0].source_cursor.clone());
    context.source_cursor.value = "different-position".into();
    let envelope = SourceRepresentationEnvelope::new(context, "binary", &[1, 2, 3]);
    transaction.changes[0].after.as_mut().unwrap()[0].datum =
        Datum::SourceRepresentationEnvelope(envelope);
    assert!(validate(transaction.clone()).is_err());

    transaction.changes[0].source_cursor.value = "different-position".into();
    assert!(
        validate(transaction).is_err(),
        "envelopes cannot be row keys"
    );
}

#[test]
fn recursive_values_preserve_nulls_and_array_shape() {
    let value = LogicalValue::ArrayWithMetadata {
        elements: vec![
            LogicalValue::Null,
            LogicalValue::Struct {
                fields: vec![StructuredField {
                    name: "value".into(),
                    value: LogicalValue::Integer {
                        signed: true,
                        bits: 32,
                        value: "7".into(),
                    },
                }],
            },
        ],
        dimensions: 2,
        lower_bounds: vec![0, -2],
        dimension_lengths: vec![1, 2],
    };

    value.validate().expect("recursive value should validate");
    let roundtrip: LogicalValue =
        serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(roundtrip, value);

    let duplicate_map = LogicalValue::Map {
        entries: vec![
            MapEntry {
                key: LogicalValue::Text {
                    charset: "utf8".into(),
                    bytes_base64url: "YQ".into(),
                    text: Some("a".into()),
                },
                value: LogicalValue::Null,
            },
            MapEntry {
                key: LogicalValue::Text {
                    charset: "utf8".into(),
                    bytes_base64url: "YQ".into(),
                    text: Some("a".into()),
                },
                value: LogicalValue::Null,
            },
        ],
    };
    assert!(validate_value(&duplicate_map).is_err());
}

#[test]
fn stable_digests_are_reproducible_and_change_with_semantics() {
    let first = LogicalValue::Integer {
        signed: true,
        bits: 32,
        value: "7".into(),
    };
    let second = first.clone();
    assert_eq!(first.stable_digest(), second.stable_digest());

    let changed = LogicalValue::Integer {
        signed: true,
        bits: 32,
        value: "8".into(),
    };
    assert_ne!(second.stable_digest(), changed.stable_digest());

    let ordered = LogicalValue::Json {
        value: JsonValue::Object(vec![
            JsonEntry {
                key: "b".into(),
                value: JsonValue::Boolean(true),
            },
            JsonEntry {
                key: "a".into(),
                value: JsonValue::Boolean(false),
            },
        ]),
    };
    let reordered = LogicalValue::Json {
        value: JsonValue::Object(vec![
            JsonEntry {
                key: "a".into(),
                value: JsonValue::Boolean(false),
            },
            JsonEntry {
                key: "b".into(),
                value: JsonValue::Boolean(true),
            },
        ]),
    };
    assert_eq!(ordered.stable_digest(), reordered.stable_digest());
}

#[test]
fn logical_type_validation_and_matching_are_database_neutral() {
    let logical = LogicalType::domain(
        "positive_int",
        LogicalType::integer(true, 32),
        vec!["value > 0".into()],
        true,
        None,
        "sha256:domain-1",
    );
    logical
        .validate()
        .expect("domain definition should validate");
    assert!(logical.matches_value(&LogicalValue::Domain {
        value: Box::new(LogicalValue::Integer {
            signed: true,
            bits: 32,
            value: "7".into(),
        }),
    }));

    let invalid = LogicalType::array_with_metadata(LogicalType::integer(true, 32), 2, vec![0]);
    assert!(invalid.validate().is_err());
}

#[test]
fn json_v03_replays_raw_and_recursive_values_without_target_metadata() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::io::Cursor;

    let mut transaction = insert_transaction();
    transaction.changes[0].after.as_mut().unwrap()[0].datum = Datum::Value(LogicalValue::Raw {
        carrier: RawValueCarrier::new(
            "custom.codec.v1",
            "vendor_type",
            "sha256:source-definition",
            "binary",
            URL_SAFE_NO_PAD.encode([1, 2, 3]),
            Some("010203"),
        ),
    });
    let validated = validate(transaction).expect("raw value with evidence is valid");
    let encoded = json(&validated).unwrap();
    assert!(encoded.contains("custom.codec.v1"));

    let mut reader = JsonReader::new(Cursor::new(encoded));
    let replayed = reader.next_transaction().unwrap().unwrap();
    reader.finish().unwrap();
    assert_eq!(
        replayed.transaction().content_digest(),
        validated.transaction().content_digest()
    );
}

#[test]
fn json_v03_replays_legacy_array_metadata_without_inventing_shape() {
    use std::io::Cursor;

    let mut transaction = insert_transaction();
    transaction.changes[0].after.as_mut().unwrap()[0].datum =
        Datum::Value(LogicalValue::ArrayWithMetadata {
            elements: vec![
                LogicalValue::Integer {
                    signed: true,
                    bits: 32,
                    value: "1".into(),
                },
                LogicalValue::Null,
                LogicalValue::Integer {
                    signed: true,
                    bits: 32,
                    value: "3".into(),
                },
                LogicalValue::Integer {
                    signed: true,
                    bits: 32,
                    value: "4".into(),
                },
            ],
            dimensions: 2,
            lower_bounds: vec![0, 2],
            dimension_lengths: vec![2, 2],
        });
    let validated = validate(transaction).expect("array with complete shape is valid");
    let encoded = json(&validated).unwrap();

    let legacy = encoded
        .lines()
        .map(|line| {
            let mut row: serde_json::Value = serde_json::from_str(line).unwrap();
            remove_dimension_lengths(&mut row);
            serde_json::to_string(&row).unwrap()
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut reader = JsonReader::new(Cursor::new(legacy.as_bytes()));
    let replayed = reader
        .next_transaction()
        .unwrap_or_else(|error| panic!("legacy v0.3 transaction validation failed: {error}"))
        .unwrap_or_else(|| panic!("legacy v0.3 transaction ended before commit"));
    reader.finish().unwrap();
    assert!(matches!(
        &replayed.transaction().changes[0].after.as_ref().unwrap()[0].datum,
        Datum::Value(LogicalValue::ArrayWithMetadata {
            elements,
            dimensions: 2,
            lower_bounds,
            dimension_lengths,
        }) if elements.len() == 4 && lower_bounds == &[0, 2] && dimension_lengths.is_empty()
    ));
    let replayed_again = json(&replayed).unwrap();
    let mut reader = JsonReader::new(Cursor::new(replayed_again.as_bytes()));
    let replayed_again = reader.next_transaction().unwrap().unwrap();
    reader.finish().unwrap();
    assert_eq!(
        replayed.transaction().content_digest(),
        replayed_again.transaction().content_digest()
    );
}

fn remove_dimension_lengths(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("dimension_lengths");
            for nested in object.values_mut() {
                remove_dimension_lengths(nested);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                remove_dimension_lengths(item);
            }
        }
        _ => {}
    }
}

#[test]
fn contract_errors_expose_stable_categories() {
    let validation = ChangeEventValidationError::new("bad event");
    assert_eq!(validation.code(), "change_event.validation_failed");
    assert_eq!(validation.class(), FailureClass::ChangeEventValidation);

    let source = SourceContractError::new("bad source");
    assert_eq!(source.code(), "source_contract.invalid");
    assert_eq!(source.class(), FailureClass::SourceContract);

    let target = TargetCapabilityFailure::new("unsupported target");
    assert_eq!(target.class(), FailureClass::TargetCapability);
    assert_eq!(target.stable_code(), "target_capability.unspecified");
}

#[test]
fn json_reader_rejects_an_incomplete_transaction() {
    use std::io::{BufReader, Cursor};

    let validated = validate(insert_transaction()).unwrap();
    let output = json(&validated).unwrap();
    let truncated = output.lines().take(2).collect::<Vec<_>>().join("\n") + "\n";
    let mut reader = JsonReader::new(BufReader::new(Cursor::new(truncated)));
    assert!(reader.next_transaction().unwrap().is_none());
    assert!(reader.finish().is_err());
}

#[test]
fn accepts_all_three_mysql_versions() {
    for version in ["5.7.44-log", "8.0.45", "8.4.8"] {
        let mut tx = insert_transaction();
        tx.source.version = version.into();
        let validated = validate(tx).unwrap();
        let rows: Vec<serde_json::Value> = json(&validated)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        for (sequence, row) in rows.iter().enumerate() {
            assert_eq!(row["source"]["version"], version);
            assert_eq!(row["transaction"]["sequence"], sequence);
            assert_eq!(
                row["transaction"]["commit_cursor"]["display"],
                "binlog.000001:200"
            );
        }
    }
}

#[test]
fn rejects_integer_overflow_and_mismatched_update_images() {
    let mut tx = insert_transaction();
    tx.changes[0].after.as_mut().unwrap()[0].datum = Datum::Value(LogicalValue::Integer {
        signed: false,
        bits: 8,
        value: "256".into(),
    });
    assert!(validate(tx).is_err());
    let mut tx = insert_transaction();
    tx.changes[0].operation = Operation::Update;
    tx.changes[0].before = tx.changes[0].after.clone();
    tx.changes[0].after.as_mut().unwrap()[0].name = "other".into();
    assert!(validate(tx).is_err());
}

#[test]
fn generic_validation_restricts_presence_states_by_image_operation() {
    let mut insert = insert_transaction();
    insert.changes[0].after.as_mut().unwrap()[0].datum = Datum::Unavailable;
    assert!(validate(insert).is_err());

    let mut insert = insert_transaction();
    insert.changes[0].after.as_mut().unwrap()[0].datum = Datum::Unchanged;
    assert!(validate(insert).is_err());

    let mut update = insert_transaction();
    update.changes[0].operation = Operation::Update;
    update.changes[0].before = update.changes[0].after.clone();
    update.changes[0].before.as_mut().unwrap()[0].datum = Datum::Unchanged;
    assert!(validate(update).is_err());

    let mut update = insert_transaction();
    update.changes[0].operation = Operation::Update;
    update.changes[0].before = update.changes[0].after.clone();
    update.changes[0].after.as_mut().unwrap()[0].datum = Datum::Unavailable;
    assert!(validate(update).is_err());
}

#[test]
fn reads_historical_mysql_v01_source_identity() {
    let original = validate(insert_transaction()).unwrap();
    let text = json(&original)
        .unwrap()
        .replace(FORMAT, LEGACY_FORMAT)
        .replace(
            "\"id\":\"430c326c-ab91-11f1-a23b-0242ac160004\"",
            "\"server_uuid\":\"430c326c-ab91-11f1-a23b-0242ac160004\"",
        );
    let mut reader = JsonReader::new(std::io::Cursor::new(text));
    assert_eq!(
        reader
            .next_transaction()
            .unwrap()
            .unwrap()
            .transaction()
            .source,
        original.transaction().source
    );
    reader.finish().unwrap();
}
#[test]
fn mysql_snapshot_rejects_absent_values() {
    let tx = insert_transaction();
    let row = tx.changes[0].after.clone().unwrap();
    let batch = SnapshotBatch {
        source: tx.source,
        schema: "CDC_test".into(),
        table: "items".into(),
        rows: vec![row],
        last_in_table: true,
    };
    assert!(validate_snapshot(batch.clone()).is_ok());
    for datum in [Datum::Unavailable, Datum::Unchanged] {
        let mut invalid = batch.clone();
        let mut column = invalid.rows[0][0].clone();
        column.ordinal = 1;
        column.name = "other".into();
        column.primary_key_ordinal = None;
        column.datum = datum;
        invalid.rows[0].push(column);
        assert!(validate_snapshot(invalid).is_err());
    }
}

#[test]
fn snapshot_validation_is_source_agnostic() {
    let mut tx = insert_transaction();
    tx.source = Source {
        kind: "oracle".into(),
        version: "23c".into(),
        id: "opaque-source-identity".into(),
    };
    let batch = SnapshotBatch {
        source: tx.source,
        schema: "CDC_test".into(),
        table: "items".into(),
        rows: vec![tx.changes[0].after.clone().unwrap()],
        last_in_table: true,
    };
    assert!(validate_snapshot(batch).is_ok());
}

#[test]
fn snapshot_boundary_validates_only_opaque_cursor_shape() {
    let boundary = SnapshotBoundary {
        source: Source {
            kind: "oracle".into(),
            version: "23c".into(),
            id: "opaque-source-identity".into(),
        },
        cursor: SourceCursor {
            format: "oracle.scn.v1".into(),
            value: "opaque-boundary".into(),
            display: "SCN 42".into(),
        },
    };
    assert!(validate_snapshot_boundary(&boundary).is_ok());
}

fn compatibility_field(key: Option<usize>) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new("source-column-1", "source-schema-1"),
        ordinal: 0,
        name: "id".into(),
        native_type: "bigint".into(),
        logical_type: LogicalType::integer(true, 64),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: key,
        unique: key.is_some(),
        row_locator: key.is_some(),
    }
}

fn compatibility_mapping() -> SourceTypeMapping {
    SourceTypeMapping::new(
        ConnectorIdentity::new("mysql", "5.7"),
        "bigint",
        LogicalType::integer(true, 64),
        "mysql.type-mapping",
        "1",
    )
}

fn compatibility_rule(
    id: &str,
    qualification: QualificationLevel,
    risk: RiskLevel,
    requires_confirmation: bool,
    allows_key: bool,
) -> ConversionRule {
    ConversionRule {
        id: id.into(),
        version: "1".into(),
        qualification,
        risk,
        risk_code: (risk != RiskLevel::None).then(|| "test.risk".into()),
        requires_confirmation,
        options: Vec::new(),
        supported_operations: vec![Operation::Insert],
        supported_presence: vec![PresenceState::Value],
        allows_key,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: "qualification-evidence-1".into(),
    }
}

fn compatibility_manifest(
    target_native_type: &str,
    rule: ConversionRule,
    requires_primary_key: bool,
) -> TargetCapabilityManifest {
    TargetCapabilityManifest::new(
        ConnectorIdentity::new("mysql", "5.7"),
        ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "build-1"),
        vec![CapabilityEntry {
            code: "test.integer".into(),
            source_logical_type: LogicalType::integer(true, 64),
            target: TargetRepresentation::new(target_native_type),
            rule,
            supported_operations: Vec::new(),
            supported_presence: Vec::new(),
        }],
        requires_primary_key,
    )
}

#[test]
fn validated_manifest_rejects_tampering_before_reuse() {
    let mut manifest = compatibility_manifest(
        "bigint",
        compatibility_rule(
            "test.integer",
            QualificationLevel::Exact,
            RiskLevel::None,
            false,
            true,
        ),
        true,
    );
    let verified = ValidatedTargetCapabilityManifest::new(std::sync::Arc::new(manifest.clone()))
        .expect("valid manifest is accepted");
    assert_eq!(verified.as_manifest().digest, manifest.digest);
    manifest.capabilities[0].code = "changed-without-new-digest".into();
    assert!(
        ValidatedTargetCapabilityManifest::new(std::sync::Arc::new(manifest)).is_err(),
        "the cache must not accept a modified manifest with its old digest"
    );
}

fn compatibility_input<'a>(
    transaction: &'a ValidatedTransaction,
    source_field: FieldDefinition,
    target_field: FieldDefinition,
    mapping: SourceTypeMapping,
    manifest: &'a TargetCapabilityManifest,
    options: RouteOptions,
) -> CompatibilityInput<'a> {
    CompatibilityInput {
        transaction,
        source_field,
        target_field,
        source_type_mapping: mapping,
        source_connector: ConnectorIdentity::new("mysql", "5.7"),
        sink_connector: ConnectorIdentity::new("mysql", "5.7"),
        source_build: Some(ServerBuildIdentity::new(
            "mysql", "oracle", "5.7.44", "build-1",
        )),
        target_build: Some(manifest.target_build.clone()),
        manifest,
        options,
    }
}

fn compatibility_options() -> RouteOptions {
    RouteOptions {
        route_id: "route-1".into(),
        configuration_revision: "revision-1".into(),
        ..RouteOptions::default()
    }
}

fn target_capability_probe() -> TargetCapabilityProbe {
    TargetCapabilityProbe::new(
        ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "build-1"),
        "cdc",
        "items",
        "id",
        TargetColumnMetadata::new("target-schema-1")
            .with_native_type("bigint")
            .with_precision(19)
            .with_constraints(["PRIMARY KEY"])
            .with_indexes(["PRIMARY"]),
        vec![CapabilityProbeEntry::qualified("test.integer")],
        vec![CapabilityProbeEntry::installed("mysql_native")],
        TargetSessionProfile::new("mysql.strict.v1", [("sql_mode", "STRICT_ALL_TABLES")]),
    )
}

#[test]
fn target_capability_probe_is_content_addressed_and_qualification_is_explicit() {
    let probe = target_capability_probe();
    assert!(probe.verify_digest());
    assert!(probe.is_qualified());
    assert_eq!(probe.column_metadata.typmod, None);
    assert_eq!(probe.column_metadata.precision, Some(19));

    let mut changed = probe.clone();
    changed
        .session
        .settings
        .insert("time_zone".into(), "UTC".into());
    changed.refresh_digest();
    assert_ne!(probe.digest, changed.digest);
    assert!(changed.verify_digest());

    let mut unqualified = probe;
    unqualified.capabilities[0].status = CapabilityProbeStatus::PermissionDenied;
    unqualified.refresh_digest();
    assert!(!unqualified.is_qualified());
    assert!(unqualified.validate().is_ok());
}

#[test]
fn compatibility_plan_is_structured_serializable_and_reproducible() {
    let transaction = validate(insert_transaction()).unwrap();
    let source = compatibility_field(Some(0));
    let mut target = source.clone();
    target.reference = DefinitionReference::new("target-column-1", "target-schema-1");
    target.native_type = "BIGINT".into();
    let manifest = compatibility_manifest(
        "bigint",
        compatibility_rule(
            "integer.exact",
            QualificationLevel::Exact,
            RiskLevel::None,
            false,
            true,
        ),
        true,
    );
    let mut input = compatibility_input(
        &transaction,
        source,
        target,
        compatibility_mapping(),
        &manifest,
        compatibility_options(),
    );
    input.options.target_probe = Some(target_capability_probe());

    let result = explain_compatibility(input.clone()).unwrap();
    assert_eq!(result.status, CompatibilityStatus::Compatible);
    assert_eq!(result.qualification, QualificationLevel::Exact);
    assert!(result.is_selectable());
    let plan = result.plan.as_ref().expect("qualified field has a plan");
    assert!(plan.verify_digest());
    assert_eq!(
        plan.target_probe_digest,
        input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone())
    );
    assert_eq!(plan.locator_impact, LocatorImpact::Preserved);
    assert!(!plan.loss.value);
    assert_eq!(plan.confirmation, PlanConfirmationState::NotRequired);
    assert_eq!(
        plan.plan_digest,
        result.summary.as_ref().unwrap().plan_digest
    );
    let encoded = serde_json::to_string(&result).unwrap();
    assert!(encoded.contains("integer.exact"));
    assert!(!encoded.contains("\"value\":\"7\""));
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let decoded_manifest: TargetCapabilityManifest = serde_json::from_str(&manifest_json).unwrap();
    assert!(decoded_manifest.verify_digest());
    assert!(plan.validate_against(input.clone()).is_ok());

    let mut stale_input = input.clone();
    stale_input.target_field.reference.schema_fingerprint = "changed-target-schema".into();
    let stale = plan.validate_against(stale_input).unwrap_err();
    assert_eq!(stale.class(), FailureClass::StaleInput);

    let mut changed_probe_input = input.clone();
    let probe = changed_probe_input.options.target_probe.as_mut().unwrap();
    probe
        .session
        .settings
        .insert("time_zone".into(), "UTC".into());
    probe.refresh_digest();
    let invalidated = plan.validate_against(changed_probe_input).unwrap_err();
    assert_eq!(invalidated.code(), "compatibility.plan_inputs_changed");
    assert!(matches!(
        invalidated,
        CompatibilityError::PlanInvalidated(_)
    ));

    let repeated = explain_compatibility(input).unwrap();
    assert_eq!(
        repeated.plan.as_ref().unwrap().plan_digest,
        plan.plan_digest,
        "the same normalized inputs must produce the same plan digest"
    );
}

fn decimal_range_plan(source_scale: i32, target_supports_nan: bool) -> ColumnConversionPlan {
    let mut target = TargetRepresentation::new("numeric(10,2)");
    target.parameters = BTreeMap::from([
        ("range_kind".into(), "decimal".into()),
        ("range_check".into(), "strict".into()),
        ("rounding_mode".into(), "reject".into()),
        ("source_precision".into(), "10".into()),
        ("source_scale".into(), source_scale.to_string()),
        ("target_precision".into(), "10".into()),
        ("target_scale".into(), "2".into()),
        ("target_integer_digits".into(), "8".into()),
    ]);
    if target_supports_nan {
        target
            .parameters
            .insert("decimal_special_values".into(), "NaN".into());
    }
    ColumnConversionPlan {
        format: COMPATIBILITY_FORMAT.into(),
        route_id: "decimal-range-test".into(),
        configuration_revision: "1".into(),
        source_field: DefinitionReference::new("source-column", "source-schema"),
        target_field: DefinitionReference::new("target-column", "target-schema"),
        source_connector: ConnectorIdentity::new("postgresql", "17"),
        sink_connector: ConnectorIdentity::new("postgresql", "17"),
        source_build: None,
        target_build: None,
        source_mapping_id: "test.numeric".into(),
        source_mapping_version: "1".into(),
        target,
        rule: RuleReference {
            id: "test.decimal-range".into(),
            version: "1".into(),
        },
        rule_digest: String::new(),
        capability_code: "test.decimal-range".into(),
        capability_manifest_digest: String::new(),
        target_probe_digest: None,
        qualification: QualificationLevel::RangeChecked,
        risk: RiskLevel::Medium,
        risk_code: None,
        loss: LossAssessment::default(),
        examples: Vec::new(),
        locator_impact: LocatorImpact::NotUsed,
        confirmation: PlanConfirmationState::NotRequired,
        parameters: BTreeMap::new(),
        failure_policy: FailurePolicy::Reject,
        input_digest: String::new(),
        plan_digest: String::new(),
    }
}

fn exact_decimal_plan(target_supports_nan: bool) -> ColumnConversionPlan {
    let mut plan = decimal_range_plan(0, target_supports_nan);
    plan.target.parameters.clear();
    if target_supports_nan {
        plan.target
            .parameters
            .insert("decimal_special_values".into(), "NaN".into());
    }
    plan.qualification = QualificationLevel::Exact;
    plan
}

#[test]
fn decimal_plans_obey_target_special_value_capabilities() {
    let nan = LogicalValue::Decimal {
        unscaled: "NaN".into(),
        scale: 0,
    };
    assert!(validate_value_against_plan(&decimal_range_plan(2, true), &nan).is_ok());
    assert_eq!(
        validate_value_against_plan(&decimal_range_plan(2, false), &nan)
            .unwrap_err()
            .code,
        "target_capability.decimal_special_value_unsupported"
    );
    assert!(
        validate_value_against_plan(
            &decimal_range_plan(0, true),
            &LogicalValue::Decimal {
                unscaled: "Infinity".into(),
                scale: 0,
            },
        )
        .is_err()
    );
    assert!(
        validate_value_against_plan(
            &decimal_range_plan(2, true),
            &LogicalValue::Decimal {
                unscaled: "NaN".into(),
                scale: 1,
            },
        )
        .is_err()
    );

    assert!(validate_value_against_plan(&exact_decimal_plan(true), &nan).is_ok());
    assert_eq!(
        validate_value_against_plan(&exact_decimal_plan(false), &nan)
            .unwrap_err()
            .code,
        "target_capability.decimal_special_value_unsupported"
    );
    assert!(
        validate_value_against_plan(
            &exact_decimal_plan(true),
            &LogicalValue::Decimal {
                unscaled: "Infinity".into(),
                scale: 0,
            },
        )
        .is_err()
    );
}

#[test]
fn compatibility_requires_parameters_and_exact_risk_confirmation() {
    let mut raw = insert_transaction();
    raw.changes[0].after.as_mut().unwrap()[0].primary_key_ordinal = None;
    let transaction = validate(raw).unwrap();
    let source = compatibility_field(None);
    let mut target = source.clone();
    target.reference = DefinitionReference::new("target-column-1", "target-schema-1");
    target.native_type = "varchar(64)".into();
    let mut rule = compatibility_rule(
        "integer.explicit",
        QualificationLevel::ExplicitConversion,
        RiskLevel::High,
        true,
        false,
    );
    rule.options.push(OptionSpec {
        name: "encoding".into(),
        value_kind: OptionValueKind::Enum,
        required: true,
        default: None,
        allowed_values: vec!["decimal-string".into()],
    });
    let manifest = compatibility_manifest("varchar(64)", rule, false);
    let mut input = compatibility_input(
        &transaction,
        source,
        target,
        compatibility_mapping(),
        &manifest,
        compatibility_options(),
    );

    let missing = explain_compatibility(input.clone()).unwrap();
    assert_eq!(missing.status, CompatibilityStatus::NeedsConfiguration);
    assert_eq!(
        missing.failure.as_ref().unwrap().class,
        FailureClass::MissingConfirmation
    );

    input
        .options
        .parameters
        .insert("encoding".into(), "decimal-string".into());
    let pending = explain_compatibility(input.clone()).unwrap();
    assert_eq!(pending.status, CompatibilityStatus::NeedsConfirmation);
    let plan = pending.plan.as_ref().unwrap();
    assert!(pending.requires_confirmation);

    input.options.confirmations.push(RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "operator-1".into(),
        confirmed_at: "2026-09-17T00:00:00Z".into(),
        reason: Some("approved for this route only".into()),
    });
    let confirmed = explain_compatibility(input).unwrap();
    assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
    assert!(confirmed.failure.is_none());
    let confirmed_plan = confirmed.plan.as_ref().unwrap();
    assert_eq!(
        confirmed_plan.confirmation,
        PlanConfirmationState::Confirmed
    );
    assert!(confirmed_plan.loss.value);
    assert_eq!(confirmed_plan.locator_impact, LocatorImpact::ValueOnly);
}

#[test]
fn unqualified_target_probe_blocks_plan_selection() {
    let transaction = validate(insert_transaction()).unwrap();
    let source = compatibility_field(Some(0));
    let mut target = source.clone();
    target.reference = DefinitionReference::new("target-column-1", "target-schema-1");
    let manifest = compatibility_manifest(
        "bigint",
        compatibility_rule(
            "integer.exact",
            QualificationLevel::Exact,
            RiskLevel::None,
            false,
            true,
        ),
        true,
    );
    let mut input = compatibility_input(
        &transaction,
        source,
        target,
        compatibility_mapping(),
        &manifest,
        compatibility_options(),
    );
    let mut probe = target_capability_probe();
    probe.capabilities[0].status = CapabilityProbeStatus::PermissionDenied;
    probe.refresh_digest();
    input.options.target_probe = Some(probe);

    let error = explain_compatibility(input).unwrap_err();
    assert_eq!(error.code(), "target_capability.probe_not_qualified");
}

#[test]
fn compatibility_reports_unsupported_and_protects_key_fields() {
    let transaction = validate(insert_transaction()).unwrap();
    let source = compatibility_field(Some(0));
    let mut target = source.clone();
    target.reference = DefinitionReference::new("target-column-1", "target-schema-1");

    let unsupported_manifest = TargetCapabilityManifest::new(
        ConnectorIdentity::new("mysql", "5.7"),
        ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "build-1"),
        Vec::new(),
        true,
    );
    let unsupported = explain_compatibility(compatibility_input(
        &transaction,
        source.clone(),
        target.clone(),
        compatibility_mapping(),
        &unsupported_manifest,
        compatibility_options(),
    ))
    .unwrap();
    assert_eq!(unsupported.status, CompatibilityStatus::Unsupported);
    assert_eq!(
        unsupported.failure.as_ref().unwrap().class,
        FailureClass::TargetCapability
    );

    let blocked_manifest = compatibility_manifest(
        "varchar(64)",
        compatibility_rule(
            "integer.lossy",
            QualificationLevel::ExplicitConversion,
            RiskLevel::High,
            true,
            false,
        ),
        true,
    );
    target.native_type = "varchar(64)".into();
    let blocked = explain_compatibility(compatibility_input(
        &transaction,
        source,
        target,
        compatibility_mapping(),
        &blocked_manifest,
        compatibility_options(),
    ))
    .unwrap();
    assert_eq!(blocked.status, CompatibilityStatus::Blocked);
    assert_eq!(
        blocked.reason_code,
        "target_capability.lossy_key_conversion"
    );
    assert_eq!(blocked.risk, RiskLevel::Critical);
}

#[test]
fn compatibility_failures_distinguish_stale_input_and_source_contract() {
    let transaction = validate(insert_transaction()).unwrap();
    let mut source = compatibility_field(Some(0));
    source.reference = DefinitionReference::new("", "");
    let target = compatibility_field(Some(0));
    let manifest = compatibility_manifest(
        "bigint",
        compatibility_rule(
            "integer.exact",
            QualificationLevel::Exact,
            RiskLevel::None,
            false,
            true,
        ),
        true,
    );
    let stale = explain_compatibility(compatibility_input(
        &transaction,
        source,
        target.clone(),
        compatibility_mapping(),
        &manifest,
        compatibility_options(),
    ))
    .unwrap_err();
    assert_eq!(stale.class(), FailureClass::StaleInput);
    assert_eq!(stale.code(), "compatibility.missing_schema_fingerprint");

    let mut mapping = compatibility_mapping();
    mapping.logical_type = LogicalType::Boolean;
    let source_contract = explain_compatibility(compatibility_input(
        &transaction,
        compatibility_field(Some(0)),
        target,
        mapping,
        &manifest,
        compatibility_options(),
    ))
    .unwrap_err();
    assert_eq!(source_contract.class(), FailureClass::SourceContract);
    assert_eq!(
        source_contract.code(),
        "source_contract.type_mapping_mismatch"
    );

    let diagnostic = TargetCapabilityFailure::new("target representation is unavailable");
    assert_eq!(diagnostic.class(), FailureClass::TargetCapability);
    assert_eq!(diagnostic.stable_code(), "target_capability.unspecified");
    assert!(!diagnostic.is_retryable());
    let json = serde_json::to_string(&diagnostic).unwrap();
    assert!(json.contains("TARGET_CAPABILITY"));
    assert!(json.contains("target_capability.unspecified"));
}

#[test]
fn target_apply_error_contract_only_retries_safe_transaction_failures() {
    assert!(TargetApplyErrorKind::Connection.is_retryable());
    assert!(TargetApplyErrorKind::LockTimeout.is_retryable());
    assert!(!TargetApplyErrorKind::Conversion.is_retryable());
    assert!(!TargetApplyErrorKind::Constraint.is_retryable());
    assert!(!TargetApplyErrorKind::Sql.is_retryable());
    assert_eq!(
        TargetApplyErrorKind::CommitUnknown.retry_classification(),
        RetryClassification::CommitUnknown
    );
    assert_eq!(
        TargetApplyErrorKind::LockTimeout.stable_code(),
        "target_apply.lock_timeout"
    );
    let json = serde_json::to_string(&CommitResolution::Unprovable).unwrap();
    assert_eq!(json, "\"UNPROVABLE\"");
}
