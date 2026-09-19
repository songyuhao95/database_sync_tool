use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, CompatibilityInput, ConnectorIdentity, Datum,
    DefinitionReference, FieldDefinition, LogicalType, LogicalValue, Operation, RouteOptions,
    ServerBuildIdentity, SinkAdapter as _, Source, SourceCursor, SourceTypeMapping,
    ValidatedTransaction, validate,
};

fn cursor(position: u32) -> SourceCursor {
    let mut raw = b"mysql-bin.000001\0".to_vec();
    raw.extend_from_slice(&position.to_be_bytes());
    SourceCursor {
        format: "mysql.binlog.file-position.v1".into(),
        value: URL_SAFE_NO_PAD.encode(raw),
        display: format!("mysql-bin.000001:{position}"),
    }
}

fn value(logical_type: &LogicalType) -> LogicalValue {
    match logical_type {
        LogicalType::Integer { signed, bits } => LogicalValue::Integer {
            signed: *signed,
            bits: *bits,
            value: if *signed { "42" } else { "7" }.into(),
        },
        LogicalType::Decimal { scale, .. } => LogicalValue::Decimal {
            unscaled: "123456".into(),
            scale: *scale as usize,
        },
        LogicalType::Float { bits } => LogicalValue::Float {
            bits: *bits,
            ieee754_hex: if *bits == 32 {
                "3f800000"
            } else {
                "3ff0000000000000"
            }
            .into(),
        },
        LogicalType::Text { charset, .. } => LogicalValue::Text {
            charset: charset.clone(),
            bytes_base64url: URL_SAFE_NO_PAD.encode("hello"),
            text: Some("hello".into()),
        },
        LogicalType::Binary { .. } => LogicalValue::Binary {
            bytes_base64url: "AP8".into(),
        },
        LogicalType::Date => LogicalValue::Date {
            year: 2026,
            month: 9,
            day: 17,
        },
        LogicalType::LocalDatetime { .. } => LogicalValue::LocalDatetime {
            year: 2026,
            month: 9,
            day: 17,
            hour: 1,
            minute: 2,
            second: 3,
            microsecond: 123456,
        },
        LogicalType::Instant { .. } => LogicalValue::Instant {
            unix_seconds: "1700000000".into(),
            nanoseconds: 123456000,
        },
        LogicalType::Duration { .. } => LogicalValue::Duration {
            negative: false,
            hours: 1,
            minutes: 2,
            seconds: 3,
            microsecond: 123456,
        },
        LogicalType::Year => LogicalValue::Year { value: 2026 },
        LogicalType::Json { .. } => LogicalValue::Json {
            value: change_event::JsonValue::Object(vec![change_event::JsonEntry {
                key: "ok".into(),
                value: change_event::JsonValue::Boolean(true),
            }]),
        },
        other => panic!("fixture does not cover {other:?}"),
    }
}

fn fixture(
    mapping: &SourceTypeMapping,
    key: Option<usize>,
) -> (ValidatedTransaction, FieldDefinition) {
    let collation =
        matches!(mapping.logical_type, LogicalType::Text { .. }).then(|| "utf8mb4_bin".to_owned());
    let column = ColumnDatum {
        ordinal: 0,
        name: "value".into(),
        native_type: mapping.native_type.clone(),
        primary_key_ordinal: key,
        generated: false,
        collation: collation.clone(),
        datum: Datum::Value(value(&mapping.logical_type)),
    };
    let transaction = ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "430c326c-ab91-11f1-a23b-0242ac160004".into(),
        },
        id: "430c326c-ab91-11f1-a23b-0242ac160004:42".into(),
        begin_cursor: cursor(100),
        commit_cursor: cursor(200),
        changes: vec![change_event::RowChange {
            database: None,
            operation: Operation::Insert,
            schema: "CDC_test".into(),
            table: "compatibility".into(),
            source_cursor: cursor(110),
            source_timestamp: 1_700_000_000,
            schema_basis: "mysql57_fixture".into(),
            before: None,
            after: Some(vec![column]),
        }],
    };
    let validated = validate(transaction).unwrap();
    let field = FieldDefinition {
        reference: DefinitionReference::new("source-value", "source-schema"),
        ordinal: 0,
        name: "value".into(),
        native_type: mapping.native_type.clone(),
        logical_type: mapping.logical_type.clone(),
        nullable: false,
        collation,
        generated: false,
        primary_key_ordinal: key,
        unique: key.is_some(),
        row_locator: key.is_some(),
    };
    (validated, field)
}

fn input<'a>(
    transaction: &'a ValidatedTransaction,
    source_field: FieldDefinition,
    target_native_type: &str,
    manifest: &'a change_event::TargetCapabilityManifest,
    mapping: SourceTypeMapping,
) -> CompatibilityInput<'a> {
    let mut target_field = source_field.clone();
    target_field.reference = DefinitionReference::new("target-value", "target-schema");
    target_field.native_type = target_native_type.into();
    CompatibilityInput {
        transaction,
        source_field,
        target_field,
        source_type_mapping: mapping,
        source_connector: ConnectorIdentity::new("mysql", "5.7"),
        sink_connector: ConnectorIdentity::new("mysql", "8.0"),
        source_build: Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(manifest.target_build.clone()),
        manifest,
        options: RouteOptions {
            route_id: "route-57-80".into(),
            configuration_revision: "revision-1".into(),
            ..RouteOptions::default()
        },
    }
}

fn mapping(native_type: &str) -> SourceTypeMapping {
    mysql_5_7::source_type_mapping(
        native_type,
        native_type.starts_with("varchar").then_some("utf8mb4"),
        native_type.starts_with("varchar").then_some("utf8mb4_bin"),
    )
    .unwrap()
}

fn target_build() -> ServerBuildIdentity {
    ServerBuildIdentity::new("mysql", "oracle", "8.0.36", "mysql-8.0.36")
}

#[test]
fn mysql_57_source_mapping_and_mysql_80_sink_share_one_exact_plan() {
    let manifest = mysql_8_0::compatibility_manifest(target_build());
    for native_type in [
        "bigint unsigned",
        "decimal(30,6)",
        "varchar(255)",
        "varbinary(32)",
        "datetime(6)",
        "json",
    ] {
        let mapping = mapping(native_type);
        let (validated, source_field) = fixture(&mapping, Some(0));
        let result = mysql_8_0::plan_compatibility(input(
            &validated,
            source_field,
            native_type,
            &manifest,
            mapping,
        ))
        .unwrap();
        assert_eq!(result.status, change_event::CompatibilityStatus::Compatible);
        let plan = result.plan.as_ref().expect("qualified field has a plan");
        assert!(plan.verify_digest());
        assert_eq!(
            result.summary.as_ref().unwrap().plan_digest,
            plan.plan_digest
        );
    }

    let source_tx = fixture(&mapping("bigint unsigned"), Some(0)).0;
    let source_tx = mysql_5_7::validate_change_event(source_tx.transaction().clone()).unwrap();
    let sql = mysql_8_0::SinkAdapter::new().plan(&source_tx).unwrap();
    assert!(sql.statements().all(|statement| statement.contains('?')));
    assert!(sql.script().contains("START TRANSACTION;"));
    assert!(sql.script().contains("COMMIT;"));
}

#[test]
fn mysql_80_manifest_blocks_unqualified_bounds_and_collations() {
    let manifest = mysql_8_0::compatibility_manifest(target_build());
    let mapping_257 = mapping("varchar(257)");
    let (validated, source_field) = fixture(&mapping_257, Some(0));
    let result = mysql_8_0::plan_compatibility(input(
        &validated,
        source_field.clone(),
        "varchar(257)",
        &manifest,
        mapping_257.clone(),
    ))
    .unwrap();
    assert_eq!(
        result.status,
        change_event::CompatibilityStatus::Unsupported
    );
    assert_eq!(
        result.failure.as_ref().unwrap().class,
        change_event::FailureClass::TargetCapability
    );

    let mut mismatched = input(
        &validated,
        source_field,
        "varchar(255)",
        &manifest,
        mapping("varchar(255)"),
    );
    mismatched.target_field.collation = Some("utf8mb4_general_ci".into());
    let error = mysql_8_0::plan_compatibility(mismatched).unwrap_err();
    assert_eq!(error.code(), "target_capability.collation_mismatch");
    assert!(matches!(
        error,
        change_event::CompatibilityError::TargetCapability(failure)
            if failure.code == "target_capability.collation_mismatch"
    ));
}

#[test]
fn mysql_80_manifest_requires_a_primary_key_and_57_mapping_rejects_unknown_types() {
    let manifest = mysql_8_0::compatibility_manifest(target_build());
    let mapping = mapping("int");
    let (validated, source_field) = fixture(&mapping, None);
    let result =
        mysql_8_0::plan_compatibility(input(&validated, source_field, "int", &manifest, mapping))
            .unwrap();
    assert_eq!(result.status, change_event::CompatibilityStatus::Blocked);
    assert_eq!(result.reason_code, "target_capability.primary_key_required");

    let mapping = mysql_5_7::source_type_mapping("enum('a','b')", None, None).unwrap();
    assert_eq!(
        mapping.logical_type,
        LogicalType::Enum {
            members: vec!["a".into(), "b".into()]
        }
    );
}
