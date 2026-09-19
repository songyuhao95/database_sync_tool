use change_event::{
    ChangeTransaction, ColumnConversionPlan, ColumnDatum, Datum, DefinitionReference,
    FieldCompatibilityInput, FieldDefinition, LogicalValue, Operation, PresenceState, RouteOptions,
    RowChange, ServerBuildIdentity, Source, SourceCursor,
};

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "fixture".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn transaction(value: LogicalValue, native_type: &str) -> ChangeTransaction {
    ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
        id: "tx-json-enum-set".into(),
        begin_cursor: cursor("begin"),
        commit_cursor: cursor("commit"),
        changes: vec![RowChange {
            database: Some("db".into()),
            operation: Operation::Insert,
            schema: "s".into(),
            table: "t".into(),
            source_cursor: cursor("row"),
            source_timestamp: 1,
            schema_basis: "fixture".into(),
            before: None,
            after: Some(vec![
                ColumnDatum {
                    ordinal: 0,
                    name: "id".into(),
                    native_type: "int".into(),
                    primary_key_ordinal: Some(0),
                    generated: false,
                    collation: None,
                    datum: Datum::Value(LogicalValue::Integer {
                        signed: true,
                        bits: 32,
                        value: "1".into(),
                    }),
                },
                ColumnDatum {
                    ordinal: 1,
                    name: "value".into(),
                    native_type: native_type.into(),
                    primary_key_ordinal: None,
                    generated: false,
                    collation: None,
                    datum: Datum::Value(value),
                },
            ]),
        }],
    }
}

fn source_field(mapping: &change_event::SourceTypeMapping) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.value", "source-fingerprint"),
        ordinal: 1,
        name: "value".into(),
        native_type: mapping.native_type.clone(),
        logical_type: mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    }
}

fn target_field(
    mapping: &change_event::SourceTypeMapping,
    collation: Option<&str>,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.value-target", "target-fingerprint"),
        ordinal: 1,
        name: "value".into(),
        native_type: mapping.native_type.clone(),
        logical_type: mapping.logical_type.clone(),
        nullable: false,
        collation: collation.map(str::to_owned),
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    }
}

fn mysql_input<'a>(
    _transaction: &'a change_event::ValidatedTransaction,
    source: FieldDefinition,
    target: FieldDefinition,
    mapping: change_event::SourceTypeMapping,
    manifest: &'a change_event::TargetCapabilityManifest,
    options: RouteOptions,
) -> FieldCompatibilityInput<'a> {
    FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: mapping,
        source_field: source,
        target_field: target,
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: true,
        options,
    }
}

fn options() -> RouteOptions {
    RouteOptions {
        route_id: "route-json-enum-set".into(),
        configuration_revision: "route-json-enum-set:r1".into(),
        ..RouteOptions::default()
    }
}

fn confirm(plan: &ColumnConversionPlan) -> change_event::RiskConfirmation {
    change_event::RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "test-operator".into(),
        confirmed_at: "2026-09-18T00:00:00Z".into(),
        reason: Some("accepted the explicit field-local conversion".into()),
    }
}

fn json_value() -> LogicalValue {
    LogicalValue::Json {
        value: change_event::JsonValue::Object(vec![
            change_event::JsonEntry {
                key: "b".into(),
                value: change_event::JsonValue::Array(vec![
                    change_event::JsonValue::Boolean(true),
                    change_event::JsonValue::Decimal {
                        unscaled: "1000".into(),
                        scale: 3,
                    },
                ]),
            },
            change_event::JsonEntry {
                key: "a".into(),
                value: change_event::JsonValue::SignedInteger("0001".into()),
            },
        ]),
    }
}

#[test]
fn json_structured_normalized_text_and_raw_text_are_distinct() {
    let source_mapping = mysql_5_7::source_type_mapping("json", None, None).unwrap();
    let validated = change_event::validate(transaction(json_value(), "json")).unwrap();
    let source = source_field(&source_mapping);
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));

    let structured_mapping = mysql_8_0::source_type_mapping("json", None, None).unwrap();
    let structured = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source.clone(),
        target_field(&structured_mapping, None),
        source_mapping.clone(),
        &manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(
        structured.status,
        change_event::CompatibilityStatus::Compatible
    );
    let structured_plan = structured.plan.unwrap();
    assert_eq!(
        structured_plan.target.parameters["json_strategy"],
        "structured"
    );
    let structured_tx = change_event::convert_transaction_with_plans(
        validated.transaction().clone(),
        std::slice::from_ref(&structured_plan),
    )
    .unwrap();
    assert!(matches!(
        structured_tx.changes[0].after.as_ref().unwrap()[1].datum,
        Datum::Value(LogicalValue::Json { .. })
    ));

    let postgres_manifest = postgresql_15::compatibility_manifest(ServerBuildIdentity::new(
        "postgresql",
        "community",
        "15.8",
        "postgresql-15.8",
    ));
    let postgres_json = postgresql_15::source_type_mapping("jsonb").unwrap();
    let postgres_plan = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source.clone(),
        target_field(&postgres_json, None),
        source_mapping.clone(),
        &postgres_manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(
        postgres_plan.status,
        change_event::CompatibilityStatus::Compatible
    );
    assert_eq!(
        postgres_plan.plan.unwrap().target.parameters["json_strategy"],
        "structured"
    );

    let text_mapping =
        mysql_8_0::source_type_mapping("longtext", Some("utf8mb4"), Some("utf8mb4_bin")).unwrap();
    let mut configured = options();
    configured.parameters.extend([
        ("target_charset".into(), "utf8mb4".into()),
        ("target_length".into(), "4294967295".into()),
        ("target_length_unit".into(), "bytes".into()),
        ("target_collation".into(), "utf8mb4_bin".into()),
        ("json_strategy".into(), "normalized_text".into()),
    ]);
    let pending = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source.clone(),
        target_field(&text_mapping, Some("utf8mb4_bin")),
        source_mapping.clone(),
        &manifest,
        configured.clone(),
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    let pending_plan = pending.plan.as_ref().unwrap();
    assert_eq!(
        pending_plan.target.parameters["json_strategy"],
        "normalized_text"
    );
    configured.confirmations.push(confirm(pending_plan));
    let confirmed = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source.clone(),
        target_field(&text_mapping, Some("utf8mb4_bin")),
        source_mapping.clone(),
        &manifest,
        configured,
    ))
    .unwrap();
    assert_eq!(
        confirmed.status,
        change_event::CompatibilityStatus::Compatible
    );
    let text_plan = confirmed.plan.unwrap();
    let converted = change_event::convert_transaction_with_plans(
        validated.transaction().clone(),
        std::slice::from_ref(&text_plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Text { text, .. }) =
        &converted.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected normalized JSON text");
    };
    assert_eq!(text.as_deref(), Some(r#"{"a":1,"b":[true,1.000]}"#));

    let mut raw_options = options();
    raw_options.parameters.extend([
        ("target_charset".into(), "utf8mb4".into()),
        ("target_length".into(), "4294967295".into()),
        ("target_length_unit".into(), "bytes".into()),
        ("target_collation".into(), "utf8mb4_bin".into()),
        ("json_strategy".into(), "raw_text".into()),
    ]);
    let raw = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source.clone(),
        target_field(&text_mapping, Some("utf8mb4_bin")),
        source_mapping.clone(),
        &manifest,
        raw_options,
    ))
    .unwrap();
    assert_eq!(raw.status, change_event::CompatibilityStatus::Blocked);
    assert_eq!(raw.reason_code, "target_capability.invalid_parameters");
    assert!(raw.explanation.contains("raw JSON text"));

    let mut bad = validated.transaction().clone();
    bad.changes[0].after.as_mut().unwrap()[1].datum = Datum::Value(LogicalValue::Json {
        value: change_event::JsonValue::DoubleBits("not-hex".into()),
    });
    let failure =
        change_event::convert_transaction_with_plans(bad.clone(), &[text_plan]).unwrap_err();
    assert_eq!(failure.code, "target_capability.json_normalization_failed");
    assert!(matches!(
        bad.changes[0].after.as_ref().unwrap()[1].datum,
        Datum::Value(LogicalValue::Json {
            value: change_event::JsonValue::DoubleBits(ref bits)
        }) if bits == "not-hex"
    ));
}

#[test]
fn enum_uses_labels_and_set_preserves_member_set_semantics() {
    let enum_source_mapping =
        mysql_5_7::source_type_mapping("enum('Ready','blocked')", None, None).unwrap();
    let enum_value = LogicalValue::Enum {
        label: "blocked".into(),
    };
    let enum_validated =
        change_event::validate(transaction(enum_value.clone(), "enum('Ready','blocked')")).unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let same_target =
        mysql_8_0::source_type_mapping("enum('Ready','blocked')", None, None).unwrap();
    let same = change_event::plan_field_compatibility(mysql_input(
        &enum_validated,
        source_field(&enum_source_mapping),
        target_field(&same_target, None),
        enum_source_mapping.clone(),
        &manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(same.status, change_event::CompatibilityStatus::Compatible);
    let same_plan = same.plan.unwrap();
    let converted = change_event::convert_transaction_with_plans(
        enum_validated.transaction().clone(),
        std::slice::from_ref(&same_plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Enum { label }) =
        &converted.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected ENUM label");
    };
    assert_eq!(label, "blocked");

    let reordered_target =
        mysql_8_0::source_type_mapping("enum('blocked','Ready')", None, None).unwrap();
    let mut reordered_options = options();
    let pending = change_event::plan_field_compatibility(mysql_input(
        &enum_validated,
        source_field(&enum_source_mapping),
        target_field(&reordered_target, None),
        enum_source_mapping.clone(),
        &manifest,
        reordered_options.clone(),
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    assert!(pending.explanation.contains("confirmation") || pending.requires_confirmation);
    reordered_options
        .confirmations
        .push(confirm(pending.plan.as_ref().unwrap()));
    let reordered = change_event::plan_field_compatibility(mysql_input(
        &enum_validated,
        source_field(&enum_source_mapping),
        target_field(&reordered_target, None),
        enum_source_mapping.clone(),
        &manifest,
        reordered_options,
    ))
    .unwrap();
    let reordered_plan = reordered.plan.unwrap();
    assert_eq!(
        reordered_plan.target.parameters["value_strategy"],
        "enum_label"
    );
    let reordered_tx = change_event::convert_transaction_with_plans(
        enum_validated.transaction().clone(),
        std::slice::from_ref(&reordered_plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Enum { label }) =
        &reordered_tx.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected reordered ENUM label");
    };
    assert_eq!(label, "blocked");
    assert_eq!(
        change_event::validate_value_against_plan(
            &reordered_plan,
            &LogicalValue::Enum {
                label: "unknown".into()
            },
        )
        .unwrap_err()
        .code,
        "target_capability.enum_unknown_label"
    );

    let set_source_mapping = mysql_5_7::source_type_mapping("set('a','b')", None, None).unwrap();
    let set_target_mapping = mysql_8_0::source_type_mapping("set('b','a')", None, None).unwrap();
    let set_value = LogicalValue::Set {
        members: vec!["a".into(), "b".into()],
    };
    let set_validated =
        change_event::validate(transaction(set_value.clone(), "set('a','b')")).unwrap();
    let set_result = change_event::plan_field_compatibility(mysql_input(
        &set_validated,
        source_field(&set_source_mapping),
        target_field(&set_target_mapping, None),
        set_source_mapping.clone(),
        &manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(
        set_result.status,
        change_event::CompatibilityStatus::Compatible
    );
    let set_plan = set_result.plan.unwrap();
    let set_converted = change_event::convert_transaction_with_plans(
        set_validated.transaction().clone(),
        std::slice::from_ref(&set_plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Set { members }) =
        &set_converted.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected SET members");
    };
    assert_eq!(members, &["b".to_owned(), "a".to_owned()]);
    assert_eq!(
        change_event::validate_value_against_plan(
            &set_plan,
            &LogicalValue::Set {
                members: vec!["a".into(), "a".into()],
            },
        )
        .unwrap_err()
        .code,
        "target_capability.set_duplicate_member"
    );
    assert_eq!(
        change_event::validate_value_against_plan(
            &set_plan,
            &LogicalValue::Set {
                members: vec!["c".into()],
            },
        )
        .unwrap_err()
        .code,
        "target_capability.set_unknown_member"
    );
    let missing_target = mysql_8_0::source_type_mapping("set('a')", None, None).unwrap();
    let missing = change_event::plan_field_compatibility(mysql_input(
        &set_validated,
        source_field(&set_source_mapping),
        target_field(&missing_target, None),
        set_source_mapping,
        &manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(
        missing.status,
        change_event::CompatibilityStatus::Unsupported
    );
}

#[test]
fn target_capability_shortfalls_block_json_enum_set_without_text_fallback() {
    let source_mapping = mysql_5_7::source_type_mapping("set('a','b')", None, None).unwrap();
    let validated = change_event::validate(transaction(
        LogicalValue::Set {
            members: vec!["a".into()],
        },
        "set('a','b')",
    ))
    .unwrap();
    let manifest = postgresql_15::compatibility_manifest(ServerBuildIdentity::new(
        "postgresql",
        "community",
        "15.8",
        "postgresql-15.8",
    ));
    let target = postgresql_15::source_type_mapping("text").unwrap();
    let result = change_event::plan_field_compatibility(mysql_input(
        &validated,
        source_field(&source_mapping),
        target_field(&target, None),
        source_mapping,
        &manifest,
        options(),
    ))
    .unwrap();
    assert_eq!(
        result.status,
        change_event::CompatibilityStatus::Unsupported
    );
    assert_eq!(
        result.reason_code,
        "target_capability.no_qualified_representation"
    );
}
