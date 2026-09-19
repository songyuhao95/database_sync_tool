use change_event::{
    ColumnConversionPlan, Datum, DefinitionReference, FieldCompatibilityInput, FieldDefinition,
    LogicalType, LogicalValue, Operation, PresenceState, RiskConfirmation, RouteOptions,
    ServerBuildIdentity, SourceTypeMapping, TargetCapabilityManifest, validate_datum_against_plan,
    validate_value_against_plan,
};

fn field(
    lineage: &str,
    name: &str,
    native_type: &str,
    logical_type: LogicalType,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new(lineage, format!("{lineage}-fingerprint")),
        ordinal: 0,
        name: name.into(),
        native_type: native_type.into(),
        logical_type,
        nullable: true,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    }
}

fn options() -> RouteOptions {
    RouteOptions {
        route_id: "range-route".into(),
        configuration_revision: "range-revision".into(),
        ..RouteOptions::default()
    }
}

fn range_input<'a>(
    source_field: FieldDefinition,
    target_field: FieldDefinition,
    mapping: SourceTypeMapping,
    manifest: &'a TargetCapabilityManifest,
) -> FieldCompatibilityInput<'a> {
    FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: None,
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: mapping,
        source_field,
        target_field,
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: true,
        options: options(),
    }
}

fn confirm(plan: &ColumnConversionPlan) -> RiskConfirmation {
    RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "range-operator".into(),
        confirmed_at: "2026-09-18T00:00:00Z".into(),
        reason: Some("accepted fixed rejection-on-overflow policy".into()),
    }
}

#[test]
fn every_sink_manifest_publishes_range_checked_numeric_capabilities() {
    let mysql57 = mysql_5_7::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "5.7.44",
        "mysql-5.7.44",
    ));
    let mysql80 = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let mysql84 = mysql_8_4::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.4.8",
        "mysql-8.4.8",
    ));
    let postgres = postgresql_15::compatibility_manifest(ServerBuildIdentity::new(
        "postgresql",
        "community",
        "15.19",
        "postgres-15.19",
    ));

    for manifest in [&mysql57, &mysql80, &mysql84, &postgres] {
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.rule.qualification == change_event::QualificationLevel::RangeChecked
                && entry.target.parameters.get("range_kind") == Some(&"integer".into())
        }));
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.rule.qualification == change_event::QualificationLevel::RangeChecked
                && entry.target.parameters.get("range_kind") == Some(&"decimal".into())
        }));
    }
}

#[test]
fn integer_range_plan_gates_confirmation_and_rejects_both_signs_of_overflow() {
    let build = ServerBuildIdentity::new("mysql", "oracle", "8.0.36", "mysql-8.0.36");
    let manifest = mysql_8_0::compatibility_manifest(build);
    let source = mysql_5_7::source_type_mapping("bigint", None, None).unwrap();
    let source_field = field(
        "source.bigint",
        "amount",
        "bigint",
        source.logical_type.clone(),
    );
    let target_field = field(
        "target.int",
        "amount",
        "int",
        LogicalType::integer(true, 32),
    );

    let pending = change_event::plan_field_compatibility(range_input(
        source_field.clone(),
        target_field.clone(),
        source.clone(),
        &manifest,
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    assert_eq!(
        pending.qualification,
        change_event::QualificationLevel::RangeChecked
    );
    assert!(!pending.is_selectable());
    let pending_plan = pending.plan.clone().expect("pending plan is auditable");
    assert_eq!(pending_plan.target.native_type, "int");
    assert_eq!(pending_plan.target.parameters["target_bits"], "32");
    assert_eq!(pending_plan.target.parameters["target_signed"], "true");
    assert!(pending_plan.verify_digest());

    let mut confirmed_input = range_input(source_field, target_field, source, &manifest);
    confirmed_input
        .options
        .confirmations
        .push(confirm(&pending_plan));
    let confirmed = change_event::plan_field_compatibility(confirmed_input).unwrap();
    assert_eq!(
        confirmed.status,
        change_event::CompatibilityStatus::Compatible
    );
    let plan = confirmed.plan.unwrap();

    for value in ["-2147483648", "2147483647"] {
        validate_value_against_plan(
            &plan,
            &LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: value.into(),
            },
        )
        .unwrap();
    }
    for value in ["-2147483649", "2147483648"] {
        let failure = validate_value_against_plan(
            &plan,
            &LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: value.into(),
            },
        )
        .unwrap_err();
        assert_eq!(failure.code, "target_capability.integer_overflow");
    }
    validate_datum_against_plan(&plan, &Datum::Null).unwrap();
    validate_datum_against_plan(&plan, &Datum::Unchanged).unwrap();
    assert!(validate_datum_against_plan(&plan, &Datum::Unavailable).is_err());
}

#[test]
fn decimal_range_plan_rejects_nonzero_scale_loss_and_precision_overflow() {
    let build = ServerBuildIdentity::new("mysql", "oracle", "8.4.8", "mysql-8.4.8");
    let manifest = mysql_8_4::compatibility_manifest(build);
    let source = mysql_5_7::source_type_mapping("decimal(30,6)", None, None).unwrap();
    let source_field = field(
        "source.decimal",
        "amount",
        "decimal(30,6)",
        source.logical_type.clone(),
    );
    let target_field = field(
        "target.decimal",
        "amount",
        "decimal(10,2)",
        LogicalType::decimal(10, 2),
    );
    let pending = change_event::plan_field_compatibility(range_input(
        source_field.clone(),
        target_field.clone(),
        source.clone(),
        &manifest,
    ))
    .unwrap();
    let pending_plan = pending.plan.clone().expect("pending plan is auditable");
    assert_eq!(pending_plan.target.parameters["source_scale"], "6");
    assert_eq!(pending_plan.target.parameters["target_precision"], "10");
    assert_eq!(pending_plan.target.parameters["target_scale"], "2");

    let mut input = range_input(source_field, target_field, source, &manifest);
    input.options.confirmations.push(confirm(&pending_plan));
    let plan = change_event::plan_field_compatibility(input)
        .unwrap()
        .plan
        .unwrap();

    validate_value_against_plan(
        &plan,
        &LogicalValue::Decimal {
            unscaled: "1234560000".into(),
            scale: 6,
        },
    )
    .unwrap();
    validate_value_against_plan(
        &plan,
        &LogicalValue::Decimal {
            unscaled: "0".into(),
            scale: 6,
        },
    )
    .unwrap();
    for (unscaled, code) in [
        ("1234567000", "target_capability.decimal_rounding_required"),
        ("123456789012340000", "target_capability.decimal_overflow"),
    ] {
        let failure = validate_value_against_plan(
            &plan,
            &LogicalValue::Decimal {
                unscaled: unscaled.into(),
                scale: 6,
            },
        )
        .unwrap_err();
        assert_eq!(failure.code, code);
    }
}

#[test]
fn float_range_plan_rejects_rounding_and_non_finite_values() {
    let build = ServerBuildIdentity::new("mysql", "oracle", "8.4.8", "mysql-8.4.8");
    let manifest = mysql_8_4::compatibility_manifest(build);
    let source = mysql_5_7::source_type_mapping("double", None, None).unwrap();
    let source_field = field(
        "source.double",
        "ratio",
        "double",
        source.logical_type.clone(),
    );
    let target_field = field("target.float", "ratio", "float", LogicalType::float(32));
    let pending = change_event::plan_field_compatibility(range_input(
        source_field.clone(),
        target_field.clone(),
        source.clone(),
        &manifest,
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    let mut input = range_input(source_field, target_field, source, &manifest);
    input
        .options
        .confirmations
        .push(confirm(pending.plan.as_ref().unwrap()));
    let plan = change_event::plan_field_compatibility(input)
        .unwrap()
        .plan
        .unwrap();

    validate_value_against_plan(
        &plan,
        &LogicalValue::Float {
            bits: 64,
            ieee754_hex: "3ff8000000000000".into(),
        },
    )
    .unwrap();
    for ieee754_hex in ["3fb999999999999a", "7ff0000000000000"] {
        let failure = validate_value_against_plan(
            &plan,
            &LogicalValue::Float {
                bits: 64,
                ieee754_hex: ieee754_hex.into(),
            },
        )
        .unwrap_err();
        assert_eq!(failure.code, "target_capability.float_rounding_or_overflow");
    }
}

#[test]
fn range_plan_digests_and_confirmation_are_stable() {
    let build = ServerBuildIdentity::new("postgresql", "community", "15.19", "postgres-15.19");
    let manifest = postgresql_15::compatibility_manifest(build);
    let source = postgresql_15::source_type_mapping("numeric(30,6)").unwrap();
    let input = range_input(
        field(
            "source.numeric",
            "amount",
            "numeric(30,6)",
            source.logical_type.clone(),
        ),
        field(
            "target.numeric",
            "amount",
            "numeric(10,2)",
            LogicalType::decimal(10, 2),
        ),
        source,
        &manifest,
    );
    let pending = change_event::plan_field_compatibility(input.clone()).unwrap();
    let plan = pending.plan.unwrap();
    let mut confirmed = input;
    confirmed.options.confirmations.push(confirm(&plan));
    let first = change_event::plan_field_compatibility(confirmed.clone())
        .unwrap()
        .plan
        .unwrap();
    let second = change_event::plan_field_compatibility(confirmed)
        .unwrap()
        .plan
        .unwrap();
    assert_eq!(first, second);
    assert!(first.verify_digest());
}
