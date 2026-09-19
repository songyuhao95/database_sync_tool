//! Cross-connector planning fixtures for PostgreSQL 15 and MySQL sinks.
//!
//! These tests exercise the public SourceTypeMapping -> Sink Manifest ->
//! ColumnConversionPlan seam directly. They intentionally do not call a
//! connector-pair helper or use row values to infer a mapping.

use change_event::{
    CompatibilityStatus, ConnectorIdentity, DefinitionReference, FieldCompatibilityInput,
    FieldDefinition, LogicalType, Operation, PresenceState, RiskConfirmation, RouteOptions,
    ServerBuildIdentity, SourceTypeMapping, TargetCapabilityManifest,
};

fn field(
    lineage: &str,
    native_type: &str,
    logical_type: LogicalType,
    key: Option<usize>,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new(lineage, format!("schema-{lineage}")),
        ordinal: 0,
        name: "value".into(),
        native_type: native_type.into(),
        logical_type,
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: key,
        unique: key.is_some(),
        row_locator: key.is_some(),
    }
}

fn options() -> RouteOptions {
    RouteOptions {
        route_id: "route-pg15-mysql".into(),
        configuration_revision: "source-7:sink-11".into(),
        ..RouteOptions::default()
    }
}

fn build(product: &str, version: &str) -> ServerBuildIdentity {
    ServerBuildIdentity::new(product, "test", version, format!("{product}-{version}"))
}

fn plan(
    source: FieldDefinition,
    target: FieldDefinition,
    mapping: SourceTypeMapping,
    manifest: &TargetCapabilityManifest,
    options: RouteOptions,
) -> change_event::CompatibilityResult {
    change_event::plan_field_compatibility(FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(build(&mapping.connector.kind, &mapping.connector.version)),
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
            PresenceState::Unavailable,
        ],
        source_has_primary_key: true,
        options,
    })
    .expect("fixture input is valid")
}

fn mysql_manifests() -> [TargetCapabilityManifest; 3] {
    [
        mysql_5_7::compatibility_manifest(build("mysql", "5.7.44")),
        mysql_8_0::compatibility_manifest(build("mysql", "8.0.46")),
        mysql_8_4::compatibility_manifest(build("mysql", "8.4.8")),
    ]
}

#[test]
fn postgresql15_source_plans_to_every_mysql_sink_without_pair_branches() {
    for manifest in mysql_manifests() {
        for (native, target_native, expected) in [
            ("integer", "int", "integer.32.signed"),
            ("numeric(30,6)", "decimal(30,6)", "decimal.30.6"),
            ("text", "longtext", "text.UTF8.longtext.unbounded"),
            ("timestamp(6) with time zone", "timestamp(6)", "instant.6"),
            ("jsonb", "json", "json"),
        ] {
            let mapping = postgresql_15::source_type_mapping(native).unwrap();
            let source = field("pg-source", native, mapping.logical_type.clone(), Some(0));
            let target = field(
                "mysql-target",
                target_native,
                LogicalType::Opaque {
                    source_type: target_native.into(),
                    format: "catalog-native".into(),
                },
                Some(0),
            );
            let result = plan(source, target, mapping, &manifest, options());
            assert_eq!(result.status, CompatibilityStatus::Compatible, "{native}");
            let plan = result.plan.expect("qualified source has a plan");
            assert!(plan.verify_digest());
            assert!(plan.capability_code.contains(expected), "{plan:?}");
            assert_eq!(
                plan.source_connector,
                ConnectorIdentity::new("postgresql", "15")
            );
            assert_eq!(plan.sink_connector, manifest.connector);
        }
    }
}

#[test]
fn mysql_sources_plan_to_postgresql15_with_declared_bounds_and_time_semantics() {
    let manifest = postgresql_15::compatibility_manifest(build("postgresql", "15.19"));
    for (native, target_native, expected) in [
        ("int", "integer", "integer.32.signed"),
        ("bigint unsigned", "numeric(20,0)", "integer.64.unsigned"),
        (
            "varchar(255)",
            "character varying(255)",
            "text.utf8mb4.varchar.255",
        ),
        (
            "datetime(6)",
            "timestamp(6) without time zone",
            "local_datetime.6",
        ),
        ("timestamp(6)", "timestamp(6) with time zone", "instant.6"),
        ("json", "jsonb", "jsonb"),
    ] {
        let mapping = mysql_5_7::source_type_mapping(
            native,
            native.starts_with("varchar").then_some("utf8mb4"),
            native.starts_with("varchar").then_some("utf8mb4_bin"),
        )
        .unwrap();
        let source = field(
            "mysql-source",
            native,
            mapping.logical_type.clone(),
            Some(0),
        );
        let target = field(
            "pg-target",
            target_native,
            LogicalType::Opaque {
                source_type: target_native.into(),
                format: "catalog-native".into(),
            },
            Some(0),
        );
        let result = plan(source, target, mapping, &manifest, options());
        assert_eq!(result.status, CompatibilityStatus::Compatible, "{native}");
        assert!(
            result
                .plan
                .expect("qualified source has a plan")
                .capability_code
                .contains(expected)
        );
    }
}

#[test]
fn boolean_and_uuid_are_explicit_confirmed_conversions_not_implicit_keys() {
    let manifest = mysql_8_4::compatibility_manifest(build("mysql", "8.4.8"));
    for (native, target_native) in [("boolean", "tinyint(1)"), ("uuid", "char(36)")] {
        let mapping = postgresql_15::source_type_mapping(native).unwrap();
        let source = field(
            "pg-explicit-source",
            native,
            mapping.logical_type.clone(),
            None,
        );
        let target = field(
            "mysql-explicit-target",
            target_native,
            LogicalType::Opaque {
                source_type: target_native.into(),
                format: "catalog-native".into(),
            },
            None,
        );
        let pending = plan(
            source.clone(),
            target.clone(),
            mapping.clone(),
            &manifest,
            options(),
        );
        assert_eq!(pending.status, CompatibilityStatus::NeedsConfirmation);
        let pending_plan = pending.plan.as_ref().expect("pending plan is explainable");
        assert!(pending_plan.verify_digest());

        let mut confirmed_options = options();
        confirmed_options.confirmations.push(RiskConfirmation {
            source_field_lineage: pending_plan.source_field.lineage_id.clone(),
            target_field_lineage: pending_plan.target_field.lineage_id.clone(),
            rule: pending_plan.rule.clone(),
            plan_digest: pending_plan.plan_digest.clone(),
            actor: "fixture-operator".into(),
            confirmed_at: "2026-09-18T00:00:00Z".into(),
            reason: Some("explicit DML conversion is approved for this route".into()),
        });
        let confirmed = plan(source, target, mapping, &manifest, confirmed_options);
        assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
    }
}

#[test]
fn arrays_and_unbounded_or_wrong_encoding_fail_at_planning() {
    assert!(postgresql_15::source_type_mapping("integer[]").is_err());

    let manifest = postgresql_15::compatibility_manifest(build("postgresql", "15.19"));
    let array = LogicalType::Array {
        element: Box::new(LogicalType::integer(true, 32)),
    };
    let mapping = SourceTypeMapping::new(
        ConnectorIdentity::new("postgresql", "15"),
        "integer[]",
        array.clone(),
        "postgresql15.source-type.array",
        postgresql_15::MAPPING_VERSION,
    );
    let result = plan(
        field("array-source", "integer[]", array, None),
        field(
            "array-target",
            "integer[]",
            LogicalType::Opaque {
                source_type: "integer[]".into(),
                format: "catalog-native".into(),
            },
            None,
        ),
        mapping,
        &manifest,
        options(),
    );
    assert_eq!(result.status, CompatibilityStatus::Unsupported);

    let latin1 = mysql_5_7::source_type_mapping("text", Some("latin1"), None).unwrap();
    let latin1_field = field("mysql-latin1", "text", latin1.logical_type.clone(), None);
    let target = field(
        "pg-text",
        "text",
        LogicalType::Opaque {
            source_type: "text".into(),
            format: "catalog-native".into(),
        },
        None,
    );
    let result = plan(latin1_field, target, latin1, &manifest, options());
    assert_eq!(result.status, CompatibilityStatus::Unsupported);
}
