use super::super::{
    catalog::{CatalogColumn, CatalogTable},
    registry::{
        AdapterKind, ConnectorIdentity, SinkRegistry, SourceRegistry,
        field_compatibility_with_parameters,
    },
};
use change_event::{CompatibilityStatus, RiskConfirmation, ServerBuildIdentity};
use std::collections::BTreeMap;

#[test]
fn source_and_sink_registries_publish_independent_connector_catalogs() {
    let sources = SourceRegistry;
    let sinks = SinkRegistry;

    let source_ids = sources
        .all()
        .map(|connector| connector.identity)
        .collect::<Vec<_>>();
    let sink_ids = sinks
        .all()
        .map(|connector| connector.identity)
        .collect::<Vec<_>>();

    assert_eq!(
        source_ids,
        vec![
            ConnectorIdentity::new("mysql", "5.7"),
            ConnectorIdentity::new("mysql", "8.0"),
            ConnectorIdentity::new("mysql", "8.4"),
            ConnectorIdentity::new("postgresql", "15"),
            ConnectorIdentity::new("postgresql", "16"),
            ConnectorIdentity::new("postgresql", "17"),
        ]
    );
    assert_eq!(
        sink_ids,
        vec![
            ConnectorIdentity::new("mysql", "5.7"),
            ConnectorIdentity::new("mysql", "8.0"),
            ConnectorIdentity::new("mysql", "8.4"),
            ConnectorIdentity::new("postgresql", "15"),
        ]
    );
    assert!(sources.find("postgresql", "15").is_some());
    assert!(sources.find("postgresql", "16").is_some());
    assert!(sources.find("postgresql", "17").is_some());
    assert!(sinks.find("postgresql", "15").is_some());
    assert_eq!(
        sinks.find("postgresql", "15").unwrap().adapter,
        AdapterKind::Postgresql15
    );
}

#[test]
fn registry_lookup_rejects_unsupported_connector_without_nearest_version_fallback() {
    assert!(SourceRegistry.find("mysql", "8.1").is_none());
    assert!(SinkRegistry.find("postgresql", "14").is_none());
    assert!(SourceRegistry.find("postgresql", "8.4").is_none());
}

#[test]
fn source_and_sink_capabilities_are_role_specific() {
    let source = SourceRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 Source is registered");
    let sink = SinkRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 Sink is registered");

    assert_eq!(source.role, "source");
    assert_eq!(source.adapter, AdapterKind::Postgresql15);
    assert!(source.capabilities.supports_transactions);
    assert!(source.capabilities.supports_primary_key_rows);
    assert_eq!(sink.role, "sink");
    assert!(sink.capabilities.supports_transactions);
    assert!(sink.capabilities.supports_primary_key_rows);
    assert_ne!(source.capabilities, sink.capabilities);
}

#[test]
fn source_and_sink_registries_have_no_pair_specific_registration() {
    let sources = SourceRegistry.all().collect::<Vec<_>>();
    let sinks = SinkRegistry.all().collect::<Vec<_>>();
    assert_eq!(sources.len() * sinks.len(), 24);
    for source in sources {
        assert_eq!(source.role, "source");
        assert!(
            SourceRegistry
                .find(source.identity.kind, source.identity.version)
                .is_some()
        );
        for sink in &sinks {
            assert_eq!(sink.role, "sink");
            assert!(
                SinkRegistry
                    .find(sink.identity.kind, sink.identity.version)
                    .is_some()
            );
        }
    }
}

#[test]
fn mysql_char_collation_mismatch_exposes_a_configurable_text_rule() {
    let source_connector = SourceRegistry
        .find("mysql", "5.7")
        .expect("MySQL 5.7 source is registered");
    let sink_connector = SinkRegistry
        .find("mysql", "8.0")
        .expect("MySQL 8.0 sink is registered");
    let source = CatalogTable {
        schema: "CDC_test".into(),
        name: "cdc_types_numeric_string".into(),
        engine: "InnoDB".into(),
        primary_key: vec!["id".into()],
        columns: vec![
            CatalogColumn {
                name: "id".into(),
                column_type: "bigint".into(),
                nullable: false,
                extra: String::new(),
                collation: None,
                default_value: None,
            },
            CatalogColumn {
                name: "char_value".into(),
                column_type: "char(255)".into(),
                nullable: true,
                extra: String::new(),
                collation: Some("utf8mb4_general_ci".into()),
                default_value: None,
            },
        ],
    };
    let mut sink = source.clone();
    sink.engine = "InnoDB".into();
    sink.columns[1].collation = Some("utf8mb4_0900_ai_ci".into());

    let result = field_compatibility_with_parameters(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-char-collation",
        "draft-char-collation:r1",
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "8.0.36",
            "mysql-8.0.36",
        )),
        &BTreeMap::new(),
        &[],
    )
    .expect("catalog mismatch should produce a configurable result");

    assert_eq!(result.status, CompatibilityStatus::NeedsConfiguration);
    let candidate = result
        .candidates
        .iter()
        .find(|candidate| {
            candidate
                .target
                .native_type
                .eq_ignore_ascii_case("char(255)")
        })
        .expect("CHAR target must expose an explicit text rule");

    let parameters = BTreeMap::from([
        ("__rule_id".into(), candidate.rule.id.clone()),
        ("__rule_version".into(), candidate.rule.version.clone()),
        ("target_charset".into(), "utf8mb4".into()),
        ("target_length".into(), "255".into()),
        ("target_length_unit".into(), "characters".into()),
        ("target_collation".into(), "utf8mb4_0900_ai_ci".into()),
        ("encoding_policy".into(), "strict".into()),
        ("length_policy".into(), "reject".into()),
        ("collation_policy".into(), "target".into()),
    ]);
    let configured = field_compatibility_with_parameters(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-char-collation",
        "draft-char-collation:r1",
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "8.0.36",
            "mysql-8.0.36",
        )),
        &parameters,
        &[],
    )
    .expect("selected CHAR rule should be configurable");
    assert_eq!(configured.status, CompatibilityStatus::NeedsConfirmation);
    let plan = configured
        .plan
        .as_ref()
        .expect("risk preview includes a plan");
    let confirmation = RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "test".into(),
        confirmed_at: "2026-09-20T00:00:00Z".into(),
        reason: Some("test conversion confirmation".into()),
    };
    let confirmed = field_compatibility_with_parameters(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-char-collation",
        "draft-char-collation:r1",
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "8.0.36",
            "mysql-8.0.36",
        )),
        &parameters,
        &[confirmation],
    )
    .expect("confirmed CHAR conversion should be accepted");
    assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
    assert!(result.candidates.iter().any(|candidate| {
        candidate
            .target
            .native_type
            .eq_ignore_ascii_case("char(255)")
    }));
}

#[test]
fn mysql_enum_collation_mismatch_keeps_label_mapping_available() {
    let source_connector = SourceRegistry
        .find("mysql", "5.7")
        .expect("MySQL 5.7 source is registered");
    let sink_connector = SinkRegistry
        .find("mysql", "8.0")
        .expect("MySQL 8.0 sink is registered");
    let source = CatalogTable {
        schema: "CDC_test".into(),
        name: "cdc_types_numeric_string".into(),
        engine: "InnoDB".into(),
        primary_key: vec!["id".into()],
        columns: vec![
            CatalogColumn {
                name: "id".into(),
                column_type: "bigint".into(),
                nullable: false,
                extra: String::new(),
                collation: None,
                default_value: None,
            },
            CatalogColumn {
                name: "enum_value".into(),
                column_type: "enum('alpha','beta','gamma')".into(),
                nullable: true,
                extra: String::new(),
                collation: Some("utf8mb4_general_ci".into()),
                default_value: None,
            },
        ],
    };
    let mut sink = source.clone();
    sink.columns[1].collation = Some("utf8mb4_0900_ai_ci".into());

    let result = field_compatibility_with_parameters(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-enum-collation",
        "draft-enum-collation:r1",
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "8.0.36",
            "mysql-8.0.36",
        )),
        &BTreeMap::new(),
        &[],
    )
    .expect("ENUM label mapping should ignore target collation spelling");

    assert_eq!(result.status, CompatibilityStatus::Compatible);
    assert_eq!(
        result
            .plan
            .as_ref()
            .expect("compatible ENUM result includes a plan")
            .target
            .parameters
            .get("value_strategy")
            .map(String::as_str),
        Some("enum_label")
    );
}
