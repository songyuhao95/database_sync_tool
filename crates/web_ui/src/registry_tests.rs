use super::super::{
    catalog::{CatalogColumn, CatalogTable},
    registry::{
        AdapterKind, ConnectorIdentity, SinkRegistry, SourceRegistry, catalog_fingerprint,
        field_compatibility_with_parameters,
        field_compatibility_with_source_evidence_and_target_probe,
    },
};
use change_event::{
    CapabilityProbeEntry, CapabilityProbeStatus, CompatibilityStatus, FailureClass,
    RiskConfirmation, ServerBuildIdentity, TargetCapabilityProbe, TargetColumnMetadata,
    TargetSessionProfile,
};
use std::collections::BTreeMap;

#[test]
fn mysql57_inventory_types_can_plan_a_user_selected_carrier_for_each_sink() {
    inventory_types_can_plan_a_user_selected_carrier_for_each_sink("mysql", "5.7");
}

#[test]
fn postgresql15_inventory_types_can_plan_a_user_selected_carrier_for_each_sink() {
    inventory_types_can_plan_a_user_selected_carrier_for_each_sink("postgresql", "15");
}

fn postgresql_inventory_catalog() -> postgresql_15::SourceTypeCatalog {
    use postgresql_15::SourceTypeDefinition as Type;

    let mut types = [
        ("int2", 21),
        ("int4", 23),
        ("int8", 20),
        ("numeric", 1700),
        ("text", 25),
        ("timestamp", 1114),
        ("timestamptz", 1184),
        ("date", 1082),
        ("money", 790),
        ("point", 600),
        ("line", 628),
        ("lseg", 601),
        ("box", 603),
        ("path", 602),
        ("polygon", 604),
        ("circle", 718),
        ("tsvector", 3614),
        ("tsquery", 3615),
        ("oidvector", 30),
        ("int2vector", 22),
        ("tid", 27),
        ("pg_lsn", 3220),
        ("pg_snapshot", 5038),
        ("txid_snapshot", 2970),
        ("regproc", 24),
        ("regprocedure", 2202),
        ("regoper", 2203),
        ("regoperator", 2204),
        ("regclass", 2205),
        ("regtype", 2206),
        ("regconfig", 3734),
        ("regdictionary", 3769),
        ("regnamespace", 4089),
        ("regrole", 4096),
        ("regcollation", 4191),
    ]
    .into_iter()
    .map(|(name, oid)| Type::builtin(oid, "pg_catalog", name))
    .collect::<Vec<_>>();
    types.extend([
        Type::array(1009, "pg_catalog", "_text", 25),
        Type::range(3904, "pg_catalog", "int4range", 23),
        Type::range(3926, "pg_catalog", "int8range", 20),
        Type::range(3906, "pg_catalog", "numrange", 1700),
        Type::range(3908, "pg_catalog", "tsrange", 1114),
        Type::range(3910, "pg_catalog", "tstzrange", 1184),
        Type::range(3912, "pg_catalog", "daterange", 1082),
        Type::multi_range(4451, "pg_catalog", "int4multirange", 3904),
        Type::multi_range(4536, "pg_catalog", "int8multirange", 3926),
        Type::multi_range(4532, "pg_catalog", "nummultirange", 3906),
        Type::multi_range(4533, "pg_catalog", "tsmultirange", 3908),
        Type::multi_range(4534, "pg_catalog", "tstzmultirange", 3910),
        Type::multi_range(4535, "pg_catalog", "datemultirange", 3912),
    ]);
    postgresql_15::SourceTypeCatalog::new(types)
}

fn inventory_types_can_plan_a_user_selected_carrier_for_each_sink(kind: &str, version: &str) {
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../../../scripts/type-inventory.json")).unwrap();
    let source_connector = SourceRegistry.find(kind, version).unwrap();
    let source_id = format!("{}_{}", kind, version.replace('.', "_"));
    let source_build = ServerBuildIdentity::new(kind, "test", version, "test-build");
    let source_catalog = (kind == "postgresql").then(postgresql_inventory_catalog);
    let results = std::thread::scope(|scope| {
        let handles = SinkRegistry
            .all()
            .map(|sink_connector| {
                let inventory = &inventory;
                let source_build = &source_build;
                let source_id = source_id.clone();
                let source_catalog = &source_catalog;
                scope.spawn(move || {
                    let sink_id = format!(
                        "{}_{}",
                        sink_connector.identity.kind,
                        sink_connector.identity.version.replace('.', "_")
                    );
                    let target_build = ServerBuildIdentity::new(
                        sink_connector.identity.kind,
                        "test",
                        sink_connector.identity.version,
                        "test-build",
                    );
                    let manifest = sink_connector.structured_manifest(target_build.clone());
                    let mut failures = Vec::new();
                    let mut qualified = 0;
                    for entry in inventory["types"].as_array().unwrap() {
                        let profile = &inventory["native_declaration_profiles"]
                            [entry["declaration_profile"].as_str().unwrap()];
                        if !profile["connectors"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|connector| connector == &source_id)
                        {
                            continue;
                        }
                        let type_id = entry["id"].as_str().unwrap();
                        let native_type = profile["examples"][0].as_str().unwrap();
                        let textual = ["char", "text", "enum", "set"]
                            .into_iter()
                            .any(|fragment| native_type.contains(fragment));
                        let source_column = CatalogColumn {
                            name: "payload".into(),
                            column_type: native_type.into(),
                            nullable: true,
                            extra: String::new(),
                            collation: (kind == "mysql" && textual)
                                .then(|| "utf8mb4_unicode_ci".into()),
                            default_value: None,
                        };
                        let source = CatalogTable {
                            schema: "CDC_test".into(),
                            name: "type_route".into(),
                            engine: if kind == "mysql" {
                                "InnoDB"
                            } else {
                                "PostgreSQL"
                            }
                            .into(),
                            primary_key: vec!["id".into()],
                            columns: vec![source_column.clone()],
                        };
                        let source_mapping = source_connector.source_type_mapping_with_evidence(
                            &source_column,
                            source_catalog.as_ref(),
                            Some(source_build.clone()),
                            None,
                        );
                        let Ok(source_mapping) = source_mapping else {
                            failures.push(format!(
                                "{type_id}@{source_id}>{sink_id} ({native_type}): {}",
                                source_mapping.unwrap_err()
                            ));
                            continue;
                        };
                        let required_carrier = if matches!(
                            source_mapping.logical_type,
                            change_event::LogicalType::Raw { .. }
                        ) {
                            "source_representation"
                        } else {
                            "logical_value_json"
                        };
                        let mut planned = false;
                        for carrier in manifest.capabilities.iter().filter(|capability| {
                            capability
                                .target
                                .parameters
                                .get("conversion_kind")
                                .map(String::as_str)
                                == Some(required_carrier)
                        }) {
                            let target_native = carrier.target.native_type.clone();
                            let sink_column = CatalogColumn {
                                name: "payload".into(),
                                column_type: target_native.clone(),
                                nullable: true,
                                extra: String::new(),
                                collation: (sink_connector.identity.kind == "mysql"
                                    && target_native.contains("text"))
                                .then(|| "utf8mb4_unicode_ci".into()),
                                default_value: None,
                            };
                            let sink = CatalogTable {
                                schema: "CDC_test".into(),
                                name: "type_route".into(),
                                engine: if sink_connector.identity.kind == "mysql" {
                                    "InnoDB"
                                } else {
                                    "PostgreSQL"
                                }
                                .into(),
                                primary_key: vec!["id".into()],
                                columns: vec![sink_column.clone()],
                            };
                            let probe = TargetCapabilityProbe::new(
                                target_build.clone(),
                                "CDC_test",
                                "type_route",
                                "payload",
                                TargetColumnMetadata::new(catalog_fingerprint(&sink))
                                    .with_native_type(target_native),
                                [CapabilityProbeEntry::new(
                                    &carrier.code,
                                    CapabilityProbeStatus::Qualified,
                                )],
                                [],
                                TargetSessionProfile::new(
                                    "test-session",
                                    std::iter::empty::<(String, String)>(),
                                ),
                            );
                            let selection = BTreeMap::from([
                                ("__rule_id".into(), carrier.rule.id.clone()),
                                ("__rule_version".into(), carrier.rule.version.clone()),
                            ]);
                            let plan = |confirmations: &[RiskConfirmation]| {
                                field_compatibility_with_source_evidence_and_target_probe(
                                    source_connector,
                                    sink_connector,
                                    &source,
                                    &sink,
                                    &source_column,
                                    &sink_column,
                                    "inventory-route",
                                    "inventory-route:r1",
                                    Some(source_build.clone()),
                                    Some(target_build.clone()),
                                    source_catalog.as_ref(),
                                    None,
                                    &selection,
                                    confirmations,
                                    Some(&probe),
                                )
                            };
                            let Ok(first) = plan(&[]) else { continue };
                            let Some(pending) = first.plan.as_ref() else {
                                continue;
                            };
                            let final_result = if first.status
                                == CompatibilityStatus::NeedsConfirmation
                            {
                                let confirmation = RiskConfirmation {
                                    source_field_lineage: pending.source_field.lineage_id.clone(),
                                    target_field_lineage: pending.target_field.lineage_id.clone(),
                                    rule: pending.rule.clone(),
                                    plan_digest: pending.plan_digest.clone(),
                                    actor: "qualification".into(),
                                    confirmed_at: "2026-09-28T00:00:00Z".into(),
                                    reason: Some("carrier limitations acknowledged".into()),
                                };
                                plan(&[confirmation]).ok()
                            } else {
                                Some(first)
                            };
                            if final_result.is_some_and(|result| {
                                result.status == CompatibilityStatus::Compatible
                                    && result.plan.is_some_and(|plan| plan.verify_digest())
                            }) {
                                planned = true;
                                break;
                            }
                        }
                        if planned {
                            qualified += 1;
                        } else {
                            failures
                                .push(format!("{type_id}@{source_id}>{sink_id} ({native_type})"));
                        }
                    }
                    eprintln!(
                        "{sink_id}: {qualified} qualified, {} missing",
                        failures.len()
                    );
                    (qualified, failures)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("sink qualification thread"))
            .collect::<Vec<_>>()
    });
    let qualified = results
        .iter()
        .map(|(qualified, _)| qualified)
        .sum::<usize>();
    let failures = results
        .into_iter()
        .flat_map(|(_, failures)| failures)
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "{qualified} carrier paths qualified; missing: {}",
        failures.join(", ")
    );
}

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
            ConnectorIdentity::new("postgresql", "16"),
            ConnectorIdentity::new("postgresql", "17"),
        ]
    );
    assert!(sources.find("postgresql", "15").is_some());
    assert!(sources.find("postgresql", "16").is_some());
    assert!(sources.find("postgresql", "17").is_some());
    assert!(sinks.find("postgresql", "15").is_some());
    assert!(sinks.find("postgresql", "16").is_some());
    assert!(sinks.find("postgresql", "17").is_some());
    assert_eq!(
        sinks.find("postgresql", "15").unwrap().adapter,
        AdapterKind::Postgresql15
    );
    assert_eq!(
        sinks.find("postgresql", "16").unwrap().adapter,
        AdapterKind::Postgresql16
    );
    assert_eq!(
        sinks.find("postgresql", "17").unwrap().adapter,
        AdapterKind::Postgresql17
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
    assert_eq!(sources.len() * sinks.len(), 36);
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
fn mysql_enum_collation_mismatch_requires_confirmation_but_keeps_label_mapping_available() {
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

    assert_eq!(result.status, CompatibilityStatus::NeedsConfirmation);
    let plan = result
        .plan
        .as_ref()
        .expect("ENUM label mapping remains available as a risk plan");
    assert_eq!(
        plan.risk_code.as_deref(),
        Some("common.collation_semantics_target")
    );
    assert_eq!(
        plan.target
            .parameters
            .get("value_strategy")
            .map(String::as_str),
        Some("enum_label")
    );

    let confirmation = RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "test".into(),
        confirmed_at: "2026-10-03T00:00:00Z".into(),
        reason: Some("accepted the target ENUM collation semantics".into()),
    };
    let confirmed = field_compatibility_with_parameters(
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
        &[confirmation],
    )
    .expect("explicitly confirmed ENUM mapping should be accepted");
    assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
    assert_eq!(
        confirmed
            .plan
            .as_ref()
            .expect("confirmed ENUM mapping keeps its plan")
            .plan_digest,
        plan.plan_digest
    );
}

#[test]
fn postgres_array_collation_difference_is_an_explicit_confirmable_risk() {
    let source_connector = SourceRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 source is registered");
    let sink_connector = SinkRegistry
        .find("postgresql", "15")
        .expect("PostgreSQL 15 sink is registered");
    let source = CatalogTable {
        schema: "public".into(),
        name: "array_source".into(),
        engine: "PostgreSQL".into(),
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
                name: "value".into(),
                column_type: "text[]".into(),
                nullable: true,
                extra: String::new(),
                collation: Some("C".into()),
                default_value: None,
            },
        ],
    };
    let sink = CatalogTable {
        schema: "public".into(),
        name: "array_sink".into(),
        engine: "PostgreSQL".into(),
        primary_key: vec!["id".into()],
        columns: vec![
            source.columns[0].clone(),
            CatalogColumn {
                name: "value".into(),
                column_type: "text".into(),
                nullable: true,
                extra: String::new(),
                collation: Some("POSIX".into()),
                default_value: None,
            },
        ],
    };
    let source_catalog = postgresql_inventory_catalog();
    let source_build = ServerBuildIdentity::new("postgresql", "community", "15.19", "pg15");
    let target_build = source_build.clone();
    let preview = field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-pg-array-collation",
        "draft-pg-array-collation:r1",
        Some(source_build.clone()),
        Some(target_build.clone()),
        Some(&source_catalog),
        None,
        &BTreeMap::new(),
        &[],
        None,
    )
    .expect("collation difference should remain configurable");
    assert_eq!(preview.status, CompatibilityStatus::NeedsConfiguration);
    let candidate = preview
        .candidates
        .iter()
        .find(|candidate| {
            candidate
                .target
                .parameters
                .get("conversion_kind")
                .is_some_and(|kind| kind == "logical_value_json")
        })
        .expect("the array should have a logical-value carrier candidate");
    let parameters = BTreeMap::from([
        ("__rule_id".into(), candidate.rule.id.clone()),
        ("__rule_version".into(), candidate.rule.version.clone()),
    ]);
    let pending = field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-pg-array-collation",
        "draft-pg-array-collation:r1",
        Some(source_build.clone()),
        Some(target_build.clone()),
        Some(&source_catalog),
        None,
        &parameters,
        &[],
        None,
    )
    .expect("selected target carrier should produce a risk plan");
    assert_eq!(pending.status, CompatibilityStatus::NeedsConfirmation);
    let plan = pending.plan.as_ref().expect("risk preview includes a plan");
    assert_eq!(
        plan.risk_code.as_deref(),
        Some("common.collation_semantics_target")
    );
    assert!(!plan.loss.value);
    assert!(plan.loss.comparison && plan.loss.ordering && plan.loss.constraints);
    assert!(
        plan.loss
            .explanation
            .contains("Source values are preserved")
    );

    let confirmation = RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "test".into(),
        confirmed_at: "2026-10-02T00:00:00Z".into(),
        reason: Some("accepted target collation comparison semantics".into()),
    };
    let confirmed = field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        &source,
        &sink,
        &source.columns[1],
        &sink.columns[1],
        "draft-pg-array-collation",
        "draft-pg-array-collation:r1",
        Some(source_build),
        Some(target_build),
        Some(&source_catalog),
        None,
        &parameters,
        &[confirmation],
        None,
    )
    .expect("field-local confirmation should enable the plan");
    assert_eq!(confirmed.status, CompatibilityStatus::Compatible);

    let mut key_source = source.clone();
    key_source.primary_key = vec!["value".into()];
    let mut key_sink = sink.clone();
    key_sink.primary_key = vec!["value".into()];
    let key_result = field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        &key_source,
        &key_sink,
        &key_source.columns[1],
        &key_sink.columns[1],
        "draft-pg-array-collation-key",
        "draft-pg-array-collation-key:r1",
        Some(ServerBuildIdentity::new(
            "postgresql",
            "community",
            "15.19",
            "pg15",
        )),
        Some(ServerBuildIdentity::new(
            "postgresql",
            "community",
            "15.19",
            "pg15",
        )),
        Some(&source_catalog),
        None,
        &BTreeMap::new(),
        &[],
        None,
    )
    .expect("key collation mismatch is a compatibility result");
    assert_eq!(key_result.status, CompatibilityStatus::Blocked);
    assert_eq!(
        key_result.reason_code,
        "target_capability.key_collation_mismatch"
    );
}

#[test]
fn target_probe_failure_is_reported_as_target_capability_failure() {
    let source_connector = SourceRegistry
        .find("mysql", "5.7")
        .expect("MySQL 5.7 source is registered");
    let sink_connector = SinkRegistry
        .find("mysql", "8.0")
        .expect("MySQL 8.0 sink is registered");
    let table = CatalogTable {
        schema: "CDC_test".into(),
        name: "orders".into(),
        engine: "InnoDB".into(),
        primary_key: vec!["id".into()],
        columns: vec![CatalogColumn {
            name: "id".into(),
            column_type: "bigint".into(),
            nullable: false,
            extra: String::new(),
            collation: None,
            default_value: None,
        }],
    };
    let target_build = ServerBuildIdentity::new("mysql", "oracle", "8.0.36", "mysql-8.0.36");
    let probe = TargetCapabilityProbe::new(
        target_build.clone(),
        "CDC_test",
        "orders",
        "id",
        TargetColumnMetadata::new(catalog_fingerprint(&table)).with_native_type("bigint"),
        [CapabilityProbeEntry::new(
            "mysql8.target.capability",
            CapabilityProbeStatus::Missing,
        )],
        [],
        TargetSessionProfile::new("mysql8-session", std::iter::empty::<(String, String)>()),
    );
    let error = field_compatibility_with_source_evidence_and_target_probe(
        source_connector,
        sink_connector,
        &table,
        &table,
        &table.columns[0],
        &table.columns[0],
        "draft-probe-failure",
        "draft-probe-failure:r1",
        Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        Some(target_build),
        None,
        None,
        &BTreeMap::new(),
        &[],
        Some(&probe),
    )
    .expect_err("an unqualified target probe must fail closed");

    assert_eq!(error.class(), FailureClass::TargetCapability);
    assert_eq!(error.code(), "target_capability.probe_not_qualified");
}

#[test]
fn each_sink_exposes_an_explicit_value_carrier_that_can_be_confirmed() {
    let source_connector = SourceRegistry.find("mysql", "5.7").unwrap();
    let source = CatalogTable {
        schema: "CDC_test".into(),
        name: "carrier_test".into(),
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
                name: "payload".into(),
                column_type: "json".into(),
                nullable: true,
                extra: String::new(),
                collation: None,
                default_value: None,
            },
        ],
    };
    let source_build = ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "mysql-5.7.44");
    for (kind, version, native_type) in [
        ("mysql", "5.7", "json"),
        ("mysql", "8.0", "json"),
        ("mysql", "8.4", "json"),
        ("postgresql", "15", "text"),
        ("postgresql", "16", "text"),
        ("postgresql", "17", "text"),
    ] {
        let sink_connector = SinkRegistry.find(kind, version).unwrap();
        let target_build = ServerBuildIdentity::new(kind, "test", version, "test-build");
        let mut sink = source.clone();
        sink.engine = if kind == "mysql" {
            "InnoDB"
        } else {
            "PostgreSQL"
        }
        .into();
        sink.columns[1].column_type = native_type.into();
        let manifest = sink_connector.structured_manifest(target_build.clone());
        let carrier = manifest
            .capabilities
            .iter()
            .find(|capability| {
                capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("logical_value_json")
            })
            .expect("every sink offers a tagged LogicalValue carrier");
        assert_eq!(carrier.target.native_type, native_type);
        let probe = TargetCapabilityProbe::new(
            target_build.clone(),
            "CDC_test",
            "carrier_test",
            "payload",
            TargetColumnMetadata::new(catalog_fingerprint(&sink)).with_native_type(native_type),
            [CapabilityProbeEntry::new(
                &carrier.code,
                CapabilityProbeStatus::Qualified,
            )],
            [],
            TargetSessionProfile::new("test-session", std::iter::empty::<(String, String)>()),
        );
        let plan = |parameters: &BTreeMap<String, String>, confirmations: &[RiskConfirmation]| {
            field_compatibility_with_source_evidence_and_target_probe(
                source_connector,
                sink_connector,
                &source,
                &sink,
                &source.columns[1],
                &sink.columns[1],
                "draft-carrier-test",
                "draft-carrier-test:r1",
                Some(source_build.clone()),
                Some(target_build.clone()),
                None,
                None,
                parameters,
                confirmations,
                Some(&probe),
            )
            .expect("qualified target probe must allow planning")
        };
        let discovery = plan(&BTreeMap::new(), &[]);
        assert_eq!(
            discovery.status,
            CompatibilityStatus::NeedsConfiguration,
            "{kind} {version}: {} ({})",
            discovery.reason_code,
            discovery.explanation
        );
        assert!(
            discovery.plan.is_none(),
            "carrier must not be selected implicitly"
        );
        assert!(
            discovery
                .candidates
                .iter()
                .any(|candidate| candidate.rule.id == carrier.rule.id)
        );
        let selection = BTreeMap::from([
            ("__rule_id".into(), carrier.rule.id.clone()),
            ("__rule_version".into(), carrier.rule.version.clone()),
        ]);
        let unconfirmed = plan(&selection, &[]);
        assert_eq!(unconfirmed.status, CompatibilityStatus::NeedsConfirmation);
        let pending = unconfirmed
            .plan
            .expect("risk preview includes the carrier plan");
        assert_eq!(
            pending
                .target
                .parameters
                .get("conversion_kind")
                .map(String::as_str),
            Some("logical_value_json")
        );
        assert_eq!(
            pending.target_probe_digest.as_deref(),
            Some(probe.digest.as_str())
        );
        let confirmation = RiskConfirmation {
            source_field_lineage: pending.source_field.lineage_id.clone(),
            target_field_lineage: pending.target_field.lineage_id.clone(),
            rule: pending.rule.clone(),
            plan_digest: pending.plan_digest.clone(),
            actor: "test".into(),
            confirmed_at: "2026-09-28T00:00:00Z".into(),
            reason: Some("carrier limitations acknowledged".into()),
        };
        let confirmed = plan(&selection, &[confirmation]);
        assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
        assert_eq!(confirmed.plan.unwrap().plan_digest, pending.plan_digest);
    }
}

#[test]
fn each_sink_exposes_an_explicit_source_representation_carrier() {
    let source_connector = SourceRegistry.find("postgresql", "15").unwrap();
    let source = CatalogTable {
        schema: "application".into(),
        name: "carrier_test".into(),
        engine: "PostgreSQL".into(),
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
                name: "payload".into(),
                column_type: "application.invoice_code".into(),
                nullable: true,
                extra: String::new(),
                collation: None,
                default_value: None,
            },
        ],
    };
    let catalog =
        postgresql_15::SourceTypeCatalog::new([postgresql_15::SourceTypeDefinition::builtin(
            90_001,
            "application",
            "invoice_code",
        )]);
    let source_build = ServerBuildIdentity::new("postgresql", "postgresql", "15.0", "test-build");
    for (kind, version, native_type) in [
        ("mysql", "5.7", "longblob"),
        ("mysql", "8.0", "longblob"),
        ("mysql", "8.4", "longblob"),
        ("postgresql", "15", "bytea"),
        ("postgresql", "16", "bytea"),
        ("postgresql", "17", "bytea"),
    ] {
        let sink_connector = SinkRegistry.find(kind, version).unwrap();
        let target_build = ServerBuildIdentity::new(kind, "test", version, "test-build");
        let mut sink = source.clone();
        sink.engine = if kind == "mysql" {
            "InnoDB"
        } else {
            "PostgreSQL"
        }
        .into();
        sink.columns[1].column_type = native_type.into();
        let manifest = sink_connector.structured_manifest(target_build.clone());
        let carrier = manifest
            .capabilities
            .iter()
            .find(|capability| {
                capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("source_representation")
            })
            .expect("every sink offers a source representation carrier");
        assert_eq!(carrier.target.native_type, native_type);
        let probe = TargetCapabilityProbe::new(
            target_build.clone(),
            "application",
            "carrier_test",
            "payload",
            TargetColumnMetadata::new(catalog_fingerprint(&sink)).with_native_type(native_type),
            [CapabilityProbeEntry::new(
                &carrier.code,
                CapabilityProbeStatus::Qualified,
            )],
            [],
            TargetSessionProfile::new("test-session", std::iter::empty::<(String, String)>()),
        );
        let plan = |parameters: &BTreeMap<String, String>, confirmations: &[RiskConfirmation]| {
            field_compatibility_with_source_evidence_and_target_probe(
                source_connector,
                sink_connector,
                &source,
                &sink,
                &source.columns[1],
                &sink.columns[1],
                "draft-raw-carrier-test",
                "draft-raw-carrier-test:r1",
                Some(source_build.clone()),
                Some(target_build.clone()),
                Some(&catalog),
                None,
                parameters,
                confirmations,
                Some(&probe),
            )
            .expect("qualified target probe must allow Raw planning")
        };
        let discovery = plan(&BTreeMap::new(), &[]);
        assert_eq!(
            discovery.status,
            CompatibilityStatus::NeedsConfiguration,
            "{kind} {version}: {} ({})",
            discovery.reason_code,
            discovery.explanation
        );
        assert!(discovery.plan.is_none());
        assert!(
            discovery
                .candidates
                .iter()
                .any(|candidate| candidate.rule.id == carrier.rule.id)
        );
        let selection = BTreeMap::from([
            ("__rule_id".into(), carrier.rule.id.clone()),
            ("__rule_version".into(), carrier.rule.version.clone()),
        ]);
        let unconfirmed = plan(&selection, &[]);
        assert_eq!(unconfirmed.status, CompatibilityStatus::NeedsConfirmation);
        let pending = unconfirmed
            .plan
            .expect("risk preview includes the Raw carrier plan");
        assert_eq!(
            pending
                .target
                .parameters
                .get("conversion_kind")
                .map(String::as_str),
            Some("source_representation")
        );
        let confirmation = RiskConfirmation {
            source_field_lineage: pending.source_field.lineage_id.clone(),
            target_field_lineage: pending.target_field.lineage_id.clone(),
            rule: pending.rule.clone(),
            plan_digest: pending.plan_digest.clone(),
            actor: "test".into(),
            confirmed_at: "2026-09-28T00:00:00Z".into(),
            reason: Some("source representation limitations acknowledged".into()),
        };
        let confirmed = plan(&selection, &[confirmation]);
        assert_eq!(confirmed.status, CompatibilityStatus::Compatible);
        assert_eq!(confirmed.plan.unwrap().plan_digest, pending.plan_digest);
    }
}
