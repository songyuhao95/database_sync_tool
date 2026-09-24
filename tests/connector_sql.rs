#[path = "support/mysql_contract.rs"]
mod contract;
use change_event::SinkAdapter as _;
use change_event::validate;
use contract::*;
use mysql::prelude::Queryable;

fn binary_plan(
    transaction: &change_event::ValidatedTransaction,
    target_manifest: &change_event::TargetCapabilityManifest,
    target_mapping: &change_event::SourceTypeMapping,
    ordinal: usize,
) -> change_event::ColumnConversionPlan {
    let source_mapping = ::mysql_5_7::source_type_mapping("varbinary(32)", None, None).unwrap();
    let source_field = change_event::FieldDefinition {
        reference: change_event::DefinitionReference::new(
            "catalog:CDC_test.cdc_contract.bytes",
            "source-bytes",
        ),
        ordinal,
        name: "bytes".into(),
        native_type: source_mapping.native_type.clone(),
        logical_type: source_mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    };
    let target_field = change_event::FieldDefinition {
        reference: change_event::DefinitionReference::new(
            "catalog:CDC_test.cdc_contract.bytes-target",
            "target-bytes",
        ),
        ordinal,
        name: "bytes".into(),
        native_type: target_mapping.native_type.clone(),
        logical_type: target_mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    };
    let result = change_event::plan_compatibility(change_event::CompatibilityInput {
        transaction,
        source_field,
        target_field,
        source_type_mapping: source_mapping.clone(),
        source_connector: source_mapping.connector.clone(),
        sink_connector: target_manifest.connector.clone(),
        source_build: Some(change_event::ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(target_manifest.target_build.clone()),
        manifest: target_manifest,
        options: change_event::RouteOptions {
            route_id: "mysql-sink-plan-test".into(),
            configuration_revision: "test-revision".into(),
            ..change_event::RouteOptions::default()
        },
    })
    .unwrap();
    assert_eq!(result.status, change_event::CompatibilityStatus::Compatible);
    result
        .plan
        .expect("binary field must have a conversion plan")
}

fn integer_plan(
    transaction: &change_event::ValidatedTransaction,
    target_manifest: &change_event::TargetCapabilityManifest,
    target_mapping: &change_event::SourceTypeMapping,
) -> change_event::ColumnConversionPlan {
    let source_mapping = ::mysql_5_7::source_type_mapping("bigint unsigned", None, None).unwrap();
    let source_field = change_event::FieldDefinition {
        reference: change_event::DefinitionReference::new(
            "catalog:CDC_test.cdc_contract.id",
            "source-id",
        ),
        ordinal: 0,
        name: "id".into(),
        native_type: source_mapping.native_type.clone(),
        logical_type: source_mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: Some(0),
        unique: false,
        row_locator: false,
    };
    let target_field = change_event::FieldDefinition {
        reference: change_event::DefinitionReference::new(
            "catalog:CDC_test.cdc_contract.id-target",
            "target-id",
        ),
        ordinal: 0,
        name: "id".into(),
        native_type: target_mapping.native_type.clone(),
        logical_type: target_mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: Some(0),
        unique: false,
        row_locator: false,
    };
    let result = change_event::plan_compatibility(change_event::CompatibilityInput {
        transaction,
        source_field,
        target_field,
        source_type_mapping: source_mapping.clone(),
        source_connector: source_mapping.connector.clone(),
        sink_connector: target_manifest.connector.clone(),
        source_build: Some(change_event::ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(target_manifest.target_build.clone()),
        manifest: target_manifest,
        options: change_event::RouteOptions {
            route_id: "mysql-sink-id-plan-test".into(),
            configuration_revision: "test-revision".into(),
            ..change_event::RouteOptions::default()
        },
    })
    .unwrap();
    assert_eq!(result.status, change_event::CompatibilityStatus::Compatible);
    result
        .plan
        .expect("integer field must have a conversion plan")
}

macro_rules! target_tests {
    ($adapter:ident, $version:literal, $port_key:literal, $port:literal, $expected:literal) => {
        mod $adapter {
            use super::*;
            #[test]
            fn local_sql() {
                // One input fixture, independently reviewed expected SQL for each version.
                for source in ["5.7.44", "8.0.46", "8.4.8"] {
                    let tx = roundtrip(&validate(fixture(source, "cdc_contract")).unwrap());
                    assert_eq!(::$adapter::SinkAdapter::new().plan(&tx).unwrap().script(), include_str!($expected).replace("\r\n", "\n"));
                }
                let mut keyless = fixture("5.7.44", "cdc_contract");
                for change in &mut keyless.changes {
                    for image in [&mut change.before, &mut change.after].into_iter().flatten() {
                        for col in image { col.primary_key_ordinal = None; }
                    }
                }
                assert!(::$adapter::SinkAdapter::new().plan(&validate(keyless).unwrap()).is_err());
            }
            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_sql() {
                let port = port($port_key, $port);
                let mut table = TestTable::create(port);
                let config = ::$adapter::TargetConfig {
                    host: setting("CDC_MYSQL_HOST", "192.168.0.10"), port,
                    user: setting("CDC_MYSQL_WRITER_USER", "mysql_writer"), password: password("WRITER"),
                };
                let source_fixtures = [
                    fixture("5.7.44", &table.name),
                    fixture("8.0.46", &table.name),
                    fixture("8.4.8", &table.name),
                    postgres_fixture_for_version(&table.name, "15"),
                    postgres_fixture_for_version(&table.name, "16"),
                    postgres_fixture_for_version(&table.name, "17"),
                ];
                for full in source_fixtures {
                    // Inspect the committed state after each operation, not only final emptiness.
                    for index in 0..3 {
                        let mut tx = full.clone(); tx.changes = vec![full.changes[index].clone()];
                        let plan = ::$adapter::SinkAdapter::new().plan(&roundtrip(&validate(tx).unwrap())).unwrap();
                        assert_eq!(::$adapter::execute(&config, &plan).unwrap().statements_executed, 1);
                        if index < 2 {
                            let row: (String, String, String, String, bool, String, u32) = table.conn.query_first(format!(
                                "SELECT CAST(id AS CHAR),HEX(message),CAST(amount AS CHAR),HEX(bytes),note IS NULL,CAST(changed_at AS CHAR),message_len FROM CDC_test.{}",table.name
                            )).unwrap().unwrap();
                            let expected = if index == 0 {
                                ("18446744073709551615","E4B8ADE69687275C0A","12345678901234567890.123456",5)
                            } else { ("42","","-0.000001",0) };
                            assert_eq!(row,(expected.0.into(),expected.1.into(),expected.2.into(),"00FF".into(),true,"2026-09-14 01:02:03.123456".into(),expected.3));
                        } else {
                            assert_eq!(table.conn.query_first::<u64,_>(format!("SELECT COUNT(*) FROM CDC_test.{}",table.name)).unwrap(),Some(0));
                        }
                    }
                    let mut duplicate = full.clone();
                    duplicate.changes = vec![full.changes[0].clone(), full.changes[0].clone()];
                    duplicate.changes[1].source_cursor = full.changes[1].source_cursor.clone();
                    let duplicate = ::$adapter::SinkAdapter::new().plan(&validate(duplicate).unwrap()).unwrap();
                    assert!(::$adapter::execute(&config, &duplicate).is_err());
                    assert_eq!(table.conn.query_first::<u64,_>(format!("SELECT COUNT(*) FROM CDC_test.{}",table.name)).unwrap(),Some(0),"duplicate failure must roll back the entire transaction");
                }
                table.cleanup();
            }
        }
    };
}
target_tests!(
    mysql_5_7,
    "5.7",
    "CDC_MYSQL57_PORT",
    33061,
    "fixtures/mysql_5_7.sql"
);
target_tests!(
    mysql_8_0,
    "8.0",
    "CDC_MYSQL80_PORT",
    33062,
    "fixtures/mysql_8_0.sql"
);
target_tests!(
    mysql_8_4,
    "8.4",
    "CDC_MYSQL84_PORT",
    33063,
    "fixtures/mysql_8_4.sql"
);

#[test]
fn mysql_sinks_consume_mysql_and_postgresql_change_events_with_parameterized_sql() {
    let source_fixtures = [
        change_event::validate(fixture("5.7.44", "cdc_contract")).unwrap(),
        change_event::validate(fixture("8.0.46", "cdc_contract")).unwrap(),
        change_event::validate(fixture("8.4.8", "cdc_contract")).unwrap(),
        change_event::validate(postgres_fixture_for_version("cdc_contract", "15")).unwrap(),
        change_event::validate(postgres_fixture_for_version("cdc_contract", "16")).unwrap(),
        change_event::validate(postgres_fixture_for_version("cdc_contract", "17")).unwrap(),
    ];

    macro_rules! assert_sink {
        ($adapter:ident) => {{
            let sink = ::$adapter::SinkAdapter::new();
            for transaction in &source_fixtures {
                let plan = sink.plan(transaction).unwrap();
                for statement in plan.statements() {
                    assert!(
                        statement.contains('?'),
                        "values must be bound parameters: {statement}"
                    );
                    assert!(!statement.contains("18446744073709551615"));
                    assert!(!statement.contains("E4B8ADE69687275C0A"));
                }
            }
        }};
    }

    assert_sink!(mysql_5_7);
    assert_sink!(mysql_8_0);
    assert_sink!(mysql_8_4);
}

#[test]
fn mysql_sinks_reject_unsupported_logical_types_before_building_a_plan() {
    let mut unsupported = fixture("5.7.44", "cdc_contract");
    for change in &mut unsupported.changes {
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            let column = image
                .iter_mut()
                .find(|column| column.name == "message")
                .unwrap();
            column.datum =
                change_event::Datum::Value(change_event::LogicalValue::Boolean { value: true });
        }
    }
    let unsupported = change_event::validate(unsupported).unwrap();

    macro_rules! assert_rejected {
        ($adapter:ident, $target:literal) => {{
            let sink = ::$adapter::SinkAdapter::new();
            assert_eq!(sink.capability_manifest().target, $target);
            let error = sink.plan(&unsupported).unwrap_err();
            assert!(error.to_string().contains("Target Capability Failure"));
            assert!(
                error
                    .get_ref()
                    .and_then(|cause| cause.downcast_ref::<change_event::TargetCapabilityFailure>())
                    .is_some()
            );
        }};
    }
    assert_rejected!(mysql_5_7, "mysql-5.7");
    assert_rejected!(mysql_8_0, "mysql-8.0");
    assert_rejected!(mysql_8_4, "mysql-8.4");
}

#[test]
fn mysql_sinks_honor_unchanged_columns_without_binding_them() {
    let mut transaction = fixture("5.7.44", "cdc_contract");
    let after = transaction.changes[1].after.as_mut().unwrap();
    after
        .iter_mut()
        .find(|column| column.name == "message")
        .unwrap()
        .datum = change_event::Datum::Unchanged;
    let transaction = change_event::validate(transaction).unwrap();

    macro_rules! assert_unchanged {
        ($adapter:ident) => {{
            let plan = ::$adapter::SinkAdapter::new().plan(&transaction).unwrap();
            let update = plan.statements().nth(1).unwrap();
            assert!(!update.contains("`message` ="));
        }};
    }
    assert_unchanged!(mysql_5_7);
    assert_unchanged!(mysql_8_0);
    assert_unchanged!(mysql_8_4);
}

#[test]
fn mysql_versioned_sinks_consume_column_conversion_plans() {
    let mut raw = fixture("5.7.44", "cdc_contract");
    for change in &mut raw.changes {
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            image.retain(|column| matches!(column.name.as_str(), "id" | "bytes"));
            for (ordinal, column) in image.iter_mut().enumerate() {
                column.ordinal = ordinal;
                column.primary_key_ordinal = (column.name == "id").then_some(0);
            }
        }
    }
    let transaction = validate(raw).unwrap();

    macro_rules! assert_planned_sink {
        ($adapter:ident, $version:literal) => {{
            let manifest =
                ::$adapter::compatibility_manifest(change_event::ServerBuildIdentity::new(
                    "mysql",
                    "oracle",
                    $version,
                    concat!("mysql-", $version),
                ));
            assert!(manifest.capabilities.iter().any(|capability| {
                capability
                    .target
                    .parameters
                    .get("target_storage")
                    .map(String::as_str)
                    == Some("mysql_geometry")
            }));
            assert!(manifest.capabilities.iter().any(|capability| {
                capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("recursive")
            }));
            let mapping = ::$adapter::source_type_mapping("varbinary(32)", None, None).unwrap();
            let integer_mapping =
                ::$adapter::source_type_mapping("bigint unsigned", None, None).unwrap();
            let plans = [
                integer_plan(&transaction, &manifest, &integer_mapping),
                binary_plan(&transaction, &manifest, &mapping, 1),
            ];
            let sql = ::$adapter::sql_with_plans(&transaction, &plans).unwrap();
            assert_eq!(sql.statements().count(), 3);
            assert!(sql.statements().all(|statement| statement.contains('?')));
            assert!(sql.parameters().all(|parameters| !parameters.is_empty()));
        }};
    }

    assert_planned_sink!(mysql_5_7, "5.7");
    assert_planned_sink!(mysql_8_0, "8.0");
    assert_planned_sink!(mysql_8_4, "8.4");
}

#[test]
fn mysql_versioned_sinks_reject_tampered_conversion_plans() {
    let transaction = validate(fixture("5.7.44", "cdc_contract")).unwrap();
    let manifest = ::mysql_8_0::compatibility_manifest(change_event::ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let mapping = ::mysql_8_0::source_type_mapping("varbinary(32)", None, None).unwrap();
    let mut plan = binary_plan(&transaction, &manifest, &mapping, 4);
    plan.plan_digest.push('x');
    let error = ::mysql_8_0::sql_with_plans(&transaction, &[plan]).unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<change_event::TargetCapabilityFailure>())
            .map(|failure| failure.code.as_str()),
        Some("target_capability.plan_digest_invalid")
    );
}

#[test]
fn mysql_versioned_sinks_require_plans_for_plan_backed_apply() {
    let transaction = validate(fixture("5.7.44", "cdc_contract")).unwrap();
    let error = ::mysql_5_7::sql_with_plans(&transaction, &[]).unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<change_event::TargetCapabilityFailure>())
            .map(|failure| failure.code.as_str()),
        Some("target_capability.plans_missing")
    );

    let manifest = ::mysql_5_7::compatibility_manifest(change_event::ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "5.7.44",
        "mysql-5.7.44",
    ));
    let mapping = ::mysql_5_7::source_type_mapping("varbinary(32)", None, None).unwrap();
    let error = ::mysql_5_7::sql_with_plans(
        &transaction,
        &[binary_plan(&transaction, &manifest, &mapping, 4)],
    )
    .unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<change_event::TargetCapabilityFailure>())
            .map(|failure| failure.code.as_str()),
        Some("target_capability.plan_missing_for_column")
    );
}
