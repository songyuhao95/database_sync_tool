//! Live protocol + decoder tests. Nonblocking bounded replay, no snapshot/global locks.
#[path = "support/mysql_contract.rs"]
mod contract;
#[path = "support/postgres_env.rs"]
mod postgres_env;
#[path = "support/type_qualification_evidence.rs"]
mod type_qualification_evidence;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ConnectorIdentity, Datum, DefinitionReference, FieldCompatibilityInput, FieldDefinition,
    LogicalValue, Operation, PresenceState, RiskConfirmation, RouteOptions, ServerBuildIdentity,
    SourceTypeMapping, validate,
};
use contract::*;
use mysql::prelude::Queryable;
use sqlx::{Connection, Row};

fn mysql_type_token(native_type: &str) -> String {
    let normalized = native_type.trim().to_ascii_lowercase();
    let token = normalized
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default();
    match token {
        "integer" => "int".into(),
        "dec" | "fixed" | "numeric" => "decimal".into(),
        other => other.into(),
    }
}

fn assert_mysql_inventory_types_are_live_captured(
    connector_id: &str,
    image: &[change_event::ColumnDatum],
) -> Vec<String> {
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../scripts/type-inventory.json"))
            .expect("native type inventory is valid JSON");
    let mut captured = std::collections::BTreeSet::new();
    for column in image {
        captured.insert(mysql_type_token(&column.native_type));
        if let Datum::Value(LogicalValue::Spatial { geometry_type, .. }) = &column.datum {
            captured.insert(mysql_type_token(geometry_type));
        }
    }

    let mut missing = Vec::new();
    let mut matched = Vec::new();
    for type_entry in inventory["types"].as_array().expect("type list") {
        let profile_id = type_entry["declaration_profile"]
            .as_str()
            .expect("native declaration profile");
        let profile = &inventory["native_declaration_profiles"][profile_id];
        let connectors = profile["connectors"].as_array().expect("connectors");
        if !connectors.iter().any(|connector| connector == connector_id) {
            continue;
        }
        let examples = profile["examples"].as_array().expect("native examples");
        let represented = examples.iter().any(|example| {
            example
                .as_str()
                .is_some_and(|declaration| captured.contains(&mysql_type_token(declaration)))
        });
        if !represented {
            missing.push(type_entry["id"].as_str().unwrap_or(profile_id).to_owned());
        } else {
            matched.push(type_entry["id"].as_str().unwrap().to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "{connector_id} BinlogStream fixture did not capture inventory native types: {}",
        missing.join(", ")
    );
    matched
}

fn assert_mysql_snapshot_equals_event(
    expected: &[change_event::ColumnDatum],
    actual: &[change_event::ColumnDatum],
    route: &str,
) {
    assert_eq!(actual.len(), expected.len(), "{route} column count");
    for (expected, actual) in expected.iter().zip(actual) {
        assert_eq!(actual.name, expected.name, "{route} column order");
        assert_eq!(
            actual.datum, expected.datum,
            "{route} value for {}",
            expected.name
        );
    }
}

#[derive(Clone)]
struct MysqlCatalogColumn {
    name: String,
    native_type: String,
    charset: Option<String>,
    collation: Option<String>,
    nullable: bool,
    generated: bool,
    ordinal: usize,
}

type MysqlCatalogRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    u64,
);

fn mysql_catalog_columns(port: u16, table: &str) -> Vec<MysqlCatalogColumn> {
    let mut conn = connection(port, false);
    let rows: Vec<MysqlCatalogRow> = conn
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, IS_NULLABLE, EXTRA, ORDINAL_POSITION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='CDC_test' AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
            (table,),
        )
        .expect("read all-types catalog metadata");
    rows.into_iter()
        .map(
            |(name, native_type, charset, collation, nullable, extra, ordinal)| {
                MysqlCatalogColumn {
                    name,
                    native_type,
                    charset,
                    collation,
                    nullable: nullable.eq_ignore_ascii_case("YES"),
                    generated: extra.to_ascii_lowercase().contains("generated"),
                    ordinal: usize::try_from(ordinal - 1).expect("column ordinal fits usize"),
                }
            },
        )
        .collect()
}

fn mysql_build(port: u16) -> ServerBuildIdentity {
    let mut conn = connection(port, false);
    let (version, distribution): (String, String) = conn
        .query_first("SELECT VERSION(), @@version_comment")
        .expect("read MySQL build")
        .expect("MySQL returned its build identity");
    ServerBuildIdentity::new(
        "mysql",
        if distribution.trim().is_empty() {
            "oracle"
        } else {
            distribution.trim()
        },
        version.clone(),
        format!("mysql-{version}"),
    )
}

fn mysql_source_mapping(
    connector: &str,
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> SourceTypeMapping {
    let result = match connector {
        "mysql_5_7" | "5.7" => ::mysql_5_7::source_type_mapping(native_type, charset, collation)
            .map_err(|error| error.to_string()),
        "mysql_8_0" | "8.0" => ::mysql_8_0::source_type_mapping(native_type, charset, collation),
        "mysql_8_4" | "8.4" => ::mysql_8_4::source_type_mapping(native_type, charset, collation),
        other => panic!("unknown MySQL source connector {other}"),
    };
    result.unwrap_or_else(|error| panic!("map native type {native_type}: {error}"))
}

fn qualify_mysql_visible_catalog_types(connector: &str, port: u16) {
    let mut conn = connection(port, true);
    let rows: Vec<(String, Option<String>, Option<String>)> = conn
        .query(
            "SELECT DISTINCT COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME
               FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
              ORDER BY COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME",
        )
        .expect("enumerate reader-visible MySQL user column declarations");
    assert!(
        !rows.is_empty(),
        "{connector} exposed no user column declarations for live type qualification"
    );

    let mut mapped_profiles = std::collections::BTreeSet::new();
    for (native_type, charset, collation) in rows {
        let mapping = mysql_source_mapping(
            connector,
            &native_type,
            charset.as_deref(),
            collation.as_deref(),
        );
        mapped_profiles.insert((
            native_type,
            charset.unwrap_or_default(),
            collation.unwrap_or_default(),
            mapping.mapping_id,
            mapping.evidence_digest.unwrap_or_default(),
        ));
    }
    let catalog_digest = change_event::stable_digest(&mapped_profiles);
    type_qualification_evidence::record_catalog_type_mapping_evidence(
        connector,
        "mysql.plugin_or_engine_types",
        &format!("{connector}.visible_user_column_type_mappings"),
        mapped_profiles.len(),
        0,
        "all reader-visible information_schema.COLUMNS outside MySQL system schemas",
        &catalog_digest,
    )
    .expect("write MySQL visible catalog type mapping evidence");
}

fn mysql_catalog_fingerprint(column: &MysqlCatalogColumn) -> String {
    change_event::stable_digest(&(
        &column.name,
        &column.native_type,
        &column.charset,
        &column.collation,
        column.nullable,
        column.generated,
        column.ordinal,
    ))
}

fn mysql_field(
    column: &MysqlCatalogColumn,
    mapping: &SourceTypeMapping,
    lineage: String,
    fingerprint: String,
) -> FieldDefinition {
    let key_ordinal = column.name.eq_ignore_ascii_case("id").then_some(0);
    FieldDefinition {
        reference: DefinitionReference::new(lineage, fingerprint),
        ordinal: column.ordinal,
        name: column.name.clone(),
        native_type: column.native_type.clone(),
        logical_type: mapping.logical_type.clone(),
        nullable: column.nullable,
        collation: column.collation.clone(),
        generated: column.generated,
        primary_key_ordinal: key_ordinal,
        unique: key_ordinal.is_some(),
        row_locator: key_ordinal.is_some(),
    }
}

fn plan_mysql_native_route(
    source_connector: &str,
    source_port: u16,
    source_table: &str,
    target_port: u16,
    target_table: &str,
    target_build: ServerBuildIdentity,
    target_manifest: change_event::TargetCapabilityManifest,
) -> Vec<change_event::ColumnConversionPlan> {
    let source_build = mysql_build(source_port);
    let source_columns = mysql_catalog_columns(source_port, source_table);
    let target_columns = mysql_catalog_columns(target_port, target_table);
    assert_eq!(source_columns.len(), target_columns.len());

    source_columns
        .iter()
        .zip(&target_columns)
        .map(|(source, target)| {
            assert_eq!(source.name, target.name);
            assert_eq!(source.ordinal, target.ordinal);
            let source_mapping = mysql_source_mapping(
                source_connector,
                &source.native_type,
                source.charset.as_deref(),
                source.collation.as_deref(),
            );
            let target_mapping = mysql_source_mapping(
                &target_manifest.connector.version,
                &target.native_type,
                target.charset.as_deref(),
                target.collation.as_deref(),
            );
            let source_lineage = format!("catalog:CDC_test.{target_table}.{}", source.name);
            let target_lineage = format!("target:CDC_test.{target_table}.{}", target.name);
            let source_field = mysql_field(
                source,
                &source_mapping,
                source_lineage,
                mysql_catalog_fingerprint(source),
            );
            let target_field = mysql_field(
                target,
                &target_mapping,
                target_lineage,
                mysql_catalog_fingerprint(target),
            );
            let mut options = RouteOptions {
                route_id: format!(
                    "{source_connector}-to-{}-all-types",
                    target_manifest.connector.version
                ),
                configuration_revision: "live-qualification:r1".into(),
                ..RouteOptions::default()
            };
            let plan_field = |options: RouteOptions| {
                change_event::plan_field_compatibility(FieldCompatibilityInput {
                    source_field: source_field.clone(),
                    target_field: target_field.clone(),
                    source_type_mapping: source_mapping.clone(),
                    source_connector: ConnectorIdentity::new(
                        "mysql",
                        source_manifest_version(source_connector),
                    ),
                    sink_connector: target_manifest.connector.clone(),
                    source_build: Some(source_build.clone()),
                    target_build: Some(target_build.clone()),
                    manifest: &target_manifest,
                    operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
                    presences: vec![
                        PresenceState::Value,
                        PresenceState::Null,
                        PresenceState::Unchanged,
                    ],
                    source_has_primary_key: true,
                    options,
                })
            };
            let mut result = plan_field(options.clone()).unwrap_or_else(|error| {
                panic!(
                    "{source_connector} -> mysql {} {} compatibility planning failed: {error}",
                    target_manifest.connector.version, source.name
                )
            });
            if result.status == change_event::CompatibilityStatus::NeedsConfirmation {
                let pending = result.plan.as_ref().expect("confirmation plan");
                options.confirmations.push(RiskConfirmation {
                    source_field_lineage: pending.source_field.lineage_id.clone(),
                    target_field_lineage: pending.target_field.lineage_id.clone(),
                    rule: pending.rule.clone(),
                    plan_digest: pending.plan_digest.clone(),
                    actor: "qualification-test".into(),
                    confirmed_at: "2026-09-27T00:00:00Z".into(),
                    reason: Some("explicitly qualify the declared native-type route".into()),
                });
                result = plan_field(options).unwrap_or_else(|error| {
                    panic!("replan {} after test confirmation: {error}", source.name)
                });
            }
            assert_eq!(
                result.status,
                change_event::CompatibilityStatus::Compatible,
                "{}: {} -> {} ({}); source semantic={:?}, target semantic={:?}, nullable={}/{}, generated={}/{}, collation={:?}/{:?}, source representation={:?}",
                source.name,
                source.native_type,
                target.native_type,
                result.explanation,
                source_field.logical_type,
                target_field.logical_type,
                source_field.nullable,
                target_field.nullable,
                source_field.generated,
                target_field.generated,
                source_field.collation,
                target_field.collation,
                source_mapping.value_representation,
            );
            result.plan.expect("qualified native-type plan")
        })
        .collect()
}

fn source_manifest_version(connector: &str) -> &'static str {
    match connector {
        "mysql_5_7" => "5.7",
        "mysql_8_0" => "8.0",
        "mysql_8_4" => "8.4",
        other => panic!("unknown MySQL source connector {other}"),
    }
}

#[test]
fn mysql_cross_version_timestamp_declarations_have_exact_sink_plans() {
    let source_mapping = ::mysql_5_7::source_type_mapping("timestamp(6)", None, None).unwrap();
    let field = |mapping: &SourceTypeMapping, lineage: &str, nullable: bool| FieldDefinition {
        reference: DefinitionReference::new(lineage, "timestamp-fingerprint"),
        ordinal: 0,
        name: "timestamp_value".into(),
        native_type: "timestamp(6)".into(),
        logical_type: mapping.logical_type.clone(),
        nullable,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    };
    for target_version in ["5.7", "8.0", "8.4"] {
        let target_mapping = mysql_source_mapping(target_version, "timestamp(6)", None, None);
        assert_eq!(source_mapping.logical_type, target_mapping.logical_type);
        let target_build = ServerBuildIdentity::new(
            "mysql",
            "oracle",
            format!("{target_version}.test"),
            format!("mysql-{target_version}-test"),
        );
        let manifest = match target_version {
            "5.7" => ::mysql_5_7::compatibility_manifest(target_build.clone()),
            "8.0" => ::mysql_8_0::compatibility_manifest(target_build.clone()),
            "8.4" => ::mysql_8_4::compatibility_manifest(target_build.clone()),
            _ => unreachable!(),
        };
        let result = change_event::plan_field_compatibility(FieldCompatibilityInput {
            source_field: field(&source_mapping, "catalog:CDC_test.t.timestamp_value", false),
            target_field: field(&target_mapping, "target:CDC_test.t.timestamp_value", true),
            source_type_mapping: source_mapping.clone(),
            source_connector: ConnectorIdentity::new("mysql", "5.7"),
            sink_connector: manifest.connector.clone(),
            source_build: None,
            target_build: Some(target_build),
            manifest: &manifest,
            operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
            presences: vec![
                PresenceState::Value,
                PresenceState::Null,
                PresenceState::Unchanged,
            ],
            source_has_primary_key: true,
            options: RouteOptions {
                route_id: format!("mysql-timestamp-to-{target_version}"),
                configuration_revision: "test:r1".into(),
                ..RouteOptions::default()
            },
        })
        .unwrap();
        assert_eq!(
            result.status,
            change_event::CompatibilityStatus::Compatible,
            "target {target_version}: {result:?}"
        );
    }
}

fn apply_all_mysql_type_transactions_to_sinks(
    source_connector: &str,
    source_port: u16,
    source_table_name: &str,
    transactions: &[change_event::ValidatedTransaction],
    source_type_ids: &[String],
) {
    apply_all_mysql_type_transactions_to_postgresql_sinks(
        source_connector,
        source_port,
        source_table_name,
        transactions,
        source_type_ids,
    );
    if std::env::var_os("CDC_TEST_POSTGRES_SINKS_ONLY").is_some() {
        return;
    }

    macro_rules! apply_to_sink {
        ($sink:ident, $port_key:literal, $port_default:literal, $sink_tag:literal, $sink_id:literal) => {{
            let target_port = port($port_key, $port_default);
            let target_table_name = format!("{source_table_name}_sink_{}", $sink_tag);
            let mut target_table = AllMysqlTypesTable::create_sink_named(
                connection(target_port, false),
                target_table_name.clone(),
            );
            let host = setting("CDC_MYSQL_HOST", "192.168.0.10");
            let mut target_config = ::$sink::TargetConfig::new(
                host.clone(),
                setting("CDC_MYSQL_WRITER_USER", "mysql_writer"),
                password("WRITER"),
            );
            target_config.port = target_port;
            let target_build = mysql_build(target_port);
            let target_manifest = ::$sink::compatibility_manifest(target_build.clone());
            let plans = plan_mysql_native_route(
                source_connector,
                source_port,
                source_table_name,
                target_port,
                &target_table_name,
                target_build,
                target_manifest,
            );

            for captured in transactions {
                let mut transaction = captured.transaction().clone();
                for change in &mut transaction.changes {
                    change.table.clone_from(&target_table_name);
                }
                let transaction = validate(transaction).unwrap_or_else(|error| {
                    panic!("{source_connector} -> {} ChangeEvent validation: {error}", $sink_tag)
                });
                let plan = ::$sink::sql_with_plans(&transaction, &plans).unwrap_or_else(|error| {
                    panic!("{source_connector} -> {} SQL plan: {error}", $sink_tag)
                });
                ::$sink::execute(&target_config, &plan).unwrap_or_else(|error| {
                    panic!("{source_connector} -> {} target apply: {error}", $sink_tag)
                });

                let change = &transaction.transaction().changes[0];
                match change.operation {
                    Operation::Insert | Operation::Update => {
                        let expected = change.after.as_ref().expect("after row image");
                        let snapshot_config = ::$sink::BinlogConfig::new(
                            host.clone(),
                            target_port,
                            setting("CDC_MYSQL_READER_USER", "mysql_reader"),
                            password("READER"),
                        );
                        let mut snapshot = ::$sink::snapshot(
                            snapshot_config,
                            vec![change_event::SnapshotTable {
                                schema: "CDC_test".into(),
                                table: target_table_name.clone(),
                                columns: Vec::new(),
                            }],
                        )
                        .unwrap_or_else(|error| {
                            panic!("{source_connector} -> {} target snapshot: {error}", $sink_tag)
                        });
                        let batch = snapshot
                            .next()
                            .expect("snapshot batch")
                            .unwrap_or_else(|error| {
                                panic!("{source_connector} -> {} snapshot read: {error}", $sink_tag)
                            });
                        assert_eq!(batch.rows.len(), 1, "target row after {:?}", change.operation);
                        assert_mysql_snapshot_equals_event(
                            expected,
                            &batch.rows[0],
                            &format!("{source_connector} -> {} {:?}", $sink_tag, change.operation),
                        );
                        drop(snapshot);
                    }
                    Operation::Delete => {
                        let count: Option<u64> = target_table
                            .conn
                            .query_first(format!(
                                "SELECT COUNT(*) FROM CDC_test.{target_table_name}"
                            ))
                            .unwrap_or_else(|error| {
                                panic!("{source_connector} -> {} delete verification: {error}", $sink_tag)
                            });
                        assert_eq!(count, Some(0), "target should be empty after DELETE");
                    }
                }
            }
            println!(
                "PASS {source_connector} -> mysql_{}: ChangeEvent INSERT/UPDATE/DELETE preserved all 61 native values",
                $sink_tag
            );
            type_qualification_evidence::record_sink_type_evidence(
                source_connector,
                $sink_id,
                &format!("{source_connector}.all_types_to_{}", $sink_tag),
                source_type_ids.iter().cloned(),
                "VALUE_PRESERVED",
                "native_target_column",
            )
            .expect("write per-native-type MySQL sink evidence");
            target_table.cleanup();
        }};
    }

    apply_to_sink!(mysql_5_7, "CDC_MYSQL57_PORT", 33061, "5_7", "mysql_5_7");
    apply_to_sink!(mysql_8_0, "CDC_MYSQL80_PORT", 33062, "8_0", "mysql_8_0");
    apply_to_sink!(mysql_8_4, "CDC_MYSQL84_PORT", 33063, "8_4", "mysql_8_4");
}

fn apply_all_mysql_type_transactions_to_postgresql_sinks(
    source_connector: &str,
    source_port: u16,
    source_table_name: &str,
    transactions: &[change_event::ValidatedTransaction],
    source_type_ids: &[String],
) {
    for target_version in ["15", "16", "17"] {
        let source_columns = mysql_catalog_columns(source_port, source_table_name);
        let representation_declarations = source_columns
            .iter()
            .filter(|column| {
                matches!(
                    mysql_source_mapping(
                        source_connector,
                        &column.native_type,
                        column.charset.as_deref(),
                        column.collation.as_deref()
                    )
                    .logical_type,
                    change_event::LogicalType::Raw { .. }
                )
            })
            .map(|column| column.native_type.clone())
            .collect::<Vec<_>>();
        let representation_type_ids = type_qualification_evidence::source_native_type_ids(
            source_connector,
            representation_declarations,
        );
        let representation_set = representation_type_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let semantic_type_ids = source_type_ids
            .iter()
            .filter(|type_id| !representation_set.contains(*type_id))
            .cloned()
            .collect::<Vec<_>>();
        let source_build = mysql_build(source_port);
        let source_identity =
            ConnectorIdentity::new("mysql", source_manifest_version(source_connector));
        let admin_user = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
        let writer_user = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
        let test_password = std::env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))
            .expect("set the PostgreSQL live qualification test password");
        let host = postgres_env::setting(target_version, "HOST", "192.168.0.10");
        let target_port: u16 = postgres_env::setting(
            target_version,
            "PORT",
            match target_version {
                "15" => "54321",
                "16" => "54322",
                "17" => "54323",
                _ => unreachable!(),
            },
        )
        .parse()
        .expect("valid PostgreSQL port");
        let target_table_name = format!(
            "cdc_mysql_{}_{}_{}",
            target_version,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        );
        let config =
            postgresql_15::TargetConfig::new(host, "CDC_test", writer_user, test_password.clone())
                .with_port(target_port);

        let runtime =
            tokio::runtime::Runtime::new().expect("create PostgreSQL qualification runtime");
        runtime.block_on(async {
            let mut admin = sqlx::PgConnection::connect_with(&postgres_env::options(
                target_version,
                &admin_user,
                &test_password,
            ))
            .await
            .unwrap_or_else(|error| {
                panic!("connect to PostgreSQL {target_version} test instance: {error}")
            });
            let server_version: String = sqlx::query_scalar("SHOW server_version")
                .fetch_one(&mut admin)
                .await
                .expect("read PostgreSQL target version");
            assert_eq!(
                server_version.split('.').next(),
                Some(target_version),
                "test endpoint must be PostgreSQL {target_version}"
            );
            let version_text: String = sqlx::query_scalar("SELECT version()")
                .fetch_one(&mut admin)
                .await
                .expect("read PostgreSQL target build identity");
            let target_build = ServerBuildIdentity::new(
                "postgresql",
                "community",
                server_version.clone(),
                version_text,
            );
            let full_target_manifest = postgresql_15::compatibility_manifest_for_version(
                target_build.clone(),
                target_version,
            );
            let target_manifest = postgresql_carrier_manifest(&full_target_manifest);
            let quoted_table = quote_postgres_identifier(&target_table_name);
            sqlx::query("CREATE SCHEMA IF NOT EXISTS \"CDC_test\"")
                .execute(&mut admin)
                .await
                .expect("ensure PostgreSQL qualification schema");

            let mut plans = Vec::with_capacity(source_columns.len());
            let mut definitions = Vec::with_capacity(source_columns.len());
            for source_column in &source_columns {
                let mapping = mysql_source_mapping(
                    source_connector,
                    &source_column.native_type,
                    source_column.charset.as_deref(),
                    source_column.collation.as_deref(),
                );
                let is_key = source_column.name.eq_ignore_ascii_case("id");
                let is_raw = matches!(mapping.logical_type, change_event::LogicalType::Raw { .. });
                let conversion_kind = if is_key {
                    None
                } else if is_raw {
                    Some("source_representation")
                } else {
                    Some("logical_value_json")
                };
                if conversion_kind == Some("source_representation") {
                    assert!(
                        mapping.source_representation_evidence.is_some(),
                        "MySQL {} raw type {} must carry representation evidence",
                        source_connector,
                        source_column.native_type
                    );
                }
                let (target_native, target_logical) = match conversion_kind {
                    None if matches!(
                        mapping.logical_type,
                        change_event::LogicalType::Integer {
                            signed: false,
                            bits: 64
                        }
                    ) => ("numeric(20,0)", mapping.logical_type.clone()),
                    None => ("bigint", change_event::LogicalType::integer(true, 64)),
                    Some("logical_value_json") => {
                        ("text", change_event::LogicalType::text("UTF8", None))
                    }
                    Some("source_representation") => (
                        "bytea",
                        change_event::LogicalType::binary(Some(1_073_741_823)),
                    ),
                    _ => unreachable!(),
                };
                definitions.push(format!(
                    "{} {}{}",
                    quote_postgres_identifier(&source_column.name),
                    target_native.to_ascii_uppercase(),
                    if is_key { " NOT NULL PRIMARY KEY" } else { "" }
                ));
                let source_lineage = format!(
                    "catalog:CDC_test.{target_table_name}.{}",
                    source_column.name
                );
                let target_lineage =
                    format!("target:CDC_test.{target_table_name}.{}", source_column.name);
                let source_field = mysql_field(
                    source_column,
                    &mapping,
                    source_lineage,
                    mysql_catalog_fingerprint(source_column),
                );
                let target_field = FieldDefinition {
                    reference: DefinitionReference::new(
                        target_lineage,
                        change_event::stable_digest(&(&source_column.name, target_native, is_key)),
                    ),
                    ordinal: source_column.ordinal,
                    name: source_column.name.clone(),
                    native_type: target_native.into(),
                    logical_type: target_logical,
                    nullable: !is_key,
                    collation: None,
                    generated: false,
                    primary_key_ordinal: is_key.then_some(0),
                    unique: is_key,
                    row_locator: is_key,
                };
                let selected_rule = conversion_kind.map(|kind| {
                    let capability = target_manifest
                        .capabilities
                        .iter()
                        .find(|entry| {
                            entry
                                .target
                                .parameters
                                .get("conversion_kind")
                                .map(String::as_str)
                                == Some(kind)
                        })
                        .unwrap_or_else(|| {
                            panic!("PostgreSQL {target_version} lacks {kind} carrier")
                        });
                    change_event::RuleReference {
                        id: capability.rule.id.clone(),
                        version: capability.rule.version.clone(),
                    }
                });
                let presences = match conversion_kind {
                    None => vec![PresenceState::Value, PresenceState::Unchanged],
                    Some("logical_value_json") => vec![
                        PresenceState::Value,
                        PresenceState::Null,
                        PresenceState::Unchanged,
                    ],
                    Some("source_representation") => vec![
                        PresenceState::SourceRepresentation,
                        PresenceState::Null,
                        PresenceState::Unchanged,
                    ],
                    _ => unreachable!(),
                };
                let plan_field = |confirmations| {
                    change_event::plan_field_compatibility(FieldCompatibilityInput {
                        source_field: source_field.clone(),
                        target_field: target_field.clone(),
                        source_type_mapping: mapping.clone(),
                        source_connector: source_identity.clone(),
                        sink_connector: target_manifest.connector.clone(),
                        source_build: Some(source_build.clone()),
                        target_build: Some(target_build.clone()),
                        manifest: &target_manifest,
                        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
                        presences: presences.clone(),
                        source_has_primary_key: true,
                        options: RouteOptions {
                            route_id: format!(
                                "{source_connector}-to-postgresql-{target_version}-all-types"
                            ),
                            configuration_revision: "live-all-types:r1".into(),
                            selected_rule: selected_rule.clone(),
                            confirmations,
                            ..RouteOptions::default()
                        },
                    })
                };
                let initial = plan_field(Vec::new()).unwrap_or_else(|error| {
                    panic!(
                        "plan MySQL {} {} to PostgreSQL {target_version}: {error}",
                        source_column.native_type, source_column.name
                    )
                });
                let mut plan = initial.plan.clone().unwrap_or_else(|| {
                    panic!(
                        "MySQL {} {} to PostgreSQL {target_version} did not produce a plan: {initial:?}",
                        source_column.native_type,
                        source_column.name
                    )
                });
                if plan.confirmation == change_event::PlanConfirmationState::Required {
                    let confirmation = RiskConfirmation {
                        source_field_lineage: plan.source_field.lineage_id.clone(),
                        target_field_lineage: plan.target_field.lineage_id.clone(),
                        rule: plan.rule.clone(),
                        plan_digest: plan.plan_digest.clone(),
                        actor: "live-type-qualification".into(),
                        confirmed_at: "2026-09-28T00:00:00Z".into(),
                        reason: Some(
                            "qualify the explicit tagged LogicalValue carrier for this native type"
                                .into(),
                        ),
                    };
                    let confirmed = plan_field(vec![confirmation]).unwrap_or_else(|error| {
                        panic!("confirm MySQL {} plan: {error}", source_column.native_type)
                    });
                    assert_eq!(
                        confirmed.status,
                        change_event::CompatibilityStatus::Compatible,
                        "MySQL {} to PostgreSQL {target_version}: {confirmed:?}",
                        source_column.native_type
                    );
                    plan = confirmed.plan.expect("confirmed plan");
                } else {
                    assert_eq!(
                        initial.status,
                        change_event::CompatibilityStatus::Compatible,
                        "MySQL {} to PostgreSQL {target_version}: {initial:?}",
                        source_column.native_type
                    );
                }
                plans.push(plan);
            }

            let create_sql = format!(
                "CREATE TABLE \"CDC_test\".{quoted_table} ({})",
                definitions.join(", ")
            );
            sqlx::query(sqlx::AssertSqlSafe(create_sql.as_str()))
                .execute(&mut admin)
                .await
                .unwrap_or_else(|error| {
                    panic!("create PostgreSQL {target_version} carrier table: {error}")
                });

            for captured in transactions {
                let mut transaction = captured.transaction().clone();
                for change in &mut transaction.changes {
                    change.table.clone_from(&target_table_name);
                }
                let transaction = validate(transaction).unwrap_or_else(|error| {
                    panic!("validate MySQL -> PostgreSQL ChangeEvent: {error}")
                });
                for change in &transaction.transaction().changes {
                    for image in [&change.before, &change.after].into_iter().flatten() {
                        for column in image {
                            if let (Some("logical_value_json"), Datum::Value(value)) = (
                                plans
                                    .iter()
                                    .find(|plan| {
                                        plan.source_field.lineage_id.rsplit('.').next()
                                            == Some(column.name.as_str())
                                    })
                                    .and_then(|plan| {
                                        plan.target
                                            .parameters
                                            .get("conversion_kind")
                                            .map(String::as_str)
                                    }),
                                &column.datum,
                            ) {
                                change_event::validate_value_against_plan(
                                    plans
                                        .iter()
                                        .find(|plan| {
                                            plan.source_field.lineage_id.rsplit('.').next()
                                                == Some(column.name.as_str())
                                        })
                                        .expect("field plan"),
                                    value,
                                )
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "MySQL {} native type {} column {} value {:?} fails its tagged carrier contract: {}",
                                        source_connector,
                                        column.native_type,
                                        column.name,
                                        value,
                                        error
                                    )
                                });
                            }
                        }
                    }
                }
                let plan = match target_version {
                    "15" => postgresql_15::sql_with_plans(&transaction, &plans),
                    "16" => postgresql_16::sql_with_plans(&transaction, &plans),
                    "17" => postgresql_17::sql_with_plans(&transaction, &plans),
                    _ => unreachable!(),
                }
                .unwrap_or_else(|error| {
                    panic!("MySQL -> PostgreSQL {target_version} SQL plan: {error}")
                });
                match target_version {
                    "15" => postgresql_15::execute(&config, &plan).await,
                    "16" => postgresql_16::execute(&config, &plan).await,
                    "17" => postgresql_17::execute(&config, &plan).await,
                    _ => unreachable!(),
                }
                .unwrap_or_else(|error| {
                    panic!("MySQL -> PostgreSQL {target_version} apply: {error}")
                });

                let change = &transaction.transaction().changes[0];
                match change.operation {
                    Operation::Insert | Operation::Update => {
                        let expected = change.after.as_deref().expect("after image");
                        assert_postgres_carrier_readback(
                            &mut admin,
                            &target_table_name,
                            expected,
                            &plans,
                            target_version,
                        )
                        .await;
                    }
                    Operation::Delete => {
                        let count_sql = format!("SELECT count(*) FROM \"CDC_test\".{quoted_table}");
                        let count: i64 =
                            sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.as_str()))
                                .fetch_one(&mut admin)
                                .await
                                .expect("verify PostgreSQL delete");
                        assert_eq!(count, 0, "PostgreSQL {target_version} DELETE applied");
                    }
                }
            }

            let drop_sql = format!("DROP TABLE \"CDC_test\".{quoted_table}");
            sqlx::query(sqlx::AssertSqlSafe(drop_sql.as_str()))
                .execute(&mut admin)
                .await
                .expect("drop PostgreSQL qualification table");
            admin
                .close()
                .await
                .expect("close PostgreSQL admin connection");
        });

        if !semantic_type_ids.is_empty() {
            type_qualification_evidence::record_sink_type_evidence(
                source_connector,
                &format!("postgresql_{target_version}"),
                &format!("{source_connector}.all_types_to_postgresql_{target_version}"),
                semantic_type_ids.iter().cloned(),
                "VALUE_PRESERVED",
                "logical_value_json_carrier",
            )
            .expect("write per-native-type PostgreSQL sink evidence");
        }
        if !representation_type_ids.is_empty() {
            type_qualification_evidence::record_sink_type_evidence(
                source_connector,
                &format!("postgresql_{target_version}"),
                &format!(
                    "{source_connector}.all_types_to_postgresql_{target_version}_representation"
                ),
                representation_type_ids,
                "SOURCE_REPRESENTATION_PRESERVED",
                "source_representation_blob_carrier",
            )
            .expect("write per-native-type PostgreSQL representation evidence");
        }
        println!(
            "PASS {source_connector} -> postgresql_{target_version}: ChangeEvent INSERT/UPDATE/DELETE preserved all captured MySQL native values"
        );
    }
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn postgresql_carrier_manifest(
    full: &change_event::TargetCapabilityManifest,
) -> change_event::TargetCapabilityManifest {
    let unsigned_bigint = change_event::LogicalType::integer(false, 64);
    let capabilities = full
        .capabilities
        .iter()
        .filter(|entry| {
            matches!(
                entry
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str),
                Some("logical_value_json" | "source_representation")
            ) || (entry.source_logical_type == unsigned_bigint
                && entry
                    .target
                    .native_type
                    .eq_ignore_ascii_case("numeric(20,0)")
                && entry.rule.qualification == change_event::QualificationLevel::Exact)
        })
        .cloned()
        .collect();
    change_event::TargetCapabilityManifest::new(
        full.connector.clone(),
        full.target_build.clone(),
        capabilities,
        full.requires_primary_key,
    )
}

async fn assert_postgres_carrier_readback(
    admin: &mut sqlx::PgConnection,
    table: &str,
    expected: &[change_event::ColumnDatum],
    plans: &[change_event::ColumnConversionPlan],
    target_version: &str,
) {
    let key = expected
        .iter()
        .find(|column| column.primary_key_ordinal == Some(0))
        .expect("source event has primary key");
    let key_value = match &key.datum {
        Datum::Value(LogicalValue::Integer { value, .. }) => value
            .parse::<i64>()
            .expect("test primary key fits PostgreSQL bigint"),
        other => panic!("unexpected MySQL test key value: {other:?}"),
    };
    let projection = expected
        .iter()
        .map(|column| quote_postgres_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {projection} FROM \"CDC_test\".{} WHERE {}=$1",
        quote_postgres_identifier(table),
        quote_postgres_identifier(&key.name)
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
        .bind(key_value)
        .fetch_optional(&mut *admin)
        .await
        .unwrap_or_else(|error| panic!("read PostgreSQL {target_version} carrier row: {error}"))
        .unwrap_or_else(|| panic!("PostgreSQL {target_version} row should exist after DML"));
    for (index, column) in expected.iter().enumerate() {
        if column.primary_key_ordinal.is_some() {
            continue;
        }
        let plan = plans
            .iter()
            .find(|plan| {
                plan.source_field.lineage_id.rsplit('.').next() == Some(column.name.as_str())
            })
            .expect("every source column has a conversion plan");
        let conversion = plan
            .target
            .parameters
            .get("conversion_kind")
            .map(String::as_str);
        match (conversion, &column.datum) {
            (Some("logical_value_json"), Datum::Value(expected_value)) => {
                let actual: Option<String> = row
                    .try_get(index)
                    .unwrap_or_else(|error| panic!("read tagged value {}: {error}", column.name));
                let actual = actual.unwrap_or_else(|| {
                    panic!(
                        "PostgreSQL {target_version} value {} is unexpectedly NULL",
                        column.name
                    )
                });
                let actual: LogicalValue = serde_json::from_str(&actual).unwrap_or_else(|error| {
                    panic!("decode PostgreSQL tagged value {}: {error}", column.name)
                });
                assert_eq!(
                    &actual, expected_value,
                    "PostgreSQL {target_version} carrier value for {}",
                    column.name
                );
            }
            (Some("logical_value_json"), Datum::Null) => {
                let actual: Option<String> = row
                    .try_get(index)
                    .unwrap_or_else(|error| panic!("read null value {}: {error}", column.name));
                assert!(actual.is_none(), "PostgreSQL NULL for {}", column.name);
            }
            (Some("source_representation"), Datum::SourceRepresentationEnvelope(expected)) => {
                let actual: Option<Vec<u8>> = row
                    .try_get(index)
                    .unwrap_or_else(|error| panic!("read raw value {}: {error}", column.name));
                assert_eq!(
                    actual,
                    Some(serde_json::to_vec(expected).expect("serialize source envelope")),
                    "PostgreSQL {target_version} raw representation for {}",
                    column.name
                );
            }
            (Some("source_representation"), Datum::Null) => {
                let actual: Option<Vec<u8>> = row
                    .try_get(index)
                    .unwrap_or_else(|error| panic!("read raw null {}: {error}", column.name));
                assert!(
                    actual.is_none(),
                    "PostgreSQL raw carrier NULL for {}",
                    column.name
                );
            }
            (_, Datum::Unchanged | Datum::Unavailable) => {}
            (conversion, datum) => panic!(
                "PostgreSQL {target_version} carrier kind {conversion:?} cannot qualify {} with {datum:?}",
                column.name
            ),
        }
    }
}

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

            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_generic_geometry_capture() {
                let port = port($port_key, $port);
                let mut table = GenericGeometryTable::create(port);
                let mut reader = connection(port, true);
                let (file, position, _, _, _): (String, u64, String, String, String) =
                    reader.query_first($status).unwrap().unwrap();

                table
                    .conn
                    .query_drop(format!(
                        "INSERT INTO CDC_test.{} (id, shape) VALUES (1, ST_GeomFromText('POINT(1 2)'))",
                        table.name
                    ))
                    .unwrap();
                table
                    .conn
                    .query_drop(format!(
                        "UPDATE CDC_test.{} SET shape=ST_GeomFromText('LINESTRING(0 0,1 1)') WHERE id=1",
                        table.name
                    ))
                    .unwrap();
                table
                    .conn
                    .query_drop(format!(
                        "DELETE FROM CDC_test.{} WHERE id=1",
                        table.name
                    ))
                    .unwrap();

                let mut config = ::$adapter::BinlogConfig::new(
                    setting("CDC_MYSQL_HOST", "192.168.0.10"),
                    port,
                    setting("CDC_MYSQL_READER_USER", "mysql_reader"),
                    password("READER"),
                );
                config.start_mode = ::$adapter::BinlogStartMode::Position;
                config.start = Some(::$adapter::BinlogPosition { file, position });
                config.non_blocking = true;
                config.max_events = Some(10_000);
                config.tables = vec![("CDC_test".into(), table.name.clone())];
                let mut capture = ::$adapter::binlog(config).unwrap();
                let mut transactions = Vec::new();
                while let Some(transaction) = capture.next_change_event().unwrap() {
                    transactions.push(transaction);
                }

                assert_eq!(transactions.len(), 3);
                let changes: Vec<_> = transactions
                    .iter()
                    .flat_map(|transaction| &transaction.transaction().changes)
                    .collect();
                assert_eq!(changes.len(), 3);
                assert!(matches!(changes[0].operation, Operation::Insert));
                assert!(matches!(changes[1].operation, Operation::Update));
                assert!(matches!(changes[2].operation, Operation::Delete));
                assert!(matches!(
                    &changes[0].after.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "point"
                ));
                assert!(matches!(
                    &changes[1].before.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "point"
                ));
                assert!(matches!(
                    &changes[1].after.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "linestring"
                ));
                assert!(matches!(
                    &changes[2].before.as_ref().unwrap()[1].datum,
                    Datum::Value(LogicalValue::Spatial { geometry_type, srid: Some(0), .. })
                        if geometry_type == "linestring"
                ));
                for transaction in &transactions {
                    assert_eq!(
                        change_event::json(&roundtrip(transaction)).unwrap(),
                        change_event::json(transaction).unwrap()
                    );
                }

                table.cleanup();
            }

            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_visible_type_catalog_mapping() {
                let port = port($port_key, $port);
                let mut table = AllMysqlTypesTable::create(port);
                qualify_mysql_visible_catalog_types(stringify!($adapter), port);
                table.cleanup();
            }

            #[test]
            #[ignore = "requires an explicitly configured test database"]
            fn live_all_mysql_type_capture() {
                let port = port($port_key, $port);
                let mut table = AllMysqlTypesTable::create(port);
                let mut reader = connection(port, true);
                let (file, position, _, _, _): (String, u64, String, String, String) =
                    reader.query_first($status).unwrap().unwrap();

                table.conn.query_drop("SET SESSION sql_mode = ''").unwrap();
                let insert = format!(
                    "INSERT INTO CDC_test.{} VALUES (
                        1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,
                        1234567890, 1234567890, 12345678901234567890.123456, 123456789.12, 123456789.123,
                        1.25, 1.5, 1.75, 12.3456, 1.125, 12.34,
                        b'1', b'1010010110100101101001011010010110100101101001011010010110100101',
                        '2024-02-29', '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.123456', '838:59:58.999999', 2024,
                        'fixed', 'base', 'tiny text', 'text value', 'medium text', 'long text',
                        X'01020304', X'00FF', X'01', X'0203', X'040506', X'070809',
                        'beta', 'a,c', JSON_OBJECT('kind','mysql','value',1),
                        ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'),
                        ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'),
                        ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
                        ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'),
                        ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))'),
                        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))'),
                        NULL,
                        99999999999999999999999999999999999.999999999999999999999999999999,
                        -0e0, -0e0,
                        '0000-00-00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00.000000',
                        'é', X'01'
                    )",
                    table.name
                );
                table.conn.query_drop(insert).unwrap();
                table.conn.query_drop(format!(
                    "UPDATE CDC_test.{} SET varchar_value='updated', null_marker='now', float_precision_boundary=2.5 WHERE id=1",
                    table.name
                )).unwrap();
                table.conn.query_drop(format!(
                    "DELETE FROM CDC_test.{} WHERE id=1",
                    table.name
                )).unwrap();

                let mut config = ::$adapter::BinlogConfig::new(
                    setting("CDC_MYSQL_HOST", "192.168.0.10"),
                    port,
                    setting("CDC_MYSQL_READER_USER", "mysql_reader"),
                    password("READER"),
                );
                config.start_mode = ::$adapter::BinlogStartMode::Position;
                config.start = Some(::$adapter::BinlogPosition { file, position });
                config.non_blocking = true;
                config.max_events = Some(20_000);
                config.tables = vec![("CDC_test".into(), table.name.clone())];
                let mut capture = ::$adapter::binlog(config).unwrap();
                let mut transactions = Vec::new();
                while let Some(transaction) = capture.next_change_event().unwrap() {
                    transactions.push(transaction);
                }

                assert_eq!(transactions.len(), 3, "INSERT/UPDATE/DELETE commit boundaries");
                let changes: Vec<_> = transactions
                    .iter()
                    .flat_map(|transaction| &transaction.transaction().changes)
                    .collect();
                assert_eq!(changes.len(), 3);
                assert!(matches!(changes[0].operation, Operation::Insert));
                assert!(matches!(changes[1].operation, Operation::Update));
                assert!(matches!(changes[2].operation, Operation::Delete));

                let columns = [
                    ("id", "integer"),
                    ("tiny_signed", "integer"), ("tiny_unsigned", "integer"),
                    ("small_signed", "integer"), ("small_unsigned", "integer"),
                    ("medium_signed", "integer"), ("medium_unsigned", "integer"),
                    ("int_signed", "integer"), ("int_unsigned", "integer"),
                    ("big_signed", "integer"), ("big_unsigned", "integer"),
                    ("decimal_default", "decimal"), ("decimal_precision", "decimal"),
                    ("decimal_scaled", "decimal"), ("decimal_unsigned", "decimal"),
                    ("numeric_alias", "decimal"),
                    ("float_plain", "float"), ("float_precision", "float"),
                    ("float_precision_boundary", "float"), ("float_scaled", "float"),
                    ("double_plain", "float"), ("double_precision", "float"),
                    ("bit_one", "bit"), ("bit_wide", "bit"),
                    ("date_value", "date"), ("datetime_value", "local_datetime"),
                    ("timestamp_value", "instant"), ("time_value", "duration"),
                    ("year_value", "year"),
                    ("char_value", "text"), ("varchar_value", "text"),
                    ("tinytext_value", "text"), ("text_value", "text"),
                    ("mediumtext_value", "text"), ("longtext_value", "text"),
                    ("binary_value", "binary"), ("varbinary_value", "binary"),
                    ("tinyblob_value", "binary"), ("blob_value", "binary"),
                    ("mediumblob_value", "binary"), ("longblob_value", "binary"),
                    ("enum_value", "enum"), ("set_value", "set"), ("json_value", "json"),
                    ("geometry_value", "spatial"), ("point_value", "spatial"),
                    ("linestring_value", "spatial"), ("polygon_value", "spatial"),
                    ("multipoint_value", "spatial"), ("multilinestring_value", "spatial"),
                    ("multipolygon_value", "spatial"), ("geometrycollection_value", "spatial"),
                    ("null_marker", "null"),
                    ("decimal_max_precision", "decimal"),
                    ("float_negative_zero", "float"), ("double_negative_zero", "float"),
                    ("date_zero", "invalid_temporal"),
                    ("datetime_zero", "invalid_temporal"),
                    ("timestamp_zero", "invalid_temporal"),
                    ("latin1_value", "text"), ("binary_padding", "binary"),
                ];
                fn kind(value: &LogicalValue) -> &'static str {
                    match value {
                        LogicalValue::Integer { .. } => "integer",
                        LogicalValue::Decimal { .. } => "decimal",
                        LogicalValue::Float { .. } => "float",
                        LogicalValue::Text { .. } => "text",
                        LogicalValue::Binary { .. } => "binary",
                        LogicalValue::BitString { .. } => "bit",
                        LogicalValue::Date { .. } => "date",
                        LogicalValue::LocalDatetime { .. } => "local_datetime",
                        LogicalValue::Instant { .. } => "instant",
                        LogicalValue::Duration { .. } => "duration",
                        LogicalValue::Year { .. } => "year",
                        LogicalValue::Enum { .. } => "enum",
                        LogicalValue::Set { .. } => "set",
                        LogicalValue::Json { .. } => "json",
                        LogicalValue::Spatial { .. } => "spatial",
                        LogicalValue::InvalidTemporal { .. } => "invalid_temporal",
                        other => panic!("unexpected MySQL LogicalValue variant: {other:?}"),
                    }
                }
                let insert_image = changes[0].after.as_ref().unwrap();
                let qualified_source_types = assert_mysql_inventory_types_are_live_captured(
                    stringify!($adapter),
                    insert_image,
                );
                assert_eq!(insert_image.len(), columns.len());
                for ((column, (expected_name, expected_kind)), ordinal) in
                    insert_image.iter().zip(columns).zip(0_usize..)
                {
                    assert_eq!(column.ordinal, ordinal);
                    assert_eq!(column.name, expected_name);
                    assert_eq!(
                        column.primary_key_ordinal.is_some(),
                        expected_name == "id"
                    );
                    match (&column.datum, expected_kind) {
                        (Datum::Null, "null") => {}
                        (Datum::Value(value), kind_expected) => {
                            assert_eq!(kind(value), kind_expected, "{}", column.name)
                        }
                        (datum, kind_expected) => panic!(
                            "{} expected {kind_expected}, found {datum:?}",
                            column.name
                        ),
                    }
                }
                let boundary_bits = if insert_image[18]
                    .native_type
                    .to_ascii_lowercase()
                    .starts_with("double")
                {
                    64
                } else {
                    32
                };
                assert!(matches!(
                    &insert_image[18].datum,
                    Datum::Value(LogicalValue::Float { bits, .. }) if *bits == boundary_bits
                ), "FLOAT(24) catalog type {:?} decoded as {:?}", insert_image[18].native_type, insert_image[18].datum);
                for (index, expected) in [
                    (1, "-128"),
                    (3, "-32768"),
                    (5, "-8388608"),
                    (7, "-2147483648"),
                    (9, "-9223372036854775808"),
                    (2, "255"),
                    (4, "65535"),
                    (6, "16777215"),
                    (8, "4294967295"),
                    (10, "18446744073709551615"),
                ] {
                    assert!(matches!(
                        &insert_image[index].datum,
                        Datum::Value(LogicalValue::Integer { value, .. }) if value == expected
                    ), "{} expected integer boundary {expected}, found {:?}", insert_image[index].name, insert_image[index].datum);
                }
                assert!(matches!(
                    &insert_image[22].datum,
                    Datum::Value(LogicalValue::BitString {
                        bit_length: 1,
                        bit_order: change_event::BitOrder::LsbFirst,
                        bytes_base64url,
                        ..
                    }) if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [1])
                ), "BIT(1) value was not preserved: {:?}", insert_image[22].datum);
                assert!(matches!(
                    &insert_image[23].datum,
                    Datum::Value(LogicalValue::BitString {
                        bit_length: 64,
                        bit_order: change_event::BitOrder::LsbFirst,
                        bytes_base64url,
                        ..
                    }) if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [0xa5; 8])
                ), "BIT(64) bytes were not preserved: {:?}", insert_image[23].datum);
                assert!(matches!(
                    &insert_image[24].datum,
                    Datum::Value(LogicalValue::Date { year: 2024, month: 2, day: 29 })
                ));
                assert!(matches!(
                    &insert_image[25].datum,
                    Datum::Value(LogicalValue::LocalDatetime {
                        year: 2024,
                        month: 2,
                        day: 29,
                        hour: 12,
                        minute: 34,
                        second: 56,
                        microsecond: 123_456,
                    })
                ));
                assert!(matches!(
                    &insert_image[28].datum,
                    Datum::Value(LogicalValue::Year { value: 2024 })
                ));
                assert!(matches!(
                    &insert_image[41].datum,
                    Datum::Value(LogicalValue::Enum { label }) if label == "beta"
                ));
                assert!(matches!(
                    &insert_image[42].datum,
                    Datum::Value(LogicalValue::Set { members }) if members == &["a", "c"]
                ));
                assert!(matches!(
                    &insert_image[53].datum,
                    Datum::Value(LogicalValue::Decimal { unscaled, scale })
                        if unscaled == "99999999999999999999999999999999999999999999999999999999999999999" && *scale == 30
                ), "DECIMAL(65,30) boundary decoded as {:?}", insert_image[53].datum);
                assert!(matches!(
                    &insert_image[54].datum,
                    Datum::Value(LogicalValue::Float { bits: 32, ieee754_hex }) if ieee754_hex == "80000000"
                ), "FLOAT negative zero decoded as {:?}", insert_image[54].datum);
                assert!(matches!(
                    &insert_image[55].datum,
                    Datum::Value(LogicalValue::Float { bits: 64, ieee754_hex }) if ieee754_hex == "8000000000000000"
                ), "DOUBLE negative zero decoded as {:?}", insert_image[55].datum);
                for (index, kind) in [(56, "mysql.date"), (57, "mysql.datetime"), (58, "mysql.timestamp")] {
                    assert!(matches!(
                        &insert_image[index].datum,
                        Datum::Value(LogicalValue::InvalidTemporal { kind: actual, raw })
                            if actual == kind && raw.starts_with("0000-00-00")
                    ), "{} zero date decoded as {:?}", insert_image[index].name, insert_image[index].datum);
                }
                assert!(matches!(
                    &insert_image[59].datum,
                    Datum::Value(LogicalValue::Text { charset, bytes_base64url, .. })
                        if charset.eq_ignore_ascii_case("latin1")
                            && URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes.first() == Some(&0xe9))
                ), "latin1 CHAR bytes were not preserved: {:?}", insert_image[59].datum);
                assert!(matches!(
                    &insert_image[60].datum,
                    Datum::Value(LogicalValue::Binary { bytes_base64url })
                        if URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| bytes == [1, 0, 0, 0])
                ), "fixed BINARY zero padding was not preserved: {:?}", insert_image[60].datum);
                let update_before = changes[1].before.as_ref().unwrap();
                let update_after = changes[1].after.as_ref().unwrap();
                assert_eq!(update_before.len(), columns.len());
                assert_eq!(update_after.len(), columns.len());
                assert!(matches!(&update_before[30].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("base")), "update before varchar: {:?}", update_before[30]);
                assert!(matches!(&update_after[30].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("updated")));
                assert!(matches!(&update_before[52].datum, Datum::Null));
                assert!(matches!(&update_after[52].datum, Datum::Value(LogicalValue::Text { text, .. }) if text.as_deref() == Some("now")));
                assert_eq!(changes[2].before.as_ref().unwrap().len(), columns.len());

                for transaction in &transactions {
                    assert_eq!(
                        change_event::json(&roundtrip(transaction)).unwrap(),
                        change_event::json(transaction).unwrap()
                    );
                }
                apply_all_mysql_type_transactions_to_sinks(
                    stringify!($adapter),
                    port,
                    &table.name,
                    &transactions,
                    &qualified_source_types,
                );
                qualify_mysql_visible_catalog_types(stringify!($adapter), port);
                type_qualification_evidence::record_source_type_evidence(
                    stringify!($adapter),
                    concat!(stringify!($adapter), ".all_types_capture"),
                    qualified_source_types,
                    Vec::new(),
                )
                .expect("write per-native-type MySQL source evidence");
                println!(
                    "PASS {}: {} native columns; typed INSERT/UPDATE/DELETE, NULL, full before/after presence and JSON replay",
                    stringify!($adapter),
                    columns.len()
                );
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
