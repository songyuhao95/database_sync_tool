//! Live PostgreSQL 15/16/17 built-in type capture qualification.
//! Run through `scripts/test.ps1 -Live`; objects are isolated by a unique suffix.

use change_event::{
    ColumnConversionPlan, ConnectorIdentity, Datum, DefinitionReference, FieldCompatibilityInput,
    FieldDefinition, LogicalValue, Operation, PlanConfirmationState, PresenceState,
    RiskConfirmation, RouteOptions, ServerBuildIdentity, SourceTypeMapping,
};
use mysql_driver::prelude::Queryable as MysqlQueryable;
use postgresql_15::{CancellationToken, Config, Result};
use sqlx::{Connection, PgConnection, Row};
#[path = "../../../tests/support/postgres_builtin_fixtures.rs"]
mod postgres_builtin_fixtures;
use postgres_builtin_fixtures::BUILTINS;

use std::{env, time::Duration};

extern crate mysql_driver as mysql;

#[path = "../../../tests/support/postgres_recursive_capture.rs"]
mod live_recursive_capture;
#[path = "../../../tests/support/mysql_contract.rs"]
mod mysql_contract;
#[path = "../../../tests/support/postgres_env.rs"]
mod postgres_env;
#[path = "../../../tests/support/type_qualification_evidence.rs"]
mod type_qualification_evidence;

#[test]
fn live_capture_fixtures_cover_every_static_postgresql_type_declaration() {
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../../../scripts/type-inventory.json"))
            .expect("native type inventory is valid JSON");
    let fixture_declarations = BUILTINS
        .iter()
        .map(|case| case.declaration)
        .collect::<std::collections::BTreeSet<_>>();

    for entry in inventory["types"].as_array().expect("type list") {
        let profile_id = entry["declaration_profile"]
            .as_str()
            .expect("declaration profile id");
        let profile = &inventory["native_declaration_profiles"][profile_id];
        let is_postgresql_type = profile["connectors"]
            .as_array()
            .expect("profile connectors")
            .iter()
            .any(|connector| {
                connector
                    .as_str()
                    .is_some_and(|id| id.starts_with("postgresql_"))
            });
        if !is_postgresql_type {
            continue;
        }

        let type_id = entry["id"].as_str().expect("native type id");
        let examples = profile["examples"].as_array().expect("type examples");
        assert!(
            examples.iter().any(|example| {
                example
                    .as_str()
                    .is_some_and(|declaration| fixture_declarations.contains(declaration))
            }),
            "PostgreSQL type {type_id} has no declaration in the live pgoutput fixture"
        );
    }
}

fn dynamic_type_class_storage_evidence(class_id: &str) -> (&'static str, &'static str) {
    if class_id == "postgresql.other_defined_catalog_types" {
        (
            "SOURCE_REPRESENTATION_PRESERVED",
            "source_representation_blob_carrier",
        )
    } else {
        ("VALUE_PRESERVED", "logical_value_json_carrier")
    }
}

#[test]
fn dynamic_catalog_fallback_evidence_is_not_mislabeled_as_logical_value() {
    assert_eq!(
        dynamic_type_class_storage_evidence("postgresql.other_defined_catalog_types"),
        (
            "SOURCE_REPRESENTATION_PRESERVED",
            "source_representation_blob_carrier"
        )
    );
    assert_eq!(
        dynamic_type_class_storage_evidence("postgresql.arrays"),
        ("VALUE_PRESERVED", "logical_value_json_carrier")
    );
}

fn postgres_representation_type_ids(
    connector_id: &str,
    image: &[change_event::ColumnDatum],
    fields: &[(String, String)],
) -> Vec<String> {
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../../../scripts/type-inventory.json"))
            .expect("native type inventory is valid JSON");
    let declarations = fields
        .iter()
        .map(|(name, declaration)| {
            (
                declaration
                    .to_ascii_lowercase()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" "),
                name.as_str(),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    let envelope_columns = image
        .iter()
        .filter(|column| matches!(&column.datum, Datum::SourceRepresentationEnvelope(_)))
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    inventory["types"]
        .as_array()
        .expect("type list")
        .iter()
        .filter_map(|entry| {
            let profile =
                &inventory["native_declaration_profiles"][entry["declaration_profile"].as_str()?];
            let belongs_to_connector = profile["connectors"]
                .as_array()?
                .iter()
                .any(|id| id.as_str() == Some(connector_id));
            let has_envelope_value = profile["examples"].as_array()?.iter().any(|example| {
                example
                    .as_str()
                    .and_then(|declaration| {
                        let normalized = declaration
                            .to_ascii_lowercase()
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ");
                        declarations.get(&normalized)
                    })
                    .is_some_and(|name| envelope_columns.contains(name))
            });
            (belongs_to_connector && has_envelope_value)
                .then(|| entry["id"].as_str().map(str::to_owned))
                .flatten()
        })
        .collect()
}

fn mysql_build_identity(port: u16) -> Result<ServerBuildIdentity> {
    let mut conn = mysql_contract::connection(port, false);
    let (version, distribution): (String, String) = conn
        .query_first("SELECT VERSION(), @@version_comment")?
        .ok_or_else(|| std::io::Error::other("MySQL target returned no build identity"))?;
    Ok(ServerBuildIdentity::new(
        "mysql",
        if distribution.trim().is_empty() {
            "oracle"
        } else {
            distribution.trim()
        },
        version.clone(),
        format!("mysql-{version}"),
    ))
}

fn mysql_carrier_plan(
    source_mapping: SourceTypeMapping,
    source_connector: &ConnectorIdentity,
    source_build: &ServerBuildIdentity,
    manifest: &change_event::TargetCapabilityManifest,
    source_schema: &str,
    table: &str,
    column: &change_event::ColumnDatum,
) -> Result<ColumnConversionPlan> {
    let is_key = column.name == "id";
    let conversion_kind = if is_key {
        None
    } else if matches!(
        source_mapping.logical_type,
        change_event::LogicalType::Raw { .. }
    ) {
        Some("source_representation")
    } else {
        Some("logical_value_json")
    };
    if conversion_kind == Some("source_representation") {
        assert!(
            source_mapping.source_representation_evidence.is_some(),
            "opaque PostgreSQL type {} must have source representation evidence",
            column.native_type
        );
    }
    let (target_native, target_logical) = match conversion_kind {
        Some("logical_value_json") => ("json", change_event::LogicalType::json()),
        Some("source_representation") => (
            "longblob",
            change_event::LogicalType::binary(Some(4_294_967_295)),
        ),
        None => ("bigint", change_event::LogicalType::integer(true, 64)),
        _ => unreachable!(),
    };
    let source_lineage = format!("catalog:{source_schema}.{table}.{}", column.name);
    let target_lineage = format!("target:CDC_test.{table}.{}", column.name);
    let source_fingerprint = source_mapping
        .source_definition_fingerprint
        .clone()
        .or_else(|| source_mapping.evidence_digest.clone())
        .unwrap_or_else(|| {
            change_event::stable_digest(&(
                &source_mapping.native_type,
                &source_mapping.mapping_id,
                &source_mapping.mapping_version,
            ))
        });
    let target_fingerprint =
        change_event::stable_digest(&(&column.name, target_native, is_key, !is_key));
    let source_key_ordinal = column.primary_key_ordinal;
    let source_field = FieldDefinition {
        reference: DefinitionReference::new(source_lineage, source_fingerprint),
        ordinal: column.ordinal,
        name: column.name.clone(),
        native_type: column.native_type.clone(),
        logical_type: source_mapping.logical_type.clone(),
        nullable: !is_key,
        collation: column.collation.clone(),
        generated: column.generated,
        primary_key_ordinal: source_key_ordinal,
        unique: is_key,
        row_locator: is_key,
    };
    let target_field = FieldDefinition {
        reference: DefinitionReference::new(target_lineage, target_fingerprint),
        ordinal: column.ordinal,
        name: column.name.clone(),
        native_type: target_native.into(),
        logical_type: target_logical,
        nullable: !is_key,
        collation: None,
        generated: false,
        primary_key_ordinal: source_key_ordinal,
        unique: is_key,
        row_locator: is_key,
    };
    let selected_rule = conversion_kind.map(|kind| {
        let capability = manifest
            .capabilities
            .iter()
            .find(|capability| {
                capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some(kind)
            })
            .unwrap_or_else(|| panic!("{} lacks the {kind} carrier", manifest.connector.version));
        change_event::RuleReference {
            id: capability.rule.id.clone(),
            version: capability.rule.version.clone(),
        }
    });
    let presences = match conversion_kind {
        Some("source_representation") => vec![
            PresenceState::SourceRepresentation,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        Some("logical_value_json") => vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        None => vec![PresenceState::Value, PresenceState::Unchanged],
        _ => unreachable!(),
    };
    let options = |confirmations| RouteOptions {
        route_id: format!(
            "{}-to-{}-all-types-live",
            source_connector.version, manifest.connector.version
        ),
        configuration_revision: "live-all-types:r1".into(),
        selected_rule: selected_rule.clone(),
        confirmations,
        ..RouteOptions::default()
    };
    let make_input = |confirmations| FieldCompatibilityInput {
        source_field: source_field.clone(),
        target_field: target_field.clone(),
        source_type_mapping: source_mapping.clone(),
        source_connector: source_connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(source_build.clone()),
        target_build: Some(manifest.target_build.clone()),
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: presences.clone(),
        source_has_primary_key: true,
        options: options(confirmations),
    };
    let initial = change_event::plan_field_compatibility(make_input(Vec::new()))?;
    let initial_plan = initial.plan.clone().ok_or_else(|| {
        std::io::Error::other(format!(
            "could not plan PostgreSQL {} column {} for MySQL {}: {initial:?}",
            column.native_type, column.name, manifest.connector.version
        ))
    })?;
    if initial_plan.confirmation == PlanConfirmationState::Required {
        let confirmation = RiskConfirmation {
            source_field_lineage: initial_plan.source_field.lineage_id.clone(),
            target_field_lineage: initial_plan.target_field.lineage_id.clone(),
            rule: initial_plan.rule.clone(),
            plan_digest: initial_plan.plan_digest.clone(),
            actor: "live-type-qualification".into(),
            confirmed_at: "2026-09-27T00:00:00Z".into(),
            reason: Some("qualify the explicit lossless tagged carrier representation".into()),
        };
        let mut input = make_input(Vec::new());
        input.options.confirmations.push(confirmation);
        let confirmed = change_event::plan_field_compatibility(input)?;
        let plan = confirmed.plan.clone().ok_or_else(|| {
            std::io::Error::other(format!(
                "PostgreSQL {} carrier confirmation failed for MySQL {}: {confirmed:?}",
                column.native_type, manifest.connector.version
            ))
        })?;
        if confirmed.status != change_event::CompatibilityStatus::Compatible {
            return Err(std::io::Error::other(format!(
                "PostgreSQL {} carrier is not compatible with MySQL {}: {confirmed:?}",
                column.native_type, manifest.connector.version
            ))
            .into());
        }
        Ok(plan)
    } else if initial.status == change_event::CompatibilityStatus::Compatible {
        Ok(initial_plan)
    } else {
        Err(std::io::Error::other(format!(
            "PostgreSQL {} carrier is not compatible with MySQL {}: {initial:?}",
            column.native_type, manifest.connector.version
        ))
        .into())
    }
}

fn assert_mysql_carrier_readback(
    conn: &mut mysql_driver::Conn,
    table: &str,
    image: &[change_event::ColumnDatum],
    plans: &[ColumnConversionPlan],
) -> Result<()> {
    let payload_columns = image
        .iter()
        .filter(|column| column.name != "id")
        .collect::<Vec<_>>();
    let projection = payload_columns
        .iter()
        .map(|column| {
            let plan = plans
                .iter()
                .find(|plan| {
                    plan.source_field.lineage_id.rsplit('.').next() == Some(column.name.as_str())
                })
                .expect("column has a plan");
            let name = column.name.replace('`', "``");
            if plan
                .target
                .parameters
                .get("conversion_kind")
                .map(String::as_str)
                == Some("logical_value_json")
            {
                format!("CAST(`{name}` AS CHAR)")
            } else {
                format!("`{name}`")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {projection} FROM CDC_test.`{}` WHERE id=1",
        table.replace('`', "``")
    );
    let row: mysql_driver::Row = conn
        .query_first(query)?
        .ok_or_else(|| std::io::Error::other("expected MySQL carrier row after DML"))?;
    for (index, column) in payload_columns.iter().enumerate() {
        let stored: Option<Vec<u8>> = row
            .get::<Option<Vec<u8>>, usize>(index)
            .ok_or_else(|| std::io::Error::other("MySQL carrier readback column is missing"))?;
        match (&column.datum, stored) {
            (Datum::Null, None) => {}
            (Datum::Null, Some(_)) => {
                return Err(std::io::Error::other(format!(
                    "MySQL carrier {} should preserve SQL NULL",
                    column.name
                ))
                .into());
            }
            (Datum::Unchanged, _) => {}
            (Datum::Value(expected), Some(bytes)) => {
                let actual: LogicalValue = serde_json::from_slice(&bytes).map_err(|error| {
                    std::io::Error::other(format!(
                        "decode MySQL LogicalValue carrier {}: {error}",
                        column.name
                    ))
                })?;
                if &actual != expected {
                    return Err(std::io::Error::other(format!(
                        "MySQL LogicalValue carrier differs for {}: expected {expected:?}, got {actual:?}",
                        column.name
                    ))
                    .into());
                }
            }
            (Datum::SourceRepresentationEnvelope(expected), Some(bytes)) => {
                let actual: change_event::SourceRepresentationEnvelope =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        std::io::Error::other(format!(
                            "decode MySQL source representation {}: {error}",
                            column.name
                        ))
                    })?;
                actual.validate()?;
                if &actual != expected {
                    return Err(std::io::Error::other(format!(
                        "MySQL source representation envelope differs for {}",
                        column.name
                    ))
                    .into());
                }
            }
            (Datum::Unavailable, _) => {
                return Err(std::io::Error::other(format!(
                    "MySQL carrier fixture unexpectedly has unavailable field {}",
                    column.name
                ))
                .into());
            }
            (_, None) => {
                return Err(std::io::Error::other(format!(
                    "MySQL carrier {} unexpectedly read back SQL NULL",
                    column.name
                ))
                .into());
            }
        }
    }
    Ok(())
}

fn apply_postgresql_builtin_transactions_to_mysql(
    source_major: u16,
    table: &str,
    catalog: &postgresql_15::SourceTypeCatalog,
    source_build: &ServerBuildIdentity,
    transactions: &[change_event::ValidatedTransaction],
    fields: &[(String, String)],
) -> Result<()> {
    let source_connector = ConnectorIdentity::new("postgresql", source_major.to_string());
    let connector_id = format!("postgresql_{source_major}");
    let declarations = fields
        .iter()
        .map(|(_, declaration)| declaration.clone())
        .chain(std::iter::once("bigint".into()))
        .collect::<Vec<_>>();
    let all_type_ids =
        type_qualification_evidence::source_native_type_ids(&connector_id, declarations);
    if all_type_ids.is_empty() {
        return Err(std::io::Error::other(format!(
            "no PostgreSQL {source_major} inventory types matched the live carrier fixture"
        ))
        .into());
    }
    let insert_image = transactions[0].transaction().changes[0]
        .after
        .as_ref()
        .expect("captured INSERT after image");
    let other_catalog_type_captured = insert_image.iter().any(|column| {
        column.name == "catalog_internal"
            && matches!(
                &column.datum,
                change_event::Datum::SourceRepresentationEnvelope(_)
            )
    });
    let source_schema = transactions[0].transaction().changes[0].schema.as_str();

    macro_rules! apply_to_sink {
        ($sink:ident, $port_key:literal, $default_port:literal, $sink_id:literal, $version:literal) => {{
            let port = mysql_contract::port($port_key, $default_port);
            let target_build = mysql_build_identity(port)?;
            if !target_build.version.starts_with($version) {
                return Err(std::io::Error::other(format!(
                    "configured MySQL target {} reports unexpected build {}",
                    $sink_id, target_build.version
                ))
                .into());
            }
            let manifest = $sink::compatibility_manifest(target_build.clone());
            let mut target = mysql_contract::connection(port, false);
            let definitions = insert_image
                .iter()
                .filter(|column| column.name != "id")
                .map(|column| {
                    let mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
                        &source_major.to_string(),
                        &column.native_type,
                        catalog,
                    )?;
                    let native = if matches!(
                        mapping.logical_type,
                        change_event::LogicalType::Raw { .. }
                    ) {
                        "LONGBLOB"
                    } else {
                        "JSON"
                    };
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        format!("`{}` {native} NULL", column.name.replace('`', "``")),
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .join(",");
            target.query_drop(format!(
                "CREATE TABLE CDC_test.`{}` (`id` BIGINT NOT NULL PRIMARY KEY,{definitions}) ENGINE=InnoDB",
                table.replace('`', "``")
            ))?;
            let plans = insert_image
                .iter()
                .map(|column| {
                    let mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
                        &source_major.to_string(),
                        &column.native_type,
                        catalog,
                    )?;
                    mysql_carrier_plan(
                        mapping,
                        &source_connector,
                        source_build,
                        &manifest,
                        source_schema,
                        table,
                        column,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let host = mysql_contract::setting("CDC_MYSQL_HOST", "192.168.0.10");
            let mut config = $sink::TargetConfig::new(
                host,
                mysql_contract::setting("CDC_MYSQL_WRITER_USER", "mysql_writer"),
                mysql_contract::password("WRITER"),
            );
            config.port = port;
            for transaction in transactions {
                let planned = $sink::sql_with_plans(transaction, &plans)?;
                $sink::execute(&config, &planned)?;
                match transaction.transaction().changes[0].operation {
                    Operation::Insert | Operation::Update => {
                        let after = transaction.transaction().changes[0]
                            .after.as_deref().expect("DML after image");
                        assert_mysql_carrier_readback(&mut target, table, after, &plans)?;
                    }
                    Operation::Delete => {
                        let count: Option<u64> = target.query_first(format!(
                            "SELECT COUNT(*) FROM CDC_test.`{}`",
                            table.replace('`', "``")
                        ))?;
                        if count != Some(0) {
                            return Err(std::io::Error::other(format!(
                                "{} did not apply PostgreSQL DELETE",
                                $sink_id
                            ))
                            .into());
                        }
                    }
                }
            }
            let representation_declarations = insert_image
                .iter()
                .filter(|column| {
                    column.name != "id"
                        && matches!(
                            postgresql_15::source_type_mapping_with_catalog_for_version(
                                &source_major.to_string(),
                                &column.native_type,
                                catalog,
                            )
                            .map(|mapping| mapping.logical_type),
                            Ok(change_event::LogicalType::Raw { .. })
                        )
                })
                .filter_map(|column| {
                    fields
                        .iter()
                        .find(|(name, _)| name == &column.name)
                        .map(|(_, declaration)| declaration.clone())
                })
                .collect::<Vec<_>>();
            let representation_ids = type_qualification_evidence::source_native_type_ids(
                &connector_id,
                representation_declarations,
            );
            let representation_set = representation_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            let semantic_ids = all_type_ids
                .iter()
                .filter(|type_id| !representation_set.contains(*type_id))
                .cloned()
                .collect::<Vec<_>>();
            if !semantic_ids.is_empty() {
                type_qualification_evidence::record_sink_type_evidence(
                    &connector_id,
                    $sink_id,
                    &format!("{connector_id}.all_builtin_types_to_{}", $sink_id),
                    semantic_ids,
                    "VALUE_PRESERVED",
                    "logical_value_json_carrier",
                )?;
            }
            if !representation_ids.is_empty() {
                type_qualification_evidence::record_sink_type_evidence(
                    &connector_id,
                    $sink_id,
                    &format!("{connector_id}.all_builtin_types_to_{}_representation", $sink_id),
                    representation_ids,
                    "SOURCE_REPRESENTATION_PRESERVED",
                    "source_representation_blob_carrier",
                )?;
            }
            if other_catalog_type_captured {
                type_qualification_evidence::record_dynamic_type_class_sink_evidence(
                    &connector_id,
                    $sink_id,
                    &format!("{connector_id}.pg_catalog_other_type_to_{}", $sink_id),
                    ["postgresql.other_defined_catalog_types".to_owned()],
                    "SOURCE_REPRESENTATION_PRESERVED",
                    "source_representation_blob_carrier",
                )?;
            }
            target.query_drop(format!(
                "DROP TABLE CDC_test.`{}`",
                table.replace('`', "``")
            ))?;
        }};
    }

    // Qualify the full PostgreSQL-native type surface against every concrete
    // MySQL sink version. Each sink executes the captured source transactions
    // and verifies the tagged value or source-representation bytes on readback.
    apply_to_sink!(mysql_5_7, "CDC_MYSQL57_PORT", 33061, "mysql_5_7", "5.7");
    apply_to_sink!(mysql_8_0, "CDC_MYSQL80_PORT", 33062, "mysql_8_0", "8.0");
    apply_to_sink!(mysql_8_4, "CDC_MYSQL84_PORT", 33063, "mysql_8_4", "8.4");
    Ok(())
}

async fn apply_postgresql_builtin_transactions_to_postgresql(
    source_major: u16,
    catalog: &postgresql_15::SourceTypeCatalog,
    source_build: &ServerBuildIdentity,
    transactions: &[change_event::ValidatedTransaction],
    fields: &[(String, String)],
) -> Result<()> {
    let connector_id = format!("postgresql_{source_major}");
    let source_connector = ConnectorIdentity::new("postgresql", source_major.to_string());
    let insert_image = transactions[0].transaction().changes[0]
        .after
        .as_deref()
        .expect("captured PostgreSQL INSERT image");
    let other_catalog_type_captured = insert_image.iter().any(|column| {
        column.name == "catalog_internal"
            && matches!(
                &column.datum,
                change_event::Datum::SourceRepresentationEnvelope(_)
            )
    });
    let source_change = &transactions[0].transaction().changes[0];
    let source_schema = source_change.schema.as_str();
    let declarations = fields
        .iter()
        .map(|(_, declaration)| declaration.clone())
        .chain(std::iter::once("bigint".into()))
        .collect::<Vec<_>>();
    let all_type_ids =
        type_qualification_evidence::source_native_type_ids(&connector_id, declarations);
    let representation_type_ids =
        postgres_representation_type_ids(&connector_id, insert_image, fields);
    let representation_set = representation_type_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let semantic_type_ids = all_type_ids
        .iter()
        .filter(|type_id| !representation_set.contains(*type_id))
        .cloned()
        .collect::<Vec<_>>();

    for target_version in ["15", "16", "17"] {
        let password = env::var(postgres_env::env_name(target_version, "TEST_PASSWORD"))
            .expect("set the PostgreSQL live qualification test password");
        let host = postgres_env::setting(target_version, "HOST", "192.168.0.10");
        let port: u16 = postgres_env::setting(
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
        .expect("valid PostgreSQL target port");
        let admin_user = postgres_env::setting(target_version, "ADMIN_USER", "postgres");
        let writer_user = postgres_env::setting(target_version, "WRITER_USER", "postgresql_writer");
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let target_table = format!(
            "cdc_pg_sink_{}_{}_{}_{}",
            source_major,
            target_version,
            std::process::id(),
            unique
        );
        let quoted_table = quote_pg_identifier(&target_table);
        let mut admin = PgConnection::connect_with(&postgres_env::options(
            target_version,
            &admin_user,
            &password,
        ))
        .await?;
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&mut admin)
            .await?;
        if server_version.split('.').next() != Some(target_version) {
            return Err(std::io::Error::other(format!(
                "PostgreSQL sink {target_version} endpoint returned {server_version}"
            ))
            .into());
        }
        let build_text: String = sqlx::query_scalar("SELECT version()")
            .fetch_one(&mut admin)
            .await?;
        let target_build =
            ServerBuildIdentity::new("postgresql", "community", server_version, build_text);
        let full_manifest =
            postgresql_15::compatibility_manifest_for_version(target_build.clone(), target_version);
        let manifest = postgresql_carrier_manifest(&full_manifest);

        let mut definitions = Vec::with_capacity(insert_image.len());
        let mut plans = Vec::with_capacity(insert_image.len());
        for column in insert_image {
            let source_mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
                &source_major.to_string(),
                &column.native_type,
                catalog,
            )?;
            let is_key = column.primary_key_ordinal.is_some();
            let is_raw = matches!(
                source_mapping.logical_type,
                change_event::LogicalType::Raw { .. }
            );
            let conversion_kind = if is_key {
                None
            } else if is_raw {
                Some("source_representation")
            } else {
                Some("logical_value_json")
            };
            if conversion_kind == Some("source_representation")
                && source_mapping.source_representation_evidence.is_none()
            {
                return Err(std::io::Error::other(format!(
                    "PostgreSQL {} lacks source-representation evidence",
                    column.native_type
                ))
                .into());
            }
            let (target_native, target_logical) = match conversion_kind {
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
                quote_pg_identifier(&column.name),
                target_native.to_ascii_uppercase(),
                if is_key { " NOT NULL PRIMARY KEY" } else { "" }
            ));
            let source_fingerprint = source_mapping
                .source_definition_fingerprint
                .clone()
                .or_else(|| source_mapping.evidence_digest.clone())
                .unwrap_or_else(|| {
                    change_event::stable_digest(&(
                        &source_mapping.native_type,
                        &source_mapping.mapping_id,
                        &source_mapping.mapping_version,
                    ))
                });
            let source_field = FieldDefinition {
                reference: DefinitionReference::new(
                    format!("catalog:{source_schema}.{target_table}.{}", column.name),
                    source_fingerprint,
                ),
                ordinal: column.ordinal,
                name: column.name.clone(),
                native_type: column.native_type.clone(),
                logical_type: source_mapping.logical_type.clone(),
                nullable: !is_key,
                collation: column.collation.clone(),
                generated: column.generated,
                primary_key_ordinal: column.primary_key_ordinal,
                unique: is_key,
                row_locator: is_key,
            };
            let target_field = FieldDefinition {
                reference: DefinitionReference::new(
                    format!("target:CDC_test.{target_table}.{}", column.name),
                    change_event::stable_digest(&(&column.name, target_native, is_key)),
                ),
                ordinal: column.ordinal,
                name: column.name.clone(),
                native_type: target_native.into(),
                logical_type: target_logical,
                nullable: !is_key,
                collation: None,
                generated: false,
                primary_key_ordinal: column.primary_key_ordinal,
                unique: is_key,
                row_locator: is_key,
            };
            let selected_rule = conversion_kind.map(|kind| {
                let capability = manifest
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
                        panic!("PostgreSQL {target_version} lacks the {kind} carrier")
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
            let make_input = |confirmations| FieldCompatibilityInput {
                source_field: source_field.clone(),
                target_field: target_field.clone(),
                source_type_mapping: source_mapping.clone(),
                source_connector: source_connector.clone(),
                sink_connector: manifest.connector.clone(),
                source_build: Some(source_build.clone()),
                target_build: Some(target_build.clone()),
                manifest: &manifest,
                operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
                presences: presences.clone(),
                source_has_primary_key: true,
                options: RouteOptions {
                    route_id: format!(
                        "postgresql_{source_major}-to-postgresql_{target_version}-all-types"
                    ),
                    configuration_revision: "live-all-types:r1".into(),
                    selected_rule: selected_rule.clone(),
                    confirmations,
                    ..RouteOptions::default()
                },
            };
            let initial = change_event::plan_field_compatibility(make_input(Vec::new()))?;
            let mut plan = initial.plan.clone().ok_or_else(|| {
                    std::io::Error::other(format!(
                        "could not plan PostgreSQL {} column {} to PostgreSQL {target_version}: {initial:?}",
                        column.native_type, column.name
                    ))
                })?;
            if plan.confirmation == PlanConfirmationState::Required {
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
                let confirmed =
                    change_event::plan_field_compatibility(make_input(vec![confirmation]))?;
                if confirmed.status != change_event::CompatibilityStatus::Compatible {
                    return Err(std::io::Error::other(format!(
                            "PostgreSQL {} carrier is not compatible with PostgreSQL {target_version}: {confirmed:?}",
                            column.native_type
                        ))
                        .into());
                }
                plan = confirmed.plan.ok_or_else(|| {
                    std::io::Error::other("confirmed PostgreSQL carrier plan is missing")
                })?;
            } else if initial.status != change_event::CompatibilityStatus::Compatible {
                return Err(std::io::Error::other(format!(
                        "PostgreSQL {} carrier is not compatible with PostgreSQL {target_version}: {initial:?}",
                        column.native_type
                    ))
                    .into());
            }
            plans.push(plan);
        }

        sqlx::query("CREATE SCHEMA IF NOT EXISTS \"CDC_test\"")
            .execute(&mut admin)
            .await?;
        let create_sql = format!(
            "CREATE TABLE \"CDC_test\".{quoted_table} ({})",
            definitions.join(", ")
        );
        sqlx::query(sqlx::AssertSqlSafe(create_sql))
            .execute(&mut admin)
            .await?;
        let grant_schema = format!(
            "GRANT USAGE ON SCHEMA \"CDC_test\" TO {}",
            quote_pg_identifier(&writer_user)
        );
        sqlx::query(sqlx::AssertSqlSafe(grant_schema))
            .execute(&mut admin)
            .await?;
        let grant_table = format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON \"CDC_test\".{quoted_table} TO {}",
            quote_pg_identifier(&writer_user)
        );
        sqlx::query(sqlx::AssertSqlSafe(grant_table))
            .execute(&mut admin)
            .await?;
        let config = postgresql_15::TargetConfig::new(host, "CDC_test", writer_user, password)
            .with_port(port);
        for captured in transactions {
            let mut transaction = captured.transaction().clone();
            for change in &mut transaction.changes {
                change.table.clone_from(&target_table);
            }
            let transaction = change_event::validate(transaction)?;
            let planned = match target_version {
                "15" => postgresql_15::sql_with_plans(&transaction, &plans),
                "16" => postgresql_15::sql_with_plans_for_version("16", &transaction, &plans),
                "17" => postgresql_15::sql_with_plans_for_version("17", &transaction, &plans),
                _ => unreachable!(),
            }?;
            postgresql_15::execute_for_version(&config, &planned).await?;
            let change = &transaction.transaction().changes[0];
            if matches!(change.operation, Operation::Insert | Operation::Update) {
                let image = change.after.as_deref().ok_or_else(|| {
                    std::io::Error::other("captured PostgreSQL DML has no after image")
                })?;
                verify_postgresql_carrier_readback(
                    &mut admin,
                    &quoted_table,
                    image,
                    &plans,
                    target_version,
                )
                .await?;
            } else if matches!(change.operation, Operation::Delete) {
                let count_sql = format!("SELECT count(*) FROM \"CDC_test\".{quoted_table}");
                let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql))
                    .fetch_one(&mut admin)
                    .await?;
                if count != 0 {
                    return Err(std::io::Error::other(format!(
                        "PostgreSQL {target_version} did not apply captured DELETE"
                    ))
                    .into());
                }
            }
        }
        let drop_sql = format!("DROP TABLE \"CDC_test\".{quoted_table}");
        sqlx::query(sqlx::AssertSqlSafe(drop_sql))
            .execute(&mut admin)
            .await?;
        admin.close().await?;

        if !semantic_type_ids.is_empty() {
            type_qualification_evidence::record_sink_type_evidence(
                &connector_id,
                &format!("postgresql_{target_version}"),
                &format!("{connector_id}.all_builtin_types_to_postgresql_{target_version}"),
                semantic_type_ids.iter().cloned(),
                "VALUE_PRESERVED",
                "logical_value_json_carrier",
            )?;
        }
        if !representation_type_ids.is_empty() {
            type_qualification_evidence::record_sink_type_evidence(
                &connector_id,
                &format!("postgresql_{target_version}"),
                &format!(
                    "{connector_id}.all_builtin_types_to_postgresql_{target_version}_representation"
                ),
                representation_type_ids.iter().cloned(),
                "SOURCE_REPRESENTATION_PRESERVED",
                "source_representation_blob_carrier",
            )?;
        }
        if other_catalog_type_captured {
            type_qualification_evidence::record_dynamic_type_class_sink_evidence(
                &connector_id,
                &format!("postgresql_{target_version}"),
                &format!("{connector_id}.pg_catalog_other_type_to_postgresql_{target_version}"),
                ["postgresql.other_defined_catalog_types".to_owned()],
                "SOURCE_REPRESENTATION_PRESERVED",
                "source_representation_blob_carrier",
            )?;
        }
        println!(
            "PASS {connector_id} -> postgresql_{target_version}: all captured native values preserved by PostgreSQL Sink"
        );
    }
    Ok(())
}

async fn verify_postgresql_carrier_readback(
    admin: &mut PgConnection,
    quoted_table: &str,
    expected: &[change_event::ColumnDatum],
    plans: &[ColumnConversionPlan],
    target_version: &str,
) -> Result<()> {
    let key = expected
        .iter()
        .find(|column| column.primary_key_ordinal == Some(0))
        .ok_or_else(|| std::io::Error::other("carrier fixture row has no primary key"))?;
    let key_value = match &key.datum {
        Datum::Value(LogicalValue::Integer { value, .. }) => value
            .parse::<i64>()
            .map_err(|error| std::io::Error::other(format!("invalid test key: {error}")))?,
        other => {
            return Err(std::io::Error::other(format!(
                "carrier fixture key is not a signed integer: {other:?}"
            ))
            .into());
        }
    };
    let projection = expected
        .iter()
        .map(|column| quote_pg_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {projection} FROM \"CDC_test\".{quoted_table} WHERE {}=$1",
        quote_pg_identifier(&key.name)
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(query))
        .bind(key_value)
        .fetch_optional(&mut *admin)
        .await?
        .ok_or_else(|| {
            std::io::Error::other(format!(
                "PostgreSQL {target_version} carrier row missing after DML"
            ))
        })?;

    for (index, column) in expected.iter().enumerate() {
        if column.primary_key_ordinal.is_some() {
            let actual: i64 = row.try_get(index)?;
            if actual != key_value {
                return Err(std::io::Error::other(format!(
                    "PostgreSQL {target_version} key readback differs for {}",
                    column.name
                ))
                .into());
            }
            continue;
        }
        let plan = plans
            .iter()
            .find(|plan| {
                plan.source_field.lineage_id.rsplit('.').next() == Some(column.name.as_str())
            })
            .ok_or_else(|| {
                std::io::Error::other(format!("carrier plan missing for {}", column.name))
            })?;
        let kind = plan
            .target
            .parameters
            .get("conversion_kind")
            .map(String::as_str);
        match (kind, &column.datum) {
            (Some("logical_value_json"), Datum::Value(expected_value)) => {
                let actual: Option<String> = row.try_get(index)?;
                let actual = actual.ok_or_else(|| {
                    std::io::Error::other(format!(
                        "PostgreSQL {target_version} LogicalValue carrier {} is NULL",
                        column.name
                    ))
                })?;
                let actual: LogicalValue = serde_json::from_str(&actual)?;
                if &actual != expected_value {
                    return Err(std::io::Error::other(format!(
                        "PostgreSQL {target_version} LogicalValue readback differs for {}",
                        column.name
                    ))
                    .into());
                }
            }
            (Some("logical_value_json"), Datum::Null) => {
                let actual: Option<String> = row.try_get(index)?;
                if actual.is_some() {
                    return Err(std::io::Error::other(format!(
                        "PostgreSQL {target_version} did not preserve NULL for {}",
                        column.name
                    ))
                    .into());
                }
            }
            (
                Some("source_representation"),
                Datum::SourceRepresentationEnvelope(expected_envelope),
            ) => {
                let actual: Option<Vec<u8>> = row.try_get(index)?;
                let actual = actual.ok_or_else(|| {
                    std::io::Error::other(format!(
                        "PostgreSQL {target_version} source representation {} is NULL",
                        column.name
                    ))
                })?;
                let actual: change_event::SourceRepresentationEnvelope =
                    serde_json::from_slice(&actual)?;
                actual.validate()?;
                if actual != *expected_envelope {
                    return Err(std::io::Error::other(format!(
                        "PostgreSQL {target_version} source representation differs for {}",
                        column.name
                    ))
                    .into());
                }
            }
            (Some("source_representation"), Datum::Null) => {
                let actual: Option<Vec<u8>> = row.try_get(index)?;
                if actual.is_some() {
                    return Err(std::io::Error::other(format!(
                        "PostgreSQL {target_version} did not preserve NULL for {}",
                        column.name
                    ))
                    .into());
                }
            }
            (_, Datum::Unchanged) => {}
            (kind, datum) => {
                return Err(std::io::Error::other(format!(
                    "PostgreSQL {target_version} carrier {kind:?} cannot verify {} with {datum:?}",
                    column.name
                ))
                .into());
            }
        }
    }
    Ok(())
}

fn postgresql_carrier_manifest(
    full: &change_event::TargetCapabilityManifest,
) -> change_event::TargetCapabilityManifest {
    let signed_bigint = change_event::LogicalType::integer(true, 64);
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
            ) || (entry.source_logical_type == signed_bigint
                && entry.target.native_type.eq_ignore_ascii_case("bigint")
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

fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

async fn qualify_live_postgresql_catalog(
    admin: &mut PgConnection,
    version: &str,
    expected_major: u16,
) -> Result<(postgresql_15::SourceTypeCatalog, usize, usize)> {
    let catalog = postgresql_15::source_type_catalog(admin).await?;
    let excluded_pseudotype_array_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM pg_catalog.pg_type array_type
           JOIN pg_catalog.pg_type element_type
             ON element_type.oid=array_type.typelem
            AND element_type.typarray=array_type.oid
          WHERE array_type.typisdefined
            AND array_type.typtype='b'
            AND array_type.typelem<>0
            AND array_type.typsubscript='pg_catalog.array_subscript_handler'::regproc
            AND element_type.typtype='p'",
    )
    .fetch_one(&mut *admin)
    .await?;
    let mut mapped_catalog_types = 0_usize;
    let mut observed_pseudotype_array_count = 0_usize;
    for definition in &catalog.types {
        if matches!(
            &definition.kind,
            postgresql_15::SourceTypeDefinitionKind::Pseudo
        ) {
            continue;
        }
        if let postgresql_15::SourceTypeDefinitionKind::Array { element_oid, .. } = &definition.kind
            && catalog.types.iter().any(|element| {
                element.oid == *element_oid
                    && matches!(
                        &element.kind,
                        postgresql_15::SourceTypeDefinitionKind::Pseudo
                    )
            })
        {
            observed_pseudotype_array_count += 1;
            continue;
        }
        let native_type = format!(
            "{}.{}",
            quote_pg_identifier(&definition.schema),
            quote_pg_identifier(&definition.name)
        );
        let mapping = postgresql_15::source_type_mapping_with_catalog_for_version(
            version,
            &native_type,
            &catalog,
        )
        .map_err(|error| {
            std::io::Error::other(format!(
                "PostgreSQL {expected_major} catalog type {} (OID {}) has no source mapping: {error}",
                native_type, definition.oid
            ))
        })?;
        if matches!(mapping.logical_type, change_event::LogicalType::Raw { .. }) {
            assert!(
                mapping.source_representation_evidence.is_some(),
                "opaque catalog type {native_type} must carry definition-bound evidence"
            );
        }
        mapped_catalog_types += 1;
    }
    assert!(
        mapped_catalog_types > 0,
        "live PostgreSQL type catalog is empty"
    );
    assert_eq!(
        observed_pseudotype_array_count,
        usize::try_from(excluded_pseudotype_array_count)?,
        "catalog pseudotype array exclusions must match PostgreSQL's live catalog"
    );
    Ok((
        catalog,
        mapped_catalog_types,
        usize::try_from(excluded_pseudotype_array_count)?,
    ))
}

async fn capture_all_builtins(expected_major: u16) -> Result<()> {
    let version = expected_major.to_string();
    let password = env::var(postgres_env::env_name(&version, "TEST_PASSWORD"))?;
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = "CDC_test".to_owned();
    let table = format!("cdc_pg{expected_major}_builtin_{tag}");
    let publication = format!("cdc_pg{expected_major}_builtin_pub_{tag}");
    let slot = format!("cdc_pg{expected_major}_builtin_slot_{tag}");
    let admin_user = postgres_env::setting(&version, "ADMIN_USER", "postgres");
    let reader_user = postgres_env::setting(&version, "READER_USER", "postgresql_reader");
    let writer_user = postgres_env::setting(&version, "WRITER_USER", "postgresql_writer");
    let mut admin =
        PgConnection::connect_with(&postgres_env::options(&version, &admin_user, &password))
            .await?;

    let result = async {
        let array_rows = sqlx::query(
            "SELECT format_type(array_type.oid, -1) AS declaration, element_type.typname AS element_name
               FROM pg_catalog.pg_type element_type
               JOIN pg_catalog.pg_namespace namespace ON namespace.oid=element_type.typnamespace
               JOIN pg_catalog.pg_type array_type ON array_type.oid=element_type.typarray
              WHERE namespace.nspname='pg_catalog'
                AND element_type.typtype IN ('b','d','e','r','m')
                AND element_type.typisdefined
                AND array_type.typtype='b'
                AND array_type.typisdefined
                AND array_type.typelem=element_type.oid
              ORDER BY array_type.oid",
        )
        .fetch_all(&mut admin)
        .await?;
        let mut fields = BUILTINS
            .iter()
            .map(|case| (case.name.to_owned(), case.declaration.to_owned()))
            .collect::<Vec<_>>();
        for (index, row) in array_rows.iter().enumerate() {
            fields.push((
                format!("array_{index:03}"),
                row.try_get::<String, _>("declaration")?,
            ));
        }
        fields.push(("nullable_marker".into(), "text".into()));
        let definitions = fields
            .iter()
            .map(|(name, declaration)| format!("\"{name}\" {declaration}"))
            .collect::<Vec<_>>()
            .join(",");
        execute(&mut admin, "SET search_path='pg_catalog'".into()).await?;
        execute(
            &mut admin,
            format!("CREATE TABLE \"{schema}\".\"{table}\" (id bigint PRIMARY KEY,{definitions})"),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("create built-in fixture table: {error}")))?;
        execute(
            &mut admin,
            format!("ALTER TABLE \"{schema}\".\"{table}\" REPLICA IDENTITY FULL"),
        )
        .await?;
        execute(
            &mut admin,
            format!("CREATE PUBLICATION {publication} FOR TABLE \"{schema}\".\"{table}\""),
        )
        .await?;
        let (catalog, mapped_catalog_types, excluded_pseudotype_array_count) =
            qualify_live_postgresql_catalog(&mut admin, &version, expected_major).await?;

        let mut config = Config::new(
            postgres_env::setting(&version, "HOST", "192.168.0.10"),
            postgres_env::setting(&version, "PORT", "54321")
                .parse()
                .map_err(|error| format!("invalid PostgreSQL test port: {error}"))?,
            "CDC_test",
            reader_user.clone(),
            &password,
            &publication,
            &slot,
        );
        config.create_slot = true;
        let initial = postgresql_15::replication_for_version(config.clone(), expected_major).await?;
        let source_id = initial.source().id.clone();
        drop(initial);
        config.create_slot = false;
        config.expected_source_id = Some(source_id);

        let mut writer = PgConnection::connect_with(&postgres_env::options(
            &version,
            &writer_user,
            &password,
        ))
        .await?;
        for setting in [
            "SET client_encoding='UTF8'",
            "SET DateStyle='ISO, YMD'",
            "SET IntervalStyle='iso_8601'",
            "SET TimeZone='UTC'",
            "SET bytea_output='hex'",
            "SET extra_float_digits=3",
            "SET search_path='pg_catalog'",
        ] {
            execute(&mut writer, setting.into()).await?;
        }
        let mut values = vec!["1".to_owned()];
        values.extend(BUILTINS.iter().map(|case| case.expression.to_owned()));
        values.extend(array_rows.iter().map(|row| {
            format!(
                "ARRAY[NULL]::{}",
                row.try_get::<String, _>("declaration").expect("catalog declaration")
            )
        }));
        values.push("NULL".into());
        let columns = std::iter::once("id".to_owned())
            .chain(fields.iter().map(|(name, _)| format!("\"{name}\"")))
            .collect::<Vec<_>>()
            .join(",");
        let insert = format!(
            "INSERT INTO \"{schema}\".\"{table}\" ({columns}) VALUES ({})",
            values.join(",")
        );
        execute(&mut writer, insert).await?;

        let mut reader = PgConnection::connect_with(&postgres_env::options(
            &version,
            &reader_user,
            &password,
        ))
        .await?;
        for setting in [
            "SET client_encoding='UTF8'",
            "SET DateStyle='ISO, YMD'",
            "SET IntervalStyle='iso_8601'",
            "SET TimeZone='UTC'",
            "SET bytea_output='hex'",
            "SET extra_float_digits=3",
            "SET search_path='pg_catalog'",
        ] {
            execute(&mut reader, setting.into()).await?;
        }
        let projection = fields
            .iter()
            .map(|(name, _)| format!("\"{name}\"::text"))
            .collect::<Vec<_>>()
            .join(",");
        let expected_row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {projection} FROM \"{schema}\".\"{table}\" WHERE id=1"
        )))
        .fetch_one(&mut reader)
        .await?;
        let expected_text = (0..fields.len())
            .map(|index| expected_row.try_get::<Option<String>, _>(index))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        execute(
            &mut writer,
            format!("UPDATE \"{schema}\".\"{table}\" SET nullable_marker='now-present' WHERE id=1"),
        )
        .await?;
        execute(
            &mut writer,
            format!("DELETE FROM \"{schema}\".\"{table}\" WHERE id=1"),
        )
        .await?;

        let transactions = read_transactions(config, expected_major, 3).await?;
        let insert_tx = transactions[0].transaction();
        assert_eq!(insert_tx.changes.len(), 1);
        assert!(matches!(insert_tx.changes[0].operation, Operation::Insert));
        let image = insert_tx.changes[0].after.as_ref().expect("INSERT after image");
        let by_name = image.iter().map(|column| (column.name.as_str(), &column.datum)).collect::<std::collections::HashMap<_, _>>();
        let mut semantic_count = 0;
        let mut envelope_count = 0;
        for (index, (name, _)) in fields.iter().enumerate() {
            let datum = by_name.get(name.as_str()).expect("captured built-in column");
            if name == "nullable_marker" {
                assert!(matches!(datum, Datum::Null));
                continue;
            }
            match datum {
                Datum::SourceRepresentationEnvelope(envelope) => {
                    envelope_count += 1;
                    assert_eq!(envelope.raw_bytes()?, expected_text[index].as_deref().unwrap().as_bytes(), "{name}");
                    assert_eq!(envelope.context.source_cursor, insert_tx.changes[0].source_cursor, "{name} cursor");
                    assert!(envelope.payload_length > 0, "{name} payload length");
                    assert_eq!(envelope.context.type_metadata["session.search_path"], "pg_catalog");
                    assert!(!envelope.context.type_metadata["environment.lc_monetary"].is_empty());
                }
                Datum::Value(value) => {
                    semantic_count += 1;
                    assert!(!matches!(value, LogicalValue::Null), "{name}");
                }
                other => panic!("{name} did not carry its inserted value: {other:?}"),
            }
        }
        assert!(semantic_count > 0, "semantic codecs must remain active");
        assert!(envelope_count > 0, "unmapped built-ins must use the evidence envelope");
        assert!(matches!(
            by_name["nullable_marker"],
            Datum::Null
        ));

        assert!(matches!(transactions[1].transaction().changes[0].operation, Operation::Update));
        let update = &transactions[1].transaction().changes[0];
        let before = update.before.as_ref().unwrap();
        let after = update.after.as_ref().unwrap();
        assert!(matches!(before.iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Null));
        assert!(matches!(&after.iter().find(|column| column.name == "nullable_marker").unwrap().datum, Datum::Value(LogicalValue::Text { text: Some(value), .. }) if value == "now-present"));
        assert!(matches!(transactions[2].transaction().changes[0].operation, Operation::Delete));

        for transaction in &transactions {
            let encoded = change_event::json(transaction)?;
            let mut json_reader = change_event::JsonReader::new(std::io::Cursor::new(&encoded));
            let replay = json_reader.next_transaction()?.expect("JSON transaction");
            assert_eq!(change_event::json(&replay)?, encoded);
            json_reader.finish()?;
        }
        let source_build_row = sqlx::query(
            "SELECT current_setting('server_version') AS version, version() AS build",
        )
        .fetch_one(&mut admin)
        .await?;
        let source_build = ServerBuildIdentity::new(
            "postgresql",
            "community",
            source_build_row.try_get::<String, _>("version")?,
            source_build_row.try_get::<String, _>("build")?,
        );
        if env::var_os("CDC_TEST_POSTGRES_SINKS_ONLY").is_none() {
            apply_postgresql_builtin_transactions_to_mysql(
                expected_major,
                &table,
                &catalog,
                &source_build,
                &transactions,
                &fields,
            )?;
        }
        apply_postgresql_builtin_transactions_to_postgresql(
            expected_major,
            &catalog,
            &source_build,
            &transactions,
            &fields,
        )
        .await?;
        let connector_id = format!("postgresql_{expected_major}");
        let fixture_declarations = fields.iter().map(|(_, declaration)| declaration.clone());
        let type_ids = type_qualification_evidence::source_native_type_ids(
            &connector_id,
            fixture_declarations,
        );
        let representation_type_ids =
            postgres_representation_type_ids(&connector_id, image, &fields);
        type_qualification_evidence::record_source_type_evidence(
            &connector_id,
            &format!("{connector_id}.all_builtin_types_capture"),
            type_ids,
            representation_type_ids,
        )?;
        type_qualification_evidence::record_catalog_type_mapping_evidence(
            &connector_id,
            "postgresql.other_defined_catalog_types",
            &format!("{connector_id}.all_defined_catalog_type_mappings"),
            mapped_catalog_types,
            excluded_pseudotype_array_count,
            "all defined, non-toast pg_type rows except non-storable pseudotypes and arrays of pseudotypes",
            &catalog.evidence_digest(),
        )?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((fields.len() - 1, semantic_count, envelope_count, array_rows.len()))
    }
    .await;

    let _ = sqlx::query(
        "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1 AND NOT active",
    )
    .bind(&slot)
    .execute(&mut admin)
    .await;
    let _ = execute(
        &mut admin,
        format!("DROP PUBLICATION IF EXISTS {publication}"),
    )
    .await;
    let _ = execute(
        &mut admin,
        format!("DROP TABLE IF EXISTS \"{schema}\".\"{table}\""),
    )
    .await;
    let (type_count, semantic_count, envelope_count, array_count) = result?;
    println!(
        "PASS: PostgreSQL {expected_major} pgoutput captured {type_count} built-in types (including {array_count} catalog arrays): {semantic_count} semantic values, {envelope_count} evidence-bound exact text representations; INSERT/UPDATE/DELETE, NULL→value presence, and ChangeEvent JSON replay"
    );
    Ok(())
}

async fn read_transactions(
    config: Config,
    expected_major: u16,
    count: usize,
) -> Result<Vec<change_event::ValidatedTransaction>> {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let mut worker = tokio::spawn(async move {
        let mut capture = postgresql_15::replication_for_version(config, expected_major).await?;
        let mut transactions = Vec::with_capacity(count);
        for _ in 0..count {
            transactions.push(capture.next_transaction(&cancel).await?);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(transactions)
    });
    match tokio::time::timeout(Duration::from_secs(40), &mut worker).await {
        Ok(result) => result?,
        Err(error) => {
            cancellation.cancel();
            let _ = worker.await;
            Err(error.into())
        }
    }
}

async fn execute(conn: &mut PgConnection, sql: String) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(sql)).execute(conn).await?;
    Ok(())
}

macro_rules! live_test {
    ($name:ident, $version:literal) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires a configured PostgreSQL live test instance"]
        async fn $name() -> Result<()> {
            capture_all_builtins($version).await
        }
    };
}

live_test!(postgresql15_all_builtin_types_capture, 15);
live_test!(postgresql16_all_builtin_types_capture, 16);
live_test!(postgresql17_all_builtin_types_capture, 17);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured PostgreSQL 15/16/17 and MySQL 5.7/8.0/8.4 live test instances"]
async fn postgresql_dynamic_types_write_to_every_sink_version() -> Result<()> {
    let sink_ids = [
        "mysql_5_7",
        "mysql_8_0",
        "mysql_8_4",
        "postgresql_15",
        "postgresql_16",
        "postgresql_17",
    ];
    let mut completed_sources = Vec::new();
    for source_major in [15, 16, 17] {
        let captured = live_recursive_capture::capture_recursive_types(source_major).await?;
        apply_postgresql_builtin_transactions_to_mysql(
            source_major,
            &captured.table_name,
            &captured.catalog,
            &captured.source_build,
            &captured.transactions,
            &captured.fields,
        )?;
        apply_postgresql_builtin_transactions_to_postgresql(
            source_major,
            &captured.catalog,
            &captured.source_build,
            &captured.transactions,
            &captured.fields,
        )
        .await?;
        completed_sources.push((source_major, captured.dynamic_classes));
    }

    // Emit dynamic-class receipts only after every source class has completed
    // the full six-sink INSERT/UPDATE/DELETE and per-field readback matrix.
    for (source_major, classes) in completed_sources {
        let source_id = format!("postgresql_{source_major}");
        for sink_id in sink_ids {
            for class_id in &classes {
                let suite_id = format!("{source_id}.recursive_types_to_{sink_id}");
                let (outcome, storage_mode) = dynamic_type_class_storage_evidence(class_id);
                if outcome == "SOURCE_REPRESENTATION_PRESERVED" {
                    type_qualification_evidence::record_dynamic_type_class_sink_evidence(
                        &source_id,
                        sink_id,
                        &suite_id,
                        [class_id.clone()],
                        outcome,
                        storage_mode,
                    )?;
                } else {
                    type_qualification_evidence::record_sink_type_evidence(
                        &source_id,
                        sink_id,
                        &suite_id,
                        [format!("dynamic:{class_id}")],
                        outcome,
                        storage_mode,
                    )?;
                }
            }
        }
        println!(
            "PASS {source_id}: recursive catalog-defined types applied and read back by all six sink versions"
        );
    }
    Ok(())
}
