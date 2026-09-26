use change_event::{
    ChangeTransaction, ColumnConversionPlan, ColumnDatum, ConnectorIdentity, Datum,
    DefinitionReference, FieldCompatibilityInput, FieldDefinition, LogicalType, LogicalValue,
    Operation, PlanConfirmationState, PresenceState, RiskConfirmation, RouteOptions, RowChange,
    ServerBuildIdentity, Source, SourceCursor, SourceRepresentationContext,
    SourceRepresentationEnvelope, SourceRepresentationFormat, SourceRepresentationTypeEvidence,
    SourceTypeMapping,
};
use postgresql_15::{CheckpointWriter, TargetConfig};
use sqlx::{Connection, PgConnection};
use std::{
    collections::BTreeMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};

#[path = "support/logical_values.rs"]
mod logical_values;
#[path = "support/postgres_env.rs"]
mod postgres_env;

const SOURCE_ID: &str = "postgresql:123456:1:16384:Q0RDX3Rlc3Q";

fn source_build() -> ServerBuildIdentity {
    ServerBuildIdentity::new("postgresql", "community", "15.19", "PostgreSQL 15.19")
}

fn target_build(version: &str) -> ServerBuildIdentity {
    let full = match version {
        "15" => "15.19",
        "16" => "16.10",
        "17" => "17.6",
        other => panic!("unexpected PostgreSQL target version {other}"),
    };
    ServerBuildIdentity::new(
        "postgresql",
        "community",
        full,
        format!("PostgreSQL {full}"),
    )
}

fn manifest(version: &str) -> change_event::TargetCapabilityManifest {
    carrier_manifest(&postgresql_15::compatibility_manifest_for_version(
        target_build(version),
        version,
    ))
}

fn carrier_manifest(
    full: &change_event::TargetCapabilityManifest,
) -> change_event::TargetCapabilityManifest {
    let integer = LogicalType::integer(true, 64);
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
            ) || (entry.source_logical_type == integer
                && entry.target.native_type.eq_ignore_ascii_case("bigint")
                && entry.rule.qualification == change_event::QualificationLevel::Exact
                && !entry.target.parameters.contains_key("conversion_kind"))
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

#[allow(clippy::too_many_arguments)]
fn carrier_plan(
    manifest: &change_event::TargetCapabilityManifest,
    table: &str,
    name: &str,
    source_native: &str,
    source_type: LogicalType,
    target_native: &str,
    target_type: LogicalType,
    key_ordinal: Option<usize>,
    conversion_kind: Option<&str>,
    presences: Vec<PresenceState>,
    source_representation: bool,
) -> ColumnConversionPlan {
    let source_connector = ConnectorIdentity::new("postgresql", "15");
    let source_ref = DefinitionReference::new(
        format!("catalog:CDC_test.{table}.{name}"),
        format!("source-{table}-{name}"),
    );
    let target_ref = DefinitionReference::new(
        format!("catalog:CDC_test.{table}.{name}"),
        format!("target-{table}-{name}"),
    );
    let source_field = FieldDefinition {
        reference: source_ref,
        ordinal: key_ordinal.unwrap_or(1),
        name: name.into(),
        native_type: source_native.into(),
        logical_type: source_type.clone(),
        nullable: key_ordinal.is_none(),
        collation: None,
        generated: false,
        primary_key_ordinal: key_ordinal,
        unique: false,
        row_locator: false,
    };
    let target_field = FieldDefinition {
        reference: target_ref,
        ordinal: key_ordinal.unwrap_or(1),
        name: name.into(),
        native_type: target_native.into(),
        logical_type: target_type,
        nullable: key_ordinal.is_none(),
        collation: None,
        generated: false,
        primary_key_ordinal: key_ordinal,
        unique: false,
        row_locator: false,
    };
    let mut mapping = SourceTypeMapping::new(
        source_connector.clone(),
        source_native,
        source_type,
        format!("fixture.{name}"),
        "fixture-v1",
    );
    if source_representation {
        let mut metadata = BTreeMap::new();
        metadata.insert("native_type".into(), "public.opaque_type".into());
        mapping.source_representation_evidence = Some(SourceRepresentationTypeEvidence {
            source_type_identity: "postgresql.pg_type.v1:90001:public.opaque_type".into(),
            protocol: "fixture.pgoutput.binary.v1".into(),
            format: SourceRepresentationFormat::Binary,
            type_metadata: metadata,
            allowed_context_metadata_keys: Default::default(),
        });
    }
    let selected_rule = conversion_kind.map(|kind| {
        let entry = manifest
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
                panic!(
                    "no {kind} capability in PostgreSQL {} manifest",
                    manifest.connector.version
                )
            });
        change_event::RuleReference {
            id: entry.rule.id.clone(),
            version: entry.rule.version.clone(),
        }
    });
    let make_input = |confirmations| FieldCompatibilityInput {
        source_field: source_field.clone(),
        target_field: target_field.clone(),
        source_type_mapping: mapping.clone(),
        source_connector: source_connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(source_build()),
        target_build: Some(manifest.target_build.clone()),
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: presences.clone(),
        source_has_primary_key: true,
        options: RouteOptions {
            route_id: format!("pg-representation-{}", manifest.connector.version),
            configuration_revision: "fixture-revision".into(),
            selected_rule: selected_rule.clone(),
            confirmations,
            ..RouteOptions::default()
        },
    };
    let initial = change_event::plan_field_compatibility(make_input(Vec::new()))
        .unwrap_or_else(|error| panic!("could not plan {name}: {error}"));
    let plan = initial.plan.expect("planned field");
    if plan.confirmation != PlanConfirmationState::Required {
        return plan;
    }
    let confirmation = RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "postgresql-representation-test".into(),
        confirmed_at: "2026-09-27T00:00:00Z".into(),
        reason: Some("explicit carrier qualification fixture".into()),
    };
    let confirmed = change_event::plan_field_compatibility(make_input(vec![confirmation]))
        .unwrap_or_else(|error| panic!("could not confirm {name}: {error}"));
    let plan = confirmed.plan.expect("confirmed field plan");
    assert_eq!(plan.confirmation, PlanConfirmationState::Confirmed);
    plan
}

fn cursor(lsn: &str) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: lsn.into(),
        display: lsn.into(),
    }
}

fn row(
    values: &[(&'static str, LogicalType, LogicalValue)],
    envelope: &SourceRepresentationEnvelope,
    id: i64,
) -> Vec<ColumnDatum> {
    let mut columns = vec![ColumnDatum {
        ordinal: 0,
        name: "id".into(),
        native_type: "bigint".into(),
        primary_key_ordinal: Some(0),
        generated: false,
        collation: None,
        datum: Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: id.to_string(),
        }),
    }];
    columns.extend(
        values
            .iter()
            .enumerate()
            .map(|(index, (name, _, value))| ColumnDatum {
                ordinal: index + 1,
                name: format!("payload_{index}_{name}"),
                native_type: format!("fixture_{name}"),
                primary_key_ordinal: None,
                generated: false,
                collation: None,
                datum: Datum::Value(value.clone()),
            }),
    );
    columns.push(ColumnDatum {
        ordinal: values.len() + 1,
        name: "raw_representation".into(),
        native_type: "public.opaque_type".into(),
        primary_key_ordinal: None,
        generated: false,
        collation: None,
        datum: Datum::SourceRepresentationEnvelope(envelope.clone()),
    });
    columns
}

fn envelope(at: &str) -> SourceRepresentationEnvelope {
    let mut metadata = BTreeMap::new();
    metadata.insert("native_type".into(), "public.opaque_type".into());
    SourceRepresentationEnvelope::new(
        SourceRepresentationContext {
            connector: ConnectorIdentity::new("postgresql", "15"),
            server_build: source_build(),
            source_type_identity: "postgresql.pg_type.v1:90001:public.opaque_type".into(),
            source_type_definition_digest: format!("sha256:{}", "a".repeat(64)),
            protocol: "fixture.pgoutput.binary.v1".into(),
            format: SourceRepresentationFormat::Binary,
            type_metadata: metadata,
            source_cursor: cursor(at),
        },
        "raw-bytes",
        &[0x00, 0xff, 0x80, 0x41, 0x00, 0xc3],
    )
}

fn transaction_for_table(
    table: &str,
    id: &str,
    at: &str,
    operation: Operation,
    before: Option<Vec<ColumnDatum>>,
    after: Option<Vec<ColumnDatum>>,
) -> change_event::ValidatedTransaction {
    change_event::validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15.19".into(),
            id: SOURCE_ID.into(),
        },
        id: format!("pg:fixture:{at}:{id}"),
        begin_cursor: cursor(at),
        commit_cursor: cursor(at),
        changes: vec![RowChange {
            database: Some("CDC_test".into()),
            schema: "CDC_test".into(),
            table: table.into(),
            operation,
            source_cursor: cursor(at),
            source_timestamp: 1_790_000_000,
            schema_basis: "qualified carrier fixture".into(),
            before,
            after,
        }],
    })
    .unwrap()
}

fn plans(
    version: &str,
    table: &str,
    values: &[(&'static str, LogicalType, LogicalValue)],
) -> Vec<ColumnConversionPlan> {
    plans_for_manifest(&manifest(version), table, values)
}

fn plans_for_manifest(
    full_manifest: &change_event::TargetCapabilityManifest,
    table: &str,
    values: &[(&'static str, LogicalType, LogicalValue)],
) -> Vec<ColumnConversionPlan> {
    let manifest = carrier_manifest(full_manifest);
    let integer = LogicalType::integer(true, 64);
    let mut plans = vec![carrier_plan(
        &manifest,
        table,
        "id",
        "bigint",
        integer.clone(),
        "bigint",
        integer,
        Some(0),
        None,
        vec![PresenceState::Value],
        false,
    )];
    for (index, (name, logical_type, _)) in values.iter().enumerate() {
        plans.push(carrier_plan(
            &manifest,
            table,
            &format!("payload_{index}_{name}"),
            &format!("fixture_{name}"),
            logical_type.clone(),
            "text",
            LogicalType::text("UTF8", None),
            None,
            Some("logical_value_json"),
            vec![
                PresenceState::Value,
                PresenceState::Null,
                PresenceState::Unchanged,
            ],
            false,
        ));
    }
    plans.push(carrier_plan(
        &manifest,
        table,
        "raw_representation",
        "public.opaque_type",
        LogicalType::raw(
            "fixture.pgoutput.source-representation.v1",
            "public.opaque_type",
            "a".repeat(64),
            "raw-bytes",
        ),
        "bytea",
        LogicalType::binary(Some(1_073_741_823)),
        None,
        Some("source_representation"),
        vec![
            PresenceState::SourceRepresentation,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        true,
    ));
    plans
}

#[test]
fn every_public_logical_value_and_binary_representation_is_plan_backed_for_postgresql_15_16_17() {
    let values = logical_values::logical_value_carrier_cases();
    assert_eq!(
        values.len(),
        34,
        "all logical value cases include bounded/unbounded decimals and NUL text"
    );
    let envelope = envelope("0/120");
    let insert = transaction_for_table(
        "carrier_fixture",
        "offline",
        "0/120",
        Operation::Insert,
        None,
        Some(row(&values, &envelope, 1)),
    );

    for version in ["15", "16", "17"] {
        let full =
            postgresql_15::compatibility_manifest_for_version(target_build(version), version);
        let logical_carrier = full
            .capabilities
            .iter()
            .find(|entry| {
                entry
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("logical_value_json")
            })
            .expect("versioned PostgreSQL TEXT LogicalValue carrier");
        assert_eq!(logical_carrier.target.native_type, "text");
        assert_eq!(
            logical_carrier
                .target
                .parameters
                .get("target_storage")
                .map(String::as_str),
            Some("postgresql_text_tagged_json")
        );
        assert_eq!(
            logical_carrier
                .target
                .parameters
                .get("target_capacity_bytes")
                .map(String::as_str),
            Some("1073741823")
        );
        let representation_carrier = full
            .capabilities
            .iter()
            .find(|entry| {
                entry
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("source_representation")
            })
            .expect("versioned PostgreSQL BYTEA Source Representation carrier");
        assert_eq!(representation_carrier.target.native_type, "bytea");
        assert!(!representation_carrier.rule.allows_key);
        let plans = plans(version, "carrier_fixture", &values);
        let sql = postgresql_15::sql_with_plans_for_version(version, &insert, &plans)
            .unwrap_or_else(|error| panic!("PostgreSQL {version} carrier plan failed: {error}"));
        let parameters = sql.parameters().next().expect("insert parameters");
        assert_eq!(parameters.len(), values.len() + 2);
        assert!(matches!(
            parameters[0],
            postgresql_15::Parameter::Integer(1)
        ));
        assert!(matches!(
            parameters.last(),
            Some(postgresql_15::Parameter::Binary(_))
        ));
        assert!(
            !sql.script().contains("00ff804100c3"),
            "diagnostics must omit source bytes"
        );
        for parameter in parameters.iter().skip(1).take(values.len()) {
            assert!(matches!(parameter, postgresql_15::Parameter::Text(_)));
        }
        let recovered: LogicalValue = match parameters.get(1).expect("first LogicalValue") {
            postgresql_15::Parameter::Text(text) => serde_json::from_str(text).unwrap(),
            other => panic!("unexpected PostgreSQL TEXT carrier parameter: {other:?}"),
        };
        assert_eq!(recovered, values[0].2);
        let nul_text = values
            .iter()
            .find(|(name, _, _)| *name == "text_with_nul")
            .expect("NUL text edge case");
        assert!(
            serde_json::to_string(&nul_text.2)
                .unwrap()
                .contains("\\u0000"),
            "JSON text encoding escapes NUL without storing it as a PostgreSQL TEXT NUL byte"
        );
    }
}

#[test]
fn postgresql_carriers_enforce_locator_and_storage_capacity_boundaries() {
    let values = logical_values::logical_value_carrier_cases();
    let envelope = envelope("0/120");
    let transaction = transaction_for_table(
        "carrier_fixture",
        "safety-boundaries",
        "0/120",
        Operation::Insert,
        None,
        Some(row(&values, &envelope, 1)),
    );
    let plans = plans("15", "carrier_fixture", &values);

    let failure_code = |plans: &[ColumnConversionPlan]| {
        let error = postgresql_15::sql_with_plans_for_version("15", &transaction, plans)
            .expect_err("unsafe carrier plan must be rejected");
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<change_event::TargetCapabilityFailure>())
            .map(|failure| failure.code.clone())
            .unwrap_or_else(|| panic!("expected structured TargetCapabilityFailure: {error}"))
    };

    let mut key_carrier = plans.clone();
    key_carrier[1].locator_impact = change_event::LocatorImpact::Preserved;
    key_carrier[1].plan_digest = key_carrier[1].computed_digest();
    assert_eq!(
        failure_code(&key_carrier),
        "target_capability.carrier_key_forbidden"
    );

    let mut undersized_json = plans.clone();
    undersized_json[1]
        .target
        .parameters
        .insert("target_capacity_bytes".into(), "1".into());
    undersized_json[1].plan_digest = undersized_json[1].computed_digest();
    assert_eq!(
        failure_code(&undersized_json),
        "target_capability.logical_value_carrier_capacity_exceeded"
    );

    let mut undersized_envelope = plans;
    let envelope_plan = undersized_envelope.last_mut().expect("SRE plan");
    envelope_plan
        .target
        .parameters
        .insert("target_capacity_bytes".into(), "1".into());
    envelope_plan.plan_digest = envelope_plan.computed_digest();
    assert_eq!(
        failure_code(&undersized_envelope),
        "target_capability.source_representation_capacity_exceeded"
    );
}

struct LiveCarrierTarget {
    runtime: tokio::runtime::Runtime,
    admin: PgConnection,
    table: String,
    task: String,
}

impl Drop for LiveCarrierTarget {
    fn drop(&mut self) {
        let table = format!("\"CDC_test\".\"{}\"", self.table);
        let task = self.task.clone();
        let runtime = &self.runtime;
        let admin = &mut self.admin;
        let result = runtime.block_on(async {
            let drop_result =
                sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
                    .execute(&mut *admin)
                    .await;
            let checkpoint_result = sqlx::query("DELETE FROM cdc.log_info WHERE task_id = $1")
                .bind(task)
                .execute(&mut *admin)
                .await;
            drop_result.and(checkpoint_result)
        });
        if let Err(error) = result {
            eprintln!("PostgreSQL representation test cleanup failed: {error}");
        }
    }
}

fn live_target(
    version: &str,
) -> Result<
    (
        LiveCarrierTarget,
        TargetConfig,
        String,
        change_event::TargetCapabilityManifest,
    ),
    Box<dyn std::error::Error>,
> {
    let password = env::var(postgres_env::env_name(version, "TEST_PASSWORD"))?;
    let host = postgres_env::setting(version, "HOST", "192.168.0.10");
    let port: u16 = postgres_env::setting(
        version,
        "PORT",
        match version {
            "15" => "54321",
            "16" => "54322",
            "17" => "54323",
            _ => unreachable!(),
        },
    )
    .parse()?;
    let writer = postgres_env::setting(version, "WRITER_USER", "postgresql_writer");
    let admin_user = postgres_env::setting(version, "ADMIN_USER", "postgres");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut admin = runtime.block_on(PgConnection::connect_with(&postgres_env::options(
        version,
        &admin_user,
        &password,
    )))?;
    let server_version: String =
        runtime.block_on(sqlx::query_scalar("SHOW server_version").fetch_one(&mut admin))?;
    if server_version.split('.').next() != Some(version) {
        return Err(format!("expected PostgreSQL {version}, connected to {server_version}").into());
    }
    let target_manifest = postgresql_15::compatibility_manifest_for_version(
        ServerBuildIdentity::new(
            "postgresql",
            "community",
            &server_version,
            format!("PostgreSQL {server_version}"),
        ),
        version,
    );
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("cdc_repr_{}_{}", std::process::id(), unique);
    let task = format!("test_pg_repr_{version}_{}_{}", std::process::id(), unique);
    let value_count = logical_values::logical_value_carrier_cases().len();
    let fields = (0..value_count)
        .map(|index| {
            format!(
                ", \"payload_{index}_{}\" text",
                logical_values::logical_value_carrier_cases()[index].0
            )
        })
        .collect::<String>();
    runtime.block_on(async {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS \"CDC_test\"").execute(&mut admin).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE TABLE \"CDC_test\".\"{table}\" (id bigint PRIMARY KEY, raw_representation bytea{fields})"))).execute(&mut admin).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!("GRANT USAGE ON SCHEMA \"CDC_test\" TO {}", quote_identifier(&writer)))).execute(&mut admin).await?;
        sqlx::query(sqlx::AssertSqlSafe(format!("GRANT SELECT, INSERT, UPDATE, DELETE ON \"CDC_test\".\"{table}\" TO {}", quote_identifier(&writer)))).execute(&mut admin).await?;
        Ok::<_, sqlx::Error>(())
    })?;
    let config = TargetConfig::new(host, "CDC_test", writer, password).with_port(port);
    Ok((
        LiveCarrierTarget {
            runtime,
            admin,
            table,
            task: task.clone(),
        },
        config,
        task,
        target_manifest,
    ))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn live_qualify_and_apply(version: &'static str) -> Result<(), Box<dyn std::error::Error>> {
    let (mut target, config, task, target_manifest) = live_target(version)?;
    let values = logical_values::logical_value_carrier_cases();
    let plans = plans_for_manifest(&target_manifest, &target.table, &values);
    let initial_envelope = envelope("0/40");
    let initial_row = row(&values, &initial_envelope, 1);
    let initial = transaction_for_table(
        &target.table,
        "insert",
        "0/40",
        Operation::Insert,
        None,
        Some(initial_row.clone()),
    );
    let duplicate = transaction_for_table(
        &target.table,
        "duplicate",
        "0/41",
        Operation::Insert,
        None,
        Some(row(&values, &envelope("0/41"), 1)),
    );
    let mut duplicate = duplicate.transaction().clone();
    duplicate.changes.push(duplicate.changes[0].clone());
    let duplicate = change_event::validate(duplicate)?;

    let mut writer =
        CheckpointWriter::open_for_version(&config, &task, SOURCE_ID, &"a".repeat(64), version)?;
    let start = writer.initialize("postgresql_lsn", "0/20", 32, None)?;
    let failed_plan = postgresql_15::sql_with_plans_for_version(version, &duplicate, &plans)?;
    assert!(
        writer.apply(&failed_plan).is_err(),
        "duplicate row must fail and roll back"
    );
    assert_eq!(
        writer.checkpoint(),
        Some(&start),
        "failed DML must not advance checkpoint"
    );
    let count: i64 = {
        let runtime = &target.runtime;
        let admin = &mut target.admin;
        runtime.block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM \"CDC_test\".\"{}\"",
                target.table
            )))
            .fetch_one(&mut *admin),
        )?
    };
    assert_eq!(count, 0, "all earlier carrier statements must roll back");

    let insert_plan = postgresql_15::sql_with_plans_for_version(version, &initial, &plans)?;
    let inserted = writer.apply(&insert_plan)?;
    assert!(inserted.checkpoint.position > start.position);
    for (index, (name, _, expected)) in values.iter().enumerate() {
        let column = format!("payload_{index}_{name}");
        let query = format!(
            "SELECT \"{column}\"::text FROM \"CDC_test\".\"{}\" WHERE id=1",
            target.table
        );
        let stored: String = {
            let runtime = &target.runtime;
            let admin = &mut target.admin;
            runtime
                .block_on(sqlx::query_scalar(sqlx::AssertSqlSafe(query)).fetch_one(&mut *admin))?
        };
        let actual: LogicalValue = serde_json::from_str(&stored)?;
        assert_eq!(
            &actual, expected,
            "PostgreSQL {version} read-back for {name}"
        );
    }
    let raw: Vec<u8> = {
        let runtime = &target.runtime;
        let admin = &mut target.admin;
        runtime.block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT raw_representation FROM \"CDC_test\".\"{}\" WHERE id=1",
                target.table
            )))
            .fetch_one(&mut *admin),
        )?
    };
    let stored_envelope: SourceRepresentationEnvelope = serde_json::from_slice(&raw)?;
    stored_envelope.validate()?;
    assert_eq!(
        stored_envelope.raw_bytes()?,
        &[0x00, 0xff, 0x80, 0x41, 0x00, 0xc3]
    );

    let before_update = row(&values, &envelope("0/50"), 1);
    let mut updated_row = before_update.clone();
    for column in updated_row.iter_mut().skip(1) {
        column.datum = Datum::Unchanged;
    }
    updated_row[1].datum = Datum::Value(LogicalValue::Boolean { value: false });
    let update = transaction_for_table(
        &target.table,
        "update",
        "0/50",
        Operation::Update,
        Some(before_update),
        Some(updated_row),
    );
    let update_plan = postgresql_15::sql_with_plans_for_version(version, &update, &plans)?;
    let updated = writer.apply(&update_plan)?;
    assert!(updated.checkpoint.position > inserted.checkpoint.position);

    let delete = transaction_for_table(
        &target.table,
        "delete",
        "0/60",
        Operation::Delete,
        Some(row(&values, &envelope("0/60"), 1)),
        None,
    );
    let delete_plan = postgresql_15::sql_with_plans_for_version(version, &delete, &plans)?;
    let deleted = writer.apply(&delete_plan)?;
    assert!(deleted.checkpoint.position > updated.checkpoint.position);
    drop(writer);
    let remaining: i64 = {
        let runtime = &target.runtime;
        let admin = &mut target.admin;
        runtime.block_on(
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM \"CDC_test\".\"{}\"",
                target.table
            )))
            .fetch_one(&mut *admin),
        )?
    };
    assert_eq!(remaining, 0);
    drop(target);
    Ok(())
}

#[test]
#[ignore = "requires configured PostgreSQL 15 test instance and writer account"]
fn postgresql15_sink_representation_carriers_live_qualification()
-> Result<(), Box<dyn std::error::Error>> {
    live_qualify_and_apply("15")
}

#[test]
#[ignore = "requires configured PostgreSQL 16 test instance and writer account"]
fn postgresql16_sink_representation_carriers_live_qualification()
-> Result<(), Box<dyn std::error::Error>> {
    live_qualify_and_apply("16")
}

#[test]
#[ignore = "requires configured PostgreSQL 17 test instance and writer account"]
fn postgresql17_sink_representation_carriers_live_qualification()
-> Result<(), Box<dyn std::error::Error>> {
    live_qualify_and_apply("17")
}
