//! Executed offline evidence; never a claim about an external database build.
#[allow(dead_code)]
#[path = "support/matrix_fixture.rs"]
mod fixture;

use change_event::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn versions() -> Vec<String> {
    serde_json::from_str::<Value>(include_str!("../scripts/qualification-matrix.json"))
        .unwrap()["databases"]
        .as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_owned()).collect()
}

fn connector(id: &str) -> Option<fixture::SourceVersion> {
    use fixture::SourceVersion::*;
    match id {
        "mysql_5_7" => Some(Mysql57),
        "mysql_8_0" => Some(Mysql80),
        "mysql_8_4" => Some(Mysql84),
        "postgresql_15" => Some(Postgresql15),
        _ => None,
    }
}

fn mapping(v: fixture::SourceVersion, native: &str) -> Result<SourceTypeMapping, String> {
    use fixture::SourceVersion::*;
    match v {
        Mysql57 => {
            mysql_5_7::source_type_mapping(native, Some("utf8mb4"), None).map_err(|e| e.to_string())
        }
        Mysql80 => mysql_8_0::source_type_mapping(native, Some("utf8mb4"), None),
        Mysql84 => mysql_8_4::source_type_mapping(native, Some("utf8mb4"), None),
        Postgresql15 => postgresql_15::source_type_mapping(native).map_err(|e| e.to_string()),
    }
}

fn manifest(v: fixture::SourceVersion) -> TargetCapabilityManifest {
    use fixture::SourceVersion::*;
    let build = ServerBuildIdentity::new(
        if v.is_postgresql() {
            "postgresql"
        } else {
            "mysql"
        },
        "offline-fixture",
        v.version(),
        "fixture-not-live-evidence",
    );
    match v {
        Mysql57 => mysql_5_7::compatibility_manifest(build),
        Mysql80 => mysql_8_0::compatibility_manifest(build),
        Mysql84 => mysql_8_4::compatibility_manifest(build),
        Postgresql15 => postgresql_15::compatibility_manifest(build),
    }
}

fn field(native: &str, logical: LogicalType, side: &str) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new(
            format!("catalog:s.t.{side}"),
            format!("{side}-fixture-definition"),
        ),
        ordinal: 0,
        name: side.into(),
        native_type: native.into(),
        logical_type: logical,
        nullable: true,
        collation: None,
        generated: false,
        primary_key_ordinal: None,
        unique: false,
        row_locator: false,
    }
}

// Native declarations are independently selected by role, never by pair.
struct Case {
    id: &'static str,
    mysql: &'static str,
    pg: &'static str,
    target_mysql: &'static str,
    target_pg: &'static str,
}
fn cases() -> Vec<Case> {
    [
        ("integer", "int", "integer", "int", "integer"),
        ("integer_range", "bigint", "bigint", "int", "integer"),
        (
            "unsigned",
            "bigint unsigned",
            "numeric(20,0)",
            "bigint unsigned",
            "numeric(20,0)",
        ),
        (
            "decimal",
            "decimal(30,6)",
            "numeric(30,6)",
            "decimal(30,6)",
            "numeric(30,6)",
        ),
        (
            "decimal_range",
            "decimal(30,6)",
            "numeric(30,6)",
            "decimal(10,2)",
            "numeric(10,2)",
        ),
        (
            "float",
            "double",
            "double precision",
            "double",
            "double precision",
        ),
        (
            "text",
            "varchar(255)",
            "character varying(255)",
            "varchar(255)",
            "character varying(255)",
        ),
        (
            "text_length",
            "varchar(255)",
            "character varying(255)",
            "varchar(10)",
            "character varying(10)",
        ),
        ("date", "date", "date", "date", "date"),
        (
            "local_datetime",
            "datetime(6)",
            "timestamp(6) without time zone",
            "datetime(6)",
            "timestamp(6) without time zone",
        ),
        (
            "instant",
            "timestamp(6)",
            "timestamp(6) with time zone",
            "timestamp(6)",
            "timestamp(6) with time zone",
        ),
        (
            "temporal_conversion",
            "datetime(6)",
            "timestamp(6) without time zone",
            "timestamp(3)",
            "timestamp(3) with time zone",
        ),
        ("duration", "time(6)", "interval", "time(6)", "interval"),
        ("year", "year", "smallint", "year", "smallint"),
        ("boolean", "boolean", "boolean", "tinyint", "boolean"),
        ("uuid", "uuid", "uuid", "varchar(36)", "uuid"),
        ("json", "json", "jsonb", "json", "jsonb"),
        ("raw_json", "unknown_json_text", "json", "json", "jsonb"),
        (
            "enum",
            "enum('a','b')",
            "enum('a','b')",
            "enum('a','b')",
            "enum('a','b')",
        ),
        (
            "set",
            "set('a','b')",
            "set('a','b')",
            "set('a','b')",
            "text",
        ),
        ("binary", "varbinary(32)", "bytea", "varbinary(32)", "bytea"),
        ("bit_string", "bit(8)", "bit(8)", "bit(8)", "bit(8)"),
        ("spatial", "geometry", "geometry", "geometry", "geometry"),
        ("array", "integer[]", "integer[]", "integer[]", "integer[]"),
        ("struct", "record", "record", "record", "record"),
        ("map", "hstore", "hstore", "hstore", "hstore"),
        ("range", "int4range", "int4range", "int4range", "int4range"),
        (
            "multirange",
            "int4multirange",
            "int4multirange",
            "int4multirange",
            "int4multirange",
        ),
        ("unknown", "unqualified", "unqualified", "text", "text"),
        (
            "invalid_parameters",
            "decimal(2,5)",
            "numeric(0,0)",
            "int",
            "integer",
        ),
        ("primary_key", "int", "integer", "int", "integer"),
        ("keyless", "int", "integer", "int", "integer"),
        ("generated_mismatch", "int", "integer", "int", "integer"),
        ("required_target", "int", "integer", "int", "integer"),
        ("lossy_key", "bigint", "bigint", "int", "integer"),
    ]
    .into_iter()
    .map(|(id, mysql, pg, target_mysql, target_pg)| Case {
        id,
        mysql,
        pg,
        target_mysql,
        target_pg,
    })
    .collect()
}

fn run_case(
    source: fixture::SourceVersion,
    sink: fixture::SourceVersion,
    manifest: &TargetCapabilityManifest,
    case: &Case,
) -> Value {
    let native = if source.is_postgresql() {
        case.pg
    } else {
        case.mysql
    };
    let target_native = if sink.is_postgresql() {
        case.target_pg
    } else {
        case.target_mysql
    };
    let source_mapping = match mapping(source, native) {
        Ok(mapping) => mapping,
        Err(_) => {
            return json!({"case":case.id,"qualification":"UNSUPPORTED/BLOCKED","status":"UNSUPPORTED","phase":"source_mapping","code":"source_type.unqualified","offline":"PASS"});
        }
    };
    let mut source_field = field(native, source_mapping.logical_type.clone(), "source");
    let target_type = mapping(sink, target_native)
        .map(|m| m.logical_type)
        .unwrap_or(LogicalType::Opaque {
            source_type: target_native.into(),
            format: "catalog-native".into(),
        });
    let mut target_field = field(target_native, target_type, "target");
    if ["primary_key", "lossy_key"].contains(&case.id) {
        source_field.primary_key_ordinal = Some(0);
        target_field.primary_key_ordinal = Some(0);
    }
    if case.id == "generated_mismatch" {
        source_field.generated = true;
    }
    if case.id == "required_target" {
        target_field.nullable = false;
    }
    let mut options = RouteOptions {
        route_id: "qualification".into(),
        configuration_revision: "fixture-v1".into(),
        ..RouteOptions::default()
    };
    if case.id == "temporal_conversion" {
        options.parameters.extend([
            ("target_precision".into(), "3".into()),
            ("temporal_strategy".into(), "local_to_absolute".into()),
            ("time_zone".into(), "+08:00".into()),
        ]);
    }
    let input = |options| FieldCompatibilityInput {
        source_connector: source_mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: None,
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: source_mapping.clone(),
        source_field: source_field.clone(),
        target_field: target_field.clone(),
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: case.id != "keyless",
        options,
    };
    let mut result = plan_field_compatibility(input(options.clone())).unwrap();
    if result.status == CompatibilityStatus::NeedsConfirmation {
        assert!(!result.is_selectable());
        let plan = result.plan.as_ref().unwrap();
        options.confirmations.push(RiskConfirmation {
            source_field_lineage: plan.source_field.lineage_id.clone(),
            target_field_lineage: plan.target_field.lineage_id.clone(),
            rule: plan.rule.clone(),
            plan_digest: plan.plan_digest.clone(),
            actor: "fixture".into(),
            confirmed_at: "2026-09-18T00:00:00Z".into(),
            reason: Some("offline risk-gating test".into()),
        });
        result = plan_field_compatibility(input(options.clone())).unwrap();
        assert!(result.is_selectable(), "{}: {:?}", case.id, result);
    }
    if ["integer", "primary_key"].contains(&case.id) {
        assert_eq!(result.qualification, QualificationLevel::Exact);
        assert!(result.is_selectable());
    }
    if case.id == "integer_range" {
        assert_eq!(result.qualification, QualificationLevel::RangeChecked);
        assert!(result.is_selectable());
    }
    if case.id == "temporal_conversion" {
        assert_eq!(result.qualification, QualificationLevel::ExplicitConversion);
        assert!(result.is_selectable());
    }
    if [
        "keyless",
        "generated_mismatch",
        "required_target",
        "lossy_key",
    ]
    .contains(&case.id)
    {
        assert!(!result.is_selectable());
    }
    let mut checks = vec![
        "source_mapping",
        "logical_type",
        "type_parameters",
        "planning",
    ];
    if result.is_selectable() {
        let plan = result.plan.as_ref().unwrap();
        assert!(plan.verify_digest());
        let saved: ColumnConversionPlan =
            serde_json::from_slice(&serde_json::to_vec(plan).unwrap()).unwrap();
        assert_eq!(saved.plan_digest, plan.plan_digest);
        if case.id == "integer" {
            assert_eq!(
                plan_field_compatibility(input(options))
                    .unwrap()
                    .plan
                    .unwrap()
                    .plan_digest,
                plan.plan_digest
            );
        }
        validate_datum_against_plan(plan, &Datum::Null).unwrap();
        validate_datum_against_plan(plan, &Datum::Unchanged).unwrap();
        assert!(validate_datum_against_plan(plan, &Datum::Unavailable).is_err());
        checks.extend([
            "plan_roundtrip",
            "stable_digest",
            "NULL",
            "Unchanged",
            "Unavailable",
        ]);
        if case.id == "integer_range" {
            for value in ["-2147483648", "2147483647"] {
                validate_value_against_plan(
                    plan,
                    &LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: value.into(),
                    },
                )
                .unwrap();
            }
            for value in ["-2147483649", "2147483648"] {
                assert!(
                    validate_value_against_plan(
                        plan,
                        &LogicalValue::Integer {
                            signed: true,
                            bits: 64,
                            value: value.into()
                        }
                    )
                    .is_err()
                );
            }
            checks.push("signed_boundary_and_overflow");
        }
        // Wrong value families must never be coerced by the fixed plan.
        let bad = if matches!(source_mapping.logical_type, LogicalType::Boolean) {
            LogicalValue::Binary {
                bytes_base64url: "AA".into(),
            }
        } else {
            LogicalValue::Boolean { value: true }
        };
        if plan.target.parameters.contains_key("range_kind")
            || plan.target.parameters.contains_key("conversion_kind")
        {
            assert!(
                validate_value_against_plan(plan, &bad).is_err(),
                "bad value accepted: {} {:?}",
                case.id,
                source
            );
            checks.push("bad_value");
        }
    }
    let qualification = if result.qualification == QualificationLevel::Unsupported {
        json!("UNSUPPORTED/BLOCKED")
    } else {
        json!(result.qualification)
    };
    json!({"case":case.id,"qualification":qualification,"status":result.status,"code":result.reason_code,"offline":"PASS","checks":checks,"plan_digest":result.plan.map(|p|p.plan_digest)})
}

fn assert_dml(source: fixture::SourceVersion, sink: fixture::SourceVersion) {
    let tx =
        fixture::validate_source(source, fixture::transaction(source, "qualification")).unwrap();
    let replay = fixture::roundtrip(&tx).unwrap();
    assert_eq!(
        change_event::json(&replay).unwrap(),
        change_event::json(&tx).unwrap()
    );
    macro_rules! check {
        ($adapter:ident) => {{
            let plan = $adapter::SinkAdapter::new().plan(&replay).unwrap();
            assert_eq!(plan.statements().count(), 3);
            assert!(plan.parameters().all(|p| !p.is_empty()));
            let mut keyless = replay.transaction().clone();
            for row in &mut keyless.changes {
                for col in row.before.iter_mut().chain(row.after.iter_mut()).flatten() {
                    col.primary_key_ordinal = None;
                }
            }
            assert!(
                $adapter::SinkAdapter::new()
                    .plan(&validate(keyless).unwrap())
                    .is_err()
            );
            let mut generated = replay.transaction().clone();
            for row in &mut generated.changes {
                for col in row.before.iter_mut().chain(row.after.iter_mut()).flatten() {
                    if col.name == "ratio" {
                        col.generated = true;
                    }
                }
            }
            // Generated columns are never ordinary writable inputs. Some
            // renderers reject the missing observation; those that can plan
            // the transaction must omit the generated field from SQL.
            if let Ok(generated) = $adapter::SinkAdapter::new().plan(&validate(generated).unwrap())
            {
                assert!(generated.statements().all(|sql| !sql.contains("ratio")));
            }
            let mut unavailable = replay.transaction().clone();
            unavailable.changes[0].after.as_mut().unwrap()[0].datum = Datum::Unavailable;
            // Either the neutral validator or the Sink rejects a missing key;
            // no execution is attempted in either case.
            if let Ok(unavailable) = validate(unavailable) {
                assert!($adapter::SinkAdapter::new().plan(&unavailable).is_err());
            }
            let mut bad = replay.transaction().clone();
            bad.changes[0].after.as_mut().unwrap()[0].datum = Datum::Value(LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "9223372036854775808".into(),
            });
            assert!(fixture::validate_source(source, bad).is_err());
        }};
    }
    use fixture::SourceVersion::*;
    match sink {
        Mysql57 => check!(mysql_5_7),
        Mysql80 => check!(mysql_8_0),
        Mysql84 => check!(mysql_8_4),
        Postgresql15 => check!(postgresql_15),
    }
}

#[test]
fn six_by_six_qualification() {
    let ids = versions();
    let config: Value =
        serde_json::from_str(include_str!("../scripts/qualification-matrix.json")).unwrap();
    assert_eq!(
        ids.iter().collect::<BTreeSet<_>>().len(),
        ids.len(),
        "duplicate connector identity"
    );
    for id in &ids {
        let implemented = config["implemented"]
            .as_array()
            .unwrap()
            .contains(&json!(id));
        let unsupported = config["unsupported"]
            .as_array()
            .unwrap()
            .contains(&json!(id));
        assert_ne!(
            implemented, unsupported,
            "every version must have an explicit support declaration"
        );
        assert_eq!(
            connector(id).is_some(),
            implemented,
            "new connector requires fixed source/sink fixtures"
        );
    }
    // Independent source fixtures may run concurrently; join in declared order
    // so the machine report is deterministic regardless of scheduling.
    let directions: Vec<Value> = std::thread::scope(|scope| {
        let handles: Vec<_> = ids
            .iter()
            .map(|source| {
                let ids = &ids;
                scope.spawn(move || {
                    ids.iter()
                        .map(|sink| qualify_direction(source, sink))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("source qualification failed"))
            .collect()
    });
    let report = json!({"schema":"cdc.qualification.v2","evidence_scope":"offline_fixture","live_semantics":"adapter_components_only","directions":directions});
    assert_eq!(
        report["directions"].as_array().unwrap().len(),
        ids.len() * ids.len()
    );
    if let Some(path) = std::env::var_os("CDC_QUALIFICATION_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

#[test]
fn new_version_adds_exactly_two_n_plus_one_directions() {
    let old = versions();
    let mut new = old.clone();
    new.push("future_connector".into());
    let product = |ids: &[String]| -> BTreeSet<(String, String)> {
        ids.iter()
            .flat_map(|s| ids.iter().map(move |t| (s.clone(), t.clone())))
            .collect()
    };
    let baseline = product(&old);
    let expanded = product(&new);
    let added: Vec<_> = expanded.difference(&baseline).collect();
    assert_eq!(added.len(), 2 * old.len() + 1);
    assert!(
        added
            .iter()
            .all(|(s, t)| s == "future_connector" || t == "future_connector")
    );
}

fn qualify_direction(source: &str, sink: &str) -> Value {
    match (connector(source), connector(sink)) {
        (Some(s), Some(t)) => {
            let manifest = manifest(t);
            manifest.validate().unwrap();
            assert_dml(s, t);
            let results: Vec<_> = cases()
                .iter()
                .map(|case| run_case(s, t, &manifest, case))
                .collect();
            let levels: BTreeSet<_> = results
                .iter()
                .map(|r| r["qualification"].as_str().unwrap())
                .collect();
            assert_eq!(
                levels,
                BTreeSet::from([
                    "EXACT",
                    "RANGE_CHECKED",
                    "EXPLICIT_CONVERSION",
                    "UNSUPPORTED/BLOCKED"
                ])
            );
            json!({"source":source,"sink":sink,"offline":"PASS","evidence_scope":"offline_fixture","manifest_digest":manifest.digest,"cases":results,"dml":"PASS","recovery":"PASS"})
        }
        _ => {
            json!({"source":source,"sink":sink,"offline":"UNSUPPORTED","live":"UNSUPPORTED","qualification":"UNSUPPORTED/BLOCKED","code":"connector.not_implemented","cases":cases().iter().map(|c|json!({"case":c.id,"offline":"UNSUPPORTED","qualification":"UNSUPPORTED/BLOCKED","code":"connector.not_implemented"})).collect::<Vec<_>>(),"dml":"UNSUPPORTED","recovery":"UNSUPPORTED"})
        }
    }
}
