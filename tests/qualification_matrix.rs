//! Executed offline evidence; never a claim about an external database build.
#[allow(dead_code)]
#[path = "support/matrix_fixture.rs"]
mod fixture;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::OnceLock;

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
        "postgresql_16" => Some(Postgresql16),
        "postgresql_17" => Some(Postgresql17),
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
        Postgresql15 => postgresql_15::source_type_mapping_with_catalog(native, postgres_catalog())
            .map_err(|e| e.to_string()),
        Postgresql16 => postgresql_16::source_type_mapping_with_catalog(native, postgres_catalog())
            .map_err(|e| e.to_string()),
        Postgresql17 => postgresql_17::source_type_mapping_with_catalog(native, postgres_catalog())
            .map_err(|e| e.to_string()),
    }
}

fn postgres_catalog() -> &'static postgresql_15::SourceTypeCatalog {
    static CATALOG: OnceLock<postgresql_15::SourceTypeCatalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        use postgresql_15::{
            SourceExtension, SourceTypeCatalog, SourceTypeDefinition, SourceTypeField,
        };

        let text = LogicalType::text("UTF8", None);
        SourceTypeCatalog::with_extensions(
            [
                SourceTypeDefinition::builtin(23, "pg_catalog", "integer"),
                SourceTypeDefinition::domain(
                    8_100,
                    "public",
                    "cdc_domain",
                    23,
                    ["VALUE > 0"],
                    true,
                    None,
                ),
                SourceTypeDefinition::enum_type(8_101, "public", "cdc_state", ["new", "done"]),
                SourceTypeDefinition::composite(
                    8_102,
                    "public",
                    "cdc_composite",
                    [SourceTypeField::new("value", 23, false)],
                ),
                SourceTypeDefinition::extension(
                    8_103,
                    "public",
                    "geometry",
                    "postgis",
                    "postgis-3.5.geometry-ewkb",
                    "ewkb",
                    Some(LogicalType::spatial("point", Some(4326), 2)),
                ),
                SourceTypeDefinition::extension(
                    8_104,
                    "public",
                    "hstore",
                    "hstore",
                    "hstore-1.text-v1",
                    "text",
                    Some(LogicalType::Map {
                        key: Box::new(text.clone()),
                        value: Box::new(text.clone()),
                    }),
                ),
                SourceTypeDefinition::extension(
                    8_105,
                    "public",
                    "custom_payload",
                    "custom_codec",
                    "custom-payload.v1",
                    "binary",
                    Some(LogicalType::raw(
                        "custom-payload.v1",
                        "custom_payload",
                        "catalog-bound",
                        "binary",
                    )),
                ),
            ],
            [
                SourceExtension {
                    name: "postgis".into(),
                    version: "3.5".into(),
                    schema: "public".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
                SourceExtension {
                    name: "hstore".into(),
                    version: "1.8".into(),
                    schema: "public".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
                SourceExtension {
                    name: "custom_codec".into(),
                    version: "1.0".into(),
                    schema: "public".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
            ],
        )
    })
}

fn manifest(v: fixture::SourceVersion) -> TargetCapabilityManifest {
    static MANIFESTS: OnceLock<[OnceLock<TargetCapabilityManifest>; 6]> = OnceLock::new();
    let manifests = MANIFESTS.get_or_init(|| std::array::from_fn(|_| OnceLock::new()));
    let index = match v {
        fixture::SourceVersion::Mysql57 => 0,
        fixture::SourceVersion::Mysql80 => 1,
        fixture::SourceVersion::Mysql84 => 2,
        fixture::SourceVersion::Postgresql15 => 3,
        fixture::SourceVersion::Postgresql16 => 4,
        fixture::SourceVersion::Postgresql17 => 5,
    };
    manifests[index]
        .get_or_init(|| {
            let server_build = ServerBuildIdentity::new(
                if v.is_postgresql() {
                    "postgresql"
                } else {
                    "mysql"
                },
                "offline-fixture",
                v.version(),
                "fixture-not-live-evidence",
            );
            let manifest = match v {
                fixture::SourceVersion::Mysql57 => mysql_5_7::compatibility_manifest(server_build),
                fixture::SourceVersion::Mysql80 => mysql_8_0::compatibility_manifest(server_build),
                fixture::SourceVersion::Mysql84 => mysql_8_4::compatibility_manifest(server_build),
                fixture::SourceVersion::Postgresql15 => {
                    postgresql_15::compatibility_manifest(server_build)
                }
                fixture::SourceVersion::Postgresql16 => {
                    postgresql_16::compatibility_manifest(server_build)
                }
                fixture::SourceVersion::Postgresql17 => {
                    postgresql_17::compatibility_manifest(server_build)
                }
            };
            manifest
                .validate()
                .expect("each connector publishes a valid offline Sink manifest");
            manifest
        })
        .clone()
}

fn field(native: &str, logical: LogicalType, side: &str) -> FieldDefinition {
    let definition_fingerprint = change_event::stable_digest(&(native, &logical, side));
    FieldDefinition {
        reference: DefinitionReference::new(format!("catalog:s.t.{side}"), definition_fingerprint),
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

fn representative_value(logical_type: &LogicalType) -> Option<LogicalValue> {
    Some(match logical_type {
        LogicalType::Boolean => LogicalValue::Boolean { value: true },
        LogicalType::Uuid => LogicalValue::Uuid {
            value: "12345678-1234-1234-1234-123456789abc".into(),
        },
        LogicalType::Integer { signed, bits } => LogicalValue::Integer {
            signed: *signed,
            bits: *bits,
            value: "1".into(),
        },
        LogicalType::Decimal { scale, .. } => LogicalValue::Decimal {
            unscaled: "1".into(),
            scale: usize::try_from((*scale).max(0)).ok()?,
        },
        LogicalType::Float { bits } => LogicalValue::Float {
            bits: *bits,
            ieee754_hex: if *bits == 32 {
                "3f800000"
            } else {
                "3ff0000000000000"
            }
            .into(),
        },
        LogicalType::Text { charset, .. } => LogicalValue::Text {
            charset: charset.clone(),
            bytes_base64url: "QQ".into(),
            text: Some("A".into()),
        },
        LogicalType::Binary { .. } => LogicalValue::Binary {
            bytes_base64url: "AA".into(),
        },
        LogicalType::BitString { length } => {
            let byte_length = usize::try_from(length.div_ceil(8)).ok()?;
            LogicalValue::BitString {
                bytes_base64url: URL_SAFE_NO_PAD.encode(vec![0; byte_length]),
                bit_length: *length,
                padding: if length % 8 == 0 {
                    BitPadding::None
                } else {
                    BitPadding::Zero
                },
                bit_order: BitOrder::MsbFirst,
            }
        }
        LogicalType::Date => LogicalValue::Date {
            year: 2026,
            month: 9,
            day: 24,
        },
        LogicalType::LocalTime { .. } => LogicalValue::LocalTime {
            hour: 1,
            minute: 2,
            second: 3,
            microsecond: 123_000,
        },
        LogicalType::LocalDatetime { .. } => LogicalValue::LocalDatetime {
            year: 2026,
            month: 9,
            day: 24,
            hour: 1,
            minute: 2,
            second: 3,
            microsecond: 123_000,
        },
        LogicalType::Instant { .. } => LogicalValue::Instant {
            unix_seconds: "1790211723".into(),
            nanoseconds: 123_000_000,
        },
        LogicalType::Duration { .. } => LogicalValue::Duration {
            negative: false,
            hours: 1,
            minutes: 2,
            seconds: 3,
            microsecond: 123_000,
        },
        LogicalType::Year => LogicalValue::Year { value: 2024 },
        LogicalType::Json { .. } => LogicalValue::Json {
            value: JsonValue::Object(vec![JsonEntry {
                key: "value".into(),
                value: JsonValue::Decimal {
                    unscaled: "12345".into(),
                    scale: 2,
                },
            }]),
        },
        LogicalType::Enum { members } => LogicalValue::Enum {
            label: members.first()?.clone(),
        },
        LogicalType::Set { members } => LogicalValue::Set {
            members: vec![members.first()?.clone()],
        },
        LogicalType::Spatial { subtype, srid, .. } => {
            let mut wkb = vec![1, 1, 0, 0, 0];
            wkb.extend([0; 16]);
            LogicalValue::Spatial {
                format: SpatialFormat::Wkb,
                bytes_base64url: URL_SAFE_NO_PAD.encode(wkb),
                geometry_type: if subtype == "*" { "point" } else { subtype }.into(),
                dimensions: 2,
                srid: *srid,
                crs: srid.map(|value| format!("EPSG:{value}")),
            }
        }
        LogicalType::Array { element } => LogicalValue::Array {
            elements: vec![representative_value(element)?],
        },
        LogicalType::ArrayWithMetadata {
            element,
            dimensions,
            lower_bounds,
        } => LogicalValue::ArrayWithMetadata {
            elements: vec![representative_value(element)?],
            dimensions: *dimensions,
            lower_bounds: lower_bounds.clone(),
        },
        LogicalType::Struct { fields } => LogicalValue::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Some(StructuredField {
                        name: field.name.clone(),
                        value: representative_value(&field.logical_type)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        },
        LogicalType::Map { key, value } => LogicalValue::Map {
            entries: vec![MapEntry {
                key: representative_value(key)?,
                value: representative_value(value)?,
            }],
        },
        LogicalType::Range { element } => LogicalValue::Range {
            empty: false,
            lower: Some(Box::new(representative_value(element)?)),
            upper: Some(Box::new(representative_value(element)?)),
            lower_inclusive: true,
            upper_inclusive: false,
        },
        LogicalType::MultiRange { element } => LogicalValue::MultiRange {
            ranges: vec![LogicalValue::Range {
                empty: false,
                lower: Some(Box::new(representative_value(element)?)),
                upper: Some(Box::new(representative_value(element)?)),
                lower_inclusive: true,
                upper_inclusive: false,
            }],
        },
        LogicalType::Domain { base, .. } => LogicalValue::Domain {
            value: Box::new(representative_value(base)?),
        },
        LogicalType::Network {
            address_family,
            cidr,
        } => LogicalValue::Network {
            family: address_family.clone(),
            address: "192.0.2.1".into(),
            prefix_length: cidr.then_some(24),
        },
        LogicalType::Xml => LogicalValue::Xml {
            bytes_base64url: URL_SAFE_NO_PAD.encode("<a/>"),
            text: Some("<a/>".into()),
        },
        LogicalType::Raw {
            codec_identity,
            native_type,
            source_definition_digest,
            encoding,
        } => LogicalValue::Raw {
            carrier: RawValueCarrier::new(
                codec_identity.clone(),
                native_type.clone(),
                source_definition_digest.clone(),
                encoding.clone(),
                "AA",
                None::<String>,
            ),
        },
        LogicalType::Opaque {
            source_type,
            format,
        } => LogicalValue::Raw {
            carrier: RawValueCarrier::new(
                "opaque-fixture-v1",
                source_type.clone(),
                "opaque-fixture-definition",
                format.clone(),
                "AA",
                None::<String>,
            ),
        },
        LogicalType::InvalidTemporal { kind } => LogicalValue::InvalidTemporal {
            kind: kind.clone(),
            raw: "0000-00-00".into(),
        },
    })
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
        (
            "spatial",
            "geometry",
            "public.geometry",
            "geometry",
            "public.geometry",
        ),
        ("array", "integer[]", "integer[]", "json", "integer[]"),
        (
            "struct",
            "record",
            "public.cdc_composite",
            "json",
            "public.cdc_composite",
        ),
        (
            "map",
            "unqualified",
            "public.hstore",
            "json",
            "public.hstore",
        ),
        ("range", "int4range", "int4range", "json", "int4range"),
        (
            "multirange",
            "int4multirange",
            "int4multirange",
            "json",
            "int4multirange",
        ),
        (
            "domain",
            "unqualified",
            "public.cdc_domain",
            "json",
            "public.cdc_domain",
        ),
        ("network", "unqualified", "inet", "varchar(64)", "inet"),
        ("xml", "unqualified", "xml", "text", "xml"),
        (
            "custom_type",
            "unqualified",
            "public.custom_payload",
            "json",
            "public.custom_payload",
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
            return json!({"case":case.id,"qualification":"UNSUPPORTED/BLOCKED","status":"UNSUPPORTED","evidence_outcome":"UNSUPPORTED","phase":"source_mapping","code":"source_type.unqualified","offline":"PASS"});
        }
    };
    let sample_value = if case.id == "decimal_range" {
        Some(LogicalValue::Decimal {
            unscaled: "1000000".into(),
            scale: 6,
        })
    } else {
        representative_value(&source_mapping.logical_type)
    };
    if let Some(value) = &sample_value {
        assert!(
            source_mapping.logical_type.matches_value(value),
            "typed fixture does not match source mapping: {} {:?}",
            case.id,
            source_mapping.logical_type
        );
    }
    let mut source_field = field(native, source_mapping.logical_type.clone(), "source");
    let target_mapping = mapping(sink, target_native).ok();
    let target_type = target_mapping
        .as_ref()
        .map(|mapping| mapping.logical_type.clone())
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
        if let Some(value) = &sample_value {
            validate_value_against_plan(plan, value).unwrap_or_else(|error| {
                panic!("representative value rejected for {}: {error}", case.id)
            });
            checks.push("typed_value");
        }
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
    let evidence_outcome = if !result.is_selectable() {
        if result.status == CompatibilityStatus::Unsupported {
            "UNSUPPORTED"
        } else {
            "BLOCKED"
        }
    } else {
        match result.qualification {
            QualificationLevel::Exact | QualificationLevel::RangeChecked => "Native Equivalent",
            QualificationLevel::ExplicitConversion
                if result.plan.as_ref().is_some_and(|plan| {
                    plan.target
                        .parameters
                        .get("structure_mapping")
                        .map(String::as_str)
                        == Some("json_value_carrier")
                }) =>
            {
                "Value Preserved"
            }
            QualificationLevel::ExplicitConversion => "Explicit Conversion",
            QualificationLevel::Unsupported => "UNSUPPORTED",
        }
    };
    let plan = result.plan.as_ref();
    let target_representation = plan
        .map(|plan| plan.target.clone())
        .unwrap_or_else(|| TargetRepresentation::new(target_native));
    json!({
        "case":case.id,
        "qualification":qualification,
        "evidence_outcome":evidence_outcome,
        "status":result.status,
        "code":result.reason_code,
        "offline":"PASS",
        "checks":checks,
        "source_connector":source_mapping.connector,
        "source_native_type":source_mapping.native_type,
        "source_mapping_id":source_mapping.mapping_id,
        "source_mapping_version":source_mapping.mapping_version,
        "source_mapping_digest":change_event::stable_digest(&source_mapping),
        "source_definition_fingerprint":source_field.reference.schema_fingerprint,
        "logical_type_fingerprint":source_mapping.logical_type.stable_digest(),
        "target_connector":manifest.connector,
        "target_build":manifest.target_build,
        "target_native_type":target_native,
        "target_mapping_id":target_mapping.as_ref().map(|mapping|mapping.mapping_id.clone()),
        "target_mapping_version":target_mapping.as_ref().map(|mapping|mapping.mapping_version.clone()),
        "target_mapping_digest":target_mapping.as_ref().map(change_event::stable_digest),
        "target_definition_fingerprint":target_field.reference.schema_fingerprint,
        "target_representation":target_representation,
        "conversion_rule":plan.map(|plan|plan.rule.clone()),
        "rule_digest":plan.map(|plan|plan.rule_digest.clone()),
        "manifest_digest":manifest.digest,
        "risk":plan.map(|plan|plan.risk),
        "loss":plan.map(|plan|plan.loss.clone()),
        "locator_impact":plan.map(|plan|plan.locator_impact),
        "fixture_value_digest":sample_value.as_ref().map(change_event::stable_digest),
        "plan_digest":plan.map(|plan|plan.plan_digest.clone())
    })
}

fn qualification_outcomes(cases: &[Value]) -> Value {
    let mut counts = json!({
        "Native Equivalent": 0,
        "Value Preserved": 0,
        "Explicit Conversion": 0,
        "UNSUPPORTED": 0,
        "BLOCKED": 0,
        "REQUIRES_LIVE": 0,
        "FAIL": 0
    });
    for case in cases {
        if let Some(outcome) = case["evidence_outcome"].as_str() {
            let count = counts[outcome].as_u64().unwrap_or_default();
            counts[outcome] = json!(count + 1);
        }
    }
    counts
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
        Postgresql16 => check!(postgresql_16),
        Postgresql17 => check!(postgresql_17),
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
        let source_only = config["source_only"]
            .as_array()
            .is_some_and(|values| values.contains(&json!(id)));
        let unsupported = config["unsupported"]
            .as_array()
            .unwrap()
            .contains(&json!(id));
        assert_ne!(
            implemented || source_only,
            unsupported,
            "every version must have an explicit support declaration"
        );
        assert_eq!(
            connector(id).is_some(),
            implemented || source_only,
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
    for direction in report["directions"].as_array().unwrap() {
        let outcomes = direction["qualification_outcomes"]
            .as_object()
            .expect("each direction reports every qualification outcome");
        for outcome in [
            "Native Equivalent",
            "Value Preserved",
            "Explicit Conversion",
            "UNSUPPORTED",
            "BLOCKED",
            "REQUIRES_LIVE",
            "FAIL",
        ] {
            assert!(outcomes.contains_key(outcome), "missing {outcome} outcome");
        }
        assert_eq!(
            outcomes
                .values()
                .map(|count| count.as_u64().unwrap())
                .sum::<u64>(),
            direction["cases"].as_array().unwrap().len() as u64
        );
    }
    if let Some(path) = std::env::var_os("CDC_QUALIFICATION_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

#[test]
fn direction_report_records_value_preservation_as_its_own_outcome() {
    let direction = qualify_direction("postgresql_15", "mysql_8_0");
    assert!(
        direction["qualification_outcomes"]["Value Preserved"]
            .as_u64()
            .unwrap()
            > 0,
        "the qualified recursive JSON carrier must be reported separately from explicit conversion"
    );
}

#[test]
fn six_connector_fixtures_and_manifests_are_registered() {
    let ids = versions();
    let config: Value =
        serde_json::from_str(include_str!("../scripts/qualification-matrix.json")).unwrap();
    let implemented = config["implemented"].as_array().unwrap();
    assert_eq!(
        implemented.len(),
        6,
        "all six connectors need Sink fixtures"
    );
    assert_eq!(config["source_only"].as_array().unwrap().len(), 0);

    for id in &ids {
        assert!(
            implemented.contains(&json!(id)),
            "missing Sink fixture for {id}"
        );
        let version = connector(id).unwrap_or_else(|| panic!("missing Source fixture for {id}"));
        let target_manifest = manifest(version);
        let expected_release = id
            .strip_prefix("mysql_")
            .or_else(|| id.strip_prefix("postgresql_"))
            .unwrap()
            .replace('_', ".");
        assert_eq!(target_manifest.connector.version, expected_release);
        assert_eq!(target_manifest.target_build.version, version.version());
    }

    let live = &config["live_qualification"];
    assert_eq!(live["source_fixture_roster"].as_array().unwrap().len(), 6);
    assert_eq!(live["sources"].as_array().unwrap().len(), 6);
    assert_eq!(live["sinks"].as_array().unwrap().len(), 6);
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

fn unqualified_direction(source: &str, sink: &str, outcome: &str, code: &str) -> Value {
    let cases: Vec<_> = cases()
        .iter()
        .map(|case| {
            json!({
                "case": case.id,
                "qualification": "UNSUPPORTED/BLOCKED",
                "evidence_outcome": outcome,
                "status": outcome,
                "code": code,
                "offline": outcome
            })
        })
        .collect();
    json!({
        "source": source,
        "sink": sink,
        "offline": outcome,
        "live": outcome,
        "qualification": "UNSUPPORTED/BLOCKED",
        "code": code,
        "qualification_outcomes": qualification_outcomes(&cases),
        "cases": cases,
        "dml": outcome,
        "recovery": outcome
    })
}

fn qualify_direction(source: &str, sink: &str) -> Value {
    let config: Value = serde_json::from_str(include_str!("../scripts/qualification-matrix.json"))
        .expect("qualification matrix must be valid JSON");
    if config["source_only"]
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value == sink))
    {
        return unqualified_direction(source, sink, "BLOCKED", "connector.sink_not_implemented");
    }
    if config["unsupported"]
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value == source || value == sink))
    {
        return unqualified_direction(source, sink, "UNSUPPORTED", "connector.not_implemented");
    }
    match (connector(source), connector(sink)) {
        (Some(s), Some(t)) => {
            let manifest = manifest(t);
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
            json!({"source":source,"sink":sink,"offline":"PASS","evidence_scope":"offline_fixture","manifest_digest":manifest.digest,"qualification_outcomes":qualification_outcomes(&results),"cases":results,"dml":"PASS","recovery":"PASS"})
        }
        _ => unqualified_direction(source, sink, "BLOCKED", "connector.fixture_missing"),
    }
}
