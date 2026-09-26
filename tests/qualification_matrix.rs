//! Executed offline evidence; never a claim about an external database build.
#[allow(dead_code)]
#[path = "support/matrix_fixture.rs"]
mod fixture;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

fn versions() -> Vec<String> {
    type_inventory()["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_owned())
        .collect()
}

const PINNED_TYPE_INVENTORY_DIGESTS: [(&str, &str); 6] = [
    (
        "mysql_5_7",
        "39d2cba8f1ad127e43c2d456a783d135018a4518615056fb9c303f4be4ba168e",
    ),
    (
        "mysql_8_0",
        "ceba9a8b13c3e4083b92ddf1ec0bab95280803ad6296d1ff52a3325a85dcac9b",
    ),
    (
        "mysql_8_4",
        "48bf555f15f7e1837c7f026e999bd50a2217311b6414a5d0fa4448adbc4a61d7",
    ),
    (
        "postgresql_15",
        "9b69a58b3c5faf04dd3cba8fdd2eac7308bc174adc34a6603f76d20868a89714",
    ),
    (
        "postgresql_16",
        "937c55cac4d2db0aba2fc1f5bf4375598b4482fdf1658dfd2d0336b46b6ef254",
    ),
    (
        "postgresql_17",
        "4109071301817d29f0ceac1b4cb8ad88ac6f53e82732eb25a65cf68ccdbb1564",
    ),
];

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
            scale: *scale,
        },
        LogicalType::DecimalUnbounded => LogicalValue::Decimal {
            unscaled: "1".into(),
            scale: 0,
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
        LogicalType::OffsetTime { .. } => LogicalValue::OffsetTime {
            hour: 1,
            minute: 2,
            second: 3,
            microsecond: 123_000,
            offset_seconds: 5 * 60 * 60 + 30 * 60,
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
        LogicalType::CalendarInterval { .. } => LogicalValue::CalendarInterval {
            months: 2,
            days: -3,
            microseconds: 4_000_005,
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
            dimension_lengths: vec![1; usize::from(*dimensions)],
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
            carrier: qualified_fixture_raw_carrier(
                codec_identity.clone(),
                native_type.clone(),
                source_definition_digest.clone(),
                encoding.clone(),
            )?,
        },
        LogicalType::Opaque {
            source_type,
            format,
        } => LogicalValue::Raw {
            carrier: qualified_fixture_raw_carrier(
                "opaque-fixture-v1",
                source_type.clone(),
                stable_digest(&("opaque-fixture-definition", source_type, format)),
                format.clone(),
            )?,
        },
        LogicalType::InvalidTemporal { kind } => LogicalValue::InvalidTemporal {
            kind: kind.clone(),
            raw: "0000-00-00".into(),
        },
    })
}

fn qualified_fixture_raw_carrier(
    codec_identity: impl Into<String>,
    native_type: impl Into<String>,
    source_definition_digest: impl Into<String>,
    encoding: impl Into<String>,
) -> Option<RawValueCarrier> {
    let codec_identity = codec_identity.into();
    let native_type = native_type.into();
    let source_definition_digest = source_definition_digest.into();
    let encoding = encoding.into();
    let context = SourceRepresentationContext {
        connector: ConnectorIdentity::new("qualification-fixture", "1"),
        server_build: ServerBuildIdentity::new("qualification-fixture", "offline", "1", "fixture"),
        source_type_identity: native_type.clone(),
        source_type_definition_digest: source_definition_digest.clone(),
        protocol: "offline-fixture-v1".into(),
        format: SourceRepresentationFormat::Binary,
        type_metadata: std::collections::BTreeMap::new(),
        source_cursor: SourceCursor {
            format: "offline-fixture-cursor-v1".into(),
            value: "fixture:1".into(),
            display: "fixture:1".into(),
        },
    };
    RawValueCarrier::new(
        codec_identity,
        native_type,
        source_definition_digest,
        encoding,
        "AA",
        None::<String>,
    )
    .with_source_context(context, true)
    .ok()
}

fn zero_date_fixture_evidence() -> Value {
    let value = mysql_5_7::decode_temporal_components("date", 0, 0, 0, 0, 0, 0, 0)
        .expect("MySQL zero date is captured as an invalid temporal value");
    let LogicalValue::InvalidTemporal { kind, raw } = &value else {
        panic!("zero date must not be normalized into a valid date")
    };
    assert_eq!(kind, "mysql.date");
    assert_eq!(raw, "0000-00-00");
    assert!(!LogicalType::Date.matches_value(&value));

    json!({
        "fixture": "mysql_zero_date",
        "source_connector": "mysql_5_7",
        "source_capture": "PASS",
        "captured_logical_type": "invalid_temporal",
        "captured_raw": raw,
        "captured_value_digest": change_event::stable_digest(&value),
        "date_write_validation": "BLOCKED"
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
            "float_nan",
            "double",
            "double precision",
            "double",
            "double precision",
        ),
        (
            "float_positive_infinity",
            "double",
            "double precision",
            "double",
            "double precision",
        ),
        (
            "float_negative_infinity",
            "double",
            "double precision",
            "double",
            "double precision",
        ),
        (
            "float_negative_zero",
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

fn type_inventory() -> Value {
    serde_json::from_str(include_str!("../scripts/type-inventory.json"))
        .expect("versioned native type inventory must be valid JSON")
}

fn connector_inventory_digest(inventory: &Value, connector_id: &str) -> String {
    let connector = inventory["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == connector_id)
        .unwrap();
    let declarations: Vec<_> = inventory["types"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| {
            let profile_id = entry["declaration_profile"].as_str().unwrap();
            let profile = &inventory["native_declaration_profiles"][profile_id];
            profile["connectors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|source| source == connector_id)
                .then(|| {
                    json!({
                        "type": entry,
                        "native_declaration_profile": profile,
                        "mapping_aliases": inventory["mapping_aliases"].get(profile_id)
                    })
                })
        })
        .collect();
    let dynamic_classes: Vec<_> = inventory["dynamic_type_classes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|class| {
            class["connectors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|source| source == connector_id)
        })
        .collect();
    let per_type_qualification_entries: serde_json::Map<String, Value> =
        inventory["per_type_qualification"]["entries"]
            .as_object()
            .unwrap()
            .iter()
            .filter_map(|(key, receipt)| {
                let (type_id, source_id) = key.rsplit_once('@')?;
                let source_evidence = (source_id == connector_id).then(|| {
                    json!({
                        "source": receipt["source"],
                        "web": receipt["web"]
                    })
                });
                let sink_evidence = receipt["sinks"][connector_id]
                    .as_object()
                    .map(|sink| json!({ "sink": sink }));
                (source_evidence.is_some() || sink_evidence.is_some()).then(|| {
                    let mut projection = json!({
                        "type_id": type_id,
                        "source_connector_id": source_id
                    });
                    if let Some(source_evidence) = source_evidence {
                        projection["source_evidence"] = source_evidence;
                    }
                    if let Some(sink_evidence) = sink_evidence {
                        projection["sink_evidence"] = sink_evidence;
                    }
                    (key.clone(), projection)
                })
            })
            .collect();
    let per_type_qualification_evidence_registry: Vec<_> =
        inventory["per_type_qualification"]["evidence_registry"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|record| {
                record["source_connector_id"] == connector_id
                    || record["sink_connector_id"] == connector_id
            })
            .collect();
    change_event::stable_digest(&json!({
        "connector": connector,
        "static_declarations": declarations,
        "source_type_identity": inventory["source_type_identity"],
        "source_mapping_code_seams": inventory["source_mapping_code_seams"][connector_id],
        "alias_resolution": inventory["alias_resolution"],
        "dynamic_type_classes": dynamic_classes,
        "dynamic_catalog_status": inventory["dynamic_catalog_status_by_connector"][connector_id],
        "per_type_qualification_entries": per_type_qualification_entries,
        "per_type_qualification_evidence_registry": per_type_qualification_evidence_registry,
        "excluded_type_classes": inventory["excluded_type_classes"]
    }))
}

fn inventory_status(
    has_missing_implementation: bool,
    all_evidence_qualified: bool,
) -> &'static str {
    if has_missing_implementation {
        "MISSING_IMPLEMENTATION"
    } else if all_evidence_qualified {
        "PASS"
    } else {
        "REQUIRES_PER_TYPE_QUALIFICATION"
    }
}

fn evidence_axis_pass(
    axis: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_id: Option<&str>,
    axis_name: &str,
) -> bool {
    let entries = match evidence_registry["entries"].as_array() {
        Some(entries) => entries,
        None => return false,
    };
    axis["status"] == "PASS"
        && axis["evidence"].as_array().is_some_and(|items| {
            !items.is_empty()
                && items.iter().all(|item| {
                    let Some(evidence_id) = item.as_str() else {
                        return false;
                    };
                    let mut matching = entries.iter().filter(|record| record["id"] == evidence_id);
                    let Some(record) = matching.next() else {
                        return false;
                    };
                    matching.next().is_none() && evidence_artifact_passes(record) && {
                        let digest = record["report_digest"].as_str().unwrap_or_default();
                        record["type_id"] == type_id
                            && record["source_connector_id"] == source_id
                            && record["sink_connector_id"]
                                == sink_id.map_or(Value::Null, |sink| json!(sink))
                            && record["axis"] == axis_name
                            && record["status"] == "PASS"
                            && record["run_id"]
                                .as_str()
                                .is_some_and(|run_id| !run_id.trim().is_empty())
                            && digest.len() == 64
                            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                    }
                })
        })
}

fn evidence_artifact_passes(record: &Value) -> bool {
    let Some(relative_path) = record["artifact_path"].as_str() else {
        return false;
    };
    let relative_path = Path::new(relative_path);
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return false;
    }
    let workspace = match PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize() {
        Ok(workspace) => workspace,
        Err(_) => return false,
    };
    let artifact_path = match workspace.join(relative_path).canonicalize() {
        Ok(path) if path.starts_with(&workspace) => path,
        _ => return false,
    };
    let artifact: Value = match std::fs::read(artifact_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    {
        Some(artifact) => artifact,
        None => return false,
    };
    let Some(records) = artifact["evidence"].as_array() else {
        return false;
    };
    if artifact["schema"] != "cdc.type_qualification_evidence_report.v1"
        || artifact["run_id"] != record["run_id"]
        || change_event::stable_digest(&artifact) != record["report_digest"]
    {
        return false;
    }
    let mut matching = records
        .iter()
        .filter(|evidence| evidence["id"] == record["id"]);
    let Some(evidence) = matching.next() else {
        return false;
    };
    matching.next().is_none()
        && [
            "type_id",
            "source_connector_id",
            "sink_connector_id",
            "axis",
            "status",
            "run_id",
        ]
        .into_iter()
        .all(|field| evidence[field] == record[field])
}

fn per_type_qualification_pass(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
) -> bool {
    let source = &evidence["source"];
    let source_axes_pass = ["protocol_capture", "semantic_codec", "change_event", "live"]
        .into_iter()
        .all(|axis| {
            evidence_axis_pass(
                &source[axis],
                evidence_registry,
                type_id,
                source_id,
                None,
                &format!("source.{axis}"),
            )
        });
    let sinks = match evidence["sinks"].as_object() {
        Some(sinks) => sinks,
        None => return false,
    };
    let expected_sinks: BTreeSet<_> = sink_ids.iter().map(String::as_str).collect();
    let actual_sinks: BTreeSet<_> = sinks.keys().map(String::as_str).collect();
    let sink_evidence_pass = expected_sinks == actual_sinks
        && sink_ids.iter().all(|sink_id| {
            let sink = &evidence["sinks"][sink_id];
            let native_or_representation_outcome = match sink["outcome"].as_str() {
                Some("VALUE_PRESERVED") => true,
                Some("SOURCE_REPRESENTATION_PRESERVED") => {
                    evidence_axis_pass(
                        &json!({
                            "status": "PASS",
                            "evidence": sink["representation_carrier_evidence"]
                        }),
                        evidence_registry,
                        type_id,
                        source_id,
                        Some(sink_id),
                        "sink.representation_carrier",
                    ) && evidence_axis_pass(
                        &json!({
                            "status": "PASS",
                            "evidence": sink["representation_preservation_evidence"]
                        }),
                        evidence_registry,
                        type_id,
                        source_id,
                        Some(sink_id),
                        "sink.representation_preserved",
                    )
                }
                _ => false,
            };
            native_or_representation_outcome
                && evidence_axis_pass(
                    &json!({ "status": "PASS", "evidence": sink["offline_evidence"] }),
                    evidence_registry,
                    type_id,
                    source_id,
                    Some(sink_id),
                    "sink.offline",
                )
                && evidence_axis_pass(
                    &json!({ "status": "PASS", "evidence": sink["live_evidence"] }),
                    evidence_registry,
                    type_id,
                    source_id,
                    Some(sink_id),
                    "sink.live",
                )
        });
    let representation_required = sink_ids
        .iter()
        .any(|sink_id| evidence["sinks"][sink_id]["outcome"] == "SOURCE_REPRESENTATION_PRESERVED");
    let representation_axis_pass = !representation_required
        || (evidence_axis_pass(
            &source["source_representation_capture"],
            evidence_registry,
            type_id,
            source_id,
            None,
            "source.source_representation_capture",
        ) && evidence_axis_pass(
            &source["protocol_framing"],
            evidence_registry,
            type_id,
            source_id,
            None,
            "source.protocol_framing",
        ));
    let web = &evidence["web"];
    source_axes_pass
        && sink_evidence_pass
        && representation_axis_pass
        && web["selection"] == "explicit_per_field_user_choice_required"
        && web["risk_confirmation"] == "required_before_plan_activation"
        && evidence_axis_pass(web, evidence_registry, type_id, source_id, None, "web.plan")
}

fn per_type_source_live_status(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
) -> &'static str {
    if evidence_axis_pass(
        &evidence["source"]["live"],
        evidence_registry,
        type_id,
        source_id,
        None,
        "source.live",
    ) {
        "PASS"
    } else {
        "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
    }
}

fn per_type_sink_live_status(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
) -> &'static str {
    let sinks = match evidence["sinks"].as_object() {
        Some(sinks) => sinks,
        None => return "REQUIRES_PER_TYPE_LIVE_EVIDENCE",
    };
    let expected: BTreeSet<_> = sink_ids.iter().map(String::as_str).collect();
    let actual: BTreeSet<_> = sinks.keys().map(String::as_str).collect();
    if expected == actual && sink_ids.iter().all(|sink_id| {
        evidence_axis_pass(
            &json!({ "status": "PASS", "evidence": evidence["sinks"][sink_id]["live_evidence"] }),
            evidence_registry,
            type_id,
            source_id,
            Some(sink_id),
            "sink.live",
        )
    }) {
        "PASS"
    } else {
        "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
    }
}

fn per_type_web_status(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
) -> &'static str {
    let web = &evidence["web"];
    if web["selection"] == "explicit_per_field_user_choice_required"
        && web["risk_confirmation"] == "required_before_plan_activation"
        && evidence_axis_pass(web, evidence_registry, type_id, source_id, None, "web.plan")
    {
        "PASS"
    } else {
        "NOT_QUALIFIED_PER_NATIVE_TYPE"
    }
}

fn qualified_type_evidence_fixture(type_id: &str, source_id: &str, sink_ids: &[String]) -> Value {
    let id = |axis: &str, sink_id: Option<&str>| match sink_id {
        Some(sink_id) => format!("{type_id}@{source_id}>{sink_id}:{axis}"),
        None => format!("{type_id}@{source_id}:{axis}"),
    };
    let pass_axis = |axis: &str| json!({ "status": "PASS", "evidence": [id(axis, None)] });
    let sinks = sink_ids
        .iter()
        .map(|sink_id| {
            (
                sink_id.clone(),
                json!({
                    "outcome": "VALUE_PRESERVED",
                    "offline_evidence": [id("sink.offline", Some(sink_id))],
                    "live_evidence": [id("sink.live", Some(sink_id))],
                    "representation_carrier_evidence": [id("sink.representation_carrier", Some(sink_id))],
                    "representation_preservation_evidence": [id("sink.representation_preserved", Some(sink_id))]
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "source": {
            "protocol_capture": pass_axis("source.protocol_capture"),
            "semantic_codec": pass_axis("source.semantic_codec"),
            "change_event": pass_axis("source.change_event"),
            "live": pass_axis("source.live"),
            "source_representation_capture": pass_axis("source.source_representation_capture"),
            "protocol_framing": pass_axis("source.protocol_framing")
        },
        "sinks": sinks,
        "web": {
            "status": "PASS",
            "selection": "explicit_per_field_user_choice_required",
            "risk_confirmation": "required_before_plan_activation",
            "evidence": [id("web.plan", None)]
        }
    })
}

fn qualified_type_evidence_registry(type_id: &str, source_id: &str, sink_ids: &[String]) -> Value {
    let run_id = format!("issue59-test-run-{}", std::process::id());
    let mut evidence_records = Vec::new();
    let mut register = |axis: &str, sink_id: Option<&str>| {
        let id = match sink_id {
            Some(sink_id) => format!("{type_id}@{source_id}>{sink_id}:{axis}"),
            None => format!("{type_id}@{source_id}:{axis}"),
        };
        evidence_records.push(json!({
            "id": id,
            "type_id": type_id,
            "source_connector_id": source_id,
            "sink_connector_id": sink_id,
            "axis": axis,
            "status": "PASS",
            "run_id": run_id
        }));
    };
    for axis in [
        "source.protocol_capture",
        "source.semantic_codec",
        "source.change_event",
        "source.live",
        "source.source_representation_capture",
        "source.protocol_framing",
        "web.plan",
    ] {
        register(axis, None);
    }
    for sink_id in sink_ids {
        register("sink.offline", Some(sink_id));
        register("sink.live", Some(sink_id));
        register("sink.representation_carrier", Some(sink_id));
        register("sink.representation_preserved", Some(sink_id));
    }
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "evidence": evidence_records
    });
    let report_digest = change_event::stable_digest(&report);
    let artifact_path = format!("target/qualification-evidence-fixtures/{run_id}.json");
    let artifact_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&artifact_path);
    std::fs::create_dir_all(artifact_file.parent().unwrap())
        .expect("create ignored qualification evidence fixture directory");
    std::fs::write(
        &artifact_file,
        serde_json::to_vec_pretty(&report).expect("serialize qualification evidence fixture"),
    )
    .expect("write qualification evidence fixture");
    let entries = report["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .cloned()
        .map(|mut entry| {
            entry["artifact_path"] = json!(artifact_path);
            entry["report_digest"] = json!(report_digest);
            entry
        })
        .collect::<Vec<_>>();
    json!({ "entries": entries })
}

fn qualification_fixture_status(inventory: &Value, entry: &Value, source_id: &str) -> &'static str {
    let has_family_fixture = entry["qualification_case_ids"]
        .as_array()
        .is_some_and(|ids| !ids.is_empty());
    if !has_family_fixture {
        return "MISSING_TEST";
    }
    let type_id = entry["id"].as_str().unwrap();
    let evidence_key = format!("{type_id}@{source_id}");
    let evidence = &inventory["per_type_qualification"]["entries"][evidence_key];
    if per_type_qualification_pass(
        evidence,
        &inventory["per_type_qualification"]["evidence_registry"],
        type_id,
        source_id,
        &versions(),
    ) {
        "PER_TYPE_QUALIFIED"
    } else {
        "FAMILY_FIXTURE_ONLY"
    }
}

fn receipt_uses_representation(evidence: &Value) -> bool {
    evidence["sinks"].as_object().is_some_and(|sinks| {
        sinks
            .values()
            .any(|sink| sink["outcome"] == "SOURCE_REPRESENTATION_PRESERVED")
    })
}

fn representation_scopes(entries: &Value) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut sources = BTreeSet::new();
    let mut sinks = BTreeSet::new();
    if let Some(entries) = entries.as_object() {
        for (key, evidence) in entries {
            if !receipt_uses_representation(evidence) {
                continue;
            }
            if let Some((_, source_id)) = key.rsplit_once('@') {
                sources.insert(source_id.to_owned());
            }
            if let Some(receipt_sinks) = evidence["sinks"].as_object() {
                for (sink_id, sink) in receipt_sinks {
                    if sink["outcome"] == "SOURCE_REPRESENTATION_PRESERVED" {
                        sinks.insert(sink_id.clone());
                    }
                }
            }
        }
    }
    (sources, sinks)
}

fn type_inventory_coverage() -> Value {
    let inventory = type_inventory();
    let mut declarations = Vec::new();
    let mut dynamic_classes = Vec::new();
    for entry in inventory["types"].as_array().unwrap() {
        let id = entry["id"].as_str().unwrap();
        let profile_id = entry["declaration_profile"].as_str().unwrap();
        let source = &inventory["native_declaration_profiles"][profile_id];
        let mapping_aliases =
            &inventory["mapping_aliases"][entry["declaration_profile"].as_str().unwrap()];
        for source_id in source["connectors"].as_array().unwrap() {
            let source_id = source_id.as_str().unwrap();
            let fixture_status = qualification_fixture_status(&inventory, entry, source_id);
            let qualification_key = format!("{id}@{source_id}");
            let qualification_evidence =
                &inventory["per_type_qualification"]["entries"][qualification_key.as_str()];
            let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
            let sink_ids = versions();
            let source_live_status = per_type_source_live_status(
                qualification_evidence,
                evidence_registry,
                id,
                source_id,
            );
            let sink_live_status = per_type_sink_live_status(
                qualification_evidence,
                evidence_registry,
                id,
                source_id,
                &sink_ids,
            );
            let web_status =
                per_type_web_status(qualification_evidence, evidence_registry, id, source_id);
            let source_profile = &inventory["source_evidence_profiles"][source["evidence_profile"]
                .as_str()
                .unwrap_or_else(|| panic!("{id}/{source_id} lacks evidence profile"))];
            let version = connector(source_id)
                .unwrap_or_else(|| panic!("inventory refers to unknown connector {source_id}"));
            for (kind, examples) in [
                ("native_example", &source["examples"]),
                ("sql_alias", mapping_aliases),
            ] {
                if let Some(examples) = examples.as_array() {
                    for declaration in examples {
                        let native = declaration.as_str().unwrap();
                        let result = mapping(version, native);
                        declarations.push(json!({
                            "type_id": id,
                            "source": source_id,
                            "declaration_kind": kind,
                            "native_declaration": native,
                            "source_type_identity": if source_id.starts_with("mysql_") {
                                json!({
                                    "kind": "mysql_binlog_wire_type",
                                    "wire_types": inventory["source_type_identity"]["mysql_binlog_wire_types"][entry["declaration_profile"].as_str().unwrap()]
                                })
                            } else {
                                json!({
                                    "kind": "postgresql_relation_type_oid",
                                    "catalog_identity": inventory["source_type_identity"]["postgresql_catalog_oid"]
                                })
                            },
                            "source_mapping_code_seam": inventory["source_mapping_code_seams"][source_id],
                            "source_mapping": if result.is_ok() { "MAPPED" } else { "MISSING_IMPLEMENTATION" },
                            "source_mapping_error": result.err().map(|error| error.chars().take(240).collect::<String>()),
                            "protocol_capture": source_profile["protocol_capture"],
                            "semantic_codec": source_profile["semantic_codec"],
                            "change_event": source_profile["change_event"],
                            "source_representation_capture": source_profile["source_representation_capture"],
                            "source_representation_envelope_schema": inventory["source_representation_contract"]["schema"],
                            "source_representation_envelope_fields": inventory["source_representation_contract"]["required_fields"],
                            "protocol_framing_profile": inventory["source_representation_contract"]["protocol_profiles"][source_id],
                            "sink_representation_outcomes": inventory["sink_representation_outcomes"],
                            "web_representation_policy": inventory["web_representation_policy"],
                            "sink_evidence_profile": entry["sink_evidence_profile"].as_str().unwrap_or(inventory["default_sink_evidence_profile"].as_str().unwrap()),
                            "sink_evidence": inventory["sink_evidence_profiles"][entry["sink_evidence_profile"].as_str().unwrap_or(inventory["default_sink_evidence_profile"].as_str().unwrap())]["targets"],
                            "qualification_case_ids": entry["qualification_case_ids"],
                            "qualification_fixture_status": fixture_status,
                            "per_type_qualification_evidence": qualification_evidence,
                            "per_type_live_evidence": {
                                "source": source_live_status,
                                "all_sinks": sink_live_status
                            },
                            "per_type_web_status": web_status
                        }));
                    }
                }
            }
        }
    }
    for dynamic in inventory["dynamic_type_classes"].as_array().unwrap() {
        for connector_id in dynamic["connectors"].as_array().unwrap() {
            let connector_id = connector_id.as_str().unwrap();
            let connector_status = &inventory["dynamic_catalog_status_by_connector"][connector_id];
            dynamic_classes.push(json!({
                "id": dynamic["id"],
                "connector": connector_id,
                "status": connector_status["status"],
                "unmatched_type_status": connector_status["unmatched_type_status"],
                "discovery": dynamic["discovery"],
                "catalog_query": dynamic["catalog_query"],
                "evidence": dynamic["evidence"]
            }));
        }
    }
    let gaps: Vec<_> = declarations
        .iter()
        .filter(|item| item["source_mapping"] == "MISSING_IMPLEMENTATION")
        .cloned()
        .collect();
    let types_without_fixture = inventory["types"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| {
            entry["qualification_case_ids"]
                .as_array()
                .is_none_or(Vec::is_empty)
        })
        .count();
    let types_with_family_fixture_only = declarations
        .iter()
        .filter(|declaration| declaration["qualification_fixture_status"] == "FAMILY_FIXTURE_ONLY")
        .map(|declaration| declaration["type_id"].as_str().unwrap())
        .collect::<BTreeSet<_>>()
        .len();
    let per_type_live_source_gaps = declarations
        .iter()
        .filter(|declaration| {
            declaration["per_type_live_evidence"]["source"] == "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
        })
        .map(|declaration| {
            format!(
                "{}@{}",
                declaration["type_id"].as_str().unwrap(),
                declaration["source"].as_str().unwrap()
            )
        })
        .collect::<BTreeSet<_>>()
        .len();
    let per_type_live_sink_gaps = declarations
        .iter()
        .filter(|declaration| {
            declaration["per_type_live_evidence"]["all_sinks"] == "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
        })
        .map(|declaration| {
            format!(
                "{}@{}",
                declaration["type_id"].as_str().unwrap(),
                declaration["source"].as_str().unwrap()
            )
        })
        .collect::<BTreeSet<_>>()
        .len();
    let missing_evidence = |status: &Value| {
        status
            .as_str()
            .is_some_and(|value| value.starts_with("MISSING"))
    };
    let source_profiles = inventory["source_evidence_profiles"].as_object().unwrap();
    let protocol_profiles = inventory["source_representation_contract"]["protocol_profiles"]
        .as_object()
        .unwrap();
    let sink_targets = inventory["sink_evidence_profiles"]
        [inventory["default_sink_evidence_profile"].as_str().unwrap()]["targets"]
        .as_object()
        .unwrap();
    let sink_outcomes = inventory["sink_representation_outcomes"]
        .as_object()
        .unwrap();
    let (representation_sources, representation_sinks) =
        representation_scopes(&inventory["per_type_qualification"]["entries"]);
    let representation_required =
        !representation_sources.is_empty() || !representation_sinks.is_empty();
    let has_missing_implementation = !gaps.is_empty()
        || types_without_fixture > 0
        || source_profiles.values().any(|profile| {
            ["protocol_capture", "semantic_codec", "change_event"]
                .into_iter()
                .any(|axis| missing_evidence(&profile[axis]["status"]))
        })
        || protocol_profiles.iter().any(|(source_id, profile)| {
            representation_sources.contains(source_id) && missing_evidence(&profile["status"])
        })
        || (representation_required
            && missing_evidence(&inventory["source_representation_contract"]["status"]))
        || sink_targets.iter().any(|(sink_id, target)| {
            ["native_target", "web_candidate", "live"]
                .into_iter()
                .any(|axis| missing_evidence(&target[axis]["status"]))
                || (representation_sinks.contains(sink_id)
                    && missing_evidence(&target["representation_carrier"]["status"]))
        })
        || sink_outcomes.iter().any(|(sink_id, outcomes)| {
            representation_sinks.contains(sink_id)
                && missing_evidence(&outcomes["source_representation_preserved"]["status"])
        })
        || (representation_required
            && missing_evidence(&inventory["web_representation_policy"]["status"]))
        || dynamic_classes
            .iter()
            .any(|class| class["unmatched_type_status"] == "MISSING_IMPLEMENTATION");
    let all_evidence_qualified = !has_missing_implementation
        && gaps.is_empty()
        && declarations.iter().all(|declaration| {
            let evidence = &declaration["per_type_qualification_evidence"];
            let declaration_representation_sinks: Vec<_> = evidence["sinks"]
                .as_object()
                .into_iter()
                .flat_map(|sinks| sinks.iter())
                .filter(|(_, sink)| sink["outcome"] == "SOURCE_REPRESENTATION_PRESERVED")
                .map(|(sink_id, _)| sink_id.as_str())
                .collect();
            declaration["qualification_fixture_status"] == "PER_TYPE_QUALIFIED"
                && (declaration_representation_sinks.is_empty()
                    || (declaration["protocol_framing_profile"]["status"] == "PASS"
                        && declaration["source_representation_capture"]["status"] == "PASS"
                        && declaration_representation_sinks.iter().all(|sink_id| {
                            declaration["sink_evidence"][*sink_id]["representation_carrier"]["status"] == "PASS"
                                && declaration["sink_representation_outcomes"][*sink_id]["source_representation_preserved"]["status"] == "PASS"
                        })))
        })
        && source_profiles.values().all(|profile| {
            ["protocol_capture", "semantic_codec", "change_event"]
                .into_iter()
                .all(|axis| profile[axis]["status"] == "PASS")
        })
        && protocol_profiles.iter().all(|(source_id, profile)| {
            !representation_sources.contains(source_id) || profile["status"] == "PASS"
        })
        && (!representation_required
            || inventory["source_representation_contract"]["status"] == "PASS")
        && sink_targets.iter().all(|(sink_id, target)| {
            ["native_target", "web_candidate", "live"]
                .into_iter()
                .all(|axis| target[axis]["status"] == "PASS")
                && (!representation_sinks.contains(sink_id)
                    || target["representation_carrier"]["status"] == "PASS")
        })
        && sink_outcomes.iter().all(|(sink_id, outcomes)| {
            outcomes["value_preserved"]["status"] == "PASS"
                || (representation_sinks.contains(sink_id)
                    && outcomes["source_representation_preserved"]["status"] == "PASS")
        })
        && (!representation_required || inventory["web_representation_policy"]["status"] == "PASS")
        && dynamic_classes.iter().all(|class| {
            class["status"] == "PASS" && class["unmatched_type_status"] != "MISSING_IMPLEMENTATION"
        })
        && types_without_fixture == 0
        && types_with_family_fixture_only == 0;
    json!({
        "schema": inventory["schema"],
        "inventory_revision": inventory["revision"],
        "per_type_qualification_schema": inventory["per_type_qualification"]["schema"],
        "per_type_qualification_evidence_registry": inventory["per_type_qualification"]["evidence_registry"],
        "native_type_count": inventory["types"].as_array().unwrap().len(),
        "source_declaration_count": declarations.len(),
        "source_mapping_gaps": gaps.len(),
        "types_without_qualification_fixture": types_without_fixture,
        "types_with_family_fixture_only": types_with_family_fixture_only,
        "per_type_live_source_gaps": per_type_live_source_gaps,
        "per_type_live_sink_gaps": per_type_live_sink_gaps,
        "source_representation_qualification_required": representation_required,
        "status": inventory_status(has_missing_implementation, all_evidence_qualified),
        "implementation_gaps": gaps,
        "declarations": declarations,
        "source_evidence_profiles": inventory["source_evidence_profiles"],
        "sink_evidence_profiles": inventory["sink_evidence_profiles"],
        "dynamic_type_classes": dynamic_classes,
        "dynamic_catalog_status_by_connector": inventory["dynamic_catalog_status_by_connector"],
        "excluded_type_classes": inventory["excluded_type_classes"]
    })
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
    let sample_value = match case.id {
        "decimal_range" => Some(LogicalValue::Decimal {
            unscaled: "1000000".into(),
            scale: 6,
        }),
        "float_nan" => Some(LogicalValue::Float {
            bits: 64,
            ieee754_hex: "7ff8000000000000".into(),
        }),
        "float_positive_infinity" => Some(LogicalValue::Float {
            bits: 64,
            ieee754_hex: "7ff0000000000000".into(),
        }),
        "float_negative_infinity" => Some(LogicalValue::Float {
            bits: 64,
            ieee754_hex: "fff0000000000000".into(),
        }),
        "float_negative_zero" => Some(LogicalValue::Float {
            bits: 64,
            ieee754_hex: "8000000000000000".into(),
        }),
        _ => representative_value(&source_mapping.logical_type),
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
            QualificationLevel::Exact => "Native Equivalent",
            QualificationLevel::RangeChecked => "Value Preserved",
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
fn six_version_type_inventory_has_explicit_source_and_sink_evidence_axes() {
    let inventory = type_inventory();
    assert_eq!(inventory["schema"], "cdc.type_inventory.v1");
    assert_eq!(
        inventory["per_type_qualification"]["schema"],
        "cdc.native_type_qualification.v1"
    );
    for required in [
        "id",
        "type_id",
        "source_connector_id",
        "sink_connector_id",
        "axis",
        "status",
        "run_id",
        "artifact_path",
        "report_digest",
    ] {
        assert!(
            inventory["per_type_qualification"]["evidence_registry"]["required_entry_fields"]
                .as_array()
                .unwrap()
                .contains(&json!(required))
        );
    }
    for axis in [
        "source.protocol_capture",
        "source.semantic_codec",
        "source.change_event",
        "source.live",
        "source.source_representation_capture",
        "source.protocol_framing",
        "sink.offline",
        "sink.live",
        "sink.representation_carrier",
        "sink.representation_preserved",
        "web.plan",
    ] {
        assert!(
            inventory["per_type_qualification"]["evidence_registry"]["allowed_axes"]
                .as_array()
                .unwrap()
                .contains(&json!(axis))
        );
    }
    assert_eq!(
        inventory["per_type_qualification"]["required_representation_sink_evidence"],
        json!([
            "representation_carrier_evidence",
            "representation_preservation_evidence"
        ])
    );
    for record in inventory["per_type_qualification"]["evidence_registry"]["entries"]
        .as_array()
        .unwrap()
    {
        assert!(
            record["artifact_path"]
                .as_str()
                .is_some_and(|path| path.starts_with("docs/qualification/evidence/"))
        );
    }
    let representation_contract = &inventory["source_representation_contract"];
    assert_eq!(
        representation_contract["schema"],
        "cdc.source_representation_envelope.v1"
    );
    assert_eq!(representation_contract["status"], "MISSING_IMPLEMENTATION");
    for required_field in [
        "connector_id",
        "connector_build",
        "source_definition_identity",
        "source_definition_digest",
        "source_protocol",
        "protocol_format",
        "native_type_identity",
        "native_type_metadata",
        "payload_bytes",
        "payload_length_bytes",
        "payload_sha256",
        "source_cursor",
    ] {
        assert!(
            representation_contract["required_fields"]
                .as_array()
                .unwrap()
                .contains(&json!(required_field)),
            "Source Representation Envelope lacks {required_field}"
        );
    }
    assert_eq!(
        representation_contract["protocol_profiles"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        versions().into_iter().collect()
    );
    let web_representation_policy = &inventory["web_representation_policy"];
    assert_eq!(
        web_representation_policy["status"],
        "MISSING_IMPLEMENTATION"
    );
    assert_eq!(
        web_representation_policy["selection"],
        "explicit_per_field_user_choice_required"
    );
    assert_eq!(
        web_representation_policy["risk_confirmation"],
        "required_before_plan_activation"
    );
    for forbidden in ["primary_key", "unique_key", "row_locator"] {
        assert!(
            web_representation_policy["forbidden_without_separate_equivalence_evidence"]
                .as_array()
                .unwrap()
                .contains(&json!(forbidden))
        );
    }

    let connector_ids: Vec<_> = inventory["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        connector_ids,
        [
            "mysql_5_7",
            "mysql_8_0",
            "mysql_8_4",
            "postgresql_15",
            "postgresql_16",
            "postgresql_17"
        ]
    );
    assert_eq!(
        connector_ids.iter().map(String::as_str).collect::<Vec<_>>(),
        PINNED_TYPE_INVENTORY_DIGESTS
            .iter()
            .map(|(connector_id, _)| *connector_id)
            .collect::<Vec<_>>(),
        "connector versions may not disappear from the inventory silently"
    );
    for (connector_id, expected_digest) in PINNED_TYPE_INVENTORY_DIGESTS {
        assert_eq!(
            connector_inventory_digest(&inventory, connector_id),
            expected_digest,
            "{} native type inventory changed; verify the new or removed declarations, aliases, OID/wire identity, dynamic classes, and exclusions before updating its pinned digest",
            connector_id
        );
    }
    let code_seams = inventory["source_mapping_code_seams"].as_object().unwrap();
    assert_eq!(
        code_seams.keys().cloned().collect::<BTreeSet<_>>(),
        connector_ids.iter().cloned().collect()
    );
    let dynamic_catalog_status = inventory["dynamic_catalog_status_by_connector"]
        .as_object()
        .unwrap();
    assert_eq!(
        dynamic_catalog_status
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        connector_ids.iter().cloned().collect(),
        "every connector needs an explicit dynamic-catalog enumeration status"
    );
    for connector_id in &connector_ids {
        assert_eq!(
            dynamic_catalog_status[connector_id]["unmatched_type_status"], "MISSING_IMPLEMENTATION",
            "unmatched catalog types must fail closed for {connector_id}"
        );
        assert_eq!(
            dynamic_catalog_status[connector_id]["status"], "REQUIRES_LIVE_CATALOG_ENUMERATION",
            "dynamic native type coverage needs a live catalog enumeration for {connector_id}"
        );
        for path in code_seams[connector_id].as_array().unwrap() {
            let path = path.as_str().unwrap();
            assert!(
                std::path::Path::new(path).is_file(),
                "missing SourceTypeMapping code seam {path}"
            );
        }
    }
    assert_eq!(
        connector_ids,
        versions(),
        "inventory owns the six-version roster"
    );
    let config: Value =
        serde_json::from_str(include_str!("../scripts/qualification-matrix.json")).unwrap();
    let role_ids: Vec<_> = ["implemented", "source_only", "unsupported"]
        .into_iter()
        .flat_map(|key| config[key].as_array().unwrap())
        .map(|value| value.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        role_ids.iter().collect::<BTreeSet<_>>(),
        connector_ids.iter().collect::<BTreeSet<_>>(),
        "suite config roles must partition the inventory roster"
    );
    assert!(
        config.get("databases").is_none(),
        "connector roster must have one source"
    );

    let mut ids = BTreeSet::new();
    let mut declaration_count = 0usize;
    for entry in inventory["types"].as_array().unwrap() {
        let id = entry["id"].as_str().expect("type id is required");
        assert!(ids.insert(id), "duplicate native type id {id}");
        assert!(
            entry["family"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(
            entry["semantics"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );

        let profile_id = entry["declaration_profile"].as_str().unwrap();
        let source = &inventory["native_declaration_profiles"][profile_id];
        let source_ids = source["connectors"]
            .as_array()
            .expect("source versions are required");
        assert!(!source_ids.is_empty(), "{id} has no native declaration");
        assert_eq!(source["storable"], true, "{id} must be a column type");
        assert!(
            source["examples"]
                .as_array()
                .is_some_and(|values| !values.is_empty())
        );
        assert!(
            source["evidence"]
                .as_array()
                .is_some_and(|values| !values.is_empty())
        );
        for source_id in source_ids {
            let source_id = source_id.as_str().unwrap();
            assert!(connector_ids.iter().any(|known| known == source_id));
            let evidence_profile_id = source["evidence_profile"].as_str().unwrap();
            let profile = &inventory["source_evidence_profiles"][evidence_profile_id];
            if source_id.starts_with("mysql_") {
                assert!(
                    inventory["source_type_identity"]["mysql_binlog_wire_types"][profile_id]
                        .as_array()
                        .is_some_and(|wire_types| !wire_types.is_empty()),
                    "{id}/{source_id} lacks MySQL binlog wire identity"
                );
            } else {
                let oid = &inventory["source_type_identity"]["postgresql_catalog_oid"];
                assert_eq!(oid["oid_field"], "column type OID");
                assert!(oid["catalog_query"].as_str().unwrap().contains("t.oid"));
            }
            assert!(
                code_seams[source_id]
                    .as_array()
                    .is_some_and(|paths| !paths.is_empty()),
                "{id}/{source_id} lacks SourceTypeMapping code seam"
            );
            for axis in [
                "protocol_capture",
                "semantic_codec",
                "change_event",
                "source_representation_capture",
            ] {
                assert!(
                    profile[axis]["status"].as_str().is_some(),
                    "{id}/{source_id}/{axis}"
                );
                assert!(
                    profile[axis]["evidence"].as_array().is_some(),
                    "{id}/{source_id}/{axis}"
                );
            }
        }
        for alias in source["aliases"].as_array().unwrap() {
            let alias = alias.as_str().unwrap();
            let alias_name = alias.split_whitespace().next().unwrap();
            let directly_probed = inventory["mapping_aliases"][profile_id]
                .as_array()
                .is_some_and(|probes| probes.contains(&json!(alias)));
            assert!(
                directly_probed
                    || inventory["alias_resolution"][profile_id][alias_name].is_object(),
                "{id} alias '{alias}' lacks a mapping probe or explicit resolution rule"
            );
        }
        declaration_count += (source["examples"].as_array().unwrap().len()
            + inventory["mapping_aliases"][profile_id]
                .as_array()
                .map_or(0, Vec::len))
            * source_ids.len();

        let sink_profile_id = entry["sink_evidence_profile"]
            .as_str()
            .or_else(|| inventory["default_sink_evidence_profile"].as_str())
            .unwrap();
        let sinks = inventory["sink_evidence_profiles"][sink_profile_id]["targets"]
            .as_object()
            .expect("sink evidence profile must enumerate connectors");
        assert_eq!(
            sinks.len(),
            connector_ids.len(),
            "{id} must describe all six sinks"
        );
        for sink_id in &connector_ids {
            let sink = sinks
                .get(sink_id)
                .unwrap_or_else(|| panic!("{id} lacks {sink_id}"));
            for axis in [
                "native_target",
                "representation_carrier",
                "web_candidate",
                "live",
            ] {
                assert!(
                    sink[axis]["status"].as_str().is_some(),
                    "{id}/{sink_id}/{axis}"
                );
                assert!(
                    sink[axis]["evidence"].as_array().is_some(),
                    "{id}/{sink_id}/{axis}"
                );
            }
            let outcomes = inventory["sink_representation_outcomes"]
                .get(sink_id)
                .unwrap_or_else(|| panic!("missing representation outcomes for {sink_id}"));
            assert_eq!(
                outcomes["value_preserved"]["status"],
                "NOT_QUALIFIED_PER_NATIVE_TYPE"
            );
            assert_eq!(
                outcomes["source_representation_preserved"]["status"],
                "MISSING_IMPLEMENTATION"
            );
            assert!(
                outcomes["source_representation_preserved"]["carrier_candidates"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty())
            );
        }
    }
    assert!(
        ids.len() >= 60,
        "inventory is missing documented native type families"
    );
    assert!(
        declaration_count >= 100,
        "version aliases and declaration probes are missing"
    );

    let dynamic: BTreeSet<_> = inventory["dynamic_type_classes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    for required in [
        "postgresql.arrays",
        "postgresql.domains",
        "postgresql.enums",
        "postgresql.composites",
        "postgresql.ranges",
        "postgresql.extensions_and_custom_base_types",
        "mysql.plugin_or_engine_types",
    ] {
        assert!(
            dynamic.contains(required),
            "missing dynamic class {required}"
        );
    }
    for dynamic_class in inventory["dynamic_type_classes"].as_array().unwrap() {
        assert!(dynamic_class["discovery"].as_str().is_some());
        assert!(
            dynamic_class["evidence"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
        for connector_id in dynamic_class["connectors"].as_array().unwrap() {
            assert!(connector_ids.contains(&connector_id.as_str().unwrap().to_owned()));
        }
        if dynamic_class["id"] == "postgresql.other_defined_catalog_types" {
            assert!(
                dynamic_class["catalog_query"]
                    .as_str()
                    .unwrap()
                    .contains("n.nspname = 'pg_catalog'"),
                "the catch-all catalog scan must be limited to pg_catalog"
            );
        }
    }
    assert!(
        inventory["excluded_type_classes"]
            .as_array()
            .is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item["id"] == "postgresql.pseudotypes")
            })
    );

    let inventory_cases: BTreeSet<_> = inventory["qualification_case_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    let qualification_entries = inventory["per_type_qualification"]["entries"]
        .as_object()
        .unwrap();
    for key in qualification_entries.keys() {
        let (type_id, source_id) = key
            .rsplit_once('@')
            .unwrap_or_else(|| panic!("invalid per-type qualification key {key}"));
        let declaration = inventory["types"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == type_id)
            .unwrap_or_else(|| panic!("orphan per-type qualification receipt {key}"));
        let profile = &inventory["native_declaration_profiles"]
            [declaration["declaration_profile"].as_str().unwrap()];
        assert!(
            profile["connectors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|connector| connector == source_id),
            "qualification receipt {key} is not a declared source version"
        );
    }
    let actual_cases: BTreeSet<_> = cases().iter().map(|case| case.id).collect();
    assert_eq!(
        inventory_cases, actual_cases,
        "Rust fixture IDs and inventory must evolve together"
    );
    for entry in inventory["types"].as_array().unwrap() {
        for case_id in entry["qualification_case_ids"].as_array().unwrap() {
            assert!(
                inventory_cases.contains(case_id.as_str().unwrap()),
                "stale fixture reference in {}",
                entry["id"]
            );
        }
    }

    let coverage = type_inventory_coverage();
    assert_eq!(coverage["source_declaration_count"], declaration_count);
    assert!(coverage["source_mapping_gaps"].as_u64().unwrap() > 0);
    assert_eq!(coverage["schema"], "cdc.type_inventory.v1");
    assert_eq!(coverage["status"], "MISSING_IMPLEMENTATION");
    assert_eq!(
        inventory_status(false, false),
        "REQUIRES_PER_TYPE_QUALIFICATION"
    );
    assert_eq!(inventory_status(false, true), "PASS");
    assert_eq!(inventory_status(true, true), "MISSING_IMPLEMENTATION");
    for declaration in coverage["declarations"].as_array().unwrap() {
        let source_id = declaration["source"].as_str().unwrap();
        assert!(
            declaration["source_type_identity"]["kind"]
                .as_str()
                .is_some()
        );
        assert!(declaration["source_mapping_code_seam"].as_array().is_some());
        assert_eq!(
            declaration["per_type_live_evidence"]["source"],
            "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
        );
        assert_eq!(
            declaration["per_type_live_evidence"]["all_sinks"],
            "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
        );
        assert_eq!(
            declaration["per_type_web_status"],
            "NOT_QUALIFIED_PER_NATIVE_TYPE"
        );
        let protocol = &declaration["protocol_framing_profile"];
        assert_eq!(protocol["status"], "MISSING_IMPLEMENTATION");
        assert!(
            protocol["framing_requirements"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        );
        assert_eq!(
            declaration["source_representation_envelope_schema"],
            "cdc.source_representation_envelope.v1"
        );
        assert_eq!(
            declaration["sink_representation_outcomes"],
            inventory["sink_representation_outcomes"]
        );
        assert_eq!(
            declaration["web_representation_policy"],
            inventory["web_representation_policy"]
        );
        assert_eq!(
            protocol,
            &inventory["source_representation_contract"]["protocol_profiles"][source_id]
        );
    }
    for (profile_id, aliases) in inventory["mapping_aliases"].as_object().unwrap() {
        assert!(
            inventory["native_declaration_profiles"]
                .get(profile_id)
                .is_some()
        );
        assert!(aliases.as_array().is_some_and(|items| !items.is_empty()));
    }
}

#[test]
fn per_type_completion_requires_source_all_sinks_web_and_live_evidence() {
    let sink_ids = versions();
    let type_id = "example.native_type";
    let source_id = "mysql_5_7";
    let evidence_registry = qualified_type_evidence_registry(type_id, source_id, &sink_ids);
    let qualified = qualified_type_evidence_fixture(type_id, source_id, &sink_ids);
    assert_eq!(
        per_type_source_live_status(&qualified, &evidence_registry, type_id, source_id),
        "PASS"
    );
    assert_eq!(
        per_type_sink_live_status(
            &qualified,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "PASS"
    );
    assert_eq!(
        per_type_web_status(&qualified, &evidence_registry, type_id, source_id),
        "PASS"
    );
    assert!(per_type_qualification_pass(
        &qualified,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        evidence_registry["entries"][0]["artifact_path"]
            .as_str()
            .unwrap(),
    );
    let original_artifact = std::fs::read(&artifact_path).unwrap();
    let mut tampered_artifact: Value = serde_json::from_slice(&original_artifact).unwrap();
    tampered_artifact["evidence"][0]["status"] = json!("FAIL");
    std::fs::write(
        &artifact_path,
        serde_json::to_vec_pretty(&tampered_artifact).unwrap(),
    )
    .unwrap();
    let tampered_report_accepted = per_type_qualification_pass(
        &qualified,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids,
    );
    std::fs::write(&artifact_path, original_artifact).unwrap();
    assert!(!tampered_report_accepted);

    let mut unregistered_evidence = qualified.clone();
    unregistered_evidence["source"]["protocol_capture"]["evidence"] = json!(["invented-test-id"]);
    unregistered_evidence["source"]["live"]["evidence"] = json!(["invented-live-test-id"]);
    assert!(!per_type_qualification_pass(
        &unregistered_evidence,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    assert_eq!(
        per_type_source_live_status(
            &unregistered_evidence,
            &evidence_registry,
            type_id,
            source_id
        ),
        "REQUIRES_PER_TYPE_LIVE_EVIDENCE"
    );

    let mut wrong_scope_registry = evidence_registry.clone();
    wrong_scope_registry["entries"][0]["type_id"] = json!("another.native_type");
    assert!(!per_type_qualification_pass(
        &qualified,
        &wrong_scope_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut wrong_source_registry = evidence_registry.clone();
    wrong_source_registry["entries"][0]["source_connector_id"] = json!("mysql_8_0");
    assert!(!per_type_qualification_pass(
        &qualified,
        &wrong_source_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut wrong_axis_registry = evidence_registry.clone();
    wrong_axis_registry["entries"][0]["axis"] = json!("source.live");
    assert!(!per_type_qualification_pass(
        &qualified,
        &wrong_axis_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut missing_run_id_registry = evidence_registry.clone();
    missing_run_id_registry["entries"][0]["run_id"] = json!("");
    assert!(!per_type_qualification_pass(
        &qualified,
        &missing_run_id_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut whitespace_run_id_registry = evidence_registry.clone();
    whitespace_run_id_registry["entries"][0]["run_id"] = json!("   ");
    assert!(!per_type_qualification_pass(
        &qualified,
        &whitespace_run_id_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut invalid_digest_registry = evidence_registry.clone();
    invalid_digest_registry["entries"][0]["report_digest"] = json!("not-a-sha256-digest");
    assert!(!per_type_qualification_pass(
        &qualified,
        &invalid_digest_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut wrong_but_well_formed_digest_registry = evidence_registry.clone();
    wrong_but_well_formed_digest_registry["entries"][0]["report_digest"] =
        json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    assert!(!per_type_qualification_pass(
        &qualified,
        &wrong_but_well_formed_digest_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut mismatched_run_registry = evidence_registry.clone();
    mismatched_run_registry["entries"][0]["run_id"] = json!("different-run-id");
    assert!(!per_type_qualification_pass(
        &qualified,
        &mismatched_run_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut escaping_artifact_registry = evidence_registry.clone();
    escaping_artifact_registry["entries"][0]["artifact_path"] = json!("../../Cargo.toml");
    assert!(!per_type_qualification_pass(
        &qualified,
        &escaping_artifact_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut duplicate_evidence_id_registry = evidence_registry.clone();
    let duplicate = duplicate_evidence_id_registry["entries"][0].clone();
    duplicate_evidence_id_registry["entries"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(!per_type_qualification_pass(
        &qualified,
        &duplicate_evidence_id_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut wrong_sink_registry = evidence_registry.clone();
    let record = wrong_sink_registry["entries"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| record["axis"] == "sink.live")
        .unwrap();
    record["sink_connector_id"] = json!("mysql_8_4");
    assert!(!per_type_qualification_pass(
        &qualified,
        &wrong_sink_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut missing_sink = qualified.clone();
    missing_sink["sinks"]
        .as_object_mut()
        .unwrap()
        .remove("mysql_8_4");
    assert!(!per_type_qualification_pass(
        &missing_sink,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut missing_live_evidence = qualified.clone();
    missing_live_evidence["sinks"]["postgresql_17"]["live_evidence"] = json!([]);
    assert!(!per_type_qualification_pass(
        &missing_live_evidence,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut representation_only = qualified.clone();
    representation_only["sinks"]["postgresql_17"]["outcome"] =
        json!("SOURCE_REPRESENTATION_PRESERVED");
    representation_only["source"]["source_representation_capture"] = Value::Null;
    representation_only["source"]["protocol_framing"] = Value::Null;
    assert!(!per_type_qualification_pass(
        &representation_only,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    representation_only["source"]["source_representation_capture"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.source_representation_capture")] });
    representation_only["source"]["protocol_framing"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.protocol_framing")] });
    assert!(per_type_qualification_pass(
        &representation_only,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let mut missing_representation_readback = representation_only.clone();
    missing_representation_readback["sinks"]["postgresql_17"]
        .as_object_mut()
        .unwrap()
        .remove("representation_preservation_evidence");
    assert!(!per_type_qualification_pass(
        &missing_representation_readback,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let scoped_receipts = Value::Object(serde_json::Map::from_iter([(
        format!("{type_id}@{source_id}"),
        representation_only,
    )]));
    assert_eq!(
        representation_scopes(&scoped_receipts),
        (
            BTreeSet::from([source_id.to_owned()]),
            BTreeSet::from(["postgresql_17".to_owned()])
        )
    );

    let mut inventory = type_inventory().clone();
    inventory["per_type_qualification"]["evidence_registry"] = evidence_registry.clone();
    let key = format!("{type_id}@{source_id}");
    inventory["per_type_qualification"]["entries"]
        .as_object_mut()
        .unwrap()
        .insert(key, qualified);
    let entry = json!({ "id": type_id, "qualification_case_ids": ["integer"] });
    assert_eq!(
        qualification_fixture_status(&inventory, &entry, source_id),
        "PER_TYPE_QUALIFIED"
    );
    let mut no_fixture = entry.clone();
    no_fixture["qualification_case_ids"] = json!([]);
    assert_eq!(
        qualification_fixture_status(&inventory, &no_fixture, source_id),
        "MISSING_TEST"
    );
    let artifact = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        evidence_registry["entries"][0]["artifact_path"]
            .as_str()
            .unwrap(),
    );
    std::fs::remove_file(&artifact).expect("remove ignored qualification evidence fixture");
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
    let report = json!({
        "schema":"cdc.qualification.v2",
        "evidence_scope":"offline_fixture",
        "live_semantics":"adapter_components_only",
        "type_inventory":type_inventory_coverage(),
        "source_capture_edge_fixtures":[zero_date_fixture_evidence()],
        "directions":directions
    });
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
    assert_eq!(
        report["source_capture_edge_fixtures"][0]["fixture"],
        "mysql_zero_date"
    );
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
    let integer_range = direction["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == "integer_range")
        .expect("integer range evidence is present");
    assert_eq!(integer_range["evidence_outcome"], "Value Preserved");
    assert_eq!(integer_range["qualification"], "RANGE_CHECKED");
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
    assert_eq!(
        live["source_fixture_roster"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect::<Vec<_>>(),
        ids.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(live["source_fixture_roster"].as_array().unwrap().len(), 6);
    assert_eq!(live["sources"].as_array().unwrap().len(), 6);
    assert_eq!(live["sinks"].as_array().unwrap().len(), 6);
    for component in live["sources"]
        .as_array()
        .unwrap()
        .iter()
        .chain(live["sinks"].as_array().unwrap())
    {
        assert!(ids.contains(&component["database"].as_str().unwrap().to_owned()));
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
