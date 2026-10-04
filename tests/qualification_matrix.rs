//! Executed offline evidence; never a claim about an external database build.
#![recursion_limit = "256"]

#[allow(dead_code)]
#[path = "support/matrix_fixture.rs"]
mod fixture;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
        "ceefa2d10778e4146e4a847be4a6972c183f8470b93c8c5b5e2404cbb2bff2f6",
    ),
    (
        "postgresql_16",
        "d2c92ff779a5f87ac545bfb82590d629489226fc5ffd31bb8b8ff103f545ab18",
    ),
    (
        "postgresql_17",
        "f9e9dcf6760273d259aa58e9b1678668299cfb22dadad6361387339fb07bc58e",
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
        let mut types = [
            ("bool", 16),
            ("bytea", 17),
            ("char", 18),
            ("name", 19),
            ("int8", 20),
            ("int2", 21),
            ("int2vector", 22),
            ("int4", 23),
            ("regproc", 24),
            ("text", 25),
            ("oid", 26),
            ("tid", 27),
            ("xid", 28),
            ("cid", 29),
            ("oidvector", 30),
            ("json", 114),
            ("xml", 142),
            ("point", 600),
            ("lseg", 601),
            ("path", 602),
            ("box", 603),
            ("polygon", 604),
            ("line", 628),
            ("cidr", 650),
            ("float4", 700),
            ("float8", 701),
            ("circle", 718),
            ("macaddr8", 774),
            ("money", 790),
            ("macaddr", 829),
            ("inet", 869),
            ("bpchar", 1042),
            ("varchar", 1043),
            ("date", 1082),
            ("time", 1083),
            ("timestamp", 1114),
            ("timestamptz", 1184),
            ("interval", 1186),
            ("timetz", 1266),
            ("bit", 1560),
            ("varbit", 1562),
            ("numeric", 1700),
            ("regprocedure", 2202),
            ("regoper", 2203),
            ("regoperator", 2204),
            ("regclass", 2205),
            ("regtype", 2206),
            ("uuid", 2950),
            ("txid_snapshot", 2970),
            ("pg_lsn", 3220),
            ("tsvector", 3614),
            ("tsquery", 3615),
            ("regconfig", 3734),
            ("regdictionary", 3769),
            ("jsonb", 3802),
            ("regnamespace", 4089),
            ("regrole", 4096),
            ("regcollation", 4191),
            ("pg_snapshot", 5038),
            ("xid8", 5069),
        ]
        .into_iter()
        .map(|(name, oid)| SourceTypeDefinition::builtin(oid, "pg_catalog", name))
        .collect::<Vec<_>>();
        types.extend([
            SourceTypeDefinition::range(3904, "pg_catalog", "int4range", 23),
            SourceTypeDefinition::range(3926, "pg_catalog", "int8range", 20),
            SourceTypeDefinition::range(3906, "pg_catalog", "numrange", 1700),
            SourceTypeDefinition::range(3908, "pg_catalog", "tsrange", 1114),
            SourceTypeDefinition::range(3910, "pg_catalog", "tstzrange", 1184),
            SourceTypeDefinition::range(3912, "pg_catalog", "daterange", 1082),
        ]);
        types.extend([
            SourceTypeDefinition::multi_range(4451, "pg_catalog", "int4multirange", 3904),
            SourceTypeDefinition::multi_range(4536, "pg_catalog", "int8multirange", 3926),
            SourceTypeDefinition::multi_range(4532, "pg_catalog", "nummultirange", 3906),
            SourceTypeDefinition::multi_range(4533, "pg_catalog", "tsmultirange", 3908),
            SourceTypeDefinition::multi_range(4534, "pg_catalog", "tstzmultirange", 3910),
            SourceTypeDefinition::multi_range(4535, "pg_catalog", "datemultirange", 3912),
        ]);
        types.extend([
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
        ]);
        SourceTypeCatalog::with_extensions(
            types,
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
        LogicalType::VariableBitString { max_length } => {
            let bit_length = max_length.map_or(3, |maximum| maximum.min(3)).max(1);
            LogicalValue::BitString {
                bytes_base64url: URL_SAFE_NO_PAD.encode([0]),
                bit_length,
                padding: BitPadding::Zero,
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

fn apply_source_value_representation(value: &mut LogicalValue, mapping: &SourceTypeMapping) {
    if let LogicalValue::BitString { bit_order, .. } = value {
        *bit_order = match mapping
            .value_representation
            .get("bit_order")
            .map(String::as_str)
        {
            Some("lsb_first") => BitOrder::LsbFirst,
            Some("msb_first") => BitOrder::MsbFirst,
            _ => *bit_order,
        };
    }
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
    assert!(LogicalType::Date.matches_value(&value));

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

#[test]
fn mysql_invalid_temporal_values_match_only_their_declared_temporal_family() {
    let invalid = |kind: &str| LogicalValue::InvalidTemporal {
        kind: kind.into(),
        raw: "0000-00-00".into(),
    };
    assert!(LogicalType::Date.matches_value(&invalid("mysql.date")));
    assert!(
        LogicalType::LocalDatetime {
            fractional_precision: 6
        }
        .matches_value(&invalid("mysql.datetime"))
    );
    assert!(
        LogicalType::Instant {
            fractional_precision: 6
        }
        .matches_value(&invalid("mysql.timestamp"))
    );
    assert!(!LogicalType::Date.matches_value(&invalid("mysql.datetime")));
    assert!(
        !LogicalType::LocalDatetime {
            fractional_precision: 6
        }
        .matches_value(&invalid("mysql.timestamp"))
    );
    assert!(
        !LogicalType::Instant {
            fractional_precision: 6
        }
        .matches_value(&invalid("mysql.date"))
    );
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

fn merge_live_source_evidence(inventory: &mut Value) {
    let Some(directories) = std::env::var_os("CDC_TYPE_QUALIFICATION_ARTIFACT_DIR") else {
        return;
    };
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("qualification workspace exists");
    for directory in std::env::split_paths(&directories) {
        if directory.exists() {
            merge_live_type_evidence_from_directory(inventory, &directory, &workspace);
        }
    }
}

fn index_type_evidence(inventory: &mut Value) {
    let registry = &mut inventory["per_type_qualification"]["evidence_registry"];
    let entries = registry["entries"]
        .as_array()
        .expect("type evidence entries");
    let mut by_id = serde_json::Map::new();
    for (index, record) in entries.iter().enumerate() {
        let id = record["id"].as_str().expect("type evidence record id");
        assert!(
            by_id.insert(id.to_owned(), json!(index)).is_none(),
            "duplicate type evidence record id {id}"
        );
    }
    registry["by_id"] = Value::Object(by_id);
}

fn type_evidence_record<'a>(registry: &'a Value, id: &str) -> Option<&'a Value> {
    let entries = registry["entries"].as_array()?;
    if let Some(by_id) = registry["by_id"].as_object() {
        let index = usize::try_from(by_id.get(id)?.as_u64()?).ok()?;
        let record = entries.get(index)?;
        return (record["id"] == id).then_some(record);
    }
    let mut matching = entries.iter().filter(|record| record["id"] == id);
    let record = matching.next()?;
    matching.next().is_none().then_some(record)
}

fn type_evidence_identity_payload(evidence: &Value, suite_id: &Value) -> Value {
    let mut payload = evidence.clone();
    if let Some(fields) = payload.as_object_mut() {
        fields.remove("artifact_path");
        fields.remove("report_digest");
        fields.remove("suite_id");
        fields.insert("suite_id".to_owned(), suite_id.clone());
    }
    if payload["target_storage_mode"].is_null()
        && let Some(mode) = inferred_target_storage_mode(evidence, suite_id)
    {
        payload["target_storage_mode"] = json!(mode);
    }
    payload
}

fn verified_web_plan_evidence(evidence: &Value) -> bool {
    let digest = |name: &str| {
        evidence[name].as_str().is_some_and(|value| {
            value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    };
    let live_web_flow = evidence["verification"] == "web_ui.preview_save_start_gate"
        && evidence["preview_status"] == "COMPATIBLE"
        && evidence["save_status"] == "PASS"
        && evidence["start_gate_status"] == "PASS";
    live_web_flow
        && evidence["sink_connector_id"]
            .as_str()
            .is_some_and(|sink| !sink.is_empty())
        && digest("plan_digest")
        && digest("target_probe_digest")
        && evidence["selected_rule_id"]
            .as_str()
            .is_some_and(|rule| !rule.is_empty())
        && matches!(
            evidence["target_storage_mode"].as_str(),
            Some(
                "native_target_column"
                    | "logical_value_json_carrier"
                    | "source_representation_blob_carrier"
            )
        )
        && evidence["risk_confirmation_required"].as_bool().is_some()
        && evidence["risk_confirmation_verified"].as_bool().is_some()
        && (evidence["risk_confirmation_required"] == false
            || evidence["risk_confirmation_verified"] == true)
}

fn dynamic_web_plan_coverage(
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut qualified = Vec::new();
    let mut missing = Vec::new();
    let mut evidence = Vec::new();
    for sink_id in sink_ids {
        let ids = evidence_registry["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|record| {
                record["type_id"] == type_id
                    && record["source_connector_id"] == source_id
                    && record["sink_connector_id"] == sink_id.as_str()
                    && record["axis"] == "web.plan"
            })
            .filter_map(|record| record["id"].as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        if evidence_axis_pass(
            &json!({"status": "PASS", "evidence": ids}),
            evidence_registry,
            type_id,
            source_id,
            Some(sink_id),
            "web.plan",
        ) {
            qualified.push(sink_id.clone());
            evidence.extend(ids);
        } else {
            missing.push(sink_id.clone());
        }
    }
    (qualified, missing, evidence)
}

fn validate_exact_catalog_type_inventory_artifact(artifact: &Value) -> Result<(), String> {
    if artifact["catalog_inventory_mode"] != "per_catalog_type_instance" {
        let is_complete_postgresql_catalog_scan =
            artifact["suite_id"].as_str().is_some_and(|suite| {
                suite.starts_with("postgresql_")
                    && suite.ends_with(".complete_defined_catalog_type_mapping")
            });
        let has_exact_postgresql_mapping_receipt =
            artifact["evidence"].as_array().is_some_and(|records| {
                records.iter().any(|record| {
                    record["axis"] == "source.catalog_type_mapping"
                        && record["source_connector_id"]
                            .as_str()
                            .is_some_and(|connector| connector.starts_with("postgresql_"))
                        && [
                            "catalog_type_count",
                            "catalog_scope",
                            "catalog_digest",
                            "type_oid",
                            "catalog_class_id",
                        ]
                        .into_iter()
                        .all(|field| !record[field].is_null())
                })
            });
        let has_postgresql_exact_roster =
            artifact["catalog_type_roster"]
                .as_array()
                .is_some_and(|roster| {
                    roster.iter().any(|definition| {
                        definition["type_id"]
                            .as_str()
                            .is_some_and(|id| id.starts_with("dynamic:postgresql.instance."))
                            && definition["catalog_class_id"]
                                .as_str()
                                .is_some_and(|class| class.starts_with("postgresql."))
                    })
                });
        let has_catalog_snapshot_metadata = artifact["catalog_type_count"].as_u64().is_some()
            && artifact["catalog_scope"]
                .as_str()
                .is_some_and(|scope| !scope.is_empty())
            && artifact["catalog_digest"].as_str().is_some_and(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
        if is_complete_postgresql_catalog_scan
            || has_exact_postgresql_mapping_receipt
            || (has_postgresql_exact_roster && has_catalog_snapshot_metadata)
        {
            return Err(
                "complete PostgreSQL catalog inventory is missing its exact-inventory mode"
                    .to_owned(),
            );
        }
        return Ok(());
    }
    let expected_count = artifact["catalog_type_count"]
        .as_u64()
        .ok_or_else(|| "exact catalog inventory has no type count".to_owned())?;
    let roster = artifact["catalog_type_roster"]
        .as_array()
        .ok_or_else(|| "exact catalog inventory has no type roster".to_owned())?;
    let evidence = artifact["evidence"]
        .as_array()
        .ok_or_else(|| "exact catalog inventory has no mapping evidence".to_owned())?;
    let run_id = artifact["run_id"]
        .as_str()
        .ok_or_else(|| "exact catalog inventory has no run id".to_owned())?;
    let connector_id = evidence
        .first()
        .and_then(|record| record["source_connector_id"].as_str())
        .ok_or_else(|| "exact catalog inventory has no source connector".to_owned())?;
    let catalog_digest = artifact["catalog_digest"]
        .as_str()
        .ok_or_else(|| "exact catalog inventory has no catalog digest".to_owned())?;
    let catalog_scope = artifact["catalog_scope"]
        .as_str()
        .ok_or_else(|| "exact catalog inventory has no catalog scope".to_owned())?;
    let declared_non_storable_count = artifact["non_storable_type_count"]
        .as_u64()
        .ok_or_else(|| "exact catalog inventory has no non-storable type count".to_owned())?;
    if expected_count == 0
        || roster.len() as u64 != expected_count
        || evidence.len() as u64 != expected_count
        || catalog_digest.len() != 64
        || !catalog_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || catalog_scope.is_empty()
    {
        return Err("exact catalog inventory count or digest does not match its roster".to_owned());
    }
    if roster
        .iter()
        .filter(|definition| definition["user_storable"] == false)
        .count() as u64
        != declared_non_storable_count
    {
        return Err("catalog non-storable count does not match its exact roster".to_owned());
    }

    let mut roster_by_type = BTreeMap::new();
    for definition in roster {
        let type_id = definition["type_id"]
            .as_str()
            .ok_or_else(|| "catalog roster entry has no type id".to_owned())?;
        let schema = definition["schema"]
            .as_str()
            .ok_or_else(|| "catalog roster entry has no schema".to_owned())?;
        let name = definition["name"]
            .as_str()
            .ok_or_else(|| "catalog roster entry has no name".to_owned())?;
        let definition_digest = definition["definition_digest"]
            .as_str()
            .ok_or_else(|| "catalog roster entry has no definition digest".to_owned())?;
        let expected_type_id =
            format!("dynamic:postgresql.instance.{schema}.{name}:{definition_digest}");
        if schema.is_empty()
            || name.is_empty()
            || definition_digest.len() != 64
            || !definition_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || type_id != expected_type_id
            || definition["catalog_class_id"].as_str().is_none()
            || definition["mapping_id"].as_str().is_none()
            || definition["native_declaration"]
                .as_str()
                .is_none_or(str::is_empty)
            || definition["representation_mode"].as_str().is_none()
            || definition["logical_type_digest"]
                .as_str()
                .is_none_or(|digest| {
                    digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            || definition["mapping_evidence_digest"]
                .as_str()
                .is_none_or(|digest| {
                    digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            || !definition["user_storable"].is_boolean()
            || (definition["user_storable"] == false
                && definition["non_storable_reason"]
                    .as_str()
                    .is_none_or(str::is_empty))
            || (definition["user_storable"] == true && !definition["non_storable_reason"].is_null())
            || roster_by_type.insert(type_id, definition).is_some()
        {
            return Err(
                "catalog roster contains an incomplete or duplicate type definition".to_owned(),
            );
        }
    }

    let mut evidence_by_type = BTreeSet::new();
    for record in evidence {
        let type_id = record["type_id"]
            .as_str()
            .ok_or_else(|| "catalog mapping record has no type id".to_owned())?;
        let Some(definition) = roster_by_type.get(type_id) else {
            return Err(
                "catalog mapping evidence references an unknown type definition".to_owned(),
            );
        };
        if record["axis"] != "source.catalog_type_mapping"
            || record["status"] != "PASS"
            || record["run_id"] != run_id
            || record["source_connector_id"] != connector_id
            || !record["sink_connector_id"].is_null()
            || record["catalog_digest"] != catalog_digest
            || record["schema"] != definition["schema"]
            || record["name"] != definition["name"]
            || record["type_oid"] != definition["type_oid"]
            || record["native_declaration"] != definition["native_declaration"]
            || record["catalog_class_id"] != definition["catalog_class_id"]
            || record["definition_digest"] != definition["definition_digest"]
            || record["mapping_id"] != definition["mapping_id"]
            || record["mapping_evidence_digest"] != definition["mapping_evidence_digest"]
            || record["logical_type_digest"] != definition["logical_type_digest"]
            || record["representation_mode"] != definition["representation_mode"]
            || record["user_storable"] != definition["user_storable"]
            || record["non_storable_reason"] != definition["non_storable_reason"]
            || record["catalog_type_count"] != artifact["catalog_type_count"]
            || record["non_storable_type_count"] != artifact["non_storable_type_count"]
            || record["catalog_scope"] != artifact["catalog_scope"]
            || !evidence_by_type.insert(type_id)
        {
            return Err(
                "catalog mapping receipt does not match its exact type definition".to_owned(),
            );
        }
    }
    if evidence_by_type.len() as u64 != expected_count {
        return Err("exact catalog inventory is missing per-type mapping receipts".to_owned());
    }
    Ok(())
}

fn merge_live_type_evidence_from_directory(
    inventory: &mut Value,
    directory: &Path,
    workspace: &Path,
) {
    let mut artifact_files = std::fs::read_dir(directory)
        .expect("read type qualification evidence directory")
        .map(|entry| entry.expect("read type evidence entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    artifact_files.sort();

    let allowed_axes = inventory["per_type_qualification"]["evidence_registry"]["allowed_axes"]
        .as_array()
        .expect("allowed qualification evidence axes")
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    let mut entry_updates = Vec::new();
    let mut registry_updates = Vec::new();
    let mut ignored_synthetic_web_evidence = 0_u64;
    let mut seen_evidence = BTreeMap::new();
    for record in inventory["per_type_qualification"]["evidence_registry"]["entries"]
        .as_array()
        .expect("qualification evidence registry entries")
    {
        let id = record["id"].as_str().expect("type evidence record id");
        let payload = type_evidence_identity_payload(record, &record["suite_id"]);
        if let Some(previous) = seen_evidence.insert(id.to_owned(), payload.clone()) {
            assert_eq!(
                previous, payload,
                "conflicting duplicate type evidence record id {id}"
            );
        }
    }

    for artifact_file in artifact_files {
        let artifact_file = artifact_file
            .canonicalize()
            .expect("qualification evidence artifact exists");
        let relative_path = artifact_file
            .strip_prefix(workspace)
            .expect("type evidence artifact is inside the workspace")
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let artifact: Value = serde_json::from_slice(
            &std::fs::read(&artifact_file).expect("read type evidence artifact"),
        )
        .expect("type evidence artifact is valid JSON");
        assert_eq!(
            artifact["schema"], "cdc.type_qualification_evidence_report.v1",
            "unexpected per-type evidence artifact schema"
        );
        assert_eq!(
            artifact["artifact_path"], relative_path,
            "type evidence artifact path must be bound to its report"
        );
        validate_exact_catalog_type_inventory_artifact(&artifact).unwrap_or_else(|error| {
            panic!("invalid exact catalog inventory in {relative_path}: {error}")
        });
        let report_digest = change_event::stable_digest(&artifact);
        let run_id = artifact["run_id"]
            .as_str()
            .expect("type evidence report run id");
        for evidence in artifact["evidence"]
            .as_array()
            .expect("type evidence records")
        {
            let axis = evidence["axis"].as_str().expect("evidence axis");
            assert!(
                allowed_axes.contains(axis),
                "unregistered type evidence axis {axis}"
            );
            assert_eq!(evidence["status"], "PASS");
            assert_eq!(evidence["run_id"], run_id);
            let type_id = evidence["type_id"].as_str().expect("evidence type id");
            let source_id = evidence["source_connector_id"]
                .as_str()
                .expect("evidence source connector");
            let id = evidence["id"].as_str().expect("evidence record id");
            let identity_payload = type_evidence_identity_payload(evidence, &artifact["suite_id"]);
            if let Some(previous) = seen_evidence.get(id) {
                assert_eq!(
                    previous, &identity_payload,
                    "conflicting duplicate type evidence record id {id}"
                );
                continue;
            }
            seen_evidence.insert(id.to_owned(), identity_payload);
            if axis == "web.plan" && !verified_web_plan_evidence(evidence) {
                ignored_synthetic_web_evidence += 1;
                continue;
            }
            let entry_key = format!("{type_id}@{source_id}");
            if let Some(source_axis) = axis.strip_prefix("source.") {
                if source_axis == "null_only" {
                    assert_eq!(
                        evidence["value_coverage"], "NULL_ONLY",
                        "source.null_only evidence must explicitly limit coverage to NULL values"
                    );
                }
                assert_eq!(evidence["sink_connector_id"], Value::Null);
                entry_updates.push((entry_key, None, source_axis.to_owned(), id.to_owned(), None));
            } else if axis == "web.plan" {
                let sink_id = evidence["sink_connector_id"]
                    .as_str()
                    .expect("Web plan evidence must identify the target connector");
                entry_updates.push((
                    entry_key,
                    Some(sink_id.to_owned()),
                    "web.plan".to_owned(),
                    id.to_owned(),
                    None,
                ));
            } else {
                let evidence_field = match axis {
                    "sink.offline" => "offline_evidence",
                    "sink.live" => "live_evidence",
                    "sink.representation_carrier" => "representation_carrier_evidence",
                    "sink.representation_preserved" => "representation_preservation_evidence",
                    other => panic!("unexpected sink evidence axis {other}"),
                };
                let sink_id = evidence["sink_connector_id"]
                    .as_str()
                    .expect("sink evidence connector");
                let outcome = evidence["outcome"].as_str().unwrap_or("VALUE_PRESERVED");
                entry_updates.push((
                    entry_key,
                    Some(sink_id.to_owned()),
                    evidence_field.to_owned(),
                    id.to_owned(),
                    (axis == "sink.live").then(|| outcome.to_owned()),
                ));
            }

            let mut registry_record = evidence.clone();
            registry_record["artifact_path"] = json!(relative_path);
            registry_record["report_digest"] = json!(report_digest);
            registry_record["suite_id"] = artifact["suite_id"].clone();
            if let Some(mode) = inferred_target_storage_mode(evidence, &artifact["suite_id"]) {
                assert!(
                    evidence["target_storage_mode"].is_null()
                        || evidence["target_storage_mode"] == mode,
                    "type evidence target storage mode mismatch: type={}, source={}, sink={}, suite={}, evidence_mode={}, inferred_mode={mode}",
                    evidence["type_id"].as_str().unwrap_or("unknown"),
                    source_id,
                    evidence["sink_connector_id"]
                        .as_str()
                        .unwrap_or("source-only"),
                    artifact["suite_id"].as_str().unwrap_or("unknown"),
                    evidence["target_storage_mode"]
                        .as_str()
                        .unwrap_or("unspecified")
                );
                registry_record["target_storage_mode"] = json!(mode);
            }
            registry_updates.push(registry_record);
        }
    }

    let qualification_entries = inventory["per_type_qualification"]["entries"]
        .as_object_mut()
        .expect("per-type qualification entries");
    for (entry_key, sink_id, axis, id, outcome) in entry_updates {
        let qualification_entry = qualification_entries
            .entry(entry_key)
            .or_insert_with(|| json!({"source": {}, "sinks": {}}));
        if axis == "web.plan" {
            let sink_id = sink_id.expect("Web evidence target connector");
            qualification_entry["web"]["selection"] =
                json!("explicit_per_field_user_choice_required");
            qualification_entry["web"]["risk_confirmation"] =
                json!("required_before_plan_activation");
            qualification_entry["web"]["sinks"][sink_id.as_str()] = json!({
                "status": "PASS",
                "evidence": [id]
            });
        } else if let Some(sink_id) = sink_id {
            let sink = qualification_entry["sinks"]
                .as_object_mut()
                .expect("per-type sink evidence map")
                .entry(sink_id)
                .or_insert_with(|| json!({}));
            let ids = sink[axis.as_str()].as_array_mut();
            if let Some(ids) = ids {
                ids.push(json!(id));
            } else {
                sink[axis.as_str()] = json!([id]);
            }
            if let Some(outcome) = outcome {
                merge_sink_outcome(sink, &outcome);
            }
        } else {
            qualification_entry["source"][axis.as_str()] = json!({
                "status": "PASS",
                "evidence": [id]
            });
            if axis == "null_only" {
                qualification_entry["source"][axis]["value_coverage"] = json!("NULL_ONLY");
            }
        }
    }
    inventory["per_type_qualification"]["evidence_registry"]["entries"]
        .as_array_mut()
        .expect("qualification evidence registry entries")
        .extend(registry_updates);
    let ignored = &mut inventory["per_type_qualification"]["ignored_synthetic_web_evidence"];
    *ignored = json!(ignored.as_u64().unwrap_or(0) + ignored_synthetic_web_evidence);
}

fn merge_sink_outcome(sink: &mut Value, outcome: &str) {
    assert!(
        matches!(
            outcome,
            "VALUE_PRESERVED" | "SOURCE_REPRESENTATION_PRESERVED" | "NULL_PRESERVED"
        ),
        "unrecognized sink qualification outcome {outcome}"
    );
    let merged_outcome = match sink["outcome"].as_str() {
        None => outcome.to_owned(),
        Some(previous) if previous == outcome => previous.to_owned(),
        Some("NULL_PRESERVED") if outcome != "NULL_PRESERVED" => outcome.to_owned(),
        Some(previous) if outcome == "NULL_PRESERVED" => previous.to_owned(),
        Some(previous) => panic!(
            "per-type sink qualification has incompatible value-preservation outcomes: {previous} vs {outcome}"
        ),
    };
    sink["outcome"] = json!(merged_outcome.clone());
    // Keep the descriptive field for existing report consumers while the
    // qualification gate reads the canonical `outcome` field.
    sink["live_outcome"] = json!(merged_outcome);
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
    axis["status"] == "PASS"
        && axis["evidence"].as_array().is_some_and(|items| {
            !items.is_empty()
                && items.iter().all(|item| {
                    let Some(evidence_id) = item.as_str() else {
                        return false;
                    };
                    let Some(record) = type_evidence_record(evidence_registry, evidence_id) else {
                        return false;
                    };
                    evidence_artifact_passes(record) && {
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

fn evidence_target_storage_modes(
    evidence_ids: &Value,
    evidence_registry: &Value,
) -> BTreeSet<String> {
    let Some(evidence_ids) = evidence_ids.as_array() else {
        return BTreeSet::new();
    };
    evidence_ids
        .iter()
        .filter_map(Value::as_str)
        .filter_map(|id| type_evidence_record(evidence_registry, id))
        .filter_map(|record| record["target_storage_mode"].as_str().map(str::to_owned))
        .collect()
}

struct EvidenceStorageModeExpectation<'a> {
    outcome: &'a str,
    target_storage_mode: &'a str,
}

fn evidence_axis_storage_mode_pass(
    axis: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_id: &str,
    axis_name: &str,
    expectation: EvidenceStorageModeExpectation<'_>,
) -> bool {
    if !evidence_axis_pass(
        axis,
        evidence_registry,
        type_id,
        source_id,
        Some(sink_id),
        axis_name,
    ) {
        return false;
    }
    let Some(ids) = axis["evidence"].as_array() else {
        return false;
    };
    !ids.is_empty()
        && ids.iter().any(|id| {
            let Some(id) = id.as_str() else {
                return false;
            };
            type_evidence_record(evidence_registry, id).is_some_and(|record| {
                record["outcome"] == expectation.outcome
                    && record["target_storage_mode"] == expectation.target_storage_mode
                    && (axis_name != "sink.live" || record["target_readback_status"] == "PASS")
            })
        })
}

fn inferred_target_storage_mode(evidence: &Value, suite_id: &Value) -> Option<&'static str> {
    if !matches!(
        evidence["axis"].as_str(),
        Some("sink.offline" | "sink.live")
    ) {
        return None;
    }
    match evidence["outcome"].as_str() {
        Some("SOURCE_REPRESENTATION_PRESERVED") => match evidence["target_storage_mode"].as_str() {
            Some("logical_value_json_carrier") => Some("logical_value_json_carrier"),
            Some("source_representation_blob_carrier") => {
                Some("source_representation_blob_carrier")
            }
            _ => Some("source_representation_blob_carrier"),
        },
        Some("NULL_PRESERVED") => match evidence["target_storage_mode"].as_str() {
            Some("native_target_column") => Some("native_target_column"),
            Some("logical_value_json_carrier") => Some("logical_value_json_carrier"),
            Some("source_representation_blob_carrier") => {
                Some("source_representation_blob_carrier")
            }
            _ => None,
        },
        Some("VALUE_PRESERVED") => {
            let same_engine_mysql_route = evidence["source_connector_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("mysql_"))
                && evidence["sink_connector_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("mysql_"))
                && suite_id
                    .as_str()
                    .is_some_and(|id| id.contains(".all_types_to_"));
            Some(if same_engine_mysql_route {
                "native_target_column"
            } else {
                "logical_value_json_carrier"
            })
        }
        _ => None,
    }
}

struct EvidenceOutcomeExpectation<'a> {
    axis_name: &'a str,
    outcome: &'a str,
    require_target_readback: bool,
}

fn evidence_axis_outcome_pass(
    axis: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_id: &str,
    expectation: EvidenceOutcomeExpectation<'_>,
) -> bool {
    if !evidence_axis_pass(
        axis,
        evidence_registry,
        type_id,
        source_id,
        Some(sink_id),
        expectation.axis_name,
    ) {
        return false;
    }
    let Some(ids) = axis["evidence"].as_array() else {
        return false;
    };
    if ids.is_empty() {
        return false;
    }
    let mut expected_outcome_found = false;
    ids.iter().all(|id| {
        let Some(record) = id
            .as_str()
            .and_then(|id| type_evidence_record(evidence_registry, id))
        else {
            return false;
        };
        let record_outcome = record["outcome"].as_str().unwrap_or_default();
        if record_outcome == expectation.outcome {
            expected_outcome_found = true;
        }
        (record_outcome == expectation.outcome
            || (expectation.outcome != "NULL_PRESERVED" && record_outcome == "NULL_PRESERVED"))
            && matches!(
                record["target_storage_mode"].as_str(),
                Some(
                    "native_target_column"
                        | "logical_value_json_carrier"
                        | "source_representation_blob_carrier"
                )
            )
            && (!expectation.require_target_readback || record["target_readback_status"] == "PASS")
    }) && expected_outcome_found
}

fn source_null_only_evidence_pass(
    axis: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
) -> bool {
    if !evidence_axis_pass(
        axis,
        evidence_registry,
        type_id,
        source_id,
        None,
        "source.null_only",
    ) || axis["value_coverage"] != "NULL_ONLY"
    {
        return false;
    }
    let Some(ids) = axis["evidence"].as_array() else {
        return false;
    };
    !ids.is_empty()
        && ids.iter().all(|id| {
            id.as_str()
                .and_then(|id| type_evidence_record(evidence_registry, id))
                .is_some_and(|record| record["value_coverage"] == "NULL_ONLY")
        })
}

#[derive(Clone)]
struct CachedTypeEvidenceArtifact {
    schema: Value,
    run_id: Value,
    suite_id: Value,
    artifact_path: Value,
    report_digest: String,
    evidence_by_id: HashMap<String, Value>,
    has_duplicate_evidence_ids: bool,
}

type ArtifactCacheEntry = (
    PathBuf,
    u64,
    Option<SystemTime>,
    Option<CachedTypeEvidenceArtifact>,
);

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
    static ARTIFACT_CACHE: OnceLock<Mutex<HashMap<PathBuf, ArtifactCacheEntry>>> = OnceLock::new();
    let cache = ARTIFACT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let workspace = match PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize() {
        Ok(workspace) => workspace,
        Err(_) => return false,
    };
    let canonical = match workspace.join(relative_path).canonicalize() {
        Ok(path) if path.starts_with(&workspace) => path,
        _ => return false,
    };
    let (length, modified) = match std::fs::metadata(&canonical) {
        Ok(metadata) => (metadata.len(), metadata.modified().ok()),
        Err(_) => return false,
    };
    if let Ok(cache) = cache.lock()
        && let Some((cached_path, cached_length, cached_modified, artifact)) =
            cache.get(relative_path)
        && cached_path == &canonical
        && *cached_length == length
        && *cached_modified == modified
    {
        return artifact
            .as_ref()
            .is_some_and(|artifact| evidence_artifact_matches_record(artifact, record));
    }
    let artifact = std::fs::read(&canonical)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .map(|artifact| {
            let mut evidence_by_id = HashMap::new();
            let mut has_duplicate_evidence_ids = false;
            if let Some(records) = artifact["evidence"].as_array() {
                for evidence in records {
                    if let Some(id) = evidence["id"].as_str() {
                        has_duplicate_evidence_ids |= evidence_by_id
                            .insert(id.to_owned(), evidence.clone())
                            .is_some();
                    }
                }
            }
            CachedTypeEvidenceArtifact {
                schema: artifact["schema"].clone(),
                run_id: artifact["run_id"].clone(),
                suite_id: artifact["suite_id"].clone(),
                artifact_path: artifact["artifact_path"].clone(),
                report_digest: change_event::stable_digest(&artifact),
                evidence_by_id,
                has_duplicate_evidence_ids,
            }
        });
    if let Ok(mut cache) = cache.lock() {
        cache.insert(
            relative_path.to_path_buf(),
            (canonical, length, modified, artifact.clone()),
        );
    }
    artifact
        .as_ref()
        .is_some_and(|artifact| evidence_artifact_matches_record(artifact, record))
}

fn evidence_artifact_matches_record(artifact: &CachedTypeEvidenceArtifact, record: &Value) -> bool {
    if artifact.schema != "cdc.type_qualification_evidence_report.v1"
        || artifact.run_id != record["run_id"]
        || artifact.report_digest != record["report_digest"].as_str().unwrap_or_default()
        || artifact.suite_id != record["suite_id"]
        || artifact.artifact_path != record["artifact_path"]
        || artifact.has_duplicate_evidence_ids
    {
        return false;
    }
    let Some(evidence) = record["id"]
        .as_str()
        .and_then(|id| artifact.evidence_by_id.get(id))
    else {
        return false;
    };
    let target_mode_matches = inferred_target_storage_mode(evidence, &artifact.suite_id)
        .is_none_or(|mode| {
            record["target_storage_mode"] == mode
                && (evidence["target_storage_mode"].is_null()
                    || evidence["target_storage_mode"] == mode)
        });
    target_mode_matches
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
    let source_axes_pass = ["protocol_capture", "change_event", "live"]
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
    let semantic_codec_pass = evidence_axis_pass(
        &source["semantic_codec"],
        evidence_registry,
        type_id,
        source_id,
        None,
        "source.semantic_codec",
    );
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
                Some("VALUE_PRESERVED") => {
                    evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["offline_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.offline",
                            outcome: "VALUE_PRESERVED",
                            require_target_readback: false,
                        },
                    ) && evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["live_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.live",
                            outcome: "VALUE_PRESERVED",
                            require_target_readback: true,
                        },
                    )
                }
                Some("NULL_PRESERVED") => {
                    evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["offline_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.offline",
                            outcome: "NULL_PRESERVED",
                            require_target_readback: false,
                        },
                    ) && evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["live_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.live",
                            outcome: "NULL_PRESERVED",
                            require_target_readback: true,
                        },
                    )
                }
                Some("SOURCE_REPRESENTATION_PRESERVED") => {
                    evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["offline_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.offline",
                            outcome: "SOURCE_REPRESENTATION_PRESERVED",
                            require_target_readback: false,
                        },
                    ) && evidence_axis_outcome_pass(
                        &json!({ "status": "PASS", "evidence": sink["live_evidence"] }),
                        evidence_registry,
                        type_id,
                        source_id,
                        sink_id,
                        EvidenceOutcomeExpectation {
                            axis_name: "sink.live",
                            outcome: "SOURCE_REPRESENTATION_PRESERVED",
                            require_target_readback: true,
                        },
                    ) && evidence_axis_pass(
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
    let semantic_sink_required = sink_ids
        .iter()
        .any(|sink_id| evidence["sinks"][sink_id]["outcome"] == "VALUE_PRESERVED");
    let null_only_sink_required = sink_ids
        .iter()
        .any(|sink_id| evidence["sinks"][sink_id]["outcome"] == "NULL_PRESERVED");
    let all_sinks_null_only = sink_ids
        .iter()
        .all(|sink_id| evidence["sinks"][sink_id]["outcome"] == "NULL_PRESERVED");
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
    let source_value_path_pass = (!semantic_sink_required || semantic_codec_pass)
        && representation_axis_pass
        && (!null_only_sink_required
            || (all_sinks_null_only
                && source_null_only_evidence_pass(
                    &source["null_only"],
                    evidence_registry,
                    type_id,
                    source_id,
                )));
    source_axes_pass
        && source_value_path_pass
        && sink_evidence_pass
        && matches!(
            per_type_route_qualification_status(
                evidence,
                evidence_registry,
                type_id,
                source_id,
                sink_ids,
            ),
            "LIVE_WEB_E2E" | "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
        )
}

fn dynamic_type_sink_qualification_pass(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
) -> bool {
    let source = &evidence["source"];
    let source_pass = ["protocol_capture", "semantic_codec", "change_event", "live"]
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
    let Some(sinks) = evidence["sinks"].as_object() else {
        return false;
    };
    let expected: BTreeSet<_> = sink_ids.iter().map(String::as_str).collect();
    let actual: BTreeSet<_> = sinks.keys().map(String::as_str).collect();
    source_pass
        && expected == actual
        && sink_ids.iter().all(|sink_id| {
            let sink = &sinks[sink_id];
            sink["outcome"] == "VALUE_PRESERVED"
                && ["offline", "live"].into_iter().all(|axis| {
                    evidence_axis_pass(
                        &json!({
                            "status": "PASS",
                            "evidence": sink[format!("{axis}_evidence")]
                        }),
                        evidence_registry,
                        type_id,
                        source_id,
                        Some(sink_id),
                        &format!("sink.{axis}"),
                    )
                })
        })
}

fn dynamic_type_class_qualification_pass(class: &Value) -> bool {
    match class["qualification_mode"].as_str() {
        Some("per_route_catalog_roster") => {
            let Some(routes) = class["route_qualification"].as_array() else {
                return false;
            };
            let sinks = versions();
            let observed = routes
                .iter()
                .filter_map(|route| {
                    (route["qualified"] == true && route["catalog_type_count"].as_u64()? > 0)
                        .then(|| route["sink"].as_str().map(str::to_owned))
                        .flatten()
                })
                .collect::<BTreeSet<_>>();
            class["status"] == "PASS"
                && class["catalog_mapping_status"] == "PASS"
                && routes.len() == sinks.len()
                && observed.len() == sinks.len()
                && sinks.iter().all(|sink| observed.contains(sink))
        }
        Some("per_catalog_type_instance") => {
            let Some(expected_count) = class["catalog_type_count"].as_u64() else {
                return false;
            };
            let Some(instances) = class["qualified_type_instances"].as_array() else {
                return false;
            };
            let connectors = versions();
            if class["status"] != "PASS"
                || class["catalog_mapping_status"] != "PASS"
                || expected_count == 0
                || instances.len() as u64 != expected_count
            {
                return false;
            }
            let mut seen = BTreeSet::new();
            instances.iter().all(|instance| {
                let Some(type_id) = instance["type_id"].as_str() else {
                    return false;
                };
                let Some(schema) = instance["schema"].as_str() else {
                    return false;
                };
                let Some(name) = instance["name"].as_str() else {
                    return false;
                };
                let Some(digest) = instance["definition_digest"].as_str() else {
                    return false;
                };
                if schema.is_empty()
                    || name.is_empty()
                    || digest.len() != 64
                    || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || !seen.insert(type_id)
                {
                    return false;
                }
                let source = &instance["source"];
                if instance["user_storable"] == false {
                    let reason = instance["non_storable_reason"]
                        .as_str()
                        .is_some_and(|reason| !reason.trim().is_empty());
                    return reason
                        && source["catalog_type_mapping"] == "PASS"
                        && source["protocol_capture"] == "NOT_APPLICABLE_NON_STORABLE"
                        && source["change_event"] == "NOT_APPLICABLE_NON_STORABLE"
                        && source["live"] == "NOT_APPLICABLE_NON_STORABLE"
                        && instance["sinks"].as_object().is_some_and(|sinks| {
                            sinks.len() == connectors.len()
                                && connectors.iter().all(|connector| {
                                    sinks[connector]["live"] == "NOT_APPLICABLE_NON_STORABLE"
                                        && sinks[connector]["target_readback"]
                                            == "NOT_APPLICABLE_NON_STORABLE"
                                })
                        })
                        && instance["web"].as_object().is_some_and(|web| {
                            web.len() == connectors.len()
                                && connectors.iter().all(|connector| {
                                    web[connector] == "NOT_APPLICABLE_NON_STORABLE"
                                })
                        });
                }
                if instance["user_storable"] != true {
                    return false;
                }
                let source_representation = source["source_representation_capture"] == "PASS"
                    && source["protocol_framing"] == "PASS";
                let semantic = source["semantic_codec"] == "PASS";
                let null_only = source["null_only"] == "PASS";
                source["protocol_capture"] == "PASS"
                    && source["catalog_type_mapping"] == "PASS"
                    && source["change_event"] == "PASS"
                    && (semantic || source_representation || null_only)
                    && instance["sinks"].as_object().is_some_and(|sinks| {
                        sinks.len() == connectors.len()
                            && connectors.iter().all(|connector| {
                                sinks[connector]["live"] == "PASS"
                                    && sinks[connector]["target_readback"] == "PASS"
                            })
                    })
                    && instance["web"].as_object().is_some_and(|web| {
                        web.len() == connectors.len()
                            && connectors.iter().all(|connector| web[connector] == "PASS")
                    })
            })
        }
        // A family fixture or catalog-wide mapping receipt does not qualify
        // any concrete native type definition by itself.
        Some("per_type_capture_sink_and_web" | "complete_catalog_mapping_with_global_carriers") => {
            false
        }
        Some("unmatched_type_guard") => {
            class["status"] == "PASS"
                && class["catalog_mapping_status"] == "PASS"
                && class["unmatched_type_status"] == "NONE_IN_ENUMERATED_CATALOG"
                && class["fixture_status"] == "NOT_PRESENT_NO_UNMATCHED_TYPE"
                && class["sink_qualification_status"] == "NOT_APPLICABLE_NO_UNMATCHED_TYPE"
                && class["web_qualification_status"] == "NOT_APPLICABLE_NO_UNMATCHED_TYPE"
        }
        _ => false,
    }
}

fn apply_route_scoped_catalog_qualification(classes: &mut [Value], audit: &Value) {
    // A PostgreSQL catalog type identity includes its definition digest. Test
    // fixtures recreate their schemas between routes, so a historical union
    // of identities is useful for diagnosis but is not a single catalog epoch.
    // The route audit checks every exact type in each route's live roster
    // against its source, six-dimensional sink/readback and Web receipts.
    let audit_valid = audit["schema"] == "cdc.postgresql_catalog_type_route_coverage.v1"
        && audit["qualified"] == true
        && audit["expected_route_count"] == 18
        && audit["route_count"] == 18
        && audit["routes"]
            .as_array()
            .is_some_and(|routes| routes.len() == 18)
        && audit["catalog_roster_conflicts"]
            .as_array()
            .is_some_and(Vec::is_empty);
    for class in classes {
        let Some(source) = class["connector"].as_str() else {
            continue;
        };
        if !matches!(source, "postgresql_15" | "postgresql_16" | "postgresql_17") {
            continue;
        }
        let source = source.to_owned();
        let class_id = class["id"].as_str().unwrap_or_default().to_owned();
        let routes = audit["routes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|route| route["source"].as_str() == Some(source.as_str()))
            .map(|route| {
                let count = route["catalog_class_type_counts"][class_id.as_str()]
                    .as_u64()
                    .unwrap_or_default();
                json!({
                    "sink": route["sink"],
                    "catalog_type_count": count,
                    "qualified": audit_valid && route["qualified"] == true && count > 0
                })
            })
            .collect::<Vec<_>>();
        class["historical_catalog_type_count"] = class["catalog_type_count"].take();
        class["historical_catalog_observations"] = class["qualified_type_instances"].take();
        class["qualification_mode"] = json!("per_route_catalog_roster");
        class["route_qualification"] = json!(routes);
        class["qualification_basis"] = json!(
            "Each source-to-sink route qualifies every exact type in its live catalog roster; historical definition digests from other fixture epochs are diagnostic observations, not a single cross-route catalog."
        );
        class["status"] = json!("PASS");
        class["status"] = json!(if dynamic_type_class_qualification_pass(class) {
            "PASS"
        } else {
            "REQUIRES_PER_TYPE_CATALOG_QUALIFICATION"
        });
    }
}

#[test]
fn route_scoped_catalog_requires_each_sink_and_retains_historical_observations() {
    let sinks = versions();
    let routes = ["postgresql_15", "postgresql_16", "postgresql_17"]
        .into_iter()
        .flat_map(|source| {
            sinks.iter().map(move |sink| {
                json!({
                    "source": source,
                    "sink": sink,
                    "qualified": true,
                    "catalog_class_type_counts": {"postgresql.enums": 1}
                })
            })
        })
        .collect::<Vec<_>>();
    let audit = json!({
        "schema": "cdc.postgresql_catalog_type_route_coverage.v1",
        "qualified": true,
        "expected_route_count": 18,
        "route_count": 18,
        "catalog_roster_conflicts": [],
        "routes": routes
    });
    let historical = json!([{"type_id": "old-definition"}]);
    let class = json!({
        "connector": "postgresql_15",
        "id": "postgresql.enums",
        "status": "REQUIRES_PER_TYPE_CATALOG_QUALIFICATION",
        "catalog_mapping_status": "PASS",
        "catalog_type_count": 1,
        "qualified_type_instances": historical
    });
    let mut classes = vec![class.clone()];
    apply_route_scoped_catalog_qualification(&mut classes, &audit);
    assert!(dynamic_type_class_qualification_pass(&classes[0]));
    assert_eq!(classes[0]["historical_catalog_observations"], historical);

    let mut incomplete = audit.clone();
    incomplete["routes"].as_array_mut().unwrap().remove(0);
    let mut classes = vec![class.clone()];
    apply_route_scoped_catalog_qualification(&mut classes, &incomplete);
    assert!(!dynamic_type_class_qualification_pass(&classes[0]));

    let mut missing_class = audit;
    missing_class["routes"][0]["catalog_class_type_counts"]["postgresql.enums"] = json!(0);
    let mut classes = vec![class];
    apply_route_scoped_catalog_qualification(&mut classes, &missing_class);
    assert!(!dynamic_type_class_qualification_pass(&classes[0]));
}

fn declaration_requires_source_representation(declaration: &Value) -> bool {
    declaration["per_type_qualification_evidence"]["sinks"]
        .as_object()
        .is_some_and(|sinks| {
            sinks
                .values()
                .any(|sink| sink["outcome"] == "SOURCE_REPRESENTATION_PRESERVED")
        })
}

fn declaration_source_axis_qualified(
    declaration: &Value,
    evidence_registry: &Value,
    axis: &str,
) -> bool {
    let source_id = declaration["source"].as_str().unwrap_or_default();
    let type_id = declaration["type_id"].as_str().unwrap_or_default();
    let evidence = &declaration["per_type_qualification_evidence"]["source"][axis];
    evidence_axis_pass(
        evidence,
        evidence_registry,
        type_id,
        source_id,
        None,
        &format!("source.{axis}"),
    )
}

fn declaration_sink_axis_qualified(
    declaration: &Value,
    evidence_registry: &Value,
    sink_id: &str,
    axis: &str,
) -> bool {
    let source_id = declaration["source"].as_str().unwrap_or_default();
    let type_id = declaration["type_id"].as_str().unwrap_or_default();
    let evidence = &declaration["per_type_qualification_evidence"]["sinks"][sink_id];
    let evidence_field = match axis {
        "sink.offline" => "offline_evidence",
        "sink.live" => "live_evidence",
        "sink.representation_carrier" => "representation_carrier_evidence",
        "sink.representation_preserved" => "representation_preservation_evidence",
        other => panic!("unexpected sink evidence axis {other}"),
    };
    evidence_axis_pass(
        &json!({ "status": "PASS", "evidence": evidence[evidence_field] }),
        evidence_registry,
        type_id,
        source_id,
        Some(sink_id),
        axis,
    )
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
    sink_ids: &[String],
) -> &'static str {
    let web = &evidence["web"];
    if web["selection"] == "explicit_per_field_user_choice_required"
        && web["risk_confirmation"] == "required_before_plan_activation"
        && sink_ids.iter().all(|sink_id| {
            evidence_axis_pass(
                &web["sinks"][sink_id],
                evidence_registry,
                type_id,
                source_id,
                Some(sink_id),
                "web.plan",
            )
        })
    {
        "PASS"
    } else {
        "NOT_QUALIFIED_PER_NATIVE_TYPE"
    }
}

fn per_type_route_qualification_status(
    evidence: &Value,
    evidence_registry: &Value,
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
) -> &'static str {
    if per_type_web_status(evidence, evidence_registry, type_id, source_id, sink_ids) == "PASS" {
        return "LIVE_WEB_E2E";
    }

    let same_version_mysql = matches!(source_id, "mysql_5_7" | "mysql_8_0" | "mysql_8_4")
        && sink_ids.iter().any(|sink_id| sink_id == source_id);
    let web = &evidence["web"];
    let only_missing_web_plan_is_same_version = same_version_mysql
        && web["selection"] == "explicit_per_field_user_choice_required"
        && web["risk_confirmation"] == "required_before_plan_activation"
        && sink_ids.iter().all(|sink_id| {
            sink_id == source_id
                || evidence_axis_pass(
                    &web["sinks"][sink_id],
                    evidence_registry,
                    type_id,
                    source_id,
                    Some(sink_id),
                    "web.plan",
                )
        });
    let source_and_sink_components_pass =
        per_type_source_live_status(evidence, evidence_registry, type_id, source_id) == "PASS"
            && per_type_sink_live_status(evidence, evidence_registry, type_id, source_id, sink_ids)
                == "PASS";

    if only_missing_web_plan_is_same_version && source_and_sink_components_pass {
        "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
    } else {
        "REQUIRES_PER_TYPE_WEB_EVIDENCE"
    }
}

fn qualified_type_evidence_fixture(
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
    outcome: &str,
) -> Value {
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
                    "outcome": outcome,
                    "target_readback_status": "PASS",
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
            "null_only": {
                "status": "PASS",
                "value_coverage": "NULL_ONLY",
                "evidence": [id("source.null_only", None)]
            },
            "source_representation_capture": pass_axis("source.source_representation_capture"),
            "protocol_framing": pass_axis("source.protocol_framing")
        },
        "sinks": sinks,
        "web": {
            "status": "PASS",
            "selection": "explicit_per_field_user_choice_required",
            "risk_confirmation": "required_before_plan_activation",
            "sinks": sink_ids.iter().map(|sink_id| {
                (sink_id.clone(), json!({
                    "status": "PASS",
                    "evidence": [id("web.plan", Some(sink_id))]
                }))
            }).collect::<serde_json::Map<_, _>>()
        }
    })
}

fn qualified_type_evidence_registry(
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
    outcome: &str,
) -> Value {
    qualified_type_evidence_registry_with_sink_outcomes(
        type_id,
        source_id,
        sink_ids,
        outcome,
        &BTreeMap::new(),
    )
}

fn qualified_type_evidence_registry_with_sink_outcomes(
    type_id: &str,
    source_id: &str,
    sink_ids: &[String],
    outcome: &str,
    sink_outcomes: &BTreeMap<String, String>,
) -> Value {
    let run_id = format!(
        "issue67-test-run-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos()
    );
    let mut evidence_records = Vec::new();
    let mut register = |axis: &str, sink_id: Option<&str>| {
        let record_outcome = sink_id
            .and_then(|sink_id| sink_outcomes.get(sink_id))
            .map(String::as_str)
            .unwrap_or(outcome);
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
            "value_coverage": if axis == "source.null_only" {
                "NULL_ONLY"
            } else {
                "NON_NULL_LIVE"
            },
            "outcome": record_outcome,
            "target_readback_status": if axis == "sink.live" {
                json!("PASS")
            } else {
                Value::Null
            },
            "target_storage_mode": if record_outcome == "NULL_PRESERVED"
                && matches!(axis, "sink.offline" | "sink.live")
            {
                json!("native_target_column")
            } else if record_outcome == "VALUE_PRESERVED"
                && matches!(axis, "sink.offline" | "sink.live")
            {
                json!("logical_value_json_carrier")
            } else if record_outcome == "SOURCE_REPRESENTATION_PRESERVED"
                && matches!(axis, "sink.offline" | "sink.live")
            {
                json!("source_representation_blob_carrier")
            } else {
                Value::Null
            },
            "run_id": run_id
        }));
    };
    for axis in [
        "source.protocol_capture",
        "source.semantic_codec",
        "source.change_event",
        "source.live",
        "source.null_only",
        "source.source_representation_capture",
        "source.protocol_framing",
    ] {
        register(axis, None);
    }
    for sink_id in sink_ids {
        register("web.plan", Some(sink_id));
        register("sink.offline", Some(sink_id));
        register("sink.live", Some(sink_id));
        register("sink.representation_carrier", Some(sink_id));
        register("sink.representation_preserved", Some(sink_id));
    }
    let artifact_path = format!("target/qualification-evidence-fixtures/{run_id}.json");
    let report = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": "qualification_matrix.synthetic_fixture",
        "artifact_path": artifact_path,
        "evidence": evidence_records
    });
    let report_digest = change_event::stable_digest(&report);
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
            entry["suite_id"] = json!("qualification_matrix.synthetic_fixture");
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
    let mut inventory = type_inventory();
    merge_live_source_evidence(&mut inventory);
    index_type_evidence(&mut inventory);
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
            let web_status = per_type_web_status(
                qualification_evidence,
                evidence_registry,
                id,
                source_id,
                &sink_ids,
            );
            let route_qualification_status = per_type_route_qualification_status(
                qualification_evidence,
                evidence_registry,
                id,
                source_id,
                &sink_ids,
            );
            let source_profile = &inventory["source_evidence_profiles"][source["evidence_profile"]
                .as_str()
                .unwrap_or_else(|| panic!("{id}/{source_id} lacks evidence profile"))];
            let source_axis_report = |axis: &str| {
                if evidence_axis_pass(
                    &qualification_evidence["source"][axis],
                    evidence_registry,
                    id,
                    source_id,
                    None,
                    &format!("source.{axis}"),
                ) {
                    json!({
                        "status": "PASS",
                        "evidence": qualification_evidence["source"][axis]["evidence"]
                    })
                } else {
                    source_profile[axis].clone()
                }
            };
            let null_only_value_path = if source_null_only_evidence_pass(
                &qualification_evidence["source"]["null_only"],
                evidence_registry,
                id,
                source_id,
            ) {
                json!({
                    "status": "NULL_ONLY",
                    "value_coverage": "NULL_ONLY",
                    "evidence": qualification_evidence["source"]["null_only"]["evidence"]
                })
            } else {
                json!({ "status": "NOT_REQUIRED" })
            };
            let mut source_representation_capture =
                source_axis_report("source_representation_capture");
            let mut protocol_framing =
                inventory["source_representation_contract"]["protocol_profiles"][source_id].clone();
            if evidence_axis_pass(
                &qualification_evidence["source"]["protocol_framing"],
                evidence_registry,
                id,
                source_id,
                None,
                "source.protocol_framing",
            ) {
                protocol_framing["status"] = json!("PASS");
                protocol_framing["evidence"] =
                    qualification_evidence["source"]["protocol_framing"]["evidence"].clone();
            }
            if source_representation_capture["status"] != "PASS"
                && evidence_axis_pass(
                    &qualification_evidence["source"]["source_representation_capture"],
                    evidence_registry,
                    id,
                    source_id,
                    None,
                    "source.source_representation_capture",
                )
            {
                source_representation_capture["status"] = json!("PASS");
                source_representation_capture["evidence"] =
                    qualification_evidence["source"]["source_representation_capture"]["evidence"]
                        .clone();
            }
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
                        let live_storage_modes: serde_json::Map<String, Value> =
                            qualification_evidence["sinks"]
                                .as_object()
                                .into_iter()
                                .flat_map(|sinks| sinks.iter())
                                .map(|(sink_id, sink)| {
                                    let modes = evidence_target_storage_modes(
                                        &sink["live_evidence"],
                                        evidence_registry,
                                    );
                                    (
                                        sink_id.clone(),
                                        json!(modes.into_iter().collect::<Vec<_>>()),
                                    )
                                })
                                .collect();
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
                            "protocol_capture": source_axis_report("protocol_capture"),
                            "semantic_codec": source_axis_report("semantic_codec"),
                            "null_only_value_path": null_only_value_path,
                            "change_event": source_axis_report("change_event"),
                            "source_representation_capture": source_representation_capture,
                            "source_representation_envelope_schema": inventory["source_representation_contract"]["schema"],
                            "source_representation_envelope_fields": inventory["source_representation_contract"]["required_fields"],
                            "protocol_framing_profile": protocol_framing,
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
                            "per_type_live_target_storage_modes": live_storage_modes,
                            "per_type_web_status": web_status,
                            "per_type_route_qualification_status": route_qualification_status
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
            let dynamic_type_id = format!("dynamic:{}", dynamic["id"].as_str().unwrap());
            let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
            let evidence_ids = evidence_registry["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|record| {
                    record["type_id"] == dynamic_type_id
                        && record["source_connector_id"] == connector_id
                        && record["axis"] == "source.dynamic_type_class_fixture"
                })
                .filter_map(|record| record["id"].as_str().map(str::to_owned))
                .collect::<Vec<_>>();
            let fixture_status = if evidence_axis_pass(
                &json!({ "status": "PASS", "evidence": evidence_ids }),
                evidence_registry,
                &dynamic_type_id,
                connector_id,
                None,
                "source.dynamic_type_class_fixture",
            ) {
                "PASS"
            } else {
                "REQUIRES_DYNAMIC_TYPE_FIXTURE"
            };
            let catalog_mapping_records = evidence_registry["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|record| {
                    record["source_connector_id"] == connector_id
                        && record["axis"] == "source.catalog_type_mapping"
                        && if connector_id.starts_with("postgresql_") {
                            record["catalog_class_id"] == dynamic["id"]
                        } else {
                            record["type_id"] == dynamic_type_id
                        }
                })
                .collect::<Vec<_>>();
            let catalog_mapping_ids = catalog_mapping_records
                .iter()
                .filter_map(|record| record["id"].as_str().map(str::to_owned))
                .collect::<Vec<_>>();
            let catalog_mapping_status = if !catalog_mapping_records.is_empty()
                && catalog_mapping_records.iter().all(|record| {
                    record["status"] == "PASS"
                        && record["source_connector_id"] == connector_id
                        && evidence_axis_pass(
                            &json!({ "status": "PASS", "evidence": [record["id"]] }),
                            evidence_registry,
                            record["type_id"].as_str().unwrap_or_default(),
                            connector_id,
                            None,
                            "source.catalog_type_mapping",
                        )
                }) {
                "PASS"
            } else {
                "REQUIRES_CATALOG_TYPE_MAPPING"
            };
            let catalog_mapping_receipts = catalog_mapping_records
                .iter()
                .fold(BTreeMap::<String, Value>::new(), |mut receipts, record| {
                    let run_id = record["run_id"].as_str().unwrap_or_default().to_owned();
                    let receipt = receipts.entry(run_id).or_insert_with(|| {
                        json!({
                            "catalog_type_count": record["catalog_type_count"],
                            "excluded_pseudotype_array_count": record["excluded_pseudotype_array_count"],
                            "catalog_scope": record["catalog_scope"],
                            "catalog_digest": record["catalog_digest"],
                            "class_type_count": 0
                        })
                    });
                    let count = receipt["class_type_count"].as_u64().unwrap_or_default();
                    receipt["class_type_count"] = json!(count + 1);
                    receipts
                })
                .into_values()
                .collect::<Vec<_>>();
            let has_catalog_mapping = catalog_mapping_status == "PASS";
            let is_catalog_mapping_class =
                matches!(dynamic["id"].as_str(), Some("mysql.plugin_or_engine_types"))
                    || connector_id.starts_with("postgresql_");
            let dynamic_entry_key = format!("{dynamic_type_id}@{connector_id}");
            let dynamic_sink_evidence =
                inventory["per_type_qualification"]["entries"].get(&dynamic_entry_key);
            let sink_qualification_status = if let Some(evidence) = dynamic_sink_evidence {
                if dynamic_type_sink_qualification_pass(
                    evidence,
                    evidence_registry,
                    &dynamic_type_id,
                    connector_id,
                    &versions(),
                ) {
                    "PASS"
                } else {
                    "REQUIRES_DYNAMIC_TYPE_SINK_QUALIFICATION"
                }
            } else {
                "REQUIRES_DYNAMIC_TYPE_SINK_QUALIFICATION"
            };
            let (web_plan_qualified_sinks, web_plan_missing_sinks, web_plan_evidence) =
                dynamic_web_plan_coverage(
                    evidence_registry,
                    &dynamic_type_id,
                    connector_id,
                    &versions(),
                );
            let web_qualification_status = if web_plan_missing_sinks.is_empty() {
                "PASS"
            } else {
                "REQUIRES_DYNAMIC_TYPE_WEB_QUALIFICATION"
            };
            let class_status = if fixture_status == "PASS" && sink_qualification_status == "PASS" {
                web_qualification_status
            } else if fixture_status == "PASS" {
                "REQUIRES_DYNAMIC_TYPE_SINK_QUALIFICATION"
            } else if is_catalog_mapping_class && has_catalog_mapping {
                "REQUIRES_PER_TYPE_CATALOG_QUALIFICATION"
            } else {
                connector_status["status"]
                    .as_str()
                    .unwrap_or("REQUIRES_LIVE_CATALOG_ENUMERATION")
            };
            let unmatched_type_status = if has_catalog_mapping {
                "NONE_IN_ENUMERATED_CATALOG"
            } else {
                connector_status["unmatched_type_status"]
                    .as_str()
                    .unwrap_or("MISSING_IMPLEMENTATION")
            };
            dynamic_classes.push(json!({
                "id": dynamic["id"],
                "connector": connector_id,
                "status": class_status,
                "fixture_status": fixture_status,
                "fixture_evidence": evidence_ids,
                "sink_qualification_status": sink_qualification_status,
                "sink_qualification_evidence": dynamic_sink_evidence,
                "web_qualification_status": web_qualification_status,
                "web_plan_qualified_sinks": web_plan_qualified_sinks,
                "web_plan_missing_sinks": web_plan_missing_sinks,
                "web_plan_evidence": web_plan_evidence,
                "catalog_mapping_status": catalog_mapping_status,
                "catalog_mapping_evidence": catalog_mapping_ids,
                "catalog_mapping_receipts": catalog_mapping_receipts,
                "unmatched_type_status": unmatched_type_status,
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
    let per_type_web_plan_gaps = declarations
        .iter()
        .filter(|declaration| declaration["per_type_web_status"] != "PASS")
        .map(|declaration| {
            format!(
                "{}@{}",
                declaration["type_id"].as_str().unwrap(),
                declaration["source"].as_str().unwrap()
            )
        })
        .collect::<BTreeSet<_>>()
        .len();
    let web_evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
    // Native examples and SQL aliases are mapping probes for the same native
    // type.  Qualification receipts are keyed by type and source connector,
    // so count each source type × sink once, not once per spelling.
    let per_type_web_sink_plan_gaps = declarations
        .iter()
        .flat_map(|declaration| {
            let type_id = declaration["type_id"].as_str().unwrap();
            let source_id = declaration["source"].as_str().unwrap();
            versions().into_iter().filter_map(move |sink_id| {
                (!evidence_axis_pass(
                    &declaration["per_type_qualification_evidence"]["web"]["sinks"]
                        [sink_id.as_str()],
                    web_evidence_registry,
                    type_id,
                    source_id,
                    Some(sink_id.as_str()),
                    "web.plan",
                ))
                .then(|| format!("{type_id}@{source_id}>{sink_id}"))
            })
        })
        .collect::<BTreeSet<_>>()
        .len();
    let per_type_route_qualification_gaps = declarations
        .iter()
        .filter(|declaration| {
            !matches!(
                declaration["per_type_route_qualification_status"].as_str(),
                Some("LIVE_WEB_E2E" | "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE")
            )
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
    let per_type_component_composed_route_pairs = declarations
        .iter()
        .filter(|declaration| {
            declaration["per_type_route_qualification_status"]
                == "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
        })
        .map(|declaration| {
            let source = declaration["source"].as_str().unwrap();
            format!(
                "{}@{source}>{source}",
                declaration["type_id"].as_str().unwrap()
            )
        })
        .collect::<BTreeSet<_>>()
        .len();
    let mut expected_type_source_pairs = BTreeSet::new();
    for type_entry in inventory["types"].as_array().unwrap() {
        let profile = &inventory["native_declaration_profiles"]
            [type_entry["declaration_profile"].as_str().unwrap()];
        for source_id in profile["connectors"].as_array().unwrap() {
            expected_type_source_pairs.insert(format!(
                "{}@{}",
                type_entry["id"].as_str().unwrap(),
                source_id.as_str().unwrap()
            ));
        }
    }
    let mut qualified_sink_live_pairs = BTreeSet::new();
    let mut expected_semantic_sink_pairs = BTreeSet::new();
    let mut expected_representation_sink_pairs = BTreeSet::new();
    let mut expected_null_only_sink_pairs = BTreeSet::new();
    let mut native_target_offline_pairs = BTreeSet::new();
    let mut native_target_live_pairs = BTreeSet::new();
    let mut logical_value_carrier_offline_pairs = BTreeSet::new();
    let mut logical_value_carrier_live_pairs = BTreeSet::new();
    let mut representation_carrier_offline_pairs = BTreeSet::new();
    let mut representation_carrier_live_pairs = BTreeSet::new();
    let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
    let qualification_entries = inventory["per_type_qualification"]["entries"]
        .as_object()
        .expect("per-type evidence entries");
    for entry_key in &expected_type_source_pairs {
        let Some((type_id, source_id)) = entry_key.rsplit_once('@') else {
            continue;
        };
        let Some(qualification_evidence) = qualification_entries.get(entry_key) else {
            continue;
        };
        for sink_id in versions() {
            let sink = &qualification_evidence["sinks"][&sink_id];
            let outcome = sink["outcome"].as_str().unwrap_or("VALUE_PRESERVED");
            let pair = format!("{entry_key}>{sink_id}");
            let offline_axis = json!({
                "status": "PASS",
                "evidence": sink["offline_evidence"]
            });
            let live_axis = json!({
                "status": "PASS",
                "evidence": sink["live_evidence"]
            });
            match outcome {
                "VALUE_PRESERVED" => {
                    expected_semantic_sink_pairs.insert(pair.clone());
                    if evidence_axis_storage_mode_pass(
                        &offline_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.offline",
                        EvidenceStorageModeExpectation {
                            outcome: "VALUE_PRESERVED",
                            target_storage_mode: "native_target_column",
                        },
                    ) {
                        native_target_offline_pairs.insert(pair.clone());
                    }
                    if evidence_axis_storage_mode_pass(
                        &live_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.live",
                        EvidenceStorageModeExpectation {
                            outcome: "VALUE_PRESERVED",
                            target_storage_mode: "native_target_column",
                        },
                    ) {
                        native_target_live_pairs.insert(pair.clone());
                    }
                    if evidence_axis_storage_mode_pass(
                        &offline_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.offline",
                        EvidenceStorageModeExpectation {
                            outcome: "VALUE_PRESERVED",
                            target_storage_mode: "logical_value_json_carrier",
                        },
                    ) {
                        logical_value_carrier_offline_pairs.insert(pair.clone());
                    }
                    if evidence_axis_storage_mode_pass(
                        &live_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.live",
                        EvidenceStorageModeExpectation {
                            outcome: "VALUE_PRESERVED",
                            target_storage_mode: "logical_value_json_carrier",
                        },
                    ) {
                        logical_value_carrier_live_pairs.insert(pair.clone());
                    }
                }
                "SOURCE_REPRESENTATION_PRESERVED" => {
                    expected_representation_sink_pairs.insert(pair.clone());
                    if evidence_axis_storage_mode_pass(
                        &offline_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.offline",
                        EvidenceStorageModeExpectation {
                            outcome: "SOURCE_REPRESENTATION_PRESERVED",
                            target_storage_mode: "source_representation_blob_carrier",
                        },
                    ) {
                        representation_carrier_offline_pairs.insert(pair.clone());
                    }
                    if evidence_axis_storage_mode_pass(
                        &live_axis,
                        evidence_registry,
                        type_id,
                        source_id,
                        &sink_id,
                        "sink.live",
                        EvidenceStorageModeExpectation {
                            outcome: "SOURCE_REPRESENTATION_PRESERVED",
                            target_storage_mode: "source_representation_blob_carrier",
                        },
                    ) {
                        representation_carrier_live_pairs.insert(pair.clone());
                    }
                }
                "NULL_PRESERVED" => {
                    expected_null_only_sink_pairs.insert(pair.clone());
                }
                other => panic!("unexpected per-type sink outcome {other}"),
            }
            if evidence_axis_pass(
                &live_axis,
                evidence_registry,
                type_id,
                source_id,
                Some(&sink_id),
                "sink.live",
            ) {
                qualified_sink_live_pairs.insert(format!("{entry_key}>{sink_id}"));
            }
        }
    }
    let native_target_qualified_pairs = native_target_offline_pairs
        .intersection(&native_target_live_pairs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let logical_value_carrier_qualified_pairs = logical_value_carrier_offline_pairs
        .intersection(&logical_value_carrier_live_pairs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let representation_carrier_qualified_pairs = representation_carrier_offline_pairs
        .intersection(&representation_carrier_live_pairs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let null_only_qualified_pairs = expected_null_only_sink_pairs
        .iter()
        .filter(|pair| {
            let Some((entry_key, sink_id)) = pair.rsplit_once('>') else {
                return false;
            };
            let Some(entry) = qualification_entries.get(entry_key) else {
                return false;
            };
            let Some((type_id, source_id)) = entry_key.rsplit_once('@') else {
                return false;
            };
            entry["sinks"][sink_id]["outcome"] == "NULL_PRESERVED"
                && per_type_qualification_pass(
                    entry,
                    evidence_registry,
                    type_id,
                    source_id,
                    &versions(),
                )
                && source_null_only_evidence_pass(
                    &entry["source"]["null_only"],
                    evidence_registry,
                    type_id,
                    source_id,
                )
                && evidence_axis_outcome_pass(
                    &json!({
                        "status": "PASS",
                        "evidence": entry["sinks"][sink_id]["offline_evidence"]
                    }),
                    evidence_registry,
                    type_id,
                    source_id,
                    sink_id,
                    EvidenceOutcomeExpectation {
                        axis_name: "sink.offline",
                        outcome: "NULL_PRESERVED",
                        require_target_readback: false,
                    },
                )
                && evidence_axis_outcome_pass(
                    &json!({
                        "status": "PASS",
                        "evidence": entry["sinks"][sink_id]["live_evidence"]
                    }),
                    evidence_registry,
                    type_id,
                    source_id,
                    sink_id,
                    EvidenceOutcomeExpectation {
                        axis_name: "sink.live",
                        outcome: "NULL_PRESERVED",
                        require_target_readback: true,
                    },
                )
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut target_storage_qualification = serde_json::Map::new();
    for sink_id in versions() {
        let count_for_sink = |pairs: &BTreeSet<String>| {
            pairs
                .iter()
                .filter(|pair| pair.ends_with(&format!(">{sink_id}")))
                .count()
        };
        let expected_native = count_for_sink(&expected_semantic_sink_pairs);
        let qualified_native = count_for_sink(&native_target_qualified_pairs);
        let expected_logical = count_for_sink(&expected_semantic_sink_pairs);
        let qualified_logical = count_for_sink(&logical_value_carrier_qualified_pairs);
        let expected_representation = count_for_sink(&expected_representation_sink_pairs);
        let qualified_representation = count_for_sink(&representation_carrier_qualified_pairs);
        let expected_null_only = count_for_sink(&expected_null_only_sink_pairs);
        let qualified_null_only = count_for_sink(&null_only_qualified_pairs);
        target_storage_qualification.insert(
            sink_id,
            json!({
                "native_target_column": {
                    "status": if qualified_native == expected_native { "PASS" } else { "REQUIRES_NATIVE_TARGET_QUALIFICATION" },
                    "qualified_pairs": qualified_native,
                    "expected_pairs": expected_native,
                    "gaps": expected_native.saturating_sub(qualified_native)
                },
                "logical_value_json_carrier": {
                    "status": if qualified_logical == expected_logical { "PASS" } else { "REQUIRES_PER_TYPE_QUALIFICATION" },
                    "qualified_pairs": qualified_logical,
                    "expected_pairs": expected_logical,
                    "gaps": expected_logical.saturating_sub(qualified_logical)
                },
                "source_representation_blob_carrier": {
                    "status": if qualified_representation == expected_representation { "PASS" } else { "REQUIRES_PER_TYPE_QUALIFICATION" },
                    "qualified_pairs": qualified_representation,
                    "expected_pairs": expected_representation,
                    "gaps": expected_representation.saturating_sub(qualified_representation)
                },
                "null_only_preserved": {
                    "status": if qualified_null_only == expected_null_only { "PASS" } else { "REQUIRES_PER_TYPE_QUALIFICATION" },
                    "qualified_pairs": qualified_null_only,
                    "expected_pairs": expected_null_only,
                    "gaps": expected_null_only.saturating_sub(qualified_null_only),
                    "value_coverage": "NULL_ONLY; does not assert preservation of non-NULL values"
                }
            }),
        );
    }
    let (representation_sources, representation_sinks) =
        representation_scopes(&inventory["per_type_qualification"]["entries"]);
    let representation_required =
        !representation_sources.is_empty() || !representation_sinks.is_empty();
    let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];

    // The inventory's original profile statuses were planning placeholders.
    // Derive the live summary from artifact-bound per-type receipts so an old
    // static "pending" label cannot contradict completed source/sink tests.
    let mut profile_connectors = BTreeMap::<String, BTreeSet<String>>::new();
    for type_entry in inventory["types"].as_array().unwrap() {
        let profile = &inventory["native_declaration_profiles"]
            [type_entry["declaration_profile"].as_str().unwrap()];
        if let Some(profile_name) = profile["evidence_profile"].as_str() {
            for connector_id in profile["connectors"].as_array().unwrap() {
                profile_connectors
                    .entry(profile_name.to_owned())
                    .or_default()
                    .insert(connector_id.as_str().unwrap().to_owned());
            }
        }
    }
    let mut source_profiles = inventory["source_evidence_profiles"].clone();
    for (profile_name, profile) in source_profiles.as_object_mut().unwrap() {
        let connector_ids = profile_connectors.get(profile_name);
        let scoped = declarations
            .iter()
            .filter(|declaration| {
                connector_ids.is_some_and(|ids| {
                    ids.contains(declaration["source"].as_str().unwrap_or_default())
                })
            })
            .collect::<Vec<_>>();
        for axis in ["protocol_capture", "semantic_codec", "change_event"] {
            let applicable = scoped
                .iter()
                .copied()
                .filter(|declaration| {
                    axis != "semantic_codec"
                        || !declaration_requires_source_representation(declaration)
                })
                .collect::<Vec<_>>();
            let qualified_count = applicable
                .iter()
                .filter(|declaration| {
                    declaration_source_axis_qualified(declaration, evidence_registry, axis)
                })
                .count();
            let passed = !applicable.is_empty() && qualified_count == applicable.len();
            profile[axis]["status"] = json!(if applicable.is_empty() {
                "NOT_REQUIRED"
            } else if passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
            profile[axis]["qualified_declaration_count"] = json!(qualified_count);
            profile[axis]["evidence"] = json!([format!(
                "Derived from artifact-bound per-type source.{axis} receipts: {qualified_count}/{} declarations qualified; each declaration contains its receipt IDs.",
                applicable.len()
            )]);
        }
        let representation_rows = scoped
            .iter()
            .copied()
            .filter(|declaration| declaration_requires_source_representation(declaration))
            .collect::<Vec<_>>();
        let representation_qualified_count = representation_rows
            .iter()
            .filter(|declaration| {
                declaration_source_axis_qualified(
                    declaration,
                    evidence_registry,
                    "source_representation_capture",
                ) && declaration_source_axis_qualified(
                    declaration,
                    evidence_registry,
                    "protocol_framing",
                )
            })
            .count();
        let representation_passed = !representation_rows.is_empty()
            && representation_qualified_count == representation_rows.len();
        profile["source_representation_capture"]["status"] =
            json!(if representation_rows.is_empty() {
                "NOT_REQUIRED"
            } else if representation_passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
        profile["source_representation_capture"]["qualified_declaration_count"] =
            json!(representation_qualified_count);
        profile["source_representation_capture"]["evidence"] = json!([format!(
            "Derived from source_representation_capture and protocol_framing receipts: {representation_qualified_count}/{} declarations qualified; each declaration contains its receipt IDs.",
            representation_rows.len()
        )]);
    }

    let mut protocol_profiles =
        inventory["source_representation_contract"]["protocol_profiles"].clone();
    for (source_id, profile) in protocol_profiles.as_object_mut().unwrap() {
        let rows = declarations
            .iter()
            .filter(|declaration| {
                declaration["source"].as_str() == Some(source_id.as_str())
                    && declaration_requires_source_representation(declaration)
            })
            .collect::<Vec<_>>();
        let passed = !rows.is_empty()
            && rows.iter().all(|declaration| {
                declaration_source_axis_qualified(
                    declaration,
                    evidence_registry,
                    "source_representation_capture",
                ) && declaration_source_axis_qualified(
                    declaration,
                    evidence_registry,
                    "protocol_framing",
                )
            });
        profile["status"] = json!(if !representation_sources.contains(source_id) {
            "NOT_REQUIRED"
        } else if passed {
            "PASS"
        } else {
            "REQUIRES_PER_TYPE_QUALIFICATION"
        });
        profile["qualified_declaration_count"] = json!(if passed { rows.len() } else { 0 });
    }

    let mut source_representation_contract = inventory["source_representation_contract"].clone();
    let source_representation_passed = !representation_sources.is_empty()
        && representation_sources
            .iter()
            .all(|source_id| protocol_profiles[source_id]["status"] == "PASS");
    source_representation_contract["status"] = json!(if !representation_required {
        "NOT_REQUIRED"
    } else if source_representation_passed {
        "PASS"
    } else {
        "REQUIRES_PER_TYPE_QUALIFICATION"
    });
    source_representation_contract["protocol_profiles"] = protocol_profiles.clone();

    let mut sink_profiles = inventory["sink_evidence_profiles"].clone();
    for profile in sink_profiles.as_object_mut().unwrap().values_mut() {
        let targets = profile["targets"].as_object_mut().unwrap();
        for (sink_id, target) in targets {
            let live_qualified_count = declarations
                .iter()
                .filter(|declaration| {
                    declaration_sink_axis_qualified(
                        declaration,
                        evidence_registry,
                        sink_id,
                        "sink.live",
                    )
                })
                .count();
            let live_passed =
                !declarations.is_empty() && live_qualified_count == declarations.len();
            let web_qualified_count = declarations
                .iter()
                .filter(|declaration| {
                    matches!(
                        declaration["per_type_route_qualification_status"].as_str(),
                        Some("LIVE_WEB_E2E" | "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE")
                    )
                })
                .count();
            let web_passed = !declarations.is_empty() && web_qualified_count == declarations.len();
            let web_e2e_count = declarations
                .iter()
                .filter(|declaration| declaration["per_type_web_status"] == "PASS")
                .count();
            let composed_route_count = declarations
                .iter()
                .filter(|declaration| {
                    declaration["per_type_route_qualification_status"]
                        == "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
                })
                .count();
            let storage_qualification = &target_storage_qualification[sink_id];
            target["native_target"]["status"] =
                storage_qualification["native_target_column"]["status"].clone();
            target["native_target"]["qualified_pair_count"] =
                storage_qualification["native_target_column"]["qualified_pairs"].clone();
            target["native_target"]["expected_pair_count"] =
                storage_qualification["native_target_column"]["expected_pairs"].clone();
            target["native_target"]["evidence"] = json!([format!(
                "Per-type native target-column evidence: {}/{} pairs qualified; remaining types require another separately qualified representation.",
                storage_qualification["native_target_column"]["qualified_pairs"]
                    .as_u64()
                    .unwrap_or_default(),
                storage_qualification["native_target_column"]["expected_pairs"]
                    .as_u64()
                    .unwrap_or_default()
            )]);
            target["logical_value_carrier"] =
                storage_qualification["logical_value_json_carrier"].clone();
            target["web_candidate"]["status"] = json!(if web_passed {
                if composed_route_count > 0 {
                    "PASS_WITH_COMPONENT_COMPOSITION"
                } else {
                    "PASS"
                }
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
            target["web_candidate"]["qualified_declaration_count"] = json!(web_qualified_count);
            target["web_candidate"]["live_web_e2e_declaration_count"] = json!(web_e2e_count);
            target["web_candidate"]["component_composed_declaration_count"] =
                json!(composed_route_count);
            target["web_candidate"]["evidence"] = json!([format!(
                "Per-type route qualification: {web_qualified_count}/{} declarations; {web_e2e_count} have actual Web preview/save/start-gate receipts and {composed_route_count} use explicitly identified same-version MySQL Source + ChangeEvent + Sink composition. Composition is not a Web end-to-end test.",
                declarations.len(),
            )]);
            target["live"]["status"] = json!(if live_passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
            target["live"]["qualified_declaration_count"] = json!(live_qualified_count);
            target["live"]["evidence"] = json!([format!(
                "Derived from artifact-bound per-type sink.live receipts: {live_qualified_count}/{} declarations qualified; each declaration contains its receipt IDs.",
                declarations.len()
            )]);
            let representation_rows = declarations
                .iter()
                .filter(|declaration| {
                    declaration["per_type_qualification_evidence"]["sinks"][sink_id]["outcome"]
                        == "SOURCE_REPRESENTATION_PRESERVED"
                })
                .collect::<Vec<_>>();
            let representation_qualified_count = representation_rows
                .iter()
                .filter(|declaration| {
                    declaration_sink_axis_qualified(
                        declaration,
                        evidence_registry,
                        sink_id,
                        "sink.representation_carrier",
                    ) && declaration_sink_axis_qualified(
                        declaration,
                        evidence_registry,
                        sink_id,
                        "sink.representation_preserved",
                    )
                })
                .count();
            let representation_passed = !representation_rows.is_empty()
                && representation_qualified_count == representation_rows.len();
            target["representation_carrier"]["status"] = json!(if representation_rows.is_empty() {
                "NOT_REQUIRED"
            } else if representation_passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
            target["representation_carrier"]["qualified_declaration_count"] =
                json!(representation_qualified_count);
            target["representation_carrier"]["evidence"] = json!([format!(
                "Derived from sink.representation_carrier and sink.representation_preserved receipts: {representation_qualified_count}/{} declarations qualified; read-back proves envelope integrity only, not native value recovery.",
                representation_rows.len()
            )]);
        }
    }
    let sink_targets =
        &sink_profiles[inventory["default_sink_evidence_profile"].as_str().unwrap()]["targets"];

    let mut sink_outcomes = inventory["sink_representation_outcomes"].clone();
    for (sink_id, outcomes) in sink_outcomes.as_object_mut().unwrap() {
        let value_rows = declarations
            .iter()
            .filter(|declaration| {
                declaration["per_type_qualification_evidence"]["sinks"][sink_id]["outcome"]
                    == "VALUE_PRESERVED"
            })
            .collect::<Vec<_>>();
        let representation_rows = declarations
            .iter()
            .filter(|declaration| {
                declaration["per_type_qualification_evidence"]["sinks"][sink_id]["outcome"]
                    == "SOURCE_REPRESENTATION_PRESERVED"
            })
            .collect::<Vec<_>>();
        let null_only_rows = declarations
            .iter()
            .filter(|declaration| {
                declaration["per_type_qualification_evidence"]["sinks"][sink_id]["outcome"]
                    == "NULL_PRESERVED"
            })
            .collect::<Vec<_>>();
        let value_qualified_count = value_rows
            .iter()
            .filter(|declaration| {
                declaration_sink_axis_qualified(
                    declaration,
                    evidence_registry,
                    sink_id,
                    "sink.live",
                )
            })
            .count();
        let value_passed = !value_rows.is_empty() && value_qualified_count == value_rows.len();
        let representation_qualified_count = representation_rows
            .iter()
            .filter(|declaration| {
                declaration_sink_axis_qualified(
                    declaration,
                    evidence_registry,
                    sink_id,
                    "sink.representation_carrier",
                ) && declaration_sink_axis_qualified(
                    declaration,
                    evidence_registry,
                    sink_id,
                    "sink.representation_preserved",
                )
            })
            .count();
        let representation_passed = !representation_rows.is_empty()
            && representation_qualified_count == representation_rows.len();
        let null_only_qualified_count = null_only_rows
            .iter()
            .filter(|declaration| {
                declaration["per_type_qualification_status"] == "PASS"
                    && declaration_sink_axis_qualified(
                        declaration,
                        evidence_registry,
                        sink_id,
                        "sink.live",
                    )
            })
            .count();
        let null_only_passed =
            !null_only_rows.is_empty() && null_only_qualified_count == null_only_rows.len();
        outcomes["value_preserved"]["status"] =
            json!(if value_passed { "PASS" } else { "NOT_REQUIRED" });
        outcomes["value_preserved"]["qualified_declaration_count"] = json!(value_qualified_count);
        outcomes["value_preserved"]["evidence"] = json!([format!(
            "Derived from artifact-bound sink.live receipts for VALUE_PRESERVED declarations: {value_qualified_count}/{} qualified; detailed receipt IDs are attached to declarations.",
            value_rows.len()
        )]);
        outcomes["source_representation_preserved"]["status"] =
            json!(if representation_rows.is_empty() {
                "NOT_REQUIRED"
            } else if representation_passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
        outcomes["source_representation_preserved"]["qualified_declaration_count"] =
            json!(representation_qualified_count);
        outcomes["source_representation_preserved"]["evidence"] = json!([format!(
            "Derived from artifact-bound sink.representation_carrier and sink.representation_preserved receipts for SOURCE_REPRESENTATION_PRESERVED declarations: {representation_qualified_count}/{} qualified; read-back proves envelope integrity only, not native value recovery.",
            representation_rows.len()
        )]);
        outcomes["null_only_preserved"] = json!({
            "status": if null_only_rows.is_empty() {
                "NOT_REQUIRED"
            } else if null_only_passed {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            },
            "qualified_declaration_count": null_only_qualified_count,
            "expected_declaration_count": null_only_rows.len(),
            "value_coverage": "NULL_ONLY; this does not claim non-NULL value preservation",
            "evidence": [format!(
                "Derived from explicit NULL_ONLY source receipts and target NULL readback receipts: {null_only_qualified_count}/{} qualified.",
                null_only_rows.len()
            )]
        });
    }

    let representation_declarations = declarations
        .iter()
        .filter(|declaration| declaration_requires_source_representation(declaration))
        .collect::<Vec<_>>();
    let web_representation_passed = !representation_declarations.is_empty()
        && representation_declarations.iter().all(|declaration| {
            matches!(
                declaration["per_type_route_qualification_status"].as_str(),
                Some("LIVE_WEB_E2E" | "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE")
            )
        });
    let web_representation_composed_count = representation_declarations
        .iter()
        .filter(|declaration| {
            declaration["per_type_route_qualification_status"]
                == "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
        })
        .count();
    let mut web_representation_policy = inventory["web_representation_policy"].clone();
    web_representation_policy["status"] = json!(if representation_declarations.is_empty() {
        "NOT_REQUIRED"
    } else if web_representation_passed {
        if web_representation_composed_count > 0 {
            "PASS_WITH_COMPONENT_COMPOSITION"
        } else {
            "PASS"
        }
    } else {
        "REQUIRES_PER_TYPE_QUALIFICATION"
    });
    web_representation_policy["component_composed_declaration_count"] =
        json!(web_representation_composed_count);
    web_representation_policy["qualified_declaration_count"] =
        json!(if web_representation_passed {
            representation_declarations.len()
        } else {
            0
        });

    let mut dynamic_catalog_status_by_connector =
        inventory["dynamic_catalog_status_by_connector"].clone();
    for connector in inventory["connectors"].as_array().unwrap() {
        let connector_id = connector["id"].as_str().unwrap();
        let mapping_class = dynamic_classes.iter().find(|class| {
            class["connector"] == connector_id
                && class["catalog_mapping_status"] == "PASS"
                && matches!(
                    class["id"].as_str(),
                    Some("postgresql.other_defined_catalog_types" | "mysql.plugin_or_engine_types")
                )
        });
        if let Some(class) = mapping_class {
            dynamic_catalog_status_by_connector[connector_id]["unmatched_type_status"] =
                json!("NONE_IN_ENUMERATED_CATALOG");
            dynamic_catalog_status_by_connector[connector_id]["status"] =
                json!("REQUIRES_PER_TYPE_QUALIFICATION");
            dynamic_catalog_status_by_connector[connector_id]["evidence"] =
                class["catalog_mapping_evidence"].clone();
        }
    }
    let has_missing_implementation = !gaps.is_empty()
        || dynamic_classes
            .iter()
            .any(|class| class["unmatched_type_status"] == "MISSING_IMPLEMENTATION");
    let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
    let mut evidence_by_type_scope =
        BTreeMap::<(String, String, Option<String>, String), Vec<&Value>>::new();
    let mut catalog_rosters = BTreeMap::<(String, String), BTreeMap<String, &Value>>::new();
    for record in evidence_registry["entries"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let (Some(type_id), Some(connector_id), Some(axis)) = (
            record["type_id"].as_str(),
            record["source_connector_id"].as_str(),
            record["axis"].as_str(),
        ) else {
            continue;
        };
        let sink_id = record["sink_connector_id"].as_str().map(str::to_owned);
        evidence_by_type_scope
            .entry((
                type_id.to_owned(),
                connector_id.to_owned(),
                sink_id,
                axis.to_owned(),
            ))
            .or_default()
            .push(record);
        if axis == "source.catalog_type_mapping"
            && let Some(class_id) = record["catalog_class_id"].as_str()
        {
            catalog_rosters
                .entry((connector_id.to_owned(), class_id.to_owned()))
                .or_default()
                .entry(type_id.to_owned())
                .or_insert(record);
        }
    }
    for class in &mut dynamic_classes {
        let class_id = class["id"].as_str().unwrap_or_default().to_owned();
        if class_id == "mysql.plugin_or_engine_types"
            && class["catalog_mapping_status"] == "PASS"
            && class["unmatched_type_status"] == "NONE_IN_ENUMERATED_CATALOG"
        {
            class["qualification_mode"] = json!("unmatched_type_guard");
            class["status"] = json!("PASS");
            class["fixture_status"] = json!("NOT_PRESENT_NO_UNMATCHED_TYPE");
            class["sink_qualification_status"] = json!("NOT_APPLICABLE_NO_UNMATCHED_TYPE");
            class["web_qualification_status"] = json!("NOT_APPLICABLE_NO_UNMATCHED_TYPE");
            class["qualification_basis"] = json!(
                "the live visible-column and binlog type scan found no declaration outside the versioned MySQL inventory; any future unmapped declaration fails closed"
            );
        } else {
            class["qualification_mode"] = json!("per_catalog_type_instance");
            let connector_id = class["connector"].as_str().unwrap_or_default();
            let sink_ids = versions();
            let roster_by_type = catalog_rosters
                .get(&(connector_id.to_owned(), class_id.clone()))
                .cloned()
                .unwrap_or_default();
            let mut qualified_instances = Vec::new();
            for (type_id, roster_record) in &roster_by_type {
                let evidence_ids_for = |axis_name: &str, sink: Option<&str>| {
                    evidence_by_type_scope
                        .get(&(
                            type_id.clone(),
                            connector_id.to_owned(),
                            sink.map(str::to_owned),
                            axis_name.to_owned(),
                        ))
                        .into_iter()
                        .flatten()
                        .filter_map(|record| record["id"].as_str().map(str::to_owned))
                        .collect::<Vec<_>>()
                };
                let axis_pass = |axis_name: &str, sink: Option<&str>| {
                    let evidence = evidence_ids_for(axis_name, sink);
                    evidence_axis_pass(
                        &json!({ "status": "PASS", "evidence": evidence }),
                        evidence_registry,
                        type_id,
                        connector_id,
                        sink,
                        axis_name,
                    )
                };
                let representation =
                    roster_record["representation_mode"] == "SOURCE_REPRESENTATION";
                let null_only = source_null_only_evidence_pass(
                    &json!({
                        "status": "PASS",
                        "value_coverage": "NULL_ONLY",
                        "evidence": evidence_ids_for("source.null_only", None)
                    }),
                    evidence_registry,
                    type_id,
                    connector_id,
                );
                let user_storable = roster_record["user_storable"] != false;
                let (source, sinks, web) = if user_storable {
                    let source = json!({
                        "catalog_type_mapping": if axis_pass("source.catalog_type_mapping", None) { "PASS" } else { "REQUIRES" },
                        "protocol_capture": if axis_pass("source.protocol_capture", None) { "PASS" } else { "REQUIRES" },
                        "change_event": if axis_pass("source.change_event", None) { "PASS" } else { "REQUIRES" },
                        "live": if axis_pass("source.live", None) { "PASS" } else { "REQUIRES" },
                        "semantic_codec": if !representation && !null_only && axis_pass("source.semantic_codec", None) { "PASS" } else { "NOT_REQUIRED" },
                        "source_representation_capture": if representation && axis_pass("source.source_representation_capture", None) { "PASS" } else { "NOT_REQUIRED" },
                        "protocol_framing": if representation && axis_pass("source.protocol_framing", None) { "PASS" } else { "NOT_REQUIRED" },
                        "null_only": if null_only { "PASS" } else { "NOT_REQUIRED" }
                    });
                    let mut sinks = serde_json::Map::new();
                    let mut web = serde_json::Map::new();
                    for sink_id in &sink_ids {
                        let receipt_ids = evidence_ids_for("sink.live", Some(sink_id));
                        let records = evidence_by_type_scope
                            .get(&(
                                type_id.clone(),
                                connector_id.to_owned(),
                                Some(sink_id.clone()),
                                "sink.live".to_owned(),
                            ))
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>();
                        let expected_outcome = if null_only {
                            "NULL_PRESERVED"
                        } else if representation {
                            "SOURCE_REPRESENTATION_PRESERVED"
                        } else {
                            "VALUE_PRESERVED"
                        };
                        let readback_pass = axis_pass("sink.live", Some(sink_id))
                            && !records.is_empty()
                            && records.iter().all(|record| {
                                record["target_readback_status"] == "PASS"
                                    && record["outcome"] == expected_outcome
                            });
                        sinks.insert(
                            sink_id.clone(),
                            json!({
                                "live": if !receipt_ids.is_empty() && readback_pass { "PASS" } else { "REQUIRES" },
                                "target_readback": if readback_pass { "PASS" } else { "REQUIRES" }
                            }),
                        );
                        web.insert(
                            sink_id.clone(),
                            json!(if axis_pass("web.plan", Some(sink_id)) {
                                "PASS"
                            } else {
                                "REQUIRES"
                            }),
                        );
                    }
                    (source, sinks, web)
                } else {
                    let source = json!({
                        "catalog_type_mapping": if axis_pass("source.catalog_type_mapping", None) { "PASS" } else { "REQUIRES" },
                        "protocol_capture": "NOT_APPLICABLE_NON_STORABLE",
                        "change_event": "NOT_APPLICABLE_NON_STORABLE",
                        "live": "NOT_APPLICABLE_NON_STORABLE",
                        "semantic_codec": "NOT_APPLICABLE_NON_STORABLE",
                        "source_representation_capture": "NOT_APPLICABLE_NON_STORABLE",
                        "protocol_framing": "NOT_APPLICABLE_NON_STORABLE",
                        "null_only": "NOT_APPLICABLE_NON_STORABLE"
                    });
                    let sinks = sink_ids
                        .iter()
                        .map(|sink_id| {
                            (
                                sink_id.clone(),
                                json!({
                                    "live": "NOT_APPLICABLE_NON_STORABLE",
                                    "target_readback": "NOT_APPLICABLE_NON_STORABLE"
                                }),
                            )
                        })
                        .collect::<serde_json::Map<_, _>>();
                    let web = sink_ids
                        .iter()
                        .map(|sink_id| (sink_id.clone(), json!("NOT_APPLICABLE_NON_STORABLE")))
                        .collect::<serde_json::Map<_, _>>();
                    (source, sinks, web)
                };
                qualified_instances.push(json!({
                    "type_id": type_id,
                    "schema": roster_record["schema"],
                    "name": roster_record["name"],
                    "definition_digest": roster_record["definition_digest"],
                    "mapping_id": roster_record["mapping_id"],
                    "representation_mode": roster_record["representation_mode"],
                    "value_coverage": if null_only { "NULL_ONLY" } else { "NON_NULL_LIVE" },
                    "user_storable": user_storable,
                    "non_storable_reason": roster_record["non_storable_reason"],
                    "source": source,
                    "sinks": sinks,
                    "web": web
                }));
            }
            class["qualified_type_instances"] = json!(qualified_instances);
            class["catalog_type_count"] = json!(roster_by_type.len());
            class["user_storable_type_count"] = json!(
                roster_by_type
                    .values()
                    .filter(|record| record["user_storable"] != false)
                    .count()
            );
            class["non_storable_type_count"] = json!(
                roster_by_type
                    .values()
                    .filter(|record| record["user_storable"] == false)
                    .count()
            );
            class["catalog_mapping_status"] = json!(if !roster_by_type.is_empty()
                && qualified_instances
                    .iter()
                    .all(|instance| instance["source"]["catalog_type_mapping"] == "PASS")
            {
                "PASS"
            } else {
                "REQUIRES"
            });
            class["qualification_basis"] = json!(
                "catalog-wide mapping is inventory evidence only; each concrete type definition included in the live qualification fixtures needs its own source capture, six-sink value/representation readback (or explicit NULL_ONLY source and target evidence), and six Web-plan receipts"
            );
            class["status"] = json!("PASS");
            if !dynamic_type_class_qualification_pass(class) {
                class["status"] = json!("REQUIRES_PER_TYPE_CATALOG_QUALIFICATION");
            }
        }
    }
    if let Some(path) = std::env::var_os("CDC_PG_CATALOG_ROUTE_AUDIT") {
        let audit = serde_json::from_slice::<Value>(
            &std::fs::read(path).expect("read PostgreSQL route-scoped catalog audit"),
        )
        .expect("PostgreSQL route-scoped catalog audit is valid JSON");
        apply_route_scoped_catalog_qualification(&mut dynamic_classes, &audit);
    }
    for (connector_id, status) in dynamic_catalog_status_by_connector
        .as_object_mut()
        .expect("dynamic catalog connector statuses")
    {
        let classes = dynamic_classes
            .iter()
            .filter(|class| class["connector"].as_str() == Some(connector_id.as_str()))
            .collect::<Vec<_>>();
        if !classes.is_empty() {
            status["status"] = json!(if classes
                .iter()
                .all(|class| dynamic_type_class_qualification_pass(class))
            {
                "PASS"
            } else {
                "REQUIRES_PER_TYPE_QUALIFICATION"
            });
            status["qualification_basis"] = json!(
                "See dynamic_type_classes for route-scoped exact catalog receipts or the MySQL unmatched-type guard."
            );
        }
    }
    let all_evidence_qualified = !has_missing_implementation
        && gaps.is_empty()
        && declarations
            .iter()
            .all(|declaration| declaration["qualification_fixture_status"] == "PER_TYPE_QUALIFIED")
        && source_profiles
            .as_object()
            .unwrap()
            .values()
            .all(|profile| {
                ["protocol_capture", "semantic_codec", "change_event"]
                    .into_iter()
                    .all(|axis| {
                        matches!(
                            profile[axis]["status"].as_str(),
                            Some("PASS" | "NOT_REQUIRED")
                        )
                    })
                    && matches!(
                        profile["source_representation_capture"]["status"].as_str(),
                        Some("PASS" | "NOT_REQUIRED")
                    )
            })
        && protocol_profiles
            .as_object()
            .unwrap()
            .iter()
            .all(|(source_id, profile)| {
                !representation_sources.contains(source_id) || profile["status"] == "PASS"
            })
        && (!representation_required || source_representation_contract["status"] == "PASS")
        && sink_targets
            .as_object()
            .unwrap()
            .iter()
            .all(|(_sink_id, target)| {
                matches!(
                    target["web_candidate"]["status"].as_str(),
                    Some("PASS" | "PASS_WITH_COMPONENT_COMPOSITION")
                ) && target["live"]["status"] == "PASS"
                    && matches!(
                        target["representation_carrier"]["status"].as_str(),
                        Some("PASS" | "NOT_REQUIRED")
                    )
            })
        && sink_outcomes
            .as_object()
            .unwrap()
            .iter()
            .all(|(sink_id, outcomes)| {
                outcomes["value_preserved"]["status"] == "PASS"
                    || (representation_sinks.contains(sink_id)
                        && outcomes["source_representation_preserved"]["status"] == "PASS")
                    || outcomes["null_only_preserved"]["status"] == "PASS"
            })
        && (!representation_required
            || matches!(
                web_representation_policy["status"].as_str(),
                Some("PASS" | "PASS_WITH_COMPONENT_COMPOSITION")
            ))
        && dynamic_classes
            .iter()
            .all(dynamic_type_class_qualification_pass)
        && types_without_fixture == 0
        && types_with_family_fixture_only == 0;
    let mut report_evidence_registry =
        inventory["per_type_qualification"]["evidence_registry"].clone();
    report_evidence_registry
        .as_object_mut()
        .expect("type evidence registry")
        .remove("by_id");
    json!({
        "schema": inventory["schema"],
        "inventory_revision": inventory["revision"],
        "per_type_qualification_schema": inventory["per_type_qualification"]["schema"],
        "per_type_qualification_evidence_registry": report_evidence_registry,
        "native_type_count": inventory["types"].as_array().unwrap().len(),
        "source_declaration_count": declarations.len(),
        "source_mapping_gaps": gaps.len(),
        "types_without_qualification_fixture": types_without_fixture,
        "types_with_family_fixture_only": types_with_family_fixture_only,
        "per_type_live_source_gaps": per_type_live_source_gaps,
        "per_type_live_sink_gaps": per_type_live_sink_gaps,
        "per_type_web_plan_gaps": per_type_web_plan_gaps,
        "per_type_web_sink_plan_gaps": per_type_web_sink_plan_gaps,
        "per_type_web_sink_total_pairs": expected_type_source_pairs.len() * versions().len(),
        "per_type_live_web_e2e_qualified_pairs": expected_type_source_pairs.len() * versions().len()
            - per_type_web_sink_plan_gaps,
        "per_type_live_web_e2e_gaps": per_type_web_sink_plan_gaps,
        "per_type_route_qualification_gaps": per_type_route_qualification_gaps,
        "per_type_component_composed_route_pairs": per_type_component_composed_route_pairs,
        "ignored_synthetic_web_evidence": inventory["per_type_qualification"]["ignored_synthetic_web_evidence"],
        "per_type_live_sink_qualified_pairs": qualified_sink_live_pairs.len(),
        "per_type_live_sink_total_pairs": expected_type_source_pairs.len() * versions().len(),
        "per_type_live_sink_pair_scope": "qualified target representation: native target column, tagged LogicalValue carrier, or source representation carrier",
        "per_type_live_sink_pair_gaps": expected_type_source_pairs.len() * versions().len()
            - qualified_sink_live_pairs.len(),
        "target_storage_qualification": target_storage_qualification,
        "native_target_qualified_pairs": native_target_qualified_pairs.len(),
        "native_target_expected_pairs": expected_semantic_sink_pairs.len(),
        "native_target_pair_gaps": expected_semantic_sink_pairs
            .len()
            .saturating_sub(native_target_qualified_pairs.len()),
        "logical_value_carrier_qualified_pairs": logical_value_carrier_qualified_pairs.len(),
        "logical_value_carrier_expected_pairs": expected_semantic_sink_pairs.len(),
        "source_representation_carrier_qualified_pairs":
            representation_carrier_qualified_pairs.len(),
        "source_representation_carrier_expected_pairs":
            expected_representation_sink_pairs.len(),
        "source_representation_qualification_required": representation_required,
        "status": inventory_status(has_missing_implementation, all_evidence_qualified),
        "implementation_gaps": gaps,
        "declarations": declarations,
        "source_representation_contract": source_representation_contract,
        "source_evidence_profiles": source_profiles,
        "sink_evidence_profiles": sink_profiles,
        "sink_representation_outcomes": sink_outcomes,
        "web_representation_policy": web_representation_policy,
        "dynamic_type_classes": dynamic_classes,
        "dynamic_catalog_status_by_connector": dynamic_catalog_status_by_connector,
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
    let mut sample_value = match case.id {
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
    if let Some(value) = sample_value.as_mut() {
        apply_source_value_representation(value, &source_mapping);
    }
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
        validate_datum_against_plan(plan, &Datum::Unavailable).unwrap();
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
    assert_eq!(
        inventory["per_type_qualification"]["web_evidence_verification"],
        "web_ui.preview_save_start_gate"
    );
    assert_eq!(
        inventory["per_type_qualification"]["same_version_web_evidence_verification"],
        "per_type_source_change_event_sink_evidence_plus_offline_route_matrix; optional_web_ui.preview_save_start_gate_requires_distinct_same_version_sink_server"
    );
    assert!(inventory["per_type_qualification"]["route_qualification_modes"]
        ["COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"]
        .as_str()
        .unwrap()
        .contains("not a live Web route test"));
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
        "source.null_only",
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
    let distinct_source_types = coverage["declarations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|declaration| {
            (
                declaration["type_id"].as_str().unwrap(),
                declaration["source"].as_str().unwrap(),
            )
        })
        .collect::<BTreeSet<_>>()
        .len();
    assert!(distinct_source_types < declaration_count);
    assert_eq!(
        coverage["per_type_web_sink_total_pairs"].as_u64().unwrap(),
        (distinct_source_types * versions().len()) as u64,
        "aliases and parameter examples are not independent Web qualification pairs"
    );
    assert_eq!(
        coverage["per_type_web_sink_plan_gaps"],
        coverage["per_type_web_sink_total_pairs"]
    );
    assert_eq!(coverage["source_mapping_gaps"].as_u64().unwrap(), 0);
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
fn per_type_completion_requires_adapter_evidence_and_route_specific_web_or_composition() {
    let sink_ids = versions();
    let type_id = "example.native_type";
    let source_id = "mysql_5_7";
    let evidence_registry =
        qualified_type_evidence_registry(type_id, source_id, &sink_ids, "VALUE_PRESERVED");
    let qualified =
        qualified_type_evidence_fixture(type_id, source_id, &sink_ids, "VALUE_PRESERVED");
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
        per_type_web_status(
            &qualified,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "PASS"
    );
    assert_eq!(
        per_type_route_qualification_status(
            &qualified,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "LIVE_WEB_E2E"
    );
    assert!(per_type_qualification_pass(
        &qualified,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut same_version_web_missing = qualified.clone();
    same_version_web_missing["web"]["sinks"][source_id]["evidence"] = json!([]);
    assert_eq!(
        per_type_web_status(
            &same_version_web_missing,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "NOT_QUALIFIED_PER_NATIVE_TYPE",
        "component composition must never be reported as an actual Web test"
    );
    assert_eq!(
        per_type_route_qualification_status(
            &same_version_web_missing,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE"
    );
    assert!(per_type_qualification_pass(
        &same_version_web_missing,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut missing_cross_version_web = qualified.clone();
    missing_cross_version_web["web"]["sinks"]["mysql_8_0"]["evidence"] = json!([]);
    assert_eq!(
        per_type_route_qualification_status(
            &missing_cross_version_web,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "REQUIRES_PER_TYPE_WEB_EVIDENCE",
        "only the identical MySQL connector route may use component composition"
    );
    assert!(!per_type_qualification_pass(
        &missing_cross_version_web,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let postgres_source = "postgresql_15";
    let mut missing_postgres_self_web =
        qualified_type_evidence_fixture(type_id, postgres_source, &sink_ids, "VALUE_PRESERVED");
    let postgres_registry =
        qualified_type_evidence_registry(type_id, postgres_source, &sink_ids, "VALUE_PRESERVED");
    missing_postgres_self_web["web"]["sinks"][postgres_source]["evidence"] = json!([]);
    assert_eq!(
        per_type_route_qualification_status(
            &missing_postgres_self_web,
            &postgres_registry,
            type_id,
            postgres_source,
            &sink_ids
        ),
        "REQUIRES_PER_TYPE_WEB_EVIDENCE",
        "component composition is currently limited to same-version MySQL routes"
    );
    let mut missing_semantic_path = qualified.clone();
    missing_semantic_path["source"]["semantic_codec"] = Value::Null;
    assert!(
        !per_type_qualification_pass(
            &missing_semantic_path,
            &evidence_registry,
            type_id,
            source_id,
            &sink_ids
        ),
        "native/value-preserving sinks still require exact semantic source evidence"
    );
    let mut dynamic = qualified.clone();
    dynamic.as_object_mut().unwrap().remove("web");
    assert!(dynamic_type_sink_qualification_pass(
        &dynamic,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    dynamic["sinks"]["mysql_5_7"]["live_evidence"] = json!([]);
    assert!(!dynamic_type_sink_qualification_pass(
        &dynamic,
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

    let mixed_sink_outcomes = BTreeMap::from([(
        "postgresql_17".to_owned(),
        "SOURCE_REPRESENTATION_PRESERVED".to_owned(),
    )]);
    let mixed_registry = qualified_type_evidence_registry_with_sink_outcomes(
        type_id,
        source_id,
        &sink_ids,
        "VALUE_PRESERVED",
        &mixed_sink_outcomes,
    );
    let mut mixed_value_and_representation = qualified.clone();
    mixed_value_and_representation["sinks"]["postgresql_17"]["outcome"] =
        json!("SOURCE_REPRESENTATION_PRESERVED");
    mixed_value_and_representation["source"]["source_representation_capture"] = Value::Null;
    mixed_value_and_representation["source"]["protocol_framing"] = Value::Null;
    assert!(!per_type_qualification_pass(
        &mixed_value_and_representation,
        &mixed_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    mixed_value_and_representation["source"]["source_representation_capture"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.source_representation_capture")] });
    mixed_value_and_representation["source"]["protocol_framing"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.protocol_framing")] });
    assert!(per_type_qualification_pass(
        &mixed_value_and_representation,
        &mixed_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut representation_only = qualified.clone();
    let representation_sink_outcomes = sink_ids
        .iter()
        .map(|sink_id| {
            (
                sink_id.clone(),
                "SOURCE_REPRESENTATION_PRESERVED".to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let representation_registry = qualified_type_evidence_registry_with_sink_outcomes(
        type_id,
        source_id,
        &sink_ids,
        "VALUE_PRESERVED",
        &representation_sink_outcomes,
    );
    for sink_id in &sink_ids {
        representation_only["sinks"][sink_id]["outcome"] = json!("SOURCE_REPRESENTATION_PRESERVED");
    }
    representation_only["source"]["source_representation_capture"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.source_representation_capture")] });
    representation_only["source"]["protocol_framing"] = json!({ "status": "PASS", "evidence": [format!("{type_id}@{source_id}:source.protocol_framing")] });
    representation_only["source"]["semantic_codec"] = Value::Null;
    assert!(per_type_qualification_pass(
        &representation_only,
        &representation_registry,
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
        &representation_registry,
        type_id,
        source_id,
        &sink_ids
    ));
    let scoped_receipts = Value::Object(serde_json::Map::from_iter([(
        format!("{type_id}@{source_id}"),
        mixed_value_and_representation,
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
fn null_only_types_require_explicit_null_receipts_and_readback_from_every_sink() {
    let sink_ids = versions();
    let type_id = "postgresql.gtsvector";
    let source_id = "postgresql_15";
    let evidence_registry =
        qualified_type_evidence_registry(type_id, source_id, &sink_ids, "NULL_PRESERVED");
    let mut null_only =
        qualified_type_evidence_fixture(type_id, source_id, &sink_ids, "NULL_PRESERVED");
    null_only["source"]["semantic_codec"] = Value::Null;
    null_only["source"]["source_representation_capture"] = Value::Null;
    null_only["source"]["protocol_framing"] = Value::Null;

    assert!(per_type_qualification_pass(
        &null_only,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut missing_null_only_source_receipt = null_only.clone();
    missing_null_only_source_receipt["source"]["null_only"] = Value::Null;
    assert!(!per_type_qualification_pass(
        &missing_null_only_source_receipt,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut mixed_value_coverage = null_only.clone();
    mixed_value_coverage["sinks"]["mysql_5_7"]["outcome"] = json!("VALUE_PRESERVED");
    assert!(!per_type_qualification_pass(
        &mixed_value_coverage,
        &evidence_registry,
        type_id,
        source_id,
        &sink_ids
    ));

    let mut missing_null_coverage = evidence_registry.clone();
    let null_axis = missing_null_coverage["entries"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["axis"] == "source.null_only")
        .unwrap();
    null_axis["value_coverage"] = json!("NON_NULL_LIVE");
    assert!(!per_type_qualification_pass(
        &null_only,
        &missing_null_coverage,
        type_id,
        source_id,
        &sink_ids
    ));

    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        evidence_registry["entries"][0]["artifact_path"]
            .as_str()
            .unwrap(),
    );
    std::fs::remove_file(artifact_path).expect("remove null-only evidence fixture");
}

#[test]
fn bit_string_qualification_values_follow_each_source_bit_order() {
    use fixture::SourceVersion::{Mysql57, Mysql80, Mysql84, Postgresql15};

    let bit_string = cases()
        .into_iter()
        .find(|case| case.id == "bit_string")
        .expect("the matrix includes bit-string coverage");
    let manifest = manifest(Postgresql15);
    for source in [Mysql57, Mysql80, Mysql84] {
        let result = run_case(source, Postgresql15, &manifest, &bit_string);
        assert_eq!(result["offline"], "PASS", "{}", source.version());
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
fn type_inventory_live_evidence_report() {
    let Some(report_path) = std::env::var_os("CDC_QUALIFICATION_REPORT") else {
        return;
    };
    let report_path = PathBuf::from(report_path);
    if !report_path.is_file() {
        return;
    }
    let mut report: Value = serde_json::from_slice(
        &std::fs::read(&report_path).expect("read offline qualification report"),
    )
    .expect("offline qualification report is valid JSON");
    report["type_inventory"] = type_inventory_coverage();
    assert!(
        !has_stale_pass_evidence(&report["type_inventory"]),
        "a PASS qualification report must replace stale pending/not-implemented evidence"
    );
    std::fs::write(
        report_path,
        serde_json::to_vec_pretty(&report).expect("serialize live type inventory report"),
    )
    .expect("write live type inventory report");
    let evidence_artifact_count =
        report["type_inventory"]["per_type_qualification_evidence_registry"]["entries"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|entry| entry["artifact_path"].as_str())
                    .collect::<BTreeSet<_>>()
                    .len()
            })
            .unwrap_or_default();
    println!("updated native type evidence from {evidence_artifact_count} report artifact(s)");
}

fn has_stale_pass_evidence(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            let stale_pass = object.get("status").and_then(Value::as_str) == Some("PASS")
                && object.get("evidence").is_some_and(|evidence| {
                    let evidence = evidence.to_string().to_ascii_lowercase();
                    ["not implemented", "pending", "not registered"]
                        .iter()
                        .any(|stale| evidence.contains(stale))
                });
            stale_pass || object.values().any(has_stale_pass_evidence)
        }
        Value::Array(values) => values.iter().any(has_stale_pass_evidence),
        _ => false,
    }
}

#[test]
fn qualification_report_rejects_pass_with_stale_evidence_text() {
    let stale = json!({
        "status": "PASS",
        "evidence": ["Envelope write/readback qualification is pending."]
    });
    let current = json!({
        "status": "PASS",
        "evidence": ["Derived from artifact-bound sink receipts; declaration contains receipt IDs."]
    });
    assert!(has_stale_pass_evidence(&stale));
    assert!(!has_stale_pass_evidence(&current));
}

#[test]
fn merged_sink_outcomes_use_the_qualification_gate_field_and_reject_conflicts() {
    let mut sink = json!({});
    merge_sink_outcome(&mut sink, "VALUE_PRESERVED");
    assert_eq!(sink["outcome"], "VALUE_PRESERVED");
    assert_eq!(sink["live_outcome"], "VALUE_PRESERVED");

    let mut null_then_value = json!({});
    merge_sink_outcome(&mut null_then_value, "NULL_PRESERVED");
    merge_sink_outcome(&mut null_then_value, "VALUE_PRESERVED");
    merge_sink_outcome(&mut null_then_value, "NULL_PRESERVED");
    assert_eq!(null_then_value["outcome"], "VALUE_PRESERVED");

    let mut representation_then_null = json!({});
    merge_sink_outcome(
        &mut representation_then_null,
        "SOURCE_REPRESENTATION_PRESERVED",
    );
    merge_sink_outcome(&mut representation_then_null, "NULL_PRESERVED");
    assert_eq!(
        representation_then_null["outcome"],
        "SOURCE_REPRESENTATION_PRESERVED"
    );

    let conflicting = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        merge_sink_outcome(&mut sink, "SOURCE_REPRESENTATION_PRESERVED");
    }));
    assert!(conflicting.is_err());
}

#[test]
fn representation_evidence_preserves_the_explicit_carrier_mode() {
    let json_carrier = json!({
        "axis": "sink.live",
        "outcome": "SOURCE_REPRESENTATION_PRESERVED",
        "target_storage_mode": "logical_value_json_carrier"
    });
    let blob_carrier = json!({
        "axis": "sink.live",
        "outcome": "SOURCE_REPRESENTATION_PRESERVED",
        "target_storage_mode": "source_representation_blob_carrier"
    });
    let invalid_native = json!({
        "axis": "sink.live",
        "outcome": "SOURCE_REPRESENTATION_PRESERVED",
        "target_storage_mode": "native_target_column"
    });

    assert_eq!(
        inferred_target_storage_mode(&json_carrier, &json!("web.dynamic")),
        Some("logical_value_json_carrier")
    );
    assert_eq!(
        inferred_target_storage_mode(&blob_carrier, &json!("web.dynamic")),
        Some("source_representation_blob_carrier")
    );
    assert_eq!(
        inferred_target_storage_mode(&invalid_native, &json!("web.dynamic")),
        Some("source_representation_blob_carrier")
    );
}

#[test]
fn dynamic_type_web_gate_counts_only_verified_sink_routes() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("workspace exists");
    let directory = workspace.join("target").join(format!(
        "dynamic-web-evidence-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let artifact_path = directory
        .strip_prefix(&workspace)
        .unwrap()
        .join("receipt.json")
        .to_string_lossy()
        .replace('\\', "/");
    let run_id = "dynamic-web-test";
    let verified_id = format!("dynamic:postgresql.enums@postgresql_15>mysql_5_7:web.plan:{run_id}");
    let unverified_id =
        format!("dynamic:postgresql.enums@postgresql_15>mysql_8_0:web.plan:{run_id}");
    let web_record = |id: String, sink: &str, verified: bool| {
        json!({
            "id": id,
            "type_id": "dynamic:postgresql.enums",
            "source_connector_id": "postgresql_15",
            "sink_connector_id": sink,
            "axis": "web.plan",
            "status": "PASS",
            "run_id": run_id,
            "verification": if verified { "web_ui.preview_save_start_gate" } else { "synthetic" },
            "preview_status": "COMPATIBLE",
            "save_status": "PASS",
            "start_gate_status": "PASS",
            "plan_digest": "a".repeat(64),
            "target_probe_digest": "b".repeat(64),
            "selected_rule_id": "test-carrier",
            "target_storage_mode": "logical_value_json_carrier",
            "risk_confirmation_required": true,
            "risk_confirmation_verified": true
        })
    };
    let artifact = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": "web_ui.dynamic_enum_carrier_live",
        "artifact_path": artifact_path,
        "assertions": ["real preview, save, start, DML and target readback"],
        "evidence": [
            web_record(verified_id, "mysql_5_7", true),
            web_record(unverified_id, "mysql_8_0", false)
        ]
    });
    std::fs::write(
        directory.join("receipt.json"),
        serde_json::to_vec_pretty(&artifact).unwrap(),
    )
    .unwrap();
    let mut inventory = type_inventory();
    merge_live_type_evidence_from_directory(&mut inventory, &directory, &workspace);
    let registry = &inventory["per_type_qualification"]["evidence_registry"];
    let (qualified, missing, evidence) = dynamic_web_plan_coverage(
        registry,
        "dynamic:postgresql.enums",
        "postgresql_15",
        &versions(),
    );
    assert_eq!(qualified, ["mysql_5_7"]);
    assert_eq!(missing.len(), 5);
    assert!(missing.contains(&"mysql_8_0".to_owned()));
    assert_eq!(evidence.len(), 1);
    assert_eq!(
        inventory["per_type_qualification"]["ignored_synthetic_web_evidence"],
        1
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn catalog_guard_pass_requires_the_evidence_for_its_declared_coverage_mode() {
    let mut postgres_catalog = json!({
        "id": "postgresql.other_defined_catalog_types",
        "qualification_mode": "complete_catalog_mapping_with_global_carriers",
        "status": "PASS",
        "catalog_mapping_status": "PASS",
        "fixture_status": "PASS_LIVE_CATALOG_MAPPING_AND_SOURCE_CLASS_FIXTURES",
        "sink_qualification_status": "PASS_GLOBAL_CARRIER_MATRIX",
        "web_qualification_status": "PASS_GLOBAL_WEB_PLAN_MATRIX",
        "unmatched_type_status": "NONE_IN_ENUMERATED_CATALOG"
    });
    assert!(!dynamic_type_class_qualification_pass(&postgres_catalog));

    postgres_catalog["qualification_mode"] = json!("per_type_capture_sink_and_web");
    postgres_catalog["fixture_status"] = json!("PASS_LIVE_SOURCE_TYPE_CLASS_CAPTURE");
    postgres_catalog["sink_qualification_status"] = json!("PASS_SIX_SINK_PER_TYPE_READBACK");
    postgres_catalog["web_qualification_status"] = json!("PASS_SIX_WEB_SAVE_AND_START");
    let evidence = |status: &str| {
        let mut map = serde_json::Map::new();
        for sink in [
            "mysql_5_7",
            "mysql_8_0",
            "mysql_8_4",
            "postgresql_15",
            "postgresql_16",
            "postgresql_17",
        ] {
            map.insert(
                sink.into(),
                json!({"status": status, "evidence": ["actual-live-route"]}),
            );
        }
        Value::Object(map)
    };
    postgres_catalog["sink_evidence_by_connector"] = evidence("PASS");
    postgres_catalog["web_evidence_by_connector"] = evidence("PASS");
    assert!(!dynamic_type_class_qualification_pass(&postgres_catalog));
    postgres_catalog["fixture_status"] = json!("REQUIRES_LIVE_SOURCE_TYPE_CLASS_CAPTURE");
    assert!(!dynamic_type_class_qualification_pass(&postgres_catalog));
    postgres_catalog["fixture_status"] = json!("PASS_LIVE_SOURCE_TYPE_CLASS_CAPTURE");
    postgres_catalog["sink_evidence_by_connector"]["postgresql_16"]["status"] =
        json!("REQUIRES_PER_TYPE_EVIDENCE");
    assert!(!dynamic_type_class_qualification_pass(&postgres_catalog));
    postgres_catalog["sink_evidence_by_connector"]["postgresql_16"]["status"] = json!("PASS");
    postgres_catalog["web_evidence_by_connector"]["postgresql_17"]["status"] =
        json!("REQUIRES_PER_TYPE_EVIDENCE");
    assert!(!dynamic_type_class_qualification_pass(&postgres_catalog));

    let mut mysql_unmatched_type_guard = json!({
        "id": "mysql.plugin_or_engine_types",
        "qualification_mode": "unmatched_type_guard",
        "status": "PASS",
        "catalog_mapping_status": "PASS",
        "fixture_status": "NOT_PRESENT_NO_UNMATCHED_TYPE",
        "sink_qualification_status": "NOT_APPLICABLE_NO_UNMATCHED_TYPE",
        "web_qualification_status": "NOT_APPLICABLE_NO_UNMATCHED_TYPE",
        "unmatched_type_status": "NONE_IN_ENUMERATED_CATALOG"
    });
    assert!(dynamic_type_class_qualification_pass(
        &mysql_unmatched_type_guard
    ));
    mysql_unmatched_type_guard["unmatched_type_status"] = json!("MISSING_IMPLEMENTATION");
    assert!(!dynamic_type_class_qualification_pass(
        &mysql_unmatched_type_guard
    ));
}

#[test]
fn catalog_dynamic_type_class_requires_an_exact_receipt_for_every_type_definition() {
    let mut catalog_class = json!({
        "id": "postgresql.other_defined_catalog_types",
        "qualification_mode": "per_catalog_type_instance",
        "status": "PASS",
        "catalog_mapping_status": "PASS",
        "catalog_type_count": 1,
        "qualified_type_instances": []
    });
    assert!(!dynamic_type_class_qualification_pass(&catalog_class));

    let connectors = versions();
    let mut all_sinks = serde_json::Map::new();
    let mut all_web = serde_json::Map::new();
    for connector in &connectors {
        all_sinks.insert(
            connector.clone(),
            json!({
                "live": "PASS",
                "target_readback": "PASS"
            }),
        );
        all_web.insert(connector.clone(), json!("PASS"));
    }
    catalog_class["qualified_type_instances"] = json!([{
        "type_id": "dynamic:postgresql.instance.public.test_type",
        "schema": "public",
        "name": "test_type",
        "definition_digest": "a".repeat(64),
        "user_storable": true,
        "non_storable_reason": null,
        "value_coverage": "NON_NULL_LIVE",
        "source": {
            "catalog_type_mapping": "PASS",
            "protocol_capture": "PASS",
            "change_event": "PASS",
            "semantic_codec": "PASS"
        },
        "sinks": all_sinks,
        "web": all_web
    }]);
    assert!(dynamic_type_class_qualification_pass(&catalog_class));

    catalog_class["qualified_type_instances"][0]["sinks"]["postgresql_17"]["target_readback"] =
        json!("REQUIRES_LIVE");
    assert!(!dynamic_type_class_qualification_pass(&catalog_class));
}

#[test]
fn dynamic_type_class_requires_explicit_mapping_for_non_storable_catalog_types() {
    let connectors = versions();
    let sinks = connectors
        .iter()
        .map(|connector| {
            (
                connector.clone(),
                json!({
                    "live": "NOT_APPLICABLE_NON_STORABLE",
                    "target_readback": "NOT_APPLICABLE_NON_STORABLE"
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let web = connectors
        .iter()
        .map(|connector| (connector.clone(), json!("NOT_APPLICABLE_NON_STORABLE")))
        .collect::<serde_json::Map<_, _>>();
    let mut class = json!({
        "status": "PASS",
        "catalog_mapping_status": "PASS",
        "qualification_mode": "per_catalog_type_instance",
        "catalog_type_count": 1,
        "qualified_type_instances": [{
            "type_id": "dynamic:postgresql.instance.pg_catalog.pg_attribute:definition",
            "schema": "pg_catalog",
            "name": "pg_attribute",
            "definition_digest": "a".repeat(64),
            "user_storable": false,
            "non_storable_reason": "composite pg_catalog.pg_attribute field attmissingval depends on non-storable PostgreSQL pseudotype pg_catalog.anyarray",
            "source": {
                "catalog_type_mapping": "PASS",
                "protocol_capture": "NOT_APPLICABLE_NON_STORABLE",
                "change_event": "NOT_APPLICABLE_NON_STORABLE",
                "live": "NOT_APPLICABLE_NON_STORABLE"
            },
            "sinks": sinks,
            "web": web
        }]
    });
    assert!(dynamic_type_class_qualification_pass(&class));

    class["qualified_type_instances"][0]["non_storable_reason"] = Value::Null;
    assert!(!dynamic_type_class_qualification_pass(&class));
    class["qualified_type_instances"][0]["non_storable_reason"] = json!("depends on pseudotype");
    class["qualified_type_instances"][0]["sinks"][&connectors[0]]["live"] = json!("PASS");
    assert!(!dynamic_type_class_qualification_pass(&class));
}

#[test]
fn exact_catalog_inventory_requires_one_matching_mapping_receipt_per_definition() {
    let type_id = "dynamic:postgresql.instance.public.sample:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let definition = json!({
        "type_id": type_id,
        "catalog_class_id": "postgresql.enums",
        "schema": "public",
        "name": "sample",
        "type_oid": 42,
        "native_declaration": "\"public\".\"sample\"",
        "definition_digest": "a".repeat(64),
        "mapping_id": "postgresql15.source-type.public.sample",
        "mapping_evidence_digest": "b".repeat(64),
        "logical_type_digest": "c".repeat(64),
        "representation_mode": "SEMANTIC_CODEC",
        "user_storable": true,
        "non_storable_reason": null
    });
    let receipt = json!({
        "id": format!("{type_id}@postgresql_15:source.catalog_type_mapping:run-1"),
        "type_id": type_id,
        "source_connector_id": "postgresql_15",
        "sink_connector_id": null,
        "axis": "source.catalog_type_mapping",
        "status": "PASS",
        "run_id": "run-1",
        "catalog_class_id": "postgresql.enums",
        "schema": "public",
        "name": "sample",
        "type_oid": 42,
        "native_declaration": "\"public\".\"sample\"",
        "definition_digest": "a".repeat(64),
        "mapping_id": "postgresql15.source-type.public.sample",
        "mapping_evidence_digest": "b".repeat(64),
        "logical_type_digest": "c".repeat(64),
        "representation_mode": "SEMANTIC_CODEC",
        "representation_mode": "SEMANTIC_CODEC",
        "user_storable": true,
        "non_storable_reason": null,
        "catalog_type_count": 1,
        "non_storable_type_count": 0,
        "catalog_scope": "visible test catalog",
        "catalog_digest": "d".repeat(64)
    });
    let mut artifact = json!({
        "catalog_inventory_mode": "per_catalog_type_instance",
        "catalog_type_count": 1,
        "non_storable_type_count": 0,
        "catalog_scope": "visible test catalog",
        "catalog_digest": "d".repeat(64),
        "run_id": "run-1",
        "suite_id": "postgresql_15.complete_defined_catalog_type_mapping",
        "catalog_type_roster": [definition],
        "evidence": [receipt]
    });
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_ok());

    artifact["catalog_type_roster"][0]["user_storable"] = json!(false);
    artifact["catalog_type_roster"][0]["non_storable_reason"] =
        json!("composite field attmissingval depends on pseudotype anyarray");
    artifact["non_storable_type_count"] = json!(1);
    artifact["evidence"][0]["user_storable"] = json!(false);
    artifact["evidence"][0]["non_storable_reason"] =
        json!("composite field attmissingval depends on pseudotype anyarray");
    artifact["evidence"][0]["non_storable_type_count"] = json!(1);
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_ok());
    artifact["catalog_type_roster"][0]["non_storable_reason"] = Value::Null;
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());
    artifact["catalog_type_roster"][0]["non_storable_reason"] =
        json!("composite field attmissingval depends on pseudotype anyarray");

    artifact["evidence"] = json!([]);
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());

    artifact["evidence"] = json!([receipt]);
    artifact["catalog_type_roster"][0]["definition_digest"] = json!("e".repeat(64));
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());

    artifact["catalog_type_roster"][0]["definition_digest"] = json!("a".repeat(64));
    artifact["catalog_inventory_mode"] = Value::Null;
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());
    artifact["suite_id"] = json!("web_ui.postgresql_15_dynamic_type_catalog");
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());
    artifact["evidence"] = json!([]);
    assert!(validate_exact_catalog_type_inventory_artifact(&artifact).is_err());

    let mut legacy_roster_receipt = receipt.clone();
    let legacy_roster_record = legacy_roster_receipt
        .as_object_mut()
        .expect("legacy roster receipt is an object");
    for field in [
        "catalog_type_count",
        "excluded_pseudotype_array_count",
        "catalog_scope",
        "catalog_digest",
    ] {
        legacy_roster_record.remove(field);
    }
    let legacy_web_roster = json!({
        "suite_id": "web_ui.postgresql_15_dynamic_type_catalog",
        "catalog_type_roster": [definition],
        "evidence": [legacy_roster_receipt]
    });
    assert!(validate_exact_catalog_type_inventory_artifact(&legacy_web_roster).is_ok());
}

#[test]
fn evidence_import_rejects_incomplete_exact_catalog_inventory() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("qualification workspace exists");
    let artifact_dir = workspace.join("target/qualification").join(format!(
        "incomplete-catalog-import-{}",
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&artifact_dir).expect("create temporary artifact directory");
    let artifact_path = artifact_dir.join("incomplete.json");
    let relative_path = artifact_path
        .strip_prefix(&workspace)
        .expect("artifact is inside the workspace")
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    std::fs::write(
        &artifact_path,
        serde_json::to_vec(&json!({
            "schema": "cdc.type_qualification_evidence_report.v1",
            "run_id": "incomplete-catalog-run",
            "suite_id": "qualification_matrix.incomplete_catalog_import",
            "artifact_path": relative_path,
            "catalog_inventory_mode": "per_catalog_type_instance",
            "catalog_type_count": 2,
            "catalog_scope": "test catalog",
            "catalog_digest": "d".repeat(64),
            "catalog_type_roster": [{
                "type_id": "dynamic:postgresql.instance.public.sample:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "catalog_class_id": "postgresql.enums",
                "schema": "public",
                "name": "sample",
                "type_oid": 42,
                "native_declaration": "\"public\".\"sample\"",
                "definition_digest": "a".repeat(64),
                "mapping_id": "postgresql15.source-type.public.sample",
                "mapping_evidence_digest": "b".repeat(64),
                "logical_type_digest": "c".repeat(64),
                "representation_mode": "SEMANTIC_CODEC"
            }],
            "evidence": [{ "source_connector_id": "postgresql_15" }]
        }))
        .expect("serialize incomplete artifact"),
    )
    .expect("write incomplete artifact");

    let mut inventory = type_inventory();
    let import_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        merge_live_type_evidence_from_directory(&mut inventory, &artifact_dir, &workspace);
    }));
    std::fs::remove_dir_all(&artifact_dir).expect("remove temporary artifact directory");
    assert!(
        import_result.is_err(),
        "an incomplete catalog roster must be rejected by the evidence import path"
    );
}

#[test]
fn repeated_type_evidence_is_idempotent_but_conflicting_ids_are_rejected() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("workspace exists");
    let directory = workspace.join("target").join(format!(
        "type-evidence-dedupe-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let artifact_path = |name: &str| {
        directory
            .join(name)
            .strip_prefix(&workspace)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/")
    };
    let run_id = "dedupe-type-evidence-test";
    let evidence_id = format!("mysql.tinyint@mysql_5_7:source.catalog_type_mapping:{run_id}");
    let source_evidence = json!({
        "id": evidence_id,
        "type_id": "mysql.tinyint",
        "source_connector_id": "mysql_5_7",
        "sink_connector_id": null,
        "axis": "source.catalog_type_mapping",
        "status": "PASS",
        "run_id": run_id
    });
    let write_artifact = |name: &str, assertion: &str, evidence: Value| {
        let artifact = json!({
            "schema": "cdc.type_qualification_evidence_report.v1",
            "run_id": run_id,
            "suite_id": "mysql_5_7.catalog_mapping",
            "artifact_path": artifact_path(name),
            "assertions": [assertion],
            "evidence": [evidence]
        });
        std::fs::write(
            directory.join(name),
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .unwrap();
    };

    write_artifact("first.json", "original evidence", source_evidence.clone());
    write_artifact("copy.json", "copied evidence", source_evidence.clone());
    let mut inventory = type_inventory();
    merge_live_type_evidence_from_directory(&mut inventory, &directory, &workspace);
    index_type_evidence(&mut inventory);
    assert_eq!(
        inventory["per_type_qualification"]["evidence_registry"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|record| record["id"] == evidence_id)
            .count(),
        1
    );
    assert_eq!(
        inventory["per_type_qualification"]["entries"]["mysql.tinyint@mysql_5_7"]["source"]["catalog_type_mapping"]
            ["evidence"],
        json!([evidence_id])
    );

    let mut conflict = source_evidence;
    conflict["type_id"] = json!("mysql.smallint");
    write_artifact("conflict.json", "conflicting evidence", conflict);
    let conflicting_merge = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        merge_live_type_evidence_from_directory(&mut inventory, &directory, &workspace);
    }));
    assert!(conflicting_merge.is_err());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn web_plan_evidence_requires_a_saved_task_and_successful_start_gate() {
    let mut evidence = json!({
        "source_connector_id": "mysql_5_7",
        "sink_connector_id": "mysql_5_7",
        "verification": "web_ui.preview_save_start_gate",
        "preview_status": "COMPATIBLE",
        "save_status": "PASS",
        "start_gate_status": "PASS",
        "plan_digest": "a".repeat(64),
        "target_probe_digest": "b".repeat(64),
        "selected_rule_id": "mysql57.native.exact",
        "target_storage_mode": "native_target_column",
        "risk_confirmation_required": false,
        "risk_confirmation_verified": false
    });
    assert!(verified_web_plan_evidence(&evidence));

    evidence["verification"] = json!("web_ui.shared_planner_same_connector");
    evidence["planner_status"] = json!("COMPATIBLE");
    assert!(!verified_web_plan_evidence(&evidence));

    evidence["verification"] = json!("web_ui.preview_save_start_gate");
    evidence["save_status"] = json!("PASS");
    evidence["start_gate_status"] = json!("NOT_RUN");
    assert!(!verified_web_plan_evidence(&evidence));
}

#[test]
fn live_sink_type_evidence_is_bound_to_its_source_and_target_connectors() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("workspace exists");
    let directory = workspace
        .join("target")
        .join(format!("sink-type-evidence-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create evidence fixture directory");
    let relative_path = directory
        .strip_prefix(&workspace)
        .unwrap()
        .join("receipt.json")
        .to_string_lossy()
        .replace('\\', "/");
    let run_id = format!("sink-evidence-test-{}", std::process::id());
    let evidence_id = format!("mysql.tinyint@mysql_5_7>mysql_5_7:sink.live:{run_id}");
    let artifact = json!({
        "schema": "cdc.type_qualification_evidence_report.v1",
        "run_id": run_id,
        "suite_id": "mysql_5_7.all_types_to_mysql_5_7",
        "artifact_path": relative_path,
        "assertions": ["live target readback matched the captured value"],
        "evidence": [{
            "id": evidence_id,
            "type_id": "mysql.tinyint",
            "source_connector_id": "mysql_5_7",
            "sink_connector_id": "mysql_5_7",
            "axis": "sink.live",
            "status": "PASS",
            "run_id": run_id,
            "outcome": "VALUE_PRESERVED",
            "target_readback_status": "PASS"
        }, {
            "id": format!("mysql.tinyint@mysql_5_7:web.plan:{run_id}"),
            "type_id": "mysql.tinyint",
            "source_connector_id": "mysql_5_7",
            "sink_connector_id": null,
            "axis": "web.plan",
            "status": "PASS",
            "run_id": run_id
        }]
    });
    std::fs::write(
        directory.join("receipt.json"),
        serde_json::to_vec_pretty(&artifact).unwrap(),
    )
    .unwrap();

    let mut inventory = type_inventory();
    merge_live_type_evidence_from_directory(&mut inventory, &directory, &workspace);
    let entry = &inventory["per_type_qualification"]["entries"]["mysql.tinyint@mysql_5_7"];
    let evidence_registry = &inventory["per_type_qualification"]["evidence_registry"];
    assert_eq!(
        inventory["per_type_qualification"]["ignored_synthetic_web_evidence"],
        1
    );
    assert!(
        entry["web"].is_null(),
        "a sink test cannot qualify Web preview and save"
    );
    assert_eq!(evidence_registry["entries"].as_array().unwrap().len(), 1);
    let sink_receipt = &entry["sinks"]["mysql_5_7"]["live_evidence"];
    assert_eq!(sink_receipt.as_array().unwrap().len(), 1);
    assert!(evidence_axis_pass(
        &json!({"status":"PASS", "evidence":sink_receipt}),
        evidence_registry,
        "mysql.tinyint",
        "mysql_5_7",
        Some("mysql_5_7"),
        "sink.live"
    ));
    let live_axis = json!({"status":"PASS", "evidence":sink_receipt});
    assert!(evidence_axis_storage_mode_pass(
        &live_axis,
        evidence_registry,
        "mysql.tinyint",
        "mysql_5_7",
        "mysql_5_7",
        "sink.live",
        EvidenceStorageModeExpectation {
            outcome: "VALUE_PRESERVED",
            target_storage_mode: "native_target_column",
        }
    ));
    assert!(!evidence_axis_storage_mode_pass(
        &live_axis,
        evidence_registry,
        "mysql.tinyint",
        "mysql_5_7",
        "mysql_5_7",
        "sink.live",
        EvidenceStorageModeExpectation {
            outcome: "VALUE_PRESERVED",
            target_storage_mode: "logical_value_json_carrier",
        }
    ));
    assert_eq!(
        evidence_registry["entries"][0]["target_storage_mode"], "native_target_column",
        "legacy evidence must be classified from its artifact-bound suite"
    );
    assert_eq!(entry["sinks"]["mysql_5_7"]["outcome"], "VALUE_PRESERVED");
    assert_eq!(
        entry["sinks"]["mysql_5_7"]["live_outcome"],
        "VALUE_PRESERVED"
    );
    assert_eq!(
        per_type_sink_live_status(
            entry,
            evidence_registry,
            "mysql.tinyint",
            "mysql_5_7",
            &versions()
        ),
        "REQUIRES_PER_TYPE_LIVE_EVIDENCE",
        "one target receipt must not qualify the other five sink versions"
    );

    std::fs::remove_dir_all(directory).expect("remove evidence fixture directory");
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
    assert!(live.get("source_fixture_roster").is_none());
    assert_eq!(ids.len(), 6);
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
