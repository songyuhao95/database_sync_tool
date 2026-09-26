use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    BitOrder, BitPadding, ConnectorIdentity, JsonEntry, JsonValue, LogicalField, LogicalType,
    LogicalValue, MapEntry, ServerBuildIdentity, SourceCursor, SourceRepresentationContext,
    SourceRepresentationFormat, SpatialFormat, TemporalInfinityKind,
};
use std::collections::BTreeMap;
pub fn logical_value_carrier_cases() -> Vec<(&'static str, LogicalType, LogicalValue)> {
    let integer = LogicalType::integer(true, 64);
    let text = LogicalType::Text {
        charset: "UTF8".into(),
        max_length: None,
        length_unit: change_event::LengthUnit::Characters,
        collation: None,
    };
    let raw_digest = format!("sha256:{}", "a".repeat(64));
    let raw_type = LogicalType::raw(
        "fixture.raw.v1",
        "public.opaque_type",
        raw_digest.clone(),
        "UTF-8",
    );
    let raw_context = SourceRepresentationContext {
        connector: ConnectorIdentity::new("postgresql", "15"),
        server_build: ServerBuildIdentity::new(
            "postgresql",
            "community",
            "15.19",
            "PostgreSQL 15.19",
        ),
        source_type_identity: "postgresql.pg_type.v1:90001:public.opaque_type".into(),
        source_type_definition_digest: raw_digest.clone(),
        protocol: "pgoutput.v1".into(),
        format: SourceRepresentationFormat::Binary,
        type_metadata: BTreeMap::new(),
        source_cursor: SourceCursor {
            format: "postgresql.lsn.v1".into(),
            value: "0/60".into(),
            display: "0/60".into(),
        },
    };
    let raw = LogicalValue::Raw {
        carrier: change_event::RawValueCarrier::new(
            "fixture.raw.v1",
            "public.opaque_type",
            raw_digest,
            "UTF-8",
            "AA",
            None::<String>,
        )
        .with_source_context(raw_context, true)
        .unwrap(),
    };
    let spatial_bytes = URL_SAFE_NO_PAD.encode([
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    vec![
        (
            "boolean",
            LogicalType::Boolean,
            LogicalValue::Boolean { value: true },
        ),
        (
            "uuid",
            LogicalType::Uuid,
            LogicalValue::Uuid {
                value: "12345678-1234-1234-1234-123456789abc".into(),
            },
        ),
        (
            "integer",
            integer.clone(),
            LogicalValue::Integer {
                signed: true,
                bits: 64,
                value: "1".into(),
            },
        ),
        (
            "decimal",
            LogicalType::decimal(20, 2),
            LogicalValue::Decimal {
                unscaled: "12345".into(),
                scale: 2,
            },
        ),
        (
            "decimal_unbounded",
            LogicalType::DecimalUnbounded,
            LogicalValue::Decimal {
                unscaled: "123456789012345678901234567890".into(),
                scale: -4,
            },
        ),
        (
            "float",
            LogicalType::float(64),
            LogicalValue::Float {
                bits: 64,
                ieee754_hex: "3ff0000000000000".into(),
            },
        ),
        (
            "text",
            text.clone(),
            LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "QQ".into(),
                text: Some("A".into()),
            },
        ),
        (
            "text_with_nul",
            text.clone(),
            LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "QQDOqQ".into(),
                text: Some("A\0Ω".into()),
            },
        ),
        (
            "binary",
            LogicalType::binary(None),
            LogicalValue::Binary {
                bytes_base64url: "AA".into(),
            },
        ),
        (
            "bit_string",
            LogicalType::bit_string(3),
            LogicalValue::BitString {
                bytes_base64url: "AA".into(),
                bit_length: 3,
                padding: BitPadding::Zero,
                bit_order: BitOrder::MsbFirst,
            },
        ),
        (
            "date",
            LogicalType::date(),
            LogicalValue::Date {
                year: 2026,
                month: 9,
                day: 26,
            },
        ),
        (
            "local_time",
            LogicalType::LocalTime {
                fractional_precision: 6,
            },
            LogicalValue::LocalTime {
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 123_000,
            },
        ),
        (
            "offset_time",
            LogicalType::offset_time(6),
            LogicalValue::OffsetTime {
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 123_000,
                offset_seconds: 19_800,
            },
        ),
        (
            "local_datetime",
            LogicalType::local_datetime(6),
            LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 26,
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 123_000,
            },
        ),
        (
            "duration",
            LogicalType::duration(6),
            LogicalValue::Duration {
                negative: false,
                hours: 25,
                minutes: 2,
                seconds: 3,
                microsecond: 123_000,
            },
        ),
        (
            "calendar_interval",
            LogicalType::calendar_interval(6),
            LogicalValue::CalendarInterval {
                months: 2,
                days: -3,
                microseconds: 4_000_005,
            },
        ),
        (
            "instant",
            LogicalType::instant(6),
            LogicalValue::Instant {
                unix_seconds: "1790211723".into(),
                nanoseconds: 123_000_000,
            },
        ),
        (
            "year",
            LogicalType::year(),
            LogicalValue::Year { value: 2026 },
        ),
        (
            "temporal_infinity",
            LogicalType::date(),
            LogicalValue::TemporalInfinity {
                kind: TemporalInfinityKind::Date,
                negative: false,
            },
        ),
        (
            "enum",
            LogicalType::Enum {
                members: vec!["alpha".into(), "beta".into()],
            },
            LogicalValue::Enum {
                label: "beta".into(),
            },
        ),
        (
            "set",
            LogicalType::Set {
                members: vec!["alpha".into(), "beta".into()],
            },
            LogicalValue::Set {
                members: vec!["beta".into(), "alpha".into()],
            },
        ),
        (
            "spatial",
            LogicalType::spatial("point", Some(4326), 2),
            LogicalValue::Spatial {
                format: SpatialFormat::Wkb,
                bytes_base64url: spatial_bytes,
                geometry_type: "point".into(),
                dimensions: 2,
                srid: Some(4326),
                crs: Some("EPSG:4326".into()),
            },
        ),
        (
            "array",
            LogicalType::Array {
                element: Box::new(integer.clone()),
            },
            LogicalValue::Array {
                elements: vec![
                    LogicalValue::Null,
                    LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "1".into(),
                    },
                ],
            },
        ),
        (
            "struct",
            LogicalType::Struct {
                fields: vec![LogicalField {
                    name: "value".into(),
                    logical_type: text.clone(),
                    nullable: false,
                }],
            },
            LogicalValue::Struct {
                fields: vec![change_event::StructuredField {
                    name: "value".into(),
                    value: LogicalValue::Text {
                        charset: "UTF8".into(),
                        bytes_base64url: "QQ".into(),
                        text: Some("A".into()),
                    },
                }],
            },
        ),
        (
            "map",
            LogicalType::Map {
                key: Box::new(text.clone()),
                value: Box::new(integer.clone()),
            },
            LogicalValue::Map {
                entries: vec![MapEntry {
                    key: LogicalValue::Text {
                        charset: "UTF8".into(),
                        bytes_base64url: "aw".into(),
                        text: Some("k".into()),
                    },
                    value: LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "1".into(),
                    },
                }],
            },
        ),
        (
            "range",
            LogicalType::Range {
                element: Box::new(integer.clone()),
            },
            LogicalValue::Range {
                empty: false,
                lower: Some(Box::new(LogicalValue::Integer {
                    signed: true,
                    bits: 64,
                    value: "1".into(),
                })),
                upper: Some(Box::new(LogicalValue::Integer {
                    signed: true,
                    bits: 64,
                    value: "2".into(),
                })),
                lower_inclusive: true,
                upper_inclusive: false,
            },
        ),
        (
            "multirange",
            LogicalType::MultiRange {
                element: Box::new(integer.clone()),
            },
            LogicalValue::MultiRange {
                ranges: vec![LogicalValue::Range {
                    empty: false,
                    lower: Some(Box::new(LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "1".into(),
                    })),
                    upper: Some(Box::new(LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "2".into(),
                    })),
                    lower_inclusive: true,
                    upper_inclusive: false,
                }],
            },
        ),
        (
            "array_with_metadata",
            LogicalType::array_with_metadata(integer.clone(), 1, vec![0]),
            LogicalValue::ArrayWithMetadata {
                elements: vec![LogicalValue::Integer {
                    signed: true,
                    bits: 64,
                    value: "1".into(),
                }],
                dimensions: 1,
                lower_bounds: vec![0],
                dimension_lengths: vec![1],
            },
        ),
        (
            "invalid_temporal",
            LogicalType::invalid_temporal("mysql.date"),
            LogicalValue::InvalidTemporal {
                kind: "mysql.date".into(),
                raw: "0000-00-00".into(),
            },
        ),
        (
            "network",
            LogicalType::network("ipv4", false),
            LogicalValue::Network {
                family: "ipv4".into(),
                address: "192.0.2.1".into(),
                prefix_length: None,
            },
        ),
        (
            "xml",
            LogicalType::xml(),
            LogicalValue::Xml {
                bytes_base64url: "PGEvPg".into(),
                text: Some("<a/>".into()),
            },
        ),
        (
            "domain",
            LogicalType::domain(
                "public.positive_int",
                integer.clone(),
                vec!["value > 0".into()],
                true,
                None,
                "b".repeat(64),
            ),
            LogicalValue::Domain {
                value: Box::new(LogicalValue::Integer {
                    signed: true,
                    bits: 64,
                    value: "1".into(),
                }),
            },
        ),
        ("raw", raw_type, raw),
        (
            "json",
            LogicalType::json(),
            LogicalValue::Json {
                value: JsonValue::Object(vec![JsonEntry {
                    key: "value".into(),
                    value: JsonValue::Decimal {
                        unscaled: "12345".into(),
                        scale: 2,
                    },
                }]),
            },
        ),
    ]
}
