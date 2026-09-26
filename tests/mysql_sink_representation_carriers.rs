use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    BitOrder, BitPadding, ChangeTransaction, ColumnConversionPlan, ColumnDatum, ConnectorIdentity,
    Datum, DefinitionReference, FieldCompatibilityInput, FieldDefinition, JsonEntry, JsonValue,
    LogicalField, LogicalType, LogicalValue, MapEntry, Operation, PlanConfirmationState,
    PresenceState, RiskConfirmation, RouteOptions, RowChange, ServerBuildIdentity, Source,
    SourceCursor, SourceRepresentationContext, SourceRepresentationEnvelope,
    SourceRepresentationFormat, SourceRepresentationTypeEvidence, SourceTypeMapping, SpatialFormat,
    TemporalInfinityKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "support/mysql_contract.rs"]
mod contract;

fn build(version: &str) -> change_event::ServerBuildIdentity {
    let full_version = match version {
        "5.7" => "5.7.44",
        "8.0" => "8.0.46",
        "8.4" => "8.4.8",
        _ => unreachable!(),
    };
    change_event::ServerBuildIdentity::new(
        "mysql",
        "oracle",
        full_version,
        format!("mysql-{full_version}"),
    )
}

fn manifests() -> [change_event::TargetCapabilityManifest; 3] {
    [
        mysql_5_7::compatibility_manifest(build("5.7")),
        mysql_8_0::compatibility_manifest(build("8.0")),
        mysql_8_4::compatibility_manifest(build("8.4")),
    ]
}

struct MysqlPlanSpec<'a> {
    table: &'a str,
    name: &'a str,
    source_native: &'a str,
    source_type: LogicalType,
    target_native: &'a str,
    target_type: LogicalType,
    key_ordinal: Option<usize>,
    selected_kind: Option<&'a str>,
    presences: Vec<PresenceState>,
    source_protocol: &'a str,
    source_representation_format: SourceRepresentationFormat,
    allowed_context_metadata_keys: BTreeSet<String>,
}

impl<'a> MysqlPlanSpec<'a> {
    fn new(
        table: &'a str,
        name: &'a str,
        source_native: &'a str,
        source_type: LogicalType,
        target_native: &'a str,
        target_type: LogicalType,
    ) -> Self {
        Self {
            table,
            name,
            source_native,
            source_type,
            target_native,
            target_type,
            key_ordinal: None,
            selected_kind: None,
            presences: vec![PresenceState::Value],
            source_protocol: "pgoutput.v1",
            source_representation_format: SourceRepresentationFormat::Text,
            allowed_context_metadata_keys: BTreeSet::new(),
        }
    }

    fn key(mut self, ordinal: usize) -> Self {
        self.key_ordinal = Some(ordinal);
        self
    }

    fn conversion(mut self, kind: &'a str, presences: Vec<PresenceState>) -> Self {
        self.selected_kind = Some(kind);
        self.presences = presences;
        self
    }

    fn representation_protocol(
        mut self,
        protocol: &'a str,
        format: SourceRepresentationFormat,
    ) -> Self {
        self.source_protocol = protocol;
        self.source_representation_format = format;
        self
    }

    fn allow_context_metadata_key(mut self, key: impl Into<String>) -> Self {
        self.allowed_context_metadata_keys.insert(key.into());
        self
    }
}

fn mysql_plan(
    manifest: &change_event::TargetCapabilityManifest,
    spec: MysqlPlanSpec<'_>,
) -> ColumnConversionPlan {
    let MysqlPlanSpec {
        table,
        name,
        source_native,
        source_type,
        target_native,
        target_type,
        key_ordinal,
        selected_kind,
        presences,
        source_protocol,
        source_representation_format,
        allowed_context_metadata_keys,
    } = spec;
    let source_build =
        ServerBuildIdentity::new("postgresql", "community", "15.19", "PostgreSQL 15.19");
    let source_connector = ConnectorIdentity::new("postgresql", "15");
    let source_field = FieldDefinition {
        reference: DefinitionReference::new(
            format!("catalog:CDC_test.{table}.{name}"),
            format!("source-{table}-{name}"),
        ),
        ordinal: key_ordinal.map_or_else(
            || {
                name.strip_prefix("payload_")
                    .and_then(|ordinal| ordinal.parse::<usize>().ok())
                    .map_or_else(|| if name == "raw_payload" { 2 } else { 1 }, |n| n + 1)
            },
            |_| 0,
        ),
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
        reference: DefinitionReference::new(
            format!("catalog:CDC_test.{table}.{name}"),
            format!("target-{table}-{name}"),
        ),
        ordinal: key_ordinal.map_or_else(
            || {
                name.strip_prefix("payload_")
                    .and_then(|ordinal| ordinal.parse::<usize>().ok())
                    .map_or_else(|| if name == "raw_payload" { 2 } else { 1 }, |n| n + 1)
            },
            |_| 0,
        ),
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
    if selected_kind == Some("source_representation") {
        mapping.source_representation_evidence = Some(SourceRepresentationTypeEvidence {
            source_type_identity: "postgresql.pg_type.v1:90001:public.opaque_type".into(),
            protocol: source_protocol.into(),
            format: source_representation_format,
            type_metadata: BTreeMap::from([("native_type".into(), source_native.into())]),
            allowed_context_metadata_keys,
        });
    }
    let selected_rule = selected_kind.map(|kind| {
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
            .expect("selected conversion capability");
        change_event::RuleReference {
            id: capability.rule.id.clone(),
            version: capability.rule.version.clone(),
        }
    });
    let options = |confirmations| RouteOptions {
        route_id: format!("mysql-{}-representation-test", manifest.connector.version),
        configuration_revision: "fixture-revision".into(),
        selected_rule: selected_rule.clone(),
        confirmations,
        ..RouteOptions::default()
    };
    let make_input = |confirmations| FieldCompatibilityInput {
        source_field: source_field.clone(),
        target_field: target_field.clone(),
        source_type_mapping: mapping.clone(),
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
    let first = change_event::plan_field_compatibility(make_input(Vec::new())).unwrap();
    let first_plan = first.plan.expect("qualified field plan");
    if first_plan.confirmation == PlanConfirmationState::Required {
        let confirmation = RiskConfirmation {
            source_field_lineage: first_plan.source_field.lineage_id.clone(),
            target_field_lineage: first_plan.target_field.lineage_id.clone(),
            rule: first_plan.rule.clone(),
            plan_digest: first_plan.plan_digest.clone(),
            actor: "test".into(),
            confirmed_at: "2026-09-26T00:00:00Z".into(),
            reason: Some("fixture qualification".into()),
        };
        let confirmed =
            change_event::plan_field_compatibility(make_input(vec![confirmation])).unwrap();
        let plan = confirmed.plan.expect("confirmed field plan");
        assert_eq!(plan.confirmation, PlanConfirmationState::Confirmed);
        plan
    } else {
        first_plan
    }
}

fn insert_transaction(
    table: &str,
    value: Datum,
    native_type: &str,
) -> change_event::ValidatedTransaction {
    let cursor = SourceCursor {
        format: "fixture.cursor.v1".into(),
        value: "100".into(),
        display: "100".into(),
    };
    let row_cursor = SourceCursor {
        format: "fixture.cursor.v1".into(),
        value: "90".into(),
        display: "90".into(),
    };
    change_event::validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15".into(),
            id: "550e8400-e29b-41d4-a716-446655440000".into(),
        },
        id: "fixture-transaction-1".into(),
        begin_cursor: cursor.clone(),
        commit_cursor: cursor,
        changes: vec![RowChange {
            database: Some("CDC_test".into()),
            operation: Operation::Insert,
            schema: "CDC_test".into(),
            table: table.into(),
            source_cursor: row_cursor,
            source_timestamp: 1_790_000_000,
            schema_basis: "fixture-schema".into(),
            before: None,
            after: Some(vec![
                ColumnDatum {
                    ordinal: 0,
                    name: "id".into(),
                    native_type: "bigint".into(),
                    primary_key_ordinal: Some(0),
                    generated: false,
                    collation: None,
                    datum: Datum::Value(LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "1".into(),
                    }),
                },
                ColumnDatum {
                    ordinal: 1,
                    name: "payload".into(),
                    native_type: native_type.into(),
                    primary_key_ordinal: None,
                    generated: false,
                    collation: None,
                    datum: value,
                },
            ]),
        }],
    })
    .unwrap()
}

fn carrier_plans(
    manifest: &change_event::TargetCapabilityManifest,
    table: &str,
    payload_type: LogicalType,
    payload_native_type: &str,
    source_representation: bool,
) -> Vec<ColumnConversionPlan> {
    let int = LogicalType::integer(true, 64);
    let key = mysql_plan(
        manifest,
        MysqlPlanSpec::new(table, "id", "bigint", int.clone(), "bigint", int).key(0),
    );
    let payload_spec = MysqlPlanSpec::new(
        table,
        "payload",
        payload_native_type,
        payload_type,
        if source_representation {
            "longblob"
        } else {
            "json"
        },
        if source_representation {
            LogicalType::Binary {
                max_length: Some(4_294_967_295),
            }
        } else {
            LogicalType::json()
        },
    )
    .conversion(
        if source_representation {
            "source_representation"
        } else {
            "logical_value_json"
        },
        if source_representation {
            vec![PresenceState::SourceRepresentation]
        } else {
            vec![PresenceState::Value]
        },
    );
    let payload = mysql_plan(manifest, payload_spec);
    vec![key, payload]
}

fn source_envelope(table_cursor: SourceCursor) -> SourceRepresentationEnvelope {
    source_envelope_with(
        table_cursor,
        "pgoutput.v1",
        SourceRepresentationFormat::Text,
        "UTF-8",
        b"wire representation",
    )
}

fn source_binary_envelope(table_cursor: SourceCursor) -> SourceRepresentationEnvelope {
    source_envelope_with(
        table_cursor,
        "fixture.binary.v1",
        SourceRepresentationFormat::Binary,
        "raw-bytes",
        &[0x00, 0xff, 0x80, 0x41, 0x00, 0xc3],
    )
}

fn source_envelope_with(
    table_cursor: SourceCursor,
    protocol: &str,
    format: SourceRepresentationFormat,
    encoding: &str,
    payload: &[u8],
) -> SourceRepresentationEnvelope {
    let digest = "a".repeat(64);
    let mut metadata = BTreeMap::new();
    metadata.insert("native_type".into(), "public.opaque_type".into());
    SourceRepresentationEnvelope::new(
        SourceRepresentationContext {
            connector: ConnectorIdentity::new("postgresql", "15"),
            server_build: ServerBuildIdentity::new(
                "postgresql",
                "community",
                "15.19",
                "PostgreSQL 15.19",
            ),
            source_type_identity: "postgresql.pg_type.v1:90001:public.opaque_type".into(),
            source_type_definition_digest: format!("sha256:{digest}"),
            protocol: protocol.into(),
            format,
            type_metadata: metadata,
            source_cursor: table_cursor,
        },
        encoding,
        payload,
    )
}

fn logical_value_carrier_cases() -> Vec<(&'static str, LogicalType, LogicalValue)> {
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

fn dual_insert_transaction(table: &str, duplicate: bool) -> change_event::ValidatedTransaction {
    let value = LogicalValue::Map {
        entries: vec![MapEntry {
            key: LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "Y29kZXg".into(),
                text: Some("codex".into()),
            },
            value: LogicalValue::Decimal {
                unscaled: "123456789012345678901234567890".into(),
                scale: -4,
            },
        }],
    };
    let row_cursors = if duplicate {
        vec!["0/60", "0/80"]
    } else {
        vec!["0/60"]
    };
    let changes = row_cursors
        .into_iter()
        .enumerate()
        .map(|(index, cursor_value)| {
            let row_cursor = SourceCursor {
                format: "postgresql.lsn.v1".into(),
                value: cursor_value.into(),
                display: cursor_value.into(),
            };
            RowChange {
                database: Some("CDC_test".into()),
                operation: Operation::Insert,
                schema: "CDC_test".into(),
                table: table.into(),
                source_cursor: row_cursor.clone(),
                source_timestamp: 1_790_000_000 + index as u32,
                schema_basis: "fixture-schema".into(),
                before: None,
                after: Some(vec![
                    ColumnDatum {
                        ordinal: 0,
                        name: "id".into(),
                        native_type: "bigint".into(),
                        primary_key_ordinal: Some(0),
                        generated: false,
                        collation: None,
                        datum: Datum::Value(LogicalValue::Integer {
                            signed: true,
                            bits: 64,
                            value: "1".into(),
                        }),
                    },
                    ColumnDatum {
                        ordinal: 1,
                        name: "payload".into(),
                        native_type: "fixture_map".into(),
                        primary_key_ordinal: None,
                        generated: false,
                        collation: None,
                        datum: Datum::Value(value.clone()),
                    },
                    ColumnDatum {
                        ordinal: 2,
                        name: "raw_payload".into(),
                        native_type: "public.opaque_type".into(),
                        primary_key_ordinal: None,
                        generated: false,
                        collation: None,
                        datum: Datum::SourceRepresentationEnvelope(source_envelope(
                            row_cursor.clone(),
                        )),
                    },
                    ColumnDatum {
                        ordinal: 3,
                        name: "raw_binary_payload".into(),
                        native_type: "public.opaque_type".into(),
                        primary_key_ordinal: None,
                        generated: false,
                        collation: None,
                        datum: Datum::SourceRepresentationEnvelope(source_binary_envelope(
                            row_cursor,
                        )),
                    },
                ]),
            }
        })
        .collect();
    change_event::validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15".into(),
            id: "550e8400-e29b-41d4-a716-446655440000".into(),
        },
        id: if duplicate {
            "fixture-transaction-duplicate".into()
        } else {
            "fixture-transaction-success".into()
        },
        begin_cursor: SourceCursor {
            format: "postgresql.lsn.v1".into(),
            value: "0/20".into(),
            display: "0/20".into(),
        },
        commit_cursor: SourceCursor {
            format: "postgresql.lsn.v1".into(),
            value: "0/100".into(),
            display: "0/100".into(),
        },
        changes,
    })
    .unwrap()
}

fn dual_carrier_plans(
    manifest: &change_event::TargetCapabilityManifest,
    table: &str,
) -> Vec<ColumnConversionPlan> {
    let map_type = LogicalType::Map {
        key: Box::new(LogicalType::Text {
            charset: "UTF8".into(),
            max_length: None,
            length_unit: change_event::LengthUnit::Characters,
            collation: None,
        }),
        value: Box::new(LogicalType::DecimalUnbounded),
    };
    let raw_type = LogicalType::raw(
        "postgresql.pgoutput.text-envelope.v1",
        "public.opaque_type",
        "a".repeat(64),
        "UTF-8",
    );
    let mut plans = carrier_plans(manifest, table, map_type, "fixture_map", false);
    plans.push(mysql_plan(
        manifest,
        MysqlPlanSpec::new(
            table,
            "raw_payload",
            "public.opaque_type",
            raw_type,
            "longblob",
            LogicalType::Binary {
                max_length: Some(4_294_967_295),
            },
        )
        .conversion(
            "source_representation",
            vec![PresenceState::SourceRepresentation],
        ),
    ));
    plans.push(mysql_plan(
        manifest,
        MysqlPlanSpec::new(
            table,
            "raw_binary_payload",
            "public.opaque_type",
            LogicalType::raw(
                "fixture.binary-envelope.v1",
                "public.opaque_type",
                "a".repeat(64),
                "raw-bytes",
            ),
            "longblob",
            LogicalType::Binary {
                max_length: Some(4_294_967_295),
            },
        )
        .conversion(
            "source_representation",
            vec![PresenceState::SourceRepresentation],
        )
        .representation_protocol("fixture.binary.v1", SourceRepresentationFormat::Binary),
    ));
    plans
}

fn all_logical_values_transaction(
    table: &str,
    cases: &[(&'static str, LogicalType, LogicalValue)],
) -> change_event::ValidatedTransaction {
    let mut after = vec![ColumnDatum {
        ordinal: 0,
        name: "id".into(),
        native_type: "bigint".into(),
        primary_key_ordinal: Some(0),
        generated: false,
        collation: None,
        datum: Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: "2".into(),
        }),
    }];
    after.extend(
        cases
            .iter()
            .enumerate()
            .map(|(index, (name, _, value))| ColumnDatum {
                ordinal: index + 1,
                name: format!("payload_{index}"),
                native_type: format!("fixture_{name}"),
                primary_key_ordinal: None,
                generated: false,
                collation: None,
                datum: Datum::Value(value.clone()),
            }),
    );
    change_event::validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15".into(),
            id: "550e8400-e29b-41d4-a716-446655440000".into(),
        },
        id: "fixture-all-logical-values-live".into(),
        begin_cursor: SourceCursor {
            format: "postgresql.lsn.v1".into(),
            value: "0/100".into(),
            display: "0/100".into(),
        },
        commit_cursor: SourceCursor {
            format: "postgresql.lsn.v1".into(),
            value: "0/200".into(),
            display: "0/200".into(),
        },
        changes: vec![RowChange {
            database: Some("CDC_test".into()),
            operation: Operation::Insert,
            schema: "CDC_test".into(),
            table: table.into(),
            source_cursor: SourceCursor {
                format: "postgresql.lsn.v1".into(),
                value: "0/120".into(),
                display: "0/120".into(),
            },
            source_timestamp: 1_790_000_000,
            schema_basis: "fixture-schema".into(),
            before: None,
            after: Some(after),
        }],
    })
    .unwrap()
}

fn all_logical_values_plans(
    manifest: &change_event::TargetCapabilityManifest,
    table: &str,
    cases: &[(&'static str, LogicalType, LogicalValue)],
) -> Vec<ColumnConversionPlan> {
    let mut plans = vec![mysql_plan(
        manifest,
        MysqlPlanSpec::new(
            table,
            "id",
            "bigint",
            LogicalType::integer(true, 64),
            "bigint",
            LogicalType::integer(true, 64),
        )
        .key(0),
    )];
    plans.extend(
        cases
            .iter()
            .enumerate()
            .map(|(index, (name, logical_type, _))| {
                mysql_plan(
                    manifest,
                    MysqlPlanSpec::new(
                        table,
                        &format!("payload_{index}"),
                        &format!("fixture_{name}"),
                        logical_type.clone(),
                        "json",
                        LogicalType::json(),
                    )
                    .conversion("logical_value_json", vec![PresenceState::Value]),
                )
            }),
    );
    plans
}

struct LiveCarrierTable {
    conn: mysql::Conn,
    name: String,
}

impl LiveCarrierTable {
    fn create(port: u16) -> Self {
        use mysql::prelude::Queryable as _;
        let mut conn = contract::connection(port, false);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let name = format!("cdc_carrier_{}_{}", std::process::id(), unique);
        let payload_columns = (0..logical_value_carrier_cases().len())
            .map(|index| format!(", payload_{index} JSON NULL"))
            .collect::<String>();
        conn.query_drop("CREATE DATABASE IF NOT EXISTS CDC_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE CDC_test.{name} (
                id BIGINT NOT NULL PRIMARY KEY,
                payload JSON NULL,
                raw_payload LONGBLOB NULL,
                raw_binary_payload LONGBLOB NULL
                {payload_columns}
            ) ENGINE=InnoDB"
        ))
        .unwrap();
        Self { conn, name }
    }
}

impl Drop for LiveCarrierTable {
    fn drop(&mut self) {
        use mysql::prelude::Queryable as _;
        if let Err(error) = self
            .conn
            .query_drop(format!("DROP TABLE CDC_test.{}", self.name))
        {
            eprintln!("carrier test cleanup failed: {error}");
        }
    }
}

macro_rules! live_carrier_test {
    ($name:ident, $adapter:ident, $port_key:literal, $port:literal, $version:literal) => {
        #[test]
        #[ignore = "requires an explicitly configured MySQL writer account"]
        fn $name() {
            use mysql::prelude::Queryable as _;

            let port = contract::port($port_key, $port);
            let mut table = LiveCarrierTable::create(port);
            let full_version = $version;
            let manifest = ::$adapter::compatibility_manifest(build_for_live(full_version));
            let plans = dual_carrier_plans(&manifest, &table.name);
            let host = contract::setting("CDC_MYSQL_HOST", "192.168.0.10");
            let mut config = ::$adapter::TargetConfig::new(
                host,
                contract::setting("CDC_MYSQL_WRITER_USER", "mysql_writer"),
                contract::password("WRITER"),
            );
            config.port = port;

            let task_id = format!(
                "repr_{}_{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let source_uuid = "550e8400-e29b-41d4-a716-446655440000";
            let binding = "a".repeat(64);
            let mut writer =
                ::$adapter::CheckpointWriter::open(&config, &task_id, source_uuid, &binding)
                    .unwrap();
            let initial = writer
                .initialize("postgresql_lsn", "0/20", 32, None)
                .unwrap();

            let duplicate = dual_insert_transaction(&table.name, true);
            let duplicate_plan = ::$adapter::sql_with_plans(&duplicate, &plans).unwrap();
            assert!(writer.apply(&duplicate_plan).is_err());
            assert_eq!(
                table
                    .conn
                    .query_first::<u64, _>(format!("SELECT COUNT(*) FROM CDC_test.{}", table.name))
                    .unwrap(),
                Some(0),
                "a later DML failure must roll back the earlier carrier write"
            );
            assert_eq!(writer.checkpoint(), Some(&initial));

            let valid = dual_insert_transaction(&table.name, false);
            let plan = ::$adapter::sql_with_plans(&valid, &plans).unwrap();
            let applied = writer.apply(&plan).unwrap();
            assert_eq!(applied.checkpoint.position, 256);
            assert_eq!(
                applied.checkpoint.last_transaction_id.as_deref(),
                Some("fixture-transaction-success")
            );
            let (json_bytes, envelope_bytes, binary_envelope_bytes): (Vec<u8>, Vec<u8>, Vec<u8>) = table
                .conn
                .query_first(format!(
                    "SELECT CAST(payload AS CHAR), raw_payload, raw_binary_payload FROM CDC_test.{} WHERE id=1",
                    table.name
                ))
                .unwrap()
                .unwrap();
            let stored_value: LogicalValue = serde_json::from_slice(&json_bytes).unwrap();
            assert!(matches!(stored_value, LogicalValue::Map { .. }));
            let stored_envelope: SourceRepresentationEnvelope =
                serde_json::from_slice(&envelope_bytes).unwrap();
            stored_envelope.validate().unwrap();
            assert_eq!(stored_envelope.raw_bytes().unwrap(), b"wire representation");
            let stored_binary_envelope: SourceRepresentationEnvelope =
                serde_json::from_slice(&binary_envelope_bytes).unwrap();
            stored_binary_envelope.validate().unwrap();
            let expected_binary_envelope = source_binary_envelope(SourceCursor {
                format: "postgresql.lsn.v1".into(),
                value: "0/60".into(),
                display: "0/60".into(),
            });
            assert_eq!(
                stored_binary_envelope.payload_sha256,
                expected_binary_envelope.payload_sha256,
                "the binary envelope's payload digest survives MySQL storage"
            );
            assert_eq!(
                stored_binary_envelope.raw_bytes().unwrap(),
                expected_binary_envelope.raw_bytes().unwrap(),
                "NUL and non-UTF-8 bytes survive the actual target read-back"
            );

            let cases = logical_value_carrier_cases();
            let all_values = all_logical_values_transaction(&table.name, &cases);
            let all_plans = all_logical_values_plans(&manifest, &table.name, &cases);
            let all_plan = ::$adapter::sql_with_plans(&all_values, &all_plans).unwrap();
            let all_applied = writer.apply(&all_plan).unwrap();
            assert_eq!(all_applied.checkpoint.position, 512);
            let selected = (0..cases.len())
                .map(|index| format!("CAST(payload_{index} AS CHAR)"))
                .collect::<Vec<_>>()
                .join(", ");
            let row: mysql::Row = table
                .conn
                .query_first(format!(
                    "SELECT {selected} FROM CDC_test.{} WHERE id=2",
                    table.name
                ))
                .unwrap()
                .expect("target row with all LogicalValue carriers");
            for (index, (name, _, expected)) in cases.iter().enumerate() {
                let bytes = row
                    .get::<Vec<u8>, _>(index)
                    .unwrap_or_else(|| panic!("target JSON carrier {name} is NULL"));
                let recovered: LogicalValue = serde_json::from_slice(&bytes)
                    .unwrap_or_else(|error| panic!("target read-back for {name} failed: {error}"));
                assert_eq!(&recovered, expected, "actual MySQL read-back for {name}");
            }
        }
    };
}

fn build_for_live(version: &str) -> change_event::ServerBuildIdentity {
    change_event::ServerBuildIdentity::new("mysql", "oracle", version, format!("mysql-{version}"))
}

live_carrier_test!(
    mysql_5_7_live_representation_readback_and_checkpoint,
    mysql_5_7,
    "CDC_MYSQL57_PORT",
    33061,
    "5.7.44"
);
live_carrier_test!(
    mysql_8_0_live_representation_readback_and_checkpoint,
    mysql_8_0,
    "CDC_MYSQL80_PORT",
    33062,
    "8.0.46"
);
live_carrier_test!(
    mysql_8_4_live_representation_readback_and_checkpoint,
    mysql_8_4,
    "CDC_MYSQL84_PORT",
    33063,
    "8.4.8"
);

#[test]
fn every_mysql_sink_qualifies_tagged_logical_value_and_source_representation_carriers() {
    for manifest in manifests() {
        assert!(manifest.verify_digest());
        let logical_value = manifest.capabilities.iter().find(|capability| {
            capability.target.native_type == "json"
                && capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("logical_value_json")
        });
        let logical_value = logical_value.expect("versioned JSON LogicalValue carrier");
        assert_eq!(
            logical_value.target.parameters["logical_value_schema"],
            "change_event.logical_value.v0_3"
        );
        assert!(!logical_value.rule.allows_key);
        assert!(logical_value.rule.requires_confirmation);

        let source_representation = manifest.capabilities.iter().find(|capability| {
            capability.target.native_type == "longblob"
                && capability
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("source_representation")
        });
        let source_representation =
            source_representation.expect("Source Representation Envelope LONGBLOB carrier");
        assert_eq!(
            source_representation.target.parameters["source_representation_codec"],
            "source_representation_envelope_json_v1"
        );
        assert!(!source_representation.rule.allows_key);
        assert!(source_representation.rule.requires_confirmation);
        assert_eq!(
            source_representation.supported_presence,
            [
                change_event::PresenceState::SourceRepresentation,
                change_event::PresenceState::Null,
                change_event::PresenceState::Unchanged,
            ]
        );
    }
}

#[test]
fn source_representation_plan_binds_identity_protocol_format_and_type_metadata() {
    let manifest = manifests()[0].clone();
    let plan = dual_carrier_plans(&manifest, "carrier_fixture")
        .into_iter()
        .find(|plan| plan.source_field.lineage_id.ends_with(".raw_payload"))
        .expect("source representation plan");
    let cursor = SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: "0/60".into(),
        display: "0/60".into(),
    };
    let envelope = source_envelope(cursor);
    change_event::validate_source_representation_envelope_against_plan(&plan, &envelope)
        .expect("exact source type evidence is accepted");

    let mut wrong_identity = envelope.clone();
    wrong_identity.context.source_type_identity =
        "postgresql.pg_type.v1:90002:public.opaque_type".into();
    assert_eq!(
        change_event::validate_source_representation_envelope_against_plan(&plan, &wrong_identity)
            .unwrap_err()
            .code,
        "target_capability.source_representation_identity_mismatch"
    );

    let mut wrong_protocol = envelope.clone();
    wrong_protocol.context.protocol = "another.protocol.v1".into();
    assert_eq!(
        change_event::validate_source_representation_envelope_against_plan(&plan, &wrong_protocol)
            .unwrap_err()
            .code,
        "target_capability.source_representation_protocol_mismatch"
    );

    let mut wrong_format = envelope.clone();
    wrong_format.context.format = SourceRepresentationFormat::Binary;
    assert_eq!(
        change_event::validate_source_representation_envelope_against_plan(&plan, &wrong_format)
            .unwrap_err()
            .code,
        "target_capability.source_representation_protocol_mismatch"
    );

    let mut wrong_metadata = envelope;
    wrong_metadata
        .context
        .type_metadata
        .insert("native_type".into(), "public.other_type".into());
    assert_eq!(
        change_event::validate_source_representation_envelope_against_plan(&plan, &wrong_metadata)
            .unwrap_err()
            .code,
        "target_capability.source_representation_metadata_mismatch"
    );
    let mut unbound_metadata = source_envelope(SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: "0/60".into(),
        display: "0/60".into(),
    });
    unbound_metadata
        .context
        .type_metadata
        .insert("unexpected.context".into(), "unbound".into());
    assert_eq!(
        change_event::validate_source_representation_envelope_against_plan(
            &plan,
            &unbound_metadata,
        )
        .unwrap_err()
        .code,
        "target_capability.source_representation_metadata_unbound"
    );

    let unqualified_plan = mysql_plan(
        &manifest,
        MysqlPlanSpec::new(
            "carrier_fixture",
            "raw_payload",
            "opaque_type",
            LogicalType::raw(
                "postgresql.pgoutput.text-envelope.v1",
                "public.opaque_type",
                "a".repeat(64),
                "UTF-8",
            ),
            "longblob",
            LogicalType::Binary {
                max_length: Some(4_294_967_295),
            },
        )
        .conversion(
            "source_representation",
            vec![PresenceState::SourceRepresentation],
        )
        .allow_context_metadata_key("environment.lc_monetary"),
    );
    let mut unqualified_envelope = source_envelope(SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: "0/60".into(),
        display: "0/60".into(),
    });
    unqualified_envelope
        .context
        .type_metadata
        .insert("native_type".into(), "opaque_type".into());
    unqualified_envelope
        .context
        .type_metadata
        .insert("environment.lc_monetary".into(), "C".into());
    change_event::validate_source_representation_envelope_against_plan(
        &unqualified_plan,
        &unqualified_envelope,
    )
    .expect("the plan binds the relation's native spelling separately from qualified identity");
}

#[test]
fn all_mysql_sinks_render_tagged_logical_values_and_source_envelopes() {
    let logical_type = LogicalType::Map {
        key: Box::new(LogicalType::Text {
            charset: "UTF8".into(),
            max_length: None,
            length_unit: change_event::LengthUnit::Characters,
            collation: None,
        }),
        value: Box::new(LogicalType::DecimalUnbounded),
    };
    let logical_value = LogicalValue::Map {
        entries: vec![MapEntry {
            key: LogicalValue::Text {
                charset: "UTF8".into(),
                bytes_base64url: "Y29kZXg".into(),
                text: Some("codex".into()),
            },
            value: LogicalValue::Decimal {
                unscaled: "123456789012345678901234567890".into(),
                scale: -4,
            },
        }],
    };

    macro_rules! assert_rendered {
        ($adapter:ident, $manifest:expr) => {{
            let manifest = $manifest;
            let table = "carrier_fixture";
            let plans = carrier_plans(&manifest, table, logical_type.clone(), "fixture_map", false);
            let tx = insert_transaction(table, Datum::Value(logical_value.clone()), "fixture_map");
            let sql = ::$adapter::sql_with_plans(&tx, &plans).unwrap();
            let encoded = sql.parameters().next().unwrap().last().unwrap();
            let mysql::Value::Bytes(bytes) = encoded else {
                panic!("tagged LogicalValue must use a bound byte parameter");
            };
            assert_eq!(
                serde_json::from_slice::<LogicalValue>(bytes).unwrap(),
                logical_value
            );
            assert!(
                sql.statements().next().unwrap().contains("VALUES (?, ?)")
                    || sql
                        .statements()
                        .next()
                        .unwrap()
                        .contains("VALUES (?, CAST(? AS JSON))")
            );

            let table = "envelope_fixture";
            let raw = LogicalType::raw(
                "postgresql.pgoutput.text-envelope.v1",
                "public.opaque_type",
                "a".repeat(64),
                "UTF-8",
            );
            let plans = carrier_plans(&manifest, table, raw, "public.opaque_type", true);
            let source_cursor = SourceCursor {
                format: "fixture.cursor.v1".into(),
                value: "90".into(),
                display: "90".into(),
            };
            let envelope = source_envelope(source_cursor);
            let tx = insert_transaction(
                table,
                Datum::SourceRepresentationEnvelope(envelope.clone()),
                "public.opaque_type",
            );
            let sql = ::$adapter::sql_with_plans(&tx, &plans).unwrap();
            let encoded = sql.parameters().next().unwrap().last().unwrap();
            let mysql::Value::Bytes(bytes) = encoded else {
                panic!("source envelope must use a bound byte parameter");
            };
            assert_eq!(
                serde_json::from_slice::<SourceRepresentationEnvelope>(bytes).unwrap(),
                envelope
            );
        }};
    }

    let manifests = manifests();
    assert_rendered!(mysql_5_7, manifests[0].clone());
    assert_rendered!(mysql_8_0, manifests[1].clone());
    assert_rendered!(mysql_8_4, manifests[2].clone());
}

#[test]
fn every_public_logical_value_variant_round_trips_through_each_mysql_sink_carrier() {
    let cursor = SourceCursor {
        format: "fixture.cursor.v1".into(),
        value: "100".into(),
        display: "100".into(),
    };
    let (cases, changes): (Vec<_>, Vec<_>) = logical_value_carrier_cases()
        .into_iter()
        .enumerate()
        .map(|(index, (name, logical_type, value))| {
            let field_name = format!("payload_{index}");
            let datum = ColumnDatum {
                ordinal: index + 1,
                name: field_name,
                native_type: format!("fixture_{name}"),
                primary_key_ordinal: None,
                generated: false,
                collation: None,
                datum: Datum::Value(value.clone()),
            };
            ((name, logical_type, value), datum)
        })
        .unzip();
    assert_eq!(
        cases.len(),
        33,
        "bounded and unbounded decimal fixtures included"
    );
    let represented_tags: std::collections::BTreeSet<_> = cases
        .iter()
        .map(|(_, _, value)| match value {
            LogicalValue::Null => "null",
            LogicalValue::Boolean { .. } => "boolean",
            LogicalValue::Uuid { .. } => "uuid",
            LogicalValue::Integer { .. } => "integer",
            LogicalValue::Decimal { .. } => "decimal",
            LogicalValue::Float { .. } => "float",
            LogicalValue::Text { .. } => "text",
            LogicalValue::Binary { .. } => "binary",
            LogicalValue::BitString { .. } => "bit_string",
            LogicalValue::Date { .. } => "date",
            LogicalValue::LocalTime { .. } => "local_time",
            LogicalValue::OffsetTime { .. } => "offset_time",
            LogicalValue::LocalDatetime { .. } => "local_datetime",
            LogicalValue::Duration { .. } => "duration",
            LogicalValue::CalendarInterval { .. } => "calendar_interval",
            LogicalValue::Instant { .. } => "instant",
            LogicalValue::Year { .. } => "year",
            LogicalValue::TemporalInfinity { .. } => "temporal_infinity",
            LogicalValue::Enum { .. } => "enum",
            LogicalValue::Set { .. } => "set",
            LogicalValue::Spatial { .. } => "spatial",
            LogicalValue::Array { .. } => "array",
            LogicalValue::Struct { .. } => "struct",
            LogicalValue::Map { .. } => "map",
            LogicalValue::Range { .. } => "range",
            LogicalValue::MultiRange { .. } => "multirange",
            LogicalValue::ArrayWithMetadata { .. } => "array_with_metadata",
            LogicalValue::InvalidTemporal { .. } => "invalid_temporal",
            LogicalValue::Network { .. } => "network",
            LogicalValue::Xml { .. } => "xml",
            LogicalValue::Domain { .. } => "domain",
            LogicalValue::Raw { .. } => "raw",
            LogicalValue::Json { .. } => "json",
        })
        .collect();
    assert_eq!(represented_tags.len(), 32, "every public LogicalValue tag");

    let mut after = vec![ColumnDatum {
        ordinal: 0,
        name: "id".into(),
        native_type: "bigint".into(),
        primary_key_ordinal: Some(0),
        generated: false,
        collation: None,
        datum: Datum::Value(LogicalValue::Integer {
            signed: true,
            bits: 64,
            value: "1".into(),
        }),
    }];
    after.extend(changes);
    let transaction = change_event::validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "15".into(),
            id: "550e8400-e29b-41d4-a716-446655440000".into(),
        },
        id: "fixture-all-logical-values".into(),
        begin_cursor: cursor.clone(),
        commit_cursor: cursor.clone(),
        changes: vec![RowChange {
            database: Some("CDC_test".into()),
            operation: Operation::Insert,
            schema: "CDC_test".into(),
            table: "all_logical_values".into(),
            source_cursor: SourceCursor {
                format: "fixture.cursor.v1".into(),
                value: "90".into(),
                display: "90".into(),
            },
            source_timestamp: 1_790_000_000,
            schema_basis: "fixture-schema".into(),
            before: None,
            after: Some(after),
        }],
    })
    .unwrap();

    for manifest in manifests() {
        let mut plans = vec![mysql_plan(
            &manifest,
            MysqlPlanSpec::new(
                "all_logical_values",
                "id",
                "bigint",
                LogicalType::integer(true, 64),
                "bigint",
                LogicalType::integer(true, 64),
            )
            .key(0),
        )];
        for (index, (name, logical_type, _)) in cases.iter().enumerate() {
            plans.push(mysql_plan(
                &manifest,
                MysqlPlanSpec::new(
                    "all_logical_values",
                    &format!("payload_{index}"),
                    &format!("fixture_{name}"),
                    logical_type.clone(),
                    "json",
                    LogicalType::json(),
                )
                .conversion("logical_value_json", vec![PresenceState::Value]),
            ));
        }

        macro_rules! assert_sink_round_trip {
            ($adapter:ident) => {{
                let sql = ::$adapter::sql_with_plans(&transaction, &plans)
                    .expect("plan-backed SQL for every tagged LogicalValue variant");
                let parameters = sql.parameters().next().expect("insert parameters");
                assert_eq!(parameters.len(), cases.len() + 1);
                for ((name, _, expected), parameter) in cases.iter().zip(parameters.iter().skip(1))
                {
                    let mysql::Value::Bytes(bytes) = parameter else {
                        panic!("{name} must be stored as tagged JSON bytes");
                    };
                    let recovered: LogicalValue = serde_json::from_slice(bytes)
                        .unwrap_or_else(|error| panic!("{name} JSON decode failed: {error}"));
                    assert_eq!(&recovered, expected, "{name} carrier round trip");
                }
            }};
        }

        match manifest.connector.version.as_str() {
            "5.7" => assert_sink_round_trip!(mysql_5_7),
            "8.0" => assert_sink_round_trip!(mysql_8_0),
            "8.4" => assert_sink_round_trip!(mysql_8_4),
            version => panic!("unexpected MySQL version {version}"),
        }
    }
}
