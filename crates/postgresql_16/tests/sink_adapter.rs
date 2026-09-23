use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, LogicalType, LogicalValue, Operation, RowChange,
    ServerBuildIdentity, SinkAdapter as _, Source, SourceCursor, SpatialFormat, StructuredField,
    validate,
};

#[test]
fn postgres16_sink_has_its_own_versioned_contract() {
    let sink = postgresql_16::SinkAdapter::new();
    let manifest = sink.capability_manifest();
    assert_eq!(manifest.connector, "postgresql_16");
    assert_eq!(manifest.target, "postgresql-16");
    for logical_type in [
        "array",
        "composite",
        "domain",
        "range",
        "multirange",
        "spatial",
        "network",
        "xml",
        "custom",
    ] {
        assert!(manifest.supported_logical_types.contains(&logical_type));
    }
}

#[test]
fn postgres16_structured_manifest_is_versioned_and_digest_valid() {
    let manifest = postgresql_16::structured_capability_manifest(ServerBuildIdentity::new(
        "postgresql",
        "community",
        "16.4",
        "postgres-16.4",
    ));
    assert_eq!(manifest.connector.version, "16");
    assert!(manifest.verify_digest());
    manifest.validate().unwrap();
    assert!(manifest.capabilities.iter().any(|entry| {
        matches!(entry.source_logical_type, LogicalType::Array { .. })
            && entry.target.native_type == "integer[]"
            && entry.rule.version.contains("postgresql-16")
    }));
    assert!(
        manifest
            .capabilities
            .iter()
            .any(|entry| matches!(entry.source_logical_type, LogicalType::Spatial { .. }))
    );
}

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "postgresql.lsn.v1".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn column(
    ordinal: usize,
    name: &str,
    native_type: &str,
    primary_key_ordinal: Option<usize>,
    value: LogicalValue,
) -> ColumnDatum {
    ColumnDatum {
        ordinal,
        name: name.into(),
        native_type: native_type.into(),
        primary_key_ordinal,
        generated: false,
        collation: None,
        datum: Datum::Value(value),
    }
}

#[test]
fn postgres16_sink_keeps_structured_values_parameterized() {
    let spatial = URL_SAFE_NO_PAD.encode([
        1, 1, 0, 0, 0x20, 0xe6, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    let transaction = validate(ChangeTransaction {
        source: Source {
            kind: "postgresql".into(),
            version: "16.4".into(),
            id: "postgresql:test".into(),
        },
        id: "pg:test".into(),
        begin_cursor: cursor("0/1"),
        commit_cursor: cursor("0/2"),
        changes: vec![RowChange {
            database: Some("cdc".into()),
            operation: Operation::Insert,
            schema: "public".into(),
            table: "structured_values".into(),
            source_cursor: cursor("0/2"),
            source_timestamp: 1,
            schema_basis: "catalog:test".into(),
            before: None,
            after: Some(vec![
                column(
                    0,
                    "id",
                    "bigint",
                    Some(0),
                    LogicalValue::Integer {
                        signed: true,
                        bits: 64,
                        value: "1".into(),
                    },
                ),
                column(
                    1,
                    "values",
                    "integer[]",
                    None,
                    LogicalValue::Array {
                        elements: vec![
                            LogicalValue::Integer {
                                signed: true,
                                bits: 32,
                                value: "1".into(),
                            },
                            LogicalValue::Integer {
                                signed: true,
                                bits: 32,
                                value: "2".into(),
                            },
                        ],
                    },
                ),
                column(
                    2,
                    "record",
                    "cdc_composite",
                    None,
                    LogicalValue::Struct {
                        fields: vec![StructuredField {
                            name: "value".into(),
                            value: LogicalValue::Integer {
                                signed: true,
                                bits: 32,
                                value: "7".into(),
                            },
                        }],
                    },
                ),
                column(
                    3,
                    "period",
                    "int4range",
                    None,
                    LogicalValue::Range {
                        empty: false,
                        lower: Some(Box::new(LogicalValue::Integer {
                            signed: true,
                            bits: 32,
                            value: "1".into(),
                        })),
                        upper: Some(Box::new(LogicalValue::Integer {
                            signed: true,
                            bits: 32,
                            value: "3".into(),
                        })),
                        lower_inclusive: true,
                        upper_inclusive: false,
                    },
                ),
                column(
                    4,
                    "periods",
                    "int4multirange",
                    None,
                    LogicalValue::MultiRange {
                        ranges: vec![LogicalValue::Range {
                            empty: false,
                            lower: Some(Box::new(LogicalValue::Integer {
                                signed: true,
                                bits: 32,
                                value: "1".into(),
                            })),
                            upper: Some(Box::new(LogicalValue::Integer {
                                signed: true,
                                bits: 32,
                                value: "3".into(),
                            })),
                            lower_inclusive: true,
                            upper_inclusive: false,
                        }],
                    },
                ),
                column(
                    5,
                    "address",
                    "inet",
                    None,
                    LogicalValue::Network {
                        family: "ipv4".into(),
                        address: "192.0.2.1".into(),
                        prefix_length: None,
                    },
                ),
                column(
                    6,
                    "document",
                    "xml",
                    None,
                    LogicalValue::Xml {
                        bytes_base64url: URL_SAFE_NO_PAD.encode("<a/>"),
                        text: Some("<a/>".into()),
                    },
                ),
                column(
                    7,
                    "shape",
                    "geometry",
                    None,
                    LogicalValue::Spatial {
                        format: SpatialFormat::Ewkb,
                        bytes_base64url: spatial,
                        geometry_type: "point".into(),
                        dimensions: 2,
                        srid: Some(4326),
                        crs: Some("EPSG:4326".into()),
                    },
                ),
            ]),
        }],
    })
    .unwrap();

    let plan = postgresql_16::SinkAdapter::new()
        .plan(&transaction)
        .unwrap();
    let statement = plan.statements().next().unwrap();
    assert!(statement.contains("$2"));
    assert!(statement.contains("integer[]"));
    assert!(statement.contains("ST_GeomFromEWKB"));
    assert!(plan.parameters().all(|parameters| !parameters.is_empty()));
}
