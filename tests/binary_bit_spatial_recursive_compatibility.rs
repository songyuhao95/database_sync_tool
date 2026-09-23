use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    BitOrder, BitPadding, CapabilityEntry, ChangeTransaction, ColumnDatum, CompatibilityInput,
    ConnectorIdentity, ConversionRule, Datum, DefinitionReference, FailurePolicy,
    FieldCompatibilityInput, FieldDefinition, LogicalType, LogicalValue, Operation, PresenceState,
    QualificationLevel, RiskLevel, RouteOptions, RowChange, ServerBuildIdentity, Source,
    SourceCursor, SourceTypeMapping, SpatialFormat, TargetCapabilityManifest, TargetRepresentation,
};

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "fixture".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn transaction(value: LogicalValue, native_type: &str, source: Source) -> ChangeTransaction {
    ChangeTransaction {
        source,
        id: "tx-binary-bit-spatial".into(),
        begin_cursor: cursor("begin"),
        commit_cursor: cursor("commit"),
        changes: vec![RowChange {
            database: Some("db".into()),
            operation: Operation::Insert,
            schema: "s".into(),
            table: "t".into(),
            source_cursor: cursor("row"),
            source_timestamp: 1,
            schema_basis: "fixture".into(),
            before: None,
            after: Some(vec![
                ColumnDatum {
                    ordinal: 0,
                    name: "id".into(),
                    native_type: "int".into(),
                    primary_key_ordinal: Some(0),
                    generated: false,
                    collation: None,
                    datum: Datum::Value(LogicalValue::Integer {
                        signed: true,
                        bits: 32,
                        value: "1".into(),
                    }),
                },
                ColumnDatum {
                    ordinal: 1,
                    name: "value".into(),
                    native_type: native_type.into(),
                    primary_key_ordinal: None,
                    generated: false,
                    collation: None,
                    datum: Datum::Value(value),
                },
            ]),
        }],
    }
}

fn field(
    native_type: &str,
    logical_type: LogicalType,
    primary_key_ordinal: Option<usize>,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.value", "source-fingerprint"),
        ordinal: 1,
        name: "value".into(),
        native_type: native_type.into(),
        logical_type,
        nullable: true,
        collation: None,
        generated: false,
        primary_key_ordinal,
        unique: false,
        row_locator: false,
    }
}

fn target_field(
    native_type: &str,
    logical_type: LogicalType,
    primary_key_ordinal: Option<usize>,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.value-target", "target-fingerprint"),
        ordinal: 1,
        name: "value".into(),
        native_type: native_type.into(),
        logical_type,
        nullable: true,
        collation: None,
        generated: false,
        primary_key_ordinal,
        unique: false,
        row_locator: false,
    }
}

fn options(route_id: &str) -> RouteOptions {
    RouteOptions {
        route_id: route_id.into(),
        configuration_revision: format!("{route_id}:r1"),
        ..RouteOptions::default()
    }
}

fn mysql_id_plan(
    transaction: &change_event::ValidatedTransaction,
    manifest: &TargetCapabilityManifest,
) -> change_event::ColumnConversionPlan {
    let mapping = if transaction.transaction().source.kind == "postgresql" {
        SourceTypeMapping::new(
            ConnectorIdentity::new("postgresql", "15"),
            "int",
            LogicalType::integer(true, 32),
            "postgresql15.source-type.integer",
            "postgresql-test.v1",
        )
    } else {
        mysql_5_7::source_type_mapping("int", None, None).unwrap()
    };
    let source_field = FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.id", "source-id"),
        ordinal: 0,
        name: "id".into(),
        native_type: "int".into(),
        logical_type: mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: Some(0),
        unique: false,
        row_locator: false,
    };
    let target_field = FieldDefinition {
        reference: DefinitionReference::new("catalog:s.t.id-target", "target-id"),
        ordinal: 0,
        name: "id".into(),
        native_type: "int".into(),
        logical_type: mapping.logical_type.clone(),
        nullable: false,
        collation: None,
        generated: false,
        primary_key_ordinal: Some(0),
        unique: false,
        row_locator: false,
    };
    let result = change_event::plan_compatibility(CompatibilityInput {
        transaction,
        source_field,
        target_field,
        source_type_mapping: mapping.clone(),
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(if transaction.transaction().source.kind == "postgresql" {
            ServerBuildIdentity::new("postgresql", "community", "15.19", "postgres-15.19")
        } else {
            ServerBuildIdentity::new("mysql", "oracle", "5.7.44", "mysql-5.7.44")
        }),
        target_build: Some(manifest.target_build.clone()),
        manifest,
        options: options("mysql-id-route"),
    })
    .unwrap();
    assert_eq!(result.status, change_event::CompatibilityStatus::Compatible);
    result.plan.expect("id must have a conversion plan")
}

fn input<'a>(
    source: FieldDefinition,
    target: FieldDefinition,
    mapping: SourceTypeMapping,
    manifest: &'a TargetCapabilityManifest,
    route_id: &str,
) -> FieldCompatibilityInput<'a> {
    FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: None,
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: mapping,
        source_field: source,
        target_field: target,
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: true,
        options: options(route_id),
    }
}

fn spatial_manifest() -> TargetCapabilityManifest {
    let mut target = TargetRepresentation::new("geometry(point,4326)");
    target
        .parameters
        .insert("source_spatial_format".into(), "ewkb".into());
    target
        .parameters
        .insert("target_spatial_format".into(), "ewkb".into());
    target
        .parameters
        .insert("target_geometry_type".into(), "point".into());
    target
        .parameters
        .insert("target_dimensions".into(), "2".into());
    target
        .parameters
        .insert("target_srid".into(), "4326".into());
    target
        .parameters
        .insert("target_crs".into(), "srid:4326".into());
    let operations = vec![Operation::Insert, Operation::Update, Operation::Delete];
    let presence = vec![
        PresenceState::Value,
        PresenceState::Null,
        PresenceState::Unchanged,
    ];
    let logical = LogicalType::spatial("point", Some(4326), 2);
    TargetCapabilityManifest::new(
        ConnectorIdentity::new("postgis", "3"),
        ServerBuildIdentity::new("postgis", "community", "3", "postgis-3"),
        vec![CapabilityEntry {
            code: "postgis.exact.geometry.point.4326".into(),
            source_logical_type: logical,
            target,
            rule: ConversionRule {
                id: "postgis.conversion.geometry.point.4326".into(),
                version: "postgis-test.v1".into(),
                qualification: QualificationLevel::Exact,
                risk: RiskLevel::None,
                risk_code: None,
                requires_confirmation: false,
                options: Vec::new(),
                supported_operations: operations.clone(),
                supported_presence: presence.clone(),
                allows_key: true,
                failure_policy: FailurePolicy::Reject,
                evidence_digest: "postgis-test-evidence".into(),
            },
            supported_operations: operations,
            supported_presence: presence,
        }],
        true,
    )
}

fn ewkb_point(srid: i32) -> LogicalValue {
    let mut bytes = vec![1, 1, 0, 0, 0x20];
    bytes.extend_from_slice(&srid.to_le_bytes());
    LogicalValue::Spatial {
        format: SpatialFormat::Ewkb,
        bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
        geometry_type: "point".into(),
        dimensions: 2,
        srid: Some(srid),
        crs: Some(format!("srid:{srid}")),
    }
}

#[test]
fn binary_and_bit_string_keep_raw_boundaries_and_metadata() {
    let binary_mapping = mysql_5_7::source_type_mapping("binary(4)", None, None).unwrap();
    let _binary_transaction = change_event::validate(transaction(
        LogicalValue::Binary {
            bytes_base64url: URL_SAFE_NO_PAD.encode([1_u8, 2, 3]),
        },
        "binary(4)",
        Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let result = change_event::plan_field_compatibility(input(
        field("binary(4)", binary_mapping.logical_type.clone(), None),
        target_field("binary(4)", LogicalType::binary(Some(4)), None),
        binary_mapping,
        &manifest,
        "binary-route",
    ))
    .unwrap();
    let candidate_allows_key = manifest
        .capabilities
        .iter()
        .find(|capability| capability.code == result.candidates[0].capability_code)
        .map(|capability| capability.rule.allows_key)
        .unwrap_or(false);
    let plan = result.plan.unwrap();
    assert_eq!(plan.target.parameters["binary_encoding"], "raw_bytes");
    assert_eq!(plan.target.parameters["binary_length_unit"], "bytes");
    assert_eq!(plan.target.parameters["target_padding"], "zero");
    assert!(candidate_allows_key);
    assert!(
        change_event::validate_value_against_plan(
            &plan,
            &LogicalValue::Binary {
                bytes_base64url: "%%%".into(),
            },
        )
        .is_err()
    );

    let bit_mapping = mysql_5_7::source_type_mapping("bit(10)", None, None).unwrap();
    let _bit_transaction = change_event::validate(transaction(
        LogicalValue::BitString {
            bytes_base64url: URL_SAFE_NO_PAD.encode([0xaa, 0]),
            bit_length: 10,
            padding: BitPadding::Zero,
            bit_order: BitOrder::MsbFirst,
        },
        "bit(10)",
        Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let result = change_event::plan_field_compatibility(input(
        field("bit(10)", bit_mapping.logical_type.clone(), None),
        target_field("bit(10)", LogicalType::bit_string(10), None),
        bit_mapping,
        &manifest,
        "bit-route",
    ))
    .unwrap();
    let plan = result.plan.unwrap();
    assert_eq!(plan.target.parameters["target_bit_length"], "10");
    assert_eq!(plan.target.parameters["target_bit_order"], "msb_first");
    assert_eq!(plan.target.parameters["target_padding"], "zero");
    let error = change_event::validate_value_against_plan(
        &plan,
        &LogicalValue::BitString {
            bytes_base64url: URL_SAFE_NO_PAD.encode([0xaa, 1]),
            bit_length: 10,
            padding: BitPadding::Zero,
            bit_order: BitOrder::MsbFirst,
        },
    )
    .unwrap_err();
    assert_eq!(error.code, "target_capability.bit_padding_invalid");
}

#[test]
fn spatial_requires_wire_header_srid_crs_and_geometry_metadata() {
    let manifest = spatial_manifest();
    let logical = LogicalType::spatial("point", Some(4326), 2);
    let mapping = SourceTypeMapping::new(
        ConnectorIdentity::new("postgis", "3"),
        "geometry(point,4326)",
        logical.clone(),
        "postgis.source-type.geometry",
        "postgis-test.v1",
    );
    let _validated = change_event::validate(transaction(
        ewkb_point(4326),
        "geometry(point,4326)",
        Source {
            kind: "postgis".into(),
            version: "3.4".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let result = change_event::plan_field_compatibility(input(
        field("geometry(point,4326)", logical.clone(), None),
        target_field("geometry(point,4326)", logical, None),
        mapping,
        &manifest,
        "spatial-route",
    ))
    .unwrap();
    let plan = result.plan.unwrap();
    assert_eq!(plan.target.parameters["target_crs"], "srid:4326");
    assert_eq!(plan.target.parameters["target_spatial_format"], "ewkb");

    let mut wrong_srid = ewkb_point(3857);
    if let LogicalValue::Spatial { crs, .. } = &mut wrong_srid {
        *crs = Some("srid:4326".into());
    }
    let error = change_event::validate_value_against_plan(&plan, &wrong_srid).unwrap_err();
    assert_eq!(error.code, "target_capability.spatial_srid_mismatch");
}

#[test]
fn recursive_types_fail_closed_without_an_explicit_structure_rule() {
    let logical = LogicalType::Array {
        element: Box::new(LogicalType::integer(true, 32)),
    };
    let mapping = SourceTypeMapping::new(
        ConnectorIdentity::new("postgresql", "15"),
        "integer[]",
        logical.clone(),
        "postgresql15.source-type.array",
        "postgresql-test.v1",
    );
    let _validated = change_event::validate(transaction(
        LogicalValue::Array {
            elements: vec![LogicalValue::Integer {
                signed: true,
                bits: 32,
                value: "1".into(),
            }],
        },
        "integer[]",
        Source {
            kind: "postgresql".into(),
            version: "15.19".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let manifest = TargetCapabilityManifest::new(
        ConnectorIdentity::new("postgresql", "15"),
        ServerBuildIdentity::new("postgresql", "community", "15.19", "postgres-15.19"),
        Vec::new(),
        true,
    );
    let result = change_event::plan_field_compatibility(input(
        field("integer[]", logical.clone(), None),
        target_field("integer[]", logical, None),
        mapping,
        &manifest,
        "recursive-route",
    ))
    .unwrap();
    assert!(matches!(
        result.status,
        change_event::CompatibilityStatus::Unsupported | change_event::CompatibilityStatus::Blocked
    ));
    assert_eq!(
        result.reason_code,
        "target_capability.recursive_structure_unqualified"
    );
}

#[test]
fn mysql_sink_uses_json_value_carrier_for_a_qualified_recursive_plan() {
    let logical = LogicalType::Array {
        element: Box::new(LogicalType::integer(true, 32)),
    };
    let mapping = SourceTypeMapping::new(
        ConnectorIdentity::new("postgresql", "15"),
        "integer[]",
        logical.clone(),
        "postgresql15.source-type.array",
        "postgresql-test.v1",
    );
    let transaction = change_event::validate(transaction(
        LogicalValue::Array {
            elements: vec![LogicalValue::Integer {
                signed: true,
                bits: 32,
                value: "1".into(),
            }],
        },
        "integer[]",
        Source {
            kind: "postgresql".into(),
            version: "15.19".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let source = field("integer[]", logical, None);
    let target = target_field("json", LogicalType::json(), None);
    let result = change_event::plan_compatibility(CompatibilityInput {
        transaction: &transaction,
        source_field: source,
        target_field: target,
        source_type_mapping: mapping,
        source_connector: ConnectorIdentity::new("postgresql", "15"),
        sink_connector: manifest.connector.clone(),
        source_build: Some(ServerBuildIdentity::new(
            "postgresql",
            "community",
            "15.19",
            "postgres-15.19",
        )),
        target_build: Some(manifest.target_build.clone()),
        manifest: &manifest,
        options: options("mysql-recursive-route"),
    })
    .unwrap();
    assert_eq!(
        result.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    let mut plan = result.plan.expect("recursive JSON carrier should qualify");
    assert_eq!(
        plan.target
            .parameters
            .get("conversion_kind")
            .map(String::as_str),
        Some("recursive")
    );
    plan.confirmation = change_event::PlanConfirmationState::Confirmed;
    let id_plan = mysql_id_plan(&transaction, &manifest);
    let sql = mysql_8_0::sql_with_plans(&transaction, &[id_plan, plan]).unwrap();
    assert!(sql.statements().all(|statement| statement.contains('?')));
    assert!(sql.parameters().any(|parameters| {
        parameters
            .iter()
            .any(|parameter| matches!(parameter, mysql::Value::Bytes(bytes) if bytes.starts_with(b"{\"type\":\"array\"")))
    }));
}

#[test]
fn mysql_sink_uses_native_spatial_wkb_binding_and_srid() {
    let mapping = mysql_5_7::source_type_mapping("point srid 4326", None, None).unwrap();
    let logical = mapping.logical_type.clone();
    let value = LogicalValue::Spatial {
        format: SpatialFormat::Wkb,
        bytes_base64url: URL_SAFE_NO_PAD.encode([
            1_u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        geometry_type: "point".into(),
        dimensions: 2,
        srid: Some(4326),
        crs: Some("srid:4326".into()),
    };
    let transaction = change_event::validate(transaction(
        value,
        "point srid 4326",
        Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let result = change_event::plan_compatibility(CompatibilityInput {
        transaction: &transaction,
        source_field: field("point srid 4326", logical.clone(), None),
        target_field: target_field("point srid 4326", logical, None),
        source_type_mapping: mapping,
        source_connector: ConnectorIdentity::new("mysql", "5.7"),
        sink_connector: manifest.connector.clone(),
        source_build: Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(manifest.target_build.clone()),
        manifest: &manifest,
        options: options("mysql-spatial-route"),
    })
    .unwrap();
    assert_eq!(result.status, change_event::CompatibilityStatus::Compatible);
    let id_plan = mysql_id_plan(&transaction, &manifest);
    let sql = mysql_8_0::sql_with_plans(&transaction, &[id_plan, result.plan.unwrap()]).unwrap();
    assert!(
        sql.statements()
            .any(|statement| statement.contains("ST_GeomFromWKB(?, 4326)"))
    );
    assert!(sql.parameters().any(|parameters| {
        parameters.iter().any(|parameter| {
            matches!(parameter, mysql::Value::Bytes(bytes) if bytes.starts_with(&[1, 1, 0, 0, 0]))
        })
    }));
}

#[test]
fn failed_conversion_does_not_partially_write_and_null_unchanged_are_preserved() {
    let mapping = mysql_5_7::source_type_mapping("bit(10)", None, None).unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let validated = change_event::validate(transaction(
        LogicalValue::BitString {
            bytes_base64url: URL_SAFE_NO_PAD.encode([0xaa, 0]),
            bit_length: 10,
            padding: BitPadding::Zero,
            bit_order: BitOrder::MsbFirst,
        },
        "bit(10)",
        Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
    ))
    .unwrap();
    let result = change_event::plan_field_compatibility(input(
        field("bit(10)", mapping.logical_type.clone(), None),
        target_field("bit(10)", LogicalType::bit_string(10), None),
        mapping,
        &manifest,
        "atomic-route",
    ))
    .unwrap();
    let plan = result.plan.unwrap();
    let mut invalid = validated.transaction().clone();
    let after = invalid.changes[0].after.as_mut().unwrap();
    after[1].datum = Datum::Value(LogicalValue::BitString {
        bytes_base64url: URL_SAFE_NO_PAD.encode([0xaa, 1]),
        bit_length: 10,
        padding: BitPadding::Zero,
        bit_order: BitOrder::MsbFirst,
    });
    let before = format!("{invalid:?}");
    assert!(
        change_event::convert_transaction_with_plans(invalid.clone(), std::slice::from_ref(&plan))
            .is_err()
    );
    assert_eq!(before, format!("{invalid:?}"));

    let mut presence_transaction = validated.transaction().clone();
    let after = presence_transaction.changes[0].after.as_mut().unwrap();
    after.push(ColumnDatum {
        ordinal: 2,
        name: "nullable".into(),
        native_type: "bit(10)".into(),
        primary_key_ordinal: None,
        generated: false,
        collation: None,
        datum: Datum::Null,
    });
    presence_transaction.changes[0].before = Some(vec![ColumnDatum {
        ordinal: 1,
        name: "value".into(),
        native_type: "bit(10)".into(),
        primary_key_ordinal: None,
        generated: false,
        collation: None,
        datum: Datum::Unchanged,
    }]);
    let converted =
        change_event::convert_transaction_with_plans(presence_transaction, &[plan]).unwrap();
    assert!(matches!(
        converted.changes[0].after.as_ref().unwrap()[2].datum,
        Datum::Null
    ));
    assert!(matches!(
        converted.changes[0].before.as_ref().unwrap()[0].datum,
        Datum::Unchanged
    ));
}
