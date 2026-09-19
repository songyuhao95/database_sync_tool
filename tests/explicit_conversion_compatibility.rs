use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ChangeTransaction, ColumnConversionPlan, ColumnDatum, Datum, DefinitionReference,
    FieldCompatibilityInput, FieldDefinition, LogicalType, LogicalValue, Operation, PresenceState,
    RiskConfirmation, RouteOptions, RowChange, ServerBuildIdentity, Source, SourceCursor,
};

fn cursor(value: &str) -> SourceCursor {
    SourceCursor {
        format: "fixture".into(),
        value: value.into(),
        display: value.into(),
    }
}

fn insert(value: LogicalValue, native_type: &str) -> ChangeTransaction {
    ChangeTransaction {
        source: Source {
            kind: "mysql".into(),
            version: "5.7.44".into(),
            id: "source".into(),
        },
        id: "tx-1".into(),
        begin_cursor: cursor("begin"),
        commit_cursor: cursor("commit"),
        changes: vec![RowChange {
            database: Some("db".into()),
            operation: Operation::Insert,
            schema: "s".into(),
            table: "t".into(),
            source_cursor: cursor("row"),
            source_timestamp: 1,
            schema_basis: "schema".into(),
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
    lineage: &str,
    native_type: &str,
    logical_type: LogicalType,
    collation: Option<&str>,
    key: Option<usize>,
) -> FieldDefinition {
    FieldDefinition {
        reference: DefinitionReference::new(lineage, format!("{lineage}-fingerprint")),
        ordinal: if key.is_some() { 0 } else { 1 },
        name: if key.is_some() { "id" } else { "value" }.into(),
        native_type: native_type.into(),
        logical_type,
        nullable: false,
        collation: collation.map(str::to_owned),
        generated: false,
        primary_key_ordinal: key,
        unique: key.is_some(),
        row_locator: key.is_some(),
    }
}

fn options() -> RouteOptions {
    RouteOptions {
        route_id: "route".into(),
        configuration_revision: "route:r1".into(),
        ..RouteOptions::default()
    }
}

fn input<'a>(
    _tx: &'a change_event::ValidatedTransaction,
    source_field: FieldDefinition,
    target_field: FieldDefinition,
    mapping: change_event::SourceTypeMapping,
    manifest: &'a change_event::TargetCapabilityManifest,
    options: RouteOptions,
) -> FieldCompatibilityInput<'a> {
    FieldCompatibilityInput {
        source_connector: mapping.connector.clone(),
        sink_connector: manifest.connector.clone(),
        source_build: Some(ServerBuildIdentity::new(
            "mysql",
            "oracle",
            "5.7.44",
            "mysql-5.7.44",
        )),
        target_build: Some(manifest.target_build.clone()),
        source_type_mapping: mapping,
        source_field,
        target_field,
        manifest,
        operations: vec![Operation::Insert, Operation::Update, Operation::Delete],
        presences: vec![
            PresenceState::Value,
            PresenceState::Null,
            PresenceState::Unchanged,
        ],
        source_has_primary_key: true,
        options,
    }
}

fn confirm(plan: &ColumnConversionPlan) -> RiskConfirmation {
    RiskConfirmation {
        source_field_lineage: plan.source_field.lineage_id.clone(),
        target_field_lineage: plan.target_field.lineage_id.clone(),
        rule: plan.rule.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor: "operator".into(),
        confirmed_at: "2026-09-18T00:00:00Z".into(),
        reason: Some("accepted the field-local explicit conversion".into()),
    }
}

#[test]
fn text_explicit_plan_is_configured_confirmed_and_strictly_encoded() {
    let raw = insert(
        LogicalValue::Text {
            charset: "utf8mb4".into(),
            bytes_base64url: URL_SAFE_NO_PAD.encode("你好".as_bytes()),
            text: Some("你好".into()),
        },
        "varchar(255)",
    );
    let validated = change_event::validate(raw).unwrap();
    let source_mapping =
        mysql_5_7::source_type_mapping("varchar(255)", Some("utf8mb4"), Some("utf8mb4_bin"))
            .unwrap();
    let target_mapping =
        mysql_8_0::source_type_mapping("varchar(10)", Some("utf8"), Some("utf8_bin")).unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let source = field(
        "catalog:s.t.value",
        "varchar(255)",
        source_mapping.logical_type.clone(),
        Some("utf8mb4_bin"),
        None,
    );
    let target = field(
        "catalog:s.t.value-target",
        "varchar(10)",
        target_mapping.logical_type,
        Some("utf8_bin"),
        None,
    );
    let mut configured = options();
    configured.parameters.extend([
        ("target_charset".into(), "utf8".into()),
        ("target_length".into(), "10".into()),
        ("target_length_unit".into(), "characters".into()),
        ("target_collation".into(), "utf8_bin".into()),
    ]);
    let pending = change_event::plan_field_compatibility(input(
        &validated,
        source.clone(),
        target.clone(),
        source_mapping.clone(),
        &manifest,
        configured.clone(),
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    let pending_plan = pending.plan.as_ref().unwrap();
    assert_eq!(pending_plan.target.parameters["source_charset"], "utf8mb4");
    assert_eq!(
        pending_plan.target.parameters["target_collation"],
        "utf8_bin"
    );

    configured.confirmations.push(confirm(pending_plan));
    let confirmed = change_event::plan_field_compatibility(input(
        &validated,
        source,
        target,
        source_mapping,
        &manifest,
        configured,
    ))
    .unwrap();
    assert_eq!(
        confirmed.status,
        change_event::CompatibilityStatus::Compatible
    );
    let plan = confirmed.plan.unwrap();
    let converted = change_event::convert_transaction_with_plans(
        validated.transaction().clone(),
        std::slice::from_ref(&plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Text { charset, text, .. }) =
        &converted.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected converted text")
    };
    assert_eq!(charset, "utf8");
    assert_eq!(text.as_deref(), Some("你好"));
    change_event::validate_datum_against_plan(&plan, &Datum::Null).unwrap();
    change_event::validate_datum_against_plan(&plan, &Datum::Unchanged).unwrap();
    assert!(change_event::validate_datum_against_plan(&plan, &Datum::Unavailable).is_err());

    let emoji = LogicalValue::Text {
        charset: "utf8mb4".into(),
        bytes_base64url: URL_SAFE_NO_PAD.encode("😀".as_bytes()),
        text: Some("😀".into()),
    };
    assert_eq!(
        change_event::validate_value_against_plan(&plan, &emoji)
            .unwrap_err()
            .code,
        "target_capability.text_character_unrepresentable"
    );
}

#[test]
fn temporal_explicit_plan_converts_local_time_with_fixed_offset_and_rejects_precision_loss() {
    let raw = insert(
        LogicalValue::LocalDatetime {
            year: 2026,
            month: 9,
            day: 18,
            hour: 12,
            minute: 0,
            second: 0,
            microsecond: 0,
        },
        "datetime(6)",
    );
    let validated = change_event::validate(raw).unwrap();
    let source_mapping = mysql_5_7::source_type_mapping("datetime(6)", None, None).unwrap();
    let target_mapping = mysql_8_0::source_type_mapping("timestamp", None, None).unwrap();
    let manifest = mysql_8_0::compatibility_manifest(ServerBuildIdentity::new(
        "mysql",
        "oracle",
        "8.0.36",
        "mysql-8.0.36",
    ));
    let source = field(
        "catalog:s.t.value",
        "datetime(6)",
        source_mapping.logical_type.clone(),
        None,
        None,
    );
    let target = field(
        "catalog:s.t.value-target",
        "timestamp",
        target_mapping.logical_type,
        None,
        None,
    );
    let mut configured = options();
    configured.parameters.extend([
        ("target_precision".into(), "0".into()),
        ("temporal_strategy".into(), "local_to_absolute".into()),
        ("time_zone".into(), "+08:00".into()),
    ]);
    let pending = change_event::plan_field_compatibility(input(
        &validated,
        source.clone(),
        target.clone(),
        source_mapping.clone(),
        &manifest,
        configured.clone(),
    ))
    .unwrap();
    assert_eq!(
        pending.status,
        change_event::CompatibilityStatus::NeedsConfirmation
    );
    configured
        .confirmations
        .push(confirm(pending.plan.as_ref().unwrap()));
    let plan = change_event::plan_field_compatibility(input(
        &validated,
        source,
        target,
        source_mapping,
        &manifest,
        configured,
    ))
    .unwrap()
    .plan
    .unwrap();
    let converted = change_event::convert_transaction_with_plans(
        validated.transaction().clone(),
        std::slice::from_ref(&plan),
    )
    .unwrap();
    let Datum::Value(LogicalValue::Instant {
        unix_seconds,
        nanoseconds,
    }) = &converted.changes[0].after.as_ref().unwrap()[1].datum
    else {
        panic!("expected converted instant")
    };
    assert_eq!(unix_seconds, &"1789704000".to_owned());
    assert_eq!(*nanoseconds, 0);

    let mut precision = insert(
        LogicalValue::LocalDatetime {
            year: 2026,
            month: 9,
            day: 18,
            hour: 12,
            minute: 0,
            second: 0,
            microsecond: 1,
        },
        "datetime(6)",
    );
    precision.source.version = "5.7.44".into();
    let validated_precision = change_event::validate(precision).unwrap();
    assert_eq!(
        change_event::validate_value_against_plan(
            &plan,
            &LogicalValue::LocalDatetime {
                year: 2026,
                month: 9,
                day: 18,
                hour: 12,
                minute: 0,
                second: 0,
                microsecond: 1,
            },
        )
        .unwrap_err()
        .code,
        "target_capability.temporal_precision_loss"
    );
    assert_eq!(validated_precision.transaction().changes.len(), 1);
}
