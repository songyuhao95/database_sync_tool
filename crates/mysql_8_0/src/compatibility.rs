//! MySQL 8.0 Sink capability evidence for the first MySQL 5.7 route.
//!
//! The legacy `CapabilityManifest` remains available for the existing SQL
//! adapter surface.  This module publishes the structured, content-addressed
//! manifest consumed by `change_event` compatibility planning.  Every entry is
//! an EXACT representation: a target column must have the same declared
//! parameters as the source mapping, and no runtime value can widen it.

use change_event::{
    CapabilityEntry, CompatibilityError, CompatibilityInput, CompatibilityResult,
    ConnectorIdentity, ConversionRule, FailurePolicy, LogicalType, Operation, OptionSpec,
    OptionValueKind, PresenceState, QualificationLevel, RiskLevel, ServerBuildIdentity,
    SourceTypeMapping, TargetCapabilityFailure, TargetCapabilityManifest, TargetRepresentation,
};
use sha2::{Digest as _, Sha256};

const CONNECTOR_KIND: &str = "mysql";
const CONNECTOR_VERSION: &str = "8.0";
const RULE_VERSION: &str = "mysql-8.0.sink-conversion.v1";

/// MySQL 8.0 uses the same declaration semantics as the shared MySQL source
/// contract, but publishes its own connector identity and mapping version.
pub fn source_type_mapping(
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<SourceTypeMapping, String> {
    let mut mapping = mysql_5_7::source_type_mapping(native_type, charset, collation)
        .map_err(|error| error.to_string())?;
    mapping.connector = ConnectorIdentity::new(CONNECTOR_KIND, CONNECTOR_VERSION);
    mapping.mapping_id = mapping.mapping_id.strip_prefix("mysql57.").map_or_else(
        || "mysql80.source-type.unknown".to_owned(),
        |id| format!("mysql80.{id}"),
    );
    mapping.mapping_version = "mysql-8.0.source-type-mapping.v1".to_owned();
    mapping.evidence_digest = Some(mapping_evidence_digest(&mapping));
    Ok(mapping)
}

fn mapping_evidence_digest(mapping: &SourceTypeMapping) -> String {
    let bytes = serde_json::to_vec(mapping).expect("MySQL mapping evidence is serializable");
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Build the versioned MySQL 8.0 capability manifest for one exact server
/// build. The build is an explicit input so a patch release cannot silently
/// inherit a qualification from another target instance.
pub fn compatibility_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    let mut capabilities = Vec::new();

    for (bits, native) in [
        (8, "tinyint"),
        (16, "smallint"),
        (24, "mediumint"),
        (32, "int"),
        (64, "bigint"),
    ] {
        for signed in [true, false] {
            let logical = LogicalType::integer(signed, bits);
            let target = if signed {
                native.to_owned()
            } else {
                format!("{native} unsigned")
            };
            add_exact(
                &mut capabilities,
                logical,
                target.clone(),
                format!(
                    "integer.{bits}.{}",
                    if signed { "signed" } else { "unsigned" }
                ),
            );
            add_exact(
                &mut capabilities,
                LogicalType::integer(signed, bits),
                if signed {
                    format!("{native}(11)")
                } else {
                    format!("{native}(11) unsigned")
                },
                format!(
                    "integer.{bits}.{}.display_width_11",
                    if signed { "signed" } else { "unsigned" }
                ),
            );
            add_range_checked_integer(
                &mut capabilities,
                signed,
                bits,
                target,
                format!(
                    "integer.{bits}.{}.domain",
                    if signed { "signed" } else { "unsigned" }
                ),
            );
        }
    }

    for precision in 1..=65 {
        for scale in 0..=precision.min(30) {
            add_exact(
                &mut capabilities,
                LogicalType::decimal(precision, i32::from(scale)),
                format!("decimal({precision},{scale})"),
                format!("decimal.{precision}.{scale}"),
            );
        }
    }
    for precision in 1..=65 {
        for scale in 0..=precision.min(30) {
            add_range_checked_decimal(&mut capabilities, precision, scale);
        }
    }

    add_exact(
        &mut capabilities,
        LogicalType::float(32),
        "float".to_owned(),
        "float.32".to_owned(),
    );
    add_exact(
        &mut capabilities,
        LogicalType::float(64),
        "double".to_owned(),
        "float.64".to_owned(),
    );
    add_range_checked_float(&mut capabilities);

    for charset in ["utf8mb4", "utf8", "latin1", "ascii"] {
        for max_length in text_lengths() {
            add_exact(
                &mut capabilities,
                LogicalType::Text {
                    charset: charset.to_owned(),
                    max_length: Some(max_length),
                    length_unit: change_event::LengthUnit::Characters,
                    collation: None,
                },
                format!("varchar({max_length})"),
                format!("text.{charset}.varchar.{max_length}"),
            );
        }
    }
    for charset in ["utf8mb4", "utf8", "latin1", "ascii"] {
        for max_length in 1..=255 {
            add_exact(
                &mut capabilities,
                LogicalType::Text {
                    charset: charset.to_owned(),
                    max_length: Some(max_length),
                    length_unit: change_event::LengthUnit::Characters,
                    collation: None,
                },
                format!("char({max_length})"),
                format!("text.{charset}.char.{max_length}"),
            );
        }
    }
    for max_length in text_lengths() {
        add_exact(
            &mut capabilities,
            LogicalType::Text {
                charset: "UTF8".to_owned(),
                max_length: Some(max_length),
                length_unit: change_event::LengthUnit::Characters,
                collation: None,
            },
            format!("varchar({max_length})"),
            format!("text.UTF8.varchar.{max_length}"),
        );
    }
    for (max_length, native) in [
        (255, "tinytext"),
        (65_535, "text"),
        (16_777_215, "mediumtext"),
        (4_294_967_295, "longtext"),
    ] {
        for charset in ["utf8mb4", "utf8", "latin1", "ascii"] {
            add_exact(
                &mut capabilities,
                LogicalType::Text {
                    charset: charset.to_owned(),
                    max_length: Some(max_length),
                    length_unit: change_event::LengthUnit::Bytes,
                    collation: None,
                },
                native.to_owned(),
                format!("text.{charset}.{native}"),
            );
        }
    }

    for max_length in binary_lengths() {
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(max_length),
            },
            format!("varbinary({max_length})"),
            format!("binary.varbinary.{max_length}"),
        );
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(max_length),
            },
            format!("binary({max_length})"),
            format!("binary.fixed.{max_length}"),
        );
    }
    for length in 1..=64 {
        add_exact_bit_string(&mut capabilities, length);
    }
    for (max_length, native) in [
        (255, "tinyblob"),
        (65_535, "blob"),
        (16_777_215, "mediumblob"),
        (4_294_967_295, "longblob"),
    ] {
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(max_length),
            },
            native.to_owned(),
            format!("binary.{native}"),
        );
    }

    add_exact(
        &mut capabilities,
        LogicalType::date(),
        "date".to_owned(),
        "date".to_owned(),
    );
    for precision in 0..=6 {
        let suffix = if precision == 0 {
            String::new()
        } else {
            format!("({precision})")
        };
        add_exact(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: precision,
            },
            format!("datetime{suffix}"),
            format!("local_datetime.{precision}"),
        );
        add_exact(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: precision,
            },
            format!("timestamp{suffix}"),
            format!("instant.{precision}"),
        );
        add_exact(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: precision,
            },
            format!("time{suffix}"),
            format!("duration.{precision}"),
        );
    }
    add_exact(
        &mut capabilities,
        LogicalType::year(),
        "year".to_owned(),
        "year".to_owned(),
    );
    add_structured_json(&mut capabilities);
    add_enum_set(&mut capabilities, false);
    add_enum_set(&mut capabilities, true);
    add_exact(
        &mut capabilities,
        LogicalType::Text {
            charset: "UTF8".into(),
            max_length: None,
            length_unit: change_event::LengthUnit::Characters,
            collation: None,
        },
        "longtext".into(),
        "text.UTF8.longtext.unbounded".into(),
    );
    for native in text_target_types() {
        add_explicit_text(&mut capabilities, native.clone());
        add_explicit_json_text(&mut capabilities, native);
    }
    for precision in 0..=6 {
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            datetime_type(precision),
            format!("local_datetime.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision),
            format!("instant.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: u8::MAX,
            },
            time_type(precision),
            format!("duration.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision),
            format!("local_datetime_to_instant.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            datetime_type(precision),
            format!("instant_to_local_datetime.{precision}"),
        );
    }
    add_explicit(
        &mut capabilities,
        LogicalType::Boolean,
        "tinyint(1)",
        "boolean.tinyint1".into(),
    );
    add_explicit(
        &mut capabilities,
        LogicalType::Boolean,
        "tinyint",
        "boolean.tinyint".into(),
    );
    add_explicit(
        &mut capabilities,
        LogicalType::Uuid,
        "char(36)",
        "uuid.char36".into(),
    );
    add_explicit(
        &mut capabilities,
        LogicalType::Uuid,
        "varchar(36)",
        "uuid.varchar36".into(),
    );

    TargetCapabilityManifest::new(
        change_event::ConnectorIdentity::new(CONNECTOR_KIND, CONNECTOR_VERSION),
        target_build,
        capabilities,
        true,
    )
}

/// Descriptive alias for callers that distinguish the structured manifest
/// from the legacy static adapter summary.
pub fn target_capability_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

/// Explicitly named entry point for callers that also expose the legacy
/// static `CapabilityManifest` on the same connector.
pub fn structured_capability_manifest(
    target_build: ServerBuildIdentity,
) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

/// Descriptive alias used by registry and preflight code.
pub fn capability_manifest_for(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

/// Run the common planner with the MySQL 8.0 Sink manifest. This is the same
/// function used by activation/preflight and by a runtime caller that already
/// has the source/target field definitions and the validated batch.
pub fn plan_compatibility(
    input: CompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    let collation_is_configurable = matches!(
        &input.source_field.logical_type,
        LogicalType::Text { .. } | LogicalType::Enum { .. } | LogicalType::Set { .. }
    );
    let source_mapping_matches = input.source_type_mapping.connector == input.source_connector
        && input.source_type_mapping.logical_type == input.source_field.logical_type
        && input
            .source_type_mapping
            .native_type
            .eq_ignore_ascii_case(&input.source_field.native_type);
    if input.source_field.collation.is_some()
        && input.target_field.collation.is_some()
        && input.source_field.collation != input.target_field.collation
        && (!collation_is_configurable || !source_mapping_matches)
        && input.options.parameters.is_empty()
        && input.options.selected_rule.is_none()
    {
        return Err(CompatibilityError::TargetCapability(Box::new(
            TargetCapabilityFailure::new("MySQL 8.0 field collations are not equivalent")
                .with_code("target_capability.collation_mismatch")
                .with_route(input.options.route_id.clone()),
        )));
    }
    change_event::plan_compatibility(input)
}

impl crate::sql::SinkAdapter {
    /// Publish the structured manifest through the SinkAdapter seam used by
    /// preflight and runtime callers.
    pub fn structured_capability_manifest(
        &self,
        target_build: ServerBuildIdentity,
    ) -> TargetCapabilityManifest {
        compatibility_manifest(target_build)
    }

    /// Plan one field binding through the same database-neutral planner that
    /// preflight and runtime integrations call directly.
    pub fn plan_compatibility(
        &self,
        input: CompatibilityInput<'_>,
    ) -> Result<CompatibilityResult, CompatibilityError> {
        plan_compatibility(input)
    }
}

fn add_exact(
    capabilities: &mut Vec<CapabilityEntry>,
    source_logical_type: LogicalType,
    target_native_type: String,
    code_suffix: String,
) {
    add_exact_with_target(
        capabilities,
        source_logical_type,
        TargetRepresentation::new(target_native_type),
        code_suffix,
    );
}

fn add_exact_bit_string(capabilities: &mut Vec<CapabilityEntry>, length: u64) {
    let mut target = TargetRepresentation::new(format!("bit({length})"));
    target
        .parameters
        .insert("bit_length_unit".into(), "bits".into());
    target
        .parameters
        .insert("target_bit_order".into(), "msb_first".into());
    target.parameters.insert(
        "target_padding".into(),
        if length.is_multiple_of(8) {
            "none"
        } else {
            "zero"
        }
        .into(),
    );
    add_exact_with_target(
        capabilities,
        LogicalType::bit_string(length),
        target,
        format!("bit.{length}"),
    );
}

fn add_exact_with_target(
    capabilities: &mut Vec<CapabilityEntry>,
    source_logical_type: LogicalType,
    target: TargetRepresentation,
    code_suffix: String,
) {
    let code = format!("mysql80.exact.{code_suffix}");
    let rule_id = format!("mysql80.conversion.{code_suffix}");
    let evidence_digest = evidence_digest(&code, &source_logical_type, &target);
    let rule = ConversionRule {
        id: rule_id,
        version: RULE_VERSION.to_owned(),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest,
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn add_structured_json(capabilities: &mut Vec<CapabilityEntry>) {
    let code = "mysql80.exact.json".to_owned();
    let mut target = TargetRepresentation::new("json");
    target
        .parameters
        .insert("json_strategy".into(), "structured".into());
    let logical = LogicalType::json();
    let rule = ConversionRule {
        id: "mysql80.conversion.json".into(),
        version: "mysql-8.0.sink-conversion.v2".into(),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(&code, &logical, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn add_enum_set(capabilities: &mut Vec<CapabilityEntry>, is_set: bool) {
    let (logical, native, suffix, strategy) = if is_set {
        (
            LogicalType::Set {
                members: Vec::new(),
            },
            "set",
            "set",
            "set_members",
        )
    } else {
        (
            LogicalType::Enum {
                members: Vec::new(),
            },
            "enum",
            "enum",
            "enum_label",
        )
    };
    let code = format!("mysql80.exact.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("value_strategy".into(), strategy.into());
    let rule = ConversionRule {
        id: format!("mysql80.conversion.{suffix}"),
        version: "mysql-8.0.sink-conversion.v2".into(),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(&code, &logical, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical.clone(),
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
    if !is_set {
        let code = "mysql80.explicit.enum_by_label".to_owned();
        let mut target = TargetRepresentation::new("enum");
        target
            .parameters
            .insert("conversion_kind".into(), "enum".into());
        target
            .parameters
            .insert("value_strategy".into(), "enum_label".into());
        let logical = LogicalType::Enum {
            members: Vec::new(),
        };
        let rule = explicit_rule(&code, logical.clone(), target.clone(), Vec::new());
        capabilities.push(CapabilityEntry {
            code,
            source_logical_type: logical,
            target,
            supported_operations: operations(),
            supported_presence: presence(),
            rule,
        });
    }
}

fn add_explicit(
    capabilities: &mut Vec<CapabilityEntry>,
    source_logical_type: LogicalType,
    target_native_type: &str,
    code_suffix: String,
) {
    let code = format!("mysql80.explicit.{code_suffix}");
    let target = TargetRepresentation::new(target_native_type);
    let rule = ConversionRule {
        id: format!("mysql80.conversion.{code_suffix}"),
        version: RULE_VERSION.to_owned(),
        qualification: QualificationLevel::ExplicitConversion,
        risk: RiskLevel::High,
        risk_code: Some("mysql80.explicit_conversion".into()),
        requires_confirmation: true,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: false,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(&code, &source_logical_type, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn add_explicit_text(capabilities: &mut Vec<CapabilityEntry>, native: String) {
    let code = format!("mysql80.explicit.text.{native}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "text".into());
    let logical = LogicalType::Text {
        charset: "*".into(),
        max_length: None,
        length_unit: change_event::LengthUnit::Bytes,
        collation: None,
    };
    let rule = explicit_rule(&code, logical.clone(), target.clone(), text_options());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn add_explicit_json_text(capabilities: &mut Vec<CapabilityEntry>, native: String) {
    let code = format!("mysql80.explicit.json_text.{native}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "json".into());
    target
        .parameters
        .insert("json_strategy".into(), "normalized_text".into());
    let logical = LogicalType::json();
    let rule = explicit_rule(&code, logical.clone(), target.clone(), json_text_options());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn add_explicit_temporal(
    capabilities: &mut Vec<CapabilityEntry>,
    source: LogicalType,
    native: String,
    suffix: String,
) {
    let code = format!("mysql80.explicit.temporal.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "temporal".into());
    let rule = explicit_rule(&code, source.clone(), target.clone(), temporal_options());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: source,
        target,
        supported_operations: operations(),
        supported_presence: presence(),
        rule,
    });
}

fn explicit_rule(
    code: &str,
    logical: LogicalType,
    target: TargetRepresentation,
    options: Vec<OptionSpec>,
) -> ConversionRule {
    ConversionRule {
        id: format!("mysql80.conversion.{code}"),
        version: "mysql-8.0.sink-conversion.v2".into(),
        qualification: QualificationLevel::ExplicitConversion,
        risk: RiskLevel::High,
        risk_code: Some("mysql80.explicit_conversion".into()),
        requires_confirmation: true,
        options,
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: false,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(code, &logical, &target),
    }
}

fn text_options() -> Vec<OptionSpec> {
    vec![
        OptionSpec {
            name: "target_charset".into(),
            value_kind: OptionValueKind::String,
            required: true,
            default: None,
            allowed_values: vec![
                "utf8".into(),
                "utf8mb4".into(),
                "UTF8".into(),
                "ascii".into(),
                "latin1".into(),
            ],
        },
        OptionSpec {
            name: "target_length".into(),
            value_kind: OptionValueKind::String,
            required: true,
            default: None,
            allowed_values: Vec::new(),
        },
        OptionSpec {
            name: "target_length_unit".into(),
            value_kind: OptionValueKind::Enum,
            required: true,
            default: None,
            allowed_values: vec!["bytes".into(), "characters".into()],
        },
        OptionSpec {
            name: "target_collation".into(),
            value_kind: OptionValueKind::String,
            required: true,
            default: None,
            allowed_values: Vec::new(),
        },
        OptionSpec {
            name: "encoding_policy".into(),
            value_kind: OptionValueKind::Enum,
            required: false,
            default: Some("strict".into()),
            allowed_values: vec!["strict".into()],
        },
        OptionSpec {
            name: "length_policy".into(),
            value_kind: OptionValueKind::Enum,
            required: false,
            default: Some("reject".into()),
            allowed_values: vec!["reject".into()],
        },
        OptionSpec {
            name: "collation_policy".into(),
            value_kind: OptionValueKind::Enum,
            required: false,
            default: Some("target".into()),
            allowed_values: vec!["target".into()],
        },
    ]
}

fn json_text_options() -> Vec<OptionSpec> {
    let mut options = text_options();
    options.push(OptionSpec {
        name: "json_strategy".into(),
        value_kind: OptionValueKind::Enum,
        required: true,
        default: None,
        allowed_values: vec!["normalized_text".into(), "raw_text".into()],
    });
    options
}

fn temporal_options() -> Vec<OptionSpec> {
    vec![
        OptionSpec {
            name: "target_precision".into(),
            value_kind: OptionValueKind::Integer,
            required: true,
            default: None,
            allowed_values: Vec::new(),
        },
        OptionSpec {
            name: "temporal_strategy".into(),
            value_kind: OptionValueKind::Enum,
            required: true,
            default: None,
            allowed_values: vec![
                "preserve_local".into(),
                "preserve_absolute".into(),
                "preserve_duration".into(),
                "local_to_absolute".into(),
                "absolute_to_local".into(),
            ],
        },
        OptionSpec {
            name: "time_zone".into(),
            value_kind: OptionValueKind::String,
            required: false,
            default: None,
            allowed_values: Vec::new(),
        },
        OptionSpec {
            name: "precision_policy".into(),
            value_kind: OptionValueKind::Enum,
            required: false,
            default: Some("reject".into()),
            allowed_values: vec!["reject".into()],
        },
    ]
}

fn text_target_types() -> Vec<String> {
    text_lengths()
        .map(|length| format!("varchar({length})"))
        .chain([
            "tinytext".into(),
            "text".into(),
            "mediumtext".into(),
            "longtext".into(),
        ])
        .chain((1u64..=255).map(|length| format!("char({length})")))
        .collect()
}

fn datetime_type(precision: u8) -> String {
    if precision == 0 {
        "datetime".into()
    } else {
        format!("datetime({precision})")
    }
}

fn timestamp_type(precision: u8) -> String {
    if precision == 0 {
        "timestamp".into()
    } else {
        format!("timestamp({precision})")
    }
}

fn time_type(precision: u8) -> String {
    if precision == 0 {
        "time".into()
    } else {
        format!("time({precision})")
    }
}

fn add_range_checked_integer(
    capabilities: &mut Vec<CapabilityEntry>,
    target_signed: bool,
    target_bits: u8,
    native: String,
    suffix: String,
) {
    let code = format!("mysql80.range_checked.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("range_kind".into(), "integer".into());
    target
        .parameters
        .insert("target_signed".into(), target_signed.to_string());
    target
        .parameters
        .insert("target_bits".into(), target_bits.to_string());
    let rule = range_rule(&code, LogicalType::integer(true, 0), target.clone());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::integer(true, 0),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_range_checked_decimal(
    capabilities: &mut Vec<CapabilityEntry>,
    target_precision: u16,
    target_scale: u16,
) {
    let code = format!("mysql80.range_checked.decimal.{target_precision}.{target_scale}");
    let mut target =
        TargetRepresentation::new(format!("decimal({target_precision},{target_scale})"));
    target
        .parameters
        .insert("range_kind".into(), "decimal".into());
    target
        .parameters
        .insert("target_precision".into(), target_precision.to_string());
    target
        .parameters
        .insert("target_scale".into(), target_scale.to_string());
    let rule = range_rule(&code, LogicalType::decimal(0, 0), target.clone());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::decimal(0, 0),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_range_checked_float(capabilities: &mut Vec<CapabilityEntry>) {
    let code = "mysql80.range_checked.float.64_to_32".to_owned();
    let mut target = TargetRepresentation::new("float");
    target
        .parameters
        .insert("range_kind".into(), "float".into());
    target.parameters.insert("target_bits".into(), "32".into());
    let rule = range_rule(&code, LogicalType::float(0), target.clone());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::float(0),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn range_rule(code: &str, logical: LogicalType, target: TargetRepresentation) -> ConversionRule {
    ConversionRule {
        id: format!("mysql80.conversion.{code}"),
        version: RULE_VERSION.to_owned(),
        qualification: QualificationLevel::RangeChecked,
        risk: RiskLevel::Medium,
        risk_code: Some("mysql80.range_checked".into()),
        requires_confirmation: true,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: false,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(code, &logical, &target),
    }
}

fn operations() -> Vec<Operation> {
    vec![Operation::Insert, Operation::Update, Operation::Delete]
}

fn presence() -> Vec<PresenceState> {
    vec![
        PresenceState::Value,
        PresenceState::Null,
        PresenceState::Unchanged,
        PresenceState::Unavailable,
        PresenceState::GeneratedObservation,
    ]
}

fn text_lengths() -> impl Iterator<Item = u64> {
    (1u64..=255).chain([256, 512, 1024, 2048, 4096, 8192, 16_383, 32_767, 65_535])
}

fn binary_lengths() -> impl Iterator<Item = u64> {
    (1u64..=255).chain([256, 512, 1024, 2048, 4096, 8192, 16_383, 32_767, 65_535])
}

fn evidence_digest(
    code: &str,
    logical_type: &LogicalType,
    target: &TargetRepresentation,
) -> String {
    let bytes = serde_json::to_vec(&(RULE_VERSION, code, logical_type, target))
        .expect("MySQL capability evidence is serializable");
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build() -> ServerBuildIdentity {
        ServerBuildIdentity::new("mysql", "oracle", "8.0.36", "mysql-8.0.36")
    }

    #[test]
    fn manifest_is_versioned_valid_and_reproducible() {
        let first = compatibility_manifest(build());
        let second = compatibility_manifest(build());
        assert_eq!(first, second);
        assert!(first.verify_digest());
        first.validate().unwrap();
        assert!(
            first
                .capabilities
                .iter()
                .any(|entry| entry.target.native_type == "bigint unsigned")
        );
        assert!(
            first
                .capabilities
                .iter()
                .any(|entry| entry.target.native_type == "decimal(30,6)")
        );
    }

    #[test]
    fn exact_manifest_does_not_qualify_an_unlisted_text_bound() {
        let manifest = compatibility_manifest(build());
        assert!(!manifest.capabilities.iter().any(|entry| {
            entry.source_logical_type
                == LogicalType::Text {
                    charset: "utf8mb4".into(),
                    max_length: Some(257),
                    length_unit: change_event::LengthUnit::Characters,
                    collation: None,
                }
        }));
    }

    #[test]
    fn text_conversion_manifest_includes_precreated_char_targets() {
        let manifest = compatibility_manifest(build());
        assert!(manifest.capabilities.iter().any(|entry| {
            entry.target.native_type == "char(255)"
                && entry
                    .target
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("text")
        }));
    }
}
