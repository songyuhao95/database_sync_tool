//! PostgreSQL 15's version-owned Sink capability evidence.

use change_event::{
    CapabilityEntry, CompatibilityError, CompatibilityInput, CompatibilityResult, ConversionRule,
    FailurePolicy, LogicalType, Operation, OptionSpec, OptionValueKind, PresenceState,
    QualificationLevel, RiskLevel, ServerBuildIdentity, TargetCapabilityManifest,
    TargetRepresentation,
};
use sha2::{Digest as _, Sha256};

const CONNECTOR_VERSION: &str = "15";
const RULE_VERSION: &str = "postgresql-15.sink-conversion.v1";

/// Build a content-addressed manifest for one exact PostgreSQL 15 target
/// build. It contains only representations supported by the public DML
/// renderer, including the structured and extension-backed values it can
/// bind without string-splicing them into SQL.
pub fn compatibility_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    let mut capabilities = Vec::new();

    for (logical, native, suffix) in [
        (LogicalType::Boolean, "boolean", "boolean"),
        (LogicalType::Uuid, "uuid", "uuid"),
        (
            LogicalType::integer(true, 16),
            "smallint",
            "integer.16.signed",
        ),
        (
            LogicalType::integer(true, 32),
            "integer",
            "integer.32.signed",
        ),
        (
            LogicalType::integer(true, 64),
            "bigint",
            "integer.64.signed",
        ),
        (LogicalType::float(32), "real", "float.32"),
        (LogicalType::float(64), "double precision", "float.64"),
        (LogicalType::date(), "date", "date"),
        (LogicalType::year(), "smallint", "year"),
    ] {
        add_exact(&mut capabilities, logical, native, suffix);
    }
    add_structured_json(&mut capabilities);
    add_enum(&mut capabilities);
    add_structured_capabilities(&mut capabilities);
    for (bits, native) in [(16, "smallint"), (32, "integer"), (64, "bigint")] {
        add_range_checked_integer(&mut capabilities, bits, native);
    }

    // A wider signed PostgreSQL representation is selected only where it
    // contains the complete unsigned source domain.
    for (bits, native) in [
        (8, "smallint"),
        (16, "integer"),
        (24, "integer"),
        (32, "bigint"),
        (64, "numeric(20,0)"),
    ] {
        add_exact(
            &mut capabilities,
            LogicalType::integer(false, bits),
            native,
            format!("integer.{bits}.unsigned"),
        );
    }

    for precision in 1..=1000 {
        for scale in 0..=precision.min(30) {
            add_exact(
                &mut capabilities,
                LogicalType::decimal(precision, i32::from(scale)),
                format!("numeric({precision},{scale})"),
                format!("decimal.{precision}.{scale}"),
            );
        }
    }
    for precision in 1..=1000 {
        for scale in 0..=precision.min(30) {
            add_range_checked_decimal(&mut capabilities, precision, scale);
        }
    }

    add_range_checked_float(&mut capabilities);

    for charset in ["UTF8", "utf8", "utf8mb4"] {
        add_exact(
            &mut capabilities,
            LogicalType::Text {
                charset: charset.into(),
                max_length: None,
                length_unit: change_event::LengthUnit::Characters,
                collation: None,
            },
            "text",
            format!("text.{charset}.unbounded"),
        );
        for length in text_lengths() {
            add_exact(
                &mut capabilities,
                LogicalType::Text {
                    charset: charset.into(),
                    max_length: Some(length),
                    length_unit: change_event::LengthUnit::Characters,
                    collation: None,
                },
                format!("character varying({length})"),
                format!("text.{charset}.varchar.{length}"),
            );
        }
    }
    for charset in ["utf8", "utf8mb4"] {
        for length in [255, 65_535, 16_777_215, 4_294_967_295] {
            add_exact(
                &mut capabilities,
                LogicalType::Text {
                    charset: charset.into(),
                    max_length: Some(length),
                    length_unit: change_event::LengthUnit::Bytes,
                    collation: None,
                },
                "text",
                format!("text.{charset}.bytes.{length}"),
            );
        }
    }
    for native in text_target_types() {
        add_explicit_text(&mut capabilities, native.clone());
        add_explicit_json_text(&mut capabilities, native);
    }

    add_exact(
        &mut capabilities,
        LogicalType::binary(None),
        "bytea",
        "binary.unbounded",
    );
    for length in binary_lengths() {
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(length),
            },
            "bytea",
            format!("binary.{length}"),
        );
    }
    for length in [1_u64, 8, 12, 16, 32, 64, 128, 256, 512, 1_024] {
        add_exact_bit_string(&mut capabilities, length);
    }
    for precision in 0..=6 {
        add_exact(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: precision,
            },
            timestamp_type(precision, false),
            format!("local_datetime.{precision}"),
        );
        add_exact(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: precision,
            },
            timestamp_type(precision, true),
            format!("instant.{precision}"),
        );
        add_exact(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: precision,
            },
            "interval",
            format!("duration.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision, false),
            format!("local_datetime.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision, true),
            format!("instant.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: u8::MAX,
            },
            "interval".into(),
            format!("duration.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision, true),
            format!("local_datetime_to_instant.{precision}"),
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            timestamp_type(precision, false),
            format!("instant_to_local_datetime.{precision}"),
        );
    }

    TargetCapabilityManifest::new(
        change_event::ConnectorIdentity::new("postgresql", CONNECTOR_VERSION),
        target_build,
        capabilities,
        true,
    )
}

/// Build the same PostgreSQL Sink capability set for an exact supported
/// server major.  The conversion rules are versioned and content-addressed;
/// changing the connector identity, rule identity, or evidence version also
/// changes the manifest digest.
pub fn compatibility_manifest_for_version(
    target_build: ServerBuildIdentity,
    connector_version: &str,
) -> TargetCapabilityManifest {
    if connector_version == CONNECTOR_VERSION {
        return compatibility_manifest(target_build);
    }

    if !matches!(connector_version, "16" | "17") {
        return TargetCapabilityManifest::new(
            change_event::ConnectorIdentity::new("postgresql", connector_version),
            target_build,
            Vec::new(),
            true,
        );
    }

    let mut capabilities = compatibility_manifest(target_build.clone()).capabilities;
    let connector_prefix = format!("postgresql{CONNECTOR_VERSION}");
    let versioned_prefix = format!("postgresql{connector_version}");
    let rule_version = format!("postgresql-{connector_version}.sink-conversion.v1");
    for capability in &mut capabilities {
        capability.code = capability
            .code
            .replace(&connector_prefix, &versioned_prefix);
        capability.rule.id = capability
            .rule
            .id
            .replace(&connector_prefix, &versioned_prefix);
        capability.rule.version = capability
            .rule
            .version
            .replace("postgresql-15", &format!("postgresql-{connector_version}"));
        capability.rule.evidence_digest = evidence_digest_for(
            &rule_version,
            &capability.code,
            &capability.source_logical_type,
            &capability.target,
        );
    }
    TargetCapabilityManifest::new(
        change_event::ConnectorIdentity::new("postgresql", connector_version),
        target_build,
        capabilities,
        true,
    )
}

pub fn target_capability_manifest(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn structured_capability_manifest(
    target_build: ServerBuildIdentity,
) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn capability_manifest_for(target_build: ServerBuildIdentity) -> TargetCapabilityManifest {
    compatibility_manifest(target_build)
}

pub fn plan_compatibility(
    input: CompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    change_event::plan_compatibility(input)
}

impl crate::sql::SinkAdapter {
    pub fn structured_capability_manifest(
        &self,
        target_build: ServerBuildIdentity,
    ) -> TargetCapabilityManifest {
        compatibility_manifest(target_build)
    }

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
    target_native_type: impl Into<String>,
    code_suffix: impl Into<String>,
) {
    let code_suffix = code_suffix.into();
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
    let code = format!("postgresql15.exact.{code_suffix}");
    let rule = ConversionRule {
        id: format!("postgresql15.conversion.{code_suffix}"),
        version: RULE_VERSION.into(),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(&code, &source_logical_type, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type,
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_structured_json(capabilities: &mut Vec<CapabilityEntry>) {
    let code = "postgresql15.exact.jsonb".to_owned();
    let mut target = TargetRepresentation::new("jsonb");
    target
        .parameters
        .insert("json_strategy".into(), "structured".into());
    let logical = LogicalType::json();
    let rule = ConversionRule {
        id: "postgresql15.conversion.jsonb".into(),
        version: "postgresql-15.sink-conversion.v2".into(),
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

fn add_enum(capabilities: &mut Vec<CapabilityEntry>) {
    let logical = LogicalType::Enum {
        members: Vec::new(),
    };
    let code = "postgresql15.exact.enum".to_owned();
    let mut target = TargetRepresentation::new("enum");
    target
        .parameters
        .insert("value_strategy".into(), "enum_label".into());
    let rule = ConversionRule {
        id: "postgresql15.conversion.enum".into(),
        version: "postgresql-15.sink-conversion.v2".into(),
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

    let code = "postgresql15.explicit.enum_by_label".to_owned();
    let mut target = TargetRepresentation::new("enum");
    target
        .parameters
        .insert("conversion_kind".into(), "enum".into());
    target
        .parameters
        .insert("value_strategy".into(), "enum_label".into());
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

fn add_structured_capabilities(capabilities: &mut Vec<CapabilityEntry>) {
    add_exact(
        capabilities,
        LogicalType::Array {
            element: Box::new(LogicalType::integer(true, 32)),
        },
        "integer[]",
        "array.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::array_with_metadata(LogicalType::integer(true, 32), 1, vec![1]),
        "integer[]",
        "array_with_metadata.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::Struct {
            fields: vec![change_event::LogicalField {
                name: "value".into(),
                logical_type: LogicalType::integer(true, 32),
                nullable: false,
            }],
        },
        "cdc_composite",
        "composite.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::domain(
            "cdc_domain",
            LogicalType::integer(true, 32),
            Vec::new(),
            false,
            None,
            "catalog-bound",
        ),
        "cdc_domain",
        "domain.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::Range {
            element: Box::new(LogicalType::integer(true, 32)),
        },
        "int4range",
        "range.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::MultiRange {
            element: Box::new(LogicalType::integer(true, 32)),
        },
        "int4multirange",
        "multirange.integer32",
    );
    add_exact(
        capabilities,
        LogicalType::spatial("*", None, 0),
        "geometry",
        "spatial.geometry",
    );
    add_exact(
        capabilities,
        LogicalType::spatial("*", None, 0),
        "geography",
        "spatial.geography",
    );
    add_exact(
        capabilities,
        LogicalType::network("inet", false),
        "inet",
        "network.inet",
    );
    add_exact(capabilities, LogicalType::xml(), "xml", "xml");
    add_exact(
        capabilities,
        LogicalType::raw(
            "postgresql.user-defined.v1",
            "USER-DEFINED",
            "catalog-bound",
            "binary",
        ),
        "USER-DEFINED",
        "custom.raw",
    );

    let logical = LogicalType::Set {
        members: Vec::new(),
    };
    let mut target = TargetRepresentation::new("text[]");
    target
        .parameters
        .insert("set_strategy".into(), "ordered_text_array".into());
    let code = "postgresql15.explicit.set.text_array".to_owned();
    let rule = explicit_rule(&code, logical.clone(), target.clone(), Vec::new());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_explicit_text(capabilities: &mut Vec<CapabilityEntry>, native: String) {
    let code = format!("postgresql15.explicit.text.{native}");
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
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_explicit_json_text(capabilities: &mut Vec<CapabilityEntry>, native: String) {
    let code = format!("postgresql15.explicit.json_text.{native}");
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
    let code = format!("postgresql15.explicit.temporal.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "temporal".into());
    let rule = explicit_rule(&code, source.clone(), target.clone(), temporal_options());
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: source,
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn explicit_rule(
    code: &str,
    logical: LogicalType,
    target: TargetRepresentation,
    options: Vec<OptionSpec>,
) -> ConversionRule {
    ConversionRule {
        id: format!("postgresql15.conversion.{code}"),
        version: RULE_VERSION.into(),
        qualification: QualificationLevel::ExplicitConversion,
        risk: RiskLevel::High,
        risk_code: Some("postgresql15.explicit_conversion".into()),
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
        .map(|length| format!("character varying({length})"))
        .chain(["text".to_owned()])
        .collect()
}

fn add_range_checked_integer(
    capabilities: &mut Vec<CapabilityEntry>,
    target_bits: u8,
    native: &str,
) {
    let code = format!("postgresql15.range_checked.integer.{target_bits}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("range_kind".into(), "integer".into());
    target
        .parameters
        .insert("target_signed".into(), "true".into());
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
    let code = format!("postgresql15.range_checked.decimal.{target_precision}.{target_scale}");
    let mut target =
        TargetRepresentation::new(format!("numeric({target_precision},{target_scale})"));
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
    let code = "postgresql15.range_checked.float.64_to_32".to_owned();
    let mut target = TargetRepresentation::new("real");
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
        id: format!("postgresql15.conversion.{code}"),
        version: RULE_VERSION.into(),
        qualification: QualificationLevel::RangeChecked,
        risk: RiskLevel::Medium,
        risk_code: Some("postgresql15.range_checked".into()),
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

fn timestamp_type(precision: u8, with_time_zone: bool) -> String {
    let suffix = if precision == 0 {
        String::new()
    } else {
        format!("({precision})")
    };
    format!(
        "timestamp{suffix} {}",
        if with_time_zone {
            "with time zone"
        } else {
            "without time zone"
        }
    )
}

fn evidence_digest(
    code: &str,
    logical_type: &LogicalType,
    target: &TargetRepresentation,
) -> String {
    evidence_digest_for(RULE_VERSION, code, logical_type, target)
}

fn evidence_digest_for(
    rule_version: &str,
    code: &str,
    logical_type: &LogicalType,
    target: &TargetRepresentation,
) -> String {
    let bytes = serde_json::to_vec(&(rule_version, code, logical_type, target))
        .expect("PostgreSQL capability evidence is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build() -> ServerBuildIdentity {
        ServerBuildIdentity::new("postgresql", "community", "15.19", "postgres-15.19")
    }

    #[test]
    fn manifest_is_versioned_and_reproducible() {
        let first = compatibility_manifest(build());
        assert_eq!(first, compatibility_manifest(build()));
        assert!(first.verify_digest());
        first.validate().unwrap();
        assert!(first.capabilities.iter().any(|entry| {
            entry.source_logical_type == LogicalType::Boolean
                && entry.target.native_type == "boolean"
        }));
        assert!(first.capabilities.iter().any(|entry| {
            entry.source_logical_type == LogicalType::Uuid && entry.target.native_type == "uuid"
        }));
    }

    #[test]
    fn arrays_have_a_qualified_target_representation() {
        let manifest = compatibility_manifest(build());
        assert!(manifest.capabilities.iter().any(|entry| {
            matches!(entry.source_logical_type, LogicalType::Array { .. })
                && entry.target.native_type == "integer[]"
        }));
    }
}
