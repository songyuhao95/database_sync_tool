//! Shared MySQL Sink capability evidence.
//!
//! The versioned MySQL crates call this builder with their own connector and
//! rule identities.  The resulting manifest remains owned by the selected
//! Sink connector; sharing the construction code does not introduce a
//! source-to-sink compatibility table.

use change_event::{
    CapabilityEntry, ConversionRule, FailurePolicy, LogicalType, Operation, OptionSpec,
    OptionValueKind, PresenceState, QualificationLevel, RiskLevel, ServerBuildIdentity,
    TargetCapabilityManifest, TargetRepresentation,
};
use sha2::{Digest as _, Sha256};

pub fn capability_manifest(
    connector_version: &str,
    code_prefix: &str,
    target_build: ServerBuildIdentity,
) -> TargetCapabilityManifest {
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
                code_prefix,
                connector_version,
            );
            // MySQL commonly reports an integer display width (for example
            // `int(11) unsigned`). It does not change the numeric domain, so
            // the Sink manifest qualifies that catalog spelling explicitly.
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
                code_prefix,
                connector_version,
            );
            add_range_checked_integer(
                &mut capabilities,
                signed,
                bits,
                target.clone(),
                format!(
                    "integer.{bits}.{}.domain",
                    if signed { "signed" } else { "unsigned" }
                ),
                code_prefix,
                connector_version,
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
                code_prefix,
                connector_version,
            );
        }
    }
    for precision in 1..=65 {
        for scale in 0..=precision.min(30) {
            add_range_checked_decimal(
                &mut capabilities,
                precision,
                scale,
                code_prefix,
                connector_version,
            );
        }
    }

    for (bits, native) in [(32, "float"), (64, "double")] {
        add_exact(
            &mut capabilities,
            LogicalType::float(bits),
            native.to_owned(),
            format!("float.{bits}"),
            code_prefix,
            connector_version,
        );
    }
    add_range_checked_float(&mut capabilities, "float", code_prefix, connector_version);

    for charset in ["utf8mb4", "utf8", "UTF8"] {
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
                code_prefix,
                connector_version,
            );
        }
    }
    for (max_length, native) in [
        (255, "tinytext"),
        (65_535, "text"),
        (16_777_215, "mediumtext"),
        (4_294_967_295, "longtext"),
    ] {
        for charset in ["utf8mb4", "utf8"] {
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
                code_prefix,
                connector_version,
            );
        }
    }
    // PostgreSQL text is unbounded in characters.  LONGTEXT is the only
    // MySQL representation in this baseline whose byte bound is not smaller
    // than PostgreSQL's DML value limit.
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
        code_prefix,
        connector_version,
    );

    // Text conversion is explicit only for encodings with a qualified,
    // deterministic implementation.  Length and collation are bound to the
    // pre-created target column by the planner; the rule never truncates.
    for native in text_target_types() {
        add_explicit_text(
            &mut capabilities,
            native.clone(),
            code_prefix,
            connector_version,
        );
        add_explicit_json_text(&mut capabilities, native, code_prefix, connector_version);
    }
    for precision in 0..=6 {
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            format_datetime(precision),
            format!("local_datetime.{precision}"),
            code_prefix,
            connector_version,
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            format_timestamp(precision),
            format!("instant.{precision}"),
            code_prefix,
            connector_version,
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: u8::MAX,
            },
            format_time(precision),
            format!("duration.{precision}"),
            code_prefix,
            connector_version,
        );
        // A fixed offset is required when changing between local and absolute
        // time.  These candidates intentionally cannot be used by keys.
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            format_timestamp(precision),
            format!("local_datetime_to_instant.{precision}"),
            code_prefix,
            connector_version,
        );
        add_explicit_temporal(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            format_datetime(precision),
            format!("instant_to_local_datetime.{precision}"),
            code_prefix,
            connector_version,
        );
    }
    for length in 1..=64 {
        add_exact_bit_string(&mut capabilities, length, code_prefix, connector_version);
    }

    for max_length in binary_lengths() {
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(max_length),
            },
            format!("varbinary({max_length})"),
            format!("binary.varbinary.{max_length}"),
            code_prefix,
            connector_version,
        );
        add_exact(
            &mut capabilities,
            LogicalType::Binary {
                max_length: Some(max_length),
            },
            format!("binary({max_length})"),
            format!("binary.fixed.{max_length}"),
            code_prefix,
            connector_version,
        );
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
            code_prefix,
            connector_version,
        );
    }

    add_exact(
        &mut capabilities,
        LogicalType::date(),
        "date".into(),
        "date".into(),
        code_prefix,
        connector_version,
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
            code_prefix,
            connector_version,
        );
        add_exact(
            &mut capabilities,
            LogicalType::Instant {
                fractional_precision: precision,
            },
            format!("timestamp{suffix}"),
            format!("instant.{precision}"),
            code_prefix,
            connector_version,
        );
        add_exact(
            &mut capabilities,
            LogicalType::Duration {
                fractional_precision: precision,
            },
            format!("time{suffix}"),
            format!("duration.{precision}"),
            code_prefix,
            connector_version,
        );
    }
    add_exact(
        &mut capabilities,
        LogicalType::year(),
        "year".into(),
        "year".into(),
        code_prefix,
        connector_version,
    );
    add_structured_json(
        &mut capabilities,
        "json".into(),
        "json".into(),
        code_prefix,
        connector_version,
    );
    add_enum_set(&mut capabilities, false, code_prefix, connector_version);
    add_enum_set(&mut capabilities, true, code_prefix, connector_version);

    // These are deliberate, per-field conversions.  They are not usable for
    // primary keys or Row Locators and therefore cannot weaken key safety.
    for native in ["tinyint", "tinyint(1)"] {
        add_explicit(
            &mut capabilities,
            LogicalType::Boolean,
            native,
            format!("boolean.{native}"),
            code_prefix,
            connector_version,
        );
    }
    for native in ["char(36)", "varchar(36)"] {
        add_explicit(
            &mut capabilities,
            LogicalType::Uuid,
            native,
            format!("uuid.{native}"),
            code_prefix,
            connector_version,
        );
    }

    TargetCapabilityManifest::new(
        change_event::ConnectorIdentity::new("mysql", connector_version),
        target_build,
        capabilities,
        true,
    )
}

fn add_exact(
    capabilities: &mut Vec<CapabilityEntry>,
    logical: LogicalType,
    native: String,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
) {
    add_rule(
        capabilities,
        logical,
        native,
        suffix,
        code_prefix,
        connector_version,
        QualificationLevel::Exact,
        RiskLevel::None,
        false,
        true,
    );
}

fn add_exact_bit_string(
    capabilities: &mut Vec<CapabilityEntry>,
    length: u64,
    code_prefix: &str,
    connector_version: &str,
) {
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
    add_rule_with_target(
        capabilities,
        LogicalType::bit_string(length),
        target,
        format!("bit.{length}"),
        code_prefix,
        connector_version,
        QualificationLevel::Exact,
        RiskLevel::None,
        false,
        true,
    );
}

fn add_explicit(
    capabilities: &mut Vec<CapabilityEntry>,
    logical: LogicalType,
    native: &str,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
) {
    add_rule(
        capabilities,
        logical,
        native.to_owned(),
        suffix,
        code_prefix,
        connector_version,
        QualificationLevel::ExplicitConversion,
        RiskLevel::High,
        true,
        false,
    );
}

fn add_structured_json(
    capabilities: &mut Vec<CapabilityEntry>,
    native: String,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.exact.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("json_strategy".into(), "structured".into());
    let logical = LogicalType::json();
    let rule = ConversionRule {
        id: format!("{code_prefix}.conversion.{suffix}"),
        version: format!("mysql-{connector_version}.sink-conversion.v2"),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(connector_version, &code, &logical, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_enum_set(
    capabilities: &mut Vec<CapabilityEntry>,
    is_set: bool,
    code_prefix: &str,
    connector_version: &str,
) {
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
    let code = format!("{code_prefix}.exact.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("value_strategy".into(), strategy.into());
    let rule = ConversionRule {
        id: format!("{code_prefix}.conversion.{suffix}"),
        version: format!("mysql-{connector_version}.sink-conversion.v2"),
        qualification: QualificationLevel::Exact,
        risk: RiskLevel::None,
        risk_code: None,
        requires_confirmation: false,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: true,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(connector_version, &code, &logical, &target),
    };
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical.clone(),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
    if !is_set {
        let code = format!("{code_prefix}.explicit.enum_by_label");
        let mut target = TargetRepresentation::new("enum");
        target
            .parameters
            .insert("conversion_kind".into(), "enum".into());
        target
            .parameters
            .insert("value_strategy".into(), "enum_label".into());
        let rule = explicit_rule(
            &code,
            logical,
            target.clone(),
            Vec::new(),
            code_prefix,
            connector_version,
        );
        capabilities.push(CapabilityEntry {
            code,
            source_logical_type: LogicalType::Enum {
                members: Vec::new(),
            },
            target,
            rule,
            supported_operations: operations(),
            supported_presence: presence(),
        });
    }
}

fn add_explicit_text(
    capabilities: &mut Vec<CapabilityEntry>,
    native: String,
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.explicit.text.{native}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "text".into());
    let rule = explicit_rule(
        &code,
        LogicalType::Text {
            charset: "*".into(),
            max_length: None,
            length_unit: change_event::LengthUnit::Bytes,
            collation: None,
        },
        target.clone(),
        text_options(),
        code_prefix,
        connector_version,
    );
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::Text {
            charset: "*".into(),
            max_length: None,
            length_unit: change_event::LengthUnit::Bytes,
            collation: None,
        },
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_explicit_json_text(
    capabilities: &mut Vec<CapabilityEntry>,
    native: String,
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.explicit.json_text.{native}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "json".into());
    target
        .parameters
        .insert("json_strategy".into(), "normalized_text".into());
    let logical = LogicalType::json();
    let rule = explicit_rule(
        &code,
        logical.clone(),
        target.clone(),
        json_text_options(),
        code_prefix,
        connector_version,
    );
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: logical,
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_explicit_temporal(
    capabilities: &mut Vec<CapabilityEntry>,
    source: LogicalType,
    native: String,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.explicit.temporal.{suffix}");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("conversion_kind".into(), "temporal".into());
    let rule = explicit_rule(
        &code,
        source.clone(),
        target.clone(),
        temporal_options(),
        code_prefix,
        connector_version,
    );
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
    code_prefix: &str,
    connector_version: &str,
) -> ConversionRule {
    ConversionRule {
        id: format!("{code_prefix}.conversion.{code}"),
        version: format!("mysql-{connector_version}.sink-conversion.v2"),
        qualification: QualificationLevel::ExplicitConversion,
        risk: RiskLevel::High,
        risk_code: Some(format!("{code_prefix}.explicit_conversion")),
        requires_confirmation: true,
        options,
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: false,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(connector_version, code, &logical, &target),
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
        // raw_text is intentionally not a qualified option.  The planner
        // reports it as unavailable because the event contract has no source
        // spelling to replay.
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
            "tinytext".to_owned(),
            "text".to_owned(),
            "mediumtext".to_owned(),
            "longtext".to_owned(),
        ])
        .collect()
}

fn format_datetime(precision: u8) -> String {
    if precision == 0 {
        "datetime".into()
    } else {
        format!("datetime({precision})")
    }
}

fn format_timestamp(precision: u8) -> String {
    if precision == 0 {
        "timestamp".into()
    } else {
        format!("timestamp({precision})")
    }
}

fn format_time(precision: u8) -> String {
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
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.range_checked.{suffix}");
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
    let rule = range_rule(
        &code,
        LogicalType::integer(true, 0),
        target.clone(),
        code_prefix,
        connector_version,
    );
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
    code_prefix: &str,
    connector_version: &str,
) {
    let suffix = format!("decimal.{target_precision}.{target_scale}");
    let code = format!("{code_prefix}.range_checked.{suffix}");
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
    let rule = range_rule(
        &code,
        LogicalType::decimal(0, 0),
        target.clone(),
        code_prefix,
        connector_version,
    );
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::decimal(0, 0),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn add_range_checked_float(
    capabilities: &mut Vec<CapabilityEntry>,
    native: &str,
    code_prefix: &str,
    connector_version: &str,
) {
    let code = format!("{code_prefix}.range_checked.float.64_to_32");
    let mut target = TargetRepresentation::new(native);
    target
        .parameters
        .insert("range_kind".into(), "float".into());
    target.parameters.insert("target_bits".into(), "32".into());
    let rule = range_rule(
        &code,
        LogicalType::float(0),
        target.clone(),
        code_prefix,
        connector_version,
    );
    capabilities.push(CapabilityEntry {
        code,
        source_logical_type: LogicalType::float(0),
        target,
        rule,
        supported_operations: operations(),
        supported_presence: presence(),
    });
}

fn range_rule(
    code: &str,
    logical: LogicalType,
    target: TargetRepresentation,
    code_prefix: &str,
    connector_version: &str,
) -> ConversionRule {
    ConversionRule {
        id: format!("{code_prefix}.conversion.range_checked.{}", code),
        version: format!("mysql-{connector_version}.sink-conversion.v1"),
        qualification: QualificationLevel::RangeChecked,
        risk: RiskLevel::Medium,
        risk_code: Some(format!("{code_prefix}.range_checked")),
        requires_confirmation: true,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key: false,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(connector_version, code, &logical, &target),
    }
}

#[allow(clippy::too_many_arguments)]
fn add_rule(
    capabilities: &mut Vec<CapabilityEntry>,
    logical: LogicalType,
    native: String,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
    qualification: QualificationLevel,
    risk: RiskLevel,
    requires_confirmation: bool,
    allows_key: bool,
) {
    add_rule_with_target(
        capabilities,
        logical,
        TargetRepresentation::new(native),
        suffix,
        code_prefix,
        connector_version,
        qualification,
        risk,
        requires_confirmation,
        allows_key,
    );
}

#[allow(clippy::too_many_arguments)]
fn add_rule_with_target(
    capabilities: &mut Vec<CapabilityEntry>,
    logical: LogicalType,
    target: TargetRepresentation,
    suffix: String,
    code_prefix: &str,
    connector_version: &str,
    qualification: QualificationLevel,
    risk: RiskLevel,
    requires_confirmation: bool,
    allows_key: bool,
) {
    let qualification_code = qualification_code(qualification);
    let code = format!("{code_prefix}.{qualification_code}.{suffix}");
    let rule = ConversionRule {
        id: format!("{code_prefix}.conversion.{suffix}"),
        version: format!("mysql-{connector_version}.sink-conversion.v1"),
        qualification,
        risk,
        risk_code: (qualification == QualificationLevel::ExplicitConversion)
            .then(|| format!("{code_prefix}.explicit_conversion")),
        requires_confirmation,
        options: Vec::new(),
        supported_operations: operations(),
        supported_presence: presence(),
        allows_key,
        failure_policy: FailurePolicy::Reject,
        evidence_digest: evidence_digest(connector_version, &code, &logical, &target),
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

fn qualification_code(level: QualificationLevel) -> &'static str {
    match level {
        QualificationLevel::Exact => "exact",
        QualificationLevel::RangeChecked => "range_checked",
        QualificationLevel::ExplicitConversion => "explicit",
        QualificationLevel::Unsupported => "unsupported",
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
    connector_version: &str,
    code: &str,
    logical: &LogicalType,
    target: &TargetRepresentation,
) -> String {
    let bytes = serde_json::to_vec(&(connector_version, code, logical, target))
        .expect("MySQL capability evidence is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
