use crate::{
    BitOrder, BitPadding, ChangeTransaction, ColumnDatum, Datum, FailureClass, FailurePhase,
    JsonValue, LogicalValue, Operation, RetryClassification, SourceCursor, SpatialFormat,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEventValidationError(pub(crate) String);

impl ChangeEventValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub const fn code(&self) -> &'static str {
        "change_event.validation_failed"
    }

    pub const fn class(&self) -> FailureClass {
        FailureClass::ChangeEventValidation
    }
}

/// Backwards-compatible name for generic ChangeEvent validation failures.
pub type ValidationError = ChangeEventValidationError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceContractError(pub(crate) String);

impl SourceContractError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub const fn code(&self) -> &'static str {
        "source_contract.invalid"
    }

    pub const fn class(&self) -> FailureClass {
        FailureClass::SourceContract
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetCapabilityFailure {
    pub class: FailureClass,
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<crate::ConnectorIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<crate::ConnectorIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_transaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table_lineage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_field_lineage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_field_lineage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_definition_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_definition_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_schema_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_server_build: Option<crate::ServerBuildIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversion_rule: Option<crate::RuleReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualification: Option<crate::QualificationLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_code: Option<String>,
    pub phase: FailurePhase,
    pub retry: RetryClassification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor_code: Option<String>,
    /// Sanitized cause text. Row values, bound parameters, SQL and secrets do
    /// not belong in this diagnostic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

impl TargetCapabilityFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            class: FailureClass::TargetCapability,
            code: "target_capability.unspecified".to_owned(),
            route_id: None,
            source: None,
            sink: None,
            source_transaction_id: None,
            event_identity: None,
            source_table_lineage: None,
            source_field_lineage: None,
            target_field_lineage: None,
            source_definition_fingerprint: None,
            target_definition_fingerprint: None,
            target_schema_fingerprint: None,
            target_server_build: None,
            capability_identity: None,
            conversion_rule: None,
            plan_digest: None,
            qualification: None,
            risk_code: None,
            phase: FailurePhase::CapabilityQualification,
            retry: RetryClassification::NotRetryable,
            vendor_code: None,
            cause: Some(sanitize_cause(message.into())),
        }
    }

    pub(crate) fn manifest(message: impl Into<String>) -> Self {
        let mut failure = Self::new(message);
        failure.code = "target_capability.invalid_manifest".to_owned();
        failure
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = code.into();
        self
    }

    pub fn with_route(mut self, route_id: impl Into<String>) -> Self {
        self.route_id = Some(route_id.into());
        self
    }

    pub fn with_plan_digest(mut self, digest: impl Into<String>) -> Self {
        self.plan_digest = Some(digest.into());
        self
    }

    pub fn stable_code(&self) -> &str {
        &self.code
    }

    pub fn class(&self) -> FailureClass {
        self.class
    }

    pub fn phase(&self) -> FailurePhase {
        self.phase
    }

    pub fn retry_classification(&self) -> RetryClassification {
        self.retry
    }

    pub fn is_retryable(&self) -> bool {
        self.retry == RetryClassification::Retryable
    }
}

impl fmt::Display for ChangeEventValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ChangeEventValidationError {}

impl fmt::Display for SourceContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SourceContractError {}

impl fmt::Display for TargetCapabilityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Target Capability Failure [{}]", self.code)?;
        if let Some(cause) = &self.cause {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}
impl std::error::Error for TargetCapabilityFailure {}

fn sanitize_cause(message: String) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect()
}

/// Only validate() can construct this type. Access is read-only after validation.
///
/// Invalid transactions cannot be passed directly to database SQL renderers:
/// ```compile_fail
/// let raw: change_event::ChangeTransaction = todo!();
/// let validated = change_event::ValidatedTransaction(raw);
/// ```
#[derive(Debug, Clone)]
pub struct ValidatedTransaction(ChangeTransaction);
impl ValidatedTransaction {
    pub fn transaction(&self) -> &ChangeTransaction {
        &self.0
    }

    pub fn content_digest(&self) -> String {
        self.0.content_digest()
    }
}

pub(crate) type Result<T> = std::result::Result<T, ValidationError>;
pub(crate) fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(ChangeEventValidationError(message.into()))
    }
}

/// Validate one LogicalValue outside a transaction. Source adapters can use
/// this seam before assembling a ChangeEvent, while `validate` remains the
/// transaction-level gate.
pub fn validate_value(value: &LogicalValue) -> std::result::Result<(), ChangeEventValidationError> {
    logical_value(value)
}

/// Validate the database-independent row-only v0.3 contract.
///
/// Source versions, identities, cursor encodings, transaction identifiers, and native
/// row-image rules belong to a Source Contract and are deliberately not checked here.
pub fn validate(transaction: ChangeTransaction) -> Result<ValidatedTransaction> {
    ensure(!transaction.id.trim().is_empty(), "transaction id is empty")?;
    ensure(
        !transaction.source.kind.is_empty()
            && !transaction.source.version.is_empty()
            && !transaction.source.id.is_empty(),
        "source identity is incomplete",
    )?;
    cursor(&transaction.begin_cursor)?;
    cursor(&transaction.commit_cursor)?;
    ensure(
        !transaction.changes.is_empty(),
        "transaction has no changes",
    )?;
    for change in &transaction.changes {
        ensure(
            !change.schema.is_empty()
                && !change.table.is_empty()
                && !change.schema.contains('\0')
                && !change.table.contains('\0'),
            "invalid table identifier",
        )?;
        ensure(!change.schema_basis.is_empty(), "schema basis is missing")?;
        cursor(&change.source_cursor)?;
        let valid_shape = match change.operation {
            Operation::Insert => change.before.is_none() && change.after.is_some(),
            Operation::Update => change.before.is_some() && change.after.is_some(),
            Operation::Delete => change.before.is_some() && change.after.is_none(),
        };
        ensure(
            valid_shape,
            "operation has inconsistent before/after images",
        )?;
        if let Some(row) = &change.before {
            image(row, change.operation, true)?;
        }
        if let Some(row) = &change.after {
            image(row, change.operation, false)?;
        }
        if let (Some(before), Some(after)) = (&change.before, &change.after) {
            ensure(
                before.len() == after.len()
                    && before.iter().zip(after).all(|(b, a)| {
                        b.ordinal == a.ordinal
                            && b.name == a.name
                            && b.native_type == a.native_type
                            && b.primary_key_ordinal == a.primary_key_ordinal
                            && b.generated == a.generated
                            && b.collation == a.collation
                    }),
                "UPDATE before/after column definitions differ",
            )?;
        }
    }
    Ok(ValidatedTransaction(transaction))
}

fn cursor(value: &SourceCursor) -> Result<()> {
    ensure(
        !value.format.is_empty() && !value.value.is_empty(),
        "source cursor is incomplete",
    )
}
pub(crate) fn image(row: &[ColumnDatum], operation: Operation, is_before: bool) -> Result<()> {
    ensure(!row.is_empty(), "row image is empty")?;
    let mut names = HashSet::new();
    let mut ordinals = HashSet::new();
    let mut key_ordinals = HashSet::new();
    for column in row {
        ensure(
            !column.name.is_empty() && !column.name.contains('\0'),
            "invalid column identifier",
        )?;
        ensure(!column.native_type.is_empty(), "native type is empty")?;
        ensure(
            names.insert(&column.name) && ordinals.insert(column.ordinal),
            "duplicate column in image",
        )?;
        if let Some(key_ordinal) = column.primary_key_ordinal {
            ensure(
                key_ordinals.insert(key_ordinal),
                "duplicate primary key ordinal",
            )?;
            ensure(
                matches!(column.datum, Datum::Value(_)),
                "primary key value is absent, unchanged or NULL",
            )?;
            ensure(!column.generated, "primary key column is generated")?;
        }
        match column.datum {
            Datum::Unavailable => ensure(
                is_before && !matches!(operation, Operation::Insert),
                "unavailable value is only valid in a before image",
            )?,
            Datum::Unchanged => ensure(
                !is_before && matches!(operation, Operation::Update),
                "unchanged value is only valid in an UPDATE after image",
            )?,
            _ => {}
        }
        if let Datum::Value(v) = &column.datum {
            logical_value(v)?;
        }
    }
    ensure(
        (0..key_ordinals.len()).all(|ordinal| key_ordinals.contains(&ordinal)),
        "primary key ordinals are not contiguous",
    )?;
    Ok(())
}
fn integer(value: &str, signed: bool, bits: u8) -> Result<()> {
    ensure(
        matches!(bits, 8 | 16 | 24 | 32 | 64),
        "invalid integer width",
    )?;
    let digits = if signed {
        value.strip_prefix('-').unwrap_or(value)
    } else {
        value
    };
    ensure(
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
        "invalid integer syntax",
    )?;
    let valid = if signed {
        value
            .parse::<i128>()
            .is_ok_and(|v| v >= -(1_i128 << (bits - 1)) && v < (1_i128 << (bits - 1)))
    } else {
        value.parse::<u128>().is_ok_and(|v| v < (1_u128 << bits))
    };
    ensure(valid, "integer outside declared range")
}
fn float(bits: u8, value: &str) -> Result<()> {
    ensure(
        matches!(bits, 32 | 64)
            && value.len() == usize::from(bits / 4)
            && value.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid floating-point bit pattern",
    )
}
fn bytes(value: &str) -> Result<()> {
    ensure(
        URL_SAFE_NO_PAD.decode(value).is_ok(),
        "invalid base64url bytes",
    )
}
fn date(year: u16, month: u8, day: u8) -> Result<()> {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => 0,
    };
    ensure(
        (1..=9999).contains(&year) && day > 0 && day <= days,
        "invalid calendar date",
    )
}
fn logical_value(value: &LogicalValue) -> Result<()> {
    logical_value_at_depth(value, 0)
}

fn logical_value_at_depth(value: &LogicalValue, depth: usize) -> Result<()> {
    ensure(depth <= 100, "logical value nesting exceeds 100")?;
    match value {
        LogicalValue::Null | LogicalValue::Boolean { .. } => Ok(()),
        LogicalValue::Uuid { value } => ensure(
            value.len() == 36
                && value.bytes().enumerate().all(|(i, c)| {
                    if [8, 13, 18, 23].contains(&i) {
                        c == b'-'
                    } else {
                        c.is_ascii_hexdigit()
                    }
                }),
            "invalid UUID value",
        ),
        LogicalValue::Integer {
            signed,
            bits,
            value,
        } => integer(value, *signed, *bits),
        LogicalValue::Decimal { unscaled, scale } => {
            let digits = unscaled.strip_prefix('-').unwrap_or(unscaled);
            ensure(
                !digits.is_empty()
                    && digits.len() <= 1000
                    && *scale <= 1000
                    && digits.bytes().all(|b| b.is_ascii_digit()),
                "invalid or oversized decimal",
            )
        }
        LogicalValue::Float { bits, ieee754_hex } => float(*bits, ieee754_hex),
        LogicalValue::Text {
            charset,
            bytes_base64url,
            ..
        } => {
            ensure(
                !charset.is_empty()
                    && charset
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid text charset",
            )?;
            bytes(bytes_base64url)
        }
        LogicalValue::Binary { bytes_base64url }
        | LogicalValue::Xml {
            bytes_base64url, ..
        } => bytes(bytes_base64url),
        LogicalValue::BitString {
            bytes_base64url,
            bit_length,
            padding,
            bit_order,
        } => bit_string(bytes_base64url, *bit_length, *padding, *bit_order),
        LogicalValue::Date { year, month, day } => date(*year, *month, *day),
        LogicalValue::LocalTime {
            hour,
            minute,
            second,
            microsecond,
        } => ensure(
            *hour < 24 && *minute < 60 && *second < 60 && *microsecond < 1_000_000,
            "invalid local time",
        ),
        LogicalValue::LocalDatetime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => {
            date(*year, *month, *day)?;
            ensure(
                *hour < 24 && *minute < 60 && *second < 60 && *microsecond < 1_000_000,
                "invalid datetime",
            )
        }
        LogicalValue::Duration {
            minutes,
            seconds,
            microsecond,
            ..
        } => ensure(
            *minutes < 60 && *seconds < 60 && *microsecond < 1_000_000,
            "invalid duration",
        ),
        LogicalValue::Instant {
            unix_seconds,
            nanoseconds,
        } => {
            integer(unix_seconds, true, 64)?;
            ensure(*nanoseconds < 1_000_000_000, "invalid instant fraction")
        }
        LogicalValue::Year { value } => ensure(*value <= 9999, "invalid year"),
        LogicalValue::Enum { label } => ensure(
            !label.is_empty() && !label.contains('\0'),
            "invalid ENUM label",
        ),
        LogicalValue::Set { members } => {
            let mut seen = HashSet::new();
            for member in members {
                ensure(!member.contains('\0'), "invalid SET member")?;
                ensure(seen.insert(member), "duplicate SET member")?;
            }
            Ok(())
        }
        LogicalValue::Spatial {
            format,
            bytes_base64url,
            geometry_type,
            dimensions,
            srid,
            crs,
        } => spatial(
            *format,
            bytes_base64url,
            geometry_type,
            *dimensions,
            *srid,
            crs.as_deref(),
        ),
        LogicalValue::Array { elements } => {
            for element in elements {
                logical_value_at_depth(element, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Struct { fields } => {
            let mut names = HashSet::new();
            for field in fields {
                ensure(
                    !field.name.is_empty() && !field.name.contains('\0'),
                    "invalid structured field name",
                )?;
                ensure(names.insert(&field.name), "duplicate structured field name")?;
                logical_value_at_depth(&field.value, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Map { entries } => {
            let mut keys = HashSet::new();
            for entry in entries {
                ensure(keys.insert(&entry.key), "duplicate map key")?;
                logical_value_at_depth(&entry.key, depth + 1)?;
                logical_value_at_depth(&entry.value, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Range {
            empty,
            lower,
            upper,
            ..
        } => {
            if *empty {
                ensure(lower.is_none() && upper.is_none(), "empty range has bounds")?;
            }
            if let Some(value) = lower {
                logical_value_at_depth(value, depth + 1)?;
            }
            if let Some(value) = upper {
                logical_value_at_depth(value, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::MultiRange { ranges } => {
            for range in ranges {
                ensure(
                    matches!(range, LogicalValue::Range { .. }),
                    "multirange contains a non-range value",
                )?;
                logical_value_at_depth(range, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::ArrayWithMetadata {
            elements,
            dimensions,
            lower_bounds,
        } => {
            ensure(
                *dimensions > 0 && usize::from(*dimensions) == lower_bounds.len(),
                "array dimensions and lower bounds do not agree",
            )?;
            for element in elements {
                logical_value_at_depth(element, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::InvalidTemporal { kind, raw } => ensure(
            !kind.trim().is_empty() && !raw.is_empty(),
            "invalid temporal value lacks kind or raw spelling",
        ),
        LogicalValue::Network {
            family,
            address,
            prefix_length,
        } => {
            ensure(
                !family.trim().is_empty() && !address.trim().is_empty() && !address.contains('\0'),
                "network value is incomplete",
            )?;
            ensure(
                prefix_length.is_none_or(|prefix| prefix <= 128),
                "network prefix is invalid",
            )
        }
        LogicalValue::Domain { value } => logical_value_at_depth(value, depth + 1),
        LogicalValue::Raw { carrier } => carrier
            .validate()
            .map_err(|error| ChangeEventValidationError(format!("{}: {error}", error.code()))),
        LogicalValue::Json { value } => json_value(value, 0),
    }
}

fn bit_string(
    encoded: &str,
    bit_length: u64,
    padding: BitPadding,
    bit_order: BitOrder,
) -> Result<()> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ChangeEventValidationError("invalid bit string bytes".into()))?;
    ensure(bit_length > 0, "bit string length must be positive")?;
    let expected_bytes = bit_length.div_ceil(8) as usize;
    ensure(
        bytes.len() == expected_bytes,
        "bit string byte length does not match bit length",
    )?;
    let unused = (8 - (bit_length % 8)) % 8;
    if unused == 0 {
        ensure(
            padding == BitPadding::None,
            "a byte-aligned bit string cannot declare padding",
        )?;
        return Ok(());
    }
    ensure(
        padding != BitPadding::None,
        "non-byte-aligned bit string needs padding",
    )?;
    let mask = match bit_order {
        BitOrder::MsbFirst => (1_u8 << unused) - 1,
        BitOrder::LsbFirst => u8::MAX << (8 - unused),
    };
    let actual = bytes[bytes.len() - 1] & mask;
    let expected = if padding == BitPadding::One { mask } else { 0 };
    ensure(actual == expected, "bit string padding bits are invalid")
}

fn spatial(
    format: SpatialFormat,
    encoded: &str,
    expected_geometry_type: &str,
    expected_dimensions: u8,
    expected_srid: Option<i32>,
    crs: Option<&str>,
) -> Result<()> {
    ensure(
        (2..=4).contains(&expected_dimensions),
        "spatial dimensions must be 2, 3, or 4",
    )?;
    ensure(
        !expected_geometry_type.is_empty() && !expected_geometry_type.contains('\0'),
        "spatial geometry type is empty",
    )?;
    if let Some(crs) = crs {
        ensure(
            !crs.is_empty() && !crs.contains('\0'),
            "invalid spatial CRS",
        )?;
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ChangeEventValidationError("invalid spatial bytes".into()))?;
    ensure(bytes.len() >= 5, "spatial value has no complete WKB header")?;
    let little_endian = match bytes[0] {
        0 => false,
        1 => true,
        _ => {
            return Err(ChangeEventValidationError(
                "invalid spatial byte order".into(),
            ));
        }
    };
    let geometry_code = read_u32(&bytes[1..5], little_endian)?;
    let has_srid = geometry_code & 0x2000_0000 != 0;
    let has_z = geometry_code & 0x8000_0000 != 0;
    let has_m = geometry_code & 0x4000_0000 != 0;
    let mut base_code = geometry_code & 0x0fff_ffff;
    let mut dimensions = 2 + u8::from(has_z) + u8::from(has_m);
    if base_code >= 1_000 {
        let suffix = base_code / 1_000;
        base_code %= 1_000;
        dimensions = match suffix {
            1 => 3,
            2 => 3,
            3 => 4,
            _ => 0,
        };
    }
    let geometry_type = match base_code {
        1 => "point",
        2 => "linestring",
        3 => "polygon",
        4 => "multipoint",
        5 => "multilinestring",
        6 => "multipolygon",
        7 => "geometrycollection",
        _ => "unknown",
    };
    ensure(geometry_type != "unknown", "unknown spatial geometry type")?;
    ensure(
        geometry_type.eq_ignore_ascii_case(expected_geometry_type),
        "spatial geometry type does not match its declaration",
    )?;
    ensure(
        dimensions == expected_dimensions,
        "spatial dimensions do not match its declaration",
    )?;
    let embedded_srid = if has_srid {
        ensure(bytes.len() >= 9, "EWKB SRID header is truncated")?;
        Some(read_u32(&bytes[5..9], little_endian)? as i32)
    } else {
        None
    };
    if format == SpatialFormat::Ewkb {
        ensure(has_srid, "EWKB spatial value is missing its SRID")?;
    } else {
        ensure(
            !has_srid,
            "WKB spatial value unexpectedly contains EWKB SRID",
        )?;
    }
    ensure(
        embedded_srid == expected_srid,
        "spatial SRID does not match its declaration",
    )
}

fn read_u32(bytes: &[u8], little_endian: bool) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(|| ChangeEventValidationError("truncated spatial header".into()))
        .and_then(|bytes| {
            bytes
                .try_into()
                .map_err(|_| ChangeEventValidationError("truncated spatial header".into()))
        })?;
    Ok(if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}
fn json_value(value: &JsonValue, depth: usize) -> Result<()> {
    ensure(depth <= 100, "JSON nesting exceeds 100")?;
    match value {
        JsonValue::SignedInteger(v) => integer(v, true, 64),
        JsonValue::UnsignedInteger(v) => integer(v, false, 64),
        JsonValue::DoubleBits(v) => float(64, v),
        JsonValue::Decimal { unscaled, scale } => logical_value(&LogicalValue::Decimal {
            unscaled: unscaled.clone(),
            scale: *scale,
        }),
        JsonValue::Array(items) => {
            for item in items {
                json_value(item, depth + 1)?;
            }
            Ok(())
        }
        JsonValue::Object(items) => {
            let mut keys = HashSet::new();
            for item in items {
                ensure(keys.insert(&item.key), "duplicate JSON key")?;
                json_value(&item.value, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
