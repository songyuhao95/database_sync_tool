//! The small, vendor-neutral in-memory model. JSON is its current diagnostic encoding.
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const FORMAT: &str = "cdc.change-event-json.v0.3";
pub const PREVIOUS_FORMAT: &str = "cdc.change-event-json.v0.2";
pub const LEGACY_FORMAT: &str = "cdc.change-event-json.v0.1";
pub const HISTORICAL_FORMAT: &str = "cdc.change-event-json.v0";
pub const LOGICAL_CONTRACT_FORMAT: &str = "cdc.logical-value.v0.3";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceCursor {
    pub format: String,
    pub value: String,
    pub display: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Source {
    pub kind: String,
    pub version: String,
    /// Vendor-specific source identity; accepts historical MySQL JSON.
    #[serde(alias = "server_uuid")]
    pub id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangeEvent<'a> {
    pub format: &'static str,
    pub source: &'a Source,
    pub transaction: Transaction<'a>,
    pub payload: Payload<'a>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Transaction<'a> {
    pub id: &'a str,
    pub sequence: usize,
    pub begin_cursor: &'a SourceCursor,
    pub commit_cursor: &'a SourceCursor,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload<'a> {
    TransactionBegin,
    RowChange {
        #[serde(flatten)]
        change: &'a RowChange,
    },
    TransactionCommit {
        change_count: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChange {
    /// Optional source database/catalog. Legacy events may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    pub operation: Operation,
    pub schema: String,
    pub table: String,
    pub source_cursor: SourceCursor,
    pub source_timestamp: u32,
    /// MySQL 5.7 does not carry column names/signedness in TABLE_MAP.
    pub schema_basis: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<Vec<ColumnDatum>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<ColumnDatum>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDatum {
    /// Zero-based ordinal in the source table, independent of the image bitmap.
    pub ordinal: usize,
    pub name: String,
    pub native_type: String,
    /// Zero-based position within the primary key; None means not a key column.
    #[serde(default)]
    pub primary_key_ordinal: Option<usize>,
    #[serde(default)]
    pub generated: bool,
    #[serde(default)]
    pub collation: Option<String>,
    #[serde(flatten)]
    pub datum: Datum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "presence", content = "value", rename_all = "snake_case")]
pub enum Datum {
    /// No historical value was supplied by the source.
    Unavailable,
    /// UPDATE retains the previous value (e.g. unchanged TOAST).
    Unchanged,
    Null,
    Value(LogicalValue),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalValue {
    /// A nested NULL. Column-level NULL continues to use [`Datum::Null`].
    Null,
    Boolean {
        value: bool,
    },
    Uuid {
        value: String,
    },
    Integer {
        signed: bool,
        bits: u8,
        value: String,
    },
    Decimal {
        unscaled: String,
        scale: usize,
    },
    Float {
        bits: u8,
        ieee754_hex: String,
    },
    Text {
        charset: String,
        bytes_base64url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    Binary {
        bytes_base64url: String,
    },
    /// A bit string is raw bytes plus the exact number of meaningful bits.
    /// `padding` describes the unused bits in the final byte and `bit_order`
    /// describes how meaningful bits are read; neither may be inferred by a
    /// Sink from the byte length.
    BitString {
        bytes_base64url: String,
        bit_length: u64,
        padding: BitPadding,
        bit_order: BitOrder,
    },
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    LocalTime {
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    },
    LocalDatetime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    },
    Duration {
        negative: bool,
        hours: u64,
        minutes: u8,
        seconds: u8,
        microsecond: u32,
    },
    Instant {
        unix_seconds: String,
        nanoseconds: u32,
    },
    Year {
        value: u16,
    },
    /// An ENUM value is carried by its declared label.  Native ordinals are
    /// source evidence only and never enter the portable value contract.
    Enum {
        label: String,
    },
    /// A SET value is carried as a member collection.  The source adapter
    /// rejects duplicate members; a target plan may reorder the collection to
    /// the target declaration order without changing its meaning.
    Set {
        members: Vec<String>,
    },
    /// Spatial values retain their wire format and reference metadata.  WKT
    /// or arbitrary text is deliberately not part of this value contract.
    Spatial {
        format: SpatialFormat,
        bytes_base64url: String,
        geometry_type: String,
        dimensions: u8,
        srid: Option<i32>,
        crs: Option<String>,
    },
    Array {
        elements: Vec<LogicalValue>,
    },
    Struct {
        fields: Vec<StructuredField>,
    },
    Map {
        entries: Vec<MapEntry>,
    },
    Range {
        empty: bool,
        lower: Option<Box<LogicalValue>>,
        upper: Option<Box<LogicalValue>>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    MultiRange {
        ranges: Vec<LogicalValue>,
    },
    /// An array with PostgreSQL-style structural metadata. The legacy
    /// `Array` variant remains available for one-dimensional, one-based
    /// arrays whose source did not expose shape metadata.
    ArrayWithMetadata {
        elements: Vec<LogicalValue>,
        dimensions: u8,
        lower_bounds: Vec<i32>,
    },
    /// A source-invalid temporal value that must not be normalized to NULL.
    InvalidTemporal {
        kind: String,
        raw: String,
    },
    Network {
        family: String,
        address: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix_length: Option<u8>,
    },
    Xml {
        bytes_base64url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    /// A domain value retains its underlying value while the domain identity
    /// and constraints remain part of its LogicalType.
    Domain {
        value: Box<LogicalValue>,
    },
    /// A value whose native semantics are only available through a verified
    /// source codec and definition fingerprint.
    Raw {
        carrier: RawValueCarrier,
    },
    Json {
        value: JsonValue,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BitOrder {
    MsbFirst,
    LsbFirst,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BitPadding {
    None,
    Zero,
    One,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SpatialFormat {
    Wkb,
    Ewkb,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StructuredField {
    pub name: String,
    pub value: LogicalValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MapEntry {
    pub key: LogicalValue,
    pub value: LogicalValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum JsonValue {
    Null,
    Boolean(bool),
    String(String),
    SignedInteger(String),
    UnsignedInteger(String),
    DoubleBits(String),
    Decimal { unscaled: String, scale: usize },
    Array(Vec<JsonValue>),
    Object(Vec<JsonEntry>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonEntry {
    pub key: String,
    pub value: JsonValue,
}

/// Evidence-backed carrier for a source value that has no portable logical
/// representation yet. The byte payload is base64url so the JSON contract is
/// lossless and does not depend on a text encoding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RawValueCarrier {
    pub codec_identity: String,
    pub native_type: String,
    pub source_definition_digest: String,
    pub encoding: String,
    pub raw_bytes_base64url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_text: Option<String>,
}

impl RawValueCarrier {
    pub fn new(
        codec_identity: impl Into<String>,
        native_type: impl Into<String>,
        source_definition_digest: impl Into<String>,
        encoding: impl Into<String>,
        raw_bytes_base64url: impl Into<String>,
        canonical_text: Option<impl Into<String>>,
    ) -> Self {
        Self {
            codec_identity: codec_identity.into(),
            native_type: native_type.into(),
            source_definition_digest: source_definition_digest.into(),
            encoding: encoding.into(),
            raw_bytes_base64url: raw_bytes_base64url.into(),
            canonical_text: canonical_text.map(Into::into),
        }
    }

    pub fn validate(&self) -> Result<(), RawValueCarrierError> {
        for (field, value) in [
            ("codec_identity", self.codec_identity.as_str()),
            ("native_type", self.native_type.as_str()),
            (
                "source_definition_digest",
                self.source_definition_digest.as_str(),
            ),
            ("encoding", self.encoding.as_str()),
        ] {
            if value.trim().is_empty() || value.contains('\0') {
                return Err(RawValueCarrierError::MissingEvidence { field });
            }
        }
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        URL_SAFE_NO_PAD
            .decode(&self.raw_bytes_base64url)
            .map_err(|_| RawValueCarrierError::InvalidBytes)
            .map(|_| ())
    }

    pub fn stable_digest(&self) -> String {
        stable_digest(self)
    }

    pub fn raw_bytes(&self) -> Result<Vec<u8>, RawValueCarrierError> {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        self.validate()?;
        URL_SAFE_NO_PAD
            .decode(&self.raw_bytes_base64url)
            .map_err(|_| RawValueCarrierError::InvalidBytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawValueCarrierError {
    MissingEvidence { field: &'static str },
    InvalidBytes,
}

impl RawValueCarrierError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingEvidence { .. } => "raw_value_carrier.missing_evidence",
            Self::InvalidBytes => "raw_value_carrier.invalid_bytes",
        }
    }
}

impl std::fmt::Display for RawValueCarrierError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEvidence { field } => {
                write!(formatter, "raw value carrier is missing {field}")
            }
            Self::InvalidBytes => {
                formatter.write_str("raw value carrier bytes are invalid base64url")
            }
        }
    }
}

impl std::error::Error for RawValueCarrierError {}

impl LogicalValue {
    pub fn validate(&self) -> Result<(), RawValueValidationError> {
        crate::validate::validate_value(self)
            .map_err(|error| RawValueValidationError(error.to_string()))
    }

    pub fn stable_digest(&self) -> String {
        stable_digest(&canonicalize_logical_value(self))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawValueValidationError(String);

impl std::fmt::Display for RawValueValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl RawValueValidationError {
    pub const fn code(&self) -> &'static str {
        "logical_value.invalid"
    }
}

impl std::error::Error for RawValueValidationError {}

/// An ordered, committed batch submitted by a database crate for validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeTransaction {
    pub source: Source,
    pub id: String,
    pub begin_cursor: SourceCursor,
    pub commit_cursor: SourceCursor,
    pub changes: Vec<RowChange>,
}

impl ChangeTransaction {
    pub fn content_digest(&self) -> String {
        stable_digest(&canonicalize_transaction(self))
    }
}

fn canonicalize_transaction(transaction: &ChangeTransaction) -> ChangeTransaction {
    let mut canonical = transaction.clone();
    for change in &mut canonical.changes {
        for image in [&mut change.before, &mut change.after]
            .into_iter()
            .flatten()
        {
            for column in image {
                if let Datum::Value(value) = &mut column.datum {
                    *value = canonicalize_logical_value(value);
                }
            }
        }
    }
    canonical
}

fn canonicalize_logical_value(value: &LogicalValue) -> LogicalValue {
    let mut canonical = value.clone();
    match &mut canonical {
        LogicalValue::Set { members } => members.sort(),
        LogicalValue::Array { elements } | LogicalValue::ArrayWithMetadata { elements, .. } => {
            for element in elements {
                *element = canonicalize_logical_value(element);
            }
        }
        LogicalValue::Struct { fields } => {
            for field in fields {
                field.value = canonicalize_logical_value(&field.value);
            }
        }
        LogicalValue::Map { entries } => {
            for entry in entries.iter_mut() {
                entry.key = canonicalize_logical_value(&entry.key);
                entry.value = canonicalize_logical_value(&entry.value);
            }
            entries.sort_by_key(|entry| entry.key.stable_digest());
        }
        LogicalValue::Range { lower, upper, .. } => {
            if let Some(value) = lower {
                **value = canonicalize_logical_value(value);
            }
            if let Some(value) = upper {
                **value = canonicalize_logical_value(value);
            }
        }
        LogicalValue::MultiRange { ranges } => {
            for range in ranges {
                *range = canonicalize_logical_value(range);
            }
        }
        LogicalValue::Domain { value } => {
            **value = canonicalize_logical_value(value);
        }
        LogicalValue::Json { value } => canonicalize_json_value(value),
        _ => {}
    }
    canonical
}

fn canonicalize_json_value(value: &mut JsonValue) {
    match value {
        JsonValue::Array(items) => {
            for item in items {
                canonicalize_json_value(item);
            }
        }
        JsonValue::Object(entries) => {
            for entry in entries.iter_mut() {
                canonicalize_json_value(&mut entry.value);
            }
            entries.sort_by(|left, right| left.key.cmp(&right.key));
        }
        _ => {}
    }
}

/// Digest the canonical JSON representation of a contract value. All public
/// contract types use deterministic struct field order and sequence-preserving
/// arrays, so the digest is independent of a transport serializer.
pub fn stable_digest<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).expect("contract values must be serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
