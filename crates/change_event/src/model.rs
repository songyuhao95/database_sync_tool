//! The small, vendor-neutral in-memory model. JSON is its current diagnostic encoding.
use serde::{Deserialize, Serialize};

pub const FORMAT: &str = "cdc.change-event-json.v0.3";
pub const PREVIOUS_FORMAT: &str = "cdc.change-event-json.v0.2";
pub const LEGACY_FORMAT: &str = "cdc.change-event-json.v0.1";
pub const HISTORICAL_FORMAT: &str = "cdc.change-event-json.v0";

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

/// An ordered, committed batch submitted by a database crate for validation.
#[derive(Debug, Clone)]
pub struct ChangeTransaction {
    pub source: Source,
    pub id: String,
    pub begin_cursor: SourceCursor,
    pub commit_cursor: SourceCursor,
    pub changes: Vec<RowChange>,
}
