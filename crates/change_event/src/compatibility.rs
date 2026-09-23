//! Database-neutral compatibility planning primitives.
//!
//! This module deliberately contains no database names or driver types. A Source
//! connector supplies a [`SourceTypeMapping`], while a Sink supplies a
//! [`TargetCapabilityManifest`]. The planner only joins those two independent
//! descriptions for one already-existing source/target field binding.

use crate::{
    BitOrder, BitPadding, ChangeTransaction, Datum, JsonValue, LogicalValue, Operation, RowChange,
    SpatialFormat, TargetCapabilityFailure, ValidatedTransaction,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, fmt};

pub const COMPATIBILITY_FORMAT: &str = "cdc.change-event-compatibility.v0.1";
pub const DIGEST_ALGORITHM: &str = "sha256";

/// Database-neutral identity of a connector release.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConnectorIdentity {
    pub kind: String,
    pub version: String,
}

impl ConnectorIdentity {
    pub fn new(kind: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            version: version.into(),
        }
    }
}

/// Exact server build evidence used when selecting a capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ServerBuildIdentity {
    pub product: String,
    pub distribution: String,
    pub version: String,
    pub build: String,
}

impl ServerBuildIdentity {
    pub fn new(
        product: impl Into<String>,
        distribution: impl Into<String>,
        version: impl Into<String>,
        build: impl Into<String>,
    ) -> Self {
        Self {
            product: product.into(),
            distribution: distribution.into(),
            version: version.into(),
            build: build.into(),
        }
    }
}

/// The state observed for one target-side capability or extension prerequisite.
///
/// The states are intentionally not collapsed into a boolean.  An installed
/// extension is evidence that a target can load it, while a qualified
/// capability additionally requires the connector's versioned evidence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityProbeStatus {
    Detected,
    Available,
    Installed,
    Qualified,
    Missing,
    PermissionDenied,
    Incompatible,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CapabilityProbeEntry {
    pub identity: String,
    pub status: CapabilityProbeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_digest: Option<String>,
}

impl CapabilityProbeEntry {
    pub fn new(identity: impl Into<String>, status: CapabilityProbeStatus) -> Self {
        Self {
            identity: identity.into(),
            status,
            version: None,
            evidence_digest: None,
        }
    }

    pub fn qualified(identity: impl Into<String>) -> Self {
        Self::new(identity, CapabilityProbeStatus::Qualified)
    }

    pub fn installed(identity: impl Into<String>) -> Self {
        Self::new(identity, CapabilityProbeStatus::Installed)
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_evidence_digest(mut self, digest: impl Into<String>) -> Self {
        self.evidence_digest = Some(digest.into());
        self
    }
}

/// Catalog evidence for the exact target column bound by a plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TargetColumnMetadata {
    pub definition_fingerprint: String,
    #[serde(default)]
    pub native_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_oid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typmod: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<String>,
}

impl TargetColumnMetadata {
    pub fn new(definition_fingerprint: impl Into<String>) -> Self {
        Self {
            definition_fingerprint: definition_fingerprint.into(),
            ..Self::default()
        }
    }

    pub fn with_native_type(mut self, native_type: impl Into<String>) -> Self {
        self.native_type = native_type.into();
        self
    }

    pub fn with_precision(mut self, precision: u32) -> Self {
        self.precision = Some(precision);
        self
    }

    pub fn with_scale(mut self, scale: i32) -> Self {
        self.scale = Some(scale);
        self
    }

    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    pub fn with_charset(mut self, charset: impl Into<String>) -> Self {
        self.charset = Some(charset.into());
        self
    }

    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    pub fn with_timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = Some(timezone.into());
        self
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    pub fn with_extension(mut self, extension: impl Into<String>) -> Self {
        self.extension = Some(extension.into());
        self
    }

    pub fn with_constraints<I, S>(mut self, constraints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints = constraints.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_indexes<I, S>(mut self, indexes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.indexes = indexes.into_iter().map(Into::into).collect();
        self
    }
}

/// The complete target session identity used while qualifying one plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TargetSessionProfile {
    pub identity: String,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

impl TargetSessionProfile {
    pub fn new<I, K, V>(identity: impl Into<String>, settings: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            identity: identity.into(),
            settings: settings
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

/// Immutable target-side evidence collected during preflight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetCapabilityProbe {
    pub target_build: ServerBuildIdentity,
    pub database: String,
    pub table: String,
    pub column: String,
    pub column_metadata: TargetColumnMetadata,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityProbeEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<CapabilityProbeEntry>,
    pub session: TargetSessionProfile,
    pub digest: String,
}

impl TargetCapabilityProbe {
    #[allow(clippy::too_many_arguments)]
    pub fn new<I, J>(
        target_build: ServerBuildIdentity,
        database: impl Into<String>,
        table: impl Into<String>,
        column: impl Into<String>,
        column_metadata: TargetColumnMetadata,
        capabilities: I,
        extensions: J,
        session: TargetSessionProfile,
    ) -> Self
    where
        I: IntoIterator<Item = CapabilityProbeEntry>,
        J: IntoIterator<Item = CapabilityProbeEntry>,
    {
        let mut probe = Self {
            target_build,
            database: database.into(),
            table: table.into(),
            column: column.into(),
            column_metadata,
            capabilities: capabilities.into_iter().collect(),
            extensions: extensions.into_iter().collect(),
            session,
            digest: String::new(),
        };
        probe.refresh_digest();
        probe
    }

    pub fn computed_digest(&self) -> String {
        digest_of(&ProbeDigestInput {
            target_build: &self.target_build,
            database: &self.database,
            table: &self.table,
            column: &self.column,
            column_metadata: &self.column_metadata,
            capabilities: &self.capabilities,
            extensions: &self.extensions,
            session: &self.session,
        })
    }

    pub fn refresh_digest(&mut self) {
        self.digest = self.computed_digest();
    }

    pub fn verify_digest(&self) -> bool {
        !self.digest.is_empty() && self.digest == self.computed_digest()
    }

    /// A probe is usable only when every capability has qualification evidence
    /// and every extension prerequisite is at least installed.  Installed is
    /// deliberately not accepted for capability entries.
    pub fn is_qualified(&self) -> bool {
        !self.capabilities.is_empty()
            && self
                .capabilities
                .iter()
                .all(|entry| entry.status == CapabilityProbeStatus::Qualified)
            && self.extensions.iter().all(|entry| {
                matches!(
                    entry.status,
                    CapabilityProbeStatus::Installed | CapabilityProbeStatus::Qualified
                )
            })
    }

    #[allow(clippy::result_large_err)]
    pub fn validate(&self) -> Result<(), TargetCapabilityFailure> {
        if !self.verify_digest() {
            return Err(
                TargetCapabilityFailure::new("target capability probe digest is invalid")
                    .with_code("target_capability.invalid_probe"),
            );
        }
        if self.database.trim().is_empty()
            || self.table.trim().is_empty()
            || self.column.trim().is_empty()
            || self
                .column_metadata
                .definition_fingerprint
                .trim()
                .is_empty()
            || self.session.identity.trim().is_empty()
            || self.capabilities.is_empty()
        {
            return Err(TargetCapabilityFailure::new(
                "target capability probe is missing target identity evidence",
            )
            .with_code("target_capability.incomplete_probe"));
        }
        let mut identities = std::collections::BTreeSet::new();
        for entry in self.capabilities.iter().chain(&self.extensions) {
            if entry.identity.trim().is_empty() || !identities.insert(&entry.identity) {
                return Err(TargetCapabilityFailure::new(
                    "target capability probe contains an empty or duplicate evidence identity",
                )
                .with_code("target_capability.invalid_probe"));
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ProbeDigestInput<'a> {
    target_build: &'a ServerBuildIdentity,
    database: &'a str,
    table: &'a str,
    column: &'a str,
    column_metadata: &'a TargetColumnMetadata,
    capabilities: &'a [CapabilityProbeEntry],
    extensions: &'a [CapabilityProbeEntry],
    session: &'a TargetSessionProfile,
}

/// An immutable source definition identity. The target has its own independent
/// instance of this type; the two fingerprints are never conflated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DefinitionReference {
    pub lineage_id: String,
    pub schema_fingerprint: String,
}

impl DefinitionReference {
    pub fn new(lineage_id: impl Into<String>, schema_fingerprint: impl Into<String>) -> Self {
        Self {
            lineage_id: lineage_id.into(),
            schema_fingerprint: schema_fingerprint.into(),
        }
    }

    fn is_complete(&self) -> bool {
        !self.lineage_id.trim().is_empty() && !self.schema_fingerprint.trim().is_empty()
    }
}

/// The semantic family and portable constraints of one source or target field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum LogicalType {
    Boolean,
    Uuid,
    Integer {
        signed: bool,
        bits: u8,
    },
    Decimal {
        precision: u16,
        scale: i32,
    },
    Float {
        bits: u8,
    },
    Text {
        charset: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_length: Option<u64>,
        #[serde(default)]
        length_unit: LengthUnit,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collation: Option<String>,
    },
    Binary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_length: Option<u64>,
    },
    BitString {
        length: u64,
    },
    Date,
    LocalTime {
        fractional_precision: u8,
    },
    LocalDatetime {
        fractional_precision: u8,
    },
    Instant {
        fractional_precision: u8,
    },
    Duration {
        fractional_precision: u8,
    },
    Year,
    Json {
        profile: JsonProfile,
    },
    Enum {
        members: Vec<String>,
    },
    Set {
        members: Vec<String>,
    },
    Spatial {
        subtype: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        srid: Option<i32>,
        dimensions: u8,
    },
    InvalidTemporal {
        kind: String,
    },
    Network {
        address_family: String,
        cidr: bool,
    },
    Xml,
    Array {
        element: Box<LogicalType>,
    },
    ArrayWithMetadata {
        element: Box<LogicalType>,
        dimensions: u8,
        lower_bounds: Vec<i32>,
    },
    Struct {
        fields: Vec<LogicalField>,
    },
    Map {
        key: Box<LogicalType>,
        value: Box<LogicalType>,
    },
    Range {
        element: Box<LogicalType>,
    },
    MultiRange {
        element: Box<LogicalType>,
    },
    Domain {
        name: String,
        base: Box<LogicalType>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        constraints: Vec<String>,
        #[serde(default)]
        not_null: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collation: Option<String>,
        definition_digest: String,
    },
    Opaque {
        source_type: String,
        format: String,
    },
    Raw {
        codec_identity: String,
        native_type: String,
        source_definition_digest: String,
        encoding: String,
    },
}

impl LogicalType {
    pub fn boolean() -> Self {
        Self::Boolean
    }

    pub fn uuid() -> Self {
        Self::Uuid
    }

    pub fn integer(signed: bool, bits: u8) -> Self {
        Self::Integer { signed, bits }
    }

    pub fn decimal(precision: u16, scale: i32) -> Self {
        Self::Decimal { precision, scale }
    }

    pub fn float(bits: u8) -> Self {
        Self::Float { bits }
    }

    pub fn text(charset: impl Into<String>, max_length: Option<u64>) -> Self {
        Self::Text {
            charset: charset.into(),
            max_length,
            length_unit: LengthUnit::Bytes,
            collation: None,
        }
    }

    pub fn binary(max_length: Option<u64>) -> Self {
        Self::Binary { max_length }
    }

    pub fn bit_string(length: u64) -> Self {
        Self::BitString { length }
    }

    pub fn spatial(subtype: impl Into<String>, srid: Option<i32>, dimensions: u8) -> Self {
        Self::Spatial {
            subtype: subtype.into(),
            srid,
            dimensions,
        }
    }

    pub fn invalid_temporal(kind: impl Into<String>) -> Self {
        Self::InvalidTemporal { kind: kind.into() }
    }

    pub fn network(address_family: impl Into<String>, cidr: bool) -> Self {
        Self::Network {
            address_family: address_family.into(),
            cidr,
        }
    }

    pub fn xml() -> Self {
        Self::Xml
    }

    pub fn date() -> Self {
        Self::Date
    }

    pub fn local_datetime(fractional_precision: u8) -> Self {
        Self::LocalDatetime {
            fractional_precision,
        }
    }

    pub fn instant(fractional_precision: u8) -> Self {
        Self::Instant {
            fractional_precision,
        }
    }

    pub fn duration(fractional_precision: u8) -> Self {
        Self::Duration {
            fractional_precision,
        }
    }

    pub fn year() -> Self {
        Self::Year
    }

    pub fn json() -> Self {
        Self::Json {
            profile: JsonProfile::default(),
        }
    }

    pub fn array_with_metadata(
        element: LogicalType,
        dimensions: u8,
        lower_bounds: Vec<i32>,
    ) -> Self {
        Self::ArrayWithMetadata {
            element: Box::new(element),
            dimensions,
            lower_bounds,
        }
    }

    pub fn domain(
        name: impl Into<String>,
        base: LogicalType,
        constraints: Vec<String>,
        not_null: bool,
        collation: Option<String>,
        definition_digest: impl Into<String>,
    ) -> Self {
        Self::Domain {
            name: name.into(),
            base: Box::new(base),
            constraints,
            not_null,
            collation,
            definition_digest: definition_digest.into(),
        }
    }

    pub fn raw(
        codec_identity: impl Into<String>,
        native_type: impl Into<String>,
        source_definition_digest: impl Into<String>,
        encoding: impl Into<String>,
    ) -> Self {
        Self::Raw {
            codec_identity: codec_identity.into(),
            native_type: native_type.into(),
            source_definition_digest: source_definition_digest.into(),
            encoding: encoding.into(),
        }
    }

    pub fn stable_digest(&self) -> String {
        crate::stable_digest(self)
    }

    pub fn validate(&self) -> Result<(), LogicalTypeValidationError> {
        match self {
            Self::Integer { bits, .. } if !matches!(bits, 8 | 16 | 24 | 32 | 64) => Err(
                LogicalTypeValidationError("integer width is invalid".into()),
            ),
            Self::Decimal { precision, scale } => {
                if *precision == 0 || *scale < -1000 || *scale > i32::from(*precision) {
                    Err(LogicalTypeValidationError(
                        "decimal precision or scale is invalid".into(),
                    ))
                } else {
                    Ok(())
                }
            }
            Self::Float { bits } if !matches!(bits, 32 | 64) => Err(LogicalTypeValidationError(
                "floating point width is invalid".into(),
            )),
            Self::Text {
                charset, collation, ..
            } => {
                if charset.trim().is_empty() || charset.contains('\0') {
                    return Err(LogicalTypeValidationError("text charset is invalid".into()));
                }
                if collation
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Err(LogicalTypeValidationError(
                        "text collation is invalid".into(),
                    ));
                }
                Ok(())
            }
            Self::BitString { length } if *length == 0 => Err(LogicalTypeValidationError(
                "bit string length must be positive".into(),
            )),
            Self::Spatial {
                subtype,
                dimensions,
                ..
            } => {
                if subtype.trim().is_empty() || !(2..=4).contains(dimensions) {
                    Err(LogicalTypeValidationError(
                        "spatial declaration is invalid".into(),
                    ))
                } else {
                    Ok(())
                }
            }
            Self::InvalidTemporal { kind } => {
                if kind.trim().is_empty() {
                    Err(LogicalTypeValidationError(
                        "invalid temporal kind is empty".into(),
                    ))
                } else {
                    Ok(())
                }
            }
            Self::Network { address_family, .. } => {
                if address_family.trim().is_empty() {
                    Err(LogicalTypeValidationError("network family is empty".into()))
                } else {
                    Ok(())
                }
            }
            Self::ArrayWithMetadata {
                element,
                dimensions,
                lower_bounds,
            } => {
                if *dimensions == 0 || usize::from(*dimensions) != lower_bounds.len() {
                    return Err(LogicalTypeValidationError(
                        "array dimensions and lower bounds do not agree".into(),
                    ));
                }
                element.validate()
            }
            Self::Struct { fields } => {
                let mut names = std::collections::BTreeSet::new();
                for field in fields {
                    if field.name.trim().is_empty() || !names.insert(&field.name) {
                        return Err(LogicalTypeValidationError(
                            "structured field names must be unique and non-empty".into(),
                        ));
                    }
                    field.logical_type.validate()?;
                }
                Ok(())
            }
            Self::Array { element } | Self::Range { element } | Self::MultiRange { element } => {
                element.validate()
            }
            Self::Map { key, value } => {
                key.validate()?;
                value.validate()
            }
            Self::Domain {
                name,
                base,
                definition_digest,
                ..
            } => {
                if name.trim().is_empty() || definition_digest.trim().is_empty() {
                    return Err(LogicalTypeValidationError(
                        "domain identity or definition digest is empty".into(),
                    ));
                }
                base.validate()
            }
            Self::Raw {
                codec_identity,
                native_type,
                source_definition_digest,
                encoding,
            } => {
                if [
                    codec_identity,
                    native_type,
                    source_definition_digest,
                    encoding,
                ]
                .iter()
                .any(|value| value.trim().is_empty() || value.contains('\0'))
                {
                    Err(LogicalTypeValidationError(
                        "raw logical type lacks stable source evidence".into(),
                    ))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    /// Return the family name used by the legacy summary manifest.
    pub fn family_name(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Uuid => "uuid",
            Self::Integer { .. } => "integer",
            Self::Decimal { .. } => "decimal",
            Self::Float { .. } => "float",
            Self::Text { .. } => "text",
            Self::Binary { .. } => "binary",
            Self::BitString { .. } => "bit_string",
            Self::Date => "date",
            Self::LocalTime { .. } => "local_time",
            Self::LocalDatetime { .. } => "local_datetime",
            Self::Instant { .. } => "instant",
            Self::Duration { .. } => "duration",
            Self::Year => "year",
            Self::Json { .. } => "json",
            Self::Enum { .. } => "enum",
            Self::Set { .. } => "set",
            Self::Spatial { .. } => "spatial",
            Self::InvalidTemporal { .. } => "invalid_temporal",
            Self::Network { .. } => "network",
            Self::Xml => "xml",
            Self::Array { .. } => "array",
            Self::ArrayWithMetadata { .. } => "array",
            Self::Struct { .. } => "struct",
            Self::Map { .. } => "map",
            Self::Range { .. } => "range",
            Self::MultiRange { .. } => "multi_range",
            Self::Domain { .. } => "domain",
            Self::Opaque { .. } => "opaque",
            Self::Raw { .. } => "raw",
        }
    }

    pub fn matches_value(&self, value: &LogicalValue) -> bool {
        match (self, value) {
            (Self::Boolean, LogicalValue::Boolean { .. })
            | (Self::Uuid, LogicalValue::Uuid { .. })
            | (Self::Date, LogicalValue::Date { .. })
            | (Self::LocalTime { .. }, LogicalValue::LocalTime { .. })
            | (Self::Duration { .. }, LogicalValue::Duration { .. })
            | (Self::Year, LogicalValue::Year { .. })
            | (Self::Json { .. }, LogicalValue::Json { .. }) => true,
            (
                Self::Integer { signed, bits },
                LogicalValue::Integer {
                    signed: value_signed,
                    bits: value_bits,
                    ..
                },
            ) => signed == value_signed && bits == value_bits,
            (
                Self::Decimal {
                    precision, scale, ..
                },
                LogicalValue::Decimal {
                    unscaled,
                    scale: value_scale,
                },
            ) => {
                let digits = unscaled.strip_prefix('-').unwrap_or(unscaled);
                (if *scale < 0 {
                    *value_scale == 0
                } else {
                    *scale as usize == *value_scale
                }) && !digits.is_empty()
                    && digits.len() <= usize::from(*precision)
            }
            (
                Self::Float { bits },
                LogicalValue::Float {
                    bits: value_bits, ..
                },
            ) => bits == value_bits,
            (
                Self::Text {
                    charset,
                    max_length,
                    length_unit,
                    ..
                },
                LogicalValue::Text {
                    charset: value_charset,
                    bytes_base64url,
                    text,
                    ..
                },
            ) => {
                if !charset.eq_ignore_ascii_case(value_charset) {
                    return false;
                }
                let Some(max_length) = max_length else {
                    return true;
                };
                let Some(length) = text.as_ref().map(|text| match length_unit {
                    LengthUnit::Bytes => text.len(),
                    LengthUnit::Characters => text.chars().count(),
                }) else {
                    let Ok(bytes) = URL_SAFE_NO_PAD.decode(bytes_base64url) else {
                        return false;
                    };
                    return u64::try_from(bytes.len()).is_ok_and(|length| length <= *max_length);
                };
                u64::try_from(length).is_ok_and(|length| length <= *max_length)
            }
            (Self::Binary { max_length }, LogicalValue::Binary { bytes_base64url }) => {
                max_length.as_ref().is_none_or(|max_length| {
                    URL_SAFE_NO_PAD.decode(bytes_base64url).is_ok_and(|bytes| {
                        u64::try_from(bytes.len()).is_ok_and(|len| len <= *max_length)
                    })
                })
            }
            (Self::BitString { length }, LogicalValue::BitString { bit_length, .. }) => {
                length == bit_length
            }
            (Self::Enum { members }, LogicalValue::Enum { label }) => {
                !members.is_empty() && members.iter().any(|member| member == label)
            }
            (Self::Set { members }, LogicalValue::Set { members: values }) => {
                !members.is_empty()
                    && values
                        .iter()
                        .all(|value| members.iter().any(|member| member == value))
                    && values
                        .iter()
                        .enumerate()
                        .all(|(index, value)| !values[..index].contains(value))
            }
            (Self::LocalDatetime { .. }, LogicalValue::LocalDatetime { .. })
            | (Self::Instant { .. }, LogicalValue::Instant { .. }) => true,
            (
                Self::Spatial {
                    subtype,
                    srid,
                    dimensions,
                },
                LogicalValue::Spatial {
                    geometry_type,
                    srid: value_srid,
                    dimensions: value_dimensions,
                    ..
                },
            ) => {
                subtype.eq_ignore_ascii_case(geometry_type)
                    && srid == value_srid
                    && dimensions == value_dimensions
            }
            (
                Self::InvalidTemporal { kind },
                LogicalValue::InvalidTemporal {
                    kind: value_kind, ..
                },
            ) => kind == value_kind,
            (
                Self::Network {
                    address_family,
                    cidr,
                },
                LogicalValue::Network {
                    family: value_family,
                    prefix_length,
                    ..
                },
            ) => {
                address_family.eq_ignore_ascii_case(value_family)
                    && (*cidr == prefix_length.is_some())
            }
            (Self::Xml, LogicalValue::Xml { .. }) => true,
            (Self::Array { element }, LogicalValue::Array { elements }) => {
                elements.iter().all(|value| element.matches_value(value))
            }
            (
                Self::ArrayWithMetadata {
                    element,
                    dimensions,
                    lower_bounds,
                },
                LogicalValue::ArrayWithMetadata {
                    elements,
                    dimensions: value_dimensions,
                    lower_bounds: value_lower_bounds,
                },
            ) => {
                dimensions == value_dimensions
                    && lower_bounds == value_lower_bounds
                    && elements.iter().all(|value| element.matches_value(value))
            }
            (Self::Struct { fields }, LogicalValue::Struct { fields: values }) => {
                fields.len() == values.len()
                    && fields.iter().zip(values).all(|(field, value)| {
                        field.name == value.name && field.logical_type.matches_value(&value.value)
                    })
            }
            (Self::Map { key, value }, LogicalValue::Map { entries }) => entries
                .iter()
                .all(|entry| key.matches_value(&entry.key) && value.matches_value(&entry.value)),
            (Self::Range { element }, LogicalValue::Range { lower, upper, .. }) => lower
                .iter()
                .chain(upper.iter())
                .all(|bound| element.matches_value(bound)),
            (Self::MultiRange { element }, LogicalValue::MultiRange { ranges }) => {
                ranges.iter().all(|range| match range {
                    LogicalValue::Range { lower, upper, .. } => lower
                        .iter()
                        .chain(upper.iter())
                        .all(|bound| element.matches_value(bound)),
                    _ => false,
                })
            }
            (Self::Domain { base, .. }, LogicalValue::Domain { value }) => {
                base.matches_value(value)
            }
            (Self::Domain { base, .. }, value) => base.matches_value(value),
            (
                Self::Raw {
                    codec_identity,
                    native_type,
                    source_definition_digest,
                    encoding,
                },
                LogicalValue::Raw { carrier },
            ) => {
                codec_identity == &carrier.codec_identity
                    && native_type == &carrier.native_type
                    && source_definition_digest == &carrier.source_definition_digest
                    && encoding == &carrier.encoding
            }
            (Self::Opaque { .. }, LogicalValue::Raw { .. }) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTypeValidationError(String);

impl std::fmt::Display for LogicalTypeValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl LogicalTypeValidationError {
    pub const fn code(&self) -> &'static str {
        "logical_type.invalid"
    }
}

impl std::error::Error for LogicalTypeValidationError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LogicalField {
    pub name: String,
    pub logical_type: LogicalType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LengthUnit {
    #[default]
    Bytes,
    Characters,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonProfile {
    pub normalized: bool,
    pub duplicate_keys: DuplicateKeyPolicy,
}

/// Target-side JSON representation selected by a Column Conversion Plan.
/// Raw source spelling is not available in the normalized ChangeEvent value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JsonRepresentationStrategy {
    Structured,
    NormalizedText,
    RawText,
}

impl Default for JsonProfile {
    fn default() -> Self {
        Self {
            normalized: true,
            duplicate_keys: DuplicateKeyPolicy::Reject,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateKeyPolicy {
    Reject,
    PreserveLast,
}

/// A source connector's strict, versioned mapping result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTypeMapping {
    pub connector: ConnectorIdentity,
    pub native_type: String,
    pub logical_type: LogicalType,
    pub mapping_id: String,
    pub mapping_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_definition_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_build: Option<ServerBuildIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_fingerprint: Option<String>,
}

impl SourceTypeMapping {
    pub fn new(
        connector: ConnectorIdentity,
        native_type: impl Into<String>,
        logical_type: LogicalType,
        mapping_id: impl Into<String>,
        mapping_version: impl Into<String>,
    ) -> Self {
        Self {
            connector,
            native_type: native_type.into(),
            logical_type,
            mapping_id: mapping_id.into(),
            mapping_version: mapping_version.into(),
            evidence_digest: None,
            source_definition_fingerprint: None,
            source_build: None,
            environment_fingerprint: None,
        }
    }

    /// Bind a mapping to the immutable source definition, exact server build,
    /// and semantic environment that qualified its interpretation.
    pub fn with_source_evidence(
        mut self,
        source_definition_fingerprint: impl Into<String>,
        source_build: ServerBuildIdentity,
        environment_fingerprint: impl Into<String>,
    ) -> Self {
        self.source_definition_fingerprint = Some(source_definition_fingerprint.into());
        self.source_build = Some(source_build);
        self.environment_fingerprint = Some(environment_fingerprint.into());
        self.evidence_digest = Some(crate::stable_digest(&(
            &self.connector,
            &self.native_type,
            &self.logical_type,
            &self.mapping_id,
            &self.mapping_version,
            &self.source_definition_fingerprint,
            &self.source_build,
            &self.environment_fingerprint,
        )));
        self
    }
}

/// A concrete target native representation. Target type names never enter a
/// ChangeEvent; they are confined to this Sink-owned type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetRepresentation {
    pub native_type: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

impl TargetRepresentation {
    pub fn new(native_type: impl Into<String>) -> Self {
        Self {
            native_type: native_type.into(),
            parameters: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualificationLevel {
    Exact,
    RangeChecked,
    ExplicitConversion,
    Unsupported,
}

pub type Qualification = QualificationLevel;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskLevel {
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LossAssessment {
    /// Whether the selected representation can lose source value semantics.
    pub value: bool,
    /// Whether target equality/uniqueness comparison can differ.
    pub comparison: bool,
    /// Whether ordering semantics can differ.
    pub ordering: bool,
    /// Whether source constraints require an explicit target policy.
    pub constraints: bool,
    pub explanation: String,
}

impl LossAssessment {
    fn for_qualification(qualification: QualificationLevel, key_like: bool) -> Self {
        match qualification {
            QualificationLevel::Exact => Self {
                value: false,
                comparison: false,
                ordering: false,
                constraints: false,
                explanation: "source semantics are preserved by the qualified target representation"
                    .into(),
            },
            QualificationLevel::RangeChecked => Self {
                value: false,
                comparison: false,
                ordering: false,
                constraints: false,
                explanation: "values are preserved when the declared target range check passes"
                    .into(),
            },
            QualificationLevel::ExplicitConversion => Self {
                value: true,
                comparison: key_like,
                ordering: key_like,
                constraints: true,
                explanation: "the selected rule is an explicit semantic conversion and carries a declared guarantee downgrade"
                    .into(),
            },
            QualificationLevel::Unsupported => Self {
                value: true,
                comparison: true,
                ordering: true,
                constraints: true,
                explanation: "no qualified target representation exists".into(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConversionExample {
    pub source: String,
    pub target: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LocatorImpact {
    Preserved,
    #[default]
    NotUsed,
    ValueOnly,
    Blocked,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlanConfirmationState {
    #[default]
    NotRequired,
    Required,
    Confirmed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PresenceState {
    Value,
    Null,
    Unchanged,
    Unavailable,
    GeneratedObservation,
}

impl PresenceState {
    fn from_datum(datum: &Datum, generated: bool) -> Self {
        match datum {
            Datum::Value(_) if generated => Self::GeneratedObservation,
            Datum::Value(_) => Self::Value,
            Datum::Null => Self::Null,
            Datum::Unchanged => Self::Unchanged,
            Datum::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureClass {
    ChangeEventValidation,
    SourceContract,
    InvalidInput,
    TargetCapability,
    MissingConfirmation,
    StaleInput,
    TransientTarget,
    CommitUnknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailurePhase {
    InputValidation,
    SourceContract,
    CapabilityQualification,
    PlanConstruction,
    Conversion,
    Locator,
    Apply,
    Commit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetryClassification {
    NotRetryable,
    Retryable,
    CommitUnknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityStatus {
    Compatible,
    NeedsConfirmation,
    NeedsConfiguration,
    Unsupported,
    Blocked,
    Stale,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailurePolicy {
    Reject,
    RetryTransaction,
    ResolveCommitUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OptionSpec {
    pub name: String,
    pub value_kind: OptionValueKind,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_values: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OptionValueKind {
    Boolean,
    Integer,
    String,
    Enum,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuleReference {
    pub id: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConversionRule {
    pub id: String,
    pub version: String,
    pub qualification: QualificationLevel,
    pub risk: RiskLevel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_code: Option<String>,
    pub requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<OptionSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_operations: Vec<Operation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_presence: Vec<PresenceState>,
    pub allows_key: bool,
    pub failure_policy: FailurePolicy,
    pub evidence_digest: String,
}

impl ConversionRule {
    fn reference(&self) -> RuleReference {
        RuleReference {
            id: self.id.clone(),
            version: self.version.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityEntry {
    pub code: String,
    pub source_logical_type: LogicalType,
    pub target: TargetRepresentation,
    pub rule: ConversionRule,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_operations: Vec<Operation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_presence: Vec<PresenceState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetCapabilityManifest {
    pub connector: ConnectorIdentity,
    pub target_build: ServerBuildIdentity,
    pub contract: String,
    pub requires_primary_key: bool,
    pub capabilities: Vec<CapabilityEntry>,
    pub digest: String,
}

impl TargetCapabilityManifest {
    pub fn new(
        connector: ConnectorIdentity,
        target_build: ServerBuildIdentity,
        capabilities: Vec<CapabilityEntry>,
        requires_primary_key: bool,
    ) -> Self {
        let mut manifest = Self {
            connector,
            target_build,
            contract: COMPATIBILITY_FORMAT.to_owned(),
            requires_primary_key,
            capabilities,
            digest: String::new(),
        };
        manifest.digest = manifest.computed_digest();
        manifest
    }

    pub fn computed_digest(&self) -> String {
        digest_of(&ManifestDigestInput {
            connector: &self.connector,
            target_build: &self.target_build,
            contract: &self.contract,
            requires_primary_key: self.requires_primary_key,
            capabilities: &self.capabilities,
        })
    }

    pub fn verify_digest(&self) -> bool {
        !self.digest.is_empty() && self.digest == self.computed_digest()
    }

    pub fn validate(&self) -> Result<(), CompatibilityError> {
        if self.contract != COMPATIBILITY_FORMAT || !self.verify_digest() {
            return Err(CompatibilityError::TargetCapability(Box::new(
                TargetCapabilityFailure::manifest("target capability manifest is stale or invalid"),
            )));
        }
        let mut codes = std::collections::BTreeSet::new();
        for capability in &self.capabilities {
            if capability.code.trim().is_empty()
                || capability.target.native_type.trim().is_empty()
                || capability.rule.id.trim().is_empty()
                || capability.rule.version.trim().is_empty()
                || capability.rule.evidence_digest.trim().is_empty()
                || !codes.insert(&capability.code)
            {
                return Err(CompatibilityError::TargetCapability(Box::new(
                    TargetCapabilityFailure::manifest(
                        "target capability manifest contains an invalid or duplicate entry",
                    ),
                )));
            }
            if capability.rule.qualification == QualificationLevel::Unsupported {
                continue;
            }
            if capability.rule.qualification == QualificationLevel::ExplicitConversion
                && capability.rule.allows_key
            {
                return Err(CompatibilityError::TargetCapability(Box::new(
                    TargetCapabilityFailure::manifest(
                        "an explicit conversion cannot be qualified for a key or Row Locator",
                    ),
                )));
            }
            if capability.rule.supported_operations.is_empty()
                || capability.rule.supported_presence.is_empty()
            {
                return Err(CompatibilityError::TargetCapability(Box::new(
                    TargetCapabilityFailure::manifest(
                        "qualified capability is missing operation or presence coverage",
                    ),
                )));
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ManifestDigestInput<'a> {
    connector: &'a ConnectorIdentity,
    target_build: &'a ServerBuildIdentity,
    contract: &'a str,
    requires_primary_key: bool,
    capabilities: &'a [CapabilityEntry],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldDefinition {
    pub reference: DefinitionReference,
    pub ordinal: usize,
    pub name: String,
    pub native_type: String,
    pub logical_type: LogicalType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    pub generated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key_ordinal: Option<usize>,
    /// A unique key column is protected by the same lossy-conversion rule as
    /// a primary key even when the current Row Locator implementation does not
    /// use it as its preferred locator.
    #[serde(default)]
    pub unique: bool,
    /// A route may designate a non-key column as a Row Locator when a future
    /// Sink proves that strategy.  Such a column is still protected here.
    #[serde(default)]
    pub row_locator: bool,
}

pub type ColumnDefinition = FieldDefinition;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskConfirmation {
    pub source_field_lineage: String,
    pub target_field_lineage: String,
    pub rule: RuleReference,
    pub plan_digest: String,
    pub actor: String,
    pub confirmed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteOptions {
    pub route_id: String,
    pub configuration_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_rule: Option<RuleReference>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub confirmations: Vec<RiskConfirmation>,
    /// Optional target preflight evidence.  Legacy callers may omit it while
    /// connectors migrate to probe-backed activation; when present it is
    /// always part of the immutable plan identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_probe: Option<TargetCapabilityProbe>,
}

pub type CompatibilityOptions = RouteOptions;

#[derive(Debug, Clone)]
pub struct CompatibilityInput<'a> {
    pub transaction: &'a ValidatedTransaction,
    pub source_field: FieldDefinition,
    pub target_field: FieldDefinition,
    pub source_type_mapping: SourceTypeMapping,
    pub source_connector: ConnectorIdentity,
    pub sink_connector: ConnectorIdentity,
    pub source_build: Option<ServerBuildIdentity>,
    pub target_build: Option<ServerBuildIdentity>,
    pub manifest: &'a TargetCapabilityManifest,
    pub options: RouteOptions,
}

/// Catalog-time compatibility input used by Web preflight and route
/// activation.  It deliberately carries only source/target definitions and
/// the operation/presence contract; no row sample is used to choose a plan.
#[derive(Debug, Clone)]
pub struct FieldCompatibilityInput<'a> {
    pub source_field: FieldDefinition,
    pub target_field: FieldDefinition,
    pub source_type_mapping: SourceTypeMapping,
    pub source_connector: ConnectorIdentity,
    pub sink_connector: ConnectorIdentity,
    pub source_build: Option<ServerBuildIdentity>,
    pub target_build: Option<ServerBuildIdentity>,
    pub manifest: &'a TargetCapabilityManifest,
    pub operations: Vec<Operation>,
    pub presences: Vec<PresenceState>,
    pub source_has_primary_key: bool,
    pub options: RouteOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetTypeCandidate {
    pub target: TargetRepresentation,
    pub rule: RuleReference,
    pub capability_code: String,
    pub qualification: QualificationLevel,
    pub risk: RiskLevel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_code: Option<String>,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanSummary {
    pub input_digest: String,
    pub plan_digest: String,
    pub rule: RuleReference,
    pub capability_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompatibilityFailure {
    pub class: FailureClass,
    pub code: String,
    pub phase: FailurePhase,
    pub retry: RetryClassification,
    pub message: String,
}

impl CompatibilityFailure {
    fn new(
        class: FailureClass,
        code: impl Into<String>,
        phase: FailurePhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            class,
            code: code.into(),
            phase,
            retry: RetryClassification::NotRetryable,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompatibilityResult {
    pub source_field: DefinitionReference,
    pub target_field: DefinitionReference,
    pub status: CompatibilityStatus,
    pub qualification: QualificationLevel,
    pub risk: RiskLevel,
    pub reason_code: String,
    pub explanation: String,
    pub requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<RuleReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<ColumnConversionPlan>,
    #[serde(default)]
    pub candidates: Vec<TargetTypeCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<CompatibilityFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<PlanSummary>,
}

impl CompatibilityResult {
    pub fn is_selectable(&self) -> bool {
        matches!(self.status, CompatibilityStatus::Compatible)
    }
}

/// Validate one captured value against the fixed domain described by a
/// `ColumnConversionPlan`.
///
/// The planner is deliberately separate from this check: planning chooses a
/// target representation once, while this function applies that decision to
/// every row without widening the domain or consulting the observed value for
/// a different plan.  Decimal values are accepted only when reducing their
/// scale is exact; implicit rounding is never performed.
#[allow(clippy::result_large_err)]
pub fn validate_value_against_plan(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let range_kind = plan.target.parameters.get("range_kind").map(String::as_str);
    if range_kind.is_some()
        && plan
            .target
            .parameters
            .get("range_check")
            .map(String::as_str)
            != Some("strict")
    {
        return Err(plan_failure(
            plan,
            "target_capability.range_check_missing",
            "the saved plan does not require a strict range check",
        ));
    }
    if matches!(range_kind, Some("decimal" | "float"))
        && plan
            .target
            .parameters
            .get("rounding_mode")
            .map(String::as_str)
            != Some("reject")
    {
        return Err(plan_failure(
            plan,
            "target_capability.rounding_policy_missing",
            "the saved plan does not reject implicit rounding",
        ));
    }
    match range_kind {
        Some("integer") => validate_integer_plan_value(plan, value),
        Some("decimal") => validate_decimal_plan_value(plan, value),
        Some("float") => validate_float_plan_value(plan, value),
        Some(other) => Err(plan_failure(
            plan,
            "target_capability.unknown_range_rule",
            format!("unknown range rule {other:?}"),
        )),
        None => match plan
            .target
            .parameters
            .get("conversion_kind")
            .map(String::as_str)
        {
            Some("binary") => validate_binary_plan_value(plan, value),
            Some("bit_string") => validate_bit_string_plan_value(plan, value),
            Some("spatial") => validate_spatial_plan_value(plan, value),
            Some("recursive") => validate_recursive_plan_value(plan, value),
            Some("text") => validate_text_plan_value(plan, value),
            Some("json") => validate_json_plan_value(plan, value),
            Some("enum") | Some("set") => validate_enum_set_plan_value(plan, value),
            Some("temporal") => validate_temporal_plan_value(plan, value),
            Some(other) => Err(plan_failure(
                plan,
                "target_capability.unknown_conversion_rule",
                format!("unknown explicit conversion rule {other:?}"),
            )),
            None => match plan
                .target
                .parameters
                .get("value_strategy")
                .map(String::as_str)
            {
                Some("structured_json") => match value {
                    LogicalValue::Json { .. } => Ok(()),
                    _ => Err(plan_failure(
                        plan,
                        "target_capability.json_type_mismatch",
                        "the structured JSON plan received a non-JSON value",
                    )),
                },
                Some("enum_label") | Some("set_members") => {
                    validate_enum_set_plan_value(plan, value)
                }
                Some("binary") => validate_binary_plan_value(plan, value),
                Some("bit_string") => validate_bit_string_plan_value(plan, value),
                Some("spatial") => validate_spatial_plan_value(plan, value),
                Some("recursive") => validate_recursive_plan_value(plan, value),
                Some(other) => Err(plan_failure(
                    plan,
                    "target_capability.unknown_value_strategy",
                    format!("unknown value strategy {other:?}"),
                )),
                None => Ok(()),
            },
        },
    }
}

/// Validate a presence state against the same fixed plan.  `Unchanged` is a
/// control instruction for an UPDATE and therefore does not cause a value
/// conversion; `Unavailable` is never a writable value.
#[allow(clippy::result_large_err)]
pub fn validate_datum_against_plan(
    plan: &ColumnConversionPlan,
    datum: &Datum,
) -> Result<(), TargetCapabilityFailure> {
    match datum {
        Datum::Value(value) => validate_value_against_plan(plan, value),
        // Presence coverage is qualified when the plan is built. The compact
        // persisted plan stores the immutable rule reference rather than a
        // second copy of the manifest's presence list; the common event
        // validator has already checked the operation/image relationship.
        Datum::Null | Datum::Unchanged => Ok(()),
        Datum::Unavailable => Err(plan_failure(
            plan,
            "target_capability.unavailable_value",
            "an unavailable source value cannot be written by a conversion plan",
        )),
    }
}

fn plan_parameter<'a>(plan: &'a ColumnConversionPlan, name: &str) -> Option<&'a str> {
    plan.parameters
        .get(name)
        .or_else(|| plan.target.parameters.get(name))
        .map(String::as_str)
}

#[allow(clippy::result_large_err)]
fn validate_binary_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Binary { bytes_base64url } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.binary_type_mismatch",
            "the binary conversion plan received a non-binary value",
        ));
    };
    if plan_parameter(plan, "binary_encoding") != Some("raw_bytes") {
        return Err(plan_failure(
            plan,
            "target_capability.binary_encoding_invalid",
            "binary values require the raw_bytes representation; text or base64 text is not implicit",
        ));
    }
    let bytes = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.binary_format_invalid",
            "the binary value is not valid base64url bytes",
        )
    })?;
    let target_length = plan_parameter(plan, "target_length")
        .filter(|value| *value != "unbounded")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| {
            plan_failure(
                plan,
                "target_capability.binary_length_invalid",
                "the saved binary target length is invalid",
            )
        })?;
    if let Some(length) = target_length {
        let fixed = plan_parameter(plan, "target_fixed") == Some("true");
        if bytes.len() > length
            || (fixed
                && bytes.len() < length
                && plan_parameter(plan, "target_padding") == Some("none"))
        {
            return Err(plan_failure(
                plan,
                "target_capability.binary_length_out_of_range",
                "binary bytes do not fit the saved target length or padding policy",
            ));
        }
    }
    if let Some(source_length) = plan_parameter(plan, "source_length")
        .filter(|value| *value != "unbounded")
        .and_then(|value| value.parse::<usize>().ok())
    {
        let source_fixed = plan_parameter(plan, "source_fixed") == Some("true");
        if bytes.len() > source_length || (source_fixed && bytes.len() != source_length) {
            return Err(plan_failure(
                plan,
                "target_capability.binary_source_length_invalid",
                "binary bytes do not match the saved source length semantics",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_bit_string_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::BitString {
        bytes_base64url,
        bit_length,
        padding,
        bit_order,
    } = value
    else {
        return Err(plan_failure(
            plan,
            "target_capability.bit_string_type_mismatch",
            "the bit string conversion plan received another LogicalValue family",
        ));
    };
    let expected_length = plan_parameter(plan, "target_bit_length")
        .or_else(|| plan_parameter(plan, "target_length"))
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.bit_string_length_missing",
                "the bit string plan does not declare a target bit length",
            )
        })?
        .parse::<u64>()
        .map_err(|_| {
            plan_failure(
                plan,
                "target_capability.bit_string_length_invalid",
                "the saved bit string length is invalid",
            )
        })?;
    if *bit_length != expected_length {
        return Err(plan_failure(
            plan,
            "target_capability.bit_string_length_mismatch",
            "bit string length differs from the qualified target length",
        ));
    }
    if let Some(source_length) =
        plan_parameter(plan, "source_bit_length").and_then(|value| value.parse::<u64>().ok())
        && *bit_length != source_length
    {
        return Err(plan_failure(
            plan,
            "target_capability.bit_string_source_length_mismatch",
            "bit string length differs from the qualified source length",
        ));
    }
    let expected_order = match plan_parameter(plan, "target_bit_order") {
        Some("msb_first") => BitOrder::MsbFirst,
        Some("lsb_first") => BitOrder::LsbFirst,
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.bit_order_missing",
                "the bit string plan must declare bit order",
            ));
        }
    };
    if *bit_order != expected_order {
        return Err(plan_failure(
            plan,
            "target_capability.bit_order_mismatch",
            "bit order differs from the qualified target representation",
        ));
    }
    let expected_padding = match plan_parameter(plan, "target_padding") {
        Some("none") => BitPadding::None,
        Some("zero") => BitPadding::Zero,
        Some("one") => BitPadding::One,
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.bit_padding_missing",
                "the bit string plan must declare padding semantics",
            ));
        }
    };
    if *padding != expected_padding {
        return Err(plan_failure(
            plan,
            "target_capability.bit_padding_mismatch",
            "bit padding differs from the qualified target representation",
        ));
    }
    let raw = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.bit_string_format_invalid",
            "the bit string value is not valid base64url bytes",
        )
    })?;
    let expected_bytes = expected_length.div_ceil(8) as usize;
    if raw.len() != expected_bytes {
        return Err(plan_failure(
            plan,
            "target_capability.bit_string_length_invalid",
            "bit string bytes do not match the declared bit length",
        ));
    }
    let unused = (8 - (expected_length % 8)) % 8;
    if unused == 0 {
        if expected_padding != BitPadding::None {
            return Err(plan_failure(
                plan,
                "target_capability.bit_padding_invalid",
                "a byte-aligned bit string cannot contain padding bits",
            ));
        }
    } else {
        let mask = match expected_order {
            BitOrder::MsbFirst => (1_u8 << unused) - 1,
            BitOrder::LsbFirst => u8::MAX << (8 - unused),
        };
        let actual = raw[raw.len() - 1] & mask;
        let expected = if expected_padding == BitPadding::One {
            mask
        } else {
            0
        };
        if actual != expected {
            return Err(plan_failure(
                plan,
                "target_capability.bit_padding_invalid",
                "unused bit padding does not match the qualified zero/one policy",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_spatial_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Spatial {
        format,
        bytes_base64url,
        geometry_type,
        dimensions,
        srid,
        crs,
    } = value
    else {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_type_mismatch",
            "the spatial conversion plan received another LogicalValue family",
        ));
    };
    let expected_format = match plan_parameter(plan, "target_spatial_format") {
        Some("wkb") => SpatialFormat::Wkb,
        Some("ewkb") => SpatialFormat::Ewkb,
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.spatial_format_missing",
                "the spatial plan must declare WKB or EWKB format",
            ));
        }
    };
    if *format != expected_format {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_format_mismatch",
            "spatial wire format differs from the qualified target representation",
        ));
    }
    let expected_geometry = plan_parameter(plan, "target_geometry_type").ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.spatial_geometry_type_missing",
            "the spatial plan must declare geometry type",
        )
    })?;
    if !geometry_type.eq_ignore_ascii_case(expected_geometry) {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_geometry_type_mismatch",
            "spatial geometry type differs from the qualified target representation",
        ));
    }
    if let Some(source_geometry) = plan_parameter(plan, "source_geometry_type")
        && !geometry_type.eq_ignore_ascii_case(source_geometry)
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_source_geometry_type_mismatch",
            "spatial geometry type differs from the qualified source representation",
        ));
    }
    let expected_dimensions = plan_parameter(plan, "target_dimensions")
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.spatial_dimensions_missing",
                "the spatial plan must declare dimensions",
            )
        })?
        .parse::<u8>()
        .map_err(|_| {
            plan_failure(
                plan,
                "target_capability.spatial_dimensions_invalid",
                "the saved spatial dimensions are invalid",
            )
        })?;
    if *dimensions != expected_dimensions {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_dimensions_mismatch",
            "spatial dimensions differ from the qualified target representation",
        ));
    }
    if let Some(source_dimensions) = plan_parameter(plan, "source_dimensions")
        && source_dimensions != dimensions.to_string()
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_source_dimensions_mismatch",
            "spatial dimensions differ from the qualified source representation",
        ));
    }
    let expected_srid = plan_parameter(plan, "target_srid").ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.spatial_srid_missing",
            "the spatial plan must declare SRID",
        )
    })?;
    if expected_srid != "unknown" && *srid != expected_srid.parse::<i32>().ok() {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_srid_mismatch",
            "spatial SRID differs from the qualified target representation",
        ));
    }
    if let Some(source_srid) = plan_parameter(plan, "source_srid")
        && source_srid != "unknown"
        && *srid != source_srid.parse::<i32>().ok()
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_source_srid_mismatch",
            "spatial SRID differs from the qualified source representation",
        ));
    }
    let expected_crs = plan_parameter(plan, "target_crs").ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.spatial_crs_missing",
            "the spatial plan must declare CRS identity",
        )
    })?;
    if expected_crs == "unknown" || crs.as_deref() != Some(expected_crs) {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_crs_mismatch",
            "spatial CRS is missing or differs from the qualified target representation",
        ));
    }
    if let Some(source_crs) = plan_parameter(plan, "source_crs")
        && (source_crs == "unknown" || crs.as_deref() != Some(source_crs))
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_source_crs_mismatch",
            "spatial CRS is missing or differs from the qualified source representation",
        ));
    }
    if let Some(source_format) = plan_parameter(plan, "source_spatial_format")
        && ((*format == SpatialFormat::Wkb && source_format != "wkb")
            || (*format == SpatialFormat::Ewkb && source_format != "ewkb"))
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_format_mismatch",
            "the saved spatial source and target formats are not equivalent",
        ));
    }
    let bytes = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.spatial_format_invalid",
            "the spatial value is not valid base64url wire bytes",
        )
    })?;
    let wire_srid = if expected_format == SpatialFormat::Ewkb {
        *srid
    } else {
        None
    };
    validate_spatial_wire(
        plan,
        &bytes,
        expected_format,
        expected_geometry,
        expected_dimensions,
        wire_srid,
    )?;
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_spatial_wire(
    plan: &ColumnConversionPlan,
    bytes: &[u8],
    format: SpatialFormat,
    geometry_type: &str,
    dimensions: u8,
    srid: Option<i32>,
) -> Result<(), TargetCapabilityFailure> {
    if bytes.len() < 5 {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_format_invalid",
            "the spatial value has no complete WKB header",
        ));
    }
    let little_endian = match bytes[0] {
        0 => false,
        1 => true,
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.spatial_format_invalid",
                "the spatial byte-order marker is invalid",
            ));
        }
    };
    let geometry_code = read_u32_wire(&bytes[1..5], little_endian);
    let has_srid = geometry_code & 0x2000_0000 != 0;
    let has_z = geometry_code & 0x8000_0000 != 0;
    let has_m = geometry_code & 0x4000_0000 != 0;
    let mut base_code = geometry_code & 0x0fff_ffff;
    let mut actual_dimensions = 2 + u8::from(has_z) + u8::from(has_m);
    if base_code >= 1_000 {
        let suffix = base_code / 1_000;
        base_code %= 1_000;
        actual_dimensions = match suffix {
            1 | 2 => 3,
            3 => 4,
            _ => 0,
        };
    }
    let actual_geometry = match base_code {
        1 => "point",
        2 => "linestring",
        3 => "polygon",
        4 => "multipoint",
        5 => "multilinestring",
        6 => "multipolygon",
        7 => "geometrycollection",
        _ => "unknown",
    };
    if actual_geometry == "unknown"
        || !actual_geometry.eq_ignore_ascii_case(geometry_type)
        || actual_dimensions != dimensions
    {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_header_mismatch",
            "the spatial wire header does not match geometry type or dimensions",
        ));
    }
    let embedded_srid = if has_srid {
        if bytes.len() < 9 {
            return Err(plan_failure(
                plan,
                "target_capability.spatial_format_invalid",
                "the EWKB SRID header is truncated",
            ));
        }
        Some(read_u32_wire(&bytes[5..9], little_endian) as i32)
    } else {
        None
    };
    if (format == SpatialFormat::Ewkb) != has_srid {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_format_mismatch",
            "the spatial value uses a different WKB/EWKB SRID format",
        ));
    }
    if embedded_srid != srid {
        return Err(plan_failure(
            plan,
            "target_capability.spatial_srid_mismatch",
            "the embedded spatial SRID differs from the qualified target SRID",
        ));
    }
    Ok(())
}

fn read_u32_wire(bytes: &[u8], little_endian: bool) -> u32 {
    let bytes = [bytes[0], bytes[1], bytes[2], bytes[3]];
    if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    }
}

#[allow(clippy::result_large_err)]
fn validate_recursive_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let expected = plan_parameter(plan, "recursive_kind").ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.recursive_structure_rule_missing",
            "recursive values require an explicit structure mapping rule",
        )
    })?;
    let actual = match value {
        LogicalValue::Array { .. } | LogicalValue::ArrayWithMetadata { .. } => "array",
        LogicalValue::Struct { .. } => "struct",
        LogicalValue::Map { .. } => "map",
        LogicalValue::Range { .. } => "range",
        LogicalValue::MultiRange { .. } => "multi_range",
        _ => "other",
    };
    if actual != expected {
        return Err(plan_failure(
            plan,
            "target_capability.recursive_structure_mismatch",
            "the recursive value shape differs from the saved structure mapping",
        ));
    }
    if let Err(message) = validate_recursive_value(value, 0) {
        return Err(plan_failure(
            plan,
            "target_capability.recursive_structure_invalid",
            message,
        ));
    }
    Ok(())
}

fn validate_recursive_value(value: &LogicalValue, depth: usize) -> Result<(), &'static str> {
    if depth > 100 {
        return Err("recursive value exceeds the maximum structure depth");
    }
    match value {
        LogicalValue::Null => Ok(()),
        LogicalValue::Array { elements } => elements
            .iter()
            .try_for_each(|element| validate_recursive_value(element, depth + 1)),
        LogicalValue::ArrayWithMetadata {
            elements,
            dimensions,
            lower_bounds,
        } => {
            if *dimensions == 0 || usize::from(*dimensions) != lower_bounds.len() {
                return Err("array dimensions and lower bounds do not agree");
            }
            elements
                .iter()
                .try_for_each(|element| validate_recursive_value(element, depth + 1))
        }
        LogicalValue::Struct { fields } => {
            let mut names = std::collections::BTreeSet::new();
            for field in fields {
                if field.name.is_empty() || field.name.contains('\0') || !names.insert(&field.name)
                {
                    return Err("struct field names must be unique and contain no NUL");
                }
                validate_recursive_value(&field.value, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Map { entries } => {
            let mut keys = std::collections::HashSet::new();
            for entry in entries {
                if !keys.insert(&entry.key) {
                    return Err("map keys must be unique");
                }
                validate_recursive_value(&entry.key, depth + 1)?;
                validate_recursive_value(&entry.value, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Range {
            empty,
            lower,
            upper,
            ..
        } => {
            if *empty && (lower.is_some() || upper.is_some()) {
                return Err("empty ranges cannot contain bounds");
            }
            if let Some(lower) = lower {
                validate_recursive_value(lower, depth + 1)?;
            }
            if let Some(upper) = upper {
                validate_recursive_value(upper, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::MultiRange { ranges } => {
            for range in ranges {
                if !matches!(range, LogicalValue::Range { .. }) {
                    return Err("multirange entries must be ranges");
                }
                validate_recursive_value(range, depth + 1)?;
            }
            Ok(())
        }
        LogicalValue::Domain { value } => validate_recursive_value(value, depth + 1),
        LogicalValue::Raw { carrier } => carrier
            .validate()
            .map_err(|_| "raw value carrier lacks stable source evidence"),
        _ => Ok(()),
    }
}

#[allow(clippy::result_large_err)]
fn validate_json_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Json { value } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.json_type_mismatch",
            "the JSON conversion plan received a non-JSON value",
        ));
    };
    match plan
        .parameters
        .get("json_strategy")
        .map(String::as_str)
        .or_else(|| {
            plan.target
                .parameters
                .get("json_strategy")
                .map(String::as_str)
        }) {
        Some("normalized_text") => render_normalized_json(value)
            .map(|_| ())
            .map_err(|message| {
                plan_failure(plan, "target_capability.json_normalization_failed", message)
            }),
        Some("raw_text") => Err(plan_failure(
            plan,
            "target_capability.json_raw_text_unavailable",
            "the ChangeEvent does not retain source JSON spelling, whitespace, duplicate keys, or object-key order",
        )),
        Some("structured") | None => Ok(()),
        Some(other) => Err(plan_failure(
            plan,
            "target_capability.json_strategy_invalid",
            format!("unknown JSON representation strategy {other:?}"),
        )),
    }
}

#[allow(clippy::result_large_err)]
fn validate_enum_set_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let target_members = plan_members(plan, "target_members")?;
    match value {
        LogicalValue::Enum { label } => {
            if target_members.iter().any(|member| member == label) {
                Ok(())
            } else {
                Err(plan_failure(
                    plan,
                    "target_capability.enum_unknown_label",
                    "the ENUM label is not declared by the target definition",
                ))
            }
        }
        LogicalValue::Set { members } => {
            for (index, member) in members.iter().enumerate() {
                if members[..index].contains(member) {
                    return Err(plan_failure(
                        plan,
                        "target_capability.set_duplicate_member",
                        "the SET value contains a duplicate member",
                    ));
                }
                if !target_members.iter().any(|target| target == member) {
                    return Err(plan_failure(
                        plan,
                        "target_capability.set_unknown_member",
                        "the SET value contains a member not declared by the target definition",
                    ));
                }
            }
            Ok(())
        }
        _ => Err(plan_failure(
            plan,
            "target_capability.enum_set_type_mismatch",
            "the ENUM/SET conversion plan received another LogicalValue family",
        )),
    }
}

#[allow(clippy::result_large_err)]
fn plan_members(
    plan: &ColumnConversionPlan,
    name: &str,
) -> Result<Vec<String>, TargetCapabilityFailure> {
    let encoded = plan
        .target
        .parameters
        .get(name)
        .or_else(|| plan.parameters.get(name))
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.enum_set_mapping_missing",
                format!("the saved plan is missing {name}"),
            )
        })?;
    serde_json::from_str(encoded).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.enum_set_mapping_invalid",
            format!("the saved plan contains invalid {name}"),
        )
    })
}

fn plan_failure(
    plan: &ColumnConversionPlan,
    code: &str,
    message: impl Into<String>,
) -> TargetCapabilityFailure {
    let mut failure = TargetCapabilityFailure::new(message)
        .with_code(code)
        .with_route(plan.route_id.clone())
        .with_plan_digest(plan.plan_digest.clone());
    failure.source_field_lineage = Some(plan.source_field.lineage_id.clone());
    failure.target_field_lineage = Some(plan.target_field.lineage_id.clone());
    failure.source_definition_fingerprint = Some(plan.source_field.schema_fingerprint.clone());
    failure.target_definition_fingerprint = Some(plan.target_field.schema_fingerprint.clone());
    failure.target_server_build = plan.target_build.clone();
    failure.capability_identity = Some(plan.capability_code.clone());
    failure.conversion_rule = Some(plan.rule.clone());
    failure.qualification = Some(plan.qualification);
    failure.risk_code = plan.risk_code.clone();
    failure.phase = FailurePhase::Conversion;
    failure
}

#[allow(clippy::result_large_err)]
fn validate_integer_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Integer {
        signed,
        bits,
        value,
    } = value
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_type_mismatch",
            "the captured value is not an integer",
        ));
    };
    let Some(target_signed) = plan
        .target
        .parameters
        .get("target_signed")
        .and_then(|value| match value.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer range metadata is incomplete",
        ));
    };
    let Some(target_bits) = plan
        .target
        .parameters
        .get("target_bits")
        .and_then(|value| value.parse::<u8>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer target width is missing",
        ));
    };
    if !matches!(target_bits, 8 | 16 | 24 | 32 | 64) {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_invalid",
            "integer target width is not qualified",
        ));
    }
    let Some(saved_minimum) = plan
        .target
        .parameters
        .get("target_min")
        .and_then(|value| value.parse::<i128>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer lower bound is missing",
        ));
    };
    let Some(saved_maximum) = plan
        .target
        .parameters
        .get("target_max")
        .and_then(|value| value.parse::<i128>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer upper bound is missing",
        ));
    };
    let Some(source_signed) = plan
        .target
        .parameters
        .get("source_signed")
        .and_then(|value| match value.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer source signedness is missing",
        ));
    };
    let Some(source_bits) = plan
        .target
        .parameters
        .get("source_bits")
        .and_then(|value| value.parse::<u8>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_missing",
            "integer source width is missing",
        ));
    };
    if !matches!(source_bits, 8 | 16 | 24 | 32 | 64)
        || *signed != source_signed
        || *bits != source_bits
    {
        return Err(plan_failure(
            plan,
            "target_capability.integer_source_shape_mismatch",
            "captured integer metadata differs from the saved source shape",
        ));
    }
    let parsed = if *signed {
        value.parse::<i128>().map_err(|_| {
            plan_failure(
                plan,
                "target_capability.integer_invalid",
                "signed integer value is invalid",
            )
        })?
    } else {
        if value.starts_with('-') {
            return Err(plan_failure(
                plan,
                "target_capability.integer_overflow",
                "a negative value cannot be written to an unsigned target",
            ));
        }
        value.parse::<i128>().map_err(|_| {
            plan_failure(
                plan,
                "target_capability.integer_invalid",
                "unsigned integer value is invalid",
            )
        })?
    };
    let (minimum, maximum) = if target_signed {
        let magnitude = 1_i128 << (target_bits - 1);
        (-magnitude, magnitude - 1)
    } else {
        (0, (1_i128 << target_bits) - 1)
    };
    if (saved_minimum, saved_maximum) != (minimum, maximum) {
        return Err(plan_failure(
            plan,
            "target_capability.integer_range_invalid",
            "saved integer bounds do not match the target width and signedness",
        ));
    }
    if parsed < minimum || parsed > maximum {
        return Err(plan_failure(
            plan,
            "target_capability.integer_overflow",
            "integer value is outside the fixed target range",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_decimal_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Decimal { unscaled, scale } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_type_mismatch",
            "the captured value is not a Decimal",
        ));
    };
    let Some(source_precision) = plan
        .target
        .parameters
        .get("source_precision")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_missing",
            "Decimal source precision is missing",
        ));
    };
    let Some(source_scale) = plan
        .target
        .parameters
        .get("source_scale")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_missing",
            "Decimal source scale is missing",
        ));
    };
    if source_precision == 0 || source_scale > source_precision {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_invalid",
            "Decimal source precision and scale are invalid",
        ));
    }
    if *scale != source_scale {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_scale_mismatch",
            "Decimal value scale differs from the planned source scale",
        ));
    }
    let Some(target_precision) = plan
        .target
        .parameters
        .get("target_precision")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_missing",
            "Decimal target precision is missing",
        ));
    };
    let Some(target_scale) = plan
        .target
        .parameters
        .get("target_scale")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_missing",
            "Decimal target scale is missing",
        ));
    };
    let Some(target_integer_digits) = plan
        .target
        .parameters
        .get("target_integer_digits")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_missing",
            "Decimal target integer width is missing",
        ));
    };
    if target_precision == 0
        || target_scale > target_precision
        || target_integer_digits != target_precision - target_scale
    {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_range_invalid",
            "Decimal target precision and scale are invalid",
        ));
    }
    let Some(digits) = decimal_digits(unscaled) else {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_invalid",
            "Decimal unscaled value is invalid",
        ));
    };
    if *scale > target_scale {
        let discarded = scale - target_scale;
        let padded_len = digits.len().max(discarded);
        let padding = padded_len - digits.len();
        let padded = std::iter::repeat_n(b'0', padding)
            .chain(digits.iter().copied())
            .collect::<Vec<_>>();
        if padded[padded.len() - discarded..]
            .iter()
            .any(|digit| *digit != b'0')
        {
            return Err(plan_failure(
                plan,
                "target_capability.decimal_rounding_required",
                "Decimal scale reduction would require rounding",
            ));
        }
    }
    let integer_part_len = digits.len().saturating_sub(*scale);
    let integer_digits = digits[..integer_part_len]
        .iter()
        .skip_while(|digit| **digit == b'0')
        .count();
    if integer_digits > target_integer_digits {
        return Err(plan_failure(
            plan,
            "target_capability.decimal_overflow",
            "Decimal value is outside the target precision",
        ));
    }
    Ok(())
}

fn decimal_digits(value: &str) -> Option<Vec<u8>> {
    let digits = value.strip_prefix(['-', '+']).unwrap_or(value);
    (!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.as_bytes().to_vec())
}

#[allow(clippy::result_large_err)]
fn validate_float_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Float { bits, ieee754_hex } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.float_type_mismatch",
            "the captured value is not floating point",
        ));
    };
    let Some(target_bits) = plan
        .target
        .parameters
        .get("target_bits")
        .and_then(|value| value.parse::<u8>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.float_range_missing",
            "floating-point target width is missing",
        ));
    };
    let Some(source_bits) = plan
        .target
        .parameters
        .get("source_bits")
        .and_then(|value| value.parse::<u8>().ok())
    else {
        return Err(plan_failure(
            plan,
            "target_capability.float_range_missing",
            "floating-point source width is missing",
        ));
    };
    if *bits != source_bits || source_bits != 64 || target_bits != 32 || ieee754_hex.len() != 16 {
        return Err(plan_failure(
            plan,
            "target_capability.float_range_invalid",
            "floating-point range rule does not match the captured value",
        ));
    }
    let raw = u64::from_str_radix(ieee754_hex, 16).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.float_invalid",
            "floating-point bit pattern is invalid",
        )
    })?;
    let source = f64::from_bits(raw);
    let narrowed = source as f32;
    if !source.is_finite() || !narrowed.is_finite() || f64::from(narrowed) != source {
        return Err(plan_failure(
            plan,
            "target_capability.float_rounding_or_overflow",
            "floating-point conversion would round or overflow",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_text_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let LogicalValue::Text {
        charset,
        bytes_base64url,
        text,
    } = value
    else {
        return Err(plan_failure(
            plan,
            "target_capability.text_type_mismatch",
            "the captured value is not text",
        ));
    };
    let bytes = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.text_encoding_invalid",
            "the captured text bytes are not valid base64url",
        )
    })?;
    let decoded = decode_supported_text(charset, &bytes, text.as_deref()).map_err(|code| {
        plan_failure(
            plan,
            code,
            "the captured text cannot be decoded with its source charset",
        )
    })?;
    let target_charset = required_text_parameter(plan, "target_charset")?;
    let encoded = encode_supported_text(target_charset, &decoded).map_err(|code| {
        plan_failure(
            plan,
            code,
            "the text contains characters that the target charset cannot represent",
        )
    })?;
    let target_length = plan.target.parameters.get("target_length").ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.text_length_missing",
            "the target text length is missing",
        )
    })?;
    let length = if target_length.eq_ignore_ascii_case("unbounded") {
        None
    } else {
        Some(target_length.parse::<usize>().map_err(|_| {
            plan_failure(
                plan,
                "target_capability.text_length_invalid",
                "the target text length is invalid",
            )
        })?)
    };
    let unit = plan
        .target
        .parameters
        .get("target_length_unit")
        .map(String::as_str)
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.text_length_unit_missing",
                "the target text length unit is missing",
            )
        })?;
    let actual = match unit {
        "bytes" => encoded.len(),
        "characters" => decoded.chars().count(),
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.text_length_unit_invalid",
                "the target text length unit is invalid",
            ));
        }
    };
    if length.is_some_and(|limit| actual > limit) {
        return Err(plan_failure(
            plan,
            "target_capability.text_length_overflow",
            "the text exceeds the configured target length; truncation is disabled",
        ));
    }
    if plan.parameters.get("encoding_policy").map(String::as_str) != Some("strict")
        || plan.parameters.get("length_policy").map(String::as_str) != Some("reject")
        || plan.parameters.get("collation_policy").map(String::as_str) != Some("target")
    {
        return Err(plan_failure(
            plan,
            "target_capability.text_policy_missing",
            "text conversion must use strict encoding, rejection on overflow, and the saved target collation",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn required_text_parameter<'a>(
    plan: &'a ColumnConversionPlan,
    name: &str,
) -> Result<&'a str, TargetCapabilityFailure> {
    plan.target
        .parameters
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.text_parameter_missing",
                format!("text parameter {name} is missing"),
            )
        })
}

fn decode_supported_text(
    charset: &str,
    bytes: &[u8],
    declared_text: Option<&str>,
) -> Result<String, &'static str> {
    let normalized = charset.to_ascii_lowercase();
    let value = match normalized.as_str() {
        "utf8" | "utf8mb4" | "utf-8" => String::from_utf8(bytes.to_vec())
            .map_err(|_| "target_capability.text_encoding_invalid")?,
        "ascii" => {
            if bytes.iter().any(|byte| *byte > 0x7f) {
                return Err("target_capability.text_encoding_invalid");
            }
            bytes.iter().map(|byte| char::from(*byte)).collect()
        }
        "latin1" | "iso-8859-1" => bytes.iter().map(|byte| char::from(*byte)).collect(),
        _ => return Err("target_capability.text_charset_unsupported"),
    };
    if let Some(declared_text) = declared_text
        && (normalized == "utf8" || normalized == "utf8mb4" || normalized == "utf-8")
        && declared_text != value
    {
        return Err("target_capability.text_encoding_invalid");
    }
    Ok(value)
}

fn encode_supported_text(charset: &str, text: &str) -> Result<Vec<u8>, &'static str> {
    match charset.to_ascii_lowercase().as_str() {
        "utf8" | "utf-8" => {
            if text.chars().any(|character| character.len_utf8() > 3) {
                return Err("target_capability.text_character_unrepresentable");
            }
            Ok(text.as_bytes().to_vec())
        }
        "utf8mb4" => Ok(text.as_bytes().to_vec()),
        "ascii" => {
            if text.chars().any(|character| character as u32 > 0x7f) {
                return Err("target_capability.text_character_unrepresentable");
            }
            Ok(text.bytes().collect())
        }
        "latin1" | "iso-8859-1" => text
            .chars()
            .map(|character| {
                u8::try_from(character as u32)
                    .map_err(|_| "target_capability.text_character_unrepresentable")
            })
            .collect(),
        _ => Err("target_capability.text_charset_unsupported"),
    }
}

/// Render the normalized JSON strategy.  Objects are sorted by key, arrays
/// retain order, and the LogicalValue numeric kind is rendered deterministically.
/// This is intentionally not a promise to preserve source JSON spelling.
fn render_normalized_json(value: &JsonValue) -> Result<String, String> {
    match value {
        JsonValue::Null => Ok("null".into()),
        JsonValue::Boolean(value) => Ok(value.to_string()),
        JsonValue::String(value) => serde_json::to_string(value).map_err(|error| error.to_string()),
        JsonValue::SignedInteger(value) => normalized_integer(value, true),
        JsonValue::UnsignedInteger(value) => normalized_integer(value, false),
        JsonValue::DoubleBits(value) => {
            if value.len() != 16 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err("invalid JSON double bit pattern".into());
            }
            let bits = u64::from_str_radix(value, 16)
                .map_err(|_| "invalid JSON double bit pattern".to_owned())?;
            let number = f64::from_bits(bits);
            if !number.is_finite() {
                return Err("JSON text cannot represent a non-finite number".into());
            }
            if number == 0.0 && number.is_sign_negative() {
                Ok("-0.0".into())
            } else {
                serde_json::to_string(&number).map_err(|error| error.to_string())
            }
        }
        JsonValue::Decimal { unscaled, scale } => normalized_decimal(unscaled, *scale),
        JsonValue::Array(values) => values
            .iter()
            .map(render_normalized_json)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| format!("[{}]", values.join(","))),
        JsonValue::Object(entries) => {
            let mut entries = entries.clone();
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            for pair in entries.windows(2) {
                if pair[0].key == pair[1].key {
                    return Err("duplicate JSON object key".into());
                }
            }
            entries
                .iter()
                .map(|entry| {
                    let key =
                        serde_json::to_string(&entry.key).map_err(|error| error.to_string())?;
                    Ok(format!("{key}:{}", render_normalized_json(&entry.value)?))
                })
                .collect::<Result<Vec<_>, String>>()
                .map(|entries| format!("{{{}}}", entries.join(",")))
        }
    }
}

fn normalized_integer(value: &str, signed: bool) -> Result<String, String> {
    let digits = value.strip_prefix(['-', '+']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid JSON integer".into());
    }
    if !signed && value.starts_with('-') {
        return Err("unsigned JSON integer is negative".into());
    }
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    Ok(format!(
        "{}{}",
        if value.starts_with('-') { "-" } else { "" },
        digits
    ))
}

fn normalized_decimal(unscaled: &str, scale: usize) -> Result<String, String> {
    let digits = unscaled.strip_prefix(['-', '+']).unwrap_or(unscaled);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid JSON decimal".into());
    }
    let sign = if unscaled.starts_with('-') { "-" } else { "" };
    if scale == 0 {
        let whole = digits.trim_start_matches('0');
        return Ok(format!(
            "{sign}{}",
            if whole.is_empty() { "0" } else { whole }
        ));
    }
    let split = digits.len().saturating_sub(scale);
    let (whole, fraction) = if split == 0 {
        (
            "0",
            format!("{}{}", "0".repeat(scale - digits.len()), digits),
        )
    } else {
        (&digits[..split], digits[split..].to_owned())
    };
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    // Preserve the declared decimal scale.  JSON has no separate decimal
    // type, but dropping trailing zeroes would erase the exact Decimal value
    // profile carried by the ChangeEvent and make the risk invisible.
    Ok(format!("{sign}{whole}.{fraction}"))
}

#[allow(clippy::result_large_err)]
fn validate_temporal_plan_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<(), TargetCapabilityFailure> {
    let source_kind = plan
        .target
        .parameters
        .get("source_temporal_kind")
        .map(String::as_str)
        .unwrap_or_default();
    let target_kind = plan
        .target
        .parameters
        .get("target_temporal_kind")
        .map(String::as_str)
        .unwrap_or_default();
    let target_precision = plan
        .target
        .parameters
        .get("target_precision")
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|precision| *precision <= 6)
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.temporal_precision_invalid",
                "the target temporal precision is missing or invalid",
            )
        })?;
    if plan.parameters.get("precision_policy").map(String::as_str) != Some("reject") {
        return Err(plan_failure(
            plan,
            "target_capability.temporal_precision_policy_missing",
            "temporal precision conversion must reject non-zero discarded precision",
        ));
    }
    match (source_kind, target_kind, value) {
        ("local_datetime", "local_datetime", LogicalValue::LocalDatetime { microsecond, .. }) => {
            validate_microsecond_precision(plan, *microsecond, target_precision)
        }
        ("instant", "instant", LogicalValue::Instant { nanoseconds, .. }) => {
            validate_nanosecond_precision(plan, *nanoseconds, target_precision)
        }
        ("duration", "duration", LogicalValue::Duration { microsecond, .. }) => {
            validate_microsecond_precision(plan, *microsecond, target_precision)
        }
        ("local_datetime", "instant", LogicalValue::LocalDatetime { microsecond, .. }) => {
            validate_microsecond_precision(plan, *microsecond, target_precision)?;
            parse_timezone_offset(plan).map(|_| ())
        }
        ("instant", "local_datetime", LogicalValue::Instant { nanoseconds, .. }) => {
            validate_nanosecond_precision(plan, *nanoseconds, target_precision)?;
            parse_timezone_offset(plan).map(|_| ())
        }
        (_, _, _) => Err(plan_failure(
            plan,
            "target_capability.temporal_type_mismatch",
            "the captured temporal value does not match the saved conversion strategy",
        )),
    }
}

#[allow(clippy::result_large_err)]
fn validate_microsecond_precision(
    plan: &ColumnConversionPlan,
    microsecond: u32,
    precision: u8,
) -> Result<(), TargetCapabilityFailure> {
    let factor = 10_u32.pow(u32::from(6 - precision));
    if !microsecond.is_multiple_of(factor) {
        return Err(plan_failure(
            plan,
            "target_capability.temporal_precision_loss",
            "temporal conversion would discard non-zero fractional precision",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_nanosecond_precision(
    plan: &ColumnConversionPlan,
    nanoseconds: u32,
    precision: u8,
) -> Result<(), TargetCapabilityFailure> {
    let factor = 10_u32.pow(u32::from(9 - precision));
    if !nanoseconds.is_multiple_of(factor) {
        return Err(plan_failure(
            plan,
            "target_capability.temporal_precision_loss",
            "temporal conversion would discard non-zero fractional precision",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn parse_timezone_offset(plan: &ColumnConversionPlan) -> Result<i32, TargetCapabilityFailure> {
    let value = plan
        .parameters
        .get("time_zone")
        .or_else(|| plan.parameters.get("timezone"))
        .map(String::as_str)
        .ok_or_else(|| {
            plan_failure(
                plan,
                "target_capability.timezone_missing",
                "a fixed source time zone is required for local/absolute conversion",
            )
        })?;
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return Ok(0);
    }
    let sign = match value.as_bytes().first() {
        Some(b'+') => 1_i32,
        Some(b'-') => -1_i32,
        _ => {
            return Err(plan_failure(
                plan,
                "target_capability.timezone_invalid",
                "time zone must be UTC or a fixed offset such as +08:00",
            ));
        }
    };
    let (hours, minutes) = value[1..].split_once(':').ok_or_else(|| {
        plan_failure(
            plan,
            "target_capability.timezone_invalid",
            "time zone must use a fixed +HH:MM or -HH:MM offset",
        )
    })?;
    let hours = hours.parse::<i32>().map_err(|_| {
        plan_failure(
            plan,
            "target_capability.timezone_invalid",
            "time zone hour is invalid",
        )
    })?;
    let minutes = minutes.parse::<i32>().map_err(|_| {
        plan_failure(
            plan,
            "target_capability.timezone_invalid",
            "time zone minute is invalid",
        )
    })?;
    if hours > 23 || minutes > 59 {
        return Err(plan_failure(
            plan,
            "target_capability.timezone_invalid",
            "time zone offset is outside its qualified range",
        ));
    }
    Ok(sign * (hours * 3_600 + minutes * 60))
}

/// Apply the exact saved plans to a complete source transaction before a Sink
/// renders SQL.  The function returns a new transaction so the capture event
/// and the persisted plan remain immutable and every retry starts from the
/// same source values.
#[allow(clippy::result_large_err)]
pub fn convert_transaction_with_plans(
    mut transaction: ChangeTransaction,
    plans: &[ColumnConversionPlan],
) -> Result<ChangeTransaction, TargetCapabilityFailure> {
    for change in &mut transaction.changes {
        convert_image_with_plans(change, true, plans)?;
        convert_image_with_plans(change, false, plans)?;
    }
    Ok(transaction)
}

#[allow(clippy::result_large_err)]
fn convert_image_with_plans(
    change: &mut RowChange,
    before: bool,
    plans: &[ColumnConversionPlan],
) -> Result<(), TargetCapabilityFailure> {
    let Some(image) = (if before {
        change.before.as_mut()
    } else {
        change.after.as_mut()
    }) else {
        return Ok(());
    };
    for column in image {
        let lineage = format!("catalog:{}.{}.{}", change.schema, change.table, column.name);
        let Some(plan) = plans
            .iter()
            .find(|plan| plan.source_field.lineage_id == lineage)
        else {
            // The task worker validates that every active field has a plan;
            // this public helper also remains useful for a single-field
            // conversion fixture and therefore leaves unrelated fields alone.
            continue;
        };
        if column.generated {
            continue;
        }
        column.datum = match &column.datum {
            Datum::Value(value) => Datum::Value(convert_value_with_plan(plan, value)?),
            Datum::Null => Datum::Null,
            Datum::Unchanged => Datum::Unchanged,
            Datum::Unavailable => {
                return Err(plan_failure(
                    plan,
                    "target_capability.unavailable_value",
                    "an unavailable source value cannot be written by a conversion plan",
                ));
            }
        };
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn convert_value_with_plan(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    validate_value_against_plan(plan, value)?;
    match plan
        .target
        .parameters
        .get("conversion_kind")
        .map(String::as_str)
    {
        Some("binary") => convert_binary_value(plan, value),
        Some("bit_string") | Some("spatial") | Some("recursive") => Ok(value.clone()),
        Some("text") => convert_text_value(plan, value),
        Some("json") => convert_json_value(plan, value),
        Some("enum") | Some("set") => convert_enum_set_value(plan, value),
        Some("temporal") => convert_temporal_value(plan, value),
        _ => match plan
            .target
            .parameters
            .get("value_strategy")
            .map(String::as_str)
        {
            Some("structured_json") => Ok(value.clone()),
            Some("enum_label") | Some("set_members") => convert_enum_set_value(plan, value),
            _ => Ok(value.clone()),
        },
    }
}

#[allow(clippy::result_large_err)]
fn convert_binary_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    let LogicalValue::Binary { bytes_base64url } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.binary_type_mismatch",
            "the binary conversion plan received a non-binary value",
        ));
    };
    let mut bytes = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.binary_format_invalid",
            "the binary value is not valid base64url bytes",
        )
    })?;
    if plan_parameter(plan, "target_fixed") == Some("true")
        && let Some(length) = plan_parameter(plan, "target_length")
        && length != "unbounded"
    {
        let length = length.parse::<usize>().map_err(|_| {
            plan_failure(
                plan,
                "target_capability.binary_length_invalid",
                "the saved binary target length is invalid",
            )
        })?;
        let pad = match plan_parameter(plan, "target_padding") {
            Some("zero") => 0,
            Some("one") => u8::MAX,
            _ => {
                return Err(plan_failure(
                    plan,
                    "target_capability.binary_padding_invalid",
                    "fixed binary padding must be explicitly zero or one",
                ));
            }
        };
        if bytes.len() > length {
            return Err(plan_failure(
                plan,
                "target_capability.binary_length_out_of_range",
                "binary value exceeds the qualified target length",
            ));
        }
        bytes.resize(length, pad);
    }
    Ok(LogicalValue::Binary {
        bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
    })
}

#[allow(clippy::result_large_err)]
fn convert_json_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    let LogicalValue::Json { value } = value else {
        return Err(plan_failure(
            plan,
            "target_capability.json_type_mismatch",
            "the JSON conversion plan received a non-JSON value",
        ));
    };
    let strategy = plan
        .parameters
        .get("json_strategy")
        .or_else(|| plan.target.parameters.get("json_strategy"))
        .map(String::as_str)
        .unwrap_or("structured");
    if strategy == "raw_text" {
        return Err(plan_failure(
            plan,
            "target_capability.json_raw_text_unavailable",
            "raw source JSON text is not retained by the ChangeEvent contract",
        ));
    }
    if strategy != "normalized_text" {
        return Err(plan_failure(
            plan,
            "target_capability.json_strategy_invalid",
            "only normalized_text is valid for a JSON-to-text conversion plan",
        ));
    }
    let normalized = render_normalized_json(value).map_err(|message| {
        plan_failure(plan, "target_capability.json_normalization_failed", message)
    })?;
    let target_charset = required_text_parameter(plan, "target_charset")?;
    let encoded = encode_supported_text(target_charset, &normalized).map_err(|code| {
        plan_failure(
            plan,
            code,
            "the normalized JSON cannot be represented by the target charset",
        )
    })?;
    Ok(LogicalValue::Text {
        charset: target_charset.to_owned(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(encoded),
        text: Some(normalized),
    })
}

#[allow(clippy::result_large_err)]
fn convert_enum_set_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    let target_members = plan_members(plan, "target_members")?;
    match value {
        LogicalValue::Enum { label } => {
            if target_members.iter().any(|member| member == label) {
                Ok(LogicalValue::Enum {
                    label: label.clone(),
                })
            } else {
                Err(plan_failure(
                    plan,
                    "target_capability.enum_unknown_label",
                    "the ENUM label is not declared by the target definition",
                ))
            }
        }
        LogicalValue::Set { members } => {
            validate_enum_set_plan_value(plan, value)?;
            // SET is a mathematical member collection.  Canonicalizing in
            // target declaration order makes order differences explicit while
            // preserving the set rather than binding comma-delimited text.
            let ordered = target_members
                .into_iter()
                .filter(|target| members.iter().any(|member| member == target))
                .collect();
            Ok(LogicalValue::Set { members: ordered })
        }
        _ => Err(plan_failure(
            plan,
            "target_capability.enum_set_type_mismatch",
            "the ENUM/SET conversion plan received another LogicalValue family",
        )),
    }
}

#[allow(clippy::result_large_err)]
fn convert_text_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    let LogicalValue::Text {
        charset,
        bytes_base64url,
        text,
    } = value
    else {
        return Err(plan_failure(
            plan,
            "target_capability.text_type_mismatch",
            "the captured value is not text",
        ));
    };
    let bytes = URL_SAFE_NO_PAD.decode(bytes_base64url).map_err(|_| {
        plan_failure(
            plan,
            "target_capability.text_encoding_invalid",
            "the captured text bytes are not valid base64url",
        )
    })?;
    let decoded = decode_supported_text(charset, &bytes, text.as_deref())
        .map_err(|code| plan_failure(plan, code, "the captured text cannot be decoded"))?;
    let target_charset = required_text_parameter(plan, "target_charset")?;
    let encoded = encode_supported_text(target_charset, &decoded)
        .map_err(|code| plan_failure(plan, code, "the target charset cannot represent the text"))?;
    Ok(LogicalValue::Text {
        charset: target_charset.to_owned(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(encoded),
        text: Some(decoded),
    })
}

#[allow(clippy::result_large_err)]
fn convert_temporal_value(
    plan: &ColumnConversionPlan,
    value: &LogicalValue,
) -> Result<LogicalValue, TargetCapabilityFailure> {
    let source_kind = plan
        .target
        .parameters
        .get("source_temporal_kind")
        .map(String::as_str)
        .unwrap_or_default();
    let target_kind = plan
        .target
        .parameters
        .get("target_temporal_kind")
        .map(String::as_str)
        .unwrap_or_default();
    match (source_kind, target_kind, value) {
        ("local_datetime", "local_datetime", LogicalValue::LocalDatetime { .. })
        | ("instant", "instant", LogicalValue::Instant { .. })
        | ("duration", "duration", LogicalValue::Duration { .. }) => Ok(value.clone()),
        (
            "local_datetime",
            "instant",
            LogicalValue::LocalDatetime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
            },
        ) => {
            let offset = parse_timezone_offset(plan)?;
            let days = days_from_civil(i32::from(*year), u32::from(*month), u32::from(*day));
            let seconds = days * 86_400
                + i64::from(*hour) * 3_600
                + i64::from(*minute) * 60
                + i64::from(*second)
                - i64::from(offset);
            Ok(LogicalValue::Instant {
                unix_seconds: seconds.to_string(),
                nanoseconds: *microsecond * 1_000,
            })
        }
        (
            "instant",
            "local_datetime",
            LogicalValue::Instant {
                unix_seconds,
                nanoseconds,
            },
        ) => {
            let offset = parse_timezone_offset(plan)?;
            let seconds = unix_seconds.parse::<i64>().map_err(|_| {
                plan_failure(
                    plan,
                    "target_capability.temporal_value_invalid",
                    "instant seconds are invalid",
                )
            })? + i64::from(offset);
            let days = seconds.div_euclid(86_400);
            let day_seconds = seconds.rem_euclid(86_400);
            let (year, month, day) = civil_from_days(days);
            if !(0..=u16::MAX as i32).contains(&year) || year == 0 {
                return Err(plan_failure(
                    plan,
                    "target_capability.temporal_value_invalid",
                    "the converted local datetime is outside the target range",
                ));
            }
            Ok(LogicalValue::LocalDatetime {
                year: year as u16,
                month: month as u8,
                day: day as u8,
                hour: (day_seconds / 3_600) as u8,
                minute: ((day_seconds % 3_600) / 60) as u8,
                second: (day_seconds % 60) as u8,
                microsecond: *nanoseconds / 1_000,
            })
        }
        _ => Err(plan_failure(
            plan,
            "target_capability.temporal_type_mismatch",
            "the captured temporal value does not match the saved conversion strategy",
        )),
    }
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    (
        year as i32 + i32::from(month <= 2),
        month as u32,
        day as u32,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnConversionPlan {
    pub format: String,
    pub route_id: String,
    pub configuration_revision: String,
    pub source_field: DefinitionReference,
    pub target_field: DefinitionReference,
    pub source_connector: ConnectorIdentity,
    pub sink_connector: ConnectorIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_build: Option<ServerBuildIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_build: Option<ServerBuildIdentity>,
    pub source_mapping_id: String,
    pub source_mapping_version: String,
    pub target: TargetRepresentation,
    pub rule: RuleReference,
    #[serde(default)]
    pub rule_digest: String,
    pub capability_code: String,
    pub capability_manifest_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_probe_digest: Option<String>,
    pub qualification: QualificationLevel,
    pub risk: RiskLevel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_code: Option<String>,
    #[serde(default)]
    pub loss: LossAssessment,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<ConversionExample>,
    #[serde(default)]
    pub locator_impact: LocatorImpact,
    #[serde(default)]
    pub confirmation: PlanConfirmationState,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    pub failure_policy: FailurePolicy,
    pub input_digest: String,
    pub plan_digest: String,
}

impl ColumnConversionPlan {
    pub fn verify_digest(&self) -> bool {
        self.plan_digest == self.computed_digest()
    }

    pub fn computed_digest(&self) -> String {
        digest_of(&PlanDigestInput {
            format: &self.format,
            route_id: &self.route_id,
            configuration_revision: &self.configuration_revision,
            source_field: &self.source_field,
            target_field: &self.target_field,
            source_connector: &self.source_connector,
            sink_connector: &self.sink_connector,
            source_build: &self.source_build,
            target_build: &self.target_build,
            source_mapping_id: &self.source_mapping_id,
            source_mapping_version: &self.source_mapping_version,
            target: &self.target,
            rule: &self.rule,
            rule_digest: &self.rule_digest,
            capability_code: &self.capability_code,
            capability_manifest_digest: &self.capability_manifest_digest,
            target_probe_digest: &self.target_probe_digest,
            qualification: self.qualification,
            risk: self.risk,
            risk_code: &self.risk_code,
            loss: &self.loss,
            examples: &self.examples,
            locator_impact: self.locator_impact,
            parameters: &self.parameters,
            failure_policy: self.failure_policy,
            input_digest: &self.input_digest,
        })
    }

    pub fn confirmation_matches(&self, confirmation: &RiskConfirmation) -> bool {
        confirmation.source_field_lineage == self.source_field.lineage_id
            && confirmation.target_field_lineage == self.target_field.lineage_id
            && confirmation.rule == self.rule
            && confirmation.plan_digest == self.plan_digest
    }

    /// Rebuild the plan from current immutable inputs and reject any changed
    /// fingerprint, connector, capability, rule, option, or transaction shape.
    pub fn validate_against(
        &self,
        input: CompatibilityInput<'_>,
    ) -> Result<(), CompatibilityError> {
        if !self.verify_digest() {
            return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
                FailureClass::StaleInput,
                "compatibility.plan_digest_mismatch",
                FailurePhase::PlanConstruction,
                "the stored ColumnConversionPlan digest is invalid",
            )));
        }
        let current_probe_digest = input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone());
        if self.target_probe_digest != current_probe_digest {
            return Err(CompatibilityError::PlanInvalidated(
                CompatibilityFailure::new(
                    FailureClass::StaleInput,
                    "compatibility.plan_inputs_changed",
                    FailurePhase::PlanConstruction,
                    "target capability, column, extension, or session evidence changed; requalification is required",
                ),
            ));
        }
        let result = explain_compatibility(input)?;
        let Some(current) = result.plan else {
            return Err(CompatibilityError::PlanInvalidated(
                CompatibilityFailure::new(
                    FailureClass::StaleInput,
                    "compatibility.plan_no_longer_qualifies",
                    FailurePhase::PlanConstruction,
                    "the stored ColumnConversionPlan no longer qualifies for current inputs",
                ),
            ));
        };
        if current.plan_digest != self.plan_digest {
            return Err(CompatibilityError::PlanInvalidated(
                CompatibilityFailure::new(
                    FailureClass::StaleInput,
                    "compatibility.plan_inputs_changed",
                    FailurePhase::PlanConstruction,
                    "ColumnConversionPlan inputs changed; requalification is required",
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct PlanDigestInput<'a> {
    format: &'a str,
    route_id: &'a str,
    configuration_revision: &'a str,
    source_field: &'a DefinitionReference,
    target_field: &'a DefinitionReference,
    source_connector: &'a ConnectorIdentity,
    sink_connector: &'a ConnectorIdentity,
    source_build: &'a Option<ServerBuildIdentity>,
    target_build: &'a Option<ServerBuildIdentity>,
    source_mapping_id: &'a str,
    source_mapping_version: &'a str,
    target: &'a TargetRepresentation,
    rule: &'a RuleReference,
    rule_digest: &'a str,
    capability_code: &'a str,
    capability_manifest_digest: &'a str,
    target_probe_digest: &'a Option<String>,
    qualification: QualificationLevel,
    risk: RiskLevel,
    risk_code: &'a Option<String>,
    loss: &'a LossAssessment,
    examples: &'a [ConversionExample],
    locator_impact: LocatorImpact,
    parameters: &'a BTreeMap<String, String>,
    failure_policy: FailurePolicy,
    input_digest: &'a str,
}

#[derive(Serialize)]
struct TransactionShape<'a> {
    source: &'a crate::Source,
    id: &'a str,
    begin_cursor: &'a crate::SourceCursor,
    commit_cursor: &'a crate::SourceCursor,
    changes: Vec<ChangeShape<'a>>,
}

#[derive(Serialize)]
struct ChangeShape<'a> {
    operation: Operation,
    database: &'a Option<String>,
    schema: &'a str,
    table: &'a str,
    source_cursor: &'a crate::SourceCursor,
    source_timestamp: u32,
    schema_basis: &'a str,
}

#[derive(Serialize)]
struct InputDigest<'a> {
    transaction: TransactionShape<'a>,
    source_field: &'a FieldDefinition,
    target_field: &'a FieldDefinition,
    mapping: &'a SourceTypeMapping,
    source_connector: &'a ConnectorIdentity,
    sink_connector: &'a ConnectorIdentity,
    source_build: &'a Option<ServerBuildIdentity>,
    target_build: &'a Option<ServerBuildIdentity>,
    manifest_digest: &'a str,
    target_probe_digest: &'a Option<String>,
    options: RouteOptionsDigest<'a>,
    candidate: &'a TargetTypeCandidate,
}

#[derive(Serialize)]
struct RouteOptionsDigest<'a> {
    route_id: &'a str,
    configuration_revision: &'a str,
    selected_rule: &'a Option<RuleReference>,
    parameters: &'a BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompatibilityError {
    InvalidInput(CompatibilityFailure),
    SourceContract(CompatibilityFailure),
    TargetCapability(Box<TargetCapabilityFailure>),
    StaleInput(CompatibilityFailure),
    PlanInvalidated(CompatibilityFailure),
}

impl CompatibilityError {
    pub fn class(&self) -> FailureClass {
        match self {
            Self::InvalidInput(failure)
            | Self::SourceContract(failure)
            | Self::StaleInput(failure)
            | Self::PlanInvalidated(failure) => failure.class,
            Self::TargetCapability(_) => FailureClass::TargetCapability,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInput(failure)
            | Self::SourceContract(failure)
            | Self::StaleInput(failure)
            | Self::PlanInvalidated(failure) => &failure.code,
            Self::TargetCapability(failure) => &failure.code,
        }
    }
}

impl fmt::Display for CompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(failure)
            | Self::SourceContract(failure)
            | Self::StaleInput(failure)
            | Self::PlanInvalidated(failure) => f.write_str(&failure.message),
            Self::TargetCapability(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for CompatibilityError {}

/// Explain and, when possible, build the immutable plan for one field binding.
///
/// Unsupported capabilities are an ordinary structured result rather than a
/// Rust error: callers need to render them in a preview. Rust errors represent
/// malformed inputs or an invalid/stale manifest.
pub fn explain_compatibility(
    input: CompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    validate_input(&input)?;
    let presences = transaction_presences(&input);
    let operations = input
        .transaction
        .transaction()
        .changes
        .iter()
        .map(|change| change.operation)
        .collect::<Vec<_>>();

    let transaction_has_key = input
        .transaction
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
        .any(|column| column.primary_key_ordinal.is_some());
    if input.manifest.requires_primary_key && !transaction_has_key {
        let code = "target_capability.primary_key_required";
        let message = "the MySQL 8.0 Sink requires a primary-key Row Locator";
        return Ok(CompatibilityResult {
            source_field: input.source_field.reference.clone(),
            target_field: input.target_field.reference.clone(),
            status: CompatibilityStatus::Blocked,
            qualification: QualificationLevel::Unsupported,
            risk: RiskLevel::Critical,
            reason_code: code.to_owned(),
            explanation: message.to_owned(),
            requires_confirmation: false,
            rule: None,
            plan: None,
            candidates: Vec::new(),
            failure: Some(CompatibilityFailure::new(
                FailureClass::TargetCapability,
                code,
                FailurePhase::CapabilityQualification,
                message,
            )),
            summary: None,
        });
    }

    let mut candidates = input
        .manifest
        .capabilities
        .iter()
        .filter(|capability| {
            capability_matches_source(capability, &input.source_field.logical_type)
                && capability.rule.qualification != QualificationLevel::Unsupported
        })
        .filter(|capability| {
            probe_qualifies_capability(input.options.target_probe.as_ref(), capability)
        })
        .filter(|capability| target_representation_matches_binding(capability, &input.target_field))
        .filter(|capability| {
            input.options.selected_rule.as_ref().is_none_or(|selected| {
                selected.id == capability.rule.id && selected.version == capability.rule.version
            })
        })
        .filter(|capability| {
            supports_declared(
                &capability.supported_operations,
                &capability.rule.supported_operations,
                &operations,
            )
        })
        .filter(|capability| {
            supports_declared(
                &capability.supported_presence,
                &capability.rule.supported_presence,
                &presences,
            )
        })
        .filter(|capability| !(is_key_like(&input.source_field) && !capability.rule.allows_key))
        .filter(|capability| {
            exact_candidate_matches_binding(capability, &input.source_field, &input.target_field)
        })
        .filter(|capability| {
            explicit_template_matches_binding(
                capability,
                &input.source_field,
                &input.target_field,
                input.options.selected_rule.as_ref(),
            )
        })
        .filter(|_| input.source_field.generated == input.target_field.generated)
        .filter(|_| !(input.source_field.nullable && !input.target_field.nullable))
        .filter(|capability| {
            !is_range_template(capability)
                || input.source_field.logical_type != input.target_field.logical_type
        })
        .map(|capability| TargetTypeCandidate {
            target: candidate_target(capability, &input.target_field),
            rule: capability.rule.reference(),
            capability_code: capability.code.clone(),
            qualification: capability.rule.qualification,
            risk: capability.rule.risk,
            risk_code: capability.rule.risk_code.clone(),
            requires_confirmation: capability.rule.requires_confirmation
                || capability.rule.qualification == QualificationLevel::ExplicitConversion,
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        (left.qualification, left.risk, &left.capability_code).cmp(&(
            right.qualification,
            right.risk,
            &right.capability_code,
        ))
    });

    if candidates.is_empty() {
        let key_blocked = (input.manifest.requires_primary_key && !transaction_has_key)
            || is_key_like(&input.source_field)
                && input.manifest.capabilities.iter().any(|capability| {
                    capability_matches_source(capability, &input.source_field.logical_type)
                        && target_representation_matches_binding(capability, &input.target_field)
                        && !capability.rule.allows_key
                });
        let (status, code, message) = if key_blocked {
            (
                CompatibilityStatus::Blocked,
                "target_capability.lossy_key_conversion",
                "a key or Row Locator field cannot use a lossy conversion",
            )
        } else {
            match &input.source_field.logical_type {
                LogicalType::Binary { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.binary_representation_missing",
                    "no qualified raw-byte target exists; binary values are never silently encoded as text",
                ),
                LogicalType::BitString { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.bit_string_representation_missing",
                    "no qualified bit-string target declares length, padding, and bit order",
                ),
                LogicalType::Spatial { .. } => (
                    CompatibilityStatus::Blocked,
                    "target_capability.spatial_metadata_or_capability_missing",
                    "spatial conversion requires qualified SRID, CRS, geometry type, dimensions, format, and target capability",
                ),
                LogicalType::Array { .. }
                | LogicalType::Struct { .. }
                | LogicalType::Map { .. }
                | LogicalType::Range { .. }
                | LogicalType::MultiRange { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.recursive_structure_unqualified",
                    "recursive or structured values require an explicit element/field/key/range mapping rule",
                ),
                _ => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.no_qualified_representation",
                    "no qualified target representation matches this source semantic",
                ),
            }
        };
        return Ok(CompatibilityResult {
            source_field: input.source_field.reference.clone(),
            target_field: input.target_field.reference.clone(),
            status,
            qualification: QualificationLevel::Unsupported,
            risk: if key_blocked {
                RiskLevel::Critical
            } else {
                RiskLevel::High
            },
            reason_code: code.to_owned(),
            explanation: message.to_owned(),
            requires_confirmation: false,
            rule: None,
            plan: None,
            candidates,
            failure: Some(CompatibilityFailure::new(
                FailureClass::TargetCapability,
                code,
                FailurePhase::CapabilityQualification,
                message,
            )),
            summary: None,
        });
    }

    if candidates.len() > 1 {
        return Ok(CompatibilityResult {
            source_field: input.source_field.reference.clone(),
            target_field: input.target_field.reference.clone(),
            status: CompatibilityStatus::NeedsConfiguration,
            qualification: candidates[0].qualification,
            risk: candidates[0].risk,
            reason_code: "target_capability.multiple_candidates".to_owned(),
            explanation:
                "multiple qualified target representations require an explicit rule selection"
                    .to_owned(),
            requires_confirmation: false,
            rule: None,
            plan: None,
            candidates,
            failure: Some(CompatibilityFailure::new(
                FailureClass::InvalidInput,
                "target_capability.multiple_candidates",
                FailurePhase::PlanConstruction,
                "multiple qualified target representations require an explicit rule selection",
            )),
            summary: None,
        });
    }

    let candidate = candidates.into_iter().next().expect("candidate exists");
    let capability = input
        .manifest
        .capabilities
        .iter()
        .find(|capability| capability.code == candidate.capability_code)
        .expect("candidate came from manifest");
    let (parameters, missing, mut invalid) =
        normalize_options(&capability.rule, &input.options.parameters);
    if missing.is_empty()
        && let Err(reason) = validate_explicit_parameters(
            &capability.target,
            &input.source_field,
            &input.target_field,
            &parameters,
        )
    {
        invalid.push(reason);
    }
    if !invalid.is_empty() {
        let message = format!("conversion parameters are invalid: {}", invalid.join(", "));
        return Ok(CompatibilityResult {
            source_field: input.source_field.reference.clone(),
            target_field: input.target_field.reference.clone(),
            status: CompatibilityStatus::Blocked,
            qualification: candidate.qualification,
            risk: candidate.risk,
            reason_code: "target_capability.invalid_parameters".to_owned(),
            explanation: message.clone(),
            requires_confirmation: candidate.requires_confirmation,
            rule: Some(candidate.rule.clone()),
            plan: None,
            candidates: vec![candidate],
            failure: Some(CompatibilityFailure::new(
                FailureClass::InvalidInput,
                "target_capability.invalid_parameters",
                FailurePhase::PlanConstruction,
                message.clone(),
            )),
            summary: None,
        });
    }
    if !missing.is_empty() {
        let names = missing.join(", ");
        return Ok(CompatibilityResult {
            source_field: input.source_field.reference.clone(),
            target_field: input.target_field.reference.clone(),
            status: CompatibilityStatus::NeedsConfiguration,
            qualification: candidate.qualification,
            risk: candidate.risk,
            reason_code: "target_capability.missing_parameters".to_owned(),
            explanation: format!("required conversion parameters are missing: {names}"),
            requires_confirmation: candidate.requires_confirmation,
            rule: Some(candidate.rule.clone()),
            plan: None,
            candidates: vec![candidate],
            failure: Some(CompatibilityFailure::new(
                FailureClass::MissingConfirmation,
                "target_capability.missing_parameters",
                FailurePhase::PlanConstruction,
                format!("required conversion parameters are missing: {names}"),
            )),
            summary: None,
        });
    }

    let mut normalized_input = input.clone();
    normalized_input.options.parameters = parameters;
    let mut plan = build_plan(&normalized_input, &candidate, &capability.rule);
    let confirmed = candidate.requires_confirmation
        && input
            .options
            .confirmations
            .iter()
            .any(|confirmation| plan.confirmation_matches(confirmation));
    if confirmed {
        plan.confirmation = PlanConfirmationState::Confirmed;
    }
    let needs_confirmation = candidate.requires_confirmation && !confirmed;
    let status = if needs_confirmation {
        CompatibilityStatus::NeedsConfirmation
    } else {
        CompatibilityStatus::Compatible
    };
    let (reason_code, explanation, failure) = if needs_confirmation {
        (
            "target_capability.confirmation_required".to_owned(),
            "this deterministic conversion requires per-field risk confirmation".to_owned(),
            Some(CompatibilityFailure::new(
                FailureClass::MissingConfirmation,
                "target_capability.confirmation_required",
                FailurePhase::PlanConstruction,
                "this deterministic conversion requires per-field risk confirmation",
            )),
        )
    } else {
        (
            "target_capability.qualified".to_owned(),
            "the source and target field binding has a qualified conversion plan".to_owned(),
            None,
        )
    };
    let summary = PlanSummary {
        input_digest: plan.input_digest.clone(),
        plan_digest: plan.plan_digest.clone(),
        rule: plan.rule.clone(),
        capability_code: plan.capability_code.clone(),
    };
    Ok(CompatibilityResult {
        source_field: input.source_field.reference.clone(),
        target_field: input.target_field.reference.clone(),
        status,
        qualification: candidate.qualification,
        risk: candidate.risk,
        reason_code,
        explanation,
        requires_confirmation: candidate.requires_confirmation,
        rule: Some(candidate.rule.clone()),
        plan: Some(plan),
        candidates: vec![candidate],
        failure,
        summary: Some(summary),
    })
}

pub fn plan_compatibility(
    input: CompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    explain_compatibility(input)
}

/// Explain and, when possible, build a plan from catalog definitions alone.
///
/// This is the Web/activation counterpart of [`explain_compatibility`].  The
/// runtime path should use the transaction-bearing function so event values
/// are checked as well; the catalog path never fabricates a row value merely
/// to satisfy the planner.
pub fn explain_field_compatibility(
    input: FieldCompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    validate_field_input(&input)?;
    if input.manifest.requires_primary_key && !input.source_has_primary_key {
        return Ok(field_result(
            &input,
            CompatibilityStatus::Blocked,
            QualificationLevel::Unsupported,
            RiskLevel::Critical,
            "target_capability.primary_key_required",
            "the selected Sink requires a primary-key Row Locator",
            Vec::new(),
            None,
            Some(CompatibilityFailure::new(
                FailureClass::TargetCapability,
                "target_capability.primary_key_required",
                FailurePhase::CapabilityQualification,
                "the selected Sink requires a primary-key Row Locator",
            )),
        ));
    }
    if input.source_field.primary_key_ordinal != input.target_field.primary_key_ordinal {
        return Ok(field_result(
            &input,
            CompatibilityStatus::Blocked,
            QualificationLevel::Unsupported,
            RiskLevel::Critical,
            "target_capability.primary_key_mismatch",
            "source and target Row Locator key positions are not equivalent",
            Vec::new(),
            None,
            Some(CompatibilityFailure::new(
                FailureClass::TargetCapability,
                "target_capability.primary_key_mismatch",
                FailurePhase::CapabilityQualification,
                "source and target Row Locator key positions are not equivalent",
            )),
        ));
    }

    let mut candidates = input
        .manifest
        .capabilities
        .iter()
        .filter(|capability| {
            capability_matches_source(capability, &input.source_field.logical_type)
                && capability.rule.qualification != QualificationLevel::Unsupported
        })
        .filter(|capability| {
            probe_qualifies_capability(input.options.target_probe.as_ref(), capability)
        })
        .filter(|capability| target_representation_matches_binding(capability, &input.target_field))
        .filter(|capability| {
            input.options.selected_rule.as_ref().is_none_or(|selected| {
                selected.id == capability.rule.id && selected.version == capability.rule.version
            })
        })
        .filter(|capability| {
            supports_declared(
                &capability.supported_operations,
                &capability.rule.supported_operations,
                &input.operations,
            )
        })
        .filter(|capability| {
            supports_declared(
                &capability.supported_presence,
                &capability.rule.supported_presence,
                &input.presences,
            )
        })
        .filter(|capability| !(is_key_like(&input.source_field) && !capability.rule.allows_key))
        .filter(|capability| {
            exact_candidate_matches_binding(capability, &input.source_field, &input.target_field)
        })
        .filter(|capability| {
            explicit_template_matches_binding(
                capability,
                &input.source_field,
                &input.target_field,
                input.options.selected_rule.as_ref(),
            )
        })
        .filter(|_| input.source_field.generated == input.target_field.generated)
        .filter(|_| input.source_field.nullable == input.target_field.nullable)
        .filter(|capability| {
            !is_range_template(capability)
                || input.source_field.logical_type != input.target_field.logical_type
        })
        .map(|capability| TargetTypeCandidate {
            target: candidate_target(capability, &input.target_field),
            rule: capability.rule.reference(),
            capability_code: capability.code.clone(),
            qualification: capability.rule.qualification,
            risk: capability.rule.risk,
            risk_code: capability.rule.risk_code.clone(),
            requires_confirmation: capability.rule.requires_confirmation
                || capability.rule.qualification == QualificationLevel::ExplicitConversion,
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (left.qualification, left.risk, &left.capability_code).cmp(&(
            right.qualification,
            right.risk,
            &right.capability_code,
        ))
    });

    if candidates.is_empty() {
        let key_blocked = is_key_like(&input.source_field)
            && input.manifest.capabilities.iter().any(|capability| {
                capability_matches_source(capability, &input.source_field.logical_type)
                    && target_representation_matches_binding(capability, &input.target_field)
                    && !capability.rule.allows_key
            });
        let (status, code, message) = if key_blocked {
            (
                CompatibilityStatus::Blocked,
                "target_capability.lossy_key_conversion",
                "a key or Row Locator field cannot use a lossy conversion",
            )
        } else {
            match &input.source_field.logical_type {
                LogicalType::Binary { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.binary_representation_missing",
                    "no qualified raw-byte target exists; binary values are never silently encoded as text",
                ),
                LogicalType::BitString { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.bit_string_representation_missing",
                    "no qualified bit-string target declares length, padding, and bit order",
                ),
                LogicalType::Spatial { .. } => (
                    CompatibilityStatus::Blocked,
                    "target_capability.spatial_metadata_or_capability_missing",
                    "spatial conversion requires qualified SRID, CRS, geometry type, dimensions, format, and target capability",
                ),
                LogicalType::Array { .. }
                | LogicalType::Struct { .. }
                | LogicalType::Map { .. }
                | LogicalType::Range { .. }
                | LogicalType::MultiRange { .. } => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.recursive_structure_unqualified",
                    "recursive or structured values require an explicit element/field/key/range mapping rule",
                ),
                _ => (
                    CompatibilityStatus::Unsupported,
                    "target_capability.no_qualified_representation",
                    "no qualified target representation matches this source semantic",
                ),
            }
        };
        return Ok(field_result(
            &input,
            status,
            QualificationLevel::Unsupported,
            if key_blocked {
                RiskLevel::Critical
            } else {
                RiskLevel::High
            },
            code,
            message,
            candidates,
            None,
            Some(CompatibilityFailure::new(
                FailureClass::TargetCapability,
                code,
                FailurePhase::CapabilityQualification,
                message,
            )),
        ));
    }

    if candidates.len() > 1 {
        let candidate = candidates[0].clone();
        return Ok(field_result(
            &input,
            CompatibilityStatus::NeedsConfiguration,
            candidate.qualification,
            candidate.risk,
            "target_capability.multiple_candidates",
            "multiple qualified target representations require an explicit rule selection",
            candidates,
            None,
            Some(CompatibilityFailure::new(
                FailureClass::InvalidInput,
                "target_capability.multiple_candidates",
                FailurePhase::PlanConstruction,
                "multiple qualified target representations require an explicit rule selection",
            )),
        ));
    }

    let candidate = candidates.into_iter().next().expect("candidate exists");
    let capability = input
        .manifest
        .capabilities
        .iter()
        .find(|capability| capability.code == candidate.capability_code)
        .expect("candidate came from manifest");
    let (parameters, missing, mut invalid) =
        normalize_options(&capability.rule, &input.options.parameters);
    if missing.is_empty()
        && let Err(reason) = validate_explicit_parameters(
            &capability.target,
            &input.source_field,
            &input.target_field,
            &parameters,
        )
    {
        invalid.push(reason);
    }
    if !invalid.is_empty() {
        let message = format!("conversion parameters are invalid: {}", invalid.join(", "));
        return Ok(field_result(
            &input,
            CompatibilityStatus::Blocked,
            candidate.qualification,
            candidate.risk,
            "target_capability.invalid_parameters",
            &message,
            vec![candidate],
            None,
            Some(CompatibilityFailure::new(
                FailureClass::InvalidInput,
                "target_capability.invalid_parameters",
                FailurePhase::PlanConstruction,
                message.clone(),
            )),
        ));
    }
    if !missing.is_empty() {
        let names = missing.join(", ");
        return Ok(field_result(
            &input,
            CompatibilityStatus::NeedsConfiguration,
            candidate.qualification,
            candidate.risk,
            "target_capability.missing_parameters",
            &format!("required conversion parameters are missing: {names}"),
            vec![candidate],
            None,
            Some(CompatibilityFailure::new(
                FailureClass::MissingConfirmation,
                "target_capability.missing_parameters",
                FailurePhase::PlanConstruction,
                format!("required conversion parameters are missing: {names}"),
            )),
        ));
    }

    let mut plan = build_field_plan(&input, &candidate, &capability.rule, &parameters);
    let confirmed = candidate.requires_confirmation
        && input
            .options
            .confirmations
            .iter()
            .any(|confirmation| plan.confirmation_matches(confirmation));
    if confirmed {
        plan.confirmation = PlanConfirmationState::Confirmed;
    }
    let needs_confirmation = candidate.requires_confirmation && !confirmed;
    let (status, reason_code, explanation, failure) = if needs_confirmation {
        (
            CompatibilityStatus::NeedsConfirmation,
            "target_capability.confirmation_required",
            "this deterministic conversion requires per-field risk confirmation",
            Some(CompatibilityFailure::new(
                FailureClass::MissingConfirmation,
                "target_capability.confirmation_required",
                FailurePhase::PlanConstruction,
                "this deterministic conversion requires per-field risk confirmation",
            )),
        )
    } else {
        (
            CompatibilityStatus::Compatible,
            "target_capability.qualified",
            "the source and target field binding has a qualified conversion plan",
            None,
        )
    };
    let summary = PlanSummary {
        input_digest: plan.input_digest.clone(),
        plan_digest: plan.plan_digest.clone(),
        rule: plan.rule.clone(),
        capability_code: plan.capability_code.clone(),
    };
    Ok(CompatibilityResult {
        source_field: input.source_field.reference,
        target_field: input.target_field.reference,
        status,
        qualification: candidate.qualification,
        risk: candidate.risk,
        reason_code: reason_code.to_owned(),
        explanation: explanation.to_owned(),
        requires_confirmation: candidate.requires_confirmation,
        rule: Some(candidate.rule.clone()),
        plan: Some(plan),
        candidates: vec![candidate],
        failure,
        summary: Some(summary),
    })
}

pub fn plan_field_compatibility(
    input: FieldCompatibilityInput<'_>,
) -> Result<CompatibilityResult, CompatibilityError> {
    explain_field_compatibility(input)
}

fn validate_field_input(input: &FieldCompatibilityInput<'_>) -> Result<(), CompatibilityError> {
    let invalid = |code: &'static str, message: &'static str| {
        CompatibilityError::InvalidInput(CompatibilityFailure::new(
            FailureClass::InvalidInput,
            code,
            FailurePhase::InputValidation,
            message,
        ))
    };
    if !input.source_field.reference.is_complete() || !input.target_field.reference.is_complete() {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.missing_schema_fingerprint",
            FailurePhase::InputValidation,
            "source and target definition references require lineage and Schema Fingerprint",
        )));
    }
    if input.options.route_id.trim().is_empty()
        || input.options.configuration_revision.trim().is_empty()
    {
        return Err(invalid(
            "compatibility.missing_route_identity",
            "route_id and configuration_revision are required",
        ));
    }
    if input.source_field.name.trim().is_empty()
        || input.target_field.name.trim().is_empty()
        || input.source_field.native_type.trim().is_empty()
        || input.target_field.native_type.trim().is_empty()
    {
        return Err(invalid(
            "compatibility.invalid_field_definition",
            "source and target field definitions are incomplete",
        ));
    }
    let collation_mismatch = if input.source_connector == input.sink_connector {
        input.source_field.collation != input.target_field.collation
    } else {
        input.source_field.collation.is_some()
            && input.target_field.collation.is_some()
            && input.source_field.collation != input.target_field.collation
    };
    // Text-like values can carry a target-side collation policy.  ENUM and
    // SET values are label/member semantics, so a collation difference must
    // not prevent the exact label/member plan from being selected.  For
    // other logical values a catalog collation is evidence of malformed
    // metadata, so retain the fail-closed contract instead of inventing a
    // conversion.
    let collation_is_configurable = matches!(
        &input.source_field.logical_type,
        LogicalType::Text { .. } | LogicalType::Enum { .. } | LogicalType::Set { .. }
    );
    if collation_mismatch
        && !collation_is_configurable
        && input.options.parameters.is_empty()
        && input.options.selected_rule.is_none()
    {
        return Err(CompatibilityError::TargetCapability(Box::new(
            TargetCapabilityFailure::new("source and target field collations are not equivalent")
                .with_code("target_capability.collation_mismatch")
                .with_route(input.options.route_id.clone()),
        )));
    }
    if input.source_type_mapping.connector != input.source_connector
        || input.source_type_mapping.logical_type != input.source_field.logical_type
        || !input
            .source_type_mapping
            .native_type
            .eq_ignore_ascii_case(&input.source_field.native_type)
        || input.source_type_mapping.mapping_id.trim().is_empty()
        || input.source_type_mapping.mapping_version.trim().is_empty()
    {
        return Err(CompatibilityError::SourceContract(
            CompatibilityFailure::new(
                FailureClass::SourceContract,
                "source_contract.type_mapping_mismatch",
                FailurePhase::SourceContract,
                "the SourceTypeMapping does not describe the selected source field",
            ),
        ));
    }
    if input.manifest.connector != input.sink_connector {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.connector_identity_changed",
            FailurePhase::InputValidation,
            "the selected Sink capability manifest belongs to another connector identity",
        )));
    }
    if input
        .target_build
        .as_ref()
        .is_some_and(|build| build != &input.manifest.target_build)
    {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.target_build_changed",
            FailurePhase::InputValidation,
            "the target server build no longer matches the capability manifest",
        )));
    }
    if input.operations.is_empty() || input.presences.is_empty() {
        return Err(invalid(
            "compatibility.missing_requirements",
            "at least one operation and presence state are required",
        ));
    }
    input.manifest.validate()?;
    validate_target_probe(
        input.options.target_probe.as_ref(),
        &input.target_field,
        &input.manifest.target_build,
        &input.options.route_id,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn field_result(
    input: &FieldCompatibilityInput<'_>,
    status: CompatibilityStatus,
    qualification: QualificationLevel,
    risk: RiskLevel,
    reason_code: &str,
    explanation: &str,
    candidates: Vec<TargetTypeCandidate>,
    plan: Option<ColumnConversionPlan>,
    failure: Option<CompatibilityFailure>,
) -> CompatibilityResult {
    CompatibilityResult {
        source_field: input.source_field.reference.clone(),
        target_field: input.target_field.reference.clone(),
        status,
        qualification,
        risk,
        reason_code: reason_code.to_owned(),
        explanation: explanation.to_owned(),
        requires_confirmation: candidates
            .iter()
            .any(|candidate| candidate.requires_confirmation),
        rule: candidates.first().map(|candidate| candidate.rule.clone()),
        plan,
        candidates,
        failure,
        summary: None,
    }
}

fn plan_target_representation(
    source: &LogicalType,
    target_type: &LogicalType,
    representation: &TargetRepresentation,
    parameters: &BTreeMap<String, String>,
) -> TargetRepresentation {
    let mut target_repr = representation.clone();
    match (source, target_type) {
        (
            LogicalType::Integer {
                signed: source_signed,
                bits: source_bits,
            },
            LogicalType::Integer {
                signed: target_signed,
                bits: target_bits,
            },
        ) if target_repr.parameters.contains_key("range_kind") => {
            target_repr
                .parameters
                .insert("source_signed".into(), source_signed.to_string());
            target_repr
                .parameters
                .insert("source_bits".into(), source_bits.to_string());
            target_repr
                .parameters
                .insert("target_signed".into(), target_signed.to_string());
            target_repr
                .parameters
                .insert("target_bits".into(), target_bits.to_string());
            target_repr
                .parameters
                .insert("range_check".into(), "strict".into());
            if matches!(target_bits, 8 | 16 | 24 | 32 | 64) {
                let (minimum, maximum) = if *target_signed {
                    let magnitude = 1_i128 << (target_bits - 1);
                    (-magnitude, magnitude - 1)
                } else {
                    (0, (1_i128 << target_bits) - 1)
                };
                target_repr
                    .parameters
                    .insert("target_min".into(), minimum.to_string());
                target_repr
                    .parameters
                    .insert("target_max".into(), maximum.to_string());
            }
        }
        (
            LogicalType::Decimal {
                precision: source_precision,
                scale: source_scale,
            },
            LogicalType::Decimal {
                precision: target_precision,
                scale: target_scale,
            },
        ) if target_repr.parameters.contains_key("range_kind") => {
            target_repr
                .parameters
                .insert("source_precision".into(), source_precision.to_string());
            target_repr
                .parameters
                .insert("source_scale".into(), source_scale.to_string());
            target_repr
                .parameters
                .insert("target_precision".into(), target_precision.to_string());
            target_repr
                .parameters
                .insert("target_scale".into(), target_scale.to_string());
            target_repr
                .parameters
                .insert("range_check".into(), "strict".into());
            target_repr
                .parameters
                .insert("rounding_mode".into(), "reject".into());
            target_repr.parameters.insert(
                "target_integer_digits".into(),
                (i32::from(*target_precision) - *target_scale)
                    .max(0)
                    .to_string(),
            );
        }
        (LogicalType::Float { bits: source_bits }, LogicalType::Float { bits: target_bits })
            if target_repr.parameters.contains_key("range_kind") =>
        {
            target_repr
                .parameters
                .insert("source_bits".into(), source_bits.to_string());
            target_repr
                .parameters
                .insert("target_bits".into(), target_bits.to_string());
            target_repr
                .parameters
                .insert("range_check".into(), "strict".into());
            target_repr
                .parameters
                .insert("rounding_mode".into(), "reject".into());
        }
        (
            LogicalType::Binary {
                max_length: source_length,
            },
            LogicalType::Binary {
                max_length: target_length,
            },
        ) => {
            let (source_length, source_fixed, source_padding) = binary_details(
                source_length,
                &parameters
                    .get("source_native_type")
                    .cloned()
                    .unwrap_or_default(),
            );
            let (target_length, target_fixed, target_padding) = binary_details(
                target_length,
                &parameters
                    .get("target_native_type")
                    .cloned()
                    .unwrap_or_else(|| representation.native_type.clone()),
            );
            target_repr
                .parameters
                .insert("conversion_kind".into(), "binary".into());
            target_repr
                .parameters
                .insert("binary_encoding".into(), "raw_bytes".into());
            target_repr
                .parameters
                .insert("binary_length_unit".into(), "bytes".into());
            target_repr
                .parameters
                .insert("source_length".into(), source_length);
            target_repr
                .parameters
                .insert("target_length".into(), target_length);
            target_repr
                .parameters
                .insert("source_fixed".into(), source_fixed.to_string());
            target_repr
                .parameters
                .insert("target_fixed".into(), target_fixed.to_string());
            target_repr
                .parameters
                .insert("source_padding".into(), source_padding);
            target_repr
                .parameters
                .insert("target_padding".into(), target_padding);
        }
        (
            LogicalType::BitString {
                length: source_length,
            },
            LogicalType::BitString {
                length: target_length,
            },
        ) => {
            let source_order = parameters
                .get("source_bit_order")
                .or_else(|| representation.parameters.get("source_bit_order"))
                .cloned()
                .unwrap_or_else(|| "msb_first".into());
            let target_order = parameters
                .get("target_bit_order")
                .or_else(|| representation.parameters.get("target_bit_order"))
                .cloned()
                .unwrap_or_else(|| source_order.clone());
            let source_padding = parameters
                .get("source_padding")
                .or_else(|| representation.parameters.get("source_padding"))
                .cloned()
                .unwrap_or_else(|| "zero".into());
            let target_padding = parameters
                .get("target_padding")
                .or_else(|| representation.parameters.get("target_padding"))
                .cloned()
                .unwrap_or_else(|| source_padding.clone());
            target_repr
                .parameters
                .insert("conversion_kind".into(), "bit_string".into());
            target_repr
                .parameters
                .insert("source_bit_length".into(), source_length.to_string());
            target_repr
                .parameters
                .insert("target_bit_length".into(), target_length.to_string());
            target_repr
                .parameters
                .insert("source_bit_order".into(), source_order);
            target_repr
                .parameters
                .insert("target_bit_order".into(), target_order);
            target_repr
                .parameters
                .insert("source_padding".into(), source_padding);
            target_repr
                .parameters
                .insert("target_padding".into(), target_padding);
        }
        (
            LogicalType::Spatial {
                subtype: source_subtype,
                srid: source_srid,
                dimensions: source_dimensions,
            },
            LogicalType::Spatial {
                subtype: target_subtype,
                srid: target_srid,
                dimensions: target_dimensions,
            },
        ) => {
            let source_crs = parameters
                .get("source_crs")
                .cloned()
                .unwrap_or_else(|| spatial_crs(*source_srid));
            let target_crs = parameters
                .get("target_crs")
                .cloned()
                .unwrap_or_else(|| spatial_crs(*target_srid));
            target_repr
                .parameters
                .insert("conversion_kind".into(), "spatial".into());
            target_repr
                .parameters
                .insert("source_geometry_type".into(), source_subtype.clone());
            target_repr
                .parameters
                .insert("target_geometry_type".into(), target_subtype.clone());
            target_repr
                .parameters
                .insert("source_srid".into(), spatial_srid(*source_srid));
            target_repr
                .parameters
                .insert("target_srid".into(), spatial_srid(*target_srid));
            target_repr
                .parameters
                .insert("source_dimensions".into(), source_dimensions.to_string());
            target_repr
                .parameters
                .insert("target_dimensions".into(), target_dimensions.to_string());
            target_repr
                .parameters
                .insert("source_crs".into(), source_crs);
            target_repr
                .parameters
                .insert("target_crs".into(), target_crs);
            target_repr.parameters.insert(
                "source_spatial_format".into(),
                parameters
                    .get("source_spatial_format")
                    .or_else(|| representation.parameters.get("source_spatial_format"))
                    .cloned()
                    .unwrap_or_else(|| "ewkb".into()),
            );
            target_repr.parameters.insert(
                "target_spatial_format".into(),
                parameters
                    .get("target_spatial_format")
                    .or_else(|| representation.parameters.get("target_spatial_format"))
                    .cloned()
                    .unwrap_or_else(|| "ewkb".into()),
            );
        }
        (source, target)
            if recursive_type_kind(source) == recursive_type_kind(target)
                && recursive_type_kind(source).is_some()
                && target_repr.parameters.contains_key("recursive_kind") =>
        {
            target_repr.parameters.insert(
                "recursive_kind".into(),
                recursive_type_kind(source).unwrap_or_default().into(),
            );
            target_repr
                .parameters
                .insert("structure_mapping".into(), "explicit".into());
        }
        (source, LogicalType::Json { .. })
            if recursive_type_kind(source).is_some()
                && target_repr
                    .parameters
                    .get("conversion_kind")
                    .map(String::as_str)
                    == Some("recursive") =>
        {
            target_repr.parameters.insert(
                "recursive_kind".into(),
                recursive_type_kind(source).unwrap_or_default().into(),
            );
            target_repr
                .parameters
                .insert("structure_mapping".into(), "json_value_carrier".into());
        }
        (
            LogicalType::Text {
                charset: source_charset,
                max_length: source_length,
                length_unit: source_unit,
                collation: source_collation,
            },
            target_text,
        ) if target_repr
            .parameters
            .get("conversion_kind")
            .map(String::as_str)
            == Some("text") =>
        {
            let (target_charset, target_length, target_unit, target_collation) =
                target_text_details(target_text, parameters);
            target_repr
                .parameters
                .insert("source_charset".into(), source_charset.clone());
            target_repr.parameters.insert(
                "source_length".into(),
                source_length.map_or_else(|| "unbounded".into(), |length| length.to_string()),
            );
            target_repr.parameters.insert(
                "source_length_unit".into(),
                format_length_unit(*source_unit),
            );
            target_repr.parameters.insert(
                "source_collation".into(),
                source_collation.clone().unwrap_or_else(|| "none".into()),
            );
            target_repr
                .parameters
                .insert("target_charset".into(), target_charset);
            target_repr
                .parameters
                .insert("target_length".into(), target_length);
            target_repr
                .parameters
                .insert("target_length_unit".into(), target_unit);
            target_repr
                .parameters
                .insert("target_collation".into(), target_collation);
        }
        (LogicalType::Json { .. }, LogicalType::Text { .. })
            if target_repr
                .parameters
                .get("conversion_kind")
                .map(String::as_str)
                == Some("json") =>
        {
            let (target_charset, target_length, target_unit, target_collation) =
                target_text_details(target_type, parameters);
            target_repr
                .parameters
                .insert("source_charset".into(), "UTF8".into());
            target_repr
                .parameters
                .insert("source_length".into(), "unbounded".into());
            target_repr
                .parameters
                .insert("source_length_unit".into(), "characters".into());
            target_repr
                .parameters
                .insert("source_collation".into(), "none".into());
            target_repr
                .parameters
                .insert("target_charset".into(), target_charset);
            target_repr
                .parameters
                .insert("target_length".into(), target_length);
            target_repr
                .parameters
                .insert("target_length_unit".into(), target_unit);
            target_repr
                .parameters
                .insert("target_collation".into(), target_collation);
            target_repr.parameters.insert(
                "json_strategy".into(),
                parameters
                    .get("json_strategy")
                    .cloned()
                    .unwrap_or_else(|| "normalized_text".into()),
            );
        }
        (
            LogicalType::Enum {
                members: source_members,
            },
            LogicalType::Enum {
                members: target_members,
            },
        ) => {
            target_repr
                .parameters
                .insert("value_strategy".into(), "enum_label".into());
            target_repr.parameters.insert(
                "source_members".into(),
                serde_json::to_string(source_members).expect("enum members are serializable"),
            );
            target_repr.parameters.insert(
                "target_members".into(),
                serde_json::to_string(target_members).expect("enum members are serializable"),
            );
        }
        (
            LogicalType::Set {
                members: source_members,
            },
            LogicalType::Set {
                members: target_members,
            },
        ) => {
            target_repr
                .parameters
                .insert("value_strategy".into(), "set_members".into());
            target_repr.parameters.insert(
                "source_members".into(),
                serde_json::to_string(source_members).expect("SET members are serializable"),
            );
            target_repr.parameters.insert(
                "target_members".into(),
                serde_json::to_string(target_members).expect("SET members are serializable"),
            );
        }
        (source_temporal, target_temporal)
            if target_repr
                .parameters
                .get("conversion_kind")
                .map(String::as_str)
                == Some("temporal") =>
        {
            let (source_kind, source_precision) = temporal_details(source_temporal);
            let (target_kind, target_precision) =
                temporal_details_with_parameters(target_temporal, parameters);
            target_repr
                .parameters
                .insert("source_temporal_kind".into(), source_kind.into());
            target_repr
                .parameters
                .insert("target_temporal_kind".into(), target_kind.into());
            target_repr
                .parameters
                .insert("source_precision".into(), source_precision.to_string());
            target_repr
                .parameters
                .insert("target_precision".into(), target_precision.to_string());
        }
        _ => {}
    }
    target_repr
}

fn format_length_unit(unit: LengthUnit) -> String {
    match unit {
        LengthUnit::Bytes => "bytes".into(),
        LengthUnit::Characters => "characters".into(),
    }
}

fn binary_details(length: &Option<u64>, native_type: &str) -> (String, bool, String) {
    let native = native_type.trim().to_ascii_lowercase();
    let fixed = native.starts_with("binary(");
    let native_length = native
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')').map(|(value, _)| value))
        .and_then(|value| value.parse::<u64>().ok());
    let length = native_length.or(*length);
    (
        length.map_or_else(|| "unbounded".into(), |value| value.to_string()),
        fixed,
        if fixed { "zero" } else { "none" }.into(),
    )
}

fn spatial_srid(srid: Option<i32>) -> String {
    srid.map_or_else(|| "unknown".into(), |value| value.to_string())
}

fn mysql_spatial_native_type(native_type: &str) -> bool {
    let base = native_type
        .trim()
        .to_ascii_lowercase()
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default()
        .to_owned();
    matches!(
        base.as_str(),
        "geometry"
            | "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    )
}

fn spatial_crs(srid: Option<i32>) -> String {
    srid.map_or_else(|| "unknown".into(), |value| format!("srid:{value}"))
}

fn recursive_type_kind(value: &LogicalType) -> Option<&'static str> {
    match value {
        LogicalType::Array { .. } | LogicalType::ArrayWithMetadata { .. } => Some("array"),
        LogicalType::Struct { .. } => Some("struct"),
        LogicalType::Map { .. } => Some("map"),
        LogicalType::Range { .. } => Some("range"),
        LogicalType::MultiRange { .. } => Some("multi_range"),
        _ => None,
    }
}

fn target_text_details(
    target: &LogicalType,
    parameters: &BTreeMap<String, String>,
) -> (String, String, String, String) {
    if let LogicalType::Text {
        charset,
        max_length,
        length_unit,
        collation,
    } = target
    {
        return (
            parameters
                .get("target_charset")
                .cloned()
                .unwrap_or_else(|| charset.clone()),
            parameters.get("target_length").cloned().unwrap_or_else(|| {
                max_length.map_or_else(|| "unbounded".into(), |length| length.to_string())
            }),
            parameters
                .get("target_length_unit")
                .cloned()
                .unwrap_or_else(|| format_length_unit(*length_unit)),
            parameters
                .get("target_collation")
                .cloned()
                .unwrap_or_else(|| collation.clone().unwrap_or_else(|| "none".into())),
        );
    }
    (
        parameters
            .get("target_charset")
            .cloned()
            .unwrap_or_else(|| "UTF8".into()),
        parameters
            .get("target_length")
            .cloned()
            .unwrap_or_else(|| "unbounded".into()),
        parameters
            .get("target_length_unit")
            .cloned()
            .unwrap_or_else(|| "characters".into()),
        parameters
            .get("target_collation")
            .cloned()
            .unwrap_or_else(|| "none".into()),
    )
}

fn temporal_details(value: &LogicalType) -> (&'static str, u8) {
    match value {
        LogicalType::LocalDatetime {
            fractional_precision,
        } => ("local_datetime", *fractional_precision),
        LogicalType::Instant {
            fractional_precision,
        } => ("instant", *fractional_precision),
        LogicalType::Duration {
            fractional_precision,
        } => ("duration", *fractional_precision),
        _ => ("unknown", 0),
    }
}

fn temporal_details_with_parameters(
    value: &LogicalType,
    parameters: &BTreeMap<String, String>,
) -> (&'static str, u8) {
    let (kind, precision) = temporal_details(value);
    if kind != "unknown" {
        return (
            kind,
            parameters
                .get("target_precision")
                .and_then(|value| value.parse().ok())
                .unwrap_or(precision),
        );
    }
    let kind = match parameters
        .get("target_temporal_kind")
        .map(String::as_str)
        .unwrap_or("unknown")
    {
        "local_datetime" => "local_datetime",
        "instant" => "instant",
        "duration" => "duration",
        _ => "unknown",
    };
    (
        kind,
        parameters
            .get("target_precision")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
    )
}

fn build_field_plan(
    input: &FieldCompatibilityInput<'_>,
    candidate: &TargetTypeCandidate,
    rule: &ConversionRule,
    parameters: &BTreeMap<String, String>,
) -> ColumnConversionPlan {
    let input_digest = digest_of(&FieldInputDigest {
        source_field: &input.source_field,
        target_field: &input.target_field,
        mapping: &input.source_type_mapping,
        source_connector: &input.source_connector,
        sink_connector: &input.sink_connector,
        source_build: &input.source_build,
        target_build: &input.target_build,
        manifest_digest: &input.manifest.digest,
        target_probe_digest: &input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone()),
        operations: &input.operations,
        presences: &input.presences,
        options: RouteOptionsDigest {
            route_id: &input.options.route_id,
            configuration_revision: &input.options.configuration_revision,
            selected_rule: &input.options.selected_rule,
            parameters,
        },
        candidate,
    });
    let mut planning_parameters = parameters.clone();
    planning_parameters.insert(
        "source_native_type".into(),
        input.source_field.native_type.clone(),
    );
    planning_parameters.insert(
        "target_native_type".into(),
        input.target_field.native_type.clone(),
    );
    let target = plan_target_representation(
        &input.source_field.logical_type,
        &input.target_field.logical_type,
        &candidate.target,
        &planning_parameters,
    );
    let mut target = target;
    if target.parameters.get("conversion_kind").map(String::as_str) == Some("text") {
        target.parameters.insert(
            "source_collation".into(),
            input
                .source_field
                .collation
                .clone()
                .unwrap_or_else(|| "none".into()),
        );
        target.parameters.insert(
            "target_collation".into(),
            input
                .target_field
                .collation
                .clone()
                .unwrap_or_else(|| "none".into()),
        );
    }
    let mut plan = ColumnConversionPlan {
        format: COMPATIBILITY_FORMAT.to_owned(),
        route_id: input.options.route_id.clone(),
        configuration_revision: input.options.configuration_revision.clone(),
        source_field: input.source_field.reference.clone(),
        target_field: input.target_field.reference.clone(),
        source_connector: input.source_connector.clone(),
        sink_connector: input.sink_connector.clone(),
        source_build: input.source_build.clone(),
        target_build: input.target_build.clone(),
        source_mapping_id: input.source_type_mapping.mapping_id.clone(),
        source_mapping_version: input.source_type_mapping.mapping_version.clone(),
        target,
        rule: candidate.rule.clone(),
        rule_digest: digest_of(rule),
        capability_code: candidate.capability_code.clone(),
        capability_manifest_digest: input.manifest.digest.clone(),
        target_probe_digest: input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone()),
        qualification: candidate.qualification,
        risk: candidate.risk,
        risk_code: candidate.risk_code.clone(),
        loss: LossAssessment::for_qualification(
            candidate.qualification,
            is_key_like(&input.source_field),
        ),
        examples: vec![ConversionExample {
            source: input.source_field.logical_type.family_name().into(),
            target: input.target_field.native_type.clone(),
        }],
        locator_impact: if is_key_like(&input.source_field) {
            LocatorImpact::Preserved
        } else if rule.qualification == QualificationLevel::ExplicitConversion {
            LocatorImpact::ValueOnly
        } else {
            LocatorImpact::NotUsed
        },
        confirmation: if candidate.requires_confirmation {
            PlanConfirmationState::Required
        } else {
            PlanConfirmationState::NotRequired
        },
        parameters: parameters.clone(),
        failure_policy: rule.failure_policy,
        input_digest,
        plan_digest: String::new(),
    };
    plan.plan_digest = plan.computed_digest();
    plan
}

#[derive(Serialize)]
struct FieldInputDigest<'a> {
    source_field: &'a FieldDefinition,
    target_field: &'a FieldDefinition,
    mapping: &'a SourceTypeMapping,
    source_connector: &'a ConnectorIdentity,
    sink_connector: &'a ConnectorIdentity,
    source_build: &'a Option<ServerBuildIdentity>,
    target_build: &'a Option<ServerBuildIdentity>,
    manifest_digest: &'a str,
    target_probe_digest: &'a Option<String>,
    operations: &'a [Operation],
    presences: &'a [PresenceState],
    options: RouteOptionsDigest<'a>,
    candidate: &'a TargetTypeCandidate,
}

fn validate_target_probe(
    probe: Option<&TargetCapabilityProbe>,
    target_field: &FieldDefinition,
    target_build: &ServerBuildIdentity,
    route_id: &str,
) -> Result<(), CompatibilityError> {
    let Some(probe) = probe else {
        return Ok(());
    };
    probe.validate().map_err(|failure| {
        CompatibilityError::TargetCapability(Box::new(failure.with_route(route_id.to_owned())))
    })?;
    if &probe.target_build != target_build
        || probe.column != target_field.name
        || probe.column_metadata.definition_fingerprint != target_field.reference.schema_fingerprint
        || (!probe.column_metadata.native_type.is_empty()
            && !probe
                .column_metadata
                .native_type
                .eq_ignore_ascii_case(&target_field.native_type))
    {
        return Err(CompatibilityError::PlanInvalidated(
            CompatibilityFailure::new(
                FailureClass::StaleInput,
                "compatibility.target_probe_changed",
                FailurePhase::InputValidation,
                "target column metadata or target build no longer matches the plan input",
            ),
        ));
    }
    if !probe.is_qualified() {
        return Err(CompatibilityError::TargetCapability(Box::new(
            TargetCapabilityFailure::new(
                "target preflight did not produce qualified capability evidence",
            )
            .with_code("target_capability.probe_not_qualified")
            .with_route(route_id.to_owned()),
        )));
    }
    Ok(())
}

fn probe_qualifies_capability(
    probe: Option<&TargetCapabilityProbe>,
    capability: &CapabilityEntry,
) -> bool {
    probe.is_none_or(|probe| {
        probe.capabilities.iter().any(|entry| {
            entry.identity == capability.code && entry.status == CapabilityProbeStatus::Qualified
        })
    })
}

fn validate_input(input: &CompatibilityInput<'_>) -> Result<(), CompatibilityError> {
    let invalid = |code: &'static str, message: &'static str| {
        CompatibilityError::InvalidInput(CompatibilityFailure::new(
            FailureClass::InvalidInput,
            code,
            FailurePhase::InputValidation,
            message,
        ))
    };
    if !input.source_field.reference.is_complete() || !input.target_field.reference.is_complete() {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.missing_schema_fingerprint",
            FailurePhase::InputValidation,
            "source and target definition references require lineage and Schema Fingerprint",
        )));
    }
    if input.options.route_id.trim().is_empty()
        || input.options.configuration_revision.trim().is_empty()
    {
        return Err(invalid(
            "compatibility.missing_route_identity",
            "route_id and configuration_revision are required",
        ));
    }
    if input.source_field.name.trim().is_empty()
        || input.target_field.name.trim().is_empty()
        || input.source_field.native_type.trim().is_empty()
        || input.target_field.native_type.trim().is_empty()
    {
        return Err(invalid(
            "compatibility.invalid_field_definition",
            "source and target field definitions are incomplete",
        ));
    }
    if input.source_type_mapping.connector != input.source_connector
        || input.source_type_mapping.logical_type != input.source_field.logical_type
        || !input
            .source_type_mapping
            .native_type
            .eq_ignore_ascii_case(&input.source_field.native_type)
        || input.source_type_mapping.mapping_id.trim().is_empty()
        || input.source_type_mapping.mapping_version.trim().is_empty()
    {
        return Err(CompatibilityError::SourceContract(
            CompatibilityFailure::new(
                FailureClass::SourceContract,
                "source_contract.type_mapping_mismatch",
                FailurePhase::SourceContract,
                "the SourceTypeMapping does not describe the selected source field",
            ),
        ));
    }
    if input.manifest.connector != input.sink_connector {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.connector_identity_changed",
            FailurePhase::InputValidation,
            "the selected Sink capability manifest belongs to another connector identity",
        )));
    }
    if input
        .target_build
        .as_ref()
        .is_some_and(|build| build != &input.manifest.target_build)
    {
        return Err(CompatibilityError::StaleInput(CompatibilityFailure::new(
            FailureClass::StaleInput,
            "compatibility.target_build_changed",
            FailurePhase::InputValidation,
            "the target server build no longer matches the capability manifest",
        )));
    }
    if input.transaction.transaction().source.kind != input.source_connector.kind
        || !input
            .transaction
            .transaction()
            .source
            .version
            .starts_with(&input.source_connector.version)
    {
        return Err(CompatibilityError::SourceContract(
            CompatibilityFailure::new(
                FailureClass::SourceContract,
                "source_contract.connector_identity_mismatch",
                FailurePhase::SourceContract,
                "the validated transaction belongs to another source connector identity",
            ),
        ));
    }
    input.manifest.validate()?;
    validate_target_probe(
        input.options.target_probe.as_ref(),
        &input.target_field,
        &input.manifest.target_build,
        &input.options.route_id,
    )?;
    let field_seen = input
        .transaction
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
        .any(|column| {
            column.ordinal == input.source_field.ordinal && column.name == input.source_field.name
        });
    if !field_seen {
        return Err(invalid(
            "compatibility.field_not_in_transaction",
            "the selected source field is not present in the validated transaction",
        ));
    }
    let metadata_mismatch = input
        .transaction
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
        .filter(|column| {
            column.ordinal == input.source_field.ordinal && column.name == input.source_field.name
        })
        .any(|column| {
            !column
                .native_type
                .eq_ignore_ascii_case(&input.source_field.native_type)
                || column.primary_key_ordinal != input.source_field.primary_key_ordinal
                || column.generated != input.source_field.generated
                || column.collation != input.source_field.collation
        });
    if metadata_mismatch {
        return Err(CompatibilityError::SourceContract(
            CompatibilityFailure::new(
                FailureClass::SourceContract,
                "source_contract.field_definition_mismatch",
                FailurePhase::SourceContract,
                "the source field definition does not match the validated ChangeEvent",
            ),
        ));
    }
    let value_mismatch = input
        .transaction
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
        .filter(|column| {
            column.ordinal == input.source_field.ordinal && column.name == input.source_field.name
        })
        .filter_map(|column| match &column.datum {
            Datum::Value(value) => Some(value),
            Datum::Null | Datum::Unchanged | Datum::Unavailable => None,
        })
        .any(|value| !input.source_field.logical_type.matches_value(value));
    if value_mismatch {
        return Err(CompatibilityError::SourceContract(
            CompatibilityFailure::new(
                FailureClass::SourceContract,
                "source_contract.logical_value_mismatch",
                FailurePhase::SourceContract,
                "a captured LogicalValue does not match the selected LogicalType",
            ),
        ));
    }
    Ok(())
}

fn transaction_presences(input: &CompatibilityInput<'_>) -> Vec<PresenceState> {
    let mut result = Vec::new();
    for datum in input
        .transaction
        .transaction()
        .changes
        .iter()
        .flat_map(|change| change.before.iter().chain(change.after.iter()))
        .flatten()
        .filter(|column| {
            column.ordinal == input.source_field.ordinal && column.name == input.source_field.name
        })
        .map(|column| (&column.datum, column.generated))
    {
        let presence = PresenceState::from_datum(datum.0, datum.1);
        if !result.contains(&presence) {
            result.push(presence);
        }
    }
    result
}

fn supports_all<T: PartialEq + Copy>(supported: &[T], required: &[T]) -> bool {
    required.iter().all(|value| supported.contains(value))
}

fn supports_declared<T: PartialEq + Copy>(capability: &[T], rule: &[T], required: &[T]) -> bool {
    (capability.is_empty() || supports_all(capability, required))
        && (rule.is_empty() || supports_all(rule, required))
}

fn validate_explicit_parameters(
    target: &TargetRepresentation,
    source: &FieldDefinition,
    destination: &FieldDefinition,
    parameters: &BTreeMap<String, String>,
) -> Result<(), String> {
    let parameter = |name: &str| {
        parameters
            .get(name)
            .or_else(|| target.parameters.get(name))
            .map(String::as_str)
    };
    match target.parameters.get("conversion_kind").map(String::as_str) {
        Some("binary") => {
            if !matches!(source.logical_type, LogicalType::Binary { .. })
                || !matches!(destination.logical_type, LogicalType::Binary { .. })
            {
                return Err("binary conversion requires binary source and target fields".into());
            }
            if parameter("binary_encoding") != Some("raw_bytes")
                || parameter("binary_length_unit") != Some("bytes")
            {
                return Err(
                    "binary conversion must explicitly use raw bytes and byte length semantics"
                        .into(),
                );
            }
            let target_length = parameter("target_length")
                .ok_or_else(|| "binary target length is missing".to_owned())?;
            if target_length != "unbounded" && target_length.parse::<u64>().is_err() {
                return Err("binary target length is invalid".into());
            }
            if parameter("target_fixed") == Some("true")
                && !matches!(parameter("target_padding"), Some("zero" | "one"))
            {
                return Err("fixed binary targets require explicit padding semantics".into());
            }
            Ok(())
        }
        Some("bit_string") => {
            let (
                LogicalType::BitString {
                    length: source_length,
                },
                LogicalType::BitString {
                    length: target_length,
                },
            ) = (&source.logical_type, &destination.logical_type)
            else {
                return Err(
                    "bit-string conversion requires bit-string source and target fields".into(),
                );
            };
            if parameter("binary_length_unit") != Some("bits")
                && parameter("bit_length_unit") != Some("bits")
            {
                return Err("bit-string length must be explicitly measured in bits".into());
            }
            let declared_target = parameter("target_bit_length")
                .ok_or_else(|| "target bit length is missing".to_owned())?
                .parse::<u64>()
                .map_err(|_| "target bit length is invalid".to_owned())?;
            if declared_target != *target_length || source_length != target_length {
                return Err("bit-string length conversion is not qualified".into());
            }
            if !matches!(
                parameter("target_bit_order"),
                Some("msb_first" | "lsb_first")
            ) || !matches!(parameter("target_padding"), Some("none" | "zero" | "one"))
            {
                return Err("bit-string padding and bit order must be explicit".into());
            }
            Ok(())
        }
        Some("spatial") => {
            let (
                LogicalType::Spatial {
                    subtype: source_subtype,
                    srid: source_srid,
                    dimensions: source_dimensions,
                },
                LogicalType::Spatial {
                    subtype: target_subtype,
                    srid: target_srid,
                    dimensions: target_dimensions,
                },
            ) = (&source.logical_type, &destination.logical_type)
            else {
                return Err("spatial conversion requires spatial source and target fields".into());
            };
            if source_srid.is_none()
                || target_srid.is_none()
                || source_subtype != target_subtype
                || source_srid != target_srid
                || source_dimensions != target_dimensions
            {
                return Err(
                    "spatial conversion requires equal qualified SRID, geometry type, and dimensions"
                        .into(),
                );
            }
            if !matches!(parameter("source_spatial_format"), Some("wkb" | "ewkb"))
                || !matches!(parameter("target_spatial_format"), Some("wkb" | "ewkb"))
            {
                return Err("spatial wire format must be explicitly qualified".into());
            }
            let source_crs =
                parameter("source_crs").ok_or_else(|| "source CRS is missing".to_owned())?;
            let target_crs =
                parameter("target_crs").ok_or_else(|| "target CRS is missing".to_owned())?;
            if source_crs == "unknown" || target_crs == "unknown" || source_crs != target_crs {
                return Err("spatial CRS is missing or not equivalent".into());
            }
            Ok(())
        }
        Some("recursive") => {
            let source_kind = recursive_type_kind(&source.logical_type);
            let target_kind = recursive_type_kind(&destination.logical_type);
            if source_kind.is_some()
                && matches!(destination.logical_type, LogicalType::Json { .. })
                && parameter("structure_mapping") == Some("json_value_carrier")
            {
                return Ok(());
            }
            if source_kind.is_none() || source_kind != target_kind {
                return Err("recursive source and target structures are not equivalent".into());
            }
            if parameter("structure_mapping") != Some("explicit") {
                return Err("recursive conversion requires an explicit structure mapping".into());
            }
            Ok(())
        }
        Some("text") => {
            let LogicalType::Text { charset, .. } = &source.logical_type else {
                return Err("text conversion requires a text source logical type".into());
            };
            if !text_charset_supported(charset) {
                return Err(format!(
                    "source charset {charset} is not qualified for explicit conversion"
                ));
            }
            let target_charset = required_parameter(parameters, "target_charset")?;
            let target_length = required_parameter(parameters, "target_length")?;
            let target_unit = required_parameter(parameters, "target_length_unit")?;
            let target_collation = required_parameter(parameters, "target_collation")?;
            if !text_charset_supported(target_charset) {
                return Err(format!("target charset {target_charset} is not qualified"));
            }
            if target_length != "unbounded"
                && (target_length.parse::<u64>().is_err() || target_length == "0")
            {
                return Err("target_length must be a positive integer or unbounded".into());
            }
            if !matches!(target_unit, "bytes" | "characters") {
                return Err("target_length_unit must be bytes or characters".into());
            }
            if target_collation.is_empty() {
                return Err(
                    "target_collation must be explicit; use none for a binary/unqualified target"
                        .into(),
                );
            }
            if let LogicalType::Text {
                charset: destination_charset,
                max_length: destination_length,
                length_unit: destination_unit,
                ..
            } = &destination.logical_type
                && (!target_charset.eq_ignore_ascii_case(destination_charset)
                    || target_length
                        != destination_length
                            .map_or_else(|| "unbounded".into(), |length| length.to_string())
                    || target_unit != format_length_unit(*destination_unit))
            {
                return Err(
                    "text conversion parameters do not match the pre-created target column".into(),
                );
            }
            let expected_collation = destination.collation.as_deref().unwrap_or("none");
            if !target_collation.eq_ignore_ascii_case(expected_collation) {
                return Err("target_collation does not match the pre-created target column".into());
            }
            Ok(())
        }
        Some("json") => {
            let LogicalType::Json { profile } = &source.logical_type else {
                return Err("JSON text conversion requires a JSON source logical type".into());
            };
            if !profile.normalized {
                return Err("source JSON profile is not normalized".into());
            }
            let strategy = required_parameter(parameters, "json_strategy")?;
            if strategy == "raw_text" {
                return Err(
                    "raw JSON text is unavailable because ChangeEvent stores normalized structure"
                        .into(),
                );
            }
            if strategy != "normalized_text" {
                return Err("json_strategy must be normalized_text".into());
            }
            let LogicalType::Text {
                charset: destination_charset,
                max_length: destination_length,
                length_unit: destination_unit,
                ..
            } = &destination.logical_type
            else {
                return Err("JSON text conversion requires a pre-created text target".into());
            };
            let target_charset = required_parameter(parameters, "target_charset")?;
            let target_length = required_parameter(parameters, "target_length")?;
            let target_unit = required_parameter(parameters, "target_length_unit")?;
            let target_collation = required_parameter(parameters, "target_collation")?;
            if !text_charset_supported(target_charset) {
                return Err(format!("target charset {target_charset} is not qualified"));
            }
            if target_length != "unbounded"
                && (target_length.parse::<u64>().is_err() || target_length == "0")
            {
                return Err("target_length must be a positive integer or unbounded".into());
            }
            if !matches!(target_unit, "bytes" | "characters") || target_collation.is_empty() {
                return Err("JSON text target parameters are incomplete".into());
            }
            if !target_charset.eq_ignore_ascii_case(destination_charset)
                || target_length
                    != destination_length
                        .map_or_else(|| "unbounded".into(), |length| length.to_string())
                || target_unit != format_length_unit(*destination_unit)
                || !target_collation
                    .eq_ignore_ascii_case(destination.collation.as_deref().unwrap_or("none"))
            {
                return Err(
                    "JSON text parameters do not match the pre-created target column".into(),
                );
            }
            Ok(())
        }
        Some("enum") => {
            let (
                LogicalType::Enum {
                    members: source_members,
                },
                LogicalType::Enum {
                    members: target_members,
                },
            ) = (&source.logical_type, &destination.logical_type)
            else {
                return Err("ENUM label conversion requires ENUM source and target types".into());
            };
            if !same_members(source_members, target_members) {
                return Err(
                    "ENUM label conversion requires the same declared label set; ordinal remapping is forbidden"
                        .into(),
                );
            }
            if target.parameters.get("value_strategy").map(String::as_str) != Some("enum_label") {
                return Err("ENUM conversion must use enum_label".into());
            }
            Ok(())
        }
        Some("temporal") => {
            let (source_kind, source_precision) = temporal_details(&source.logical_type);
            if source_kind == "unknown" {
                return Err("temporal conversion requires a temporal source logical type".into());
            }
            let (target_kind, target_precision) = target_temporal_details(destination, parameters);
            if target_kind == "unknown" {
                return Err("target temporal kind is not qualified".into());
            }
            let declared_precision = required_parameter(parameters, "target_precision")?
                .parse::<u8>()
                .map_err(|_| "target_precision must be an integer from 0 to 6".to_owned())?;
            if declared_precision > 6 || declared_precision != target_precision {
                return Err("target_precision does not match the pre-created target column".into());
            }
            let strategy = required_parameter(parameters, "temporal_strategy")?;
            let expected = match (source_kind, target_kind) {
                ("local_datetime", "instant") => "local_to_absolute",
                ("instant", "local_datetime") => "absolute_to_local",
                ("local_datetime", "local_datetime") => "preserve_local",
                ("instant", "instant") => "preserve_absolute",
                ("duration", "duration") => "preserve_duration",
                _ => return Err("temporal conversion strategy is not qualified".into()),
            };
            if strategy != expected {
                return Err(format!("temporal_strategy must be {expected}"));
            }
            if source_kind != target_kind {
                let timezone = required_parameter(parameters, "time_zone")?;
                parse_timezone_value(timezone)?;
            }
            if source_kind == target_kind && source_precision == target_precision {
                return Err(
                    "an explicit temporal conversion must change precision or time semantics"
                        .into(),
                );
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn required_parameter<'a>(
    parameters: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, String> {
    parameters
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("required conversion parameter {name} is missing"))
}

fn target_temporal_details(
    field: &FieldDefinition,
    parameters: &BTreeMap<String, String>,
) -> (&'static str, u8) {
    let (kind, precision) = temporal_details(&field.logical_type);
    if kind != "unknown" {
        return (kind, precision);
    }
    let native = field.native_type.to_ascii_lowercase();
    let kind = if native.contains("with time zone") {
        "instant"
    } else if native.contains("without time zone")
        || native.starts_with("datetime")
        || native.starts_with("timestamp")
    {
        "local_datetime"
    } else if native.starts_with("time") || native.starts_with("interval") {
        "duration"
    } else {
        match parameters
            .get("target_temporal_kind")
            .map(String::as_str)
            .unwrap_or("unknown")
        {
            "local_datetime" => "local_datetime",
            "instant" => "instant",
            "duration" => "duration",
            _ => "unknown",
        }
    };
    let precision = native
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')').map(|(value, _)| value))
        .and_then(|value| value.parse().ok())
        .or_else(|| {
            parameters
                .get("target_precision")
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0);
    (kind, precision)
}

fn parse_timezone_value(value: &str) -> Result<i32, String> {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return Ok(0);
    }
    let sign = match value.as_bytes().first() {
        Some(b'+') => 1_i32,
        Some(b'-') => -1_i32,
        _ => return Err("time_zone must be UTC or a fixed +HH:MM/-HH:MM offset".into()),
    };
    let (hours, minutes) = value[1..]
        .split_once(':')
        .ok_or_else(|| "time_zone must be a fixed +HH:MM/-HH:MM offset".to_owned())?;
    let hours = hours
        .parse::<i32>()
        .map_err(|_| "time_zone hour is invalid".to_owned())?;
    let minutes = minutes
        .parse::<i32>()
        .map_err(|_| "time_zone minute is invalid".to_owned())?;
    if hours > 23 || minutes > 59 {
        return Err("time_zone offset is outside its qualified range".into());
    }
    Ok(sign * (hours * 3_600 + minutes * 60))
}

fn is_range_template(capability: &CapabilityEntry) -> bool {
    matches!(
        capability.source_logical_type,
        LogicalType::Integer { bits: 0, .. }
            | LogicalType::Decimal { precision: 0, .. }
            | LogicalType::Float { bits: 0 }
    )
}

fn target_representation_matches_binding(
    capability: &CapabilityEntry,
    target: &FieldDefinition,
) -> bool {
    if capability
        .target
        .native_type
        .eq_ignore_ascii_case(&target.native_type)
    {
        return true;
    }
    if capability
        .target
        .parameters
        .get("conversion_kind")
        .map(String::as_str)
        == Some("spatial")
        || capability
            .target
            .parameters
            .get("target_storage")
            .map(String::as_str)
            == Some("mysql_geometry")
    {
        return matches!(target.logical_type, LogicalType::Spatial { .. })
            && mysql_spatial_native_type(&target.native_type);
    }
    let capability_native = capability.target.native_type.to_ascii_lowercase();
    let target_native = target.native_type.trim().to_ascii_lowercase();
    if matches!(capability_native.as_str(), "geometry" | "geography")
        && target_native.starts_with(&format!("{capability_native}("))
    {
        return matches!(target.logical_type, LogicalType::Spatial { .. });
    }
    match (capability_native.as_str(), &target.logical_type) {
        ("enum", LogicalType::Enum { .. }) => target
            .native_type
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("enum("),
        ("set", LogicalType::Set { .. }) => target
            .native_type
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("set("),
        _ => false,
    }
}

fn candidate_target(
    capability: &CapabilityEntry,
    target: &FieldDefinition,
) -> TargetRepresentation {
    let mut representation = capability.target.clone();
    if matches!(
        representation.native_type.to_ascii_lowercase().as_str(),
        "enum" | "set"
    ) {
        representation.native_type = target.native_type.clone();
    }
    if representation
        .parameters
        .get("conversion_kind")
        .map(String::as_str)
        == Some("spatial")
        || capability
            .target
            .parameters
            .get("target_storage")
            .map(String::as_str)
            == Some("mysql_geometry")
    {
        representation.native_type = target.native_type.clone();
    }
    let target_native = target.native_type.trim().to_ascii_lowercase();
    if matches!(
        representation.native_type.as_str(),
        "geometry" | "geography"
    ) && target_native.starts_with(&format!("{}(", representation.native_type))
    {
        representation.native_type = target.native_type.clone();
    }
    representation
}

fn is_explicit_template(capability: &CapabilityEntry) -> bool {
    capability.target.parameters.contains_key("conversion_kind")
}

fn is_key_like(field: &FieldDefinition) -> bool {
    field.primary_key_ordinal.is_some() || field.unique || field.row_locator
}

fn text_charset_supported(charset: &str) -> bool {
    matches!(
        charset.to_ascii_lowercase().as_str(),
        "utf8" | "utf8mb4" | "utf-8" | "ascii" | "latin1" | "iso-8859-1"
    )
}

fn explicit_template_matches_binding(
    capability: &CapabilityEntry,
    input_source: &FieldDefinition,
    input_target: &FieldDefinition,
    selected_rule: Option<&RuleReference>,
) -> bool {
    if !is_explicit_template(capability) {
        return true;
    }
    if is_key_like(input_source) || is_key_like(input_target) {
        return false;
    }
    let kind = capability
        .target
        .parameters
        .get("conversion_kind")
        .map(String::as_str);
    match kind {
        Some("text") => {
            let LogicalType::Text { charset, .. } = &input_source.logical_type else {
                return false;
            };
            if !text_charset_supported(charset) {
                return false;
            }
            selected_rule.is_some_and(|rule| rule.id == capability.rule.id)
                || !text_binding_is_exact(input_source, input_target)
        }
        Some("json") => {
            matches!(input_source.logical_type, LogicalType::Json { .. })
                && matches!(input_target.logical_type, LogicalType::Text { .. })
                && (selected_rule.is_some_and(|rule| rule.id == capability.rule.id)
                    || !matches!(input_target.logical_type, LogicalType::Json { .. }))
        }
        Some("enum") => {
            let (
                LogicalType::Enum {
                    members: source_members,
                },
                LogicalType::Enum {
                    members: target_members,
                },
            ) = (&input_source.logical_type, &input_target.logical_type)
            else {
                return false;
            };
            same_members(source_members, target_members)
                && (selected_rule.is_some_and(|rule| rule.id == capability.rule.id)
                    || source_members != target_members)
        }
        Some("temporal") => {
            temporal_details(&input_source.logical_type).0 != "unknown"
                && (selected_rule.is_some_and(|rule| rule.id == capability.rule.id)
                    || !temporal_binding_is_exact(input_source, input_target))
        }
        Some("recursive") => {
            recursive_type_kind(&input_source.logical_type).is_some()
                && matches!(input_target.logical_type, LogicalType::Json { .. })
                && (selected_rule.is_none()
                    || selected_rule.is_some_and(|rule| rule.id == capability.rule.id))
        }
        _ => false,
    }
}

fn exact_candidate_matches_binding(
    capability: &CapabilityEntry,
    source: &FieldDefinition,
    target: &FieldDefinition,
) -> bool {
    if capability.rule.qualification != QualificationLevel::Exact {
        return true;
    }
    if matches!(target.logical_type, LogicalType::Opaque { .. })
        && !matches!(
            source.logical_type,
            LogicalType::Binary { .. }
                | LogicalType::BitString { .. }
                | LogicalType::Spatial { .. }
                | LogicalType::Array { .. }
                | LogicalType::Struct { .. }
                | LogicalType::Map { .. }
                | LogicalType::Range { .. }
                | LogicalType::MultiRange { .. }
        )
    {
        return true;
    }
    match &source.logical_type {
        LogicalType::Text { .. } => text_binding_is_exact(source, target),
        LogicalType::LocalDatetime { .. }
        | LogicalType::Instant { .. }
        | LogicalType::Duration { .. } => temporal_binding_is_exact(source, target),
        LogicalType::Enum {
            members: source_members,
        } => {
            matches!(
                &target.logical_type,
                LogicalType::Enum { members: target_members }
                    if source_members == target_members
            )
        }
        LogicalType::Set {
            members: source_members,
        } => matches!(
            &target.logical_type,
            LogicalType::Set { members: target_members } if same_members(source_members, target_members)
        ),
        LogicalType::Binary {
            max_length: source_length,
        } => matches!(
            &target.logical_type,
            LogicalType::Binary {
                max_length: target_length,
            } if source_length == target_length
        ),
        LogicalType::BitString {
            length: source_length,
        } => matches!(
            &target.logical_type,
            LogicalType::BitString {
                length: target_length,
            } if source_length == target_length
        ),
        LogicalType::Spatial {
            subtype: source_subtype,
            srid: source_srid,
            dimensions: source_dimensions,
        } => matches!(
            &target.logical_type,
            LogicalType::Spatial {
                subtype: target_subtype,
                srid: target_srid,
                dimensions: target_dimensions,
            } if source_subtype.eq_ignore_ascii_case(target_subtype)
                && source_srid == target_srid
                && source_dimensions == target_dimensions
        ),
        LogicalType::Array { element: source } => matches!(
            &target.logical_type,
            LogicalType::Array { element: target } if source == target
        ),
        LogicalType::Struct { fields: source } => matches!(
            &target.logical_type,
            LogicalType::Struct { fields: target } if source == target
        ),
        LogicalType::Map {
            key: source_key,
            value: source_value,
        } => matches!(
            &target.logical_type,
            LogicalType::Map {
                key: target_key,
                value: target_value,
            } if source_key == target_key && source_value == target_value
        ),
        LogicalType::Range { element: source } => matches!(
            &target.logical_type,
            LogicalType::Range { element: target } if source == target
        ),
        LogicalType::MultiRange { element: source } => matches!(
            &target.logical_type,
            LogicalType::MultiRange { element: target } if source == target
        ),
        // Integer, decimal, and floating-point exact entries may deliberately
        // use a wider target logical domain (for example unsigned INT to
        // signed BIGINT).  The manifest entry is the evidence for that
        // widening; comparing the target field back to the source here would
        // incorrectly reject those key-safe representations.
        _ => true,
    }
}

fn text_binding_is_exact(source: &FieldDefinition, target: &FieldDefinition) -> bool {
    let (
        LogicalType::Text {
            charset: source_charset,
            max_length: source_length,
            length_unit: source_unit,
            ..
        },
        LogicalType::Text {
            charset: target_charset,
            max_length: target_length,
            length_unit: target_unit,
            ..
        },
    ) = (&source.logical_type, &target.logical_type)
    else {
        return matches!(target.logical_type, LogicalType::Opaque { .. });
    };
    text_charsets_are_compatible(source_charset, target_charset)
        && source_length == target_length
        && source_unit == target_unit
        && (source.collation.is_none()
            || target.collation.is_none()
            || source.collation == target.collation)
}

fn text_charsets_are_compatible(source: &str, target: &str) -> bool {
    if source.eq_ignore_ascii_case(target) {
        return true;
    }
    // PostgreSQL's UTF8 and MySQL's utf8mb4 both represent complete UTF-8.
    // MySQL's legacy three-byte utf8 is safe only when the target is the
    // wider encoding, never in the reverse direction.
    (target == "UTF8"
        && (source.eq_ignore_ascii_case("utf8") || source.eq_ignore_ascii_case("utf8mb4")))
        || (source == "UTF8" && target.eq_ignore_ascii_case("utf8mb4"))
        || (source.eq_ignore_ascii_case("utf8") && target.eq_ignore_ascii_case("utf8mb4"))
}

fn temporal_binding_is_exact(source: &FieldDefinition, target: &FieldDefinition) -> bool {
    let (source_kind, source_precision) = temporal_details(&source.logical_type);
    let (target_kind, target_precision) = temporal_details(&target.logical_type);
    if target_kind == "unknown" {
        return matches!(target.logical_type, LogicalType::Opaque { .. });
    }
    source_kind == target_kind && source_precision == target_precision
}

fn capability_matches_source(capability: &CapabilityEntry, source: &LogicalType) -> bool {
    if capability.source_logical_type == *source {
        return true;
    }
    match (&capability.source_logical_type, source) {
        (LogicalType::Integer { bits: 0, .. }, LogicalType::Integer { bits, .. }) => {
            matches!(bits, 8 | 16 | 24 | 32 | 64)
        }
        (LogicalType::Decimal { precision: 0, .. }, LogicalType::Decimal { precision, scale }) => {
            *precision > 0 && *scale >= 0
        }
        (LogicalType::Float { bits: 0 }, LogicalType::Float { bits }) => {
            matches!(bits, 32 | 64)
        }
        (
            LogicalType::Text { charset, .. },
            LogicalType::Text {
                charset: source_charset,
                ..
            },
        ) if charset == "*" => text_charset_supported(source_charset),
        (
            LogicalType::LocalDatetime {
                fractional_precision: u8::MAX,
            },
            LogicalType::LocalDatetime { .. },
        )
        | (
            LogicalType::Instant {
                fractional_precision: u8::MAX,
            },
            LogicalType::Instant { .. },
        )
        | (
            LogicalType::Duration {
                fractional_precision: u8::MAX,
            },
            LogicalType::Duration { .. },
        ) => true,
        (
            LogicalType::Enum { members },
            LogicalType::Enum {
                members: source_members,
            },
        ) if members.is_empty() => !source_members.is_empty(),
        (
            LogicalType::Set { members },
            LogicalType::Set {
                members: source_members,
            },
        ) if members.is_empty() => !source_members.is_empty(),
        (
            LogicalType::Spatial {
                subtype,
                srid: None,
                dimensions: 0,
            },
            LogicalType::Spatial { .. },
        ) if subtype == "*" => true,
        (
            LogicalType::Opaque {
                source_type,
                format,
            },
            source,
        ) if source_type == "*" && format == "recursive" => recursive_type_kind(source).is_some(),
        _ => false,
    }
}

fn same_members(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|member| right.contains(member))
}

fn normalize_options(
    rule: &ConversionRule,
    parameters: &BTreeMap<String, String>,
) -> (BTreeMap<String, String>, Vec<String>, Vec<String>) {
    let specs = rule
        .options
        .iter()
        .map(|option| (option.name.as_str(), option))
        .collect::<BTreeMap<_, _>>();
    let mut normalized = BTreeMap::new();
    let mut missing = Vec::new();
    let mut invalid = Vec::new();
    for (name, value) in parameters {
        let Some(spec) = specs.get(name.as_str()) else {
            invalid.push(format!("unknown option {name}"));
            continue;
        };
        if valid_option_value(spec, value) {
            normalized.insert(name.clone(), value.clone());
        } else {
            invalid.push(format!("option {name}"));
        }
    }
    for option in &rule.options {
        if normalized.contains_key(&option.name) {
            continue;
        }
        if let Some(default) = &option.default {
            if valid_option_value(option, default) {
                normalized.insert(option.name.clone(), default.clone());
            } else {
                invalid.push(format!("default for option {}", option.name));
            }
        } else if option.required {
            missing.push(option.name.clone());
        }
    }
    (normalized, missing, invalid)
}

fn valid_option_value(option: &OptionSpec, value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let kind_valid = match option.value_kind {
        OptionValueKind::Boolean => matches!(value, "true" | "false"),
        OptionValueKind::Integer => value.parse::<i128>().is_ok(),
        OptionValueKind::String | OptionValueKind::Enum => true,
    };
    kind_valid
        && (option.allowed_values.is_empty()
            || option.allowed_values.iter().any(|allowed| allowed == value))
}

fn build_plan(
    input: &CompatibilityInput<'_>,
    candidate: &TargetTypeCandidate,
    rule: &ConversionRule,
) -> ColumnConversionPlan {
    let input_digest = digest_of(&InputDigest {
        transaction: TransactionShape {
            source: &input.transaction.transaction().source,
            id: &input.transaction.transaction().id,
            begin_cursor: &input.transaction.transaction().begin_cursor,
            commit_cursor: &input.transaction.transaction().commit_cursor,
            changes: input
                .transaction
                .transaction()
                .changes
                .iter()
                .map(|change| ChangeShape {
                    operation: change.operation,
                    database: &change.database,
                    schema: &change.schema,
                    table: &change.table,
                    source_cursor: &change.source_cursor,
                    source_timestamp: change.source_timestamp,
                    schema_basis: &change.schema_basis,
                })
                .collect(),
        },
        source_field: &input.source_field,
        target_field: &input.target_field,
        mapping: &input.source_type_mapping,
        source_connector: &input.source_connector,
        sink_connector: &input.sink_connector,
        source_build: &input.source_build,
        target_build: &input.target_build,
        manifest_digest: &input.manifest.digest,
        target_probe_digest: &input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone()),
        options: RouteOptionsDigest {
            route_id: &input.options.route_id,
            configuration_revision: &input.options.configuration_revision,
            selected_rule: &input.options.selected_rule,
            parameters: &input.options.parameters,
        },
        candidate,
    });
    let mut planning_parameters = input.options.parameters.clone();
    planning_parameters.insert(
        "source_native_type".into(),
        input.source_field.native_type.clone(),
    );
    planning_parameters.insert(
        "target_native_type".into(),
        input.target_field.native_type.clone(),
    );
    let target = plan_target_representation(
        &input.source_field.logical_type,
        &input.target_field.logical_type,
        &candidate.target,
        &planning_parameters,
    );
    let mut target = target;
    if target.parameters.get("conversion_kind").map(String::as_str) == Some("text") {
        target.parameters.insert(
            "source_collation".into(),
            input
                .source_field
                .collation
                .clone()
                .unwrap_or_else(|| "none".into()),
        );
        target.parameters.insert(
            "target_collation".into(),
            input
                .target_field
                .collation
                .clone()
                .unwrap_or_else(|| "none".into()),
        );
    }
    let mut plan = ColumnConversionPlan {
        format: COMPATIBILITY_FORMAT.to_owned(),
        route_id: input.options.route_id.clone(),
        configuration_revision: input.options.configuration_revision.clone(),
        source_field: input.source_field.reference.clone(),
        target_field: input.target_field.reference.clone(),
        source_connector: input.source_connector.clone(),
        sink_connector: input.sink_connector.clone(),
        source_build: input.source_build.clone(),
        target_build: input.target_build.clone(),
        source_mapping_id: input.source_type_mapping.mapping_id.clone(),
        source_mapping_version: input.source_type_mapping.mapping_version.clone(),
        target,
        rule: candidate.rule.clone(),
        rule_digest: digest_of(rule),
        capability_code: candidate.capability_code.clone(),
        capability_manifest_digest: input.manifest.digest.clone(),
        target_probe_digest: input
            .options
            .target_probe
            .as_ref()
            .map(|probe| probe.digest.clone()),
        qualification: candidate.qualification,
        risk: candidate.risk,
        risk_code: candidate.risk_code.clone(),
        loss: LossAssessment::for_qualification(
            candidate.qualification,
            is_key_like(&input.source_field),
        ),
        examples: vec![ConversionExample {
            source: input.source_field.logical_type.family_name().into(),
            target: input.target_field.native_type.clone(),
        }],
        locator_impact: if is_key_like(&input.source_field) {
            LocatorImpact::Preserved
        } else if rule.qualification == QualificationLevel::ExplicitConversion {
            LocatorImpact::ValueOnly
        } else {
            LocatorImpact::NotUsed
        },
        confirmation: if candidate.requires_confirmation {
            PlanConfirmationState::Required
        } else {
            PlanConfirmationState::NotRequired
        },
        parameters: input.options.parameters.clone(),
        failure_policy: rule.failure_policy,
        input_digest,
        plan_digest: String::new(),
    };
    plan.plan_digest = plan.computed_digest();
    plan
}

fn digest_of<T: Serialize>(value: &T) -> String {
    let encoded = serde_json::to_vec(value).expect("compatibility model must be serializable");
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
