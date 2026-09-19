//! PostgreSQL 15's strict Source Type Mapping.
//!
//! PostgreSQL's formatted catalog type is used only as source-definition
//! evidence.  Values are never inspected to choose a mapping.  Types whose
//! comparison, padding, JSON, array, or extension semantics are not carried
//! by the current ChangeEvent contract fail closed.

use change_event::{ConnectorIdentity, LengthUnit, LogicalType, SourceTypeMapping};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt};

pub const MAPPING_VERSION: &str = "postgresql-15.source-type-mapping.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTypeMappingError {
    code: &'static str,
    message: String,
}

impl SourceTypeMappingError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "postgresql15.source_type.invalid_declaration",
            message: message.into(),
        }
    }

    fn unsupported(native_type: &str) -> Self {
        Self {
            code: "postgresql15.source_type.unsupported",
            message: format!(
                "PostgreSQL 15 native type {native_type:?} has no lossless LogicalType mapping"
            ),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for SourceTypeMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl Error for SourceTypeMappingError {}

/// Map a PostgreSQL 15 catalog type such as `numeric(30,6)` or
/// `timestamp(6) with time zone` to the public Source Type Mapping model.
pub fn source_type_mapping(native_type: &str) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    let native_type = normalize(native_type)?;
    let logical_type = logical_type(&native_type)?;
    let base = base_name(&native_type).to_ascii_lowercase();
    let mapping_id = format!("postgresql15.source-type.{base}");
    let evidence_digest = evidence_digest(&native_type, &logical_type);
    Ok(SourceTypeMapping {
        connector: ConnectorIdentity::new("postgresql", "15"),
        native_type,
        logical_type,
        mapping_id,
        mapping_version: MAPPING_VERSION.to_owned(),
        evidence_digest: Some(evidence_digest),
    })
}

pub fn map_source_type(native_type: &str) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping(native_type)
}

pub fn validate_native_type(native_type: &str) -> Result<(), SourceTypeMappingError> {
    let native_type = normalize(native_type)?;
    logical_type(&native_type).map(|_| ())
}

fn normalize(native_type: &str) -> Result<String, SourceTypeMappingError> {
    let raw = native_type.trim();
    let lower = raw.to_ascii_lowercase();
    if raw.is_empty() || raw.contains(';') || raw.contains('\0') {
        return Err(SourceTypeMappingError::invalid(
            "native type declaration is empty or contains forbidden syntax",
        ));
    }
    if lower.starts_with("enum(") {
        return Ok(raw.to_owned());
    }
    Ok(lower.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn base_name(native_type: &str) -> &str {
    native_type
        .split_once(['(', ' '])
        .map_or(native_type, |(base, _)| base)
}

fn logical_type(native_type: &str) -> Result<LogicalType, SourceTypeMappingError> {
    if base_name(native_type).eq_ignore_ascii_case("enum") {
        return Ok(LogicalType::Enum {
            members: enum_members(native_type)?,
        });
    }
    match native_type {
        "boolean" => Ok(LogicalType::boolean()),
        "smallint" => Ok(LogicalType::integer(true, 16)),
        "integer" | "int" => Ok(LogicalType::integer(true, 32)),
        "bigint" => Ok(LogicalType::integer(true, 64)),
        "real" => Ok(LogicalType::float(32)),
        "double precision" => Ok(LogicalType::float(64)),
        "text" => Ok(LogicalType::Text {
            charset: "UTF8".into(),
            max_length: None,
            length_unit: LengthUnit::Characters,
            collation: None,
        }),
        "bytea" => Ok(LogicalType::binary(None)),
        "bit" => Ok(LogicalType::bit_string(1)),
        "date" => Ok(LogicalType::date()),
        "uuid" => Ok(LogicalType::uuid()),
        "jsonb" => Ok(LogicalType::json()),
        "numeric" | "decimal" => Err(SourceTypeMappingError::unsupported(native_type)),
        _ if native_type.starts_with("numeric(") => numeric(native_type),
        _ if native_type.starts_with("decimal(") => numeric(native_type),
        _ if native_type.starts_with("bit(") => {
            let length = parenthesized_u64(native_type, "bit")?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    "bit length must be positive and bounded",
                ));
            }
            Ok(LogicalType::bit_string(length))
        }
        _ if native_type.starts_with("bit varying(") => {
            let length = parenthesized_u64(native_type, "bit varying")?;
            if !(1..=10_000_000).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    "bit varying length must be positive and bounded",
                ));
            }
            Ok(LogicalType::bit_string(length))
        }
        _ if native_type.starts_with("geometry(") => spatial(native_type),
        _ if native_type.starts_with("character varying(") => {
            let length = parenthesized_u64(native_type, "character varying")?;
            if length == 0 {
                return Err(SourceTypeMappingError::invalid(
                    "character varying length must be positive",
                ));
            }
            Ok(LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(length),
                length_unit: LengthUnit::Characters,
                collation: None,
            })
        }
        _ if native_type.starts_with("timestamp") => timestamp(native_type),
        _ if native_type.starts_with("char(") || native_type.starts_with("character(") => {
            Err(SourceTypeMappingError::unsupported(native_type))
        }
        _ => Err(SourceTypeMappingError::unsupported(native_type)),
    }
}

fn enum_members(native_type: &str) -> Result<Vec<String>, SourceTypeMappingError> {
    let open = native_type.find('(').ok_or_else(|| {
        SourceTypeMappingError::invalid("PostgreSQL ENUM declaration has no members")
    })?;
    let close = native_type
        .rfind(')')
        .filter(|close| *close > open && native_type[close + 1..].trim().is_empty())
        .ok_or_else(|| {
            SourceTypeMappingError::invalid("PostgreSQL ENUM declaration is malformed")
        })?;
    let mut chars = native_type[open + 1..close].chars().peekable();
    let mut members = Vec::new();
    loop {
        while chars
            .next_if(|character| character.is_whitespace())
            .is_some()
        {}
        if chars.peek().is_none() {
            break;
        }
        if chars.next() != Some('\'') {
            return Err(SourceTypeMappingError::invalid(
                "PostgreSQL ENUM labels must be quoted",
            ));
        }
        let mut member = String::new();
        let mut closed = false;
        while let Some(character) = chars.next() {
            match character {
                '\\' => {
                    let escaped = chars.next().ok_or_else(|| {
                        SourceTypeMappingError::invalid(
                            "PostgreSQL ENUM label has a dangling escape",
                        )
                    })?;
                    member.push(escaped);
                }
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    member.push('\'');
                }
                '\'' => {
                    closed = true;
                    break;
                }
                other => member.push(other),
            }
        }
        if !closed || member.is_empty() || member.contains('\0') {
            return Err(SourceTypeMappingError::invalid(
                "PostgreSQL ENUM label is empty, unterminated, or contains NUL",
            ));
        }
        if members.iter().any(|known| known == &member) {
            return Err(SourceTypeMappingError::invalid(
                "PostgreSQL ENUM labels must be unique",
            ));
        }
        members.push(member);
        while chars
            .next_if(|character| character.is_whitespace())
            .is_some()
        {}
        match chars.next() {
            None => break,
            Some(',') => {}
            Some(_) => {
                return Err(SourceTypeMappingError::invalid(
                    "PostgreSQL ENUM labels must be comma separated",
                ));
            }
        }
    }
    if members.is_empty() {
        return Err(SourceTypeMappingError::invalid(
            "PostgreSQL ENUM requires at least one label",
        ));
    }
    Ok(members)
}

fn numeric(native_type: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let parameters = native_type
        .split_once('(')
        .and_then(|(_, value)| value.strip_suffix(')'))
        .ok_or_else(|| SourceTypeMappingError::invalid("numeric parameters are malformed"))?;
    let (precision, scale) = parameters
        .split_once(',')
        .ok_or_else(|| SourceTypeMappingError::invalid("numeric requires precision and scale"))?;
    let precision = precision
        .trim()
        .parse::<u16>()
        .map_err(|_| SourceTypeMappingError::invalid("numeric precision is invalid"))?;
    let scale = scale
        .trim()
        .parse::<i32>()
        .map_err(|_| SourceTypeMappingError::invalid("numeric scale is invalid"))?;
    if !(1..=1000).contains(&precision) || scale < 0 || scale > i32::from(precision) {
        return Err(SourceTypeMappingError::invalid(
            "numeric precision/scale is outside the ChangeEvent range",
        ));
    }
    Ok(LogicalType::decimal(precision, scale))
}

fn parenthesized_u64(native_type: &str, prefix: &str) -> Result<u64, SourceTypeMappingError> {
    let value = native_type
        .strip_prefix(&format!("{prefix}("))
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| SourceTypeMappingError::invalid("type parameters are malformed"))?;
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| SourceTypeMappingError::invalid("type length is invalid"))
}

fn spatial(native_type: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let parameters = native_type
        .strip_prefix("geometry(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| SourceTypeMappingError::invalid("geometry declaration is malformed"))?;
    let mut parts = parameters.split(',').map(str::trim);
    let declared_subtype = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SourceTypeMappingError::invalid("geometry subtype is missing"))?;
    let srid = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SourceTypeMappingError::invalid("geometry SRID is missing"))?
        .parse::<i32>()
        .map_err(|_| SourceTypeMappingError::invalid("geometry SRID is invalid"))?;
    if parts.next().is_some() || srid < 0 {
        return Err(SourceTypeMappingError::invalid(
            "geometry requires exactly one non-negative SRID",
        ));
    }
    let declared_subtype = declared_subtype.to_ascii_lowercase();
    let (subtype, dimensions) = if let Some(subtype) = declared_subtype.strip_suffix("zm") {
        (subtype, 4)
    } else if let Some(subtype) = declared_subtype.strip_suffix('z') {
        (subtype, 3)
    } else if let Some(subtype) = declared_subtype.strip_suffix('m') {
        (subtype, 3)
    } else {
        (declared_subtype.as_str(), 2)
    };
    if !matches!(
        subtype,
        "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    ) {
        return Err(SourceTypeMappingError::unsupported(native_type));
    }
    Ok(LogicalType::spatial(subtype, Some(srid), dimensions))
}

fn timestamp(native_type: &str) -> Result<LogicalType, SourceTypeMappingError> {
    let (prefix, zone) = if native_type.ends_with(" without time zone") {
        ("timestamp", " without time zone")
    } else if native_type.ends_with(" with time zone") {
        ("timestamp", " with time zone")
    } else {
        return Err(SourceTypeMappingError::invalid(
            "timestamp must declare its time-zone semantics",
        ));
    };
    let precision = native_type
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(zone))
        .map(|value| value.trim_matches(['(', ')']))
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u8>()
                .map_err(|_| SourceTypeMappingError::invalid("timestamp precision is invalid"))
        })
        .transpose()?
        .unwrap_or(6);
    if precision > 6 {
        return Err(SourceTypeMappingError::invalid(
            "timestamp precision is outside PostgreSQL 15 limits",
        ));
    }
    Ok(if zone == " with time zone" {
        LogicalType::Instant {
            fractional_precision: precision,
        }
    } else {
        LogicalType::LocalDatetime {
            fractional_precision: precision,
        }
    })
}

fn evidence_digest(native_type: &str, logical_type: &LogicalType) -> String {
    let bytes = serde_json::to_vec(&(MAPPING_VERSION, native_type, logical_type))
        .expect("PostgreSQL SourceTypeMapping evidence is serializable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_postgresql_native_shapes_without_value_inference() {
        assert_eq!(
            source_type_mapping("numeric(30,6)").unwrap().logical_type,
            LogicalType::decimal(30, 6)
        );
        assert_eq!(
            source_type_mapping("timestamp(3) with time zone")
                .unwrap()
                .logical_type,
            LogicalType::Instant {
                fractional_precision: 3
            }
        );
        assert_eq!(
            source_type_mapping("character varying(255)")
                .unwrap()
                .logical_type,
            LogicalType::Text {
                charset: "UTF8".into(),
                max_length: Some(255),
                length_unit: LengthUnit::Characters,
                collation: None,
            }
        );
    }

    #[test]
    fn blocks_unproven_json_arrays_and_unbounded_numeric() {
        for native_type in ["json", "integer[]", "numeric", "character(10)"] {
            assert!(source_type_mapping(native_type).is_err(), "{native_type}");
        }
    }

    #[test]
    fn maps_postgresql_enum_labels_without_using_ordinals() {
        assert_eq!(
            source_type_mapping("enum('Ready','blocked')")
                .unwrap()
                .logical_type,
            LogicalType::Enum {
                members: vec!["Ready".into(), "blocked".into()]
            }
        );
        assert!(source_type_mapping("enum('a','a')").is_err());
        assert_eq!(
            source_type_mapping("bit varying(12)").unwrap().logical_type,
            LogicalType::bit_string(12)
        );
        assert_eq!(
            source_type_mapping("geometry(point,4326)")
                .unwrap()
                .logical_type,
            LogicalType::spatial("point", Some(4326), 2)
        );
    }
}
