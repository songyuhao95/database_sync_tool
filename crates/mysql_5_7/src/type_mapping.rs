//! MySQL 5.7's strict Source Type Mapping.
//!
//! The binlog contains values, but it is not a schema authority: in
//! particular, TABLE_MAP does not carry all of the declaration semantics that
//! are needed to interpret signedness, text encoding, or temporal precision.
//! This module therefore maps the exact INFORMATION_SCHEMA declaration and its
//! catalog semantics before a value is decoded.  Unknown or value-less native
//! types remain blocked instead of being converted to text or binary.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{ConnectorIdentity, LengthUnit, LogicalType, LogicalValue, SourceTypeMapping};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt};

pub const MAPPING_VERSION: &str = "mysql-5.7.source-type-mapping.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTypeMappingError {
    code: &'static str,
    message: String,
}

impl SourceTypeMappingError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "mysql57.source_type.invalid_declaration",
            message: message.into(),
        }
    }

    fn unsupported(base: &str) -> Self {
        Self {
            code: "mysql57.source_type.unsupported",
            message: format!("MySQL 5.7 native type {base:?} has no lossless LogicalType mapping"),
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

#[derive(Debug)]
struct NativeDeclaration {
    base: String,
    arguments: Vec<String>,
    modifiers: Vec<String>,
}

/// Map one MySQL 5.7 column declaration to the database-neutral Source Type
/// Mapping. `charset` and `collation` come from the same catalog row as the
/// native declaration; a text mapping without a proven character set is
/// intentionally rejected.
pub fn source_type_mapping(
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    let declaration = parse_declaration(native_type)?;
    let logical_type = logical_type(&declaration, charset, collation)?;
    let normalized_native = native_type.trim().to_ascii_lowercase();
    let mapping_id = format!("mysql57.source-type.{}", declaration.base);
    let evidence_digest = evidence_digest(&normalized_native, charset, collation, &logical_type);
    Ok(SourceTypeMapping {
        connector: ConnectorIdentity::new("mysql", "5.7"),
        native_type: normalized_native,
        logical_type,
        mapping_id,
        mapping_version: MAPPING_VERSION.to_owned(),
        evidence_digest: Some(evidence_digest),
        source_definition_fingerprint: None,
        source_build: None,
        environment_fingerprint: None,
    })
}

/// Alias named after the public Source Type Mapping boundary.
pub fn map_source_type(
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<SourceTypeMapping, SourceTypeMappingError> {
    source_type_mapping(native_type, charset, collation)
}

/// Decode MySQL's native ENUM/SET numeric binlog representation into the
/// database-neutral label/member representation. The numeric value is only a
/// wire encoding; it never crosses the SourceAdapter boundary as an ordinal.
pub fn decode_enum_set_ordinal(
    logical_type: &LogicalType,
    value: u64,
) -> Result<LogicalValue, String> {
    match logical_type {
        LogicalType::Enum { members } => {
            let index = usize::try_from(value)
                .map_err(|_| "ENUM ordinal does not fit the declared member index".to_owned())?;
            if index == 0 || index > members.len() {
                return Err("ENUM ordinal is outside the declared label list".to_owned());
            }
            Ok(LogicalValue::Enum {
                label: members[index - 1].clone(),
            })
        }
        LogicalType::Set { members } => {
            if members.len() < u64::BITS as usize && value >> members.len() != 0 {
                return Err("SET bitmask contains a member outside the declared list".to_owned());
            }
            Ok(LogicalValue::Set {
                members: members
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| value & (1_u64 << index) != 0)
                    .map(|(_, member)| member.clone())
                    .collect(),
            })
        }
        _ => Err("native type is not ENUM or SET".to_owned()),
    }
}

/// Decode the little-endian bitmask used by MySQL's SET binlog value.
pub fn decode_set_bitmask(
    logical_type: &LogicalType,
    bytes: &[u8],
) -> Result<LogicalValue, String> {
    if bytes.len() > u64::BITS as usize {
        return Err("SET bitmask is wider than the portable 64-bit SET limit".to_owned());
    }
    let value = bytes
        .iter()
        .enumerate()
        .try_fold(0_u64, |value, (index, byte)| {
            let shift = u32::try_from(index * 8)
                .map_err(|_| "SET bitmask byte offset is invalid".to_owned())?;
            Ok::<_, String>(value | (u64::from(*byte) << shift))
        })?;
    decode_enum_set_ordinal(logical_type, value)
}

/// Decode MySQL's internal spatial representation. MySQL prefixes the WKB
/// payload with a little-endian four-byte SRID; the public value contract
/// carries the standard WKB payload and records that SRID separately.
pub fn decode_spatial_value(
    logical_type: &LogicalType,
    bytes: &[u8],
) -> Result<LogicalValue, String> {
    let LogicalType::Spatial {
        subtype,
        srid: expected_srid,
        dimensions: expected_dimensions,
    } = logical_type
    else {
        return Err("native type is not spatial".to_owned());
    };
    if bytes.len() < 9 {
        return Err("MySQL spatial value is missing its SRID or WKB header".to_owned());
    }
    let actual_srid = i32::try_from(u32::from_le_bytes(bytes[..4].try_into().unwrap()))
        .map_err(|_| "MySQL spatial SRID exceeds the portable range".to_owned())?;
    if expected_srid.is_some_and(|expected| expected != actual_srid) {
        return Err("MySQL spatial value SRID differs from the column declaration".to_owned());
    }
    let wkb = &bytes[4..];
    let (geometry_type, dimensions) = spatial_wire_header(wkb)?;
    if !geometry_type.eq_ignore_ascii_case(subtype) || dimensions != *expected_dimensions {
        return Err("MySQL spatial WKB header differs from the column declaration".to_owned());
    }
    Ok(LogicalValue::Spatial {
        format: change_event::SpatialFormat::Wkb,
        bytes_base64url: URL_SAFE_NO_PAD.encode(wkb),
        geometry_type: geometry_type.to_owned(),
        dimensions,
        srid: Some(actual_srid),
        crs: Some(format!("srid:{actual_srid}")),
    })
}

fn spatial_wire_header(bytes: &[u8]) -> Result<(&'static str, u8), String> {
    if bytes.len() < 5 {
        return Err("spatial WKB header is truncated".to_owned());
    }
    let little_endian = match bytes[0] {
        0 => false,
        1 => true,
        _ => return Err("spatial WKB byte order marker is invalid".to_owned()),
    };
    let geometry_code = if little_endian {
        u32::from_le_bytes(bytes[1..5].try_into().unwrap())
    } else {
        u32::from_be_bytes(bytes[1..5].try_into().unwrap())
    };
    if geometry_code & 0x2000_0000 != 0 {
        return Err("MySQL spatial value unexpectedly contains an EWKB SRID".to_owned());
    }
    let has_z = geometry_code & 0x8000_0000 != 0;
    let has_m = geometry_code & 0x4000_0000 != 0;
    let mut base_code = geometry_code & 0x0fff_ffff;
    let mut dimensions = 2 + u8::from(has_z) + u8::from(has_m);
    if base_code >= 1_000 {
        let suffix = base_code / 1_000;
        base_code %= 1_000;
        dimensions = match suffix {
            1 | 2 => 3,
            3 => 4,
            _ => return Err("spatial WKB dimension code is invalid".to_owned()),
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
        _ => return Err("spatial WKB geometry type is unsupported".to_owned()),
    };
    Ok((geometry_type, dimensions))
}

/// Validate a native declaration when there is no row value from which a text
/// character set could be recovered (for example, a NULL-only image).
pub fn validate_native_type(native_type: &str) -> Result<(), SourceTypeMappingError> {
    let declaration = parse_declaration(native_type)?;
    validate_shape(&declaration)?;
    Ok(())
}

fn parse_declaration(native_type: &str) -> Result<NativeDeclaration, SourceTypeMappingError> {
    let raw = native_type.trim();
    let value = raw.to_ascii_lowercase();
    if value.is_empty() {
        return Err(SourceTypeMappingError::invalid(
            "native type declaration is empty",
        ));
    }

    // ENUM and SET arguments are quoted labels, not comma-separated numeric
    // parameters. Keep their spelling and order because those are catalog
    // evidence; the runtime conversion will use labels/members, never an
    // ordinal or a comma-delimited text fallback.
    let raw_head = raw.split('(').next().unwrap_or(raw);
    let raw_base = raw_head
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(raw_base.as_str(), "enum" | "set") {
        let open = raw.find('(').ok_or_else(|| {
            SourceTypeMappingError::invalid(format!("{raw_base} requires quoted members"))
        })?;
        let close = raw
            .rfind(')')
            .filter(|close| *close > open)
            .ok_or_else(|| SourceTypeMappingError::invalid("ENUM/SET member list is unclosed"))?;
        if !raw[close + 1..].trim().is_empty() || raw[..open].contains(')') {
            return Err(SourceTypeMappingError::invalid(
                "ENUM/SET declaration has misplaced characters",
            ));
        }
        let modifiers = raw_head
            .split_whitespace()
            .skip(1)
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        return Ok(NativeDeclaration {
            base: raw_base,
            arguments: parse_quoted_members(&raw[open + 1..close])?,
            modifiers,
        });
    }

    let (head, arguments, tail) = if let Some(open) = value.find('(') {
        let close = value
            .rfind(')')
            .filter(|close| *close > open)
            .ok_or_else(|| SourceTypeMappingError::invalid("native type arguments are unclosed"))?;
        if value[close + 1..].contains('(') || value[..open].contains(')') {
            return Err(SourceTypeMappingError::invalid(
                "native type declaration has misplaced parentheses",
            ));
        }
        (
            &value[..open],
            value[open + 1..close]
                .split(',')
                .map(str::trim)
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            &value[close + 1..],
        )
    } else {
        (value.as_str(), Vec::new(), "")
    };

    let mut words = head.split_whitespace();
    let base = words
        .next()
        .ok_or_else(|| SourceTypeMappingError::invalid("native type has no base name"))?;
    let mut modifiers = words.map(str::to_owned).collect::<Vec<_>>();
    modifiers.extend(tail.split_whitespace().map(str::to_owned));
    if arguments.iter().any(|argument| argument.is_empty()) {
        return Err(SourceTypeMappingError::invalid(
            "native type contains an empty argument",
        ));
    }
    Ok(NativeDeclaration {
        base: base.to_owned(),
        arguments,
        modifiers,
    })
}

fn parse_quoted_members(value: &str) -> Result<Vec<String>, SourceTypeMappingError> {
    let mut chars = value.chars().peekable();
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
                "ENUM/SET members must use single-quoted labels",
            ));
        }
        let mut member = String::new();
        let mut closed = false;
        while let Some(character) = chars.next() {
            match character {
                '\\' => {
                    let escaped = chars.next().ok_or_else(|| {
                        SourceTypeMappingError::invalid("ENUM/SET label has a dangling escape")
                    })?;
                    member.push(match escaped {
                        '0' => '\0',
                        'b' => '\u{0008}',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        'Z' => '\u{001a}',
                        other => other,
                    });
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
                "ENUM/SET label is empty, unterminated, or contains NUL",
            ));
        }
        if members.iter().any(|known| known == &member) {
            return Err(SourceTypeMappingError::invalid(
                "ENUM/SET labels must be unique",
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
                    "ENUM/SET members must be comma separated",
                ));
            }
        }
    }
    if members.is_empty() {
        return Err(SourceTypeMappingError::invalid(
            "ENUM/SET requires at least one member",
        ));
    }
    Ok(members)
}

fn logical_type(
    declaration: &NativeDeclaration,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<LogicalType, SourceTypeMappingError> {
    validate_shape(declaration)?;
    let has_unsigned = declaration
        .modifiers
        .iter()
        .any(|value| value == "unsigned");
    let base = declaration.base.as_str();
    match base {
        "tinyint" => Ok(LogicalType::integer(!has_unsigned, 8)),
        "smallint" => Ok(LogicalType::integer(!has_unsigned, 16)),
        "mediumint" => Ok(LogicalType::integer(!has_unsigned, 24)),
        "int" | "integer" => Ok(LogicalType::integer(!has_unsigned, 32)),
        "bigint" => Ok(LogicalType::integer(!has_unsigned, 64)),
        "decimal" | "numeric" => {
            if has_unsigned {
                return Err(SourceTypeMappingError::unsupported(base));
            }
            let precision = parse_u16(&declaration.arguments[0], "precision")?;
            let scale = parse_u16(&declaration.arguments[1], "scale")?;
            Ok(LogicalType::decimal(precision, i32::from(scale)))
        }
        "float" => {
            if declaration
                .modifiers
                .iter()
                .any(|value| value == "unsigned")
            {
                return Err(SourceTypeMappingError::unsupported("float unsigned"));
            }
            Ok(LogicalType::float(float_bits(declaration)?))
        }
        "double" | "real" => {
            reject_modifiers(declaration, &[])?;
            Ok(LogicalType::float(64))
        }
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
            let charset = charset
                .map(str::trim)
                .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
                .ok_or_else(|| {
                    SourceTypeMappingError::invalid(
                        "text SourceTypeMapping requires a catalog character set",
                    )
                })?
                .to_ascii_lowercase();
            let max_length = text_length(declaration)?;
            let length_unit = if matches!(base, "char" | "varchar") {
                LengthUnit::Characters
            } else {
                LengthUnit::Bytes
            };
            // Collation is retained in the source FieldDefinition. It is not
            // a value-level property and therefore is deliberately not copied
            // into LogicalType.
            let _ = collation;
            Ok(LogicalType::Text {
                charset,
                max_length,
                length_unit,
                collation: None,
            })
        }
        "binary" => Ok(LogicalType::Binary {
            max_length: Some(binary_fixed_length(declaration)?),
        }),
        "bit" => Ok(LogicalType::bit_string(bit_length(declaration)?)),
        "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => Ok(LogicalType::Binary {
            max_length: binary_length(declaration)?,
        }),
        "date" => Ok(LogicalType::date()),
        "datetime" => Ok(LogicalType::LocalDatetime {
            fractional_precision: fractional_precision(declaration)?,
        }),
        "timestamp" => Ok(LogicalType::Instant {
            fractional_precision: fractional_precision(declaration)?,
        }),
        "time" => Ok(LogicalType::Duration {
            fractional_precision: fractional_precision(declaration)?,
        }),
        "year" => Ok(LogicalType::year()),
        "json" => Ok(LogicalType::json()),
        "point" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
        | "geometrycollection" => {
            let (subtype, srid, dimensions) = spatial_shape(declaration)?;
            Ok(LogicalType::spatial(subtype, srid, dimensions))
        }
        "enum" => Ok(LogicalType::Enum {
            members: declaration.arguments.clone(),
        }),
        "set" => Ok(LogicalType::Set {
            members: declaration.arguments.clone(),
        }),
        _ => Err(SourceTypeMappingError::unsupported(base)),
    }
}

fn validate_shape(declaration: &NativeDeclaration) -> Result<(), SourceTypeMappingError> {
    let base = declaration.base.as_str();
    match base {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(format!(
                    "{base} accepts at most one display-width argument"
                )));
            }
            reject_modifiers(declaration, &["unsigned"])
        }
        "decimal" | "numeric" => {
            if declaration.arguments.len() != 2 {
                return Err(SourceTypeMappingError::invalid(
                    "DECIMAL requires precision and scale",
                ));
            }
            let precision = parse_u16(&declaration.arguments[0], "precision")?;
            let scale = parse_u16(&declaration.arguments[1], "scale")?;
            if !(1..=65).contains(&precision) || scale > 30 || scale > precision {
                return Err(SourceTypeMappingError::invalid(
                    "DECIMAL precision/scale is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "float" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(
                    "FLOAT accepts at most one precision argument",
                ));
            }
            if let Some(precision) = declaration.arguments.first() {
                let precision = parse_u16(precision, "FLOAT precision")?;
                if precision > 53 {
                    return Err(SourceTypeMappingError::invalid(
                        "FLOAT precision is outside MySQL 5.7 limits",
                    ));
                }
            }
            reject_modifiers(declaration, &[])
        }
        "double" | "real" => {
            if !declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(
                    "DOUBLE/REAL does not accept a precision argument here",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "char" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(
                    "CHAR accepts at most one length argument",
                ));
            }
            let length = declaration
                .arguments
                .first()
                .map(|value| parse_u64(value, "text length"))
                .transpose()?
                .unwrap_or(1);
            if length == 0 || length > 255 {
                return Err(SourceTypeMappingError::invalid(
                    "CHAR length is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "varchar" => {
            if declaration.arguments.len() != 1 {
                return Err(SourceTypeMappingError::invalid(
                    "VARCHAR requires one length argument",
                ));
            }
            let length = parse_u64(&declaration.arguments[0], "text length")?;
            if length == 0 || length > 65_535 {
                return Err(SourceTypeMappingError::invalid(
                    "CHAR/VARCHAR length is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "tinytext" | "text" | "mediumtext" | "longtext" => {
            if !declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(
                    "TEXT types do not accept a length argument",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "binary" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(
                    "BINARY accepts at most one length argument",
                ));
            }
            let length = declaration
                .arguments
                .first()
                .map(|value| parse_u64(value, "binary length"))
                .transpose()?
                .unwrap_or(1);
            if !(1..=255).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    "BINARY length is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "bit" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(
                    "BIT accepts at most one length argument",
                ));
            }
            let length = declaration
                .arguments
                .first()
                .map(|value| parse_u64(value, "bit length"))
                .transpose()?
                .unwrap_or(1);
            if !(1..=64).contains(&length) {
                return Err(SourceTypeMappingError::invalid(
                    "BIT length is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "varbinary" => {
            if declaration.arguments.len() != 1 {
                return Err(SourceTypeMappingError::invalid(
                    "VARBINARY requires one length argument",
                ));
            }
            let length = parse_u64(&declaration.arguments[0], "binary length")?;
            if length == 0 || length > 65_535 {
                return Err(SourceTypeMappingError::invalid(
                    "BINARY/VARBINARY length is outside MySQL 5.7 limits",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            if !declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(
                    "BLOB types do not accept a length argument",
                ));
            }
            reject_modifiers(declaration, &[])
        }
        "date" | "json" => {
            if !declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(format!(
                    "{base} does not accept arguments"
                )));
            }
            reject_modifiers(declaration, &[])
        }
        "point" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
        | "geometrycollection" => {
            if !declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(format!(
                    "{base} does not accept arguments"
                )));
            }
            spatial_shape(declaration).map(|_| ())
        }
        "enum" | "set" => {
            if declaration.arguments.is_empty() {
                return Err(SourceTypeMappingError::invalid(format!(
                    "{base} requires at least one member"
                )));
            }
            reject_modifiers(declaration, &[])
        }
        "datetime" | "timestamp" | "time" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(format!(
                    "{base} accepts at most one fractional-precision argument"
                )));
            }
            if let Some(precision) = declaration.arguments.first() {
                let precision = parse_u16(precision, "fractional precision")?;
                if precision > 6 {
                    return Err(SourceTypeMappingError::invalid(
                        "fractional precision is outside MySQL 5.7 limits",
                    ));
                }
            }
            reject_modifiers(declaration, &[])
        }
        "year" => {
            if declaration.arguments.len() > 1 {
                return Err(SourceTypeMappingError::invalid(
                    "YEAR accepts at most one display-width argument",
                ));
            }
            if let Some(width) = declaration.arguments.first()
                && parse_u16(width, "YEAR width")? != 4
            {
                return Err(SourceTypeMappingError::unsupported("year(2)"));
            }
            reject_modifiers(declaration, &[])
        }
        _ => Err(SourceTypeMappingError::unsupported(base)),
    }
}

fn reject_modifiers(
    declaration: &NativeDeclaration,
    allowed: &[&str],
) -> Result<(), SourceTypeMappingError> {
    if let Some(modifier) = declaration
        .modifiers
        .iter()
        .find(|modifier| !allowed.iter().any(|value| value == modifier))
    {
        return Err(SourceTypeMappingError::invalid(format!(
            "native type modifier {modifier:?} is not qualified"
        )));
    }
    Ok(())
}

fn spatial_shape(
    declaration: &NativeDeclaration,
) -> Result<(String, Option<i32>, u8), SourceTypeMappingError> {
    let srid = match declaration.modifiers.as_slice() {
        [] => None,
        [marker, value] if marker == "srid" => {
            let value = value.parse::<u64>().map_err(|_| {
                SourceTypeMappingError::invalid(format!("invalid spatial SRID: {value:?}"))
            })?;
            Some(i32::try_from(value).map_err(|_| {
                SourceTypeMappingError::unsupported("spatial SRID outside LogicalType range")
            })?)
        }
        _ => {
            return Err(SourceTypeMappingError::invalid(
                "spatial type accepts only an optional SRID declaration",
            ));
        }
    };
    Ok((declaration.base.clone(), srid, 2))
}

fn parse_u16(value: &str, label: &str) -> Result<u16, SourceTypeMappingError> {
    value
        .parse()
        .map_err(|_| SourceTypeMappingError::invalid(format!("invalid {label}: {value:?}")))
}

fn parse_u64(value: &str, label: &str) -> Result<u64, SourceTypeMappingError> {
    value
        .parse()
        .map_err(|_| SourceTypeMappingError::invalid(format!("invalid {label}: {value:?}")))
}

fn float_bits(declaration: &NativeDeclaration) -> Result<u8, SourceTypeMappingError> {
    let Some(precision) = declaration.arguments.first() else {
        return Ok(32);
    };
    let precision = parse_u16(precision, "FLOAT precision")?;
    Ok(if precision <= 24 { 32 } else { 64 })
}

fn text_length(declaration: &NativeDeclaration) -> Result<Option<u64>, SourceTypeMappingError> {
    match declaration.base.as_str() {
        "char" => Ok(Some(
            declaration
                .arguments
                .first()
                .map(|value| parse_u64(value, "text length"))
                .transpose()?
                .unwrap_or(1),
        )),
        "varchar" => Ok(Some(parse_u64(&declaration.arguments[0], "text length")?)),
        "tinytext" => Ok(Some(255)),
        "text" => Ok(Some(65_535)),
        "mediumtext" => Ok(Some(16_777_215)),
        "longtext" => Ok(Some(4_294_967_295)),
        _ => unreachable!("text_length called for non-text type"),
    }
}

fn binary_length(declaration: &NativeDeclaration) -> Result<Option<u64>, SourceTypeMappingError> {
    match declaration.base.as_str() {
        "varbinary" => Ok(Some(parse_u64(&declaration.arguments[0], "binary length")?)),
        "tinyblob" => Ok(Some(255)),
        "blob" => Ok(Some(65_535)),
        "mediumblob" => Ok(Some(16_777_215)),
        "longblob" => Ok(Some(4_294_967_295)),
        _ => unreachable!("binary_length called for non-binary type"),
    }
}

fn binary_fixed_length(declaration: &NativeDeclaration) -> Result<u64, SourceTypeMappingError> {
    Ok(declaration
        .arguments
        .first()
        .map(|value| parse_u64(value, "binary length"))
        .transpose()?
        .unwrap_or(1))
}

fn bit_length(declaration: &NativeDeclaration) -> Result<u64, SourceTypeMappingError> {
    Ok(declaration
        .arguments
        .first()
        .map(|value| parse_u64(value, "bit length"))
        .transpose()?
        .unwrap_or(1))
}

fn fractional_precision(declaration: &NativeDeclaration) -> Result<u8, SourceTypeMappingError> {
    declaration
        .arguments
        .first()
        .map(|value| parse_u16(value, "fractional precision"))
        .transpose()
        .map(|value| value.unwrap_or(0) as u8)
}

fn evidence_digest(
    native_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
    logical_type: &LogicalType,
) -> String {
    let input = serde_json::to_vec(&(
        MAPPING_VERSION,
        native_type,
        charset,
        collation,
        logical_type,
    ))
    .expect("SourceTypeMapping evidence is serializable");
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use change_event::{LengthUnit, LogicalType};

    #[test]
    fn maps_signedness_width_and_declared_parameters() {
        assert_eq!(
            source_type_mapping("INT(11) UNSIGNED", None, None)
                .unwrap()
                .logical_type,
            LogicalType::integer(false, 32)
        );
        assert_eq!(
            source_type_mapping("mediumint", None, None)
                .unwrap()
                .logical_type,
            LogicalType::integer(true, 24)
        );
        assert_eq!(
            source_type_mapping("decimal(30,6)", None, None)
                .unwrap()
                .logical_type,
            LogicalType::decimal(30, 6)
        );
        assert_eq!(
            source_type_mapping("datetime(6)", None, None)
                .unwrap()
                .logical_type,
            LogicalType::LocalDatetime {
                fractional_precision: 6
            }
        );
    }

    #[test]
    fn tinyint_one_is_integer_not_boolean() {
        assert_eq!(
            source_type_mapping("tinyint(1)", None, None)
                .unwrap()
                .logical_type,
            LogicalType::integer(true, 8)
        );
    }

    #[test]
    fn text_mapping_requires_catalog_encoding_and_keeps_bounds() {
        let mapping =
            source_type_mapping("varchar(255)", Some("utf8mb4"), Some("utf8mb4_bin")).unwrap();
        assert_eq!(
            mapping.logical_type,
            LogicalType::Text {
                charset: "utf8mb4".into(),
                max_length: Some(255),
                length_unit: LengthUnit::Characters,
                collation: None,
            }
        );
        assert!(source_type_mapping("varchar(255)", None, None).is_err());
    }

    #[test]
    fn char_mapping_is_text_with_fixed_declared_length() {
        let mapping =
            source_type_mapping("char(10)", Some("utf8mb4"), Some("utf8mb4_bin")).unwrap();
        assert_eq!(
            mapping.logical_type,
            LogicalType::Text {
                charset: "utf8mb4".into(),
                max_length: Some(10),
                length_unit: LengthUnit::Characters,
                collation: None,
            }
        );
    }

    #[test]
    fn unsupported_or_out_of_range_types_fail_closed() {
        assert_eq!(
            source_type_mapping("geometry", Some("utf8mb4"), None)
                .unwrap_err()
                .code(),
            "mysql57.source_type.unsupported"
        );
        assert!(source_type_mapping("decimal(66,0)", None, None).is_err());
        assert!(source_type_mapping("datetime(7)", None, None).is_err());
        assert!(source_type_mapping("decimal(10,2) unsigned", None, None).is_err());
        assert_eq!(
            source_type_mapping("bit(8)", None, None)
                .unwrap()
                .logical_type,
            LogicalType::bit_string(8)
        );
        assert_eq!(
            source_type_mapping("binary(16)", None, None)
                .unwrap()
                .logical_type,
            LogicalType::binary(Some(16))
        );
    }

    #[test]
    fn enum_and_set_preserve_labels_without_ordinal_semantics() {
        assert_eq!(
            source_type_mapping("ENUM('Ready','blocked')", None, None)
                .unwrap()
                .logical_type,
            LogicalType::Enum {
                members: vec!["Ready".into(), "blocked".into()]
            }
        );
        assert_eq!(
            source_type_mapping("set('a','b\\'s')", None, None)
                .unwrap()
                .logical_type,
            LogicalType::Set {
                members: vec!["a".into(), "b's".into()]
            }
        );
        assert!(source_type_mapping("enum('a','a')", None, None).is_err());
        assert!(source_type_mapping("set(a,b)", None, None).is_err());
    }

    #[test]
    fn native_validation_rejects_unsupported_null_only_columns() {
        assert!(validate_native_type("enum('a','b')").is_ok());
        assert!(validate_native_type("varchar(255)").is_ok());
    }

    #[test]
    fn maps_declared_spatial_subtypes_and_rejects_unbounded_geometry() {
        assert_eq!(
            source_type_mapping("POINT SRID 4326", None, None)
                .unwrap()
                .logical_type,
            LogicalType::Spatial {
                subtype: "point".into(),
                srid: Some(4326),
                dimensions: 2,
            }
        );
        assert_eq!(
            source_type_mapping("linestring", None, None)
                .unwrap()
                .logical_type,
            LogicalType::Spatial {
                subtype: "linestring".into(),
                srid: None,
                dimensions: 2,
            }
        );
        assert_eq!(
            source_type_mapping("geometry", None, None)
                .unwrap_err()
                .code(),
            "mysql57.source_type.unsupported"
        );
    }
}
