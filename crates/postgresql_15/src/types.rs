use crate::type_mapping::{SourceTypeCatalog, SourceTypeDefinitionKind};
use crate::{Result, invalid};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    JsonEntry, JsonValue as J, LogicalValue as V, MapEntry, SpatialFormat, StructuredField,
    TemporalInfinityKind,
};

pub(crate) fn supported(oid: u32) -> bool {
    matches!(
        oid,
        16 | 17
            | 18
            | 19
            | 20
            | 21
            | 23
            | 26
            | 28
            | 29
            | 25
            | 114
            | 142
            | 650
            | 774
            | 829
            | 869
            | 700
            | 701
            | 1042
            | 1043
            | 1082
            | 1083
            | 1114
            | 1186
            | 1184
            | 1266
            | 1560
            | 1562
            | 1700
            | 2950
            | 3802
            | 5069
    )
}
#[cfg(test)]
pub(crate) fn decode(oid: u32, bytes: &[u8]) -> Result<V> {
    decode_for_version("15", oid, bytes)
}

fn decode_for_version(version: &str, oid: u32, bytes: &[u8]) -> Result<V> {
    let text = std::str::from_utf8(bytes)?;
    Ok(match oid {
        16 => V::Boolean {
            value: match text {
                "t" => true,
                "f" => false,
                _ => return Err(invalid("invalid PostgreSQL boolean")),
            },
        },
        20 | 21 | 23 | 26 | 28 | 29 => {
            let value = text.parse::<i64>()?;
            let bits = match oid {
                20 => 64,
                21 => 16,
                _ => 32,
            };
            V::Integer {
                signed: matches!(oid, 20 | 21 | 23),
                bits,
                value: value.to_string(),
            }
        }
        5069 => V::Integer {
            signed: false,
            bits: 64,
            value: text.parse::<u64>()?.to_string(),
        },
        1700 => {
            let (unscaled, scale) = decimal(text)?;
            V::Decimal { unscaled, scale }
        }
        700 => V::Float {
            bits: 32,
            ieee754_hex: format!("{:08x}", text.parse::<f32>()?.to_bits()),
        },
        701 => V::Float {
            bits: 64,
            ieee754_hex: format!("{:016x}", text.parse::<f64>()?.to_bits()),
        },
        18 | 19 | 25 | 1042 | 1043 => V::Text {
            charset: "UTF8".into(),
            bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
            text: Some(text.into()),
        },
        114 => V::Json {
            value: json(serde_json::from_str(text)?, 0)?,
        },
        17 => {
            let hex = text
                .strip_prefix("\\x")
                .ok_or_else(|| invalid("bytea_output must be hex"))?;
            if !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(invalid("invalid bytea hex"));
            }
            let data = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>();
            V::Binary {
                bytes_base64url: URL_SAFE_NO_PAD.encode(data),
            }
        }
        1082 => {
            if let Some(value) = temporal_infinity(text, TemporalInfinityKind::Date) {
                value
            } else {
                let (year, month, day) = postgres_date(text)?;
                V::Date { year, month, day }
            }
        }
        1083 => local_time(text)?,
        1114 => {
            if let Some(value) = temporal_infinity(text, TemporalInfinityKind::LocalDatetime) {
                value
            } else {
                let (year, month, day, hour, minute, second, microsecond) =
                    postgres_datetime(text)?;
                V::LocalDatetime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond,
                }
            }
        }
        1184 => {
            if let Some(value) = temporal_infinity(text, TemporalInfinityKind::Instant) {
                value
            } else {
                let (unix_seconds, nanoseconds) = postgres_instant(text)?;
                V::Instant {
                    unix_seconds: unix_seconds.to_string(),
                    nanoseconds,
                }
            }
        }
        1186 => {
            if version_major(version) >= 17
                && let Some(value) = temporal_infinity(text, TemporalInfinityKind::CalendarInterval)
            {
                value
            } else {
                calendar_interval(text)?
            }
        }
        1266 => offset_time(text)?,
        142 => V::Xml {
            bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
            text: Some(text.into()),
        },
        650 | 774 | 829 | 869 => network(oid, text)?,
        1560 | 1562 => bit_string(text)?,
        2950 => V::Uuid { value: text.into() },
        3802 => V::Json {
            value: json(serde_json::from_str(text)?, 0)?,
        },
        _ => return Err(invalid(format!("unsupported PostgreSQL type OID {oid}"))),
    })
}

pub(crate) fn decode_column_for_version(
    version: &str,
    oid: u32,
    native_type: &str,
    bytes: &[u8],
) -> Result<V> {
    if native_type.trim().to_ascii_lowercase().starts_with("enum(") {
        let label = std::str::from_utf8(bytes)?.to_owned();
        let mapping = crate::type_mapping::source_type_mapping_for_version(version, native_type)
            .map_err(|error| invalid(error.to_string()))?;
        let change_event::LogicalType::Enum { members } = mapping.logical_type else {
            return Err(invalid("PostgreSQL ENUM declaration did not map to ENUM"));
        };
        if !members.iter().any(|member| member == &label) {
            return Err(invalid("PostgreSQL ENUM value is not a declared label"));
        }
        return Ok(V::Enum { label });
    }
    decode_for_version(version, oid, bytes)
}

pub(crate) fn decode_catalog_column_for_version(
    version: &str,
    catalog: &SourceTypeCatalog,
    oid: u32,
    native_type: &str,
    bytes: &[u8],
) -> Result<V> {
    decode_catalog_value(version, catalog, oid, native_type, bytes, 0)
}

fn decode_catalog_value(
    version: &str,
    catalog: &SourceTypeCatalog,
    oid: u32,
    native_type: &str,
    bytes: &[u8],
    depth: usize,
) -> Result<V> {
    if depth > 64 {
        return Err(invalid(
            "PostgreSQL recursive value exceeds the 64-level limit",
        ));
    }
    let definition = catalog
        .types
        .iter()
        .find(|definition| definition.oid == oid)
        .ok_or_else(|| invalid(format!("PostgreSQL type catalog is missing OID {oid}")))?;
    match &definition.kind {
        SourceTypeDefinitionKind::Builtin { .. } => {
            decode_column_for_version(version, oid, native_type, bytes)
        }
        SourceTypeDefinitionKind::Enum { labels } => {
            let label = std::str::from_utf8(bytes)?.to_owned();
            if !labels.iter().any(|known| known == &label) {
                return Err(invalid(format!(
                    "PostgreSQL enum {}.{} contains an undeclared label",
                    definition.schema, definition.name
                )));
            }
            Ok(V::Enum { label })
        }
        SourceTypeDefinitionKind::Domain { base_oid, .. } => {
            let base = catalog
                .types
                .iter()
                .find(|item| item.oid == *base_oid)
                .ok_or_else(|| {
                    invalid(format!(
                        "PostgreSQL domain {}.{} has a missing base type",
                        definition.schema, definition.name
                    ))
                })?;
            let value =
                decode_catalog_value(version, catalog, *base_oid, &base.name, bytes, depth + 1)?;
            Ok(V::Domain {
                value: Box::new(value),
            })
        }
        SourceTypeDefinitionKind::Composite { fields } => {
            let values = parse_record(bytes).map_err(|error| {
                invalid(format!(
                    "PostgreSQL composite {}.{} could not be decoded: {error}",
                    definition.schema, definition.name
                ))
            })?;
            if values.len() != fields.len() {
                return Err(invalid(format!(
                    "PostgreSQL composite {}.{} field count mismatch",
                    definition.schema, definition.name
                )));
            }
            let mut decoded = Vec::with_capacity(fields.len());
            for (field, raw) in fields.iter().zip(values) {
                let value = match raw {
                    None => V::Null,
                    Some(raw) => {
                        let field_type = catalog
                            .types
                            .iter()
                            .find(|item| item.oid == field.type_oid)
                            .ok_or_else(|| {
                                invalid(format!(
                                    "PostgreSQL composite field {} has an unknown type OID",
                                    field.name
                                ))
                            })?;
                        decode_catalog_value(
                            version,
                            catalog,
                            field.type_oid,
                            &field_type.name,
                            &raw,
                            depth + 1,
                        )?
                    }
                };
                decoded.push(StructuredField {
                    name: field.name.clone(),
                    value,
                });
            }
            Ok(V::Struct { fields: decoded })
        }
        SourceTypeDefinitionKind::Array {
            element_oid,
            delimiter,
        } => {
            let element_type = catalog
                .types
                .iter()
                .find(|item| item.oid == *element_oid)
                .ok_or_else(|| {
                    invalid(format!(
                        "PostgreSQL array {}.{} has an unknown element type",
                        definition.schema, definition.name
                    ))
                })?;
            let (tree, lower_bounds) = parse_array(bytes, *delimiter).map_err(|error| {
                invalid(format!(
                    "PostgreSQL array {}.{} could not be decoded: {error}",
                    definition.schema, definition.name
                ))
            })?;
            let (dimension_lengths, mut raw_values) = flatten_array(&tree)?;
            let dimensions = u8::try_from(dimension_lengths.len())
                .map_err(|_| invalid("PostgreSQL array dimension count exceeds 255"))?;
            let lower_bounds = if dimension_lengths.is_empty() {
                Vec::new()
            } else {
                lower_bounds.unwrap_or_else(|| vec![1; dimension_lengths.len()])
            };
            if lower_bounds.len() != dimension_lengths.len() {
                return Err(invalid("PostgreSQL array bounds and dimensions disagree"));
            }
            let mut elements = Vec::with_capacity(raw_values.len());
            for raw in raw_values.drain(..) {
                elements.push(match raw {
                    None => V::Null,
                    Some(raw) => decode_catalog_value(
                        version,
                        catalog,
                        *element_oid,
                        &element_type.name,
                        &raw,
                        depth + 1,
                    )?,
                });
            }
            if dimensions <= 1 && lower_bounds.first().is_none_or(|bound| *bound == 1) {
                Ok(V::Array { elements })
            } else {
                Ok(V::ArrayWithMetadata {
                    elements,
                    dimensions,
                    lower_bounds,
                    dimension_lengths,
                })
            }
        }
        SourceTypeDefinitionKind::Range { subtype_oid } => {
            let subtype = catalog
                .types
                .iter()
                .find(|item| item.oid == *subtype_oid)
                .ok_or_else(|| {
                    invalid(format!(
                        "PostgreSQL range {}.{} has an unknown subtype",
                        definition.schema, definition.name
                    ))
                })?;
            decode_range(
                version,
                catalog,
                *subtype_oid,
                &subtype.name,
                bytes,
                depth + 1,
            )
        }
        SourceTypeDefinitionKind::MultiRange { range_oid } => {
            let range = catalog
                .types
                .iter()
                .find(|item| item.oid == *range_oid)
                .ok_or_else(|| {
                    invalid(format!(
                        "PostgreSQL multirange {}.{} has an unknown range type",
                        definition.schema, definition.name
                    ))
                })?;
            let SourceTypeDefinitionKind::Range { subtype_oid } = range.kind else {
                return Err(invalid(
                    "PostgreSQL multirange catalog entry does not refer to a range",
                ));
            };
            let subtype = catalog
                .types
                .iter()
                .find(|item| item.oid == subtype_oid)
                .ok_or_else(|| invalid("PostgreSQL multirange subtype is missing"))?;
            let text = std::str::from_utf8(bytes)?;
            let body = text
                .strip_prefix('{')
                .and_then(|value| value.strip_suffix('}'))
                .ok_or_else(|| invalid("invalid PostgreSQL multirange text"))?;
            let mut ranges = Vec::new();
            for item in split_multirange(body)? {
                ranges.push(decode_range(
                    version,
                    catalog,
                    subtype_oid,
                    &subtype.name,
                    item.as_bytes(),
                    depth + 1,
                )?);
            }
            Ok(V::MultiRange { ranges })
        }
        SourceTypeDefinitionKind::Extension { extension, .. }
            if extension.eq_ignore_ascii_case("hstore") && definition.name == "hstore" =>
        {
            decode_hstore(bytes)
        }
        SourceTypeDefinitionKind::Extension { extension, .. }
            if extension.eq_ignore_ascii_case("postgis")
                && matches!(definition.name.as_str(), "geometry" | "geography") =>
        {
            decode_postgis_ewkb(bytes)
        }
        SourceTypeDefinitionKind::Extension { .. } => Err(invalid(format!(
            "PostgreSQL extension type {}.{} has no qualified semantic codec",
            definition.schema, definition.name
        ))),
    }
}

fn parse_record(bytes: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
    let mut parser = TextParser::new(bytes);
    parser.consume(b'(')?;
    let mut fields = Vec::new();
    if parser.peek() == Some(b')') {
        parser.consume(b')')?;
        parser.finish()?;
        return Ok(fields);
    }
    loop {
        let (value, quoted) = parser.token(b",)")?;
        fields.push(if !quoted && value.is_empty() {
            None
        } else {
            Some(value)
        });
        match parser.take()? {
            b',' => continue,
            b')' => break,
            _ => unreachable!(),
        }
    }
    parser.finish()?;
    Ok(fields)
}

#[derive(Debug)]
enum ArrayNode {
    Branch(Vec<ArrayNode>),
    Leaf(Option<Vec<u8>>),
}

fn parse_array(bytes: &[u8], delimiter: char) -> Result<(ArrayNode, Option<Vec<i32>>)> {
    let delimiter = u8::try_from(delimiter as u32)
        .map_err(|_| invalid("PostgreSQL array delimiter is not ASCII"))?;
    let mut parser = TextParser::new(bytes);
    let mut lower_bounds = Vec::new();
    while parser.peek() == Some(b'[') {
        parser.take()?;
        let lower = parser.number_until(b':')?;
        parser.consume(b':')?;
        let upper = parser.number_until(b']')?;
        parser.consume(b']')?;
        let lower = lower.parse::<i32>()?;
        let upper = upper.parse::<i32>()?;
        let length = i64::from(upper) - i64::from(lower) + 1;
        if !(0..=10_000_000).contains(&length) {
            return Err(invalid("PostgreSQL array bound has invalid length"));
        }
        lower_bounds.push(lower);
    }
    let explicit_bounds = if lower_bounds.is_empty() {
        None
    } else {
        Some(lower_bounds)
    };
    if explicit_bounds.is_some() {
        parser.consume(b'=')?;
    }
    let tree = parse_array_level(&mut parser, delimiter)?;
    parser.finish()?;
    Ok((tree, explicit_bounds))
}

fn parse_array_level(parser: &mut TextParser<'_>, delimiter: u8) -> Result<ArrayNode> {
    parser.consume(b'{')?;
    let mut items = Vec::new();
    if parser.peek() == Some(b'}') {
        parser.take()?;
        return Ok(ArrayNode::Branch(items));
    }
    loop {
        items.push(if parser.peek() == Some(b'{') {
            parse_array_level(parser, delimiter)?
        } else {
            let (value, quoted) = parser.token(&[delimiter, b'}'])?;
            let null = !quoted && value.eq_ignore_ascii_case(b"NULL");
            ArrayNode::Leaf(if null { None } else { Some(value) })
        });
        match parser.take()? {
            value if value == delimiter => continue,
            b'}' => break,
            _ => unreachable!(),
        }
    }
    Ok(ArrayNode::Branch(items))
}

type ArrayShape = Vec<u64>;
type ArrayElements = Vec<Option<Vec<u8>>>;

fn flatten_array(node: &ArrayNode) -> Result<(ArrayShape, ArrayElements)> {
    match node {
        ArrayNode::Leaf(value) => Ok((Vec::new(), vec![value.clone()])),
        ArrayNode::Branch(children) if children.is_empty() => Ok((vec![0], Vec::new())),
        ArrayNode::Branch(children) => {
            let mut expected_shape: Option<Vec<u64>> = None;
            let mut flat = Vec::new();
            for child in children {
                let (shape, values) = flatten_array(child)?;
                if expected_shape.as_ref().is_some_and(|known| known != &shape) {
                    return Err(invalid("PostgreSQL array value is ragged"));
                }
                expected_shape.get_or_insert(shape);
                flat.extend(values);
            }
            let mut shape = vec![u64::try_from(children.len())?];
            shape.extend(expected_shape.unwrap_or_default());
            Ok((shape, flat))
        }
    }
}

fn decode_range(
    version: &str,
    catalog: &SourceTypeCatalog,
    subtype_oid: u32,
    subtype_name: &str,
    bytes: &[u8],
    depth: usize,
) -> Result<V> {
    let text = std::str::from_utf8(bytes)?;
    if text == "empty" {
        return Ok(V::Range {
            empty: true,
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        });
    }
    let raw = text.as_bytes();
    if raw.len() < 2 || !matches!(raw[0], b'[' | b'(') || !matches!(raw[raw.len() - 1], b']' | b')')
    {
        return Err(invalid("invalid PostgreSQL range text"));
    }
    let comma = find_unquoted(raw, b',')
        .ok_or_else(|| invalid("PostgreSQL range is missing its separator"))?;
    let lower_raw = unquote_range_bound(&raw[1..comma])?;
    let upper_raw = unquote_range_bound(&raw[comma + 1..raw.len() - 1])?;
    let lower = lower_raw
        .map(|value| {
            decode_catalog_value(
                version,
                catalog,
                subtype_oid,
                subtype_name,
                &value,
                depth + 1,
            )
            .map(Box::new)
        })
        .transpose()?;
    let upper = upper_raw
        .map(|value| {
            decode_catalog_value(
                version,
                catalog,
                subtype_oid,
                subtype_name,
                &value,
                depth + 1,
            )
            .map(Box::new)
        })
        .transpose()?;
    Ok(V::Range {
        empty: false,
        lower,
        upper,
        lower_inclusive: raw[0] == b'[',
        upper_inclusive: raw[raw.len() - 1] == b']',
    })
}

fn split_multirange(body: &str) -> Result<Vec<String>> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = body.as_bytes();
    let mut result = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0_i32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match byte {
            b'[' | b'(' => depth += 1,
            b']' | b')' => depth -= 1,
            b',' if depth == 0 => {
                result.push(body[start..index].trim().to_owned());
                start = index + 1;
            }
            _ => {}
        }
    }
    if quoted || escaped || depth != 0 {
        return Err(invalid("invalid PostgreSQL multirange quoting or bounds"));
    }
    result.push(body[start..].trim().to_owned());
    Ok(result)
}

fn decode_hstore(bytes: &[u8]) -> Result<V> {
    let mut parser = TextParser::new(bytes);
    let mut entries = Vec::new();
    if parser.peek().is_none() {
        return Ok(V::Map { entries });
    }
    loop {
        let (key, quoted) = parser.token(b"=")?;
        if !quoted {
            return Err(invalid("PostgreSQL hstore key must be quoted"));
        }
        parser.consume(b'=')?;
        parser.consume(b'>')?;
        let value = if parser.remaining().starts_with(b"NULL") {
            parser.consume_bytes(b"NULL")?;
            V::Null
        } else {
            let (value, quoted) = parser.token(b",")?;
            if !quoted {
                return Err(invalid("PostgreSQL hstore value must be quoted or NULL"));
            }
            text_value(&value)?
        };
        entries.push(MapEntry {
            key: text_value(&key)?,
            value,
        });
        if parser.peek().is_none() {
            break;
        }
        parser.consume(b',')?;
        parser.skip_spaces();
    }
    parser.finish()?;
    Ok(V::Map { entries })
}

fn text_value(bytes: &[u8]) -> Result<V> {
    let text = std::str::from_utf8(bytes)?;
    Ok(V::Text {
        charset: "UTF8".into(),
        bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
        text: Some(text.into()),
    })
}

fn decode_postgis_ewkb(bytes: &[u8]) -> Result<V> {
    if bytes.len() < 10
        || !bytes.len().is_multiple_of(2)
        || !bytes.iter().all(u8::is_ascii_hexdigit)
    {
        return Err(invalid("PostGIS text output is not valid hexadecimal EWKB"));
    }
    let raw = (0..bytes.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(std::str::from_utf8(&bytes[index..index + 2]).unwrap(), 16).unwrap()
        })
        .collect::<Vec<_>>();
    let little = match raw[0] {
        0 => false,
        1 => true,
        _ => return Err(invalid("EWKB has an invalid byte-order marker")),
    };
    let word = |offset: usize| -> Result<u32> {
        let bytes: [u8; 4] = raw
            .get(offset..offset + 4)
            .ok_or_else(|| invalid("truncated EWKB header"))?
            .try_into()
            .unwrap();
        Ok(if little {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    };
    let type_word = word(1)?;
    let ewkb_has_z = type_word & 0x8000_0000 != 0;
    let ewkb_has_m = type_word & 0x4000_0000 != 0;
    let has_srid = type_word & 0x2000_0000 != 0;
    let encoded_type = type_word & 0x1fff_ffff;
    let iso_dimensions = encoded_type / 1000;
    let geometry_id = if iso_dimensions == 0 {
        encoded_type
    } else {
        encoded_type % 1000
    };
    let geometry_type = match geometry_id {
        1 => "point",
        2 => "linestring",
        3 => "polygon",
        4 => "multipoint",
        5 => "multilinestring",
        6 => "multipolygon",
        7 => "geometrycollection",
        _ => return Err(invalid("EWKB uses an unsupported geometry type")),
    };
    if iso_dimensions > 3 {
        return Err(invalid("EWKB uses an unsupported coordinate dimension"));
    }
    let iso_has_z = matches!(iso_dimensions, 1 | 3);
    let iso_has_m = matches!(iso_dimensions, 2 | 3);
    let dimensions = 2 + u8::from(ewkb_has_z || iso_has_z) + u8::from(ewkb_has_m || iso_has_m);
    let srid = if has_srid {
        Some(word(5)? as i32)
    } else {
        None
    };
    Ok(V::Spatial {
        format: SpatialFormat::Ewkb,
        bytes_base64url: URL_SAFE_NO_PAD.encode(raw),
        geometry_type: geometry_type.into(),
        dimensions,
        srid,
        crs: None,
    })
}

fn find_unquoted(bytes: &[u8], target: u8) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
            continue;
        }
        if !quoted && byte == target {
            return Some(index);
        }
    }
    None
}

fn unquote_range_bound(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.first() == Some(&b'"') {
        let mut parser = TextParser::new(bytes);
        let (value, quoted) = parser.token(&[])?;
        parser.finish()?;
        return if quoted {
            Ok(Some(value))
        } else {
            Err(invalid("invalid quoted PostgreSQL range bound"))
        };
    }
    Ok(Some(bytes.to_vec()))
}

struct TextParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> TextParser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
    fn take(&mut self) -> Result<u8> {
        let value = self
            .peek()
            .ok_or_else(|| invalid("unexpected end of PostgreSQL text value"))?;
        self.pos += 1;
        Ok(value)
    }
    fn consume(&mut self, expected: u8) -> Result<()> {
        let offset = self.pos;
        let actual = self.take()?;
        if actual != expected {
            return Err(invalid(format!(
                "malformed PostgreSQL text value at offset {offset}: expected byte 0x{expected:02x}, found 0x{actual:02x}"
            )));
        }
        Ok(())
    }
    fn consume_bytes(&mut self, expected: &[u8]) -> Result<()> {
        if !self.remaining().starts_with(expected) {
            return Err(invalid("malformed PostgreSQL text value"));
        }
        self.pos += expected.len();
        Ok(())
    }
    fn skip_spaces(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }
    fn number_until(&mut self, stop: u8) -> Result<String> {
        let start = self.pos;
        while self.peek().is_some_and(|byte| byte != stop) {
            self.pos += 1;
        }
        if self.peek().is_none() {
            return Err(invalid("truncated PostgreSQL array bounds"));
        }
        std::str::from_utf8(&self.bytes[start..self.pos])
            .map(str::to_owned)
            .map_err(Into::into)
    }
    fn token(&mut self, terminators: &[u8]) -> Result<(Vec<u8>, bool)> {
        let quoted = self.peek() == Some(b'"');
        if quoted {
            self.take()?;
        }
        let mut value = Vec::new();
        loop {
            let Some(byte) = self.peek() else {
                if quoted {
                    return Err(invalid("unterminated quoted PostgreSQL text value"));
                }
                break;
            };
            if quoted && byte == b'"' {
                self.take()?;
                if self.peek() == Some(b'"') {
                    self.take()?;
                    value.push(b'"');
                    continue;
                }
                break;
            }
            if !quoted && terminators.contains(&byte) {
                break;
            }
            self.take()?;
            if byte == b'\\' {
                value.push(self.take()?);
            } else {
                value.push(byte);
            }
        }
        if quoted && self.peek().is_some_and(|byte| !terminators.contains(&byte)) {
            self.skip_spaces();
            if self.peek().is_some_and(|byte| !terminators.contains(&byte)) {
                return Err(invalid(format!(
                    "quoted PostgreSQL text value at offset {} is followed by non-delimiter byte {:?}",
                    self.pos,
                    self.peek()
                )));
            }
        }
        Ok((value, quoted))
    }
    fn finish(&self) -> Result<()> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("trailing bytes in PostgreSQL text value"))
        }
    }
}

fn version_major(version: &str) -> u16 {
    version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u16>().ok())
        .unwrap_or_default()
}

fn temporal_infinity(text: &str, kind: TemporalInfinityKind) -> Option<V> {
    match text {
        "infinity" => Some(V::TemporalInfinity {
            kind,
            negative: false,
        }),
        "-infinity" => Some(V::TemporalInfinity {
            kind,
            negative: true,
        }),
        _ => None,
    }
}

fn postgres_date(text: &str) -> Result<(i32, u8, u8)> {
    let (date, bc) = strip_era(text);
    let fields = date.split('-').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(invalid("invalid PostgreSQL date"));
    }
    let display_year = fields[0].parse::<i32>()?;
    if display_year <= 0 {
        return Err(invalid("invalid PostgreSQL date year"));
    }
    let year = if bc {
        1_i64 - i64::from(display_year)
    } else {
        i64::from(display_year)
    };
    let year = i32::try_from(year)?;
    let month = fields[1].parse::<u8>()?;
    let day = fields[2].parse::<u8>()?;
    validate_postgres_date(year, month, day)?;
    Ok((year, month, day))
}

fn strip_era(text: &str) -> (&str, bool) {
    if let Some(value) = text.strip_suffix(" BC") {
        (value, true)
    } else if let Some(value) = text.strip_suffix(" AD") {
        (value, false)
    } else {
        (text, false)
    }
}

fn validate_postgres_date(year: i32, month: u8, day: u8) -> Result<()> {
    if !(-4_712..=5_874_897).contains(&year) {
        return Err(invalid(
            "PostgreSQL date year is outside the supported range",
        ));
    }
    let leap = year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if day == 0 || day > days {
        return Err(invalid("invalid PostgreSQL calendar date"));
    }
    Ok(())
}

fn postgres_datetime(text: &str) -> Result<(i32, u8, u8, u8, u8, u8, u32)> {
    let (datetime, bc) = strip_era(text);
    let (date_text, time_text) = datetime
        .split_once(' ')
        .ok_or_else(|| invalid("invalid PostgreSQL timestamp"))?;
    let (year, month, day) = postgres_date_with_era(date_text, bc)?;
    validate_postgres_timestamp_year(year)?;
    let (hour, minute, second, microsecond) = clock_time(time_text)?;
    Ok((year, month, day, hour, minute, second, microsecond))
}

fn postgres_date_with_era(text: &str, bc: bool) -> Result<(i32, u8, u8)> {
    if bc {
        postgres_date(&format!("{text} BC"))
    } else {
        postgres_date(text)
    }
}

fn postgres_instant(text: &str) -> Result<(i64, u32)> {
    let (datetime, bc) = strip_era(text);
    let (date_text, time_zone_text) = datetime
        .split_once(' ')
        .ok_or_else(|| invalid("invalid PostgreSQL timestamp with time zone"))?;
    let (year, month, day) = postgres_date_with_era(date_text, bc)?;
    validate_postgres_timestamp_year(year)?;
    let (time_text, offset_text) = split_time_zone(time_zone_text)?;
    let (hour, minute, second, microsecond) = clock_time(time_text)?;
    let offset_seconds = postgres_offset_seconds(offset_text)?;
    let day_number = days_from_civil(year, month, day);
    let seconds = day_number
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(i64::from(hour) * 3_600))
        .and_then(|value| value.checked_add(i64::from(minute) * 60))
        .and_then(|value| value.checked_add(i64::from(second)))
        .and_then(|value| value.checked_sub(i64::from(offset_seconds)))
        .ok_or_else(|| invalid("PostgreSQL timestamp is outside the supported range"))?;
    Ok((seconds, microsecond * 1_000))
}

fn validate_postgres_timestamp_year(year: i32) -> Result<()> {
    if !(-4_712..=294_276).contains(&year) {
        return Err(invalid(
            "PostgreSQL timestamp year is outside the supported range",
        ));
    }
    Ok(())
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 {
        year / 400
    } else {
        (year - 399) / 400
    };
    let year_of_era = year - era * 400;
    let adjusted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn clock_time(text: &str) -> Result<(u8, u8, u8, u32)> {
    let fields = text.split(':').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(invalid("invalid PostgreSQL time"));
    }
    let hour = fields[0].parse::<u8>()?;
    let minute = fields[1].parse::<u8>()?;
    let (second_text, fraction) = fields[2]
        .split_once('.')
        .map_or((fields[2], None), |(s, f)| (s, Some(f)));
    let second = second_text.parse::<u8>()?;
    let microsecond = match fraction {
        None => 0,
        Some(value)
            if !value.is_empty()
                && value.len() <= 6
                && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            let digits = format!("{value:0<6}");
            digits.parse::<u32>()?
        }
        Some(_) => return Err(invalid("PostgreSQL time precision exceeds microseconds")),
    };
    if hour > 24
        || minute >= 60
        || second >= 60
        || (hour == 24 && (minute != 0 || second != 0 || microsecond != 0))
    {
        return Err(invalid("invalid PostgreSQL time fields"));
    }
    Ok((hour, minute, second, microsecond))
}

fn split_time_zone(text: &str) -> Result<(&str, &str)> {
    let Some((index, _)) = text
        .char_indices()
        .rfind(|(index, character)| *index > 0 && matches!(character, '+' | '-'))
    else {
        return Err(invalid("PostgreSQL time zone offset is missing"));
    };
    Ok(text.split_at(index))
}

fn postgres_offset_seconds(text: &str) -> Result<i32> {
    let (sign, value) = match text.as_bytes().first() {
        Some(b'+') => (1_i32, &text[1..]),
        Some(b'-') => (-1_i32, &text[1..]),
        _ => return Err(invalid("invalid PostgreSQL UTC offset")),
    };
    let parts = if value.contains(':') {
        value.split(':').collect::<Vec<_>>()
    } else {
        match value.len() {
            1 | 2 => vec![value],
            3 | 4 => vec![&value[..value.len() - 2], &value[value.len() - 2..]],
            5 | 6 => vec![
                &value[..value.len() - 4],
                &value[value.len() - 4..value.len() - 2],
                &value[value.len() - 2..],
            ],
            _ => return Err(invalid("invalid PostgreSQL UTC offset")),
        }
    };
    if parts.is_empty() || parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(invalid("invalid PostgreSQL UTC offset"));
    }
    let hour = parts[0].parse::<i32>()?;
    let minute = parts.get(1).map_or(Ok(0), |part| part.parse::<i32>())?;
    let second = parts.get(2).map_or(Ok(0), |part| part.parse::<i32>())?;
    if !(0..=15).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
        return Err(invalid(
            "PostgreSQL UTC offset is outside the supported range",
        ));
    }
    let magnitude = hour * 3_600 + minute * 60 + second;
    Ok(sign * magnitude)
}

fn offset_time(text: &str) -> Result<V> {
    let (time_text, offset_text) = split_time_zone(text)?;
    let (hour, minute, second, microsecond) = clock_time(time_text)?;
    Ok(V::OffsetTime {
        hour,
        minute,
        second,
        microsecond,
        offset_seconds: postgres_offset_seconds(offset_text)?,
    })
}

fn calendar_interval(text: &str) -> Result<V> {
    let (global_sign, body) = if let Some(body) = text.strip_prefix("-P") {
        (-1_i64, body)
    } else if let Some(body) = text.strip_prefix("+P") {
        (1_i64, body)
    } else if let Some(body) = text.strip_prefix('P') {
        (1_i64, body)
    } else {
        return Err(invalid("PostgreSQL interval is not in ISO 8601 form"));
    };
    let bytes = body.as_bytes();
    let mut index = 0;
    let mut in_time = false;
    let mut months = 0_i64;
    let mut days = 0_i64;
    let mut microseconds = 0_i64;
    let mut saw_component = false;
    while index < bytes.len() {
        if bytes[index] == b'T' && !in_time {
            in_time = true;
            index += 1;
            continue;
        }
        let start = index;
        if matches!(bytes[index], b'+' | b'-') {
            index += 1;
        }
        let mut saw_digit = false;
        let mut saw_dot = false;
        while index < bytes.len() {
            match bytes[index] {
                b'0'..=b'9' => {
                    saw_digit = true;
                    index += 1;
                }
                b'.' if !saw_dot => {
                    saw_dot = true;
                    index += 1;
                }
                _ => break,
            }
        }
        if !saw_digit || index >= bytes.len() {
            return Err(invalid("invalid PostgreSQL ISO interval component"));
        }
        let quantity = &body[start..index];
        let unit = bytes[index] as char;
        index += 1;
        saw_component = true;
        let factor = match (in_time, unit) {
            (false, 'Y') => {
                months = months
                    .checked_add(
                        parse_interval_integer(quantity)?
                            .checked_mul(12)
                            .ok_or_else(|| invalid("PostgreSQL interval month field overflow"))?,
                    )
                    .ok_or_else(|| invalid("PostgreSQL interval month field overflow"))?;
                continue;
            }
            (false, 'M') => {
                months = months
                    .checked_add(parse_interval_integer(quantity)?)
                    .ok_or_else(|| invalid("PostgreSQL interval month field overflow"))?;
                continue;
            }
            (false, 'W') => {
                days = days
                    .checked_add(
                        parse_interval_integer(quantity)?
                            .checked_mul(7)
                            .ok_or_else(|| invalid("PostgreSQL interval day field overflow"))?,
                    )
                    .ok_or_else(|| invalid("PostgreSQL interval day field overflow"))?;
                continue;
            }
            (false, 'D') => {
                days = days
                    .checked_add(parse_interval_integer(quantity)?)
                    .ok_or_else(|| invalid("PostgreSQL interval day field overflow"))?;
                continue;
            }
            (true, 'H') => 3_600_000_000_i64,
            (true, 'M') => 60_000_000_i64,
            (true, 'S') => 1_000_000_i64,
            _ => return Err(invalid("unsupported PostgreSQL ISO interval unit")),
        };
        let component = parse_interval_scaled(quantity, factor)?
            .checked_mul(global_sign)
            .ok_or_else(|| invalid("PostgreSQL interval time field overflow"))?;
        microseconds = microseconds
            .checked_add(component)
            .ok_or_else(|| invalid("PostgreSQL interval time field overflow"))?;
    }
    if !saw_component {
        return Err(invalid("empty PostgreSQL ISO interval"));
    }
    months = months
        .checked_mul(global_sign)
        .ok_or_else(|| invalid("PostgreSQL interval month field overflow"))?;
    days = days
        .checked_mul(global_sign)
        .ok_or_else(|| invalid("PostgreSQL interval day field overflow"))?;
    Ok(V::CalendarInterval {
        months: i32::try_from(months)?,
        days: i32::try_from(days)?,
        microseconds,
    })
}

fn parse_interval_integer(quantity: &str) -> Result<i64> {
    if quantity.contains('.') {
        return Err(invalid("fractional PostgreSQL interval date unit"));
    }
    Ok(quantity.parse::<i64>()?)
}

fn parse_interval_scaled(quantity: &str, factor: i64) -> Result<i64> {
    let (negative, quantity) = if let Some(value) = quantity.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = quantity.strip_prefix('+') {
        (false, value)
    } else {
        (false, quantity)
    };
    let (integer, fraction) = quantity
        .split_once('.')
        .map_or((quantity, None), |(integer, fraction)| {
            (integer, Some(fraction))
        });
    if integer.is_empty() || !integer.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("invalid PostgreSQL interval quantity"));
    }
    let integer = i128::from(integer.parse::<i64>()?);
    let mut magnitude = integer
        .checked_mul(i128::from(factor))
        .ok_or_else(|| invalid("PostgreSQL interval time field overflow"))?;
    if let Some(fraction) = fraction {
        if fraction.is_empty()
            || fraction.len() > 18
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid("invalid PostgreSQL interval fraction"));
        }
        let denominator = 10_i128.pow(u32::try_from(fraction.len())?);
        let numerator = i128::from(fraction.parse::<i64>()?);
        let scaled = numerator
            .checked_mul(i128::from(factor))
            .ok_or_else(|| invalid("PostgreSQL interval time field overflow"))?;
        if scaled % denominator != 0 {
            return Err(invalid(
                "PostgreSQL interval fraction exceeds microsecond precision",
            ));
        }
        magnitude = magnitude
            .checked_add(scaled / denominator)
            .ok_or_else(|| invalid("PostgreSQL interval time field overflow"))?;
    }
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).map_err(Into::into)
}

fn local_time(text: &str) -> Result<V> {
    let (hour, minute, second, microsecond) = clock_time(text)?;
    Ok(V::LocalTime {
        hour,
        minute,
        second,
        microsecond,
    })
}

fn bit_string(text: &str) -> Result<V> {
    if text.is_empty() || !text.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
        return Err(invalid("invalid PostgreSQL bit string"));
    }
    let bit_length = u64::try_from(text.len())?;
    let mut bytes = vec![0_u8; text.len().div_ceil(8)];
    for (index, bit) in text.bytes().enumerate() {
        if bit == b'1' {
            bytes[index / 8] |= 1 << (7 - index % 8);
        }
    }
    Ok(V::BitString {
        bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
        bit_length,
        padding: if bit_length.is_multiple_of(8) {
            change_event::BitPadding::None
        } else {
            change_event::BitPadding::Zero
        },
        bit_order: change_event::BitOrder::MsbFirst,
    })
}

fn network(oid: u32, text: &str) -> Result<V> {
    let (address, prefix_length) = match text.split_once('/') {
        Some((address, prefix)) => (
            address,
            Some(
                prefix
                    .parse::<u8>()
                    .map_err(|_| invalid("invalid network prefix"))?,
            ),
        ),
        None => (text, None),
    };
    if address.is_empty() {
        return Err(invalid("invalid PostgreSQL network value"));
    }
    let family = match oid {
        774 => "macaddr8",
        829 => "macaddr",
        _ if address.contains(':') => "ipv6",
        _ => "ipv4",
    };
    Ok(V::Network {
        family: family.into(),
        address: address.into(),
        prefix_length,
    })
}
fn decimal(text: &str) -> Result<(String, i32)> {
    match text.to_ascii_lowercase().as_str() {
        "nan" => return Ok(("NaN".into(), 0)),
        "infinity" | "inf" | "+infinity" | "+inf" => {
            return Ok(("Infinity".into(), 0));
        }
        "-infinity" | "-inf" => return Ok(("-Infinity".into(), 0)),
        _ => {}
    }
    let (coefficient, exponent) = match text.find(['e', 'E']) {
        Some(i) => (&text[..i], text[i + 1..].parse::<i32>()?),
        None => (text, 0),
    };
    let negative = coefficient.starts_with('-');
    let s = coefficient.strip_prefix(['-', '+']).unwrap_or(coefficient);
    let parts = s.split('.').collect::<Vec<_>>();
    if parts.len() > 2
        || parts[0].is_empty()
        || !parts.iter().all(|s| s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(invalid("unsupported numeric special value or syntax"));
    }
    let mut digits = parts.concat();
    let scale = i64::try_from(parts.get(1).map_or(0, |p| p.len()))? - i64::from(exponent);
    let scale = i32::try_from(scale)?;
    if negative {
        digits.insert(0, '-');
    }
    Ok((digits, scale))
}
fn json(value: serde_json::Value, depth: usize) -> Result<J> {
    if depth > 100 {
        return Err(invalid("jsonb nesting exceeds 100"));
    }
    Ok(match value {
        serde_json::Value::Null => J::Null,
        serde_json::Value::Bool(v) => J::Boolean(v),
        serde_json::Value::String(v) => J::String(v),
        serde_json::Value::Number(v) => {
            let (mut unscaled, mut scale) = decimal(v.as_str())?;
            if scale < 0 {
                let zeros = usize::try_from(scale.unsigned_abs())?;
                if unscaled.len().saturating_add(zeros) > 131_072 {
                    return Err(invalid("JSON number exceeds PostgreSQL numeric precision"));
                }
                unscaled.extend(std::iter::repeat_n('0', zeros));
                scale = 0;
            }
            J::Decimal {
                unscaled,
                scale: usize::try_from(scale)?,
            }
        }
        serde_json::Value::Array(v) => J::Array(
            v.into_iter()
                .map(|x| json(x, depth + 1))
                .collect::<Result<_>>()?,
        ),
        serde_json::Value::Object(v) => J::Object(
            v.into_iter()
                .map(|(key, v)| {
                    Ok(JsonEntry {
                        key,
                        value: json(v, depth + 1)?,
                    })
                })
                .collect::<Result<_>>()?,
        ),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_values() {
        assert!(
            matches!(decode(1700,b"-12345678901234567890.123456").unwrap(),V::Decimal{unscaled,scale:6} if unscaled=="-12345678901234567890123456")
        );
        assert!(matches!(decode(25,b"").unwrap(),V::Text{text:Some(s),..} if s.is_empty()));
        assert!(
            matches!(decode(17,b"\\x00ff").unwrap(),V::Binary{bytes_base64url} if bytes_base64url=="AP8")
        );
        assert!(matches!(
            decode(1700, b"NaN").unwrap(),
            V::Decimal { unscaled, scale: 0 } if unscaled == "NaN"
        ));
        assert!(matches!(
            decode(1700, b"Infinity").unwrap(),
            V::Decimal { unscaled, scale: 0 } if unscaled == "Infinity"
        ));
        assert!(decode(114, b"{").is_err());
        assert!(decode(1184, b"2026-09-13 12:13:14.123456+00").is_ok());
        assert!(matches!(
            decode(1082, b"0001-01-01 BC").unwrap(),
            V::Date {
                year: 0,
                month: 1,
                day: 1
            }
        ));
        assert!(matches!(
            decode(1082, b"0001-02-29 BC").unwrap(),
            V::Date {
                year: 0,
                month: 2,
                day: 29
            }
        ));
        assert!(matches!(
            decode(1082, b"5874897-12-31").unwrap(),
            V::Date {
                year: 5_874_897,
                ..
            }
        ));
        assert!(matches!(
            decode(1114, b"0001-01-01 12:13:14.123456 BC").unwrap(),
            V::LocalDatetime {
                year: 0,
                microsecond: 123456,
                ..
            }
        ));
        assert!(matches!(
            decode(1184, b"infinity").unwrap(),
            V::TemporalInfinity {
                kind: TemporalInfinityKind::Instant,
                negative: false
            }
        ));
        assert!(matches!(
            decode(1082, b"-infinity").unwrap(),
            V::TemporalInfinity {
                kind: TemporalInfinityKind::Date,
                negative: true
            }
        ));
        assert!(matches!(
            decode(1114, b"294276-12-31 23:59:59.999999").unwrap(),
            V::LocalDatetime { year: 294_276, .. }
        ));
        assert_eq!(decimal("1e-4").unwrap(), ("1".into(), 4));
        assert_eq!(decimal("1e4").unwrap(), ("1".into(), -4));
        assert_eq!(
            decimal(&format!("1{}", "0".repeat(1_200))).unwrap().0.len(),
            1_201
        );
    }

    #[test]
    fn decodes_json_text_time_bit_network_and_xml_without_stringifying() {
        assert!(matches!(
            decode(114, br#"{"n":1}"#).unwrap(),
            V::Json { .. }
        ));
        assert!(matches!(
            decode(1083, b"12:13:14.123456").unwrap(),
            V::LocalTime {
                hour: 12,
                microsecond: 123456,
                ..
            }
        ));
        assert!(matches!(
            decode(1266, b"04:05:06.123456+07:30:15").unwrap(),
            V::OffsetTime {
                hour: 4,
                microsecond: 123456,
                offset_seconds: 27_015,
                ..
            }
        ));
        assert!(matches!(
            decode(1266, b"24:00:00-15:59").unwrap(),
            V::OffsetTime {
                hour: 24,
                offset_seconds: -57_540,
                ..
            }
        ));
        assert!(decode(1266, b"24:00:00.000001+00").is_err());
        assert!(matches!(
            decode(1186, b"P-1Y-2M3DT-4H-5M-6.123456S").unwrap(),
            V::CalendarInterval {
                months: -14,
                days: 3,
                microseconds: -14_706_123_456,
            }
        ));
        assert!(matches!(
            decode(1186, b"PT0S").unwrap(),
            V::CalendarInterval {
                months: 0,
                days: 0,
                microseconds: 0
            }
        ));
        assert!(decode(1186, b"infinity").is_err());
        assert!(matches!(
            decode_column_for_version("17", 1186, "interval", b"infinity").unwrap(),
            V::TemporalInfinity {
                kind: TemporalInfinityKind::CalendarInterval,
                negative: false
            }
        ));
        assert!(matches!(
            decode_column_for_version("17.0", 1186, "interval", b"-infinity").unwrap(),
            V::TemporalInfinity {
                kind: TemporalInfinityKind::CalendarInterval,
                negative: true
            }
        ));
        assert!(decode_column_for_version("16", 1186, "interval", b"infinity").is_err());
        assert!(decode(1186, b"PT1.1234567S").is_err());
        assert!(supported(1186));
        assert!(supported(1266));
        assert!(matches!(
            decode(26, b"4294967295").unwrap(),
            V::Integer { signed: false, bits: 32, value } if value == "4294967295"
        ));
        assert!(matches!(
            decode(5069, b"18446744073709551615").unwrap(),
            V::Integer { signed: false, bits: 64, value } if value == "18446744073709551615"
        ));
        assert!(matches!(
            decode(829, b"08:00:2b:01:02:03").unwrap(),
            V::Network { family, .. } if family == "macaddr"
        ));
        assert!(matches!(
            decode(774, b"08:00:2b:01:02:03:04:05").unwrap(),
            V::Network { family, .. } if family == "macaddr8"
        ));
        assert!(matches!(
            decode(1560, b"101").unwrap(),
            V::BitString { bit_length: 3, .. }
        ));
        assert!(matches!(
            decode(1560, b"10100000").unwrap(),
            V::BitString {
                bit_length: 8,
                padding: change_event::BitPadding::None,
                ..
            }
        ));
        assert!(matches!(
            decode(869, b"127.0.0.1/32").unwrap(),
            V::Network { family, prefix_length: Some(32), .. } if family == "ipv4"
        ));
        assert!(matches!(decode(142, b"<x/>").unwrap(), V::Xml { .. }));
    }

    #[test]
    fn decodes_hstore_and_postgis_values_from_versioned_catalog_codecs() {
        use crate::type_mapping::{SourceExtension, SourceTypeCatalog, SourceTypeDefinition};

        let catalog = SourceTypeCatalog::with_extensions(
            [
                SourceTypeDefinition::extension(
                    90_001,
                    "extensions",
                    "hstore",
                    "hstore",
                    "postgresql.hstore.text.v1",
                    "UTF-8",
                    Some(change_event::LogicalType::Map {
                        key: Box::new(change_event::LogicalType::text("UTF8", None)),
                        value: Box::new(change_event::LogicalType::text("UTF8", None)),
                    }),
                ),
                SourceTypeDefinition::extension(
                    90_002,
                    "extensions",
                    "geometry",
                    "postgis",
                    "postgresql.postgis.ewkb-hex.v1",
                    "hex-EWKB",
                    Some(change_event::LogicalType::spatial("*", None, 0)),
                ),
            ],
            [
                SourceExtension {
                    name: "hstore".into(),
                    version: "1.8".into(),
                    schema: "extensions".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
                SourceExtension {
                    name: "postgis".into(),
                    version: "3.4.2".into(),
                    schema: "extensions".into(),
                    installed: true,
                    available: true,
                    target_compatible: None,
                },
            ],
        );

        let value = decode_catalog_column_for_version(
            "15",
            &catalog,
            90_001,
            "extensions.hstore",
            b"broken",
        );
        assert!(value.is_err(), "malformed hstore must fail closed");
        let value = decode_catalog_column_for_version(
            "15",
            &catalog,
            90_001,
            "extensions.hstore",
            br#""name"=>"Ada", "nullable"=>NULL, "quote\"key"=>"a\\b""#,
        )
        .unwrap();
        let V::Map { entries } = value else {
            panic!("hstore must decode as a logical map")
        };
        assert_eq!(entries.len(), 3);
        assert!(matches!(&entries[0].key, V::Text { text: Some(key), .. } if key == "name"));
        assert!(matches!(&entries[0].value, V::Text { text: Some(value), .. } if value == "Ada"));
        assert!(matches!(&entries[1].value, V::Null));
        assert!(matches!(&entries[2].key, V::Text { text: Some(key), .. } if key == "quote\"key"));
        assert!(matches!(&entries[2].value, V::Text { text: Some(value), .. } if value == "a\\b"));

        let value = decode_catalog_column_for_version(
            "15",
            &catalog,
            90_002,
            "extensions.geometry",
            b"0101000020e6100000000000000000f03f0000000000000040",
        )
        .unwrap();
        assert!(matches!(
            value,
            V::Spatial {
                geometry_type,
                dimensions: 2,
                srid: Some(4326),
                ..
            } if geometry_type == "point"
        ));

        let iso_z =
            decode_postgis_ewkb(b"01e9030000000000000000f03f00000000000000400000000000000840")
                .unwrap();
        assert!(matches!(iso_z, V::Spatial { dimensions: 3, .. }));
        let iso_zm = decode_postgis_ewkb(
            b"01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
        )
        .unwrap();
        assert!(matches!(iso_zm, V::Spatial { dimensions: 4, .. }));
        assert!(decode_postgis_ewkb(b"02ffffffff").is_err());
    }
}
