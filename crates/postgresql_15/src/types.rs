use crate::{Result, invalid};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{JsonEntry, JsonValue as J, LogicalValue as V};
use chrono::{Datelike, Timelike};

pub(crate) fn supported(oid: u32) -> bool {
    matches!(
        oid,
        16 | 17
            | 20
            | 21
            | 23
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
            | 1184
            | 1560
            | 1562
            | 1700
            | 2950
            | 3802
    )
}
pub(crate) fn decode(oid: u32, bytes: &[u8]) -> Result<V> {
    let text = std::str::from_utf8(bytes)?;
    Ok(match oid {
        16 => V::Boolean {
            value: match text {
                "t" => true,
                "f" => false,
                _ => return Err(invalid("invalid PostgreSQL boolean")),
            },
        },
        20 | 21 | 23 => {
            let value = text.parse::<i64>()?;
            let bits = match oid {
                20 => 64,
                21 => 16,
                _ => 32,
            };
            V::Integer {
                signed: true,
                bits,
                value: value.to_string(),
            }
        }
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
        25 | 1042 | 1043 => V::Text {
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
            let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")?;
            V::Date {
                year: u16::try_from(date.year())?,
                month: date.month() as u8,
                day: date.day() as u8,
            }
        }
        1083 => local_time(text)?,
        1114 => {
            let d = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")?;
            V::LocalDatetime {
                year: u16::try_from(d.year())?,
                month: d.month() as u8,
                day: d.day() as u8,
                hour: d.hour() as u8,
                minute: d.minute() as u8,
                second: d.second() as u8,
                microsecond: d.nanosecond() / 1000,
            }
        }
        1184 => {
            let s = if text.ends_with("+00") {
                format!("{text}:00")
            } else {
                text.into()
            };
            let d = chrono::DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f%:z")?;
            V::Instant {
                unix_seconds: d.timestamp().to_string(),
                nanoseconds: d.timestamp_subsec_nanos(),
            }
        }
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
    decode(oid, bytes)
}

fn local_time(text: &str) -> Result<V> {
    let time = chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f")?;
    Ok(V::LocalTime {
        hour: time.hour() as u8,
        minute: time.minute() as u8,
        second: time.second() as u8,
        microsecond: time.nanosecond() / 1000,
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
        padding: change_event::BitPadding::Zero,
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
        774 => "macaddr",
        829 => "macaddr8",
        _ if address.contains(':') => "ipv6",
        _ => "ipv4",
    };
    Ok(V::Network {
        family: family.into(),
        address: address.into(),
        prefix_length,
    })
}

fn decimal(text: &str) -> Result<(String, usize)> {
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
    if scale < 0 {
        let zeros = usize::try_from(-scale)?;
        if digits.len().saturating_add(zeros) > 1000 {
            return Err(invalid("numeric exceeds 1000 digits"));
        }
        digits.extend(std::iter::repeat_n('0', zeros));
    }
    let scale = usize::try_from(scale.max(0))?;
    if digits.len() > 1000 || scale > 1000 {
        return Err(invalid("numeric exceeds ChangeEvent range"));
    }
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
            let (unscaled, scale) = decimal(v.as_str())?;
            J::Decimal { unscaled, scale }
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
        assert!(decode(1700, b"NaN").is_err());
        assert!(decode(114, b"{").is_err());
        assert!(decode(1184, b"2026-09-13 12:13:14.123456+00").is_ok());
        assert_eq!(decimal("1e-4").unwrap(), ("1".into(), 4));
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
            decode(1560, b"101").unwrap(),
            V::BitString { bit_length: 3, .. }
        ));
        assert!(matches!(
            decode(869, b"127.0.0.1/32").unwrap(),
            V::Network { family, prefix_length: Some(32), .. } if family == "ipv4"
        ));
        assert!(matches!(decode(142, b"<x/>").unwrap(), V::Xml { .. }));
    }
}
