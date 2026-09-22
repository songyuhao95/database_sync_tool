//! MySQL-native Source Contract validation shared by the versioned adapters.
//!
//! This crate is intentionally outside `change_event`: the core contract remains
//! database-neutral while the MySQL adapters share only their native rules.

mod compatibility;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ColumnDatum, Datum, LogicalValue, SourceContractError, SourceCursor, ValidatedTransaction,
};
pub use compatibility::capability_manifest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlVersion {
    V57,
    V80,
    V84,
}

pub fn validate_source(
    transaction: &ValidatedTransaction,
    expected: MysqlVersion,
) -> Result<(), SourceContractError> {
    let tx = transaction.transaction();
    ensure(
        tx.source.kind == "mysql",
        "MySQL Source Contract requires source kind mysql",
    )?;
    let numeric = tx.source.version.split('-').next().unwrap_or("");
    let parts: Vec<_> = numeric.split('.').collect();
    ensure(
        parts.len() == 3 && parts[2].parse::<u32>().is_ok(),
        "invalid MySQL source version",
    )?;
    let family = (parts[0], parts[1]);
    let expected_family = match expected {
        MysqlVersion::V57 => ("5", "7"),
        MysqlVersion::V80 => ("8", "0"),
        MysqlVersion::V84 => ("8", "4"),
    };
    ensure(family == expected_family, "unexpected MySQL source version")?;
    ensure(uuid(&tx.source.id), "invalid MySQL server UUID")?;
    if let Some(position) = tx.id.strip_prefix("anonymous:") {
        let valid = position
            .rsplit_once(':')
            .is_some_and(|(file, p)| !file.is_empty() && p.parse::<u32>().is_ok_and(|n| n >= 4));
        ensure(valid, "invalid anonymous MySQL transaction id")?;
    } else {
        let id: Vec<_> = tx.id.split(':').collect();
        ensure(id.len() == 2 || id.len() == 3, "invalid MySQL GTID")?;
        ensure(uuid(id[0]), "invalid GTID source UUID")?;
        ensure(
            id[0].eq_ignore_ascii_case(&tx.source.id),
            "MySQL GTID source UUID does not match source identity",
        )?;
        ensure(
            id.last().unwrap().parse::<i64>().is_ok_and(|n| n > 0),
            "invalid GTID sequence",
        )?;
        if id.len() == 3 {
            ensure(
                expected == MysqlVersion::V84,
                "tagged GTID requires MySQL 8.4",
            )?;
            let tag = id[1].as_bytes();
            ensure(
                !tag.is_empty()
                    && tag.len() <= 32
                    && (tag[0].is_ascii_alphabetic() || tag[0] == b'_')
                    && tag.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_'),
                "invalid MySQL 8.4 GTID tag",
            )?;
        }
    }
    let (file, begin) = position(&tx.begin_cursor)?;
    let (commit_file, commit) = position(&tx.commit_cursor)?;
    ensure(
        file == commit_file && begin < commit,
        "MySQL transaction must have ordered begin/commit positions in one file",
    )?;
    let mut previous = begin;
    for change in &tx.changes {
        ensure(
            change.database.is_none(),
            "MySQL database is carried in schema",
        )?;
        let (row_file, row) = position(&change.source_cursor)?;
        ensure(
            file == row_file && row > begin && row >= previous && row < commit,
            "MySQL row cursor is outside its transaction or out of order",
        )?;
        previous = row;
        for image in [&change.before, &change.after].into_iter().flatten() {
            ensure(
                image
                    .iter()
                    .all(|c| matches!(c.datum, Datum::Null | Datum::Value(_))),
                "MySQL FULL images cannot contain unavailable or unchanged values",
            )?;
            ensure(
                image.iter().enumerate().all(|(i, col)| col.ordinal == i),
                "MySQL FULL row image must contain every column in ordinal order",
            )?;
            ensure(
                image
                    .iter()
                    .any(|column| column.primary_key_ordinal.is_some()),
                "MySQL capture requires a primary key Row Locator",
            )?;
            for column in image {
                validate_column(column)?;
            }
        }
    }
    Ok(())
}

fn validate_column(column: &ColumnDatum) -> Result<(), SourceContractError> {
    let Datum::Value(value) = &column.datum else {
        return Ok(());
    };
    let native = column.native_type.trim().to_ascii_lowercase();
    let base = native.split(['(', ' ', '\t']).next().unwrap_or_default();
    let valid = match value {
        LogicalValue::Integer { signed, bits, .. } => {
            let expected_bits = match base {
                "tinyint" => 8,
                "smallint" => 16,
                "mediumint" => 24,
                "int" | "integer" => 32,
                "bigint" => 64,
                _ => 0,
            };
            expected_bits == *bits && *signed == !native.contains(" unsigned")
        }
        LogicalValue::Decimal { scale, .. } => {
            matches!(base, "decimal" | "numeric")
                && decimal_scale(&native).is_none_or(|declared| declared == *scale)
        }
        LogicalValue::Float { bits, .. } => float_bits(&native) == Some(*bits),
        LogicalValue::Text { charset, .. } => {
            matches!(
                base,
                "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
            ) && !charset.is_empty()
                && charset != "unknown"
        }
        LogicalValue::Binary { bytes_base64url } => {
            let Ok(bytes) = URL_SAFE_NO_PAD.decode(bytes_base64url) else {
                return Err(SourceContractError::new(
                    "binary value is not valid base64url bytes",
                ));
            };
            binary_length_matches(&native, base, bytes.len())
        }
        LogicalValue::BitString {
            bytes_base64url,
            bit_length,
            ..
        } => {
            let declared = native
                .split_once('(')
                .and_then(|(_, rest)| rest.split_once(')').map(|(value, _)| value))
                .unwrap_or("1")
                .parse::<u64>()
                .ok();
            let Ok(bytes) = URL_SAFE_NO_PAD.decode(bytes_base64url) else {
                return Err(SourceContractError::new(
                    "BIT value is not valid base64url bytes",
                ));
            };
            base == "bit"
                && declared == Some(*bit_length)
                && bytes.len() == bit_length.div_ceil(8) as usize
        }
        LogicalValue::Date { .. } => base == "date",
        LogicalValue::LocalDatetime { .. } => base == "datetime",
        LogicalValue::Instant { .. } => base == "timestamp",
        LogicalValue::Duration { .. } => base == "time",
        LogicalValue::Year { .. } => base == "year",
        LogicalValue::Json { .. } => base == "json",
        LogicalValue::Enum { label } => {
            base == "enum" && !label.is_empty() && !label.contains('\0')
        }
        LogicalValue::Set { members } => {
            base == "set"
                && members
                    .iter()
                    .all(|member| !member.is_empty() && !member.contains('\0'))
        }
        LogicalValue::Boolean { .. } | LogicalValue::Uuid { .. } => false,
        LogicalValue::Spatial { .. }
        | LogicalValue::Array { .. }
        | LogicalValue::ArrayWithMetadata { .. }
        | LogicalValue::Struct { .. }
        | LogicalValue::Map { .. }
        | LogicalValue::Range { .. }
        | LogicalValue::MultiRange { .. }
        | LogicalValue::Null
        | LogicalValue::LocalTime { .. }
        | LogicalValue::InvalidTemporal { .. }
        | LogicalValue::Network { .. }
        | LogicalValue::Xml { .. }
        | LogicalValue::Domain { .. }
        | LogicalValue::Raw { .. } => false,
    };
    ensure(valid, "MySQL native type does not match LogicalValue")
}

fn decimal_scale(native: &str) -> Option<usize> {
    let parameters = native
        .strip_prefix("decimal(")
        .or_else(|| native.strip_prefix("numeric("))?;
    let parameters = parameters.strip_suffix(')')?;
    let (_, scale) = parameters.split_once(',')?;
    scale.trim().parse().ok()
}

fn binary_length_matches(native: &str, base: &str, length: usize) -> bool {
    let declared = native
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')').map(|(value, _)| value))
        .and_then(|value| value.trim().parse::<usize>().ok());
    match base {
        "binary" => length == declared.unwrap_or(1),
        "varbinary" => declared.is_none_or(|maximum| length <= maximum),
        "tinyblob" => length <= 255,
        "blob" => length <= 65_535,
        "mediumblob" => length <= 16_777_215,
        "longblob" => true,
        _ => false,
    }
}

fn float_bits(native: &str) -> Option<u8> {
    let base = native.split(['(', ' ', '\t']).next().unwrap_or_default();
    match base {
        "float" => {
            let precision = native
                .strip_prefix("float(")
                .and_then(|value| value.split_once(')'))
                .and_then(|(value, _)| value.trim().parse::<u16>().ok());
            Some(if precision.is_some_and(|value| value > 24) {
                64
            } else {
                32
            })
        }
        "double" | "real" => Some(64),
        _ => None,
    }
}

fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

fn position(cursor: &SourceCursor) -> Result<(String, u32), SourceContractError> {
    ensure(
        cursor.format == "mysql.binlog.file-position.v1",
        "invalid MySQL cursor format",
    )?;
    let bytes = URL_SAFE_NO_PAD
        .decode(&cursor.value)
        .map_err(|_| SourceContractError::new("invalid MySQL cursor encoding"))?;
    ensure(bytes.len() >= 6, "truncated MySQL cursor")?;
    let split = bytes.len() - 5;
    ensure(
        bytes[split] == 0 && !bytes[..split].contains(&0),
        "invalid MySQL cursor filename",
    )?;
    let file = std::str::from_utf8(&bytes[..split])
        .map_err(|_| SourceContractError::new("invalid MySQL cursor filename UTF-8"))?;
    ensure(
        file.len() <= 255 && file.is_ascii() && !file.contains(['\0', '/', '\\']),
        "invalid MySQL cursor filename",
    )?;
    let pos = u32::from_be_bytes(bytes[split + 1..].try_into().unwrap());
    ensure(
        pos >= 4 && cursor.display == format!("{file}:{pos}"),
        "MySQL cursor display/value mismatch",
    )?;
    Ok((file.to_owned(), pos))
}

fn ensure(condition: bool, message: &str) -> Result<(), SourceContractError> {
    condition
        .then_some(())
        .ok_or_else(|| SourceContractError::new(message))
}
