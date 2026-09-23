//! PostgreSQL 15-native Source Contract validation.
//!
//! This module deliberately lives in the versioned adapter.  The database-neutral
//! `change_event` crate validates shape and logical values; PostgreSQL owns LSN,
//! identity, pgoutput image, and native type rules.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    ColumnDatum, Datum, LogicalValue, Operation, SourceContractError, SourceCursor,
    ValidatedTransaction,
};

pub(crate) fn validate(transaction: &ValidatedTransaction) -> Result<(), SourceContractError> {
    let tx = transaction.transaction();
    ensure(
        tx.source.kind == "postgresql",
        "PostgreSQL Source Contract requires source kind postgresql",
    )?;
    let version = tx.source.version.split('.').collect::<Vec<_>>();
    ensure(
        version.len() == 2
            && matches!(version[0], "15" | "16" | "17")
            && version[1].parse::<u32>().is_ok(),
        "unsupported PostgreSQL source version; expected 15.x, 16.x, or 17.x",
    )?;
    let identity = tx.source.id.split(':').collect::<Vec<_>>();
    ensure(
        matches!(identity.len(), 4 | 5)
            && identity[0] == "postgresql"
            && identity[1].parse::<u64>().is_ok_and(|value| value > 0)
            && identity[2].parse::<u32>().is_ok_and(|value| value > 0)
            && identity[3].parse::<u32>().is_ok_and(|value| value > 0),
        "invalid PostgreSQL source identity",
    )?;

    let begin = lsn(&tx.begin_cursor)?;
    let end = lsn(&tx.commit_cursor)?;
    ensure(
        begin < end,
        "PostgreSQL commit end must follow its commit record",
    )?;
    let (xid, suffix) = tx
        .id
        .strip_prefix("pg:")
        .and_then(|value| value.split_once(':'))
        .ok_or_else(|| SourceContractError::new("invalid PostgreSQL transaction id"))?;
    ensure(
        xid.parse::<u32>().is_ok_and(|value| value > 0) && suffix == tx.commit_cursor.value,
        "PostgreSQL transaction id/end LSN mismatch",
    )?;

    let database = tx
        .changes
        .first()
        .and_then(|change| change.database.as_deref())
        .unwrap_or("");
    ensure(
        !database.is_empty() && !database.contains('\0'),
        "PostgreSQL database is required",
    )?;
    if let Some(encoded_database) = identity.get(4) {
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded_database)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok());
        ensure(
            decoded.as_deref() == Some(database),
            "PostgreSQL source identity/database mismatch",
        )?;
    }
    for change in &tx.changes {
        ensure(
            change.database.as_deref() == Some(database),
            "PostgreSQL transaction spans databases",
        )?;
        ensure(
            lsn(&change.source_cursor)? == end,
            "PostgreSQL row cursor must equal its transaction end",
        )?;
        for (is_before, image) in [(true, &change.before), (false, &change.after)] {
            if let Some(image) = image {
                validate_image(change, is_before, image, version[0])?;
            }
        }
    }
    Ok(())
}

fn validate_image(
    change: &change_event::RowChange,
    is_before: bool,
    image: &[ColumnDatum],
    version: &str,
) -> Result<(), SourceContractError> {
    ensure(
        change
            .schema_basis
            .strip_prefix("pgoutput+catalog:")
            .is_some_and(|oid| oid.parse::<u32>().is_ok_and(|value| value > 0)),
        "invalid PostgreSQL schema basis",
    )?;
    ensure(
        image
            .iter()
            .any(|column| column.primary_key_ordinal.is_some()),
        "PostgreSQL capture requires a primary key",
    )?;
    ensure(
        image
            .iter()
            .enumerate()
            .all(|(ordinal, column)| column.ordinal == ordinal),
        "PostgreSQL image ordinals must be contiguous",
    )?;
    for column in image {
        ensure(
            !column.generated,
            "generated PostgreSQL columns are unsupported",
        )?;
        validate_presence(change.operation, is_before, column)?;
        validate_native_type_for_version(version, &column.native_type)?;
        if let Datum::Value(value) = &column.datum {
            ensure(
                logical_matches_native(value, &column.native_type, version),
                "PostgreSQL native/logical type mismatch",
            )?;
        }
    }
    Ok(())
}

fn validate_presence(
    operation: Operation,
    is_before: bool,
    column: &ColumnDatum,
) -> Result<(), SourceContractError> {
    match column.datum {
        Datum::Unavailable => ensure(
            is_before
                && !matches!(operation, Operation::Insert)
                && column.primary_key_ordinal.is_none(),
            "unavailable value is only valid in non-key before columns",
        ),
        Datum::Unchanged => ensure(
            !is_before
                && matches!(operation, Operation::Update)
                && column.primary_key_ordinal.is_none(),
            "unchanged value is only valid in non-key UPDATE after columns",
        ),
        _ => Ok(()),
    }
}

fn validate_native_type_for_version(
    version: &str,
    native_type: &str,
) -> Result<(), SourceContractError> {
    let native = native_type.trim().to_ascii_lowercase();
    let supported = matches!(
        native.as_str(),
        "boolean"
            | "uuid"
            | "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "text"
            | "bytea"
            | "date"
            | "time"
            | "time without time zone"
            | "inet"
            | "cidr"
            | "macaddr"
            | "macaddr8"
            | "xml"
            | "jsonb"
            | "json"
            | "timestamp without time zone"
            | "timestamp with time zone"
    ) || valid_character(&native)
        || valid_parameterized_numeric(&native)
        || valid_parameterized_timestamp(&native)
        || valid_parameterized_time(&native)
        || valid_parameterized_bit(&native)
        || (native.starts_with("enum(")
            && crate::type_mapping::validate_native_type_for_version(version, native_type).is_ok());
    ensure(supported, "unsupported PostgreSQL native type")
}

fn valid_character(native: &str) -> bool {
    if matches!(native, "character" | "character varying") {
        return true;
    }
    let rest = native
        .strip_prefix("character(")
        .or_else(|| native.strip_prefix("character varying("));
    rest.and_then(|value| value.strip_suffix(')'))
        .is_some_and(|length| length.parse::<usize>().is_ok_and(|value| value > 0))
}

fn valid_parameterized_numeric(native: &str) -> bool {
    let Some(parameters) = native
        .strip_prefix("numeric(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return false;
    };
    let Some((precision, scale)) = parameters.split_once(',') else {
        return false;
    };
    let Ok(precision) = precision.trim().parse::<usize>() else {
        return false;
    };
    let Ok(scale) = scale.trim().parse::<usize>() else {
        return false;
    };
    precision > 0 && precision <= 1000 && scale <= precision
}

fn valid_parameterized_timestamp(native: &str) -> bool {
    let Some(rest) = native.strip_prefix("timestamp(") else {
        return false;
    };
    let Some((precision, suffix)) = rest.split_once(") ") else {
        return false;
    };
    precision.parse::<u8>().is_ok_and(|value| value <= 6)
        && matches!(suffix, "without time zone" | "with time zone")
}

fn logical_matches_native(value: &LogicalValue, native_type: &str, version: &str) -> bool {
    let native_raw = native_type.trim();
    let native = native_raw.to_ascii_lowercase();
    match value {
        LogicalValue::Boolean { .. } => native == "boolean",
        LogicalValue::Uuid { .. } => native == "uuid",
        LogicalValue::Integer {
            signed: true,
            bits: 16,
            ..
        } => native == "smallint",
        LogicalValue::Integer {
            signed: true,
            bits: 32,
            ..
        } => native == "integer",
        LogicalValue::Integer {
            signed: true,
            bits: 64,
            ..
        } => native == "bigint",
        LogicalValue::Decimal { .. } => valid_parameterized_numeric(&native),
        LogicalValue::Float { bits: 32, .. } => native == "real",
        LogicalValue::Float { bits: 64, .. } => native == "double precision",
        LogicalValue::Text { charset, .. } => {
            charset == "UTF8" && (native == "text" || valid_character(&native))
        }
        LogicalValue::Binary { .. } => native == "bytea",
        LogicalValue::Date { .. } => native == "date",
        LogicalValue::LocalTime { .. } => {
            native == "time"
                || native == "time without time zone"
                || valid_parameterized_time(&native)
        }
        LogicalValue::LocalDatetime { .. } => {
            native.ends_with("without time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::Instant { .. } => {
            native.ends_with("with time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::Json { .. } => native == "jsonb" || native == "json",
        LogicalValue::BitString { .. } => native == "bit" || valid_parameterized_bit(&native),
        LogicalValue::Network {
            family,
            address,
            prefix_length,
        } => valid_network(&native, family, address, *prefix_length),
        LogicalValue::Xml { .. } => native == "xml",
        LogicalValue::Enum { label } => {
            if !native.starts_with("enum(") || label.is_empty() {
                false
            } else {
                crate::type_mapping::source_type_mapping_for_version(version, native_raw)
                    .ok()
                    .and_then(|mapping| match mapping.logical_type {
                        change_event::LogicalType::Enum { members } => Some(members),
                        _ => None,
                    })
                    .is_some_and(|members| members.iter().any(|member| member == label))
            }
        }
        LogicalValue::Duration { .. } | LogicalValue::Year { .. } => false,
        _ => false,
    }
}

fn valid_network(native: &str, family: &str, address: &str, prefix_length: Option<u8>) -> bool {
    match native {
        "inet" | "cidr" => {
            let Ok(parsed) = address.parse::<std::net::IpAddr>() else {
                return false;
            };
            let is_v6 = parsed.is_ipv6();
            if (family == "ipv6") != is_v6 || (family != "ipv6" && family != "ipv4") {
                return false;
            }
            prefix_length.is_none_or(|prefix| prefix <= if is_v6 { 128 } else { 32 })
        }
        "macaddr" | "macaddr8" => {
            let expected = if native == "macaddr" { 6 } else { 8 };
            family == native
                && address.split(':').count() == expected
                && address.split(':').all(|part| {
                    part.len() == 2 && part.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                && prefix_length.is_none()
        }
        _ => false,
    }
}

fn valid_parameterized_bit(native: &str) -> bool {
    ["bit(", "bit varying(", "varbit("]
        .iter()
        .find_map(|prefix| native.strip_prefix(prefix))
        .and_then(|rest| rest.strip_suffix(')'))
        .is_some_and(|length| length.parse::<u64>().is_ok_and(|value| value > 0))
}

fn valid_parameterized_time(native: &str) -> bool {
    let Some(rest) = native.strip_prefix("time(") else {
        return false;
    };
    let Some((precision, suffix)) = rest.split_once(")") else {
        return false;
    };
    precision.trim().parse::<u8>().is_ok_and(|value| value <= 6)
        && matches!(suffix.trim(), "" | "without time zone")
}

fn lsn(cursor: &SourceCursor) -> Result<u64, SourceContractError> {
    ensure(
        cursor.format == "postgresql.lsn.v1" && cursor.display == cursor.value,
        "invalid PostgreSQL cursor format or display",
    )?;
    let (high, low) = cursor
        .value
        .split_once('/')
        .ok_or_else(|| SourceContractError::new("invalid LSN"))?;
    ensure(
        [high, low].iter().all(|part| {
            !part.is_empty() && part.len() <= 8 && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        }),
        "invalid PostgreSQL LSN",
    )?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|_| SourceContractError::new("invalid PostgreSQL LSN"))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|_| SourceContractError::new("invalid PostgreSQL LSN"))?;
    let value = (high << 32) | low;
    ensure(value > 0, "zero PostgreSQL event LSN")?;
    Ok(value)
}

fn ensure(condition: bool, message: &str) -> Result<(), SourceContractError> {
    condition
        .then_some(())
        .ok_or_else(|| SourceContractError::new(message))
}
