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
        version.len() == 2 && version[0] == "15" && version[1].parse::<u32>().is_ok(),
        "unsupported PostgreSQL source version; expected 15.x",
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
                validate_image(change, is_before, image)?;
            }
        }
    }
    Ok(())
}

fn validate_image(
    change: &change_event::RowChange,
    is_before: bool,
    image: &[ColumnDatum],
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
        validate_native_type(&column.native_type)?;
        if let Datum::Value(value) = &column.datum {
            ensure(
                logical_matches_native(value, &column.native_type),
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

fn validate_native_type(native_type: &str) -> Result<(), SourceContractError> {
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
            | "jsonb"
            | "timestamp without time zone"
            | "timestamp with time zone"
    ) || valid_character(&native)
        || native == "numeric"
        || valid_parameterized_numeric(&native)
        || valid_parameterized_timestamp(&native)
        || (native.starts_with("enum(")
            && crate::type_mapping::validate_native_type(native_type).is_ok());
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

fn logical_matches_native(value: &LogicalValue, native_type: &str) -> bool {
    let native = native_type.trim().to_ascii_lowercase();
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
        LogicalValue::Decimal { .. } => native == "numeric" || valid_parameterized_numeric(&native),
        LogicalValue::Float { bits: 32, .. } => native == "real",
        LogicalValue::Float { bits: 64, .. } => native == "double precision",
        LogicalValue::Text { charset, .. } => {
            charset == "UTF8" && (native == "text" || valid_character(&native))
        }
        LogicalValue::Binary { .. } => native == "bytea",
        LogicalValue::Date { .. } => native == "date",
        LogicalValue::LocalDatetime { .. } => {
            native.ends_with("without time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::Instant { .. } => {
            native.ends_with("with time zone")
                && (native.starts_with("timestamp") || valid_parameterized_timestamp(&native))
        }
        LogicalValue::Json { .. } => native == "jsonb",
        LogicalValue::Enum { label } => native.starts_with("enum(") && !label.is_empty(),
        LogicalValue::Duration { .. } | LogicalValue::Year { .. } => false,
        _ => false,
    }
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
