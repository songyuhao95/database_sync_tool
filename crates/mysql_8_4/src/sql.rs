use std::io;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

use change_event::{
    CapabilityManifest, ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue, Operation,
    RowChange, TargetCapabilityFailure, ValidatedTransaction,
};
use mysql_driver::prelude::Queryable;
use mysql_driver::{Conn, OptsBuilder, TxOpts, Value};

#[derive(Debug)]
struct CommitUnknown(String);
impl std::fmt::Display for CommitUnknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COMMIT acknowledgement unavailable: {}", self.0)
    }
}
impl std::error::Error for CommitUnknown {}
/// A failed COMMIT acknowledgement must never be blindly retried.
pub fn commit_outcome_unknown(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|cause| cause.is::<CommitUnknown>())
}

/// Classify an apply error without exposing driver-specific errors to the Web
/// worker.  MySQL error 1205 is a lock wait timeout and 1213 is a deadlock;
/// both roll back the whole transaction and are safe to replay.
pub fn classify_apply_error(error: &io::Error) -> change_event::TargetApplyErrorKind {
    if commit_outcome_unknown(error) {
        return change_event::TargetApplyErrorKind::CommitUnknown;
    }
    if error
        .get_ref()
        .is_some_and(|cause| cause.is::<TargetCapabilityFailure>())
    {
        return change_event::TargetApplyErrorKind::Capability;
    }
    if let Some(cause) = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<mysql_driver::Error>())
    {
        if cause.is_connectivity_error() {
            return change_event::TargetApplyErrorKind::Connection;
        }
        if let mysql_driver::Error::MySqlError(server) = cause {
            if matches!(server.code, 1205 | 1213) {
                return change_event::TargetApplyErrorKind::LockTimeout;
            }
            if matches!(server.code, 1022 | 1062 | 1216 | 1217 | 1451 | 1452 | 1048) {
                return change_event::TargetApplyErrorKind::Constraint;
            }
        }
        return change_event::TargetApplyErrorKind::Sql;
    }
    match error.kind() {
        io::ErrorKind::TimedOut
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof => change_event::TargetApplyErrorKind::Connection,
        _ => change_event::TargetApplyErrorKind::Sql,
    }
}

pub(crate) fn unknown_commit(error: mysql_driver::Error) -> io::Error {
    io::Error::other(CommitUnknown(error.to_string()))
}

struct SqlRenderer;

const TARGET_VERSION: &str = "8.4";

pub const CAPABILITY_MANIFEST: CapabilityManifest = CapabilityManifest {
    connector: "mysql_8_4",
    target: "mysql-8.4",
    contract: change_event::FORMAT,
    supported_logical_types: &[
        "boolean",
        "uuid",
        "integer",
        "decimal",
        "float",
        "text",
        "binary",
        "bit_string",
        "date",
        "local_datetime",
        "duration",
        "instant",
        "year",
        "json",
        "enum",
        "set",
    ],
    supported_presence: &["value", "null", "unchanged", "unavailable"],
    requires_primary_key: true,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct SinkAdapter;

impl SinkAdapter {
    pub const fn new() -> Self {
        Self
    }
}

pub const fn capability_manifest() -> CapabilityManifest {
    CAPABILITY_MANIFEST
}

impl change_event::SinkAdapter for SinkAdapter {
    type Plan = SqlTransaction;
    type Error = io::Error;

    fn capability_manifest(&self) -> CapabilityManifest {
        CAPABILITY_MANIFEST
    }

    fn qualify(&self, transaction: &ValidatedTransaction) -> io::Result<()> {
        sql(transaction).map(|_| ())
    }

    fn plan(&self, transaction: &ValidatedTransaction) -> io::Result<SqlTransaction> {
        sql(transaction)
    }
}

#[derive(Clone)]
pub struct TargetConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

impl std::fmt::Debug for TargetConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TargetConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .finish()
    }
}
impl TargetConfig {
    pub fn new(
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 3306,
            user: user.into(),
            password: password.into(),
        }
    }
}
#[derive(Debug, Clone)]
struct PlannedStatement {
    sql: String,
    params: Vec<Value>,
    probe: Option<PreparedQuery>,
    operation: Operation,
}

#[derive(Debug, Clone)]
struct PreparedQuery {
    sql: String,
    params: Vec<Value>,
}
#[derive(Debug, Clone)]
pub struct SqlTransaction {
    source_transaction_id: String,
    pub(crate) source_uuid: String,
    pub(crate) commit_cursor: change_event::SourceCursor,
    tables: Vec<(String, String)>,
    statements: Vec<PlannedStatement>,
}
impl SqlTransaction {
    pub fn source_transaction_id(&self) -> &str {
        &self.source_transaction_id
    }
    pub fn statements(&self) -> impl Iterator<Item = &str> {
        self.statements.iter().map(|s| s.sql.as_str())
    }
    pub fn parameters(&self) -> impl Iterator<Item = &[Value]> {
        self.statements
            .iter()
            .map(|statement| statement.params.as_slice())
    }
    pub fn script(&self) -> String {
        let mut output = format!(
            "-- cdc transaction={} target=mysql-{TARGET_VERSION}\nSTART TRANSACTION;\n",
            sanitize_comment(&self.source_transaction_id)
        );
        for statement in &self.statements {
            output.push_str(&statement.sql);
            output.push('\n');
        }
        output.push_str("COMMIT;\n");
        output
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub source_transaction_id: String,
    pub statements_executed: usize,
}

pub fn sql(validated: &ValidatedTransaction) -> io::Result<SqlTransaction> {
    let transaction = validated.transaction();
    let mut statements = Vec::with_capacity(transaction.changes.len());
    for change in &transaction.changes {
        ensure_supported_change(change)?;
        let (rendered, params) = SqlRenderer.write_change(change)?;
        let probe = match change.operation {
            Operation::Insert => None,
            Operation::Update | Operation::Delete => {
                let (predicates, params) =
                    key_predicates(change.before.as_ref().expect("validated image"))?;
                Some(PreparedQuery {
                    sql: format!(
                        "SELECT 1 FROM {} WHERE {predicates} LIMIT 2 FOR UPDATE",
                        qualified_table(&change.schema, &change.table),
                    ),
                    params,
                })
            }
        };
        statements.push(PlannedStatement {
            sql: rendered,
            params,
            probe,
            operation: change.operation,
        });
    }
    Ok(SqlTransaction {
        source_transaction_id: transaction.id.clone(),
        source_uuid: transaction.source.id.clone(),
        commit_cursor: transaction.commit_cursor.clone(),
        tables: transaction
            .changes
            .iter()
            .map(|c| (c.schema.clone(), c.table.clone()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        statements,
    })
}

pub fn execute(config: &TargetConfig, plan: &SqlTransaction) -> io::Result<ApplyResult> {
    let mut conn = connect(config)?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .map_err(io::Error::other)?;
    execute_statements(&mut tx, plan)?;
    tx.commit().map_err(unknown_commit)?;
    Ok(ApplyResult {
        source_transaction_id: plan.source_transaction_id.clone(),
        statements_executed: plan.statements.len(),
    })
}

pub(crate) fn connect(config: &TargetConfig) -> io::Result<Conn> {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(config.host.clone()))
        .tcp_port(config.port)
        .user(Some(config.user.clone()))
        .pass(Some(config.password.clone()))
        .tcp_connect_timeout(Some(std::time::Duration::from_secs(5)))
        .read_timeout(Some(std::time::Duration::from_secs(15)))
        .write_timeout(Some(std::time::Duration::from_secs(15)));
    let mut conn = Conn::new(opts).map_err(io::Error::other)?;
    let version: String = conn
        .query_first("SELECT VERSION()")
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("target returned no version"))?;
    if !version.starts_with(TARGET_VERSION) {
        return Err(capability_failure(format!(
            "mysql_{TARGET_VERSION} cannot write target version {version}"
        )));
    }
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .map_err(io::Error::other)?;
    conn.query_drop(
        "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,NO_AUTO_VALUE_ON_ZERO'",
    )
    .map_err(io::Error::other)?;
    Ok(conn)
}

pub(crate) fn execute_statements(
    tx: &mut mysql_driver::Transaction<'_>,
    plan: &SqlTransaction,
) -> io::Result<()> {
    for (schema, table) in &plan.tables {
        if schema.eq_ignore_ascii_case("CDC") {
            return Err(capability_failure(
                "CDC is reserved for replication control data",
            ));
        }
        // Hold a metadata lock until COMMIT so ALTER ENGINE cannot race this check.
        tx.query_drop(format!(
            "SELECT * FROM {} LIMIT 0",
            qualified_table(schema, table)
        ))
        .map_err(io::Error::other)?;
        let engine: Option<String> = tx.exec_first(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (schema, table)
        ).map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(capability_failure(
                "atomic Sink writes require InnoDB business tables",
            ));
        }
        let triggers: u64 = tx.exec_first(
            "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?", (schema, table)
        ).map_err(io::Error::other)?.unwrap_or(0);
        if triggers != 0 {
            return Err(capability_failure(
                "Sink triggers require separate qualification; transaction rolled back",
            ));
        }
    }
    for statement in &plan.statements {
        if let Some(probe) = &statement.probe {
            let rows: Vec<u8> = tx
                .exec(&probe.sql, probe.params.clone())
                .map_err(io::Error::other)?;
            if rows.len() != 1 {
                return Err(io::Error::other(format!(
                    "row locator matched {} rows; transaction rolled back",
                    rows.len()
                )));
            }
        }
        tx.exec_drop(&statement.sql, statement.params.clone())
            .map_err(io::Error::other)?;
        if tx.warnings() != 0 {
            return Err(io::Error::other(
                "target reported SQL warnings; transaction rolled back",
            ));
        }
        let affected = tx.affected_rows();
        if matches!(statement.operation, Operation::Insert | Operation::Delete) && affected != 1
            || matches!(statement.operation, Operation::Update) && affected > 1
        {
            return Err(io::Error::other(format!(
                "unexpected affected row count {affected}; transaction rolled back"
            )));
        }
    }
    Ok(())
}

impl SqlRenderer {
    fn write_change(&self, change: &RowChange) -> io::Result<(String, Vec<Value>)> {
        let table = qualified_table(&change.schema, &change.table);
        match change.operation {
            Operation::Insert => self.write_insert(&table, change),
            Operation::Update => self.write_update(&table, change),
            Operation::Delete => self.write_delete(&table, change),
        }
    }

    fn write_insert(&self, table: &str, change: &RowChange) -> io::Result<(String, Vec<Value>)> {
        let after = change
            .after
            .as_ref()
            .ok_or_else(|| io::Error::other("INSERT ChangeEvent is missing its after image"))?;
        ensure_non_empty_row(after, "INSERT")?;
        let writable = writable_columns(after);
        if writable.is_empty() {
            return Err(capability_failure("INSERT has no writable columns"));
        }
        Ok((
            format!(
                "INSERT INTO {table} ({}) VALUES ({});",
                column_list(&writable),
                placeholders(writable.len())
            ),
            bind_columns(&writable)?,
        ))
    }

    fn write_update(&self, table: &str, change: &RowChange) -> io::Result<(String, Vec<Value>)> {
        let before = change
            .before
            .as_ref()
            .ok_or_else(|| io::Error::other("UPDATE ChangeEvent is missing its before image"))?;
        let after = change
            .after
            .as_ref()
            .ok_or_else(|| io::Error::other("UPDATE ChangeEvent is missing its after image"))?;
        ensure_non_empty_row(before, "UPDATE before")?;
        ensure_non_empty_row(after, "UPDATE after")?;

        let writable = writable_columns(after);
        let (assignments, mut params) = assignments(&writable)?;
        let (predicates, key_params) = key_predicates(before)?;
        params.extend(key_params);
        Ok((
            format!("UPDATE {table} SET {assignments} WHERE {predicates};"),
            params,
        ))
    }

    fn write_delete(&self, table: &str, change: &RowChange) -> io::Result<(String, Vec<Value>)> {
        let before = change
            .before
            .as_ref()
            .ok_or_else(|| io::Error::other("DELETE ChangeEvent is missing its before image"))?;
        ensure_non_empty_row(before, "DELETE before")?;
        let (predicates, params) = key_predicates(before)?;
        Ok((format!("DELETE FROM {table} WHERE {predicates};"), params))
    }
}

fn ensure_non_empty_row(row: &[ColumnDatum], label: &str) -> io::Result<()> {
    if row.is_empty() {
        return Err(capability_failure(format!("{label} image has no columns")));
    }
    Ok(())
}

pub(crate) fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    let quote = char::from(96);
    let escaped_quote = format!("{quote}{quote}");
    let escaped = identifier.replace(quote, &escaped_quote);
    format!("{quote}{escaped}{quote}")
}

fn writable_columns(row: &[ColumnDatum]) -> Vec<&ColumnDatum> {
    row.iter()
        .filter(|column| {
            !column.generated && !matches!(column.datum, Datum::Unavailable | Datum::Unchanged)
        })
        .collect()
}

fn column_list(row: &[&ColumnDatum]) -> String {
    row.iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn assignments(row: &[&ColumnDatum]) -> io::Result<(String, Vec<Value>)> {
    if row.is_empty() {
        return Err(capability_failure("UPDATE has no writable columns"));
    }
    Ok((
        row.iter()
            .map(|column| format!("{} = ?", quote_identifier(&column.name)))
            .collect::<Vec<_>>()
            .join(", "),
        bind_columns(row)?,
    ))
}

fn key_predicates(row: &[ColumnDatum]) -> io::Result<(String, Vec<Value>)> {
    let mut keys: Vec<_> = row
        .iter()
        .filter(|column| column.primary_key_ordinal.is_some())
        .collect();
    keys.sort_by_key(|column| column.primary_key_ordinal);
    if keys.is_empty() {
        return Err(capability_failure(
            "table has no primary key; first Sink version requires one",
        ));
    }
    let mut params = Vec::new();
    let predicates = keys
        .into_iter()
        .map(|column| {
            let name = quote_identifier(&column.name);
            match &column.datum {
                Datum::Unavailable | Datum::Unchanged => Err(capability_failure(
                    "cannot use an absent value as a predicate",
                )),
                Datum::Null => Ok(format!("{name} IS NULL")),
                Datum::Value(LogicalValue::Text { .. } | LogicalValue::Binary { .. }) => {
                    params.push(bind_datum(&column.datum)?);
                    Ok(format!("CAST({name} AS BINARY) = CAST(? AS BINARY)"))
                }
                Datum::Value(_) => {
                    params.push(bind_datum(&column.datum)?);
                    Ok(format!("{name} = ?"))
                }
            }
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok((predicates.join(" AND "), params))
}

fn bind_columns(row: &[&ColumnDatum]) -> io::Result<Vec<Value>> {
    row.iter().map(|column| bind_datum(&column.datum)).collect()
}

fn bind_datum(datum: &Datum) -> io::Result<Value> {
    match datum {
        Datum::Unavailable | Datum::Unchanged => Err(capability_failure(
            "cannot render an unavailable or unchanged datum",
        )),
        Datum::Null => Ok(Value::NULL),
        Datum::Value(value) => bind_logical_value(value),
    }
}

fn bind_logical_value(value: &LogicalValue) -> io::Result<Value> {
    match value {
        LogicalValue::Boolean { value } => Ok(Value::Int(i64::from(*value))),
        LogicalValue::Uuid { value } => {
            validate_uuid(value)?;
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }
        LogicalValue::Integer { signed, value, .. } => {
            if *signed {
                value
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| capability_failure("signed integer is outside MySQL range"))
            } else {
                value
                    .parse::<u64>()
                    .map(Value::UInt)
                    .map_err(|_| capability_failure("unsigned integer is outside MySQL range"))
            }
        }
        LogicalValue::Decimal { unscaled, scale } => {
            let digits = unscaled.strip_prefix(['-', '+']).unwrap_or(unscaled);
            if digits.len() > 65 || *scale > 30 {
                return Err(capability_failure(
                    "DECIMAL precision exceeds the MySQL target capability",
                ));
            }
            Ok(Value::Bytes(render_decimal(unscaled, *scale)?.into_bytes()))
        }
        LogicalValue::Float { bits, ieee754_hex } => match bits {
            32 => {
                let value = f32::from_bits(
                    parse_hex_bits(ieee754_hex, 8)
                        .map_err(|error| capability_failure(error.to_string()))?
                        as u32,
                );
                if value.is_finite() {
                    Ok(Value::Float(value))
                } else {
                    Err(capability_failure(
                        "MySQL does not preserve non-finite FLOAT values",
                    ))
                }
            }
            64 => {
                let value = f64::from_bits(
                    parse_hex_bits(ieee754_hex, 16)
                        .map_err(|error| capability_failure(error.to_string()))?,
                );
                if value.is_finite() {
                    Ok(Value::Double(value))
                } else {
                    Err(capability_failure(
                        "MySQL does not preserve non-finite DOUBLE values",
                    ))
                }
            }
            _ => Err(capability_failure("unsupported floating point width")),
        },
        LogicalValue::Text {
            charset,
            bytes_base64url,
            ..
        } => {
            if !matches!(charset.to_ascii_lowercase().as_str(), "utf8" | "utf8mb4") {
                return Err(capability_failure(format!(
                    "text charset {charset} has no qualified MySQL mapping"
                )));
            }
            Ok(Value::Bytes(decode_bytes(bytes_base64url)?))
        }
        LogicalValue::Binary { bytes_base64url } => {
            Ok(Value::Bytes(decode_bytes(bytes_base64url)?))
        }
        LogicalValue::BitString {
            bytes_base64url, ..
        } => Ok(Value::Bytes(decode_bytes(bytes_base64url)?)),
        LogicalValue::Date { year, month, day } => Ok(Value::Date(*year, *month, *day, 0, 0, 0, 0)),
        LogicalValue::LocalDatetime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => Ok(Value::Date(
            *year,
            *month,
            *day,
            *hour,
            *minute,
            *second,
            *microsecond,
        )),
        LogicalValue::Duration {
            negative,
            hours,
            minutes,
            seconds,
            microsecond,
        } => {
            let days = hours / 24;
            let hours = hours % 24;
            let days =
                u32::try_from(days).map_err(|_| capability_failure("duration is too large"))?;
            Ok(Value::Time(
                *negative,
                days,
                u8::try_from(hours).unwrap_or(0),
                *minutes,
                *seconds,
                *microsecond,
            ))
        }
        LogicalValue::Instant {
            unix_seconds,
            nanoseconds,
        } => unix_instant_value(unix_seconds, *nanoseconds),
        LogicalValue::Year { value } => Ok(Value::UInt(u64::from(*value))),
        LogicalValue::Enum { label } => Ok(Value::Bytes(label.as_bytes().to_vec())),
        LogicalValue::Set { members } => Ok(Value::Bytes(members.join(",").into_bytes())),
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
        | LogicalValue::Raw { .. } => Err(capability_failure(
            "MySQL 8.4 has no qualified target representation for this structured value",
        )),
        LogicalValue::Json { value } => {
            let json = render_json(value).map_err(|error| capability_failure(error.to_string()))?;
            Ok(Value::Bytes(json.into_bytes()))
        }
    }
}

fn unix_instant_value(seconds: &str, nanoseconds: u32) -> io::Result<Value> {
    if nanoseconds > 999_999_999 || !nanoseconds.is_multiple_of(1_000) {
        return Err(capability_failure(
            "MySQL target supports TIMESTAMP precision only through microseconds",
        ));
    }
    let seconds = validate_integer(seconds, true)?
        .parse::<i64>()
        .map_err(|_| capability_failure("instant seconds are outside MySQL range"))?;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let month_part = (5 * doy + 2) / 153;
    let day = doy - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    if !(1..=65_535).contains(&year) {
        return Err(capability_failure("instant is outside MySQL date range"));
    }
    Ok(Value::Date(
        year as u16,
        month as u8,
        day as u8,
        (day_seconds / 3_600) as u8,
        ((day_seconds % 3_600) / 60) as u8,
        (day_seconds % 60) as u8,
        nanoseconds / 1_000,
    ))
}

fn decode_bytes(value: &str) -> io::Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn validate_integer(value: &str, signed: bool) -> io::Result<String> {
    let digits = if signed {
        value.strip_prefix(['-', '+']).unwrap_or(value)
    } else {
        value
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid integer literal {value:?}"),
        ));
    }
    if !signed && value.starts_with('-') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsigned integer cannot be negative: {value:?}"),
        ));
    }
    Ok(value.to_owned())
}

fn render_decimal(unscaled: &str, scale: usize) -> io::Result<String> {
    let negative = unscaled.starts_with('-');
    let digits = unscaled.strip_prefix(['-', '+']).unwrap_or(unscaled);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid decimal unscaled value {unscaled:?}"),
        ));
    }
    if scale == 0 {
        return Ok(format!("{}{}", if negative { "-" } else { "" }, digits));
    }

    let value = if digits.len() > scale {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    Ok(format!("{}{}", if negative { "-" } else { "" }, value))
}

fn render_float(bits: u8, hexadecimal: &str) -> io::Result<String> {
    match bits {
        32 => {
            let raw = parse_hex_bits(hexadecimal, 8)?;
            let value = f32::from_bits(raw as u32);
            if !value.is_finite() {
                return Err(io::Error::other(
                    "MySQL SQL output does not support non-finite FLOAT values",
                ));
            }
            Ok(if value == 0.0 && value.is_sign_negative() {
                "-0.0".to_owned()
            } else {
                value.to_string()
            })
        }
        64 => {
            let raw = parse_hex_bits(hexadecimal, 16)?;
            let value = f64::from_bits(raw);
            if !value.is_finite() {
                return Err(io::Error::other(
                    "MySQL SQL output does not support non-finite DOUBLE values",
                ));
            }
            Ok(if value == 0.0 && value.is_sign_negative() {
                "-0.0".to_owned()
            } else {
                value.to_string()
            })
        }
        _ => Err(io::Error::other(format!(
            "unsupported floating point width: {bits}"
        ))),
    }
}

fn parse_hex_bits(value: &str, width: usize) -> io::Result<u64> {
    if value.len() != width || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid IEEE-754 bit pattern {value:?}"),
        ));
    }
    u64::from_str_radix(value, 16)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn render_json(value: &JsonValue) -> io::Result<String> {
    match value {
        JsonValue::Null => Ok("null".to_owned()),
        JsonValue::Boolean(value) => Ok(value.to_string()),
        JsonValue::String(value) => serde_json::to_string(value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        JsonValue::SignedInteger(value) => validate_integer(value, true),
        JsonValue::UnsignedInteger(value) => validate_integer(value, false),
        JsonValue::DoubleBits(value) => render_json_double(value),
        JsonValue::Decimal { unscaled, scale } => render_decimal(unscaled, *scale),
        JsonValue::Array(values) => {
            let items = values
                .iter()
                .map(render_json)
                .collect::<io::Result<Vec<_>>>()?;
            Ok(format!("[{}]", items.join(",")))
        }
        JsonValue::Object(entries) => {
            let items = entries
                .iter()
                .map(render_json_entry)
                .collect::<io::Result<Vec<_>>>()?;
            Ok(format!("{{{}}}", items.join(",")))
        }
    }
}

fn render_json_double(value: &str) -> io::Result<String> {
    let mut rendered = render_float(64, value)?;
    if !rendered.contains(['.', 'e', 'E']) {
        rendered.push_str(".0");
    }
    Ok(rendered)
}

fn ensure_supported_change(change: &RowChange) -> io::Result<()> {
    let identity = change
        .after
        .as_ref()
        .or(change.before.as_ref())
        .expect("validated image");
    ensure_supported_image(identity)?;
    if let Some(before) = &change.before {
        ensure_supported_image(before)?;
    }
    if let Some(after) = &change.after {
        ensure_supported_image(after)?;
    }
    Ok(())
}
fn ensure_supported_image(image: &[ColumnDatum]) -> io::Result<()> {
    if !image
        .iter()
        .any(|column| column.primary_key_ordinal.is_some())
    {
        return Err(capability_failure("row has no primary key"));
    }
    for column in image.iter().filter(|column| !column.generated) {
        if let Datum::Value(value) = &column.datum {
            ensure_source_value_type(&column.native_type, value, column.primary_key_ordinal)?;
            bind_logical_value(value)
                .map_err(|error| capability_failure(format!("column {}: {error}", column.name)))?;
        }
    }
    Ok(())
}

fn ensure_source_value_type(
    native_type: &str,
    value: &LogicalValue,
    key: Option<usize>,
) -> io::Result<()> {
    let native_type = native_type.trim().to_ascii_lowercase();
    match value {
        LogicalValue::Boolean { .. } if native_type != "boolean" => Err(capability_failure(
            "Boolean value does not match its source type",
        )),
        LogicalValue::Uuid { .. } if native_type != "uuid" => Err(capability_failure(
            "Uuid value does not match its source type",
        )),
        LogicalValue::Boolean { .. } | LogicalValue::Uuid { .. } if key.is_some() => Err(
            capability_failure("Boolean and Uuid conversions cannot be used for a key"),
        ),
        _ => Ok(()),
    }
}

fn validate_uuid(value: &str) -> io::Result<()> {
    if value.len() != 36
        || !value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(byte == b'-')
                .unwrap_or(byte.is_ascii_hexdigit())
        })
    {
        return Err(capability_failure("Uuid value is not in canonical form"));
    }
    Ok(())
}

fn capability_failure(message: impl std::fmt::Display) -> io::Error {
    io::Error::other(TargetCapabilityFailure::new(format!(
        "Target Capability Failure: {message}"
    )))
}

fn render_json_entry(entry: &JsonEntry) -> io::Result<String> {
    let key = serde_json::to_string(&entry.key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(format!("{key}:{}", render_json(&entry.value)?))
}

fn sanitize_comment(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

/// Snapshot SQL carries no invented source transaction ID or binlog event cursor.
pub struct SnapshotSql {
    statements: Vec<String>,
    parameters: Vec<Vec<Value>>,
}
impl SnapshotSql {
    pub fn statements(&self) -> impl Iterator<Item = &str> {
        self.statements.iter().map(String::as_str)
    }
    pub fn parameters(&self) -> impl Iterator<Item = &[Value]> {
        self.parameters.iter().map(Vec::as_slice)
    }
}
pub fn snapshot_sql(validated: &change_event::ValidatedSnapshotBatch) -> io::Result<SnapshotSql> {
    let batch = validated.batch();
    let table = qualified_table(&batch.schema, &batch.table);
    if batch.schema.eq_ignore_ascii_case("CDC") {
        return Err(capability_failure("CDC is reserved"));
    }
    let mut statements = Vec::new();
    let mut parameters = Vec::new();
    for row in &batch.rows {
        ensure_supported_image(row)?;
        let writable = writable_columns(row);
        if writable.is_empty() {
            return Err(capability_failure("snapshot has no writable columns"));
        }
        statements.push(format!(
            "INSERT INTO {table} ({}) VALUES ({});",
            column_list(&writable),
            placeholders(writable.len())
        ));
        parameters.push(bind_columns(&writable)?);
    }
    Ok(SnapshotSql {
        statements,
        parameters,
    })
}
pub(crate) fn execute_snapshot(
    tx: &mut mysql_driver::Transaction<'_>,
    plan: &SnapshotSql,
) -> io::Result<()> {
    for (statement, parameters) in plan.statements.iter().zip(&plan.parameters) {
        tx.exec_drop(statement, parameters.clone())
            .map_err(io::Error::other)?;
        if tx.warnings() != 0 || tx.affected_rows() != 1 {
            return Err(io::Error::other(
                "snapshot INSERT warnings or unexpected affected rows; rolling back",
            ));
        }
    }
    Ok(())
}
/// Lock and recheck inside the final write transaction as well as before source locking.
pub(crate) fn snapshot_targets(
    conn: &mut impl Queryable,
    tables: &[change_event::SnapshotTable],
    lock: bool,
) -> io::Result<()> {
    if tables.is_empty() {
        return Err(capability_failure("snapshot scope is empty"));
    }
    for table in tables {
        if table.schema.eq_ignore_ascii_case("CDC") {
            return Err(capability_failure("CDC is reserved"));
        }
        let name = qualified_table(&table.schema, &table.table);
        let present: Option<u8> = conn
            .query_first(format!(
                "SELECT 1 FROM {name} LIMIT 1{}",
                if lock { " FOR UPDATE" } else { "" }
            ))
            .map_err(io::Error::other)?;
        if present.is_some() {
            return Err(io::Error::other(format!(
                "全量同步要求目的表为空：{name}；未删除任何已有数据"
            )));
        }
        let engine: Option<String> = conn.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (&table.schema, &table.table)).map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(capability_failure("snapshot targets require InnoDB"));
        }
        let triggers: u64 = conn.exec_first("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?", (&table.schema, &table.table)).map_err(io::Error::other)?.unwrap_or(0);
        let foreign_keys: u64 = conn.exec_first("SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE (TABLE_SCHEMA=? AND TABLE_NAME=? AND REFERENCED_TABLE_NAME IS NOT NULL) OR (REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?)", (&table.schema, &table.table, &table.schema, &table.table)).map_err(io::Error::other)?.unwrap_or(0);
        if triggers != 0 || foreign_keys != 0 {
            return Err(capability_failure(format!(
                "{name}: 第一版全量同步暂不支持目的端触发器或外键"
            )));
        }
    }
    Ok(())
}
