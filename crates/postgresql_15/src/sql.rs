use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use change_event::{
    BitOrder, CapabilityManifest, ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue,
    Operation, RowChange, SourceCursor, TargetCapabilityFailure, ValidatedTransaction,
};
use sqlx::{
    Connection, PgConnection, Postgres, Row,
    postgres::{PgArguments, PgConnectOptions, PgSslMode},
    query::Query,
};
use std::{collections::BTreeSet, io};

const TARGET_VERSION: &str = "15";

pub const CAPABILITY_MANIFEST: CapabilityManifest = CapabilityManifest {
    connector: "postgresql_15",
    target: "postgresql-15",
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
        "instant",
        "duration",
        "year",
        "json",
        "enum",
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

/// A lossless value prepared for SQLx's PostgreSQL encoder.
#[derive(Debug, Clone, PartialEq)]
pub enum Parameter {
    Integer(i64),
    Numeric(String),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Text(String),
    Binary(Vec<u8>),
    Date(String),
    Timestamp(String),
    Timestamptz(String),
    Interval(String),
    Uuid(String),
    Json(String),
}

#[derive(Debug, Clone)]
struct PreparedQuery {
    sql: String,
    parameters: Vec<Parameter>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedStatement {
    sql: String,
    diagnostic_sql: String,
    parameters: Vec<Parameter>,
    probe: Option<PreparedQuery>,
    verification: Option<PreparedQuery>,
}

#[derive(Debug, Clone)]
pub struct SqlTransaction {
    pub(crate) source_uuid: String,
    pub(crate) source_transaction_id: String,
    pub(crate) commit_cursor: SourceCursor,
    pub(crate) tables: Vec<(String, String)>,
    pub(crate) statements: Vec<PlannedStatement>,
}

impl SqlTransaction {
    pub fn source_transaction_id(&self) -> &str {
        &self.source_transaction_id
    }

    /// Parameterized statements are the execution contract.
    pub fn statements(&self) -> impl Iterator<Item = &str> {
        self.statements
            .iter()
            .map(|statement| statement.sql.as_str())
    }

    pub fn parameters(&self) -> impl Iterator<Item = &[Parameter]> {
        self.statements
            .iter()
            .map(|statement| statement.parameters.as_slice())
    }

    /// Diagnostic output may inline values for human inspection; it is never executed.
    pub fn script(&self) -> String {
        let mut output = format!(
            "-- cdc transaction={} target=postgresql-{TARGET_VERSION}\nBEGIN;\n",
            sanitize_comment(&self.source_transaction_id)
        );
        for statement in &self.statements {
            output.push_str(&statement.diagnostic_sql);
            output.push('\n');
        }
        output.push_str("COMMIT;\n");
        output
    }
}

#[derive(Debug, Clone)]
struct RenderedDatum {
    sql: String,
    diagnostic_sql: String,
}

#[derive(Debug, Default)]
struct StatementBuilder {
    parameters: Vec<Parameter>,
}

impl StatementBuilder {
    fn datum(&mut self, datum: &Datum) -> io::Result<RenderedDatum> {
        match datum {
            Datum::Unavailable | Datum::Unchanged => Err(capability_failure(
                "unavailable or unchanged values cannot be written in this position",
            )),
            Datum::Null => Ok(RenderedDatum {
                sql: "NULL".into(),
                diagnostic_sql: "NULL".into(),
            }),
            Datum::Value(value) => {
                let parameter = parameter_for(value)?;
                let diagnostic_sql = render_logical_value(value).map_err(capability_failure)?;
                self.parameters.push(parameter.clone());
                let index = self.parameters.len();
                Ok(RenderedDatum {
                    sql: parameter_expression(index, &parameter),
                    diagnostic_sql,
                })
            }
        }
    }

    fn finish(self) -> Vec<Parameter> {
        self.parameters
    }
}

struct RenderedStatement {
    sql: String,
    diagnostic_sql: String,
    parameters: Vec<Parameter>,
}

pub fn sql(validated: &ValidatedTransaction) -> io::Result<SqlTransaction> {
    let transaction = validated.transaction();
    let mut statements = Vec::with_capacity(transaction.changes.len());
    for change in &transaction.changes {
        ensure_supported_change(change)?;
        if change.schema.eq_ignore_ascii_case("cdc") {
            return Err(capability_failure(
                "cdc schema is reserved for replication control data",
            ));
        }
        let rendered = render_change(change)?;
        let probe = render_probe(change)?;
        let verification = render_verification(change)?;
        statements.push(PlannedStatement {
            sql: rendered.sql,
            diagnostic_sql: rendered.diagnostic_sql,
            parameters: rendered.parameters,
            probe,
            verification,
        });
    }
    Ok(SqlTransaction {
        source_uuid: transaction.source.id.clone(),
        source_transaction_id: transaction.id.clone(),
        commit_cursor: transaction.commit_cursor.clone(),
        tables: transaction
            .changes
            .iter()
            .map(|change| (change.schema.clone(), change.table.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        statements,
    })
}

fn render_change(change: &RowChange) -> io::Result<RenderedStatement> {
    let table = qualified_table(&change.schema, &change.table);
    match change.operation {
        Operation::Insert => render_insert(&table, change),
        Operation::Update => render_update(&table, change),
        Operation::Delete => render_delete(&table, change),
    }
}

fn render_insert(table: &str, change: &RowChange) -> io::Result<RenderedStatement> {
    let after = change
        .after
        .as_ref()
        .ok_or_else(|| capability_failure("INSERT ChangeEvent is missing its after image"))?;
    let columns = writable_columns(after);
    if columns.is_empty() {
        return Err(capability_failure("INSERT has no writable columns"));
    }
    let mut builder = StatementBuilder::default();
    let values = columns
        .iter()
        .map(|column| builder.datum(&column.datum))
        .collect::<io::Result<Vec<_>>>()?;
    let parameters = builder.finish();
    Ok(RenderedStatement {
        sql: format!(
            "INSERT INTO {table} ({}) VALUES ({});",
            column_list(&columns),
            values
                .iter()
                .map(|value| value.sql.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        diagnostic_sql: format!(
            "INSERT INTO {table} ({}) VALUES ({});",
            column_list(&columns),
            values
                .iter()
                .map(|value| value.diagnostic_sql.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        parameters,
    })
}

fn render_update(table: &str, change: &RowChange) -> io::Result<RenderedStatement> {
    let before = change
        .before
        .as_ref()
        .ok_or_else(|| capability_failure("UPDATE ChangeEvent is missing its before image"))?;
    let after = change
        .after
        .as_ref()
        .ok_or_else(|| capability_failure("UPDATE ChangeEvent is missing its after image"))?;
    let columns = writable_columns(after);
    if columns.is_empty() {
        return Err(capability_failure(
            "UPDATE has no supplied writable columns",
        ));
    }
    let mut builder = StatementBuilder::default();
    let mut assignments = Vec::with_capacity(columns.len());
    let mut diagnostic_assignments = Vec::with_capacity(columns.len());
    for column in columns {
        let value = builder.datum(&column.datum)?;
        assignments.push(format!(
            "{} = {}",
            quote_identifier(&column.name),
            value.sql
        ));
        diagnostic_assignments.push(format!(
            "{} = {}",
            quote_identifier(&column.name),
            value.diagnostic_sql
        ));
    }
    let (predicates, diagnostic_predicates) = key_predicates(before, &mut builder)?;
    let parameters = builder.finish();
    Ok(RenderedStatement {
        sql: format!(
            "UPDATE {table} SET {} WHERE {};",
            assignments.join(", "),
            predicates
        ),
        diagnostic_sql: format!(
            "UPDATE {table} SET {} WHERE {};",
            diagnostic_assignments.join(", "),
            diagnostic_predicates
        ),
        parameters,
    })
}

fn render_delete(table: &str, change: &RowChange) -> io::Result<RenderedStatement> {
    let before = change
        .before
        .as_ref()
        .ok_or_else(|| capability_failure("DELETE ChangeEvent is missing its before image"))?;
    let mut builder = StatementBuilder::default();
    let (predicates, diagnostic_predicates) = key_predicates(before, &mut builder)?;
    let parameters = builder.finish();
    Ok(RenderedStatement {
        sql: format!("DELETE FROM {table} WHERE {predicates};"),
        diagnostic_sql: format!("DELETE FROM {table} WHERE {diagnostic_predicates};"),
        parameters,
    })
}

fn render_probe(change: &RowChange) -> io::Result<Option<PreparedQuery>> {
    if matches!(change.operation, Operation::Insert) {
        return Ok(None);
    }
    let before = change
        .before
        .as_ref()
        .ok_or_else(|| capability_failure("row locator probe is missing its before image"))?;
    let mut builder = StatementBuilder::default();
    let (predicates, _) = key_predicates(before, &mut builder)?;
    Ok(Some(PreparedQuery {
        sql: format!(
            "SELECT 1 FROM {} WHERE {predicates} LIMIT 2 FOR UPDATE",
            qualified_table(&change.schema, &change.table)
        ),
        parameters: builder.finish(),
    }))
}

fn render_verification(change: &RowChange) -> io::Result<Option<PreparedQuery>> {
    if matches!(change.operation, Operation::Delete) {
        return Ok(None);
    }
    let after = change.after.as_ref().ok_or_else(|| {
        capability_failure("generated-column verification is missing its after image")
    })?;
    let generated = after
        .iter()
        .filter(|column| column.generated)
        .collect::<Vec<_>>();
    if generated.is_empty() {
        return Ok(None);
    }
    let mut builder = StatementBuilder::default();
    let (keys, _) = key_predicates(after, &mut builder)?;
    let mut predicates = vec![keys];
    for column in generated {
        let name = quote_identifier(&column.name);
        match &column.datum {
            Datum::Null => predicates.push(format!("{name} IS NULL")),
            Datum::Value(_) => {
                let value = builder.datum(&column.datum)?;
                predicates.push(format!("{name} IS NOT DISTINCT FROM {}", value.sql));
            }
            Datum::Unavailable | Datum::Unchanged => {
                return Err(capability_failure(format!(
                    "generated column {} has no captured observation",
                    column.name
                )));
            }
        }
    }
    Ok(Some(PreparedQuery {
        sql: format!(
            "SELECT 1 FROM {} WHERE {} LIMIT 2 FOR UPDATE",
            qualified_table(&change.schema, &change.table),
            predicates.join(" AND ")
        ),
        parameters: builder.finish(),
    }))
}

fn key_predicates(
    row: &[ColumnDatum],
    builder: &mut StatementBuilder,
) -> io::Result<(String, String)> {
    let mut keys = row
        .iter()
        .filter(|column| column.primary_key_ordinal.is_some())
        .collect::<Vec<_>>();
    keys.sort_by_key(|column| column.primary_key_ordinal);
    if keys.is_empty() {
        return Err(capability_failure(
            "table has no primary key; first PostgreSQL Sink version requires one",
        ));
    }
    let mut predicates = Vec::with_capacity(keys.len());
    let mut diagnostic_predicates = Vec::with_capacity(keys.len());
    for column in keys {
        let name = quote_identifier(&column.name);
        match &column.datum {
            Datum::Null => {
                predicates.push(format!("{name} IS NULL"));
                diagnostic_predicates.push(format!("{name} IS NULL"));
            }
            Datum::Unavailable | Datum::Unchanged => {
                return Err(capability_failure(
                    "cannot use an absent value as a row locator",
                ));
            }
            Datum::Value(_) => {
                let value = builder.datum(&column.datum)?;
                predicates.push(format!("{name} = {}", value.sql));
                diagnostic_predicates.push(format!("{name} = {}", value.diagnostic_sql));
            }
        }
    }
    Ok((
        predicates.join(" AND "),
        diagnostic_predicates.join(" AND "),
    ))
}

fn writable_columns(row: &[ColumnDatum]) -> Vec<&ColumnDatum> {
    row.iter()
        .filter(|column| {
            !column.generated && !matches!(column.datum, Datum::Unavailable | Datum::Unchanged)
        })
        .collect()
}

fn column_list(columns: &[&ColumnDatum]) -> String {
    columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ensure_supported_change(change: &RowChange) -> io::Result<()> {
    let identity = change
        .after
        .as_ref()
        .or(change.before.as_ref())
        .ok_or_else(|| capability_failure("row change image is missing"))?;
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
    if image.is_empty() {
        return Err(capability_failure("row image has no columns"));
    }
    if !image
        .iter()
        .any(|column| column.primary_key_ordinal.is_some())
    {
        return Err(capability_failure("row has no primary key"));
    }
    for column in image {
        if column.generated && matches!(column.datum, Datum::Unavailable | Datum::Unchanged) {
            return Err(capability_failure(format!(
                "generated column {} has no captured observation",
                column.name
            )));
        }
        if let Datum::Value(value) = &column.datum {
            parameter_for(value)
                .map_err(|error| capability_failure(format!("column {}: {error}", column.name)))?;
        }
    }
    Ok(())
}

fn parameter_for(value: &LogicalValue) -> io::Result<Parameter> {
    match value {
        LogicalValue::Boolean { value } => Ok(Parameter::Boolean(*value)),
        LogicalValue::Uuid { value } => Ok(Parameter::Uuid(value.clone())),
        LogicalValue::Integer { signed, value, .. } => {
            if *signed {
                value
                    .parse::<i64>()
                    .map(Parameter::Integer)
                    .map_err(|_| capability_failure("signed integer is outside PostgreSQL range"))
            } else {
                let integer = value
                    .parse::<u64>()
                    .map_err(|_| capability_failure("unsigned integer is invalid"))?;
                if integer <= i64::MAX as u64 {
                    Ok(Parameter::Integer(integer as i64))
                } else {
                    Ok(Parameter::Numeric(value.clone()))
                }
            }
        }
        LogicalValue::Decimal { unscaled, scale } => Ok(Parameter::Numeric(
            render_decimal(unscaled, *scale).map_err(capability_failure)?,
        )),
        LogicalValue::Float { bits, ieee754_hex } => match bits {
            32 => Ok(Parameter::Float32(f32::from_bits(
                parse_hex_bits(ieee754_hex, 8)? as u32,
            ))),
            64 => Ok(Parameter::Float64(f64::from_bits(parse_hex_bits(
                ieee754_hex,
                16,
            )?))),
            _ => Err(capability_failure("unsupported floating point width")),
        },
        LogicalValue::Text {
            charset,
            bytes_base64url,
            ..
        } => {
            if !matches!(charset.to_ascii_lowercase().as_str(), "utf8" | "utf8mb4") {
                return Err(capability_failure(format!(
                    "text charset {charset} has no qualified PostgreSQL mapping"
                )));
            }
            let bytes = decode_bytes(bytes_base64url)?;
            let text = String::from_utf8(bytes)
                .map_err(|_| capability_failure("text value is not valid UTF-8"))?;
            Ok(Parameter::Text(text))
        }
        LogicalValue::Binary { bytes_base64url } => {
            Ok(Parameter::Binary(decode_bytes(bytes_base64url)?))
        }
        LogicalValue::BitString {
            bytes_base64url,
            bit_length,
            bit_order,
            ..
        } => Ok(Parameter::Text(bit_string_text(
            &decode_bytes(bytes_base64url)?,
            *bit_length,
            *bit_order,
        )?)),
        LogicalValue::Date { year, month, day } => {
            Ok(Parameter::Date(format!("{year:04}-{month:02}-{day:02}")))
        }
        LogicalValue::LocalDatetime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => Ok(Parameter::Timestamp(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{microsecond:06}"
        ))),
        LogicalValue::Duration {
            negative,
            hours,
            minutes,
            seconds,
            microsecond,
        } => Ok(Parameter::Interval(format!(
            "{}{hours}:{minutes:02}:{seconds:02}.{microsecond:06}",
            if *negative { "-" } else { "" }
        ))),
        LogicalValue::Instant {
            unix_seconds,
            nanoseconds,
        } => {
            if !nanoseconds.is_multiple_of(1_000) {
                return Err(capability_failure(
                    "PostgreSQL 15 timestamp precision stops at microseconds",
                ));
            }
            let seconds = unix_seconds
                .parse::<i64>()
                .map_err(|_| capability_failure("instant seconds are outside PostgreSQL range"))?;
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, *nanoseconds)
                .ok_or_else(|| capability_failure("instant outside PostgreSQL timestamp range"))?;
            Ok(Parameter::Timestamptz(format!(
                "{}.{:06}+00",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                timestamp.timestamp_subsec_micros()
            )))
        }
        LogicalValue::Year { value } => Ok(Parameter::Integer(i64::from(*value))),
        LogicalValue::Enum { label } => Ok(Parameter::Text(label.clone())),
        LogicalValue::Set { .. } => Err(capability_failure(
            "PostgreSQL 15 has no qualified SET target representation",
        )),
        LogicalValue::Spatial { .. }
        | LogicalValue::Array { .. }
        | LogicalValue::Struct { .. }
        | LogicalValue::Map { .. }
        | LogicalValue::Range { .. }
        | LogicalValue::MultiRange { .. } => Err(capability_failure(
            "PostgreSQL 15 has no qualified target representation for this structured value",
        )),
        LogicalValue::Json { value } => Ok(Parameter::Json(
            render_json(value).map_err(capability_failure)?,
        )),
    }
}

fn parameter_expression(index: usize, parameter: &Parameter) -> String {
    let placeholder = format!("{}{}", "$", index);
    match parameter {
        Parameter::Numeric(_) => format!("CAST({placeholder} AS numeric)"),
        Parameter::Date(_) => format!("CAST({placeholder} AS date)"),
        Parameter::Timestamp(_) => format!("CAST({placeholder} AS timestamp)"),
        Parameter::Timestamptz(_) => format!("CAST({placeholder} AS timestamptz)"),
        Parameter::Interval(_) => format!("CAST({placeholder} AS interval)"),
        Parameter::Uuid(_) => format!("CAST({placeholder} AS uuid)"),
        Parameter::Json(_) => format!("CAST({placeholder} AS jsonb)"),
        Parameter::Integer(_)
        | Parameter::Float32(_)
        | Parameter::Float64(_)
        | Parameter::Boolean(_)
        | Parameter::Text(_)
        | Parameter::Binary(_) => placeholder,
    }
}

fn parse_hex_bits(value: &str, width: usize) -> io::Result<u64> {
    if value.len() != width || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(capability_failure("invalid IEEE-754 bit pattern"));
    }
    u64::from_str_radix(value, 16).map_err(|_| capability_failure("invalid IEEE-754 bit pattern"))
}

fn decode_bytes(value: &str) -> io::Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|error| capability_failure(format!("invalid base64url bytes: {error}")))
}

fn render_decimal(unscaled: &str, scale: usize) -> io::Result<String> {
    let negative = unscaled.starts_with('-');
    let digits = unscaled.strip_prefix(['-', '+']).unwrap_or(unscaled);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(capability_failure("invalid decimal"));
    }
    let magnitude = if scale == 0 {
        digits.to_owned()
    } else if digits.len() > scale {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    Ok(format!("{}{}", if negative { "-" } else { "" }, magnitude))
}

fn render_logical_value(value: &LogicalValue) -> io::Result<String> {
    match value {
        LogicalValue::Boolean { value } => Ok(if *value { "TRUE" } else { "FALSE" }.into()),
        LogicalValue::Uuid { value } => Ok(format!("UUID {}", quote_literal(value))),
        LogicalValue::Integer { value, .. } => Ok(value.clone()),
        LogicalValue::Decimal { unscaled, scale } => render_decimal(unscaled, *scale),
        LogicalValue::Float { bits, ieee754_hex } => render_float(*bits, ieee754_hex),
        LogicalValue::Text {
            charset,
            bytes_base64url,
            ..
        } if charset.eq_ignore_ascii_case("utf8") || charset.eq_ignore_ascii_case("utf8mb4") => {
            let bytes = URL_SAFE_NO_PAD
                .decode(bytes_base64url)
                .map_err(capability_failure)?;
            Ok(format!(
                "convert_from(decode('{}', 'hex'), 'UTF8')",
                hex(&bytes)
            ))
        }
        LogicalValue::Text { charset, .. } => Err(capability_failure(format!(
            "PostgreSQL 15 Sink has no qualified text conversion for charset {charset}"
        ))),
        LogicalValue::Binary { bytes_base64url } => {
            let bytes = URL_SAFE_NO_PAD
                .decode(bytes_base64url)
                .map_err(capability_failure)?;
            Ok(format!("decode('{}', 'hex')", hex(&bytes)))
        }
        LogicalValue::BitString {
            bytes_base64url,
            bit_length,
            bit_order,
            ..
        } => Ok(format!(
            "B'{}'",
            bit_string_text(
                &URL_SAFE_NO_PAD
                    .decode(bytes_base64url)
                    .map_err(capability_failure)?,
                *bit_length,
                *bit_order,
            )?
        )),
        LogicalValue::Date { year, month, day } => {
            Ok(format!("DATE '{year:04}-{month:02}-{day:02}'"))
        }
        LogicalValue::LocalDatetime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => Ok(format!(
            "TIMESTAMP '{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{microsecond:06}'"
        )),
        LogicalValue::Duration {
            negative,
            hours,
            minutes,
            seconds,
            microsecond,
        } => Ok(format!(
            "({}({hours} * INTERVAL '1 hour' + {minutes} * INTERVAL '1 minute' + {seconds} * INTERVAL '1 second' + {microsecond} * INTERVAL '1 microsecond'))",
            if *negative { "-1 * " } else { "" }
        )),
        LogicalValue::Instant {
            unix_seconds,
            nanoseconds,
        } => {
            if nanoseconds % 1_000 != 0 {
                return Err(capability_failure(
                    "PostgreSQL 15 timestamp precision stops at microseconds",
                ));
            }
            Ok(format!(
                "(TIMESTAMPTZ 'epoch' + {unix_seconds} * INTERVAL '1 second' + {} * INTERVAL '1 microsecond')",
                nanoseconds / 1_000
            ))
        }
        LogicalValue::Year { value } => Ok(value.to_string()),
        LogicalValue::Enum { label } => Ok(quote_literal(label)),
        LogicalValue::Set { .. } => Err(capability_failure(
            "PostgreSQL 15 has no qualified SET target representation",
        )),
        LogicalValue::Spatial { .. }
        | LogicalValue::Array { .. }
        | LogicalValue::Struct { .. }
        | LogicalValue::Map { .. }
        | LogicalValue::Range { .. }
        | LogicalValue::MultiRange { .. } => Err(capability_failure(
            "PostgreSQL 15 has no qualified SQL rendering for this structured value",
        )),
        LogicalValue::Json { value } => {
            Ok(format!("{}::jsonb", quote_literal(&render_json(value)?)))
        }
    }
}

fn bit_string_text(bytes: &[u8], bit_length: u64, bit_order: BitOrder) -> io::Result<String> {
    let bit_length = usize::try_from(bit_length)
        .map_err(|_| capability_failure("bit string length is too large"))?;
    if bit_length == 0 || bit_length > bytes.len().saturating_mul(8) {
        return Err(capability_failure(
            "bit string length exceeds its raw byte payload",
        ));
    }
    let mut text = String::with_capacity(bit_length);
    for position in 0..bit_length {
        let byte = bytes[position / 8];
        let offset = position % 8;
        let mask = match bit_order {
            BitOrder::MsbFirst => 1_u8 << (7 - offset),
            BitOrder::LsbFirst => 1_u8 << offset,
        };
        text.push(if byte & mask == 0 { '0' } else { '1' });
    }
    Ok(text)
}

fn render_float(bits: u8, hexadecimal: &str) -> io::Result<String> {
    match bits {
        32 => {
            let raw = u32::from_str_radix(hexadecimal, 16).map_err(capability_failure)?;
            let value = f32::from_bits(raw);
            if value == 0.0 && value.is_sign_negative() {
                Ok("-0.0".into())
            } else {
                render_float_text(value.to_string())
            }
        }
        64 => {
            let raw = u64::from_str_radix(hexadecimal, 16).map_err(capability_failure)?;
            let value = f64::from_bits(raw);
            if value == 0.0 && value.is_sign_negative() {
                Ok("-0.0".into())
            } else {
                render_float_text(value.to_string())
            }
        }
        _ => Err(capability_failure("unsupported floating point width")),
    }
}

fn render_float_text(value: String) -> io::Result<String> {
    match value.as_str() {
        "NaN" => Ok("'NaN'".into()),
        "inf" => Ok("'Infinity'".into()),
        "-inf" => Ok("'-Infinity'".into()),
        _ => Ok(value),
    }
}

fn render_json(value: &JsonValue) -> io::Result<String> {
    match value {
        JsonValue::Null => Ok("null".into()),
        JsonValue::Boolean(value) => Ok(value.to_string()),
        JsonValue::String(value) => serde_json::to_string(value).map_err(capability_failure),
        JsonValue::SignedInteger(value) | JsonValue::UnsignedInteger(value) => Ok(value.clone()),
        JsonValue::DoubleBits(value) => {
            let rendered = render_float(64, value)?;
            if rendered.starts_with('\'') {
                return Err(capability_failure(
                    "JSON does not support non-finite numbers",
                ));
            }
            Ok(rendered)
        }
        JsonValue::Decimal { unscaled, scale } => render_decimal(unscaled, *scale),
        JsonValue::Array(values) => values
            .iter()
            .map(render_json)
            .collect::<io::Result<Vec<_>>>()
            .map(|values| format!("[{}]", values.join(","))),
        JsonValue::Object(entries) => entries
            .iter()
            .map(render_json_entry)
            .collect::<io::Result<Vec<_>>>()
            .map(|entries| format!("{{{}}}", entries.join(","))),
    }
}

fn render_json_entry(entry: &JsonEntry) -> io::Result<String> {
    Ok(format!(
        "{}:{}",
        serde_json::to_string(&entry.key).map_err(capability_failure)?,
        render_json(&entry.value)?
    ))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn capability_failure(message: impl std::fmt::Display) -> io::Error {
    io::Error::other(TargetCapabilityFailure::new(format!(
        "Target Capability Failure: {message}"
    )))
}

fn sanitize_comment(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

#[derive(Clone)]
pub struct TargetConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl std::fmt::Debug for TargetConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TargetConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl TargetConfig {
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 5432,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        }
    }

    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
}

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

/// PostgreSQL SQLSTATEs 40001 (serialization failure), 40P01 (deadlock) and
/// 55P03 (lock not available) are transaction-local failures. The complete
/// source transaction can be replayed after the failed transaction is gone.
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
        .and_then(|cause| cause.downcast_ref::<sqlx::Error>())
    {
        if let sqlx::Error::Database(database) = cause {
            let code = database.code();
            if matches!(code.as_deref(), Some("40001" | "40P01" | "55P03")) {
                return change_event::TargetApplyErrorKind::LockTimeout;
            }
            if code.as_deref().is_some_and(|code| code.starts_with("23")) {
                return change_event::TargetApplyErrorKind::Constraint;
            }
            return change_event::TargetApplyErrorKind::Sql;
        }
        return change_event::TargetApplyErrorKind::Connection;
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

pub(crate) fn commit_error(error: sqlx::Error) -> io::Error {
    if matches!(&error, sqlx::Error::Database(_)) {
        io::Error::other(error)
    } else {
        io::Error::other(CommitUnknown(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResult {
    pub source_transaction_id: String,
    pub statements_executed: usize,
}

pub async fn execute(config: &TargetConfig, plan: &SqlTransaction) -> io::Result<ApplyResult> {
    let mut connection = connect(config).await?;
    let mut transaction = connection.begin().await.map_err(io::Error::other)?;
    execute_statements(&mut transaction, plan).await?;
    transaction.commit().await.map_err(commit_error)?;
    Ok(ApplyResult {
        source_transaction_id: plan.source_transaction_id.clone(),
        statements_executed: plan.statements.len(),
    })
}

pub(crate) async fn connect(config: &TargetConfig) -> io::Result<PgConnection> {
    let options = PgConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .database(&config.database)
        .username(&config.user)
        .password(&config.password)
        .ssl_mode(PgSslMode::Prefer)
        .statement_cache_capacity(0);
    let mut connection = PgConnection::connect_with(&options)
        .await
        .map_err(io::Error::other)?;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&mut connection)
        .await
        .map_err(io::Error::other)?;
    if !version
        .split('.')
        .next()
        .is_some_and(|major| major == TARGET_VERSION)
    {
        return Err(capability_failure(format!(
            "postgresql_{TARGET_VERSION} cannot write target version {version}"
        )));
    }
    sqlx::query("SET TIME ZONE 'UTC'")
        .execute(&mut connection)
        .await
        .map_err(io::Error::other)?;
    sqlx::query("SET standard_conforming_strings = on")
        .execute(&mut connection)
        .await
        .map_err(io::Error::other)?;
    Ok(connection)
}

pub(crate) async fn execute_statements(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    plan: &SqlTransaction,
) -> io::Result<()> {
    check_tables(transaction, &plan.tables).await?;
    for statement in &plan.statements {
        if let Some(probe) = &statement.probe {
            let rows = bind_query(&probe.sql, &probe.parameters)
                .fetch_all(&mut **transaction)
                .await
                .map_err(io::Error::other)?;
            if rows.len() != 1 {
                return Err(io::Error::other(format!(
                    "row locator matched {} rows; transaction rolled back",
                    rows.len()
                )));
            }
        }
        let result = bind_query(&statement.sql, &statement.parameters)
            .execute(&mut **transaction)
            .await
            .map_err(io::Error::other)?;
        if result.rows_affected() != 1 {
            return Err(io::Error::other(format!(
                "unexpected affected row count {}; transaction rolled back",
                result.rows_affected()
            )));
        }
        if let Some(verification) = &statement.verification {
            let rows = bind_query(&verification.sql, &verification.parameters)
                .fetch_all(&mut **transaction)
                .await
                .map_err(io::Error::other)?;
            if rows.len() != 1 {
                return Err(io::Error::other(
                    "generated-column observation disagrees with target; transaction rolled back",
                ));
            }
        }
    }
    Ok(())
}

fn bind_query(sql: &str, parameters: &[Parameter]) -> Query<'static, Postgres, PgArguments> {
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()));
    for parameter in parameters {
        query = match parameter {
            Parameter::Integer(value) => query.bind(*value),
            Parameter::Numeric(value)
            | Parameter::Text(value)
            | Parameter::Date(value)
            | Parameter::Timestamp(value)
            | Parameter::Timestamptz(value)
            | Parameter::Interval(value)
            | Parameter::Uuid(value)
            | Parameter::Json(value) => query.bind(value.clone()),
            Parameter::Float32(value) => query.bind(*value),
            Parameter::Float64(value) => query.bind(*value),
            Parameter::Boolean(value) => query.bind(*value),
            Parameter::Binary(value) => query.bind(value.clone()),
        };
    }
    query
}

async fn check_tables(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    tables: &[(String, String)],
) -> io::Result<()> {
    for (schema, table) in tables {
        if schema.eq_ignore_ascii_case("cdc") {
            return Err(capability_failure(
                "cdc schema is reserved for replication control data",
            ));
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE {} IN ROW EXCLUSIVE MODE",
            qualified_table(schema, table)
        )))
        .execute(&mut **transaction)
        .await
        .map_err(io::Error::other)?;

        let relation = sqlx::query(
            "SELECT c.relkind::text AS relkind, c.relpersistence::text AS relpersistence, \
             c.relrowsecurity, c.relforcerowsecurity \
             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname=$1 AND c.relname=$2",
        )
        .bind(schema)
        .bind(table)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(io::Error::other)?;
        let Some(relation) = relation else {
            return Err(capability_failure(format!(
                "target table {schema}.{table} does not exist"
            )));
        };
        let relkind: String = relation.try_get("relkind").map_err(io::Error::other)?;
        let relpersistence: String = relation
            .try_get("relpersistence")
            .map_err(io::Error::other)?;
        let row_security: bool = relation
            .try_get("relrowsecurity")
            .map_err(io::Error::other)?;
        let force_row_security: bool = relation
            .try_get("relforcerowsecurity")
            .map_err(io::Error::other)?;
        if relkind != "r" || relpersistence != "p" {
            return Err(capability_failure(format!(
                "target {schema}.{table} must be an ordinary permanent table"
            )));
        }
        if row_security || force_row_security {
            return Err(capability_failure(format!(
                "target {schema}.{table} uses row-level security"
            )));
        }
        let trigger_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_trigger t \
             JOIN pg_class c ON c.oid=t.tgrelid \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname=$1 AND c.relname=$2 AND NOT t.tgisinternal AND t.tgenabled <> 'D'",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&mut **transaction)
        .await
        .map_err(io::Error::other)?;
        if trigger_count != 0 {
            return Err(capability_failure(format!(
                "target {schema}.{table} has user triggers"
            )));
        }
    }
    Ok(())
}

/// Snapshot SQL carries no invented source transaction ID or source cursor.
pub struct SnapshotSql {
    statements: Vec<String>,
    parameters: Vec<Vec<Parameter>>,
}

impl SnapshotSql {
    pub fn statements(&self) -> impl Iterator<Item = &str> {
        self.statements.iter().map(String::as_str)
    }

    pub fn parameters(&self) -> impl Iterator<Item = &[Parameter]> {
        self.parameters.iter().map(Vec::as_slice)
    }
}

pub fn snapshot_sql(validated: &change_event::ValidatedSnapshotBatch) -> io::Result<SnapshotSql> {
    let batch = validated.batch();
    if batch.schema.eq_ignore_ascii_case("cdc") {
        return Err(capability_failure(
            "cdc schema is reserved for replication control data",
        ));
    }
    let table = qualified_table(&batch.schema, &batch.table);
    let mut statements = Vec::with_capacity(batch.rows.len());
    let mut parameters = Vec::with_capacity(batch.rows.len());
    for row in &batch.rows {
        ensure_supported_image(row)?;
        let writable = writable_columns(row);
        if writable.is_empty() {
            return Err(capability_failure("snapshot has no writable columns"));
        }
        let mut builder = StatementBuilder::default();
        let values = writable
            .iter()
            .map(|column| builder.datum(&column.datum))
            .collect::<io::Result<Vec<_>>>()?;
        parameters.push(builder.finish());
        statements.push(format!(
            "INSERT INTO {table} ({}) VALUES ({});",
            column_list(&writable),
            values
                .iter()
                .map(|value| value.sql.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(SnapshotSql {
        statements,
        parameters,
    })
}

pub(crate) async fn execute_snapshot(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    plan: &SnapshotSql,
) -> io::Result<()> {
    for (statement, parameters) in plan.statements.iter().zip(&plan.parameters) {
        let result = bind_query(statement, parameters)
            .execute(&mut **transaction)
            .await
            .map_err(io::Error::other)?;
        if result.rows_affected() != 1 {
            return Err(io::Error::other(
                "snapshot INSERT affected an unexpected number of rows; rolling back",
            ));
        }
    }
    Ok(())
}

/// Lock and recheck snapshot targets inside the transaction that performs the copy.
pub(crate) async fn snapshot_targets(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    tables: &[change_event::SnapshotTable],
    lock: bool,
) -> io::Result<()> {
    if tables.is_empty() {
        return Err(capability_failure("snapshot scope is empty"));
    }
    let scopes = tables
        .iter()
        .map(|table| (table.schema.clone(), table.table.clone()))
        .collect::<Vec<_>>();
    check_tables(transaction, &scopes).await?;
    for table in tables {
        let query = format!(
            "SELECT 1 FROM {} LIMIT 1{}",
            qualified_table(&table.schema, &table.table),
            if lock { " FOR UPDATE" } else { "" }
        );
        let present = sqlx::query(sqlx::AssertSqlSafe(query))
            .fetch_optional(&mut **transaction)
            .await
            .map_err(io::Error::other)?;
        if present.is_some() {
            return Err(io::Error::other(format!(
                "全量同步要求目的表为空：{}.{}；未删除任何已有数据",
                table.schema, table.table
            )));
        }
        let foreign_keys: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_constraint con \
             JOIN pg_class c ON c.oid=con.conrelid \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE con.contype='f' AND n.nspname=$1 AND c.relname=$2",
        )
        .bind(&table.schema)
        .bind(&table.table)
        .fetch_one(&mut **transaction)
        .await
        .map_err(io::Error::other)?;
        if foreign_keys != 0 {
            return Err(capability_failure(format!(
                "snapshot target {}.{} has foreign keys",
                table.schema, table.table
            )));
        }
    }
    Ok(())
}
