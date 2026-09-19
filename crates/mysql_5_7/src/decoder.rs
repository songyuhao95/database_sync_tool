use std::collections::HashMap;
use std::fmt::Write as _;
use std::io;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use mysql_driver::binlog::events::{Event, EventData, RowsEventData, TableMapEvent};
use mysql_driver::binlog::jsonb::{JsonContainer, JsonDom, JsonNumber, JsonScalar};
use mysql_driver::binlog::row::BinlogRow;
use mysql_driver::binlog::value::BinlogValue;
use mysql_driver::prelude::Queryable;
use mysql_driver::{Conn, Value};

use crate::type_mapping::{source_type_mapping, validate_native_type};
use change_event::{
    BitOrder, BitPadding, ChangeTransaction, ColumnDatum, Datum, JsonEntry, JsonValue,
    LogicalValue, Operation, RowChange, Source, SourceCursor,
};

const MAX_TRANSACTION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct ColumnInfo {
    pub(crate) name: String,
    pub(crate) native_type: String,
    pub(crate) data_type: String,
    pub(crate) charset: Option<String>,
    pub(crate) collation: Option<String>,
    pub(crate) generated: bool,
    pub(crate) primary_key_ordinal: Option<usize>,
}

struct TableInfo {
    event: TableMapEvent<'static>,
    columns: Vec<ColumnInfo>,
}

pub(crate) struct Decoder {
    current_file: String,
    source: Source,
    tables: HashMap<u64, TableInfo>,
    metadata: Conn,
    pending: Option<PendingTransaction>,
    pub(crate) reject_statements: bool,
    pub(crate) capture_tables: Vec<(String, String)>,
}

impl Decoder {
    pub fn new(current_file: String, metadata: Conn, version: String, server_uuid: String) -> Self {
        Self {
            current_file,
            source: Source {
                kind: "mysql".to_owned(),
                version,
                id: server_uuid,
            },
            tables: HashMap::new(),
            metadata,
            pending: None,
            reject_statements: true,
            capture_tables: Vec::new(),
        }
    }

    pub(crate) fn decode(&mut self, event: &Event) -> io::Result<Option<ChangeTransaction>> {
        check_event_type(event.header().event_type_raw())?;
        let header = event.header();
        let event_cursor = self.cursor(header.log_pos());

        match event.read_data() {
            Ok(Some(EventData::RotateEvent(rotate))) => {
                if self.pending.as_ref().is_some() {
                    return Err(io::Error::other(
                        "binlog rotated with an uncommitted transaction in the buffer",
                    ));
                }
                self.pending = None;
                self.current_file = rotate.name().into_owned();
                self.tables.clear();
                Ok(None)
            }
            Ok(Some(EventData::TableMapEvent(table))) => self.store_table_map(table).map(|_| None),
            Ok(Some(EventData::GtidEvent(gtid))) => self
                .start_transaction(format_gtid(gtid.sid(), gtid.gno()), event_cursor)
                .map(|_| None),
            Ok(Some(EventData::AnonymousGtidEvent(_))) => self
                .start_transaction(format!("anonymous:{}", event_cursor.display), event_cursor)
                .map(|_| None),
            Ok(Some(EventData::QueryEvent(query))) => {
                self.handle_query(query.query().as_ref(), event_cursor)
            }
            Ok(Some(EventData::RowsEvent(rows))) => self
                .handle_rows(rows, event_cursor, header.timestamp())
                .map(|_| None),
            Ok(Some(EventData::XidEvent(_))) => self.finish_transaction(&event_cursor),
            Ok(Some(EventData::HeartbeatEvent))
            | Ok(Some(EventData::FormatDescriptionEvent(_)))
            | Ok(Some(EventData::PreviousGtidsEvent(_)))
            | Ok(Some(EventData::StopEvent)) => Ok(None),
            Ok(Some(EventData::RowsQueryEvent(_))) | Ok(Some(EventData::IgnorableEvent(_))) => {
                Ok(None)
            }
            Ok(Some(other)) => Err(io::Error::other(format!(
                "unsupported 5.7 binlog event: {other:?}"
            ))),
            Ok(None) => Err(io::Error::other("unknown binlog event; capture stopped")),
            Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }

    fn store_table_map(&mut self, table: TableMapEvent<'_>) -> io::Result<()> {
        let schema = table.database_name().into_owned();
        let name = table.table_name().into_owned();
        if !self.capture_tables.is_empty()
            && !self
                .capture_tables
                .iter()
                .any(|(s, t)| s == &schema && t == &name)
        {
            self.tables.insert(
                table.table_id(),
                TableInfo {
                    event: table.into_owned(),
                    columns: Vec::new(),
                },
            );
            return Ok(());
        }
        let columns = self.load_columns(&schema, &name)?;
        let expected = table.columns_count() as usize;
        if columns.len() != expected {
            return Err(io::Error::other(format!(
                "cannot construct ChangeEvent for {schema}.{name}: TABLE_MAP has {expected} columns but INFORMATION_SCHEMA returned {}",
                columns.len()
            )));
        }
        self.tables.insert(
            table.table_id(),
            TableInfo {
                event: table.into_owned(),
                columns,
            },
        );
        Ok(())
    }

    fn load_columns(&mut self, schema: &str, table: &str) -> io::Result<Vec<ColumnInfo>> {
        self.metadata
            .exec_map(
                "SELECT c.COLUMN_NAME, c.COLUMN_TYPE, c.DATA_TYPE, c.CHARACTER_SET_NAME,
                        c.COLLATION_NAME, c.GENERATION_EXPRESSION, k.ORDINAL_POSITION
                   FROM INFORMATION_SCHEMA.COLUMNS c
                   LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                     ON k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME
                    AND k.COLUMN_NAME = c.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'
                  WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                  ORDER BY c.ORDINAL_POSITION",
                (schema, table),
                |(name, native_type, data_type, charset, collation, expression, key): (
                    String,
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    String,
                    Option<u64>,
                )| ColumnInfo {
                    name,
                    native_type,
                    data_type,
                    charset,
                    collation,
                    generated: !expression.is_empty(),
                    primary_key_ordinal: key.map(|ordinal| ordinal as usize - 1),
                },
            )
            .map_err(io::Error::other)
    }

    fn start_transaction(&mut self, id: String, cursor: SourceCursor) -> io::Result<()> {
        if self.pending.as_ref().is_some() {
            return Err(io::Error::other(
                "a new GTID arrived before the previous transaction committed",
            ));
        }
        self.pending = Some(PendingTransaction::new(id, cursor, false));
        Ok(())
    }

    fn handle_query(
        &mut self,
        query: &str,
        cursor: SourceCursor,
    ) -> io::Result<Option<ChangeTransaction>> {
        let statement = query.trim().to_ascii_uppercase();
        if statement == "BEGIN" || statement.starts_with("BEGIN ") {
            if self.pending.is_none() {
                self.pending = Some(PendingTransaction::new(
                    format!("anonymous:{}", cursor.display),
                    cursor.clone(),
                    true,
                ));
            } else if let Some(transaction) = self.pending.as_mut() {
                if transaction.began {
                    return Err(io::Error::other("nested BEGIN in MySQL transaction"));
                }
                transaction.begin_cursor = cursor;
                transaction.began = true;
            }
            return Ok(None);
        }

        if statement == "COMMIT" {
            return self.finish_transaction(&cursor);
        }

        if statement == "ROLLBACK" {
            self.pending = None;
            return Ok(None);
        }

        // Our reserved control schema can be initialized by other Sink tasks.
        let control_ddl = statement == "CREATE DATABASE IF NOT EXISTS CDC"
            || statement.starts_with("CREATE TABLE IF NOT EXISTS CDC.LOG_INFO (")
            || statement
                == "ALTER TABLE CDC.LOG_INFO ADD COLUMN PHASE VARCHAR(16) NOT NULL DEFAULT 'INCREMENTAL'"
            || statement
                == "ALTER TABLE CDC.LOG_INFO ADD COLUMN SNAPSHOT_ROWS BIGINT UNSIGNED NOT NULL DEFAULT 0"
            || statement
                == "ALTER TABLE CDC.LOG_INFO ADD COLUMN SNAPSHOT_GTIDS LONGTEXT CHARACTER SET ASCII";
        if self.reject_statements && !control_ddl {
            return Err(io::Error::other(
                "发现 DDL 或语句型事件；当前任务仅支持行变更，已停止且未推进该事件位点",
            ));
        }
        if control_ddl && !self.capture_tables.is_empty() {
            if let Some(pending) = self.pending.as_mut() {
                pending.began = true;
            }
            return self.finish_transaction(&cursor);
        }
        // This is a DDL or a statement-based DML event. The row-only slice does
        // not invent a RowChange for it.
        if self
            .pending
            .as_ref()
            .is_some_and(|tx| tx.changes.is_empty())
        {
            self.pending = None;
        }
        Ok(None)
    }

    fn handle_rows(
        &mut self,
        rows: RowsEventData<'_>,
        cursor: SourceCursor,
        timestamp: u32,
    ) -> io::Result<()> {
        let table_id = rows.table_id();
        let Some(table) = self.tables.get(&table_id) else {
            return Err(io::Error::other(format!(
                "row event refers to unknown TABLE_MAP id {table_id}"
            )));
        };
        let operation = match &rows {
            _ if table.columns.is_empty() && !self.capture_tables.is_empty() => return Ok(()),
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                Operation::Insert
            }
            RowsEventData::UpdateRowsEvent(_)
            | RowsEventData::UpdateRowsEventV1(_)
            | RowsEventData::PartialUpdateRowsEvent(_) => Operation::Update,
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                Operation::Delete
            }
        };
        let schema = table.event.database_name().into_owned();
        let name = table.event.table_name().into_owned();
        let mut changes = Vec::new();
        for decoded in rows.rows(&table.event) {
            let (before, after) =
                decoded.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            changes.push(RowChange {
                database: None,
                operation,
                schema: schema.clone(),
                table: name.clone(),
                source_cursor: cursor.clone(),
                source_timestamp: timestamp,
                schema_basis: "information_schema_at_capture".into(),
                before: before
                    .map(|row| decode_row(&row, &table.columns))
                    .transpose()?,
                after: after
                    .map(|row| decode_row(&row, &table.columns))
                    .transpose()?,
            });
        }

        let Some(transaction) = self.pending.as_mut() else {
            return Err(io::Error::other(
                "row event arrived without a GTID/BEGIN; resume from a transaction boundary",
            ));
        };
        if !transaction.began {
            return Err(io::Error::other(
                "row event arrived before BEGIN; resume from a transaction boundary",
            ));
        }
        for change in changes {
            transaction.push(change, MAX_TRANSACTION_BYTES)?;
        }
        Ok(())
    }

    fn finish_transaction(
        &mut self,
        cursor: &SourceCursor,
    ) -> io::Result<Option<ChangeTransaction>> {
        let Some(transaction) = self.pending.take() else {
            return Ok(None);
        };
        if !transaction.began {
            return Err(io::Error::other("commit without BEGIN"));
        }
        if transaction.changes.is_empty() && self.capture_tables.is_empty() {
            return Ok(None);
        }
        Ok(Some(ChangeTransaction {
            source: self.source.clone(),
            id: transaction.id,
            begin_cursor: transaction.begin_cursor,
            commit_cursor: cursor.clone(),
            changes: transaction.changes,
        }))
    }

    pub(crate) fn ensure_complete(&self) -> io::Result<()> {
        if self.pending.as_ref().is_some() {
            return Err(io::Error::other(
                "stream ended inside a transaction; no partial transaction was emitted",
            ));
        }
        Ok(())
    }

    fn cursor(&self, position: u32) -> SourceCursor {
        let mut raw = Vec::with_capacity(self.current_file.len() + 1 + 4);
        raw.extend_from_slice(self.current_file.as_bytes());
        raw.push(0);
        raw.extend_from_slice(&position.to_be_bytes());
        SourceCursor {
            format: "mysql.binlog.file-position.v1".to_owned(),
            value: URL_SAFE_NO_PAD.encode(raw),
            display: format!("{}:{position}", self.current_file),
        }
    }
}

fn decode_row(row: &BinlogRow, columns: &[ColumnInfo]) -> io::Result<Vec<ColumnDatum>> {
    if row.len() != columns.len() {
        return Err(io::Error::other(format!(
            "row image is incomplete: decoded {} columns, expected {}; binlog_row_image must be FULL",
            row.len(),
            columns.len()
        )));
    }
    row.columns_ref()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let ordinal = column
                .name_str()
                .strip_prefix('@')
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(index);
            let info = columns.get(ordinal).ok_or_else(|| {
                io::Error::other(format!(
                    "row image column ordinal {ordinal} is out of range"
                ))
            })?;
            validate_native_type(&info.native_type).map_err(io::Error::other)?;
            let datum = match row.as_ref(index) {
                Some(BinlogValue::Value(Value::NULL)) => Datum::Null,
                Some(value) => Datum::Value(decode_value(value, info)?),
                None => return Err(io::Error::other("row value was consumed before conversion")),
            };
            Ok(ColumnDatum {
                ordinal,
                name: info.name.clone(),
                native_type: info.native_type.clone(),
                primary_key_ordinal: info.primary_key_ordinal,
                generated: info.generated,
                collation: info.collation.clone(),
                datum,
            })
        })
        .collect()
}

fn decode_value(value: &BinlogValue<'_>, info: &ColumnInfo) -> io::Result<LogicalValue> {
    match value {
        BinlogValue::Jsonb(value) => {
            let dom = value
                .clone()
                .parse()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            Ok(LogicalValue::Json {
                value: convert_json_dom(dom)?,
            })
        }
        BinlogValue::JsonDiff(_) => Err(io::Error::other(
            "partial JSON image cannot become a complete ChangeEvent row",
        )),
        BinlogValue::Value(value) => decode_mysql_value(value, info),
    }
}

pub(crate) fn decode_mysql_value(value: &Value, info: &ColumnInfo) -> io::Result<LogicalValue> {
    // Decode only declarations that have a published MySQL 5.7 Source Type
    // Mapping. This check is deliberately before the Value match: otherwise
    // an unsupported byte-oriented type could silently become Text.
    let mapping = source_type_mapping(
        &info.native_type,
        info.charset.as_deref(),
        info.collation.as_deref(),
    )
    .map_err(io::Error::other)?;
    let data_type = info.data_type.to_ascii_lowercase();
    if matches!(data_type.as_str(), "enum" | "set") && !matches!(value, Value::Bytes(_)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ENUM/SET binlog value is not label/member text; refusing ordinal conversion",
        ));
    }
    match value {
        Value::Int(value) => {
            let bits = integer_bits(&info.data_type);
            let unsigned = info.native_type.to_ascii_lowercase().contains("unsigned");
            // 5.7 TABLE_MAP omits signedness; the driver interprets integer bytes as signed.
            let text = if unsigned {
                let mask = u64::MAX >> (64 - bits);
                ((*value as u64) & mask).to_string()
            } else {
                value.to_string()
            };
            Ok(LogicalValue::Integer {
                signed: !unsigned,
                bits,
                value: text,
            })
        }
        Value::UInt(value) => Ok(LogicalValue::Integer {
            signed: false,
            bits: integer_bits(&info.data_type),
            value: value.to_string(),
        }),
        Value::Float(value) => Ok(LogicalValue::Float {
            bits: 32,
            ieee754_hex: format!("{:08x}", value.to_bits()),
        }),
        Value::Double(value) => Ok(LogicalValue::Float {
            bits: 64,
            ieee754_hex: format!("{:016x}", value.to_bits()),
        }),
        Value::Date(year, month, day, hour, minute, second, microsecond) => {
            if info.data_type.eq_ignore_ascii_case("date") {
                Ok(LogicalValue::Date {
                    year: *year,
                    month: *month,
                    day: *day,
                })
            } else if info.data_type.eq_ignore_ascii_case("timestamp") {
                timestamp_from_components(
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *microsecond,
                )
            } else {
                Ok(LogicalValue::LocalDatetime {
                    year: *year,
                    month: *month,
                    day: *day,
                    hour: *hour,
                    minute: *minute,
                    second: *second,
                    microsecond: *microsecond,
                })
            }
        }
        Value::Time(negative, days, hours, minutes, seconds, microsecond) => {
            Ok(LogicalValue::Duration {
                negative: *negative,
                hours: u64::from(*days) * 24 + u64::from(*hours),
                minutes: *minutes,
                seconds: *seconds,
                microsecond: *microsecond,
            })
        }
        Value::Bytes(bytes) => {
            if data_type == "enum" || data_type == "set" {
                let text = String::from_utf8(bytes.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                match mapping.logical_type {
                    change_event::LogicalType::Enum { members } => {
                        if !members.iter().any(|member| member == &text) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "ENUM value is not one of the declared labels",
                            ));
                        }
                        return Ok(LogicalValue::Enum { label: text });
                    }
                    change_event::LogicalType::Set { members } => {
                        if text.is_empty() {
                            return Ok(LogicalValue::Set {
                                members: Vec::new(),
                            });
                        }
                        let values = text.split(',').map(str::to_owned).collect::<Vec<_>>();
                        if values.iter().enumerate().any(|(index, value)| {
                            !members.iter().any(|member| member == value)
                                || values[..index].contains(value)
                        }) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "SET value contains an unknown or duplicate member",
                            ));
                        }
                        return Ok(LogicalValue::Set { members: values });
                    }
                    _ => unreachable!("enum/set native type mapped to another logical type"),
                }
            }
            if data_type == "decimal" || data_type == "numeric" {
                let text = String::from_utf8(bytes.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                return parse_decimal(&text);
            }
            if data_type == "timestamp" {
                let text = String::from_utf8(bytes.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                return timestamp(&text);
            }
            if data_type == "year" {
                let text = String::from_utf8(bytes.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                return Ok(LogicalValue::Year {
                    value: text
                        .parse()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
                });
            }
            if data_type == "bit" {
                let bit_length = native_bit_length(&info.native_type)?;
                return Ok(LogicalValue::BitString {
                    bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
                    bit_length,
                    padding: if bit_length.is_multiple_of(8) {
                        BitPadding::None
                    } else {
                        BitPadding::Zero
                    },
                    bit_order: BitOrder::MsbFirst,
                });
            }
            if is_binary_type(&data_type) {
                return Ok(LogicalValue::Binary {
                    bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
                });
            }
            Ok(LogicalValue::Text {
                charset: info.charset.clone().unwrap_or_else(|| "unknown".to_owned()),
                bytes_base64url: URL_SAFE_NO_PAD.encode(bytes),
                text: String::from_utf8(bytes.clone()).ok(),
            })
        }
        Value::NULL => Err(io::Error::other("NULL must be represented by Datum::Null")),
    }
}

fn parse_decimal(text: &str) -> io::Result<LogicalValue> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() || !digits.chars().all(|char| char.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid decimal value {text:?}"),
        ));
    }
    let unscaled = if negative {
        format!("-{digits}")
    } else {
        digits
    };
    Ok(LogicalValue::Decimal {
        unscaled,
        scale: fraction.len(),
    })
}

fn convert_json_dom(value: JsonDom) -> io::Result<JsonValue> {
    Ok(match value {
        JsonDom::Container(JsonContainer::Array(values)) => JsonValue::Array(
            values
                .into_iter()
                .map(convert_json_dom)
                .collect::<io::Result<_>>()?,
        ),
        JsonDom::Container(JsonContainer::Object(values)) => JsonValue::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    Ok(JsonEntry {
                        key,
                        value: convert_json_dom(value)?,
                    })
                })
                .collect::<io::Result<_>>()?,
        ),
        JsonDom::Scalar(JsonScalar::Null) => JsonValue::Null,
        JsonDom::Scalar(JsonScalar::Boolean(value)) => JsonValue::Boolean(value),
        JsonDom::Scalar(JsonScalar::String(value)) => JsonValue::String(value),
        JsonDom::Scalar(JsonScalar::Number(JsonNumber::Int(value))) => {
            JsonValue::SignedInteger(value.to_string())
        }
        JsonDom::Scalar(JsonScalar::Number(JsonNumber::Uint(value))) => {
            JsonValue::UnsignedInteger(value.to_string())
        }
        JsonDom::Scalar(JsonScalar::Number(JsonNumber::Decimal(value))) => {
            let LogicalValue::Decimal { unscaled, scale } = parse_decimal(&value.to_string())?
            else {
                unreachable!()
            };
            JsonValue::Decimal { unscaled, scale }
        }
        JsonDom::Scalar(JsonScalar::Number(JsonNumber::Double(value))) => {
            JsonValue::DoubleBits(format!("{:016x}", value.to_bits()))
        }
        JsonDom::Scalar(_) => {
            return Err(io::Error::other(
                "JSON temporal/opaque scalar is unsupported; refusing lossy string conversion",
            ));
        }
    })
}

fn timestamp(value: &str) -> io::Result<LogicalValue> {
    let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
    if seconds.parse::<u32>().is_err()
        || fraction.len() > 6
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(io::Error::other("invalid TIMESTAMP binlog value"));
    }
    let nanoseconds = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>().map_err(io::Error::other)? * 10_u32.pow(9 - fraction.len() as u32)
    };
    Ok(LogicalValue::Instant {
        unix_seconds: seconds.to_owned(),
        nanoseconds,
    })
}

fn timestamp_from_components(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    microsecond: u32,
) -> io::Result<LogicalValue> {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0
        || day == 0
        || day > days_in_month
        || hour >= 24
        || minute >= 60
        || second >= 60
        || microsecond >= 1_000_000
    {
        return Err(io::Error::other("invalid TIMESTAMP date value"));
    }
    let y = i64::from(year) - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = yoe * 365 + yoe / 4 - yoe / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let unix_seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    Ok(LogicalValue::Instant {
        unix_seconds: unix_seconds.to_string(),
        nanoseconds: microsecond * 1_000,
    })
}

fn integer_bits(data_type: &str) -> u8 {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" => 8,
        "smallint" => 16,
        "mediumint" => 24,
        "int" | "integer" => 32,
        _ => 64,
    }
}

fn is_binary_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry"
    )
}

fn native_bit_length(native_type: &str) -> io::Result<u64> {
    let length = native_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')').map(|(value, _)| value))
        .unwrap_or("1")
        .trim()
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid BIT length"))?;
    if !(1..=64).contains(&length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "BIT length is outside MySQL 5.7 limits",
        ));
    }
    Ok(length)
}

fn format_gtid(sid: [u8; 16], gno: u64) -> String {
    format!(
        "{}:{gno}",
        [
            hex(&sid[0..4]),
            hex(&sid[4..6]),
            hex(&sid[6..8]),
            hex(&sid[8..10]),
            hex(&sid[10..16]),
        ]
        .join("-")
    )
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

const PARTIAL_UPDATE_ROWS_EVENT: u8 = 0x27;
const TRANSACTION_PAYLOAD_EVENT: u8 = 0x28;
const XA_PREPARE_LOG_EVENT: u8 = 0x26;
const GTID_TAGGED_LOG_EVENT: u8 = 0x2a;

fn check_event_type(raw: u8) -> io::Result<()> {
    match raw {
        TRANSACTION_PAYLOAD_EVENT => Err(io::Error::other(
            "compressed binlog transactions are unsupported; transaction was not emitted",
        )),
        PARTIAL_UPDATE_ROWS_EVENT => Err(io::Error::other(
            "partial JSON updates are unsupported; require binlog_row_value_options=''",
        )),
        XA_PREPARE_LOG_EVENT => Err(io::Error::other("XA transactions are unsupported")),
        GTID_TAGGED_LOG_EVENT => Err(io::Error::other("tagged GTID requires mysql_8_4")),
        _ => Ok(()),
    }
}

/// Buffer only validated row changes; nothing is published before commit.
struct PendingTransaction {
    pub id: String,
    pub begin_cursor: SourceCursor,
    pub began: bool,
    pub changes: Vec<RowChange>,
    bytes: usize,
}

impl PendingTransaction {
    pub fn new(id: String, begin_cursor: SourceCursor, began: bool) -> Self {
        Self {
            id,
            begin_cursor,
            began,
            changes: Vec::new(),
            bytes: 0,
        }
    }

    pub fn push(&mut self, change: RowChange, byte_limit: usize) -> io::Result<()> {
        if !self.began {
            return Err(io::Error::other(
                "row before BEGIN; start at a transaction boundary",
            ));
        }
        let size = serde_json::to_vec(&change)?.len();
        if size > byte_limit.saturating_sub(self.bytes) {
            return Err(io::Error::other(
                "transaction exceeds buffer limit; no events from it were emitted",
            ));
        }
        self.bytes += size;
        self.changes.push(change);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_date_values_are_instants() {
        let info = ColumnInfo {
            name: "created_at".into(),
            native_type: "timestamp(6)".into(),
            data_type: "timestamp".into(),
            charset: None,
            collation: None,
            generated: false,
            primary_key_ordinal: None,
        };
        let value = decode_mysql_value(&Value::Date(2026, 9, 14, 1, 2, 3, 123456), &info).unwrap();
        assert!(matches!(
            value,
            LogicalValue::Instant {
                unix_seconds,
                nanoseconds: 123_456_000,
            } if unix_seconds == "1789347723"
        ));
    }

    #[test]
    fn enum_and_set_bytes_decode_as_labels_and_members() {
        let enum_info = ColumnInfo {
            name: "state".into(),
            native_type: "enum('Ready','blocked')".into(),
            data_type: "enum".into(),
            charset: None,
            collation: None,
            generated: false,
            primary_key_ordinal: None,
        };
        assert!(matches!(
            decode_mysql_value(&Value::Bytes(b"blocked".to_vec()), &enum_info).unwrap(),
            LogicalValue::Enum { label } if label == "blocked"
        ));
        let set_info = ColumnInfo {
            name: "flags".into(),
            native_type: "set('a','b')".into(),
            data_type: "set".into(),
            charset: None,
            collation: None,
            generated: false,
            primary_key_ordinal: None,
        };
        assert!(matches!(
            decode_mysql_value(&Value::Bytes(b"b,a".to_vec()), &set_info).unwrap(),
            LogicalValue::Set { members } if members == vec!["b", "a"]
        ));
        assert!(decode_mysql_value(&Value::Bytes(b"unknown".to_vec()), &enum_info).is_err());
    }
}
