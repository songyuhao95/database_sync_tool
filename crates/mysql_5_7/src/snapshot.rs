//! A single InnoDB read view shared by every selected table and batch.
use crate::{
    BinlogConfig, BinlogStartMode,
    decoder::{ColumnInfo, decode_mysql_value},
    sql::{qualified_table, quote_identifier},
    type_mapping::validate_native_type,
};
use change_event::{
    ColumnDatum, Datum, JsonEntry, JsonValue, LogicalValue, SnapshotBatch, SnapshotBoundary,
    SnapshotTable, Source, SourceCursor,
};
use mysql_driver::{Conn, OptsBuilder, Params, Row, Value, prelude::Queryable};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const VERSION: &str = "5.7";
const STATUS: &str = "SHOW MASTER STATUS";
const BATCH_ROWS: usize = 256;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

struct ReadLock {
    conn: Conn,
    held: bool,
}
impl ReadLock {
    fn acquire(mut conn: Conn) -> io::Result<Self> {
        // Server-side bounds: lock acquisition and idle lock connection, not a lease.
        conn.query_drop("SET SESSION lock_wait_timeout=5, wait_timeout=10")
            .map_err(io::Error::other)?;
        let mut lock = Self { conn, held: true };
        lock.conn
            .query_drop("FLUSH TABLES WITH READ LOCK")
            .map_err(|e| {
                io::Error::other(format!(
                    "源端全局读锁失败（读取账号需要 RELOAD 或相应 FLUSH 权限）：{e}"
                ))
            })?;
        Ok(lock)
    }
    fn release(&mut self) -> io::Result<()> {
        // Never reconnect: an expired original connection invalidates the entire snapshot.
        self.conn
            .query_drop("UNLOCK TABLES")
            .map_err(|e| io::Error::other(format!("锁连接已失效或解锁未确认，放弃快照：{e}")))?;
        self.held = false;
        Ok(())
    }
}
impl Drop for ReadLock {
    fn drop(&mut self) {
        if self.held {
            let _ = self.conn.query_drop("UNLOCK TABLES");
        }
    }
}
struct ScanTable {
    scope: SnapshotTable,
    columns: Vec<ColumnInfo>,
    keys: Vec<usize>,
}
pub struct SnapshotReader {
    conn: Conn,
    boundary: SnapshotBoundary,
    tables: Vec<ScanTable>,
    table_index: usize,
    last_key: Vec<Value>,
    stop: Option<Arc<AtomicBool>>,
    ended: bool,
}
impl SnapshotReader {
    pub fn boundary(&self) -> &SnapshotBoundary {
        &self.boundary
    }
}
fn check_stop(stop: Option<&Arc<AtomicBool>>) -> io::Result<()> {
    if stop.is_some_and(|s| s.load(Ordering::Acquire)) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "全量同步已取消，未提交数据将回滚",
        ))
    } else {
        Ok(())
    }
}
fn connection(config: &BinlogConfig) -> io::Result<Conn> {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(&config.host))
            .tcp_port(config.port)
            .user(Some(&config.user))
            .pass(Some(&config.password))
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(5)))
            .write_timeout(Some(Duration::from_secs(5))),
    )
    .map_err(io::Error::other)
}
/// Returns only after the original lock connection has confirmed UNLOCK TABLES.
/// The read-only transaction remains open until the reader is dropped.
pub fn snapshot(config: BinlogConfig, tables: Vec<SnapshotTable>) -> io::Result<SnapshotReader> {
    check_stop(config.stop.as_ref())?;
    if tables.is_empty()
        || tables.iter().any(|t| {
            t.schema.eq_ignore_ascii_case("CDC") || t.schema.is_empty() || t.table.is_empty()
        })
    {
        return Err(io::Error::other("invalid or empty snapshot scope"));
    }
    let unique: std::collections::HashSet<_> =
        tables.iter().map(|t| (&t.schema, &t.table)).collect();
    if unique.len() != tables.len() {
        return Err(io::Error::other("duplicate snapshot table"));
    }
    let mut conn = connection(&config)?;
    let (version, uuid, log_bin, format, image, gtid): (String,String,u8,String,String,String) = conn.query_first(
        "SELECT VERSION(),@@server_uuid,@@log_bin,@@binlog_format,@@binlog_row_image,@@gtid_mode"
    ).map_err(io::Error::other)?.ok_or_else(|| io::Error::other("missing source metadata"))?;
    if !version.starts_with(VERSION) || log_bin != 1 || format != "ROW" || image != "FULL" {
        return Err(io::Error::other(
            "snapshot requires the matching MySQL version and ROW/FULL binlog",
        ));
    }
    let mode = match config.start_mode {
        BinlogStartMode::Gtid if gtid != "ON" => {
            return Err(io::Error::other(
                "首次全量的 GTID 模式要求源端 GTID_MODE=ON",
            ));
        }
        BinlogStartMode::Gtid => "gtid",
        BinlogStartMode::Auto if gtid == "ON" => "gtid",
        _ => "binlog",
    };
    conn.query_drop("SET SESSION time_zone='+00:00', character_set_results=binary, lock_wait_timeout=5, max_execution_time=5000").map_err(io::Error::other)?;
    conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .map_err(io::Error::other)?;
    let mut lock_conn = connection(&config)?;
    let lock_uuid: Option<String> = lock_conn
        .query_first("SELECT @@server_uuid")
        .map_err(io::Error::other)?;
    if lock_uuid.as_deref() != Some(&uuid) {
        return Err(io::Error::other(
            "snapshot and lock connections reached different servers",
        ));
    }
    let began = Instant::now();
    let mut lock = ReadLock::acquire(lock_conn)?;
    let (file, position, _, _, executed_gtids): (String, u64, String, String, String) = conn
        .query_first(STATUS)
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("source returned no binlog boundary"))?;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        .map_err(io::Error::other)?;
    // Pin selected schemas under the global lock. These metadata locks allow DML but block DDL.
    for table in &tables {
        check_stop(config.stop.as_ref())?;
        if began.elapsed() >= Duration::from_secs(8) {
            return Err(io::Error::other(
                "全量快照初始化超过 8 秒预算，放弃本次初始化",
            ));
        }
        conn.query_drop(format!(
            "SELECT * FROM {} LIMIT 0",
            qualified_table(&table.schema, &table.table)
        ))
        .map_err(io::Error::other)?;
    }
    if began.elapsed() >= Duration::from_secs(8) {
        return Err(io::Error::other(
            "snapshot initialization exceeded time budget",
        ));
    }
    lock.release()?;
    drop(lock);
    check_stop(config.stop.as_ref())?;
    let mut scans = Vec::new();
    for scope in tables {
        let engine: Option<String> = conn.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (&scope.schema,&scope.table)).map_err(io::Error::other)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(io::Error::other("snapshot requires InnoDB source tables"));
        }
        let columns = load_columns(&mut conn, &scope)?;
        let mut keys: Vec<_> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key_ordinal.is_some())
            .map(|(i, _)| i)
            .collect();
        keys.sort_by_key(|i| columns[*i].primary_key_ordinal);
        if keys.is_empty()
            || keys
                .iter()
                .any(|i| !scope.columns.is_empty() && !scope.columns.contains(&columns[*i].name))
        {
            return Err(io::Error::other(
                "snapshot selection must contain every primary-key column",
            ));
        }
        if scope
            .columns
            .iter()
            .any(|n| !columns.iter().any(|c| &c.name == n))
        {
            return Err(io::Error::other("snapshot selected column is missing"));
        }
        for c in &columns {
            if !scope.columns.is_empty() && !scope.columns.contains(&c.name) {
                continue;
            }
            if !matches!(
                c.data_type.as_str(),
                "tinyint"
                    | "smallint"
                    | "mediumint"
                    | "int"
                    | "bigint"
                    | "decimal"
                    | "float"
                    | "double"
                    | "char"
                    | "varchar"
                    | "tinytext"
                    | "text"
                    | "mediumtext"
                    | "longtext"
                    | "binary"
                    | "varbinary"
                    | "tinyblob"
                    | "blob"
                    | "mediumblob"
                    | "longblob"
                    | "date"
                    | "datetime"
                    | "time"
                    | "timestamp"
                    | "year"
                    | "json"
            ) {
                return Err(io::Error::other(format!(
                    "snapshot type unsupported: {}.{}.{} {}",
                    scope.schema, scope.table, c.name, c.native_type
                )));
            }
        }
        scans.push(ScanTable {
            scope,
            columns,
            keys,
        });
    }
    Ok(SnapshotReader {
        conn,
        boundary: SnapshotBoundary {
            source: Source {
                kind: "mysql".into(),
                version,
                id: uuid,
            },
            cursor: SourceCursor {
                format: "mysql.snapshot-boundary.v1".into(),
                value: serde_json::json!({
                    "mode": mode,
                    "file": file.clone(),
                    "position": position,
                    "executed_gtids": executed_gtids.clone(),
                })
                .to_string(),
                display: format!("{mode}:{file}:{position}"),
            },
        },
        tables: scans,
        table_index: 0,
        last_key: Vec::new(),
        stop: config.stop,
        ended: false,
    })
}
fn load_columns(conn: &mut Conn, table: &SnapshotTable) -> io::Result<Vec<ColumnInfo>> {
    let columns = conn.exec_map("SELECT c.COLUMN_NAME,c.COLUMN_TYPE,c.DATA_TYPE,c.CHARACTER_SET_NAME,c.COLLATION_NAME,c.GENERATION_EXPRESSION,k.ORDINAL_POSITION FROM information_schema.COLUMNS c LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION", (&table.schema,&table.table),
        |(name,native_type,data_type,charset,collation,expression,key):(String,String,String,Option<String>,Option<String>,String,Option<u64>)| ColumnInfo { name,native_type,data_type,charset,collation,generated:!expression.is_empty(),primary_key_ordinal:key.map(|v|v as usize-1) }).map_err(io::Error::other)?;
    for column in &columns {
        validate_native_type(&column.native_type).map_err(io::Error::other)?;
    }
    Ok(columns)
}
impl SnapshotReader {
    fn next_batch(&mut self) -> io::Result<SnapshotBatch> {
        check_stop(self.stop.as_ref())?;
        let table = &self.tables[self.table_index];
        let selected: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                table.scope.columns.is_empty() || table.scope.columns.contains(&c.name)
            })
            .collect();
        let mut expressions: Vec<_> = selected
            .iter()
            .map(|(_, c)| {
                let name = quote_identifier(&c.name);
                match c.data_type.as_str() {
                    "timestamp" => format!("CAST(UNIX_TIMESTAMP({name}) AS CHAR)"),
                    "year" => format!("CAST({name} AS CHAR)"),
                    _ => name,
                }
            })
            .collect();
        let keys: Vec<_> = table
            .keys
            .iter()
            .map(|i| quote_identifier(&table.columns[*i].name))
            .collect();
        expressions.extend(keys.clone()); // Original key values, never a formatted timestamp.
        // JSON text cannot represent MySQL temporal/opaque scalars. Require a
        // server-side round trip before decoding so those values cannot silently become strings.
        for (_, c) in selected.iter().filter(|(_, c)| c.data_type == "json") {
            let name = quote_identifier(&c.name);
            expressions.push(format!("({name} IS NULL OR {name}=CAST(CAST({name} AS CHAR CHARACTER SET utf8mb4) AS JSON))"));
        }

        let condition = if self.last_key.is_empty() {
            String::new()
        } else {
            format!(
                " WHERE ({}) > ({})",
                keys.join(","),
                vec!["?"; keys.len()].join(",")
            )
        };
        let query = format!(
            "SELECT {} FROM {}{} ORDER BY {} LIMIT {}",
            expressions.join(","),
            qualified_table(&table.scope.schema, &table.scope.table),
            condition,
            keys.join(","),
            BATCH_ROWS
        );
        let mut result = self
            .conn
            .exec_iter(query, Params::Positional(self.last_key.clone()))
            .map_err(io::Error::other)?;
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        for raw in &mut result {
            check_stop(self.stop.as_ref())?;
            let raw: Row = raw.map_err(io::Error::other)?;
            let values = raw.unwrap();
            bytes += values
                .iter()
                .map(|v| if let Value::Bytes(b) = v { b.len() } else { 32 })
                .sum::<usize>();
            if bytes > MAX_BATCH_BYTES {
                return Err(io::Error::other(
                    "snapshot batch exceeds 16 MiB limit; no data committed",
                ));
            }

            let key_end = selected.len() + keys.len();
            if values[key_end..]
                .iter()
                .any(|v| !matches!(v, Value::Int(1) | Value::UInt(1)))
            {
                return Err(io::Error::other(
                    "snapshot JSON contains a value that cannot round-trip through JSON text; refusing lossy conversion",
                ));
            }
            self.last_key = values[selected.len()..key_end].to_vec();
            let mut row = Vec::new();
            for ((ordinal, c), v) in selected.iter().zip(&values) {
                let datum = if matches!(v, Value::NULL) {
                    Datum::Null
                } else {
                    Datum::Value(decode_value(v, c)?)
                };
                row.push(ColumnDatum {
                    ordinal: *ordinal,
                    name: c.name.clone(),
                    native_type: c.native_type.clone(),
                    primary_key_ordinal: c.primary_key_ordinal,
                    generated: c.generated,
                    collation: c.collation.clone(),
                    datum,
                });
            }
            rows.push(row);
        }
        let last_in_table = rows.len() < BATCH_ROWS;
        let batch = SnapshotBatch {
            source: self.boundary.source.clone(),
            schema: table.scope.schema.clone(),
            table: table.scope.table.clone(),
            rows,
            last_in_table,
        };
        if last_in_table {
            self.table_index += 1;
            self.last_key.clear();
        }
        Ok(batch)
    }
}
impl Iterator for SnapshotReader {
    type Item = io::Result<SnapshotBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended || self.table_index == self.tables.len() {
            return None;
        }
        let result = self.next_batch();
        if result.is_err() {
            self.ended = true;
        }
        Some(result)
    }
}
// Dropping this non-pooled connection closes the read-only transaction even on failure.
fn decode_value(value: &Value, info: &ColumnInfo) -> io::Result<LogicalValue> {
    if info.data_type == "json" {
        let Value::Bytes(bytes) = value else {
            return Err(io::Error::other("invalid snapshot JSON representation"));
        };
        return Ok(LogicalValue::Json {
            value: json_value(serde_json::from_slice(bytes).map_err(io::Error::other)?)?,
        });
    }
    decode_mysql_value(value, info)
}
fn json_value(value: serde_json::Value) -> io::Result<JsonValue> {
    Ok(match value {
        serde_json::Value::Null => JsonValue::Null,
        serde_json::Value::Bool(v) => JsonValue::Boolean(v),
        serde_json::Value::String(v) => JsonValue::String(v),
        serde_json::Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                JsonValue::SignedInteger(n.to_string())
            } else if let Some(n) = v.as_u64() {
                JsonValue::UnsignedInteger(n.to_string())
            } else {
                JsonValue::DoubleBits(format!(
                    "{:016x}",
                    v.as_f64()
                        .ok_or_else(|| io::Error::other("invalid JSON number"))?
                        .to_bits()
                ))
            }
        }
        serde_json::Value::Array(v) => {
            JsonValue::Array(v.into_iter().map(json_value).collect::<io::Result<_>>()?)
        }
        serde_json::Value::Object(v) => JsonValue::Object(
            v.into_iter()
                .map(|(key, v)| {
                    Ok(JsonEntry {
                        key,
                        value: json_value(v)?,
                    })
                })
                .collect::<io::Result<_>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancelled_snapshot_does_not_connect() {
        let mut config = BinlogConfig::new("invalid.example", 1, "reader", "");
        config.stop = Some(Arc::new(AtomicBool::new(true)));
        assert_eq!(
            snapshot(config, Vec::new()).err().unwrap().kind(),
            io::ErrorKind::Interrupted
        );
    }
    #[test]
    #[ignore = "needs live source reader RELOAD permission; holds a read lock for ~2 seconds"]
    fn idle_lock_connection_expires_and_cannot_validate_snapshot() {
        let config = BinlogConfig::new(
            "192.168.0.10",
            33061,
            "mysql_reader",
            std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap(),
        );
        let mut conn = connection(&config).unwrap();
        let principal: Option<String> = conn.query_first("SELECT CURRENT_USER()").unwrap();
        println!("source principal: {}", principal.unwrap());
        let mut lock = ReadLock::acquire(conn).unwrap();
        lock.conn.query_drop("SET SESSION wait_timeout=2").unwrap();
        let started = Instant::now();
        std::thread::sleep(Duration::from_secs(3));
        assert!(
            lock.release().is_err(),
            "must reject expired original lock connection"
        );
        assert!(started.elapsed() < Duration::from_secs(8));
    }
    #[test]
    #[ignore = "helper for the parent process-exit test"]
    fn lock_child_process() {
        if std::env::var("CDC_LOCK_CHILD").as_deref() != Ok("1") {
            return;
        }
        let config = BinlogConfig::new(
            "192.168.0.10",
            33061,
            "mysql_reader",
            std::env::var("CDC_MYSQL_READER_PASSWORD").unwrap(),
        );
        let _lock = ReadLock::acquire(connection(&config).unwrap()).unwrap();
        println!("LOCK_READY");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        std::thread::sleep(Duration::from_secs(30));
    }
    #[test]
    #[ignore = "live process termination while source reader holds a global lock"]
    fn process_exit_releases_source_lock() {
        use std::{
            io::BufRead,
            process::{Command, Stdio},
        };
        let config = BinlogConfig::new(
            "192.168.0.10",
            33061,
            "mysql_writer",
            std::env::var("CDC_MYSQL_WRITER_PASSWORD").unwrap(),
        );
        let mut writer = connection(&config).unwrap();
        writer
            .query_drop("SET SESSION lock_wait_timeout=3")
            .unwrap();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("CDC_test.lock_exit_{nonce}");
        writer
            .query_drop(format!(
                "CREATE TABLE {table}(id INT PRIMARY KEY) ENGINE=InnoDB"
            ))
            .unwrap();
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "snapshot::tests::lock_child_process",
                "--nocapture",
            ])
            .env("CDC_LOCK_CHILD", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = Child(command.spawn().unwrap());
        let output = child.0.stdout.take().unwrap();
        let (ready, wait) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(output)
                .lines()
                .map_while(Result::ok)
            {
                if line.contains("LOCK_READY") {
                    let _ = ready.send(());
                    return;
                }
            }
        });
        wait.recv_timeout(Duration::from_secs(12))
            .expect("child did not acquire source lock");
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        reader.join().unwrap();
        let started = Instant::now();
        writer
            .query_drop(format!("INSERT INTO {table} VALUES(1)"))
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "source stayed blocked after lock process terminated"
        );
        writer.query_drop(format!("DROP TABLE {table}")).unwrap();
    }
}
