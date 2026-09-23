//! Bounded, authenticated reads of task-owned source capture logs.
use crate::{
    Error, Result, Store,
    registry::{AdapterKind, SourceRegistry},
    runtime_store::TaskLog,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    time::UNIX_EPOCH,
};
const READ_BYTES: usize = 128 * 1024;
const PAGE_LINES: usize = 200;
#[derive(Deserialize, Default)]
pub(crate) struct SourceLogQuery {
    #[serde(default)]
    pub after: u64,
    pub generation: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct SourceLogPage {
    pub entries: Vec<TaskLog>,
    pub next: u64,
    pub generation: String,
    pub reset: bool,
    pub has_more: bool,
}
impl Store {
    pub(crate) fn source_logs(&self, id: &str, query: SourceLogQuery) -> Result<SourceLogPage> {
        let task = self.task(id)?;
        if id.is_empty()
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err(Error::Invalid("任务 ID 无效"));
        }
        let (kind, version): (String, String) = self.db()?.query_row(
            "SELECT kind,version FROM instances WHERE id=?1",
            [task.source_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let connector = SourceRegistry
            .find(&kind, &version)
            .ok_or(Error::Invalid("Web 未注册该数据库连接器"))?;
        let empty = || SourceLogPage {
            entries: Vec::new(),
            next: 0,
            generation: String::new(),
            reset: query.after > 0,
            has_more: false,
        };
        let filename = match connector.adapter {
            AdapterKind::Mysql57 | AdapterKind::Mysql80 | AdapterKind::Mysql84 => {
                format!("mysql-{version}-binlog.log")
            }
            AdapterKind::Postgresql15 => "postgresql-15-source.log".into(),
            AdapterKind::Postgresql16 => "postgresql-16-source.log".into(),
            AdapterKind::Postgresql17 => "postgresql-17-source.log".into(),
        };
        let path = self.log_dir.join(id).join(filename);
        let metadata = match path.symlink_metadata() {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(empty()),
            Err(_) => return Err(Error::Internal),
        };
        if !metadata.is_file() {
            return Err(Error::Invalid("源端日志不是普通文件"));
        }
        // Neither a task directory junction nor a file symlink may expose another
        // task's logs or arbitrary local files through an authenticated URL.
        let root = self.log_dir.canonicalize().map_err(|_| Error::Internal)?;
        let actual = path.canonicalize().map_err(|_| Error::Internal)?;
        if actual.parent() != Some(root.join(id).as_path()) {
            return Err(Error::Invalid("日志路径无效"));
        }
        let mut file = File::open(actual).map_err(|_| Error::Internal)?;
        read_page(&mut file, query).map_err(|_| Error::Internal)
    }
}
fn read_page(file: &mut File, query: SourceLogQuery) -> std::io::Result<SourceLogPage> {
    let metadata = file.metadata()?;
    let len = metadata.len();
    let generation = metadata
        .created()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_default();
    let reset = query.after > len || query.generation.as_ref().is_some_and(|g| g != &generation);
    let initial = query.after == 0 || reset;
    let start = if initial {
        len.saturating_sub(READ_BYTES as u64)
    } else {
        query.after
    };
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.take(READ_BYTES as u64).read_to_end(&mut bytes)?;
    let mut offset = start;
    let mut entries = Vec::new();
    for (i, line) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
        if !line.ends_with(b"\n") {
            break;
        } // Retry incomplete UTF-8/line on the next poll.
        offset += line.len() as u64;
        if initial && start > 0 && i == 0 {
            continue;
        }
        let message = String::from_utf8_lossy(line)
            .trim_end_matches(['\n', '\r'])
            .to_owned();
        entries.push(TaskLog {
            id: i64::try_from(offset).map_err(std::io::Error::other)?,
            timestamp: 0,
            level: "binlog".into(),
            message,
        });
        if !initial && entries.len() == PAGE_LINES {
            break;
        }
    }
    if entries.len() > PAGE_LINES {
        entries.drain(..entries.len() - PAGE_LINES);
    }
    // A single malformed/oversized line must not trap the reader at one cursor.
    if offset == start && bytes.len() == READ_BYTES {
        offset += bytes.len() as u64;
        entries.push(TaskLog {
            id: i64::try_from(offset).map_err(std::io::Error::other)?,
            timestamp: 0,
            level: "warning".into(),
            message: "[日志行超过 128 KiB，本段已省略]".into(),
        });
    }
    let has_more = offset < len && (entries.len() == PAGE_LINES || bytes.len() == READ_BYTES);
    Ok(SourceLogPage {
        entries,
        next: offset,
        generation,
        reset,
        has_more,
    })
}
