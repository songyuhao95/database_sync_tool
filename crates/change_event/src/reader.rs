//! Bounded JSONL transaction reader. EOF is temporary until finish() is called.
use crate::{
    ChangeTransaction, FORMAT, HISTORICAL_FORMAT, LEGACY_FORMAT, PREVIOUS_FORMAT, RowChange,
    Source, SourceCursor, ValidatedTransaction,
};
use serde::Deserialize;
use std::io::{self, BufRead};

const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format: String,
    source: Source,
    transaction: TransactionHeader,
    payload: Payload,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionHeader {
    id: String,
    sequence: usize,
    begin_cursor: SourceCursor,
    commit_cursor: SourceCursor,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    TransactionBegin,
    RowChange {
        #[serde(flatten)]
        change: Box<RowChange>,
    },
    TransactionCommit {
        change_count: usize,
    },
}

/// Call next_transaction repeatedly, including after a temporary EOF when following a file.
/// Only complete, validated commits are returned. Any error permanently stops this reader.
pub struct JsonReader<R> {
    reader: R,
    line: Vec<u8>,
    pending: Option<ChangeTransaction>,
    bytes: usize,
    failed: bool,
}
impl<R: BufRead> JsonReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
            pending: None,
            bytes: 0,
            failed: false,
        }
    }

    pub fn next_transaction(&mut self) -> io::Result<Option<ValidatedTransaction>> {
        if self.failed {
            return Err(invalid("reader stopped after an earlier error"));
        }
        let result = self.read_transaction();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn read_transaction(&mut self) -> io::Result<Option<ValidatedTransaction>> {
        loop {
            let available = self.reader.fill_buf()?;
            if available.is_empty() {
                return Ok(None);
            }
            let length = available
                .iter()
                .position(|b| *b == b'\n')
                .map_or(available.len(), |i| i + 1);
            if length > MAX_BYTES.saturating_sub(self.bytes + self.line.len()) {
                return Err(invalid("JSON transaction exceeds 64 MiB"));
            }
            let complete = available[length - 1] == b'\n';
            self.line.extend_from_slice(&available[..length]);
            self.reader.consume(length);
            if !complete {
                continue;
            }
            let line = std::mem::take(&mut self.line);
            let envelope: Envelope = serde_json::from_slice(&line).map_err(invalid)?;
            let supported = match envelope.format.as_str() {
                FORMAT | PREVIOUS_FORMAT => true,
                LEGACY_FORMAT | HISTORICAL_FORMAT => envelope.source.kind == "mysql",
                _ => false,
            };
            if !supported {
                return Err(invalid(format!(
                    "unsupported ChangeEvent format {}; recapture with {FORMAT}",
                    envelope.format
                )));
            }
            let header = &envelope.transaction;
            if let Some(tx) = &self.pending
                && (tx.source != envelope.source
                    || tx.id != header.id
                    || tx.begin_cursor != header.begin_cursor
                    || tx.commit_cursor != header.commit_cursor
                    || header.sequence != tx.changes.len() + 1)
            {
                return Err(invalid(
                    "interleaved transaction or inconsistent identity/cursor/sequence",
                ));
            }
            self.bytes += line.len();
            match envelope.payload {
                Payload::TransactionBegin => {
                    if self.pending.is_some() || header.sequence != 0 {
                        return Err(invalid("nested transaction or invalid begin sequence"));
                    }
                    self.pending = Some(ChangeTransaction {
                        source: envelope.source,
                        id: header.id.clone(),
                        begin_cursor: header.begin_cursor.clone(),
                        commit_cursor: header.commit_cursor.clone(),
                        changes: Vec::new(),
                    });
                }
                Payload::RowChange { change } => {
                    self.pending
                        .as_mut()
                        .ok_or_else(|| invalid("row without begin"))?
                        .changes
                        .push(*change);
                }
                Payload::TransactionCommit { change_count } => {
                    let tx = self
                        .pending
                        .take()
                        .ok_or_else(|| invalid("commit without begin"))?;
                    if tx.changes.len() != change_count {
                        return Err(invalid("commit change_count mismatch"));
                    }
                    let validated = crate::validate(tx).map_err(invalid)?;
                    self.bytes = 0;
                    return Ok(Some(validated));
                }
            }
        }
    }

    /// Finite input must end on a newline and a complete commit.
    pub fn finish(&self) -> io::Result<()> {
        if self.failed || self.pending.is_some() || !self.line.is_empty() {
            return Err(invalid(
                "incomplete or failed ChangeEvent transaction at end of input",
            ));
        }
        Ok(())
    }
}
fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
