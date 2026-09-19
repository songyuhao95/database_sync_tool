use crate::{ChangeEvent, FORMAT, Payload, Transaction, ValidatedTransaction};
use std::io::{self, Write};

/// Complete JSON Lines for one validated transaction; same v0 envelope as the CLI.
pub fn json(validated: &ValidatedTransaction) -> io::Result<String> {
    let transaction = validated.transaction();
    let mut buffer = Vec::new();
    let writer = &mut buffer;
    let emit = |sequence, payload, writer: &mut dyn Write| -> io::Result<()> {
        let event = ChangeEvent {
            format: FORMAT,
            source: &transaction.source,
            transaction: Transaction {
                id: &transaction.id,
                sequence,
                begin_cursor: &transaction.begin_cursor,
                commit_cursor: &transaction.commit_cursor,
            },
            payload,
        };
        serde_json::to_writer(&mut *writer, &event)?;
        writer.write_all(b"\n")
    };
    emit(0, Payload::TransactionBegin, writer)?;
    for (index, change) in transaction.changes.iter().enumerate() {
        emit(index + 1, Payload::RowChange { change }, writer)?;
    }
    emit(
        transaction.changes.len() + 1,
        Payload::TransactionCommit {
            change_count: transaction.changes.len(),
        },
        writer,
    )?;

    String::from_utf8(buffer).map_err(io::Error::other)
}
