use crate::{Result, catalog::Table, cursor, invalid, types, validate_change_event};
use change_event::{
    ChangeTransaction, ColumnDatum, Datum, Operation, RowChange, Source, ValidatedTransaction,
};
use pg_walstream::{LogicalReplicationMessage as M, TupleData};
use std::collections::{HashMap, HashSet};
struct Pending {
    xid: u32,
    final_lsn: u64,
    timestamp: i64,
    bytes: usize,
    rows: Vec<RowChange>,
}
pub(crate) struct Decoder {
    pub source: Source,
    database: String,
    tables: HashMap<u32, Table>,
    seen: HashSet<u32>,
    pending: Option<Pending>,
    previous_end: u64,
    max_bytes: usize,
}
impl Decoder {
    pub fn new(
        source: Source,
        database: String,
        tables: HashMap<u32, Table>,
        max_bytes: usize,
    ) -> Self {
        Self {
            source,
            database,
            tables,
            seen: HashSet::new(),
            pending: None,
            previous_end: 0,
            max_bytes,
        }
    }
    pub fn push(&mut self, message: M, bytes: usize) -> Result<Option<ValidatedTransaction>> {
        if let Some(p) = self.pending.as_mut() {
            p.bytes = p
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("transaction size overflow"))?;
            if p.bytes > self.max_bytes {
                return Err(invalid(
                    "transaction exceeds capture wire-size limit; checkpoint has not advanced",
                ));
            }
        }
        match message {
            M::Begin {
                final_lsn,
                timestamp,
                xid,
            } => {
                if self.pending.is_some()
                    || xid == 0
                    || final_lsn == 0
                    || final_lsn <= self.previous_end
                {
                    return Err(invalid("invalid/nested BEGIN or out-of-order transaction"));
                }
                self.pending = Some(Pending {
                    xid,
                    final_lsn,
                    timestamp,
                    bytes,
                    rows: Vec::new(),
                });
            }
            M::Commit {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                let p = self
                    .pending
                    .take()
                    .ok_or_else(|| invalid("COMMIT without BEGIN"))?;
                if flags != 0
                    || commit_lsn != p.final_lsn
                    || end_lsn <= commit_lsn
                    || timestamp != p.timestamp
                    || end_lsn <= self.previous_end
                {
                    return Err(invalid("inconsistent PostgreSQL COMMIT boundary"));
                }
                self.previous_end = end_lsn;
                let mut rows = p.rows;
                for row in &mut rows {
                    row.source_cursor = cursor(end_lsn);
                }
                if rows.is_empty() {
                    return Ok(None);
                }
                let validated = validate_change_event(ChangeTransaction {
                    source: self.source.clone(),
                    id: format!("pg:{}:{}", p.xid, pg_walstream::format_lsn(end_lsn)),
                    begin_cursor: cursor(commit_lsn),
                    commit_cursor: cursor(end_lsn),
                    changes: rows,
                })?;
                return Ok(Some(validated));
            }
            M::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                let table=self.tables.get(&relation_id).ok_or_else(||invalid("new publication relation: restart with a checked schema; DDL capture is not supported"))?;
                if table.schema != namespace.as_ref()
                    || table.name != relation_name.as_ref()
                    || table.identity != replica_identity
                    || table.columns.len() != columns.len()
                    || !table.columns.iter().zip(&columns).all(|(a, b)| {
                        a.name == b.name.as_ref()
                            && a.oid == b.type_id
                            && a.modifier == b.type_modifier
                            && (replica_identity == b'f' || a.key.is_some() == b.is_key())
                    })
                {
                    return Err(invalid(
                        "pgoutput relation disagrees with checked catalog; schema changed",
                    ));
                }
                self.seen.insert(relation_id);
            }
            M::Insert { relation_id, tuple } => {
                let t = self.table(relation_id)?;
                let after = image(t, &tuple, false)?;
                self.row(relation_id, Operation::Insert, None, Some(after))?;
            }
            M::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                let t = self.table(relation_id)?;
                let after = image(t, &new_tuple, false)?;
                let before = match (key_type, old_tuple) {
                    (Some('K'), Some(old)) => image(t, &old, true)?,
                    (Some('O'), Some(old)) if t.identity == b'f' => image(t, &old, false)?,
                    (None, None) if t.identity == b'd' => after
                        .iter()
                        .map(|c| {
                            let mut c = c.clone();
                            if c.primary_key_ordinal.is_none() {
                                c.datum = Datum::Unavailable;
                            }
                            c
                        })
                        .collect(),
                    _ => return Err(invalid("invalid UPDATE old tuple/replica identity")),
                };
                self.row(relation_id, Operation::Update, Some(before), Some(after))?;
            }
            M::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                let t = self.table(relation_id)?;
                let key_only = match key_type {
                    'K' if t.identity == b'd' => true,
                    'O' if t.identity == b'f' => false,
                    _ => return Err(invalid("invalid DELETE identity")),
                };
                let before = image(t, &old_tuple, key_only)?;
                self.row(relation_id, Operation::Delete, Some(before), None)?;
            }
            M::Truncate { .. } => {
                return Err(invalid(
                    "TRUNCATE is not supported; capture stopped without acknowledging it",
                ));
            }
            M::Type { .. } => return Err(invalid("custom PostgreSQL types are not supported yet")),
            M::Origin { .. } => return Err(invalid("replicated origins are not supported yet")),
            _ => return Err(invalid("unexpected protocol message for pgoutput v1")),
        }
        Ok(None)
    }
    fn table(&self, oid: u32) -> Result<&Table> {
        if !self.seen.contains(&oid) {
            return Err(invalid("row arrived before its Relation message"));
        }
        self.tables
            .get(&oid)
            .ok_or_else(|| invalid("unknown relation"))
    }
    fn row(
        &mut self,
        oid: u32,
        operation: Operation,
        before: Option<Vec<ColumnDatum>>,
        after: Option<Vec<ColumnDatum>>,
    ) -> Result<()> {
        let t = self
            .tables
            .get(&oid)
            .ok_or_else(|| invalid("unknown table"))?;
        let p = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid("row outside a transaction"))?;
        if p.rows.len() >= 100_000 {
            return Err(invalid("transaction exceeds 100000 row capture limit"));
        }
        let seconds = p
            .timestamp
            .div_euclid(1_000_000)
            .checked_add(946_684_800)
            .ok_or_else(|| invalid("timestamp overflow"))?;
        p.rows.push(RowChange {
            database: Some(self.database.clone()),
            schema: t.schema.clone(),
            table: t.name.clone(),
            operation,
            source_cursor: cursor(p.final_lsn),
            source_timestamp: u32::try_from(seconds)?,
            schema_basis: format!("pgoutput+catalog:{}", t.oid),
            before,
            after,
        });
        Ok(())
    }
}
fn image(table: &Table, tuple: &TupleData, key_only: bool) -> Result<Vec<ColumnDatum>> {
    if tuple.columns.len() != table.columns.len() {
        return Err(invalid("tuple column count differs from Relation"));
    }
    table
        .columns
        .iter()
        .zip(&tuple.columns)
        .enumerate()
        .map(|(ordinal, (meta, data))| {
            let datum = if key_only && meta.key.is_none() {
                Datum::Unavailable
            } else {
                match data.data_type {
                    b'n' => Datum::Null,
                    b'u' => Datum::Unchanged,
                    b't' => Datum::Value(types::decode_column(
                        meta.oid,
                        &meta.native_type,
                        data.as_bytes(),
                    )?),
                    _ => {
                        return Err(invalid(
                            "unexpected binary/invalid pgoutput value; text transfer required",
                        ));
                    }
                }
            };
            Ok(ColumnDatum {
                ordinal,
                name: meta.name.clone(),
                native_type: meta.native_type.clone(),
                primary_key_ordinal: meta.key,
                generated: false,
                collation: None,
                datum,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog;
    use pg_walstream::{ColumnData, ColumnInfo, LogicalReplicationMessage as M, TupleData};

    fn table() -> Table {
        Table {
            oid: 42,
            schema: "public".into(),
            name: "items".into(),
            identity: b'd',
            columns: vec![
                catalog::Column {
                    name: "id".into(),
                    oid: 20,
                    modifier: -1,
                    native_type: "bigint".into(),
                    key: Some(0),
                },
                catalog::Column {
                    name: "message".into(),
                    oid: 25,
                    modifier: -1,
                    native_type: "text".into(),
                    key: None,
                },
            ],
        }
    }

    fn relation() -> M {
        M::Relation {
            relation_id: 42,
            namespace: "public".into(),
            relation_name: "items".into(),
            replica_identity: b'd',
            columns: vec![
                ColumnInfo::new(1, "id".into(), 20, -1),
                ColumnInfo::new(0, "message".into(), 25, -1),
            ],
        }
    }

    fn tuple(id: &str, message: &str) -> TupleData {
        TupleData::new(vec![
            ColumnData::text(id.as_bytes().to_vec()),
            ColumnData::text(message.as_bytes().to_vec()),
        ])
    }

    fn decoder() -> Decoder {
        Decoder::new(
            Source {
                kind: "postgresql".into(),
                version: "15.19".into(),
                id: "postgresql:123456:1:16384:Q0RDX3Rlc3Q".into(),
            },
            "CDC_test".into(),
            [(42, table())].into_iter().collect(),
            1024 * 1024,
        )
    }

    #[test]
    fn decodes_transaction_boundaries_and_all_row_operations() {
        let mut decoder = decoder();
        let timestamp = 1_700_000_000_i64 * 1_000_000;
        assert!(
            decoder
                .push(
                    M::Begin {
                        final_lsn: 16,
                        timestamp,
                        xid: 42,
                    },
                    1,
                )
                .unwrap()
                .is_none()
        );
        decoder.push(relation(), 1).unwrap();
        decoder
            .push(
                M::Insert {
                    relation_id: 42,
                    tuple: tuple("7", "inserted"),
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Update {
                    relation_id: 42,
                    old_tuple: Some(tuple("7", "old")),
                    new_tuple: tuple("8", "updated"),
                    key_type: Some('K'),
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Delete {
                    relation_id: 42,
                    old_tuple: tuple("8", "updated"),
                    key_type: 'K',
                },
                1,
            )
            .unwrap();
        let validated = decoder
            .push(
                M::Commit {
                    flags: 0,
                    commit_lsn: 16,
                    end_lsn: 32,
                    timestamp,
                },
                1,
            )
            .unwrap()
            .expect("commit with rows should be emitted");
        let tx = validated.transaction();
        assert_eq!(tx.changes.len(), 3);
        assert!(matches!(tx.changes[0].operation, Operation::Insert));
        assert!(matches!(tx.changes[1].operation, Operation::Update));
        assert!(matches!(tx.changes[2].operation, Operation::Delete));
        assert_eq!(tx.begin_cursor.value, "0/10");
        assert_eq!(tx.commit_cursor.value, "0/20");
        assert!(
            tx.changes
                .iter()
                .all(|change| change.source_cursor.value == "0/20")
        );
        assert!(matches!(
            &tx.changes[2].before.as_ref().unwrap()[1].datum,
            Datum::Unavailable
        ));
        assert_eq!(tx.changes[0].source_timestamp, 2_646_684_800);
    }

    #[test]
    fn rejects_non_monotonic_begin_lsn() {
        let mut decoder = decoder();
        let timestamp = 1_700_000_000_i64 * 1_000_000;
        decoder
            .push(
                M::Begin {
                    final_lsn: 16,
                    timestamp,
                    xid: 42,
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Commit {
                    flags: 0,
                    commit_lsn: 16,
                    end_lsn: 32,
                    timestamp,
                },
                1,
            )
            .unwrap();
        assert!(
            decoder
                .push(
                    M::Begin {
                        final_lsn: 32,
                        timestamp,
                        xid: 43,
                    },
                    1,
                )
                .is_err()
        );
    }
}
