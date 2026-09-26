use crate::{Result, catalog::Table, cursor, invalid, types, validate_change_event};
use change_event::{
    ChangeTransaction, ColumnDatum, ConnectorIdentity, Datum, Operation, RowChange,
    ServerBuildIdentity, Source, SourceRepresentationContext, SourceRepresentationEnvelope,
    SourceRepresentationFormat, ValidatedTransaction,
};
use pg_walstream::{LogicalReplicationMessage as M, TupleData};
use std::collections::{BTreeMap, HashMap, HashSet};
struct Pending {
    xid: u32,
    final_lsn: u64,
    timestamp: i64,
    bytes: usize,
    rows: Vec<RowChange>,
}
pub(crate) struct Decoder {
    pub source: Source,
    server_build: ServerBuildIdentity,
    lc_monetary: String,
    database: String,
    tables: HashMap<u32, Table>,
    type_names: HashMap<u32, (String, String)>,
    seen: HashSet<u32>,
    pending: Option<Pending>,
    previous_end: u64,
    max_bytes: usize,
}
impl Decoder {
    pub fn new(
        source: Source,
        server_build: ServerBuildIdentity,
        lc_monetary: String,
        database: String,
        tables: HashMap<u32, Table>,
        type_names: HashMap<u32, (String, String)>,
        max_bytes: usize,
    ) -> Self {
        Self {
            source,
            server_build,
            lc_monetary,
            database,
            tables,
            type_names,
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
                    for column in row.before.iter_mut().chain(row.after.iter_mut()).flatten() {
                        if let Datum::SourceRepresentationEnvelope(envelope) = &mut column.datum {
                            envelope.context.source_cursor = cursor(end_lsn);
                        }
                    }
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
                let source_cursor = self.current_cursor()?;
                let after = image(
                    t,
                    &tuple,
                    false,
                    &self.source,
                    &self.server_build,
                    &self.lc_monetary,
                    source_cursor,
                )?;
                self.row(relation_id, Operation::Insert, None, Some(after))?;
            }
            M::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                let t = self.table(relation_id)?;
                let source_cursor = self.current_cursor()?;
                let after = image(
                    t,
                    &new_tuple,
                    false,
                    &self.source,
                    &self.server_build,
                    &self.lc_monetary,
                    source_cursor.clone(),
                )?;
                let before = match (key_type, old_tuple) {
                    (Some('K'), Some(old)) => image(
                        t,
                        &old,
                        true,
                        &self.source,
                        &self.server_build,
                        &self.lc_monetary,
                        source_cursor.clone(),
                    )?,
                    (Some('O'), Some(old)) if t.identity == b'f' => image(
                        t,
                        &old,
                        false,
                        &self.source,
                        &self.server_build,
                        &self.lc_monetary,
                        source_cursor.clone(),
                    )?,
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
                let source_cursor = self.current_cursor()?;
                let key_only = match key_type {
                    'K' if t.identity == b'd' => true,
                    'O' if t.identity == b'f' => false,
                    _ => return Err(invalid("invalid DELETE identity")),
                };
                let before = image(
                    t,
                    &old_tuple,
                    key_only,
                    &self.source,
                    &self.server_build,
                    &self.lc_monetary,
                    source_cursor,
                )?;
                self.row(relation_id, Operation::Delete, Some(before), None)?;
            }
            M::Truncate { .. } => {
                return Err(invalid(
                    "TRUNCATE is not supported; capture stopped without acknowledging it",
                ));
            }
            M::Type {
                type_id,
                namespace,
                type_name,
            } => {
                let Some((expected_schema, expected_name)) = self.type_names.get(&type_id) else {
                    return Err(invalid(format!(
                        "pgoutput TYPE references OID {type_id} absent from the checked PostgreSQL type catalog"
                    )));
                };
                if expected_schema != &namespace || expected_name != &type_name {
                    return Err(invalid(format!(
                        "pgoutput TYPE identity for OID {type_id} disagrees with the checked type catalog"
                    )));
                }
            }
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
    fn current_cursor(&self) -> Result<change_event::SourceCursor> {
        self.pending
            .as_ref()
            .map(|pending| cursor(pending.final_lsn))
            .ok_or_else(|| invalid("row outside a transaction"))
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
            schema_basis: format!(
                "pgoutput+catalog:{}:{}",
                t.oid, t.source_type_catalog_digest
            ),
            before,
            after,
        });
        Ok(())
    }
}
fn image(
    table: &Table,
    tuple: &TupleData,
    key_only: bool,
    source: &Source,
    server_build: &ServerBuildIdentity,
    lc_monetary: &str,
    source_cursor: change_event::SourceCursor,
) -> Result<Vec<ColumnDatum>> {
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
                    b't' if meta.representation_only && meta.representation_capture_allowed => {
                        let type_metadata = BTreeMap::from([
                            ("relation_oid".into(), table.oid.to_string()),
                            ("relation_schema".into(), table.schema.clone()),
                            ("relation_name".into(), table.name.clone()),
                            ("column_name".into(), meta.name.clone()),
                            ("column_oid".into(), meta.oid.to_string()),
                            ("type_modifier".into(), meta.modifier.to_string()),
                            ("native_type".into(), meta.native_type.clone()),
                            ("type_schema".into(), meta.type_schema.clone()),
                            ("type_name".into(), meta.type_name.clone()),
                            (
                                "type_definition".into(),
                                meta.type_definition_evidence.clone(),
                            ),
                            (
                                "type_definition_digest".into(),
                                format!("sha256:{}", meta.type_definition_digest),
                            ),
                            (
                                "source_catalog_digest".into(),
                                table.source_type_catalog_digest.clone(),
                            ),
                            ("text_output_profile".into(), "pgoutput-v1/text/UTF8".into()),
                            ("session.datestyle".into(), "ISO, YMD".into()),
                            ("session.intervalstyle".into(), "iso_8601".into()),
                            ("session.timezone".into(), "UTC".into()),
                            ("session.bytea_output".into(), "hex".into()),
                            ("session.extra_float_digits".into(), "3".into()),
                            ("session.search_path".into(), "pg_catalog".into()),
                            ("environment.lc_monetary".into(), lc_monetary.into()),
                        ]);
                        let context = SourceRepresentationContext {
                            connector: ConnectorIdentity::new(
                                "postgresql",
                                source.version.split('.').next().unwrap_or("15"),
                            ),
                            server_build: server_build.clone(),
                            source_type_identity: format!(
                                "postgresql.pg_type.v1:{}:{}.{}",
                                meta.oid, meta.type_schema, meta.type_name
                            ),
                            source_type_definition_digest: format!(
                                "sha256:{}",
                                meta.type_definition_digest
                            ),
                            protocol: "pgoutput.v1".into(),
                            format: SourceRepresentationFormat::Text,
                            type_metadata,
                            source_cursor: source_cursor.clone(),
                        };
                        Datum::SourceRepresentationEnvelope(SourceRepresentationEnvelope::new(
                            context,
                            "UTF-8",
                            data.as_bytes(),
                        ))
                    }
                    b't' => Datum::Value(types::decode_column_for_version(
                        source.version.split('.').next().unwrap_or("15"),
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
            source_type_catalog_digest: "0".repeat(64),
            columns: vec![
                catalog::Column {
                    name: "id".into(),
                    oid: 20,
                    modifier: -1,
                    native_type: "bigint".into(),
                    type_schema: "pg_catalog".into(),
                    type_name: "int8".into(),
                    type_definition_digest: "1".repeat(64),
                    type_definition_evidence: format!(
                        "{{\"oid\":20,\"schema\":\"pg_catalog\",\"name\":\"int8\",\"kind\":{{\"Builtin\":{{\"native_type\":\"int8\"}}}},\"collation\":null,\"definition_digest\":\"{}\"}}",
                        "1".repeat(64)
                    ),
                    representation_only: false,
                    representation_capture_allowed: false,
                    key: Some(0),
                },
                catalog::Column {
                    name: "message".into(),
                    oid: 25,
                    modifier: -1,
                    native_type: "text".into(),
                    type_schema: "pg_catalog".into(),
                    type_name: "text".into(),
                    type_definition_digest: "2".repeat(64),
                    type_definition_evidence: format!(
                        "{{\"oid\":25,\"schema\":\"pg_catalog\",\"name\":\"text\",\"kind\":{{\"Builtin\":{{\"native_type\":\"text\"}}}},\"collation\":null,\"definition_digest\":\"{}\"}}",
                        "2".repeat(64)
                    ),
                    representation_only: false,
                    representation_capture_allowed: false,
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
            ServerBuildIdentity::new("postgresql", "community", "15.19", "PostgreSQL 15.19"),
            "C".into(),
            "CDC_test".into(),
            [(42, table())].into_iter().collect(),
            [
                (20, ("pg_catalog".into(), "int8".into())),
                (25, ("pg_catalog".into(), "text".into())),
            ]
            .into_iter()
            .collect(),
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
    fn captures_unmapped_builtin_text_with_catalog_evidence_and_json_replay() {
        let money = crate::SourceTypeDefinition::builtin(790, "pg_catalog", "money");
        let money_column = catalog::Column {
            name: "amount".into(),
            oid: money.oid,
            modifier: -1,
            native_type: "money".into(),
            type_schema: money.schema.clone(),
            type_name: money.name.clone(),
            type_definition_digest: money.definition_digest.clone(),
            type_definition_evidence: serde_json::to_string(&money).unwrap(),
            representation_only: true,
            representation_capture_allowed: true,
            key: None,
        };
        let table = Table {
            oid: 43,
            schema: "public".into(),
            name: "money_values".into(),
            identity: b'd',
            source_type_catalog_digest: "a".repeat(64),
            columns: vec![
                catalog::Column {
                    name: "id".into(),
                    oid: 20,
                    modifier: -1,
                    native_type: "bigint".into(),
                    type_schema: "pg_catalog".into(),
                    type_name: "int8".into(),
                    type_definition_digest: "1".repeat(64),
                    type_definition_evidence: "{}".into(),
                    representation_only: false,
                    representation_capture_allowed: false,
                    key: Some(0),
                },
                money_column,
            ],
        };
        let mut decoder = Decoder::new(
            Source {
                kind: "postgresql".into(),
                version: "15.19".into(),
                id: "postgresql:123456:1:16384:Q0RDX3Rlc3Q".into(),
            },
            ServerBuildIdentity::new("postgresql", "community", "15.19", "PostgreSQL 15.19"),
            "C".into(),
            "CDC_test".into(),
            [(43, table)].into_iter().collect(),
            [
                (20, ("pg_catalog".into(), "int8".into())),
                (790, ("pg_catalog".into(), "money".into())),
            ]
            .into_iter()
            .collect(),
            1024 * 1024,
        );
        let timestamp = 1_700_000_000_i64 * 1_000_000;
        decoder
            .push(
                M::Begin {
                    final_lsn: 16,
                    timestamp,
                    xid: 44,
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Type {
                    type_id: 790,
                    namespace: "pg_catalog".into(),
                    type_name: "money".into(),
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Relation {
                    relation_id: 43,
                    namespace: "public".into(),
                    relation_name: "money_values".into(),
                    replica_identity: b'd',
                    columns: vec![
                        ColumnInfo::new(1, "id".into(), 20, -1),
                        ColumnInfo::new(0, "amount".into(), 790, -1),
                    ],
                },
                1,
            )
            .unwrap();
        decoder
            .push(
                M::Insert {
                    relation_id: 43,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"7".to_vec()),
                        ColumnData::text(b"$1.00".to_vec()),
                    ]),
                },
                1,
            )
            .unwrap();
        let transaction = decoder
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
            .unwrap();
        let row = &transaction.transaction().changes[0];
        let envelope = match &row.after.as_ref().unwrap()[1].datum {
            Datum::SourceRepresentationEnvelope(envelope) => envelope,
            datum => panic!("expected source representation, got {datum:?}"),
        };
        assert_eq!(envelope.raw_bytes().unwrap(), b"$1.00");
        assert_eq!(envelope.context.type_metadata["column_oid"], "790");
        assert_eq!(envelope.context.type_metadata["type_name"], "money");
        assert_eq!(envelope.context.source_cursor, row.source_cursor);

        let json = change_event::json(&transaction).unwrap();
        let mut reader = change_event::JsonReader::new(std::io::Cursor::new(json));
        let replay = reader.next_transaction().unwrap().unwrap();
        assert_eq!(
            change_event::json(&replay).unwrap(),
            change_event::json(&transaction).unwrap()
        );
        reader.finish().unwrap();

        let mut tampered = transaction.transaction().clone();
        let Datum::SourceRepresentationEnvelope(envelope) =
            &mut tampered.changes[0].after.as_mut().unwrap()[1].datum
        else {
            unreachable!();
        };
        let mut definition: serde_json::Value =
            serde_json::from_str(&envelope.context.type_metadata["type_definition"]).unwrap();
        definition["name"] = serde_json::Value::String("text".into());
        envelope.context.type_metadata.insert(
            "type_definition".into(),
            serde_json::to_string(&definition).unwrap(),
        );
        let error = crate::validate_change_event(tampered).unwrap_err();
        assert!(error.to_string().contains("source representation context"));
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
