//! Snapshot rows have a boundary, but are not binlog transactions.
use crate::{
    ColumnDatum, Datum, Operation, Source, ValidationError,
    validate::{ensure, image},
};

#[derive(Debug, Clone)]
pub struct SnapshotTable {
    pub schema: String,
    pub table: String,
    /// Empty means all columns; every primary-key column must be included.
    pub columns: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct SnapshotBoundary {
    pub source: Source,
    /// The source-defined snapshot boundary. The core validates only that it is present;
    /// the Source Contract owns its encoding, ordering, and recovery meaning.
    pub cursor: crate::SourceCursor,
}
#[derive(Debug, Clone)]
pub struct SnapshotBatch {
    pub source: Source,
    pub schema: String,
    pub table: String,
    pub rows: Vec<Vec<ColumnDatum>>,
    /// Every table, including an empty one, emits a final batch.
    pub last_in_table: bool,
}

pub fn validate_snapshot_boundary(boundary: &SnapshotBoundary) -> Result<(), ValidationError> {
    ensure(
        !boundary.source.kind.is_empty()
            && !boundary.source.version.is_empty()
            && !boundary.source.id.is_empty(),
        "incomplete snapshot source",
    )?;
    ensure(
        !boundary.cursor.format.is_empty() && !boundary.cursor.value.is_empty(),
        "snapshot cursor is incomplete",
    )
}

pub struct ValidatedSnapshotBatch(SnapshotBatch);
impl ValidatedSnapshotBatch {
    pub fn batch(&self) -> &SnapshotBatch {
        &self.0
    }
}
pub fn validate_snapshot(batch: SnapshotBatch) -> Result<ValidatedSnapshotBatch, ValidationError> {
    ensure(
        !batch.source.kind.is_empty()
            && !batch.source.version.is_empty()
            && !batch.source.id.is_empty(),
        "incomplete snapshot source",
    )?;
    ensure(
        !batch.schema.is_empty()
            && !batch.table.is_empty()
            && !batch.schema.contains('\0')
            && !batch.table.contains('\0'),
        "invalid snapshot table",
    )?;
    ensure(
        !batch.rows.is_empty() || batch.last_in_table,
        "empty non-final snapshot batch",
    )?;
    for row in &batch.rows {
        image(row, Operation::Insert, false)?;
        ensure(
            row.iter()
                .all(|c| matches!(c.datum, Datum::Null | Datum::Value(_))),
            "snapshot rows must contain complete values",
        )?;
        ensure(
            row.iter().any(|c| c.primary_key_ordinal.is_some()),
            "snapshot requires primary keys",
        )?;
    }
    Ok(ValidatedSnapshotBatch(batch))
}
