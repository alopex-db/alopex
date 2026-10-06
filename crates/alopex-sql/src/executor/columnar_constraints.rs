//! Shared key validation for columnar COPY and post-load UNIQUE declarations.
//! This validates segment contents; it does not maintain a physical B-tree.

use crate::catalog::{ColumnMetadata, IndexMetadata, TableMetadata};
use crate::executor::memory::{DEFAULT_SPILL_THRESHOLD_BYTES, MemoryPolicy, SpillPolicy};
use crate::executor::query::iterator::{RowIterator, SortIterator};
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::{ResolvedType, SortExpr, TypedExpr};
use crate::storage::{KeyEncoder, SqlValue, StorageError};

pub(super) fn validate_index_columns(table: &TableMetadata, index: &IndexMetadata) -> Result<()> {
    if index.column_indices.is_empty()
        || index.column_indices.len() != index.columns.len()
        || index
            .column_indices
            .iter()
            .zip(&index.columns)
            .any(|(&position, name)| {
                table
                    .columns
                    .get(position)
                    .is_none_or(|column| column.name != *name)
            })
    {
        return Err(ExecutorError::BulkLoad(format!(
            "invalid constraint metadata: {}",
            index.name
        )));
    }
    Ok(())
}

pub(super) fn key(
    index: &IndexMetadata,
    row: &[SqlValue],
) -> std::result::Result<Option<Vec<u8>>, StorageError> {
    let mut has_null = false;
    for &position in &index.column_indices {
        let value = row
            .get(position)
            .ok_or_else(|| StorageError::TypeMismatch {
                expected: format!("row with column {position}"),
                actual: format!("{} columns", row.len()),
            })?;
        has_null |= value.is_null();
    }
    if has_null {
        return Ok(None);
    }
    if index.column_indices.len() == 1 {
        KeyEncoder::index_value_prefix(index.index_id, &row[index.column_indices[0]]).map(Some)
    } else {
        let values = index
            .column_indices
            .iter()
            .map(|&position| row[position].clone())
            .collect::<Vec<_>>();
        KeyEncoder::composite_index_prefix(index.index_id, &values).map(Some)
    }
}

pub(super) fn memory_policy(policy: Option<&MemoryPolicy>) -> MemoryPolicy {
    policy.cloned().unwrap_or_else(|| {
        MemoryPolicy::new(
            Some(DEFAULT_SPILL_THRESHOLD_BYTES),
            SpillPolicy::SpillToDisk {
                directory: std::env::temp_dir(),
            },
        )
    })
}

/// The input RowID identifies the constraint, not a segment row locator.
/// The caller owns PK-versus-UNIQUE error classification.
pub(super) fn duplicate_constraint(
    next: impl FnMut() -> Result<Option<Row>>,
    policy: MemoryPolicy,
) -> Result<Option<usize>> {
    let input = ConstraintKeys {
        next,
        schema: [ColumnMetadata::new("constraint_key", ResolvedType::Blob)],
    };
    let order = [SortExpr {
        expr: TypedExpr::column_ref(
            String::new(),
            "constraint_key".into(),
            0,
            ResolvedType::Blob,
            crate::Span::default(),
        ),
        asc: true,
        nulls_first: false,
    }];
    let mut sorted = SortIterator::new_with_policy(input, &order, Some(policy))?;
    let mut previous = None;
    while let Some(row) = sorted.next_row() {
        let row = row?;
        if previous.as_ref() == Some(&row.values) {
            return Ok(Some(row.row_id as usize));
        }
        previous = Some(row.values);
    }
    Ok(None)
}

struct ConstraintKeys<F> {
    next: F,
    schema: [ColumnMetadata; 1],
}

impl<F: FnMut() -> Result<Option<Row>>> RowIterator for ConstraintKeys<F> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        (self.next)().transpose()
    }
    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}
