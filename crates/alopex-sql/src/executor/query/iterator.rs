//! Iterator-based query execution pipeline.
//!
//! This module provides an iterator-based execution model for SQL queries,
//! enabling streaming execution and reduced memory usage for large datasets.
//!
//! # Architecture
//!
//! The execution pipeline is built from composable iterators:
//! - [`ScanIterator`]: Reads rows from storage
//! - [`FilterIterator`]: Filters rows based on predicates
//! - [`SortIterator`]: Sorts rows (requires materialization)
//! - [`LimitIterator`]: Applies LIMIT/OFFSET constraints
//!
//! Each iterator implements the [`RowIterator`] trait, allowing them to be
//! composed into a pipeline that processes rows one at a time.

use std::cmp::Ordering;
use std::marker::PhantomData;

use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::executor::evaluator::EvalContext;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::typed_expr::{SortExpr, TypedExpr};
use crate::storage::{SqlValue, TableScanIterator};

/// A trait for row-producing iterators in the query execution pipeline.
///
/// This trait abstracts over different types of iterators (scan, filter, sort, etc.)
/// allowing them to be composed into execution pipelines.
pub trait RowIterator {
    /// Advances the iterator and returns the next row, or `None` if exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails (e.g., storage errors,
    /// evaluation errors).
    fn next_row(&mut self) -> Option<Result<Row>>;

    /// Returns the schema of rows produced by this iterator.
    fn schema(&self) -> &[ColumnMetadata];
}

// Implement RowIterator for Box<dyn RowIterator> to allow dynamic dispatch.
impl RowIterator for Box<dyn RowIterator + '_> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        (**self).next_row()
    }

    fn schema(&self) -> &[ColumnMetadata] {
        (**self).schema()
    }
}

// ============================================================================
// ScanIterator - Reads rows from storage (reserved for future streaming)
// ============================================================================

/// Iterator that reads rows from table storage.
///
/// This is the leaf node in the iterator tree, providing rows from the
/// underlying storage layer.
///
/// **Note**: Currently unused due to lifetime constraints with SqlTransaction.
/// Reserved for future true streaming execution when those constraints are resolved.
/// The current implementation uses VecIterator after collecting all rows from scan.
#[allow(dead_code)]
pub struct ScanIterator<'a> {
    inner: TableScanIterator<'a>,
    schema: Vec<ColumnMetadata>,
}

#[allow(dead_code)]
impl<'a> ScanIterator<'a> {
    /// Creates a new scan iterator from a table scan iterator and metadata.
    pub fn new(inner: TableScanIterator<'a>, table_meta: &TableMetadata) -> Self {
        Self {
            inner,
            schema: table_meta.columns.clone(),
        }
    }
}

impl RowIterator for ScanIterator<'_> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.inner.next().map(|result| {
            result
                .map(|(row_id, values)| Row::new(row_id, values))
                .map_err(ExecutorError::from)
        })
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

// ============================================================================
// FilterIterator - Filters rows based on a predicate
// ============================================================================

/// Iterator that filters rows based on a predicate expression.
///
/// Only rows where the predicate evaluates to `true` are yielded.
/// Rows where the predicate evaluates to `false` or `NULL` are skipped.
pub struct FilterIterator<I: RowIterator> {
    input: I,
    predicate: TypedExpr,
}

impl<I: RowIterator> FilterIterator<I> {
    /// Creates a new filter iterator with the given input and predicate.
    pub fn new(input: I, predicate: TypedExpr) -> Self {
        Self { input, predicate }
    }
}

impl<I: RowIterator> RowIterator for FilterIterator<I> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        loop {
            match self.input.next_row()? {
                Ok(row) => {
                    let ctx = EvalContext::new(&row.values);
                    match crate::executor::evaluator::evaluate(&self.predicate, &ctx) {
                        Ok(SqlValue::Boolean(true)) => return Some(Ok(row)),
                        Ok(_) => continue, // false or null - skip this row
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn schema(&self) -> &[ColumnMetadata] {
        self.input.schema()
    }
}

// ============================================================================
// SortIterator - Sorts rows (materializes all input)
// ============================================================================

/// Iterator that sorts rows according to ORDER BY expressions.
///
/// **Note**: Sorting requires materializing all input rows into memory.
/// This iterator collects all rows from its input, sorts them, and then
/// yields them one at a time.
pub struct SortIterator<I: RowIterator> {
    /// Sorted rows ready for iteration.
    sorted_rows: std::vec::IntoIter<Row>,
    /// Schema from input.
    schema: Vec<ColumnMetadata>,
    /// Marker for input iterator type.
    _marker: PhantomData<I>,
}

impl<I: RowIterator> SortIterator<I> {
    /// Creates a new sort iterator.
    ///
    /// This constructor immediately materializes all input rows and sorts them.
    ///
    /// # Errors
    ///
    /// Returns an error if reading from input fails or if sort key evaluation fails.
    pub fn new(mut input: I, order_by: &[SortExpr]) -> Result<Self> {
        let schema = input.schema().to_vec();

        // Collect all rows from input
        let mut rows = Vec::new();
        while let Some(result) = input.next_row() {
            rows.push(result?);
        }

        if order_by.is_empty() {
            return Ok(Self {
                sorted_rows: rows.into_iter(),
                schema,
                _marker: PhantomData,
            });
        }

        // Precompute sort keys to avoid repeated evaluation during comparisons
        let mut keyed: Vec<(Row, Vec<SqlValue>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(order_by.len());
            for expr in order_by {
                let ctx = EvalContext::new(&row.values);
                keys.push(crate::executor::evaluator::evaluate(&expr.expr, &ctx)?);
            }
            keyed.push((row, keys));
        }

        // Sort by keys
        keyed.sort_by(|a, b| compare_keys(a, b, order_by));

        let sorted: Vec<Row> = keyed.into_iter().map(|(row, _)| row).collect();

        Ok(Self {
            sorted_rows: sorted.into_iter(),
            schema,
            _marker: PhantomData,
        })
    }
}

impl<I: RowIterator> RowIterator for SortIterator<I> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.sorted_rows.next().map(Ok)
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

/// Compare two rows by their precomputed sort keys.
fn compare_keys(
    a: &(Row, Vec<SqlValue>),
    b: &(Row, Vec<SqlValue>),
    order_by: &[SortExpr],
) -> Ordering {
    for (i, sort_expr) in order_by.iter().enumerate() {
        let left = &a.1[i];
        let right = &b.1[i];
        let cmp = compare_single(left, right, sort_expr.asc, sort_expr.nulls_first);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Compare two SqlValues according to sort direction and NULL ordering.
fn compare_single(left: &SqlValue, right: &SqlValue, asc: bool, nulls_first: bool) -> Ordering {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => match left.partial_cmp(right).unwrap_or(Ordering::Equal) {
            Ordering::Equal => Ordering::Equal,
            ord if asc => ord,
            ord => ord.reverse(),
        },
    }
}

// ============================================================================
// LimitIterator - Applies LIMIT and OFFSET
// ============================================================================

/// Iterator that applies LIMIT and OFFSET constraints.
///
/// This iterator skips the first `offset` rows and yields at most `limit` rows.
/// It provides early termination - once the limit is reached, no more rows
/// are requested from the input.
pub struct LimitIterator<I: RowIterator> {
    input: I,
    limit: Option<u64>,
    offset: u64,
    /// Number of rows skipped so far (for OFFSET).
    skipped: u64,
    /// Number of rows yielded so far (for LIMIT).
    yielded: u64,
}

impl<I: RowIterator> LimitIterator<I> {
    /// Creates a new limit iterator with the given LIMIT and OFFSET.
    pub fn new(input: I, limit: Option<u64>, offset: Option<u64>) -> Self {
        Self {
            input,
            limit,
            offset: offset.unwrap_or(0),
            skipped: 0,
            yielded: 0,
        }
    }
}

impl<I: RowIterator> RowIterator for LimitIterator<I> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        // Check if limit already reached
        if let Some(limit) = self.limit {
            if self.yielded >= limit {
                return None;
            }
        }

        loop {
            match self.input.next_row()? {
                Ok(row) => {
                    // Skip rows for OFFSET
                    if self.skipped < self.offset {
                        self.skipped += 1;
                        continue;
                    }

                    // Check limit again after skipping
                    if let Some(limit) = self.limit {
                        if self.yielded >= limit {
                            return None;
                        }
                    }

                    self.yielded += 1;
                    return Some(Ok(row));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn schema(&self) -> &[ColumnMetadata] {
        self.input.schema()
    }
}

// ============================================================================
// VecIterator - Wraps a Vec<Row> for testing and compatibility
// ============================================================================

/// Iterator that wraps a `Vec<Row>` for testing and compatibility.
///
/// This is useful for converting materialized results back into an iterator
/// or for testing iterator-based code with fixed data.
pub struct VecIterator {
    rows: std::vec::IntoIter<Row>,
    schema: Vec<ColumnMetadata>,
}

impl VecIterator {
    /// Creates a new vec iterator from rows and schema.
    pub fn new(rows: Vec<Row>, schema: Vec<ColumnMetadata>) -> Self {
        Self {
            rows: rows.into_iter(),
            schema,
        }
    }
}

impl RowIterator for VecIterator {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.rows.next().map(Ok)
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::types::ResolvedType;
    use crate::Span;

    fn sample_schema() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
        ]
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            Row::new(1, vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]),
            Row::new(2, vec![SqlValue::Integer(2), SqlValue::Text("bob".into())]),
            Row::new(3, vec![SqlValue::Integer(3), SqlValue::Text("carol".into())]),
            Row::new(4, vec![SqlValue::Integer(4), SqlValue::Text("dave".into())]),
            Row::new(5, vec![SqlValue::Integer(5), SqlValue::Text("eve".into())]),
        ]
    }

    #[test]
    fn vec_iterator_returns_all_rows() {
        let rows = sample_rows();
        let expected_len = rows.len();
        let mut iter = VecIterator::new(rows, sample_schema());

        let mut count = 0;
        while let Some(Ok(_)) = iter.next_row() {
            count += 1;
        }
        assert_eq!(count, expected_len);
    }

    #[test]
    fn filter_iterator_filters_rows() {
        use crate::ast::expr::BinaryOp;
        use crate::planner::typed_expr::{TypedExpr, TypedExprKind};

        let rows = sample_rows();
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        // Filter: id > 2
        let predicate = TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: TypedExprKind::ColumnRef {
                        table: "test".into(),
                        column: "id".into(),
                        column_index: 0,
                    },
                    resolved_type: ResolvedType::Integer,
                    span: Span::default(),
                }),
                op: BinaryOp::Gt,
                right: Box::new(TypedExpr::literal(
                    crate::ast::expr::Literal::Number("2".into()),
                    ResolvedType::Integer,
                    Span::default(),
                )),
            },
            resolved_type: ResolvedType::Boolean,
            span: Span::default(),
        };

        let mut filter = FilterIterator::new(input, predicate);

        let mut results = Vec::new();
        while let Some(Ok(row)) = filter.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].row_id, 3);
        assert_eq!(results[1].row_id, 4);
        assert_eq!(results[2].row_id, 5);
    }

    #[test]
    fn limit_iterator_limits_rows() {
        let rows = sample_rows();
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        let mut limit = LimitIterator::new(input, Some(2), None);

        let mut results = Vec::new();
        while let Some(Ok(row)) = limit.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 1);
        assert_eq!(results[1].row_id, 2);
    }

    #[test]
    fn limit_iterator_applies_offset() {
        let rows = sample_rows();
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        let mut limit = LimitIterator::new(input, Some(2), Some(2));

        let mut results = Vec::new();
        while let Some(Ok(row)) = limit.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 3);
        assert_eq!(results[1].row_id, 4);
    }

    #[test]
    fn limit_iterator_offset_only() {
        let rows = sample_rows();
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        let mut limit = LimitIterator::new(input, None, Some(3));

        let mut results = Vec::new();
        while let Some(Ok(row)) = limit.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 4);
        assert_eq!(results[1].row_id, 5);
    }

    #[test]
    fn sort_iterator_sorts_rows() {
        use crate::planner::typed_expr::{SortExpr, TypedExpr, TypedExprKind};

        let rows = vec![
            Row::new(1, vec![SqlValue::Integer(3), SqlValue::Text("carol".into())]),
            Row::new(2, vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]),
            Row::new(3, vec![SqlValue::Integer(2), SqlValue::Text("bob".into())]),
        ];
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        // Sort by id ASC
        let order_by = vec![SortExpr {
            expr: TypedExpr {
                kind: TypedExprKind::ColumnRef {
                    table: "test".into(),
                    column: "id".into(),
                    column_index: 0,
                },
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            },
            asc: true,
            nulls_first: false,
        }];

        let mut sort = SortIterator::new(input, &order_by).unwrap();

        let mut results = Vec::new();
        while let Some(Ok(row)) = sort.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].values[0], SqlValue::Integer(1));
        assert_eq!(results[1].values[0], SqlValue::Integer(2));
        assert_eq!(results[2].values[0], SqlValue::Integer(3));
    }

    #[test]
    fn sort_iterator_sorts_descending() {
        use crate::planner::typed_expr::{SortExpr, TypedExpr, TypedExprKind};

        let rows = vec![
            Row::new(1, vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]),
            Row::new(2, vec![SqlValue::Integer(3), SqlValue::Text("carol".into())]),
            Row::new(3, vec![SqlValue::Integer(2), SqlValue::Text("bob".into())]),
        ];
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        // Sort by id DESC
        let order_by = vec![SortExpr {
            expr: TypedExpr {
                kind: TypedExprKind::ColumnRef {
                    table: "test".into(),
                    column: "id".into(),
                    column_index: 0,
                },
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            },
            asc: false,
            nulls_first: false,
        }];

        let mut sort = SortIterator::new(input, &order_by).unwrap();

        let mut results = Vec::new();
        while let Some(Ok(row)) = sort.next_row() {
            results.push(row);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].values[0], SqlValue::Integer(3));
        assert_eq!(results[1].values[0], SqlValue::Integer(2));
        assert_eq!(results[2].values[0], SqlValue::Integer(1));
    }

    #[test]
    fn composed_pipeline_filter_then_limit() {
        use crate::ast::expr::BinaryOp;
        use crate::planner::typed_expr::{TypedExpr, TypedExprKind};

        let rows = sample_rows();
        let schema = sample_schema();
        let input = VecIterator::new(rows, schema);

        // Filter: id > 1
        let predicate = TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: TypedExprKind::ColumnRef {
                        table: "test".into(),
                        column: "id".into(),
                        column_index: 0,
                    },
                    resolved_type: ResolvedType::Integer,
                    span: Span::default(),
                }),
                op: BinaryOp::Gt,
                right: Box::new(TypedExpr::literal(
                    crate::ast::expr::Literal::Number("1".into()),
                    ResolvedType::Integer,
                    Span::default(),
                )),
            },
            resolved_type: ResolvedType::Boolean,
            span: Span::default(),
        };

        let filtered = FilterIterator::new(input, predicate);
        let mut limited = LimitIterator::new(filtered, Some(2), None);

        let mut results = Vec::new();
        while let Some(Ok(row)) = limited.next_row() {
            results.push(row);
        }

        // Should get rows 2, 3 (id > 1, then limit 2)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 2);
        assert_eq!(results[1].row_id, 3);
    }
}
