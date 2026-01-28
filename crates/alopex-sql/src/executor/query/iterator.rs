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
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::executor::evaluator::EvalContext;
use crate::executor::memory::{MemoryPolicy, MemoryTracker};
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::typed_expr::{SortExpr, TypedExpr};
use crate::storage::{RowCodec, SqlValue, TableScanIterator};

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
// ScanIterator - Reads rows from storage for true streaming execution
// ============================================================================

/// Iterator that reads rows from table storage.
///
/// This is the leaf node in the iterator tree, providing rows from the
/// underlying storage layer. Used for FR-7 streaming output compliance.
pub struct ScanIterator<'a> {
    inner: TableScanIterator<'a>,
    schema: Vec<ColumnMetadata>,
}

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
    output: SortOutput,
    /// Schema from input.
    schema: Vec<ColumnMetadata>,
    /// Marker for input iterator type.
    _marker: PhantomData<I>,
}

enum SortOutput {
    InMemory(std::vec::IntoIter<Row>),
    External(ExternalSortState),
}

impl<I: RowIterator> SortIterator<I> {
    /// Creates a new sort iterator.
    ///
    /// This constructor immediately materializes all input rows and sorts them.
    ///
    /// # Errors
    ///
    /// Returns an error if reading from input fails or if sort key evaluation fails.
    pub fn new(input: I, order_by: &[SortExpr]) -> Result<Self> {
        Self::new_with_policy(input, order_by, None)
    }

    /// Creates a new sort iterator with an optional memory policy.
    pub fn new_with_policy(
        mut input: I,
        order_by: &[SortExpr],
        policy: Option<MemoryPolicy>,
    ) -> Result<Self> {
        let schema = input.schema().to_vec();
        let mut tracker = policy.clone().map(MemoryTracker::new);

        if order_by.is_empty() {
            let mut rows = Vec::new();
            while let Some(result) = input.next_row() {
                rows.push(result?);
                if let Some(tracker) = &mut tracker {
                    let row = rows.last().expect("row just pushed");
                    tracker.add_row(&row.values)?;
                }
            }
            return Ok(Self {
                output: SortOutput::InMemory(rows.into_iter()),
                schema,
                _marker: PhantomData,
            });
        }

        let allow_spill = policy
            .as_ref()
            .and_then(|policy| policy.spill_directory())
            .is_some();
        let mut runs: Vec<PathBuf> = Vec::new();
        let mut keyed: Vec<(Row, Vec<SqlValue>)> = Vec::new();

        while let Some(result) = input.next_row() {
            let row = result?;
            let mut keys = Vec::with_capacity(order_by.len());
            for expr in order_by {
                let ctx = EvalContext::new(&row.values);
                keys.push(crate::executor::evaluator::evaluate(&expr.expr, &ctx)?);
            }
            if let Some(tracker) = &mut tracker {
                tracker.add_row(&row.values)?;
                tracker.add_values(&keys)?;
            }
            keyed.push((row, keys));

            if allow_spill && tracker.as_ref().map(|t| t.over_limit()).unwrap_or(false) {
                let policy = policy
                    .as_ref()
                    .ok_or_else(|| ExecutorError::InvalidOperation {
                        operation: "sort spill".into(),
                        reason: "spill policy missing".into(),
                    })?;
                let path = spill_run(&mut keyed, order_by, policy)?;
                runs.push(path);
                if let Some(tracker) = &mut tracker {
                    tracker.reset();
                }
            }
        }

        if runs.is_empty() {
            keyed.sort_by(|a, b| compare_key_values(&a.1, &b.1, order_by));
            let sorted: Vec<Row> = keyed.into_iter().map(|(row, _)| row).collect();
            return Ok(Self {
                output: SortOutput::InMemory(sorted.into_iter()),
                schema,
                _marker: PhantomData,
            });
        }

        if !keyed.is_empty() {
            let policy = policy
                .as_ref()
                .ok_or_else(|| ExecutorError::InvalidOperation {
                    operation: "sort spill".into(),
                    reason: "spill policy missing".into(),
                })?;
            let path = spill_run(&mut keyed, order_by, policy)?;
            runs.push(path);
        }

        let external = ExternalSortState::new(order_by.to_vec(), runs)?;

        Ok(Self {
            output: SortOutput::External(external),
            schema,
            _marker: PhantomData,
        })
    }
}

impl<I: RowIterator> RowIterator for SortIterator<I> {
    fn next_row(&mut self) -> Option<Result<Row>> {
        match &mut self.output {
            SortOutput::InMemory(iter) => iter.next().map(Ok),
            SortOutput::External(state) => state.next_row(),
        }
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn spill_run(
    entries: &mut Vec<(Row, Vec<SqlValue>)>,
    order_by: &[SortExpr],
    policy: &MemoryPolicy,
) -> Result<PathBuf> {
    let directory = policy
        .spill_directory()
        .ok_or_else(|| ExecutorError::InvalidOperation {
            operation: "sort spill".into(),
            reason: "spill directory not configured".into(),
        })?;
    ensure_spill_dir(directory)?;
    let (path, file) = create_spill_file(directory, "sort-run")?;
    let mut writer = BufWriter::new(file);

    entries.sort_by(|a, b| compare_key_values(&a.1, &b.1, order_by));

    let mut bytes_written = 0u64;
    for (row, keys) in entries.iter() {
        let key_bytes = RowCodec::encode(keys);
        let row_bytes = RowCodec::encode(&row.values);
        let key_len =
            u32::try_from(key_bytes.len()).map_err(|_| ExecutorError::InvalidOperation {
                operation: "sort spill".into(),
                reason: "sort key size exceeds u32::MAX".into(),
            })?;
        let row_len =
            u32::try_from(row_bytes.len()).map_err(|_| ExecutorError::InvalidOperation {
                operation: "sort spill".into(),
                reason: "row size exceeds u32::MAX".into(),
            })?;

        writer
            .write_all(&row.row_id.to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&key_len.to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&row_len.to_le_bytes())
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&key_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        writer
            .write_all(&row_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        bytes_written = bytes_written
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(key_bytes.len() as u64)
            .saturating_add(row_bytes.len() as u64);
    }

    writer
        .flush()
        .map_err(|err| spill_io_error("sort spill", err))?;
    policy.record_spill(bytes_written, 1);
    entries.clear();

    Ok(path)
}

fn ensure_spill_dir(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory).map_err(|err| spill_io_error("sort spill", err))?;
    Ok(())
}

fn create_spill_file(directory: &Path, prefix: &str) -> Result<(PathBuf, File)> {
    for _ in 0..16 {
        let counter = SPILL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = directory.join(format!("{prefix}-{timestamp}-{counter}.bin"));
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(spill_io_error("sort spill", err)),
        }
    }
    Err(ExecutorError::InvalidOperation {
        operation: "sort spill".into(),
        reason: "failed to allocate spill file".into(),
    })
}

fn spill_io_error(operation: &str, err: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: operation.into(),
        reason: err.to_string(),
    }
}

struct SpillEntry {
    row: Row,
    keys: Vec<SqlValue>,
}

struct SpillRunReader {
    path: PathBuf,
    reader: BufReader<File>,
}

impl SpillRunReader {
    fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path).map_err(|err| spill_io_error("sort spill", err))?;
        Ok(Self {
            path,
            reader: BufReader::new(file),
        })
    }

    fn next_entry(&mut self) -> Result<Option<SpillEntry>> {
        let mut row_id_buf = [0u8; 8];
        match self.reader.read_exact(&mut row_id_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(spill_io_error("sort spill", err)),
        }
        let row_id = u64::from_le_bytes(row_id_buf);
        let key_len = self.read_u32()?;
        let row_len = self.read_u32()?;

        let mut key_bytes = vec![0u8; key_len as usize];
        self.reader
            .read_exact(&mut key_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;
        let mut row_bytes = vec![0u8; row_len as usize];
        self.reader
            .read_exact(&mut row_bytes)
            .map_err(|err| spill_io_error("sort spill", err))?;

        let keys = RowCodec::decode(&key_bytes).map_err(ExecutorError::Storage)?;
        let values = RowCodec::decode(&row_bytes).map_err(ExecutorError::Storage)?;

        Ok(Some(SpillEntry {
            row: Row::new(row_id, values),
            keys,
        }))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.reader
            .read_exact(&mut buf)
            .map_err(|err| spill_io_error("sort spill", err))?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl Drop for SpillRunReader {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct ExternalSortState {
    order_by: Arc<Vec<SortExpr>>,
    readers: Vec<SpillRunReader>,
    heap: BinaryHeap<SpillHeapItem>,
}

impl ExternalSortState {
    fn new(order_by: Vec<SortExpr>, runs: Vec<PathBuf>) -> Result<Self> {
        let order_by = Arc::new(order_by);
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();

        for (idx, path) in runs.into_iter().enumerate() {
            let mut reader = SpillRunReader::open(path)?;
            if let Some(entry) = reader.next_entry()? {
                heap.push(SpillHeapItem {
                    run_idx: idx,
                    row: entry.row,
                    keys: entry.keys,
                    order_by: Arc::clone(&order_by),
                });
            }
            readers.push(reader);
        }

        Ok(Self {
            order_by,
            readers,
            heap,
        })
    }

    fn next_row(&mut self) -> Option<Result<Row>> {
        let item = self.heap.pop()?;
        let row = item.row;
        let run_idx = item.run_idx;

        match self.readers[run_idx].next_entry() {
            Ok(Some(entry)) => {
                self.heap.push(SpillHeapItem {
                    run_idx,
                    row: entry.row,
                    keys: entry.keys,
                    order_by: Arc::clone(&self.order_by),
                });
            }
            Ok(None) => {}
            Err(err) => return Some(Err(err)),
        }

        Some(Ok(row))
    }
}

#[derive(Clone)]
struct SpillHeapItem {
    run_idx: usize,
    row: Row,
    keys: Vec<SqlValue>,
    order_by: Arc<Vec<SortExpr>>,
}

impl PartialEq for SpillHeapItem {
    fn eq(&self, other: &Self) -> bool {
        compare_key_values(&self.keys, &other.keys, &self.order_by) == Ordering::Equal
            && self.run_idx == other.run_idx
            && self.row.row_id == other.row.row_id
    }
}

impl Eq for SpillHeapItem {}

impl PartialOrd for SpillHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpillHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = compare_key_values(&self.keys, &other.keys, &self.order_by);
        let order = if order == Ordering::Equal {
            self.run_idx
                .cmp(&other.run_idx)
                .then_with(|| self.row.row_id.cmp(&other.row.row_id))
        } else {
            order
        };
        order.reverse()
    }
}

fn compare_key_values(a: &[SqlValue], b: &[SqlValue], order_by: &[SortExpr]) -> Ordering {
    for (i, sort_expr) in order_by.iter().enumerate() {
        let left = &a[i];
        let right = &b[i];
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
        if let Some(limit) = self.limit
            && self.yielded >= limit
        {
            return None;
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
                    if let Some(limit) = self.limit
                        && self.yielded >= limit
                    {
                        return None;
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
    use crate::Span;
    use crate::planner::types::ResolvedType;

    fn sample_schema() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
        ]
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            Row::new(
                1,
                vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            ),
            Row::new(2, vec![SqlValue::Integer(2), SqlValue::Text("bob".into())]),
            Row::new(
                3,
                vec![SqlValue::Integer(3), SqlValue::Text("carol".into())],
            ),
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
            Row::new(
                1,
                vec![SqlValue::Integer(3), SqlValue::Text("carol".into())],
            ),
            Row::new(
                2,
                vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            ),
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
            Row::new(
                1,
                vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            ),
            Row::new(
                2,
                vec![SqlValue::Integer(3), SqlValue::Text("carol".into())],
            ),
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
