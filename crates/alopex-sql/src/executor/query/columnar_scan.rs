use alopex_core::columnar::encoding::Column;
use alopex_core::columnar::encoding_v2::Bitmap;
use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::columnar::segment_v2::{
    ColumnSegmentV2, InMemorySegmentSource, RecordBatch, SegmentReaderV2,
};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::format::bincode_config;
use bincode::config::Options;

use crate::ast::expr::BinaryOp;
use crate::catalog::{ColumnMetadata, RowIdMode, TableMetadata};
use crate::columnar::statistics::RowGroupStatistics;
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::query::iterator::RowIterator;
use crate::executor::{ExecutorError, Result, Row};
use crate::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use crate::planner::types::ResolvedType;
use crate::storage::{SqlTxn, SqlValue};
use std::collections::BTreeSet;

/// ColumnarScan オペレータ。
#[derive(Debug, Clone)]
pub struct ColumnarScan {
    pub table_id: u32,
    pub projected_columns: Vec<usize>,
    pub pushed_filter: Option<PushdownFilter>,
    pub residual_filter: Option<TypedExpr>,
}

/// プッシュダウン可能なフィルタ。
#[derive(Debug, Clone, PartialEq)]
pub enum PushdownFilter {
    Eq {
        column_idx: usize,
        value: SqlValue,
    },
    Range {
        column_idx: usize,
        min: Option<SqlValue>,
        max: Option<SqlValue>,
    },
    IsNull {
        column_idx: usize,
        is_null: bool,
    },
    And(Vec<PushdownFilter>),
    Or(Vec<PushdownFilter>),
}

impl ColumnarScan {
    pub fn new(
        table_id: u32,
        projected_columns: Vec<usize>,
        pushed_filter: Option<PushdownFilter>,
        residual_filter: Option<TypedExpr>,
    ) -> Self {
        Self {
            table_id,
            projected_columns,
            pushed_filter,
            residual_filter,
        }
    }

    /// RowGroup をプルーニングするか判定する。
    pub fn should_skip_row_group(&self, stats: &RowGroupStatistics) -> bool {
        match &self.pushed_filter {
            None => false,
            Some(filter) => Self::evaluate_pushdown(filter, stats),
        }
    }

    /// プッシュダウンフィルタを統計情報で評価する。
    pub fn evaluate_pushdown(filter: &PushdownFilter, stats: &RowGroupStatistics) -> bool {
        match filter {
            PushdownFilter::Eq { column_idx, value } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if col_stats.total_count == 0 {
                        return true;
                    }
                    if matches!(
                        value.partial_cmp(&col_stats.min),
                        Some(std::cmp::Ordering::Less)
                    ) {
                        return true;
                    }
                    matches!(
                        value.partial_cmp(&col_stats.max),
                        Some(std::cmp::Ordering::Greater)
                    )
                }
                None => false,
            },

            PushdownFilter::Range {
                column_idx,
                min,
                max,
            } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if col_stats.total_count == 0 {
                        return true;
                    }
                    if let Some(filter_min) = min
                        && matches!(
                            col_stats.max.partial_cmp(filter_min),
                            Some(std::cmp::Ordering::Less)
                        )
                    {
                        return true;
                    }
                    if let Some(filter_max) = max
                        && matches!(
                            col_stats.min.partial_cmp(filter_max),
                            Some(std::cmp::Ordering::Greater)
                        )
                    {
                        return true;
                    }
                    false
                }
                None => false,
            },

            PushdownFilter::IsNull {
                column_idx,
                is_null,
            } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if *is_null {
                        col_stats.null_count == 0
                    } else {
                        col_stats.null_count == col_stats.total_count
                    }
                }
                None => false,
            },

            PushdownFilter::And(filters) => {
                if filters.is_empty() {
                    return false;
                }
                filters.iter().any(|f| Self::evaluate_pushdown(f, stats))
            }

            PushdownFilter::Or(filters) => {
                if filters.is_empty() {
                    return false;
                }
                filters.iter().all(|f| Self::evaluate_pushdown(f, stats))
            }
        }
    }
}

// ============================================================================
// ColumnarScanIterator - FR-7 Streaming Iterator for Columnar Storage
// ============================================================================

/// Pre-loaded segment data for streaming iteration.
struct LoadedSegment {
    /// Segment reader for reading row groups.
    reader: SegmentReaderV2,
    /// Row group statistics for pruning (if available).
    row_group_stats: Option<Vec<RowGroupStatistics>>,
    /// Row IDs for RowIdMode::Direct.
    row_ids: Vec<u64>,
    /// Row group metadata for row ID slicing.
    row_groups: Vec<alopex_core::columnar::segment_v2::RowGroupMeta>,
}

/// Streaming iterator for columnar storage (FR-7 compliant).
///
/// This iterator yields rows one at a time from columnar storage,
/// avoiding the need to materialize all rows into a `Vec<Row>` upfront.
/// Segments are pre-loaded during construction, but row conversion is
/// performed lazily as rows are requested.
pub struct ColumnarScanIterator {
    /// Pre-loaded segments.
    segments: Vec<LoadedSegment>,
    /// Current segment index.
    segment_idx: usize,
    /// Current row group index within the segment.
    row_group_idx: usize,
    /// Current row index within the batch.
    row_idx: usize,
    /// Current loaded RecordBatch (lazy loaded per row group).
    current_batch: Option<RecordBatch>,
    /// Projected column indices.
    projected: Vec<usize>,
    /// Table metadata.
    table_meta: TableMetadata,
    /// Schema for RowIterator trait.
    schema: Vec<ColumnMetadata>,
    /// ColumnarScan operator for filter evaluation.
    scan: ColumnarScan,
    /// RowID column index for RowIdMode::Direct.
    row_id_col_idx: Option<usize>,
    /// Next synthetic row ID (for RowIdMode::None).
    next_row_id: u64,
}

impl ColumnarScanIterator {
    /// Advances to the next valid row, loading batches as needed.
    ///
    /// Returns `Some(Ok(row))` for valid rows, `Some(Err(_))` for errors,
    /// or `None` when all rows have been consumed.
    fn advance(&mut self) -> Option<Result<Row>> {
        loop {
            // Check if we need to load a new batch
            if self.current_batch.is_none() && !self.load_next_batch() {
                return None; // No more batches
            }

            // Get row count to check if exhausted
            let row_count = match &self.current_batch {
                Some(batch) => batch.num_rows(),
                None => continue,
            };

            // Check if we've exhausted the current batch
            if self.row_idx >= row_count {
                self.current_batch = None;
                self.row_idx = 0;
                self.row_group_idx += 1;
                continue;
            }

            // Convert current row (take batch temporarily to avoid borrow conflict)
            let row_idx = self.row_idx;
            match self.convert_current_row(row_idx) {
                Ok(Some(row)) => {
                    self.row_idx += 1;
                    return Some(Ok(row));
                }
                Ok(None) => {
                    // Row filtered out by residual filter
                    self.row_idx += 1;
                    continue;
                }
                Err(e) => {
                    self.row_idx += 1;
                    return Some(Err(e));
                }
            }
        }
    }

    /// Loads the next row group batch, advancing segments as needed.
    ///
    /// Returns `true` if a batch was successfully loaded, `false` if no more data.
    fn load_next_batch(&mut self) -> bool {
        while self.segment_idx < self.segments.len() {
            let segment = &self.segments[self.segment_idx];
            let row_group_count = segment.row_groups.len();

            while self.row_group_idx < row_group_count {
                // Check if this row group should be skipped via pushdown
                let should_skip = match segment.row_group_stats.as_ref() {
                    Some(stats) if stats.len() == row_group_count => {
                        self.scan.should_skip_row_group(&stats[self.row_group_idx])
                    }
                    _ => false,
                };

                if should_skip {
                    self.row_group_idx += 1;
                    continue;
                }

                // Load the batch
                match segment
                    .reader
                    .read_row_group_by_index(&self.projected, self.row_group_idx)
                {
                    Ok(mut batch) => {
                        // Attach row IDs if available
                        if !segment.row_ids.is_empty()
                            && let Some(meta) = segment.row_groups.get(self.row_group_idx)
                        {
                            let start = meta.row_start as usize;
                            let end = start + meta.row_count as usize;
                            if end <= segment.row_ids.len() {
                                batch =
                                    batch.with_row_ids(Some(segment.row_ids[start..end].to_vec()));
                            }
                        }
                        self.current_batch = Some(batch);
                        self.row_idx = 0;
                        return true;
                    }
                    Err(_) => {
                        // Skip this row group on error
                        self.row_group_idx += 1;
                        continue;
                    }
                }
            }

            // Move to next segment
            self.segment_idx += 1;
            self.row_group_idx = 0;
        }

        false
    }

    /// Converts a row from the current batch, applying residual filter.
    ///
    /// Returns `Ok(Some(row))` if row passes filter, `Ok(None)` if filtered out.
    fn convert_current_row(&mut self, row_idx: usize) -> Result<Option<Row>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| ExecutorError::Columnar("no current batch".into()))?;

        let column_count = self.table_meta.column_count();
        let mut values = vec![SqlValue::Null; column_count];

        for (pos, &table_col_idx) in self.projected.iter().enumerate() {
            let column = batch
                .columns
                .get(pos)
                .ok_or_else(|| ExecutorError::Columnar("missing projected column".into()))?;
            let bitmap = batch.null_bitmaps.get(pos).and_then(|b| b.as_ref());
            let col_meta = self
                .table_meta
                .columns
                .get(table_col_idx)
                .ok_or_else(|| ExecutorError::Columnar("column index out of bounds".into()))?;
            let value = value_from_column(column, bitmap, row_idx, &col_meta.data_type)?;
            values[table_col_idx] = value;
        }

        // Apply residual filter
        if let Some(predicate) = self.scan.residual_filter.as_ref() {
            let ctx = EvalContext::new(&values);
            let keep = matches!(evaluate(predicate, &ctx)?, SqlValue::Boolean(true));
            if !keep {
                return Ok(None);
            }
        }

        // Determine row ID - need to access batch again for row_ids
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| ExecutorError::Columnar("no current batch".into()))?;

        let row_id = match self.table_meta.storage_options.row_id_mode {
            RowIdMode::Direct => {
                if let Some(row_ids) = batch.row_ids.as_ref() {
                    *row_ids.get(row_idx).ok_or_else(|| {
                        ExecutorError::Columnar(
                            "row_id missing for row in row_id_mode=direct".into(),
                        )
                    })?
                } else if let Some(idx) = self.row_id_col_idx {
                    let val = values.get(idx).ok_or_else(|| {
                        ExecutorError::Columnar("row_id column missing in projected values".into())
                    })?;
                    match val {
                        SqlValue::Integer(v) if *v >= 0 => *v as u64,
                        SqlValue::BigInt(v) if *v >= 0 => *v as u64,
                        other => {
                            return Err(ExecutorError::Columnar(format!(
                                "row_id column must be non-negative integer, got {}",
                                other.type_name()
                            )));
                        }
                    }
                } else {
                    let rid = self.next_row_id;
                    self.next_row_id = self.next_row_id.saturating_add(1);
                    rid
                }
            }
            RowIdMode::None => {
                let rid = self.next_row_id;
                self.next_row_id = self.next_row_id.saturating_add(1);
                rid
            }
        };

        Ok(Some(Row::new(row_id, values)))
    }
}

impl RowIterator for ColumnarScanIterator {
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.advance()
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

/// Create a streaming columnar scan iterator (FR-7 compliant).
///
/// This function pre-loads segment data during construction but yields rows
/// one at a time during iteration, avoiding full materialization of all rows.
///
/// # Arguments
///
/// * `txn` - Transaction for loading segment data
/// * `table_meta` - Table metadata
/// * `scan` - ColumnarScan operator with projection and filters
///
/// # Returns
///
/// A `ColumnarScanIterator` that implements `RowIterator`.
pub fn create_columnar_scan_iterator<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    scan: &ColumnarScan,
) -> Result<ColumnarScanIterator> {
    debug_assert_eq!(scan.table_id, table_meta.table_id);

    let projected: Vec<usize> = if scan.projected_columns.is_empty() {
        (0..table_meta.columns.len()).collect()
    } else {
        scan.projected_columns.clone()
    };

    let segment_ids = load_segment_index(txn, table_meta.table_id)?;

    let row_id_col_idx = if table_meta.storage_options.row_id_mode == RowIdMode::Direct {
        table_meta
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("row_id"))
    } else {
        None
    };

    // Pre-load all segments
    let mut segments = Vec::with_capacity(segment_ids.len());
    for segment_id in segment_ids {
        let segment = load_segment(txn, table_meta.table_id, segment_id)?;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
        let row_group_stats = load_row_group_stats(txn, table_meta.table_id, segment_id);

        segments.push(LoadedSegment {
            reader,
            row_group_stats,
            row_ids: segment.row_ids.clone(),
            row_groups: segment.meta.row_groups.clone(),
        });
    }

    Ok(ColumnarScanIterator {
        segments,
        segment_idx: 0,
        row_group_idx: 0,
        row_idx: 0,
        current_batch: None,
        projected,
        schema: table_meta.columns.clone(),
        table_meta: table_meta.clone(),
        scan: scan.clone(),
        row_id_col_idx,
        next_row_id: 0,
    })
}

/// ColumnarScan を実行する。
pub fn execute_columnar_scan<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    scan: &ColumnarScan,
) -> Result<Vec<Row>> {
    debug_assert_eq!(scan.table_id, table_meta.table_id);
    let projected: Vec<usize> = if scan.projected_columns.is_empty() {
        (0..table_meta.columns.len()).collect()
    } else {
        scan.projected_columns.clone()
    };

    let segment_ids = load_segment_index(txn, table_meta.table_id)?;
    if segment_ids.is_empty() {
        return Ok(Vec::new());
    }

    let row_id_col_idx = if table_meta.storage_options.row_id_mode == RowIdMode::Direct {
        table_meta
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("row_id"))
    } else {
        None
    };

    let mut results = Vec::new();
    let mut next_row_id = 0u64;
    for segment_id in segment_ids {
        let segment = load_segment(txn, table_meta.table_id, segment_id)?;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;

        let row_group_stats = load_row_group_stats(txn, table_meta.table_id, segment_id);
        let row_group_count = segment.meta.row_groups.len();
        for rg_index in 0..row_group_count {
            let should_skip = match row_group_stats.as_ref() {
                Some(stats) if stats.len() == row_group_count => {
                    scan.should_skip_row_group(&stats[rg_index])
                }
                _ => false,
            };
            if should_skip {
                continue;
            }

            let batch = reader
                .read_row_group_by_index(&projected, rg_index)
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
            let batch = if !segment.row_ids.is_empty() {
                if let Some(meta) = segment.meta.row_groups.get(rg_index) {
                    let start = meta.row_start as usize;
                    let end = start + meta.row_count as usize;
                    if end <= segment.row_ids.len() {
                        batch.with_row_ids(Some(segment.row_ids[start..end].to_vec()))
                    } else {
                        batch
                    }
                } else {
                    batch
                }
            } else {
                batch
            };
            append_rows_from_batch(
                &mut results,
                &batch,
                table_meta,
                &projected,
                scan.residual_filter.as_ref(),
                table_meta.storage_options.row_id_mode,
                row_id_col_idx,
                &mut next_row_id,
            )?;
        }
    }

    Ok(results)
}

/// ColumnarScan を実行し、フィルタ後の RowID のみを返す。
///
/// RowIdMode::Direct で columnar ストレージの場合、行本体の読み込みを避けて
/// RowID 再フェッチ用の候補セットを得る目的で使用する。
pub fn execute_columnar_row_ids<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    scan: &ColumnarScan,
) -> Result<Vec<u64>> {
    if table_meta.storage_options.storage_type != crate::catalog::StorageType::Columnar {
        return Err(ExecutorError::Columnar(
            "execute_columnar_row_ids requires columnar storage".into(),
        ));
    }

    let mut needed: BTreeSet<usize> = scan.projected_columns.iter().copied().collect();
    if let Some(pred) = &scan.residual_filter {
        collect_column_indices(pred, &mut needed);
    }
    let projected: Vec<usize> = needed.into_iter().collect();

    let segment_ids = load_segment_index(txn, table_meta.table_id)?;
    if segment_ids.is_empty() {
        return Ok(Vec::new());
    }

    let row_id_col_idx = if table_meta.storage_options.row_id_mode == RowIdMode::Direct {
        table_meta
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("row_id"))
    } else {
        None
    };

    let mut results = Vec::new();
    let mut next_row_id = 0u64;
    for segment_id in segment_ids {
        let segment = load_segment(txn, table_meta.table_id, segment_id)?;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;

        let row_group_stats = load_row_group_stats(txn, table_meta.table_id, segment_id);
        let row_group_count = segment.meta.row_groups.len();
        for rg_index in 0..row_group_count {
            let should_skip = match row_group_stats.as_ref() {
                Some(stats) if stats.len() == row_group_count => {
                    scan.should_skip_row_group(&stats[rg_index])
                }
                _ => false,
            };
            if should_skip {
                continue;
            }

            let batch = reader
                .read_row_group_by_index(&projected, rg_index)
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
            let batch = if !segment.row_ids.is_empty() {
                if let Some(meta) = segment.meta.row_groups.get(rg_index) {
                    let start = meta.row_start as usize;
                    let end = start + meta.row_count as usize;
                    if end <= segment.row_ids.len() {
                        batch.with_row_ids(Some(segment.row_ids[start..end].to_vec()))
                    } else {
                        batch
                    }
                } else {
                    batch
                }
            } else {
                batch
            };

            let row_count = batch.num_rows();
            for row_idx in 0..row_count {
                // 残余フィルタの評価に必要なカラムだけ値を復元する。
                let mut values = vec![SqlValue::Null; table_meta.column_count()];
                for (pos, &table_col_idx) in projected.iter().enumerate() {
                    let column = batch.columns.get(pos).ok_or_else(|| {
                        ExecutorError::Columnar("missing projected column".into())
                    })?;
                    let bitmap = batch.null_bitmaps.get(pos).and_then(|b| b.as_ref());
                    let value = value_from_column(
                        column,
                        bitmap,
                        row_idx,
                        &table_meta
                            .columns
                            .get(table_col_idx)
                            .ok_or_else(|| {
                                ExecutorError::Columnar("column index out of bounds".into())
                            })?
                            .data_type,
                    )?;
                    values[table_col_idx] = value;
                }

                if let Some(predicate) = scan.residual_filter.as_ref() {
                    let ctx = EvalContext::new(&values);
                    let keep = matches!(evaluate(predicate, &ctx)?, SqlValue::Boolean(true));
                    if !keep {
                        continue;
                    }
                }

                let row_id = match table_meta.storage_options.row_id_mode {
                    RowIdMode::Direct => {
                        if let Some(row_ids) = batch.row_ids.as_ref() {
                            *row_ids.get(row_idx).ok_or_else(|| {
                                ExecutorError::Columnar(
                                    "row_id missing for row in row_id_mode=direct".into(),
                                )
                            })?
                        } else if let Some(idx) = row_id_col_idx {
                            let val = values.get(idx).ok_or_else(|| {
                                ExecutorError::Columnar(
                                    "row_id column missing in projected values".into(),
                                )
                            })?;
                            match val {
                                SqlValue::Integer(v) if *v >= 0 => *v as u64,
                                SqlValue::BigInt(v) if *v >= 0 => *v as u64,
                                other => {
                                    return Err(ExecutorError::Columnar(format!(
                                        "row_id column must be non-negative integer, got {}",
                                        other.type_name()
                                    )));
                                }
                            }
                        } else {
                            let rid = next_row_id;
                            next_row_id = next_row_id.saturating_add(1);
                            rid
                        }
                    }
                    RowIdMode::None => {
                        let rid = next_row_id;
                        next_row_id = next_row_id.saturating_add(1);
                        rid
                    }
                };
                results.push(row_id);
            }
        }
    }

    Ok(results)
}

/// TypedExpr から PushdownFilter へ変換する（変換不可なら None）。
pub fn expr_to_pushdown(expr: &TypedExpr) -> Option<PushdownFilter> {
    match &expr.kind {
        TypedExprKind::BinaryOp { left, op, right } => match op {
            BinaryOp::And => {
                let l = expr_to_pushdown(left)?;
                let r = expr_to_pushdown(right)?;
                Some(PushdownFilter::And(vec![l, r]))
            }
            BinaryOp::Or => {
                let l = expr_to_pushdown(left)?;
                let r = expr_to_pushdown(right)?;
                Some(PushdownFilter::Or(vec![l, r]))
            }
            BinaryOp::Eq => extract_eq(left, right),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                extract_range(op, left, right)
            }
            _ => None,
        },
        TypedExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            if *negated {
                return None;
            }
            let (column_idx, value_min, value_max) = match expr.kind {
                TypedExprKind::ColumnRef { column_index, .. } => {
                    let low_v = literal_value(low)?;
                    let high_v = literal_value(high)?;
                    (column_index, low_v, high_v)
                }
                _ => return None,
            };
            Some(PushdownFilter::Range {
                column_idx,
                min: Some(value_min),
                max: Some(value_max),
            })
        }
        TypedExprKind::IsNull { expr, negated } => match expr.kind {
            TypedExprKind::ColumnRef { column_index, .. } => Some(PushdownFilter::IsNull {
                column_idx: column_index,
                is_null: !negated,
            }),
            _ => None,
        },
        _ => None,
    }
}

fn extract_eq(left: &TypedExpr, right: &TypedExpr) -> Option<PushdownFilter> {
    if let Some((col_idx, value)) = extract_column_literal(left, right) {
        return Some(PushdownFilter::Eq {
            column_idx: col_idx,
            value,
        });
    }
    if let Some((col_idx, value)) = extract_column_literal(right, left) {
        return Some(PushdownFilter::Eq {
            column_idx: col_idx,
            value,
        });
    }
    None
}

fn extract_range(op: &BinaryOp, left: &TypedExpr, right: &TypedExpr) -> Option<PushdownFilter> {
    match (
        extract_column_literal(left, right),
        extract_column_literal(right, left),
    ) {
        (Some((col_idx, value)), _) => match op {
            BinaryOp::Lt | BinaryOp::LtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: None,
                max: Some(value),
            }),
            BinaryOp::Gt | BinaryOp::GtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: Some(value),
                max: None,
            }),
            _ => None,
        },
        (_, Some((col_idx, value))) => match op {
            BinaryOp::Lt | BinaryOp::LtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: Some(value),
                max: None,
            }),
            BinaryOp::Gt | BinaryOp::GtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: None,
                max: Some(value),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn extract_column_literal(
    column_expr: &TypedExpr,
    literal_expr: &TypedExpr,
) -> Option<(usize, SqlValue)> {
    match column_expr.kind {
        TypedExprKind::ColumnRef { column_index, .. } => {
            let value = literal_value(literal_expr)?;
            Some((column_index, value))
        }
        _ => None,
    }
}

fn literal_value(expr: &TypedExpr) -> Option<SqlValue> {
    match &expr.kind {
        TypedExprKind::Literal(_) | TypedExprKind::VectorLiteral(_) => {
            evaluate(expr, &EvalContext::new(&[])).ok()
        }
        _ => None,
    }
}

/// projection 情報からカラムインデックスを推定する（現状は全カラム）。
pub fn projection_to_columns(projection: &Projection, table_meta: &TableMetadata) -> Vec<usize> {
    match projection {
        Projection::All(names) => names
            .iter()
            .filter_map(|name| table_meta.columns.iter().position(|c| &c.name == name))
            .collect(),
        Projection::Columns(cols) => {
            let mut indices = BTreeSet::new();
            for col in cols {
                collect_column_indices(&col.expr, &mut indices);
            }
            if indices.is_empty() {
                return (0..table_meta.columns.len()).collect();
            }
            indices
                .into_iter()
                .filter(|idx| *idx < table_meta.columns.len())
                .collect()
        }
    }
}

/// フィルタと Projection を ColumnarScan にまとめるユーティリティ。
pub fn build_columnar_scan_for_filter(
    table_meta: &TableMetadata,
    projection: Projection,
    predicate: &TypedExpr,
) -> ColumnarScan {
    let mut projected_columns = projection_to_columns(&projection, table_meta);
    let mut predicate_indices = BTreeSet::new();
    collect_column_indices(predicate, &mut predicate_indices);
    for idx in predicate_indices {
        if !projected_columns.contains(&idx) {
            projected_columns.push(idx);
        }
    }
    projected_columns.sort_unstable();
    let pushed_filter = expr_to_pushdown(predicate);
    ColumnarScan::new(
        table_meta.table_id,
        projected_columns,
        pushed_filter,
        Some(predicate.clone()),
    )
}

/// Projection だけを指定して ColumnarScan を構築する。
pub fn build_columnar_scan(table_meta: &TableMetadata, projection: &Projection) -> ColumnarScan {
    let projected_columns = projection_to_columns(projection, table_meta);
    ColumnarScan::new(table_meta.table_id, projected_columns, None, None)
}

/// 式中に現れるカラムインデックスを収集する。
fn collect_column_indices(expr: &TypedExpr, acc: &mut BTreeSet<usize>) {
    match &expr.kind {
        TypedExprKind::ColumnRef { column_index, .. } => {
            acc.insert(*column_index);
        }
        TypedExprKind::BinaryOp { left, right, .. } => {
            collect_column_indices(left, acc);
            collect_column_indices(right, acc);
        }
        TypedExprKind::UnaryOp { operand, .. } => collect_column_indices(operand, acc),
        TypedExprKind::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, acc);
            collect_column_indices(low, acc);
            collect_column_indices(high, acc);
        }
        TypedExprKind::InList { expr, list, .. } => {
            collect_column_indices(expr, acc);
            for item in list {
                collect_column_indices(item, acc);
            }
        }
        TypedExprKind::IsNull { expr, .. } => collect_column_indices(expr, acc),
        TypedExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_indices(arg, acc);
            }
        }
        _ => {}
    }
}

fn load_segment_index<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_id: u32,
) -> Result<Vec<u64>> {
    let key = key_layout::segment_index_key(table_id);
    let bytes = txn.inner_mut().get(&key)?;
    if let Some(raw) = bytes {
        bincode_config()
            .deserialize(&raw)
            .map_err(|e| ExecutorError::Columnar(e.to_string()))
    } else {
        Ok(Vec::new())
    }
}

fn load_segment<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_id: u32,
    segment_id: u64,
) -> Result<ColumnSegmentV2> {
    let key = key_layout::column_segment_key(table_id, segment_id, 0);
    let bytes = txn
        .inner_mut()
        .get(&key)?
        .ok_or_else(|| ExecutorError::Columnar(format!("segment {segment_id} missing")))?;
    bincode_config()
        .deserialize(&bytes)
        .map_err(|e| ExecutorError::Columnar(e.to_string()))
}

fn load_row_group_stats<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_id: u32,
    segment_id: u64,
) -> Option<Vec<RowGroupStatistics>> {
    let key = key_layout::row_group_stats_key(table_id, segment_id);
    match txn.inner_mut().get(&key) {
        Ok(Some(bytes)) => bincode_config().deserialize(&bytes).ok(),
        Ok(None) => None,
        Err(_) => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn append_rows_from_batch(
    out: &mut Vec<Row>,
    batch: &alopex_core::columnar::segment_v2::RecordBatch,
    table_meta: &TableMetadata,
    projected: &[usize],
    residual_filter: Option<&TypedExpr>,
    row_id_mode: RowIdMode,
    row_id_col_idx: Option<usize>,
    next_row_id: &mut u64,
) -> Result<()> {
    if batch.columns.len() != projected.len() {
        return Err(ExecutorError::Columnar(format!(
            "projected column count mismatch: requested {}, got {}",
            projected.len(),
            batch.columns.len()
        )));
    }

    let row_count = batch.num_rows();
    for row_idx in 0..row_count {
        let mut values = vec![SqlValue::Null; table_meta.column_count()];
        for (pos, &table_col_idx) in projected.iter().enumerate() {
            let column = batch
                .columns
                .get(pos)
                .ok_or_else(|| ExecutorError::Columnar("missing projected column".into()))?;
            let bitmap = batch.null_bitmaps.get(pos).and_then(|b| b.as_ref());
            let value = value_from_column(
                column,
                bitmap,
                row_idx,
                &table_meta
                    .columns
                    .get(table_col_idx)
                    .ok_or_else(|| ExecutorError::Columnar("column index out of bounds".into()))?
                    .data_type,
            )?;
            values[table_col_idx] = value;
        }

        if let Some(predicate) = residual_filter {
            let ctx = EvalContext::new(&values);
            let keep = matches!(evaluate(predicate, &ctx)?, SqlValue::Boolean(true));
            if !keep {
                continue;
            }
        }

        let row_id = match row_id_mode {
            RowIdMode::Direct => {
                if let Some(row_ids) = batch.row_ids.as_ref() {
                    *row_ids.get(row_idx).ok_or_else(|| {
                        ExecutorError::Columnar(
                            "row_id missing for row in row_id_mode=direct".into(),
                        )
                    })?
                } else if let Some(idx) = row_id_col_idx {
                    let val = values.get(idx).ok_or_else(|| {
                        ExecutorError::Columnar("row_id column missing in projected values".into())
                    })?;
                    match val {
                        SqlValue::Integer(v) if *v >= 0 => *v as u64,
                        SqlValue::BigInt(v) if *v >= 0 => *v as u64,
                        other => {
                            return Err(ExecutorError::Columnar(format!(
                                "row_id column must be non-negative integer, got {}",
                                other.type_name()
                            )));
                        }
                    }
                } else {
                    let rid = *next_row_id;
                    *next_row_id = next_row_id.saturating_add(1);
                    rid
                }
            }
            RowIdMode::None => {
                let rid = *next_row_id;
                *next_row_id = next_row_id.saturating_add(1);
                rid
            }
        };
        out.push(Row::new(row_id, values));
    }

    Ok(())
}

fn value_from_column(
    column: &Column,
    bitmap: Option<&Bitmap>,
    row_idx: usize,
    ty: &ResolvedType,
) -> Result<SqlValue> {
    if let Some(bm) = bitmap
        && !bm.get(row_idx)
    {
        return Ok(SqlValue::Null);
    }

    match (ty, column) {
        (ResolvedType::Integer, Column::Int64(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Integer(v as i32))
        }
        (ResolvedType::BigInt | ResolvedType::Timestamp, Column::Int64(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            if matches!(ty, ResolvedType::Timestamp) {
                Ok(SqlValue::Timestamp(v))
            } else {
                Ok(SqlValue::BigInt(v))
            }
        }
        (ResolvedType::Float, Column::Float32(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Float(v))
        }
        (ResolvedType::Double, Column::Float64(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Double(v))
        }
        (ResolvedType::Boolean, Column::Bool(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Boolean(v))
        }
        (ResolvedType::Text, Column::Binary(values)) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            String::from_utf8(raw.clone())
                .map(SqlValue::Text)
                .map_err(|e| ExecutorError::Columnar(e.to_string()))
        }
        (ResolvedType::Blob, Column::Binary(values)) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Blob(raw.clone()))
        }
        (ResolvedType::Vector { .. }, Column::Fixed { values, .. }) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            if raw.len() % 4 != 0 {
                return Err(ExecutorError::Columnar(
                    "invalid vector byte length in columnar segment".into(),
                ));
            }
            let floats: Vec<f32> = raw
                .chunks_exact(4)
                .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                .collect();
            Ok(SqlValue::Vector(floats))
        }
        (_, Column::Binary(values)) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Blob(raw.clone()))
        }
        _ => Err(ExecutorError::Columnar(
            "unsupported column type for columnar read".into(),
        )),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::Literal;
    use crate::catalog::{ColumnMetadata, RowIdMode, TableMetadata};
    use crate::columnar::statistics::ColumnStatistics;
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::typed_expr::TypedExprKind;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use bincode::config::Options;
    use std::sync::Arc;

    #[test]
    fn evaluate_pushdown_eq_prunes_out_of_range() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::Eq {
            column_idx: 0,
            value: SqlValue::Integer(10),
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_range_allows_overlap() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(5),
                max: SqlValue::Integer(10),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::Range {
            column_idx: 0,
            min: Some(SqlValue::Integer(8)),
            max: Some(SqlValue::Integer(12)),
        };
        assert!(!ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_is_null_skips_when_no_nulls() {
        let stats = RowGroupStatistics {
            row_count: 2,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(2),
                null_count: 0,
                total_count: 2,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::IsNull {
            column_idx: 0,
            is_null: true,
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_is_not_null_skips_when_all_null() {
        let stats = RowGroupStatistics {
            row_count: 2,
            columns: vec![ColumnStatistics {
                min: SqlValue::Null,
                max: SqlValue::Null,
                null_count: 2,
                total_count: 2,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::IsNull {
            column_idx: 0,
            is_null: false,
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_and_prunes_if_any_branch_skips() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::And(vec![
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(10),
            },
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(2),
            },
        ]);
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_or_keeps_if_any_branch_may_match() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
            row_id_min: None,
            row_id_max: None,
        };
        let filter = PushdownFilter::Or(vec![
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(10),
            },
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(2),
            },
        ]);
        assert!(!ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn expr_to_pushdown_converts_eq() {
        let expr = TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(TypedExpr::column_ref(
                    "t".into(),
                    "c".into(),
                    0,
                    ResolvedType::Integer,
                    crate::Span::default(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(TypedExpr::literal(
                    Literal::Number("1".into()),
                    ResolvedType::Integer,
                    crate::Span::default(),
                )),
            },
            resolved_type: ResolvedType::Boolean,
            span: crate::Span::default(),
        };
        let filter = expr_to_pushdown(&expr).unwrap();
        assert_eq!(
            filter,
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(1)
            }
        );
    }

    #[test]
    fn execute_columnar_scan_applies_residual_filter() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_table_id(1);
        table.storage_options.storage_type = crate::catalog::StorageType::Columnar;

        // Columnar セグメントを直接書き込む。
        let schema = alopex_core::columnar::segment_v2::Schema {
            columns: vec![
                alopex_core::columnar::segment_v2::ColumnSchema {
                    name: "id".into(),
                    logical_type: alopex_core::columnar::encoding::LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
                alopex_core::columnar::segment_v2::ColumnSchema {
                    name: "name".into(),
                    logical_type: alopex_core::columnar::encoding::LogicalType::Binary,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        };
        let batch = alopex_core::columnar::segment_v2::RecordBatch::new(
            schema.clone(),
            vec![
                alopex_core::columnar::encoding::Column::Int64(vec![1]),
                alopex_core::columnar::encoding::Column::Binary(vec![b"alice".to_vec()]),
            ],
            vec![None, None],
        );
        let mut writer =
            alopex_core::columnar::segment_v2::SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        let segment = writer.finish().unwrap();

        let stats = vec![crate::columnar::statistics::compute_row_group_statistics(
            &[vec![SqlValue::Integer(1), SqlValue::Text("alice".into())]],
        )];

        let mut txn = bridge.begin_write().unwrap();
        let segment_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&segment)
            .unwrap();
        let meta_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&segment.meta)
            .unwrap();
        let stats_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&stats)
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::column_segment_key(1, 0, 0),
                segment_bytes,
            )
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::statistics_key(1, 0),
                meta_bytes,
            )
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::row_group_stats_key(1, 0),
                stats_bytes,
            )
            .unwrap();
        let index_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&vec![0u64])
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::segment_index_key(1),
                index_bytes,
            )
            .unwrap();
        txn.commit().unwrap();

        let scan = ColumnarScan::new(
            table.table_id,
            vec![0, 1],
            Some(PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(1),
            }),
            Some(TypedExpr {
                kind: TypedExprKind::BinaryOp {
                    left: Box::new(TypedExpr::column_ref(
                        "users".into(),
                        "id".into(),
                        0,
                        ResolvedType::Integer,
                        crate::Span::default(),
                    )),
                    op: BinaryOp::Eq,
                    right: Box::new(TypedExpr::literal(
                        Literal::Number("1".into()),
                        ResolvedType::Integer,
                        crate::Span::default(),
                    )),
                },
                resolved_type: ResolvedType::Boolean,
                span: crate::Span::default(),
            }),
        );

        let mut read_txn = bridge.begin_read().unwrap();
        let rows = execute_columnar_scan(&mut read_txn, &table, &scan).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1], SqlValue::Text("alice".into()));
    }

    #[test]
    fn rowid_mode_direct_prefers_rowid_column() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("row_id", ResolvedType::BigInt),
                ColumnMetadata::new("val", ResolvedType::Integer),
            ],
        )
        .with_table_id(20);
        table.storage_options.storage_type = crate::catalog::StorageType::Columnar;
        table.storage_options.row_id_mode = RowIdMode::Direct;

        let schema = alopex_core::columnar::segment_v2::Schema {
            columns: vec![
                alopex_core::columnar::segment_v2::ColumnSchema {
                    name: "row_id".into(),
                    logical_type: alopex_core::columnar::encoding::LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
                alopex_core::columnar::segment_v2::ColumnSchema {
                    name: "val".into(),
                    logical_type: alopex_core::columnar::encoding::LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        };
        let batch = alopex_core::columnar::segment_v2::RecordBatch::new(
            schema.clone(),
            vec![
                alopex_core::columnar::encoding::Column::Int64(vec![999]),
                alopex_core::columnar::encoding::Column::Int64(vec![7]),
            ],
            vec![None, None],
        );
        let mut writer =
            alopex_core::columnar::segment_v2::SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        let segment = writer.finish().unwrap();
        let stats = vec![crate::columnar::statistics::compute_row_group_statistics(
            &[vec![SqlValue::BigInt(999), SqlValue::Integer(7)]],
        )];

        persist_segment_for_test(&bridge, table.table_id, &segment, &stats);

        let scan = ColumnarScan::new(table.table_id, vec![0, 1], None, None);
        let mut read_txn = bridge.begin_read().unwrap();
        let rows = execute_columnar_scan(&mut read_txn, &table, &scan).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, 999);
        assert_eq!(rows[0].values[1], SqlValue::Integer(7));
    }

    #[test]
    fn rowid_mode_none_uses_position() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("val", ResolvedType::Integer)],
        )
        .with_table_id(21);
        table.storage_options.storage_type = crate::catalog::StorageType::Columnar;
        table.storage_options.row_id_mode = RowIdMode::Direct;

        let schema = alopex_core::columnar::segment_v2::Schema {
            columns: vec![alopex_core::columnar::segment_v2::ColumnSchema {
                name: "val".into(),
                logical_type: alopex_core::columnar::encoding::LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            }],
        };
        let batch = alopex_core::columnar::segment_v2::RecordBatch::new(
            schema.clone(),
            vec![alopex_core::columnar::encoding::Column::Int64(vec![3, 4])],
            vec![None],
        );
        let mut writer =
            alopex_core::columnar::segment_v2::SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        let segment = writer.finish().unwrap();
        let stats = vec![crate::columnar::statistics::compute_row_group_statistics(
            &[vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
        )];

        persist_segment_for_test(&bridge, table.table_id, &segment, &stats);

        let scan = ColumnarScan::new(table.table_id, vec![0], None, None);
        let mut read_txn = bridge.begin_read().unwrap();
        let rows = execute_columnar_scan(&mut read_txn, &table, &scan).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row_id, 0);
        assert_eq!(rows[1].row_id, 1);
    }

    fn persist_segment_for_test(
        bridge: &TxnBridge<MemoryKV>,
        table_id: u32,
        segment: &alopex_core::columnar::segment_v2::ColumnSegmentV2,
        row_group_stats: &[crate::columnar::statistics::RowGroupStatistics],
    ) {
        let mut txn = bridge.begin_write().unwrap();
        let segment_bytes = alopex_core::storage::format::bincode_config()
            .serialize(segment)
            .unwrap();
        let meta_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&segment.meta)
            .unwrap();
        let stats_bytes = alopex_core::storage::format::bincode_config()
            .serialize(row_group_stats)
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::column_segment_key(table_id, 0, 0),
                segment_bytes,
            )
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::statistics_key(table_id, 0),
                meta_bytes,
            )
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::row_group_stats_key(table_id, 0),
                stats_bytes,
            )
            .unwrap();
        let index_bytes = alopex_core::storage::format::bincode_config()
            .serialize(&vec![0u64])
            .unwrap();
        txn.inner_mut()
            .put(
                alopex_core::columnar::kvs_bridge::key_layout::segment_index_key(table_id),
                index_bytes,
            )
            .unwrap();
        txn.commit().unwrap();
    }
}
