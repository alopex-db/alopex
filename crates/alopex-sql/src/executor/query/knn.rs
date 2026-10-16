use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use alopex_core::columnar::encoding::Column;
use alopex_core::columnar::encoding_v2::Bitmap;
use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::columnar::segment_v2::{ColumnSegmentV2, InMemorySegmentSource, SegmentReaderV2};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::format::bincode_config;
use alopex_core::vector::hnsw::SearchStats as HnswSearchStats;
use bincode::Options;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata, RowIdMode, StorageType, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result, Row};
use crate::planner::knn_optimizer::{
    KnnPattern, SortDirection, VectorFunction, detect_knn_pattern,
};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Projection, TypedExpr};
use crate::storage::{SqlTxn, SqlValue};

use super::{columnar_scan, project};

// #449 measured exact scan as faster at 8k rows, 128 dimensions, and k=10.
const HNSW_BASE_MIN_ROWS: usize = 8_192;
const HNSW_BASE_DIMENSIONS: usize = 128;
const HNSW_BASE_K: usize = 10;
const HNSW_MINIMUM_ROWS: usize = 1_024;

#[derive(Debug, Default)]
pub(crate) struct KnnExecutionStats {
    pub(crate) hnsw_stats: Option<HnswSearchStats>,
    pub(crate) ef_search: Option<usize>,
    pub(crate) fallback: bool,
}

/// LogicalPlan が KNN 最適化パターンに合致する場合、実行に必要な情報を抽出する。
pub fn extract_knn_context(
    plan: &LogicalPlan,
) -> Option<(KnnPattern, Projection, Option<TypedExpr>)> {
    let pattern = detect_knn_pattern(plan)?;
    match plan {
        LogicalPlan::Limit { input, .. } => match input.as_ref() {
            LogicalPlan::Sort { input, .. } => match input.as_ref() {
                LogicalPlan::Filter { input, predicate } => match input.as_ref() {
                    LogicalPlan::Scan { projection, .. } => {
                        Some((pattern, projection.clone(), Some(predicate.clone())))
                    }
                    _ => None,
                },
                LogicalPlan::Scan { projection, .. } => Some((pattern, projection.clone(), None)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Returns the physical KNN path selected for a logical plan.
pub fn explain_knn_path<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    plan: &LogicalPlan,
) -> Result<Option<String>> {
    let Some((pattern, _, filter)) = extract_knn_context(plan) else {
        return Ok(None);
    };
    let Some(table) = catalog.get_table(&pattern.table) else {
        return Ok(None);
    };
    let path = match selected_hnsw_index(txn, catalog, table, &pattern, filter.as_ref())? {
        Some(index) => format_hnsw_path(&index, pattern.k, filter.is_some()),
        None => "ExactKnnScan".to_string(),
    };
    Ok(Some(path))
}

fn format_hnsw_path(index: &IndexMetadata, k: u64, has_filter: bool) -> String {
    if has_filter {
        // A filtered approximate result can be incomplete. The executor then
        // preserves SQL semantics with an exact fallback.
        format!(
            "HnswSearchPostFilter index={} k={} fallback=ExactKnnScan",
            index.name, k
        )
    } else {
        format!("HnswSearch index={} k={}", index.name, k)
    }
}

/// KNN 最適化クエリを実行する。HNSW インデックスが存在すれば索引経路、
/// それ以外はヒープベースの全件スキャンで Top-K を選択する。
pub fn execute_knn_query<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    pattern: &KnnPattern,
    projection: &Projection,
    filter: Option<&TypedExpr>,
) -> Result<ExecutionResult> {
    execute_knn_query_with_stats(txn, catalog, pattern, projection, filter)
        .map(|(result, _)| result)
}

pub(crate) fn execute_knn_query_with_stats<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    pattern: &KnnPattern,
    projection: &Projection,
    filter: Option<&TypedExpr>,
) -> Result<(ExecutionResult, KnnExecutionStats)> {
    let table_meta = catalog
        .get_table(&pattern.table)
        .cloned()
        .ok_or(ExecutorError::TableNotFound(pattern.table.clone()))?;

    if pattern.k == 0 {
        let empty = project::execute_project(Vec::new(), projection, &table_meta.columns)?;
        return Ok((ExecutionResult::Query(empty), KnnExecutionStats::default()));
    }

    let vector_idx = table_meta
        .get_column_index(&pattern.column)
        .ok_or(ExecutorError::ColumnNotFound(pattern.column.clone()))?;

    let higher_is_better = pattern.sort_direction == SortDirection::Desc;

    if let Some(index) = selected_hnsw_index(txn, catalog, &table_meta, pattern, filter)? {
        let (mut entries, stats) = execute_hnsw_search_with_stats(
            txn,
            &table_meta,
            &index,
            (projection, vector_idx, higher_is_better),
            pattern,
            filter,
        )?;
        order_entries(&mut entries, higher_is_better);
        let rows = materialize_rows_by_id(txn, &table_meta, projection, entries)?;
        let projected = project::execute_project(rows, projection, &table_meta.columns)?;
        return Ok((ExecutionResult::Query(projected), stats));
    }

    let mut entries = execute_heap_scan(
        txn,
        &table_meta,
        projection,
        filter,
        pattern,
        vector_idx,
        higher_is_better,
    )?;
    order_entries(&mut entries, higher_is_better);
    let rows = materialize_rows_by_id(txn, &table_meta, projection, entries)?;
    let projected = project::execute_project(rows, projection, &table_meta.columns)?;
    Ok((
        ExecutionResult::Query(projected),
        KnnExecutionStats::default(),
    ))
}

fn selected_hnsw_index<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    table: &TableMetadata,
    pattern: &KnnPattern,
    _filter: Option<&TypedExpr>,
) -> Result<Option<IndexMetadata>> {
    if pattern.options.enable_hnsw == Some(false) {
        return Ok(None);
    }
    let force_hnsw = pattern.options.enable_hnsw == Some(true);
    if table.storage_options.storage_type != StorageType::Row {
        if force_hnsw {
            return Err(ExecutorError::InvalidOperation {
                operation: "HNSW search".into(),
                reason: "enable_hnsw=true requires row storage".into(),
            });
        }
        return Ok(None);
    }
    let Some(index) = find_hnsw_index(catalog, table, &pattern.column) else {
        if force_hnsw {
            return Err(ExecutorError::InvalidOperation {
                operation: "HNSW search".into(),
                reason: "enable_hnsw=true requires an HNSW index on the query column".into(),
            });
        }
        return Ok(None);
    };
    if force_hnsw {
        // This forces initial HNSW selection only. execute_hnsw_search_with_stats
        // retains its exact post-filter fallback when HNSW cannot yield enough rows.
        return Ok(Some(index));
    }
    let dimension = vector_dimension(table, &pattern.column).unwrap_or(HNSW_BASE_DIMENSIONS);
    let threshold = hnsw_row_threshold(pattern.k as usize, dimension);
    let indexed_rows = txn.hnsw_entry(&index.name)?.stats().node_count;
    Ok((indexed_rows > threshold as u64).then_some(index))
}

fn hnsw_row_threshold(k: usize, dimension: usize) -> usize {
    HNSW_BASE_MIN_ROWS
        .saturating_mul(k.max(HNSW_BASE_K))
        .saturating_div(HNSW_BASE_K)
        .saturating_mul(HNSW_BASE_DIMENSIONS)
        .saturating_div(dimension.max(1))
        .max(HNSW_MINIMUM_ROWS)
}

fn vector_dimension(table: &TableMetadata, column: &str) -> Option<usize> {
    match &table
        .columns
        .get(table.get_column_index(column)?)?
        .data_type
    {
        crate::planner::types::ResolvedType::Vector { dimension, .. } => Some(*dimension as usize),
        _ => None,
    }
}

#[cfg(test)]
fn execute_hnsw_search<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    index: &IndexMetadata,
    (projection, vector_idx, higher_is_better): (&Projection, usize, bool),
    pattern: &KnnPattern,
    filter: Option<&TypedExpr>,
) -> Result<Vec<HeapEntry>> {
    execute_hnsw_search_with_stats(
        txn,
        table_meta,
        index,
        (projection, vector_idx, higher_is_better),
        pattern,
        filter,
    )
    .map(|(entries, _)| entries)
}

fn execute_hnsw_search_with_stats<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    index: &IndexMetadata,
    (projection, vector_idx, higher_is_better): (&Projection, usize, bool),
    pattern: &KnnPattern,
    filter: Option<&TypedExpr>,
) -> Result<(Vec<HeapEntry>, KnnExecutionStats)> {
    let mut requested = pattern.k as usize;
    if filter.is_some() {
        requested = requested.saturating_mul(4).max(64);
    }
    let configured_ef_search = match pattern.options.ef_search {
        Some(ef_search) => Some(ef_search),
        None => HnswBridge::search_ef(index)?,
    };
    let mut hnsw_stats = HnswSearchStats::default();
    let mut effective_ef_search = 0;

    loop {
        let ef_search = configured_ef_search.unwrap_or_else(|| requested.max(50));
        effective_ef_search = effective_ef_search.max(ef_search);
        let (hits, search_stats) = HnswBridge::search_knn(
            txn,
            &index.name,
            &pattern.query_vector,
            requested,
            Some(ef_search),
        )?;
        hnsw_stats.nodes_visited = hnsw_stats
            .nodes_visited
            .saturating_add(search_stats.nodes_visited);
        hnsw_stats.distance_computations = hnsw_stats
            .distance_computations
            .saturating_add(search_stats.distance_computations);
        hnsw_stats.search_time_us = hnsw_stats
            .search_time_us
            .saturating_add(search_stats.search_time_us);
        let exhausted = hits.len() < requested;
        let mut storage = txn.table_storage(table_meta);
        let mut entries = Vec::with_capacity(hits.len());
        for (row_id, _) in hits {
            if let Some(values) = storage.get(row_id)? {
                let row = Row::new(row_id, values);
                if let Some(predicate) = filter
                    && !evaluate_filter(predicate, &row)?
                {
                    continue;
                }
                if let Some(score) = score_row(&row, vector_idx, pattern)? {
                    entries.push(HeapEntry::new(score, row, higher_is_better));
                }
            }
        }
        if filter.is_none() || entries.len() >= pattern.k as usize || exhausted {
            if filter.is_some() && entries.len() < pattern.k as usize {
                let entries = execute_heap_scan(
                    txn,
                    table_meta,
                    projection,
                    filter,
                    pattern,
                    vector_idx,
                    higher_is_better,
                )?;
                return Ok((
                    entries,
                    KnnExecutionStats {
                        hnsw_stats: Some(hnsw_stats),
                        ef_search: Some(effective_ef_search),
                        fallback: true,
                    },
                ));
            }
            order_entries(&mut entries, higher_is_better);
            entries.truncate(pattern.k as usize);
            return Ok((
                entries,
                KnnExecutionStats {
                    hnsw_stats: Some(hnsw_stats),
                    ef_search: Some(effective_ef_search),
                    fallback: false,
                },
            ));
        }
        let next = requested.saturating_mul(2);
        if next == requested {
            return Ok((
                entries,
                KnnExecutionStats {
                    hnsw_stats: Some(hnsw_stats),
                    ef_search: Some(effective_ef_search),
                    fallback: false,
                },
            ));
        }
        requested = next;
    }
}

fn execute_heap_scan<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    projection: &Projection,
    filter: Option<&TypedExpr>,
    pattern: &KnnPattern,
    vector_idx: usize,
    higher_is_better: bool,
) -> Result<Vec<HeapEntry>> {
    match table_meta.storage_options.storage_type {
        StorageType::Columnar => collect_heap_entries(
            columnar_rows(txn, table_meta, projection, filter, vector_idx)?
                .into_iter()
                .map(Ok),
            filter,
            pattern,
            vector_idx,
            higher_is_better,
        ),
        StorageType::Row => {
            let mut storage = txn.table_storage(table_meta);
            let rows = storage.range_scan(0, u64::MAX)?.map(|entry| {
                entry
                    .map(|(row_id, values)| Row::new(row_id, values))
                    .map_err(ExecutorError::from)
            });
            collect_heap_entries(rows, filter, pattern, vector_idx, higher_is_better)
        }
    }
}

fn collect_heap_entries(
    rows: impl Iterator<Item = Result<Row>>,
    filter: Option<&TypedExpr>,
    pattern: &KnnPattern,
    vector_idx: usize,
    higher_is_better: bool,
) -> Result<Vec<HeapEntry>> {
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let k = pattern.k as usize;
    let mut evaluation_error = None;
    for row in rows {
        // The former materialized scan decoded every row before evaluating
        // expressions. Keep storage errors ahead of even earlier SQL errors.
        let row = row?;
        if evaluation_error.is_some() {
            continue;
        }
        let score = (|| {
            if let Some(predicate) = filter
                && !evaluate_filter(predicate, &row)?
            {
                return Ok(None);
            }
            score_row(&row, vector_idx, pattern)
        })();
        match score {
            Ok(Some(score)) => {
                retain_top_k(&mut heap, HeapEntry::new(score, row, higher_is_better), k);
            }
            Ok(None) => {}
            Err(error) => evaluation_error = Some(error),
        }
    }
    match evaluation_error {
        Some(error) => Err(error),
        None => Ok(heap.into_vec()),
    }
}

// The max-heap root is the worst retained candidate. Rejected rows need only
// one comparison once the heap is full; replacing the root sifts just once.
fn retain_top_k<T: Ord>(heap: &mut BinaryHeap<T>, candidate: T, k: usize) {
    if k == 0 {
        return;
    }
    if heap.len() < k {
        heap.push(candidate);
    } else if let Some(mut worst) = heap.peek_mut()
        && candidate < *worst
    {
        *worst = candidate;
    }
}

fn evaluate_filter(predicate: &TypedExpr, row: &Row) -> Result<bool> {
    let ctx = EvalContext::new(&row.values);
    let value = evaluate(predicate, &ctx)?;
    Ok(matches!(value, SqlValue::Boolean(true)))
}

fn columnar_rows<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    projection: &Projection,
    filter: Option<&TypedExpr>,
    vector_idx: usize,
) -> Result<Vec<Row>> {
    let mut scan = match filter {
        Some(predicate) => {
            columnar_scan::build_columnar_scan_for_filter(table_meta, projection.clone(), predicate)
        }
        None => columnar_scan::build_columnar_scan(table_meta, projection),
    };

    if !scan.projected_columns.contains(&vector_idx) {
        scan.projected_columns.push(vector_idx);
        scan.projected_columns.sort_unstable();
    }

    columnar_scan::execute_columnar_scan(txn, table_meta, &scan)
}

fn score_row(row: &Row, vector_idx: usize, pattern: &KnnPattern) -> Result<Option<f64>> {
    let value = row.values.get(vector_idx).ok_or(ExecutorError::Evaluation(
        crate::executor::EvaluationError::InvalidColumnRef { index: vector_idx },
    ))?;

    let vector = match value {
        SqlValue::Vector(v) => v,
        SqlValue::Null => return Ok(None),
        other => {
            return Err(ExecutorError::Evaluation(
                crate::executor::EvaluationError::TypeMismatch {
                    expected: "VECTOR".into(),
                    actual: other.type_name().into(),
                },
            ));
        }
    };

    let score = match pattern.function {
        VectorFunction::Similarity => crate::executor::evaluator::vector_ops::vector_similarity(
            vector,
            &pattern.query_vector,
            pattern.metric,
        ),
        VectorFunction::Distance => crate::executor::evaluator::vector_ops::vector_distance(
            vector,
            &pattern.query_vector,
            pattern.metric,
        ),
    };
    score
        .map(Some)
        .map_err(|e| ExecutorError::Evaluation(e.into()))
}

fn order_entries(entries: &mut [HeapEntry], higher_is_better: bool) {
    entries.sort_by(|a, b| {
        if higher_is_better {
            b.score.total_cmp(&a.score)
        } else {
            a.score.total_cmp(&b.score)
        }
    });
}

fn materialize_rows_by_id<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    projection: &Projection,
    entries: Vec<HeapEntry>,
) -> Result<Vec<Row>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    if table_meta.storage_options.storage_type == StorageType::Columnar
        && matches!(table_meta.storage_options.row_id_mode, RowIdMode::Direct)
    {
        let row_ids: Vec<u64> = entries.iter().map(|e| e.row.row_id).collect();
        return fetch_columnar_rows_by_id(txn, table_meta, projection, &row_ids);
    }
    Ok(entries.into_iter().map(|e| e.row).collect())
}

fn fetch_columnar_rows_by_id<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    projection: &Projection,
    row_ids: &[u64],
) -> Result<Vec<Row>> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let projected_columns = columnar_scan::projection_to_columns(projection, table_meta);
    let mut by_segment: BTreeMap<u64, Vec<(usize, u64, u64)>> = BTreeMap::new();
    for (pos, &row_id) in row_ids.iter().enumerate() {
        let (segment_id, offset) = alopex_core::columnar::segment_v2::decode_row_id(row_id);
        by_segment
            .entry(segment_id)
            .or_default()
            .push((pos, row_id, offset));
    }

    let mut results: Vec<Option<Row>> = vec![None; row_ids.len()];

    for (segment_id, entries) in by_segment {
        let key = key_layout::column_segment_key(table_meta.table_id, segment_id, 0);
        let bytes = txn
            .inner_mut()
            .get(&key)?
            .ok_or_else(|| ExecutorError::Columnar(format!("segment {segment_id} missing")))?;
        let segment: ColumnSegmentV2 = bincode_config()
            .deserialize(&bytes)
            .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                .and_then(|reader| reader.with_legacy_row_group_metadata(&segment.meta.row_groups))
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;

        let mut by_row_group: BTreeMap<usize, Vec<(usize, u64, usize)>> = BTreeMap::new();
        for (pos, row_id, offset) in entries {
            let (rg_idx, row_idx) = locate_row_group(&segment, offset)
                .ok_or_else(|| ExecutorError::Columnar(format!("row_id {row_id} out of range")))?;
            by_row_group
                .entry(rg_idx)
                .or_default()
                .push((pos, row_id, row_idx));
        }

        for (rg_idx, rows) in by_row_group {
            let batch = reader
                .read_row_group_by_index(&projected_columns, rg_idx)
                .map_err(|e| ExecutorError::Columnar(e.to_string()))?;
            for (pos, row_id, row_idx) in rows {
                let values = build_row_from_batch(&batch, &projected_columns, row_idx, table_meta)?;
                results[pos] = Some(Row::new(row_id, values));
            }
        }
    }

    if results.iter().any(|r| r.is_none()) {
        return Err(ExecutorError::Columnar(
            "failed to materialize some row_ids".into(),
        ));
    }
    Ok(results.into_iter().map(|r| r.unwrap()).collect())
}

fn locate_row_group(segment: &ColumnSegmentV2, local_offset: u64) -> Option<(usize, usize)> {
    for (idx, meta) in segment.meta.row_groups.iter().enumerate() {
        let start = meta.row_start;
        let end = meta.row_start.saturating_add(meta.row_count);
        if local_offset >= start && local_offset < end {
            let row_idx = (local_offset - start) as usize;
            return Some((idx, row_idx));
        }
    }
    None
}

fn build_row_from_batch(
    batch: &alopex_core::columnar::segment_v2::RecordBatch,
    projected_columns: &[usize],
    row_idx: usize,
    table_meta: &TableMetadata,
) -> Result<Vec<SqlValue>> {
    if batch.columns.len() != projected_columns.len() {
        return Err(ExecutorError::Columnar(format!(
            "projected column count mismatch: expected {}, got {}",
            projected_columns.len(),
            batch.columns.len()
        )));
    }

    let mut values = vec![SqlValue::Null; table_meta.column_count()];
    for (pos, &table_col_idx) in projected_columns.iter().enumerate() {
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
    Ok(values)
}

fn value_from_column(
    column: &Column,
    bitmap: Option<&Bitmap>,
    row_idx: usize,
    ty: &crate::planner::types::ResolvedType,
) -> Result<SqlValue> {
    if let Some(bm) = bitmap
        && !bm.get(row_idx)
    {
        return Ok(SqlValue::Null);
    }

    use crate::planner::types::ResolvedType;
    match (ty, column) {
        (ResolvedType::Integer, Column::Int64(values)) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            Ok(SqlValue::Integer(v as i32))
        }
        (
            ResolvedType::BigInt
            | ResolvedType::Timestamp
            | ResolvedType::Date
            | ResolvedType::Time,
            Column::Int64(values),
        ) => {
            let v = *values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            match ty {
                ResolvedType::Timestamp => Ok(SqlValue::Timestamp(v)),
                ResolvedType::Date => i32::try_from(v)
                    .map(SqlValue::Date)
                    .map_err(|_| ExecutorError::Columnar("date is out of range".into())),
                ResolvedType::Time => Ok(SqlValue::Time(v)),
                _ => Ok(SqlValue::BigInt(v)),
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
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f32::from_le_bytes(*bytes))
                .collect();
            Ok(SqlValue::Vector(floats))
        }
        (ResolvedType::Interval, Column::Fixed { values, .. }) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            if raw.len() != 16 {
                return Err(ExecutorError::Columnar(
                    "invalid interval byte length".into(),
                ));
            }
            Ok(SqlValue::Interval {
                months: i32::from_le_bytes(raw[0..4].try_into().unwrap()),
                days: i32::from_le_bytes(raw[4..8].try_into().unwrap()),
                micros: i64::from_le_bytes(raw[8..16].try_into().unwrap()),
            })
        }
        (ResolvedType::Decimal { scale, .. }, Column::Fixed { values, .. }) => {
            let raw = values
                .get(row_idx)
                .ok_or_else(|| ExecutorError::Columnar("row index out of bounds".into()))?;
            let coefficient = i128::from_le_bytes(
                raw.as_slice()
                    .try_into()
                    .map_err(|_| ExecutorError::Columnar("invalid decimal byte length".into()))?,
            );
            Ok(SqlValue::Decimal(crate::storage::DecimalValue::new(
                coefficient,
                *scale,
            )))
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

fn find_hnsw_index<C: Catalog + ?Sized>(
    catalog: &C,
    table: &TableMetadata,
    column: &str,
) -> Option<IndexMetadata> {
    catalog
        .get_indexes_for_table(&table.name)
        .into_iter()
        .find(|idx| {
            matches!(idx.method, Some(IndexMethod::Hnsw))
                && (idx.covers_column(column)
                    || idx
                        .column_indices
                        .first()
                        .is_some_and(|&i| table.columns.get(i).is_some_and(|c| c.name == column)))
        })
        .cloned()
}

#[derive(Debug)]
struct HeapEntry {
    score: f64,
    row: Row,
    higher_is_better: bool,
}

impl HeapEntry {
    fn new(score: f64, row: Row, higher_is_better: bool) -> Self {
        Self {
            score,
            row,
            higher_is_better,
        }
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.higher_is_better == other.higher_is_better
            && self.score.total_cmp(&other.score) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.higher_is_better {
            other.score.total_cmp(&self.score)
        } else {
            self.score.total_cmp(&other.score)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::ast::ddl::VectorMetric as AstVectorMetric;
    use crate::ast::expr::{BinaryOp, Literal};
    use crate::ast::span::Span;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_index::execute_create_index;
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::executor::dml::execute_insert;
    use crate::executor::evaluator::vector_ops::VectorMetric;
    use crate::executor::query::scan;
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::types::ResolvedType;
    use crate::storage::{SqlTransaction, TxnBridge};
    use alopex_core::kv::memory::MemoryKV;

    #[test]
    fn columnar_exact_knn_copies_vectors_and_materializes_only_projected_ids() {
        use crate::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
        use crate::planner::typed_expr::ProjectedColumn;
        use std::io::Write;
        use std::sync::RwLock;

        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store.clone());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = crate::executor::Executor::new(store, catalog.clone());
        let stmt = crate::parser::Parser::parse_sql(
            &crate::dialect::AlopexDialect,
            "CREATE TABLE items (id INT, embedding VECTOR(2, COSINE)) WITH (storage='columnar', rowid_mode='direct')",
        ).unwrap().pop().unwrap();
        let plan = crate::planner::Planner::new(&*catalog.read().unwrap())
            .plan(&stmt)
            .unwrap();
        executor.execute(plan).unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "id,embedding\n1,\"[1,0]\"\n2,NULL\n3,\"[0.8,0.6]\"\n4,\"[0,1]\"\n5,\"[-1,0]\"\n"
        )
        .unwrap();
        let catalog = catalog.read().unwrap();
        let table = catalog.get_table("items").unwrap();
        assert_eq!(table.storage_options.storage_type, StorageType::Columnar);
        assert_eq!(table.storage_options.row_id_mode, RowIdMode::Direct);
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*catalog,
            "items",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
        let projection = Projection::Columns(vec![ProjectedColumn {
            expr: TypedExpr::column_ref(
                "items".into(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::empty(),
            ),
            alias: None,
        }]);
        let filter = TypedExpr::binary_op(
            TypedExpr::column_ref(
                "items".into(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::empty(),
            ),
            BinaryOp::Gt,
            TypedExpr::literal(
                Literal::Number("1".into()),
                ResolvedType::Integer,
                Span::empty(),
            ),
            ResolvedType::Boolean,
            Span::empty(),
        );
        let mut txn = bridge.begin_read().unwrap();
        let ExecutionResult::Query(result) = execute_knn_query(
            &mut txn,
            &*catalog,
            &base_pattern(2),
            &projection,
            Some(&filter),
        )
        .unwrap() else {
            panic!("expected query")
        };
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]]
        );
    }

    #[test]
    fn streaming_heap_preserves_row_bounds_nulls_and_ties() {
        for table_id in [7, u32::MAX] {
            let (bridge, _, input_table) = setup_table();
            let table = input_table.with_table_id(table_id);
            let mut txn = bridge.begin_write().unwrap();
            for (row_id, value) in [
                (0, SqlValue::Vector(vec![1.0, 0.0])),
                (1, SqlValue::Null),
                (u64::MAX, SqlValue::Vector(vec![1.0, 0.0])),
            ] {
                txn.table_storage(&table)
                    .insert(row_id, &[SqlValue::Integer(1), value])
                    .unwrap();
            }
            let projection = Projection::All(vec!["id".into(), "embedding".into()]);
            let scanned = scan::execute_scan(&mut txn, &table).unwrap();
            assert_eq!(
                scanned.iter().map(|row| row.row_id).collect::<Vec<_>>(),
                vec![0, 1, u64::MAX]
            );
            for descending in [false, true] {
                let entries = execute_heap_scan(
                    &mut txn,
                    &table,
                    &projection,
                    None,
                    &base_pattern(4),
                    1,
                    descending,
                )
                .unwrap();
                let mut ids = entries
                    .iter()
                    .map(|entry| entry.row.row_id)
                    .collect::<Vec<_>>();
                ids.sort();
                assert_eq!(ids, vec![0, u64::MAX]);
                assert!(entries.iter().all(|entry| entry.score == 1.0));
            }
        }
    }

    #[test]
    fn streaming_heap_preserves_decode_error_priority() {
        let (bridge, catalog, _) = setup_table();
        let table = catalog.get_table("items").unwrap().clone();
        let mut txn = bridge.begin_write().unwrap();
        insert_rows(&mut txn, &catalog, &[[0.0, 0.0], [1.0, 0.0]]);
        let projection = Projection::All(vec!["id".into(), "embedding".into()]);
        let expected_evaluation = execute_heap_scan(
            &mut txn,
            &table,
            &projection,
            None,
            &base_pattern(1),
            1,
            true,
        )
        .unwrap_err()
        .to_string();
        assert!(
            expected_evaluation.contains("zero-norm"),
            "{expected_evaluation}"
        );
        txn.inner_mut()
            .put(
                crate::storage::KeyEncoder::row_key(table.table_id, u64::MAX),
                vec![],
            )
            .unwrap();
        let expected_storage = scan::execute_scan(&mut txn, &table)
            .unwrap_err()
            .to_string();
        let actual = execute_heap_scan(
            &mut txn,
            &table,
            &projection,
            None,
            &base_pattern(1),
            1,
            true,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(actual, expected_storage);
        assert_ne!(actual, expected_evaluation);
    }

    #[test]
    fn streaming_heap_keeps_first_evaluation_error_and_drains_rows() {
        let visited = std::cell::Cell::new(0);
        let rows = vec![
            Row::new(0, vec![SqlValue::Vector(vec![0.0, 0.0])]),
            Row::new(1, vec![SqlValue::Integer(1)]),
            Row::new(2, vec![SqlValue::Null]),
        ];
        let actual = collect_heap_entries(
            rows.into_iter().map(|row| {
                visited.set(visited.get() + 1);
                Ok(row)
            }),
            None,
            &base_pattern(1),
            0,
            true,
        )
        .unwrap_err();
        assert!(actual.to_string().contains("zero-norm"), "{actual}");
        assert_eq!(visited.get(), 3);
    }

    #[test]
    fn bounded_heap_matches_full_sort_and_reduces_rejected_comparisons() {
        use std::cell::Cell;
        use std::rc::Rc;

        #[derive(Eq, PartialEq)]
        struct Counted(i32, Rc<Cell<usize>>);
        impl Ord for Counted {
            fn cmp(&self, other: &Self) -> Ordering {
                self.1.set(self.1.get() + 1);
                self.0.cmp(&other.0)
            }
        }
        impl PartialOrd for Counted {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        for values in [vec![3, 1, 2, 1, 4], vec![4, 3, 2, 1, 0], vec![]] {
            for descending in [false, true] {
                let values: Vec<_> = values
                    .iter()
                    .map(|&v| if descending { -v } else { v })
                    .collect();
                for k in [0, 1, values.len(), values.len() + 1] {
                    let mut expected = values.clone();
                    expected.sort();
                    expected.truncate(k);
                    let mut heap = BinaryHeap::new();
                    for &value in &values {
                        retain_top_k(&mut heap, value, k);
                        assert!(heap.len() <= k);
                    }
                    assert_eq!(heap.into_sorted_vec(), expected);
                }
            }
        }

        let count = Rc::new(Cell::new(0));
        let mut bounded = BinaryHeap::new();
        for value in 0..1000 {
            retain_top_k(&mut bounded, Counted(value, Rc::clone(&count)), 10);
        }
        let bounded_comparisons = count.replace(0);
        let mut previous = BinaryHeap::new();
        for value in 0..1000 {
            previous.push(Counted(value, Rc::clone(&count)));
            if previous.len() > 10 {
                previous.pop();
            }
        }
        let previous_comparisons = count.get();
        eprintln!(
            "top_k_comparisons rows=1000 k=10 bounded={bounded_comparisons} previous_push_pop={previous_comparisons}"
        );
        assert!(bounded_comparisons < previous_comparisons);
        assert_eq!(
            bounded
                .into_sorted_vec()
                .into_iter()
                .map(|v| v.0)
                .collect::<Vec<_>>(),
            previous
                .into_sorted_vec()
                .into_iter()
                .map(|v| v.0)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn exact_scan_matches_sorted_scores_at_k_boundaries() {
        let (bridge, catalog, _) = setup_table();
        // CREATE TABLE assigns the persisted keyspace ID; the setup input
        // metadata still has the unregistered ID and cannot drive a scan.
        let table = catalog.get_table("items").unwrap().clone();
        let mut txn = bridge.begin_write().unwrap();
        insert_rows(
            &mut txn,
            &catalog,
            &[[1.0, 0.0], [0.0, 1.0], [0.7, 0.7], [-1.0, 0.0]],
        );
        let projection = Projection::All(
            table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            for k in [0, 1, 3, 4, 5, u64::MAX] {
                let mut pattern = base_pattern(k);
                pattern.sort_direction = direction;
                let result =
                    execute_knn_query(&mut txn, &catalog, &pattern, &projection, None).unwrap();
                let mut expected = scan::execute_scan(&mut txn, &table).unwrap();
                assert_eq!(
                    expected.len(),
                    4,
                    "oracle scan: direction={direction:?} k={k}"
                );
                expected.sort_by(|a, b| {
                    let order = score_row(a, 1, &pattern)
                        .unwrap()
                        .unwrap()
                        .total_cmp(&score_row(b, 1, &pattern).unwrap().unwrap());
                    if direction == SortDirection::Desc {
                        order.reverse()
                    } else {
                        order
                    }
                });
                expected.truncate(k as usize);
                let ExecutionResult::Query(actual) = result else {
                    panic!("expected query")
                };
                assert_eq!(
                    actual.rows,
                    expected
                        .into_iter()
                        .map(|row| row.values)
                        .collect::<Vec<_>>(),
                    "direction={direction:?} k={k}"
                );
            }
        }
        let pattern = base_pattern(1);
        assert_eq!(
            score_row(&Row::new(0, vec![SqlValue::Null]), 0, &pattern).unwrap(),
            None
        );
        assert!(score_row(&Row::new(0, vec![SqlValue::Integer(1)]), 0, &pattern).is_err());
        assert!(score_row(&Row::new(0, vec![SqlValue::Vector(vec![1.0])]), 0, &pattern).is_err());
    }

    #[test]
    fn full_heap_does_not_hide_later_score_errors() {
        let (bridge, catalog, table) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        insert_rows(&mut txn, &catalog, &[[1.0, 0.0], [0.0, 0.0]]);
        let projection = Projection::All(
            table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
        assert!(
            execute_knn_query(&mut txn, &catalog, &base_pattern(1), &projection, None).is_err()
        );
        let filter = TypedExpr::binary_op(
            TypedExpr::column_ref(
                "items".into(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::empty(),
            ),
            BinaryOp::Eq,
            TypedExpr::literal(
                Literal::Number("0".into()),
                ResolvedType::Integer,
                Span::empty(),
            ),
            ResolvedType::Boolean,
            Span::empty(),
        );
        let ExecutionResult::Query(result) = execute_knn_query(
            &mut txn,
            &catalog,
            &base_pattern(1),
            &projection,
            Some(&filter),
        )
        .unwrap() else {
            panic!("expected query")
        };
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], SqlValue::Integer(0));
    }

    #[test]
    fn hnsw_threshold_scales_with_k_and_vector_dimension() {
        assert_eq!(hnsw_row_threshold(10, 128), 8_192);
        assert_eq!(hnsw_row_threshold(20, 128), 16_384);
        assert_eq!(hnsw_row_threshold(10, 256), 4_096);
        assert_eq!(hnsw_row_threshold(10, usize::MAX), HNSW_MINIMUM_ROWS);
    }

    fn setup_table() -> (TxnBridge<MemoryKV>, MemoryCatalog, TableMetadata) {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new(
                    "embedding",
                    ResolvedType::Vector {
                        dimension: 2,
                        metric: AstVectorMetric::Cosine,
                    },
                ),
            ],
        );

        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
        ddl_txn.commit().unwrap();
        (bridge, catalog, table)
    }

    fn insert_rows(
        txn: &mut SqlTransaction<'_, MemoryKV>,
        catalog: &MemoryCatalog,
        values: &[[f64; 2]],
    ) {
        for (idx, vec) in values.iter().enumerate() {
            let row = vec![
                TypedExpr::literal(
                    Literal::Number(idx.to_string()),
                    ResolvedType::Integer,
                    Span::empty(),
                ),
                TypedExpr::vector_literal(vec![vec[0], vec[1]], 2, Span::empty()),
            ];
            execute_insert(
                txn,
                catalog,
                "items",
                vec!["id".into(), "embedding".into()],
                vec![row],
            )
            .unwrap();
        }
    }

    fn base_pattern(k: u64) -> KnnPattern {
        KnnPattern {
            table: "items".to_string(),
            column: "embedding".to_string(),
            query_vector: vec![1.0, 0.0],
            metric: VectorMetric::Cosine,
            function: VectorFunction::Similarity,
            k,
            sort_direction: SortDirection::Desc,
            options: crate::planner::logical_plan::KnnQueryOptions::default(),
        }
    }

    #[test]
    fn heap_based_knn_returns_top_k() {
        let (bridge, catalog, table) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        insert_rows(&mut txn, &catalog, &[[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]]);

        let projection = Projection::All(
            table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
        let result =
            execute_knn_query(&mut txn, &catalog, &base_pattern(2), &projection, None).unwrap();

        match result {
            ExecutionResult::Query(q) => {
                assert_eq!(q.rows.len(), 2);
                // ベクトル [1,0] が最上位、その次に [0.7,0.7]
                assert_eq!(q.rows[0][0], SqlValue::Integer(0));
                assert_eq!(q.rows[1][0], SqlValue::Integer(2));
            }
            other => panic!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn knn_uses_hnsw_when_available() {
        let (bridge, mut catalog, table) = setup_table();

        // HNSW インデックスを作成
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_index(
            &mut ddl_txn,
            &mut catalog,
            IndexMetadata::new(0, "idx_items_embedding", "items", vec!["embedding".into()])
                .with_method(IndexMethod::Hnsw),
            false,
        )
        .unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        insert_rows(&mut txn, &catalog, &[[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]]);

        let projection = Projection::All(
            table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
        let result =
            execute_knn_query(&mut txn, &catalog, &base_pattern(1), &projection, None).unwrap();

        match result {
            ExecutionResult::Query(q) => {
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.rows[0][0], SqlValue::Integer(0));
            }
            other => panic!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn knn_respects_filter() {
        let (bridge, catalog, table) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        insert_rows(&mut txn, &catalog, &[[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]]);

        let filter = TypedExpr::binary_op(
            TypedExpr::column_ref(
                "items".into(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::empty(),
            ),
            BinaryOp::Eq,
            TypedExpr::literal(
                Literal::Number("1".into()),
                ResolvedType::Integer,
                Span::empty(),
            ),
            ResolvedType::Boolean,
            Span::empty(),
        );

        let projection = Projection::All(
            table
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
        let result = execute_knn_query(
            &mut txn,
            &catalog,
            &base_pattern(2),
            &projection,
            Some(&filter),
        )
        .unwrap();

        match result {
            ExecutionResult::Query(q) => {
                // id = 1 の行のみが返る
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.rows[0][0], SqlValue::Integer(1));
            }
            other => panic!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn hnsw_post_filter_falls_back_to_exact_when_candidates_are_insufficient() {
        let (bridge, mut catalog, _) = setup_table();
        let mut values = vec![[1.0, 0.0]; 65];
        values[64] = [-1.0, 0.0];
        let mut insert_txn = bridge.begin_write().unwrap();
        insert_rows(&mut insert_txn, &catalog, &values);
        insert_txn.commit().unwrap();

        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_index(
            &mut ddl_txn,
            &mut catalog,
            IndexMetadata::new(0, "idx_items_embedding", "items", vec!["embedding".into()])
                .with_method(IndexMethod::Hnsw),
            false,
        )
        .unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let table = catalog.get_table("items").unwrap().clone();
        let filter = TypedExpr::binary_op(
            TypedExpr::column_ref(
                "items".into(),
                "id".into(),
                0,
                ResolvedType::Integer,
                Span::empty(),
            ),
            BinaryOp::Eq,
            TypedExpr::literal(
                Literal::Number("64".into()),
                ResolvedType::Integer,
                Span::empty(),
            ),
            ResolvedType::Boolean,
            Span::empty(),
        );
        let index = catalog.get_index("idx_items_embedding").unwrap().clone();
        assert!(
            scan::execute_scan(&mut txn, &table)
                .unwrap()
                .iter()
                .any(|row| row.values[0] == SqlValue::Integer(64))
        );
        let entries = execute_hnsw_search(
            &mut txn,
            &table,
            &index,
            (
                &Projection::All(
                    table
                        .column_names()
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                ),
                1,
                true,
            ),
            &base_pattern(1),
            Some(&filter),
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row.values[0], SqlValue::Integer(64));
    }

    #[test]
    fn filtered_hnsw_explain_discloses_exact_fallback() {
        let index = IndexMetadata::new(0, "idx_items_embedding", "items", vec!["embedding".into()])
            .with_method(IndexMethod::Hnsw);
        assert_eq!(
            format_hnsw_path(&index, 10, true),
            "HnswSearchPostFilter index=idx_items_embedding k=10 fallback=ExactKnnScan"
        );
    }
}
