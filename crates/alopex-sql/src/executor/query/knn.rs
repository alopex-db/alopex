use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use alopex_core::columnar::encoding::Column;
use alopex_core::columnar::encoding_v2::Bitmap;
use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::columnar::segment_v2::{ColumnSegmentV2, InMemorySegmentSource, SegmentReaderV2};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::format::bincode_config;
use bincode::Options;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata, RowIdMode, StorageType, TableMetadata};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutionResult, ExecutorError, Result, Row};
use crate::planner::knn_optimizer::{KnnPattern, SortDirection, detect_knn_pattern};
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Projection, TypedExpr};
use crate::storage::{SqlTxn, SqlValue};

use super::{columnar_scan, project, scan};

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

/// KNN 最適化クエリを実行する。HNSW インデックスが存在しフィルタ無しならインデックス経路、
/// それ以外はヒープベースの全件スキャンで Top-K を選択する。
pub fn execute_knn_query<'txn, S: KVStore + 'txn, C: Catalog>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &C,
    pattern: &KnnPattern,
    projection: &Projection,
    filter: Option<&TypedExpr>,
) -> Result<ExecutionResult> {
    let table_meta = catalog
        .get_table(&pattern.table)
        .cloned()
        .ok_or(ExecutorError::TableNotFound(pattern.table.clone()))?;

    if pattern.k == 0 {
        let empty = project::execute_project(Vec::new(), projection, &table_meta.columns)?;
        return Ok(ExecutionResult::Query(empty));
    }

    let vector_idx = table_meta
        .get_column_index(&pattern.column)
        .ok_or(ExecutorError::ColumnNotFound(pattern.column.clone()))?;

    let higher_is_better = pattern.sort_direction == SortDirection::Desc;

    // HNSW インデックスがあり、フィルタ無し、Row ストレージの場合のみインデックス経路を使う。
    if filter.is_none()
        && table_meta.storage_options.storage_type == StorageType::Row
        && let Some(index) = find_hnsw_index(catalog, &table_meta, &pattern.column)
    {
        let mut entries = execute_hnsw_search(
            txn,
            &table_meta,
            &index,
            vector_idx,
            pattern,
            higher_is_better,
        )?;
        order_entries(&mut entries, higher_is_better);
        let rows = materialize_rows_by_id(txn, &table_meta, projection, entries)?;
        let projected = project::execute_project(rows, projection, &table_meta.columns)?;
        return Ok(ExecutionResult::Query(projected));
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
    Ok(ExecutionResult::Query(projected))
}

fn execute_hnsw_search<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table_meta: &TableMetadata,
    index: &IndexMetadata,
    vector_idx: usize,
    pattern: &KnnPattern,
    higher_is_better: bool,
) -> Result<Vec<HeapEntry>> {
    let hits = HnswBridge::search_knn(
        txn,
        &index.name,
        &pattern.query_vector,
        pattern.k as usize,
        None,
    )?;

    let mut storage = txn.table_storage(table_meta);
    let mut entries = Vec::with_capacity(hits.len());
    for (row_id, _) in hits {
        if let Some(values) = storage.get(row_id)? {
            let row = Row::new(row_id, values);
            let score = score_row(&row, vector_idx, pattern)?;
            entries.push(HeapEntry::new(score, row, higher_is_better));
        }
    }
    Ok(entries)
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
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let k = pattern.k as usize;

    let rows = match table_meta.storage_options.storage_type {
        StorageType::Columnar => columnar_rows(txn, table_meta, projection, filter, vector_idx)?,
        StorageType::Row => scan::execute_scan(txn, table_meta)?,
    };

    for row in rows {
        if let Some(predicate) = filter
            && !evaluate_filter(predicate, &row)?
        {
            continue;
        }

        let score = score_row(&row, vector_idx, pattern)?;
        heap.push(HeapEntry::new(score, row, higher_is_better));
        if heap.len() > k {
            heap.pop();
        }
    }

    Ok(heap.into_vec())
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

fn score_row(row: &Row, vector_idx: usize, pattern: &KnnPattern) -> Result<f64> {
    let value = row.values.get(vector_idx).ok_or(ExecutorError::Evaluation(
        crate::executor::EvaluationError::InvalidColumnRef { index: vector_idx },
    ))?;

    let vector = match value {
        SqlValue::Vector(v) => v,
        other => {
            return Err(ExecutorError::Evaluation(
                crate::executor::EvaluationError::TypeMismatch {
                    expected: "VECTOR".into(),
                    actual: other.type_name().into(),
                },
            ));
        }
    };

    crate::executor::evaluator::vector_ops::vector_similarity(
        vector,
        &pattern.query_vector,
        pattern.metric,
    )
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

fn find_hnsw_index<C: Catalog>(
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
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::types::ResolvedType;
    use crate::storage::{SqlTransaction, TxnBridge};
    use alopex_core::kv::memory::MemoryKV;

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
            k,
            sort_direction: SortDirection::Desc,
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
}
