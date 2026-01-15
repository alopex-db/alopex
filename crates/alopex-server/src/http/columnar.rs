use std::sync::Arc;

use alopex_core::columnar::encoding::Column;
use alopex_core::columnar::kvs_bridge::ColumnarKvsBridge;
use alopex_core::columnar::segment_v2::{ColumnSegmentV2, RecordBatch};
use alopex_core::columnar::ColumnarError;
use alopex_core::kv::KVTransaction;
use alopex_core::storage::format::bincode_config;
use alopex_core::types::TxnMode;
use alopex_core::KVStore;
use alopex_sql::SqlValue;
use axum::extract::Extension;
use axum::response::Response;
use axum::Json;
use bincode::config::Options;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::time::Instant;

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::server::ServerState;

#[derive(Debug, Deserialize)]
pub struct ColumnarScanRequest {
    pub segment_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarStatsRequest {
    pub segment_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarIndexCreateRequest {
    pub segment_id: String,
    pub column: String,
    pub index_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarIndexListRequest {
    pub segment_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarIndexDropRequest {
    pub segment_id: String,
    pub column: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarIngestRequest {
    pub table: String,
    pub compression: String,
    pub segment: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub struct ColumnarScanResponse {
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Debug, Serialize)]
pub struct ColumnarStatsResponse {
    pub row_count: usize,
    pub column_count: usize,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct ColumnarListResponse {
    pub segments: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ColumnarIndexInfo {
    pub column: String,
    pub index_type: String,
}

#[derive(Debug, Serialize)]
pub struct ColumnarIndexListResponse {
    pub indexes: Vec<ColumnarIndexInfo>,
}

#[derive(Debug, Serialize)]
pub struct ColumnarIngestResponse {
    pub row_count: u64,
    pub segment_id: String,
    pub size_bytes: u64,
    pub compression: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct ColumnarStatusResponse {
    pub success: bool,
}

pub async fn scan(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarScanRequest>,
) -> Response {
    match scan_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn stats(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarStatsRequest>,
) -> Response {
    match stats_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn list(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    match list_impl(state.clone()) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_create(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarIndexCreateRequest>,
) -> Response {
    match index_create_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_list(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarIndexListRequest>,
) -> Response {
    match index_list_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_drop(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarIndexDropRequest>,
) -> Response {
    match index_drop_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn ingest(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<ColumnarIngestRequest>,
) -> Response {
    match ingest_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

fn scan_impl(
    state: Arc<ServerState>,
    request: ColumnarScanRequest,
) -> Result<ColumnarScanResponse> {
    let bridge = ColumnarKvsBridge::new(state.store.clone());
    let (table_id, seg_id) = parse_segment_id(&request.segment_id)?;
    let column_count = bridge
        .column_count(table_id, seg_id)
        .map_err(map_columnar_error)?;
    let batches = bridge
        .read_segment(table_id, seg_id, &(0..column_count).collect::<Vec<_>>())
        .map_err(map_columnar_error)?;
    Ok(ColumnarScanResponse {
        rows: batches_to_rows(batches),
    })
}

fn stats_impl(
    state: Arc<ServerState>,
    request: ColumnarStatsRequest,
) -> Result<ColumnarStatsResponse> {
    let bridge = ColumnarKvsBridge::new(state.store.clone());
    let (table_id, seg_id) = parse_segment_id(&request.segment_id)?;
    let column_count = bridge
        .column_count(table_id, seg_id)
        .map_err(map_columnar_error)?;
    let batches = bridge
        .read_segment(table_id, seg_id, &(0..column_count).collect::<Vec<_>>())
        .map_err(map_columnar_error)?;
    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    Ok(ColumnarStatsResponse {
        row_count,
        column_count,
        size_bytes: 0,
    })
}

fn list_impl(state: Arc<ServerState>) -> Result<ColumnarListResponse> {
    let bridge = ColumnarKvsBridge::new(state.store.clone());
    let segments = bridge.list_segments().map_err(map_columnar_error)?;
    let segment_ids = segments
        .into_iter()
        .map(|(table_id, seg_id)| format!("{}:{}", table_id, seg_id))
        .collect();
    Ok(ColumnarListResponse {
        segments: segment_ids,
    })
}

fn index_create_impl(
    state: Arc<ServerState>,
    request: ColumnarIndexCreateRequest,
) -> Result<ColumnarStatusResponse> {
    validate_segment_exists(state.store.clone(), &request.segment_id)?;
    let index_type = parse_index_type(&request.index_type)?;
    let key = columnar_index_key(&request.segment_id, &request.column);
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(key, index_type.as_bytes().to_vec())?;
    txn.commit_self()?;
    Ok(ColumnarStatusResponse { success: true })
}

fn index_list_impl(
    state: Arc<ServerState>,
    request: ColumnarIndexListRequest,
) -> Result<ColumnarIndexListResponse> {
    validate_segment_exists(state.store.clone(), &request.segment_id)?;
    let prefix = columnar_index_prefix(&request.segment_id);
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let mut indexes = Vec::new();
    for (key, value) in txn.scan_prefix(&prefix)? {
        let column = parse_index_column(&request.segment_id, &key)?;
        let index_type = parse_index_type(std::str::from_utf8(&value).map_err(|_| {
            ServerError::BadRequest("columnar index type is not valid UTF-8".into())
        })?)?;
        indexes.push(ColumnarIndexInfo { column, index_type });
    }
    txn.commit_self()?;
    Ok(ColumnarIndexListResponse { indexes })
}

fn index_drop_impl(
    state: Arc<ServerState>,
    request: ColumnarIndexDropRequest,
) -> Result<ColumnarStatusResponse> {
    validate_segment_exists(state.store.clone(), &request.segment_id)?;
    let key = columnar_index_key(&request.segment_id, &request.column);
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let exists = txn.get(&key)?.is_some();
    if !exists {
        txn.rollback_self()?;
        return Err(ServerError::NotFound(format!(
            "columnar index {}:{} not found",
            request.segment_id, request.column
        )));
    }
    txn.delete(key)?;
    txn.commit_self()?;
    Ok(ColumnarStatusResponse { success: true })
}

fn ingest_impl(
    state: Arc<ServerState>,
    request: ColumnarIngestRequest,
) -> Result<ColumnarIngestResponse> {
    if request.table.is_empty() {
        return Err(ServerError::BadRequest("table is required".into()));
    }
    let start = Instant::now();
    let segment: ColumnSegmentV2 =
        bincode_config()
            .deserialize(&request.segment)
            .map_err(|err| {
                ServerError::BadRequest(format!("invalid columnar segment payload: {err}"))
            })?;
    let bridge = ColumnarKvsBridge::new(state.store.clone());
    let table_id = table_id(&request.table)?;
    let seg_id = bridge
        .write_segment(table_id, &segment)
        .map_err(map_columnar_error)?;
    let segment_id = format!("{}:{}", table_id, seg_id);
    Ok(ColumnarIngestResponse {
        row_count: segment.meta.num_rows,
        segment_id,
        size_bytes: segment.meta.compressed_size,
        compression: request.compression,
        elapsed_ms: start.elapsed().as_millis() as u64,
    })
}

fn validate_segment_exists(store: Arc<alopex_core::kv::AnyKV>, segment_id: &str) -> Result<()> {
    let bridge = ColumnarKvsBridge::new(store);
    let (table_id, seg_id) = parse_segment_id(segment_id)?;
    bridge
        .column_count(table_id, seg_id)
        .map_err(map_columnar_error)?;
    Ok(())
}

fn batches_to_rows(batches: Vec<RecordBatch>) -> Vec<Vec<SqlValue>> {
    let mut rows = Vec::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(batch.columns.len());
            for col in &batch.columns {
                row.push(column_value_to_sql_value(col, row_idx));
            }
            rows.push(row);
        }
    }
    rows
}

fn column_value_to_sql_value(col: &Column, row_idx: usize) -> SqlValue {
    match col {
        Column::Int64(vals) => vals
            .get(row_idx)
            .map(|&v| SqlValue::BigInt(v))
            .unwrap_or(SqlValue::Null),
        Column::Float32(vals) => vals
            .get(row_idx)
            .map(|&v| SqlValue::Float(v))
            .unwrap_or(SqlValue::Null),
        Column::Float64(vals) => vals
            .get(row_idx)
            .map(|&v| SqlValue::Double(v))
            .unwrap_or(SqlValue::Null),
        Column::Bool(vals) => vals
            .get(row_idx)
            .copied()
            .map(SqlValue::Boolean)
            .unwrap_or(SqlValue::Null),
        Column::Binary(vals) => vals
            .get(row_idx)
            .cloned()
            .map(SqlValue::Blob)
            .unwrap_or(SqlValue::Null),
        Column::Fixed { values, .. } => values
            .get(row_idx)
            .cloned()
            .map(SqlValue::Blob)
            .unwrap_or(SqlValue::Null),
    }
}

fn parse_segment_id(segment_id: &str) -> Result<(u32, u64)> {
    let parts: Vec<&str> = segment_id.split(':').collect();
    if parts.len() != 2 {
        return Err(ServerError::BadRequest(format!(
            "invalid segment ID format: expected 'table_id:segment_id', got '{}'",
            segment_id
        )));
    }

    let table_id: u32 = parts[0].parse().map_err(|_| {
        ServerError::BadRequest(format!("invalid table_id in segment ID: '{}'", parts[0]))
    })?;

    let seg_id: u64 = parts[1].parse().map_err(|_| {
        ServerError::BadRequest(format!("invalid segment_id in segment ID: '{}'", parts[1]))
    })?;

    Ok((table_id, seg_id))
}

fn table_id(table: &str) -> Result<u32> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    Ok((hasher.finish() & 0xffff_ffff) as u32)
}

const COLUMNAR_INDEX_PREFIX: &str = "__alopex_columnar_index__:";

fn columnar_index_key(segment: &str, column: &str) -> Vec<u8> {
    let mut key =
        String::with_capacity(COLUMNAR_INDEX_PREFIX.len() + segment.len() + column.len() + 1);
    key.push_str(COLUMNAR_INDEX_PREFIX);
    key.push_str(segment);
    key.push(':');
    key.push_str(column);
    key.into_bytes()
}

fn columnar_index_prefix(segment: &str) -> Vec<u8> {
    let mut key = String::with_capacity(COLUMNAR_INDEX_PREFIX.len() + segment.len() + 1);
    key.push_str(COLUMNAR_INDEX_PREFIX);
    key.push_str(segment);
    key.push(':');
    key.into_bytes()
}

fn parse_index_column(segment: &str, key: &[u8]) -> Result<String> {
    let prefix = columnar_index_prefix(segment);
    if !key.starts_with(&prefix) {
        return Err(ServerError::BadRequest(
            "columnar index key is invalid".into(),
        ));
    }
    let suffix = &key[prefix.len()..];
    String::from_utf8(suffix.to_vec())
        .map_err(|_| ServerError::BadRequest("columnar index column is not valid UTF-8".into()))
}

fn parse_index_type(raw: &str) -> Result<String> {
    match raw {
        "minmax" | "bloom" => Ok(raw.to_string()),
        other => Err(ServerError::BadRequest(format!(
            "unknown columnar index type: {other}"
        ))),
    }
}

fn map_columnar_error(err: ColumnarError) -> ServerError {
    match err {
        ColumnarError::NotFound => ServerError::NotFound("columnar segment not found".into()),
        ColumnarError::InvalidFormat(message) => {
            ServerError::BadRequest(format!("columnar segment invalid: {message}"))
        }
        ColumnarError::MemoryLimitExceeded { limit, requested } => ServerError::PayloadTooLarge(
            format!("memory limit {limit} exceeded by {requested} bytes"),
        ),
        other => ServerError::Core(other.into()),
    }
}
