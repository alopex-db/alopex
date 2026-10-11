use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;
use axum::extract::Extension;
use axum::response::Response;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
use crate::http::sql::{
    execute_non_session_statements_with_routing,
    execute_non_session_statements_with_routing_under_catalog_read,
};
use crate::http::{error_response, json_response, RequestContext};
use crate::server::ServerState;

#[derive(Debug, Deserialize)]
pub struct VectorSearchRequest {
    pub table: String,
    pub vector: Vec<f32>,
    #[serde(default = "default_k")]
    pub k: usize,
    pub index: Option<String>,
    pub column: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VectorUpsertRequest {
    pub table: String,
    pub id: u64,
    pub vector: Vec<f32>,
    pub column: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VectorUpsertBatchRequest {
    pub table: String,
    pub vectors: Vec<VectorUpsertBatchItem>,
    pub column: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VectorUpsertBatchItem {
    pub id: u64,
    pub vector: Vec<f32>,
}

#[derive(Debug, Deserialize)]
pub struct VectorDeleteRequest {
    pub table: String,
    pub id: u64,
    pub column: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexCreateRequest {
    pub name: String,
    pub table: String,
    pub column: String,
    pub method: Option<String>,
    #[serde(default)]
    pub options: HashMap<String, String>,
    #[serde(default)]
    pub if_not_exists: bool,
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexUpdateRequest {
    pub name: String,
    pub table: String,
    pub column: String,
    pub method: Option<String>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexDeleteRequest {
    pub name: String,
    #[serde(default)]
    pub if_exists: bool,
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexCompactRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct VectorSearchResult {
    pub id: u64,
    pub distance: f32,
    pub row: Vec<alopex_sql::storage::SqlValue>,
}

#[derive(Debug, Serialize)]
pub struct VectorSearchResponse {
    pub results: Vec<VectorSearchResult>,
}

#[derive(Debug, Serialize)]
pub struct VectorUpsertResponse {
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct VectorDeleteResponse {
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct VectorIndexResponse {
    pub success: bool,
}

pub async fn search(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorSearchRequest>,
) -> Response {
    match search_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn upsert(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorUpsertRequest>,
) -> Response {
    match upsert_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn upsert_batch(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorUpsertBatchRequest>,
) -> Response {
    match upsert_batch_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorDeleteRequest>,
) -> Response {
    match delete_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_create(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorIndexCreateRequest>,
) -> Response {
    match index_create_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_update(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorIndexUpdateRequest>,
) -> Response {
    match index_update_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorIndexDeleteRequest>,
) -> Response {
    match index_delete_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn index_compact(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<VectorIndexCompactRequest>,
) -> Response {
    match index_compact_impl(state.clone(), request).await {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub(crate) async fn search_impl(
    state: Arc<ServerState>,
    request: VectorSearchRequest,
) -> Result<VectorSearchResponse> {
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + state.config.query_timeout;
    let catalog_read = match acquire_catalog_read(&state, deadline).await {
        Ok(guard) => guard,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let (table_meta, vector_col, metric) =
        match resolve_vector_table(&state, &request.table, request.column.as_deref()) {
            Ok(values) => values,
            Err(err) => {
                state.metrics.record_query(start.elapsed(), false);
                return Err(err);
            }
        };
    let vector_literal = format_vector_literal(&request.vector);
    let distance_expr = format!(
        "vector_distance({}, {}, '{}')",
        quote_ident(&vector_col),
        vector_literal,
        metric_to_string(metric)
    );
    let sql = format!(
        "SELECT *, {distance_expr} AS score FROM {} ORDER BY {distance_expr} ASC LIMIT {}",
        quote_ident(&table_meta.name),
        request.k
    );

    let exec_result = match execute_vector_sql_under_catalog_read(
        state.clone(),
        start,
        &sql,
        "vector-search",
        catalog_read,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    let results = match exec_result {
        alopex_sql::executor::ExecutionResult::Query(query) => {
            let pk_index = match primary_key_index(&table_meta) {
                Some(index) => index,
                None => {
                    state.metrics.record_query(start.elapsed(), false);
                    return Err(ServerError::BadRequest(
                        "table must have a primary key for vector search".into(),
                    ));
                }
            };
            query
                .rows
                .into_iter()
                .filter_map(|mut row| {
                    let score_value = row.pop();
                    let score = match score_value {
                        Some(alopex_sql::storage::SqlValue::Float(v)) => v,
                        Some(alopex_sql::storage::SqlValue::Double(v)) => v as f32,
                        _ => return None,
                    };
                    let id = match row.get(pk_index) {
                        Some(alopex_sql::storage::SqlValue::Integer(v)) => *v as u64,
                        Some(alopex_sql::storage::SqlValue::BigInt(v)) => *v as u64,
                        _ => return None,
                    };
                    Some(VectorSearchResult {
                        id,
                        distance: score,
                        row,
                    })
                })
                .collect()
        }
        other => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(ServerError::BadRequest(format!(
                "vector search returned non-query result: {other:?}"
            )));
        }
    };

    state.metrics.record_query(start.elapsed(), true);
    Ok(VectorSearchResponse { results })
}

pub(crate) async fn upsert_impl(
    state: Arc<ServerState>,
    request: VectorUpsertRequest,
) -> Result<VectorUpsertResponse> {
    upsert_batch_impl(
        state,
        VectorUpsertBatchRequest {
            table: request.table,
            vectors: vec![VectorUpsertBatchItem {
                id: request.id,
                vector: request.vector,
            }],
            column: request.column,
        },
    )
    .await
}

pub(crate) async fn upsert_batch_impl(
    state: Arc<ServerState>,
    request: VectorUpsertBatchRequest,
) -> Result<VectorUpsertResponse> {
    let start = Instant::now();
    state.lifecycle_state.check_write_allowed()?;
    if request.vectors.is_empty() {
        state.metrics.record_query(start.elapsed(), false);
        return Err(ServerError::BadRequest(
            "vector upsert batch must not be empty".into(),
        ));
    }
    let deadline = tokio::time::Instant::now() + state.config.query_timeout;
    let catalog_read = match acquire_catalog_read(&state, deadline).await {
        Ok(guard) => guard,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let (table_meta, vector_col, _) =
        match resolve_vector_table(&state, &request.table, request.column.as_deref()) {
            Ok(values) => values,
            Err(err) => {
                state.metrics.record_query(start.elapsed(), false);
                return Err(err);
            }
        };
    let pk_index = match primary_key_index(&table_meta) {
        Some(index) => index,
        None => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(ServerError::BadRequest(
                "table must have a primary key for vector upsert".into(),
            ));
        }
    };
    let pk_name = match table_meta.columns.get(pk_index).map(|c| c.name.clone()) {
        Some(name) => name,
        None => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(ServerError::BadRequest(
                "primary key column not found".into(),
            ));
        }
    };

    let values = request
        .vectors
        .iter()
        .map(|item| format!("({}, {})", item.id, format_vector_literal(&item.vector)))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {} ({}, {}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {} = EXCLUDED.{}",
        quote_ident(&table_meta.name),
        quote_ident(&pk_name),
        quote_ident(&vector_col),
        values,
        quote_ident(&pk_name),
        quote_ident(&vector_col),
        quote_ident(&vector_col),
    );

    let exec_result = match execute_vector_sql_under_catalog_read(
        state.clone(),
        start,
        &insert_sql,
        "vector-upsert",
        catalog_read,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    let response = match exec_result {
        alopex_sql::executor::ExecutionResult::Success
        | alopex_sql::executor::ExecutionResult::RowsAffected(_) => {
            Ok(VectorUpsertResponse { success: true })
        }
        _ => {
            state.metrics.record_query(start.elapsed(), false);
            Err(ServerError::BadRequest(
                "vector upsert returned unexpected result".into(),
            ))
        }
    }?;

    state.metrics.record_query(start.elapsed(), true);
    Ok(response)
}

pub(crate) async fn delete_impl(
    state: Arc<ServerState>,
    request: VectorDeleteRequest,
) -> Result<VectorDeleteResponse> {
    let start = Instant::now();
    state.lifecycle_state.check_write_allowed()?;
    let deadline = tokio::time::Instant::now() + state.config.query_timeout;
    let catalog_read = match acquire_catalog_read(&state, deadline).await {
        Ok(guard) => guard,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let (table_meta, _, _) =
        resolve_vector_table(&state, &request.table, request.column.as_deref())?;
    let pk_index = primary_key_index(&table_meta).ok_or_else(|| {
        ServerError::BadRequest("table must have a primary key for vector delete".into())
    })?;
    let pk_name = table_meta
        .columns
        .get(pk_index)
        .map(|c| c.name.clone())
        .ok_or_else(|| ServerError::BadRequest("primary key column not found".into()))?;

    let delete_sql = format!(
        "DELETE FROM {} WHERE {} = {}",
        quote_ident(&table_meta.name),
        quote_ident(&pk_name),
        request.id
    );

    let exec_result = match execute_vector_sql_under_catalog_read(
        state.clone(),
        start,
        &delete_sql,
        "vector-delete",
        catalog_read,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    let success = match exec_result {
        alopex_sql::executor::ExecutionResult::RowsAffected(rows) => rows > 0,
        alopex_sql::executor::ExecutionResult::Success => true,
        _ => {
            return Err(ServerError::BadRequest(
                "vector delete returned unexpected result".into(),
            ));
        }
    };

    state.metrics.record_query(start.elapsed(), true);
    Ok(VectorDeleteResponse { success })
}

pub(crate) async fn index_create_impl(
    state: Arc<ServerState>,
    request: VectorIndexCreateRequest,
) -> Result<VectorIndexResponse> {
    let start = Instant::now();
    state.lifecycle_state.check_write_allowed()?;
    let sql = match build_create_index_sql(
        &request.name,
        &request.table,
        &request.column,
        request.method.as_deref(),
        &request.options,
        request.if_not_exists,
    ) {
        Ok(sql) => sql,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let response = execute_index_sql(state, start, &sql).await?;
    Ok(response)
}

pub(crate) async fn index_update_impl(
    state: Arc<ServerState>,
    request: VectorIndexUpdateRequest,
) -> Result<VectorIndexResponse> {
    let start = Instant::now();
    state.lifecycle_state.check_write_allowed()?;
    let create_sql = match build_create_index_sql(
        &request.name,
        &request.table,
        &request.column,
        request.method.as_deref(),
        &request.options,
        true,
    ) {
        Ok(sql) => sql,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let drop_sql = build_drop_index_sql(&request.name, true);
    let sql = format!("{drop_sql}; {create_sql}");
    let response = execute_index_sql(state, start, &sql).await?;
    Ok(response)
}

pub(crate) async fn index_delete_impl(
    state: Arc<ServerState>,
    request: VectorIndexDeleteRequest,
) -> Result<VectorIndexResponse> {
    let start = Instant::now();
    state.lifecycle_state.check_write_allowed()?;
    let sql = build_drop_index_sql(&request.name, request.if_exists);
    let response = execute_index_sql(state, start, &sql).await?;
    Ok(response)
}

pub(crate) async fn index_compact_impl(
    state: Arc<ServerState>,
    request: VectorIndexCompactRequest,
) -> Result<VectorIndexResponse> {
    let start = Instant::now();
    let deadline = tokio::time::Instant::now() + state.config.query_timeout;
    state.lifecycle_state.check_write_allowed()?;
    let catalog_txn_operation = acquire_catalog_read(&state, deadline).await?;
    let index = {
        let guard = match state.catalog.read() {
            Ok(guard) => guard,
            Err(_) => {
                state.metrics.record_query(start.elapsed(), false);
                return Err(ServerError::Internal("catalog lock poisoned".into()));
            }
        };
        match guard.get_index(&request.name).cloned() {
            Some(index) => index,
            None => {
                state.metrics.record_query(start.elapsed(), false);
                return Err(ServerError::NotFound("index not found".into()));
            }
        }
    };
    if !matches!(index.method, Some(alopex_sql::ast::ddl::IndexMethod::Hnsw)) {
        state.metrics.record_query(start.elapsed(), false);
        return Err(ServerError::BadRequest(
            "index compact is only supported for HNSW".into(),
        ));
    }

    let store = state.store.clone();
    let name = request.name.clone();
    let compacted = match run_catalog_blocking_until(catalog_txn_operation, deadline, move || {
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        let mut hnsw = HnswIndex::load(&name, &mut txn)?;
        let result = hnsw.compact()?;
        hnsw.save(&mut txn)?;
        txn.commit_self()?;
        Ok(result)
    })
    .await
    {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    state.metrics.record_query(start.elapsed(), true);
    let _ = compacted;
    Ok(VectorIndexResponse { success: true })
}

async fn execute_index_sql(
    state: Arc<ServerState>,
    start: Instant,
    sql: &str,
) -> Result<VectorIndexResponse> {
    let exec_result = match execute_vector_sql(state.clone(), start, sql, "vector-index").await {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    match exec_result {
        alopex_sql::executor::ExecutionResult::Success
        | alopex_sql::executor::ExecutionResult::RowsAffected(_) => {
            state.metrics.record_query(start.elapsed(), true);
            Ok(VectorIndexResponse { success: true })
        }
        _ => {
            state.metrics.record_query(start.elapsed(), false);
            Err(ServerError::BadRequest(
                "vector index operation returned unexpected result".into(),
            ))
        }
    }
}

async fn execute_vector_sql(
    state: Arc<ServerState>,
    start: Instant,
    sql: &str,
    correlation_id: &str,
) -> Result<alopex_sql::executor::ExecutionResult> {
    let remaining = state.config.query_timeout.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        return Err(ServerError::Timeout("query timeout".into()));
    }
    let (mut results, _) =
        execute_non_session_statements_with_routing(state, sql, correlation_id, remaining).await?;
    results
        .pop()
        .ok_or_else(|| ServerError::BadRequest("vector SQL must not be empty".into()))
}

async fn execute_vector_sql_under_catalog_read(
    state: Arc<ServerState>,
    start: Instant,
    sql: &str,
    correlation_id: &str,
    catalog_read: tokio::sync::OwnedRwLockReadGuard<()>,
) -> Result<alopex_sql::executor::ExecutionResult> {
    let remaining = state.config.query_timeout.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        return Err(ServerError::Timeout("query timeout".into()));
    }
    let (mut results, _) = execute_non_session_statements_with_routing_under_catalog_read(
        state,
        sql,
        correlation_id,
        remaining,
        catalog_read,
    )
    .await?;
    results
        .pop()
        .ok_or_else(|| ServerError::BadRequest("vector SQL must not be empty".into()))
}

async fn run_catalog_blocking_until<T, F>(
    catalog_txn_operation: tokio::sync::OwnedRwLockReadGuard<()>,
    deadline: tokio::time::Instant,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    if deadline <= tokio::time::Instant::now() {
        return Err(ServerError::Timeout("query timeout".into()));
    }
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let _catalog_txn_operation = catalog_txn_operation;
            operation()
        })
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))
        .and_then(|result| result);
        if let Err(Err(err)) = response_tx.send(result) {
            tracing::error!(error = %err, "detached vector operation failed");
        }
    });
    await_vector_response_until(response_rx, deadline).await
}

async fn await_vector_response_until<T>(
    response_rx: tokio::sync::oneshot::Receiver<Result<T>>,
    deadline: tokio::time::Instant,
) -> Result<T> {
    let deadline_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(deadline_sleep);
    tokio::pin!(response_rx);
    tokio::select! {
        biased;
        _ = deadline_sleep.as_mut() => Err(ServerError::Timeout("query timeout".into())),
        response = response_rx.as_mut() => response.map_err(|_| {
            ServerError::Internal("vector operation coordinator stopped before responding".into())
        })?,
    }
}

async fn acquire_catalog_read(
    state: &Arc<ServerState>,
    deadline: tokio::time::Instant,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>> {
    if deadline <= tokio::time::Instant::now() {
        return Err(ServerError::Timeout("query timeout".into()));
    }
    let deadline_sleep = tokio::time::sleep_until(deadline);
    let read_guard = Arc::clone(&state.catalog_txn_gate).read_owned();
    tokio::pin!(deadline_sleep);
    tokio::pin!(read_guard);
    tokio::select! {
        biased;
        _ = deadline_sleep.as_mut() => Err(ServerError::Timeout("query timeout".into())),
        guard = read_guard.as_mut() => Ok(guard),
    }
}

fn build_create_index_sql(
    name: &str,
    table: &str,
    column: &str,
    method: Option<&str>,
    options: &HashMap<String, String>,
    if_not_exists: bool,
) -> Result<String> {
    if name.trim().is_empty() || table.trim().is_empty() || column.trim().is_empty() {
        return Err(ServerError::BadRequest(
            "index name, table, and column must not be empty".into(),
        ));
    }

    let method_sql = match method.map(|m| m.to_ascii_lowercase()) {
        None => String::new(),
        Some(m) if m == "hnsw" => " USING HNSW".to_string(),
        Some(m) if m == "btree" => " USING BTREE".to_string(),
        Some(other) => {
            return Err(ServerError::BadRequest(format!(
                "unsupported index method: {other}"
            )));
        }
    };
    let options_sql = build_index_options_sql(options);
    let if_not_exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
    Ok(format!(
        "CREATE INDEX{if_not_exists} {} ON {} ({}){method_sql}{options_sql}",
        quote_ident(name),
        quote_ident(table),
        quote_ident(column)
    ))
}

fn build_drop_index_sql(name: &str, if_exists: bool) -> String {
    let if_exists = if if_exists { " IF EXISTS" } else { "" };
    format!("DROP INDEX{if_exists} {}", quote_ident(name))
}

fn build_index_options_sql(options: &HashMap<String, String>) -> String {
    if options.is_empty() {
        return String::new();
    }
    let mut items = Vec::with_capacity(options.len());
    for (key, value) in options {
        let quoted = value.replace('\'', "''");
        items.push(format!("{}='{}'", quote_ident(key), quoted));
    }
    format!(" WITH ({})", items.join(", "))
}

fn resolve_vector_table(
    state: &ServerState,
    table: &str,
    column: Option<&str>,
) -> Result<(
    alopex_sql::catalog::TableMetadata,
    String,
    alopex_sql::ast::ddl::VectorMetric,
)> {
    let catalog = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    let table_meta = catalog
        .get_table(table)
        .cloned()
        .ok_or_else(|| ServerError::NotFound("table not found".into()))?;

    let (vector_col, metric) = match column {
        Some(name) => {
            let col = table_meta
                .columns
                .iter()
                .find(|c| c.name == name)
                .ok_or_else(|| ServerError::NotFound("vector column not found".into()))?;
            match &col.data_type {
                alopex_sql::planner::ResolvedType::Vector { metric, .. } => {
                    (col.name.clone(), *metric)
                }
                _ => {
                    return Err(ServerError::BadRequest(
                        "specified column is not a vector".into(),
                    ));
                }
            }
        }
        None => {
            let col = table_meta
                .columns
                .iter()
                .find(|c| {
                    matches!(
                        c.data_type,
                        alopex_sql::planner::ResolvedType::Vector { .. }
                    )
                })
                .ok_or_else(|| ServerError::BadRequest("vector column not found".into()))?;
            match &col.data_type {
                alopex_sql::planner::ResolvedType::Vector { metric, .. } => {
                    (col.name.clone(), *metric)
                }
                _ => return Err(ServerError::BadRequest("vector column not found".into())),
            }
        }
    };

    Ok((table_meta, vector_col, metric))
}

fn primary_key_index(table: &alopex_sql::catalog::TableMetadata) -> Option<usize> {
    if let Some(keys) = table.primary_key.as_ref() {
        if let Some(primary) = keys.first() {
            return table.columns.iter().position(|col| col.name == *primary);
        }
    }
    table.columns.iter().position(|col| col.primary_key)
}

fn format_vector_literal(vector: &[f32]) -> String {
    let body = vector
        .iter()
        .map(|v| format!("{v}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{body}]")
}

fn metric_to_string(metric: alopex_sql::ast::ddl::VectorMetric) -> &'static str {
    match metric {
        alopex_sql::ast::ddl::VectorMetric::Cosine => "cosine",
        alopex_sql::ast::ddl::VectorMetric::L2 => "l2",
        alopex_sql::ast::ddl::VectorMetric::Inner => "inner",
    }
}

fn quote_ident(ident: &str) -> String {
    ident.to_string()
}

fn default_k() -> usize {
    10
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn failed_vector_index_sql_records_failure_metric() {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");

        execute_index_sql(server.state.clone(), Instant::now(), "not valid SQL")
            .await
            .expect_err("invalid index SQL");

        let metrics = server.state.metrics.expose_prometheus().expect("metrics");
        assert!(metrics.contains("error_count{kind=\"query\"} 1"));
        assert!(metrics.contains("query_count 1"));
    }

    #[tokio::test]
    async fn vector_metadata_is_resolved_under_execution_catalog_guard() {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let state = server.state.clone();
        execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE vector_guard_rows (old_id INTEGER PRIMARY KEY, old_embedding VECTOR(2, L2))",
            "vector-guard-setup",
            std::time::Duration::from_secs(2),
        )
        .await
        .expect("create original table");

        let replacement_table = {
            let catalog = state.catalog.read().expect("catalog");
            let planned = alopex_sql::planner::plan_sql_for_routing(
                &*catalog,
                "CREATE TABLE replacement_vector_guard_rows (new_id INTEGER PRIMARY KEY, new_embedding VECTOR(2, L2))",
            )
            .expect("plan replacement table");
            let alopex_sql::planner::LogicalPlan::CreateTable { table, .. } = &planned[0].plan
            else {
                panic!("expected CREATE TABLE plan");
            };
            let mut table = table.clone();
            table.name = "vector_guard_rows".into();
            table
        };

        let catalog_reader = Arc::clone(&state.catalog_txn_gate).read_owned().await;
        let replacement_state = state.clone();
        let replacement = async move {
            let _ordering = Arc::clone(&replacement_state.catalog_ddl_order_gate)
                .lock_owned()
                .await;
            let _writer = Arc::clone(&replacement_state.catalog_txn_gate)
                .write_owned()
                .await;
            let mut catalog = replacement_state.catalog.write().expect("catalog");
            catalog
                .drop_table("vector_guard_rows")
                .expect("drop original table");
            catalog
                .create_table(replacement_table)
                .expect("create replacement table");
        };
        tokio::pin!(replacement);
        assert!(futures::poll!(replacement.as_mut()).is_pending());

        let mut writer_is_queued = false;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if Arc::clone(&state.catalog_txn_gate)
                .try_read_owned()
                .is_err()
            {
                writer_is_queued = true;
                break;
            }
        }
        assert!(
            writer_is_queued,
            "replacement DDL must queue its catalog writer"
        );

        let upsert = upsert_batch_impl(
            state,
            VectorUpsertBatchRequest {
                table: "vector_guard_rows".into(),
                vectors: vec![VectorUpsertBatchItem {
                    id: 1,
                    vector: vec![1.0, 0.0],
                }],
                column: Some("old_embedding".into()),
            },
        );
        tokio::pin!(upsert);
        assert!(futures::poll!(upsert.as_mut()).is_pending());

        drop(catalog_reader);
        replacement.await;
        let error = upsert
            .await
            .expect_err("the replacement schema must reject the old vector column");
        assert!(
            matches!(error, ServerError::NotFound(ref message) if message == "vector column not found")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_read_deadline_expires_without_acquiring_after_release() {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let state = server.state.clone();
        let write_guard = Arc::clone(&state.catalog_txn_gate).write_owned().await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let read_guard = acquire_catalog_read(&state, deadline);
        tokio::pin!(read_guard);
        assert!(futures::poll!(read_guard.as_mut()).is_pending());

        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        drop(write_guard);
        assert!(matches!(read_guard.await, Err(ServerError::Timeout(_))));
        assert!(Arc::clone(&state.catalog_txn_gate)
            .try_write_owned()
            .is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_does_not_start_blocking_vector_operation() {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let guard = Arc::clone(&server.state.catalog_txn_gate)
            .read_owned()
            .await;
        let started = Arc::new(AtomicBool::new(false));
        let started_for_operation = Arc::clone(&started);

        let result = run_catalog_blocking_until(guard, tokio::time::Instant::now(), move || {
            started_for_operation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert!(matches!(result, Err(ServerError::Timeout(_))));
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_wins_over_ready_vector_response() {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        response_tx.send(Ok(())).expect("response receiver");

        let result = await_vector_response_until(response_rx, tokio::time::Instant::now()).await;

        assert!(matches!(result, Err(ServerError::Timeout(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_blocking_vector_operation_holds_catalog_guard_until_completion() {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let state = server.state.clone();
        let guard = Arc::clone(&state.catalog_txn_gate).read_owned().await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let operation = tokio::spawn(run_catalog_blocking_until(guard, deadline, move || {
            started_tx.send(()).expect("started receiver");
            release_rx.recv().expect("operation release");
            Ok(())
        }));
        started_rx.await.expect("blocking operation starts");

        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert!(matches!(
            operation.await.expect("operation task"),
            Err(ServerError::Timeout(_))
        ));
        assert!(Arc::clone(&state.catalog_txn_gate)
            .try_write_owned()
            .is_err());

        release_tx.send(()).expect("release operation");
        for _ in 0..20 {
            if Arc::clone(&state.catalog_txn_gate)
                .try_write_owned()
                .is_ok()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("catalog guard must be released after blocking operation completion");
    }
}
