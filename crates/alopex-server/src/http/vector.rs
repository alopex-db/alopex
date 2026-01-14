use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;
use alopex_sql::storage::AsyncSqlTransaction;
use axum::extract::Extension;
use axum::response::Response;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
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
    let (table_meta, vector_col, metric) =
        match resolve_vector_table(&state, &request.table, request.column.as_deref()) {
            Ok(values) => values,
            Err(err) => {
                state.metrics.record_query(start.elapsed(), false);
                return Err(err);
            }
        };
    let vector_literal = format_vector_literal(&request.vector);
    let score_expr = format!(
        "vector_similarity({}, {}, '{}')",
        quote_ident(&vector_col),
        vector_literal,
        metric_to_string(metric)
    );
    let order = match metric {
        alopex_sql::ast::ddl::VectorMetric::L2 => "ASC",
        _ => "DESC",
    };
    let sql = format!(
        "SELECT *, {score_expr} AS score FROM {} ORDER BY {score_expr} {order} LIMIT {}",
        quote_ident(&table_meta.name),
        request.k
    );

    let mut txn = match state.begin_sql_txn().await {
        Ok(txn) => txn,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let exec_result =
        match tokio::time::timeout(state.config.query_timeout, txn.async_execute(&sql)).await {
            Ok(result) => result.map_err(|err| ServerError::Sql(err.into())),
            Err(_) => Err(ServerError::Timeout("query timeout".into())),
        };
    let exec_result = match exec_result {
        Ok(result) => {
            if let Err(err) = txn.async_rollback().await {
                state.metrics.record_query(start.elapsed(), false);
                return Err(ServerError::Sql(err.into()));
            }
            result
        }
        Err(err) => {
            let _ = txn.async_rollback().await;
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
    let start = Instant::now();
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

    let vector_literal = format_vector_literal(&request.vector);
    let insert_sql = format!(
        "INSERT INTO {} ({}, {}) VALUES ({}, {})",
        quote_ident(&table_meta.name),
        quote_ident(&pk_name),
        quote_ident(&vector_col),
        request.id,
        vector_literal
    );
    let update_sql = format!(
        "UPDATE {} SET {} = {} WHERE {} = {}",
        quote_ident(&table_meta.name),
        quote_ident(&vector_col),
        vector_literal,
        quote_ident(&pk_name),
        request.id
    );

    let mut txn = match state.begin_sql_txn().await {
        Ok(txn) => txn,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    let exec_result = match tokio::time::timeout(
        state.config.query_timeout,
        txn.async_execute(&insert_sql),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            let _ = txn.async_rollback().await;
            state.metrics.record_query(start.elapsed(), false);
            return Err(ServerError::Timeout("query timeout".into()));
        }
    };

    let exec_result = match exec_result {
        Ok(result) => Ok(result),
        Err(err) => {
            if is_unique_violation(&err) {
                match tokio::time::timeout(
                    state.config.query_timeout,
                    txn.async_execute(&update_sql),
                )
                .await
                {
                    Ok(result) => result.map_err(|err| ServerError::Sql(err.into())),
                    Err(_) => Err(ServerError::Timeout("query timeout".into())),
                }
            } else {
                Err(ServerError::Sql(err.into()))
            }
        }
    };

    let exec_result = match exec_result {
        Ok(result) => result,
        Err(err) => {
            let _ = txn.async_rollback().await;
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    if let Err(err) = txn.async_commit().await {
        state.metrics.record_query(start.elapsed(), false);
        return Err(ServerError::Sql(err.into()));
    }

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

    let mut txn = state.begin_sql_txn().await?;
    let exec_result = match tokio::time::timeout(
        state.config.query_timeout,
        txn.async_execute(&delete_sql),
    )
    .await
    {
        Ok(result) => result.map_err(|err| ServerError::Sql(err.into())),
        Err(_) => Err(ServerError::Timeout("query timeout".into())),
    };
    let exec_result = match exec_result {
        Ok(result) => result,
        Err(err) => {
            let _ = txn.async_rollback().await;
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    if let Err(err) = txn.async_commit().await {
        state.metrics.record_query(start.elapsed(), false);
        return Err(ServerError::Sql(err.into()));
    }

    let success = match exec_result {
        alopex_sql::executor::ExecutionResult::RowsAffected(rows) => rows > 0,
        alopex_sql::executor::ExecutionResult::Success => true,
        _ => {
            return Err(ServerError::BadRequest(
                "vector delete returned unexpected result".into(),
            ))
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
    let create_sql = match build_create_index_sql(
        &request.name,
        &request.table,
        &request.column,
        request.method.as_deref(),
        &request.options,
        false,
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
    let sql = build_drop_index_sql(&request.name, request.if_exists);
    let response = execute_index_sql(state, start, &sql).await?;
    Ok(response)
}

pub(crate) async fn index_compact_impl(
    state: Arc<ServerState>,
    request: VectorIndexCompactRequest,
) -> Result<VectorIndexResponse> {
    let start = Instant::now();
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
    let compacted = tokio::task::spawn_blocking(move || {
        let mut txn = store.begin(TxnMode::ReadWrite)?;
        let mut hnsw = HnswIndex::load(&name, &mut txn)?;
        let result = hnsw.compact()?;
        hnsw.save(&mut txn)?;
        txn.commit_self()?;
        Ok::<_, ServerError>(result)
    })
    .await
    .map_err(|err| ServerError::Internal(err.to_string()));
    let compacted = match compacted {
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
    let mut txn = state.begin_sql_txn().await?;
    let exec_result =
        match tokio::time::timeout(state.config.query_timeout, txn.async_execute(sql)).await {
            Ok(result) => result.map_err(|err| ServerError::Sql(err.into())),
            Err(_) => Err(ServerError::Timeout("query timeout".into())),
        };
    let exec_result = match exec_result {
        Ok(result) => result,
        Err(err) => {
            let _ = txn.async_rollback().await;
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };
    if let Err(err) = txn.async_commit().await {
        state.metrics.record_query(start.elapsed(), false);
        return Err(ServerError::Sql(err.into()));
    }

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
            )))
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
                    ))
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

fn is_unique_violation(err: &alopex_sql::executor::ExecutorError) -> bool {
    use alopex_sql::executor::ConstraintViolation;
    use alopex_sql::executor::ExecutorError;

    matches!(
        err,
        ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { .. })
            | ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
    )
}

fn default_k() -> usize {
    10
}
