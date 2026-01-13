use std::sync::Arc;
use std::time::Instant;

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

pub(crate) async fn search_impl(
    state: Arc<ServerState>,
    request: VectorSearchRequest,
) -> Result<VectorSearchResponse> {
    let start = Instant::now();
    let (table_meta, vector_col, metric) =
        resolve_vector_table(&state, &request.table, request.column.as_deref())?;
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
        "SELECT *, {score_expr} AS score FROM {} ORDER BY score {order} LIMIT {}",
        quote_ident(&table_meta.name),
        request.k
    );

    let mut txn = state.begin_sql_txn().await?;
    let exec_result = tokio::time::timeout(state.config.query_timeout, txn.async_execute(&sql))
        .await
        .map_err(|_| ServerError::Timeout("query timeout".into()))?;
    let exec_result = match exec_result {
        Ok(result) => {
            txn.async_rollback()
                .await
                .map_err(|err| ServerError::Sql(err.into()))?;
            result
        }
        Err(err) => {
            let _ = txn.async_rollback().await;
            return Err(ServerError::Sql(err.into()));
        }
    };

    let results = match exec_result {
        alopex_sql::executor::ExecutionResult::Query(query) => {
            let pk_index = primary_key_index(&table_meta).ok_or_else(|| {
                ServerError::BadRequest("table must have a primary key for vector search".into())
            })?;
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
        resolve_vector_table(&state, &request.table, request.column.as_deref())?;
    let pk_index = primary_key_index(&table_meta).ok_or_else(|| {
        ServerError::BadRequest("table must have a primary key for vector upsert".into())
    })?;
    let pk_name = table_meta
        .columns
        .get(pk_index)
        .map(|c| c.name.clone())
        .ok_or_else(|| ServerError::BadRequest("primary key column not found".into()))?;

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

    let mut txn = state.begin_sql_txn().await?;
    let exec_result =
        tokio::time::timeout(state.config.query_timeout, txn.async_execute(&insert_sql))
            .await
            .map_err(|_| ServerError::Timeout("query timeout".into()))?;

    let exec_result = match exec_result {
        Ok(result) => Ok(result),
        Err(err) => {
            if is_unique_violation(&err) {
                tokio::time::timeout(state.config.query_timeout, txn.async_execute(&update_sql))
                    .await
                    .map_err(|_| ServerError::Timeout("query timeout".into()))?
                    .map_err(|err| ServerError::Sql(err.into()))
            } else {
                Err(ServerError::Sql(err.into()))
            }
        }
    };

    let exec_result = match exec_result {
        Ok(result) => result,
        Err(err) => {
            let _ = txn.async_rollback().await;
            return Err(err);
        }
    };

    txn.async_commit()
        .await
        .map_err(|err| ServerError::Sql(err.into()))?;

    let response = match exec_result {
        alopex_sql::executor::ExecutionResult::Success
        | alopex_sql::executor::ExecutionResult::RowsAffected(_) => {
            Ok(VectorUpsertResponse { success: true })
        }
        _ => Err(ServerError::BadRequest(
            "vector upsert returned unexpected result".into(),
        )),
    }?;

    state.metrics.record_query(start.elapsed(), true);
    Ok(response)
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
    format!("\"{}\"", ident.replace('"', "\"\""))
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
