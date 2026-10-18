use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_cluster::{
    CatalogTableRef, CatalogTableSnapshot, PlacementCatalog, PlanId, QueryRouter,
    QueryRoutingRequest, QueryTableReference, QueryTableReferenceAccess, QueryTableReferenceSource,
    RequestId, RoutingDecisionKind, RoutingDiagnostics, TableLifecycleEffect, TableRef,
};
use alopex_core::kv::async_adapter::AsyncKVTransactionAdapter;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::format::bincode_config;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::persistent::{PersistedTableMeta, TableFqn, TABLES_PREFIX};
use alopex_sql::catalog::{IndexMetadata, TableMetadata};
use alopex_sql::planner::{
    LogicalPlan, PlannedStatement, TableReference, TableReferenceAccess, TableReferenceSource,
};
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use alopex_sql::storage::AsyncSqlTransaction;
use alopex_sql::AlopexDialect;
use axum::extract::{Extension, Path};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bincode::Options;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::ops::distributed_read::{
    PreparedReadLease, ReadCancellation, ReadExecutionOutcome, ReadExecutionSummary,
};
use crate::ops::memory::MemoryControlPolicy;
use crate::server::{CatalogTxnOperation, ServerState};
use crate::session::{CatalogRollbackEffect, SessionCatalogTxnGuardKind, SessionId, TxnHandle};

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub streaming: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfoResponse {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SqlResultResponse {
    pub columns: Vec<ColumnInfoResponse>,
    pub rows: Vec<Vec<alopex_sql::storage::SqlValue>>,
    pub affected_rows: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct SqlResponse {
    /// Legacy single-result fields. They contain the last statement result so
    /// existing clients can migrate to `results` without an abrupt break.
    #[serde(flatten)]
    pub last_result: SqlResultResponse,
    /// Results in input statement order.
    pub results: Vec<SqlResultResponse>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routing_diagnostics: Vec<RoutingDiagnostics>,
}

/// Explicit remote-read entry point. The coordinator registration is kept
/// separate from legacy `/sql`, so an unsupported distributed request can
/// never silently execute as a local SQL query.
#[derive(Debug, Deserialize)]
pub struct DistributedReadRequest {
    pub sql: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub read_mode: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DistributedReadStreamItem {
    Prepared {
        execution_id: RequestId,
        columns: Vec<ColumnInfoResponse>,
    },
    Row {
        values: Vec<alopex_sql::storage::SqlValue>,
    },
    Terminal {
        summary: ReadExecutionSummary,
    },
}

#[derive(Debug, Serialize)]
struct DistributedReadCancelResponse {
    #[serde(flatten)]
    cancellation: ReadCancellation,
}

#[derive(Debug, Serialize)]
struct StreamItem {
    row: Option<Vec<alopex_sql::storage::SqlValue>>,
    error: Option<StreamError>,
    done: bool,
}

#[derive(Debug, Serialize)]
struct StreamError {
    code: String,
    message: String,
    correlation_id: String,
}

type AsyncTxn = AsyncTxnBridge<'static, AsyncKVTransactionAdapter>;

enum StreamSource {
    Txn {
        txn: AsyncTxn,
        _catalog_txn_operation: CatalogTxnOperation,
    },
    Handle {
        handle: TxnHandle,
        operation: tokio::sync::OwnedMutexGuard<()>,
        _catalog_txn_operation: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    },
}

struct RoutingPlan {
    planned: Vec<PlannedStatement>,
    diagnostics: Vec<RoutingDiagnostics>,
}

#[derive(Clone)]
struct TableLifecycleState {
    table_ref: TableRef,
    table_id: u32,
    table: TableMetadata,
}

enum TableLifecycleCandidate {
    Created {
        table_name: String,
        before: Option<TableLifecycleState>,
    },
    Dropped {
        table_name: String,
        before: Option<TableLifecycleState>,
        indexes: Vec<IndexMetadata>,
    },
    CreateIndex {
        index_name: String,
        before: Option<(TableLifecycleState, IndexMetadata)>,
    },
    DropIndex {
        index_name: String,
        before: Option<(TableLifecycleState, IndexMetadata)>,
    },
}

pub async fn handle(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<SqlRequest>,
) -> Response {
    if request.sql.trim().is_empty() {
        return error_response(
            ServerError::BadRequest("sql must not be empty".into()),
            &ctx,
        );
    }

    if request.streaming {
        return stream_response(state, request, &ctx);
    }

    let result = execute_non_streaming(state.clone(), &request, &ctx).await;
    match result {
        Ok(response) => json_response(response, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

/// Reject an explicit distributed-read request when this server has no
/// registered fenced range-read coordinator. This route intentionally does
/// not call the legacy local SQL route: capability absence is observable and
/// classified before a query can produce local rows.
pub async fn begin_distributed_read(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<DistributedReadRequest>,
) -> Response {
    if request.sql.trim().is_empty() {
        return error_response(
            ServerError::BadRequest("sql must not be empty".into()),
            &ctx,
        );
    }
    if let Err(error) =
        crate::http::session::distributed_read_owner(&state, &ctx, request.session_id.as_deref())
            .await
    {
        return error_response(error, &ctx);
    }
    let requested_mode = request.read_mode.as_deref().unwrap_or("inherit");
    error_response(
        ServerError::CapabilityUnavailable(format!(
            "distributed read mode '{requested_mode}' has no registered fenced range-read coordinator; the SQL was not executed locally"
        )),
        &ctx,
    )
}

/// Stream an already prepared distributed result. A coordinator registers the
/// execution before dispatch and calls `publish_prepared` only after every
/// range has supplied its P2.10 cleanup acknowledgement through P2.11.
pub async fn stream_distributed_read(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(execution_id): Path<String>,
) -> Response {
    if execution_id.trim().is_empty() {
        return error_response(
            ServerError::BadRequest("distributed read execution id must not be empty".into()),
            &ctx,
        );
    }
    let execution_id = RequestId::new(execution_id);
    let lease = match state
        .distributed_read_registry
        .open_prepared(&execution_id, ctx.actor.as_deref())
    {
        Ok(lease) => lease,
        Err(error) => return error_response(error, &ctx),
    };
    if let Err(error) = preflight_distributed_stream(&lease, state.config.max_response_size) {
        // Dropping an unconsumed lease dispatches the same cancellation and
        // cleanup callbacks as an explicit HTTP cancellation request.
        return error_response(error, &ctx);
    }

    let prepared = DistributedReadStreamItem::Prepared {
        execution_id: execution_id.clone(),
        columns: lease
            .columns()
            .iter()
            .map(|column| ColumnInfoResponse {
                name: column.name.clone(),
                data_type: type_to_string(&column.data_type),
            })
            .collect(),
    };
    let (sender, receiver) = mpsc::channel(32);
    tokio::spawn(async move {
        if sender.send(prepared).await.is_err() {
            return;
        }
        stream_prepared_distributed_rows(lease, sender).await;
    });

    let stream = ReceiverStream::new(receiver).map(|item| {
        let json = serde_json::to_string(&item).unwrap_or_else(|_| {
            "{\"type\":\"terminal\",\"summary\":{\"outcome\":\"terminal_failure\",\"reason\":\"response serialization failed\"}}".to_string()
        });
        Ok::<axum::body::Bytes, Infallible>(axum::body::Bytes::from(json + "\n"))
    });
    let body = axum::body::Body::from_stream(stream);
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/jsonl")
        .body(body)
        .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Deliver an idempotent cancellation to the coordinator and every registered
/// range peer. Ownership is verified against the authenticated HTTP profile.
pub async fn cancel_distributed_read(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(execution_id): Path<String>,
) -> Response {
    if execution_id.trim().is_empty() {
        return error_response(
            ServerError::BadRequest("distributed read execution id must not be empty".into()),
            &ctx,
        );
    }
    let execution_id = RequestId::new(execution_id);
    match state
        .distributed_read_registry
        .cancel(&execution_id, ctx.actor.as_deref())
    {
        Ok(cancellation) => json_response(
            DistributedReadCancelResponse { cancellation },
            state.config.max_response_size,
            &ctx,
        ),
        Err(error) => error_response(error, &ctx),
    }
}

async fn stream_prepared_distributed_rows(
    mut lease: PreparedReadLease,
    sender: mpsc::Sender<DistributedReadStreamItem>,
) {
    loop {
        let summary = lease.summary();
        if summary.outcome != ReadExecutionOutcome::Success {
            let _ = sender
                .send(DistributedReadStreamItem::Terminal { summary })
                .await;
            lease.finish();
            return;
        }
        let Some(values) = lease.next_row() else {
            let summary = lease.summary();
            let _ = sender
                .send(DistributedReadStreamItem::Terminal { summary })
                .await;
            lease.finish();
            return;
        };
        if sender
            .send(DistributedReadStreamItem::Row { values })
            .await
            .is_err()
        {
            // The lease Drop implementation invokes the cancellation registry
            // and releases any still-active peer work exactly once.
            return;
        }
    }
}

fn preflight_distributed_stream(lease: &PreparedReadLease, max_size: usize) -> Result<()> {
    let header = DistributedReadStreamItem::Prepared {
        execution_id: lease.summary().execution_id,
        columns: lease
            .columns()
            .iter()
            .map(|column| ColumnInfoResponse {
                name: column.name.clone(),
                data_type: type_to_string(&column.data_type),
            })
            .collect(),
    };
    let mut total = serialized_jsonl_size(&header)?;
    let mut preview = lease.preview_stream().ok_or_else(|| {
        ServerError::Conflict("distributed read result has already been released".into())
    })?;
    while let Some(values) = preview.next_row() {
        total = total.saturating_add(serialized_jsonl_size(&DistributedReadStreamItem::Row {
            values,
        })?);
        if total > max_size {
            return Err(ServerError::PayloadTooLarge(
                "response size exceeds limit".into(),
            ));
        }
    }
    total = total.saturating_add(serialized_jsonl_size(
        &DistributedReadStreamItem::Terminal {
            summary: lease.summary(),
        },
    )?);
    if total > max_size {
        return Err(ServerError::PayloadTooLarge(
            "response size exceeds limit".into(),
        ));
    }
    Ok(())
}

fn serialized_jsonl_size<T: Serialize>(item: &T) -> Result<usize> {
    serde_json::to_vec(item)
        .map(|encoded| encoded.len().saturating_add(1))
        .map_err(|error| ServerError::Internal(error.to_string()))
}

/// SQL を非ストリーミングで実行する共有経路。
///
/// HTTP `/sql` と gRPC `ExecuteSql` (issue #25) の両方から呼ばれる。
/// SQL の実行セマンティクス (タイムアウト・コミット/ロールバック・
/// 監査ログ・カタログ同期・メトリクス) はこの関数に集約する。
pub(crate) async fn execute_non_streaming(
    state: Arc<ServerState>,
    request: &SqlRequest,
    ctx: &RequestContext,
) -> Result<SqlResponse> {
    let start = Instant::now();
    let sql = request.sql.as_str();
    let is_ddl = is_ddl(sql);
    if is_write_sql(sql) {
        state.lifecycle_state.check_write_allowed()?;
    }

    let exec_result: Result<(
        Vec<alopex_sql::executor::ExecutionResult>,
        Vec<RoutingDiagnostics>,
    )> = async {
        if let Some(session_id) = &request.session_id {
            let session_id = session_id
                .parse::<SessionId>()
                .map_err(|_| ServerError::BadRequest("invalid session_id".into()))?;
            execute_session_statements_with_routing(
                state.clone(),
                &session_id,
                sql,
                &ctx.correlation_id,
                state.config.query_timeout,
            )
            .await
        } else {
            execute_non_session_statements_with_routing(
                state.clone(),
                sql,
                &ctx.correlation_id,
                state.config.query_timeout,
            )
            .await
        }
    }
    .await;
    let exec_result = match exec_result {
        Ok(result) => result,
        Err(err) => {
            state.metrics.record_query(start.elapsed(), false);
            return Err(err);
        }
    };

    if state.config.audit_log_enabled && is_ddl {
        state
            .audit
            .log_ddl(sql, ctx.actor.as_deref(), &ctx.correlation_id);
    }

    state.metrics.record_query(start.elapsed(), true);

    Ok(map_execution_results(exec_result.0, exec_result.1))
}

pub(crate) async fn execute_session_statements_with_routing(
    state: Arc<ServerState>,
    session_id: &SessionId,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    Vec<alopex_sql::executor::ExecutionResult>,
    Vec<RoutingDiagnostics>,
)> {
    let deadline = tokio::time::Instant::now() + timeout;
    let handle = match wait_until_deadline(
        deadline,
        state.session_manager.get_transaction(session_id),
    )
    .await
    {
        SqlDeadline::Completed(result) => result?,
        SqlDeadline::TimedOut { .. } => {
            return Err(ServerError::Timeout("query timeout".into()));
        }
    };
    let operation = match wait_until_deadline(deadline, handle.lock_operation()).await {
        SqlDeadline::Completed(operation) => operation,
        SqlDeadline::TimedOut { .. } => {
            return Err(ServerError::Timeout("query timeout".into()));
        }
    };
    let schema_mutation = is_ddl(sql);
    let retain_catalog_reader = is_write_sql(sql) && !schema_mutation;
    let guard_kind = handle.catalog_txn_guard_kind(&operation).await;
    let catalog_txn_operation = match (guard_kind, schema_mutation) {
        (SessionCatalogTxnGuardKind::Write, _) | (SessionCatalogTxnGuardKind::Read, false) => None,
        (SessionCatalogTxnGuardKind::Read, true) => {
            let ordering = Arc::clone(&state.catalog_ddl_order_gate)
                .try_lock_owned()
                .map_err(|_| {
                    ServerError::Conflict(
                        "catalog DDL is already waiting for this transaction; retry after rollback"
                            .into(),
                    )
                })?;
            drop(handle.take_catalog_txn_guard(&operation).await);
            let catalog = match wait_until_deadline(
                deadline,
                Arc::clone(&state.catalog_txn_gate).write_owned(),
            )
            .await
            {
                SqlDeadline::Completed(guard) => guard,
                SqlDeadline::TimedOut { .. } => {
                    let read = Arc::clone(&state.catalog_txn_gate).read_owned().await;
                    handle.store_catalog_read_guard(read, &operation).await;
                    drop(ordering);
                    return Err(ServerError::Timeout("query timeout".into()));
                }
            };
            handle
                .store_catalog_write_guard(catalog, ordering, &operation)
                .await;
            None
        }
        (SessionCatalogTxnGuardKind::None, true) => {
            let ordering = match wait_until_deadline(
                deadline,
                Arc::clone(&state.catalog_ddl_order_gate).lock_owned(),
            )
            .await
            {
                SqlDeadline::Completed(guard) => guard,
                SqlDeadline::TimedOut { .. } => {
                    return Err(ServerError::Timeout("query timeout".into()));
                }
            };
            let catalog = match wait_until_deadline(
                deadline,
                Arc::clone(&state.catalog_txn_gate).write_owned(),
            )
            .await
            {
                SqlDeadline::Completed(guard) => guard,
                SqlDeadline::TimedOut { .. } => {
                    return Err(ServerError::Timeout("query timeout".into()));
                }
            };
            Some(CatalogTxnOperation::Write {
                _guard: catalog,
                _ordering: ordering,
            })
        }
        (SessionCatalogTxnGuardKind::None, false) => {
            let catalog = match wait_until_deadline(
                deadline,
                Arc::clone(&state.catalog_txn_gate).read_owned(),
            )
            .await
            {
                SqlDeadline::Completed(guard) => guard,
                SqlDeadline::TimedOut { .. } => {
                    return Err(ServerError::Timeout("query timeout".into()));
                }
            };
            Some(CatalogTxnOperation::Read { _guard: catalog })
        }
    };
    let routing_plan = match wait_until_deadline(
        deadline,
        route_session_sql(&state, &handle, sql, correlation_id),
    )
    .await
    {
        SqlDeadline::Completed(result) => result?,
        SqlDeadline::TimedOut { .. } => {
            return Err(ServerError::Timeout("query timeout".into()));
        }
    };
    if let Some(diagnostic) = future_distributed_diagnostic(&routing_plan.diagnostics) {
        return Err(future_distributed_error(diagnostic));
    }
    let diagnostics = routing_plan.diagnostics;
    let planned = routing_plan.planned;
    let sql = sql.to_string();
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut response_tx = Some(response_tx);
        let lifecycle_candidates = match table_lifecycle_candidates(&state, &planned) {
            Ok(candidates) => candidates,
            Err(err) => {
                if let Some(response_tx) = response_tx.take() {
                    let _ = response_tx.send(Err(err));
                }
                return;
            }
        };
        if response_tx
            .as_ref()
            .is_some_and(tokio::sync::oneshot::Sender::is_closed)
        {
            drop(operation);
            return;
        }
        let acquired_catalog_writer = matches!(
            &catalog_txn_operation,
            Some(CatalogTxnOperation::Write { .. })
        );
        let _catalog_txn_operation = match catalog_txn_operation {
            Some(CatalogTxnOperation::Write {
                _guard: guard,
                _ordering: ordering,
            }) => {
                handle
                    .store_catalog_write_guard(guard, ordering, &operation)
                    .await;
                None
            }
            Some(CatalogTxnOperation::Read { _guard: guard }) if retain_catalog_reader => {
                handle.store_catalog_read_guard(guard, &operation).await;
                None
            }
            operation => operation,
        };
        let execution_outcome = {
            let execution = handle.execute_multi_with_operation(&sql, &operation);
            tokio::pin!(execution);
            match wait_for_sql_deadline(execution.as_mut(), deadline).await {
                SqlDeadline::TimedOut { started: false } => {
                    if let Some(response_tx) = response_tx.take() {
                        let _ = response_tx.send(Err(ServerError::Timeout("query timeout".into())));
                    }
                    None
                }
                SqlDeadline::TimedOut { started: true } => {
                    if let Some(response_tx) = response_tx.take() {
                        let _ = response_tx.send(Err(ServerError::Timeout("query timeout".into())));
                    }
                    Some((execution.as_mut().await, true))
                }
                SqlDeadline::Completed(result) => Some((result, false)),
            }
        };
        let Some((result, timed_out)) = execution_outcome else {
            if acquired_catalog_writer || retain_catalog_reader {
                drop(handle.take_catalog_txn_guard(&operation).await);
            }
            drop(operation);
            return;
        };

        let bookkeeping = statement_effects_after_execution(&state, lifecycle_candidates);
        let completion = match bookkeeping {
            Ok((lifecycle_effects, catalog_rollback_effects)) => {
                if acquired_catalog_writer
                    && result.is_err()
                    && lifecycle_effects.is_empty()
                    && catalog_rollback_effects.is_empty()
                {
                    drop(handle.take_catalog_txn_guard(&operation).await);
                }
                handle
                    .buffer_table_lifecycle_effects(lifecycle_effects)
                    .await;
                handle
                    .buffer_catalog_rollback_effects(catalog_rollback_effects)
                    .await;
                result.map_err(|err| ServerError::Sql(err.into()))
            }
            Err(err) => Err(err),
        };

        if timed_out {
            if let Err(err) = completion {
                tracing::error!(error = %err, "timed-out session SQL coordinator failed after responding");
            }
        } else if let Some(response_tx) = response_tx.take() {
            let _ = response_tx.send(completion);
        }
        drop(operation);
    });
    let result = await_sql_coordinator_response_until(response_rx, deadline).await?;
    Ok((result, diagnostics))
}

pub(crate) async fn execute_session_statement_with_routing(
    state: Arc<ServerState>,
    session_id: &SessionId,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    alopex_sql::executor::ExecutionResult,
    Vec<RoutingDiagnostics>,
)> {
    let (mut results, diagnostics) =
        execute_session_statements_with_routing(state, session_id, sql, correlation_id, timeout)
            .await?;
    let result = results
        .pop()
        .ok_or_else(|| ServerError::BadRequest("sql must not be empty".into()))?;
    Ok((result, diagnostics))
}

pub(crate) async fn execute_non_session_statements_with_routing(
    state: Arc<ServerState>,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    Vec<alopex_sql::executor::ExecutionResult>,
    Vec<RoutingDiagnostics>,
)> {
    execute_non_session_statements_with_routing_inner(state, sql, correlation_id, timeout, None)
        .await
}

pub(crate) async fn execute_non_session_statements_with_routing_under_catalog_read(
    state: Arc<ServerState>,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
    catalog_read: tokio::sync::OwnedRwLockReadGuard<()>,
) -> Result<(
    Vec<alopex_sql::executor::ExecutionResult>,
    Vec<RoutingDiagnostics>,
)> {
    if is_ddl(sql) {
        return Err(ServerError::Internal(
            "pre-acquired catalog read guard cannot execute DDL".into(),
        ));
    }
    execute_non_session_statements_with_routing_inner(
        state,
        sql,
        correlation_id,
        timeout,
        Some(catalog_read),
    )
    .await
}

async fn execute_non_session_statements_with_routing_inner(
    state: Arc<ServerState>,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
    catalog_read: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
) -> Result<(
    Vec<alopex_sql::executor::ExecutionResult>,
    Vec<RoutingDiagnostics>,
)> {
    let deadline = tokio::time::Instant::now() + timeout;
    let sql = sql.to_string();
    let correlation_id = correlation_id.to_string();
    let schema_mutation = is_ddl(&sql);
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut response_tx = Some(response_tx);
        let catalog_txn_operation = if schema_mutation {
            let ordering = match wait_until_deadline(
                deadline,
                Arc::clone(&state.catalog_ddl_order_gate).lock_owned(),
            )
            .await
            {
                SqlDeadline::Completed(guard) => guard,
                SqlDeadline::TimedOut { .. } => {
                    let _ = response_tx
                        .take()
                        .expect("response sender")
                        .send(Err(ServerError::Timeout("query timeout".into())));
                    return;
                }
            };
            match wait_until_deadline(deadline, Arc::clone(&state.catalog_txn_gate).write_owned())
                .await
            {
                SqlDeadline::Completed(guard) => CatalogTxnOperation::Write {
                    _guard: guard,
                    _ordering: ordering,
                },
                SqlDeadline::TimedOut { .. } => {
                    let _ = response_tx
                        .take()
                        .expect("response sender")
                        .send(Err(ServerError::Timeout("query timeout".into())));
                    return;
                }
            }
        } else if let Some(guard) = catalog_read {
            CatalogTxnOperation::Read { _guard: guard }
        } else {
            match wait_until_deadline(deadline, Arc::clone(&state.catalog_txn_gate).read_owned())
                .await
            {
                SqlDeadline::Completed(guard) => CatalogTxnOperation::Read { _guard: guard },
                SqlDeadline::TimedOut { .. } => {
                    let _ = response_tx
                        .take()
                        .expect("response sender")
                        .send(Err(ServerError::Timeout("query timeout".into())));
                    return;
                }
            }
        };
        let mut txn = match wait_until_deadline(deadline, state.begin_sql_txn()).await {
            SqlDeadline::Completed(Ok(txn)) => txn,
            SqlDeadline::Completed(Err(err)) => {
                let _ = response_tx.take().expect("response sender").send(Err(err));
                return;
            }
            SqlDeadline::TimedOut { .. } => {
                let _ = response_tx
                    .take()
                    .expect("response sender")
                    .send(Err(ServerError::Timeout("query timeout".into())));
                return;
            }
        };
        let routing_plan = match wait_until_deadline(
            deadline,
            route_non_session_sql(&state, &txn, &sql, &correlation_id),
        )
        .await
        {
            SqlDeadline::Completed(Ok(plan)) => plan,
            SqlDeadline::Completed(Err(err)) => {
                let _ = txn.async_rollback().await;
                let _ = response_tx.take().expect("response sender").send(Err(err));
                return;
            }
            SqlDeadline::TimedOut { .. } => {
                let _ = response_tx
                    .take()
                    .expect("response sender")
                    .send(Err(ServerError::Timeout("query timeout".into())));
                let _ = txn.async_rollback().await;
                return;
            }
        };
        if let Some(diagnostic) = future_distributed_diagnostic(&routing_plan.diagnostics) {
            let err = future_distributed_error(diagnostic);
            let _ = txn.async_rollback().await;
            let _ = response_tx.take().expect("response sender").send(Err(err));
            return;
        }
        let diagnostics = routing_plan.diagnostics;
        let lifecycle_candidates = match table_lifecycle_candidates(&state, &routing_plan.planned) {
            Ok(candidates) => candidates,
            Err(err) => {
                let _ = txn.async_rollback().await;
                let _ = response_tx.take().expect("response sender").send(Err(err));
                return;
            }
        };
        if response_tx
            .as_ref()
            .is_some_and(tokio::sync::oneshot::Sender::is_closed)
        {
            let _ = txn.async_rollback().await;
            drop(catalog_txn_operation);
            return;
        }
        let execution_outcome = {
            let execution = txn.async_execute_multi(&sql);
            tokio::pin!(execution);
            match wait_for_sql_deadline(execution.as_mut(), deadline).await {
                SqlDeadline::TimedOut { started: false } => {
                    if let Some(response_tx) = response_tx.take() {
                        let _ = response_tx.send(Err(ServerError::Timeout("query timeout".into())));
                    }
                    None
                }
                SqlDeadline::TimedOut { started: true } => {
                    if let Some(response_tx) = response_tx.take() {
                        let _ = response_tx.send(Err(ServerError::Timeout("query timeout".into())));
                    }
                    Some((execution.as_mut().await, true))
                }
                SqlDeadline::Completed(result) => Some((result, false)),
            }
        };
        let Some((execution_result, timed_out)) = execution_outcome else {
            let _ = txn.async_rollback().await;
            drop(catalog_txn_operation);
            return;
        };

        if timed_out {
            let catalog_rollback_effects =
                statement_effects_after_execution(&state, lifecycle_candidates)
                    .map(|(_, effects)| effects);
            let execution_status = execution_result
                .as_ref()
                .map(|_| "ok".to_string())
                .unwrap_or_else(|err| err.to_string());
            let transaction_rollback = txn
                .async_rollback()
                .await
                .map_err(|err| ServerError::Sql(err.into()));
            let catalog_rollback = catalog_rollback_effects
                .and_then(|effects| state.apply_catalog_rollback_effects(effects));
            if transaction_rollback.is_err() || catalog_rollback.is_err() {
                tracing::error!(
                    execution = %execution_status,
                    transaction_rollback = %cleanup_result(&transaction_rollback),
                    catalog_rollback = %cleanup_result(&catalog_rollback),
                    "timed-out non-session SQL coordinator cleanup failed after responding"
                );
            }
            drop(catalog_txn_operation);
            return;
        }

        let completion = match execution_result {
            Ok(result) => match statement_effects_after_execution(&state, lifecycle_candidates) {
                Ok((lifecycle_effects, catalog_rollback_effects)) => {
                    match txn.async_commit().await {
                        Ok(()) => state
                            .apply_table_lifecycle_effects(lifecycle_effects)
                            .and_then(|()| {
                                if schema_mutation {
                                    sync_catalog_to_store(&state)
                                } else {
                                    Ok(())
                                }
                            })
                            .map(|()| result),
                        Err(commit_error) => {
                            let catalog_rollback =
                                state.apply_catalog_rollback_effects(catalog_rollback_effects);
                            match catalog_rollback {
                                Ok(()) => Err(ServerError::Sql(commit_error.into())),
                                Err(catalog_error) => Err(ServerError::Internal(format!(
                                    "SQL commit failed: {commit_error}; catalog rollback: {catalog_error}"
                                ))),
                            }
                        }
                    }
                }
                Err(effect_error) => {
                    let transaction_rollback = txn
                        .async_rollback()
                        .await
                        .map_err(|err| ServerError::Sql(err.into()));
                    match transaction_rollback {
                        Ok(()) => Err(effect_error),
                        Err(rollback_error) => Err(ServerError::Internal(format!(
                            "SQL effect collection failed: {effect_error}; transaction rollback: {rollback_error}"
                        ))),
                    }
                }
            },
            Err(execution_error) => {
                let catalog_rollback_effects =
                    statement_effects_after_execution(&state, lifecycle_candidates)
                        .map(|(_, effects)| effects);
                let transaction_rollback = txn
                    .async_rollback()
                    .await
                    .map_err(|err| ServerError::Sql(err.into()));
                let catalog_rollback = catalog_rollback_effects
                    .and_then(|effects| state.apply_catalog_rollback_effects(effects));
                match (transaction_rollback, catalog_rollback) {
                    (Ok(()), Ok(())) => Err(ServerError::Sql(execution_error.into())),
                    (transaction, catalog) => Err(ServerError::Internal(format!(
                        "SQL execution failed: {execution_error}; transaction rollback: {}; catalog rollback: {}",
                        cleanup_result(&transaction),
                        cleanup_result(&catalog),
                    ))),
                }
            }
        };
        if let Some(response_tx) = response_tx.take() {
            let _ = response_tx.send(completion.map(|result| (result, diagnostics)));
        }
        drop(catalog_txn_operation);
    });
    await_sql_coordinator_response_until(response_rx, deadline).await
}

fn cleanup_result(result: &Result<()>) -> String {
    match result {
        Ok(()) => "ok".to_string(),
        Err(err) => err.to_string(),
    }
}

async fn await_sql_coordinator_response_until<T>(
    response_rx: tokio::sync::oneshot::Receiver<Result<T>>,
    deadline: tokio::time::Instant,
) -> Result<T> {
    match wait_until_deadline(deadline, response_rx).await {
        SqlDeadline::Completed(result) => result.map_err(|_| {
            ServerError::Internal("SQL coordinator stopped before responding".into())
        })?,
        SqlDeadline::TimedOut { .. } => Err(ServerError::Timeout("query timeout".into())),
    }
}

enum SqlDeadline<T> {
    Completed(T),
    TimedOut { started: bool },
}

async fn wait_for_sql_deadline<F>(
    mut execution: std::pin::Pin<&mut F>,
    deadline: tokio::time::Instant,
) -> SqlDeadline<F::Output>
where
    F: std::future::Future + ?Sized,
{
    if deadline <= tokio::time::Instant::now() {
        return SqlDeadline::TimedOut { started: false };
    }
    let deadline_sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(deadline_sleep);
    let started = Arc::new(AtomicBool::new(false));
    let started_for_poll = Arc::clone(&started);
    let tracked_execution = futures::future::poll_fn(move |cx| {
        started_for_poll.store(true, Ordering::Release);
        execution.as_mut().poll(cx)
    });
    tokio::pin!(tracked_execution);
    tokio::select! {
        biased;
        _ = deadline_sleep.as_mut() => SqlDeadline::TimedOut {
            started: started.load(Ordering::Acquire),
        },
        result = tracked_execution.as_mut() => SqlDeadline::Completed(result),
    }
}

async fn wait_until_deadline<F>(deadline: tokio::time::Instant, future: F) -> SqlDeadline<F::Output>
where
    F: Future,
{
    tokio::pin!(future);
    wait_for_sql_deadline(future.as_mut(), deadline).await
}

enum StreamStart<T> {
    Completed(T),
    TimedOut,
    Closed,
}

async fn wait_for_stream_start<F>(
    sender: &mpsc::Sender<StreamItem>,
    deadline: tokio::time::Instant,
    future: F,
) -> StreamStart<F::Output>
where
    F: Future,
{
    if sender.is_closed() {
        return StreamStart::Closed;
    }
    tokio::select! {
        biased;
        _ = sender.closed() => StreamStart::Closed,
        _ = tokio::time::sleep_until(deadline) => StreamStart::TimedOut,
        result = future => StreamStart::Completed(result),
    }
}

async fn send_stream_item_until(
    sender: &mpsc::Sender<StreamItem>,
    item: StreamItem,
    deadline: tokio::time::Instant,
) -> bool {
    tokio::select! {
        biased;
        _ = sender.closed() => false,
        _ = tokio::time::sleep_until(deadline) => false,
        result = sender.send(item) => result.is_ok(),
    }
}

async fn send_stream_error_until(
    sender: &mpsc::Sender<StreamItem>,
    err: ServerError,
    correlation_id: &str,
    deadline: tokio::time::Instant,
) {
    let _ = send_stream_item_until(sender, stream_item_error(err, correlation_id), deadline).await;
}

fn response_stream_until(
    receiver: mpsc::Receiver<StreamItem>,
    deadline: tokio::time::Instant,
    correlation_id: String,
) -> impl futures::Stream<Item = StreamItem> + Send + 'static {
    futures::stream::unfold(Some(receiver), move |receiver| {
        let correlation_id = correlation_id.clone();
        async move {
            let mut receiver = receiver?;
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => Some((
                    stream_item_error(
                        ServerError::Timeout("query timeout".into()),
                        &correlation_id,
                    ),
                    None,
                )),
                item = receiver.recv() => item.map(|item| {
                    let timeout = item.error.as_ref().is_some_and(|error| {
                        error.code == ServerError::Timeout("query timeout".into()).error_code()
                    });
                    let next = (!item.done && !timeout).then_some(receiver);
                    (item, next)
                }),
            }
        }
    })
}

pub(crate) async fn execute_non_session_statement_with_routing(
    state: Arc<ServerState>,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    alopex_sql::executor::ExecutionResult,
    Vec<RoutingDiagnostics>,
)> {
    let (mut results, diagnostics) =
        execute_non_session_statements_with_routing(state, sql, correlation_id, timeout).await?;
    let result = results
        .pop()
        .ok_or_else(|| ServerError::BadRequest("sql must not be empty".into()))?;
    Ok((result, diagnostics))
}

pub(crate) async fn route_session_statement_for_execution(
    state: &ServerState,
    handle: &TxnHandle,
    sql: &str,
    correlation_id: &str,
) -> Result<Vec<RoutingDiagnostics>> {
    let routing_plan = route_session_sql(state, handle, sql, correlation_id).await?;
    if let Some(diagnostic) = future_distributed_diagnostic(&routing_plan.diagnostics) {
        return Err(future_distributed_error(diagnostic));
    }
    Ok(routing_plan.diagnostics)
}

async fn route_non_session_sql(
    state: &ServerState,
    txn: &AsyncTxn,
    sql: &str,
    correlation_id: &str,
) -> Result<RoutingPlan> {
    let planned = txn
        .async_plan_for_routing(sql)
        .await
        .map_err(|err| ServerError::Sql(err.into()))?;
    route_planned_sql(state, planned, correlation_id)
}

async fn route_session_sql(
    state: &ServerState,
    handle: &TxnHandle,
    sql: &str,
    correlation_id: &str,
) -> Result<RoutingPlan> {
    let planned = handle
        .plan_for_routing(sql)
        .await
        .map_err(|err| ServerError::Sql(err.into()))?;
    route_planned_sql(state, planned, correlation_id)
}

fn route_planned_sql(
    state: &ServerState,
    planned: Vec<PlannedStatement>,
    correlation_id: &str,
) -> Result<RoutingPlan> {
    let cluster_snapshot = state.cluster_status_snapshot()?;
    let placement_catalog = PlacementCatalog::from_view(cluster_snapshot.placement);
    let membership = cluster_snapshot.membership;
    let catalog_snapshot = catalog_table_snapshot(state, cluster_snapshot.identity.update_epoch)?;
    let router = QueryRouter::new(&placement_catalog, &membership);

    let mut diagnostics = Vec::with_capacity(planned.len());
    for (index, statement) in planned.iter().enumerate() {
        let plan_id = PlanId::new(format!("{correlation_id}:{index}"));
        let request = query_routing_request(plan_id, statement, &catalog_snapshot);
        diagnostics.push(router.route(request));
    }
    Ok(RoutingPlan {
        planned,
        diagnostics,
    })
}

fn future_distributed_diagnostic(
    diagnostics: &[RoutingDiagnostics],
) -> Option<&RoutingDiagnostics> {
    diagnostics.iter().find(|diagnostic| {
        diagnostic.decision == RoutingDecisionKind::FutureDistributedExecutionRequired
    })
}

fn future_distributed_error(diagnostic: &RoutingDiagnostics) -> ServerError {
    ServerError::FutureDistributedExecutionRequired(format!(
        "routing decision {:?} for plan {}: {:?}",
        diagnostic.decision,
        diagnostic.plan_id.as_str(),
        diagnostic.reason
    ))
}

fn query_routing_request(
    plan_id: PlanId,
    statement: &PlannedStatement,
    catalog_snapshot: &CatalogTableSnapshot,
) -> QueryRoutingRequest {
    let table_references = statement
        .table_references()
        .iter()
        .map(|reference| query_table_reference(reference, catalog_snapshot))
        .collect();

    QueryRoutingRequest::new(plan_id, catalog_snapshot.clone(), table_references)
}

fn query_table_reference(
    reference: &TableReference,
    catalog_snapshot: &CatalogTableSnapshot,
) -> QueryTableReference {
    QueryTableReference::new(
        table_ref_for_reference(&reference.table_name, catalog_snapshot),
        query_table_reference_access(reference.access),
        query_table_reference_source(reference.source),
    )
}

fn query_table_reference_access(access: TableReferenceAccess) -> QueryTableReferenceAccess {
    match access {
        TableReferenceAccess::Read => QueryTableReferenceAccess::Read,
        TableReferenceAccess::Write => QueryTableReferenceAccess::Write,
        TableReferenceAccess::Create => QueryTableReferenceAccess::Create,
        TableReferenceAccess::Drop => QueryTableReferenceAccess::Drop,
        TableReferenceAccess::Metadata => QueryTableReferenceAccess::Metadata,
    }
}

fn query_table_reference_source(source: TableReferenceSource) -> QueryTableReferenceSource {
    match source {
        TableReferenceSource::TopLevelPlanTableName => {
            QueryTableReferenceSource::TopLevelPlanTableName
        }
        TableReferenceSource::LogicalPlanScan => QueryTableReferenceSource::LogicalPlanScan,
        TableReferenceSource::LogicalPlanMutationTarget => {
            QueryTableReferenceSource::LogicalPlanMutationTarget
        }
        TableReferenceSource::LogicalPlanDdlTarget => {
            QueryTableReferenceSource::LogicalPlanDdlTarget
        }
        TableReferenceSource::LogicalPlanIndexTarget => {
            QueryTableReferenceSource::LogicalPlanIndexTarget
        }
        TableReferenceSource::TypedExprSubquery => QueryTableReferenceSource::TypedExprSubquery,
    }
}

fn catalog_table_snapshot(state: &ServerState, update_epoch: u64) -> Result<CatalogTableSnapshot> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    let tables = guard
        .list_tables()
        .iter()
        .map(|table| CatalogTableRef::new(table_fqn_string(table), table.table_id))
        .collect();
    Ok(CatalogTableSnapshot::from_tables(update_epoch, tables))
}

fn table_ref_for_reference(table_name: &str, snapshot: &CatalogTableSnapshot) -> TableRef {
    snapshot
        .tables
        .iter()
        .find(|table| {
            table.table_ref.as_str() == table_name
                || table.table_ref.as_str().rsplit('.').next() == Some(table_name)
        })
        .map(|table| table.table_ref.clone())
        .unwrap_or_else(|| TableRef::new(default_table_ref(table_name)))
}

fn table_fqn_string(table: &TableMetadata) -> String {
    let fqn = TableFqn::from(table);
    format!("{}.{}.{}", fqn.catalog, fqn.namespace, fqn.table)
}

fn table_lifecycle_candidates(
    state: &ServerState,
    planned: &[PlannedStatement],
) -> Result<Vec<TableLifecycleCandidate>> {
    let mut candidates = Vec::new();
    for statement in planned {
        let mut plan = &statement.plan;
        while let LogicalPlan::Explain {
            analyze: true,
            input,
            ..
        } = plan
        {
            plan = input;
        }
        match plan {
            LogicalPlan::CreateTable { table, .. } | LogicalPlan::CreateTableAs { table, .. } => {
                candidates.push(TableLifecycleCandidate::Created {
                    table_name: table.name.clone(),
                    before: table_lifecycle_state(state, &table.name)?,
                });
            }
            LogicalPlan::DropTable { name, .. } => {
                // Capture the table and its dependent indexes under one read guard.
                // Other statements do not need an all-index snapshot.
                let guard = state
                    .catalog
                    .read()
                    .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
                let before = guard.get_table(name).map(|table| TableLifecycleState {
                    table_ref: TableRef::new(table_fqn_string(table)),
                    table_id: table.table_id,
                    table: table.clone(),
                });
                let indexes = guard
                    .get_indexes_for_table(name)
                    .into_iter()
                    .cloned()
                    .collect();
                candidates.push(TableLifecycleCandidate::Dropped {
                    table_name: name.clone(),
                    before,
                    indexes,
                });
            }
            LogicalPlan::CreateIndex { index, .. } => {
                candidates.push(TableLifecycleCandidate::CreateIndex {
                    index_name: index.name.clone(),
                    before: index_table_lifecycle_state(state, &index.name)?,
                });
            }
            LogicalPlan::DropIndex { name, .. } => {
                candidates.push(TableLifecycleCandidate::DropIndex {
                    index_name: name.clone(),
                    before: index_table_lifecycle_state(state, name)?,
                });
            }
            _ => {}
        }
    }
    Ok(candidates)
}

fn statement_effects_after_execution(
    state: &ServerState,
    candidates: Vec<TableLifecycleCandidate>,
) -> Result<(Vec<TableLifecycleEffect>, Vec<CatalogRollbackEffect>)> {
    let mut lifecycle_effects = Vec::new();
    let mut catalog_rollback_effects = Vec::new();
    for candidate in candidates {
        match candidate {
            TableLifecycleCandidate::Created { table_name, before } => {
                let after = table_lifecycle_state(state, &table_name)?;
                if let Some(after) = after {
                    let changed = match before.as_ref() {
                        Some(before) => before.table_id != after.table_id,
                        None => true,
                    };
                    if changed {
                        push_lifecycle_effect(
                            &mut lifecycle_effects,
                            TableLifecycleEffect::Created {
                                table_ref: after.table_ref,
                                table_id: after.table_id,
                            },
                        );
                        push_catalog_rollback_effect(
                            &mut catalog_rollback_effects,
                            CatalogRollbackEffect::DropTable {
                                table_name: after.table.name,
                                table_id: after.table_id,
                            },
                        );
                    }
                }
            }
            TableLifecycleCandidate::Dropped {
                table_name,
                before,
                indexes,
            } => {
                let after = table_lifecycle_state(state, &table_name)?;
                if let Some(before) = before {
                    let changed = match after.as_ref() {
                        Some(after) => after.table_id != before.table_id,
                        None => true,
                    };
                    if changed {
                        push_lifecycle_effect(
                            &mut lifecycle_effects,
                            TableLifecycleEffect::Dropped {
                                table_ref: before.table_ref.clone(),
                                table_id: before.table_id,
                            },
                        );
                        push_catalog_rollback_effect(
                            &mut catalog_rollback_effects,
                            CatalogRollbackEffect::CreateTable {
                                table: Box::new(before.table),
                                indexes,
                            },
                        );
                    }
                }
            }
            TableLifecycleCandidate::CreateIndex { index_name, before } => {
                if let Some((after, index)) = index_table_lifecycle_state(state, &index_name)? {
                    let changed = before
                        .as_ref()
                        .is_none_or(|(_, before)| before.index_id != index.index_id);
                    if changed {
                        push_lifecycle_effect(
                            &mut lifecycle_effects,
                            TableLifecycleEffect::SchemaChanged {
                                table_ref: after.table_ref,
                                table_id: after.table_id,
                            },
                        );
                        push_catalog_rollback_effect(
                            &mut catalog_rollback_effects,
                            CatalogRollbackEffect::DropIndex {
                                index_name: index.name,
                                index_id: index.index_id,
                            },
                        );
                    }
                }
            }
            TableLifecycleCandidate::DropIndex { index_name, before } => {
                if let Some((before, index)) = before {
                    let after = index_table_lifecycle_state(state, &index_name)?;
                    let changed = after
                        .as_ref()
                        .is_none_or(|(_, after)| after.index_id != index.index_id);
                    if changed {
                        push_lifecycle_effect(
                            &mut lifecycle_effects,
                            TableLifecycleEffect::SchemaChanged {
                                table_ref: before.table_ref,
                                table_id: before.table_id,
                            },
                        );
                        push_catalog_rollback_effect(
                            &mut catalog_rollback_effects,
                            CatalogRollbackEffect::CreateIndex {
                                index: Box::new(index),
                                table_id: before.table_id,
                            },
                        );
                    }
                }
            }
        }
    }
    Ok((lifecycle_effects, catalog_rollback_effects))
}

fn push_lifecycle_effect(effects: &mut Vec<TableLifecycleEffect>, effect: TableLifecycleEffect) {
    effects.retain(|existing| existing != &effect);
    effects.push(effect);
}

fn push_catalog_rollback_effect(
    effects: &mut Vec<CatalogRollbackEffect>,
    effect: CatalogRollbackEffect,
) {
    effects.retain(|existing| !same_catalog_rollback_identity(existing, &effect));
    effects.push(effect);
}

fn same_catalog_rollback_identity(
    left: &CatalogRollbackEffect,
    right: &CatalogRollbackEffect,
) -> bool {
    match (left, right) {
        (
            CatalogRollbackEffect::DropTable {
                table_name: left_name,
                table_id: left_id,
            },
            CatalogRollbackEffect::DropTable {
                table_name: right_name,
                table_id: right_id,
            },
        ) => left_name == right_name && left_id == right_id,
        (
            CatalogRollbackEffect::CreateTable {
                table: left_table, ..
            },
            CatalogRollbackEffect::CreateTable {
                table: right_table, ..
            },
        ) => left_table.name == right_table.name && left_table.table_id == right_table.table_id,
        (
            CatalogRollbackEffect::DropIndex {
                index_name: left_name,
                index_id: left_id,
            },
            CatalogRollbackEffect::DropIndex {
                index_name: right_name,
                index_id: right_id,
            },
        ) => left_name == right_name && left_id == right_id,
        (
            CatalogRollbackEffect::CreateIndex {
                index: left_index,
                table_id: left_table_id,
            },
            CatalogRollbackEffect::CreateIndex {
                index: right_index,
                table_id: right_table_id,
            },
        ) => {
            left_index.name == right_index.name
                && left_index.index_id == right_index.index_id
                && left_table_id == right_table_id
        }
        _ => false,
    }
}

fn table_lifecycle_state(
    state: &ServerState,
    table_name: &str,
) -> Result<Option<TableLifecycleState>> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    Ok(guard
        .get_table(table_name)
        .map(|table| TableLifecycleState {
            table_ref: TableRef::new(table_fqn_string(table)),
            table_id: table.table_id,
            table: table.clone(),
        }))
}

fn index_table_lifecycle_state(
    state: &ServerState,
    index_name: &str,
) -> Result<Option<(TableLifecycleState, IndexMetadata)>> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    let Some(index) = guard.get_index(index_name) else {
        return Ok(None);
    };
    Ok(guard.get_table(&index.table).map(|table| {
        (
            TableLifecycleState {
                table_ref: TableRef::new(table_fqn_string(table)),
                table_id: table.table_id,
                table: table.clone(),
            },
            index.clone(),
        )
    }))
}

fn default_table_ref(table_name: &str) -> String {
    if table_name.matches('.').count() >= 2 {
        table_name.to_string()
    } else {
        format!("default.default.{table_name}")
    }
}

pub(crate) fn sync_catalog_to_store(state: &ServerState) -> Result<()> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    let tables = guard.list_tables();
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    delete_prefix(&mut txn, TABLES_PREFIX)?;
    for table in tables {
        let persisted = PersistedTableMeta::try_from(&table)
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        let value = bincode_config()
            .serialize(&persisted)
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        txn.put(
            table_key(&table.catalog_name, &table.namespace_name, &table.name),
            value,
        )?;
    }
    txn.commit_self()?;
    Ok(())
}

fn delete_prefix<'a, T: KVTransaction<'a>>(txn: &mut T, prefix: &[u8]) -> Result<()> {
    let mut keys = Vec::new();
    for (key, _) in txn.scan_prefix(prefix)? {
        keys.push(key);
    }
    for key in keys {
        txn.delete(key)?;
    }
    Ok(())
}

fn table_key(catalog_name: &str, namespace_name: &str, table_name: &str) -> Vec<u8> {
    let mut key = TABLES_PREFIX.to_vec();
    key.extend_from_slice(catalog_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(namespace_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(table_name.as_bytes());
    key
}

fn stream_response(state: Arc<ServerState>, request: SqlRequest, ctx: &RequestContext) -> Response {
    if is_write_sql(&request.sql) {
        if let Err(err) = state.lifecycle_state.check_write_allowed() {
            return error_response(err, ctx);
        }
    }
    let (sender, receiver) = mpsc::channel(32);
    let sql = request.sql.clone();
    let correlation_id = ctx.correlation_id.clone();
    let max_response_size = state.config.max_response_size;
    let timeout = state.config.query_timeout;
    let memory_policy = MemoryControlPolicy::from_env();
    let metrics = state.metrics.clone();
    let mut audit = None;
    if state.config.audit_log_enabled && is_ddl(&sql) {
        audit = Some(state.audit.clone());
    }

    let session_id = request.session_id.clone();
    let state_clone = state.clone();
    let memory_policy = memory_policy.clone();
    let deadline = tokio::time::Instant::now() + timeout;
    let response_correlation_id = correlation_id.clone();
    tokio::spawn(async move {
        let start = Instant::now();
        if sender.is_closed() {
            return;
        }
        let mut bytes_sent = 0usize;
        let mut success = true;
        let mut source = match session_id {
            Some(id) => {
                let parsed = match id.parse::<SessionId>() {
                    Ok(id) => id,
                    Err(_) => {
                        send_stream_error_until(
                            &sender,
                            ServerError::BadRequest("invalid session_id".into()),
                            &correlation_id,
                            deadline,
                        )
                        .await;
                        return;
                    }
                };
                match wait_for_stream_start(
                    &sender,
                    deadline,
                    state_clone.session_manager.get_transaction(&parsed),
                )
                .await
                {
                    StreamStart::Completed(Ok(handle)) => {
                        let operation =
                            match wait_for_stream_start(&sender, deadline, handle.lock_operation())
                                .await
                            {
                                StreamStart::Completed(operation) => operation,
                                StreamStart::Closed => return,
                                StreamStart::TimedOut => {
                                    send_stream_error_until(
                                        &sender,
                                        ServerError::Timeout("query timeout".into()),
                                        &correlation_id,
                                        deadline,
                                    )
                                    .await;
                                    return;
                                }
                            };
                        let catalog_txn_guard_kind = match wait_for_stream_start(
                            &sender,
                            deadline,
                            handle.catalog_txn_guard_kind(&operation),
                        )
                        .await
                        {
                            StreamStart::Completed(kind) => kind,
                            StreamStart::Closed => return,
                            StreamStart::TimedOut => {
                                send_stream_error_until(
                                    &sender,
                                    ServerError::Timeout("query timeout".into()),
                                    &correlation_id,
                                    deadline,
                                )
                                .await;
                                return;
                            }
                        };
                        let catalog_txn_operation =
                            if catalog_txn_guard_kind != SessionCatalogTxnGuardKind::None {
                                None
                            } else {
                                let catalog_txn_guard = match wait_for_stream_start(
                                    &sender,
                                    deadline,
                                    Arc::clone(&state_clone.catalog_txn_gate).read_owned(),
                                )
                                .await
                                {
                                    StreamStart::Completed(guard) => guard,
                                    StreamStart::Closed => return,
                                    StreamStart::TimedOut => {
                                        send_stream_error_until(
                                            &sender,
                                            ServerError::Timeout("query timeout".into()),
                                            &correlation_id,
                                            deadline,
                                        )
                                        .await;
                                        return;
                                    }
                                };
                                Some(catalog_txn_guard)
                            };
                        match wait_for_stream_start(
                            &sender,
                            deadline,
                            route_session_statement_for_execution(
                                &state_clone,
                                &handle,
                                &sql,
                                &correlation_id,
                            ),
                        )
                        .await
                        {
                            StreamStart::Completed(Ok(_)) => {}
                            StreamStart::Completed(Err(err)) => {
                                send_stream_error_until(&sender, err, &correlation_id, deadline)
                                    .await;
                                return;
                            }
                            StreamStart::Closed => return,
                            StreamStart::TimedOut => {
                                send_stream_error_until(
                                    &sender,
                                    ServerError::Timeout("query timeout".into()),
                                    &correlation_id,
                                    deadline,
                                )
                                .await;
                                return;
                            }
                        }
                        StreamSource::Handle {
                            handle,
                            operation,
                            _catalog_txn_operation: catalog_txn_operation,
                        }
                    }
                    StreamStart::Completed(Err(err)) => {
                        send_stream_error_until(&sender, err, &correlation_id, deadline).await;
                        return;
                    }
                    StreamStart::Closed => return,
                    StreamStart::TimedOut => {
                        send_stream_error_until(
                            &sender,
                            ServerError::Timeout("query timeout".into()),
                            &correlation_id,
                            deadline,
                        )
                        .await;
                        return;
                    }
                }
            }
            None => {
                let catalog_txn_operation = if is_ddl(&sql) {
                    let ordering = match wait_for_stream_start(
                        &sender,
                        deadline,
                        Arc::clone(&state_clone.catalog_ddl_order_gate).lock_owned(),
                    )
                    .await
                    {
                        StreamStart::Completed(guard) => guard,
                        StreamStart::Closed => return,
                        StreamStart::TimedOut => {
                            send_stream_error_until(
                                &sender,
                                ServerError::Timeout("query timeout".into()),
                                &correlation_id,
                                deadline,
                            )
                            .await;
                            return;
                        }
                    };
                    match wait_for_stream_start(
                        &sender,
                        deadline,
                        Arc::clone(&state_clone.catalog_txn_gate).write_owned(),
                    )
                    .await
                    {
                        StreamStart::Completed(guard) => CatalogTxnOperation::Write {
                            _guard: guard,
                            _ordering: ordering,
                        },
                        StreamStart::Closed => return,
                        StreamStart::TimedOut => {
                            send_stream_error_until(
                                &sender,
                                ServerError::Timeout("query timeout".into()),
                                &correlation_id,
                                deadline,
                            )
                            .await;
                            return;
                        }
                    }
                } else {
                    match wait_for_stream_start(
                        &sender,
                        deadline,
                        Arc::clone(&state_clone.catalog_txn_gate).read_owned(),
                    )
                    .await
                    {
                        StreamStart::Completed(guard) => {
                            CatalogTxnOperation::Read { _guard: guard }
                        }
                        StreamStart::Closed => return,
                        StreamStart::TimedOut => {
                            send_stream_error_until(
                                &sender,
                                ServerError::Timeout("query timeout".into()),
                                &correlation_id,
                                deadline,
                            )
                            .await;
                            return;
                        }
                    }
                };
                match wait_for_stream_start(&sender, deadline, state_clone.begin_sql_txn()).await {
                    StreamStart::Completed(Ok(txn)) => StreamSource::Txn {
                        txn,
                        _catalog_txn_operation: catalog_txn_operation,
                    },
                    StreamStart::Completed(Err(err)) => {
                        send_stream_error_until(&sender, err, &correlation_id, deadline).await;
                        return;
                    }
                    StreamStart::Closed => return,
                    StreamStart::TimedOut => {
                        send_stream_error_until(
                            &sender,
                            ServerError::Timeout("query timeout".into()),
                            &correlation_id,
                            deadline,
                        )
                        .await;
                        return;
                    }
                }
            }
        };

        if sender.is_closed() {
            if let StreamSource::Txn { txn, .. } = source {
                let _ = txn.async_rollback().await;
            }
            return;
        }
        let mut stream = match &mut source {
            StreamSource::Handle {
                handle, operation, ..
            } => handle.query_with_operation(&sql, operation),
            StreamSource::Txn { txn, .. } => txn.async_query(&sql),
        };
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                send_stream_error_until(
                    &sender,
                    ServerError::Timeout("query timeout".into()),
                    &correlation_id,
                    deadline,
                )
                .await;
                success = false;
                break;
            }

            tokio::select! {
                _ = sender.closed() => {
                    success = false;
                    break;
                }
                item = tokio::time::timeout(remaining, stream.next()) => {
                    let next = match item {
                        Ok(value) => value,
                        Err(_) => {
                            send_stream_error_until(
                                &sender,
                                ServerError::Timeout("query timeout".into()),
                                &correlation_id,
                                deadline,
                            )
                            .await;
                            success = false;
                            break;
                        }
                    };

                    match next {
                        Some(Ok(row)) => {
                            let item = StreamItem {
                                row: Some(row.values),
                                error: None,
                                done: false,
                            };
                            match serde_json::to_vec(&item) {
                                Ok(bytes) => {
                                    bytes_sent += bytes.len();
                                    if let Err(err) =
                                        memory_policy.enforce_output_bytes(bytes_sent as u64)
                                    {
                                        send_stream_error_until(
                                            &sender,
                                            err,
                                            &correlation_id,
                                            deadline,
                                        )
                                        .await;
                                        success = false;
                                        break;
                                    }
                                    if bytes_sent > max_response_size {
                                        send_stream_error_until(
                                            &sender,
                                            ServerError::PayloadTooLarge(
                                                "response size exceeds limit".into(),
                                            ),
                                            &correlation_id,
                                            deadline,
                                        )
                                        .await;
                                        success = false;
                                        break;
                                    }
                                }
                                Err(err) => {
                                    send_stream_error_until(
                                        &sender,
                                        ServerError::Internal(err.to_string()),
                                        &correlation_id,
                                        deadline,
                                    )
                                    .await;
                                    success = false;
                                    break;
                                }
                            }
                            match sender.try_send(item) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(item)) => {
                                    metrics.record_backpressure();
                                    if !send_stream_item_until(&sender, item, deadline).await {
                                        success = false;
                                        break;
                                    }
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    success = false;
                                    break;
                                }
                            }
                        }
                        Some(Err(err)) => {
                            send_stream_error_until(
                                &sender,
                                ServerError::Sql(err.into()),
                                &correlation_id,
                                deadline,
                            )
                            .await;
                            success = false;
                            break;
                        }
                        None => break,
                    }
                }
            }
        }

        drop(stream);
        if let StreamSource::Handle {
            handle, operation, ..
        } = &source
        {
            let _ = handle.wait_for_idle_with_operation(operation).await;
        }
        if let StreamSource::Txn { txn, .. } = source {
            let _ = txn.async_rollback().await;
        }
        if let Some(logger) = audit {
            logger.log_ddl(&sql, None, &correlation_id);
        }
        metrics.record_query(start.elapsed(), success);
        let _ = send_stream_item_until(
            &sender,
            StreamItem {
                row: None,
                error: None,
                done: true,
            },
            deadline,
        )
        .await;
    });

    let stream = response_stream_until(receiver, deadline, response_correlation_id).map(|item| {
        let json = serde_json::to_string(&item).unwrap_or_else(|_| "{}".to_string());
        Ok::<axum::body::Bytes, Infallible>(axum::body::Bytes::from(json + "\n"))
    });

    let body = axum::body::Body::from_stream(stream);
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/jsonl")
        .body(body)
        .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn stream_item_error(err: ServerError, correlation_id: &str) -> StreamItem {
    StreamItem {
        row: None,
        error: Some(StreamError {
            code: err.error_code(),
            message: err.to_string(),
            correlation_id: correlation_id.to_string(),
        }),
        done: false,
    }
}

fn map_execution_results(
    exec_results: Vec<alopex_sql::executor::ExecutionResult>,
    routing_diagnostics: Vec<RoutingDiagnostics>,
) -> SqlResponse {
    let results: Vec<SqlResultResponse> =
        exec_results.into_iter().map(map_execution_result).collect();
    let last_result = results
        .last()
        .cloned()
        .unwrap_or_else(|| SqlResultResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
        });
    SqlResponse {
        last_result,
        results,
        routing_diagnostics,
    }
}

fn map_execution_result(exec_result: alopex_sql::executor::ExecutionResult) -> SqlResultResponse {
    match exec_result {
        alopex_sql::executor::ExecutionResult::Query(query) => SqlResultResponse {
            columns: query
                .columns
                .into_iter()
                .map(|col| ColumnInfoResponse {
                    name: col.name,
                    data_type: type_to_string(&col.data_type),
                })
                .collect(),
            rows: query.rows,
            affected_rows: None,
        },
        alopex_sql::executor::ExecutionResult::RowsAffected(rows) => SqlResultResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: Some(rows),
        },
        alopex_sql::executor::ExecutionResult::Success => SqlResultResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
        },
    }
}

fn type_to_string(data_type: &alopex_sql::planner::ResolvedType) -> String {
    match data_type {
        alopex_sql::planner::ResolvedType::Integer => "INTEGER".to_string(),
        alopex_sql::planner::ResolvedType::BigInt => "BIGINT".to_string(),
        alopex_sql::planner::ResolvedType::Float => "FLOAT".to_string(),
        alopex_sql::planner::ResolvedType::Double => "DOUBLE".to_string(),
        alopex_sql::planner::ResolvedType::Text => "TEXT".to_string(),
        alopex_sql::planner::ResolvedType::Blob => "BLOB".to_string(),
        alopex_sql::planner::ResolvedType::Boolean => "BOOLEAN".to_string(),
        alopex_sql::planner::ResolvedType::Timestamp => "TIMESTAMP".to_string(),
        alopex_sql::planner::ResolvedType::Date => "DATE".to_string(),
        alopex_sql::planner::ResolvedType::Time => "TIME".to_string(),
        alopex_sql::planner::ResolvedType::Interval => "INTERVAL".to_string(),
        alopex_sql::planner::ResolvedType::Decimal { precision, scale } => {
            format!("DECIMAL({precision},{scale})")
        }
        alopex_sql::planner::ResolvedType::Json => "JSON".to_string(),
        alopex_sql::planner::ResolvedType::Array(_)
        | alopex_sql::planner::ResolvedType::Map { .. }
        | alopex_sql::planner::ResolvedType::Struct(_) => data_type.to_string(),
        alopex_sql::planner::ResolvedType::Vector { dimension, metric } => {
            format!("VECTOR({dimension}, {metric:?})")
        }
        alopex_sql::planner::ResolvedType::Null => "NULL".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementSchemaClass {
    SchemaMutation,
    NonSchema,
    Unsupported,
}

impl StatementSchemaClass {
    fn is_schema_mutation(self) -> bool {
        matches!(self, Self::SchemaMutation)
    }
}

fn classify_statement_schema(
    statement_kind: &alopex_sql::ast::StatementKind,
) -> StatementSchemaClass {
    // A statement owned by a future specialized host must not inherit schema
    // authorization merely because the generic classifier predates it.
    #[allow(unreachable_patterns)]
    match statement_kind {
        alopex_sql::ast::StatementKind::Explain {
            analyze: true,
            statement,
            ..
        } => classify_statement_schema(&statement.kind),
        alopex_sql::ast::StatementKind::Explain { .. } => StatementSchemaClass::NonSchema,
        alopex_sql::ast::StatementKind::CreateTable(_)
        | alopex_sql::ast::StatementKind::DropTable(_)
        | alopex_sql::ast::StatementKind::CreateIndex(_)
        | alopex_sql::ast::StatementKind::DropIndex(_) => StatementSchemaClass::SchemaMutation,
        alopex_sql::ast::StatementKind::Select(_)
        | alopex_sql::ast::StatementKind::Values(_)
        | alopex_sql::ast::StatementKind::Insert(_)
        | alopex_sql::ast::StatementKind::Update(_)
        | alopex_sql::ast::StatementKind::Delete(_)
        | alopex_sql::ast::StatementKind::Merge(_)
        | alopex_sql::ast::StatementKind::Pragma { .. } => StatementSchemaClass::NonSchema,
        _ => StatementSchemaClass::Unsupported,
    }
}

fn is_ddl(sql: &str) -> bool {
    let Ok(statements) = alopex_sql::parser::Parser::parse_sql(&AlopexDialect, sql) else {
        return false;
    };
    statements
        .iter()
        .any(|statement| classify_statement_schema(&statement.kind).is_schema_mutation())
}

fn is_write_sql(sql: &str) -> bool {
    let Ok(statements) = alopex_sql::parser::Parser::parse_sql(&AlopexDialect, sql) else {
        return false;
    };
    statements.iter().any(|stmt| stmt.kind.requires_write())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn done_stream_item() -> StreamItem {
        StreamItem {
            row: None,
            error: None,
            done: true,
        }
    }

    #[tokio::test]
    async fn stream_send_stops_at_absolute_deadline_or_receiver_close() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(done_stream_item()).await.expect("fill channel");
        assert!(
            !send_stream_item_until(&sender, done_stream_item(), tokio::time::Instant::now()).await
        );
        receiver.close();
        assert!(
            !send_stream_item_until(
                &sender,
                done_stream_item(),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn response_stream_times_out_once_and_closes_receiver_before_producer_cleanup() {
        let (sender, receiver) = mpsc::channel(1);
        let (cleanup_started_tx, cleanup_started_rx) = tokio::sync::oneshot::channel();
        let (release_cleanup_tx, release_cleanup_rx) = tokio::sync::oneshot::channel();
        let producer = tokio::spawn(async move {
            sender.closed().await;
            cleanup_started_tx.send(()).expect("cleanup observer");
            release_cleanup_rx.await.expect("cleanup release");
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let stream = response_stream_until(receiver, deadline, "stream-deadline".into());
        tokio::pin!(stream);

        let timeout = {
            let next = stream.next();
            tokio::pin!(next);
            assert!(futures::poll!(next.as_mut()).is_pending());
            tokio::time::advance(Duration::from_secs(5)).await;
            next.await.expect("timeout item")
        };
        assert_eq!(
            timeout.error.expect("timeout error").code,
            ServerError::Timeout("query timeout".into()).error_code()
        );
        cleanup_started_rx
            .await
            .expect("deadline must release the producer");
        assert!(stream.next().await.is_none(), "timeout must be terminal");
        assert!(
            !producer.is_finished(),
            "response must finish while producer cleanup continues"
        );
        release_cleanup_tx.send(()).expect("cleanup receiver");
        producer.await.expect("producer cleanup");
    }

    #[tokio::test]
    async fn closed_stream_does_not_poll_pre_start_work() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let polled = Arc::new(AtomicBool::new(false));
        let polled_by_future = Arc::clone(&polled);
        let future = futures::future::poll_fn(move |_| {
            polled_by_future.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(())
        });

        assert!(matches!(
            wait_for_stream_start(
                &sender,
                tokio::time::Instant::now() + Duration::from_secs(1),
                future,
            )
            .await,
            StreamStart::Closed
        ));
        assert!(!polled.load(Ordering::SeqCst));
    }

    fn catalog_guard_test_server() -> (tempfile::TempDir, Arc<ServerState>) {
        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        (temp_dir, server.state)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_ddl_session_does_not_block_idle_foreign_reads() {
        let (_temp_dir, state) = catalog_guard_test_server();
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "SELECT 1",
            "idle-session-read",
            Duration::from_secs(2),
        )
        .await
        .expect("session SELECT");

        let foreign_read = execute_non_session_statements_with_routing(
            state.clone(),
            "SELECT 2",
            "foreign-read",
            Duration::from_millis(100),
        )
        .await;
        foreign_read.expect("an idle non-DDL session must not block a foreign SELECT");

        let foreign_ddl = execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE idle_session_foreign_ddl (id INTEGER)",
            "foreign-ddl-after-idle-session-read",
            Duration::from_secs(2),
        )
        .await;
        foreign_ddl.expect("an idle SELECT-only session must not block foreign DDL");
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_dml_blocks_foreign_drop_until_commit() {
        use alopex_sql::storage::KeyEncoder;

        let (_temp_dir, state) = catalog_guard_test_server();
        execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE guarded_rows (value INTEGER)",
            "create-guarded-rows",
            Duration::from_secs(2),
        )
        .await
        .expect("table creation");
        let table_id = state
            .catalog
            .read()
            .expect("catalog")
            .get_table("guarded_rows")
            .expect("table")
            .table_id;
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "INSERT INTO guarded_rows VALUES (7)",
            "stage-guarded-row",
            Duration::from_secs(2),
        )
        .await
        .expect("session insert");
        assert!(Arc::clone(&state.catalog_txn_gate)
            .try_write_owned()
            .is_err());

        let drop_state = state.clone();
        let foreign_drop = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                drop_state,
                "DROP TABLE guarded_rows",
                "drop-guarded-rows",
                Duration::from_secs(2),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!foreign_drop.is_finished());

        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Commit,
            )
            .await
            .expect("session commit");
        foreign_drop
            .await
            .expect("foreign DROP task")
            .expect("foreign DROP");

        assert!(!state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("guarded_rows"));
        let mut raw = state.store.begin(TxnMode::ReadOnly).expect("raw read");
        assert!(raw
            .scan_prefix(&KeyEncoder::table_prefix(table_id))
            .expect("row prefix")
            .next()
            .is_none());
        assert!(raw
            .get(&KeyEncoder::sequence_key(table_id))
            .expect("sequence key")
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_dml_upgrades_to_ddl_without_catalog_gap() {
        use alopex_sql::storage::KeyEncoder;

        let (_temp_dir, state) = catalog_guard_test_server();
        execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE upgraded_rows (value INTEGER)",
            "create-upgraded-rows",
            Duration::from_secs(2),
        )
        .await
        .expect("table creation");
        let table_id = state
            .catalog
            .read()
            .expect("catalog")
            .get_table("upgraded_rows")
            .expect("table")
            .table_id;
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "INSERT INTO upgraded_rows VALUES (11)",
            "stage-upgraded-row",
            Duration::from_secs(2),
        )
        .await
        .expect("session insert");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "DROP TABLE upgraded_rows",
            "upgrade-session-to-ddl",
            Duration::from_secs(2),
        )
        .await
        .expect("session DDL upgrade");
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Commit,
            )
            .await
            .expect("session commit");

        assert!(!state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("upgraded_rows"));
        let mut raw = state.store.begin(TxnMode::ReadOnly).expect("raw read");
        assert!(raw
            .scan_prefix(&KeyEncoder::table_prefix(table_id))
            .expect("row prefix")
            .next()
            .is_none());
        assert!(raw
            .get(&KeyEncoder::sequence_key(table_id))
            .expect("sequence key")
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_dml_upgrade_conflicts_with_waiting_foreign_ddl() {
        let (_temp_dir, state) = catalog_guard_test_server();
        execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE upgrade_conflict_rows (value INTEGER)",
            "create-upgrade-conflict-rows",
            Duration::from_secs(2),
        )
        .await
        .expect("table creation");
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "INSERT INTO upgrade_conflict_rows VALUES (13)",
            "stage-upgrade-conflict-row",
            Duration::from_secs(2),
        )
        .await
        .expect("session insert");

        let ddl_state = state.clone();
        let foreign_ddl = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                ddl_state,
                "CREATE TABLE waiting_foreign_ddl (id INTEGER)",
                "waiting-foreign-ddl",
                Duration::from_secs(2),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if Arc::clone(&state.catalog_ddl_order_gate)
                    .try_lock_owned()
                    .is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("foreign DDL must acquire the ordering gate");

        let upgrade = execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "DROP TABLE upgrade_conflict_rows",
            "conflicting-session-upgrade",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(upgrade, Err(ServerError::Conflict(_))));
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        foreign_ddl
            .await
            .expect("foreign DDL task")
            .expect("foreign DDL");

        assert!(state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("upgrade_conflict_rows"));
        assert!(state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("waiting_foreign_ddl"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_dml_failed_ddl_planning_retains_catalog_guard() {
        let (_temp_dir, state) = catalog_guard_test_server();
        execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE guarded_after_planning_error (value INTEGER)",
            "create-guarded-after-planning-error",
            Duration::from_secs(2),
        )
        .await
        .expect("table creation");
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "INSERT INTO guarded_after_planning_error VALUES (17)",
            "stage-before-planning-error",
            Duration::from_secs(2),
        )
        .await
        .expect("session insert");

        let planning = execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "CREATE INDEX invalid_planning_idx ON missing_table (id)",
            "failed-upgrade-planning",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(planning, Err(ServerError::Sql(_))));
        assert!(Arc::clone(&state.catalog_txn_gate)
            .try_write_owned()
            .is_err());

        let drop_state = state.clone();
        let foreign_drop = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                drop_state,
                "DROP TABLE guarded_after_planning_error",
                "drop-after-planning-error",
                Duration::from_secs(2),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!foreign_drop.is_finished());
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        foreign_drop
            .await
            .expect("foreign DROP task")
            .expect("foreign DROP");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_session_ddl_planning_does_not_hold_catalog_writer() {
        let (_temp_dir, state) = catalog_guard_test_server();
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        let ddl = execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "CREATE INDEX invalid_idx ON missing_table (id)",
            "failed-ddl-planning",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(ddl, Err(ServerError::Sql(_))));
        let foreign_read = execute_non_session_statements_with_routing(
            state.clone(),
            "SELECT 2",
            "foreign-read-after-failed-ddl",
            Duration::from_millis(100),
        )
        .await;
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        foreign_read.expect("failed DDL planning must release a newly acquired catalog writer");
    }

    #[tokio::test]
    async fn cancelled_session_ddl_before_execution_does_not_hold_catalog_writer() {
        let (_temp_dir, state) = catalog_guard_test_server();
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        let mut execution = Box::pin(execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "CREATE TABLE cancelled_unstarted_ddl (id INTEGER)",
            "cancelled-before-execution",
            Duration::from_secs(2),
        ));
        assert!(futures::poll!(execution.as_mut()).is_pending());
        drop(execution);
        let foreign_read = execute_non_session_statements_with_routing(
            state.clone(),
            "SELECT 2",
            "foreign-read-after-cancel",
            Duration::from_millis(100),
        )
        .await;
        assert!(!state
            .catalog
            .read()
            .unwrap()
            .table_exists("cancelled_unstarted_ddl"));
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        foreign_read.expect("cancelled unstarted DDL must release its new catalog writer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_ddl_execution_errors_release_only_unused_new_catalog_writer() {
        for (case, prior_ddl, ddl, retain_writer) in [
            (
                "no-catalog-effects",
                false,
                "CREATE UNIQUE INDEX failed_unique_idx ON duplicate_items (duplicate_value)",
                false,
            ),
            (
                "partial-batch-success",
                false,
                "CREATE INDEX retained_idx ON duplicate_items (id); \
                 CREATE UNIQUE INDEX failed_unique_idx ON duplicate_items (duplicate_value)",
                true,
            ),
            (
                "previous-operation-writer",
                true,
                "CREATE UNIQUE INDEX failed_unique_idx ON duplicate_items (duplicate_value)",
                true,
            ),
        ] {
            let (_temp_dir, state) = catalog_guard_test_server();
            for sql in [
                "CREATE TABLE duplicate_items (id INTEGER, duplicate_value INTEGER)",
                "INSERT INTO duplicate_items VALUES (1, 7), (2, 7)",
            ] {
                execute_non_session_statements_with_routing(
                    state.clone(),
                    sql,
                    case,
                    Duration::from_secs(2),
                )
                .await
                .expect("committed duplicate-value fixture");
            }
            let session_id = state
                .session_manager
                .create_session()
                .await
                .expect("session");
            let handle = state
                .session_manager
                .begin_transaction(&session_id)
                .await
                .expect("transaction");
            if prior_ddl {
                execute_session_statements_with_routing(
                    state.clone(),
                    &session_id,
                    "CREATE INDEX retained_idx ON duplicate_items (id)",
                    case,
                    Duration::from_secs(2),
                )
                .await
                .expect("earlier successful DDL");
            }
            handle
                .plan_for_routing(ddl)
                .await
                .expect("DDL planning succeeds before duplicate-value execution failure");
            let ddl_result = execute_session_statements_with_routing(
                state.clone(),
                &session_id,
                ddl,
                case,
                Duration::from_secs(2),
            )
            .await;
            assert!(
                matches!(
                    ddl_result,
                    Err(ServerError::Sql(alopex_sql::SqlError::Execution { .. }))
                ),
                "{case}: duplicate values must fail in execution: {ddl_result:?}"
            );
            {
                let catalog = state.catalog.read().expect("catalog");
                assert!(!catalog.index_exists("failed_unique_idx"));
                assert_eq!(
                    catalog.index_exists("retained_idx"),
                    retain_writer,
                    "{case}"
                );
            }
            let foreign_read = execute_non_session_statements_with_routing(
                state.clone(),
                "SELECT 2",
                case,
                Duration::from_millis(100),
            )
            .await;
            state
                .finalize_session(
                    &session_id,
                    crate::server::SessionFinalizationAction::Rollback,
                )
                .await
                .expect("session rollback");
            assert!(!state
                .catalog
                .read()
                .expect("catalog")
                .index_exists("retained_idx"));
            if retain_writer {
                assert!(
                    matches!(foreign_read, Err(ServerError::Timeout(_))),
                    "{case}: pending catalog effects must retain the writer"
                );
            } else {
                foreign_read
                    .expect("failed DDL without catalog effects must release its new writer");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_session_does_not_block_idle_foreign_reads() {
        let (_temp_dir, state) = catalog_guard_test_server();
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        let response = stream_response(
            state.clone(),
            SqlRequest {
                sql: "SELECT 1".into(),
                session_id: Some(session_id.to_string()),
                streaming: true,
            },
            &RequestContext {
                correlation_id: "idle-stream-session".into(),
                actor: None,
            },
        );
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("stream body");
        let body = String::from_utf8(body.to_vec()).expect("stream text");
        assert!(
            body.contains("\"row\":["),
            "stream must emit its SELECT result: {body}"
        );
        let foreign_read = execute_non_session_statements_with_routing(
            state.clone(),
            "SELECT 2",
            "foreign-read-after-stream",
            Duration::from_millis(100),
        )
        .await;
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        foreign_read.expect("a completed session SELECT stream must not retain a catalog writer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_ddl_session_finalization_shares_the_catalog_read_gate() {
        let (_temp_dir, state) = catalog_guard_test_server();
        for action in [
            crate::server::SessionFinalizationAction::Commit,
            crate::server::SessionFinalizationAction::Rollback,
        ] {
            let session_id = state
                .session_manager
                .create_session()
                .await
                .expect("session");
            state
                .session_manager
                .begin_transaction(&session_id)
                .await
                .expect("transaction");
            let reader = Arc::clone(&state.catalog_txn_gate).read_owned().await;
            let finalization = state.finalize_session(&session_id, action);
            tokio::pin!(finalization);
            let result =
                tokio::time::timeout(Duration::from_millis(100), finalization.as_mut()).await;
            drop(reader);
            if result.is_err() {
                finalization
                    .await
                    .expect("finalization after reader release");
            }
            result
                .expect("non-DDL finalization must share the catalog read gate")
                .expect("finalization");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_wins_when_execution_becomes_ready_before_the_next_poll() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let execution = async move { release_rx.await.expect("execution release") };
        tokio::pin!(execution);
        let timeout_at = tokio::time::Instant::now() + Duration::from_secs(5);
        let deadline = wait_for_sql_deadline(execution.as_mut(), timeout_at);
        tokio::pin!(deadline);
        assert!(futures::poll!(deadline.as_mut()).is_pending());

        tokio::time::advance(Duration::from_secs(5)).await;
        release_tx.send(()).expect("execution receiver");

        assert!(matches!(
            deadline.await,
            SqlDeadline::TimedOut { started: true }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn session_operation_gate_timeout_does_not_execute_after_release() {
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
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        let handle = state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        let operation = handle.lock_operation().await;

        let execution = execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "CREATE TABLE late_session_sql (id INTEGER)",
            "session-operation-deadline",
            Duration::from_secs(5),
        );
        tokio::pin!(execution);
        assert!(futures::poll!(execution.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(execution.await, Err(ServerError::Timeout(_))));

        drop(operation);
        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        assert!(!state
            .catalog
            .read()
            .unwrap()
            .table_exists("late_session_sql"));
    }

    #[tokio::test(start_paused = true)]
    async fn non_session_catalog_gate_timeout_does_not_execute_after_release() {
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
        let gate = Arc::clone(&state.catalog_txn_gate).write_owned().await;

        let execution = execute_non_session_statements_with_routing(
            state.clone(),
            "CREATE TABLE late_non_session_sql (id INTEGER)",
            "catalog-gate-deadline",
            Duration::from_secs(5),
        );
        tokio::pin!(execution);
        assert!(futures::poll!(execution.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(execution.await, Err(ServerError::Timeout(_))));

        drop(gate);
        tokio::task::yield_now().await;
        assert!(!state
            .catalog
            .read()
            .unwrap()
            .table_exists("late_non_session_sql"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_session_create_is_isolated_until_rollback() {
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
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        execute_session_statements_with_routing(
            state.clone(),
            &session_id,
            "CREATE TABLE pending_session_table (id INTEGER)",
            "pending-session-create",
            Duration::from_secs(5),
        )
        .await
        .expect("session create");
        assert!(Arc::clone(&state.catalog_txn_gate)
            .try_read_owned()
            .is_err());

        let foreign_insert = execute_non_session_statements_with_routing(
            state.clone(),
            "INSERT INTO pending_session_table VALUES (1)",
            "foreign-insert",
            Duration::from_secs(5),
        );
        tokio::pin!(foreign_insert);
        assert!(futures::poll!(foreign_insert.as_mut()).is_pending());

        state
            .finalize_session(
                &session_id,
                crate::server::SessionFinalizationAction::Rollback,
            )
            .await
            .expect("session rollback");
        let foreign_error = foreign_insert.await.expect_err("foreign insert must fail");
        assert!(matches!(foreign_error, ServerError::Sql(_)));
        assert!(!state
            .catalog
            .read()
            .unwrap()
            .table_exists("pending_session_table"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_same_name_ddl_keeps_the_successful_table() {
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
        let gate = Arc::clone(&state.catalog_txn_gate).write_owned().await;

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                first_state,
                "CREATE TABLE concurrent_ddl (id INTEGER)",
                "first-ddl",
                Duration::from_secs(2),
            )
            .await
        });
        let second_state = state.clone();
        let second = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                second_state,
                "CREATE TABLE concurrent_ddl (id INTEGER)",
                "second-ddl",
                Duration::from_secs(2),
            )
            .await
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        drop(gate);

        let first = first.await.expect("first task");
        let second = second.await.expect("second task");
        assert_ne!(first.is_ok(), second.is_ok());
        assert!(state.catalog.read().unwrap().table_exists("concurrent_ddl"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_rollback_serializes_with_session_and_non_session_ddl() {
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
        let session_id = state
            .session_manager
            .create_session()
            .await
            .expect("session");
        let handle = state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .expect("transaction");
        let catalog_gate = Arc::clone(&state.catalog_txn_gate).write_owned().await;

        let session_state = state.clone();
        let session_id_for_ddl = session_id.clone();
        let session_ddl = tokio::spawn(async move {
            execute_session_statements_with_routing(
                session_state,
                &session_id_for_ddl,
                "CREATE TABLE rolled_back_session_ddl (id INTEGER)",
                "session-ddl",
                Duration::from_secs(10),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.operation_is_locked() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("session DDL must acquire the operation gate before the catalog gate");

        let finalization = state.finalize_session(
            &session_id,
            crate::server::SessionFinalizationAction::Rollback,
        );
        let non_session_state = state.clone();
        let non_session_ddl = tokio::spawn(async move {
            execute_non_session_statements_with_routing(
                non_session_state,
                "CREATE TABLE foreign_concurrent_ddl (id INTEGER)",
                "non-session-ddl",
                Duration::from_secs(10),
            )
            .await
        });
        drop(catalog_gate);

        tokio::time::timeout(Duration::from_secs(10), async {
            session_ddl
                .await
                .expect("session DDL task")
                .expect("session DDL result");
            non_session_ddl
                .await
                .expect("non-session DDL task")
                .expect("non-session DDL result");
            finalization.await.expect("session rollback");
        })
        .await
        .expect("consistent lock ordering must avoid a deadlock");

        let catalog = state.catalog.read().unwrap();
        assert!(!catalog.table_exists("rolled_back_session_ddl"));
        assert!(catalog.table_exists("foreign_concurrent_ddl"));
    }

    fn assert_index_metadata_eq(actual: &IndexMetadata, expected: &IndexMetadata) {
        assert_eq!(actual.index_id, expected.index_id);
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.catalog_name, expected.catalog_name);
        assert_eq!(actual.namespace_name, expected.namespace_name);
        assert_eq!(actual.table, expected.table);
        assert_eq!(actual.columns, expected.columns);
        assert_eq!(actual.column_indices, expected.column_indices);
        assert_eq!(actual.unique, expected.unique);
        assert_eq!(actual.method, expected.method);
        assert_eq!(actual.options, expected.options);
    }

    #[tokio::test]
    async fn index_replacement_rollback_restores_original_metadata() {
        use alopex_sql::ast::ddl::IndexMethod;
        use alopex_sql::planner::plan_sql_for_routing;

        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let state = &server.state;

        let table = {
            let catalog = state.catalog.read().expect("catalog");
            let planned = plan_sql_for_routing(&*catalog, "CREATE TABLE items (id INTEGER)")
                .expect("plan table creation");
            let LogicalPlan::CreateTable { table, .. } = &planned[0].plan else {
                panic!("expected CREATE TABLE plan");
            };
            table.clone()
        };
        state
            .catalog
            .write()
            .expect("catalog")
            .create_table(table)
            .expect("register table");

        let original = IndexMetadata::new(41, "idx_items_id", "items", vec!["id".into()])
            .with_column_indices(vec![0])
            .with_unique(true)
            .with_method(IndexMethod::BTree)
            .with_option("fillfactor", "70");
        state
            .catalog
            .write()
            .expect("catalog")
            .create_index(original.clone())
            .expect("register original index");

        let planned = {
            let catalog = state.catalog.read().expect("catalog");
            plan_sql_for_routing(
                &*catalog,
                "DROP INDEX IF EXISTS idx_items_id; \
                 CREATE INDEX IF NOT EXISTS idx_items_id ON items (id)",
            )
            .expect("plan index replacement")
        };
        let candidates =
            table_lifecycle_candidates(state, &planned).expect("index lifecycle candidates");

        let replacement = IndexMetadata::new(42, "idx_items_id", "items", vec!["id".into()])
            .with_column_indices(vec![0])
            .with_method(IndexMethod::BTree)
            .with_option("fillfactor", "90");
        {
            let mut catalog = state.catalog.write().expect("catalog");
            catalog
                .drop_index("idx_items_id")
                .expect("drop original index");
            catalog
                .create_index(replacement)
                .expect("register replacement index");
        }

        let (_, rollback) =
            statement_effects_after_execution(state, candidates).expect("replacement effects");
        state
            .apply_catalog_rollback_effects(rollback)
            .expect("rollback replacement");

        let catalog = state.catalog.read().expect("catalog");
        let restored = catalog
            .get_index("idx_items_id")
            .expect("original index restored");
        assert_index_metadata_eq(restored, &original);
    }

    #[tokio::test]
    async fn ctas_lifecycle_tracks_creation_and_rolls_back_only_new_tables() {
        use alopex_sql::planner::plan_sql_for_routing;

        let temp_dir = tempfile::tempdir().expect("temporary server data");
        let server = crate::Server::new(crate::config::ServerConfig {
            data_dir: temp_dir.path().join("data"),
            audit_log_enabled: false,
            tracing_enabled: false,
            metrics_enabled: false,
            ..crate::config::ServerConfig::default()
        })
        .expect("server");
        let state = &server.state;
        let planned = {
            let catalog = state.catalog.read().expect("catalog");
            plan_sql_for_routing(&*catalog, "CREATE TABLE copied AS SELECT 1 AS id")
                .expect("plan CTAS")
        };
        let candidates = table_lifecycle_candidates(state, &planned).expect("CTAS candidates");
        assert!(matches!(
            candidates.as_slice(),
            [TableLifecycleCandidate::Created { table_name, before: None }]
                if table_name == "copied"
        ));

        let LogicalPlan::CreateTableAs { table, .. } = &planned[0].plan else {
            panic!("expected CTAS plan");
        };
        // Isolate lifecycle bookkeeping from row execution: expose the catalog
        // mutation that the executor makes before effects are collected.
        state
            .catalog
            .write()
            .expect("catalog")
            .create_table(table.clone())
            .expect("register created table");
        let (effects, rollback) =
            statement_effects_after_execution(state, candidates).expect("creation effects");
        assert!(matches!(
            effects.as_slice(),
            [TableLifecycleEffect::Created { table_id, .. }] if *table_id == table.table_id
        ));
        assert!(matches!(
            rollback.as_slice(),
            [CatalogRollbackEffect::DropTable { table_name, .. }] if table_name == "copied"
        ));

        let no_op = {
            let catalog = state.catalog.read().expect("catalog");
            plan_sql_for_routing(
                &*catalog,
                "CREATE TABLE IF NOT EXISTS copied AS SELECT 2 AS id",
            )
            .expect("plan no-op CTAS")
        };
        let no_op_candidates = table_lifecycle_candidates(state, &no_op).expect("no-op candidates");
        let (no_op_effects, no_op_rollback) =
            statement_effects_after_execution(state, no_op_candidates).expect("no-op effects");
        assert!(no_op_effects.is_empty());
        assert!(no_op_rollback.is_empty());
        state
            .apply_catalog_rollback_effects(no_op_rollback)
            .expect("no-op rollback");
        assert!(state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("copied"));

        state
            .apply_catalog_rollback_effects(rollback)
            .expect("rollback new CTAS table");
        assert!(!state
            .catalog
            .read()
            .expect("catalog")
            .table_exists("copied"));
    }

    fn continuous_aggregate_statement_kind() -> alopex_sql::ast::StatementKind {
        alopex_sql::ast::StatementKind::CreateContinuousAggregate(
            alopex_sql::ast::CreateContinuousAggregate {
                name: "hourly_metrics".to_string(),
                name_span: alopex_sql::ast::Span::default(),
                query: alopex_sql::ast::Select {
                    with: None,
                    distinct: false,
                    distinct_on: vec![],
                    set_operations: vec![],
                    projection: vec![alopex_sql::ast::SelectItem::Wildcard {
                        span: alopex_sql::ast::Span::default(),
                    }],
                    from: vec![],
                    selection: None,
                    group_by: None,
                    having: None,
                    windows: vec![],
                    qualify: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    limit_with_ties: false,
                    knn_options: vec![],
                    span: alopex_sql::ast::Span::default(),
                },
                options: vec![],
                span: alopex_sql::ast::Span::default(),
            },
        )
    }

    #[test]
    fn unsupported_generic_statement_is_not_schema_ddl() {
        assert!(!StatementSchemaClass::Unsupported.is_schema_mutation());
    }

    #[test]
    fn continuous_aggregate_is_unsupported_and_not_schema_ddl() {
        let class = classify_statement_schema(&continuous_aggregate_statement_kind());

        assert_eq!(class, StatementSchemaClass::Unsupported);
        assert!(!class.is_schema_mutation());
    }

    #[test]
    fn current_schema_and_non_schema_statements_keep_their_classification() {
        assert!(is_ddl("CREATE TABLE items (id INT PRIMARY KEY);"));
        assert!(is_ddl("DROP TABLE items;"));
        assert!(!is_ddl("SELECT 1;"));
        assert!(!is_ddl("VALUES (1);"));
        assert!(!is_write_sql("VALUES (1);"));
        assert!(!is_ddl("INSERT INTO items (id) VALUES (1);"));
    }

    #[test]
    fn explain_only_inherits_nested_write_classification_when_analyzing() {
        assert!(!is_write_sql("EXPLAIN INSERT INTO items VALUES (1)"));
        assert!(is_write_sql("EXPLAIN ANALYZE INSERT INTO items VALUES (1)"));
        assert!(!is_ddl("EXPLAIN CREATE TABLE items (id INT)"));
        assert!(is_ddl("EXPLAIN ANALYZE CREATE TABLE items (id INT)"));
    }
}
