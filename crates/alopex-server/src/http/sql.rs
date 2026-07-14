use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_cluster::{
    CatalogTableRef, CatalogTableSnapshot, PlacementCatalog, PlanId, QueryRouter,
    QueryRoutingRequest, QueryTableReference, QueryTableReferenceAccess, QueryTableReferenceSource,
    RoutingDecisionKind, RoutingDiagnostics, TableLifecycleEffect, TableRef,
};
use alopex_core::kv::async_adapter::AsyncKVTransactionAdapter;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::storage::format::bincode_config;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::persistent::{PersistedTableMeta, TableFqn, TABLES_PREFIX};
use alopex_sql::catalog::TableMetadata;
use alopex_sql::planner::{
    LogicalPlan, PlannedStatement, TableReference, TableReferenceAccess, TableReferenceSource,
};
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use alopex_sql::storage::AsyncSqlTransaction;
use alopex_sql::AlopexDialect;
use axum::extract::Extension;
use axum::response::{IntoResponse, Response};
use axum::Json;
use bincode::Options;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::ops::memory::MemoryControlPolicy;
use crate::server::ServerState;
use crate::session::{CatalogRollbackEffect, SessionId, TxnHandle};

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub streaming: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfoResponse {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct SqlResponse {
    pub columns: Vec<ColumnInfoResponse>,
    pub rows: Vec<Vec<alopex_sql::storage::SqlValue>>,
    pub affected_rows: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routing_diagnostics: Vec<RoutingDiagnostics>,
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
    Txn(AsyncTxn),
    Handle(TxnHandle),
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
    },
    CreateIndex {
        index_name: String,
        index_existed_before: bool,
    },
    DropIndex {
        index_name: String,
        before: Option<TableLifecycleState>,
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
        alopex_sql::executor::ExecutionResult,
        Vec<RoutingDiagnostics>,
    )> = async {
        if let Some(session_id) = &request.session_id {
            let session_id = session_id
                .parse::<SessionId>()
                .map_err(|_| ServerError::BadRequest("invalid session_id".into()))?;
            execute_session_statement_with_routing(
                &state,
                &session_id,
                sql,
                &ctx.correlation_id,
                state.config.query_timeout,
            )
            .await
        } else {
            execute_non_session_statement_with_routing(
                &state,
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

    if is_ddl && request.session_id.is_none() {
        sync_catalog_to_store(&state)?;
    }

    state.metrics.record_query(start.elapsed(), true);

    Ok(map_execution_result(exec_result.0, exec_result.1))
}

pub(crate) async fn execute_session_statement_with_routing(
    state: &ServerState,
    session_id: &SessionId,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    alopex_sql::executor::ExecutionResult,
    Vec<RoutingDiagnostics>,
)> {
    let handle = state.session_manager.get_transaction(session_id).await?;
    let routing_plan = route_session_sql(state, &handle, sql, correlation_id).await?;
    if let Some(diagnostic) = future_distributed_diagnostic(&routing_plan.diagnostics) {
        return Err(future_distributed_error(diagnostic));
    }
    let lifecycle_candidates = table_lifecycle_candidates(state, &routing_plan.planned)?;
    let result = tokio::time::timeout(timeout, handle.execute(sql))
        .await
        .map_err(|_| ServerError::Timeout("query timeout".into()))?
        .map_err(|err| ServerError::Sql(err.into()))?;
    let (lifecycle_effects, catalog_rollback_effects) =
        statement_effects_after_execution(state, lifecycle_candidates)?;
    handle
        .buffer_table_lifecycle_effects(lifecycle_effects)
        .await;
    handle
        .buffer_catalog_rollback_effects(catalog_rollback_effects)
        .await;
    Ok((result, routing_plan.diagnostics))
}

pub(crate) async fn execute_non_session_statement_with_routing(
    state: &ServerState,
    sql: &str,
    correlation_id: &str,
    timeout: Duration,
) -> Result<(
    alopex_sql::executor::ExecutionResult,
    Vec<RoutingDiagnostics>,
)> {
    let mut txn = state.begin_sql_txn().await?;
    let routing_plan = match route_non_session_sql(state, &txn, sql, correlation_id).await {
        Ok(plan) => plan,
        Err(err) => {
            let _ = txn.async_rollback().await;
            return Err(err);
        }
    };
    if let Some(diagnostic) = future_distributed_diagnostic(&routing_plan.diagnostics) {
        let _ = txn.async_rollback().await;
        return Err(future_distributed_error(diagnostic));
    }
    let lifecycle_candidates = table_lifecycle_candidates(state, &routing_plan.planned)?;
    let fut = match tokio::time::timeout(timeout, txn.async_execute(sql)).await {
        Ok(result) => result,
        Err(_) => {
            let _ = txn.async_rollback().await;
            return Err(ServerError::Timeout("query timeout".into()));
        }
    };
    match fut {
        Ok(result) => {
            let (lifecycle_effects, _) =
                statement_effects_after_execution(state, lifecycle_candidates)?;
            txn.async_commit()
                .await
                .map_err(|err| ServerError::Sql(err.into()))?;
            state.apply_table_lifecycle_effects(lifecycle_effects)?;
            Ok((result, routing_plan.diagnostics))
        }
        Err(err) => {
            let _ = txn.async_rollback().await;
            Err(ServerError::Sql(err.into()))
        }
    }
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
        match &statement.plan {
            LogicalPlan::CreateTable { table, .. } => {
                candidates.push(TableLifecycleCandidate::Created {
                    table_name: table.name.clone(),
                    before: table_lifecycle_state(state, &table.name)?,
                });
            }
            LogicalPlan::DropTable { name, .. } => {
                candidates.push(TableLifecycleCandidate::Dropped {
                    table_name: name.clone(),
                    before: table_lifecycle_state(state, name)?,
                });
            }
            LogicalPlan::CreateIndex { index, .. } => {
                candidates.push(TableLifecycleCandidate::CreateIndex {
                    index_name: index.name.clone(),
                    index_existed_before: index_exists(state, &index.name)?,
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
                        lifecycle_effects.push(TableLifecycleEffect::Created {
                            table_ref: after.table_ref,
                            table_id: after.table_id,
                        });
                        catalog_rollback_effects.push(CatalogRollbackEffect::DropTable {
                            table_name: after.table.name,
                        });
                    }
                }
            }
            TableLifecycleCandidate::Dropped { table_name, before } => {
                let after = table_lifecycle_state(state, &table_name)?;
                if let Some(before) = before {
                    let changed = match after.as_ref() {
                        Some(after) => after.table_id != before.table_id,
                        None => true,
                    };
                    if changed {
                        lifecycle_effects.push(TableLifecycleEffect::Dropped {
                            table_ref: before.table_ref.clone(),
                            table_id: before.table_id,
                        });
                        catalog_rollback_effects.push(CatalogRollbackEffect::CreateTable {
                            table: Box::new(before.table),
                        });
                    }
                }
            }
            TableLifecycleCandidate::CreateIndex {
                index_name,
                index_existed_before,
            } => {
                if !index_existed_before {
                    if let Some(after) = index_table_lifecycle_state(state, &index_name)? {
                        lifecycle_effects.push(TableLifecycleEffect::SchemaChanged {
                            table_ref: after.table_ref,
                            table_id: after.table_id,
                        });
                    }
                }
            }
            TableLifecycleCandidate::DropIndex { index_name, before } => {
                if let Some(before) = before {
                    if !index_exists(state, &index_name)? {
                        lifecycle_effects.push(TableLifecycleEffect::SchemaChanged {
                            table_ref: before.table_ref,
                            table_id: before.table_id,
                        });
                    }
                }
            }
        }
    }
    Ok((lifecycle_effects, catalog_rollback_effects))
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
) -> Result<Option<TableLifecycleState>> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    let Some(index) = guard.get_index(index_name) else {
        return Ok(None);
    };
    Ok(guard
        .get_table(&index.table)
        .map(|table| TableLifecycleState {
            table_ref: TableRef::new(table_fqn_string(table)),
            table_id: table.table_id,
            table: table.clone(),
        }))
}

fn index_exists(state: &ServerState, index_name: &str) -> Result<bool> {
    let guard = state
        .catalog
        .read()
        .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
    Ok(guard.get_index(index_name).is_some())
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
        let persisted = PersistedTableMeta::from(&table);
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
    tokio::spawn(async move {
        let start = Instant::now();
        let mut bytes_sent = 0usize;
        let mut success = true;
        let mut source = match session_id {
            Some(id) => {
                let parsed = match id.parse::<SessionId>() {
                    Ok(id) => id,
                    Err(_) => {
                        let _ = sender
                            .send(stream_item_error(
                                ServerError::BadRequest("invalid session_id".into()),
                                &correlation_id,
                            ))
                            .await;
                        return;
                    }
                };
                match state_clone.session_manager.get_transaction(&parsed).await {
                    Ok(handle) => {
                        match route_session_statement_for_execution(
                            &state_clone,
                            &handle,
                            &sql,
                            &correlation_id,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(err) => {
                                let _ = sender.send(stream_item_error(err, &correlation_id)).await;
                                return;
                            }
                        }
                        StreamSource::Handle(handle)
                    }
                    Err(err) => {
                        let _ = sender.send(stream_item_error(err, &correlation_id)).await;
                        return;
                    }
                }
            }
            None => match state_clone.begin_sql_txn().await {
                Ok(txn) => StreamSource::Txn(txn),
                Err(err) => {
                    let _ = sender.send(stream_item_error(err, &correlation_id)).await;
                    return;
                }
            },
        };

        let mut stream = match &mut source {
            StreamSource::Handle(handle) => handle.query(&sql),
            StreamSource::Txn(txn) => txn.async_query(&sql),
        };
        let deadline = start + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = sender
                    .send(stream_item_error(
                        ServerError::Timeout("query timeout".into()),
                        &correlation_id,
                    ))
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
                            let _ = sender
                                .send(stream_item_error(
                                    ServerError::Timeout("query timeout".into()),
                                    &correlation_id,
                                ))
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
                                        let _ = sender
                                            .send(stream_item_error(err, &correlation_id))
                                            .await;
                                        success = false;
                                        break;
                                    }
                                    if bytes_sent > max_response_size {
                                        let _ = sender
                                            .send(stream_item_error(
                                                ServerError::PayloadTooLarge(
                                                    "response size exceeds limit".into(),
                                                ),
                                                &correlation_id,
                                            ))
                                            .await;
                                        success = false;
                                        break;
                                    }
                                }
                                Err(err) => {
                                    let _ = sender
                                        .send(stream_item_error(
                                            ServerError::Internal(err.to_string()),
                                            &correlation_id,
                                        ))
                                        .await;
                                    success = false;
                                    break;
                                }
                            }
                            match sender.try_send(item) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(item)) => {
                                    metrics.record_backpressure();
                                    if sender.send(item).await.is_err() {
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
                            let _ = sender
                                .send(stream_item_error(
                                    ServerError::Sql(err.into()),
                                    &correlation_id,
                                ))
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
        if let StreamSource::Txn(txn) = source {
            let _ = txn.async_rollback().await;
        }
        if let Some(logger) = audit {
            logger.log_ddl(&sql, None, &correlation_id);
        }
        metrics.record_query(start.elapsed(), success);
        let _ = sender
            .send(StreamItem {
                row: None,
                error: None,
                done: true,
            })
            .await;
    });

    let stream = ReceiverStream::new(receiver).map(|item| {
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

fn map_execution_result(
    exec_result: alopex_sql::executor::ExecutionResult,
    routing_diagnostics: Vec<RoutingDiagnostics>,
) -> SqlResponse {
    match exec_result {
        alopex_sql::executor::ExecutionResult::Query(query) => SqlResponse {
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
            routing_diagnostics,
        },
        alopex_sql::executor::ExecutionResult::RowsAffected(rows) => SqlResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: Some(rows),
            routing_diagnostics,
        },
        alopex_sql::executor::ExecutionResult::Success => SqlResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            routing_diagnostics,
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
        alopex_sql::planner::ResolvedType::Vector { dimension, metric } => {
            format!("VECTOR({dimension}, {metric:?})")
        }
        alopex_sql::planner::ResolvedType::Null => "NULL".to_string(),
    }
}

fn is_ddl(sql: &str) -> bool {
    let Ok(statements) = alopex_sql::parser::Parser::parse_sql(&AlopexDialect, sql) else {
        return false;
    };
    statements.iter().any(|stmt| match &stmt.kind {
        alopex_sql::ast::StatementKind::CreateTable(_)
        | alopex_sql::ast::StatementKind::DropTable(_)
        | alopex_sql::ast::StatementKind::CreateIndex(_)
        | alopex_sql::ast::StatementKind::DropIndex(_) => true,
        alopex_sql::ast::StatementKind::Select(_)
        | alopex_sql::ast::StatementKind::Insert(_)
        | alopex_sql::ast::StatementKind::Update(_)
        | alopex_sql::ast::StatementKind::Delete(_) => false,
    })
}

fn is_write_sql(sql: &str) -> bool {
    let Ok(statements) = alopex_sql::parser::Parser::parse_sql(&AlopexDialect, sql) else {
        return false;
    };
    statements
        .iter()
        .any(|stmt| !matches!(stmt.kind, alopex_sql::ast::StatementKind::Select(_)))
}
