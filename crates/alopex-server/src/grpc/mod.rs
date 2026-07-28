use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{future::BoxFuture, StreamExt};
use prost::Message;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::{async_trait, Request, Response, Status};
use tower::{Layer, Service};
use uuid::Uuid;

use alopex_cluster::crdt::{CrdtOutcome, CrdtValue};
use alopex_cluster::RangeIdentity;

use crate::error::{Result, ServerError};
use crate::http::sql::{
    execute_non_session_statement_with_routing, execute_session_statement_with_routing,
    sync_catalog_to_store,
};
use crate::metrics::Metrics;
use crate::ops::memory::MemoryControlPolicy;
use crate::server::ServerState;
use crate::session::SessionId;
use crate::tls;

/// Thin newtype wrapper around a [`tokio_rustls::server::TlsStream`] over a
/// plain TCP connection.
///
/// This exists solely so we can implement tonic's [`Connected`] trait
/// locally: neither `tokio_rustls::server::TlsStream` nor `Connected` are
/// defined in this crate, so a direct impl would violate the orphan rule.
/// (Previously this bridging impl was provided by tonic's own `tls` feature,
/// but that feature pulls in tonic's bundled rustls 0.21 stack, which is
/// exactly the vulnerable dependency chain this migration removes.)
struct TlsIo(tokio_rustls::server::TlsStream<TcpStream>);

impl AsyncRead for TlsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl Connected for TlsIo {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        let (tcp, _session) = self.0.get_ref();
        TcpConnectInfo {
            local_addr: tcp.local_addr().ok(),
            remote_addr: tcp.peer_addr().ok(),
        }
    }
}

pub mod proto {
    tonic::include_proto!("alopex.v0");
}

use proto::alopex_service_server::{AlopexService, AlopexServiceServer};

#[derive(Clone)]
struct GrpcContext {
    correlation_id: String,
    actor: Option<String>,
    span: tracing::Span,
}

#[derive(Clone)]
struct ConnectionMetricsLayer {
    metrics: Metrics,
}

impl ConnectionMetricsLayer {
    fn new(metrics: Metrics) -> Self {
        Self { metrics }
    }
}

#[derive(Clone)]
struct ConnectionMetricsService<S> {
    inner: S,
    metrics: Metrics,
}

impl<S> Layer<S> for ConnectionMetricsLayer {
    type Service = ConnectionMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectionMetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

impl<S, Req> Service<Req> for ConnectionMetricsService<S>
where
    S: Service<Req> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.metrics.record_connection(1);
        let metrics = self.metrics.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            metrics.record_connection(-1);
            result
        })
    }
}

// tonic のインターセプタは `Result<Request<()>, tonic::Status>` を返す契約のため、
// `Status` を縮小できない。large エラー型は tonic 側の API 制約として許容する。
#[allow(clippy::result_large_err)]
pub async fn serve(
    state: Arc<ServerState>,
    addr: SocketAddr,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let svc = AlopexServiceImpl {
        state: state.clone(),
    };
    let auth = state.auth.clone();
    let interceptor = move |mut req: Request<()>| {
        let correlation_id =
            extract_correlation_id(req.metadata()).unwrap_or_else(|| Uuid::new_v4().to_string());
        let traceparent = extract_traceparent(req.metadata());
        let actor = auth
            .validate_grpc(req.metadata())
            .map_err(|_| Status::unauthenticated("unauthorized"))?;
        let span = tracing::info_span!(
            "grpc_request",
            correlation_id = %correlation_id,
            traceparent = %traceparent.as_deref().unwrap_or("")
        );
        req.extensions_mut().insert(GrpcContext {
            correlation_id,
            actor,
            span,
        });
        Ok(req)
    };

    let mut server = tonic::transport::Server::builder()
        .layer(ConnectionMetricsLayer::new(state.metrics.clone()));
    let shutdown_signal = async move {
        let _ = shutdown.recv().await;
    };

    let service = AlopexServiceServer::with_interceptor(svc, interceptor);
    if let Some(tls) = &state.config.tls {
        let rustls_config = tls::build_rustls_config(tls)?;
        let acceptor = TlsAcceptor::from(rustls_config);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(ServerError::Io)?;
        let incoming = TcpListenerStream::new(listener).then(move |conn| {
            let acceptor = acceptor.clone();
            async move {
                let stream = conn?;
                acceptor
                    .accept(stream)
                    .await
                    .map(TlsIo)
                    .map_err(std::io::Error::other)
            }
        });
        server
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, shutdown_signal)
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    } else {
        server
            .add_service(service)
            .serve_with_shutdown(addr, shutdown_signal)
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    }

    Ok(())
}

pub fn service(state: Arc<ServerState>) -> AlopexServiceServer<impl AlopexService> {
    let svc = AlopexServiceImpl {
        state: state.clone(),
    };
    AlopexServiceServer::new(svc)
}

#[derive(Clone)]
struct AlopexServiceImpl {
    state: Arc<ServerState>,
}

#[async_trait]
impl AlopexService for AlopexServiceImpl {
    type ExecuteSqlStream =
        tokio_stream::Iter<std::vec::IntoIter<std::result::Result<proto::SqlResultSet, Status>>>;

    async fn execute_sql(
        &self,
        request: Request<proto::SqlRequest>,
    ) -> std::result::Result<Response<Self::ExecuteSqlStream>, Status> {
        let ctx = read_context(&request);
        let span = ctx.span.clone();
        let _enter = span.enter();
        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }

        // issue #25: HTTP `/sql` (非ストリーミング) と同一の実行経路に統一する。
        // 旧来のストリーミング専用経路 (async_query) は SELECT リスト内の
        // スカラーサブクエリ等を実行できず、同一 SQL でも HTTP と結果が
        // 分岐していた。書き込み許可チェック・タイムアウト・コミット/
        // ロールバック・監査ログ・メトリクスは execute_non_streaming に集約する。
        let http_request = crate::http::sql::SqlRequest {
            sql: req.sql,
            session_id: if req.session_id.is_empty() {
                None
            } else {
                Some(req.session_id)
            },
            streaming: false,
        };
        let http_ctx = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let result =
            crate::http::sql::execute_non_streaming(self.state.clone(), &http_request, &http_ctx)
                .await
                .map_err(|err| map_status(err, &ctx.correlation_id))?;

        // Each statement is represented by one result-set message. This keeps
        // empty result sets and DDL/DML status results observable, while also
        // carrying the column metadata that the old Row-only stream dropped.
        let memory_policy = MemoryControlPolicy::from_env();
        let mut bytes_total = 0usize;
        let mut result_sets = Vec::with_capacity(result.results.len());
        for result_set in result.results {
            let is_query = !result_set.columns.is_empty();
            let columns = result_set
                .columns
                .into_iter()
                .map(|column| proto::SqlColumn {
                    name: column.name,
                    data_type: column.data_type,
                })
                .collect();
            let rows = result_set
                .rows
                .into_iter()
                .map(|row| proto::Row {
                    values: row.iter().map(sql_value_to_proto).collect(),
                })
                .collect();
            let has_affected_rows = result_set.affected_rows.is_some();
            let affected_rows = result_set.affected_rows.unwrap_or_default();
            let message = proto::SqlResultSet {
                columns,
                rows,
                affected_rows,
                has_affected_rows,
                success: !has_affected_rows && !is_query,
            };
            bytes_total = bytes_total.saturating_add(message.encoded_len());
            memory_policy
                .enforce_output_bytes(bytes_total as u64)
                .map_err(|err| map_status(err, &ctx.correlation_id))?;
            if bytes_total > self.state.config.max_response_size {
                return Err(map_status(
                    ServerError::PayloadTooLarge("response size exceeds limit".into()),
                    &ctx.correlation_id,
                ));
            }
            result_sets.push(Ok(message));
        }

        // Return the result sets directly without an intermediate channel.
        Ok(Response::new(tokio_stream::iter(result_sets)))
    }

    async fn execute_ddl(
        &self,
        request: Request<proto::DdlRequest>,
    ) -> std::result::Result<Response<proto::DdlResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }
        if let Err(err) = self.state.lifecycle_state.check_write_allowed() {
            return Err(map_status(err, &ctx.correlation_id));
        }
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::invalid_argument("invalid session_id"));
                }
            };
            let exec_result = execute_session_statement_with_routing(
                &self.state,
                &session_id,
                &req.sql,
                &ctx.correlation_id,
                self.state.config.query_timeout,
            )
            .await
            .map(|(result, _)| result)
            .map_err(|err| map_status(err, &ctx.correlation_id));
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(err);
                }
            }
        } else {
            match execute_non_session_statement_with_routing(
                &self.state,
                &req.sql,
                &ctx.correlation_id,
                self.state.config.query_timeout,
            )
            .await
            {
                Ok((result, _)) => result,
                Err(err) => {
                    let status = map_status(err, &ctx.correlation_id);
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(status);
                }
            }
        };

        if self.state.config.audit_log_enabled {
            self.state
                .audit
                .log_ddl(&req.sql, ctx.actor.as_deref(), &ctx.correlation_id);
        }
        if req.session_id.is_empty() {
            if let Err(err) = sync_catalog_to_store(&self.state) {
                self.state.metrics.record_query(start.elapsed(), false);
                return Err(map_status(err, &ctx.correlation_id));
            }
        }
        self.state.metrics.record_query(start.elapsed(), true);
        match exec_result {
            alopex_sql::executor::ExecutionResult::Success => {
                Ok(Response::new(proto::DdlResponse { success: true }))
            }
            _ => Err(Status::invalid_argument("DDL returned unexpected result")),
        }
    }

    async fn execute_dml(
        &self,
        request: Request<proto::DmlRequest>,
    ) -> std::result::Result<Response<proto::DmlResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }
        if let Err(err) = self.state.lifecycle_state.check_write_allowed() {
            return Err(map_status(err, &ctx.correlation_id));
        }
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::invalid_argument("invalid session_id"));
                }
            };
            let exec_result = execute_session_statement_with_routing(
                &self.state,
                &session_id,
                &req.sql,
                &ctx.correlation_id,
                self.state.config.query_timeout,
            )
            .await
            .map(|(result, _)| result)
            .map_err(|err| map_status(err, &ctx.correlation_id));
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(err);
                }
            }
        } else {
            match execute_non_session_statement_with_routing(
                &self.state,
                &req.sql,
                &ctx.correlation_id,
                self.state.config.query_timeout,
            )
            .await
            {
                Ok((result, _)) => result,
                Err(err) => {
                    let status = map_status(err, &ctx.correlation_id);
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(status);
                }
            }
        };

        self.state.metrics.record_query(start.elapsed(), true);
        match exec_result {
            alopex_sql::executor::ExecutionResult::RowsAffected(count) => {
                Ok(Response::new(proto::DmlResponse {
                    affected_rows: count,
                }))
            }
            _ => Err(Status::invalid_argument("DML returned unexpected result")),
        }
    }

    async fn begin_transaction(
        &self,
        request: Request<proto::BeginRequest>,
    ) -> std::result::Result<Response<proto::TransactionHandle>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let session_id = self
            .state
            .session_manager
            .create_session()
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        self.state
            .session_manager
            .begin_transaction(&session_id)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        let snapshot = self
            .state
            .session_manager
            .get_session(&session_id)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        let expires_at = snapshot
            .expires_at
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Ok(Response::new(proto::TransactionHandle {
            session_id: session_id.to_string(),
            expires_at_ms: expires_at,
        }))
    }

    async fn commit_transaction(
        &self,
        request: Request<proto::TransactionHandle>,
    ) -> std::result::Result<Response<proto::CommitResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let session_id = request
            .into_inner()
            .session_id
            .parse::<SessionId>()
            .map_err(|_| Status::invalid_argument("invalid session_id"))?;
        let effects = self
            .state
            .session_manager
            .commit(&session_id)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        if !effects.is_empty() {
            self.state
                .apply_table_lifecycle_effects(effects)
                .map_err(|err| map_status(err, &ctx.correlation_id))?;
            sync_catalog_to_store(&self.state)
                .map_err(|err| map_status(err, &ctx.correlation_id))?;
        }
        Ok(Response::new(proto::CommitResponse { success: true }))
    }

    async fn rollback_transaction(
        &self,
        request: Request<proto::TransactionHandle>,
    ) -> std::result::Result<Response<proto::RollbackResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let session_id = request
            .into_inner()
            .session_id
            .parse::<SessionId>()
            .map_err(|_| Status::invalid_argument("invalid session_id"))?;
        let effects = self
            .state
            .session_manager
            .rollback(&session_id)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        self.state
            .apply_catalog_rollback_effects(effects)
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::RollbackResponse { success: true }))
    }

    async fn vector_search(
        &self,
        request: Request<proto::VectorSearchRequest>,
    ) -> std::result::Result<Response<proto::VectorSearchResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let search_request = crate::http::vector::VectorSearchRequest {
            table: req.table,
            vector: req.vector,
            k: req.k as usize,
            index: if req.index.is_empty() {
                None
            } else {
                Some(req.index)
            },
            column: if req.column.is_empty() {
                None
            } else {
                Some(req.column)
            },
        };
        let results = crate::http::vector::search_impl(self.state.clone(), search_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        let mapped = results
            .results
            .into_iter()
            .map(|row| proto::VectorSearchResult {
                id: row.id,
                distance: row.distance,
                row: Some(proto::Row {
                    values: row.row.iter().map(sql_value_to_proto).collect(),
                }),
            })
            .collect();
        Ok(Response::new(proto::VectorSearchResponse {
            results: mapped,
        }))
    }

    async fn vector_upsert(
        &self,
        request: Request<proto::VectorUpsertRequest>,
    ) -> std::result::Result<Response<proto::VectorUpsertResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let upsert_request = crate::http::vector::VectorUpsertRequest {
            table: req.table,
            id: req.id,
            vector: req.vector,
            column: if req.column.is_empty() {
                None
            } else {
                Some(req.column)
            },
        };
        crate::http::vector::upsert_impl(self.state.clone(), upsert_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorUpsertResponse { success: true }))
    }

    async fn vector_delete(
        &self,
        request: Request<proto::VectorDeleteRequest>,
    ) -> std::result::Result<Response<proto::VectorDeleteResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let delete_request = crate::http::vector::VectorDeleteRequest {
            table: req.table,
            id: req.id,
            column: if req.column.is_empty() {
                None
            } else {
                Some(req.column)
            },
        };
        let response = crate::http::vector::delete_impl(self.state.clone(), delete_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorDeleteResponse {
            success: response.success,
        }))
    }

    async fn vector_index_create(
        &self,
        request: Request<proto::VectorIndexCreateRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let create_request = crate::http::vector::VectorIndexCreateRequest {
            name: req.name,
            table: req.table,
            column: req.column,
            method: if req.method.is_empty() {
                None
            } else {
                Some(req.method)
            },
            options: req.options,
            if_not_exists: req.if_not_exists,
        };
        let response = crate::http::vector::index_create_impl(self.state.clone(), create_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorIndexResponse {
            success: response.success,
        }))
    }

    async fn vector_index_update(
        &self,
        request: Request<proto::VectorIndexUpdateRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let update_request = crate::http::vector::VectorIndexUpdateRequest {
            name: req.name,
            table: req.table,
            column: req.column,
            method: if req.method.is_empty() {
                None
            } else {
                Some(req.method)
            },
            options: req.options,
        };
        let response = crate::http::vector::index_update_impl(self.state.clone(), update_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorIndexResponse {
            success: response.success,
        }))
    }

    async fn vector_index_delete(
        &self,
        request: Request<proto::VectorIndexDeleteRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let delete_request = crate::http::vector::VectorIndexDeleteRequest {
            name: req.name,
            if_exists: req.if_exists,
        };
        let response = crate::http::vector::index_delete_impl(self.state.clone(), delete_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorIndexResponse {
            success: response.success,
        }))
    }

    async fn vector_index_compact(
        &self,
        request: Request<proto::VectorIndexCompactRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let compact_request = crate::http::vector::VectorIndexCompactRequest { name: req.name };
        let response = crate::http::vector::index_compact_impl(self.state.clone(), compact_request)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        Ok(Response::new(proto::VectorIndexResponse {
            success: response.success,
        }))
    }

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> std::result::Result<Response<proto::HealthResponse>, Status> {
        Ok(Response::new(proto::HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn cluster_status(
        &self,
        request: Request<proto::ClusterStatusRequest>,
    ) -> std::result::Result<Response<proto::ClusterStatusResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let cluster = self
            .state
            .cluster_status_snapshot()
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterStatusResponse {
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
        }))
    }

    async fn cluster_join(
        &self,
        request: Request<proto::ClusterJoinRequest>,
    ) -> std::result::Result<Response<proto::ClusterOperationResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let cluster = self
            .state
            .cluster_join()
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterOperationResponse {
            action: "join".to_string(),
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
            operation_id: Uuid::new_v4().to_string(),
            state: "committed".to_string(),
            reason_code: "membership_changed".to_string(),
        }))
    }

    async fn cluster_leave(
        &self,
        request: Request<proto::ClusterLeaveRequest>,
    ) -> std::result::Result<Response<proto::ClusterOperationResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let cluster = self
            .state
            .cluster_leave()
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterOperationResponse {
            action: "leave".to_string(),
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
            operation_id: Uuid::new_v4().to_string(),
            state: "committed".to_string(),
            reason_code: "membership_changed".to_string(),
        }))
    }

    async fn create_counter(
        &self,
        request: Request<proto::CreateCounterRequest>,
    ) -> std::result::Result<Response<proto::CounterOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::CounterCreateRequest {
            object_id: request.object_id,
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            initial_value: request.initial_value,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::create_counter_outcome(
            self.state.as_ref(),
            &http_context,
            core_request,
        );
        let response = counter_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn read_counter(
        &self,
        request: Request<proto::ReadCounterRequest>,
    ) -> std::result::Result<Response<proto::CounterOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::CounterReadRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::read_counter_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = counter_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn increment_counter(
        &self,
        request: Request<proto::IncrementCounterRequest>,
    ) -> std::result::Result<Response<proto::CounterOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::CounterIncrementRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            delta: request.delta,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::increment_counter_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = counter_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn decrement_counter(
        &self,
        request: Request<proto::DecrementCounterRequest>,
    ) -> std::result::Result<Response<proto::CounterOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::CounterDecrementRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            delta: request.delta,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::decrement_counter_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = counter_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }
}

fn range_identity_from_proto(range: proto::CrdtRangeIdentity) -> RangeIdentity {
    RangeIdentity::new(
        range.cluster_id,
        range.table_id,
        range.range_id,
        range.has_lower_bound.then_some(range.lower_bound),
        range.has_upper_bound.then_some(range.upper_bound),
        range.schema_version,
        range.data_epoch,
    )
}

fn range_identity_to_proto(range: &RangeIdentity) -> proto::CrdtRangeIdentity {
    proto::CrdtRangeIdentity {
        cluster_id: range.cluster_id.as_str().to_owned(),
        table_id: range.table_id,
        range_id: range.range_id.as_str().to_owned(),
        lower_bound: range.lower_bound.clone().unwrap_or_default(),
        has_lower_bound: range.lower_bound.is_some(),
        upper_bound: range.upper_bound.clone().unwrap_or_default(),
        has_upper_bound: range.upper_bound.is_some(),
        schema_version: range.schema_version,
        data_epoch: range.data_epoch,
    }
}

fn counter_outcome_to_proto(outcome: &CrdtOutcome) -> proto::CounterOutcome {
    let common = outcome.common();
    let (has_value, initial_value, accepted_delta_total, value) = match outcome.value() {
        Some(CrdtValue::Counter {
            initial_value,
            accepted_delta_total,
            value,
            ..
        }) => (true, *initial_value, *accepted_delta_total, *value),
        _ => (false, 0, 0, 0),
    };
    proto::CounterOutcome {
        object_type: enum_wire(&common.object_type),
        object_id: common.object_id.clone(),
        range: Some(range_identity_to_proto(&common.range)),
        state_epoch: common.state_epoch,
        actor: common.actor.as_str().to_owned(),
        request_id: common.request_id.as_str().to_owned(),
        operation_id: common.operation_id.clone(),
        state: enum_wire(&common.state),
        failure_class: common
            .failure_class
            .as_ref()
            .map(enum_wire)
            .unwrap_or_default(),
        routing_kind: enum_wire(&common.routing.kind),
        routing_metadata_version: common.routing.metadata_version,
        routing_reason_code: common.routing.reason_code.clone(),
        retryable: common.retryable,
        original_operation_id: common.idempotency.operation_id.clone(),
        original_request_id: common.idempotency.request_id.as_str().to_owned(),
        first_outcome: common.idempotency.first_outcome.clone(),
        first_state: enum_wire(&common.idempotency.state),
        duplicate_count: common.idempotency.duplicate_count,
        has_value,
        initial_value,
        accepted_delta_total,
        value,
        value_unavailable: outcome.value_unavailable().unwrap_or_default().to_owned(),
    }
}

fn enum_wire(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .expect("CRDT enum serializes")
        .as_str()
        .expect("CRDT enum serializes as string")
        .to_owned()
}

fn crdt_status(code: &str, correlation_id: &str) -> Status {
    let code = match code {
        "UNAUTHENTICATED" => tonic::Code::Unauthenticated,
        "ABORTED" => tonic::Code::Aborted,
        "UNAVAILABLE" => tonic::Code::Unavailable,
        "DEADLINE_EXCEEDED" => tonic::Code::DeadlineExceeded,
        "INVALID_ARGUMENT" => tonic::Code::InvalidArgument,
        "UNIMPLEMENTED" => tonic::Code::Unimplemented,
        _ => tonic::Code::Internal,
    };
    Status::new(
        code,
        format!("CRDT request failed (correlation_id={correlation_id})"),
    )
}

fn cluster_json(
    cluster: &alopex_cluster::ClusterStatusSnapshot,
    correlation_id: &str,
) -> std::result::Result<String, Status> {
    serde_json::to_string(cluster).map_err(|err| {
        Status::internal(format!(
            "failed to serialize cluster status: {err} (correlation_id={correlation_id})"
        ))
    })
}

fn sql_value_to_proto(value: &alopex_sql::storage::SqlValue) -> proto::Value {
    use proto::value::Kind;
    let kind = match value {
        alopex_sql::storage::SqlValue::Null => None,
        alopex_sql::storage::SqlValue::Integer(v) => Some(Kind::IntValue(*v)),
        alopex_sql::storage::SqlValue::BigInt(v) => Some(Kind::BigintValue(*v)),
        alopex_sql::storage::SqlValue::Float(v) => Some(Kind::FloatValue(*v)),
        alopex_sql::storage::SqlValue::Double(v) => Some(Kind::DoubleValue(*v)),
        alopex_sql::storage::SqlValue::Text(v) => Some(Kind::TextValue(v.clone())),
        alopex_sql::storage::SqlValue::Blob(v) => Some(Kind::BlobValue(v.clone())),
        alopex_sql::storage::SqlValue::Boolean(v) => Some(Kind::BoolValue(*v)),
        alopex_sql::storage::SqlValue::Timestamp(v) => Some(Kind::TimestampValue(*v)),
        alopex_sql::storage::SqlValue::Vector(values) => Some(Kind::VectorValue(proto::Vector {
            values: values.clone(),
        })),
    };
    proto::Value { kind }
}

fn read_context<T>(request: &Request<T>) -> GrpcContext {
    request
        .extensions()
        .get::<GrpcContext>()
        .cloned()
        .unwrap_or_else(|| {
            let correlation_id = Uuid::new_v4().to_string();
            GrpcContext {
                correlation_id: correlation_id.clone(),
                actor: None,
                span: tracing::info_span!(
                    "grpc_request",
                    correlation_id = %correlation_id,
                    traceparent = ""
                ),
            }
        })
}

fn extract_correlation_id(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    metadata
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .or_else(|| {
            metadata
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string())
        })
}

fn extract_traceparent(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    metadata
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
}

fn map_status(err: ServerError, correlation_id: &str) -> Status {
    let code = match err.status_code() {
        axum::http::StatusCode::BAD_REQUEST => tonic::Code::InvalidArgument,
        axum::http::StatusCode::UNAUTHORIZED => tonic::Code::Unauthenticated,
        axum::http::StatusCode::NOT_FOUND => tonic::Code::NotFound,
        axum::http::StatusCode::CONFLICT => tonic::Code::Aborted,
        axum::http::StatusCode::REQUEST_TIMEOUT => tonic::Code::DeadlineExceeded,
        axum::http::StatusCode::PAYLOAD_TOO_LARGE => tonic::Code::ResourceExhausted,
        axum::http::StatusCode::NOT_IMPLEMENTED => tonic::Code::Unimplemented,
        axum::http::StatusCode::GONE => tonic::Code::NotFound,
        _ => tonic::Code::Internal,
    };
    let message = if correlation_id.is_empty() {
        err.to_string()
    } else {
        format!("{} (correlation_id={})", err, correlation_id)
    };
    Status::new(code, message)
}
