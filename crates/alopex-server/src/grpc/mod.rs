use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use alopex_sql::{
    AlopexDialect, CommitMetadata, ExecutionResult, ExecutionStep, ExecutionStepError,
    ExecutionStepErrorKind, ExecutionStepKind, ExecutionStepOutcome, ExecutionStepResult, Parser,
    SharedExecutionReport, SharedExecutionRequest,
};
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

// tonic/async-trait emits `must_use` on generated async methods whose Future
// return type already carries the same contract.
#[allow(clippy::double_must_use)]
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

    async fn execute_shared(
        &self,
        request: Request<proto::SharedExecutionRequest>,
    ) -> std::result::Result<Response<proto::SharedExecutionReport>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let deadline = grpc_timeout(request.metadata()).map(|duration| {
            Instant::now()
                + duration
                    .checked_sub(Duration::from_millis(1))
                    .unwrap_or(Duration::ZERO)
        });
        let shared = shared_request_from_proto(request.into_inner())?;
        let report =
            execute_shared_request(self.state.clone(), shared, &ctx.correlation_id, deadline).await;
        let report = shared_report_to_proto(report);
        let encoded_len = report.encoded_len();
        MemoryControlPolicy::from_env()
            .enforce_output_bytes(encoded_len as u64)
            .map_err(|error| map_status(error, &ctx.correlation_id))?;
        if encoded_len > self.state.config.max_response_size {
            return Err(map_status(
                ServerError::PayloadTooLarge("response size exceeds limit".into()),
                &ctx.correlation_id,
            ));
        }
        Ok(Response::new(report))
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
        }))
    }
}

struct SharedSessionGuard {
    state: Arc<ServerState>,
    session_id: Option<SessionId>,
}

impl SharedSessionGuard {
    fn new(state: Arc<ServerState>) -> Self {
        Self {
            state,
            session_id: None,
        }
    }

    fn arm(&mut self, session_id: SessionId) {
        self.session_id = Some(session_id);
    }

    fn disarm(&mut self) {
        self.session_id = None;
    }

    async fn rollback(&mut self) {
        let Some(session_id) = self.session_id.take() else {
            return;
        };
        if let Ok(effects) = self.state.session_manager.rollback(&session_id).await {
            let _ = self.state.apply_catalog_rollback_effects(effects);
        }
    }
}

impl Drop for SharedSessionGuard {
    fn drop(&mut self) {
        let Some(session_id) = self.session_id.take() else {
            return;
        };
        let state = self.state.clone();
        tokio::spawn(async move {
            if let Ok(effects) = state.session_manager.rollback(&session_id).await {
                let _ = state.apply_catalog_rollback_effects(effects);
            }
        });
    }
}

fn shared_request_from_proto(
    request: proto::SharedExecutionRequest,
) -> std::result::Result<SharedExecutionRequest, Status> {
    use proto::shared_execution_step::Kind;

    if request.execution_id.is_empty() {
        return Err(Status::invalid_argument("execution_id must not be empty"));
    }
    if request.transaction_id.is_empty() {
        return Err(Status::invalid_argument("transaction_id must not be empty"));
    }
    if request.steps.is_empty() {
        return Err(Status::invalid_argument("steps must not be empty"));
    }
    let mut step_ids = std::collections::HashSet::with_capacity(request.steps.len());
    let mut steps = Vec::with_capacity(request.steps.len());
    for step in request.steps {
        if step.step_id.is_empty() {
            return Err(Status::invalid_argument("step_id must not be empty"));
        }
        if !step_ids.insert(step.step_id.clone()) {
            return Err(Status::invalid_argument("step_id must be unique"));
        }
        let kind = match step.kind {
            Some(Kind::TransactionStatement(sql)) => {
                ExecutionStepKind::TransactionStatement { sql }
            }
            Some(Kind::CommitBarrier(_)) => ExecutionStepKind::CommitBarrier,
            Some(Kind::PostCommitRead(sql)) => ExecutionStepKind::PostCommitRead { sql },
            None => return Err(Status::invalid_argument("step kind must be set")),
        };
        steps.push(ExecutionStep::new(step.step_id, kind));
    }
    Ok(SharedExecutionRequest::new(
        request.execution_id,
        request.transaction_id,
        steps,
    ))
}

async fn execute_shared_request(
    state: Arc<ServerState>,
    request: SharedExecutionRequest,
    correlation_id: &str,
    deadline: Option<Instant>,
) -> SharedExecutionReport {
    let SharedExecutionRequest {
        execution_id,
        transaction_id,
        steps,
    } = request;
    let mut results = Vec::with_capacity(steps.len());
    let mut committed = false;
    let mut session = SharedSessionGuard::new(state.clone());

    for (step_index, step) in steps.into_iter().enumerate() {
        let outcome = match step.kind {
            ExecutionStepKind::TransactionStatement { .. } if committed => shared_step_error(
                ExecutionStepErrorKind::InvalidOrder,
                "transaction statement follows the commit barrier",
            ),
            ExecutionStepKind::TransactionStatement { sql } => {
                match validate_shared_statement(&sql, false) {
                    Err(error) => shared_step_error(ExecutionStepErrorKind::Transaction, error),
                    Ok(is_query) => {
                        if !is_query {
                            if let Err(error) = state.lifecycle_state.check_write_allowed() {
                                let outcome =
                                    shared_step_error(ExecutionStepErrorKind::Transaction, error);
                                results.push(shared_step_result(
                                    &execution_id,
                                    &transaction_id,
                                    step.step_id,
                                    step_index,
                                    outcome,
                                ));
                                break;
                            }
                        }
                        if session.session_id.is_none() {
                            match begin_shared_session(&state).await {
                                Ok(session_id) => session.arm(session_id),
                                Err(error) => {
                                    let outcome = shared_step_error(
                                        ExecutionStepErrorKind::Transaction,
                                        error,
                                    );
                                    results.push(shared_step_result(
                                        &execution_id,
                                        &transaction_id,
                                        step.step_id,
                                        step_index,
                                        outcome,
                                    ));
                                    break;
                                }
                            }
                        }
                        let timeout = shared_step_timeout(deadline, state.config.query_timeout);
                        match timeout {
                            Err(message) => {
                                shared_step_error(ExecutionStepErrorKind::Transaction, message)
                            }
                            Ok(timeout) => {
                                let session_id = session
                                    .session_id
                                    .as_ref()
                                    .expect("shared session was opened");
                                match execute_session_statement_with_routing(
                                    &state,
                                    session_id,
                                    &sql,
                                    correlation_id,
                                    timeout,
                                )
                                .await
                                {
                                    Ok((result, _)) => ExecutionStepOutcome::Execution(result),
                                    Err(error) => shared_step_error(
                                        ExecutionStepErrorKind::Transaction,
                                        error,
                                    ),
                                }
                            }
                        }
                    }
                }
            }
            ExecutionStepKind::CommitBarrier if committed => shared_step_error(
                ExecutionStepErrorKind::InvalidOrder,
                "commit barrier follows a successful commit barrier",
            ),
            ExecutionStepKind::CommitBarrier => {
                let Some(session_id) = session.session_id.as_ref() else {
                    let outcome = shared_step_error(
                        ExecutionStepErrorKind::Commit,
                        "commit barrier requires a transaction statement",
                    );
                    results.push(shared_step_result(
                        &execution_id,
                        &transaction_id,
                        step.step_id,
                        step_index,
                        outcome,
                    ));
                    break;
                };
                match shared_step_timeout(deadline, state.config.query_timeout) {
                    Err(message) => shared_step_error(ExecutionStepErrorKind::Commit, message),
                    Ok(timeout) => {
                        let commit =
                            tokio::time::timeout(timeout, state.session_manager.commit(session_id))
                                .await;
                        match commit {
                            Err(_) => shared_step_error(
                                ExecutionStepErrorKind::Commit,
                                "deadline exceeded during commit",
                            ),
                            Ok(Err(error)) => {
                                shared_step_error(ExecutionStepErrorKind::Commit, error)
                            }
                            Ok(Ok(effects)) => {
                                session.disarm();
                                let publish = if effects.is_empty() {
                                    Ok(())
                                } else {
                                    state
                                        .apply_table_lifecycle_effects(effects)
                                        .and_then(|()| sync_catalog_to_store(&state))
                                };
                                match publish {
                                    Ok(()) => {
                                        committed = true;
                                        ExecutionStepOutcome::Commit(CommitMetadata {
                                            transaction_id: transaction_id.clone(),
                                        })
                                    }
                                    Err(error) => {
                                        shared_step_error(ExecutionStepErrorKind::Commit, error)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            ExecutionStepKind::PostCommitRead { .. } if !committed => shared_step_error(
                ExecutionStepErrorKind::InvalidOrder,
                "post-commit read precedes a successful commit barrier",
            ),
            ExecutionStepKind::PostCommitRead { sql } => {
                match validate_shared_statement(&sql, true) {
                    Err(error) => shared_step_error(ExecutionStepErrorKind::PostCommitRead, error),
                    Ok(_) => match shared_step_timeout(deadline, state.config.query_timeout) {
                        Err(message) => {
                            shared_step_error(ExecutionStepErrorKind::PostCommitRead, message)
                        }
                        Ok(timeout) => {
                            match execute_non_session_statement_with_routing(
                                &state,
                                &sql,
                                correlation_id,
                                timeout,
                            )
                            .await
                            {
                                Ok((result, _)) => ExecutionStepOutcome::Execution(result),
                                Err(error) => {
                                    shared_step_error(ExecutionStepErrorKind::PostCommitRead, error)
                                }
                            }
                        }
                    },
                }
            }
        };
        let failed = matches!(outcome, ExecutionStepOutcome::Error(_));
        results.push(shared_step_result(
            &execution_id,
            &transaction_id,
            step.step_id,
            step_index,
            outcome,
        ));
        if failed {
            session.rollback().await;
            break;
        }
    }

    session.rollback().await;
    SharedExecutionReport {
        execution_id,
        transaction_id,
        steps: results,
    }
}

async fn begin_shared_session(state: &ServerState) -> Result<SessionId> {
    let session_id = state.session_manager.create_session().await?;
    state.session_manager.begin_transaction(&session_id).await?;
    Ok(session_id)
}

fn validate_shared_statement(sql: &str, require_query: bool) -> std::result::Result<bool, String> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).map_err(|error| error.to_string())?;
    if statements.len() != 1 {
        return Err("shared execution steps require exactly one SQL statement".into());
    }
    if require_query && statements[0].kind.requires_write() {
        return Err("post-commit read requires a query statement".into());
    }
    Ok(!statements[0].kind.requires_write())
}

fn shared_step_timeout(
    deadline: Option<Instant>,
    configured: Duration,
) -> std::result::Result<Duration, &'static str> {
    let Some(deadline) = deadline else {
        return Ok(configured);
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err("gRPC deadline exceeded before step execution")
    } else {
        Ok(configured.min(remaining))
    }
}

fn shared_step_error(
    kind: ExecutionStepErrorKind,
    error: impl std::fmt::Display,
) -> ExecutionStepOutcome {
    ExecutionStepOutcome::Error(ExecutionStepError {
        kind,
        message: error.to_string(),
    })
}

fn shared_step_result(
    execution_id: &str,
    transaction_id: &str,
    step_id: String,
    step_index: usize,
    outcome: ExecutionStepOutcome,
) -> ExecutionStepResult {
    ExecutionStepResult {
        execution_id: execution_id.into(),
        transaction_id: transaction_id.into(),
        step_id,
        step_index,
        outcome,
    }
}

fn shared_report_to_proto(report: SharedExecutionReport) -> proto::SharedExecutionReport {
    proto::SharedExecutionReport {
        execution_id: report.execution_id,
        transaction_id: report.transaction_id,
        steps: report
            .steps
            .into_iter()
            .map(|step| {
                let outcome = match step.outcome {
                    ExecutionStepOutcome::Execution(result) => {
                        proto::shared_execution_step_result::Outcome::Execution(
                            execution_result_to_proto(result),
                        )
                    }
                    ExecutionStepOutcome::Commit(metadata) => {
                        proto::shared_execution_step_result::Outcome::Commit(
                            proto::SharedCommitMetadata {
                                transaction_id: metadata.transaction_id,
                            },
                        )
                    }
                    ExecutionStepOutcome::Error(error) => {
                        proto::shared_execution_step_result::Outcome::Error(
                            proto::SharedExecutionStepError {
                                kind: shared_error_kind_to_proto(error.kind) as i32,
                                message: error.message,
                            },
                        )
                    }
                };
                proto::SharedExecutionStepResult {
                    execution_id: step.execution_id,
                    transaction_id: step.transaction_id,
                    step_id: step.step_id,
                    step_index: step.step_index as u64,
                    outcome: Some(outcome),
                }
            })
            .collect(),
    }
}

fn shared_error_kind_to_proto(kind: ExecutionStepErrorKind) -> proto::SharedExecutionStepErrorKind {
    match kind {
        ExecutionStepErrorKind::Transaction => proto::SharedExecutionStepErrorKind::Transaction,
        ExecutionStepErrorKind::Commit => proto::SharedExecutionStepErrorKind::Commit,
        ExecutionStepErrorKind::PostCommitRead => {
            proto::SharedExecutionStepErrorKind::PostCommitRead
        }
        ExecutionStepErrorKind::InvalidOrder => proto::SharedExecutionStepErrorKind::InvalidOrder,
    }
}

fn execution_result_to_proto(result: ExecutionResult) -> proto::SqlResultSet {
    match result {
        ExecutionResult::Success => proto::SqlResultSet {
            success: true,
            ..Default::default()
        },
        ExecutionResult::RowsAffected(affected_rows) => proto::SqlResultSet {
            affected_rows,
            has_affected_rows: true,
            ..Default::default()
        },
        ExecutionResult::Query(result) => proto::SqlResultSet {
            columns: result
                .columns
                .into_iter()
                .map(|column| proto::SqlColumn {
                    name: column.name,
                    data_type: column.data_type.to_string(),
                })
                .collect(),
            rows: result
                .rows
                .into_iter()
                .map(|row| proto::Row {
                    values: row.iter().map(sql_value_to_proto).collect(),
                })
                .collect(),
            ..Default::default()
        },
    }
}

fn grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    let amount = digits.parse::<u64>().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(amount.saturating_mul(60 * 60))),
        "M" => Some(Duration::from_secs(amount.saturating_mul(60))),
        "S" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_millis(amount)),
        "u" => Some(Duration::from_micros(amount)),
        "n" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
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
        alopex_sql::storage::SqlValue::Date(v) => Some(Kind::DateValue(*v)),
        alopex_sql::storage::SqlValue::Time(v) => Some(Kind::TimeValue(*v)),
        alopex_sql::storage::SqlValue::Interval {
            months,
            days,
            micros,
        } => Some(Kind::IntervalValue(proto::Interval {
            months: *months,
            days: *days,
            microseconds: *micros,
        })),
        alopex_sql::storage::SqlValue::Decimal(value) => Some(Kind::DecimalValue(proto::Decimal {
            coefficient: value.coefficient.to_be_bytes().to_vec(),
            scale: u32::from(value.scale),
        })),
        alopex_sql::storage::SqlValue::Json(value) => Some(Kind::JsonValue(value.to_string())),
        alopex_sql::storage::SqlValue::Vector(values) => Some(Kind::VectorValue(proto::Vector {
            values: values.clone(),
        })),
        value @ alopex_sql::storage::SqlValue::Array(_) => Some(Kind::ArrayJsonValue(
            value.nested_json_text().expect("ARRAY has a JSON mapping"),
        )),
        value @ alopex_sql::storage::SqlValue::Map(_) => Some(Kind::MapJsonValue(
            value.nested_json_text().expect("MAP has a JSON mapping"),
        )),
        value @ alopex_sql::storage::SqlValue::Struct(_) => Some(Kind::StructJsonValue(
            value.nested_json_text().expect("STRUCT has a JSON mapping"),
        )),
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

#[cfg(test)]
mod temporal_wire_tests {
    use super::{grpc_timeout, proto, shared_step_timeout, sql_value_to_proto};
    use alopex_sql::{storage::DecimalValue, SqlValue};
    use prost::Message;
    use std::time::{Duration, Instant};
    use tonic::Request;

    #[test]
    fn shared_execution_honors_standard_grpc_timeout_budget() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(25));
        assert_eq!(
            grpc_timeout(request.metadata()),
            Some(Duration::from_millis(25))
        );
        let remaining = shared_step_timeout(
            Some(Instant::now() + Duration::from_millis(10)),
            Duration::from_secs(1),
        )
        .unwrap()
        .as_millis();
        assert!((9..=10).contains(&remaining));
        assert!(shared_step_timeout(
            Some(Instant::now() - Duration::from_millis(1)),
            Duration::from_secs(1),
        )
        .is_err());
    }

    #[test]
    fn temporal_values_use_appended_wire_variants() {
        let date = sql_value_to_proto(&SqlValue::Date(19_782));
        let time = sql_value_to_proto(&SqlValue::Time(3));
        let value = sql_value_to_proto(&SqlValue::Interval {
            months: 1,
            days: -2,
            micros: 3,
        });
        for value in [&date, &time, &value] {
            assert_eq!(
                proto::Value::decode(value.encode_to_vec().as_slice()).unwrap(),
                value.clone()
            );
        }
        assert!(matches!(
            date.kind,
            Some(proto::value::Kind::DateValue(19_782))
        ));
        assert!(matches!(time.kind, Some(proto::value::Kind::TimeValue(3))));
        let Some(proto::value::Kind::IntervalValue(interval)) = value.kind else {
            panic!("expected interval wire value");
        };
        assert_eq!(
            (interval.months, interval.days, interval.microseconds),
            (1, -2, 3)
        );
    }

    #[test]
    fn decimal_value_uses_appended_wire_variant() {
        let value = sql_value_to_proto(&SqlValue::Decimal(DecimalValue::new(-12345, 2)));
        assert_eq!(
            proto::Value::decode(value.encode_to_vec().as_slice()).unwrap(),
            value
        );
        let Some(proto::value::Kind::DecimalValue(decimal)) = value.kind else {
            panic!("expected decimal wire value");
        };
        assert_eq!(decimal.coefficient, (-12345_i128).to_be_bytes());
        assert_eq!(decimal.scale, 2);
    }

    #[test]
    fn json_value_uses_appended_wire_variant() {
        let value = sql_value_to_proto(&SqlValue::Json(
            alopex_sql::storage::JsonValue::parse(r#"{"b":1,"a":2}"#).unwrap(),
        ));
        assert_eq!(
            proto::Value::decode(value.encode_to_vec().as_slice()).unwrap(),
            value
        );
        assert!(matches!(
            value.kind,
            Some(proto::value::Kind::JsonValue(ref json)) if json == r#"{"a":2,"b":1}"#
        ));
    }

    #[test]
    fn nested_values_use_appended_json_wire_variants() {
        let values = [
            (
                SqlValue::Array(vec![SqlValue::Integer(1), SqlValue::Null]),
                r#"[1,null]"#,
                "array",
            ),
            (
                SqlValue::Map(vec![(SqlValue::Text("a".into()), SqlValue::Integer(1))]),
                r#"{"a":1}"#,
                "map",
            ),
            (
                SqlValue::Struct(vec![("name".into(), SqlValue::Text("Ada".into()))]),
                r#"{"name":"Ada"}"#,
                "struct",
            ),
        ];
        for (input, expected, kind) in values {
            let value = sql_value_to_proto(&input);
            assert_eq!(
                proto::Value::decode(value.encode_to_vec().as_slice()).unwrap(),
                value
            );
            let actual = match value.kind {
                Some(proto::value::Kind::ArrayJsonValue(value)) if kind == "array" => value,
                Some(proto::value::Kind::MapJsonValue(value)) if kind == "map" => value,
                Some(proto::value::Kind::StructJsonValue(value)) if kind == "struct" => value,
                other => panic!("unexpected {kind} wire value: {other:?}"),
            };
            assert_eq!(actual, expected);
        }
    }
}
