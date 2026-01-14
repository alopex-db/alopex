use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use alopex_core::kv::async_adapter::AsyncKVTransactionAdapter;
use alopex_sql::storage::AsyncSqlTransaction;
use futures::{future::BoxFuture, StreamExt};
use prost::Message;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{async_trait, Request, Response, Status};
use tower::{Layer, Service};
use uuid::Uuid;

use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::server::ServerState;
use crate::session::{SessionId, TxnHandle};
use crate::tls;

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
                acceptor.accept(stream).await.map_err(std::io::Error::other)
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

type AsyncTxn =
    alopex_sql::storage::async_storage::AsyncTxnBridge<'static, AsyncKVTransactionAdapter>;

enum StreamSource {
    Txn(AsyncTxn),
    Handle(TxnHandle),
}

#[async_trait]
impl AlopexService for AlopexServiceImpl {
    type ExecuteSqlStream = ReceiverStream<std::result::Result<proto::Row, Status>>;

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

        let (sender, receiver) = mpsc::channel(32);
        let sql = req.sql;
        let session_id = if req.session_id.is_empty() {
            None
        } else {
            Some(
                req.session_id
                    .parse::<SessionId>()
                    .map_err(|_| Status::invalid_argument("invalid session_id"))?,
            )
        };
        let state = self.state.clone();
        let correlation_id = ctx.correlation_id.clone();
        let span = ctx.span.clone();
        tokio::spawn(async move {
            let _enter = span.enter();
            let start = Instant::now();
            let deadline = start + state.config.query_timeout;
            let mut bytes_sent = 0usize;
            let mut success = true;
            let mut source = match session_id {
                Some(id) => match state.session_manager.get_transaction(&id).await {
                    Ok(handle) => StreamSource::Handle(handle),
                    Err(err) => {
                        let _ = sender.send(Err(map_status(err, &correlation_id))).await;
                        return;
                    }
                },
                None => match state.begin_sql_txn().await {
                    Ok(txn) => StreamSource::Txn(txn),
                    Err(err) => {
                        let _ = sender.send(Err(map_status(err, &correlation_id))).await;
                        return;
                    }
                },
            };

            let mut stream = match &mut source {
                StreamSource::Handle(handle) => handle.query(&sql),
                StreamSource::Txn(txn) => txn.async_query(&sql),
            };
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    let _ = sender
                        .send(Err(map_status(
                            ServerError::Timeout("query timeout".into()),
                            &correlation_id,
                        )))
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
                                    .send(Err(map_status(
                                        ServerError::Timeout("query timeout".into()),
                                        &correlation_id,
                                    )))
                                    .await;
                                success = false;
                                break;
                            }
                        };

                        match next {
                            Some(Ok(row)) => {
                                let proto_row = proto::Row {
                                    values: row.values.iter().map(sql_value_to_proto).collect(),
                                };
                                bytes_sent = bytes_sent.saturating_add(proto_row.encoded_len());
                                if bytes_sent > state.config.max_response_size {
                                    let _ = sender
                                        .send(Err(map_status(
                                            ServerError::PayloadTooLarge(
                                                "response size exceeds limit".into(),
                                            ),
                                            &correlation_id,
                                        )))
                                        .await;
                                    success = false;
                                    break;
                                }
                                match sender.try_send(Ok(proto_row)) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(item)) => {
                                        state.metrics.record_backpressure();
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
                                    .send(Err(map_status(
                                        ServerError::Sql(err.into()),
                                        &correlation_id,
                                    )))
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
            state.metrics.record_query(start.elapsed(), success);
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
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
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::invalid_argument("invalid session_id"));
                }
            };
            let fut = self
                .state
                .session_manager
                .execute_in_session(&session_id, &req.sql);
            let exec_result = match tokio::time::timeout(self.state.config.query_timeout, fut).await
            {
                Ok(result) => result.map_err(|err| map_status(err, &ctx.correlation_id)),
                Err(_) => Err(Status::deadline_exceeded("query timeout")),
            };
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(err);
                }
            }
        } else {
            let mut txn = match self.state.begin_sql_txn().await {
                Ok(txn) => txn,
                Err(err) => {
                    let status = map_status(err, &ctx.correlation_id);
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(status);
                }
            };
            let exec_result =
                tokio::time::timeout(self.state.config.query_timeout, txn.async_execute(&req.sql))
                    .await;
            let exec_result = match exec_result {
                Ok(result) => match result {
                    Ok(result) => {
                        if let Err(err) = txn.async_commit().await {
                            let status =
                                map_status(ServerError::Sql(err.into()), &ctx.correlation_id);
                            self.state.metrics.record_query(start.elapsed(), false);
                            return Err(status);
                        }
                        result
                    }
                    Err(err) => {
                        let _ = txn.async_rollback().await;
                        let status = map_status(ServerError::Sql(err.into()), &ctx.correlation_id);
                        self.state.metrics.record_query(start.elapsed(), false);
                        return Err(status);
                    }
                },
                Err(_) => {
                    let _ = txn.async_rollback().await;
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::deadline_exceeded("query timeout"));
                }
            };
            exec_result
        };

        if self.state.config.audit_log_enabled {
            self.state
                .audit
                .log_ddl(&req.sql, ctx.actor.as_deref(), &ctx.correlation_id);
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
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::invalid_argument("invalid session_id"));
                }
            };
            let fut = self
                .state
                .session_manager
                .execute_in_session(&session_id, &req.sql);
            let exec_result = match tokio::time::timeout(self.state.config.query_timeout, fut).await
            {
                Ok(result) => result.map_err(|err| map_status(err, &ctx.correlation_id)),
                Err(_) => Err(Status::deadline_exceeded("query timeout")),
            };
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(err);
                }
            }
        } else {
            let mut txn = match self.state.begin_sql_txn().await {
                Ok(txn) => txn,
                Err(err) => {
                    let status = map_status(err, &ctx.correlation_id);
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(status);
                }
            };
            let exec_result =
                tokio::time::timeout(self.state.config.query_timeout, txn.async_execute(&req.sql))
                    .await;
            let exec_result = match exec_result {
                Ok(result) => match result {
                    Ok(result) => {
                        if let Err(err) = txn.async_commit().await {
                            let status =
                                map_status(ServerError::Sql(err.into()), &ctx.correlation_id);
                            self.state.metrics.record_query(start.elapsed(), false);
                            return Err(status);
                        }
                        result
                    }
                    Err(err) => {
                        let _ = txn.async_rollback().await;
                        let status = map_status(ServerError::Sql(err.into()), &ctx.correlation_id);
                        self.state.metrics.record_query(start.elapsed(), false);
                        return Err(status);
                    }
                },
                Err(_) => {
                    let _ = txn.async_rollback().await;
                    self.state.metrics.record_query(start.elapsed(), false);
                    return Err(Status::deadline_exceeded("query timeout"));
                }
            };
            exec_result
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
        self.state
            .session_manager
            .commit(&session_id)
            .await
            .map_err(|err| map_status(err, &ctx.correlation_id))?;
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
        self.state
            .session_manager
            .rollback(&session_id)
            .await
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
