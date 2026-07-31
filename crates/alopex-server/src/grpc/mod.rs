use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{future::BoxFuture, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, OwnedMutexGuard};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::{async_trait, Request, Response, Status};
use tower::{Layer, Service};
use uuid::Uuid;

use alopex_cluster::crdt::{CrdtOutcome, CrdtValue};
use alopex_cluster::{FailureClass, OperationState, RangeIdentity, RequestId, RoutingOutcomeKind};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;

use crate::error::{Result, ServerError};
use crate::http::sql::{
    execute_non_session_statement_with_routing, execute_session_statement_with_routing,
    sync_catalog_to_store,
};
use crate::http::{
    transaction_failure_outcome, transaction_metadata_version, transaction_request_id,
    HttpTransactionOutcome,
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

mod changefeed;

use proto::alopex_service_server::{AlopexService, AlopexServiceServer};

#[derive(Clone)]
struct GrpcContext {
    correlation_id: String,
    actor: Option<String>,
    span: tracing::Span,
}

// A gRPC retry may arrive on a fresh connection or after a server restart,
// while local sessions deliberately remain process-local. Keep a durable
// reservation before each explicit retry identity reaches a side effect. A
// completed terminal response can be replayed after restart; a response that
// still requires a live local transaction is converted to a retained
// tombstone so it can never create or apply the operation again.
const GRPC_IDEMPOTENCY_PREFIX: &[u8] = b"__alopex_internal/grpc-transaction-idempotency/v1/";
static GRPC_IDEMPOTENCY_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredGrpcResponse {
    messages: Vec<Vec<u8>>,
    #[serde(default)]
    status: Option<StoredGrpcStatus>,
    requires_active_transaction: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredGrpcStatus {
    code: i32,
    message: String,
    details: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GrpcLedgerRecord {
    request_id: String,
    operation: String,
    fingerprint: String,
    transaction_id: Option<String>,
    duplicate_count: u64,
    /// A missing response is a durable tombstone. It prevents a retry from
    /// re-running a request when interruption occurred after reservation.
    response: Option<StoredGrpcResponse>,
}

enum GrpcLedgerClaim {
    Execute(GrpcLedgerReservation),
    Replay(StoredGrpcResponse, u64),
    Expired(Option<String>),
    Conflict,
}

struct GrpcLedgerReservation {
    key: Vec<u8>,
    request_id: RequestId,
    operation: String,
    fingerprint: String,
    _guard: OwnedMutexGuard<()>,
}

fn grpc_idempotency_lock() -> Arc<Mutex<()>> {
    GRPC_IDEMPOTENCY_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

fn grpc_ledger_key(operation: &str, request_id: &RequestId) -> Vec<u8> {
    let digest = Sha256::digest(format!("{operation}\n{}", request_id.as_str()).as_bytes());
    let mut key = GRPC_IDEMPOTENCY_PREFIX.to_vec();
    key.extend_from_slice(format!("{digest:x}").as_bytes());
    key
}

fn grpc_request_fingerprint(
    ctx: &GrpcContext,
    operation: &str,
    transaction_id: &str,
    payload: &[u8],
) -> String {
    let mut canonical = Vec::new();
    grpc_fingerprint_field(
        &mut canonical,
        "actor_present",
        if ctx.actor.is_some() {
            b"true"
        } else {
            b"false"
        },
    );
    grpc_fingerprint_field(
        &mut canonical,
        "actor",
        ctx.actor.as_deref().unwrap_or_default().as_bytes(),
    );
    grpc_fingerprint_field(&mut canonical, "operation", operation.as_bytes());
    grpc_fingerprint_field(&mut canonical, "target", transaction_id.as_bytes());
    grpc_fingerprint_field(&mut canonical, "payload", payload);
    format!("{:x}", Sha256::digest(canonical))
}

/// Canonically encodes externally supplied fields without delimiter ambiguity.
/// Field order is part of the operation contract; each name and value carries
/// its byte length so arbitrary SQL, identifiers, and map keys cannot collide.
fn grpc_canonical_payload(fields: &[(&str, String)]) -> Vec<u8> {
    let mut payload = Vec::new();
    for (name, value) in fields {
        grpc_fingerprint_field(&mut payload, name, value.as_bytes());
    }
    payload
}

fn grpc_fingerprint_field(output: &mut Vec<u8>, name: &str, value: &[u8]) {
    output.extend_from_slice(&(name.len() as u64).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn grpc_string_map_fingerprint(options: &std::collections::HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = options.iter().collect();
    pairs.sort_unstable_by_key(|(key, _)| *key);
    let fields = pairs
        .into_iter()
        .map(|(key, value)| (key.as_str(), value.clone()))
        .collect::<Vec<_>>();
    grpc_canonical_payload(&fields)
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn grpc_f32_values_fingerprint(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| format!("{:08x}", value.to_bits()))
        .collect()
}

fn read_grpc_ledger(state: &ServerState, key: &[u8]) -> Result<Option<GrpcLedgerRecord>> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let value = txn.get(&key.to_vec())?;
    txn.rollback_self()?;
    value
        .map(|value| {
            serde_json::from_slice(&value).map_err(|error| {
                ServerError::Internal(format!("invalid gRPC idempotency ledger: {error}"))
            })
        })
        .transpose()
}

fn write_grpc_ledger(state: &ServerState, key: Vec<u8>, record: &GrpcLedgerRecord) -> Result<()> {
    let value = serde_json::to_vec(record).map_err(|error| {
        ServerError::Internal(format!("encode gRPC idempotency ledger: {error}"))
    })?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    txn.put(key, value)?;
    txn.commit_self()?;
    Ok(())
}

async fn grpc_response_is_live(state: &ServerState, transaction_id: Option<&str>) -> bool {
    let Some(transaction_id) = transaction_id else {
        return false;
    };
    let Ok(session_id) = transaction_id.parse::<SessionId>() else {
        return false;
    };
    matches!(
        state.session_manager.get_session(&session_id).await,
        Ok(snapshot) if snapshot.has_transaction
    )
}

async fn claim_grpc_request(
    state: &ServerState,
    request_id: RequestId,
    operation: &str,
    fingerprint: String,
) -> Result<GrpcLedgerClaim> {
    let guard = grpc_idempotency_lock().lock_owned().await;
    let key = grpc_ledger_key(operation, &request_id);
    let Some(mut record) = read_grpc_ledger(state, &key)? else {
        write_grpc_ledger(
            state,
            key.clone(),
            &GrpcLedgerRecord {
                request_id: request_id.as_str().to_owned(),
                operation: operation.to_owned(),
                fingerprint: fingerprint.clone(),
                transaction_id: None,
                duplicate_count: 0,
                response: None,
            },
        )?;
        return Ok(GrpcLedgerClaim::Execute(GrpcLedgerReservation {
            key,
            request_id,
            operation: operation.to_owned(),
            fingerprint,
            _guard: guard,
        }));
    };

    if record.request_id != request_id.as_str()
        || record.operation != operation
        || record.fingerprint != fingerprint
    {
        return Ok(GrpcLedgerClaim::Conflict);
    }

    let Some(response) = record.response.clone() else {
        return Ok(GrpcLedgerClaim::Expired(record.transaction_id));
    };
    if response.requires_active_transaction
        && !grpc_response_is_live(state, record.transaction_id.as_deref()).await
    {
        record.response = None;
        let transaction_id = record.transaction_id.clone();
        write_grpc_ledger(state, key, &record)?;
        return Ok(GrpcLedgerClaim::Expired(transaction_id));
    }
    record.duplicate_count = record
        .duplicate_count
        .checked_add(1)
        .ok_or_else(|| ServerError::Internal("gRPC idempotency duplicate overflow".into()))?;
    let duplicate_count = record.duplicate_count;
    write_grpc_ledger(state, key, &record)?;
    Ok(GrpcLedgerClaim::Replay(response, duplicate_count))
}

impl GrpcLedgerReservation {
    fn complete(
        self,
        state: &ServerState,
        transaction_id: Option<String>,
        response: StoredGrpcResponse,
    ) -> Result<()> {
        write_grpc_ledger(
            state,
            self.key,
            &GrpcLedgerRecord {
                request_id: self.request_id.as_str().to_owned(),
                operation: self.operation,
                fingerprint: self.fingerprint,
                transaction_id,
                duplicate_count: 0,
                response: Some(response),
            },
        )
    }
}

fn stored_grpc_message<M: Message>(
    message: &M,
    _requires_active_transaction: bool,
) -> StoredGrpcResponse {
    StoredGrpcResponse {
        messages: vec![message.encode_to_vec()],
        status: None,
        requires_active_transaction: _requires_active_transaction,
    }
}

fn stored_grpc_status(status: &Status, _requires_active_transaction: bool) -> StoredGrpcResponse {
    StoredGrpcResponse {
        messages: Vec::new(),
        status: Some(StoredGrpcStatus {
            code: status.code() as i32,
            message: status.message().to_owned(),
            details: status.details().to_vec(),
        }),
        requires_active_transaction: _requires_active_transaction,
    }
}

fn replay_grpc_status(
    response: &StoredGrpcResponse,
    duplicate_count: u64,
) -> Result<Option<Status>> {
    let Some(stored) = &response.status else {
        return Ok(None);
    };
    let mut details = stored.details.clone();
    if let Ok(mut outcome) = proto::TransactionOutcomeV09::decode(details.as_slice()) {
        if let Some(idempotency) = outcome.idempotency.as_mut() {
            idempotency.duplicate_count = duplicate_count;
        }
        details = outcome.encode_to_vec();
    }
    Ok(Some(Status::with_details(
        tonic::Code::from_i32(stored.code),
        stored.message.clone(),
        details.into(),
    )))
}

fn complete_grpc_error(
    reservation: &mut Option<GrpcLedgerReservation>,
    state: &ServerState,
    transaction_id: String,
    status: &Status,
    requires_active_transaction: bool,
) -> Result<()> {
    if let Some(reservation) = reservation.take() {
        reservation.complete(
            state,
            Some(transaction_id),
            stored_grpc_status(status, requires_active_transaction),
        )?;
    }
    Ok(())
}

fn grpc_idempotency_persistence_pending_status(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    correlation_id: &str,
) -> Status {
    let outcome = HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::RecoveryPending,
        Some(FailureClass::Internal),
        Some("idempotency_persistence_failed".to_owned()),
        RoutingOutcomeKind::Unavailable,
        "idempotency_persistence_failed",
        false,
    );
    let status = map_status(
        ServerError::Internal("gRPC idempotency outcome could not be durably stored".into()),
        correlation_id,
    );
    Status::with_details(
        status.code(),
        status.message().to_owned(),
        transaction_outcome_to_proto(&outcome)
            .encode_to_vec()
            .into(),
    )
}

macro_rules! complete_grpc_error_or_recovery_pending {
    ($reservation:expr, $state:expr, $transaction_id:expr, $request_id:expr, $status:expr, $correlation_id:expr, $requires_active_transaction:expr $(,)?) => {{
        if complete_grpc_error(
            $reservation,
            $state,
            $transaction_id.clone(),
            $status,
            $requires_active_transaction,
        )
        .is_err()
        {
            return Err(grpc_idempotency_persistence_pending_status(
                $state,
                $transaction_id.clone(),
                $request_id.clone(),
                $correlation_id,
            ));
        }
    }};
}

macro_rules! complete_grpc_response_or_recovery_pending {
    ($reservation:expr, $state:expr, $transaction_id:expr, $request_id:expr, $stored:expr, $correlation_id:expr $(,)?) => {{
        if let Some(reservation) = $reservation {
            if reservation
                .complete($state, Some($transaction_id.clone()), $stored)
                .is_err()
            {
                return Err(grpc_idempotency_persistence_pending_status(
                    $state,
                    $transaction_id.clone(),
                    $request_id.clone(),
                    $correlation_id,
                ));
            }
        }
    }};
}

fn decode_grpc_message<M: Message + Default>(response: &StoredGrpcResponse) -> Result<M> {
    let Some(message) = response.messages.first() else {
        return Err(ServerError::Internal(
            "stored gRPC idempotency response is empty".into(),
        ));
    };
    M::decode(message.as_slice()).map_err(|error| {
        ServerError::Internal(format!("decode stored gRPC idempotency response: {error}"))
    })
}

fn decode_grpc_messages<M: Message + Default>(response: &StoredGrpcResponse) -> Result<Vec<M>> {
    response
        .messages
        .iter()
        .map(|message| {
            M::decode(message.as_slice()).map_err(|error| {
                ServerError::Internal(format!("decode stored gRPC idempotency response: {error}"))
            })
        })
        .collect()
}

fn set_transaction_duplicate_count(
    transaction: &mut Option<proto::TransactionOutcomeV09>,
    duplicate_count: u64,
) {
    if let Some(idempotency) = transaction
        .as_mut()
        .and_then(|outcome| outcome.idempotency.as_mut())
    {
        idempotency.duplicate_count = duplicate_count;
    }
}

async fn claim_explicit_grpc_request(
    state: &ServerState,
    ctx: &GrpcContext,
    transaction_id: &str,
    request_id: &RequestId,
    supplied_request_id: bool,
    operation: &str,
    payload: Vec<u8>,
) -> std::result::Result<Option<GrpcLedgerClaim>, Status> {
    if !supplied_request_id {
        return Ok(None);
    }
    let fingerprint = grpc_request_fingerprint(ctx, operation, transaction_id, &payload);
    claim_grpc_request(state, request_id.clone(), operation, fingerprint)
        .await
        .map(Some)
        .map_err(|error| {
            grpc_transaction_status(
                state,
                transaction_id.to_owned(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            )
        })
}

macro_rules! claim_grpc_typed_response {
    ($state:expr, $ctx:expr, $transaction_id:expr, $request_id:expr, $supplied:expr, $operation:expr, $payload:expr, $response_type:ty) => {{
        match claim_explicit_grpc_request(
            $state,
            $ctx,
            &$transaction_id,
            &$request_id,
            $supplied,
            $operation,
            $payload,
        )
        .await?
        {
            Some(GrpcLedgerClaim::Execute(reservation)) => Some(reservation),
            Some(GrpcLedgerClaim::Replay(stored, duplicate_count)) => {
                if let Some(status) = replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                    grpc_transaction_status(
                        $state,
                        $transaction_id.clone(),
                        $request_id.clone(),
                        error,
                        &$ctx.correlation_id,
                    )
                })? {
                    return Err(status);
                }
                let mut response = decode_grpc_message::<$response_type>(&stored).map_err(|error| {
                    grpc_transaction_status(
                        $state,
                        $transaction_id.clone(),
                        $request_id.clone(),
                        error,
                        &$ctx.correlation_id,
                    )
                })?;
                set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                return Ok(Response::new(response));
            }
            Some(GrpcLedgerClaim::Expired(stored_transaction_id)) => {
                return Err(grpc_transaction_status(
                    $state,
                    stored_transaction_id.unwrap_or_else(|| $transaction_id.clone()),
                    $request_id.clone(),
                    ServerError::SessionExpired(
                        "local transaction retry is unavailable after restart or terminal transition"
                            .into(),
                    ),
                    &$ctx.correlation_id,
                ));
            }
            Some(GrpcLedgerClaim::Conflict) => {
                return Err(grpc_idempotency_conflict_status(
                    $state,
                    $transaction_id.clone(),
                    $request_id.clone(),
                    &$ctx.correlation_id,
                ));
            }
            None => None,
        }
    }};
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
    let auth_state = state.clone();
    let interceptor = move |mut req: Request<()>| {
        let correlation_id =
            extract_correlation_id(req.metadata()).unwrap_or_else(|| Uuid::new_v4().to_string());
        let traceparent = extract_traceparent(req.metadata());
        let actor = match auth.validate_grpc(req.metadata()) {
            Ok(actor) => actor,
            Err(_) => {
                // Interceptors receive only `Request<()>`, so the generated
                // RPC path/body is intentionally unavailable here.  Return a
                // versioned blocked outcome with a correlation-scoped identity
                // for every denied request rather than risking a method-specific
                // auth bypass or losing the outcome on transaction RPCs.
                let transaction_id = format!("grpc:unauthorized:{correlation_id}");
                let request_id = RequestId::new(format!("{transaction_id}:authenticate"));
                return Err(grpc_unauthenticated_status(
                    &auth_state,
                    transaction_id,
                    request_id,
                ));
            }
        };
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
    type StreamChangefeedStream = changefeed::ChangefeedStream;

    async fn execute_sql(
        &self,
        request: Request<proto::SqlRequest>,
    ) -> std::result::Result<Response<Self::ExecuteSqlStream>, Status> {
        let ctx = read_context(&request);
        let span = ctx.span.clone();
        let _enter = span.enter();
        let req = request.into_inner();
        let (error_transaction_id, error_request_id) = grpc_sql_identity(
            &self.state,
            &ctx,
            &req.session_id,
            req.request_id.as_deref(),
        )?;
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let mut reservation = if req.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "ExecuteSql",
                &error_transaction_id,
                &grpc_canonical_payload(&[
                    ("session_id", req.session_id.clone()),
                    ("sql", req.sql.clone()),
                    (
                        "require_distributed",
                        format!("{:?}", req.require_distributed),
                    ),
                ]),
            );
            match claim_grpc_request(
                &self.state,
                error_request_id.clone(),
                "ExecuteSql",
                fingerprint,
            )
            .await
            .map_err(|error| {
                grpc_transaction_status(
                    &self.state,
                    error_transaction_id.clone(),
                    error_request_id.clone(),
                    error,
                    &ctx.correlation_id,
                )
            })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                error_transaction_id.clone(),
                                error_request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut messages = decode_grpc_messages::<proto::SqlResultSet>(&stored)
                        .map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                error_transaction_id.clone(),
                                error_request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    for message in &mut messages {
                        set_transaction_duplicate_count(&mut message.transaction, duplicate_count);
                    }
                    return Ok(Response::new(tokio_stream::iter(
                        messages.into_iter().map(Ok).collect::<Vec<_>>(),
                    )));
                }
                GrpcLedgerClaim::Expired(stored_transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        stored_transaction_id.unwrap_or(error_transaction_id),
                        error_request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        error_transaction_id,
                        error_request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        // Explicit requests reserve their idempotency record before any
        // pre-execution rejection.  That makes unavailable/invalid outcomes
        // durable and replayable just like successful execution.
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                error_transaction_id.clone(),
                error_request_id.clone(),
                "ExecuteSql",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                error_transaction_id,
                error_request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if req.sql.trim().is_empty() {
            let status = grpc_transaction_status(
                &self.state,
                error_transaction_id.clone(),
                error_request_id.clone(),
                ServerError::BadRequest("sql must not be empty".into()),
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                error_transaction_id,
                error_request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
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
            request_id: req.request_id.map(RequestId::new),
            streaming: false,
        };
        let http_ctx = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let result = match crate::http::sql::execute_non_streaming(
            self.state.clone(),
            &http_request,
            &http_ctx,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                let error = if http_request.session_id.is_some() {
                    grpc_local_session_liveness_error(error)
                } else {
                    error
                };
                let status = grpc_transaction_status(
                    &self.state,
                    error_transaction_id.clone(),
                    error_request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    error_transaction_id,
                    error_request_id,
                    &status,
                    &ctx.correlation_id,
                    http_request.session_id.is_some(),
                );
                return Err(status);
            }
        };
        let transaction_outcome = result.transaction.clone();
        let transaction = transaction_outcome_to_proto(&result.transaction);

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
                transaction: (!legacy_request).then(|| transaction.clone()),
            };
            bytes_total = bytes_total.saturating_add(message.encoded_len());
            if let Err(error) = memory_policy.enforce_output_bytes(bytes_total as u64) {
                let status =
                    grpc_status_with_outcome(error, &transaction_outcome, &ctx.correlation_id);
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    error_transaction_id,
                    error_request_id,
                    &status,
                    &ctx.correlation_id,
                    http_request.session_id.is_some(),
                );
                return Err(status);
            }
            if bytes_total > self.state.config.max_response_size {
                let status = grpc_status_with_outcome(
                    ServerError::PayloadTooLarge("response size exceeds limit".into()),
                    &transaction_outcome,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    error_transaction_id,
                    error_request_id,
                    &status,
                    &ctx.correlation_id,
                    http_request.session_id.is_some(),
                );
                return Err(status);
            }
            result_sets.push(message);
        }

        if let Some(reservation) = reservation {
            let stored = StoredGrpcResponse {
                messages: result_sets
                    .iter()
                    .map(|message| message.encode_to_vec())
                    .collect(),
                status: None,
                requires_active_transaction: http_request.session_id.is_some(),
            };
            if reservation
                .complete(&self.state, Some(error_transaction_id.clone()), stored)
                .is_err()
            {
                return Err(grpc_idempotency_persistence_pending_status(
                    &self.state,
                    error_transaction_id,
                    error_request_id,
                    &ctx.correlation_id,
                ));
            }
        }

        // Return the result sets directly without an intermediate channel.
        Ok(Response::new(tokio_stream::iter(
            result_sets.into_iter().map(Ok).collect::<Vec<_>>(),
        )))
    }

    async fn execute_ddl(
        &self,
        request: Request<proto::DdlRequest>,
    ) -> std::result::Result<Response<proto::DdlResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let (transaction_id, request_id) = grpc_sql_identity(
            &self.state,
            &ctx,
            &req.session_id,
            req.request_id.as_deref(),
        )?;
        let mut reservation = if req.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "ExecuteDdl",
                &transaction_id,
                &grpc_canonical_payload(&[
                    ("sql", req.sql.clone()),
                    (
                        "require_distributed",
                        format!("{:?}", req.require_distributed),
                    ),
                ]),
            );
            match claim_grpc_request(&self.state, request_id.clone(), "ExecuteDdl", fingerprint)
                .await
                .map_err(|error| {
                    grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        error,
                        &ctx.correlation_id,
                    )
                })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut response =
                        decode_grpc_message::<proto::DdlResponse>(&stored).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                    return Ok(Response::new(response));
                }
                GrpcLedgerClaim::Expired(stored_transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        stored_transaction_id.unwrap_or(transaction_id),
                        request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        transaction_id,
                        request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_unsupported_distributed_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "ExecuteDdl",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if req.sql.trim().is_empty() {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                ServerError::BadRequest("sql must not be empty".into()),
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if let Err(error) = validate_grpc_ddl(&req.sql) {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if let Err(error) = self.state.lifecycle_state.check_write_allowed() {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        ServerError::BadRequest("invalid session_id".into()),
                        &ctx.correlation_id,
                    );
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        true,
                    );
                    return Err(status);
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
            .map_err(|err| {
                let err = grpc_local_session_liveness_error(err);
                grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    err,
                    &ctx.correlation_id,
                )
            });
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &err,
                        &ctx.correlation_id,
                        true,
                    );
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
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        err,
                        &ctx.correlation_id,
                    );
                    self.state.metrics.record_query(start.elapsed(), false);
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        false,
                    );
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
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    err,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        }
        self.state.metrics.record_query(start.elapsed(), true);
        match exec_result {
            alopex_sql::executor::ExecutionResult::Success => {
                let outcome = grpc_sql_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    if req.session_id.is_empty() {
                        OperationState::Committed
                    } else {
                        OperationState::Running
                    },
                    if req.session_id.is_empty() {
                        "local_sql_ddl"
                    } else {
                        "local_session_sql"
                    },
                );
                let response = proto::DdlResponse {
                    success: true,
                    transaction: (!(req.request_id.is_none() && req.require_distributed.is_none()))
                        .then(|| transaction_outcome_to_proto(&outcome)),
                };
                complete_grpc_response_or_recovery_pending!(
                    reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    stored_grpc_message(&response, !req.session_id.is_empty()),
                    &ctx.correlation_id,
                );
                Ok(Response::new(response))
            }
            _ => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    ServerError::BadRequest("DDL returned unexpected result".into()),
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    !req.session_id.is_empty(),
                );
                Err(status)
            }
        }
    }

    async fn execute_dml(
        &self,
        request: Request<proto::DmlRequest>,
    ) -> std::result::Result<Response<proto::DmlResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let (transaction_id, request_id) = grpc_sql_identity(
            &self.state,
            &ctx,
            &req.session_id,
            req.request_id.as_deref(),
        )?;
        let mut reservation = if req.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "ExecuteDml",
                &transaction_id,
                &grpc_canonical_payload(&[
                    ("sql", req.sql.clone()),
                    (
                        "require_distributed",
                        format!("{:?}", req.require_distributed),
                    ),
                ]),
            );
            match claim_grpc_request(&self.state, request_id.clone(), "ExecuteDml", fingerprint)
                .await
                .map_err(|error| {
                    grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        error,
                        &ctx.correlation_id,
                    )
                })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut response =
                        decode_grpc_message::<proto::DmlResponse>(&stored).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                    return Ok(Response::new(response));
                }
                GrpcLedgerClaim::Expired(stored_transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        stored_transaction_id.unwrap_or(transaction_id),
                        request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        transaction_id,
                        request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "ExecuteDml",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if req.sql.trim().is_empty() {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                ServerError::BadRequest("sql must not be empty".into()),
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if let Err(error) = validate_grpc_dml(&req.sql) {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        if let Err(error) = self.state.lifecycle_state.check_write_allowed() {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let start = Instant::now();
        let exec_result = if !req.session_id.is_empty() {
            let session_id = match req.session_id.parse::<SessionId>() {
                Ok(id) => id,
                Err(_) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        ServerError::BadRequest("invalid session_id".into()),
                        &ctx.correlation_id,
                    );
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        true,
                    );
                    return Err(status);
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
            .map_err(|err| {
                let err = grpc_local_session_liveness_error(err);
                grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    err,
                    &ctx.correlation_id,
                )
            });
            match exec_result {
                Ok(result) => result,
                Err(err) => {
                    self.state.metrics.record_query(start.elapsed(), false);
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &err,
                        &ctx.correlation_id,
                        true,
                    );
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
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        err,
                        &ctx.correlation_id,
                    );
                    self.state.metrics.record_query(start.elapsed(), false);
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        false,
                    );
                    return Err(status);
                }
            }
        };

        self.state.metrics.record_query(start.elapsed(), true);
        match exec_result {
            alopex_sql::executor::ExecutionResult::RowsAffected(count) => {
                let outcome = grpc_sql_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    if req.session_id.is_empty() {
                        OperationState::Committed
                    } else {
                        OperationState::Running
                    },
                    if req.session_id.is_empty() {
                        "local_sql_autocommit"
                    } else {
                        "local_session_sql"
                    },
                );
                let response = proto::DmlResponse {
                    affected_rows: count,
                    transaction: (!(req.request_id.is_none() && req.require_distributed.is_none()))
                        .then(|| transaction_outcome_to_proto(&outcome)),
                };
                complete_grpc_response_or_recovery_pending!(
                    reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    stored_grpc_message(&response, !req.session_id.is_empty()),
                    &ctx.correlation_id,
                );
                Ok(Response::new(response))
            }
            _ => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    ServerError::BadRequest("DML returned unexpected result".into()),
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    !req.session_id.is_empty(),
                );
                Err(status)
            }
        }
    }

    async fn begin_transaction(
        &self,
        request: Request<proto::BeginRequest>,
    ) -> std::result::Result<Response<proto::TransactionHandle>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let requested = request.into_inner();
        let provisional_transaction_id = format!("grpc:begin:{}", ctx.correlation_id);
        // Validate explicit request IDs before creating a session.  A rejected
        // request must not leave an otherwise invisible transaction behind.
        let _preflight_request_id = grpc_request_id(
            &self.state,
            &ctx.correlation_id,
            requested.request_id.as_deref(),
            &provisional_transaction_id,
            "begin",
        )?;
        let mut reservation = if requested.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "BeginTransaction",
                "grpc:begin",
                &grpc_canonical_payload(&[(
                    "require_distributed",
                    format!("{:?}", requested.require_distributed),
                )]),
            );
            match claim_grpc_request(
                &self.state,
                _preflight_request_id.clone(),
                "BeginTransaction",
                fingerprint,
            )
            .await
            .map_err(|error| {
                grpc_transaction_status(
                    &self.state,
                    provisional_transaction_id.clone(),
                    _preflight_request_id.clone(),
                    error,
                    &ctx.correlation_id,
                )
            })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                provisional_transaction_id.clone(),
                                _preflight_request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut handle = decode_grpc_message::<proto::TransactionHandle>(&stored)
                        .map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                provisional_transaction_id.clone(),
                                _preflight_request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    set_transaction_duplicate_count(&mut handle.transaction, duplicate_count);
                    return Ok(Response::new(handle));
                }
                GrpcLedgerClaim::Expired(transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        transaction_id.unwrap_or(provisional_transaction_id.clone()),
                        _preflight_request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        provisional_transaction_id,
                        _preflight_request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        if requested.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                provisional_transaction_id.clone(),
                _preflight_request_id.clone(),
                "BeginTransaction",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                provisional_transaction_id,
                _preflight_request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let session_id = match self.state.session_manager.create_session().await {
            Ok(session_id) => session_id,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    provisional_transaction_id.clone(),
                    _preflight_request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    provisional_transaction_id,
                    _preflight_request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        if let Err(error) = self
            .state
            .session_manager
            .begin_transaction(&session_id)
            .await
        {
            let status = grpc_transaction_status(
                &self.state,
                provisional_transaction_id.clone(),
                _preflight_request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                session_id.to_string(),
                _preflight_request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let snapshot = match self.state.session_manager.get_session(&session_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    provisional_transaction_id.clone(),
                    _preflight_request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    session_id.to_string(),
                    _preflight_request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let expires_at = snapshot
            .expires_at
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let session_id = session_id.to_string();
        let request_id = grpc_request_id(
            &self.state,
            &ctx.correlation_id,
            requested.request_id.as_deref(),
            &session_id,
            "begin",
        )?;
        let outcome = grpc_local_outcome(
            &self.state,
            session_id.clone(),
            request_id.clone(),
            OperationState::Running,
            "session_started",
        );
        let response = proto::TransactionHandle {
            session_id,
            expires_at_ms: expires_at,
            request_id: Some(request_id.as_str().to_owned()),
            transaction: Some(transaction_outcome_to_proto(&outcome)),
            require_distributed: requested.require_distributed,
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            response.session_id,
            request_id,
            stored_grpc_message(&response, true),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn commit_transaction(
        &self,
        request: Request<proto::TransactionHandle>,
    ) -> std::result::Result<Response<proto::CommitResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let handle = request.into_inner();
        let transaction_id = handle.session_id.clone();
        let request_id = grpc_request_id(
            &self.state,
            &ctx.correlation_id,
            handle.request_id.as_deref(),
            &transaction_id,
            "commit",
        )?;
        let mut reservation = if handle.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "CommitTransaction",
                &transaction_id,
                &grpc_canonical_payload(&[
                    ("expires_at_ms", handle.expires_at_ms.to_string()),
                    (
                        "require_distributed",
                        format!("{:?}", handle.require_distributed),
                    ),
                ]),
            );
            match claim_grpc_request(
                &self.state,
                request_id.clone(),
                "CommitTransaction",
                fingerprint,
            )
            .await
            .map_err(|error| {
                grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                )
            })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut response = decode_grpc_message::<proto::CommitResponse>(&stored)
                        .map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                    return Ok(Response::new(response));
                }
                GrpcLedgerClaim::Expired(stored_transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        stored_transaction_id.unwrap_or(transaction_id),
                        request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        transaction_id,
                        request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        if handle.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "CommitTransaction",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let session_id = match handle.session_id.parse::<SessionId>() {
            Ok(session_id) => session_id,
            Err(_) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    ServerError::BadRequest("invalid session_id".into()),
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let effects = match self.state.session_manager.commit(&session_id).await {
            Ok(effects) => effects,
            Err(error) => {
                let error = grpc_local_session_liveness_error(error);
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        if !effects.is_empty() {
            if let Err(error) = self.state.apply_table_lifecycle_effects(effects) {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
            if let Err(error) = sync_catalog_to_store(&self.state) {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        }
        let outcome = grpc_local_outcome(
            &self.state,
            transaction_id.clone(),
            request_id.clone(),
            OperationState::Committed,
            "local_session_committed",
        );
        let response = proto::CommitResponse {
            success: true,
            transaction: Some(transaction_outcome_to_proto(&outcome)),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn rollback_transaction(
        &self,
        request: Request<proto::TransactionHandle>,
    ) -> std::result::Result<Response<proto::RollbackResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let handle = request.into_inner();
        let transaction_id = handle.session_id.clone();
        let request_id = grpc_request_id(
            &self.state,
            &ctx.correlation_id,
            handle.request_id.as_deref(),
            &transaction_id,
            "rollback",
        )?;
        let mut reservation = if handle.request_id.is_some() {
            let fingerprint = grpc_request_fingerprint(
                &ctx,
                "RollbackTransaction",
                &transaction_id,
                &grpc_canonical_payload(&[
                    ("expires_at_ms", handle.expires_at_ms.to_string()),
                    (
                        "require_distributed",
                        format!("{:?}", handle.require_distributed),
                    ),
                ]),
            );
            match claim_grpc_request(
                &self.state,
                request_id.clone(),
                "RollbackTransaction",
                fingerprint,
            )
            .await
            .map_err(|error| {
                grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                )
            })? {
                GrpcLedgerClaim::Execute(reservation) => Some(reservation),
                GrpcLedgerClaim::Replay(stored, duplicate_count) => {
                    if let Some(status) =
                        replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?
                    {
                        return Err(status);
                    }
                    let mut response = decode_grpc_message::<proto::RollbackResponse>(&stored)
                        .map_err(|error| {
                            grpc_transaction_status(
                                &self.state,
                                transaction_id.clone(),
                                request_id.clone(),
                                error,
                                &ctx.correlation_id,
                            )
                        })?;
                    set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                    return Ok(Response::new(response));
                }
                GrpcLedgerClaim::Expired(stored_transaction_id) => {
                    return Err(grpc_transaction_status(
                        &self.state,
                        stored_transaction_id.unwrap_or(transaction_id),
                        request_id,
                        ServerError::SessionExpired(
                            "local transaction retry is unavailable after restart or terminal transition"
                                .into(),
                        ),
                        &ctx.correlation_id,
                    ));
                }
                GrpcLedgerClaim::Conflict => {
                    return Err(grpc_idempotency_conflict_status(
                        &self.state,
                        transaction_id,
                        request_id,
                        &ctx.correlation_id,
                    ));
                }
            }
        } else {
            None
        };
        if handle.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "RollbackTransaction",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let session_id = match handle.session_id.parse::<SessionId>() {
            Ok(session_id) => session_id,
            Err(_) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    ServerError::BadRequest("invalid session_id".into()),
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let effects = match self.state.session_manager.rollback(&session_id).await {
            Ok(effects) => effects,
            Err(error) => {
                let error = grpc_local_session_liveness_error(error);
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        if let Err(error) = self.state.apply_catalog_rollback_effects(effects) {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let outcome = grpc_local_outcome(
            &self.state,
            transaction_id.clone(),
            request_id.clone(),
            OperationState::Cancelled,
            "local_session_rolled_back",
        );
        let response = proto::RollbackResponse {
            success: true,
            transaction: Some(transaction_outcome_to_proto(&outcome)),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_search(
        &self,
        request: Request<proto::VectorSearchRequest>,
    ) -> std::result::Result<Response<proto::VectorSearchResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_search",
            req.request_id.as_deref(),
        )?;
        let mut reservation = match claim_explicit_grpc_request(
            &self.state,
            &ctx,
            &transaction_id,
            &request_id,
            req.request_id.is_some(),
            "VectorSearch",
            grpc_canonical_payload(&[
                ("table", req.table.clone()),
                ("k", req.k.to_string()),
                ("index", req.index.clone()),
                ("column", req.column.clone()),
                ("vector", grpc_f32_values_fingerprint(&req.vector)),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed),
                ),
            ]),
        )
        .await?
        {
            Some(GrpcLedgerClaim::Execute(reservation)) => Some(reservation),
            Some(GrpcLedgerClaim::Replay(stored, duplicate_count)) => {
                if let Some(status) =
                    replay_grpc_status(&stored, duplicate_count).map_err(|error| {
                        grpc_transaction_status(
                            &self.state,
                            transaction_id.clone(),
                            request_id.clone(),
                            error,
                            &ctx.correlation_id,
                        )
                    })?
                {
                    return Err(status);
                }
                let mut response = decode_grpc_message::<proto::VectorSearchResponse>(&stored)
                    .map_err(|error| {
                        grpc_transaction_status(
                            &self.state,
                            transaction_id.clone(),
                            request_id.clone(),
                            error,
                            &ctx.correlation_id,
                        )
                    })?;
                set_transaction_duplicate_count(&mut response.transaction, duplicate_count);
                return Ok(Response::new(response));
            }
            Some(GrpcLedgerClaim::Expired(stored_transaction_id)) => {
                return Err(grpc_transaction_status(
                    &self.state,
                    stored_transaction_id.unwrap_or(transaction_id),
                    request_id,
                    ServerError::SessionExpired(
                        "local transaction retry is unavailable after restart or terminal transition"
                            .into(),
                    ),
                    &ctx.correlation_id,
                ));
            }
            Some(GrpcLedgerClaim::Conflict) => {
                return Err(grpc_idempotency_conflict_status(
                    &self.state,
                    transaction_id,
                    request_id,
                    &ctx.correlation_id,
                ));
            }
            None => None,
        };
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorSearch",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
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
        let results =
            match crate::http::vector::search_impl(self.state.clone(), search_request).await {
                Ok(results) => results,
                Err(error) => {
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        error,
                        &ctx.correlation_id,
                    );
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        false,
                    );
                    return Err(status);
                }
            };
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
        let response = proto::VectorSearchResponse {
            results: mapped,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_search",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_upsert(
        &self,
        request: Request<proto::VectorUpsertRequest>,
    ) -> std::result::Result<Response<proto::VectorUpsertResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_upsert",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorUpsert",
            grpc_canonical_payload(&[
                ("table", req.table.clone()),
                ("id", req.id.to_string()),
                ("column", req.column.clone()),
                ("vector", grpc_f32_values_fingerprint(&req.vector)),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorUpsertResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorUpsert",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
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
        if let Err(error) =
            crate::http::vector::upsert_impl(self.state.clone(), upsert_request).await
        {
            let status = grpc_transaction_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                error,
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let response = proto::VectorUpsertResponse {
            success: true,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_upsert",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_delete(
        &self,
        request: Request<proto::VectorDeleteRequest>,
    ) -> std::result::Result<Response<proto::VectorDeleteResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_delete",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorDelete",
            grpc_canonical_payload(&[
                ("table", req.table.clone()),
                ("id", req.id.to_string()),
                ("column", req.column.clone()),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorDeleteResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_prerequisite_missing_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorDelete",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let delete_request = crate::http::vector::VectorDeleteRequest {
            table: req.table,
            id: req.id,
            column: if req.column.is_empty() {
                None
            } else {
                Some(req.column)
            },
        };
        let response =
            match crate::http::vector::delete_impl(self.state.clone(), delete_request).await {
                Ok(response) => response,
                Err(error) => {
                    let status = grpc_transaction_status(
                        &self.state,
                        transaction_id.clone(),
                        request_id.clone(),
                        error,
                        &ctx.correlation_id,
                    );
                    complete_grpc_error_or_recovery_pending!(
                        &mut reservation,
                        &self.state,
                        transaction_id,
                        request_id,
                        &status,
                        &ctx.correlation_id,
                        false,
                    );
                    return Err(status);
                }
            };
        let response = proto::VectorDeleteResponse {
            success: response.success,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_delete",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_index_create(
        &self,
        request: Request<proto::VectorIndexCreateRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_index_create",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorIndexCreate",
            grpc_canonical_payload(&[
                ("name", req.name.clone()),
                ("table", req.table.clone()),
                ("column", req.column.clone()),
                ("method", req.method.clone()),
                ("options", grpc_string_map_fingerprint(&req.options)),
                ("if_not_exists", req.if_not_exists.to_string()),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorIndexResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_unsupported_distributed_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorIndexCreate",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
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
        let response = match crate::http::vector::index_create_impl(
            self.state.clone(),
            create_request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let response = proto::VectorIndexResponse {
            success: response.success,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_index_create",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_index_update(
        &self,
        request: Request<proto::VectorIndexUpdateRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_index_update",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorIndexUpdate",
            grpc_canonical_payload(&[
                ("name", req.name.clone()),
                ("table", req.table.clone()),
                ("column", req.column.clone()),
                ("method", req.method.clone()),
                ("options", grpc_string_map_fingerprint(&req.options)),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorIndexResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_unsupported_distributed_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorIndexUpdate",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
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
        let response = match crate::http::vector::index_update_impl(
            self.state.clone(),
            update_request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let response = proto::VectorIndexResponse {
            success: response.success,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_index_update",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_index_delete(
        &self,
        request: Request<proto::VectorIndexDeleteRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_index_delete",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorIndexDelete",
            grpc_canonical_payload(&[
                ("name", req.name.clone()),
                ("if_exists", req.if_exists.to_string()),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorIndexResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_unsupported_distributed_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorIndexDelete",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let delete_request = crate::http::vector::VectorIndexDeleteRequest {
            name: req.name,
            if_exists: req.if_exists,
        };
        let response = match crate::http::vector::index_delete_impl(
            self.state.clone(),
            delete_request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let response = proto::VectorIndexResponse {
            success: response.success,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_index_delete",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn vector_index_compact(
        &self,
        request: Request<proto::VectorIndexCompactRequest>,
    ) -> std::result::Result<Response<proto::VectorIndexResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let req = request.into_inner();
        let legacy_request = req.request_id.is_none() && req.require_distributed.is_none();
        let (transaction_id, request_id) = grpc_surface_request_identity(
            &self.state,
            &ctx,
            "vector_index_compact",
            req.request_id.as_deref(),
        )?;
        let mut reservation = claim_grpc_typed_response!(
            &self.state,
            &ctx,
            transaction_id,
            request_id,
            req.request_id.is_some(),
            "VectorIndexCompact",
            grpc_canonical_payload(&[
                ("name", req.name.clone()),
                (
                    "require_distributed",
                    format!("{:?}", req.require_distributed)
                ),
            ]),
            proto::VectorIndexResponse
        );
        if req.require_distributed.unwrap_or(false) {
            let status = grpc_unsupported_distributed_status(
                &self.state,
                transaction_id.clone(),
                request_id.clone(),
                "VectorIndexCompact",
                &ctx.correlation_id,
            );
            complete_grpc_error_or_recovery_pending!(
                &mut reservation,
                &self.state,
                transaction_id,
                request_id,
                &status,
                &ctx.correlation_id,
                false,
            );
            return Err(status);
        }
        let compact_request = crate::http::vector::VectorIndexCompactRequest { name: req.name };
        let response = match crate::http::vector::index_compact_impl(
            self.state.clone(),
            compact_request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                let status = grpc_transaction_status(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    error,
                    &ctx.correlation_id,
                );
                complete_grpc_error_or_recovery_pending!(
                    &mut reservation,
                    &self.state,
                    transaction_id,
                    request_id,
                    &status,
                    &ctx.correlation_id,
                    false,
                );
                return Err(status);
            }
        };
        let response = proto::VectorIndexResponse {
            success: response.success,
            transaction: (!legacy_request).then(|| {
                transaction_outcome_to_proto(&grpc_local_outcome(
                    &self.state,
                    transaction_id.clone(),
                    request_id.clone(),
                    OperationState::Committed,
                    "local_vector_index_compact",
                ))
            }),
        };
        complete_grpc_response_or_recovery_pending!(
            reservation,
            &self.state,
            transaction_id,
            request_id,
            stored_grpc_message(&response, false),
            &ctx.correlation_id,
        );
        Ok(Response::new(response))
    }

    async fn health(
        &self,
        request: Request<proto::HealthRequest>,
    ) -> std::result::Result<Response<proto::HealthResponse>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        Ok(Response::new(proto::HealthResponse {
            status: "ok".to_string(),
            transaction: Some(transaction_outcome_to_proto(&grpc_surface_outcome(
                &self.state,
                &ctx,
                "health",
                "local_health",
            ))),
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
            .map_err(|err| grpc_surface_status(&self.state, &ctx, "cluster_status", err))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterStatusResponse {
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
            transaction: Some(transaction_outcome_to_proto(&grpc_surface_outcome(
                &self.state,
                &ctx,
                "cluster_status",
                "local_cluster_status",
            ))),
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
            .map_err(|err| grpc_surface_status(&self.state, &ctx, "cluster_join", err))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterOperationResponse {
            action: "join".to_string(),
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
            operation_id: Uuid::new_v4().to_string(),
            state: "committed".to_string(),
            reason_code: "membership_changed".to_string(),
            transaction: Some(transaction_outcome_to_proto(&grpc_surface_outcome(
                &self.state,
                &ctx,
                "cluster_join",
                "local_cluster_join",
            ))),
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
            .map_err(|err| grpc_surface_status(&self.state, &ctx, "cluster_leave", err))?;
        self.state.metrics.record_cluster_status(&cluster);
        Ok(Response::new(proto::ClusterOperationResponse {
            action: "leave".to_string(),
            cluster_json: cluster_json(&cluster, &ctx.correlation_id)?,
            operation_id: Uuid::new_v4().to_string(),
            state: "committed".to_string(),
            reason_code: "membership_changed".to_string(),
            transaction: Some(transaction_outcome_to_proto(&grpc_surface_outcome(
                &self.state,
                &ctx,
                "cluster_leave",
                "local_cluster_leave",
            ))),
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

    async fn create_set(
        &self,
        request: Request<proto::CreateSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetCreateRequest {
            object_id: request.object_id,
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome =
            crate::http::crdt::create_set_outcome(self.state.as_ref(), &http_context, core_request);
        let response = set_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn read_set(
        &self,
        request: Request<proto::ReadSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetReadRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::read_set_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = set_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn add_set(
        &self,
        request: Request<proto::AddSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetAddRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            member: request.member,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::add_set_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = set_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn remove_set(
        &self,
        request: Request<proto::RemoveSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetRemoveRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            member: request.member,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::remove_set_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = set_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn contains_set(
        &self,
        request: Request<proto::ContainsSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetContainsRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
            member: request.member,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::contains_set_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = set_outcome_to_proto(&outcome);
        let status = outcome.surface_status();
        if status.grpc_code != "OK" {
            return Err(crdt_status(status.grpc_code, &ctx.correlation_id));
        }
        Ok(Response::new(response))
    }

    async fn list_set(
        &self,
        request: Request<proto::ListSetRequest>,
    ) -> std::result::Result<Response<proto::SetOutcome>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        let request = request.into_inner();
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("range is required"))?;
        let core_request = crate::http::crdt::SetListRequest {
            range: range_identity_from_proto(range),
            request_id: request.request_id.into(),
            operation_id: request.operation_id,
            update_version: request.update_version,
        };
        let http_context = crate::http::RequestContext {
            correlation_id: ctx.correlation_id.clone(),
            actor: ctx.actor.clone(),
        };
        let outcome = crate::http::crdt::list_set_outcome(
            self.state.as_ref(),
            &http_context,
            request.object_id,
            core_request,
        );
        let response = set_outcome_to_proto(&outcome);
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

    async fn create_changefeed(
        &self,
        request: Request<proto::CreateChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::create(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn subscribe_changefeed(
        &self,
        request: Request<proto::SubscribeChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::subscribe(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn poll_changefeed(
        &self,
        request: Request<proto::DeliveryChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedDeliveryV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::poll(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn stream_changefeed(
        &self,
        request: Request<proto::DeliveryChangefeedRequestV1>,
    ) -> std::result::Result<Response<Self::StreamChangefeedStream>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::stream(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn ack_changefeed(
        &self,
        request: Request<proto::AckChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::ack(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn resume_changefeed(
        &self,
        request: Request<proto::ResumeChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedDeliveryV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::resume(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn cancel_changefeed(
        &self,
        request: Request<proto::LifecycleChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::cancel(self.state.as_ref(), &ctx, request.into_inner())
    }

    async fn close_changefeed(
        &self,
        request: Request<proto::LifecycleChangefeedRequestV1>,
    ) -> std::result::Result<Response<proto::ChangefeedOutcomeV1>, Status> {
        let ctx = read_context(&request);
        let _enter = ctx.span.enter();
        changefeed::close(self.state.as_ref(), &ctx, request.into_inner())
    }
}

fn grpc_request_id(
    state: &ServerState,
    correlation_id: &str,
    supplied_request_id: Option<&str>,
    transaction_id: &str,
    operation: &str,
) -> std::result::Result<RequestId, Status> {
    transaction_request_id(
        supplied_request_id.map(|request_id| RequestId::new(request_id.to_owned())),
        transaction_id,
        operation,
    )
    .map_err(|error| {
        grpc_transaction_status(
            state,
            transaction_id.to_owned(),
            RequestId::new(format!("{transaction_id}:{operation}")),
            error,
            correlation_id,
        )
    })
}

fn grpc_local_outcome(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    operation_state: OperationState,
    reason_code: &str,
) -> HttpTransactionOutcome {
    HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        operation_state,
        None,
        Some(reason_code.to_owned()),
        RoutingOutcomeKind::LocalOnly,
        reason_code,
        false,
    )
}

fn grpc_sql_outcome(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    operation_state: OperationState,
    reason_code: &str,
) -> HttpTransactionOutcome {
    grpc_local_outcome(
        state,
        transaction_id,
        request_id,
        operation_state,
        reason_code,
    )
}

fn grpc_local_session_liveness_error(error: ServerError) -> ServerError {
    match error {
        ServerError::NotFound(_) => ServerError::SessionExpired(
            "local session is unavailable after restart or terminal transition".into(),
        ),
        error => error,
    }
}

/// Resolve the durable identity before executing a gRPC SQL request.  This is
/// deliberately shared by DDL/DML and error paths so invalid caller-supplied
/// request IDs fail before a write and all outcomes use the same identity.
fn grpc_sql_identity(
    state: &ServerState,
    ctx: &GrpcContext,
    session_id: &str,
    supplied_request_id: Option<&str>,
) -> std::result::Result<(String, RequestId), Status> {
    let provisional_transaction_id = if session_id.is_empty() {
        format!("local-sql:{}", ctx.correlation_id)
    } else {
        session_id.to_owned()
    };
    let request_id = grpc_request_id(
        state,
        &ctx.correlation_id,
        supplied_request_id,
        &provisional_transaction_id,
        "execute",
    )?;
    let transaction_id = if session_id.is_empty() && supplied_request_id.is_some() {
        format!("local-sql:{}", request_id.as_str())
    } else {
        provisional_transaction_id
    };
    Ok((transaction_id, request_id))
}

fn grpc_surface_outcome(
    state: &ServerState,
    ctx: &GrpcContext,
    operation: &str,
    reason_code: &str,
) -> HttpTransactionOutcome {
    let (transaction_id, request_id) = grpc_surface_identity(ctx, operation);
    grpc_local_outcome(
        state,
        transaction_id,
        request_id,
        OperationState::Committed,
        reason_code,
    )
}

fn grpc_surface_status(
    state: &ServerState,
    ctx: &GrpcContext,
    operation: &str,
    error: ServerError,
) -> Status {
    let (transaction_id, request_id) = grpc_surface_identity(ctx, operation);
    grpc_transaction_status(
        state,
        transaction_id,
        request_id,
        error,
        &ctx.correlation_id,
    )
}

fn grpc_surface_request_identity(
    state: &ServerState,
    ctx: &GrpcContext,
    operation: &str,
    supplied_request_id: Option<&str>,
) -> std::result::Result<(String, RequestId), Status> {
    let (provisional_transaction_id, _) = grpc_surface_identity(ctx, operation);
    let request_id = grpc_request_id(
        state,
        &ctx.correlation_id,
        supplied_request_id,
        &provisional_transaction_id,
        operation,
    )?;
    let transaction_id = if supplied_request_id.is_some() {
        format!("grpc:{operation}:{}", request_id.as_str())
    } else {
        provisional_transaction_id
    };
    Ok((transaction_id, request_id))
}

fn grpc_surface_identity(ctx: &GrpcContext, operation: &str) -> (String, RequestId) {
    let transaction_id = format!("grpc:{operation}:{}", ctx.correlation_id);
    let request_id = RequestId::new(format!("{transaction_id}:{operation}"));
    (transaction_id, request_id)
}

fn grpc_transaction_status(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    error: ServerError,
    correlation_id: &str,
) -> Status {
    let outcome = transaction_failure_outcome(state, transaction_id, request_id, &error);
    let status = map_status(error, correlation_id);
    Status::with_details(
        status.code(),
        status.message().to_owned(),
        transaction_outcome_to_proto(&outcome)
            .encode_to_vec()
            .into(),
    )
}

fn grpc_idempotency_conflict_status(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    correlation_id: &str,
) -> Status {
    let outcome = HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::Rejected,
        Some(FailureClass::Conflict),
        Some("idempotency_conflict".to_owned()),
        RoutingOutcomeKind::LocalOnly,
        "idempotency_conflict",
        false,
    );
    let status = map_status(
        ServerError::Conflict("request_id was already used for a different gRPC operation".into()),
        correlation_id,
    );
    Status::with_details(
        status.code(),
        status.message().to_owned(),
        transaction_outcome_to_proto(&outcome)
            .encode_to_vec()
            .into(),
    )
}

/// Preserve the completed transaction outcome when response streaming or a
/// transport limit fails after execution.  Reclassifying this as an unexecuted
/// request would make a client retry a write whose result is already known.
fn grpc_status_with_outcome(
    error: ServerError,
    outcome: &HttpTransactionOutcome,
    correlation_id: &str,
) -> Status {
    let status = map_status(error, correlation_id);
    Status::with_details(
        status.code(),
        status.message().to_owned(),
        transaction_outcome_to_proto(outcome).encode_to_vec().into(),
    )
}

fn grpc_prerequisite_missing_status(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    operation: &str,
    correlation_id: &str,
) -> Status {
    grpc_transaction_status(
        state,
        transaction_id,
        request_id,
        ServerError::CapabilityUnavailable(format!(
            "gRPC {operation} requested distributed execution, but no approved range/TSO coordinator is available"
        )),
        correlation_id,
    )
}

fn grpc_unsupported_distributed_status(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
    operation: &str,
    correlation_id: &str,
) -> Status {
    grpc_transaction_status(
        state,
        transaction_id,
        request_id,
        ServerError::FutureDistributedExecutionRequired(format!(
            "gRPC {operation} is pre-execution unsupported for distributed transactions"
        )),
        correlation_id,
    )
}

fn validate_grpc_ddl(sql: &str) -> std::result::Result<(), ServerError> {
    let statements = alopex_sql::parser::Parser::parse_sql(&alopex_sql::AlopexDialect, sql)
        .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    let ddl_only = !statements.is_empty()
        && statements.iter().all(|statement| {
            matches!(
                statement.kind,
                alopex_sql::StatementKind::CreateTable(_)
                    | alopex_sql::StatementKind::DropTable(_)
                    | alopex_sql::StatementKind::CreateIndex(_)
                    | alopex_sql::StatementKind::DropIndex(_)
            )
        });
    if ddl_only {
        Ok(())
    } else {
        Err(ServerError::BadRequest(
            "ExecuteDdl accepts only CREATE/DROP TABLE or INDEX statements".into(),
        ))
    }
}

fn validate_grpc_dml(sql: &str) -> std::result::Result<(), ServerError> {
    let statements = alopex_sql::parser::Parser::parse_sql(&alopex_sql::AlopexDialect, sql)
        .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    let dml_only = !statements.is_empty()
        && statements.iter().all(|statement| {
            matches!(
                statement.kind,
                alopex_sql::StatementKind::Insert(_)
                    | alopex_sql::StatementKind::Update(_)
                    | alopex_sql::StatementKind::Delete(_)
            )
        });
    if dml_only {
        Ok(())
    } else {
        Err(ServerError::BadRequest(
            "ExecuteDml accepts only INSERT, UPDATE, or DELETE statements".into(),
        ))
    }
}

fn grpc_unauthenticated_status(
    state: &ServerState,
    transaction_id: String,
    request_id: RequestId,
) -> Status {
    let error = ServerError::Unauthorized("unauthorized".into());
    let outcome = transaction_failure_outcome(state, transaction_id, request_id, &error);
    Status::with_details(
        tonic::Code::Unauthenticated,
        "unauthorized",
        transaction_outcome_to_proto(&outcome)
            .encode_to_vec()
            .into(),
    )
}

fn transaction_range_identity_to_proto(
    range: &RangeIdentity,
) -> proto::TransactionRangeIdentityV09 {
    proto::TransactionRangeIdentityV09 {
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

fn transaction_state_to_proto(state: &OperationState) -> i32 {
    use proto::TransactionOperationStateV09 as Wire;
    match enum_wire(state).as_str() {
        "accepted" => Wire::Accepted as i32,
        "running" => Wire::Running as i32,
        "committed" => Wire::Committed as i32,
        "rejected" => Wire::Rejected as i32,
        "retryable_failure" => Wire::RetryableFailure as i32,
        "terminal_failure" => Wire::TerminalFailure as i32,
        "recovery_pending" => Wire::RecoveryPending as i32,
        "cancelled" => Wire::Cancelled as i32,
        _ => Wire::Unspecified as i32,
    }
}

fn transaction_failure_class_to_proto(failure_class: &alopex_cluster::FailureClass) -> i32 {
    use proto::TransactionFailureClassV09 as Wire;
    match enum_wire(failure_class).as_str() {
        "unauthorized" => Wire::Unauthorized as i32,
        "stale_metadata" => Wire::StaleMetadata as i32,
        "gap" => Wire::Gap as i32,
        "overlap" => Wire::Overlap as i32,
        "epoch_mismatch" => Wire::EpochMismatch as i32,
        "not_leader" => Wire::NotLeader as i32,
        "node_unavailable" => Wire::NodeUnavailable as i32,
        "prerequisite_missing" => Wire::PrerequisiteMissing as i32,
        "timeout" => Wire::Timeout as i32,
        "conflict" => Wire::Conflict as i32,
        "invalid_request" => Wire::InvalidRequest as i32,
        "internal" => Wire::Internal as i32,
        _ => Wire::Unspecified as i32,
    }
}

fn transaction_routing_kind_to_proto(kind: &alopex_cluster::RoutingOutcomeKind) -> i32 {
    use proto::TransactionRoutingKindV09 as Wire;
    match enum_wire(kind).as_str() {
        "local" => Wire::Local as i32,
        "single_range" => Wire::SingleRange as i32,
        "multi_range" => Wire::MultiRange as i32,
        "local_only" => Wire::LocalOnly as i32,
        "unsupported" => Wire::Unsupported as i32,
        "unavailable" => Wire::Unavailable as i32,
        "retryable" => Wire::Retryable as i32,
        "blocked" => Wire::Blocked as i32,
        _ => Wire::Unspecified as i32,
    }
}

fn transaction_outcome_to_proto(outcome: &HttpTransactionOutcome) -> proto::TransactionOutcomeV09 {
    let read_point = outcome
        .read_point
        .as_ref()
        .map(|read_point| proto::TransactionReadPointV09 {
            data_epoch: read_point.data_epoch,
            metadata_version: read_point.metadata_version,
            schema_manifest_id: read_point
                .schema_manifest_id
                .as_ref()
                .and_then(|id| {
                    serde_json::to_value(id)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                })
                .unwrap_or_default(),
            has_schema_manifest_id: read_point.schema_manifest_id.is_some(),
            range_generations: read_point
                .range_generations
                .iter()
                .map(|(range_id, value)| proto::TransactionReadPointRangeV09 {
                    range_id: range_id.as_str().to_owned(),
                    value: *value,
                })
                .collect(),
            index_epochs: read_point
                .index_epochs
                .iter()
                .map(|(range_id, value)| proto::TransactionReadPointRangeV09 {
                    range_id: range_id.as_str().to_owned(),
                    value: *value,
                })
                .collect(),
            consistency: enum_wire(&read_point.consistency),
        });
    let reason_code = outcome.reason_code.clone().unwrap_or_default();
    proto::TransactionOutcomeV09 {
        outcome_version: outcome.outcome_version.to_owned(),
        transaction_id: outcome.transaction_id.clone(),
        request_id: outcome.request_id.as_str().to_owned(),
        participating_ranges: outcome
            .participating_ranges
            .iter()
            .map(transaction_range_identity_to_proto)
            .collect(),
        read_point,
        schema_version: outcome.schema_version.unwrap_or_default(),
        has_schema_version: outcome.schema_version.is_some(),
        data_epoch: outcome.data_epoch.unwrap_or_default(),
        has_data_epoch: outcome.data_epoch.is_some(),
        isolation: proto::TransactionIsolationV09::Snapshot as i32,
        state: transaction_state_to_proto(&outcome.state),
        failure_class: outcome
            .failure_class
            .as_ref()
            .map(transaction_failure_class_to_proto)
            .unwrap_or(proto::TransactionFailureClassV09::Unspecified as i32),
        has_failure_class: outcome.failure_class.is_some(),
        reason_code,
        has_reason_code: outcome.reason_code.is_some(),
        routing: Some(proto::TransactionRoutingOutcomeV09 {
            kind: transaction_routing_kind_to_proto(&outcome.routing.kind),
            range: outcome
                .routing
                .range_identity
                .as_ref()
                .map(transaction_range_identity_to_proto),
            has_range: outcome.routing.range_identity.is_some(),
            metadata_version: outcome.routing.metadata_version,
            reason_code: outcome.routing.reason_code.clone(),
        }),
        retryable: outcome.retryable,
        idempotency: Some(proto::TransactionIdempotencyResultV09 {
            operation_id: outcome.idempotency.operation_id.clone(),
            request_id: outcome.idempotency.request_id.as_str().to_owned(),
            first_outcome: outcome.idempotency.first_outcome.clone(),
            state: transaction_state_to_proto(&outcome.idempotency.state),
            duplicate_count: outcome.idempotency.duplicate_count,
        }),
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

fn set_outcome_to_proto(outcome: &CrdtOutcome) -> proto::SetOutcome {
    let common = outcome.common();
    let (has_value, members, member_versions) = match outcome.value() {
        Some(CrdtValue::Set {
            members,
            member_versions,
            ..
        }) => (
            true,
            members.clone(),
            member_versions
                .iter()
                .map(|(member, version)| proto::SetMemberVersion {
                    member: member.clone(),
                    update_version: version.update_version,
                    operation_id: version.operation_id.clone(),
                    present: version.present,
                })
                .collect(),
        ),
        _ => (false, Vec::new(), Vec::new()),
    };
    proto::SetOutcome {
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
        members,
        member_versions,
        membership_unavailable: outcome.value_unavailable().unwrap_or_default().to_owned(),
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
        axum::http::StatusCode::SERVICE_UNAVAILABLE => tonic::Code::Unavailable,
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
mod idempotency_fingerprint_tests {
    use std::collections::HashMap;

    use super::*;

    fn context() -> GrpcContext {
        GrpcContext {
            correlation_id: "fingerprint-test".to_owned(),
            actor: Some("test-actor".to_owned()),
            span: tracing::info_span!("grpc_fingerprint_test"),
        }
    }

    #[test]
    fn length_delimited_fields_distinguish_delimiter_injection() {
        let first = grpc_canonical_payload(&[("sql", "a=b\nc".to_owned())]);
        let second = grpc_canonical_payload(&[("sql", "a\nb=c".to_owned())]);
        assert_ne!(first, second);
        assert_ne!(
            grpc_request_fingerprint(&context(), "ExecuteSql", "target", &first),
            grpc_request_fingerprint(&context(), "ExecuteSql", "target", &second),
        );
    }

    #[test]
    fn map_and_vector_payloads_preserve_exact_values() {
        let mut first = HashMap::new();
        first.insert("a=b".to_owned(), "c".to_owned());
        let mut second = HashMap::new();
        second.insert("a".to_owned(), "b=c".to_owned());
        assert_ne!(
            grpc_string_map_fingerprint(&first),
            grpc_string_map_fingerprint(&second),
        );
        assert_ne!(
            grpc_f32_values_fingerprint(&[0.0]),
            grpc_f32_values_fingerprint(&[-0.0]),
        );
    }
}
