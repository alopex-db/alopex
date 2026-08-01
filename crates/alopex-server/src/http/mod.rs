pub mod admin;
pub mod admin_api;
pub mod admin_resources;
pub mod changefeed;
pub mod columnar;
pub mod crdt;
pub mod hnsw;
pub mod kv;
pub mod session;
pub mod sql;
pub mod vector;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use alopex_cluster::{
    ClusterReadPoint, FailureClass, IdempotencyResult, OperationState, RangeIdentity, RequestId,
    RoutingOutcome, RoutingOutcomeKind, TransactionIsolation,
};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::Serialize;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::Span;
use uuid::Uuid;

use crate::auth::AuthError;
use crate::error::ServerError;
use crate::server::ServerState;

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub correlation_id: String,
    pub actor: Option<String>,
}

#[derive(Clone, Debug)]
struct TransactionRouteIdentity {
    transaction_id: String,
    operation: &'static str,
    routing_kind: RoutingOutcomeKind,
}

/// Internal marker used by the outer context middleware to distinguish a
/// handler-produced v0.9 transaction error from an extractor/body-limit
/// rejection. It never crosses the HTTP boundary.
#[derive(Clone, Debug)]
struct TransactionOutcomeResponseMarker;

/// Mark a handler-built transaction response so the outer context middleware
/// does not mistake a known execution outcome for an extractor rejection.
/// The marker is purely in-process and never appears on the wire.
pub(crate) fn mark_transaction_outcome_response(response: &mut Response) {
    response
        .extensions_mut()
        .insert(TransactionOutcomeResponseMarker);
}

fn transaction_route_identity(
    path: &str,
    correlation_id: &str,
    api_prefix: &str,
) -> Option<TransactionRouteIdentity> {
    // Legacy routes may be nested under a configured API prefix. Normalize to
    // the route's declared path before applying the transaction boundary
    // matrix; do not let a partial prefix such as `/apiary` match `/api`.
    let path = if api_prefix.is_empty() {
        path
    } else {
        path.strip_prefix(api_prefix)
            .filter(|remaining| remaining.starts_with('/'))?
    };
    match path {
        "/sql" | "/api/sql/query" => Some(TransactionRouteIdentity {
            transaction_id: format!("local-sql:{correlation_id}"),
            operation: "execute",
            routing_kind: RoutingOutcomeKind::LocalOnly,
        }),
        "/session/begin" => Some(TransactionRouteIdentity {
            transaction_id: format!("http-session:{correlation_id}"),
            operation: "begin",
            routing_kind: RoutingOutcomeKind::LocalOnly,
        }),
        "/kv/txn/begin" => Some(TransactionRouteIdentity {
            transaction_id: format!("local-kv:{correlation_id}"),
            operation: "begin",
            routing_kind: RoutingOutcomeKind::SingleRange,
        }),
        _ => {
            let segments: Vec<_> = path.trim_start_matches('/').split('/').collect();
            match segments.as_slice() {
                ["session", session_id, "commit"] => Some(TransactionRouteIdentity {
                    transaction_id: (*session_id).to_owned(),
                    operation: "commit",
                    routing_kind: RoutingOutcomeKind::LocalOnly,
                }),
                ["session", session_id, "rollback"] => Some(TransactionRouteIdentity {
                    transaction_id: (*session_id).to_owned(),
                    operation: "rollback",
                    routing_kind: RoutingOutcomeKind::LocalOnly,
                }),
                ["kv", "txn", operation @ ("get" | "put" | "delete" | "commit" | "rollback")] => {
                    let operation = match *operation {
                        "get" => "get",
                        "put" => "put",
                        "delete" => "delete",
                        "commit" => "commit",
                        "rollback" => "rollback",
                        _ => unreachable!("transaction route was matched above"),
                    };
                    Some(TransactionRouteIdentity {
                        // Extractor rejections occur before a JSON body can
                        // safely expose `txn_id`; retain a correlation-bound
                        // identity instead of manufacturing a transaction.
                        transaction_id: format!("local-kv:{correlation_id}"),
                        operation,
                        routing_kind: RoutingOutcomeKind::SingleRange,
                    })
                }
                _ => None,
            }
        }
    }
}

/// Convert an extractor/body-limit rejection into the existing error envelope
/// plus the additive transaction outcome. The outer status and any established
/// JSON `error.code`/`error.message` are preserved; plain Axum rejections use
/// their original text as the legacy message.
async fn transaction_rejection_response(
    state: &ServerState,
    identity: TransactionRouteIdentity,
    response: Response,
    correlation_id: &str,
) -> Response {
    let status = response.status();
    let (_, body) = response.into_parts();
    // This is an internal, bounded conversion of an already-generated Axum
    // rejection. It must not inherit `max_response_size`: a deliberately tiny
    // successful-response limit must not erase the legacy rejection message.
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .unwrap_or_default();
    let fallback_code = match status {
        StatusCode::PAYLOAD_TOO_LARGE => "PAYLOAD_TOO_LARGE",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "UNSUPPORTED_MEDIA_TYPE",
        StatusCode::UNPROCESSABLE_ENTITY => "UNPROCESSABLE_ENTITY",
        _ => "INVALID_REQUEST",
    };
    let fallback_message = String::from_utf8(bytes.to_vec())
        .ok()
        .map(|message| message.trim().to_owned())
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| "invalid transaction request".to_owned());
    let (code, message) = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|body| {
            let error = body.get("error")?;
            Some((
                error.get("code")?.as_str()?.to_owned(),
                error.get("message")?.as_str()?.to_owned(),
            ))
        })
        .unwrap_or_else(|| (fallback_code.to_owned(), fallback_message));
    let request_id = transaction_request_id(None, &identity.transaction_id, identity.operation)
        .expect("derived transaction request id is non-empty");
    let reason_code = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "resource_limit"
    } else {
        "invalid_request"
    };
    let transaction = HttpTransactionOutcome::new(
        identity.transaction_id,
        request_id,
        transaction_metadata_version(state),
        OperationState::Rejected,
        Some(FailureClass::InvalidRequest),
        Some(reason_code.to_owned()),
        identity.routing_kind,
        reason_code,
        false,
    );
    let mut response = (
        status,
        Json(TransactionErrorResponse {
            error: ErrorBody {
                code,
                message,
                correlation_id: correlation_id.to_owned(),
            },
            transaction,
        }),
    )
        .into_response();
    mark_transaction_outcome_response(&mut response);
    response
}

/// Additive v0.9 projection for an HTTP transaction operation.
///
/// [`alopex_cluster::TransactionOutcome`] remains the authoritative result
/// once a distributed coordinator has a committed participant set.  Legacy
/// local HTTP sessions and pre-execution failures cannot manufacture that
/// type: the former have no committed range fence and the latter must not
/// pretend that a participant was enlisted.  This transport projection keeps
/// every public field visible while making those absences explicit as
/// `null`/an empty list and through the routing classification.
#[derive(Clone, Debug, Serialize)]
pub struct HttpTransactionOutcome {
    /// Stable wire version for the additive field set.
    pub outcome_version: &'static str,
    pub transaction_id: String,
    pub request_id: RequestId,
    pub participating_ranges: Vec<RangeIdentity>,
    pub read_point: Option<ClusterReadPoint>,
    pub schema_version: Option<u64>,
    pub data_epoch: Option<u64>,
    pub isolation: TransactionIsolation,
    pub state: OperationState,
    pub failure_class: Option<FailureClass>,
    pub reason_code: Option<String>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
}

impl HttpTransactionOutcome {
    /// Construct an outcome for a local compatibility operation or a
    /// pre-execution decision.  No caller may use this constructor to claim a
    /// distributed commit: only a coordinator-produced `TransactionOutcome`
    /// has participant and read-point evidence for that claim.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_id: impl Into<String>,
        request_id: RequestId,
        metadata_version: u64,
        state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<String>,
        routing_kind: RoutingOutcomeKind,
        routing_reason_code: impl Into<String>,
        retryable: bool,
    ) -> Self {
        let transaction_id = transaction_id.into();
        let first_outcome = match state {
            OperationState::Accepted => "accepted",
            OperationState::Running => "running",
            OperationState::Committed => "committed",
            OperationState::Rejected => "rejected",
            OperationState::RetryableFailure => "retryable_failure",
            OperationState::TerminalFailure => "terminal_failure",
            OperationState::RecoveryPending => "recovery_pending",
            OperationState::Cancelled => "cancelled",
        }
        .to_string();
        Self {
            outcome_version: "v0.9",
            transaction_id: transaction_id.clone(),
            request_id: request_id.clone(),
            participating_ranges: Vec::new(),
            read_point: None,
            schema_version: None,
            data_epoch: None,
            isolation: TransactionIsolation::Snapshot,
            state,
            failure_class,
            reason_code,
            routing: RoutingOutcome::new(routing_kind, None, metadata_version, routing_reason_code),
            retryable,
            idempotency: IdempotencyResult {
                operation_id: transaction_id,
                request_id,
                first_outcome,
                state,
                duplicate_count: 0,
            },
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: String,
    message: String,
    correlation_id: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Serialize)]
struct TransactionErrorResponse {
    error: ErrorBody,
    transaction: HttpTransactionOutcome,
}

struct QueueWaitGuard<'a> {
    counter: &'a std::sync::atomic::AtomicUsize,
}

impl<'a> QueueWaitGuard<'a> {
    fn new(counter: &'a std::sync::atomic::AtomicUsize) -> Self {
        Self { counter }
    }
}

impl Drop for QueueWaitGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn router(state: Arc<ServerState>) -> Router {
    let api = Router::new()
        .route("/crdt/counters", axum::routing::post(crdt::create_counter))
        .route("/crdt/sets", axum::routing::post(crdt::create_set))
        .route(
            "/crdt/counters/{object_id}/read",
            axum::routing::post(crdt::read_counter),
        )
        .route(
            "/crdt/sets/{object_id}/read",
            axum::routing::post(crdt::read_set),
        )
        .route(
            "/crdt/sets/{object_id}/add",
            axum::routing::post(crdt::add_set),
        )
        .route(
            "/crdt/sets/{object_id}/remove",
            axum::routing::post(crdt::remove_set),
        )
        .route(
            "/crdt/sets/{object_id}/contains",
            axum::routing::post(crdt::contains_set),
        )
        .route(
            "/crdt/sets/{object_id}/members",
            axum::routing::post(crdt::list_set),
        )
        .route(
            "/crdt/counters/{object_id}/increment",
            axum::routing::post(crdt::increment_counter),
        )
        .route(
            "/crdt/counters/{object_id}/decrement",
            axum::routing::post(crdt::decrement_counter),
        )
        .route("/kv/get", axum::routing::post(kv::get))
        .route("/kv/put", axum::routing::post(kv::put))
        .route("/kv/delete", axum::routing::post(kv::delete))
        .route("/kv/list", axum::routing::post(kv::list))
        .route("/kv/txn/begin", axum::routing::post(kv::txn_begin))
        .route("/kv/txn/get", axum::routing::post(kv::txn_get))
        .route("/kv/txn/put", axum::routing::post(kv::txn_put))
        .route("/kv/txn/delete", axum::routing::post(kv::txn_delete))
        .route("/kv/txn/commit", axum::routing::post(kv::txn_commit))
        .route("/kv/txn/rollback", axum::routing::post(kv::txn_rollback))
        .route("/columnar/scan", axum::routing::post(columnar::scan))
        .route("/columnar/stats", axum::routing::post(columnar::stats))
        .route("/columnar/list", axum::routing::post(columnar::list))
        .route("/columnar/ingest", axum::routing::post(columnar::ingest))
        .route(
            "/columnar/index/create",
            axum::routing::post(columnar::index_create),
        )
        .route(
            "/columnar/index/list",
            axum::routing::post(columnar::index_list),
        )
        .route(
            "/columnar/index/drop",
            axum::routing::post(columnar::index_drop),
        )
        .route("/hnsw/search", axum::routing::post(hnsw::search))
        .route("/hnsw/upsert", axum::routing::post(hnsw::upsert))
        .route("/hnsw/delete", axum::routing::post(hnsw::delete))
        .route("/hnsw/create", axum::routing::post(hnsw::create))
        .route("/hnsw/drop", axum::routing::post(hnsw::drop))
        .route("/hnsw/stats", axum::routing::post(hnsw::stats))
        .route("/sql", axum::routing::post(sql::handle))
        .route("/api/sql/query", axum::routing::post(sql::handle))
        .route("/vector/search", axum::routing::post(vector::search))
        .route("/vector/upsert", axum::routing::post(vector::upsert))
        .route("/vector/delete", axum::routing::post(vector::delete))
        .route(
            "/vector/index/create",
            axum::routing::post(vector::index_create),
        )
        .route(
            "/vector/index/update",
            axum::routing::post(vector::index_update),
        )
        .route(
            "/vector/index/delete",
            axum::routing::post(vector::index_delete),
        )
        .route(
            "/vector/index/compact",
            axum::routing::post(vector::index_compact),
        )
        .route(
            "/api/admin/capabilities",
            axum::routing::get(admin_api::capabilities),
        )
        .route(
            "/api/admin/resources",
            axum::routing::get(admin_resources::list),
        )
        .route("/api/admin/status", axum::routing::get(admin_api::status))
        .route("/api/admin/metrics", axum::routing::get(admin_api::metrics))
        .route("/api/admin/health", axum::routing::get(admin_api::health))
        .route(
            "/api/admin/cluster/join",
            axum::routing::post(admin_api::cluster_join),
        )
        .route(
            "/api/admin/cluster/leave",
            axum::routing::post(admin_api::cluster_leave),
        )
        .route(
            "/api/admin/cluster/metadata",
            axum::routing::get(admin_api::cluster_metadata),
        )
        .route(
            "/api/admin/cluster/operations",
            axum::routing::post(admin_api::cluster_management),
        )
        .route(
            "/api/admin/backup",
            axum::routing::post(admin_api::start_backup),
        )
        .route("/api/admin/export", axum::routing::post(admin_api::export))
        .route(
            "/api/admin/backup/{id}",
            axum::routing::get(admin_api::backup_status),
        )
        .route(
            "/api/admin/restore",
            axum::routing::post(admin_api::start_restore),
        )
        .route(
            "/api/admin/restore/{id}",
            axum::routing::get(admin_api::restore_status),
        )
        .route(
            "/api/admin/lifecycle",
            axum::routing::post(admin_api::lifecycle),
        )
        .route(
            "/api/admin/compaction",
            axum::routing::post(admin_api::compaction),
        )
        .route("/session/begin", axum::routing::post(session::begin))
        .route("/session/{id}/commit", axum::routing::post(session::commit))
        .route(
            "/session/{id}/rollback",
            axum::routing::post(session::rollback),
        );

    let api = if state.config.api_prefix.is_empty() {
        api
    } else {
        Router::new().nest(&state.config.api_prefix, api)
    };

    // The distributed-read protocol is versioned independently from the
    // configurable legacy API prefix. This keeps its documented cancel route
    // stable even when an installation mounts existing SQL endpoints under a
    // compatibility prefix.
    let api = api
        .route("/v1/changefeeds", axum::routing::post(changefeed::create))
        .route(
            "/v1/changefeeds/{id}/subscribe",
            axum::routing::post(changefeed::subscribe),
        )
        .route(
            "/v1/changefeeds/{id}/events",
            axum::routing::get(changefeed::poll),
        )
        .route(
            "/v1/changefeeds/{id}/stream",
            axum::routing::get(changefeed::stream),
        )
        .route(
            "/v1/changefeeds/{id}/ack",
            axum::routing::post(changefeed::ack),
        )
        .route(
            "/v1/changefeeds/{id}/resume",
            axum::routing::post(changefeed::resume),
        )
        .route(
            "/v1/changefeeds/{id}/cancel",
            axum::routing::post(changefeed::cancel),
        )
        .route(
            "/v1/changefeeds/{id}/close",
            axum::routing::post(changefeed::close),
        )
        .route(
            "/v1/sql/reads",
            axum::routing::post(sql::begin_distributed_read),
        )
        .route(
            "/v1/sql/reads/{id}",
            axum::routing::get(sql::stream_distributed_read),
        )
        .route(
            "/v1/sql/reads/{id}/cancel",
            axum::routing::post(sql::cancel_distributed_read),
        );

    let middleware = middleware::from_fn(context_middleware);
    let connection_middleware = middleware::from_fn(connection_middleware);
    let admission_middleware = middleware::from_fn(admission_middleware);
    // `RequestBodyLimitLayer` rewraps the request body as `Limited<Body>`, so
    // it must be the innermost layer (applied last): the `from_fn`
    // middlewares above are typed against the plain `axum::extract::Request`
    // (`Request<Body>`) and would not satisfy `Service<Request<Limited<Body>>>`
    // if body-limiting ran before them (axum 0.7 middleware is no longer
    // generic over the body type).
    api.layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http().make_span_with(make_trace_span))
            .layer(admission_middleware)
            .layer(middleware)
            .layer(connection_middleware)
            .layer(RequestBodyLimitLayer::new(state.config.max_request_size)),
    )
    .layer(axum::Extension(state))
}

pub fn admin_router(state: Arc<ServerState>) -> Router {
    admin::router(state)
}

pub async fn context_middleware(
    axum::extract::Extension(state): axum::extract::Extension<Arc<ServerState>>,
    mut req: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let correlation_id =
        extract_correlation_id(req.headers()).unwrap_or_else(|| Uuid::new_v4().to_string());
    let transaction_identity =
        transaction_route_identity(req.uri().path(), &correlation_id, &state.config.api_prefix);

    let actor = match state.auth.validate_http(req.headers()) {
        Ok(actor) => actor,
        Err(err) => {
            if state.config.audit_log_enabled {
                state.audit.log(crate::audit::AuditLogEntry {
                    event_type: crate::audit::AuditEventType::AuthFailure,
                    actor: None,
                    target: "auth".into(),
                    correlation_id: correlation_id.clone(),
                    timestamp: chrono::Utc::now(),
                    details: serde_json::json!({ "error": err.to_string() }),
                });
            }
            return auth_error_response(&state, err, &correlation_id, transaction_identity);
        }
    };

    req.extensions_mut().insert(RequestContext {
        correlation_id: correlation_id.clone(),
        actor: actor.clone(),
    });

    let mut res = next.run(req).await;
    // `Json` extraction and request-size enforcement run beneath this
    // middleware, so their rejections never reach the SQL/session handlers.
    // Preserve the existing status/error envelope while attaching the same
    // additive outcome used by handler-level transaction errors.
    if let Some(identity) = transaction_identity {
        let status = res.status();
        let extractor_rejection = matches!(
            status,
            StatusCode::BAD_REQUEST
                | StatusCode::PAYLOAD_TOO_LARGE
                | StatusCode::UNSUPPORTED_MEDIA_TYPE
                | StatusCode::UNPROCESSABLE_ENTITY
        );
        if extractor_rejection
            && res
                .extensions()
                .get::<TransactionOutcomeResponseMarker>()
                .is_none()
        {
            res = transaction_rejection_response(&state, identity, res, &correlation_id).await;
        }
    }
    let _ = res.headers_mut().insert(
        "x-correlation-id",
        HeaderValue::from_str(&correlation_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    res
}

pub async fn connection_middleware(
    axum::extract::Extension(state): axum::extract::Extension<Arc<ServerState>>,
    req: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    state.metrics.record_connection(1);
    let res = next.run(req).await;
    state.metrics.record_connection(-1);
    res
}

pub async fn admission_middleware(
    axum::extract::Extension(state): axum::extract::Extension<Arc<ServerState>>,
    req: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    if let Ok(permit) = state.admission_permits.clone().try_acquire_owned() {
        let res = next.run(req).await;
        drop(permit);
        return res;
    }

    let queued_now = state.admission_waiters.fetch_add(1, Ordering::AcqRel) + 1;
    if queued_now > state.config.max_queue_len {
        state.admission_waiters.fetch_sub(1, Ordering::AcqRel);
        let correlation_id =
            extract_correlation_id(req.headers()).unwrap_or_else(|| Uuid::new_v4().to_string());
        return queue_overflow_response(&correlation_id);
    }
    let queue_wait_guard = QueueWaitGuard::new(&state.admission_waiters);

    let permit = match state.admission_permits.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            let correlation_id =
                extract_correlation_id(req.headers()).unwrap_or_else(|| Uuid::new_v4().to_string());
            return queue_overflow_response(&correlation_id);
        }
    };
    drop(queue_wait_guard);

    let res = next.run(req).await;
    drop(permit);
    res
}

fn auth_error_response(
    state: &ServerState,
    err: AuthError,
    correlation_id: &str,
    transaction_identity: Option<TransactionRouteIdentity>,
) -> Response {
    let message = err.to_string();
    if let Some(identity) = transaction_identity {
        return transaction_auth_error_response(state, identity, message, correlation_id);
    }
    let body = Json(ErrorResponse {
        error: ErrorBody {
            code: "UNAUTHORIZED".to_string(),
            message,
            correlation_id: correlation_id.to_string(),
        },
    });
    (StatusCode::UNAUTHORIZED, body).into_response()
}

/// Authentication is rejected before a handler has a `RequestContext`, but
/// transaction endpoints still need the additive outcome. Keep the legacy
/// message exactly as `AuthError::to_string()` produced it rather than routing
/// it through `ServerError`'s display prefix.
fn transaction_auth_error_response(
    state: &ServerState,
    identity: TransactionRouteIdentity,
    legacy_message: String,
    correlation_id: &str,
) -> Response {
    let error = ServerError::Unauthorized(legacy_message.clone());
    let request_id = transaction_request_id(None, &identity.transaction_id, identity.operation)
        .expect("derived transaction request id is non-empty");
    let transaction =
        transaction_failure_outcome(state, identity.transaction_id, request_id, &error);
    let mut response = (
        StatusCode::UNAUTHORIZED,
        Json(TransactionErrorResponse {
            error: ErrorBody {
                code: "UNAUTHORIZED".to_owned(),
                message: legacy_message,
                correlation_id: correlation_id.to_owned(),
            },
            transaction,
        }),
    )
        .into_response();
    mark_transaction_outcome_response(&mut response);
    response
}

fn queue_overflow_response(correlation_id: &str) -> Response {
    let body = Json(ErrorResponse {
        error: ErrorBody {
            code: "SERVER_BACKPRESSURE".to_string(),
            message: "server request queue is full".to_string(),
            correlation_id: correlation_id.to_string(),
        },
    });
    (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
}

pub fn error_response(err: ServerError, ctx: &RequestContext) -> Response {
    let body = Json(ErrorResponse {
        error: ErrorBody {
            code: err.error_code(),
            message: err.to_string(),
            correlation_id: ctx.correlation_id.clone(),
        },
    });
    (err.status_code(), body).into_response()
}

/// Return the legacy error envelope together with the additive v0.9
/// transaction projection.  Keeping `error` unchanged lets v0.8 clients
/// retain their existing parsing and status handling.
pub fn transaction_error_response(
    err: ServerError,
    transaction: HttpTransactionOutcome,
    ctx: &RequestContext,
) -> Response {
    let body = Json(TransactionErrorResponse {
        error: ErrorBody {
            code: err.error_code(),
            message: err.to_string(),
            correlation_id: ctx.correlation_id.clone(),
        },
        transaction,
    });
    let mut response = (err.status_code(), body).into_response();
    mark_transaction_outcome_response(&mut response);
    response
}

/// A public request id is either supplied by the caller or derived from a
/// stable transaction identity and operation.  Empty caller values are an
/// invalid pre-execution request rather than an implicit new identity.
pub fn transaction_request_id(
    supplied: Option<RequestId>,
    transaction_id: &str,
    operation: &str,
) -> std::result::Result<RequestId, ServerError> {
    match supplied {
        Some(request_id) if request_id.as_str().trim().is_empty() => Err(ServerError::BadRequest(
            "request_id must not be empty".into(),
        )),
        Some(request_id) => Ok(request_id),
        None => Ok(RequestId::new(format!("{transaction_id}:{operation}"))),
    }
}

/// The local/blocked adapter deliberately has no distributed read point.  It
/// still reports the current metadata version used for the classification when
/// it is available; zero means no committed metadata version was observable.
pub fn transaction_metadata_version(state: &ServerState) -> u64 {
    state
        .cluster_status_snapshot()
        .map(|snapshot| snapshot.placement.update_epoch)
        .unwrap_or_default()
}

/// Map the existing HTTP/server error vocabulary to the Phase 1 transaction
/// state vocabulary without changing the legacy HTTP status or `error.code`.
pub fn transaction_failure_outcome(
    state: &ServerState,
    transaction_id: impl Into<String>,
    request_id: RequestId,
    error: &ServerError,
) -> HttpTransactionOutcome {
    let (operation_state, failure_class, routing_kind, reason_code, retryable) = match error {
        ServerError::Unauthorized(_) => (
            OperationState::Rejected,
            FailureClass::Unauthorized,
            RoutingOutcomeKind::Blocked,
            "unauthorized",
            false,
        ),
        ServerError::CapabilityUnavailable(_) => (
            OperationState::Rejected,
            FailureClass::PrerequisiteMissing,
            RoutingOutcomeKind::Blocked,
            "prerequisite_missing",
            false,
        ),
        ServerError::FutureDistributedExecutionRequired(_) | ServerError::NotImplemented(_) => (
            OperationState::Rejected,
            FailureClass::InvalidRequest,
            RoutingOutcomeKind::Unsupported,
            "unsupported",
            false,
        ),
        ServerError::Timeout(_) => (
            OperationState::RetryableFailure,
            FailureClass::Timeout,
            RoutingOutcomeKind::Retryable,
            "timeout",
            true,
        ),
        ServerError::Conflict(_) | ServerError::RestoreIntegrityMismatch(_) => (
            OperationState::RetryableFailure,
            FailureClass::Conflict,
            RoutingOutcomeKind::Retryable,
            "conflict",
            true,
        ),
        // `SqlError::Storage(TransactionConflict)` intentionally keeps its
        // established HTTP 409/error code (`ALOPEX-S001`).  Its additive v0.9
        // classification must agree with that retryable conflict boundary.
        ServerError::Sql(error) if error.code() == "ALOPEX-S001" => (
            OperationState::RetryableFailure,
            FailureClass::Conflict,
            RoutingOutcomeKind::Retryable,
            "conflict",
            true,
        ),
        ServerError::PayloadTooLarge(_) => (
            OperationState::Rejected,
            FailureClass::InvalidRequest,
            RoutingOutcomeKind::LocalOnly,
            "resource_limit",
            false,
        ),
        ServerError::InvalidConfig(_)
        | ServerError::BadRequest(_)
        | ServerError::NotFound(_)
        | ServerError::Sql(_) => (
            OperationState::Rejected,
            FailureClass::InvalidRequest,
            RoutingOutcomeKind::LocalOnly,
            "invalid_request",
            false,
        ),
        ServerError::SessionExpired(_) => (
            OperationState::Rejected,
            FailureClass::InvalidRequest,
            RoutingOutcomeKind::LocalOnly,
            "session_expired",
            false,
        ),
        ServerError::Core(_)
        | ServerError::Catalog(_)
        | ServerError::Io(_)
        | ServerError::Internal(_) => (
            OperationState::TerminalFailure,
            FailureClass::Internal,
            RoutingOutcomeKind::Unavailable,
            "internal",
            false,
        ),
    };
    HttpTransactionOutcome::new(
        transaction_id,
        request_id,
        transaction_metadata_version(state),
        operation_state,
        Some(failure_class),
        Some(reason_code.to_string()),
        routing_kind,
        reason_code,
        retryable,
    )
}

fn make_trace_span<B>(request: &axum::http::Request<B>) -> Span {
    let correlation_id = request
        .extensions()
        .get::<RequestContext>()
        .map(|ctx| ctx.correlation_id.clone())
        .or_else(|| extract_correlation_id(request.headers()))
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let traceparent = request
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    tracing::info_span!(
        "http_request",
        correlation_id = %correlation_id,
        traceparent = %traceparent,
        method = %request.method(),
        path = %request.uri().path()
    )
}

pub fn json_response<T: Serialize>(value: T, max_size: usize, ctx: &RequestContext) -> Response {
    match serde_json::to_vec(&value) {
        Ok(bytes) if bytes.len() <= max_size => (StatusCode::OK, Json(value)).into_response(),
        Ok(_) => error_response(
            ServerError::PayloadTooLarge("response size exceeds limit".into()),
            ctx,
        ),
        Err(err) => error_response(ServerError::Internal(err.to_string()), ctx),
    }
}

/// Serialize a successful transaction adapter result without allowing a
/// post-execution response-size failure to rewrite its known outcome as an
/// unexecuted invalid request. The legacy HTTP error remains 413/500 while the
/// additive transaction field reports the operation state reached before
/// serialization failed.
pub fn transaction_json_response<T: Serialize>(
    value: T,
    transaction: HttpTransactionOutcome,
    max_size: usize,
    ctx: &RequestContext,
) -> Response {
    match serde_json::to_vec(&value) {
        Ok(bytes) if bytes.len() <= max_size => (StatusCode::OK, Json(value)).into_response(),
        Ok(_) => transaction_error_response(
            ServerError::PayloadTooLarge("response size exceeds limit".into()),
            transaction,
            ctx,
        ),
        Err(error) => {
            transaction_error_response(ServerError::Internal(error.to_string()), transaction, ctx)
        }
    }
}

fn extract_correlation_id(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .or_else(|| {
            headers
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string())
        })
}
