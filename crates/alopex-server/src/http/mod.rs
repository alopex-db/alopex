pub mod admin;
pub mod admin_api;
pub mod admin_resources;
pub mod columnar;
pub mod crdt;
pub mod hnsw;
pub mod kv;
pub mod session;
pub mod sql;
pub mod vector;

use std::sync::atomic::Ordering;
use std::sync::Arc;

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
            return auth_error_response(err, &correlation_id);
        }
    };

    req.extensions_mut().insert(RequestContext {
        correlation_id: correlation_id.clone(),
        actor,
    });

    let mut res = next.run(req).await;
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

fn auth_error_response(err: AuthError, correlation_id: &str) -> Response {
    let message = err.to_string();
    let body = Json(ErrorResponse {
        error: ErrorBody {
            code: "UNAUTHORIZED".to_string(),
            message,
            correlation_id: correlation_id.to_string(),
        },
    });
    (StatusCode::UNAUTHORIZED, body).into_response()
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
