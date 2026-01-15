pub mod admin;
pub mod columnar;
pub mod hnsw;
pub mod kv;
pub mod session;
pub mod sql;
pub mod vector;

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

pub fn router(state: Arc<ServerState>) -> Router {
    let api = Router::new()
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
        .route("/session/begin", axum::routing::post(session::begin))
        .route("/session/:id/commit", axum::routing::post(session::commit))
        .route(
            "/session/:id/rollback",
            axum::routing::post(session::rollback),
        );

    let api = if state.config.api_prefix.is_empty() {
        api
    } else {
        Router::new().nest(&state.config.api_prefix, api)
    };

    let middleware = middleware::from_fn(context_middleware);
    let connection_middleware = middleware::from_fn(connection_middleware);
    api.layer(
        ServiceBuilder::new()
            .layer(RequestBodyLimitLayer::new(state.config.max_request_size))
            .layer(tower::limit::ConcurrencyLimitLayer::new(
                state.config.max_connections,
            ))
            .layer(TraceLayer::new_for_http().make_span_with(make_trace_span))
            .layer(middleware)
            .layer(connection_middleware),
    )
    .layer(axum::Extension(state))
}

pub fn admin_router(state: Arc<ServerState>) -> Router {
    admin::router(state)
}

pub async fn context_middleware<B>(
    axum::extract::Extension(state): axum::extract::Extension<Arc<ServerState>>,
    mut req: axum::http::Request<B>,
    next: middleware::Next<B>,
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

pub async fn connection_middleware<B>(
    axum::extract::Extension(state): axum::extract::Extension<Arc<ServerState>>,
    req: axum::http::Request<B>,
    next: middleware::Next<B>,
) -> Response {
    state.metrics.record_connection(1);
    let res = next.run(req).await;
    state.metrics.record_connection(-1);
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
