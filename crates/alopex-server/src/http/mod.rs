pub mod admin;
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
        .route("/sql", axum::routing::post(sql::handle))
        .route("/vector/search", axum::routing::post(vector::search))
        .route("/vector/upsert", axum::routing::post(vector::upsert))
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
    api.layer(
        ServiceBuilder::new()
            .layer(RequestBodyLimitLayer::new(state.config.max_request_size))
            .layer(tower::limit::ConcurrencyLimitLayer::new(
                state.config.max_connections,
            ))
            .layer(TraceLayer::new_for_http())
            .layer(middleware),
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
