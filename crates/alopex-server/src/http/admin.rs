use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Extension};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::Serialize;

use crate::server::ServerState;

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
}

pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/status", axum::routing::get(status))
        .route("/metrics", axum::routing::get(metrics))
        .layer(middleware::from_fn(allowlist_middleware))
        .layer(axum::Extension(state))
}

async fn healthz() -> impl IntoResponse {
    StatusCode::OK
}

async fn status() -> impl IntoResponse {
    Json(StatusResponse { status: "ok" })
}

async fn metrics(Extension(state): Extension<Arc<ServerState>>) -> Response {
    if !state.config.metrics_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.metrics.expose_prometheus() {
        Ok(body) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            body,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn allowlist_middleware<B>(
    Extension(state): Extension<Arc<ServerState>>,
    req: axum::http::Request<B>,
    next: middleware::Next<B>,
) -> Response {
    if state.config.admin_bind.ip().is_loopback() {
        return next.run(req).await;
    }
    let Some(addr) = req.extensions().get::<ConnectInfo<SocketAddr>>() else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let ip = addr.ip();
    if ip.is_loopback() || state.config.admin_allowlist.contains(&ip) {
        next.run(req).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}
