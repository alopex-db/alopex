use std::sync::Arc;
use std::time::Duration;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http::admission_middleware;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use futures::future::join_all;
use serde_json::Value;
use tempfile::tempdir;
use tokio::time::sleep;
use tower::ServiceExt;

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(60),
        max_concurrency: 1,
        max_queue_len: 1,
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp)
}

async fn send_post(router: Router, path: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .body(Body::empty())
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
    (status, body.to_vec())
}

async fn slow_handler() -> impl IntoResponse {
    sleep(Duration::from_millis(120)).await;
    StatusCode::OK
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn queue_overflow_rejects_immediately_and_keeps_inflight_running() {
    let (state, _temp) = build_state().await;
    let router = Router::new()
        .route("/slow", post(slow_handler))
        .layer(middleware::from_fn(admission_middleware))
        .layer(axum::Extension(state));

    let futures = (0..10).map(|_| send_post(router.clone(), "/slow"));
    let results = join_all(futures).await;

    let mut success = 0usize;
    let mut rejected = 0usize;
    for (status, body) in results {
        if status == StatusCode::OK {
            success += 1;
        } else if status == StatusCode::SERVICE_UNAVAILABLE {
            rejected += 1;
            let payload: Value = serde_json::from_slice(&body).expect("overflow json");
            let error = payload.get("error").expect("error");
            assert_eq!(
                error.get("code").and_then(|v| v.as_str()),
                Some("SERVER_BACKPRESSURE")
            );
            assert!(
                error
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| !value.is_empty()),
                "overflow response must include correlation_id"
            );
        }
    }

    assert!(success >= 1, "at least one in-flight request must complete");
    assert!(
        rejected >= 1,
        "overflow requests must be rejected with service unavailable"
    );
}
