//! HTTP timeout contract with one constant query and a virtual clock.

use std::time::Duration;

use alopex_server::config::ServerConfig;
use alopex_server::{http, Server};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn expired_http_stream_reports_one_timeout_and_ends() {
    let temp = tempfile::tempdir().expect("tempdir");
    let deadline = Duration::from_secs(5);
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("data"),
        query_timeout: deadline,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    let router = http::router(server.state);
    let request = Request::builder()
        .method("POST")
        .uri("/sql")
        .header("content-type", "application/json")
        .header("x-correlation-id", "bounded-stream-deadline")
        .body(Body::from(
            json!({"sql": "SELECT 1;", "streaming": true}).to_string(),
        ))
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    // Expire the absolute deadline before consuming the body. No data volume,
    // wall-clock speed, database reopen, or producer scheduling is required.
    tokio::time::advance(deadline).await;
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("bounded terminal body");
    let text = std::str::from_utf8(&body).expect("UTF-8");
    let items: Vec<Value> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("NDJSON item"))
        .collect();
    assert_eq!(items.len(), 1, "timeout must be terminal: {text}");
    assert_eq!(items[0]["error"]["code"], "QUERY_TIMEOUT");
    assert_eq!(
        items[0]["error"]["correlation_id"],
        "bounded-stream-deadline"
    );
    assert!(items[0]["row"].is_null());
}
