use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use futures::future::join_all;
use serde_json::{json, Value};
use tempfile::tempdir;
use tokio::time::sleep;
use tower::ServiceExt;

async fn build_state(query_timeout: Duration) -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout,
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp)
}

async fn send_json(router: axum::Router, path: &str, body: Value) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
    (status, body.to_vec())
}

async fn send_get(router: axum::Router, path: &str) -> StatusCode {
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    response.status()
}

fn parse_metric(metrics: &str, name: &str) -> Option<f64> {
    for line in metrics.lines() {
        if line.starts_with(name) {
            let mut parts = line.split_whitespace();
            let _ = parts.next();
            if let Some(value) = parts.next() {
                if let Ok(parsed) = value.parse::<f64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn load_test_concurrent_requests() {
    let (state, _temp) = build_state(Duration::from_secs(5)).await;
    let router = http::admin_router(state.clone());

    let request_count = 1000usize;
    let start = Instant::now();
    let futures = (0..request_count).map(|_| send_get(router.clone(), "/healthz"));
    let results = join_all(futures).await;
    let elapsed = start.elapsed();
    assert!(results.iter().all(|status| *status == StatusCode::OK));

    let min_ops = std::env::var("ALOPEX_LOAD_TEST_MIN_OPS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(50.0);
    let ops_per_sec = request_count as f64 / elapsed.as_secs_f64().max(0.000_001);
    assert!(
        ops_per_sec >= min_ops,
        "throughput too low: {ops_per_sec:.2} ops/sec < {min_ops:.2}"
    );

    let mut active_connections = None;
    for _ in 0..10 {
        let metrics = state.metrics.expose_prometheus().expect("metrics");
        active_connections = parse_metric(&metrics, "active_connections");
        if matches!(active_connections, Some(value) if value <= 0.0) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let active_connections = active_connections.expect("active_connections metric");
    assert!(
        active_connections <= 0.0,
        "active_connections should be 0 after load, got {active_connections}"
    );
}

#[tokio::test]
async fn load_test_backpressure_with_slow_client() {
    let (state, _temp) = build_state(Duration::from_secs(5)).await;
    let router = http::router(state.clone());

    let (status, _) = send_json(
        router.clone(),
        "/sql",
        json!({
            "sql": "CREATE TABLE items (id INT PRIMARY KEY, value TEXT);"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut values = String::new();
    for id in 0..2000 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'v{}')", id, id));
    }
    let insert_sql = format!("INSERT INTO items (id, value) VALUES {values};");
    let (status, _) = send_json(router.clone(), "/sql", json!({ "sql": insert_sql })).await;
    assert_eq!(status, StatusCode::OK);

    let metrics_before = state.metrics.expose_prometheus().expect("metrics");
    let before = parse_metric(&metrics_before, "stream_backpressure").unwrap_or(0.0);

    let request = Request::builder()
        .method(Method::POST)
        .uri("/sql")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "sql": "SELECT id FROM items ORDER BY id;",
                "streaming": true
            })
            .to_string(),
        ))
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    sleep(Duration::from_millis(200)).await;
    let metrics_after = state.metrics.expose_prometheus().expect("metrics");
    let after = parse_metric(&metrics_after, "stream_backpressure").unwrap_or(0.0);
    assert!(after > before);

    let _ = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
}

#[tokio::test]
async fn load_test_timeout_cancels_stream() {
    let (state, _temp) = build_state(Duration::from_millis(0)).await;
    let router = http::router(state.clone());

    let (status, body) = send_json(
        router,
        "/sql",
        json!({
            "sql": "SELECT 1;",
            "streaming": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf8");
    let mut saw_timeout = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let item: Value = serde_json::from_str(line).expect("json");
        if let Some(error) = item.get("error") {
            if error.get("code").and_then(|v| v.as_str()) == Some("QUERY_TIMEOUT") {
                saw_timeout = true;
                break;
            }
        }
    }
    assert!(saw_timeout);
}
