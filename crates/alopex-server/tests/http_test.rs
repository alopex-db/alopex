use std::sync::Arc;
use std::time::Duration;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

async fn build_state(
    auth_mode: AuthMode,
    query_timeout: Duration,
) -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode,
        query_timeout,
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp)
}

async fn send_json(
    router: axum::Router,
    method: Method,
    path: &str,
    body: Value,
    headers: &[(&str, &str)],
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    for (name, value) in headers {
        let header_name = axum::http::HeaderName::from_bytes(name.as_bytes()).expect("header name");
        request.headers_mut().insert(
            header_name,
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
    (status, headers, body.to_vec())
}

async fn send_empty(
    router: axum::Router,
    method: Method,
    path: &str,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
    (status, headers, body.to_vec())
}

#[tokio::test]
async fn http_sql_vector_session_flow() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(5)).await;
    let router = http::router(state.clone());

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));"
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/vector/upsert",
        json!({
            "table": "items",
            "id": 1,
            "vector": [0.0, 0.0]
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, embedding) VALUES (2, [1.0, 0.0]), (3, [0.5, 0.0]);"
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/vector/search",
        json!({
            "table": "items",
            "vector": [0.8, 0.0],
            "k": 2
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("json");
    let results = value
        .get("results")
        .and_then(|v| v.as_array())
        .expect("results");
    assert_eq!(results.len(), 2);

    let (status, _, body) = send_empty(router.clone(), Method::POST, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session_body: Value = serde_json::from_slice(&body).expect("session json");
    let session_id = session_body
        .get("session_id")
        .and_then(|v| v.as_str())
        .expect("session id");

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, embedding) VALUES (4, [0.2, 0.0]);",
            "session_id": session_id
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_empty(
        router.clone(),
        Method::POST,
        &format!("/session/{}/commit", session_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "SELECT id FROM items ORDER BY id;",
            "streaming": true
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf8");
    let mut rows = Vec::new();
    let mut done = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let item: Value = serde_json::from_str(line).expect("jsonl");
        if item.get("done").and_then(|v| v.as_bool()) == Some(true) {
            done = true;
            continue;
        }
        if let Some(row) = item.get("row") {
            rows.push(row.clone());
        }
    }
    assert!(done);
    assert!(rows.len() >= 3);
}

#[tokio::test]
async fn http_session_rollback_discards_changes() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(5)).await;
    let router = http::router(state.clone());

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "CREATE TABLE items (id INT PRIMARY KEY, value TEXT);"
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send_empty(router.clone(), Method::POST, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session_body: Value = serde_json::from_slice(&body).expect("session json");
    let session_id = session_body
        .get("session_id")
        .and_then(|v| v.as_str())
        .expect("session id");

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, value) VALUES (1, 'shadow');",
            "session_id": session_id
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_empty(
        router.clone(),
        Method::POST,
        &format!("/session/{}/rollback", session_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "SELECT id FROM items WHERE id = 1;"
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).expect("sql json");
    let rows = response
        .get("rows")
        .and_then(|v| v.as_array())
        .expect("rows");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn http_streaming_timeout_returns_error() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_millis(0)).await;
    let router = http::router(state.clone());

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/sql",
        json!({
            "sql": "SELECT 1;",
            "streaming": true
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf8");
    let mut saw_timeout = false;
    let mut saw_correlation = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let item: Value = serde_json::from_str(line).expect("jsonl");
        if let Some(error) = item.get("error") {
            if error.get("code").and_then(|v| v.as_str()) == Some("QUERY_TIMEOUT") {
                let correlation_id = error
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                assert!(!correlation_id.is_empty());
                saw_timeout = true;
                saw_correlation = true;
                break;
            }
        }
    }
    assert!(saw_timeout);
    assert!(saw_correlation);
}

#[tokio::test]
async fn http_auth_failure_includes_correlation_id() {
    let (state, _temp) = build_state(
        AuthMode::Dev {
            api_key: "secret".to_string(),
        },
        Duration::from_secs(5),
    )
    .await;
    let router = http::router(state.clone());

    let (status, headers, body) = send_empty(router, Method::POST, "/session/begin").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let value: Value = serde_json::from_slice(&body).expect("json");
    let correlation_id = value
        .get("error")
        .and_then(|v| v.get("correlation_id"))
        .and_then(|v| v.as_str())
        .expect("correlation id");
    assert!(!correlation_id.is_empty());
    let _ = headers;
}
