#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use hyper::body::to_bytes;
use serde_json::Value;
use tower::ServiceExt;

fn test_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let config = ServerConfig {
        data_dir: temp_dir.path().join("data"),
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state.clone(), temp_dir)
}

async fn send_json(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body()).await.expect("body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8");
    (status, body)
}

async fn send_empty(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body()).await.expect("body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8");
    (status, body)
}

#[tokio::test]
async fn http_session_commit_and_rollback() {
    let (state, _temp_dir) = test_state();
    let app = http::router(state);

    let create = serde_json::json!({
        "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let session_id = session["session_id"].as_str().expect("session_id");

    let insert = serde_json::json!({
        "sql": "INSERT INTO users (id, name) VALUES (1, 'alice')",
        "session_id": session_id,
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", insert).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_empty(&app, &format!("/session/{session_id}/commit")).await;
    assert_eq!(status, StatusCode::OK);

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM users",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::OK);
    let response = serde_json::from_str::<Value>(&body).expect("select");
    let rows = response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1);

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let rollback_id = session["session_id"].as_str().expect("session_id");

    let insert = serde_json::json!({
        "sql": "INSERT INTO users (id, name) VALUES (2, 'ghost')",
        "session_id": rollback_id,
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", insert).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_empty(&app, &format!("/session/{rollback_id}/rollback")).await;
    assert_eq!(status, StatusCode::OK);

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM users",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::OK);
    let response = serde_json::from_str::<Value>(&body).expect("select");
    let rows = response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn http_streaming_select_and_error_propagation() {
    let (state, _temp_dir) = test_state();
    let app = http::router(state);

    let create = serde_json::json!({
        "sql": "CREATE TABLE stream_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let insert = serde_json::json!({
        "sql": "INSERT INTO stream_users (id, name) VALUES (1, 'alpha')",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", insert).await;
    assert_eq!(status, StatusCode::OK);

    let stream = serde_json::json!({
        "sql": "SELECT id, name FROM stream_users",
        "streaming": true
    });
    let (status, body) = send_json(&app, "/sql", stream).await;
    assert_eq!(status, StatusCode::OK);
    let mut saw_row = false;
    let mut saw_done = false;
    for line in body.lines() {
        let item = serde_json::from_str::<Value>(line).expect("stream item");
        if item["done"].as_bool().unwrap_or(false) {
            saw_done = true;
        }
        if item["row"].is_array() {
            saw_row = true;
        }
    }
    assert!(saw_row);
    assert!(saw_done);

    let stream = serde_json::json!({
        "sql": "SELECT missing FROM stream_users",
        "streaming": true
    });
    let (status, body) = send_json(&app, "/sql", stream).await;
    assert_eq!(status, StatusCode::OK);
    let mut saw_error = false;
    for line in body.lines() {
        let item = serde_json::from_str::<Value>(line).expect("stream item");
        if !item["error"].is_null() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error);
}
