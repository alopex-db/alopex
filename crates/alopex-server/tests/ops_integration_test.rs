use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tokio::time::{sleep, Duration as TokioDuration};
use tower::ServiceExt;

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(5),
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
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
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

async fn send_empty(router: axum::Router, method: Method, path: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
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

async fn wait_for_backup(router: axum::Router, handle: &str) -> Value {
    let path = format!("/api/admin/backup/{handle}");
    for _ in 0..50 {
        let (status, body) = send_empty(router.clone(), Method::GET, &path).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_slice(&body).expect("backup status json");
        let state = value.get("state").cloned().expect("state");
        let status_value = state
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if status_value != "running" && status_value != "queued" {
            return state;
        }
        sleep(TokioDuration::from_millis(50)).await;
    }
    panic!("backup did not complete in time");
}

async fn wait_for_restore(router: axum::Router, handle: &str) -> Value {
    let path = format!("/api/admin/restore/{handle}");
    for _ in 0..50 {
        let (status, body) = send_empty(router.clone(), Method::GET, &path).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_slice(&body).expect("restore status json");
        let state = value.get("state").cloned().expect("state");
        let status_value = state
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if status_value != "running" && status_value != "queued" {
            return value;
        }
        sleep(TokioDuration::from_millis(50)).await;
    }
    panic!("restore did not complete in time");
}

#[tokio::test]
async fn backup_restore_flow_reports_status_and_metadata() {
    let (state, _temp) = build_state().await;
    let router = http::router(state.clone());

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, name) VALUES (1, 'alpha');"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(router.clone(), Method::POST, "/api/admin/backup").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("backup json");
    let handle = value
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("handle");
    let location = value
        .get("location")
        .and_then(|v| v.as_str())
        .expect("location");

    let state_value = wait_for_backup(router.clone(), handle).await;
    assert_eq!(
        state_value.get("status").and_then(|v| v.as_str()),
        Some("completed")
    );
    assert!(Path::new(location).exists());

    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/restore",
        json!({ "source": location }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("restore json");
    let restore_handle = value
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("handle");

    let restore_value = wait_for_restore(router.clone(), restore_handle).await;
    let restore_state = restore_value.get("state").expect("restore state");
    assert_eq!(
        restore_state.get("status").and_then(|v| v.as_str()),
        Some("completed")
    );
    let metadata = restore_value.get("metadata").expect("metadata");
    assert!(metadata.get("backup_id").and_then(|v| v.as_str()).is_some());
    assert!(metadata.get("location").and_then(|v| v.as_str()).is_some());
    assert!(metadata
        .get("restored_at_ms")
        .and_then(|v| v.as_u64())
        .is_some());
    assert!(metadata
        .get("size_bytes")
        .and_then(|v| v.as_u64())
        .is_some());
}

#[tokio::test]
async fn status_payload_includes_operational_fields() {
    let (state, _temp) = build_state().await;
    let router = http::router(state.clone());

    let (status, body) = send_empty(router, Method::GET, "/api/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("status json");
    assert!(value.get("overall_status").is_some());
    assert!(value.get("read_only").is_some());
    assert!(value.get("maintenance").is_some());
    assert!(value.get("recovery_state").is_some());
    assert!(value.get("backup_state").is_some());
    assert!(value.get("restore_state").is_some());
}
