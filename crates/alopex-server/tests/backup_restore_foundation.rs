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
use tokio::time::{sleep, Duration as TokioDuration, Instant as TokioInstant};
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, body.to_vec())
}

async fn wait_for_restore(router: axum::Router, handle: &str) -> Value {
    let path = format!("/api/admin/restore/{handle}");
    let timeout = if cfg!(windows) {
        TokioDuration::from_secs(60)
    } else {
        TokioDuration::from_secs(20)
    };
    let deadline = TokioInstant::now() + timeout;
    loop {
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
        if TokioInstant::now() >= deadline {
            break;
        }
        sleep(TokioDuration::from_millis(100)).await;
    }
    panic!("restore did not complete in time");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn export_restore_consistency_verify() {
    let (state, temp) = build_state().await;
    let router = http::router(state.clone());

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "INSERT INTO items (id, name) VALUES (1, 'alpha');" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(router.clone(), Method::POST, "/api/admin/export").await;
    assert_eq!(status, StatusCode::OK);
    let export_value: Value = serde_json::from_slice(&body).expect("export json");
    let export_location = export_value
        .get("location")
        .and_then(|v| v.as_str())
        .expect("export location");
    assert!(Path::new(export_location).exists());
    assert!(Path::new(export_location)
        .join("snapshot.manifest")
        .exists());

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "INSERT INTO items (id, name) VALUES (2, 'beta');" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/restore",
        json!({ "source": export_location }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let restore_value: Value = serde_json::from_slice(&body).expect("restore start json");
    let restore_handle = restore_value
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("restore handle");
    let restore_done = wait_for_restore(router.clone(), restore_handle).await;
    let restore_state = restore_done.get("state").expect("restore state");
    assert_eq!(
        restore_state.get("status").and_then(|v| v.as_str()),
        Some("completed")
    );

    drop(router);
    drop(state);

    let reloaded_server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(5),
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("reloaded server");
    let reloaded_router = http::router(reloaded_server.state.clone());

    let (status, body) = send_json(
        reloaded_router,
        Method::POST,
        "/sql",
        json!({ "sql": "SELECT id, name FROM items ORDER BY id;" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sql_value: Value = serde_json::from_slice(&body).expect("sql json");
    let rows = sql_value
        .get("rows")
        .and_then(|v| v.as_array())
        .expect("rows");
    assert_eq!(rows.len(), 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn restore_rejects_integrity_mismatch_fail_fast() {
    let (state, _temp) = build_state().await;
    let router = http::router(state.clone());

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "INSERT INTO items (id, name) VALUES (1, 'alpha');" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(router.clone(), Method::POST, "/api/admin/export").await;
    assert_eq!(status, StatusCode::OK);
    let export_value: Value = serde_json::from_slice(&body).expect("export json");
    let export_location = export_value
        .get("location")
        .and_then(|v| v.as_str())
        .expect("export location");
    std::fs::write(
        Path::new(export_location).join("snapshot.manifest"),
        b"{\"version\":1,\"entries\":[]",
    )
    .expect("corrupt manifest");

    let (status, body) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/restore",
        json!({ "source": export_location }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let error_value: Value = serde_json::from_slice(&body).expect("error json");
    assert_eq!(
        error_value
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|v| v.as_str()),
        Some("RESTORE_INTEGRITY_MISMATCH")
    );
}
