use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::error::ServerError;
use alopex_server::http;
use alopex_server::ops::backup::{BackupCoordinator, BackupHandle};
use alopex_server::ops::restore::{RestoreCoordinator, RestoreHandle, RestoreSource};
use alopex_server::ops::state::{LifecycleStateManager, Mode, OperationState, OperationStatus};
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tokio::time::{sleep, Duration as TokioDuration, Instant as TokioInstant};
use tower::ServiceExt;
use uuid::Uuid;

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

async fn wait_for_terminal_state<F>(mut status: F, operation: &str) -> OperationState
where
    F: FnMut() -> alopex_server::error::Result<OperationState>,
{
    let timeout = if cfg!(windows) {
        // The default 512 MiB WAL must be scanned during backup. Windows hosted
        // runners have taken nearly three minutes under the full release gate,
        // so retain a finite bound without treating normal I/O as a deadlock.
        TokioDuration::from_secs(5 * 60)
    } else {
        TokioDuration::from_secs(20)
    };
    let deadline = TokioInstant::now() + timeout;
    let mut delay = TokioDuration::from_millis(10);
    loop {
        let state = status().unwrap_or_else(|err| panic!("{operation} status: {err}"));
        if state.status != OperationStatus::Running && state.status != OperationStatus::Queued {
            return state;
        }
        if TokioInstant::now() >= deadline {
            panic!(
                "{operation} did not complete in time (last status: {:?})",
                state.status
            );
        }
        sleep(delay).await;
        delay = std::cmp::min(delay.saturating_mul(2), TokioDuration::from_millis(250));
    }
}

async fn wait_for_backup(state: &ServerState, router: axum::Router, handle: &str) -> Value {
    let backup_handle = BackupHandle {
        id: Uuid::parse_str(handle).expect("backup handle UUID"),
    };
    wait_for_terminal_state(|| state.backup_coordinator.status(&backup_handle), "backup").await;

    let path = format!("/api/admin/backup/{handle}");
    let (status, body) = send_empty(router, Method::GET, &path).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("backup status json");
    value.get("state").cloned().expect("state")
}

async fn wait_for_restore(state: &ServerState, router: axum::Router, handle: &str) -> Value {
    let restore_handle = RestoreHandle {
        id: Uuid::parse_str(handle).expect("restore handle UUID"),
    };
    wait_for_terminal_state(
        || state.restore_coordinator.status(&restore_handle),
        "restore",
    )
    .await;

    let path = format!("/api/admin/restore/{handle}");
    let (status, body) = send_empty(router, Method::GET, &path).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("restore status json");
    value
}

async fn wait_for_backup_state(
    coordinator: &BackupCoordinator,
    handle: &BackupHandle,
) -> alopex_server::ops::state::OperationState {
    for _ in 0..50 {
        let state = coordinator.status(handle).expect("backup status");
        if state.status != OperationStatus::Running && state.status != OperationStatus::Queued {
            return state;
        }
        sleep(TokioDuration::from_millis(50)).await;
    }
    panic!("backup did not complete in time");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

    let state_value = wait_for_backup(state.as_ref(), router.clone(), handle).await;
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

    let restore_value = wait_for_restore(state.as_ref(), router.clone(), restore_handle).await;
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn backup_fails_on_corrupt_wal_sets_reason() {
    let temp = tempdir().expect("tempdir");
    let (store, _recovery) =
        LsmKV::open_with_config(temp.path(), LsmKVConfig::default()).expect("open lsm");
    store.checkpoint().expect("checkpoint");
    drop(store);

    let wal_path = temp.path().join("lsm.wal");
    fs::write(&wal_path, b"corrupt").expect("corrupt wal");

    let lifecycle = Arc::new(LifecycleStateManager::new(Mode::Normal));
    let checkpoint = Arc::new(|| Ok(()));
    let coordinator =
        BackupCoordinator::new(temp.path().to_path_buf(), lifecycle.clone(), checkpoint);

    let handle = coordinator.start_backup().await.expect("start backup");
    let state = wait_for_backup_state(&coordinator, &handle).await;
    assert_eq!(state.status, OperationStatus::Failed);
    assert!(state.reason.is_some());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn restore_invalid_source_sets_failed_and_read_only() {
    let temp = tempdir().expect("tempdir");
    let lifecycle = Arc::new(LifecycleStateManager::new(Mode::Normal));
    let coordinator = RestoreCoordinator::new(temp.path().to_path_buf(), lifecycle.clone());

    let source = RestoreSource {
        path: temp.path().join("missing-backup"),
    };
    let err = coordinator
        .start_restore(source)
        .await
        .expect_err("restore should fail");
    assert!(matches!(err, ServerError::NotFound(_)));
    assert_eq!(lifecycle.current_mode(), Mode::Normal);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn maintenance_blocks_writes_after_restore_start() {
    let (state, _temp) = build_state().await;
    let router = http::router(state.clone());

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let source_dir = tempdir().expect("tempdir");
    let source_file = source_dir.path().join("bulk.bin");
    fs::write(&source_file, vec![0u8; 5_000_000]).expect("write source");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/restore",
        json!({ "source": source_dir.path().display().to_string() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut in_maintenance = false;
    for _ in 0..10 {
        if state.lifecycle_state.current_mode() == Mode::Maintenance {
            in_maintenance = true;
            break;
        }
        sleep(TokioDuration::from_millis(10)).await;
    }
    assert!(in_maintenance, "restore did not enter maintenance mode");

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = send_json(
        router.clone(),
        Method::POST,
        "/kv/put",
        json!({
            "key": "blocked",
            "value": [1, 2, 3]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = send_json(
        router,
        Method::POST,
        "/vector/upsert",
        json!({
            "table": "items",
            "id": 1,
            "vector": [0.1, 0.2]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}
