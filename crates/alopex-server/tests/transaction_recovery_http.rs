//! HTTP checks for the fixed F4 recovery register.

use std::sync::Arc;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::{http, Server};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: u32,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    request_id: String,
    coverage: Vec<String>,
}

fn manifest() -> Manifest {
    serde_json::from_str(include_str!("../../../tests/fixtures/f4_recovery.json"))
        .expect("F4 recovery manifest must be valid JSON")
}

fn server_at(data_dir: std::path::PathBuf) -> Arc<alopex_server::server::ServerState> {
    Server::new(ServerConfig {
        data_dir,
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server")
    .state
}

async fn send(router: axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("JSON response")
    };
    (status, value)
}

fn assert_pre_execution_unsupported(response: &Value, fixture: &Fixture, duplicate_count: u64) {
    assert_eq!(
        response["error"]["code"], "NOT_IMPLEMENTED",
        "{}",
        fixture.id
    );
    assert_eq!(response["transaction"]["request_id"], fixture.request_id);
    assert_eq!(response["transaction"]["state"], "rejected");
    assert_eq!(response["transaction"]["failure_class"], "invalid_request");
    assert_eq!(response["transaction"]["routing"]["kind"], "unsupported");
    assert_eq!(response["transaction"]["reason_code"], "unsupported");
    assert_eq!(
        response["transaction"]["idempotency"]["duplicate_count"],
        json!(duplicate_count)
    );
}

#[tokio::test]
async fn recovery_fixture_http_requests_are_durable_pre_execution_rejections() {
    let manifest = manifest();
    assert_eq!(manifest.schema_version, 1);
    let data = tempdir().expect("tempdir");
    let data_dir = data.path().join("server-data");
    let first_state = server_at(data_dir.clone());
    let first_router = http::router(first_state.clone());

    for fixture in &manifest.fixtures {
        let body = json!({
            "request_id": fixture.request_id,
            "require_distributed": true
        });
        let (status, first) = send(first_router.clone(), "/kv/txn/begin", body.clone()).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{}", fixture.id);
        assert_pre_execution_unsupported(&first, fixture, 0);
        let (status, replay) = send(first_router.clone(), "/kv/txn/begin", body).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{}", fixture.id);
        assert_pre_execution_unsupported(&replay, fixture, 1);
    }

    let restart = manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.coverage.iter().any(|item| item == "restart"))
        .expect("restart fixture");
    drop(first_router);
    drop(first_state);
    let restarted_router = http::router(server_at(data_dir));
    let (status, replay) = send(
        restarted_router,
        "/kv/txn/begin",
        json!({
            "request_id": restart.request_id,
            "require_distributed": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_pre_execution_unsupported(&replay, restart, 2);
}

#[tokio::test]
async fn cancellation_fixture_has_no_implicit_recover_or_cancel_http_operation() {
    let data = tempdir().expect("tempdir");
    let router = http::router(server_at(data.path().join("server-data")));
    for path in ["/kv/txn/recover", "/kv/txn/cancel"] {
        let (status, _) = send(router.clone(), path, json!({ "request_id": "f4-absent" })).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path} must remain absent");
    }
}
