#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use alopex_cluster::{ClusterMode, MembershipSource};
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

fn default_config(data_dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: data_dir.to_path_buf(),
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        ..ServerConfig::default()
    }
}

fn open_default_server(data_dir: &std::path::Path) -> Server {
    Server::new(default_config(data_dir)).expect("default server")
}

fn assert_single_node_compatibility(state: &Arc<ServerState>) {
    let snapshot = state.cluster_status_snapshot().expect("cluster status");
    assert_eq!(snapshot.mode, ClusterMode::SingleNode);
    assert_eq!(snapshot.membership.source, MembershipSource::LocalDefault);
    assert!(snapshot.membership.members.is_empty());
    assert!(snapshot.placement.placements.is_empty());
    assert!(!snapshot.degraded);
    assert!(snapshot.routing_capabilities.local_only);

    let diagnostics = state
        .cluster_startup_diagnostics()
        .expect("startup diagnostics");
    assert_eq!(diagnostics.mode, ClusterMode::SingleNode);
    assert_eq!(diagnostics.node_id, "local");
    assert_eq!(diagnostics.cluster_id, None);
    assert_eq!(
        diagnostics.membership_source,
        MembershipSource::LocalDefault
    );
    assert!(!diagnostics.degraded);
}

async fn send_json(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = serde_json::from_slice::<Value>(&bytes).unwrap_or_else(|err| {
        panic!(
            "invalid json response ({err}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, body)
}

async fn send_empty(app: &axum::Router, method: Method, uri: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn default_server_reopens_existing_v06_data_dir_with_idempotent_metadata() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");

    {
        let server = open_default_server(&data_dir);
        assert_single_node_compatibility(&server.state);
        let app = http::router(server.state.clone());

        let (status, _) = send_json(
            &app,
            "/sql",
            json!({
                "sql": "CREATE TABLE compat_users (id INTEGER PRIMARY KEY, name TEXT)",
                "streaming": false
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = send_json(
            &app,
            "/sql",
            json!({
                "sql": "INSERT INTO compat_users (id, name) VALUES (1, 'alice')",
                "streaming": false
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    {
        let server = open_default_server(&data_dir);
        assert_single_node_compatibility(&server.state);
        let app = http::router(server.state.clone());

        let (status, status_body) = send_empty(&app, Method::GET, "/api/admin/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(status_body["cluster"]["mode"], "single_node");
        assert_eq!(status_body["cluster"]["degraded"], false);
        assert_eq!(
            status_body["cluster"]["membership"]["source"],
            "local_default"
        );
        assert_eq!(
            status_body["cluster"]["placement"]["placements"]
                .as_array()
                .expect("placements")
                .len(),
            0
        );

        let (status, body) = send_json(
            &app,
            "/sql",
            json!({
                "sql": "SELECT id, name FROM compat_users ORDER BY id",
                "streaming": false
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
        assert_eq!(body["routing_diagnostics"][0]["decision"], "local_only");
        assert_eq!(body["routing_diagnostics"][0]["reason"], "placement_absent");
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_sql_defaults_remain_local_only_without_cluster_opt_in() {
    let temp = tempdir().expect("tempdir");
    let server = open_default_server(&temp.path().join("data"));
    assert_single_node_compatibility(&server.state);
    let app = http::router(server.state.clone());

    let (status, session) = send_empty(&app, Method::POST, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session_id = session["session_id"].as_str().expect("session id");

    let (status, _) = send_json(
        &app,
        "/sql",
        json!({
            "sql": "CREATE TABLE local_session_items (id INTEGER PRIMARY KEY, label TEXT)",
            "session_id": session_id,
            "streaming": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        &app,
        "/sql",
        json!({
            "sql": "SELECT id, label FROM local_session_items",
            "session_id": session_id,
            "streaming": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["routing_diagnostics"][0]["decision"], "local_only");

    let (status, _) =
        send_empty(&app, Method::POST, &format!("/session/{session_id}/commit")).await;
    assert_eq!(status, StatusCode::OK);
}
