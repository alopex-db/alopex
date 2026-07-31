#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use alopex_cluster::{
    ClusterId, ClusterIdentity, ClusterManager, ClusterManagerConfig, Endpoint, MemberIdentity,
    MemberStatus, MembershipSource, MembershipView, NodeId, NodeRole, NodeState,
    PlacementLifecycleState, PlacementMetadata, RoutingTarget, TableLifecycleEffect, TableRef,
};
use alopex_server::config::{ClusterServerConfig, ServerConfig};
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Request, StatusCode};
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

fn cluster_aware_test_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let config = ServerConfig {
        data_dir: temp_dir.path().join("data"),
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        cluster: ClusterServerConfig {
            mode: alopex_cluster::ClusterMode::ClusterAware,
            node_id: Some("node-a".to_string()),
            cluster_id: Some("cluster-a".to_string()),
            advertised_endpoint: Some("127.0.0.1:7001".to_string()),
            role: NodeRole::Worker,
            lifecycle_state: NodeState::Active,
            ..ClusterServerConfig::default()
        },
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8");
    (status, body)
}

fn table_ref_and_id(state: &ServerState, table_name: &str) -> (String, u32) {
    let guard = state.catalog.read().expect("catalog lock");
    let table = guard
        .list_tables()
        .into_iter()
        .find(|table| table.name == table_name)
        .expect("table metadata");
    (
        format!(
            "{}.{}.{}",
            table.catalog_name, table.namespace_name, table.name
        ),
        table.table_id,
    )
}

fn install_multi_node_placement(state: &ServerState, table_ref: &str, table_id: u32) {
    install_placement(state, table_ref, table_id, &["node-a", "node-b"]);
}

fn install_single_node_placement(state: &ServerState, table_ref: &str, table_id: u32) {
    install_placement(state, table_ref, table_id, &["node-a"]);
}

fn install_placement(state: &ServerState, table_ref: &str, table_id: u32, nodes: &[&str]) {
    let mut placement = PlacementMetadata::new(table_ref, table_id, 7);
    for node in nodes {
        placement
            .targets
            .push(RoutingTarget::table(*node, table_ref, table_id));
    }

    let identity = ClusterIdentity {
        cluster_id: Some(ClusterId::new("cluster-a")),
        advertised_endpoint: Some(Endpoint::new("127.0.0.1:7001")),
        ..ClusterIdentity::new("node-a", NodeRole::Worker, NodeState::Active)
    };
    let mut membership = MembershipView::new(MembershipSource::Persisted, 7);
    for node in nodes {
        membership.members.push(member(node));
    }

    let mut config = ClusterManagerConfig::cluster_aware(identity);
    config.membership_source = MembershipSource::Persisted;
    config.initial_membership = Some(membership);
    config.initial_placements = vec![placement];

    let manager = ClusterManager::new(config).expect("cluster manager");
    *state.cluster_manager.write().expect("cluster manager lock") = manager;
}

fn placement_state(state: &ServerState, table_ref: &str, table_id: u32) -> PlacementLifecycleState {
    state
        .cluster_status_snapshot()
        .expect("cluster status")
        .placement
        .placements
        .into_iter()
        .find(|placement| {
            placement.table_ref.as_str() == table_ref && placement.table_id == table_id
        })
        .expect("placement")
        .lifecycle_state
}

fn member(node_id: &str) -> MemberStatus {
    let endpoint = match node_id {
        "node-a" => "127.0.0.1:7001",
        "node-b" => "127.0.0.1:7002",
        _ => "127.0.0.1:7999",
    };
    MemberStatus {
        identity: MemberIdentity {
            node_id: NodeId::new(node_id),
            cluster_id: Some(ClusterId::new("cluster-a")),
            advertised_endpoint: Some(Endpoint::new(endpoint)),
            role: NodeRole::Worker,
        },
        raw_reachability_state: None,
        derived_state: NodeState::Active,
        transition_reason: Some("test".to_string()),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn non_session_sql_returns_local_only_routing_diagnostics() {
    let (state, _temp_dir) = test_state();
    let app = http::router(state);

    let create = serde_json::json!({
        "sql": "CREATE TABLE route_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM route_users",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::OK);

    let response = serde_json::from_str::<Value>(&body).expect("select");
    let diagnostics = response["routing_diagnostics"]
        .as_array()
        .expect("routing diagnostics");
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0]["decision"], "local_only");
    assert_eq!(diagnostics[0]["reason"], "placement_absent");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn non_session_sql_rejects_future_distributed_routing() {
    let (state, _temp_dir) = cluster_aware_test_state();
    let app = http::router(state.clone());

    let create = serde_json::json!({
        "sql": "CREATE TABLE distributed_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let (table_ref, table_id) = table_ref_and_id(&state, "distributed_users");
    install_multi_node_placement(&state, &table_ref, table_id);

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM distributed_users",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let response = serde_json::from_str::<Value>(&body).expect("error");
    assert_eq!(
        response["error"]["code"],
        "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"
    );
    assert!(response["error"]["message"]
        .as_str()
        .expect("message")
        .contains("FutureDistributedExecutionRequired"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn non_session_streaming_sql_rejects_future_distributed_routing_before_http_ok() {
    let (state, _temp_dir) = cluster_aware_test_state();
    let app = http::router(state.clone());

    let create = serde_json::json!({
        "sql": "CREATE TABLE distributed_stream_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let (table_ref, table_id) = table_ref_and_id(&state, "distributed_stream_users");
    install_multi_node_placement(&state, &table_ref, table_id);

    let stream = serde_json::json!({
        "sql": "SELECT id, name FROM distributed_stream_users",
        "streaming": true,
        "request_id": "future-distributed-stream"
    });
    let (status, body) = send_json(&app, "/sql", stream).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let response = serde_json::from_str::<Value>(&body).expect("error");
    assert_eq!(
        response["error"]["code"],
        "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"
    );
    assert_eq!(response["transaction"]["state"], "rejected");
    assert_eq!(response["transaction"]["routing"]["kind"], "unsupported");
    assert!(
        response.get("done").is_none(),
        "routing rejection must precede a JSONL stream success envelope"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn future_distributed_write_is_rejected_before_local_execution() {
    let (state, _temp_dir) = cluster_aware_test_state();
    let app = http::router(state.clone());

    let create = serde_json::json!({
        "sql": "CREATE TABLE distributed_writes (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let (table_ref, table_id) = table_ref_and_id(&state, "distributed_writes");
    install_multi_node_placement(&state, &table_ref, table_id);

    let insert = serde_json::json!({
        "sql": "INSERT INTO distributed_writes (id, name) VALUES (1, 'blocked')",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", insert).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let response = serde_json::from_str::<Value>(&body).expect("error");
    assert_eq!(
        response["error"]["code"],
        "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"
    );

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let session_id = session["session_id"].as_str().expect("session_id");

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM distributed_writes",
        "session_id": session_id,
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let response = serde_json::from_str::<Value>(&body).expect("error");
    assert_eq!(
        response["error"]["code"],
        "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"
    );

    let stream = serde_json::json!({
        "sql": "SELECT id, name FROM distributed_writes",
        "session_id": session_id,
        "streaming": true,
        "request_id": "session-future-distributed-stream"
    });
    let (status, body) = send_json(&app, "/sql", stream).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let response = serde_json::from_str::<Value>(&body).expect("stream error");
    assert_eq!(
        response["error"]["code"],
        "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"
    );
    assert_eq!(response["transaction"]["state"], "rejected");
    assert_eq!(response["transaction"]["routing"]["kind"], "unsupported");

    let (status, _) = send_empty(&app, &format!("/session/{session_id}/rollback")).await;
    assert_eq!(status, StatusCode::OK);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_multi_statement_returns_result_per_statement() {
    let (state, _temp_dir) = test_state();
    let app = http::router(state);

    let (status, body) = send_json(
        &app,
        "/sql",
        serde_json::json!({
            "sql": "CREATE TABLE multi_statement_users (id INTEGER PRIMARY KEY)",
            "streaming": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create response body: {body}");

    let (status, body) = send_json(
        &app,
        "/sql",
        serde_json::json!({
            "sql": "INSERT INTO multi_statement_users (id) VALUES (1); SELECT id FROM multi_statement_users;",
            "streaming": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "response body: {body}");
    let response = serde_json::from_str::<Value>(&body).expect("multi-statement response");
    let results = response["results"]
        .as_array()
        .expect("HTTP response must expose per-statement results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["affected_rows"], 1);
    assert_eq!(results[1]["rows"].as_array().expect("select rows").len(), 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_ddl_then_select_uses_same_transaction_catalog_view() {
    let (state, _temp_dir) = test_state();
    let app = http::router(state);

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let session_id = session["session_id"].as_str().expect("session_id");

    let create = serde_json::json!({
        "sql": "CREATE TABLE session_ddl_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "session_id": session_id,
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM session_ddl_users",
        "session_id": session_id,
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::OK);
    let response = serde_json::from_str::<Value>(&body).expect("select");
    assert_eq!(response["rows"].as_array().expect("rows").len(), 0);
    assert_eq!(response["routing_diagnostics"][0]["decision"], "local_only");

    let (status, _) = send_empty(&app, &format!("/session/{session_id}/commit")).await;
    assert_eq!(status, StatusCode::OK);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_ddl_commit_applies_placement_effects_idempotently() {
    let (state, _temp_dir) = cluster_aware_test_state();
    let app = http::router(state.clone());

    let create = serde_json::json!({
        "sql": "CREATE TABLE lifecycle_commit_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);
    let (table_ref, table_id) = table_ref_and_id(&state, "lifecycle_commit_users");
    install_single_node_placement(&state, &table_ref, table_id);

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let session_id = session["session_id"].as_str().expect("session_id");

    let drop_table = serde_json::json!({
        "sql": "DROP TABLE lifecycle_commit_users",
        "session_id": session_id,
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", drop_table).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        placement_state(&state, &table_ref, table_id),
        PlacementLifecycleState::Active
    );

    let (status, _) = send_empty(&app, &format!("/session/{session_id}/commit")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        placement_state(&state, &table_ref, table_id),
        PlacementLifecycleState::Tombstoned
    );

    state
        .apply_table_lifecycle_effects(vec![TableLifecycleEffect::Dropped {
            table_ref: TableRef::new(table_ref.clone()),
            table_id,
        }])
        .expect("repeat lifecycle effect");
    assert_eq!(
        placement_state(&state, &table_ref, table_id),
        PlacementLifecycleState::Tombstoned
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_ddl_rollback_discards_placement_effects() {
    let (state, _temp_dir) = cluster_aware_test_state();
    let app = http::router(state.clone());

    let create = serde_json::json!({
        "sql": "CREATE TABLE lifecycle_rollback_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", create).await;
    assert_eq!(status, StatusCode::OK);
    let (table_ref, table_id) = table_ref_and_id(&state, "lifecycle_rollback_users");
    install_single_node_placement(&state, &table_ref, table_id);

    let (status, body) = send_empty(&app, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session = serde_json::from_str::<Value>(&body).expect("session");
    let session_id = session["session_id"].as_str().expect("session_id");

    let drop_table = serde_json::json!({
        "sql": "DROP TABLE lifecycle_rollback_users",
        "session_id": session_id,
        "streaming": false
    });
    let (status, _) = send_json(&app, "/sql", drop_table).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_empty(&app, &format!("/session/{session_id}/rollback")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        placement_state(&state, &table_ref, table_id),
        PlacementLifecycleState::Active
    );

    let select = serde_json::json!({
        "sql": "SELECT id, name FROM lifecycle_rollback_users",
        "streaming": false
    });
    let (status, body) = send_json(&app, "/sql", select).await;
    assert_eq!(status, StatusCode::OK);
    let response = serde_json::from_str::<Value>(&body).expect("select");
    assert_eq!(response["rows"].as_array().expect("rows").len(), 0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
    assert_eq!(status, StatusCode::OK, "{body}");
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
            assert_eq!(item["transaction"]["state"], "rejected");
            assert_eq!(item["transaction"]["failure_class"], "invalid_request");
            saw_error = true;
            break;
        }
    }
    assert!(saw_error);
}
