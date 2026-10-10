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
    for executing_prefix in ["", "EXPLAIN ANALYZE "] {
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
        for (nonexecuting_prefix, expected_status) in [
            ("EXPLAIN ", StatusCode::OK),
            ("EXPLAIN ANALYZE EXPLAIN ", StatusCode::BAD_REQUEST),
        ] {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({
                    "sql": format!("{nonexecuting_prefix}DROP TABLE lifecycle_commit_users"),
                    "streaming": false
                }),
            )
            .await;
            assert_eq!(status, expected_status, "{body}");
            if expected_status == StatusCode::BAD_REQUEST {
                assert!(body.contains("ALOPEX-P001"), "{body}");
            }
            assert_eq!(
                placement_state(&state, &table_ref, table_id),
                PlacementLifecycleState::Active
            );
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({
                    "sql": "SELECT id, name FROM lifecycle_commit_users",
                    "streaming": false
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }

        let (status, body) = send_empty(&app, "/session/begin").await;
        assert_eq!(status, StatusCode::OK);
        let session = serde_json::from_str::<Value>(&body).expect("session");
        let session_id = session["session_id"].as_str().expect("session_id");

        let drop_table = serde_json::json!({
            "sql": format!("{executing_prefix}DROP TABLE lifecycle_commit_users"),
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
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_ddl_rollback_discards_placement_effects() {
    for executing_prefix in ["", "EXPLAIN ANALYZE "] {
        let (state, _temp_dir) = cluster_aware_test_state();
        let app = http::router(state.clone());

        let create = serde_json::json!({
            "sql": "CREATE TABLE lifecycle_rollback_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "streaming": false
        });
        let (status, _) = send_json(&app, "/sql", create).await;
        assert_eq!(status, StatusCode::OK);
        let (table_ref, table_id) = table_ref_and_id(&state, "lifecycle_rollback_users");
        for sql in [
            "INSERT INTO lifecycle_rollback_users VALUES (1, 'alpha'), (2, 'beta')",
            "CREATE INDEX lifecycle_rollback_name_idx ON lifecycle_rollback_users (name)",
        ] {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": sql, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        // Install the active placement after fixture DDL, which invalidates placement.
        install_single_node_placement(&state, &table_ref, table_id);
        for (nonexecuting_prefix, expected_status) in [
            ("EXPLAIN ", StatusCode::OK),
            ("EXPLAIN ANALYZE EXPLAIN ", StatusCode::BAD_REQUEST),
        ] {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({
                    "sql": format!("{nonexecuting_prefix}DROP TABLE lifecycle_rollback_users"),
                    "streaming": false
                }),
            )
            .await;
            assert_eq!(status, expected_status, "{body}");
            if expected_status == StatusCode::BAD_REQUEST {
                assert!(body.contains("ALOPEX-P001"), "{body}");
            }
            assert_eq!(
                placement_state(&state, &table_ref, table_id),
                PlacementLifecycleState::Active
            );
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({
                    "sql": "SELECT id, name FROM lifecycle_rollback_users",
                    "streaming": false
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }

        let (status, body) = send_empty(&app, "/session/begin").await;
        assert_eq!(status, StatusCode::OK);
        let session = serde_json::from_str::<Value>(&body).expect("session");
        let session_id = session["session_id"].as_str().expect("session_id");

        let drop_table = serde_json::json!({
            "sql": format!("{executing_prefix}DROP TABLE lifecycle_rollback_users"),
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
            "sql": "SELECT id, name FROM lifecycle_rollback_users ORDER BY id",
            "streaming": false
        });
        let (status, body) = send_json(&app, "/sql", select).await;
        assert_eq!(status, StatusCode::OK);
        let response = serde_json::from_str::<Value>(&body).expect("select");
        assert_eq!(
            response["rows"],
            serde_json::json!([
                [{"Integer": 1}, {"Text": "alpha"}],
                [{"Integer": 2}, {"Text": "beta"}]
            ])
        );
        assert!(
            state
                .catalog
                .read()
                .expect("catalog")
                .index_exists("lifecycle_rollback_name_idx"),
            "{executing_prefix}DROP TABLE rollback must restore its index"
        );
        let query = "SELECT id FROM lifecycle_rollback_users WHERE name = 'alpha'";
        let (status, body) = send_json(
            &app,
            "/sql",
            serde_json::json!({ "sql": query, "streaming": false }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let response: Value = serde_json::from_str(&body).expect("indexed rows");
        assert_eq!(response["rows"], serde_json::json!([[{"Integer": 1}]]));
        let (status, body) = send_json(
            &app,
            "/sql",
            serde_json::json!({ "sql": format!("EXPLAIN {query}"), "streaming": false }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let response: Value = serde_json::from_str(&body).expect("index plan");
        let plan = response["rows"]
            .as_array()
            .expect("plan rows")
            .iter()
            .map(|row| row[0]["Text"].as_str().expect("plan text"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains(
                "IndexScan index=lifecycle_rollback_name_idx table=lifecycle_rollback_users"
            ),
            "{plan}"
        );
    }
}

async fn assert_session_index_rollback(dropping: bool) {
    for prefix in ["", "EXPLAIN ANALYZE "] {
        let (state, _temp_dir) = test_state();
        let app = http::router(state.clone());
        let create_index = "CREATE INDEX rollback_value_idx ON rollback_index_rows (value)";
        for sql in [
            "CREATE TABLE rollback_index_rows (id INT PRIMARY KEY, value INT)",
            "INSERT INTO rollback_index_rows VALUES (1, 7), (2, 9)",
        ] {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": sql, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        if dropping {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": create_index, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (status, body) = send_empty(&app, "/session/begin").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let session: Value = serde_json::from_str(&body).expect("session");
        let session_id = session["session_id"].as_str().expect("session id");
        let ddl = if dropping {
            "DROP INDEX rollback_value_idx"
        } else {
            create_index
        };
        let (status, body) = send_json(
            &app,
            "/sql",
            serde_json::json!({
                "sql": format!("{prefix}{ddl}"),
                "session_id": session_id,
                "streaming": false
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{prefix}{ddl}: {body}");
        let (status, body) = send_empty(&app, &format!("/session/{session_id}/rollback")).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // Check live metadata, not source wording or only the rollback response.
        assert_eq!(
            state
                .catalog
                .read()
                .expect("catalog")
                .index_exists("rollback_value_idx"),
            dropping,
            "{prefix}{ddl}: rollback must restore the previous index presence"
        );
        if dropping {
            // This row-store equality predicate selects its matching single-column
            // B-tree without a row-count threshold. Verify entries as well as metadata.
            let query = "SELECT id FROM rollback_index_rows WHERE value = 7";
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": query, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let response: Value = serde_json::from_str(&body).expect("indexed rows");
            assert_eq!(response["rows"], serde_json::json!([[{"Integer": 1}]]));
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": format!("EXPLAIN {query}"), "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let response: Value = serde_json::from_str(&body).expect("index plan");
            let plan = response["rows"]
                .as_array()
                .expect("plan rows")
                .iter()
                .map(|row| row[0]["Text"].as_str().expect("plan text"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("IndexScan index=rollback_value_idx table=rollback_index_rows"),
                "{plan}"
            );
        }
        // A committed DROP proves restoration after rolled-back DROP; re-CREATE
        // proves absence after rolled-back CREATE. Neither uses IF EXISTS.
        let verify_ddl = if dropping {
            "DROP INDEX rollback_value_idx"
        } else {
            create_index
        };
        let (status, body) = send_json(
            &app,
            "/sql",
            serde_json::json!({ "sql": verify_ddl, "streaming": false }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{verify_ddl}: {body}");
        let (status, body) = send_json(
            &app,
            "/sql",
            serde_json::json!({ "sql": "SELECT id, value FROM rollback_index_rows ORDER BY id", "streaming": false }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let response: Value = serde_json::from_str(&body).expect("rows");
        assert_eq!(
            response["rows"],
            serde_json::json!([[{"Integer": 1}, {"Integer": 7}], [{"Integer": 2}, {"Integer": 9}]])
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_create_index_rollback_restores_catalog() {
    assert_session_index_rollback(false).await;
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_drop_index_rollback_restores_catalog() {
    assert_session_index_rollback(true).await;
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn session_index_undo_preserves_noops_order_and_identity() {
    use alopex_server::error::ServerError;
    use alopex_server::session::CatalogRollbackEffect;

    for prefix in ["", "EXPLAIN ANALYZE "] {
        let (state, _temp_dir) = test_state();
        let app = http::router(state.clone());
        for sql in [
            "CREATE TABLE undo_rows (id INT PRIMARY KEY, value INT)",
            "INSERT INTO undo_rows VALUES (1, 7), (2, 9)",
            "CREATE INDEX undo_value_idx ON undo_rows (value)",
        ] {
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": sql, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (original_table, original_index) = {
            let catalog = state.catalog.read().expect("catalog");
            (
                catalog.get_table("undo_rows").expect("table").clone(),
                catalog.get_index("undo_value_idx").expect("index").clone(),
            )
        };
        for statements in [
            vec![
                "CREATE INDEX IF NOT EXISTS undo_value_idx ON undo_rows (value)",
                "DROP INDEX IF EXISTS undo_missing_idx",
            ],
            vec!["DROP INDEX undo_value_idx", "DROP TABLE undo_rows"],
            vec![
                "CREATE INDEX undo_new_idx ON undo_rows (value)",
                "DROP TABLE undo_rows",
            ],
        ] {
            let (status, body) = send_empty(&app, "/session/begin").await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let session: Value = serde_json::from_str(&body).expect("session");
            let session_id = session["session_id"].as_str().expect("session id");
            // Each successful statement records its own undo before the next request.
            for statement in statements {
                let (status, body) = send_json(
                    &app,
                    "/sql",
                    serde_json::json!({
                        "sql": format!("{prefix}{statement}"),
                        "session_id": session_id, "streaming": false
                    }),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{prefix}{statement}: {body}");
            }
            let (status, body) = send_empty(&app, &format!("/session/{session_id}/rollback")).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            {
                let catalog = state.catalog.read().expect("catalog");
                assert_eq!(
                    catalog
                        .get_table("undo_rows")
                        .expect("restored table")
                        .table_id,
                    original_table.table_id
                );
                assert_eq!(
                    catalog
                        .get_index("undo_value_idx")
                        .expect("restored index")
                        .index_id,
                    original_index.index_id
                );
                assert!(!catalog.index_exists("undo_new_idx"));
                assert!(!catalog.index_exists("undo_missing_idx"));
            }
            let query = "SELECT id FROM undo_rows WHERE value = 7";
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": query, "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let response: Value = serde_json::from_str(&body).expect("rows");
            assert_eq!(response["rows"], serde_json::json!([[{"Integer": 1}]]));
            let (status, body) = send_json(
                &app,
                "/sql",
                serde_json::json!({ "sql": format!("EXPLAIN {query}"), "streaming": false }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let response: Value = serde_json::from_str(&body).expect("plan");
            let plan = response["rows"]
                .as_array()
                .expect("plan rows")
                .iter()
                .map(|row| row[0]["Text"].as_str().expect("plan text"))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("IndexScan index=undo_value_idx table=undo_rows"),
                "{plan}"
            );
        }

        // Exercise stale undo identities deterministically, without claiming a
        // concurrent-session isolation test. Existing metadata must survive.
        let assert_conflict = |effect| {
            assert!(matches!(
                state.apply_catalog_rollback_effects(vec![effect]),
                Err(ServerError::Conflict(_))
            ));
            let catalog = state.catalog.read().expect("catalog");
            assert_eq!(
                catalog.get_table("undo_rows").expect("table").table_id,
                original_table.table_id
            );
            let index = catalog.get_index("undo_value_idx").expect("index");
            assert_eq!(index.index_id, original_index.index_id);
            assert_eq!(index.name, original_index.name);
            assert_eq!(index.catalog_name, original_index.catalog_name);
            assert_eq!(index.namespace_name, original_index.namespace_name);
            assert_eq!(index.table, original_index.table);
            assert_eq!(index.columns, original_index.columns);
            assert_eq!(index.column_indices, original_index.column_indices);
            assert_eq!(index.unique, original_index.unique);
            assert_eq!(index.method, original_index.method);
            assert_eq!(index.options, original_index.options);
        };
        let different_table_id = original_table.table_id.checked_add(1_000).unwrap();
        let mut stale_index = original_index.clone();
        stale_index.index_id = original_index.index_id.checked_add(1_000).unwrap();
        assert_conflict(CatalogRollbackEffect::DropTable {
            table_name: original_table.name.clone(),
            table_id: different_table_id,
        });
        assert_conflict(CatalogRollbackEffect::DropIndex {
            index_name: original_index.name.clone(),
            index_id: stale_index.index_id,
        });
        assert_conflict(CatalogRollbackEffect::CreateIndex {
            index: Box::new(stale_index.clone()),
            table_id: original_table.table_id,
        });
        assert_conflict(CatalogRollbackEffect::CreateIndex {
            index: Box::new(original_index.clone()),
            table_id: different_table_id,
        });
        let mut replacement_table = original_table.clone();
        replacement_table.table_id = different_table_id;
        assert_conflict(CatalogRollbackEffect::CreateTable {
            table: Box::new(replacement_table.clone()),
            indexes: vec![],
        });

        // The later dependent-index conflict must be detected before inserting
        // either the missing table or the earlier, nonconflicting index.
        replacement_table.name = "undo_restore_target".to_string();
        let mut missing_index = original_index.clone();
        missing_index.name = "undo_restore_first_idx".to_string();
        missing_index.table = replacement_table.name.clone();
        missing_index.index_id = original_index.index_id.checked_add(2_000).unwrap();
        stale_index.table = replacement_table.name.clone();
        assert_conflict(CatalogRollbackEffect::CreateTable {
            table: Box::new(replacement_table),
            indexes: vec![missing_index, stale_index],
        });
        let catalog = state.catalog.read().expect("catalog");
        assert!(!catalog.table_exists("undo_restore_target"));
        assert!(!catalog.index_exists("undo_restore_first_idx"));
        drop(catalog);

        let mut independent_table = original_table.clone();
        independent_table.name = "undo_independent".to_string();
        independent_table.table_id = original_table.table_id.checked_add(3_000).unwrap();
        state
            .catalog
            .write()
            .expect("catalog")
            .create_table(independent_table.clone())
            .expect("independent table");
        // Effects are applied in reverse: the stale index conflicts first.
        // That conflict must not suppress the unrelated table undo afterward.
        let result = state.apply_catalog_rollback_effects(vec![
            CatalogRollbackEffect::DropTable {
                table_name: independent_table.name.clone(),
                table_id: independent_table.table_id,
            },
            CatalogRollbackEffect::DropIndex {
                index_name: original_index.name.clone(),
                index_id: original_index.index_id.checked_add(1_000).unwrap(),
            },
        ]);
        assert!(matches!(result, Err(ServerError::Conflict(_))));
        let catalog = state.catalog.read().expect("catalog");
        assert!(
            !catalog.table_exists(&independent_table.name),
            "catalog undo conflict must not skip independent undo"
        );
        assert_eq!(
            catalog
                .get_index("undo_value_idx")
                .expect("protected index")
                .index_id,
            original_index.index_id
        );
        assert_eq!(
            catalog
                .get_table("undo_rows")
                .expect("protected table")
                .table_id,
            original_table.table_id
        );
        drop(catalog);

        let expected_table = format!("{original_table:?}");
        let expected_index = format!("{original_index:?}");
        let mut restore_table = original_table.clone();
        restore_table.name = "undo_dependency_restore".to_string();
        restore_table.table_id = original_table.table_id.checked_add(4_000).unwrap();
        let mut conflicting_index = original_index.clone();
        conflicting_index.table = restore_table.name.clone();
        conflicting_index.index_id = original_index.index_id.checked_add(4_000).unwrap();
        for conflicting_effect in [
            CatalogRollbackEffect::DropIndex {
                index_name: original_index.name.clone(),
                index_id: original_index.index_id.checked_add(1_000).unwrap(),
            },
            // The attached index name belongs to a different, existing table.
            // Its conflict must protect that table against a later cascade.
            CatalogRollbackEffect::CreateTable {
                table: Box::new(restore_table.clone()),
                indexes: vec![conflicting_index],
            },
        ] {
            state
                .catalog
                .write()
                .expect("catalog")
                .create_table(independent_table.clone())
                .expect("independent table for dependency case");
            let result = state.apply_catalog_rollback_effects(vec![
                CatalogRollbackEffect::DropTable {
                    table_name: independent_table.name.clone(),
                    table_id: independent_table.table_id,
                },
                CatalogRollbackEffect::DropTable {
                    table_name: original_table.name.clone(),
                    table_id: original_table.table_id,
                },
                conflicting_effect,
            ]);
            assert!(matches!(result, Err(ServerError::Conflict(_))));
            let catalog = state.catalog.read().expect("catalog");
            assert!(
                !catalog.table_exists(&independent_table.name),
                "dependent undo protection must still allow independent undo"
            );
            assert_eq!(
                format!(
                    "{:?}",
                    catalog
                        .get_table(&original_table.name)
                        .expect("protected table")
                ),
                expected_table,
                "conflicting index undo must protect its parent table from cascade"
            );
            assert_eq!(
                format!(
                    "{:?}",
                    catalog
                        .get_index(&original_index.name)
                        .expect("protected index")
                ),
                expected_index,
                "conflicting index undo must preserve all index metadata"
            );
            assert!(
                !catalog.table_exists(&restore_table.name),
                "conflicting attached index must not partially restore a table"
            );
        }
    }
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
