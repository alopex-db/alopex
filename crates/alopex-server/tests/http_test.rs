use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_cluster::{ClusterMode, NodeRole, NodeState};
use alopex_server::auth::AuthMode;
use alopex_server::config::{ClusterServerConfig, ServerConfig};
use alopex_server::http;
use alopex_server::server::ServerState;
use alopex_server::Server;
use arrow_array::types::Float32Type;
use arrow_array::{Array, FixedSizeListArray, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, StatusCode};
use parquet::arrow::ArrowWriter;
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

async fn build_state_with_copy_dir() -> (Arc<ServerState>, tempfile::TempDir, PathBuf) {
    let temp = tempdir().expect("tempdir");
    let copy_dir = temp.path().join("copy");
    fs::create_dir(&copy_dir).expect("copy directory");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        copy_allowed_dirs: vec![copy_dir.clone()],
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp, copy_dir)
}

fn write_vector_parquet(path: &std::path::Path, rows: &[(i32, [f32; 2])]) {
    let ids = Int32Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.iter().map(|(_, vector)| Some(vector.map(Some))),
        2,
    );
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("embedding", embeddings.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(embeddings)])
        .expect("record batch");
    let mut writer = ArrowWriter::try_new(File::create(path).expect("parquet file"), schema, None)
        .expect("parquet writer");
    writer.write(&batch).expect("parquet rows");
    writer.close().expect("parquet close");
}

fn write_vector_csv(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).expect("CSV file");
    writeln!(file, "id,embedding").expect("CSV header");
    for id in 1..=rows {
        writeln!(file, "{id},\"[{},{}]\"", id % 97, (id + 1) % 97).expect("CSV row");
    }
}

async fn build_cluster_aware_state(
    membership_source_available: bool,
) -> (Arc<ServerState>, tempfile::TempDir) {
    build_cluster_aware_state_with_lifecycle(membership_source_available, NodeState::Active).await
}

async fn build_cluster_aware_state_with_lifecycle(
    membership_source_available: bool,
    lifecycle_state: NodeState,
) -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        cluster: ClusterServerConfig {
            mode: ClusterMode::ClusterAware,
            node_id: Some("node-a".to_string()),
            cluster_id: Some("cluster-a".to_string()),
            advertised_endpoint: Some("127.0.0.1:7001".to_string()),
            role: NodeRole::Worker,
            lifecycle_state,
            membership_source_available,
            ..ClusterServerConfig::default()
        },
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, headers, body.to_vec())
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn admin_api_endpoints_return_expected_payloads() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(5)).await;
    let router = http::router(state.clone());

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("status json");
    assert!(value.get("version").and_then(|v| v.as_str()).is_some());
    assert!(value.get("uptime_secs").and_then(|v| v.as_u64()).is_some());
    assert_eq!(value["cluster"]["schema_version"].as_u64(), Some(1));
    assert_eq!(value["cluster"]["mode"].as_str(), Some("single_node"));
    assert_eq!(
        value["cluster"]["identity"]["node_id"].as_str(),
        Some("local")
    );
    assert_eq!(
        value["cluster"]["routing_capabilities"]["local_only"].as_bool(),
        Some(true)
    );
    assert_eq!(value["cluster"]["degraded"].as_bool(), Some(false));
    assert_eq!(
        value["cluster"]["metrics_summary"]["source"].as_str(),
        Some("live_status_surface")
    );

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("metrics json");
    assert!(value.get("qps").is_some());
    assert!(value.get("avg_latency_ms").is_some());
    assert!(value.get("p99_latency_ms").is_some());
    assert!(value.get("memory_usage_mb").is_some());
    assert!(value.get("active_connections").is_some());
    assert_eq!(value["cluster"]["mode"].as_str(), Some("single_node"));
    assert_eq!(
        value["cluster_metrics"]["source"].as_str(),
        Some("live_status_surface")
    );
    assert_eq!(value["cluster_metrics"]["degraded"].as_bool(), Some(false));
    assert_eq!(
        value["cluster_metrics"]["summary"]["members"]
            .as_array()
            .expect("member metrics")
            .len(),
        0
    );

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/health").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("health json");
    assert_eq!(value.get("status").and_then(|v| v.as_str()), Some("ok"));
    assert_eq!(value.get("message").and_then(|v| v.as_str()), Some("ready"));
    assert_eq!(value["degraded"].as_bool(), Some(false));
    assert_eq!(value["cluster"]["mode"].as_str(), Some("single_node"));

    let (status, _, body) =
        send_empty(router.clone(), Method::GET, "/api/admin/capabilities").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("capabilities json");
    assert_eq!(value["scope"].as_str(), Some("full"));
    assert!(value["unsupported_actions"]
        .as_array()
        .expect("unsupported actions")
        .iter()
        .any(|action| action.as_str() == Some("compaction")));

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/compaction",
        json!({}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    let value: Value = serde_json::from_slice(&body).expect("compaction json");
    assert_eq!(value["error"]["code"].as_str(), Some("NOT_IMPLEMENTED"));
    assert!(value["error"]["message"]
        .as_str()
        .expect("compaction error")
        .contains("LSM storage engine"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn admin_cluster_aware_degraded_status_health_and_metrics_payloads() {
    let (state, _temp) = build_cluster_aware_state(false).await;
    let router = http::router(state.clone());

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(value["cluster"]["mode"].as_str(), Some("cluster_aware"));
    assert_eq!(
        value["cluster"]["identity"]["node_id"].as_str(),
        Some("node-a")
    );
    assert_eq!(
        value["cluster"]["membership"]["members"][0]["raw_reachability_state"],
        Value::Null
    );
    assert_eq!(
        value["cluster"]["membership"]["members"][0]["derived_state"].as_str(),
        Some("active")
    );
    assert_eq!(value["cluster"]["degraded"].as_bool(), Some(true));
    assert_eq!(
        value["cluster"]["diagnostics"][0]["code"].as_str(),
        Some("chirps_unavailable")
    );

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/health").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("health json");
    assert_eq!(value["status"].as_str(), Some("degraded"));
    assert_eq!(value["message"].as_str(), Some("cluster status degraded"));
    assert_eq!(value["degraded"].as_bool(), Some(true));

    let (status, _, body) = send_empty(router.clone(), Method::GET, "/api/admin/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("metrics json");
    assert_eq!(value["cluster"]["mode"].as_str(), Some("cluster_aware"));
    assert_eq!(
        value["cluster"]["metrics_summary"]["members"][0]["node_id"].as_str(),
        Some("node-a")
    );
    assert_eq!(
        value["cluster"]["metrics_summary"]["members"][0]["source"].as_str(),
        Some("live_status_surface")
    );
    assert_eq!(
        value["cluster"]["metrics_summary"]["members"][0]["latency_ms"],
        Value::Null
    );
    assert_eq!(
        value["cluster_metrics"]["source"].as_str(),
        Some("live_status_surface")
    );
    assert_eq!(value["cluster_metrics"]["degraded"].as_bool(), Some(true));
    assert_eq!(
        value["cluster_metrics"]["summary"]["members"]
            .as_array()
            .expect("member metrics")
            .len(),
        1
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn prometheus_metrics_include_cluster_source_without_remote_observations() {
    let (state, _temp) = build_cluster_aware_state(false).await;
    let router = http::admin::router(state);

    let (status, _, body) = send_empty(router, Method::GET, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("metrics utf8");
    assert!(text.contains("cluster_mode{mode=\"cluster_aware\"} 1"));
    assert!(text.contains("cluster_degraded 1"));
    assert!(text.contains("cluster_metrics_source{source=\"live_status_surface\"} 1"));
    assert!(text.contains(
        "cluster_member_metrics_source{node_id=\"node-a\",source=\"live_status_surface\"} 1"
    ));
    assert!(!text.contains("cluster_member_latency"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn admin_cluster_join_leave_returns_status_schema_after_transition() {
    let (state, _temp) = build_cluster_aware_state_with_lifecycle(true, NodeState::Joining).await;
    let router = http::router(state.clone());

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/api/admin/cluster/join",
        json!({}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let value: Value = serde_json::from_slice(&body).expect("join json");
    assert_eq!(value["action"].as_str(), Some("join"));
    assert_eq!(value["cluster"]["schema_version"].as_u64(), Some(1));
    assert_eq!(value["cluster"]["mode"].as_str(), Some("cluster_aware"));
    assert_eq!(
        value["cluster"]["identity"]["node_id"].as_str(),
        Some("node-a")
    );
    assert_eq!(
        value["cluster"]["identity"]["lifecycle_state"].as_str(),
        Some("active")
    );
    assert_eq!(
        value["cluster"]["membership"]["members"][0]["transition_reason"].as_str(),
        Some("join_completed")
    );

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/api/admin/cluster/leave",
        json!({}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("leave json");
    assert_eq!(value["action"].as_str(), Some("leave"));
    assert_eq!(value["cluster"]["schema_version"].as_u64(), Some(1));
    assert_eq!(
        value["cluster"]["identity"]["lifecycle_state"].as_str(),
        Some("leaving")
    );
    assert_eq!(
        value["cluster"]["membership"]["members"][0]["transition_reason"].as_str(),
        Some("leave_requested")
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn admin_cluster_join_rejects_single_node_config() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(5)).await;
    let router = http::router(state.clone());

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/api/admin/cluster/join",
        json!({}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let value: Value = serde_json::from_slice(&body).expect("error json");
    assert_eq!(value["error"]["code"].as_str(), Some("INVALID_REQUEST"));
    assert!(value["error"]["message"]
        .as_str()
        .expect("message")
        .contains("cluster_aware mode"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
        "/sql",
        json!({
            "sql": "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;"
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/vector/upsert-batch",
        json!({
            "table": "items",
            "vectors": [
                { "id": 1, "vector": [1.0, 0.0] },
                { "id": 2, "vector": [0.0, 1.0] }
            ]
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("batch response")["affected_rows"],
        2
    );

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/vector/search",
        json!({ "table": "items", "vector": [0.0, 1.0], "k": 1 }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("batch index search")["results"][0]["id"],
        2
    );

    let (status, _, body) = send_json(
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
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("single response"),
        json!({ "success": true })
    );

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({
            "sql": "INSERT INTO items (id, embedding) VALUES (3, [1.0, 0.0]), (4, [0.5, 0.0]);"
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
            "sql": "INSERT INTO items (id, embedding) VALUES (5, [0.2, 0.0]);",
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_vector_upsert_batch_rejects_duplicate_ids_atomically() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(5)).await;
    let router = http::router(state);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/vector/upsert-batch",
        json!({
            "table": "items",
            "vectors": [
                { "id": 1, "vector": [1.0, 0.0] },
                { "id": 1, "vector": [0.0, 1.0] }
            ]
        }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/sql",
        json!({ "sql": "SELECT id FROM items;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).expect("query response");
    assert!(response["rows"].as_array().expect("rows").is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_vector_upsert_batch_accepts_10k_structured_vectors() {
    let (state, _temp) = build_state(AuthMode::None, Duration::from_secs(30)).await;
    let router = http::router(state);
    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let vectors: Vec<_> = (1..=10_000)
        .map(|id| json!({ "id": id, "vector": [id % 97, (id + 1) % 97] }))
        .collect();
    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/vector/upsert-batch",
        json!({ "table": "items", "vectors": vectors }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("batch response")["affected_rows"],
        10_000
    );

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/sql",
        json!({ "sql": "SELECT COUNT(*) FROM items;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("count response")["rows"][0][0]["BigInt"],
        10_000
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "the performance workflow runs one API/phase/size case per process"]
async fn http_ingestion_measurement() {
    let operation = std::env::var("ALOPEX_INGESTION_OPERATION").expect("ingestion operation");
    let rows: usize = std::env::var("ALOPEX_INGESTION_ROWS")
        .expect("ingestion rows")
        .parse()
        .expect("numeric ingestion rows");
    assert!(matches!(
        operation.as_str(),
        "single" | "batch" | "batch_existing" | "csv" | "parquet"
    ));
    assert!(matches!(rows, 10_000 | 50_000));

    async fn run(operation: &str, rows: usize) -> Duration {
        let (state, _temp, copy_dir) = build_state_with_copy_dir().await;
        let router = http::router(state);
        for sql in ["CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));"] {
            let (status, _, body) = send_json(
                router.clone(),
                Method::POST,
                "/sql",
                json!({ "sql": sql }),
                &[],
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        }
        let copy_path = match operation {
            "csv" | "parquet" => {
                let path = copy_dir.join(format!("items.{operation}"));
                if operation == "csv" {
                    write_vector_csv(&path, rows);
                } else {
                    write_vector_parquet(
                        &path,
                        &(1..=rows)
                            .map(|id| (id as i32, [(id % 97) as f32, ((id + 1) % 97) as f32]))
                            .collect::<Vec<_>>(),
                    );
                }
                Some(path)
            }
            _ => None,
        };
        if operation == "batch_existing" {
            let vectors = (1..=rows)
                .map(|id| json!({ "id": id, "vector": [id % 97, (id + 1) % 97] }))
                .collect::<Vec<_>>();
            let (status, _, body) = send_json(
                router.clone(),
                Method::POST,
                "/vector/upsert-batch",
                json!({ "table": "items", "vectors": vectors }),
                &[],
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        }
        let started = Instant::now();
        match operation {
            "single" => {
                for id in 1..=rows {
                    let (status, _, body) = send_json(
                        router.clone(),
                        Method::POST,
                        "/vector/upsert",
                        json!({ "table": "items", "id": id, "vector": [id % 97, (id + 1) % 97] }),
                        &[],
                    )
                    .await;
                    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
                }
            }
            "batch" | "batch_existing" => {
                let vectors = (1..=rows)
                    .map(|id| json!({ "id": id, "vector": [(id + 1) % 97, (id + 2) % 97] }))
                    .collect::<Vec<_>>();
                let (status, _, body) = send_json(
                    router,
                    Method::POST,
                    "/vector/upsert-batch",
                    json!({ "table": "items", "vectors": vectors }),
                    &[],
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
                assert_eq!(
                    serde_json::from_slice::<Value>(&body).expect("batch response")
                        ["affected_rows"],
                    rows
                );
            }
            "csv" | "parquet" => {
                let format = operation.to_uppercase();
                let header = if operation == "csv" {
                    ", HEADER TRUE"
                } else {
                    ""
                };
                let (status, _, body) = send_json(
                    router,
                    Method::POST,
                    "/sql",
                    json!({ "sql": format!(
                        "COPY items (id, embedding) FROM '{}' WITH (FORMAT {format}{header});",
                        copy_path.expect("COPY input").display(),
                    ) }),
                    &[],
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
                assert_eq!(
                    serde_json::from_slice::<Value>(&body).expect("COPY response")["affected_rows"],
                    rows
                );
            }
            _ => unreachable!(),
        }
        started.elapsed()
    }

    let _ = run(&operation, rows).await;
    let elapsed = run(&operation, rows).await;
    let output = PathBuf::from(std::env::var("ALOPEX_INGESTION_OUTPUT").expect("output path"));
    fs::create_dir_all(output.parent().expect("output directory")).expect("output directory");
    let raw = json!({
        "schema": "alopex.ingestion-measurement/v1",
        "source_commit": std::env::var("ALOPEX_INGESTION_SOURCE_COMMIT").unwrap_or_else(|_| "local".into()),
        "release_version": env!("CARGO_PKG_VERSION"),
        "protocol": "http",
        "operation": operation,
        "rows": rows,
        "warmups": 1,
        "samples": 1,
        "storage": "temporary local filesystem",
        "runtime_threads": 1,
        "dimension": 2,
        "elapsed_seconds": elapsed.as_secs_f64(),
        "rows_per_second": rows as f64 / elapsed.as_secs_f64(),
    });
    let temporary = output.with_extension("tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&raw).expect("raw JSON"),
    )
    .expect("temporary raw JSON");
    fs::rename(temporary, output).expect("atomic raw JSON output");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_sql_copy_uses_configured_directory_and_is_atomic() {
    let (state, temp, copy_dir) = build_state_with_copy_dir().await;
    let router = http::router(state);
    let valid = copy_dir.join("items.csv");
    let invalid = copy_dir.join("invalid.csv");
    let outside = temp.path().join("outside.csv");
    fs::write(&valid, "id,embedding\n1,\"[1.0,0.0]\"\n2,\"[0.0,1.0]\"\n").expect("valid CSV");
    fs::write(&invalid, "id,embedding\n3,\"[1.0,0.0]\"\n4,\"[1.0]\"\n").expect("invalid CSV");
    fs::write(&outside, "id,embedding\n5,\"[1.0,0.0]\"\n").expect("outside CSV");

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let copy_sql = |path: &std::path::Path| {
        format!(
            "COPY items (id, embedding) FROM '{}' WITH (FORMAT CSV, HEADER TRUE);",
            path.display()
        )
    };
    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&valid) }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let response: Value = serde_json::from_slice(&body).expect("COPY response");
    assert_eq!(response["affected_rows"], 2, "{response}");

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&invalid) }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&outside) }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    #[cfg(unix)]
    {
        let linked = copy_dir.join("linked.csv");
        std::os::unix::fs::symlink(&valid, &linked).expect("symlink");
        let (status, _, _) = send_json(
            router.clone(),
            Method::POST,
            "/sql",
            json!({ "sql": copy_sql(&linked) }),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/vector/search",
        json!({ "table": "items", "vector": [0.9, 0.1], "k": 1 }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).expect("search response");
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/sql",
        json!({ "sql": "SELECT id FROM items ORDER BY id;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).expect("query response");
    assert_eq!(response["rows"].as_array().unwrap().len(), 2);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_sql_copy_rejects_process_stdio_targets() {
    let (state, _temp, _copy_dir) = build_state_with_copy_dir().await;
    let router = http::router(state);

    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY);",
        "INSERT INTO items VALUES (1);",
    ] {
        let (status, _, body) = send_json(
            router.clone(),
            Method::POST,
            "/sql",
            json!({ "sql": sql }),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    }

    for sql in [
        "COPY items FROM STDIN WITH (FORMAT CSV);",
        "COPY items TO STDOUT WITH (FORMAT CSV);",
    ] {
        let (status, _, body) = send_json(
            router.clone(),
            Method::POST,
            "/sql",
            json!({ "sql": sql }),
            &[],
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_sql_copy_parquet_vector_is_transactional() {
    let (state, _temp, copy_dir) = build_state_with_copy_dir().await;
    let router = http::router(state);
    let valid = copy_dir.join("items.parquet");
    let duplicate = copy_dir.join("duplicate.parquet");
    write_vector_parquet(&valid, &[(1, [1.0, 0.0]), (2, [0.0, 1.0])]);
    write_vector_parquet(&duplicate, &[(3, [1.0, 0.0]), (3, [0.0, 1.0])]);

    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let copy_sql = |path: &std::path::Path| {
        format!(
            "COPY items (id, embedding) FROM '{}' WITH (FORMAT PARQUET);",
            path.display()
        )
    };
    let (status, _, body) = send_empty(router.clone(), Method::POST, "/session/begin").await;
    assert_eq!(status, StatusCode::OK);
    let session_id = serde_json::from_slice::<Value>(&body).expect("session response")
        ["session_id"]
        .as_str()
        .expect("session ID")
        .to_owned();
    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&valid), "session_id": session_id }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["affected_rows"],
        2
    );
    let (status, _, _) = send_empty(
        router.clone(),
        Method::POST,
        &format!("/session/{session_id}/rollback"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&duplicate) }),
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "SELECT id FROM items;" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(serde_json::from_slice::<Value>(&body).unwrap()["rows"]
        .as_array()
        .expect("rows")
        .is_empty());

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": copy_sql(&valid) }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["affected_rows"],
        2
    );

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/vector/search",
        json!({ "table": "items", "vector": [0.9, 0.1], "k": 1 }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["results"]
            .as_array()
            .expect("results")
            .len(),
        1
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn http_streaming_timeout_returns_error() {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        query_timeout: Duration::from_secs(5),
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let mut server = Server::new(config).expect("server");
    let router = http::router(server.state.clone());

    let (status, _, body) = send_json(
        router.clone(),
        Method::POST,
        "/sql",
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, value TEXT);" }),
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "CREATE TABLE failed: {}",
        String::from_utf8_lossy(&body)
    );

    const TOTAL_ROWS: usize = 2_000;
    const INSERT_BATCH_ROWS: usize = 100;
    for batch_start in (0..TOTAL_ROWS).step_by(INSERT_BATCH_ROWS) {
        let batch_end = (batch_start + INSERT_BATCH_ROWS).min(TOTAL_ROWS);
        let mut values = String::new();
        for id in batch_start..batch_end {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, 'v{id}')"));
        }
        let insert_sql = format!("INSERT INTO items (id, value) VALUES {values};");
        let (status, _, body) = send_json(
            router.clone(),
            Method::POST,
            "/sql",
            json!({ "sql": insert_sql }),
            &[],
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "bounded INSERT batch {batch_start}..{batch_end} failed: {}",
            String::from_utf8_lossy(&body)
        );
    }

    drop(router);
    Arc::get_mut(&mut server.state)
        .expect("no HTTP state references after setup")
        .config
        .query_timeout = Duration::from_millis(1);
    let router = http::router(server.state.clone());

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/sql",
        json!({
            "sql": "SELECT id FROM items ORDER BY id;",
            "streaming": true
        }),
        &[],
    )
    .await;
    assert!(status == StatusCode::OK || status == StatusCode::REQUEST_TIMEOUT);
    let text = String::from_utf8(body.clone()).expect("utf8");
    let mut saw_timeout = false;
    if status == StatusCode::OK {
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
                    break;
                }
            }
        }
    } else {
        let payload: Value = serde_json::from_slice(&body).expect("json");
        if let Some(error) = payload.get("error") {
            saw_timeout = error.get("code").and_then(|v| v.as_str()) == Some("QUERY_TIMEOUT");
        }
    }
    assert!(saw_timeout);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn admin_cluster_join_uses_existing_auth_boundary() {
    let (state, _temp) = build_state(
        AuthMode::Dev {
            api_key: "secret".to_string(),
        },
        Duration::from_secs(5),
    )
    .await;
    let router = http::router(state.clone());

    let (status, _, body) = send_json(
        router,
        Method::POST,
        "/api/admin/cluster/join",
        json!({}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["error"]["code"].as_str(), Some("UNAUTHORIZED"));
    assert!(!value["error"]["correlation_id"]
        .as_str()
        .expect("correlation id")
        .is_empty());
}
