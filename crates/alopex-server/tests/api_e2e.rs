use std::collections::HashSet;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{
    ColumnSchema, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
};
use alopex_core::storage::format::bincode_config;
use bincode::config::Options;
use http_body_util::BodyExt;
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use tempfile::tempdir;
use tokio::time::sleep;

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child missing")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    listener.local_addr().expect("local addr").port()
}

fn reserve_unique_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let port = reserve_port();
        if used.insert(port) {
            return port;
        }
    }
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

fn write_config(dir: &Path, http_port: u16, admin_port: u16, grpc_port: u16) -> PathBuf {
    let config_path = dir.join("alopex.toml");
    let contents = format!(
        "\
http_bind = \"127.0.0.1:{http_port}\"
grpc_bind = \"127.0.0.1:{grpc_port}\"
admin_bind = \"127.0.0.1:{admin_port}\"
data_dir = \"{data_dir}\"
metrics_enabled = true
tracing_enabled = false
audit_log_enabled = false
",
        data_dir = toml_path(dir),
    );
    std::fs::write(&config_path, contents).expect("write config");
    config_path
}

fn read_stderr(child: &mut Child) -> String {
    let mut stderr_output = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut stderr_output);
    }
    stderr_output
}

async fn send_json(
    client: &Client<HttpConnector, http_body_util::Full<axum::body::Bytes>>,
    method: Method,
    url: &str,
    body: Value,
) -> (StatusCode, Value) {
    let bytes = serde_json::to_vec(&body).expect("serialize json");
    let request = Request::builder()
        .method(method)
        .uri(url)
        .header(CONTENT_TYPE, "application/json")
        .body(http_body_util::Full::new(axum::body::Bytes::from(bytes)))
        .expect("request");
    let response = client.request(request).await.expect("response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value: Value = serde_json::from_slice(&body)
        .unwrap_or_else(|err| panic!("invalid json ({err}): {}", String::from_utf8_lossy(&body)));
    (status, value)
}

async fn send_empty(
    client: &Client<HttpConnector, http_body_util::Full<axum::body::Bytes>>,
    method: Method,
    url: &str,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
        .uri(url)
        .body(http_body_util::Full::new(axum::body::Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, body.to_vec())
}

async fn try_send_empty(
    client: &Client<HttpConnector, http_body_util::Full<axum::body::Bytes>>,
    method: Method,
    url: &str,
) -> Option<(StatusCode, Vec<u8>)> {
    let request = Request::builder()
        .method(method)
        .uri(url)
        .body(http_body_util::Full::new(axum::body::Bytes::new()))
        .ok()?;
    let response = client.request(request).await.ok()?;
    let status = response.status();
    let body = response.into_body().collect().await.ok()?.to_bytes();
    Some((status, body.to_vec()))
}

fn build_columnar_segment_bytes() -> Vec<u8> {
    let schema = Schema {
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            logical_type: LogicalType::Int64,
            nullable: false,
            fixed_len: None,
        }],
    };
    let batch = RecordBatch::new(schema.clone(), vec![Column::Int64(vec![1, 2])], vec![None]);
    let mut writer = SegmentWriterV2::new(SegmentConfigV2::default());
    writer.write_batch(batch).expect("write batch");
    let segment = writer.finish().expect("segment");
    bincode_config()
        .serialize(&segment)
        .expect("serialize segment")
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_binary_exposes_all_http_apis() {
    let temp = tempdir().expect("tempdir");
    let mut used = HashSet::new();
    let http_port = reserve_unique_port(&mut used);
    let admin_port = reserve_unique_port(&mut used);
    let grpc_port = reserve_unique_port(&mut used);
    let config_path = write_config(temp.path(), http_port, admin_port, grpc_port);

    let child = Command::new(env!("CARGO_BIN_EXE_alopex-server"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut guard = ChildGuard::new(child);

    let client = Client::builder(TokioExecutor::new()).build_http();
    let deadline = Instant::now() + Duration::from_secs(10);
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let http_url = format!("http://127.0.0.1:{http_port}");

    let mut admin_ready = false;
    let mut api_ready = false;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.child_mut().try_wait() {
            let stderr_output = read_stderr(guard.child_mut());
            panic!("alopex-server exited early ({status}). stderr:\n{stderr_output}");
        }

        if !admin_ready {
            if let Some((status, _)) =
                try_send_empty(&client, Method::GET, &format!("{admin_url}/healthz")).await
            {
                admin_ready = status == StatusCode::OK;
            }
        }
        if !api_ready {
            if let Some((status, _)) = try_send_empty(
                &client,
                Method::GET,
                &format!("{http_url}/api/admin/health"),
            )
            .await
            {
                api_ready = status == StatusCode::OK;
            }
        }

        if admin_ready && api_ready {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    if !(admin_ready && api_ready) {
        let stderr_output = read_stderr(guard.child_mut());
        panic!("alopex-server failed health checks. stderr:\n{stderr_output}");
    }

    // Admin server endpoints.
    let (status, _) = send_empty(&client, Method::GET, &format!("{admin_url}/healthz")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(&client, Method::GET, &format!("{admin_url}/status")).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(value.get("status").and_then(|v| v.as_str()), Some("ok"));

    let (status, body) = send_empty(&client, Method::GET, &format!("{admin_url}/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.is_empty());

    // Admin API endpoints.
    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/health"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/status"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/metrics"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/capabilities"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/api/admin/compaction"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/api/admin/lifecycle"),
        json!({ "action": "archive" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, backup_body) = send_empty(
        &client,
        Method::POST,
        &format!("{http_url}/api/admin/backup"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let backup_value: Value = serde_json::from_slice(&backup_body).expect("backup json");
    let backup_handle = backup_value
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("backup handle");

    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/backup/{backup_handle}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/restore/00000000-0000-0000-0000-000000000000"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // SQL + vector endpoints.
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({ "sql": "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, embedding VECTOR(2, L2));" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({
            "sql": "INSERT INTO items (id, name, embedding) VALUES (1, 'alpha', [0.1, 0.2]), (2, 'beta', [0.2, 0.1]);"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/api/sql/query"),
        json!({ "sql": "SELECT id, name FROM items ORDER BY id;" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({ "sql": "CREATE TABLE vectors (id INT PRIMARY KEY, embedding VECTOR(2, L2));" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/upsert"),
        json!({ "table": "vectors", "id": 3, "vector": [0.3, 0.4] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/search"),
        json!({ "table": "vectors", "vector": [0.1, 0.2], "k": 1 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/delete"),
        json!({ "table": "vectors", "id": 3 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/index/create"),
        json!({
            "name": "vec_idx",
            "table": "vectors",
            "column": "embedding",
            "method": "hnsw"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/index/update"),
        json!({
            "name": "vec_idx",
            "table": "vectors",
            "column": "embedding",
            "method": "hnsw"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/index/compact"),
        json!({ "name": "vec_idx" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/index/delete"),
        json!({ "name": "vec_idx", "if_exists": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // KV endpoints.
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/put"),
        json!({ "key": "alpha", "value": [1, 2, 3] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/get"),
        json!({ "key": "alpha" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/list"),
        json!({ "prefix": "a" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/delete"),
        json!({ "key": "alpha" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, txn_body) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/begin"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = txn_body
        .get("txn_id")
        .and_then(|v| v.as_str())
        .expect("txn id");
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/put"),
        json!({ "txn_id": txn_id, "key": "beta", "value": [9] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/get"),
        json!({ "txn_id": txn_id, "key": "beta" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/commit"),
        json!({ "txn_id": txn_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, txn_body) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/begin"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = txn_body
        .get("txn_id")
        .and_then(|v| v.as_str())
        .expect("txn id");
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/delete"),
        json!({ "txn_id": txn_id, "key": "beta" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/txn/rollback"),
        json!({ "txn_id": txn_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // HNSW endpoints.
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/create"),
        json!({ "index": "hnsw_test", "dim": 2, "metric": "l2" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/upsert"),
        json!({ "index": "hnsw_test", "key": [1], "vector": [0.0, 0.1] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/search"),
        json!({ "index": "hnsw_test", "query": [0.0, 0.1], "k": 1 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/stats"),
        json!({ "index": "hnsw_test" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/delete"),
        json!({ "index": "hnsw_test", "key": [1] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/hnsw/drop"),
        json!({ "index": "hnsw_test" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Columnar endpoints.
    let segment_bytes = build_columnar_segment_bytes();
    let (status, ingest_body) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/ingest"),
        json!({
            "table": "col_table",
            "compression": "none",
            "segment": segment_bytes
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let segment_id = ingest_body
        .get("segment_id")
        .and_then(|v| v.as_str())
        .expect("segment id");

    let (status, body) =
        send_empty(&client, Method::POST, &format!("{http_url}/columnar/list")).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).expect("columnar list json");
    let segments = value
        .get("segments")
        .and_then(|v| v.as_array())
        .expect("segments");
    assert!(segments.iter().any(|v| v.as_str() == Some(segment_id)));

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/stats"),
        json!({ "segment_id": segment_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/scan"),
        json!({ "segment_id": segment_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/index/create"),
        json!({ "segment_id": segment_id, "column": "id", "index_type": "minmax" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/index/list"),
        json!({ "segment_id": segment_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/columnar/index/drop"),
        json!({ "segment_id": segment_id, "column": "id" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Admin resources endpoint.
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/put"),
        json!({ "key": "gamma", "value": [7, 8, 9] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/put"),
        json!({ "key": "__alopex_test", "value": [0] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_empty(
        &client,
        Method::GET,
        &format!(
            "{http_url}/api/admin/resources?limit=1&include_columnar_columns=true&columnar_column_limit=1"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resources: Value = serde_json::from_slice(&body).expect("resources json");
    let truncated = resources.get("truncated").expect("truncated sections");
    assert_eq!(
        truncated.get("sql_tables").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        truncated.get("kv_keys").and_then(|v| v.as_bool()),
        Some(true)
    );
    let sql_tables = resources
        .get("sql_tables")
        .and_then(|v| v.as_array())
        .expect("sql tables");
    assert_eq!(sql_tables.len(), 1);
    assert_eq!(
        sql_tables[0].get("name").and_then(|v| v.as_str()),
        Some("items")
    );
    let columns = sql_tables[0]
        .get("columns")
        .and_then(|v| v.as_array())
        .expect("sql columns");
    let column_names: HashSet<_> = columns
        .iter()
        .filter_map(|col| col.get("name").and_then(|v| v.as_str()))
        .collect();
    assert!(column_names.contains("id"));
    assert!(column_names.contains("name"));
    assert!(column_names.contains("embedding"));
    let embedding_type = columns
        .iter()
        .find(|col| col.get("name").and_then(|v| v.as_str()) == Some("embedding"))
        .and_then(|col| col.get("data_type").and_then(|v| v.as_str()));
    assert_eq!(embedding_type, Some("VECTOR(2, L2)"));

    let columnar_segments = resources
        .get("columnar_segments")
        .and_then(|v| v.as_array())
        .expect("columnar segments");
    assert_eq!(columnar_segments.len(), 1);
    let columnar_columns = columnar_segments[0]
        .get("columns")
        .and_then(|v| v.as_array())
        .expect("columnar columns");
    assert!(columnar_columns.len() <= 1);

    let (status, body) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/resources?limit=50"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resources: Value = serde_json::from_slice(&body).expect("resources json");
    let kv_keys = resources
        .get("kv_keys")
        .and_then(|v| v.as_array())
        .expect("kv keys");
    let kv_keys: HashSet<_> = kv_keys.iter().filter_map(|v| v.as_str()).collect();
    assert!(kv_keys.contains("beta"));
    assert!(kv_keys.contains("gamma"));
    assert!(!kv_keys.contains("__alopex_test"));
    let columnar_segments = resources
        .get("columnar_segments")
        .and_then(|v| v.as_array())
        .expect("columnar segments");
    if let Some(first_segment) = columnar_segments.first() {
        assert!(first_segment.get("columns").is_some_and(Value::is_null));
    }

    // Session endpoints.
    let (status, body) =
        send_empty(&client, Method::POST, &format!("{http_url}/session/begin")).await;
    assert_eq!(status, StatusCode::OK);
    let session_body: Value = serde_json::from_slice(&body).expect("session json");
    let session_id = session_body
        .get("session_id")
        .and_then(|v| v.as_str())
        .expect("session id");
    let (status, _) = send_empty(
        &client,
        Method::POST,
        &format!("{http_url}/session/{session_id}/commit"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) =
        send_empty(&client, Method::POST, &format!("{http_url}/session/begin")).await;
    assert_eq!(status, StatusCode::OK);
    let session_body: Value = serde_json::from_slice(&body).expect("session json");
    let session_id = session_body
        .get("session_id")
        .and_then(|v| v.as_str())
        .expect("session id");
    let (status, _) = send_empty(
        &client,
        Method::POST,
        &format!("{http_url}/session/{session_id}/rollback"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Restore endpoint (run last to avoid disrupting other operations).
    let (status, restore_body) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/api/admin/restore"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let restore_handle = restore_body
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("restore handle");
    let (status, _) = send_empty(
        &client,
        Method::GET,
        &format!("{http_url}/api/admin/restore/{restore_handle}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
