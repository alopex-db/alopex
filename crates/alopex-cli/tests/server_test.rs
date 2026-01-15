use std::hash::{Hash, Hasher};

use alopex_cli::client::http::{ClientError, HttpClient};
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use alopex_core::columnar::encoding::LogicalType;
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema, SegmentWriterV2};
use alopex_core::storage::format::bincode_config;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::Server;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use bincode::config::Options;
use serde_json::{json, Value};
use tower::ServiceExt;

fn build_server() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: dir.path().to_path_buf(),
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (http::router(server.state.clone()), dir)
}

async fn send_json(router: axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = hyper::body::to_bytes(response.into_body())
        .await
        .expect("body");
    let json = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).expect("json")
    };
    (status, json)
}

fn table_id(table: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    (hasher.finish() & 0xffff_ffff) as u32
}

#[tokio::test]
async fn server_kv_txn_success_paths() {
    let (router, _dir) = build_server();

    let (status, body) = send_json(
        router.clone(),
        "/kv/txn/begin",
        serde_json::json!({ "timeout_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = body["txn_id"].as_str().expect("txn_id").to_string();

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/put",
        serde_json::json!({
            "txn_id": txn_id,
            "key": "alpha",
            "value": b"beta".to_vec()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/commit",
        serde_json::json!({ "txn_id": txn_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        router.clone(),
        "/kv/get",
        serde_json::json!({ "key": "alpha" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], serde_json::json!(b"beta".to_vec()));

    let (status, _) = send_json(
        router.clone(),
        "/kv/put",
        serde_json::json!({ "key": "gamma", "value": b"delta".to_vec() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(router.clone(), "/kv/txn/begin", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = body["txn_id"].as_str().expect("txn_id").to_string();

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/delete",
        serde_json::json!({ "txn_id": txn_id, "key": "gamma" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/commit",
        serde_json::json!({ "txn_id": txn_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        router.clone(),
        "/kv/get",
        serde_json::json!({ "key": "gamma" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["value"].is_null());
}

#[tokio::test]
async fn server_kv_txn_rollback_and_failures() {
    let (router, _dir) = build_server();

    let (status, _) = send_json(
        router.clone(),
        "/kv/put",
        serde_json::json!({ "key": "alpha", "value": b"orig".to_vec() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(router.clone(), "/kv/txn/begin", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = body["txn_id"].as_str().expect("txn_id").to_string();

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/put",
        serde_json::json!({
            "txn_id": txn_id,
            "key": "alpha",
            "value": b"new".to_vec()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/rollback",
        serde_json::json!({ "txn_id": txn_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        router.clone(),
        "/kv/get",
        serde_json::json!({ "key": "alpha" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], serde_json::json!(b"orig".to_vec()));

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/get",
        serde_json::json!({ "txn_id": "missing", "key": "alpha" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = send_json(
        router.clone(),
        "/kv/txn/begin",
        serde_json::json!({ "timeout_secs": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let txn_id = body["txn_id"].as_str().expect("txn_id").to_string();

    let (status, _) = send_json(
        router.clone(),
        "/kv/txn/put",
        serde_json::json!({
            "txn_id": txn_id,
            "key": "alpha",
            "value": b"late".to_vec()
        }),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test]
async fn server_columnar_ingest_paths() {
    let (router, _dir) = build_server();

    let schema = Schema {
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            logical_type: LogicalType::Int64,
            nullable: false,
            fixed_len: None,
        }],
    };
    let batch = RecordBatch::new(
        schema,
        vec![alopex_core::columnar::encoding::Column::Int64(vec![1, 2])],
        vec![None],
    );
    let mut writer = SegmentWriterV2::new(Default::default());
    writer.write_batch(batch).expect("write batch");
    let segment = writer.finish().expect("segment");
    let payload = bincode_config()
        .serialize(&segment)
        .expect("serialize segment");

    let table = "metrics";
    let (status, body) = send_json(
        router.clone(),
        "/columnar/ingest",
        serde_json::json!({
            "table": table,
            "compression": "lz4",
            "segment": payload
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let segment_id = body["segment_id"].as_str().expect("segment_id");
    assert!(segment_id.starts_with(&format!("{}:", table_id(table))));
    assert_eq!(body["row_count"], serde_json::json!(2));

    let (status, stats) = send_json(
        router.clone(),
        "/columnar/stats",
        serde_json::json!({ "segment_id": segment_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats["row_count"], serde_json::json!(2));

    let (status, _) = send_json(
        router.clone(),
        "/columnar/ingest",
        serde_json::json!({
            "table": "",
            "compression": "lz4",
            "segment": payload
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send_json(
        router.clone(),
        "/columnar/ingest",
        json!({
            "table": "bad",
            "compression": "lz4",
            "segment": vec![1, 2, 3]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn server_sql_success_and_http_error() {
    let (router, _dir) = build_server();

    let (status, _) = send_json(
        router.clone(),
        "/api/sql/query",
        json!({ "sql": "CREATE TABLE server_sql (id INTEGER PRIMARY KEY)" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send_json(
        router.clone(),
        "/api/sql/query",
        json!({ "sql": "INSERT INTO server_sql (id) VALUES (1)" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_json(
        router.clone(),
        "/api/sql/query",
        json!({ "sql": "SELECT id FROM server_sql" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows array");
    let has_id = rows.iter().any(|row| {
        row.get(0)
            .and_then(|value| value.get("Integer"))
            .and_then(|value| value.as_i64())
            .is_some_and(|value| value == 1)
    });
    assert!(has_id, "expected SELECT to return id=1: {body}");

    let (status, _) = send_json(router.clone(), "/api/sql/query", json!({ "sql": "" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn server_sql_connection_error() {
    let config = CliServerConfig {
        url: "https://127.0.0.1:1/".to_string(),
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    let client = HttpClient::new(&config).expect("client");
    let result: Result<Value, ClientError> = client
        .post_json("api/sql/query", &json!({ "sql": "SELECT 1" }))
        .await;
    match result {
        Err(ClientError::Request { .. }) => {}
        other => panic!("expected request error, got {other:?}"),
    }
}
