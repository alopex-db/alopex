use std::convert::Infallible;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alopex_cli::client::http::{ClientError, HttpClient};
use alopex_cli::commands::lifecycle::{
    execute_remote_with_formatter as execute_lifecycle_remote, RemoteLifecycleSupport, SupportLevel,
};
use alopex_cli::commands::server::execute_remote as execute_server_remote;
use alopex_cli::commands::sql::execute_remote_with_formatter_control;
use alopex_cli::commands::sql::SqlExecutionOptions;
use alopex_cli::error::CliError;
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::output::server as server_output;
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use alopex_cli::streaming::{CancelSignal, Deadline};
use alopex_cli::ui::mode::UiMode;
use alopex_cli::{
    batch::BatchMode, cli::CompactionCommand, cli::LifecycleBackupCommand, cli::LifecycleCommand,
    cli::LifecycleRestoreCommand, cli::ServerCommand, cli::SqlCommand,
};
use alopex_cli::{batch::BatchModeSource, cli::OutputFormat};
use alopex_core::columnar::encoding::LogicalType;
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema, SegmentWriterV2};
use alopex_core::storage::format::bincode_config;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::Server;
use axum::body::{Body, Bytes};
use axum::extract::{Json, Path, State};
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use bincode::config::Options;
use futures_util::stream;
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).expect("json")
    };
    (status, json)
}

fn batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: true,
        source: BatchModeSource::Explicit,
    }
}

fn build_test_client(base_url: &str) -> HttpClient {
    let config = CliServerConfig {
        url: base_url.to_string(),
        insecure: false,
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    let client = reqwest::ClientBuilder::new()
        .danger_accept_invalid_certs(true)
        .use_rustls_tls()
        .build()
        .expect("reqwest client");
    HttpClient::new_with_client(&config, client).expect("http client")
}

struct StreamServerState {
    chunks: Vec<Bytes>,
    delay: Option<Duration>,
    request_body: Mutex<Option<Value>>,
    cancel_count: AtomicUsize,
}

async fn spawn_tls_server(
    router: axum::Router,
) -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    // rustls 0.23: axum-server's `tls-rustls-no-provider` feature does not
    // auto-install a process-level CryptoProvider (unlike `tls-rustls`,
    // which hardcodes aws-lc-rs). Install `ring` explicitly so
    // `RustlsConfig::from_pem_file` (which calls `ServerConfig::builder()`
    // internally) doesn't panic. Ignore the error if another codepath
    // (e.g. reqwest) already installed a provider first.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().expect("tempdir");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.serialize_pem().expect("cert pem")).expect("write cert");
    std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");

    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("rustls config");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    });

    let server = axum_server::bind_rustls(addr, rustls_config)
        .handle(handle)
        .serve(router.into_make_service());
    tokio::spawn(server);

    (format!("https://{}", addr), shutdown_tx, dir)
}

fn build_chunk_stream(
    chunks: Vec<Bytes>,
    delay: Option<Duration>,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> + Send {
    stream::unfold(
        (chunks, 0usize, delay),
        |(chunks, index, delay)| async move {
            if index >= chunks.len() {
                return None;
            }
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let item = Ok::<Bytes, Infallible>(chunks[index].clone());
            Some((item, (chunks, index + 1, delay)))
        },
    )
}

async fn streaming_handler(
    State(state): State<Arc<StreamServerState>>,
    Json(body): Json<Value>,
) -> Response {
    let mut guard = state.request_body.lock().await;
    *guard = Some(body);
    drop(guard);

    let stream = build_chunk_stream(state.chunks.clone(), state.delay);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

async fn streaming_jsonl_handler(
    State(state): State<Arc<StreamServerState>>,
    Json(body): Json<Value>,
) -> Response {
    let mut guard = state.request_body.lock().await;
    *guard = Some(body);
    drop(guard);

    let stream = build_chunk_stream(state.chunks.clone(), state.delay);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/jsonl"),
    );
    response
}

async fn cancel_handler(State(state): State<Arc<StreamServerState>>) -> StatusCode {
    state.cancel_count.fetch_add(1, Ordering::SeqCst);
    StatusCode::OK
}

async fn start_streaming_server(
    chunks: Vec<&'static str>,
    delay: Option<Duration>,
) -> (
    String,
    oneshot::Sender<()>,
    Arc<StreamServerState>,
    tempfile::TempDir,
) {
    let state = Arc::new(StreamServerState {
        chunks: chunks
            .into_iter()
            .map(|chunk| Bytes::from_static(chunk.as_bytes()))
            .collect(),
        delay,
        request_body: Mutex::new(None),
        cancel_count: AtomicUsize::new(0),
    });

    let router = axum::Router::new()
        .route("/api/sql/query", post(streaming_handler))
        .route("/api/sql/cancel", post(cancel_handler))
        .with_state(state.clone());

    let (base_url, shutdown, dir) = spawn_tls_server(router).await;
    (base_url, shutdown, state, dir)
}

async fn start_jsonl_streaming_server(
    chunks: Vec<&'static str>,
    delay: Option<Duration>,
) -> (
    String,
    oneshot::Sender<()>,
    Arc<StreamServerState>,
    tempfile::TempDir,
) {
    let state = Arc::new(StreamServerState {
        chunks: chunks
            .into_iter()
            .map(|chunk| Bytes::from_static(chunk.as_bytes()))
            .collect(),
        delay,
        request_body: Mutex::new(None),
        cancel_count: AtomicUsize::new(0),
    });

    let router = axum::Router::new()
        .route("/api/sql/query", post(streaming_jsonl_handler))
        .route("/api/sql/cancel", post(cancel_handler))
        .with_state(state.clone());

    let (base_url, shutdown, dir) = spawn_tls_server(router).await;
    (base_url, shutdown, state, dir)
}

async fn start_admin_server() -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    let router = axum::Router::new()
        .route(
            "/api/admin/status",
            get(|| async {
                Json(json!({
                    "version": "0.4.1",
                    "uptime_secs": 42,
                    "connections": 3,
                    "queries_per_second": 9.3,
                    "cluster": {
                        "schema_version": 1,
                        "mode": "single_node",
                        "identity": {
                            "node_id": "local",
                            "lifecycle_state": "active"
                        },
                        "routing_capabilities": {
                            "local_only": true,
                            "future_distributed_execution_required": true,
                            "scatter_gather_simulated": true
                        },
                        "degraded": false,
                        "diagnostics": []
                    }
                }))
            }),
        )
        .route(
            "/api/admin/metrics",
            get(|| async {
                Json(json!({
                    "qps": 12.5,
                    "avg_latency_ms": 4.2,
                    "p99_latency_ms": 9.7,
                    "memory_usage_mb": 512,
                    "active_connections": 7
                }))
            }),
        )
        .route(
            "/api/admin/health",
            get(|| async {
                Json(json!({
                    "status": "ok",
                    "message": "ready",
                    "degraded": false,
                    "cluster": {
                        "schema_version": 1,
                        "mode": "single_node",
                        "identity": {
                            "node_id": "local",
                            "lifecycle_state": "active"
                        },
                        "routing_capabilities": {
                            "local_only": true,
                            "future_distributed_execution_required": true,
                            "scatter_gather_simulated": true
                        },
                        "degraded": false,
                        "diagnostics": []
                    }
                }))
            }),
        )
        .route(
            "/api/admin/compaction",
            post(|| async { Json(json!({ "success": true, "message": "started" })) }),
        )
        .route(
            "/api/admin/cluster/join",
            post(|| async {
                Json(json!({
                    "action": "join",
                    "cluster": {
                        "schema_version": 1,
                        "mode": "cluster_aware",
                        "identity": {
                            "node_id": "node-a",
                            "lifecycle_state": "active"
                        },
                        "degraded": false
                    }
                }))
            }),
        )
        .route(
            "/api/admin/cluster/leave",
            post(|| async {
                Json(json!({
                    "action": "leave",
                    "cluster": {
                        "schema_version": 1,
                        "mode": "cluster_aware",
                        "identity": {
                            "node_id": "node-a",
                            "lifecycle_state": "leaving"
                        },
                        "degraded": false
                    }
                }))
            }),
        );

    spawn_tls_server(router).await
}

struct AdminLifecycleState {
    backup_body: Mutex<Option<Value>>,
    restore_body: Mutex<Option<Value>>,
}

async fn start_admin_lifecycle_server() -> (
    String,
    oneshot::Sender<()>,
    Arc<AdminLifecycleState>,
    tempfile::TempDir,
) {
    let state = Arc::new(AdminLifecycleState {
        backup_body: Mutex::new(None),
        restore_body: Mutex::new(None),
    });
    let router = axum::Router::new()
        .route(
            "/api/admin/backup",
            post(|State(state): State<Arc<AdminLifecycleState>>, Json(body): Json<Value>| async move {
                let mut guard = state.backup_body.lock().await;
                *guard = Some(body);
                Json(json!({
                    "status": "OK",
                    "handle": "backup-1",
                    "state": "running",
                    "location": "s3://bucket/backup-1",
                    "message": "started"
                }))
            }),
        )
        .route(
            "/api/admin/backup/{id}",
            get(|Path(handle): Path<String>| async move {
                Json(json!({
                    "status": "OK",
                    "handle": handle,
                    "state": "completed",
                    "location": "s3://bucket/backup-1"
                }))
            }),
        )
        .route(
            "/api/admin/restore",
            post(|State(state): State<Arc<AdminLifecycleState>>, Json(body): Json<Value>| async move {
                let mut guard = state.restore_body.lock().await;
                *guard = Some(body.clone());
                Json(json!({
                    "status": "OK",
                    "handle": "restore-1",
                    "state": "running",
                    "metadata": { "source": body.get("source").cloned().unwrap_or(Value::Null) }
                }))
            }),
        )
        .route(
            "/api/admin/restore/{id}",
            get(|Path(handle): Path<String>| async move {
                Json(json!({
                    "status": "OK",
                    "handle": handle,
                    "state": "completed",
                    "metadata": { "stage": "done" }
                }))
            }),
        )
        .with_state(state.clone());

    let (base_url, shutdown, dir) = spawn_tls_server(router).await;
    (base_url, shutdown, state, dir)
}

async fn execute_streaming_request(
    base_url: &str,
    cmd: SqlCommand,
    cancel: &CancelSignal,
    deadline: &Deadline,
    format: OutputFormat,
) -> Result<String, CliError> {
    let client = build_test_client(base_url);
    let mut output = Vec::new();
    execute_remote_with_formatter_control(
        &client,
        &cmd,
        &batch_mode(),
        UiMode::Batch,
        &mut output,
        format,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel,
            deadline,
            admin_launcher: None,
        },
    )
    .await?;
    Ok(String::from_utf8(output).expect("utf8"))
}

async fn execute_lifecycle_request(
    base_url: &str,
    command: &LifecycleCommand,
    support: RemoteLifecycleSupport,
) -> Result<Value, CliError> {
    let client = build_test_client(base_url);
    let formatter = create_formatter(OutputFormat::Json);
    let mut output = Vec::new();
    execute_lifecycle_remote(&client, command, support, &mut output, formatter).await?;
    let value: Value = serde_json::from_slice(&output).expect("json");
    Ok(value)
}

fn table_id(table: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    (hasher.finish() & 0xffff_ffff) as u32
}

fn load_cluster_status_fixture() -> Value {
    let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/cluster_status_cross_surface_expected.json");
    serde_json::from_slice(&fs::read(&fixture_path).expect("cluster status fixture bytes"))
        .expect("cluster status fixture json")
}

fn status_row_json(fields: server_output::StatusRowFields<'_>) -> Value {
    let columns = server_output::status_columns();
    let row = server_output::status_row(fields);
    let mut formatter = create_formatter(OutputFormat::Json);
    let mut output = Vec::new();
    formatter.write_header(&mut output, &columns).unwrap();
    formatter.write_row(&mut output, &row).unwrap();
    formatter.write_footer(&mut output).unwrap();

    let value: Value = serde_json::from_slice(&output).expect("json");
    value.as_array().expect("array")[0].clone()
}

fn cluster_status_row_subset(row: &Value) -> Value {
    json!({
        "Cluster Schema": row["Cluster Schema"].clone(),
        "Cluster Mode": row["Cluster Mode"].clone(),
        "Node ID": row["Node ID"].clone(),
        "Lifecycle": row["Lifecycle"].clone(),
        "Degraded": row["Degraded"].clone(),
        "Local Only": row["Local Only"].clone(),
        "Future Distributed": row["Future Distributed"].clone(),
        "Scatter/Gather": row["Scatter/Gather"].clone(),
        "Diagnostics": row["Diagnostics"].clone(),
    })
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_connection_error() {
    let config = CliServerConfig {
        url: "https://127.0.0.1:1/".to_string(),
        insecure: false,
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_json_array_success() {
    let chunks = vec![r#"[{"id":1,"name":"a"},"#, r#" {"id":2,"name":"b"}]"#];
    let (base_url, shutdown, _state, _dir) = start_streaming_server(chunks, None).await;

    let cmd = SqlCommand {
        query: Some("SELECT id, name FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect("streaming output");
    let value: Value = serde_json::from_str(&output).expect("json array");
    let sets = value.as_array().expect("array of result sets");
    assert_eq!(sets.len(), 1, "remote sql yields one result set");
    let rows = sets[0].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);

    let first_obj_start = output.find('{').expect("object start");
    let first_obj_end = output[first_obj_start..]
        .find('}')
        .map(|idx| first_obj_start + idx)
        .expect("object end");
    let first_obj = &output[first_obj_start..=first_obj_end];
    assert!(
        first_obj.find("\"id\"").unwrap() < first_obj.find("\"name\"").unwrap(),
        "expected column order to follow first object keys: {first_obj}"
    );

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_jsonl_success() {
    let chunks = vec![
        r#"{"row":[{"Integer":1},{"Text":"alpha"}],"error":null,"done":false}
"#,
        r#"{"row":[{"Integer":2},{"Text":"beta"}],"error":null,"done":false}
"#,
        r#"{"row":null,"error":null,"done":true}
"#,
    ];
    let (base_url, shutdown, _state, _dir) = start_jsonl_streaming_server(chunks, None).await;

    let cmd = SqlCommand {
        query: Some("SELECT id, name FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect("streaming output");
    let value: Value = serde_json::from_str(&output).expect("json array");
    let sets = value.as_array().expect("array of result sets");
    assert_eq!(sets.len(), 1, "remote sql yields one result set");
    let rows = sets[0].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["col1"], json!(1));
    assert_eq!(rows[0]["col2"], json!("alpha"));
    assert_eq!(rows[1]["col1"], json!(2));
    assert_eq!(rows[1]["col2"], json!("beta"));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_empty_array_outputs_json() {
    let (base_url, shutdown, _state, _dir) = start_streaming_server(vec!["[]"], None).await;

    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect("empty output");
    let value: Value = serde_json::from_str(&output).expect("json array");
    let sets = value.as_array().expect("array of result sets");
    assert_eq!(sets.len(), 1, "remote sql yields one result set");
    let rows = sets[0].as_array().expect("rows array");
    assert!(rows.is_empty());

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_invalid_rows_error() {
    let cases = vec![
        vec![r#"[{"id":1,"name":"a"},{"id":2}]"#],
        vec![r#"[{"id":1,"name":"a"},{"id":2,"name":"b","extra":3}]"#],
        vec![r#"[1]"#],
        vec![r#"[{"id":{"nested":1}}]"#],
        vec![r#"[{"id":1}]garbage"#],
    ];

    for chunks in cases {
        let (base_url, shutdown, _state, _dir) = start_streaming_server(chunks, None).await;
        let cmd = SqlCommand {
            query: Some("SELECT id FROM items".to_string()),
            file: None,
            fetch_size: None,
            max_rows: None,
            deadline: None,
            tui: false,
        };
        let cancel = CancelSignal::new();
        let deadline = Deadline::new(Duration::from_secs(5));
        let err = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
            .await
            .expect_err("expected invalid argument");
        assert!(matches!(err, CliError::InvalidArgument(_)));
        let _ = shutdown.send(());
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_csv_output() {
    let (base_url, shutdown, _state, _dir) =
        start_streaming_server(vec![r#"[{"id":1,"name":"a"}]"#], None).await;
    let cmd = SqlCommand {
        query: Some("SELECT id, name FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Csv)
        .await
        .expect("csv output");
    let mut lines = output.lines();
    assert_eq!(lines.next(), Some("id,name"));
    assert_eq!(lines.next(), Some("1,a"));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_tsv_output() {
    let (base_url, shutdown, _state, _dir) =
        start_streaming_server(vec![r#"[{"id":1,"name":"a"}]"#], None).await;
    let cmd = SqlCommand {
        query: Some("SELECT id, name FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Tsv)
        .await
        .expect("tsv output");
    let mut lines = output.lines();
    assert_eq!(lines.next(), Some("id\tname"));
    assert_eq!(lines.next(), Some("1\ta"));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_non_array_error() {
    let (base_url, shutdown, _state, _dir) =
        start_streaming_server(vec![r#"{"id":1}"#], None).await;
    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));
    let err = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect_err("expected invalid argument");
    assert!(matches!(err, CliError::InvalidArgument(_)));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_sends_fetch_size_and_max_rows() {
    let (base_url, shutdown, state, _dir) = start_streaming_server(vec!["[]"], None).await;
    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: Some(10),
        max_rows: Some(25),
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let output = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect("output");
    let _value: Value = serde_json::from_str(&output).expect("json array");

    let guard = state.request_body.lock().await;
    let body = guard.as_ref().expect("request body");
    assert_eq!(body["streaming"], json!(true));
    assert_eq!(body["fetch_size"], json!(10));
    assert_eq!(body["max_rows"], json!(25));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_deadline_sends_cancel() {
    let (base_url, shutdown, state, _dir) =
        start_streaming_server(vec!["["], Some(Duration::from_millis(200))).await;
    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_millis(10));

    let err = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect_err("expected timeout");
    assert!(matches!(err, CliError::Timeout(_)));
    assert!(state.cancel_count.load(Ordering::SeqCst) >= 1);

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_cancel_sends_cancel_endpoint() {
    let (base_url, shutdown, state, _dir) =
        start_streaming_server(vec!["["], Some(Duration::from_millis(200))).await;
    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));

    let cancel_signal = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel_signal.cancel();
    });

    let err = execute_streaming_request(&base_url, cmd, &cancel, &deadline, OutputFormat::Json)
        .await
        .expect_err("expected cancellation");
    assert!(matches!(err, CliError::Cancelled));
    assert!(state.cancel_count.load(Ordering::SeqCst) >= 1);

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_commands_success() {
    let (base_url, shutdown, _dir) = start_admin_server().await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();

    execute_server_remote(&client, &ServerCommand::Status, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("Version"));
    assert!(text.contains("0.4.1"));
    assert!(text.contains("Uptime"));
    assert!(text.contains("42"));
    assert!(text.contains("Connections"));
    assert!(text.contains("3"));
    assert!(text.contains("QPS"));
    assert!(text.contains("9.30"));
    assert!(text.contains("Cluster Mode"));
    assert!(text.contains("single_node"));
    assert!(text.contains("Node ID"));
    assert!(text.contains("local"));
    assert!(text.contains("Future Distributed"));
    assert!(text.contains("Scatter/Gather"));

    execute_server_remote(&client, &ServerCommand::Metrics, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("QPS"));
    assert!(text.contains("12.50"));
    assert!(text.contains("Avg Latency"));
    assert!(text.contains("4.20"));
    assert!(text.contains("P99 Latency"));
    assert!(text.contains("9.70"));
    assert!(text.contains("Memory"));
    assert!(text.contains("512"));
    assert!(text.contains("Active Connections"));
    assert!(text.contains("7"));

    execute_server_remote(&client, &ServerCommand::Health, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("Status"));
    assert!(text.contains("ok"));
    assert!(text.contains("ready"));
    assert!(text.contains("Degraded"));
    assert!(text.contains("single_node"));

    execute_server_remote(
        &client,
        &ServerCommand::Compaction {
            command: CompactionCommand::Trigger,
        },
        &mut output,
        false,
    )
    .await
    .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("Status"));
    assert!(text.contains("OK"));
    assert!(text.contains("started"));

    execute_server_remote(&client, &ServerCommand::Join, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("Action"));
    assert!(text.contains("join"));
    assert!(text.contains("cluster_aware"));
    assert!(text.contains("node-a"));
    assert!(text.contains("active"));

    execute_server_remote(&client, &ServerCommand::Leave, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("leave"));
    assert!(text.contains("leaving"));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_compaction_unsupported_is_reported() {
    let router = axum::Router::new().route(
        "/api/admin/compaction",
        post(|| async {
            (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({
                    "error": {
                        "code": "NOT_IMPLEMENTED",
                        "message": "manual compaction is not available"
                    }
                })),
            )
        }),
    );
    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();

    let err = execute_server_remote(
        &client,
        &ServerCommand::Compaction {
            command: CompactionCommand::Trigger,
        },
        &mut output,
        false,
    )
    .await
    .expect_err("unsupported compaction should fail");
    assert!(matches!(err, CliError::ServerUnsupported(message) if message.contains("501")));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_status_outputs_degraded_cluster_fields() {
    let router = axum::Router::new()
        .route(
            "/api/admin/status",
            get(|| async {
                Json(json!({
                    "version": "0.4.1",
                    "uptime_secs": 7,
                    "connections": 1,
                    "queries_per_second": 0.5,
                    "cluster": {
                        "schema_version": 1,
                        "mode": "cluster_aware",
                        "identity": {
                            "node_id": "node-a",
                            "lifecycle_state": "active"
                        },
                        "routing_capabilities": {
                            "local_only": false,
                            "future_distributed_execution_required": true,
                            "scatter_gather_simulated": false
                        },
                        "degraded": true,
                        "diagnostics": [
                            { "code": "chirps_unavailable" }
                        ]
                    }
                }))
            }),
        )
        .route(
            "/api/admin/metrics",
            get(|| async { Json(json!({ "qps": 0.0 })) }),
        )
        .route(
            "/api/admin/health",
            get(|| async {
                Json(json!({
                    "status": "degraded",
                    "message": "cluster status degraded",
                    "degraded": true,
                    "cluster": {
                        "schema_version": 1,
                        "mode": "cluster_aware",
                        "identity": {
                            "node_id": "node-a",
                            "lifecycle_state": "active"
                        },
                        "routing_capabilities": {
                            "local_only": false,
                            "future_distributed_execution_required": true,
                            "scatter_gather_simulated": false
                        },
                        "degraded": true,
                        "diagnostics": [
                            { "code": "chirps_unavailable" }
                        ]
                    }
                }))
            }),
        )
        .route(
            "/api/admin/compaction",
            post(|| async { Json(json!({ "success": true })) }),
        );

    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();

    execute_server_remote(&client, &ServerCommand::Status, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("cluster_aware"));
    assert!(text.contains("node-a"));
    assert!(text.contains("chirps_unavailable"));
    assert!(text.contains("false"));
    assert!(text.contains("true"));

    execute_server_remote(&client, &ServerCommand::Health, &mut output, false)
        .await
        .unwrap();
    let text = String::from_utf8(std::mem::take(&mut output)).unwrap();
    assert!(text.contains("degraded"));
    assert!(text.contains("cluster_aware"));
    assert!(text.contains("node-a"));

    let _ = shutdown.send(());
}

#[test]
fn server_status_json_output_contains_cluster_fields() {
    let columns = server_output::status_columns();
    let row = server_output::status_row(server_output::StatusRowFields {
        version: Some("0.4.1"),
        uptime_secs: Some(7),
        connections: Some(1),
        qps: Some(0.5),
        cluster_schema_version: Some(1),
        cluster_mode: Some("cluster_aware"),
        node_id: Some("node-a"),
        lifecycle_state: Some("active"),
        degraded: Some(true),
        local_only: Some(false),
        future_distributed: Some(true),
        scatter_gather: Some(false),
        diagnostics: Some("chirps_unavailable"),
    });
    let mut formatter = create_formatter(OutputFormat::Json);
    let mut output = Vec::new();
    formatter.write_header(&mut output, &columns).unwrap();
    formatter.write_row(&mut output, &row).unwrap();
    formatter.write_footer(&mut output).unwrap();

    let value: Value = serde_json::from_slice(&output).expect("json");
    let row = &value.as_array().expect("array")[0];
    assert_eq!(row["Cluster Mode"].as_str(), Some("cluster_aware"));
    assert_eq!(row["Node ID"].as_str(), Some("node-a"));
    assert_eq!(row["Degraded"].as_bool(), Some(true));
    assert_eq!(row["Local Only"].as_bool(), Some(false));
    assert_eq!(row["Future Distributed"].as_bool(), Some(true));
    assert_eq!(row["Scatter/Gather"].as_bool(), Some(false));
    assert_eq!(row["Diagnostics"].as_str(), Some("chirps_unavailable"));
}

#[test]
fn server_status_json_output_matches_cluster_cross_surface_fixture() {
    let expected = load_cluster_status_fixture();
    let expected = &expected["cli_status_rows"];
    let cases = [
        (
            "single_node",
            server_output::StatusRowFields {
                version: Some("0.7.0"),
                uptime_secs: Some(0),
                connections: Some(0),
                qps: Some(0.0),
                cluster_schema_version: Some(1),
                cluster_mode: Some("single_node"),
                node_id: Some("local"),
                lifecycle_state: Some("unconfigured"),
                degraded: Some(false),
                local_only: Some(true),
                future_distributed: Some(true),
                scatter_gather: Some(true),
                diagnostics: None,
            },
        ),
        (
            "cluster_aware",
            server_output::StatusRowFields {
                version: Some("0.7.0"),
                uptime_secs: Some(0),
                connections: Some(0),
                qps: Some(0.0),
                cluster_schema_version: Some(1),
                cluster_mode: Some("cluster_aware"),
                node_id: Some("node-a"),
                lifecycle_state: Some("active"),
                degraded: Some(false),
                local_only: Some(true),
                future_distributed: Some(true),
                scatter_gather: Some(true),
                diagnostics: None,
            },
        ),
        (
            "cluster_aware_degraded",
            server_output::StatusRowFields {
                version: Some("0.7.0"),
                uptime_secs: Some(0),
                connections: Some(0),
                qps: Some(0.0),
                cluster_schema_version: Some(1),
                cluster_mode: Some("cluster_aware"),
                node_id: Some("node-a"),
                lifecycle_state: Some("active"),
                degraded: Some(true),
                local_only: Some(true),
                future_distributed: Some(true),
                scatter_gather: Some(true),
                diagnostics: Some("chirps_unavailable"),
            },
        ),
    ];

    for (label, fields) in cases {
        let actual = cluster_status_row_subset(&status_row_json(fields));
        assert_eq!(
            expected[label], actual,
            "CLI cluster status row mismatch for {label}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_http_error() {
    let router = axum::Router::new()
        .route(
            "/api/admin/status",
            get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        )
        .route(
            "/api/admin/metrics",
            get(|| async { (StatusCode::OK, Json(json!({ "qps": 0.0 }))) }),
        )
        .route(
            "/api/admin/health",
            get(|| async { (StatusCode::OK, Json(json!({ "status": "ok" }))) }),
        )
        .route(
            "/api/admin/compaction",
            post(|| async { (StatusCode::OK, Json(json!({ "success": true }))) }),
        );

    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();

    let err = execute_server_remote(&client, &ServerCommand::Status, &mut output, false)
        .await
        .unwrap_err();
    match err {
        CliError::InvalidArgument(message) => {
            assert!(message.contains("HTTP 500"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_connection_error() {
    let client = build_test_client("https://127.0.0.1:1");
    let mut output = Vec::new();

    let err = execute_server_remote(&client, &ServerCommand::Status, &mut output, false)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::ServerConnection(_)));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_lifecycle_backup_restore_success() {
    let (base_url, shutdown, state, _dir) = start_admin_lifecycle_server().await;
    let support = RemoteLifecycleSupport {
        backup: SupportLevel::Supported,
        restore: SupportLevel::Supported,
    };

    let value = execute_lifecycle_request(
        &base_url,
        &LifecycleCommand::Backup { command: None },
        support,
    )
    .await
    .expect("backup start");
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .expect("row object");
    assert_eq!(row.get("Status").and_then(|v| v.as_str()), Some("OK"));
    assert_eq!(row.get("Handle").and_then(|v| v.as_str()), Some("backup-1"));
    assert_eq!(row.get("State").and_then(|v| v.as_str()), Some("running"));
    assert_eq!(
        row.get("Location").and_then(|v| v.as_str()),
        Some("s3://bucket/backup-1")
    );

    let value = execute_lifecycle_request(
        &base_url,
        &LifecycleCommand::Backup {
            command: Some(LifecycleBackupCommand::Status {
                handle: "backup-1".to_string(),
            }),
        },
        support,
    )
    .await
    .expect("backup status");
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .expect("row object");
    assert_eq!(row.get("State").and_then(|v| v.as_str()), Some("completed"));

    let value = execute_lifecycle_request(
        &base_url,
        &LifecycleCommand::Restore {
            source: Some("s3://bucket/restore".to_string()),
            command: None,
        },
        support,
    )
    .await
    .expect("restore start");
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .expect("row object");
    assert_eq!(
        row.get("Handle").and_then(|v| v.as_str()),
        Some("restore-1")
    );
    assert_eq!(row.get("State").and_then(|v| v.as_str()), Some("running"));
    assert!(row
        .get("Metadata")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .contains("source"));

    let value = execute_lifecycle_request(
        &base_url,
        &LifecycleCommand::Restore {
            source: None,
            command: Some(LifecycleRestoreCommand::Status {
                handle: "restore-1".to_string(),
            }),
        },
        support,
    )
    .await
    .expect("restore status");
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .expect("row object");
    assert_eq!(row.get("State").and_then(|v| v.as_str()), Some("completed"));

    let restore_body = state.restore_body.lock().await;
    assert_eq!(
        restore_body
            .as_ref()
            .and_then(|body| body.get("source"))
            .and_then(|value| value.as_str()),
        Some("s3://bucket/restore")
    );

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_lifecycle_unsupported_endpoint_maps_error() {
    let router = axum::Router::new().route(
        "/api/admin/backup/{id}",
        get(|| async { (StatusCode::NOT_FOUND, Json(json!({ "error": "missing" }))) }),
    );
    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();
    let formatter = create_formatter(OutputFormat::Json);

    let err = execute_lifecycle_remote(
        &client,
        &LifecycleCommand::Backup {
            command: Some(LifecycleBackupCommand::Status {
                handle: "missing".to_string(),
            }),
        },
        RemoteLifecycleSupport::unknown(),
        &mut output,
        formatter,
    )
    .await
    .expect_err("unsupported");
    assert!(matches!(err, CliError::ServerUnsupported(_)));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_lifecycle_http_error_maps_to_connection_error() {
    let router = axum::Router::new().route(
        "/api/admin/restore",
        post(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
    );
    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();
    let formatter = create_formatter(OutputFormat::Json);

    let err = execute_lifecycle_remote(
        &client,
        &LifecycleCommand::Restore {
            source: Some("s3://bucket/restore".to_string()),
            command: None,
        },
        RemoteLifecycleSupport::unknown(),
        &mut output,
        formatter,
    )
    .await
    .expect_err("http error");
    assert!(matches!(err, CliError::ServerConnection(_)));

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_admin_lifecycle_invalid_json_maps_to_connection_error() {
    let router = axum::Router::new().route(
        "/api/admin/backup",
        post(|| async { (StatusCode::OK, "not-json") }),
    );
    let (base_url, shutdown, _dir) = spawn_tls_server(router).await;
    let client = build_test_client(&base_url);
    let mut output = Vec::new();
    let formatter = create_formatter(OutputFormat::Json);

    let err = execute_lifecycle_remote(
        &client,
        &LifecycleCommand::Backup { command: None },
        RemoteLifecycleSupport::unknown(),
        &mut output,
        formatter,
    )
    .await
    .expect_err("invalid json");
    assert!(matches!(err, CliError::ServerConnection(_)));

    let _ = shutdown.send(());
}

/// Helper for the "stdout stays valid JSON on stream errors" tests: runs a
/// remote streaming request and returns the error together with whatever was
/// written to stdout before the failure.
async fn execute_streaming_request_capturing_output(
    base_url: &str,
    cmd: SqlCommand,
) -> (CliError, String) {
    let client = build_test_client(base_url);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(5));
    let mut output = Vec::new();
    let err = execute_remote_with_formatter_control(
        &client,
        &cmd,
        &batch_mode(),
        UiMode::Batch,
        &mut output,
        OutputFormat::Json,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel: &cancel,
            deadline: &deadline,
            admin_launcher: None,
        },
    )
    .await
    .expect_err("stream error expected");
    (err, String::from_utf8(output).expect("utf8"))
}

fn assert_stdout_is_valid_json_array(stdout: &str) {
    let value: Value = serde_json::from_str(stdout).unwrap_or_else(|err| {
        panic!("stdout must remain valid JSON on stream errors: {err}\nstdout:\n{stdout}")
    });
    assert!(value.is_array(), "stdout: {stdout}");
}

fn streaming_error_test_cmd() -> SqlCommand {
    SqlCommand {
        query: Some("SELECT id, name FROM items".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_jsonl_error_before_rows_keeps_stdout_valid_json() {
    let chunks = vec![
        r#"{"row":null,"error":{"message":"boom"},"done":false}
"#,
    ];
    let (base_url, shutdown, _state, _dir) = start_jsonl_streaming_server(chunks, None).await;

    let (err, stdout) =
        execute_streaming_request_capturing_output(&base_url, streaming_error_test_cmd()).await;
    assert!(matches!(err, CliError::InvalidArgument(_)));
    assert_stdout_is_valid_json_array(&stdout);

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_jsonl_error_after_rows_keeps_stdout_valid_json() {
    let chunks = vec![
        r#"{"row":[{"Integer":1},{"Text":"alpha"}],"error":null,"done":false}
"#,
        r#"{"row":null,"error":{"message":"boom"},"done":false}
"#,
    ];
    let (base_url, shutdown, _state, _dir) = start_jsonl_streaming_server(chunks, None).await;

    let (err, stdout) =
        execute_streaming_request_capturing_output(&base_url, streaming_error_test_cmd()).await;
    assert!(matches!(err, CliError::InvalidArgument(_)));
    assert_stdout_is_valid_json_array(&stdout);

    let _ = shutdown.send(());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn server_sql_streaming_invalid_stream_after_rows_keeps_stdout_valid_json() {
    // Second row drops a column, which fails validation after the first row
    // has already been streamed to stdout.
    let chunks = vec![r#"[{"id":1,"name":"a"},{"id":2}]"#];
    let (base_url, shutdown, _state, _dir) = start_streaming_server(chunks, None).await;

    let (err, stdout) =
        execute_streaming_request_capturing_output(&base_url, streaming_error_test_cmd()).await;
    assert!(matches!(err, CliError::InvalidArgument(_)));
    assert_stdout_is_valid_json_array(&stdout);

    let _ = shutdown.send(());
}
