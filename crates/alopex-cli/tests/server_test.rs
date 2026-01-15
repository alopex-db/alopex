use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alopex_cli::client::http::{ClientError, HttpClient};
use alopex_cli::commands::sql::execute_remote_with_formatter_control;
use alopex_cli::commands::sql::SqlExecutionOptions;
use alopex_cli::error::CliError;
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use alopex_cli::streaming::{CancelSignal, Deadline};
use alopex_cli::{batch::BatchMode, cli::SqlCommand};
use alopex_cli::{batch::BatchModeSource, cli::OutputFormat};
use alopex_core::columnar::encoding::LogicalType;
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema, SegmentWriterV2};
use alopex_core::storage::format::bincode_config;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::server::Server;
use axum::body::{boxed, Body, Bytes};
use axum::extract::{Json, State};
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use axum::routing::post;
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
    let body = boxed(Body::wrap_stream(stream));
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
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

async fn execute_streaming_request(
    base_url: &str,
    cmd: SqlCommand,
    cancel: &CancelSignal,
    deadline: &Deadline,
    format: OutputFormat,
) -> Result<String, CliError> {
    let client = build_test_client(base_url);
    let formatter = create_formatter(format);
    let mut output = Vec::new();
    execute_remote_with_formatter_control(
        &client,
        &cmd,
        &batch_mode(),
        &mut output,
        formatter,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel,
            deadline,
        },
    )
    .await?;
    Ok(String::from_utf8(output).expect("utf8"))
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
    let rows = value.as_array().expect("array");
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
    let rows = value.as_array().expect("array");
    assert!(rows.is_empty());

    let _ = shutdown.send(());
}

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
