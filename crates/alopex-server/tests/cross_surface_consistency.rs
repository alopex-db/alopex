use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::cli::{OutputFormat, SqlCommand};
use alopex_cli::commands::{kv as cli_kv, sql as cli_sql};
use alopex_cli::output::jsonl::JsonlFormatter;
use alopex_cli::streaming::StreamingWriter;
use alopex_cli::ui::mode::UiMode;
use alopex_cluster::{ClusterMode, NodeRole, NodeState};
use alopex_embedded::Database;
use alopex_server::config::{ClusterServerConfig, ServerConfig};
use alopex_server::{http, Server};
use http_body_util::BodyExt;
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use tempfile::tempdir;
use tokio::time::{sleep, timeout};
use tower::ServiceExt;

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

    fn stop_and_read_stderr(&mut self) -> String {
        let child = self.child_mut();
        let _ = child.kill();
        let _ = child.wait();
        read_stderr(child)
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

fn server_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

fn write_config_with_extra(
    dir: &Path,
    http_port: u16,
    admin_port: u16,
    grpc_port: u16,
    extra: &str,
) -> PathBuf {
    let config_path = dir.join("alopex.toml");
    let contents = format!(
        "\
http_bind = \"127.0.0.1:{http_port}\"
grpc_bind = \"127.0.0.1:{grpc_port}\"
admin_bind = \"127.0.0.1:{admin_port}\"
data_dir = \"{data_dir}\"
metrics_enabled = false
tracing_enabled = false
audit_log_enabled = false
{extra}",
        data_dir = toml_path(dir),
    );
    fs::write(&config_path, contents).expect("write config");
    config_path
}

fn write_config(dir: &Path, http_port: u16, admin_port: u16, grpc_port: u16) -> PathBuf {
    write_config_with_extra(dir, http_port, admin_port, grpc_port, "")
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
    let response = timeout(Duration::from_secs(15), client.request(request))
        .await
        .expect("http response timeout")
        .expect("response");
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

async fn try_send_empty(
    client: &Client<HttpConnector, http_body_util::Full<axum::body::Bytes>>,
    method: Method,
    url: &str,
) -> Option<StatusCode> {
    let request = Request::builder()
        .method(method)
        .uri(url)
        .body(http_body_util::Full::new(axum::body::Bytes::new()))
        .ok()?;
    let response = timeout(Duration::from_secs(1), client.request(request))
        .await
        .ok()?
        .ok()?;
    Some(response.status())
}

fn default_batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: false,
        source: BatchModeSource::Explicit,
    }
}

fn parse_jsonl(output: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(output)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("jsonl row"))
        .collect()
}

fn extract_sql_scalar(value: &Value) -> Value {
    if let Some(obj) = value.as_object() {
        if let Some(v) = obj.get("Integer") {
            return v.clone();
        }
        if let Some(v) = obj.get("Text") {
            return v.clone();
        }
        if let Some(v) = obj.get("Float") {
            return v.clone();
        }
    }
    value.clone()
}

fn normalize_sql_rows(rows: &Value) -> Value {
    let Some(items) = rows.as_array() else {
        return Value::Array(Vec::new());
    };
    let normalized = items
        .iter()
        .map(|row| {
            if let Some(obj) = row.as_object() {
                if obj.contains_key("id") && obj.contains_key("name") {
                    return json!({
                        "id": extract_sql_scalar(obj.get("id").unwrap_or(&Value::Null)),
                        "name": extract_sql_scalar(obj.get("name").unwrap_or(&Value::Null)),
                    });
                }
            }
            if let Some(values) = row.as_array() {
                if values.len() >= 2 {
                    return json!({
                        "id": extract_sql_scalar(&values[0]),
                        "name": extract_sql_scalar(&values[1]),
                    });
                }
            }
            row.clone()
        })
        .collect::<Vec<_>>();
    Value::Array(normalized)
}

fn run_cli_sql_rows(db: &Database, query: &str) -> Vec<Value> {
    let cmd = SqlCommand {
        query: Some(query.to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        request_id: None,
        read_mode: None,
        routing_report: None,
        tui: false,
    };
    let mut output = Vec::new();
    cli_sql::execute_with_formatter(
        db,
        cmd,
        &default_batch_mode(),
        UiMode::Batch,
        &mut output,
        OutputFormat::Jsonl,
        None,
        None,
        true,
    )
    .expect("cli sql");
    parse_jsonl(&output)
}

fn run_cli_kv_get(db: &Database, key: &str) -> String {
    let columns = cli_kv::kv_columns();
    let mut output = Vec::new();
    let mut writer =
        StreamingWriter::new(&mut output, Box::new(JsonlFormatter::new()), columns, None);
    cli_kv::execute(
        db,
        alopex_cli::cli::KvCommand::Get {
            key: key.to_string(),
        },
        &mut writer,
    )
    .expect("cli kv get");
    let rows = parse_jsonl(&output);
    rows.first()
        .and_then(|v| v.get("value"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn diff_values(path: &str, expected: &Value, actual: &Value, diffs: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            let keys: std::collections::BTreeSet<&String> = e.keys().chain(a.keys()).collect();
            for key in keys {
                let next = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                match (e.get(key), a.get(key)) {
                    (Some(ev), Some(av)) => diff_values(&next, ev, av, diffs),
                    (Some(_), None) => diffs.push(format!("{next}: missing in actual")),
                    (None, Some(_)) => diffs.push(format!("{next}: unexpected in actual")),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(e), Value::Array(a)) => {
            if e.len() != a.len() {
                diffs.push(format!(
                    "{path}: length expected={} actual={}",
                    e.len(),
                    a.len()
                ));
            }
            for i in 0..e.len().min(a.len()) {
                diff_values(&format!("{path}[{i}]"), &e[i], &a[i], diffs);
            }
        }
        _ => {
            if expected != actual {
                diffs.push(format!("{path}: expected={expected} actual={actual}"));
            }
        }
    }
}

fn assert_json_eq_with_diff(label: &str, expected: &Value, actual: &Value) {
    if expected == actual {
        return;
    }
    let mut diffs = Vec::new();
    diff_values("", expected, actual, &mut diffs);
    let joined = diffs
        .into_iter()
        .take(20)
        .collect::<Vec<_>>()
        .join("\n  - ");
    panic!("{label} mismatch\nexpected: {expected}\nactual: {actual}\ndiff:\n  - {joined}");
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../tests/fixtures/{name}"))
}

fn load_fixture(name: &str) -> Value {
    let path = fixture_path(name);
    serde_json::from_slice(&fs::read(&path).expect("fixture bytes")).expect("fixture json")
}

async fn fetch_admin_cluster_status(config: ServerConfig) -> Value {
    let server = Server::new(config).expect("server");
    let router = http::router(server.state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/admin/status")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = router.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("admin status json");
    stable_cluster_status_fields(&payload["cluster"])
}

fn stable_cluster_status_fields(cluster: &Value) -> Value {
    json!({
        "schema_version": cluster["schema_version"].clone(),
        "mode": cluster["mode"].clone(),
        "identity": {
            "node_id": cluster["identity"]["node_id"].clone(),
            "cluster_id": cluster["identity"]["cluster_id"].clone(),
            "advertised_endpoint": cluster["identity"]["advertised_endpoint"].clone(),
            "role": cluster["identity"]["role"].clone(),
            "lifecycle_state": cluster["identity"]["lifecycle_state"].clone(),
            "metadata_schema_version": cluster["identity"]["metadata_schema_version"].clone(),
            "update_epoch": cluster["identity"]["update_epoch"].clone(),
        },
        "membership": {
            "schema_version": cluster["membership"]["schema_version"].clone(),
            "update_epoch": cluster["membership"]["update_epoch"].clone(),
            "source": cluster["membership"]["source"].clone(),
            "members": cluster["membership"]["members"].clone(),
        },
        "routing_capabilities": cluster["routing_capabilities"].clone(),
        "metrics_summary": cluster["metrics_summary"].clone(),
        "degraded": cluster["degraded"].clone(),
        "diagnostics": cluster["diagnostics"].as_array().map(|diagnostics| {
            Value::Array(diagnostics.iter().map(|diagnostic| {
                json!({
                    "code": diagnostic["code"].clone(),
                    "degraded": diagnostic["degraded"].clone(),
                })
            }).collect())
        }).unwrap_or_else(|| Value::Array(Vec::new())),
    })
}

fn base_server_config(data_dir: PathBuf) -> ServerConfig {
    ServerConfig {
        data_dir,
        audit_log_enabled: false,
        tracing_enabled: false,
        metrics_enabled: false,
        ..ServerConfig::default()
    }
}

fn cluster_aware_config(data_dir: PathBuf, membership_source_available: bool) -> ServerConfig {
    ServerConfig {
        cluster: ClusterServerConfig {
            mode: ClusterMode::ClusterAware,
            node_id: Some("node-a".to_string()),
            cluster_id: Some("cluster-a".to_string()),
            advertised_endpoint: Some("127.0.0.1:7001".to_string()),
            role: NodeRole::Worker,
            lifecycle_state: NodeState::Active,
            membership_source_available,
            ..ClusterServerConfig::default()
        },
        ..base_server_config(data_dir)
    }
}

/// サーバーバイナリを起動し、admin、HTTP API、gRPC の readiness を待つ。
/// (v0.7 以降は surface ごとに起動タイミングが異なるため、すべてを確認する)
async fn spawn_server_and_wait(
    config_path: &Path,
    http_url: &str,
    admin_url: &str,
    grpc_url: &str,
) -> (
    ChildGuard,
    Client<HttpConnector, http_body_util::Full<axum::body::Bytes>>,
    tonic::transport::Channel,
) {
    let child = Command::new(env!("CARGO_BIN_EXE_alopex-server"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut guard = ChildGuard::new(child);

    let client = Client::builder(TokioExecutor::new()).build_http();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut admin_ready = false;
    let mut api_ready = false;
    let grpc_endpoint = tonic::transport::Endpoint::from_shared(grpc_url.to_string())
        .expect("grpc uri")
        .connect_timeout(Duration::from_secs(1));
    let mut grpc_channel = None;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = guard.child_mut().try_wait() {
            let stderr_output = read_stderr(guard.child_mut());
            panic!("alopex-server exited early ({status}). stderr:\n{stderr_output}");
        }
        if !admin_ready {
            if let Some(status) =
                try_send_empty(&client, Method::GET, &format!("{admin_url}/healthz")).await
            {
                admin_ready = status == StatusCode::OK;
            }
        }
        if !api_ready {
            if let Some(status) = try_send_empty(
                &client,
                Method::GET,
                &format!("{http_url}/api/admin/health"),
            )
            .await
            {
                api_ready = status == StatusCode::OK;
            }
        }
        if grpc_channel.is_none() {
            if let Ok(Ok(channel)) =
                timeout(Duration::from_secs(2), grpc_endpoint.clone().connect()).await
            {
                grpc_channel = Some(channel);
            }
        }
        if admin_ready && api_ready && grpc_channel.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    if !(admin_ready && api_ready && grpc_channel.is_some()) {
        let stderr_output = guard.stop_and_read_stderr();
        panic!("alopex-server failed health check. stderr:\n{stderr_output}");
    }
    (
        guard,
        client,
        grpc_channel.expect("gRPC channel must be ready"),
    )
}

fn http_sql_value_to_i64(value: &Value) -> i64 {
    if let Some(obj) = value.as_object() {
        if let Some(v) = obj.get("Integer").and_then(Value::as_i64) {
            return v;
        }
        if let Some(v) = obj.get("BigInt").and_then(Value::as_i64) {
            return v;
        }
    }
    panic!("unexpected http sql value: {value}");
}

fn grpc_sql_value_to_i64(value: &alopex_server::grpc::proto::Value) -> i64 {
    use alopex_server::grpc::proto::value::Kind;
    match &value.kind {
        Some(Kind::IntValue(v)) => i64::from(*v),
        Some(Kind::BigintValue(v)) => *v,
        other => panic!("unexpected grpc sql value: {other:?}"),
    }
}

/// gRPC ExecuteSql は HTTP `/sql` と同一の SQL 実行経路を通ること (issue #25)。
/// SELECT リスト内スカラーサブクエリを含む SELECT が、HTTP と gRPC で
/// 同一の結果集合を返すことを検証する。
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_execute_sql_matches_http_for_scalar_subquery_select() {
    let _server_test_guard = server_test_lock().lock().await;
    let temp = tempdir().expect("tempdir");
    let mut used = HashSet::new();
    let http_port = reserve_unique_port(&mut used);
    let admin_port = reserve_unique_port(&mut used);
    let grpc_port = reserve_unique_port(&mut used);
    let config_path = write_config(temp.path(), http_port, admin_port, grpc_port);
    let http_url = format!("http://127.0.0.1:{http_port}");
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let grpc_url = format!("http://127.0.0.1:{grpc_port}");
    let (_guard, client, channel) =
        spawn_server_and_wait(&config_path, &http_url, &admin_url, &grpc_url).await;

    for sql in [
        "CREATE TABLE parity_items (id INT PRIMARY KEY, val INT);",
        "CREATE TABLE parity_totals (id INT PRIMARY KEY, amount INT);",
        "INSERT INTO parity_items (id, val) VALUES (1, 10), (2, 20);",
        "INSERT INTO parity_totals (id, amount) VALUES (1, 100), (2, 250);",
    ] {
        let (status, body) = send_json(
            &client,
            Method::POST,
            &format!("{http_url}/sql"),
            json!({ "sql": sql }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "setup sql failed: {sql}: {body}");
    }

    let query = "SELECT id, (SELECT MAX(amount) FROM parity_totals) FROM parity_items ORDER BY id;";

    let (status, http_result) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({ "sql": query }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "http scalar subquery select failed: {http_result}"
    );
    let http_rows: Vec<Vec<i64>> = http_result
        .get("rows")
        .and_then(Value::as_array)
        .expect("http rows")
        .iter()
        .map(|row| {
            row.as_array()
                .expect("http row array")
                .iter()
                .map(http_sql_value_to_i64)
                .collect()
        })
        .collect();
    assert_eq!(http_rows, vec![vec![1, 250], vec![2, 250]]);

    let mut grpc_client =
        alopex_server::grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);
    let mut stream = timeout(
        Duration::from_secs(15),
        grpc_client.execute_sql(alopex_server::grpc::proto::SqlRequest {
            sql: query.to_string(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }),
    )
    .await
    .expect("grpc execute_sql timeout")
    .expect("grpc execute_sql call")
    .into_inner();

    let mut grpc_rows: Vec<Vec<i64>> = Vec::new();
    loop {
        match timeout(Duration::from_secs(15), stream.message())
            .await
            .expect("grpc stream timeout")
        {
            Ok(Some(result_set)) => {
                grpc_rows.extend(
                    result_set
                        .rows
                        .iter()
                        .map(|row| row.values.iter().map(grpc_sql_value_to_i64).collect()),
                );
            }
            Ok(None) => break,
            Err(status) => panic!(
                "grpc scalar subquery select must return rows like http, got status: {status}"
            ),
        }
    }

    assert_eq!(
        grpc_rows, http_rows,
        "gRPC ExecuteSql must return the same rows as HTTP /sql for scalar subquery SELECT"
    );
}

/// max_response_size 超過時、HTTP `/sql` と gRPC ExecuteSql が同一経路で
/// 同様にサイズ上限エラーを返すこと (issue #25 統一経路の回帰ガード)。
/// HTTP は 413 PAYLOAD_TOO_LARGE、gRPC はその写像である ResourceExhausted。
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_execute_sql_enforces_max_response_size_like_http() {
    let _server_test_guard = server_test_lock().lock().await;
    let temp = tempdir().expect("tempdir");
    let mut used = HashSet::new();
    let http_port = reserve_unique_port(&mut used);
    let admin_port = reserve_unique_port(&mut used);
    let grpc_port = reserve_unique_port(&mut used);
    // v0.7 以降の SqlResponse は routing_diagnostics を含むため、セットアップ
    // 応答 (CREATE/INSERT) が上限に触れない程度の余裕を持たせつつ、
    // 大きな行を返す SELECT だけが確実に超過するサイズに設定する。
    let config_path = write_config_with_extra(
        temp.path(),
        http_port,
        admin_port,
        grpc_port,
        "max_response_size = 8192\n",
    );
    let http_url = format!("http://127.0.0.1:{http_port}");
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let grpc_url = format!("http://127.0.0.1:{grpc_port}");
    let (_guard, client, channel) =
        spawn_server_and_wait(&config_path, &http_url, &admin_url, &grpc_url).await;

    let payload = "x".repeat(32 * 1024);
    for sql in [
        "CREATE TABLE limit_items (id INT PRIMARY KEY, payload TEXT);".to_string(),
        format!("INSERT INTO limit_items (id, payload) VALUES (1, '{payload}');"),
    ] {
        let (status, body) = send_json(
            &client,
            Method::POST,
            &format!("{http_url}/sql"),
            json!({ "sql": sql }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "setup sql failed: {body}");
    }

    let query = "SELECT payload FROM limit_items;";

    let (status, http_result) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({ "sql": query }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "http must reject oversized response: {http_result}"
    );

    let mut grpc_client =
        alopex_server::grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);
    let err = timeout(
        Duration::from_secs(15),
        grpc_client.execute_sql(alopex_server::grpc::proto::SqlRequest {
            sql: query.to_string(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }),
    )
    .await
    .expect("grpc execute_sql timeout")
    .expect_err("grpc must reject oversized response");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn cross_surface_consistency_cli_and_server_share_expected_results() {
    let _server_test_guard = server_test_lock().lock().await;
    let expected = load_fixture("cross_surface_expected.json");

    let temp = tempdir().expect("tempdir");
    let mut used = HashSet::new();
    let http_port = reserve_unique_port(&mut used);
    let admin_port = reserve_unique_port(&mut used);
    let grpc_port = reserve_unique_port(&mut used);
    let config_path = write_config(temp.path(), http_port, admin_port, grpc_port);
    let http_url = format!("http://127.0.0.1:{http_port}");
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let grpc_url = format!("http://127.0.0.1:{grpc_port}");
    let (guard, client, _grpc_channel) =
        spawn_server_and_wait(&config_path, &http_url, &admin_url, &grpc_url).await;

    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({
            "sql": "CREATE TABLE surface_items (id INT PRIMARY KEY, name TEXT, embedding VECTOR(2, L2));"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({
            "sql": "INSERT INTO surface_items (id, name, embedding) VALUES (1, 'alpha', [0.0, 0.0]), (2, 'beta', [1.0, 0.0]);"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/put"),
        json!({ "key": "shared-key", "value": [115, 104, 97, 114, 101, 100, 45, 118, 97, 108, 117, 101] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, sql_result) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/sql"),
        json!({ "sql": "SELECT id, name FROM surface_items ORDER BY id;" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, kv_result) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/kv/get"),
        json!({ "key": "shared-key" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, vector_result) = send_json(
        &client,
        Method::POST,
        &format!("{http_url}/vector/search"),
        json!({
            "table": "surface_items",
            "vector": [0.1, 0.0],
            "k": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let server_actual = json!({
        "sql_rows": normalize_sql_rows(sql_result.get("rows").unwrap_or(&Value::Null)),
        "kv_value": String::from_utf8(
            kv_result
                .get("value")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_u64().map(|x| x as u8))
                .collect::<Vec<u8>>()
        ).unwrap_or_default(),
        "vector_top_id": vector_result
            .get("results")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .and_then(|v| v.get("id"))
            .cloned()
            .unwrap_or(Value::Null),
    });

    drop(guard);

    let db = Database::open(temp.path()).expect("open db");
    let cli_sql_rows = run_cli_sql_rows(&db, "SELECT id, name FROM surface_items ORDER BY id;");
    let cli_vector_row = run_cli_sql_rows(
        &db,
        "SELECT id FROM surface_items ORDER BY vector_similarity(embedding, [0.1, 0.0], 'l2') ASC LIMIT 1;",
    );
    let cli_actual = json!({
        "sql_rows": normalize_sql_rows(&Value::Array(cli_sql_rows)),
        "kv_value": run_cli_kv_get(&db, "shared-key"),
        "vector_top_id": cli_vector_row
            .first()
            .and_then(|v| v.get("id"))
            .cloned()
            .unwrap_or(Value::Null),
    });

    let expected_common = json!({
        "sql_rows": expected.get("sql_rows").cloned().unwrap_or(Value::Null),
        "kv_value": expected.get("kv_value").cloned().unwrap_or(Value::Null),
        "vector_top_id": expected.get("vector_top_id").cloned().unwrap_or(Value::Null),
    });

    assert_json_eq_with_diff("server", &expected_common, &server_actual);
    assert_json_eq_with_diff("cli", &expected_common, &cli_actual);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn cluster_status_cross_surface_fixture_matches_server_admin_status() {
    let expected = load_fixture("cluster_status_cross_surface_expected.json");
    let expected = &expected["server_cluster_status"];

    let single_node_dir = tempdir().expect("single node tempdir");
    let single_node_actual =
        fetch_admin_cluster_status(base_server_config(single_node_dir.path().to_path_buf())).await;
    assert_json_eq_with_diff(
        "server single_node cluster status",
        &expected["single_node"],
        &single_node_actual,
    );

    let cluster_dir = tempdir().expect("cluster aware tempdir");
    let cluster_actual =
        fetch_admin_cluster_status(cluster_aware_config(cluster_dir.path().to_path_buf(), true))
            .await;
    assert_json_eq_with_diff(
        "server cluster_aware cluster status",
        &expected["cluster_aware"],
        &cluster_actual,
    );

    let degraded_dir = tempdir().expect("degraded tempdir");
    let degraded_actual = fetch_admin_cluster_status(cluster_aware_config(
        degraded_dir.path().to_path_buf(),
        false,
    ))
    .await;
    assert_json_eq_with_diff(
        "server cluster_aware_degraded cluster status",
        &expected["cluster_aware_degraded"],
        &degraded_actual,
    );
}
