use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::client::http::HttpClient;
use alopex_cli::cli::{OutputFormat, SqlCommand};
use alopex_cli::commands::sql::{execute_remote_with_formatter_control, SqlExecutionOptions};
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use alopex_cli::streaming::{CancelSignal, Deadline};
use axum::body::{boxed, Body, Bytes};
use axum::extract::State;
use axum::routing::post;
use axum::Router;
use futures_util::stream;
use rcgen::generate_simple_self_signed;
use serde_json::json;
use tokio::sync::oneshot;

fn batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: true,
        source: BatchModeSource::Explicit,
    }
}

async fn spawn_tls_server(router: Router) -> (String, oneshot::Sender<()>) {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let dir = tempfile::tempdir().expect("tempdir");
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

    (format!("https://{}", addr), shutdown_tx)
}

fn build_client(base_url: &str) -> HttpClient {
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

fn build_stream_body(total_rows: usize) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    current.push('[');
    for idx in 0..total_rows {
        if idx > 0 {
            current.push(',');
        }
        current.push_str(&format!("{{\"id\":{}}}", idx));
        if current.len() > 1024 {
            chunks.push(Bytes::from(current.clone()));
            current.clear();
        }
    }
    current.push(']');
    if !current.is_empty() {
        chunks.push(Bytes::from(current));
    }
    chunks
}

#[tokio::test]
async fn e2e_streaming_large_dataset() {
    let chunks = Arc::new(build_stream_body(500));

    let router = Router::new()
        .route(
            "/api/sql/query",
            post(
                |State(chunks): State<Arc<Vec<Bytes>>>| async move {
                    let stream = stream::iter(
                        chunks
                            .iter()
                            .cloned()
                            .map(Ok::<Bytes, Infallible>),
                    );
                    let body = boxed(Body::wrap_stream(stream));
                    let mut response = axum::response::Response::new(body);
                    *response.status_mut() = axum::http::StatusCode::OK;
                    response
                },
            ),
        )
        .with_state(chunks.clone());

    let (base_url, shutdown) = spawn_tls_server(router).await;
    let client = build_client(&base_url);

    let cmd = SqlCommand {
        query: Some("SELECT id FROM items".to_string()),
        file: None,
        fetch_size: Some(200),
        max_rows: None,
        deadline: None,
        tui: false,
    };

    let formatter = create_formatter(OutputFormat::Json);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(10));
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
            cancel: &cancel,
            deadline: &deadline,
        },
    )
    .await
    .expect("streaming");

    let value: serde_json::Value = serde_json::from_slice(&output).expect("json");
    let rows = value.as_array().expect("array");
    assert_eq!(rows.len(), 500);
    assert_eq!(rows[0]["id"], json!(0));
    assert_eq!(rows[499]["id"], json!(499));

    let _ = shutdown.send(());
}
