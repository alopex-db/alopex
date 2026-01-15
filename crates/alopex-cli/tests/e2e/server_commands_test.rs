use std::time::Duration;

use alopex_cli::client::http::HttpClient;
use alopex_cli::commands::server::execute_remote as execute_server_remote;
use alopex_cli::cli::{CompactionCommand, ServerCommand};
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use axum::routing::{get, post};
use axum::Json;
use rcgen::generate_simple_self_signed;
use serde_json::json;
use tokio::sync::oneshot;

async fn spawn_tls_server(router: axum::Router) -> (String, oneshot::Sender<()>) {
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

#[tokio::test]
async fn e2e_server_commands_workflow() {
    let router = axum::Router::new()
        .route(
            "/api/admin/status",
            get(|| async {
                Json(json!({
                    "version": "0.4.1",
                    "uptime_secs": 120,
                    "connections": 4,
                    "queries_per_second": 2.5
                }))
            }),
        )
        .route(
            "/api/admin/metrics",
            get(|| async {
                Json(json!({
                    "qps": 2.5,
                    "avg_latency_ms": 1.2,
                    "p99_latency_ms": 5.4,
                    "memory_usage_mb": 256,
                    "active_connections": 4
                }))
            }),
        )
        .route(
            "/api/admin/health",
            get(|| async { Json(json!({ "status": "ok", "message": "ready" })) }),
        )
        .route(
            "/api/admin/compaction",
            post(|| async { Json(json!({ "success": true, "message": "started" })) }),
        );

    let (base_url, shutdown) = spawn_tls_server(router).await;
    let client = build_client(&base_url);
    let mut output = Vec::new();

    execute_server_remote(&client, &ServerCommand::Status, &mut output, false)
        .await
        .expect("status");
    let text = String::from_utf8(std::mem::take(&mut output)).expect("utf8");
    assert!(text.contains("Version"));
    assert!(text.contains("0.4.1"));
    assert!(text.contains("QPS"));

    execute_server_remote(&client, &ServerCommand::Metrics, &mut output, false)
        .await
        .expect("metrics");
    let text = String::from_utf8(std::mem::take(&mut output)).expect("utf8");
    assert!(text.contains("P99"));
    assert!(text.contains("256"));

    execute_server_remote(&client, &ServerCommand::Health, &mut output, false)
        .await
        .expect("health");
    let text = String::from_utf8(std::mem::take(&mut output)).expect("utf8");
    assert!(text.contains("ok"));

    execute_server_remote(
        &client,
        &ServerCommand::Compaction {
            command: CompactionCommand::Trigger,
        },
        &mut output,
        false,
    )
    .await
    .expect("compaction");
    let text = String::from_utf8(std::mem::take(&mut output)).expect("utf8");
    assert!(text.contains("OK"));

    let _ = shutdown.send(());
}
