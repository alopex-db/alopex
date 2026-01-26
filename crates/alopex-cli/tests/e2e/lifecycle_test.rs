use std::fs;
use std::time::Duration;

use alopex_cli::cli::{
    LifecycleBackupCommand, LifecycleCommand, LifecycleRestoreCommand, OutputFormat,
};
use alopex_cli::client::http::HttpClient;
use alopex_cli::commands::lifecycle;
use alopex_cli::commands::lifecycle::{
    execute_remote_with_formatter as execute_lifecycle_remote, RemoteLifecycleSupport, SupportLevel,
};
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use axum::extract::Path;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::oneshot;

#[test]
fn e2e_lifecycle_actions_report_success() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path();
    fs::write(data_dir.join("data.txt"), "payload").expect("seed data");

    let cases = [
        (LifecycleCommand::Archive, "Archived"),
        (LifecycleCommand::Backup { command: None }, "Backup"),
        (LifecycleCommand::Export, "Exported"),
        (
            LifecycleCommand::Restore {
                source: None,
                command: None,
            },
            "Restored",
        ),
    ];

    for (command, label) in cases {
        let mut output = Vec::new();
        lifecycle::execute(&command, Some(data_dir), &mut output, OutputFormat::Json)
            .expect("lifecycle execute");
        let text = String::from_utf8(output).expect("utf8");
        assert!(text.contains("OK"));
        assert!(text.contains(label));
    }

    assert!(data_dir.join("data.txt").exists());
}

fn build_test_client(base_url: &str) -> HttpClient {
    let config = CliServerConfig {
        url: base_url.to_string(),
        insecure: true,
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    HttpClient::new(&config).expect("http client")
}

async fn spawn_http_server(router: Router) -> (String, oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = axum::Server::from_tcp(listener)
        .expect("server")
        .serve(router.into_make_service())
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
    tokio::spawn(server);
    (format!("http://{}", addr), shutdown_tx)
}

#[tokio::test]
async fn e2e_lifecycle_status_outputs_include_fields() {
    let router = Router::new()
        .route(
            "/api/admin/backup/:id",
            get(|Path(handle): Path<String>| async move {
                Json(json!({
                    "status": "OK",
                    "handle": handle,
                    "state": "running",
                    "location": "s3://bucket/backup",
                    "message": "started"
                }))
            }),
        )
        .route(
            "/api/admin/restore/:id",
            get(|Path(handle): Path<String>| async move {
                Json(json!({
                    "status": "OK",
                    "handle": handle,
                    "state": "completed",
                    "metadata": { "files": 2 },
                    "reason": "done"
                }))
            }),
        );
    let (base_url, shutdown) = spawn_http_server(router).await;
    let client = build_test_client(&base_url);
    let support = RemoteLifecycleSupport {
        backup: SupportLevel::Supported,
        restore: SupportLevel::Supported,
    };

    let mut output = Vec::new();
    let formatter = alopex_cli::output::formatter::create_formatter(OutputFormat::Json);
    execute_lifecycle_remote(
        &client,
        &LifecycleCommand::Backup {
            command: Some(LifecycleBackupCommand::Status {
                handle: "backup-123".to_string(),
            }),
        },
        support,
        &mut output,
        formatter,
    )
    .await
    .expect("backup status");
    let value: serde_json::Value = serde_json::from_slice(&output).expect("json");
    let row = value
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .expect("row object");
    for key in [
        "Status", "Handle", "State", "Location", "Metadata", "Message",
    ] {
        assert!(row.contains_key(key), "missing {key}");
    }

    tokio::time::sleep(Duration::from_millis(5)).await;
    output.clear();
    let formatter = alopex_cli::output::formatter::create_formatter(OutputFormat::Table);
    execute_lifecycle_remote(
        &client,
        &LifecycleCommand::Restore {
            source: None,
            command: Some(LifecycleRestoreCommand::Status {
                handle: "restore-456".to_string(),
            }),
        },
        support,
        &mut output,
        formatter,
    )
    .await
    .expect("restore status");
    let text = String::from_utf8(output).expect("utf8");
    for label in [
        "Status", "Handle", "State", "Location", "Metadata", "Message",
    ] {
        assert!(text.contains(label), "missing {label}");
    }

    let _ = shutdown.send(());
}
