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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
    let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    (format!("http://{}", addr), shutdown_tx)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
    let headers = extract_table_headers(&text);
    for label in [
        "Status", "Handle", "State", "Location", "Metadata", "Message",
    ] {
        let lower = label.to_ascii_lowercase();
        assert!(
            headers
                .iter()
                .any(|header| header.to_ascii_lowercase() == lower),
            "missing {label}"
        );
    }

    let _ = shutdown.send(());
}

fn extract_table_headers(text: &str) -> Vec<String> {
    let mut header_lines = Vec::new();
    let mut in_header = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('┌') {
            in_header = true;
            continue;
        }
        if in_header && (trimmed.starts_with('╞') || trimmed.starts_with('├')) {
            break;
        }
        if in_header && (trimmed.contains('│') || trimmed.contains('┆')) {
            header_lines.push(trimmed.to_string());
        }
    }
    if header_lines.is_empty() {
        return Vec::new();
    }
    let first: Vec<char> = header_lines[0].chars().collect();
    let mut boundaries = Vec::new();
    for (idx, ch) in first.iter().enumerate() {
        if *ch == '│' || *ch == '┆' {
            boundaries.push(idx);
        }
    }
    let mut headers = Vec::new();
    for window in boundaries.windows(2) {
        let start = window[0] + 1;
        let end = window[1];
        let mut label = String::new();
        for line in &header_lines {
            let chars: Vec<char> = line.chars().collect();
            if end <= chars.len() {
                let slice: String = chars[start..end].iter().collect();
                label.push_str(slice.trim());
            }
        }
        let label = label.replace(' ', "");
        if !label.is_empty() {
            headers.push(label);
        }
    }
    headers
}
