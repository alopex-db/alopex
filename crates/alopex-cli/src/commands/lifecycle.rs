//! Lifecycle command handlers.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{LifecycleBackupCommand, LifecycleCommand, LifecycleRestoreCommand, OutputFormat};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy)]
pub enum SupportLevel {
    Supported,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteLifecycleSupport {
    pub backup: SupportLevel,
    pub restore: SupportLevel,
}

impl RemoteLifecycleSupport {
    pub fn unknown() -> Self {
        Self {
            backup: SupportLevel::Unknown,
            restore: SupportLevel::Unknown,
        }
    }
}

pub fn execute_with_formatter<W: Write>(
    command: &LifecycleCommand,
    data_dir: Option<&Path>,
    writer: &mut W,
    mut formatter: Box<dyn Formatter>,
) -> Result<()> {
    let message = perform_lifecycle_action(command, data_dir)?;
    let columns = vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text("OK".to_string()),
        Value::Text(message),
    ])];
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

#[derive(Serialize)]
struct RemoteLegacyRequest {
    action: String,
}

#[derive(Deserialize)]
struct RemoteLegacyResponse {
    status: String,
    message: String,
}

#[derive(Serialize)]
struct RemoteRestoreRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RemoteLifecycleStatusResponse {
    status: Option<String>,
    handle: Option<String>,
    state: Option<JsonValue>,
    location: Option<String>,
    message: Option<String>,
    reason: Option<String>,
    error: Option<String>,
    metadata: Option<JsonValue>,
}

pub async fn execute_remote_with_formatter<W: Write>(
    client: &HttpClient,
    command: &LifecycleCommand,
    support: RemoteLifecycleSupport,
    writer: &mut W,
    mut formatter: Box<dyn Formatter>,
) -> Result<()> {
    match command {
        LifecycleCommand::Archive => {
            execute_remote_legacy(client, "archive", writer, &mut formatter).await
        }
        LifecycleCommand::Export => {
            execute_remote_legacy(client, "export", writer, &mut formatter).await
        }
        LifecycleCommand::Backup { command } => match command {
            None => {
                ensure_supported(support.backup, "backup")?;
                let response: RemoteLifecycleStatusResponse = client
                    .post_json("api/admin/backup", &serde_json::json!({}))
                    .await
                    .map_err(|err| map_remote_error(err, "backup"))?;
                write_remote_status(writer, &mut formatter, response)
            }
            Some(LifecycleBackupCommand::Status { handle }) => {
                ensure_supported(support.backup, "backup status")?;
                let handle = handle.trim();
                if handle.is_empty() {
                    return Err(CliError::InvalidArgument(
                        "Backup handle must be provided.".to_string(),
                    ));
                }
                let path = format!("api/admin/backup/{handle}");
                let mut response: RemoteLifecycleStatusResponse = client
                    .get_json(&path)
                    .await
                    .map_err(|err| map_remote_error(err, "backup status"))?;
                if response.handle.is_none() {
                    response.handle = Some(handle.to_string());
                }
                write_remote_status(writer, &mut formatter, response)
            }
        },
        LifecycleCommand::Restore { command, source } => match command {
            None => {
                ensure_supported(support.restore, "restore")?;
                let source = source
                    .as_ref()
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string());
                let request = RemoteRestoreRequest { source };
                let response: RemoteLifecycleStatusResponse = client
                    .post_json("api/admin/restore", &request)
                    .await
                    .map_err(|err| map_remote_error(err, "restore"))?;
                write_remote_status(writer, &mut formatter, response)
            }
            Some(LifecycleRestoreCommand::Status { handle }) => {
                ensure_supported(support.restore, "restore status")?;
                let handle = handle.trim();
                if handle.is_empty() {
                    return Err(CliError::InvalidArgument(
                        "Restore handle must be provided.".to_string(),
                    ));
                }
                let path = format!("api/admin/restore/{handle}");
                let mut response: RemoteLifecycleStatusResponse = client
                    .get_json(&path)
                    .await
                    .map_err(|err| map_remote_error(err, "restore status"))?;
                if response.handle.is_none() {
                    response.handle = Some(handle.to_string());
                }
                write_remote_status(writer, &mut formatter, response)
            }
        },
    }
}

pub fn execute<W: Write>(
    command: &LifecycleCommand,
    data_dir: Option<&Path>,
    writer: &mut W,
    output: OutputFormat,
) -> Result<()> {
    let formatter = create_formatter(output);
    execute_with_formatter(command, data_dir, writer, formatter)
}

fn map_client_error(err: ClientError) -> CliError {
    match err {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) => CliError::InvalidArgument(message),
        ClientError::Build(message) => CliError::InvalidArgument(message),
        ClientError::Auth(err) => CliError::InvalidArgument(err.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::ServerConnection(format!("server error {status}: {body}"))
        }
    }
}

async fn execute_remote_legacy<W: Write>(
    client: &HttpClient,
    action: &str,
    writer: &mut W,
    formatter: &mut Box<dyn Formatter>,
) -> Result<()> {
    let request = RemoteLegacyRequest {
        action: action.to_string(),
    };
    let response: RemoteLegacyResponse = client
        .post_json("api/admin/lifecycle", &request)
        .await
        .map_err(map_client_error)?;

    let columns = vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text(response.status),
        Value::Text(response.message),
    ])];
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

fn write_remote_status<W: Write>(
    writer: &mut W,
    formatter: &mut Box<dyn Formatter>,
    response: RemoteLifecycleStatusResponse,
) -> Result<()> {
    let columns = remote_status_columns();
    let rows = vec![remote_status_row(response)];
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

fn remote_status_columns() -> Vec<Column> {
    vec![
        Column::new("Status", DataType::Text),
        Column::new("Handle", DataType::Text),
        Column::new("State", DataType::Text),
        Column::new("Location", DataType::Text),
        Column::new("Metadata", DataType::Text),
        Column::new("Message", DataType::Text),
    ]
}

fn remote_status_row(response: RemoteLifecycleStatusResponse) -> Row {
    let status = resolve_status(&response);
    let message = resolve_message(&response);
    Row::new(vec![
        Value::Text(status),
        text_or_null(response.handle),
        text_or_null(state_label(&response.state)),
        text_or_null(response.location),
        metadata_to_value(response.metadata),
        text_or_null(message),
    ])
}

fn resolve_status(response: &RemoteLifecycleStatusResponse) -> String {
    if let Some(status) = response
        .status
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        return status.to_string();
    }
    if let Some(state) = state_label(&response.state)
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        if is_failure_state(&state) {
            return "Error".to_string();
        }
    }
    "OK".to_string()
}

fn resolve_message(response: &RemoteLifecycleStatusResponse) -> Option<String> {
    for candidate in [&response.message, &response.reason, &response.error] {
        if let Some(value) = candidate
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            return Some(value.to_string());
        }
    }
    if let Some(reason) = state_reason(&response.state) {
        return Some(reason);
    }
    None
}

fn is_failure_state(state: &str) -> bool {
    matches!(
        state.to_lowercase().as_str(),
        "failed" | "error" | "failure"
    )
}

fn state_label(state: &Option<JsonValue>) -> Option<String> {
    let value = state.as_ref()?;
    match value {
        JsonValue::String(text) => Some(text.clone()),
        JsonValue::Object(map) => map
            .get("status")
            .and_then(|status| status.as_str())
            .map(|status| status.to_string())
            .or_else(|| Some(value.to_string())),
        _ => Some(value.to_string()),
    }
}

fn state_reason(state: &Option<JsonValue>) -> Option<String> {
    let JsonValue::Object(map) = state.as_ref()? else {
        return None;
    };
    map.get("reason")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn text_or_null(value: Option<String>) -> Value {
    match value {
        Some(value) if !value.trim().is_empty() => Value::Text(value),
        _ => Value::Null,
    }
}

fn metadata_to_value(metadata: Option<JsonValue>) -> Value {
    match metadata {
        Some(value) => Value::Text(value.to_string()),
        None => Value::Null,
    }
}

fn ensure_supported(level: SupportLevel, action: &str) -> Result<()> {
    if matches!(level, SupportLevel::Unsupported) {
        return Err(CliError::ServerUnsupported(unsupported_message(action)));
    }
    Ok(())
}

fn map_remote_error(err: ClientError, action: &str) -> CliError {
    match err {
        ClientError::HttpStatus { status, .. } if is_unsupported_status(status) => {
            CliError::ServerUnsupported(unsupported_message(action))
        }
        _ => map_client_error(err),
    }
}

fn is_unsupported_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED
    )
}

fn unsupported_message(action: &str) -> String {
    format!(
        "Remote {action} requires a server that supports admin API v0.5. Upgrade the server or use archive/export instead."
    )
}

fn perform_lifecycle_action(command: &LifecycleCommand, data_dir: Option<&Path>) -> Result<String> {
    let data_dir = data_dir.ok_or_else(|| {
        CliError::InvalidArgument("Lifecycle actions require a local data directory.".to_string())
    })?;
    if !data_dir.exists() {
        return Err(CliError::InvalidArgument(format!(
            "Data directory does not exist: {}",
            data_dir.display()
        )));
    }
    if !data_dir.is_dir() {
        return Err(CliError::InvalidArgument(format!(
            "Data directory is not a directory: {}",
            data_dir.display()
        )));
    }

    let lifecycle_root = data_dir.join(".lifecycle");
    fs::create_dir_all(&lifecycle_root)?;

    match command {
        LifecycleCommand::Archive => {
            let dest = lifecycle_root.join("archive").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("archive"), &dest)?;
            Ok(format!("Archived data to {}", dest.display()))
        }
        LifecycleCommand::Restore { command, .. } => {
            if command.is_some() {
                return Err(CliError::InvalidArgument(
                    "Restore status is only available for server profiles.".to_string(),
                ));
            }
            let archive_root = lifecycle_root.join("archive");
            let latest = read_latest_marker(&archive_root)?;
            let backup_dir = lifecycle_root.join("restore-backup").join(timestamp_dir());
            copy_data_dir(data_dir, &backup_dir)?;
            clear_data_dir(data_dir)?;
            copy_data_dir(&latest, data_dir)?;
            Ok(format!(
                "Restored data from {} (backup at {})",
                latest.display(),
                backup_dir.display()
            ))
        }
        LifecycleCommand::Backup { command } => {
            if command.is_some() {
                return Err(CliError::InvalidArgument(
                    "Backup status is only available for server profiles.".to_string(),
                ));
            }
            let dest = lifecycle_root.join("backup").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("backup"), &dest)?;
            Ok(format!("Backup created at {}", dest.display()))
        }
        LifecycleCommand::Export => {
            let dest = lifecycle_root.join("export").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("export"), &dest)?;
            Ok(format!("Exported data to {}", dest.display()))
        }
    }
}

fn timestamp_dir() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("ts-{seconds}")
}

fn copy_data_dir(src: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    copy_dir_filtered(src, dest)
}

fn copy_dir_filtered(src: &Path, dest: &Path) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let src_path = entry.path();
        let dest_path = dest.join(&name);
        if file_type.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_dir_filtered(&src_path, &dest_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dest_path)?;
        }
    }
    Ok(())
}

fn clear_data_dir(data_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else if path.is_file() {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn write_latest_marker(root: &Path, latest: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    let marker = root.join("latest");
    fs::write(marker, latest.display().to_string().as_bytes())?;
    Ok(())
}

fn read_latest_marker(root: &Path) -> Result<PathBuf> {
    let marker = root.join("latest");
    if !marker.exists() {
        return Err(CliError::InvalidArgument(
            "No archive snapshot found to restore.".to_string(),
        ));
    }
    let path = fs::read_to_string(&marker)?;
    let path = PathBuf::from(path.trim());
    if !path.exists() {
        return Err(CliError::InvalidArgument(format!(
            "Latest archive path does not exist: {}",
            path.display()
        )));
    }
    Ok(path)
}
