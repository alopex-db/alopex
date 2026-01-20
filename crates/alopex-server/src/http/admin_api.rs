use std::path::Path;
use std::sync::Arc;

use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthMode;
use crate::server::ServerState;

#[derive(Serialize)]
struct AdminCapabilitiesResponse {
    scope: &'static str,
    allowed_actions: Vec<&'static str>,
}

#[derive(Serialize)]
struct AdminStatusResponse {
    version: Option<String>,
    uptime_secs: Option<u64>,
    connections: Option<u64>,
    queries_per_second: Option<f64>,
}

#[derive(Serialize)]
struct AdminMetricsResponse {
    qps: Option<f64>,
    avg_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    memory_usage_mb: Option<u64>,
    active_connections: Option<u64>,
}

#[derive(Serialize)]
struct AdminHealthResponse {
    status: &'static str,
    message: &'static str,
}

#[derive(Deserialize)]
pub struct AdminLifecycleRequest {
    action: String,
}

#[derive(Serialize)]
struct AdminLifecycleResponse {
    status: &'static str,
    message: String,
}

#[derive(Serialize)]
struct AdminCompactionResponse {
    success: bool,
    message: String,
}

pub async fn capabilities(Extension(state): Extension<Arc<ServerState>>) -> impl IntoResponse {
    let (scope, allowed_actions) = capabilities_for_auth(&state.auth);
    Json(AdminCapabilitiesResponse {
        scope,
        allowed_actions,
    })
}

pub async fn status(Extension(state): Extension<Arc<ServerState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs();
    Json(AdminStatusResponse {
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        uptime_secs: Some(uptime),
        connections: None,
        queries_per_second: None,
    })
}

pub async fn metrics(Extension(_state): Extension<Arc<ServerState>>) -> impl IntoResponse {
    Json(AdminMetricsResponse {
        qps: None,
        avg_latency_ms: None,
        p99_latency_ms: None,
        memory_usage_mb: None,
        active_connections: None,
    })
}

pub async fn health() -> impl IntoResponse {
    Json(AdminHealthResponse {
        status: "ok",
        message: "ready",
    })
}

pub async fn compaction() -> impl IntoResponse {
    Json(AdminCompactionResponse {
        success: false,
        message: "Compaction is not available on this server build.".to_string(),
    })
}

pub async fn lifecycle(
    Extension(state): Extension<Arc<ServerState>>,
    Json(request): Json<AdminLifecycleRequest>,
) -> impl IntoResponse {
    let data_dir = state.config.data_dir.clone();
    let action = request.action;
    let result = tokio::task::spawn_blocking(move || {
        perform_lifecycle_action(action.as_str(), Path::new(&data_dir))
    })
    .await
    .map_err(|err| err.to_string())
    .and_then(|res| res.map_err(|err| err.to_string()));

    match result {
        Ok(message) => (
            StatusCode::OK,
            Json(AdminLifecycleResponse {
                status: "OK",
                message,
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(AdminLifecycleResponse {
                status: "Error",
                message: err,
            }),
        )
            .into_response(),
    }
}

fn capabilities_for_auth(auth: &crate::auth::AuthMiddleware) -> (&'static str, Vec<&'static str>) {
    match auth.mode() {
        AuthMode::None => ("full", Vec::new()),
        AuthMode::Dev { .. } => ("restricted", all_actions()),
    }
}

fn all_actions() -> Vec<&'static str> {
    vec![
        "read", "create", "update", "delete", "archive", "restore", "backup", "export",
    ]
}

fn perform_lifecycle_action(action: &str, data_dir: &Path) -> Result<String, String> {
    if !data_dir.exists() {
        return Err(format!(
            "Data directory does not exist: {}",
            data_dir.display()
        ));
    }
    if !data_dir.is_dir() {
        return Err(format!(
            "Data directory is not a directory: {}",
            data_dir.display()
        ));
    }

    let lifecycle_root = data_dir.join(".lifecycle");
    std::fs::create_dir_all(&lifecycle_root).map_err(|err| err.to_string())?;

    match action {
        "archive" => {
            let dest = lifecycle_root.join("archive").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("archive"), &dest)?;
            Ok(format!("Archived data to {}", dest.display()))
        }
        "restore" => {
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
        "backup" => {
            let dest = lifecycle_root.join("backup").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("backup"), &dest)?;
            Ok(format!("Backup created at {}", dest.display()))
        }
        "export" => {
            let dest = lifecycle_root.join("export").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("export"), &dest)?;
            Ok(format!("Exported data to {}", dest.display()))
        }
        _ => Err("Unknown lifecycle action.".to_string()),
    }
}

fn timestamp_dir() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("ts-{seconds}")
}

fn copy_data_dir(src: &Path, dest: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dest).map_err(|err| err.to_string())?;
    copy_dir_filtered(src, dest)
}

fn copy_dir_filtered(src: &Path, dest: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(src).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let file_type = entry.file_type().map_err(|err| err.to_string())?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let dest_path = dest.join(name);
        if file_type.is_dir() {
            copy_data_dir(&entry.path(), &dest_path)?;
        } else {
            std::fs::copy(entry.path(), &dest_path).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

fn clear_data_dir(dir: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path).map_err(|err| err.to_string())?;
        } else {
            std::fs::remove_file(&path).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

fn write_latest_marker(root: &Path, dest: &Path) -> Result<(), String> {
    let marker = root.join("latest");
    std::fs::create_dir_all(root).map_err(|err| err.to_string())?;
    std::fs::write(&marker, dest.to_string_lossy().as_bytes()).map_err(|err| err.to_string())?;
    Ok(())
}

fn read_latest_marker(root: &Path) -> Result<std::path::PathBuf, String> {
    let marker = root.join("latest");
    let contents = std::fs::read_to_string(&marker).map_err(|err| err.to_string())?;
    let path = contents.trim();
    if path.is_empty() {
        return Err("Lifecycle archive marker is empty.".to_string());
    }
    Ok(std::path::PathBuf::from(path))
}
