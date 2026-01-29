use std::path::Path;
use std::sync::Arc;

use axum::extract::{Extension, Path as AxumPath};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthMode;
use crate::http::{error_response, RequestContext};
use crate::ops::backup::BackupHandle;
use crate::ops::restore::{RestoreHandle, RestoreSource};
use crate::ops::state::{OperationState, RestoreMetadata};
use crate::ops::status::StatusReporter;
use crate::ops::status::StatusView;
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
    #[serde(flatten)]
    status: StatusView,
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

#[derive(Deserialize)]
pub struct AdminRestoreRequest {
    #[serde(default)]
    source: Option<String>,
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

#[derive(Serialize)]
struct AdminBackupResponse {
    handle: String,
    location: String,
    state: OperationState,
}

#[derive(Serialize)]
struct AdminRestoreResponse {
    handle: String,
    state: OperationState,
    metadata: Option<RestoreMetadata>,
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
    let reporter = StatusReporter::new(state.lifecycle_state.clone(), state.recovery_info.clone());
    let status = reporter.status_view();
    Json(AdminStatusResponse {
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        uptime_secs: Some(uptime),
        connections: None,
        queries_per_second: None,
        status,
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

pub async fn start_backup(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    match state.backup_coordinator.start_backup().await {
        Ok(handle) => match backup_response(&state, &handle) {
            Ok(response) => Json(response).into_response(),
            Err(err) => error_response(err, &ctx),
        },
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn backup_status(
    AxumPath(id): AxumPath<String>,
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let handle = match parse_backup_handle(&id) {
        Ok(handle) => handle,
        Err(err) => return error_response(err, &ctx),
    };
    match backup_response(&state, &handle) {
        Ok(response) => Json(response).into_response(),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn start_restore(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<AdminRestoreRequest>,
) -> Response {
    let source_path = match request.source {
        Some(source) => source.into(),
        None => match crate::ops::restore::resolve_default_source(&state.config.data_dir) {
            Ok(path) => path,
            Err(crate::error::ServerError::NotFound(_)) => {
                match state.backup_coordinator.latest_location() {
                    Some(path) => path,
                    None => {
                        let data_dir = state.config.data_dir.clone();
                        let archive_result = tokio::task::spawn_blocking(move || {
                            perform_lifecycle_action("archive", Path::new(&data_dir))
                        })
                        .await
                        .map_err(|err| crate::error::ServerError::Internal(err.to_string()))
                        .and_then(|res| res.map_err(crate::error::ServerError::BadRequest));
                        if let Err(err) = archive_result {
                            return error_response(err, &ctx);
                        }
                        match crate::ops::restore::resolve_default_source(&state.config.data_dir) {
                            Ok(path) => path,
                            Err(err) => return error_response(err, &ctx),
                        }
                    }
                }
            }
            Err(err) => return error_response(err, &ctx),
        },
    };
    let source = RestoreSource { path: source_path };
    match state.restore_coordinator.start_restore(source).await {
        Ok(handle) => match restore_response(&state, &handle) {
            Ok(response) => Json(response).into_response(),
            Err(err) => error_response(err, &ctx),
        },
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn restore_status(
    AxumPath(id): AxumPath<String>,
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let handle = match parse_restore_handle(&id) {
        Ok(handle) => handle,
        Err(err) => return error_response(err, &ctx),
    };
    match restore_response(&state, &handle) {
        Ok(response) => Json(response).into_response(),
        Err(err) => error_response(err, &ctx),
    }
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

fn parse_backup_handle(id: &str) -> crate::error::Result<BackupHandle> {
    let id = Uuid::parse_str(id)
        .map_err(|_| crate::error::ServerError::BadRequest("invalid backup handle".into()))?;
    Ok(BackupHandle { id })
}

fn parse_restore_handle(id: &str) -> crate::error::Result<RestoreHandle> {
    let id = Uuid::parse_str(id)
        .map_err(|_| crate::error::ServerError::BadRequest("invalid restore handle".into()))?;
    Ok(RestoreHandle { id })
}

fn backup_response(
    state: &ServerState,
    handle: &BackupHandle,
) -> crate::error::Result<AdminBackupResponse> {
    let location = state.backup_coordinator.location(handle)?;
    let status = state.backup_coordinator.status(handle)?;
    Ok(AdminBackupResponse {
        handle: handle.id.to_string(),
        location: location.display().to_string(),
        state: status,
    })
}

fn restore_response(
    state: &ServerState,
    handle: &RestoreHandle,
) -> crate::error::Result<AdminRestoreResponse> {
    let status = state.restore_coordinator.status(handle)?;
    let metadata = state.restore_coordinator.metadata(handle)?;
    Ok(AdminRestoreResponse {
        handle: handle.id.to_string(),
        state: status,
        metadata,
    })
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

fn write_latest_marker(root: &Path, dest: &Path) -> Result<(), String> {
    let marker = root.join("latest");
    std::fs::create_dir_all(root).map_err(|err| err.to_string())?;
    std::fs::write(&marker, dest.to_string_lossy().as_bytes()).map_err(|err| err.to_string())?;
    Ok(())
}
