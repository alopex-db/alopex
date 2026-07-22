use std::path::Path;
use std::sync::Arc;

use alopex_cluster::{
    bootstrap_cluster_control, ClusterBootstrapConfig, ClusterBootstrapMode,
    ClusterBootstrapOutcome, ClusterMode, ClusterStatusSnapshot, UpgradeOperation,
};
use alopex_core::kv::any::AnyKV;
use axum::extract::{Extension, Path as AxumPath};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthMode;
use crate::http::{error_response, RequestContext};
use crate::metrics::ClusterMetricsSurface;
use crate::ops::backup::{export_snapshot, BackupHandle};
use crate::ops::restore::{RestoreHandle, RestoreSource};
use crate::ops::state::{OperationState, RestoreMetadata};
use crate::ops::status::StatusReporter;
use crate::ops::status::StatusView;
use crate::server::ServerState;

#[derive(Serialize)]
struct AdminCapabilitiesResponse {
    scope: &'static str,
    allowed_actions: Vec<&'static str>,
    unsupported_actions: Vec<&'static str>,
}

#[derive(Serialize)]
struct AdminStatusResponse {
    version: Option<String>,
    uptime_secs: Option<u64>,
    connections: Option<u64>,
    queries_per_second: Option<f64>,
    cluster: ClusterStatusSnapshot,
    cluster_control: ClusterControlAvailability,
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
    cluster: ClusterStatusSnapshot,
    cluster_metrics: ClusterMetricsSurface,
}

#[derive(Serialize)]
struct AdminHealthResponse {
    status: &'static str,
    message: &'static str,
    degraded: bool,
    cluster: ClusterStatusSnapshot,
}

#[derive(Serialize)]
struct AdminClusterOperationResponse {
    action: &'static str,
    cluster: ClusterStatusSnapshot,
}

/// Whether the running process can safely accept multi-node metadata control.
/// A missing prerequisite is a normal, machine-readable state rather than an
/// implicit in-memory fallback.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterControlAvailability {
    pub available: bool,
    pub mode: ClusterMode,
    pub reason: &'static str,
    pub missing_prerequisites: Vec<alopex_cluster::ClusterCapabilityPrerequisite>,
}

#[derive(Serialize)]
struct AdminClusterMetadataResponse {
    cluster: ClusterStatusSnapshot,
    control: ClusterControlAvailability,
    metadata_state_version: Option<u64>,
    schema_rollout: Option<serde_json::Value>,
    upgrade: Option<UpgradeOperation>,
}

/// Explicitly typed cluster management operations.  These are management API
/// verbs, not SQL DDL statements and never carry a user SQL string.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminClusterManagementOperation {
    MetadataShow,
    MembersList,
    MembersReplace,
    RangesList,
    RangesRegister,
    RangesUpdate,
    RangesRetire,
    PlacementGet,
    PlacementSet,
    PlacementReplace,
    ReadPolicyGet,
    ReadPolicySet,
    SchemaOwnerGet,
    SchemaOwnerSet,
    SchemaRolloutStart,
    SchemaRolloutStatus,
    RecoveryStatus,
    RecoveryRestore,
    UpgradeStatus,
    UpgradeStart,
}

impl AdminClusterManagementOperation {
    fn is_mutation(self) -> bool {
        !matches!(
            self,
            Self::MetadataShow
                | Self::MembersList
                | Self::RangesList
                | Self::PlacementGet
                | Self::ReadPolicyGet
                | Self::SchemaOwnerGet
                | Self::SchemaRolloutStatus
                | Self::RecoveryStatus
                | Self::UpgradeStatus
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct AdminClusterManagementRequest {
    pub request_id: String,
    pub operation: AdminClusterManagementOperation,
    #[serde(default)]
    pub expected_version: Option<u64>,
    /// The public target is opaque at this adapter boundary; the later typed
    /// consensus adapter validates it against immutable metadata.
    #[serde(default)]
    pub target: Option<serde_json::Value>,
    #[serde(default)]
    pub confirmed: bool,
}

#[derive(Serialize)]
struct AdminClusterManagementResponse {
    operation_id: String,
    operation: AdminClusterManagementOperation,
    outcome_class: &'static str,
    reason: &'static str,
    state_version: Option<u64>,
    control: ClusterControlAvailability,
    actor: Option<String>,
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
struct AdminExportResponse {
    status: &'static str,
    location: String,
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
    let (scope, allowed_actions, unsupported_actions) = capabilities_for_auth(&state.auth);
    Json(AdminCapabilitiesResponse {
        scope,
        allowed_actions,
        unsupported_actions,
    })
}

pub async fn status(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let uptime = state.start_time.elapsed().as_secs();
    let reporter = StatusReporter::new(state.lifecycle_state.clone(), state.recovery_info.clone());
    let status = reporter.status_view();
    let cluster = match state.cluster_status_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, &ctx),
    };
    let cluster_control = match cluster_control_availability(&cluster) {
        Ok(control) => control,
        Err(err) => return error_response(err, &ctx),
    };
    state.metrics.record_cluster_status(&cluster);
    Json(AdminStatusResponse {
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        uptime_secs: Some(uptime),
        connections: None,
        queries_per_second: None,
        cluster,
        cluster_control,
        status,
    })
    .into_response()
}

pub async fn cluster_metadata(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let cluster = match state.cluster_status_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, &ctx),
    };
    let control = match cluster_control_availability(&cluster) {
        Ok(control) => control,
        Err(err) => return error_response(err, &ctx),
    };
    let upgrade = state.upgrade_coordinator.status().ok();
    Json(AdminClusterMetadataResponse {
        cluster,
        control,
        metadata_state_version: None,
        schema_rollout: None,
        upgrade,
    })
    .into_response()
}

pub async fn cluster_management(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<AdminClusterManagementRequest>,
) -> Response {
    let cluster = match state.cluster_status_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, &ctx),
    };
    let control = match cluster_control_availability(&cluster) {
        Ok(control) => control,
        Err(err) => return error_response(err, &ctx),
    };
    let (outcome_class, reason) = if request.operation.is_mutation() && !request.confirmed {
        ("terminal_failure", "confirmation_required")
    } else if !control.available {
        ("terminal_failure", "cluster_capability_unavailable")
    } else {
        // This branch becomes reachable only after a compatible Chirps
        // consensus adapter is installed. Keeping it classified as pending
        // prevents a route registration from falsely claiming a commit.
        ("pending", "metadata_consensus_adapter_not_attached")
    };
    Json(AdminClusterManagementResponse {
        operation_id: request.request_id,
        operation: request.operation,
        outcome_class,
        reason,
        state_version: None,
        control,
        actor: ctx.actor,
    })
    .into_response()
}

pub async fn metrics(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let cluster = match state.cluster_status_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, &ctx),
    };
    state.metrics.record_cluster_status(&cluster);
    Json(AdminMetricsResponse {
        qps: None,
        avg_latency_ms: None,
        p99_latency_ms: None,
        memory_usage_mb: None,
        active_connections: None,
        cluster_metrics: ClusterMetricsSurface::from(&cluster),
        cluster,
    })
    .into_response()
}

pub async fn health(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let cluster = match state.cluster_status_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, &ctx),
    };
    state.metrics.record_cluster_status(&cluster);
    let (status, message) = if cluster.degraded {
        ("degraded", "cluster status degraded")
    } else {
        ("ok", "ready")
    };
    Json(AdminHealthResponse {
        status,
        message,
        degraded: cluster.degraded,
        cluster,
    })
    .into_response()
}

pub async fn cluster_join(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    cluster_operation_response(&state, &ctx, "join")
}

pub async fn cluster_leave(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    cluster_operation_response(&state, &ctx, "leave")
}

/// Applies the same bootstrap gate used by the Rust control plane before an
/// HTTP route advertises metadata mutation support.
pub fn cluster_control_availability(
    cluster: &ClusterStatusSnapshot,
) -> crate::error::Result<ClusterControlAvailability> {
    if cluster.mode == ClusterMode::SingleNode {
        return Ok(ClusterControlAvailability {
            available: false,
            mode: cluster.mode,
            reason: "single_node_mode",
            missing_prerequisites: Vec::new(),
        });
    }
    let outcome = bootstrap_cluster_control(&ClusterBootstrapConfig::compiled_chirps(
        ClusterBootstrapMode::ClusterAware,
    ));
    match outcome {
        ClusterBootstrapOutcome::ReadyForClusterControl => Ok(ClusterControlAvailability {
            available: true,
            mode: cluster.mode,
            reason: "ready",
            missing_prerequisites: Vec::new(),
        }),
        ClusterBootstrapOutcome::CapabilityUnavailable {
            missing_prerequisites,
        } => Ok(ClusterControlAvailability {
            available: false,
            mode: cluster.mode,
            reason: "cluster_capability_unavailable",
            missing_prerequisites,
        }),
        ClusterBootstrapOutcome::SingleNode => unreachable!("cluster-aware input was supplied"),
    }
}

pub async fn compaction(
    Extension(_state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    error_response(
        crate::error::ServerError::NotImplemented(
            "manual compaction is not available for the server's LSM storage engine".into(),
        ),
        &ctx,
    )
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

pub async fn export(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
) -> Response {
    let export_state = state.clone();
    let result = tokio::task::spawn_blocking(move || perform_export(export_state.as_ref()))
        .await
        .map_err(|err| crate::error::ServerError::Internal(err.to_string()))
        .and_then(|res| res);

    match result {
        Ok(location) => Json(AdminExportResponse {
            status: "OK",
            location,
        })
        .into_response(),
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

fn capabilities_for_auth(
    auth: &crate::auth::AuthMiddleware,
) -> (&'static str, Vec<&'static str>, Vec<&'static str>) {
    match auth.mode() {
        AuthMode::None => ("full", Vec::new(), unsupported_actions()),
        AuthMode::Dev { .. } => ("restricted", all_actions(), unsupported_actions()),
    }
}

fn unsupported_actions() -> Vec<&'static str> {
    vec!["compaction"]
}

fn all_actions() -> Vec<&'static str> {
    vec![
        "read", "create", "update", "delete", "archive", "restore", "backup", "export", "join",
        "leave",
    ]
}

fn cluster_operation_response(
    state: &Arc<ServerState>,
    ctx: &RequestContext,
    action: &'static str,
) -> Response {
    let cluster = match action {
        "join" => state.cluster_join(),
        "leave" => state.cluster_leave(),
        _ => unreachable!("cluster membership action is fixed by route"),
    };
    let cluster = match cluster {
        Ok(snapshot) => snapshot,
        Err(err) => return error_response(err, ctx),
    };
    state.metrics.record_cluster_status(&cluster);
    Json(AdminClusterOperationResponse { action, cluster }).into_response()
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

fn perform_export(state: &ServerState) -> crate::error::Result<String> {
    match state.store.as_ref() {
        AnyKV::Lsm(kv) => {
            let _ = kv.checkpoint()?;
        }
        _ => {
            return Err(crate::error::ServerError::BadRequest(
                "checkpoint unsupported for current storage engine".to_string(),
            ));
        }
    }
    let data_dir = state.config.data_dir.as_path();
    let lifecycle_root = data_dir.join(".lifecycle");
    std::fs::create_dir_all(&lifecycle_root)?;
    let dest = lifecycle_root.join("export").join(timestamp_dir());
    std::fs::create_dir_all(&dest)?;
    export_snapshot(data_dir, &dest)?;
    write_latest_marker(&lifecycle_root.join("export"), &dest)
        .map_err(crate::error::ServerError::Internal)?;
    Ok(dest.display().to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_cluster::{ClusterManager, ClusterManagerConfig};

    #[test]
    fn single_node_metadata_route_never_advertises_multi_node_control() {
        let manager = ClusterManager::new(ClusterManagerConfig::single_node()).unwrap();
        let availability = cluster_control_availability(&manager.status_snapshot()).unwrap();

        assert!(!availability.available);
        assert_eq!(availability.reason, "single_node_mode");
        assert!(availability.missing_prerequisites.is_empty());
    }

    #[test]
    fn only_read_operations_skip_explicit_mutation_confirmation() {
        assert!(!AdminClusterManagementOperation::MetadataShow.is_mutation());
        assert!(!AdminClusterManagementOperation::UpgradeStatus.is_mutation());
        assert!(AdminClusterManagementOperation::RangesRegister.is_mutation());
        assert!(AdminClusterManagementOperation::SchemaRolloutStart.is_mutation());
    }
}
