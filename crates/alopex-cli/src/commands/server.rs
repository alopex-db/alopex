//! Server management commands.

use std::io::Write;

use serde::Deserialize;

use crate::batch::ClusterManagementOutcome;
use crate::cli::{
    ClusterCommand, ClusterMembersCommand, ClusterMetadataCommand, ClusterMutationRequest,
    ClusterOperationRequest, ClusterPlacementCommand, ClusterRangesCommand,
    ClusterReadPolicyCommand, ClusterRecoveryCommand, ClusterSchemaCommand,
    ClusterSchemaOwnerCommand, ClusterSchemaRolloutCommand, ClusterTargetedReadRequest,
    ClusterUpgradeCommand, CompactionCommand, OutputFormat, ServerCommand,
};
use crate::client::admin_resources::{
    invoke_cluster_management, ClusterManagementOperation, ClusterManagementRequest,
    ClusterManagementResponse,
};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, Row};
use crate::output::server as server_output;
use crate::output::table::TableFormatter;
use crate::output::Formatter;
use crate::tui::renderer::render_output;

#[derive(Debug, Deserialize)]
struct ServerStatusResponse {
    version: Option<String>,
    uptime_secs: Option<u64>,
    connections: Option<u64>,
    queries_per_second: Option<f64>,
    cluster: Option<ServerClusterStatus>,
}

#[derive(Debug, Deserialize)]
struct ServerMetricsResponse {
    qps: Option<f64>,
    avg_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    memory_usage_mb: Option<u64>,
    active_connections: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ServerHealthResponse {
    status: Option<String>,
    message: Option<String>,
    degraded: Option<bool>,
    cluster: Option<ServerClusterStatus>,
}

#[derive(Debug, Deserialize)]
struct ServerCompactionResponse {
    success: Option<bool>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerClusterOperationResponse {
    action: Option<String>,
    cluster: Option<ServerClusterStatus>,
}

#[derive(Debug, Deserialize)]
struct ServerClusterStatus {
    schema_version: Option<u32>,
    mode: Option<String>,
    identity: Option<ServerClusterIdentity>,
    routing_capabilities: Option<ServerRoutingCapabilities>,
    degraded: Option<bool>,
    diagnostics: Option<Vec<ServerClusterDiagnostic>>,
}

#[derive(Debug, Deserialize)]
struct ServerClusterIdentity {
    node_id: Option<String>,
    lifecycle_state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerRoutingCapabilities {
    local_only: Option<bool>,
    future_distributed_execution_required: Option<bool>,
    scatter_gather_simulated: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ServerClusterDiagnostic {
    code: Option<String>,
}

/// Execute a server management command against a remote server.
#[allow(dead_code)] // Retained as the table-output compatibility API for library callers.
pub async fn execute_remote<W: Write>(
    client: &HttpClient,
    cmd: &ServerCommand,
    writer: &mut W,
    quiet: bool,
) -> Result<()> {
    execute_remote_with_format(client, cmd, writer, quiet, OutputFormat::Table).await
}

/// Execute a server management command with the requested non-TUI output
/// format. Existing callers retain table output through `execute_remote`.
pub async fn execute_remote_with_format<W: Write>(
    client: &HttpClient,
    cmd: &ServerCommand,
    writer: &mut W,
    quiet: bool,
    output_format: OutputFormat,
) -> Result<()> {
    match cmd {
        ServerCommand::Status => {
            let response: ServerStatusResponse = client
                .get_json("api/admin/status")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_table(
                writer,
                server_output::status_columns(),
                vec![status_row_from_response(&response)],
            )
        }
        ServerCommand::Metrics => {
            let response: ServerMetricsResponse = client
                .get_json("api/admin/metrics")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_table(
                writer,
                server_output::metrics_columns(),
                vec![server_output::metrics_row(
                    response.qps,
                    response.avg_latency_ms,
                    response.p99_latency_ms,
                    response.memory_usage_mb,
                    response.active_connections,
                )],
            )
        }
        ServerCommand::Health => {
            let response: ServerHealthResponse = client
                .get_json("api/admin/health")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_table(
                writer,
                server_output::health_columns(),
                vec![health_row_from_response(&response)],
            )
        }
        ServerCommand::Join => {
            let response = execute_cluster_operation(client, "join").await?;
            if quiet {
                return Ok(());
            }
            render_cluster_operation(writer, &response)
        }
        ServerCommand::Leave => {
            let response = execute_cluster_operation(client, "leave").await?;
            if quiet {
                return Ok(());
            }
            render_cluster_operation(writer, &response)
        }
        ServerCommand::Compaction { command } => match command {
            CompactionCommand::Trigger => {
                let request = serde_json::json!({});
                let response: ServerCompactionResponse = client
                    .post_json("api/admin/compaction", &request)
                    .await
                    .map_err(map_client_error)?;
                if quiet {
                    return Ok(());
                }
                render_table(
                    writer,
                    server_output::compaction_columns(),
                    vec![server_output::compaction_row(
                        response.success,
                        response.message.as_deref(),
                    )],
                )
            }
        },
        ServerCommand::Cluster { command } => {
            let response = execute_cluster_management_command(client, command).await?;
            if quiet {
                return ensure_cluster_management_succeeded(&response);
            }
            render_cluster_management(writer, &response, output_format)?;
            ensure_cluster_management_succeeded(&response)
        }
    }
}

pub async fn execute_remote_tui(
    client: &HttpClient,
    cmd: &ServerCommand,
    quiet: bool,
    connection_label: impl Into<String>,
    output_format: OutputFormat,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>>,
) -> Result<()> {
    match cmd {
        ServerCommand::Status => {
            let response: ServerStatusResponse = client
                .get_json("api/admin/status")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_output(
                server_output::status_columns(),
                vec![status_row_from_response(&response)],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )
        }
        ServerCommand::Metrics => {
            let response: ServerMetricsResponse = client
                .get_json("api/admin/metrics")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_output(
                server_output::metrics_columns(),
                vec![server_output::metrics_row(
                    response.qps,
                    response.avg_latency_ms,
                    response.p99_latency_ms,
                    response.memory_usage_mb,
                    response.active_connections,
                )],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )
        }
        ServerCommand::Health => {
            let response: ServerHealthResponse = client
                .get_json("api/admin/health")
                .await
                .map_err(map_client_error)?;
            if quiet {
                return Ok(());
            }
            render_output(
                server_output::health_columns(),
                vec![health_row_from_response(&response)],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )
        }
        ServerCommand::Join => {
            let response = execute_cluster_operation(client, "join").await?;
            if quiet {
                return Ok(());
            }
            render_output(
                server_output::cluster_operation_columns(),
                vec![cluster_operation_row(&response)],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )
        }
        ServerCommand::Leave => {
            let response = execute_cluster_operation(client, "leave").await?;
            if quiet {
                return Ok(());
            }
            render_output(
                server_output::cluster_operation_columns(),
                vec![cluster_operation_row(&response)],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )
        }
        ServerCommand::Compaction { command } => match command {
            CompactionCommand::Trigger => {
                let request = serde_json::json!({});
                let response: ServerCompactionResponse = client
                    .post_json("api/admin/compaction", &request)
                    .await
                    .map_err(map_client_error)?;
                if quiet {
                    return Ok(());
                }
                render_output(
                    server_output::compaction_columns(),
                    vec![server_output::compaction_row(
                        response.success,
                        response.message.as_deref(),
                    )],
                    connection_label,
                    Some(server_command_context(cmd)),
                    true,
                    None,
                    output_format,
                    admin_launcher,
                )
            }
        },
        ServerCommand::Cluster { command } => {
            let response = execute_cluster_management_command(client, command).await?;
            if quiet {
                return ensure_cluster_management_succeeded(&response);
            }
            render_output(
                server_output::cluster_management_columns(),
                vec![cluster_management_row(&response)],
                connection_label,
                Some(server_command_context(cmd)),
                true,
                None,
                output_format,
                admin_launcher,
            )?;
            ensure_cluster_management_succeeded(&response)
        }
    }
}

fn server_command_context(cmd: &ServerCommand) -> String {
    match cmd {
        ServerCommand::Status => "server status".to_string(),
        ServerCommand::Metrics => "server metrics".to_string(),
        ServerCommand::Health => "server health".to_string(),
        ServerCommand::Join => "server join".to_string(),
        ServerCommand::Leave => "server leave".to_string(),
        ServerCommand::Compaction { .. } => "server compaction trigger".to_string(),
        ServerCommand::Cluster { .. } => "server cluster management".to_string(),
    }
}

fn render_table<W: Write>(writer: &mut W, columns: Vec<Column>, rows: Vec<Row>) -> Result<()> {
    let mut formatter = TableFormatter::new();
    formatter.write_header(writer, &columns)?;
    for row in rows {
        formatter.write_row(writer, &row)?;
    }
    formatter.write_footer(writer)
}

fn status_row_from_response(response: &ServerStatusResponse) -> Row {
    let cluster = ClusterDisplayFields::from(response.cluster.as_ref());
    server_output::status_row(server_output::StatusRowFields {
        version: response.version.as_deref(),
        uptime_secs: response.uptime_secs,
        connections: response.connections,
        qps: response.queries_per_second,
        cluster_schema_version: cluster.schema_version,
        cluster_mode: cluster.mode,
        node_id: cluster.node_id,
        lifecycle_state: cluster.lifecycle_state,
        degraded: cluster.degraded,
        local_only: cluster.local_only,
        future_distributed: cluster.future_distributed,
        scatter_gather: cluster.scatter_gather,
        diagnostics: cluster.diagnostics.as_deref(),
    })
}

fn health_row_from_response(response: &ServerHealthResponse) -> Row {
    let cluster = ClusterDisplayFields::from(response.cluster.as_ref());
    server_output::health_row(
        response.status.as_deref(),
        response.message.as_deref(),
        response.degraded.or(cluster.degraded),
        cluster.mode,
        cluster.node_id,
    )
}

async fn execute_cluster_operation(
    client: &HttpClient,
    action: &str,
) -> Result<ServerClusterOperationResponse> {
    let request = serde_json::json!({});
    let path = format!("api/admin/cluster/{action}");
    client
        .post_json(&path, &request)
        .await
        .map_err(map_client_error)
}

fn render_cluster_operation<W: Write>(
    writer: &mut W,
    response: &ServerClusterOperationResponse,
) -> Result<()> {
    render_table(
        writer,
        server_output::cluster_operation_columns(),
        vec![cluster_operation_row(response)],
    )
}

async fn execute_cluster_management_command(
    client: &HttpClient,
    command: &ClusterCommand,
) -> Result<ClusterManagementResponse> {
    let request = cluster_management_request(command)?;
    invoke_cluster_management(client, &request)
        .await
        .map_err(map_client_error)
}

struct ClusterInvocation<'a> {
    operation: ClusterManagementOperation,
    request: &'a ClusterOperationRequest,
    target: Option<&'a str>,
    confirmed: bool,
}

impl<'a> ClusterInvocation<'a> {
    fn read(operation: ClusterManagementOperation, request: &'a ClusterOperationRequest) -> Self {
        Self {
            operation,
            request,
            target: None,
            confirmed: false,
        }
    }

    fn targeted_read(
        operation: ClusterManagementOperation,
        request: &'a ClusterTargetedReadRequest,
    ) -> Self {
        Self {
            operation,
            request: &request.operation,
            target: Some(&request.target),
            confirmed: false,
        }
    }

    fn mutation(
        operation: ClusterManagementOperation,
        request: &'a ClusterMutationRequest,
    ) -> Self {
        Self {
            operation,
            request: &request.operation,
            target: Some(&request.target),
            confirmed: request.confirm,
        }
    }
}

fn cluster_management_request(command: &ClusterCommand) -> Result<ClusterManagementRequest> {
    let invocation = match command {
        ClusterCommand::Metadata {
            command: ClusterMetadataCommand::Show { request },
        } => ClusterInvocation::read(ClusterManagementOperation::MetadataShow, request),
        ClusterCommand::Members {
            command: ClusterMembersCommand::List { request },
        } => ClusterInvocation::read(ClusterManagementOperation::MembersList, request),
        ClusterCommand::Members {
            command: ClusterMembersCommand::Replace { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::MembersReplace, request),
        ClusterCommand::Ranges {
            command: ClusterRangesCommand::List { request },
        } => ClusterInvocation::read(ClusterManagementOperation::RangesList, request),
        ClusterCommand::Ranges {
            command: ClusterRangesCommand::Show { request },
        } => ClusterInvocation::targeted_read(ClusterManagementOperation::RangesList, request),
        ClusterCommand::Ranges {
            command: ClusterRangesCommand::Register { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::RangesRegister, request),
        ClusterCommand::Ranges {
            command: ClusterRangesCommand::Update { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::RangesUpdate, request),
        ClusterCommand::Ranges {
            command: ClusterRangesCommand::Retire { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::RangesRetire, request),
        ClusterCommand::Placement {
            command: ClusterPlacementCommand::Get { request },
        } => ClusterInvocation::targeted_read(ClusterManagementOperation::PlacementGet, request),
        ClusterCommand::Placement {
            command: ClusterPlacementCommand::Set { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::PlacementSet, request),
        ClusterCommand::Placement {
            command: ClusterPlacementCommand::Replace { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::PlacementReplace, request),
        ClusterCommand::ReadPolicy {
            command: ClusterReadPolicyCommand::Get { request },
        } => ClusterInvocation::read(ClusterManagementOperation::ReadPolicyGet, request),
        ClusterCommand::ReadPolicy {
            command: ClusterReadPolicyCommand::Set { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::ReadPolicySet, request),
        ClusterCommand::Schema {
            command:
                ClusterSchemaCommand::Owner {
                    command: ClusterSchemaOwnerCommand::Get { request },
                },
        } => ClusterInvocation::read(ClusterManagementOperation::SchemaOwnerGet, request),
        ClusterCommand::Schema {
            command:
                ClusterSchemaCommand::Owner {
                    command: ClusterSchemaOwnerCommand::Set { request },
                },
        } => ClusterInvocation::mutation(ClusterManagementOperation::SchemaOwnerSet, request),
        ClusterCommand::Schema {
            command:
                ClusterSchemaCommand::Rollout {
                    command: ClusterSchemaRolloutCommand::Start { request },
                },
        } => ClusterInvocation::mutation(ClusterManagementOperation::SchemaRolloutStart, request),
        ClusterCommand::Schema {
            command:
                ClusterSchemaCommand::Rollout {
                    command: ClusterSchemaRolloutCommand::Status { request },
                },
        } => ClusterInvocation::read(ClusterManagementOperation::SchemaRolloutStatus, request),
        ClusterCommand::Recovery {
            command: ClusterRecoveryCommand::Status { request },
        } => ClusterInvocation::read(ClusterManagementOperation::RecoveryStatus, request),
        ClusterCommand::Recovery {
            command: ClusterRecoveryCommand::Restore { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::RecoveryRestore, request),
        ClusterCommand::Upgrade {
            command: ClusterUpgradeCommand::Status { request },
        } => ClusterInvocation::read(ClusterManagementOperation::UpgradeStatus, request),
        ClusterCommand::Upgrade {
            command: ClusterUpgradeCommand::Start { request },
        } => ClusterInvocation::mutation(ClusterManagementOperation::UpgradeStart, request),
    };

    let target = invocation
        .target
        .map(|target| {
            serde_json::from_str(target).map_err(|err| {
                CliError::InvalidArgument(format!(
                    "cluster management target must be valid JSON: {err}"
                ))
            })
        })
        .transpose()?;
    Ok(ClusterManagementRequest::new(
        &invocation.request.request_id,
        invocation.operation,
        invocation.request.expected_version,
        target,
        invocation.confirmed,
    ))
}

fn render_cluster_management<W: Write>(
    writer: &mut W,
    response: &ClusterManagementResponse,
    output_format: OutputFormat,
) -> Result<()> {
    let mut formatter = crate::output::create_formatter(output_format);
    let columns = server_output::cluster_management_columns();
    let row = cluster_management_row(response);
    formatter.write_header(writer, &columns)?;
    formatter.write_row(writer, &row)?;
    formatter.write_footer(writer)
}

fn cluster_management_row(response: &ClusterManagementResponse) -> Row {
    let prerequisites = response
        .control
        .missing_prerequisites
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join(",");
    server_output::cluster_management_row(server_output::ClusterManagementRowFields {
        operation_id: &response.operation_id,
        operation: &response.operation,
        outcome_class: &response.outcome_class,
        reason: &response.reason,
        state_version: response.state_version,
        control_available: response.control.available,
        control_mode: &response.control.mode,
        control_reason: &response.control.reason,
        missing_prerequisites: (!prerequisites.is_empty()).then_some(&prerequisites),
        actor: response.actor.as_deref(),
    })
}

fn ensure_cluster_management_succeeded(response: &ClusterManagementResponse) -> Result<()> {
    let outcome = ClusterManagementOutcome::from_wire(&response.outcome_class, &response.reason);
    if outcome.is_success() {
        return Ok(());
    }
    Err(CliError::ClusterManagementOutcome {
        outcome: response.outcome_class.clone(),
        reason: response.reason.clone(),
        exit_code: outcome.exit_code(),
    })
}

fn cluster_operation_row(response: &ServerClusterOperationResponse) -> Row {
    let cluster = response.cluster.as_ref();
    let identity = cluster.and_then(|cluster| cluster.identity.as_ref());
    server_output::cluster_operation_row(
        response.action.as_deref(),
        cluster.and_then(|cluster| cluster.mode.as_deref()),
        identity.and_then(|identity| identity.node_id.as_deref()),
        identity.and_then(|identity| identity.lifecycle_state.as_deref()),
        cluster.and_then(|cluster| cluster.degraded),
    )
}

struct ClusterDisplayFields<'a> {
    mode: Option<&'a str>,
    schema_version: Option<u32>,
    node_id: Option<&'a str>,
    lifecycle_state: Option<&'a str>,
    degraded: Option<bool>,
    local_only: Option<bool>,
    future_distributed: Option<bool>,
    scatter_gather: Option<bool>,
    diagnostics: Option<String>,
}

impl<'a> From<Option<&'a ServerClusterStatus>> for ClusterDisplayFields<'a> {
    fn from(cluster: Option<&'a ServerClusterStatus>) -> Self {
        let identity = cluster.and_then(|cluster| cluster.identity.as_ref());
        let routing = cluster.and_then(|cluster| cluster.routing_capabilities.as_ref());
        Self {
            mode: cluster.and_then(|cluster| cluster.mode.as_deref()),
            schema_version: cluster.and_then(|cluster| cluster.schema_version),
            node_id: identity.and_then(|identity| identity.node_id.as_deref()),
            lifecycle_state: identity.and_then(|identity| identity.lifecycle_state.as_deref()),
            degraded: cluster.and_then(|cluster| cluster.degraded),
            local_only: routing.and_then(|routing| routing.local_only),
            future_distributed: routing
                .and_then(|routing| routing.future_distributed_execution_required),
            scatter_gather: routing.and_then(|routing| routing.scatter_gather_simulated),
            diagnostics: cluster.and_then(diagnostic_codes),
        }
    }
}

fn diagnostic_codes(cluster: &ServerClusterStatus) -> Option<String> {
    let diagnostics = cluster.diagnostics.as_ref()?;
    let codes = diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic.code.as_deref())
        .collect::<Vec<_>>();
    if codes.is_empty() {
        None
    } else {
        Some(codes.join(","))
    }
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
            if status == reqwest::StatusCode::NOT_IMPLEMENTED {
                CliError::ServerUnsupported(format!(
                    "server returned HTTP {}: {}",
                    status.as_u16(),
                    body
                ))
            } else {
                CliError::InvalidArgument(format!(
                    "Server error: HTTP {} - {}",
                    status.as_u16(),
                    body
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{
        cluster_management_request, ensure_cluster_management_succeeded, render_cluster_management,
        ClusterManagementOperation, ClusterManagementResponse,
    };
    use crate::batch::ExitCode;
    use crate::cli::{Cli, ClusterCommand, Command, OutputFormat, ServerCommand};
    use crate::client::admin_resources::ClusterControlAvailability;
    use crate::error::CliError;

    fn cluster_command(args: Vec<&str>) -> ClusterCommand {
        let cli = Cli::try_parse_from(args).unwrap();
        match cli.command {
            Some(Command::Server {
                command: Some(ServerCommand::Cluster { command }),
            }) => command,
            other => panic!("expected server cluster command, got {other:?}"),
        }
    }

    #[test]
    fn every_public_cluster_grammar_maps_to_its_management_operation() {
        let cases = vec![
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "metadata",
                    "show",
                    "--request-id",
                    "metadata-show",
                ],
                ClusterManagementOperation::MetadataShow,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "members",
                    "list",
                    "--request-id",
                    "members-list",
                ],
                ClusterManagementOperation::MembersList,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "members",
                    "replace",
                    "--request-id",
                    "members-replace",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::MembersReplace,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "ranges",
                    "list",
                    "--request-id",
                    "ranges-list",
                ],
                ClusterManagementOperation::RangesList,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "ranges",
                    "show",
                    "--request-id",
                    "ranges-show",
                    "--target",
                    "{}",
                ],
                ClusterManagementOperation::RangesList,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "ranges",
                    "register",
                    "--request-id",
                    "ranges-register",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::RangesRegister,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "ranges",
                    "update",
                    "--request-id",
                    "ranges-update",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::RangesUpdate,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "ranges",
                    "retire",
                    "--request-id",
                    "ranges-retire",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::RangesRetire,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "placement",
                    "get",
                    "--request-id",
                    "placement-get",
                    "--target",
                    "{}",
                ],
                ClusterManagementOperation::PlacementGet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "placement",
                    "set",
                    "--request-id",
                    "placement-set",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::PlacementSet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "placement",
                    "replace",
                    "--request-id",
                    "placement-replace",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::PlacementReplace,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "read-policy",
                    "get",
                    "--request-id",
                    "read-policy-get",
                ],
                ClusterManagementOperation::ReadPolicyGet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "read-policy",
                    "set",
                    "--request-id",
                    "read-policy-set",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::ReadPolicySet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "schema",
                    "owner",
                    "get",
                    "--request-id",
                    "schema-owner-get",
                ],
                ClusterManagementOperation::SchemaOwnerGet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "schema",
                    "owner",
                    "set",
                    "--request-id",
                    "schema-owner-set",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::SchemaOwnerSet,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "schema",
                    "rollout",
                    "start",
                    "--request-id",
                    "schema-rollout-start",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::SchemaRolloutStart,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "schema",
                    "rollout",
                    "status",
                    "--request-id",
                    "schema-rollout-status",
                ],
                ClusterManagementOperation::SchemaRolloutStatus,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "recovery",
                    "status",
                    "--request-id",
                    "recovery-status",
                ],
                ClusterManagementOperation::RecoveryStatus,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "recovery",
                    "restore",
                    "--request-id",
                    "recovery-restore",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::RecoveryRestore,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "upgrade",
                    "status",
                    "--request-id",
                    "upgrade-status",
                ],
                ClusterManagementOperation::UpgradeStatus,
            ),
            (
                vec![
                    "alopex",
                    "server",
                    "cluster",
                    "upgrade",
                    "start",
                    "--request-id",
                    "upgrade-start",
                    "--target",
                    "{}",
                    "--confirm",
                ],
                ClusterManagementOperation::UpgradeStart,
            ),
        ];

        for (args, expected_operation) in cases {
            let command = cluster_command(args);
            let request = cluster_management_request(&command).unwrap();
            assert_eq!(request.operation, expected_operation);
        }
    }

    #[test]
    fn mutation_target_and_confirmation_are_preserved_for_the_http_contract() {
        let command = cluster_command(vec![
            "alopex",
            "server",
            "cluster",
            "ranges",
            "register",
            "--request-id",
            "range-register-8",
            "--expected-version",
            "7",
            "--target",
            r#"{"range_id":"primary/0"}"#,
            "--confirm",
        ]);
        let request = cluster_management_request(&command).unwrap();

        assert_eq!(request.request_id, "range-register-8");
        assert_eq!(request.expected_version, Some(7));
        assert_eq!(
            request.target,
            Some(serde_json::json!({"range_id": "primary/0"}))
        );
        assert!(request.confirmed);
    }

    #[test]
    fn invalid_target_json_is_rejected_before_http_invocation() {
        let command = cluster_command(vec![
            "alopex",
            "server",
            "cluster",
            "placement",
            "set",
            "--request-id",
            "placement-set",
            "--target",
            "not-json",
            "--confirm",
        ]);

        assert!(cluster_management_request(&command).is_err());
    }

    fn cluster_response(outcome_class: &str, reason: &str) -> ClusterManagementResponse {
        ClusterManagementResponse {
            operation_id: "operation-17".to_string(),
            operation: "ranges_register".to_string(),
            outcome_class: outcome_class.to_string(),
            reason: reason.to_string(),
            state_version: Some(12),
            control: ClusterControlAvailability {
                available: true,
                mode: "cluster_aware".to_string(),
                reason: "ready".to_string(),
                missing_prerequisites: Vec::new(),
            },
            actor: Some("operator-a".to_string()),
        }
    }

    #[test]
    fn cluster_management_json_output_retains_the_response_classification() {
        let response = cluster_response("pending", "metadata_consensus_adapter_not_attached");
        let mut output = Vec::new();
        render_cluster_management(&mut output, &response, OutputFormat::Json).unwrap();

        let row = &serde_json::from_slice::<serde_json::Value>(&output).unwrap()[0];
        assert_eq!(row["Operation ID"], "operation-17");
        assert_eq!(row["Operation"], "ranges_register");
        assert_eq!(row["Outcome"], "pending");
        assert_eq!(row["Reason"], "metadata_consensus_adapter_not_attached");
        assert_eq!(row["Control Available"], true);
    }

    #[test]
    fn cluster_management_non_success_outcomes_have_distinct_exit_codes() {
        let cases = [
            ("pending", "waiting_for_quorum", ExitCode::Warning),
            ("retryable_failure", "not_leader", ExitCode::Retryable),
            ("terminal_failure", "stale_version", ExitCode::Error),
            (
                "terminal_failure",
                "authorization_denied",
                ExitCode::Authorization,
            ),
        ];

        for (outcome_class, reason, exit_code) in cases {
            let response = cluster_response(outcome_class, reason);
            let err = ensure_cluster_management_succeeded(&response).unwrap_err();
            assert!(matches!(
                err,
                CliError::ClusterManagementOutcome { exit_code: actual, .. } if actual == exit_code
            ));
        }
        assert!(
            ensure_cluster_management_succeeded(&cluster_response("succeeded", "committed"))
                .is_ok()
        );
    }
}
