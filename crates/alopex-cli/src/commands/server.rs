//! Server management commands.

use std::io::Write;

use serde::Deserialize;

use crate::cli::{CompactionCommand, OutputFormat, ServerCommand};
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
pub async fn execute_remote<W: Write>(
    client: &HttpClient,
    cmd: &ServerCommand,
    writer: &mut W,
    quiet: bool,
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
