//! Server management commands.

use std::io::Write;

use serde::Deserialize;

use crate::cli::{CompactionCommand, ServerCommand};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, Row};
use crate::output::server as server_output;
use crate::output::table::TableFormatter;
use crate::output::Formatter;

#[derive(Debug, Deserialize)]
struct ServerStatusResponse {
    version: Option<String>,
    uptime_secs: Option<u64>,
    connections: Option<u64>,
    queries_per_second: Option<f64>,
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
}

#[derive(Debug, Deserialize)]
struct ServerCompactionResponse {
    success: Option<bool>,
    message: Option<String>,
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
                vec![server_output::status_row(
                    response.version.as_deref(),
                    response.uptime_secs,
                    response.connections,
                    response.queries_per_second,
                )],
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
                vec![server_output::health_row(
                    response.status.as_deref(),
                    response.message.as_deref(),
                )],
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

fn render_table<W: Write>(writer: &mut W, columns: Vec<Column>, rows: Vec<Row>) -> Result<()> {
    let mut formatter = TableFormatter::new();
    formatter.write_header(writer, &columns)?;
    for row in rows {
        formatter.write_row(writer, &row)?;
    }
    formatter.write_footer(writer)
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
            CliError::InvalidArgument(format!("Server error: HTTP {} - {}", status.as_u16(), body))
        }
    }
}
