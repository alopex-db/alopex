//! Server management output helpers.

use crate::models::{Column, DataType, Row, Value};

pub fn status_columns() -> Vec<Column> {
    vec![
        Column::new("Version", DataType::Text),
        Column::new("Uptime (s)", DataType::Text),
        Column::new("Connections", DataType::Text),
        Column::new("QPS", DataType::Text),
        Column::new("Cluster Schema", DataType::Text),
        Column::new("Cluster Mode", DataType::Text),
        Column::new("Node ID", DataType::Text),
        Column::new("Lifecycle", DataType::Text),
        Column::new("Degraded", DataType::Bool),
        Column::new("Local Only", DataType::Bool),
        Column::new("Future Distributed", DataType::Bool),
        Column::new("Scatter/Gather", DataType::Bool),
        Column::new("Diagnostics", DataType::Text),
    ]
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StatusRowFields<'a> {
    pub version: Option<&'a str>,
    pub uptime_secs: Option<u64>,
    pub connections: Option<u64>,
    pub qps: Option<f64>,
    pub cluster_schema_version: Option<u32>,
    pub cluster_mode: Option<&'a str>,
    pub node_id: Option<&'a str>,
    pub lifecycle_state: Option<&'a str>,
    pub degraded: Option<bool>,
    pub local_only: Option<bool>,
    pub future_distributed: Option<bool>,
    pub scatter_gather: Option<bool>,
    pub diagnostics: Option<&'a str>,
}

pub fn status_row(fields: StatusRowFields<'_>) -> Row {
    Row::new(vec![
        Value::Text(opt_text(fields.version)),
        Value::Text(opt_u64(fields.uptime_secs)),
        Value::Text(opt_u64(fields.connections)),
        Value::Text(opt_f64(fields.qps)),
        Value::Text(opt_u32(fields.cluster_schema_version)),
        Value::Text(opt_text(fields.cluster_mode)),
        Value::Text(opt_text(fields.node_id)),
        Value::Text(opt_text(fields.lifecycle_state)),
        opt_bool_value(fields.degraded),
        opt_bool_value(fields.local_only),
        opt_bool_value(fields.future_distributed),
        opt_bool_value(fields.scatter_gather),
        Value::Text(opt_text(fields.diagnostics)),
    ])
}

pub fn metrics_columns() -> Vec<Column> {
    vec![
        Column::new("QPS", DataType::Text),
        Column::new("Avg Latency (ms)", DataType::Text),
        Column::new("P99 Latency (ms)", DataType::Text),
        Column::new("Memory (MB)", DataType::Text),
        Column::new("Active Connections", DataType::Text),
    ]
}

pub fn metrics_row(
    qps: Option<f64>,
    avg_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    memory_usage_mb: Option<u64>,
    active_connections: Option<u64>,
) -> Row {
    Row::new(vec![
        Value::Text(opt_f64(qps)),
        Value::Text(opt_f64(avg_latency_ms)),
        Value::Text(opt_f64(p99_latency_ms)),
        Value::Text(opt_u64(memory_usage_mb)),
        Value::Text(opt_u64(active_connections)),
    ])
}

pub fn health_columns() -> Vec<Column> {
    vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
        Column::new("Degraded", DataType::Bool),
        Column::new("Cluster Mode", DataType::Text),
        Column::new("Node ID", DataType::Text),
    ]
}

pub fn health_row(
    status: Option<&str>,
    message: Option<&str>,
    degraded: Option<bool>,
    cluster_mode: Option<&str>,
    node_id: Option<&str>,
) -> Row {
    Row::new(vec![
        Value::Text(opt_text(status)),
        Value::Text(opt_text(message)),
        opt_bool_value(degraded),
        Value::Text(opt_text(cluster_mode)),
        Value::Text(opt_text(node_id)),
    ])
}

pub fn compaction_columns() -> Vec<Column> {
    vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ]
}

pub fn compaction_row(success: Option<bool>, message: Option<&str>) -> Row {
    let result = match success {
        Some(true) => "OK",
        Some(false) => "Failed",
        None => "N/A",
    };
    Row::new(vec![
        Value::Text(result.to_string()),
        Value::Text(opt_text(message)),
    ])
}

pub fn cluster_operation_columns() -> Vec<Column> {
    vec![
        Column::new("Action", DataType::Text),
        Column::new("Mode", DataType::Text),
        Column::new("Node", DataType::Text),
        Column::new("Lifecycle", DataType::Text),
        Column::new("Degraded", DataType::Text),
    ]
}

pub fn cluster_operation_row(
    action: Option<&str>,
    mode: Option<&str>,
    node_id: Option<&str>,
    lifecycle_state: Option<&str>,
    degraded: Option<bool>,
) -> Row {
    Row::new(vec![
        Value::Text(opt_text(action)),
        Value::Text(opt_text(mode)),
        Value::Text(opt_text(node_id)),
        Value::Text(opt_text(lifecycle_state)),
        Value::Text(opt_bool(degraded)),
    ])
}

/// Fields emitted for every `server cluster` operation.  These names are kept
/// stable for table and JSON output so operators and automation observe the
/// same operation ID, outcome and capability prerequisite.
#[derive(Debug, Clone, Copy)]
pub struct ClusterManagementRowFields<'a> {
    pub operation_id: &'a str,
    pub operation: &'a str,
    pub outcome_class: &'a str,
    pub reason: &'a str,
    pub state_version: Option<u64>,
    pub control_available: bool,
    pub control_mode: &'a str,
    pub control_reason: &'a str,
    pub missing_prerequisites: Option<&'a str>,
    pub actor: Option<&'a str>,
}

pub fn cluster_management_columns() -> Vec<Column> {
    vec![
        Column::new("Operation ID", DataType::Text),
        Column::new("Operation", DataType::Text),
        Column::new("Outcome", DataType::Text),
        Column::new("Reason", DataType::Text),
        Column::new("State Version", DataType::Text),
        Column::new("Control Available", DataType::Bool),
        Column::new("Control Mode", DataType::Text),
        Column::new("Control Reason", DataType::Text),
        Column::new("Missing Prerequisites", DataType::Text),
        Column::new("Actor", DataType::Text),
    ]
}

pub fn cluster_management_row(fields: ClusterManagementRowFields<'_>) -> Row {
    Row::new(vec![
        Value::Text(fields.operation_id.to_string()),
        Value::Text(fields.operation.to_string()),
        Value::Text(fields.outcome_class.to_string()),
        Value::Text(fields.reason.to_string()),
        Value::Text(opt_u64(fields.state_version)),
        Value::Bool(fields.control_available),
        Value::Text(fields.control_mode.to_string()),
        Value::Text(fields.control_reason.to_string()),
        Value::Text(opt_text(fields.missing_prerequisites)),
        Value::Text(opt_text(fields.actor)),
    ])
}

fn opt_text(value: Option<&str>) -> String {
    value.unwrap_or("N/A").to_string()
}

fn opt_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}

fn opt_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}

fn opt_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}", value))
        .unwrap_or_else(|| "N/A".to_string())
}

fn opt_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}

fn opt_bool_value(value: Option<bool>) -> Value {
    value.map(Value::Bool).unwrap_or(Value::Null)
}

#[cfg(test)]
mod cluster_management_tests {
    use super::*;

    #[test]
    fn cluster_management_row_contains_machine_contract_fields() {
        let columns = cluster_management_columns();
        let row = cluster_management_row(ClusterManagementRowFields {
            operation_id: "operation-7",
            operation: "ranges_register",
            outcome_class: "pending",
            reason: "metadata_consensus_adapter_not_attached",
            state_version: None,
            control_available: true,
            control_mode: "cluster_aware",
            control_reason: "ready",
            missing_prerequisites: None,
            actor: Some("operator-a"),
        });

        assert_eq!(columns.len(), row.columns.len());
        assert_eq!(row.columns[0], Value::Text("operation-7".to_string()));
        assert_eq!(row.columns[2], Value::Text("pending".to_string()));
        assert_eq!(row.columns[5], Value::Bool(true));
    }
}
