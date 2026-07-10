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
