//! Server management output helpers.

use crate::models::{Column, DataType, Row, Value};

pub fn status_columns() -> Vec<Column> {
    vec![
        Column::new("Version", DataType::Text),
        Column::new("Uptime (s)", DataType::Text),
        Column::new("Connections", DataType::Text),
        Column::new("QPS", DataType::Text),
    ]
}

pub fn status_row(
    version: Option<&str>,
    uptime_secs: Option<u64>,
    connections: Option<u64>,
    qps: Option<f64>,
) -> Row {
    Row::new(vec![
        Value::Text(opt_text(version)),
        Value::Text(opt_u64(uptime_secs)),
        Value::Text(opt_u64(connections)),
        Value::Text(opt_f64(qps)),
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
    ]
}

pub fn health_row(status: Option<&str>, message: Option<&str>) -> Row {
    Row::new(vec![
        Value::Text(opt_text(status)),
        Value::Text(opt_text(message)),
    ])
}

pub fn compaction_columns() -> Vec<Column> {
    vec![
        Column::new("Result", DataType::Text),
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

fn opt_text(value: Option<&str>) -> String {
    value.unwrap_or("N/A").to_string()
}

fn opt_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}

fn opt_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}", value))
        .unwrap_or_else(|| "N/A".to_string())
}
