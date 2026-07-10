//! Unified TUI renderer for CLI command output.

use crate::cli::OutputFormat;
use crate::error::Result;
use crate::models::{Column, Row, Value};
use crate::output::formatter::create_formatter;

use super::TuiApp;

#[allow(clippy::too_many_arguments)]
pub fn render_output<'a>(
    columns: Vec<Column>,
    rows: Vec<Row>,
    connection_label: impl Into<String>,
    context_message: Option<String>,
    processing: bool,
    status_message: Option<String>,
    output_format: OutputFormat,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
) -> Result<()> {
    let fallback_columns = columns.clone();
    let fallback_rows = rows.clone();
    let output_status = status_message_for(&columns, &rows);
    let status_message = output_status.clone().or(status_message);
    let rows = if output_status.is_some() {
        Vec::new()
    } else {
        rows
    };
    let mut app = TuiApp::new(columns, rows, connection_label, processing)
        .with_context_message(context_message)
        .with_admin_launcher(admin_launcher);
    if let Some(message) = status_message {
        app = app.with_status_message(message);
    }
    if app.run().is_err() {
        let mut formatter = create_formatter(output_format);
        let mut writer = std::io::stdout().lock();
        formatter.write_header(&mut writer, &fallback_columns)?;
        for row in &fallback_rows {
            formatter.write_row(&mut writer, row)?;
        }
        formatter.write_footer(&mut writer)?;
        return Ok(());
    }
    Ok(())
}

fn status_message_for(columns: &[Column], rows: &[Row]) -> Option<String> {
    let row = rows.first()?;
    if rows.len() != 1 || columns.len() != 2 {
        return None;
    }
    if !column_name_eq(&columns[0].name, "status") || !column_name_eq(&columns[1].name, "message") {
        return None;
    }
    let status = row.columns.first().map(value_to_string)?;
    let message = row.columns.get(1).map(value_to_string)?;
    Some(format!("{status}: {message}"))
}

fn column_name_eq(name: &str, expected: &str) -> bool {
    name.eq_ignore_ascii_case(expected)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:.6}"),
        Value::Text(text) => text.clone(),
        Value::Bytes(bytes) => {
            let hex: String = bytes
                .iter()
                .take(32)
                .map(|byte| format!("{byte:02x}"))
                .collect();
            if bytes.len() > 32 {
                format!("{hex}...")
            } else {
                hex
            }
        }
        Value::Vector(values) => {
            if values.len() <= 4 {
                format!(
                    "[{}]",
                    values
                        .iter()
                        .map(|value| format!("{value:.4}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
                    "[{}, ... ({} dims)]",
                    values
                        .iter()
                        .take(3)
                        .map(|value| format!("{value:.4}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.len()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DataType;

    #[test]
    fn detects_status_message() {
        let columns = vec![
            Column::new("status", DataType::Text),
            Column::new("message", DataType::Text),
        ];
        let rows = vec![Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text("Updated key".to_string()),
        ])];

        let message = status_message_for(&columns, &rows);
        assert_eq!(message, Some("OK: Updated key".to_string()));
    }

    #[test]
    fn ignores_non_status_output() {
        let columns = vec![Column::new("id", DataType::Int)];
        let rows = vec![Row::new(vec![Value::Int(1)])];

        let message = status_message_for(&columns, &rows);
        assert_eq!(message, None);
    }

    #[test]
    fn keeps_rich_health_rows_visible() {
        let columns = vec![
            Column::new("status", DataType::Text),
            Column::new("message", DataType::Text),
            Column::new("degraded", DataType::Bool),
        ];
        let rows = vec![Row::new(vec![
            Value::Text("degraded".to_string()),
            Value::Text("cluster status degraded".to_string()),
            Value::Bool(true),
        ])];

        let message = status_message_for(&columns, &rows);
        assert_eq!(message, None);
    }
}
