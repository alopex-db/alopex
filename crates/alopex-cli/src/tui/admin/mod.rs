//! Admin TUI entry point.

use crate::error::Result;
use crate::models::{Column, DataType, Row, Value};

use super::renderer::render_output;

pub fn run_admin_ui(connection_label: impl Into<String>) -> Result<()> {
    let columns = vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text("Info".to_string()),
        Value::Text("Admin UI is not implemented yet.".to_string()),
    ])];
    render_output(columns, rows, connection_label, true, None)
}
