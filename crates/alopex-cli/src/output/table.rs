//! Table formatter using comfy-table
//!
//! Human-readable table output with auto-adjusting column widths.

use std::io::Write;

use comfy_table::{presets::UTF8_FULL, ContentArrangement, Table};

use crate::error::Result;
use crate::models::{Column, Row, Value};

use super::formatter::Formatter;

/// Table formatter using comfy-table.
///
/// Outputs data as a human-readable table with auto-adjusting column widths.
/// Does not support streaming (requires buffering to determine column widths).
pub struct TableFormatter {
    /// The table being built
    table: Table,
    /// Whether the header has been set
    header_set: bool,
}

impl TableFormatter {
    /// Create a new table formatter.
    pub fn new() -> Self {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        Self {
            table,
            header_set: false,
        }
    }
}

impl Default for TableFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for TableFormatter {
    fn write_header(&mut self, _writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        // Set the header row
        let headers: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        self.table.set_header(headers);
        self.header_set = true;
        Ok(())
    }

    fn write_row(&mut self, _writer: &mut dyn Write, row: &Row) -> Result<()> {
        // Add a row to the table
        let cells: Vec<String> = row.columns.iter().map(format_value_table).collect();
        self.table.add_row(cells);
        Ok(())
    }

    fn write_footer(&mut self, writer: &mut dyn Write) -> Result<()> {
        // Write the complete table to the output
        if self.header_set {
            writeln!(writer, "{}", self.table)?;
        }
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        false
    }
}

/// Format a value for table output.
fn format_value_table(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.6}", f),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => {
            // Format bytes as hex string (truncated if too long)
            let hex: String = b
                .iter()
                .take(32)
                .map(|byte| format!("{:02x}", byte))
                .collect();
            if b.len() > 32 {
                format!("{}...", hex)
            } else {
                hex
            }
        }
        Value::Vector(v) => {
            // Format vector (truncated if too long)
            if v.len() <= 4 {
                format!(
                    "[{}]",
                    v.iter()
                        .map(|x| format!("{:.4}", x))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
                    "[{}, ... ({} dims)]",
                    v.iter()
                        .take(3)
                        .map(|x| format!("{:.4}", x))
                        .collect::<Vec<_>>()
                        .join(", "),
                    v.len()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DataType;

    fn test_columns() -> Vec<Column> {
        vec![
            Column::new("id", DataType::Int),
            Column::new("name", DataType::Text),
        ]
    }

    #[test]
    fn test_table_basic() {
        let mut formatter = TableFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("1"));
        assert!(result.contains("Alice"));
    }

    #[test]
    fn test_table_multiple_rows() {
        let mut formatter = TableFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row1 = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        let row2 = Row::new(vec![Value::Int(2), Value::Text("Bob".to_string())]);

        formatter.write_row(&mut output, &row1).unwrap();
        formatter.write_row(&mut output, &row2).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
    }

    #[test]
    fn test_table_null_value() {
        let mut formatter = TableFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Null]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("NULL"));
    }

    #[test]
    fn test_table_does_not_support_streaming() {
        let formatter = TableFormatter::new();
        assert!(!formatter.supports_streaming());
    }

    #[test]
    fn test_format_value_vector_short() {
        let v = Value::Vector(vec![1.0, 2.0, 3.0]);
        let result = format_value_table(&v);
        assert!(result.contains("["));
        assert!(result.contains("]"));
    }

    #[test]
    fn test_format_value_vector_long() {
        let v = Value::Vector(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let result = format_value_table(&v);
        assert!(result.contains("..."));
        assert!(result.contains("6 dims"));
    }
}
