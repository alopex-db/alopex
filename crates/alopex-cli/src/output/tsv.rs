//! TSV formatter
//!
//! Outputs data as tab-separated values.
//! Supports streaming output.

use std::io::Write;

use crate::error::Result;
use crate::models::{Column, Row, Value};

use super::formatter::Formatter;

/// TSV formatter.
///
/// Outputs data as tab-separated values. Tabs and newlines in values
/// are escaped as \t and \n.
pub struct TsvFormatter {
    /// Whether a row has been written (for potential future use)
    _row_count: usize,
}

impl TsvFormatter {
    /// Create a new TSV formatter.
    pub fn new() -> Self {
        Self { _row_count: 0 }
    }
}

impl Default for TsvFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for TsvFormatter {
    fn write_header(&mut self, writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        let header: Vec<String> = columns.iter().map(|c| escape_tsv(&c.name)).collect();
        writeln!(writer, "{}", header.join("\t"))?;
        Ok(())
    }

    fn write_row(&mut self, writer: &mut dyn Write, row: &Row) -> Result<()> {
        let values: Vec<String> = row.columns.iter().map(format_value_tsv).collect();
        writeln!(writer, "{}", values.join("\t"))?;
        self._row_count += 1;
        Ok(())
    }

    fn write_footer(&mut self, _writer: &mut dyn Write) -> Result<()> {
        // TSV has no footer
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

/// Format a value for TSV output.
fn format_value_tsv(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => escape_tsv(s),
        Value::Bytes(b) => {
            // Format bytes as hex string
            escape_tsv(
                &b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>(),
            )
        }
        Value::Vector(v) => {
            // Format vector as JSON array string
            let json = serde_json::to_string(v).unwrap_or_default();
            escape_tsv(&json)
        }
    }
}

/// Escape a string for TSV output.
///
/// Escapes tabs as \t and newlines as \n.
fn escape_tsv(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
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
    fn test_tsv_basic() {
        let mut formatter = TsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, "id\tname\n1\tAlice\n");
    }

    #[test]
    fn test_tsv_escape_tab() {
        let mut formatter = TsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice\tBob".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Alice\\tBob"));
    }

    #[test]
    fn test_tsv_escape_newline() {
        let mut formatter = TsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Line1\nLine2".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Line1\\nLine2"));
    }

    #[test]
    fn test_tsv_escape_backslash() {
        let mut formatter = TsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![
            Value::Int(1),
            Value::Text("path\\to\\file".to_string()),
        ]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("path\\\\to\\\\file"));
    }

    #[test]
    fn test_tsv_null_value() {
        let mut formatter = TsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Null]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, "id\tname\n1\t\n");
    }

    #[test]
    fn test_tsv_supports_streaming() {
        let formatter = TsvFormatter::new();
        assert!(formatter.supports_streaming());
    }
}
