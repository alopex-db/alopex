//! CSV formatter (RFC 4180)
//!
//! Outputs data as CSV with proper escaping.
//! Supports streaming output.

use std::io::Write;

use crate::error::Result;
use crate::models::{Column, Row, Value};

use super::formatter::Formatter;

/// CSV formatter (RFC 4180 compliant).
///
/// Outputs data as comma-separated values with proper quoting and escaping.
pub struct CsvFormatter {
    /// Whether a row has been written (for potential future use)
    _row_count: usize,
}

impl CsvFormatter {
    /// Create a new CSV formatter.
    pub fn new() -> Self {
        Self { _row_count: 0 }
    }
}

impl Default for CsvFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for CsvFormatter {
    fn write_header(&mut self, writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        let header: Vec<String> = columns.iter().map(|c| escape_csv(&c.name)).collect();
        writeln!(writer, "{}", header.join(","))?;
        Ok(())
    }

    fn write_row(&mut self, writer: &mut dyn Write, row: &Row) -> Result<()> {
        let values: Vec<String> = row.columns.iter().map(format_value_csv).collect();
        writeln!(writer, "{}", values.join(","))?;
        self._row_count += 1;
        Ok(())
    }

    fn write_footer(&mut self, _writer: &mut dyn Write) -> Result<()> {
        // CSV has no footer
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

/// Format a value for CSV output.
fn format_value_csv(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => escape_csv(s),
        Value::Bytes(b) => {
            // Format bytes as hex string
            escape_csv(
                &b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>(),
            )
        }
        Value::Vector(v) => {
            // Format vector as JSON array string
            let json = serde_json::to_string(v).unwrap_or_default();
            escape_csv(&json)
        }
    }
}

/// Escape a string for CSV output (RFC 4180).
///
/// Quotes the string if it contains special characters.
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        // Quote and escape internal quotes
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
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
    fn test_csv_basic() {
        let mut formatter = CsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, "id,name\n1,Alice\n");
    }

    #[test]
    fn test_csv_escape_comma() {
        let mut formatter = CsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice, Bob".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"Alice, Bob\""));
    }

    #[test]
    fn test_csv_escape_quote() {
        let mut formatter = CsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![
            Value::Int(1),
            Value::Text("Alice \"The Great\"".to_string()),
        ]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"Alice \"\"The Great\"\"\""));
    }

    #[test]
    fn test_csv_escape_newline() {
        let mut formatter = CsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Line1\nLine2".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"Line1\nLine2\""));
    }

    #[test]
    fn test_csv_null_value() {
        let mut formatter = CsvFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Null]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, "id,name\n1,\n");
    }

    #[test]
    fn test_csv_supports_streaming() {
        let formatter = CsvFormatter::new();
        assert!(formatter.supports_streaming());
    }
}
