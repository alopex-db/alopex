//! JSON Lines formatter
//!
//! Outputs data as JSON Lines (one JSON object per line).
//! Supports streaming output.

use std::io::Write;

use crate::error::Result;
use crate::models::{Column, Row, Value};

use super::formatter::Formatter;

/// JSON Lines formatter.
///
/// Outputs each row as a single-line JSON object.
/// Format: `{"col1": val1, "col2": val2, ...}\n`
pub struct JsonlFormatter {
    /// Column names (set on write_header)
    columns: Vec<String>,
}

impl JsonlFormatter {
    /// Create a new JSON Lines formatter.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }
}

impl Default for JsonlFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for JsonlFormatter {
    fn write_header(&mut self, _writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        // JSON Lines doesn't have a header, just store column names
        self.columns = columns.iter().map(|c| c.name.clone()).collect();
        Ok(())
    }

    fn write_row(&mut self, writer: &mut dyn Write, row: &Row) -> Result<()> {
        // Build a JSON object from column names and values
        let mut obj = serde_json::Map::new();

        for (i, value) in row.columns.iter().enumerate() {
            let key = self
                .columns
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col{}", i));
            let json_value = value_to_json(value);
            obj.insert(key, json_value);
        }

        let json = serde_json::Value::Object(obj);
        writeln!(writer, "{}", json)?;
        Ok(())
    }

    fn write_footer(&mut self, _writer: &mut dyn Write) -> Result<()> {
        // JSON Lines has no footer
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

/// Convert a Value to a serde_json::Value.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            // Encode bytes as array of numbers
            serde_json::Value::Array(
                b.iter()
                    .map(|&byte| serde_json::Value::Number(byte.into()))
                    .collect(),
            )
        }
        Value::Vector(v) => serde_json::Value::Array(
            v.iter()
                .filter_map(|&f| serde_json::Number::from_f64(f as f64))
                .map(serde_json::Value::Number)
                .collect(),
        ),
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
    fn test_jsonl_basic() {
        let mut formatter = JsonlFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert_eq!(result, r#"{"id":1,"name":"Alice"}"#.to_string() + "\n");
    }

    #[test]
    fn test_jsonl_multiple_rows() {
        let mut formatter = JsonlFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row1 = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        let row2 = Row::new(vec![Value::Int(2), Value::Text("Bob".to_string())]);

        formatter.write_row(&mut output, &row1).unwrap();
        formatter.write_row(&mut output, &row2).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[0].contains("\"name\":\"Alice\""));
        assert!(lines[1].contains("\"id\":2"));
        assert!(lines[1].contains("\"name\":\"Bob\""));
    }

    #[test]
    fn test_jsonl_null_value() {
        let mut formatter = JsonlFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Null]);
        formatter.write_row(&mut output, &row).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"name\":null"));
    }

    #[test]
    fn test_jsonl_supports_streaming() {
        let formatter = JsonlFormatter::new();
        assert!(formatter.supports_streaming());
    }
}
