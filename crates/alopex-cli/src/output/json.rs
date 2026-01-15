//! JSON array formatter
//!
//! Outputs data as a JSON array. Supports streaming output.

use std::io::Write;

use crate::error::Result;
use crate::models::{Column, Row, Value};

use super::formatter::Formatter;

/// JSON array formatter.
///
/// Outputs data as a JSON array of objects.
/// Supports streaming by writing array elements incrementally.
pub struct JsonFormatter {
    /// Column names (set on write_header)
    columns: Vec<String>,
    /// Whether the first row has been written
    first_row: bool,
}

impl JsonFormatter {
    /// Create a new JSON formatter.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            first_row: true,
        }
    }
}

impl Default for JsonFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for JsonFormatter {
    fn write_header(&mut self, writer: &mut dyn Write, columns: &[Column]) -> Result<()> {
        // Store column names and start the JSON array
        self.columns = columns.iter().map(|c| c.name.clone()).collect();
        self.first_row = true;
        writeln!(writer, "[")?;
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
        let json_str = serde_json::to_string_pretty(&json)?;

        // Add comma before all rows except the first
        if self.first_row {
            self.first_row = false;
        } else {
            write!(writer, ",")?;
            writeln!(writer)?;
        }

        // Indent the JSON object
        for line in json_str.lines() {
            writeln!(writer, "  {}", line)?;
        }

        Ok(())
    }

    fn write_footer(&mut self, writer: &mut dyn Write) -> Result<()> {
        // Close the JSON array
        if !self.first_row {
            writeln!(writer)?;
        }
        writeln!(writer, "]")?;
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
    fn test_json_empty() {
        let mut formatter = JsonFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();
        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains('['));
        assert!(result.contains(']'));
    }

    #[test]
    fn test_json_single_row() {
        let mut formatter = JsonFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"id\": 1"));
        assert!(result.contains("\"name\": \"Alice\""));

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_json_multiple_rows() {
        let mut formatter = JsonFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row1 = Row::new(vec![Value::Int(1), Value::Text("Alice".to_string())]);
        let row2 = Row::new(vec![Value::Int(2), Value::Text("Bob".to_string())]);

        formatter.write_row(&mut output, &row1).unwrap();
        formatter.write_row(&mut output, &row2).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_json_null_value() {
        let mut formatter = JsonFormatter::new();
        let mut output = Vec::new();

        let columns = test_columns();
        formatter.write_header(&mut output, &columns).unwrap();

        let row = Row::new(vec![Value::Int(1), Value::Null]);
        formatter.write_row(&mut output, &row).unwrap();

        formatter.write_footer(&mut output).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("\"name\": null"));
    }

    #[test]
    fn test_json_supports_streaming() {
        let formatter = JsonFormatter::new();
        assert!(formatter.supports_streaming());
    }
}
