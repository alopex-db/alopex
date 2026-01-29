//! Data models for CLI output
//!
//! This module defines the data structures used for representing
//! query results and output data.

use serde::Serialize;

/// A single value in a row.
///
/// Represents all possible data types that can appear in query results.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    /// Null/missing value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Floating-point value
    Float(f64),
    /// Text/string value
    Text(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Vector/embedding data
    Vector(Vec<f32>),
}

/// A single row of output data.
///
/// Contains a list of values corresponding to the columns.
#[derive(Debug, Clone, Serialize)]
pub struct Row {
    /// Column values in order
    pub columns: Vec<Value>,
}

impl Row {
    /// Create a new row with the given values.
    pub fn new(columns: Vec<Value>) -> Self {
        Self { columns }
    }
}

/// Column metadata.
///
/// Describes the name and data type of a column in the result set.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    #[allow(dead_code)]
    pub data_type: DataType,
}

impl Column {
    /// Create a new column with the given name and type.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Data types supported by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum DataType {
    /// Boolean type
    Bool,
    /// Integer type
    Int,
    /// Floating-point type
    Float,
    /// Text/string type
    Text,
    /// Binary data type
    Bytes,
    /// Vector/embedding type
    Vector,
}

/// Result of a CLI command execution.
///
/// Commands can return rows of data, a message, or affected row count.
#[allow(dead_code)]
pub enum CommandResult {
    /// Query returned rows of data
    Rows {
        /// Column metadata
        columns: Vec<Column>,
        /// Iterator over result rows
        rows: Box<dyn Iterator<Item = Row>>,
    },
    /// Command returned a message
    Message(String),
    /// Command affected some number of rows
    AffectedRows(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null() {
        let v = Value::Null;
        assert_eq!(serde_json::to_string(&v).unwrap(), "null");
    }

    #[test]
    fn test_value_bool() {
        let v = Value::Bool(true);
        assert_eq!(serde_json::to_string(&v).unwrap(), "true");
    }

    #[test]
    fn test_value_int() {
        let v = Value::Int(42);
        assert_eq!(serde_json::to_string(&v).unwrap(), "42");
    }

    #[test]
    fn test_value_float() {
        let v = Value::Float(1.23);
        assert_eq!(serde_json::to_string(&v).unwrap(), "1.23");
    }

    #[test]
    fn test_value_text() {
        let v = Value::Text("hello".to_string());
        assert_eq!(serde_json::to_string(&v).unwrap(), "\"hello\"");
    }

    #[test]
    fn test_value_bytes() {
        let v = Value::Bytes(vec![1, 2, 3]);
        assert_eq!(serde_json::to_string(&v).unwrap(), "[1,2,3]");
    }

    #[test]
    fn test_value_vector() {
        let v = Value::Vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(serde_json::to_string(&v).unwrap(), "[1.0,2.0,3.0]");
    }

    #[test]
    fn test_row_new() {
        let row = Row::new(vec![Value::Int(1), Value::Text("test".to_string())]);
        assert_eq!(row.columns.len(), 2);
    }

    #[test]
    fn test_row_serialize() {
        let row = Row::new(vec![Value::Int(1), Value::Text("test".to_string())]);
        let json = serde_json::to_string(&row).unwrap();
        assert_eq!(json, r#"{"columns":[1,"test"]}"#);
    }

    #[test]
    fn test_column_new() {
        let col = Column::new("name", DataType::Text);
        assert_eq!(col.name, "name");
        assert_eq!(col.data_type, DataType::Text);
    }

    #[test]
    fn test_data_type_equality() {
        assert_eq!(DataType::Int, DataType::Int);
        assert_ne!(DataType::Int, DataType::Float);
    }
}
