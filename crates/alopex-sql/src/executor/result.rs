//! Result types for the Executor module.
//!
//! This module defines the output types for SQL execution:
//! - [`ExecutionResult`]: Top-level execution result
//! - [`QueryResult`]: SELECT query results with column info
//! - [`Row`]: Internal row representation with row_id

use crate::planner::ResolvedType;
use crate::storage::SqlValue;

/// Result of executing a SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    /// DDL operation success (CREATE/DROP TABLE/INDEX).
    Success,

    /// DML operation success with affected row count.
    RowsAffected(u64),

    /// Query result with columns and rows.
    Query(QueryResult),
}

/// Result of a SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Column information for the result set.
    pub columns: Vec<ColumnInfo>,

    /// Result rows as vectors of SqlValue.
    pub rows: Vec<Vec<SqlValue>>,
}

impl QueryResult {
    /// Create a new query result with column info and rows.
    pub fn new(columns: Vec<ColumnInfo>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self { columns, rows }
    }

    /// Create an empty query result with column info.
    pub fn empty(columns: Vec<ColumnInfo>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Returns the number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns the number of columns in the result.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Column information for query results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name (or alias if specified).
    pub name: String,

    /// Column data type.
    pub data_type: ResolvedType,
}

impl ColumnInfo {
    /// Create a new column info.
    pub fn new(name: impl Into<String>, data_type: ResolvedType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Internal row representation with row_id for DML operations.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Row identifier (unique within table).
    pub row_id: u64,

    /// Column values.
    pub values: Vec<SqlValue>,
}

impl Row {
    /// Create a new row with row_id and values.
    pub fn new(row_id: u64, values: Vec<SqlValue>) -> Self {
        Self { row_id, values }
    }

    /// Get a column value by index.
    pub fn get(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }

    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result_success() {
        let result = ExecutionResult::Success;
        assert!(matches!(result, ExecutionResult::Success));
    }

    #[test]
    fn test_execution_result_rows_affected() {
        let result = ExecutionResult::RowsAffected(5);
        if let ExecutionResult::RowsAffected(count) = result {
            assert_eq!(count, 5);
        } else {
            panic!("Expected RowsAffected variant");
        }
    }

    #[test]
    fn test_query_result_new() {
        let columns = vec![
            ColumnInfo::new("id", ResolvedType::Integer),
            ColumnInfo::new("name", ResolvedType::Text),
        ];
        let rows = vec![
            vec![SqlValue::Integer(1), SqlValue::Text("Alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("Bob".into())],
        ];
        let result = QueryResult::new(columns, rows);

        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_query_result_empty() {
        let columns = vec![ColumnInfo::new("id", ResolvedType::Integer)];
        let result = QueryResult::empty(columns);

        assert_eq!(result.row_count(), 0);
        assert_eq!(result.column_count(), 1);
        assert!(result.is_empty());
    }

    #[test]
    fn test_row_new() {
        let row = Row::new(
            42,
            vec![SqlValue::Integer(1), SqlValue::Text("test".into())],
        );

        assert_eq!(row.row_id, 42);
        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
        assert_eq!(row.get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(row.get(1), Some(&SqlValue::Text("test".into())));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_column_info_new() {
        let info = ColumnInfo::new("age", ResolvedType::Integer);
        assert_eq!(info.name, "age");
        assert_eq!(info.data_type, ResolvedType::Integer);
    }
}
