//! Result types for the Executor module.
//!
//! This module defines the output types for SQL execution:
//! - [`ExecutionResult`]: Top-level execution result
//! - [`QueryResult`]: SELECT query results with column info
//! - [`QueryRowIterator`]: Streaming query result with row iterator
//! - [`Row`]: Internal row representation with row_id

use crate::catalog::ColumnMetadata;
use crate::executor::evaluator::EvalContext;
use crate::executor::query::iterator::RowIterator;
use crate::planner::ResolvedType;
use crate::planner::typed_expr::Projection;
use crate::storage::SqlValue;

use super::ExecutorError;

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

// ============================================================================
// QueryRowIterator - Streaming query result
// ============================================================================

/// Streaming query result that yields rows one at a time.
///
/// This type enables true streaming output for SELECT queries by applying
/// projection on-the-fly and yielding projected rows through an iterator.
///
/// # Example
///
/// ```ignore
/// let mut iter = db.execute_sql_streaming("SELECT * FROM users")?;
/// while let Some(row) = iter.next_row()? {
///     println!("{:?}", row);
/// }
/// ```
pub struct QueryRowIterator<'a> {
    /// Underlying row iterator.
    inner: Box<dyn RowIterator + 'a>,
    /// Projection to apply to each row.
    projection: Projection,
    /// Schema of the input rows (kept for potential future use).
    #[allow(dead_code)]
    schema: Vec<ColumnMetadata>,
    /// Column information for output rows.
    columns: Vec<ColumnInfo>,
}

impl<'a> QueryRowIterator<'a> {
    /// Create a new streaming query result.
    pub fn new(
        inner: Box<dyn RowIterator + 'a>,
        projection: Projection,
        schema: Vec<ColumnMetadata>,
    ) -> Self {
        // Build column info from projection
        let columns = match &projection {
            Projection::All(_) => schema
                .iter()
                .map(|col| ColumnInfo::new(&col.name, col.data_type.clone()))
                .collect(),
            Projection::Columns(cols) => cols
                .iter()
                .map(|col| {
                    let name = col.alias.clone().unwrap_or_else(|| match &col.expr.kind {
                        crate::planner::typed_expr::TypedExprKind::ColumnRef { column, .. } => {
                            column.clone()
                        }
                        _ => "?column?".to_string(),
                    });
                    ColumnInfo::new(name, col.expr.resolved_type.clone())
                })
                .collect(),
        };

        Self {
            inner,
            projection,
            schema,
            columns,
        }
    }

    /// Returns column information for the result set.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Advance and return the next projected row, or None if exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or projection fails.
    pub fn next_row(&mut self) -> Result<Option<Vec<SqlValue>>, ExecutorError> {
        match self.inner.next_row() {
            Some(Ok(row)) => {
                let projected = self.project_row(&row)?;
                Ok(Some(projected))
            }
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Apply projection to a single row.
    fn project_row(&self, row: &Row) -> Result<Vec<SqlValue>, ExecutorError> {
        match &self.projection {
            Projection::All(_) => Ok(row.values.clone()),
            Projection::Columns(proj_cols) => {
                let mut output = Vec::with_capacity(proj_cols.len());
                let ctx = EvalContext::new(&row.values);
                for col in proj_cols {
                    let value = crate::executor::evaluator::evaluate(&col.expr, &ctx)?;
                    output.push(value);
                }
                Ok(output)
            }
        }
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
