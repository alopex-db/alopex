//! Error types for the Executor module.
//!
//! This module defines error types for SQL execution:
//! - [`ExecutorError`]: Top-level executor errors
//! - [`ConstraintViolation`]: Constraint violation details
//! - [`EvaluationError`]: Expression evaluation errors

use crate::executor::evaluator::VectorError;
use crate::storage::StorageError;
use thiserror::Error;

/// Errors that can occur during SQL execution.
#[derive(Debug, Error)]
pub enum ExecutorError {
    /// Underlying HNSW/kv error.
    #[error("hnsw error: {0}")]
    Core(#[from] alopex_core::Error),

    /// Table not found in catalog.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// Table already exists in catalog.
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    /// Index not found in catalog.
    #[error("index not found: {0}")]
    IndexNotFound(String),

    /// Index already exists in catalog.
    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    /// Column not found in table.
    #[error("column not found: {0}")]
    ColumnNotFound(String),

    /// Constraint violation during DML operation.
    #[error("constraint violation: {0}")]
    ConstraintViolation(#[from] ConstraintViolation),

    /// Expression evaluation error.
    #[error("evaluation error: {0}")]
    Evaluation(#[from] EvaluationError),

    /// Unsupported SQL operation.
    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),

    /// Transaction conflict (concurrent modification).
    #[error("transaction conflict")]
    TransactionConflict,

    /// Storage layer error.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Column value required (DEFAULT not supported in v0.1.2).
    #[error("column required: {column} (DEFAULT not supported in v0.1.2)")]
    ColumnRequired { column: String },

    /// Invalid index name (reserved prefix).
    #[error("invalid index name: {name} - {reason}")]
    InvalidIndexName { name: String, reason: String },

    /// Invalid operation.
    #[error("invalid operation: {operation} - {reason}")]
    InvalidOperation { operation: String, reason: String },

    /// Input file not found.
    #[error("file not found: {0}")]
    FileNotFound(String),

    /// File path failed validation.
    #[error("path validation failed for '{path}': {reason}")]
    PathValidationFailed { path: String, reason: String },

    /// Unsupported file format.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Schema mismatch between input and table.
    #[error("schema mismatch: expected {expected} columns, got {actual} - {reason}")]
    SchemaMismatch {
        expected: usize,
        actual: usize,
        reason: String,
    },

    /// Invalid storage type option.
    #[error("invalid storage type: {0}")]
    InvalidStorageType(String),

    /// Invalid compression option.
    #[error("invalid compression: {0}")]
    InvalidCompression(String),

    /// Invalid row group size option.
    #[error("invalid row_group_size: {0}")]
    InvalidRowGroupSize(String),

    /// Invalid rowid_mode option.
    #[error("invalid rowid_mode: {0}")]
    InvalidRowIdMode(String),

    /// Unknown table option key.
    #[error("unknown table option: {0}")]
    UnknownTableOption(String),

    /// Duplicate option key.
    #[error("duplicate option: {0}")]
    DuplicateOption(String),

    /// Vector processing error.
    #[error("vector error: {0}")]
    Vector(#[from] VectorError),

    /// Bulk load generic error.
    #[error("bulk load error: {0}")]
    BulkLoad(String),

    /// Columnar engine error.
    #[error("columnar engine error: {0}")]
    Columnar(String),

    /// Planner error (wrapped for convenience).
    #[error("planner error: {0}")]
    Planner(#[from] crate::planner::PlannerError),
}

/// Constraint violation details.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConstraintViolation {
    /// NOT NULL constraint violated.
    #[error("NOT NULL constraint violated on column: {column}")]
    NotNull { column: String },

    /// PRIMARY KEY constraint violated.
    #[error("PRIMARY KEY constraint violated on columns: {columns:?}, value: {value:?}")]
    PrimaryKey {
        columns: Vec<String>,
        value: Option<String>,
    },

    /// UNIQUE constraint violated.
    #[error(
        "UNIQUE constraint violated on index: {index_name}, columns: {columns:?}, value: {value:?}"
    )]
    Unique {
        index_name: String,
        columns: Vec<String>,
        value: Option<String>,
    },
}

/// Expression evaluation errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EvaluationError {
    /// Division by zero.
    #[error("division by zero")]
    DivisionByZero,

    /// Integer overflow.
    #[error("integer overflow")]
    Overflow,

    /// Type mismatch during evaluation.
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Invalid column reference (index out of bounds).
    #[error("invalid column reference: index {index}")]
    InvalidColumnRef { index: usize },

    /// Unsupported expression type.
    #[error("unsupported expression: {0}")]
    UnsupportedExpression(String),

    /// Unsupported function call.
    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),

    /// Vector evaluation error.
    #[error("vector error: {0}")]
    Vector(#[from] VectorError),
}

/// Type alias for executor results.
pub type Result<T> = std::result::Result<T, ExecutorError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_error_display() {
        let err = ExecutorError::TableNotFound("users".into());
        assert_eq!(err.to_string(), "table not found: users");
    }

    #[test]
    fn test_constraint_violation_display() {
        let err = ConstraintViolation::NotNull {
            column: "name".into(),
        };
        assert_eq!(
            err.to_string(),
            "NOT NULL constraint violated on column: name"
        );
    }

    #[test]
    fn test_evaluation_error_display() {
        let err = EvaluationError::DivisionByZero;
        assert_eq!(err.to_string(), "division by zero");
    }

    #[test]
    fn test_constraint_violation_from() {
        let violation = ConstraintViolation::NotNull {
            column: "age".into(),
        };
        let err: ExecutorError = violation.into();
        assert!(matches!(err, ExecutorError::ConstraintViolation(_)));
    }

    #[test]
    fn test_evaluation_error_from() {
        let eval_err = EvaluationError::Overflow;
        let err: ExecutorError = eval_err.into();
        assert!(matches!(err, ExecutorError::Evaluation(_)));
    }

    #[test]
    fn test_vector_error_conversion() {
        let vector_err = VectorError::ZeroNormVector;
        let eval_err: EvaluationError = vector_err.clone().into();
        assert!(matches!(eval_err, EvaluationError::Vector(_)));

        let exec_err: ExecutorError = vector_err.into();
        assert!(matches!(exec_err, ExecutorError::Vector(_)));
    }

    #[test]
    fn test_path_validation_failed_display() {
        let err = ExecutorError::PathValidationFailed {
            path: "/tmp/data.parquet".into(),
            reason: "symbolic links not allowed".into(),
        };
        assert_eq!(
            err.to_string(),
            "path validation failed for '/tmp/data.parquet': symbolic links not allowed"
        );
    }
}
