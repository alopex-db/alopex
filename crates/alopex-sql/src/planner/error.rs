//! Planner error types for the Alopex SQL dialect.
//!
//! This module defines error types for the planning phase, including:
//! - Catalog errors (ALOPEX-C*): Table/column/index lookup failures
//! - Type errors (ALOPEX-T*): Type mismatches, constraint violations
//! - Feature errors (ALOPEX-F*): Unsupported features

use crate::ast::Span;
use thiserror::Error;

/// Planner errors for the Alopex SQL dialect.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlannerError {
    // === Catalog Errors (ALOPEX-C*) ===
    /// ALOPEX-C001: Table not found.
    #[error("error[ALOPEX-C001]: table '{name}' not found at line {line}, column {column}")]
    TableNotFound {
        name: String,
        line: u64,
        column: u64,
    },

    /// ALOPEX-C002: Table already exists.
    #[error("error[ALOPEX-C002]: table '{name}' already exists")]
    TableAlreadyExists { name: String },

    /// ALOPEX-C003: Column not found.
    #[error(
        "error[ALOPEX-C003]: column '{column}' not found in table '{table}' at line {line}, column {col}"
    )]
    ColumnNotFound {
        column: String,
        table: String,
        line: u64,
        col: u64,
    },

    /// ALOPEX-C004: Ambiguous column reference.
    #[error(
        "error[ALOPEX-C004]: ambiguous column '{column}' found in tables: {tables:?} at line {line}, column {col}"
    )]
    AmbiguousColumn {
        column: String,
        tables: Vec<String>,
        line: u64,
        col: u64,
    },

    /// ALOPEX-C005: Index already exists.
    #[error("error[ALOPEX-C005]: index '{name}' already exists")]
    IndexAlreadyExists { name: String },

    /// ALOPEX-C006: Index not found.
    #[error("error[ALOPEX-C006]: index '{name}' not found")]
    IndexNotFound { name: String },

    // === Type Errors (ALOPEX-T*) ===
    /// ALOPEX-T001: Type mismatch.
    #[error(
        "error[ALOPEX-T001]: type mismatch at line {line}, column {column}: expected {expected}, found {found}"
    )]
    TypeMismatch {
        expected: String,
        found: String,
        line: u64,
        column: u64,
    },

    /// ALOPEX-T002: Invalid operator for type.
    #[error(
        "error[ALOPEX-T002]: invalid operator '{op}' for type '{type_name}' at line {line}, column {column}"
    )]
    InvalidOperator {
        op: String,
        type_name: String,
        line: u64,
        column: u64,
    },

    /// ALOPEX-T003: NULL constraint violation.
    #[error(
        "error[ALOPEX-T003]: null constraint violation for column '{column}' at line {line}, column {col}"
    )]
    NullConstraintViolation { column: String, line: u64, col: u64 },

    /// ALOPEX-T004: Vector dimension mismatch.
    #[error(
        "error[ALOPEX-T004]: vector dimension mismatch at line {line}, column {column}: expected {expected}, found {found}"
    )]
    VectorDimensionMismatch {
        expected: u32,
        found: u32,
        line: u64,
        column: u64,
    },

    /// ALOPEX-T005: Invalid metric.
    #[error(
        "error[ALOPEX-T005]: invalid metric '{value}' at line {line}, column {column}. Valid options: cosine, l2, inner"
    )]
    InvalidMetric {
        value: String,
        line: u64,
        column: u64,
    },

    /// ALOPEX-T006: Column count does not match value count.
    #[error(
        "error[ALOPEX-T006]: column count ({columns}) does not match value count ({values}) at line {line}, column {column}"
    )]
    ColumnValueCountMismatch {
        columns: usize,
        values: usize,
        line: u64,
        column: u64,
    },

    // === Feature Errors (ALOPEX-F*) ===
    /// ALOPEX-F001: Unsupported feature.
    #[error(
        "error[ALOPEX-F001]: feature '{feature}' is not supported in this version. Expected in {version}"
    )]
    UnsupportedFeature {
        feature: String,
        version: String,
        line: u64,
        column: u64,
    },
}

impl PlannerError {
    /// Create a TableNotFound error from a span.
    pub fn table_not_found(name: impl Into<String>, span: Span) -> Self {
        Self::TableNotFound {
            name: name.into(),
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create a TableAlreadyExists error.
    pub fn table_already_exists(name: impl Into<String>) -> Self {
        Self::TableAlreadyExists { name: name.into() }
    }

    /// Create a ColumnNotFound error from a span.
    pub fn column_not_found(
        column: impl Into<String>,
        table: impl Into<String>,
        span: Span,
    ) -> Self {
        Self::ColumnNotFound {
            column: column.into(),
            table: table.into(),
            line: span.start.line,
            col: span.start.column,
        }
    }

    /// Create an AmbiguousColumn error from a span.
    pub fn ambiguous_column(column: impl Into<String>, tables: Vec<String>, span: Span) -> Self {
        Self::AmbiguousColumn {
            column: column.into(),
            tables,
            line: span.start.line,
            col: span.start.column,
        }
    }

    /// Create an IndexAlreadyExists error.
    pub fn index_already_exists(name: impl Into<String>) -> Self {
        Self::IndexAlreadyExists { name: name.into() }
    }

    /// Create an IndexNotFound error.
    pub fn index_not_found(name: impl Into<String>) -> Self {
        Self::IndexNotFound { name: name.into() }
    }

    /// Create a TypeMismatch error from a span.
    pub fn type_mismatch(
        expected: impl Into<String>,
        found: impl Into<String>,
        span: Span,
    ) -> Self {
        Self::TypeMismatch {
            expected: expected.into(),
            found: found.into(),
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create an InvalidOperator error from a span.
    pub fn invalid_operator(
        op: impl Into<String>,
        type_name: impl Into<String>,
        span: Span,
    ) -> Self {
        Self::InvalidOperator {
            op: op.into(),
            type_name: type_name.into(),
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create a NullConstraintViolation error from a span.
    pub fn null_constraint_violation(column: impl Into<String>, span: Span) -> Self {
        Self::NullConstraintViolation {
            column: column.into(),
            line: span.start.line,
            col: span.start.column,
        }
    }

    /// Create a VectorDimensionMismatch error from a span.
    pub fn vector_dimension_mismatch(expected: u32, found: u32, span: Span) -> Self {
        Self::VectorDimensionMismatch {
            expected,
            found,
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create an InvalidMetric error from a span.
    pub fn invalid_metric(value: impl Into<String>, span: Span) -> Self {
        Self::InvalidMetric {
            value: value.into(),
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create a ColumnValueCountMismatch error from a span.
    pub fn column_value_count_mismatch(columns: usize, values: usize, span: Span) -> Self {
        Self::ColumnValueCountMismatch {
            columns,
            values,
            line: span.start.line,
            column: span.start.column,
        }
    }

    /// Create an UnsupportedFeature error from a span.
    pub fn unsupported_feature(
        feature: impl Into<String>,
        version: impl Into<String>,
        span: Span,
    ) -> Self {
        Self::UnsupportedFeature {
            feature: feature.into(),
            version: version.into(),
            line: span.start.line,
            column: span.start.column,
        }
    }
}
