use std::path::PathBuf;

/// Errors returned by `alopex-dataframe` operations.
#[derive(Debug, thiserror::Error)]
pub enum DataFrameError {
    /// OS-level I/O error (optionally associated with a path).
    #[error("I/O error{path}: {source}", path = path_display(.path))]
    Io {
        source: std::io::Error,
        path: Option<PathBuf>,
    },

    /// Arrow schema-related mismatch (e.g. different schema across batches).
    #[error("schema mismatch: {message}")]
    SchemaMismatch { message: String },

    /// Data type mismatch (e.g. non-numeric aggregation or incompatible dtypes).
    #[error(
        "type mismatch{column}: expected {expected}, got {actual}",
        column = column_display(.column)
    )]
    TypeMismatch {
        column: Option<String>,
        expected: String,
        actual: String,
    },

    /// Referenced column does not exist.
    #[error("column not found: {name}")]
    ColumnNotFound { name: String },

    /// Operation is not supported or invalid for the current inputs.
    #[error("invalid operation: {message}")]
    InvalidOperation { message: String },

    /// Invalid configuration option was provided.
    #[error("invalid configuration option '{option}': {message}")]
    Configuration { option: String, message: String },

    /// Error originating from Arrow compute / record batch APIs.
    #[error("arrow error: {source}")]
    Arrow { source: arrow::error::ArrowError },

    /// Error originating from Parquet APIs.
    #[error("parquet error: {source}")]
    Parquet {
        source: parquet::errors::ParquetError,
    },
}

/// Result type used throughout this crate.
pub type Result<T> = std::result::Result<T, DataFrameError>;

impl DataFrameError {
    /// Create an I/O error without a path.
    pub fn io(source: std::io::Error) -> Self {
        Self::Io { source, path: None }
    }

    /// Create an I/O error associated with a path.
    pub fn io_with_path(source: std::io::Error, path: impl Into<PathBuf>) -> Self {
        Self::Io {
            source,
            path: Some(path.into()),
        }
    }

    /// Create a schema mismatch error with a message.
    pub fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::SchemaMismatch {
            message: message.into(),
        }
    }

    /// Create a type mismatch error with optional column context.
    pub fn type_mismatch(
        column: impl Into<Option<String>>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self::TypeMismatch {
            column: column.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create a missing column error.
    pub fn column_not_found(name: impl Into<String>) -> Self {
        Self::ColumnNotFound { name: name.into() }
    }

    /// Create an invalid operation error.
    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

    /// Create an invalid configuration error.
    pub fn configuration(option: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Configuration {
            option: option.into(),
            message: message.into(),
        }
    }
}

fn column_display(column: &Option<String>) -> String {
    column
        .as_ref()
        .map(|c| format!(" for column '{c}'"))
        .unwrap_or_default()
}

fn path_display(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map(|p| format!(" for path '{}'", p.display()))
        .unwrap_or_default()
}
