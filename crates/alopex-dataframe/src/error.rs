use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DataFrameError {
    #[error("I/O error{path}: {source}", path = path_display(.path))]
    Io {
        source: std::io::Error,
        path: Option<PathBuf>,
    },

    #[error("schema mismatch: {message}")]
    SchemaMismatch { message: String },

    #[error(
        "type mismatch{column}: expected {expected}, got {actual}",
        column = column_display(.column)
    )]
    TypeMismatch {
        column: Option<String>,
        expected: String,
        actual: String,
    },

    #[error("column not found: {name}")]
    ColumnNotFound { name: String },

    #[error("invalid operation: {message}")]
    InvalidOperation { message: String },

    #[error("invalid configuration option '{option}': {message}")]
    Configuration { option: String, message: String },

    #[error("arrow error: {source}")]
    Arrow { source: arrow::error::ArrowError },

    #[error("parquet error: {source}")]
    Parquet {
        source: parquet::errors::ParquetError,
    },
}

pub type Result<T> = std::result::Result<T, DataFrameError>;

impl DataFrameError {
    pub fn io(source: std::io::Error) -> Self {
        Self::Io { source, path: None }
    }

    pub fn io_with_path(source: std::io::Error, path: impl Into<PathBuf>) -> Self {
        Self::Io {
            source,
            path: Some(path.into()),
        }
    }

    pub fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::SchemaMismatch {
            message: message.into(),
        }
    }

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

    pub fn column_not_found(name: impl Into<String>) -> Self {
        Self::ColumnNotFound { name: name.into() }
    }

    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

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
