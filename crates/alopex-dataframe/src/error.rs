use std::path::PathBuf;

use crate::physical::budget::{ResourceScope, StreamFailureClass};

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

    /// A bounded execution reservation was rejected before the requested ownership began.
    #[error(
        "resource limit exceeded in {scope}: requested total {observed} bytes exceeds {limit} bytes \\
         (in-flight batches {observed_batches}/{max_in_flight_batches})",
        scope = scope.as_str()
    )]
    ResourceLimitExceeded {
        /// Configured byte limit.
        limit: u64,
        /// Requested byte total at the failed reservation point.
        observed: u64,
        /// Configured in-flight batch limit.
        max_in_flight_batches: usize,
        /// Requested in-flight batch total at the failed reservation point.
        observed_batches: usize,
        /// Allocation domain that requested the reservation.
        scope: ResourceScope,
    },

    /// A stream was closed by its consumer before normal completion.
    #[error("stream is closed")]
    StreamClosed,

    /// A stream was cancelled by its consumer before normal completion.
    #[error("stream is cancelled")]
    StreamCancelled,

    /// A stream failed and must reproduce its initial structured diagnostic.
    #[error("stream failed: {code} ({classification:?})")]
    StreamFailed {
        /// Stable failure code.
        code: &'static str,
        /// Stable broad failure classification.
        classification: StreamFailureClass,
    },

    /// Streaming was rejected before the source was opened.
    #[error("streaming unsupported for {subject}: {reason}")]
    StreamingUnsupported { subject: String, reason: String },

    /// Streaming would require global materialization and has no approved bounded algorithm.
    #[error("streaming requires materialization for {subject}: {reason}")]
    StreamingRequiresMaterialization { subject: String, reason: String },
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

    /// Classify a roadmap DataFrame operation before a caller attempts execution.
    ///
    /// `cast`, `pivot`, `unpivot`, and `window` are intentionally not exposed as working
    /// DataFrame operations in v0.9.  Returning their stable, pre-execution rejection here lets
    /// adapters report the supported boundary instead of implying that a missing method ran and
    /// failed. `cse` and `concat` are backed by the existing optimizer/execution paths.
    pub fn preflight_dataframe_operation(operation: &str) -> Result<()> {
        match operation {
            "cse" | "concat" => Ok(()),
            "cast" | "pivot" | "unpivot" | "window" => Err(Self::invalid_operation(format!(
                "dataframe operation '{operation}' is planned and unavailable: pre_execution_unsupported"
            ))),
            // Phase 2 deliberately has no DataFrame CRDT namespace.  Classify
            // Counter and Set create/read/increment/decrement operations explicitly so callers
            // receive a stable boundary result before any eager or lazy plan can be built.
            "crdt_counter_create"
            | "crdt_counter_read"
            | "crdt_counter_increment"
            | "crdt_counter_decrement"
            | "crdt_set_create"
            | "crdt_set_read"
            | "crdt_set_add" => {
                Err(Self::invalid_operation(format!(
                "dataframe CRDT operation '{operation}' is unsupported: pre_execution_unsupported"
                )))
            }
            _ => Err(Self::invalid_operation(format!(
                "unknown dataframe operation '{operation}'"
            ))),
        }
    }

    /// Create an invalid configuration error.
    pub fn configuration(option: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Configuration {
            option: option.into(),
            message: message.into(),
        }
    }

    /// Create a structured resource-limit error.
    pub fn resource_limit_exceeded(
        limit: u64,
        observed: u64,
        max_in_flight_batches: usize,
        observed_batches: usize,
        scope: ResourceScope,
    ) -> Self {
        Self::ResourceLimitExceeded {
            limit,
            observed,
            max_in_flight_batches,
            observed_batches,
            scope,
        }
    }

    /// Create the repeatable early-close terminal error.
    pub const fn stream_closed() -> Self {
        Self::StreamClosed
    }

    /// Create the repeatable cancellation terminal error.
    pub const fn stream_cancelled() -> Self {
        Self::StreamCancelled
    }

    /// Create the repeatable structured stream-failure error.
    pub const fn stream_failed(code: &'static str, classification: StreamFailureClass) -> Self {
        Self::StreamFailed {
            code,
            classification,
        }
    }

    /// Create a source-open preflight rejection without falling back to eager execution.
    pub fn streaming_unsupported(subject: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::StreamingUnsupported {
            subject: subject.into(),
            reason: reason.into(),
        }
    }

    /// Create a preflight rejection for a plan requiring unimplemented global materialization.
    pub fn streaming_requires_materialization(
        subject: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::StreamingRequiresMaterialization {
            subject: subject.into(),
            reason: reason.into(),
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
