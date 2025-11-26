//! Error and Result types for AlopexDB.
use thiserror::Error;

/// A convenience `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for AlopexDB operations.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested key was not found.
    #[error("key not found")]
    NotFound,

    /// The transaction has already been closed (committed or rolled back).
    #[error("transaction is closed")]
    TxnClosed,

    /// A transaction conflict occurred (e.g., optimistic concurrency control failure).
    #[error("transaction conflict")]
    TxnConflict,

    /// An underlying I/O error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The on-disk format is invalid or corrupted.
    #[error("invalid format: {0}")]
    InvalidFormat(String),

    /// A checksum validation failed.
    #[error("checksum mismatch")]
    ChecksumMismatch,

    /// A vector with an unexpected dimension was provided.
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Expected dimension.
        expected: usize,
        /// Dimension of the provided vector.
        actual: usize,
    },

    /// A metric that is not supported was requested.
    #[error("unsupported metric: {metric}")]
    UnsupportedMetric {
        /// Name of the unsupported metric.
        metric: String,
    },
}
