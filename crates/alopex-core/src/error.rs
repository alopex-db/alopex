//! Error and Result types for AlopexDB.
use std::path::PathBuf;
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

    /// On-disk segment is corrupted (e.g., checksum failure).
    #[error("corrupted segment: {reason}")]
    CorruptedSegment {
        /// Reason for corruption detection.
        reason: String,
    },

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

    /// Memory usage exceeded configured limit.
    #[error("memory limit exceeded: limit={limit}, requested={requested}")]
    MemoryLimitExceeded {
        /// Maximum allowed memory (bytes).
        limit: usize,
        /// Requested memory (bytes) that triggered the limit.
        requested: usize,
    },

    /// The provided path already exists and cannot be overwritten.
    #[error("path exists: {0}")]
    PathExists(PathBuf),
}
