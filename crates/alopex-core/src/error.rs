//! Error and Result types for AlopexDB.
use std::path::PathBuf;
use thiserror::Error;

use crate::columnar::error::ColumnarError;

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

    /// Read-only トランザクションで書き込み操作を試みた。
    #[error("transaction is read-only")]
    TxnReadOnly,

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

    /// WAL recovery stopped before completion.
    #[error("recovery incomplete: recovered={recovered_entries}, stop_offset={stop_offset}, reason={reason}")]
    RecoveryIncomplete {
        /// Number of entries recovered before stopping.
        recovered_entries: usize,
        /// Byte offset where recovery stopped.
        stop_offset: u64,
        /// Reason for stopping.
        reason: String,
    },

    /// On-disk segment is corrupted (e.g., checksum failure).
    #[error("corrupted segment {segment_id}: {reason}")]
    CorruptedSegment {
        /// Segment identifier.
        segment_id: u64,
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

    /// A vector value is invalid for the requested operation.
    #[error("invalid vector at index {index}: {reason}")]
    InvalidVector {
        /// Zero-based index of the offending vector.
        index: usize,
        /// Reason for invalidation.
        reason: String,
    },

    /// A filter expression is malformed or unsupported.
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

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

    /// An index configuration parameter is invalid.
    #[error("invalid parameter {param}: {reason}")]
    InvalidParameter {
        /// Name of the invalid parameter.
        param: String,
        /// Description of the expected range or constraint.
        reason: String,
    },

    /// An index with the requested name was not found.
    #[error("index not found: {name}")]
    IndexNotFound {
        /// Name of the missing index.
        name: String,
    },

    /// An index failed integrity checks.
    #[error("corrupted index {name}: {reason}")]
    CorruptedIndex {
        /// Name of the corrupted index.
        name: String,
        /// Description of the corruption.
        reason: String,
    },

    /// The stored index version is unsupported by this binary.
    #[error("unsupported index version: found {found}, supported {supported}")]
    UnsupportedIndexVersion {
        /// Version detected in storage.
        found: u32,
        /// Highest supported version.
        supported: u32,
    },

    /// An unknown configuration or runtime option was provided.
    #[error("unknown option: {key}")]
    UnknownOption {
        /// Name of the option.
        key: String,
    },

    /// A column type does not match the expected layout.
    #[error("invalid column type for {column}, expected {expected}")]
    InvalidColumnType {
        /// Column name.
        column: String,
        /// Expected type description.
        expected: String,
    },

    /// The index is busy and cannot serve the requested operation.
    #[error("index busy during {operation}")]
    IndexBusy {
        /// Operation that was attempted.
        operation: String,
    },

    /// Errors originating from columnar components.
    #[error("columnar error: {0}")]
    Columnar(#[from] ColumnarError),

    /// An S3 operation failed.
    #[error("S3 error: {0}")]
    S3(String),

    /// Required credentials are missing.
    #[error("missing credentials: {0}")]
    MissingCredentials(String),
}
