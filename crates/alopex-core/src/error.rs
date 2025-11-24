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
}
