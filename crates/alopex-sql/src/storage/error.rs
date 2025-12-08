use thiserror::Error;

/// Result alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage-layer error type.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("primary key violation: table_id={table_id}, row_id={row_id}")]
    PrimaryKeyViolation { table_id: u32, row_id: u64 },

    #[error("unique constraint violation: index_id={index_id}")]
    UniqueViolation { index_id: u32 },

    #[error("null constraint violation: column={column}")]
    NullConstraintViolation { column: String },

    #[error("corrupted data: {reason}")]
    CorruptedData { reason: String },

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("transaction conflict")]
    TransactionConflict,

    #[error("transaction closed")]
    TransactionClosed,

    #[error("invalid key format")]
    InvalidKeyFormat,

    #[error("kv error: {0}")]
    KvError(alopex_core::error::Error),
}

impl From<alopex_core::error::Error> for StorageError {
    fn from(err: alopex_core::error::Error) -> Self {
        use alopex_core::error::Error as CoreError;
        match err {
            CoreError::TxnConflict => StorageError::TransactionConflict,
            CoreError::TxnClosed => StorageError::TransactionClosed,
            other => StorageError::KvError(other),
        }
    }
}
