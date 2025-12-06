//! カラムナー処理専用のエラー型と結果型。
use thiserror::Error;

use crate::Error as CoreError;

/// カラムナー系処理用の結果型。
pub type Result<T> = std::result::Result<T, ColumnarError>;

/// カラムナー処理で発生するエラー。
#[derive(Debug, Error)]
pub enum ColumnarError {
    /// チェックサムが一致しない。
    #[error("checksum mismatch")]
    ChecksumMismatch,
    /// サポートされないフォーマットバージョン。
    #[error("unsupported format version: found={found}, expected={expected}")]
    UnsupportedFormatVersion {
        /// 実際に検出したバージョン。
        found: u16,
        /// 期待するバージョン。
        expected: u16,
    },
    /// サポートされない圧縮方式。
    #[error("unsupported compression: {algorithm}")]
    UnsupportedCompression {
        /// 圧縮アルゴリズムの識別子。
        algorithm: String,
    },
    /// メモリ上限を超過した。
    #[error("memory limit exceeded: limit={limit}, requested={requested}")]
    MemoryLimitExceeded {
        /// 許容上限（バイト）。
        limit: usize,
        /// 要求サイズ（バイト）。
        requested: usize,
    },
    /// フォーマットが不正。
    #[error("invalid format: {0}")]
    InvalidFormat(String),
    /// リソースが存在しない。
    #[error("not found")]
    NotFound,
    /// 取引の競合。
    #[error("transaction conflict")]
    TxnConflict,
    /// RowGroup が許容サイズを超えた。
    #[error("row group too large: size={size}, max={max}")]
    RowGroupTooLarge {
        /// RowGroup のサイズ（バイト）。
        size: u64,
        /// 許容上限（バイト）。
        max: u64,
    },
    /// In-memory モードでのみ許可される操作。
    #[error("not in in-memory mode")]
    NotInMemoryMode,
    /// テーブルが存在しない。
    #[error("table not found: {table}")]
    TableNotFound {
        /// 見つからなかったテーブル名。
        table: String,
    },
    /// セグメントが壊れている、またはフォーマットが不正。
    #[error("corrupted segment: {reason}")]
    CorruptedSegment {
        /// 壊れている理由。
        reason: String,
    },
    /// エンコード/デコードに失敗。
    #[error("encoding error: {reason}")]
    EncodingError {
        /// 失敗理由。
        reason: String,
    },
    /// I/O エラー。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<CoreError> for ColumnarError {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::ChecksumMismatch => ColumnarError::ChecksumMismatch,
            CoreError::InvalidFormat(reason) => ColumnarError::InvalidFormat(reason),
            CoreError::CorruptedSegment { reason } => ColumnarError::CorruptedSegment { reason },
            CoreError::MemoryLimitExceeded { limit, requested } => {
                ColumnarError::MemoryLimitExceeded { limit, requested }
            }
            CoreError::TxnConflict => ColumnarError::TxnConflict,
            CoreError::NotFound => ColumnarError::NotFound,
            CoreError::Io(e) => ColumnarError::Io(e),
            other => ColumnarError::EncodingError {
                reason: other.to_string(),
            },
        }
    }
}
