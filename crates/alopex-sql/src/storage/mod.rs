#[cfg(feature = "tokio")]
pub mod async_storage;
pub mod bridge;
pub mod changefeed_journal;
pub mod codec;
#[cfg(feature = "tokio")]
pub mod erased;
pub mod error;
pub mod index;
pub mod key;
pub mod range_read;
pub mod table;
pub mod value;

#[cfg(feature = "tokio")]
pub use async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
pub use bridge::{
    BorrowedSqlTransaction, LocalRangeChangeJournal, RangeChangeJournalScope, SqlTransaction,
    SqlTxn, TxnBridge, TxnContext,
};
pub use changefeed_journal::{ChangefeedJournalError, CommittedSqlChangeJournal};
pub use codec::RowCodec;
#[cfg(feature = "tokio")]
pub use erased::ErasedAsyncSqlTransaction;
pub use error::StorageError;
pub use index::{IndexScanIterator, IndexStorage};
pub use key::KeyEncoder;
pub use range_read::{
    RangeBoundedScanIterator, RangeReadSnapshot, StorageRangeConstraint,
    StorageRangeConstraintError,
};
pub use table::{TableScanIterator, TableStorage};
pub use value::SqlValue;

// A transaction adapter must complete this parser/classifier boundary before
// it asks storage to acquire a distributed participant.  Re-exporting it at
// the storage boundary keeps that precondition visible without altering the
// legacy local `SqlTransaction` API.
pub use crate::transaction_classifier::{
    TransactionSqlClassification, TransactionSqlPreflightError, TransactionSqlPreflightFailure,
    TransactionSqlPreflightResult, preflight_transaction_sql, preflight_transaction_statement,
};

#[cfg(test)]
mod disk;
