//! Transaction management traits.

use crate::error::Result;
use crate::types::TxnMode;

/// A manager for creating and committing transactions.
///
/// This trait is generic over the transaction type it manages.
pub trait TxnManager<'a, T> {
    /// Begins a new transaction in the specified mode.
    fn begin(&'a self, mode: TxnMode) -> Result<T>;

    /// Commits a transaction, applying its changes to the store.
    fn commit(&'a self, txn: T) -> Result<()>;

    /// Rolls back a transaction, discarding its changes.
    fn rollback(&'a self, txn: T) -> Result<()>;
}
