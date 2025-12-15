//! Traits for the Key-Value storage layer.

use crate::error::Result;
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, Value};

/// MemoryKV / LsmKV を 1 つの型として扱うためのラッパー。
pub mod any;
pub mod memory;
/// Storage mode selection helpers (disk vs memory).
pub mod storage;

pub use any::AnyKV;

/// A transaction for interacting with the key-value store.
///
/// Transactions provide snapshot isolation.
pub trait KVTransaction<'a> {
    /// Returns the transaction's unique ID.
    fn id(&self) -> TxnId;

    /// Returns the transaction's mode (ReadOnly or ReadWrite).
    fn mode(&self) -> TxnMode;

    /// Retrieves the value for a given key.
    fn get(&mut self, key: &Key) -> Result<Option<Value>>;

    /// Sets a value for a given key.
    /// This operation is buffered and will be applied on commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is read-only.
    fn put(&mut self, key: Key, value: Value) -> Result<()>;

    /// Deletes a key-value pair.
    /// This operation is buffered and will be applied on commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is read-only.
    fn delete(&mut self, key: Key) -> Result<()>;

    /// Scans all key-value pairs whose keys start with the given prefix.
    ///
    /// Implementations must respect snapshot isolation: results should reflect
    /// the transaction's start version plus its in-flight writes.
    fn scan_prefix(&mut self, prefix: &[u8])
        -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>>;

    /// Scans key-value pairs in the half-open range [start, end).
    ///
    /// Implementations must respect snapshot isolation: results should reflect
    /// the transaction's start version plus its in-flight writes.
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>>;

    /// Commits the transaction, applying all buffered writes.
    ///
    /// This method consumes the transaction. On success, all writes become
    /// visible to subsequent transactions. On failure, no changes are applied.
    fn commit_self(self) -> Result<()>;

    /// Rolls back the transaction, discarding all buffered writes.
    ///
    /// This method consumes the transaction. All pending writes are discarded.
    fn rollback_self(self) -> Result<()>;
}

/// The main trait for a key-value storage engine.
///
/// This trait provides the primary entry point for interacting with the database.
pub trait KVStore: Send + Sync {
    /// The transaction type for this store.
    type Transaction<'a>: KVTransaction<'a>
    where
        Self: 'a;

    /// The transaction manager for this store.
    type Manager<'a>: TxnManager<'a, Self::Transaction<'a>>
    where
        Self: 'a;

    /// Returns the transaction manager for this store.
    fn txn_manager(&self) -> Self::Manager<'_>;

    /// A convenience method to begin a new transaction.
    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>>;
}
