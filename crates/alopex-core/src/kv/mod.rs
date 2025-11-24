//! Traits for the Key-Value storage layer.

use crate::error::Result;
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, Value};

pub mod memory;

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
