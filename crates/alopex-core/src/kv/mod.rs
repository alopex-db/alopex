//! Traits for the Key-Value storage layer.

use crate::error::Result;
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, Value};

/// Runtime statistics exposed by SQL system functions.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeStats {
    /// Statistics for the in-memory store.
    Memory(crate::kv::memory::MemoryStats),
    /// Statistics for the LSM store.
    Lsm(crate::lsm::metrics::LsmMetricsSnapshot),
}

#[cfg(feature = "test-hooks")]
pub mod hooks;

/// MemoryKV / LsmKV を 1 つの型として扱うためのラッパー。
pub mod any;
/// Async adapter for sync KV stores (requires `tokio` feature).
#[cfg(feature = "tokio")]
pub mod async_adapter;
/// Async KV traits (requires `async` feature).
#[cfg(feature = "async")]
pub mod async_kv;
/// Atomic local range-change journal capability.
pub mod change_journal;
pub mod memory;
/// Owned session contracts for long-lived local consumers.
pub mod owned;
/// Read-point capability for fenced distributed reads.
pub mod read_at;
/// Storage mode selection helpers (disk vs memory).
pub mod storage;

/// S3-backed storage (requires `s3` feature).
#[cfg(feature = "s3")]
pub mod s3;

pub use any::AnyKV;
#[cfg(feature = "tokio")]
pub use async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
#[cfg(feature = "async")]
pub use async_kv::{AsyncKVStore, AsyncKVTransaction};
pub use change_journal::{
    decode_range_change, journal_key, stage_range_change, RangeChangeJournalCapability,
    RangeChangePayload, RangeChangeRecord,
};
pub use owned::{
    OwnedKVScan, OwnedKVStore, OwnedKVTransaction, OwnedKVTransactionAdapter, OwnedReadLease,
    OwnedReadOptions, OwnedReadSession, OwnedReadSessionApi, OwnedSessionFactory,
    OwnedTransactionLease, OwnedTransactionSession, OwnedTransactionSessionApi,
};
pub use read_at::{ReadAtCapability, ReadAtError, ReadAtPoint, ReadAtResult};

#[cfg(feature = "s3")]
pub use s3::{S3Config, S3KV};

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

    /// Reports whether this backend can open a retained snapshot at a
    /// caller-provided cluster data epoch.
    ///
    /// A normal `begin(ReadOnly)` snapshot is intentionally not evidence for a
    /// distributed read point: it has only node-local transaction semantics.
    /// Backends must return [`ReadAtCapability::Unavailable`] unless they can
    /// retain and prove the requested epoch, schema, and index cut.
    fn read_at_capability(&self) -> ReadAtCapability {
        ReadAtCapability::unavailable("backend does not prove retained cluster read points")
    }

    /// Opens a read-only snapshot at a previously fenced cluster read point.
    ///
    /// The safe default never substitutes a local transaction for `point`.
    /// A backend that advertises [`ReadAtCapability::Available`] must override
    /// this method and validate retention before returning a transaction.
    fn begin_read_at(&self, point: &ReadAtPoint) -> ReadAtResult<Self::Transaction<'_>> {
        Err(self
            .read_at_capability()
            .unavailable_error(point, "backend did not implement begin_read_at"))
    }

    /// Returns a point-in-time runtime statistics snapshot, when supported.
    fn runtime_stats(&self) -> Option<RuntimeStats> {
        None
    }

    /// Sets the memory limit in bytes, when supported.
    fn set_memory_limit_bytes(&self, _limit: Option<usize>) -> Result<()> {
        Ok(())
    }

    /// Sets the cache capacity in bytes, when supported.
    fn set_cache_capacity_bytes(&self, _capacity: usize) -> Result<()> {
        Ok(())
    }

    /// Clears the cache and returns the number of bytes removed.
    fn clear_cache(&self) -> Result<usize> {
        Ok(0)
    }
}
