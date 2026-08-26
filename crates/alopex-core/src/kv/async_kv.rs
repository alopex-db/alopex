//! Async key-value store traits.

use crate::async_runtime::{BoxFuture, BoxStream, MaybeSend};
use crate::error::Result;
use crate::kv::{KeySearchCancellation, KeySearchPage, KeySearchRequest};
use crate::types::{Key, Value};

/// Async version of [`KVStore`].
pub trait AsyncKVStore: MaybeSend {
    /// The async transaction type for this store.
    type Transaction<'a>: AsyncKVTransaction<'a>
    where
        Self: 'a;

    /// Begins an async transaction.
    fn begin_async<'a>(&'a self) -> BoxFuture<'a, Result<Self::Transaction<'a>>>;
}

/// Async version of [`KVTransaction`].
pub trait AsyncKVTransaction<'txn>: MaybeSend {
    /// Retrieves the value for a given key.
    fn async_get<'a>(&'a self, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Value>>>;

    /// Sets a value for a given key.
    fn async_put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFuture<'a, Result<()>>;

    /// Deletes a key-value pair.
    fn async_delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, Result<()>>;

    /// Scans all key-value pairs whose keys start with the given prefix.
    fn async_scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> BoxStream<'a, Result<(Key, Value)>>;

    /// Searches opaque key bytes with bounded pagination.
    fn async_search_keys<'a>(
        &'a self,
        request: KeySearchRequest,
    ) -> BoxFuture<'a, Result<KeySearchPage>>;

    /// Searches key bytes while observing a cooperative cancellation token.
    fn async_search_keys_with_cancellation<'a>(
        &'a self,
        request: KeySearchRequest,
        cancellation: KeySearchCancellation,
    ) -> BoxFuture<'a, Result<KeySearchPage>>;

    /// Commits the transaction, applying all buffered writes.
    fn async_commit(self) -> BoxFuture<'txn, Result<()>>;

    /// Rolls back the transaction, discarding all buffered writes.
    fn async_rollback(self) -> BoxFuture<'txn, Result<()>>;
}
