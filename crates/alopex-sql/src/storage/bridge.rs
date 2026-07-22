use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use alopex_core::kv::{
    KVStore, KVTransaction, RangeChangePayload, RangeChangeRecord, ReadAtPoint, ReadAtResult,
    stage_range_change,
};
use alopex_core::types::{Key, TxnMode, Value};
use alopex_core::vector::hnsw::{HnswIndex, HnswTransactionState};
use alopex_core::{CanonicalRowKey, Error as CoreError, Result as CoreResult};
use sha2::{Digest, Sha256};

use crate::catalog::CatalogOverlay;
use crate::catalog::TableMetadata;

use super::error::Result;
use super::{IndexStorage, TableStorage};

const SQL_DATA_START: &[u8] = &[0x01];
const SQL_DATA_END: &[u8] = &[0x03];
const JOURNAL_EPOCH_PREFIX: &[u8] = b"\x00alopex/range-change/epoch/";

/// Scope used to turn local SQL row and secondary-index changes into one
/// ordered range-change record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeChangeJournalScope {
    range_id: String,
    generation: u64,
    index_tables: BTreeMap<u32, u32>,
}

impl RangeChangeJournalScope {
    /// Creates a journal scope with the committed range identity and the
    /// index-to-primary-table mapping used to reconstruct canonical row keys.
    pub fn new(
        range_id: impl Into<String>,
        generation: u64,
        index_tables: BTreeMap<u32, u32>,
    ) -> Self {
        Self {
            range_id: range_id.into(),
            generation,
            index_tables,
        }
    }

    /// Returns the local-only scope used by embedded single-node SQL commits.
    pub fn local(index_tables: BTreeMap<u32, u32>) -> Self {
        Self::new("local-default", 1, index_tables)
    }
}

/// Captures the pre-write SQL keyspace for one local transaction. Calling
/// [`Self::stage`] before the transaction commits records the exact row/index
/// delta in that same durable boundary.
#[derive(Debug, Clone)]
pub struct LocalRangeChangeJournal {
    scope: RangeChangeJournalScope,
    before: BTreeMap<Key, Value>,
}

impl LocalRangeChangeJournal {
    /// Captures the row and B-tree secondary-index state visible at transaction
    /// start. Catalog keys, sequence keys, and journal metadata are excluded.
    pub fn capture<'txn, T: KVTransaction<'txn>>(
        transaction: &mut T,
        scope: RangeChangeJournalScope,
    ) -> CoreResult<Self> {
        Ok(Self {
            scope,
            before: sql_data_snapshot(transaction)?,
        })
    }

    /// Stages one immutable range-change record when SQL row/index state
    /// changed. No epoch/outbox entry is written when the final state matches
    /// the captured state, which makes an already-applied replay a no-op.
    pub fn stage<'txn, T: KVTransaction<'txn>>(
        self,
        transaction: &mut T,
    ) -> CoreResult<Option<RangeChangeRecord>> {
        let after = sql_data_snapshot(transaction)?;
        let payload = self.payload(&after)?;
        if payload.is_empty() {
            return Ok(None);
        }

        let epoch_key = journal_epoch_key(&self.scope.range_id);
        let predecessor_epoch = transaction
            .get(&epoch_key)?
            .map(|value| decode_epoch(&value))
            .transpose()?;
        let epoch = predecessor_epoch
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| CoreError::InvalidFormat("range-change epoch overflow".to_string()))?;
        let record = RangeChangeRecord {
            range_id: self.scope.range_id.clone(),
            generation: self.scope.generation,
            epoch,
            predecessor_epoch,
            replay_id: replay_id(&payload)?,
            payload,
        };

        stage_range_change(transaction, &record)?;
        transaction.put(epoch_key, epoch.to_be_bytes().to_vec())?;
        Ok(Some(record))
    }

    fn payload(&self, after: &BTreeMap<Key, Value>) -> CoreResult<Vec<RangeChangePayload>> {
        let keys: BTreeSet<&Key> = self.before.keys().chain(after.keys()).collect();
        let mut payload = Vec::new();
        for key in keys {
            let before = self.before.get(key);
            let after = after.get(key);
            if before == after {
                continue;
            }
            match key.first().copied() {
                Some(0x01) => {
                    CanonicalRowKey::decode(key).map_err(|error| {
                        CoreError::InvalidFormat(format!("invalid SQL row key in journal: {error}"))
                    })?;
                    match after {
                        Some(encoded_row) => payload.push(RangeChangePayload::UpsertRow {
                            row_key: key.clone(),
                            encoded_row: encoded_row.clone(),
                        }),
                        None => payload.push(RangeChangePayload::DeleteRow {
                            row_key: key.clone(),
                            tombstone: before.cloned().unwrap_or_default(),
                        }),
                    }
                }
                Some(0x02) => {
                    let Some((index_id, row_key)) = self.index_reference(key)? else {
                        continue;
                    };
                    match after {
                        Some(_) => payload.push(RangeChangePayload::UpsertIndex {
                            index_id,
                            index_key: key.clone(),
                            row_key,
                        }),
                        None => payload.push(RangeChangePayload::DeleteIndex {
                            index_id,
                            index_key: key.clone(),
                            row_key,
                        }),
                    }
                }
                _ => unreachable!("SQL data snapshot includes only row/index keyspaces"),
            }
        }
        Ok(payload)
    }

    fn index_reference(&self, key: &Key) -> CoreResult<Option<(u32, Key)>> {
        if key.len() < 13 {
            return Err(CoreError::InvalidFormat(
                "invalid SQL secondary-index key in journal".to_string(),
            ));
        }
        let index_id = u32::from_be_bytes(key[1..5].try_into().expect("four bytes"));
        let Some(table_id) = self.scope.index_tables.get(&index_id).copied() else {
            // A newly-created index belongs to schema/catalog work. Its
            // definition is not yet a committed data-plane contract, so do
            // not emit an unverifiable index payload from this local bridge.
            return Ok(None);
        };
        let row_id = u64::from_be_bytes(key[key.len() - 8..].try_into().expect("eight bytes"));
        Ok(Some((
            index_id,
            CanonicalRowKey::new(table_id, row_id).encode(),
        )))
    }
}

fn sql_data_snapshot<'txn, T: KVTransaction<'txn>>(
    transaction: &mut T,
) -> CoreResult<BTreeMap<Key, Value>> {
    Ok(transaction
        .scan_range(SQL_DATA_START, SQL_DATA_END)?
        .collect::<BTreeMap<_, _>>())
}

fn journal_epoch_key(range_id: &str) -> Key {
    let mut key = JOURNAL_EPOCH_PREFIX.to_vec();
    key.extend_from_slice(range_id.as_bytes());
    key
}

fn decode_epoch(value: &Value) -> CoreResult<u64> {
    let bytes: [u8; 8] = value
        .as_slice()
        .try_into()
        .map_err(|_| CoreError::InvalidFormat("invalid range-change epoch state".to_string()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn replay_id(payload: &[RangeChangePayload]) -> CoreResult<String> {
    let encoded = bincode::serialize(payload).map_err(|error| {
        CoreError::InvalidFormat(format!(
            "cannot encode range-change replay identity: {error}"
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub trait SqlTxn<'txn, S: KVStore + 'txn> {
    fn mode(&self) -> TxnMode;

    fn ensure_write_txn(&self) -> CoreResult<()>;

    fn inner_mut(&mut self) -> &mut S::Transaction<'txn>;

    fn hnsw_entry(&mut self, name: &str) -> CoreResult<&HnswIndex>;

    fn hnsw_entry_mut(&mut self, name: &str) -> CoreResult<&mut HnswTxnEntry>;

    fn flush_hnsw(&mut self) -> Result<()>;

    fn abandon_hnsw(&mut self) -> Result<()>;

    fn delete_prefix(&mut self, prefix: &[u8]) -> Result<()>;

    fn table_storage<'a>(
        &'a mut self,
        table_meta: &TableMetadata,
    ) -> TableStorage<'a, 'txn, S::Transaction<'txn>> {
        TableStorage::new(self.inner_mut(), table_meta)
    }

    fn index_storage<'a>(
        &'a mut self,
        index_id: u32,
        unique: bool,
        column_indices: Vec<usize>,
    ) -> IndexStorage<'a, 'txn, S::Transaction<'txn>> {
        IndexStorage::new(self.inner_mut(), index_id, unique, column_indices)
    }

    fn with_table<R, F>(&mut self, table_meta: &TableMetadata, f: F) -> Result<R>
    where
        F: FnOnce(&mut TableStorage<'_, 'txn, S::Transaction<'txn>>) -> Result<R>,
    {
        let mut storage = self.table_storage(table_meta);
        f(&mut storage)
    }

    fn with_index<R, F>(
        &mut self,
        index_id: u32,
        unique: bool,
        column_indices: Vec<usize>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut IndexStorage<'_, 'txn, S::Transaction<'txn>>) -> Result<R>,
    {
        let mut storage = self.index_storage(index_id, unique, column_indices);
        f(&mut storage)
    }
}

/// TxnBridge provides SQL-friendly transaction handles on top of a KVStore.
pub struct TxnBridge<S: KVStore> {
    store: Arc<S>,
}

impl<S: KVStore> TxnBridge<S> {
    /// Create a new bridge for the given store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Returns the underlying store's runtime statistics.
    pub fn runtime_stats(&self) -> Option<alopex_core::kv::RuntimeStats> {
        self.store.runtime_stats()
    }

    /// Applies a memory-limit setting to the underlying store.
    pub fn set_memory_limit_bytes(&self, limit: Option<usize>) -> CoreResult<()> {
        self.store.set_memory_limit_bytes(limit)
    }

    /// Applies a cache-capacity setting to the underlying store.
    pub fn set_cache_capacity_bytes(&self, capacity: usize) -> CoreResult<()> {
        self.store.set_cache_capacity_bytes(capacity)
    }

    /// Clears the underlying cache.
    pub fn clear_cache(&self) -> CoreResult<usize> {
        self.store.clear_cache()
    }

    /// Begin a read-only transaction.
    pub fn begin_read(&self) -> Result<SqlTransaction<'_, S>> {
        let txn = self.store.begin(TxnMode::ReadOnly)?;
        Ok(SqlTransaction {
            inner: txn,
            mode: TxnMode::ReadOnly,
            read_at: None,
            hnsw_indices: HashMap::new(),
        })
    }

    /// Begin a read-write transaction.
    pub fn begin_write(&self) -> Result<SqlTransaction<'_, S>> {
        let txn = self.store.begin(TxnMode::ReadWrite)?;
        Ok(SqlTransaction {
            inner: txn,
            mode: TxnMode::ReadWrite,
            read_at: None,
            hnsw_indices: HashMap::new(),
        })
    }

    /// Opens a read-only SQL transaction bound to a cluster-issued read point.
    ///
    /// No normal local transaction is substituted when the backend cannot
    /// prove the requested retained snapshot.
    pub fn begin_read_at(&self, point: ReadAtPoint) -> ReadAtResult<SqlTransaction<'_, S>> {
        let txn = self.store.begin_read_at(&point)?;
        Ok(Self::from_read_at(txn, point))
    }

    /// Wraps an already opened read-at transaction and retains its full fence.
    pub fn from_read_at<'a>(txn: S::Transaction<'a>, point: ReadAtPoint) -> SqlTransaction<'a, S> {
        SqlTransaction {
            inner: txn,
            mode: TxnMode::ReadOnly,
            read_at: Some(point),
            hnsw_indices: HashMap::new(),
        }
    }

    pub fn wrap_external<'a, 'b, 'c>(
        txn: &'a mut S::Transaction<'b>,
        mode: TxnMode,
        overlay: &'c mut CatalogOverlay,
    ) -> BorrowedSqlTransaction<'a, 'b, 'c, S> {
        BorrowedSqlTransaction {
            inner: txn,
            mode,
            overlay,
            hnsw_indices: HashMap::new(),
        }
    }

    /// Execute a read-only transaction with automatic commit.
    ///
    /// The closure receives a `SqlTransaction` that provides access to table and index storage.
    /// The transaction is automatically committed on success or rolled back on error.
    pub fn with_read_txn<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut SqlTransaction<'_, S>) -> Result<R>,
    {
        let mut txn = self.begin_read()?;
        let result = f(&mut txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// Execute a read-write transaction with automatic commit.
    ///
    /// The closure receives a `SqlTransaction` that provides access to table and index storage.
    /// The transaction is automatically committed on success or rolled back on error.
    pub fn with_write_txn<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut SqlTransaction<'_, S>) -> Result<R>,
    {
        let mut txn = self.begin_write()?;
        let result = f(&mut txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// Execute a read-write transaction with explicit commit control.
    ///
    /// Returns `(result, should_commit)` from the closure. If `should_commit` is true,
    /// the transaction is committed; otherwise it is rolled back.
    pub fn with_write_txn_explicit<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut SqlTransaction<'_, S>) -> Result<(R, bool)>,
    {
        let mut txn = self.begin_write()?;
        let (result, should_commit) = f(&mut txn)?;
        if should_commit {
            txn.commit()?;
        } else {
            txn.rollback()?;
        }
        Ok(result)
    }
}

/// SQL transaction wrapping a KV transaction.
///
/// Provides SQL-friendly access to table and index storage,
/// with explicit commit/rollback control.
pub struct SqlTransaction<'a, S: KVStore + 'a> {
    inner: S::Transaction<'a>,
    mode: TxnMode,
    read_at: Option<ReadAtPoint>,
    hnsw_indices: HashMap<String, HnswTxnEntry>,
}

pub struct HnswTxnEntry {
    pub index: HnswIndex,
    pub state: HnswTransactionState,
    pub dirty: bool,
}

impl<'a, S: KVStore + 'a> SqlTransaction<'a, S> {
    /// Returns the transaction mode.
    pub fn mode(&self) -> TxnMode {
        self.mode
    }

    /// Returns the fenced read point held by this transaction, if it was
    /// opened by [`TxnBridge::begin_read_at`].
    pub fn read_at_point(&self) -> Option<ReadAtPoint> {
        self.read_at
    }

    /// Get a table storage handle.
    ///
    /// Returns a TableStorage instance that borrows this transaction.
    /// The returned storage is valid for the duration of the borrow.
    ///
    /// The `table_id` is obtained from `table_meta.table_id`.
    pub fn table_storage<'b>(
        &'b mut self,
        table_meta: &TableMetadata,
    ) -> TableStorage<'b, 'a, S::Transaction<'a>> {
        TableStorage::new(&mut self.inner, table_meta)
    }

    /// Get an index storage handle.
    ///
    /// Returns an IndexStorage instance that borrows this transaction.
    /// The returned storage is valid for the duration of the borrow.
    pub fn index_storage<'b>(
        &'b mut self,
        index_id: u32,
        unique: bool,
        column_indices: Vec<usize>,
    ) -> IndexStorage<'b, 'a, S::Transaction<'a>> {
        IndexStorage::new(&mut self.inner, index_id, unique, column_indices)
    }

    /// HNSW インデックスのキャッシュエントリを取得する（存在しなければロードする）。
    #[allow(dead_code)]
    pub(crate) fn hnsw_entry(&mut self, name: &str) -> CoreResult<&HnswIndex> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, &mut self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(&self.hnsw_indices.get(name).expect("inserted above").index)
    }

    /// HNSW インデックスのキャッシュエントリを可変で取得する（存在しなければロードする）。
    pub(crate) fn hnsw_entry_mut(&mut self, name: &str) -> CoreResult<&mut HnswTxnEntry> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, &mut self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(self.hnsw_indices.get_mut(name).expect("inserted above"))
    }

    pub(crate) fn ensure_write_txn(&self) -> CoreResult<()> {
        if self.mode != TxnMode::ReadWrite {
            return Err(CoreError::TxnConflict);
        }
        Ok(())
    }

    /// 内部の KV トランザクションへの可変参照を返す（HNSW ブリッジ向け）。
    pub(crate) fn inner_mut(&mut self) -> &mut S::Transaction<'a> {
        &mut self.inner
    }

    /// Delete all keys with the given prefix.
    pub fn delete_prefix(&mut self, prefix: &[u8]) -> Result<()> {
        // Process in small batches to avoid unbounded memory use.
        const BATCH: usize = 512;
        loop {
            let mut keys = Vec::with_capacity(BATCH);
            {
                let iter = self.inner.scan_prefix(prefix)?;
                for (key, _) in iter.take(BATCH) {
                    keys.push(key);
                }
            }

            if keys.is_empty() {
                break;
            }

            for key in keys {
                self.inner.delete(key)?;
            }
        }

        Ok(())
    }

    /// Execute operations on a table within a closure.
    ///
    /// This is a convenience method that creates a TableStorage,
    /// executes the closure, and returns the result.
    ///
    /// The `table_id` is obtained from `table_meta.table_id`.
    pub fn with_table<R, F>(&mut self, table_meta: &TableMetadata, f: F) -> Result<R>
    where
        F: FnOnce(&mut TableStorage<'_, 'a, S::Transaction<'a>>) -> Result<R>,
    {
        let mut storage = self.table_storage(table_meta);
        f(&mut storage)
    }

    /// Execute operations on an index within a closure.
    ///
    /// This is a convenience method that creates an IndexStorage,
    /// executes the closure, and returns the result.
    pub fn with_index<R, F>(
        &mut self,
        index_id: u32,
        unique: bool,
        column_indices: Vec<usize>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut IndexStorage<'_, 'a, S::Transaction<'a>>) -> Result<R>,
    {
        let mut storage = self.index_storage(index_id, unique, column_indices);
        f(&mut storage)
    }

    /// Commit the transaction.
    ///
    /// Consumes the transaction. On success, all writes become visible.
    pub fn commit(mut self) -> Result<()> {
        self.commit_hnsw()?;
        self.inner.commit_self()?;
        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// Consumes the transaction. All pending writes are discarded.
    pub fn rollback(mut self) -> Result<()> {
        self.rollback_hnsw()?;
        self.inner.rollback_self()?;
        Ok(())
    }

    fn commit_hnsw(&mut self) -> Result<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry
                    .index
                    .commit_staged(&mut self.inner, &mut entry.state)?;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }

    fn rollback_hnsw(&mut self) -> Result<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry.index.rollback(&mut entry.state)?;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }
}

impl<'a, S: KVStore + 'a> SqlTxn<'a, S> for SqlTransaction<'a, S> {
    fn mode(&self) -> TxnMode {
        self.mode()
    }

    fn ensure_write_txn(&self) -> CoreResult<()> {
        self.ensure_write_txn()
    }

    fn inner_mut(&mut self) -> &mut S::Transaction<'a> {
        self.inner_mut()
    }

    fn hnsw_entry(&mut self, name: &str) -> CoreResult<&HnswIndex> {
        self.hnsw_entry(name)
    }

    fn hnsw_entry_mut(&mut self, name: &str) -> CoreResult<&mut HnswTxnEntry> {
        self.hnsw_entry_mut(name)
    }

    fn flush_hnsw(&mut self) -> Result<()> {
        self.commit_hnsw()
    }

    fn abandon_hnsw(&mut self) -> Result<()> {
        self.rollback_hnsw()
    }

    fn delete_prefix(&mut self, prefix: &[u8]) -> Result<()> {
        self.delete_prefix(prefix)
    }
}

pub struct BorrowedSqlTransaction<'a, 'b, 'c, S: KVStore + 'b> {
    inner: &'a mut S::Transaction<'b>,
    mode: TxnMode,
    overlay: &'c mut CatalogOverlay,
    hnsw_indices: HashMap<String, HnswTxnEntry>,
}

impl<'a, 'b, 'c, S: KVStore + 'b> BorrowedSqlTransaction<'a, 'b, 'c, S> {
    pub fn mode(&self) -> TxnMode {
        self.mode
    }

    pub fn split_parts(&mut self) -> (BorrowedSqlTxn<'_, 'b, S>, &mut CatalogOverlay) {
        (
            BorrowedSqlTxn {
                inner: self.inner,
                mode: self.mode,
                hnsw_indices: &mut self.hnsw_indices,
            },
            self.overlay,
        )
    }
}

impl<'a, 'b, 'c, S: KVStore + 'b> Drop for BorrowedSqlTransaction<'a, 'b, 'c, S> {
    fn drop(&mut self) {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                let _ = entry.index.rollback(&mut entry.state);
                entry.dirty = false;
            }
        }
        self.hnsw_indices.clear();
    }
}

pub struct BorrowedSqlTxn<'a, 'b, S: KVStore + 'b> {
    inner: &'a mut S::Transaction<'b>,
    mode: TxnMode,
    hnsw_indices: &'a mut HashMap<String, HnswTxnEntry>,
}

impl<'a, 'b, S: KVStore + 'b> SqlTxn<'b, S> for BorrowedSqlTxn<'a, 'b, S> {
    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn ensure_write_txn(&self) -> CoreResult<()> {
        if self.mode != TxnMode::ReadWrite {
            return Err(CoreError::TxnReadOnly);
        }
        Ok(())
    }

    fn inner_mut(&mut self) -> &mut S::Transaction<'b> {
        self.inner
    }

    fn hnsw_entry(&mut self, name: &str) -> CoreResult<&HnswIndex> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(&self.hnsw_indices.get(name).expect("inserted above").index)
    }

    fn hnsw_entry_mut(&mut self, name: &str) -> CoreResult<&mut HnswTxnEntry> {
        if !self.hnsw_indices.contains_key(name) {
            let index = HnswIndex::load(name, self.inner)?;
            self.hnsw_indices.insert(
                name.to_string(),
                HnswTxnEntry {
                    index,
                    state: HnswTransactionState::default(),
                    dirty: false,
                },
            );
        }
        Ok(self.hnsw_indices.get_mut(name).expect("inserted above"))
    }

    fn flush_hnsw(&mut self) -> Result<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry.index.commit_staged(self.inner, &mut entry.state)?;
                entry.dirty = false;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }

    fn abandon_hnsw(&mut self) -> Result<()> {
        for entry in self.hnsw_indices.values_mut() {
            if entry.dirty {
                entry.index.rollback(&mut entry.state)?;
                entry.dirty = false;
            }
        }
        self.hnsw_indices.clear();
        Ok(())
    }

    fn delete_prefix(&mut self, prefix: &[u8]) -> Result<()> {
        // Process in small batches to avoid unbounded memory use.
        const BATCH: usize = 512;
        loop {
            let mut keys = Vec::with_capacity(BATCH);
            {
                let iter = self.inner.scan_prefix(prefix)?;
                for (key, _) in iter.take(BATCH) {
                    keys.push(key);
                }
            }

            if keys.is_empty() {
                break;
            }

            for key in keys {
                self.inner.delete(key)?;
            }
        }

        Ok(())
    }
}

/// Backwards-compatible alias for SqlTransaction.
pub type TxnContext<'a, S> = SqlTransaction<'a, S>;

#[cfg(test)]
mod tests {
    use super::super::SqlValue;
    use super::*;
    use crate::catalog::ColumnMetadata;
    use crate::planner::types::ResolvedType;
    use crate::storage::KeyEncoder;
    use alopex_core::kv::memory::MemoryKV;
    use alopex_core::kv::{KVStore, decode_range_change, journal_key};
    use alopex_core::types::TxnMode;
    use std::sync::Arc;

    fn sample_table_meta() -> TableMetadata {
        TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true)
                    .with_not_null(true),
                ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ],
        )
        .with_table_id(1)
    }

    #[test]
    fn read_txn_mode_is_readonly() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store);

        bridge
            .with_read_txn(|ctx| {
                assert_eq!(ctx.mode(), TxnMode::ReadOnly);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn read_at_never_substitutes_a_normal_local_read_transaction() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let point = ReadAtPoint::new(7, 11, 13, 17);

        assert!(matches!(
            bridge.begin_read_at(point),
            Err(alopex_core::ReadAtError::Unavailable { .. })
        ));
    }

    #[test]
    fn write_txn_mode_is_readwrite() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store);

        bridge
            .with_write_txn(|ctx| {
                assert_eq!(ctx.mode(), TxnMode::ReadWrite);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn commit_persists_changes_and_read_sees_them() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store.clone());
        let meta = sample_table_meta();

        // Write transaction
        bridge
            .with_write_txn(|ctx| {
                ctx.with_table(&meta, |table| {
                    table.insert(1, &[SqlValue::Integer(1), SqlValue::Text("alice".into())])
                })
            })
            .unwrap();

        // Read transaction
        let row = bridge
            .with_read_txn(|ctx| ctx.with_table(&meta, |table| table.get(1)))
            .unwrap()
            .unwrap();

        assert_eq!(row[1], SqlValue::Text("alice".into()));
    }

    #[test]
    fn rollback_discards_uncommitted_writes() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store.clone());
        let meta = sample_table_meta();

        // Write transaction with explicit rollback
        bridge
            .with_write_txn_explicit(|ctx| {
                ctx.with_table(&meta, |table| {
                    table.insert(1, &[SqlValue::Integer(1), SqlValue::Text("bob".into())])
                })?;
                Ok(((), false)) // false = rollback
            })
            .unwrap();

        // Read transaction should not see the row
        let row = bridge
            .with_read_txn(|ctx| ctx.with_table(&meta, |table| table.get(1)))
            .unwrap();

        assert!(row.is_none());
    }

    #[test]
    fn conflicting_commits_trigger_transaction_conflict() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store);
        let meta = sample_table_meta();

        // Start first write transaction
        let mut txn1 = bridge.begin_write().unwrap();
        {
            let mut table = txn1.table_storage(&meta);
            table
                .insert(1, &[SqlValue::Integer(1), SqlValue::Text("alice".into())])
                .unwrap();
        }

        // Start second write transaction
        let mut txn2 = bridge.begin_write().unwrap();
        {
            let mut table = txn2.table_storage(&meta);
            table
                .insert(1, &[SqlValue::Integer(1), SqlValue::Text("bob".into())])
                .unwrap();
        }

        // Commit first, second should conflict
        txn1.commit().unwrap();
        let err = txn2.commit().unwrap_err();
        assert!(matches!(
            err,
            super::super::StorageError::TransactionConflict
        ));
    }

    #[test]
    fn scan_rows_in_transaction() {
        let store = Arc::new(MemoryKV::new());
        let bridge = TxnBridge::new(store.clone());
        let meta = sample_table_meta();

        // Insert multiple rows
        bridge
            .with_write_txn(|ctx| {
                ctx.with_table(&meta, |table| {
                    for i in 1..=3 {
                        table.insert(
                            i,
                            &[
                                SqlValue::Integer(i as i32),
                                SqlValue::Text(format!("user{i}")),
                            ],
                        )?;
                    }
                    Ok(())
                })
            })
            .unwrap();

        // Scan all rows
        let rows: Vec<u64> = bridge
            .with_read_txn(|ctx| {
                ctx.with_table(&meta, |table| {
                    let iter = table.scan()?;
                    let ids: Vec<u64> = iter.filter_map(|r| r.ok().map(|(id, _)| id)).collect();
                    Ok(ids)
                })
            })
            .unwrap();

        assert_eq!(rows, vec![1, 2, 3]);
    }

    #[test]
    fn local_journal_stages_row_and_index_payloads_in_one_commit() {
        let store = MemoryKV::new();
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        let journal = LocalRangeChangeJournal::capture(
            &mut transaction,
            RangeChangeJournalScope::local([(7, 1)].into()),
        )
        .unwrap();
        let row_key = KeyEncoder::row_key(1, 9);
        let index_key = KeyEncoder::index_key(7, &SqlValue::Text("alice".into()), 9).unwrap();
        transaction
            .put(row_key.clone(), b"encoded-row".to_vec())
            .unwrap();
        transaction.put(index_key.clone(), Vec::new()).unwrap();

        let record = journal.stage(&mut transaction).unwrap().unwrap();
        let outbox_key = journal_key(&record);
        transaction.commit_self().unwrap();

        let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(reader.get(&row_key).unwrap(), Some(b"encoded-row".to_vec()));
        assert_eq!(
            decode_range_change(&reader.get(&outbox_key).unwrap().unwrap()).unwrap(),
            record
        );
        assert_eq!(
            record.payload,
            vec![
                RangeChangePayload::UpsertRow {
                    row_key: row_key.clone(),
                    encoded_row: b"encoded-row".to_vec(),
                },
                RangeChangePayload::UpsertIndex {
                    index_id: 7,
                    index_key,
                    row_key,
                },
            ]
        );
    }

    #[test]
    fn failed_journalled_commit_publishes_neither_data_nor_outbox() {
        let store = MemoryKV::new_with_limit(Some(0));
        let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
        let journal = LocalRangeChangeJournal::capture(
            &mut transaction,
            RangeChangeJournalScope::local(Default::default()),
        )
        .unwrap();
        let row_key = KeyEncoder::row_key(1, 1);
        transaction.put(row_key.clone(), b"row".to_vec()).unwrap();
        let record = journal.stage(&mut transaction).unwrap().unwrap();
        let outbox_key = journal_key(&record);
        assert!(transaction.commit_self().is_err());

        let mut reader = store.begin(TxnMode::ReadOnly).unwrap();
        assert!(reader.get(&row_key).unwrap().is_none());
        assert!(reader.get(&outbox_key).unwrap().is_none());
    }

    #[test]
    fn local_journal_records_tombstones_and_a_contiguous_epoch() {
        let store = MemoryKV::new();
        let row_key = KeyEncoder::row_key(1, 9);
        let index_key = KeyEncoder::index_key(7, &SqlValue::Text("alice".into()), 9).unwrap();
        let scope = RangeChangeJournalScope::local([(7, 1)].into());

        let first = {
            let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
            let journal =
                LocalRangeChangeJournal::capture(&mut transaction, scope.clone()).unwrap();
            transaction
                .put(row_key.clone(), b"encoded-row".to_vec())
                .unwrap();
            transaction.put(index_key.clone(), Vec::new()).unwrap();
            let record = journal.stage(&mut transaction).unwrap().unwrap();
            transaction.commit_self().unwrap();
            record
        };
        let second = {
            let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
            let journal = LocalRangeChangeJournal::capture(&mut transaction, scope).unwrap();
            transaction.delete(row_key.clone()).unwrap();
            transaction.delete(index_key.clone()).unwrap();
            let record = journal.stage(&mut transaction).unwrap().unwrap();
            transaction.commit_self().unwrap();
            record
        };

        assert_eq!(first.epoch, 1);
        assert_eq!(first.predecessor_epoch, None);
        assert_eq!(second.epoch, 2);
        assert_eq!(second.predecessor_epoch, Some(1));
        assert_eq!(
            second.payload,
            vec![
                RangeChangePayload::DeleteRow {
                    row_key: row_key.clone(),
                    tombstone: b"encoded-row".to_vec(),
                },
                RangeChangePayload::DeleteIndex {
                    index_id: 7,
                    index_key,
                    row_key,
                },
            ]
        );
    }

    #[test]
    fn replay_without_a_new_sql_delta_emits_no_second_record() {
        let store = MemoryKV::new();
        let row_key = KeyEncoder::row_key(1, 1);
        {
            let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
            let journal = LocalRangeChangeJournal::capture(
                &mut transaction,
                RangeChangeJournalScope::local(Default::default()),
            )
            .unwrap();
            transaction.put(row_key.clone(), b"row".to_vec()).unwrap();
            assert!(journal.stage(&mut transaction).unwrap().is_some());
            transaction.commit_self().unwrap();
        }
        let mut replay = store.begin(TxnMode::ReadWrite).unwrap();
        let journal = LocalRangeChangeJournal::capture(
            &mut replay,
            RangeChangeJournalScope::local(Default::default()),
        )
        .unwrap();
        assert!(journal.stage(&mut replay).unwrap().is_none());
        replay.commit_self().unwrap();
    }
}
