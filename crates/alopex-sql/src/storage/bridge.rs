use std::collections::HashMap;
use std::sync::Arc;

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::{HnswIndex, HnswTransactionState};
use alopex_core::{Error as CoreError, Result as CoreResult};

use crate::catalog::CatalogOverlay;
use crate::catalog::TableMetadata;

use super::error::Result;
use super::{IndexStorage, TableStorage};

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

    /// Begin a read-only transaction.
    pub fn begin_read(&self) -> Result<SqlTransaction<'_, S>> {
        let txn = self.store.begin(TxnMode::ReadOnly)?;
        Ok(SqlTransaction {
            inner: txn,
            mode: TxnMode::ReadOnly,
            hnsw_indices: HashMap::new(),
        })
    }

    /// Begin a read-write transaction.
    pub fn begin_write(&self) -> Result<SqlTransaction<'_, S>> {
        let txn = self.store.begin(TxnMode::ReadWrite)?;
        Ok(SqlTransaction {
            inner: txn,
            mode: TxnMode::ReadWrite,
            hnsw_indices: HashMap::new(),
        })
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
    use alopex_core::kv::memory::MemoryKV;
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
}
