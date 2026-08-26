//! 複数の KV 実装（MemoryKV / LsmKV / S3KV）を 1 つの型にまとめるためのラッパー。
//!
//! `KVStore` / `KVTransaction` は GAT を含むため `dyn KVStore` としてオブジェクト安全に扱いづらい。
//! そのため、列挙型で具体型をまとめて扱う。

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::kv::memory::{MemoryKV, MemoryTransaction, MemoryTxnManager};
use crate::kv::{
    KVStore, KVTransaction, OwnedKVScan, OwnedKVStore, OwnedKVTransaction,
    OwnedKVTransactionAdapter, ReadAtCapability, ReadAtPoint, ReadAtResult,
};
use crate::lsm::sstable::{SSTableCursor, SSTableEntry};
use crate::lsm::{
    LsmKV, LsmTransaction, LsmTxnManagerRef, OwnedLsmScanBounds, OwnedLsmSnapshotReader,
};
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, TxnState, Value};

#[cfg(feature = "s3")]
use crate::kv::s3::S3KV;

/// ディスク/インメモリ/S3 いずれの KV も扱えるラッパー。
pub enum AnyKV {
    /// 既存のインメモリ KV（永続化パスも持つ）。
    Memory(MemoryKV),
    /// LSM-Tree（file-mode）。
    Lsm(Box<LsmKV>),
    /// S3-backed storage (requires `s3` feature).
    #[cfg(feature = "s3")]
    S3(Box<S3KV>),
}

impl AnyKV {
    /// Returns the local directory that contains persisted file-format metadata.
    ///
    /// Keeping this match in `alopex-core` makes it exhaustive under this
    /// crate's own feature set. Downstream crates cannot reliably match on the
    /// optional `S3` variant because Cargo may enable `alopex-core/s3` through
    /// feature unification without enabling their corresponding local feature.
    pub fn file_format_storage_dir(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) => None,
            Self::Lsm(kv) => Some(&kv.data_dir),
            #[cfg(feature = "s3")]
            Self::S3(kv) => Some(kv.cache_dir()),
        }
    }

    /// MemTable をフラッシュする。
    pub fn flush(&self) -> Result<()> {
        match self {
            Self::Memory(kv) => kv.flush(),
            Self::Lsm(kv) => kv.flush(),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.flush(),
        }
    }

    /// 単一 `.alopex` ファイルへ収束する（対象外のバックエンドでは no-op）。
    pub fn converge(&self) -> Result<()> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Lsm(kv) => kv.converge().map(|_| ()),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.inner().converge().map(|_| ()),
        }
    }

    /// 収束したうえでストアを閉じる（冪等）。
    pub fn close(&self) -> Result<()> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Lsm(kv) => kv.close(),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.inner().close(),
        }
    }

    /// 収束先の `.alopex` コンテナパス（対象外なら `None`）。
    pub fn container_path(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) => None,
            Self::Lsm(kv) => kv.container_path(),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.inner().container_path(),
        }
    }

    /// Reports whether this backend supports durable local range-change journaling.
    pub fn range_change_journal_capability(&self) -> crate::kv::RangeChangeJournalCapability {
        match self {
            Self::Memory(_) | Self::Lsm(_) => crate::kv::RangeChangeJournalCapability::Supported,
            #[cfg(feature = "s3")]
            Self::S3(_) => crate::kv::RangeChangeJournalCapability::Unavailable,
        }
    }

    /// Reports explicit distributed read-point eligibility for the selected
    /// storage backend. Current local backends have durable journals but do
    /// not yet prove retained, cluster-wide read points, so none may advertise
    /// strong or stale remote-read capability.
    pub fn read_at_capability(&self) -> ReadAtCapability {
        match self {
            Self::Memory(_) => ReadAtCapability::unavailable(
                "memory backend has no retained cluster read-point proof",
            ),
            Self::Lsm(_) => ReadAtCapability::unavailable(
                "lsm backend has no retained cluster read-point proof",
            ),
            #[cfg(feature = "s3")]
            Self::S3(_) => {
                ReadAtCapability::unavailable("s3 backend has no retained cluster read-point proof")
            }
        }
    }
}

impl OwnedKVStore for AnyKV {
    fn begin_owned_kv_transaction(
        self: Arc<Self>,
        mode: TxnMode,
    ) -> Result<Box<dyn OwnedKVTransaction>> {
        match self.as_ref() {
            Self::Memory(kv) => Arc::new(kv.clone()).begin_owned_kv_transaction(mode),
            Self::Lsm(_) => Ok(Box::new(OwnedLsmTransaction::new(self.clone(), mode)?)),
            #[cfg(feature = "s3")]
            Self::S3(_) => Err(Error::InvalidParameter {
                param: "owned_session_backend".to_owned(),
                reason: "S3 is outside the embedded-local owned-session scope".to_owned(),
            }),
        }
    }
}

/// Owned local-disk transaction.  The surrounding `Arc<AnyKV>` keeps the concrete `LsmKV`
/// allocation alive until the transaction and every cursor derived from it have closed.
struct OwnedLsmTransaction {
    store: Arc<AnyKV>,
    state: Arc<Mutex<OwnedLsmTransactionState>>,
}

struct OwnedLsmTransactionState {
    id: TxnId,
    mode: TxnMode,
    state: TxnState,
    start_timestamp: u64,
    read_set: HashSet<Key>,
    write_set: BTreeMap<Key, Option<Value>>,
    cursor_open: bool,
}

impl OwnedLsmTransaction {
    fn new(store: Arc<AnyKV>, mode: TxnMode) -> Result<Self> {
        let lsm = Self::lsm_from(&store)?;
        let id = lsm.allocate_owned_transaction_id();
        let start_timestamp = lsm.ts_oracle.next_timestamp();
        Ok(Self {
            store,
            state: Arc::new(Mutex::new(OwnedLsmTransactionState {
                id,
                mode,
                state: TxnState::Active,
                start_timestamp,
                read_set: HashSet::new(),
                write_set: BTreeMap::new(),
                cursor_open: false,
            })),
        })
    }

    fn lsm_from(store: &Arc<AnyKV>) -> Result<&LsmKV> {
        match store.as_ref() {
            AnyKV::Lsm(lsm) => Ok(lsm.as_ref()),
            _ => Err(Error::InvalidParameter {
                param: "owned_session_backend".to_owned(),
                reason: "owned LSM transaction requires a local LSM backend".to_owned(),
            }),
        }
    }

    fn lsm(&self) -> Result<&LsmKV> {
        Self::lsm_from(&self.store)
    }

    fn ensure_active(state: &OwnedLsmTransactionState) -> Result<()> {
        if state.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        Ok(())
    }

    fn open_cursor(&mut self, bounds: OwnedLsmScanBounds) -> Result<Box<dyn OwnedKVScan>> {
        let lsm = self.lsm()?;
        let snapshot = lsm.acquire_owned_snapshot_reader();
        let read_timestamp = {
            let state = self
                .state
                .lock()
                .expect("owned LSM transaction mutex poisoned");
            Self::ensure_active(&state)?;
            if state.cursor_open {
                return Err(Error::TxnClosed);
            }
            state.start_timestamp
        };
        let tables = lsm.open_owned_sstable_cursors(&bounds, read_timestamp)?;
        let mut state = self
            .state
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        Self::ensure_active(&state)?;
        if state.cursor_open {
            return Err(Error::TxnClosed);
        }
        state.cursor_open = true;
        drop(state);
        Ok(Box::new(OwnedLsmCursor {
            store: self.store.clone(),
            transaction: self.state.clone(),
            snapshot: Some(snapshot),
            bounds,
            tables: tables.into_iter().map(OwnedLsmTableCursor::new).collect(),
            last_key: None,
        }))
    }
}

impl OwnedKVTransaction for OwnedLsmTransaction {
    fn id(&self) -> TxnId {
        self.state
            .lock()
            .expect("owned LSM transaction mutex poisoned")
            .id
    }

    fn mode(&self) -> TxnMode {
        self.state
            .lock()
            .expect("owned LSM transaction mutex poisoned")
            .mode
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        let mut state = self
            .state
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        Self::ensure_active(&state)?;
        if let Some(value) = state.write_set.get(key) {
            return Ok(value.clone());
        }
        let entry = self.lsm()?.owned_visible_at(key, state.start_timestamp)?;
        state.read_set.insert(key.clone());
        Ok(entry.and_then(|entry| entry.value))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        Self::ensure_active(&state)?;
        if state.mode == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly);
        }
        if state.cursor_open {
            return Err(Error::TxnClosed);
        }
        state.write_set.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        Self::ensure_active(&state)?;
        if state.mode == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly);
        }
        if state.cursor_open {
            return Err(Error::TxnClosed);
        }
        state.write_set.insert(key, None);
        Ok(())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
        self.open_cursor(OwnedLsmScanBounds::prefix(prefix.to_vec()))
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<Box<dyn OwnedKVScan>> {
        self.open_cursor(OwnedLsmScanBounds::range(start.to_vec(), end.to_vec()))
    }

    fn commit(self: Box<Self>) -> Result<()> {
        let (mode, start_timestamp, read_set, write_set) = {
            let mut state = self
                .state
                .lock()
                .expect("owned LSM transaction mutex poisoned");
            Self::ensure_active(&state)?;
            if state.cursor_open {
                return Err(Error::TxnClosed);
            }
            state.state = TxnState::Committed;
            (
                state.mode,
                state.start_timestamp,
                std::mem::take(&mut state.read_set),
                std::mem::take(&mut state.write_set),
            )
        };
        if mode == TxnMode::ReadOnly || write_set.is_empty() {
            return Ok(());
        }
        self.lsm()?
            .commit_write_set(start_timestamp, &read_set, write_set)
    }

    fn rollback(self: Box<Self>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        Self::ensure_active(&state)?;
        if state.cursor_open {
            return Err(Error::TxnClosed);
        }
        state.read_set.clear();
        state.write_set.clear();
        state.state = TxnState::RolledBack;
        Ok(())
    }
}

/// One persisted-table cursor plus an unconsumed candidate.  The candidate is at most one entry,
/// so merge state remains bounded by the number of SSTables rather than result cardinality.
struct OwnedLsmTableCursor {
    cursor: SSTableCursor,
    candidate: Option<SSTableEntry>,
}

impl OwnedLsmTableCursor {
    fn new(cursor: SSTableCursor) -> Self {
        Self {
            cursor,
            candidate: None,
        }
    }

    fn candidate_after(&mut self, lsm: &LsmKV, after: Option<&Key>) -> Result<Option<Key>> {
        loop {
            if let Some(candidate) = &self.candidate {
                if after.is_none_or(|last| candidate.key.as_slice() > last.as_slice()) {
                    return Ok(Some(candidate.key.clone()));
                }
                self.candidate = None;
            }
            let Some(candidate) = self.cursor.next_entry(&lsm.buffer_pool, &lsm.metrics)? else {
                return Ok(None);
            };
            self.candidate = Some(candidate);
        }
    }
}

/// Incremental owned LSM cursor.  It owns all state needed to advance without borrowing either
/// the transaction or the store and never creates a full `Vec`/`BTreeMap` of scan results.
struct OwnedLsmCursor {
    store: Arc<AnyKV>,
    transaction: Arc<Mutex<OwnedLsmTransactionState>>,
    snapshot: Option<OwnedLsmSnapshotReader>,
    bounds: OwnedLsmScanBounds,
    tables: Vec<OwnedLsmTableCursor>,
    last_key: Option<Key>,
}

impl OwnedLsmCursor {
    fn read_timestamp(&self) -> Result<u64> {
        let state = self
            .transaction
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        if state.state != TxnState::Active || !state.cursor_open {
            return Err(Error::TxnClosed);
        }
        Ok(state.start_timestamp)
    }

    fn table_candidate(
        tables: &mut [OwnedLsmTableCursor],
        last_key: Option<&Key>,
        lsm: &LsmKV,
    ) -> Result<Option<Key>> {
        let mut candidate = None;
        for table in tables {
            let Some(key) = table.candidate_after(lsm, last_key)? else {
                continue;
            };
            if candidate
                .as_ref()
                .is_none_or(|current: &Key| key < *current)
            {
                candidate = Some(key);
            }
        }
        Ok(candidate)
    }

    fn write_candidate(&self) -> Result<Option<(Key, Option<Value>)>> {
        let state = self
            .transaction
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        if state.state != TxnState::Active || !state.cursor_open {
            return Err(Error::TxnClosed);
        }
        let entry = match self.last_key.as_ref() {
            Some(last) => state
                .write_set
                .range::<Key, _>((Excluded(last), Unbounded))
                .next(),
            None => state
                .write_set
                .range::<Key, _>((Included(self.bounds.start()), Unbounded))
                .next(),
        };
        Ok(entry
            .filter(|(key, _)| self.bounds.contains(key))
            .map(|(key, value)| (key.clone(), value.clone())))
    }

    fn record_read(&self, key: Key) -> Result<()> {
        let mut state = self
            .transaction
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        if state.state != TxnState::Active || !state.cursor_open {
            return Err(Error::TxnClosed);
        }
        state.read_set.insert(key);
        Ok(())
    }

    fn finish(&mut self) {
        self.snapshot.take();
        let mut state = self
            .transaction
            .lock()
            .expect("owned LSM transaction mutex poisoned");
        state.cursor_open = false;
    }
}

impl OwnedKVScan for OwnedLsmCursor {
    fn next_entry(&mut self) -> Result<Option<(Key, Value)>> {
        if self.snapshot.is_none() {
            return Ok(None);
        }
        loop {
            let read_timestamp = self.read_timestamp()?;
            let lsm = OwnedLsmTransaction::lsm_from(&self.store)?;
            let memtable_key = lsm.owned_next_memtable_key_after(
                self.last_key.as_ref(),
                &self.bounds,
                read_timestamp,
            );
            let table_key = Self::table_candidate(&mut self.tables, self.last_key.as_ref(), lsm)?;
            let base_key = match (memtable_key, table_key) {
                (Some(memory), Some(table)) => Some(memory.min(table)),
                (Some(memory), None) => Some(memory),
                (None, Some(table)) => Some(table),
                (None, None) => None,
            };
            let write = self.write_candidate()?;

            let next = match (base_key, write) {
                (Some(base_key), Some((write_key, write_value))) if base_key == write_key => {
                    self.record_read(base_key.clone())?;
                    (base_key, write_value)
                }
                (Some(base_key), Some((write_key, _))) if base_key < write_key => {
                    self.record_read(base_key.clone())?;
                    let value = lsm
                        .owned_visible_at(&base_key, read_timestamp)?
                        .and_then(|entry| entry.value);
                    (base_key, value)
                }
                (Some(_), Some((write_key, write_value))) => {
                    self.record_read(write_key.clone())?;
                    (write_key, write_value)
                }
                (Some(base_key), None) => {
                    self.record_read(base_key.clone())?;
                    let value = lsm
                        .owned_visible_at(&base_key, read_timestamp)?
                        .and_then(|entry| entry.value);
                    (base_key, value)
                }
                (None, Some((write_key, write_value))) => {
                    self.record_read(write_key.clone())?;
                    (write_key, write_value)
                }
                (None, None) => {
                    self.finish();
                    return Ok(None);
                }
            };
            self.last_key = Some(next.0.clone());
            if let Some(value) = next.1 {
                return Ok(Some((next.0, value)));
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.finish();
        Ok(())
    }
}

impl Drop for OwnedLsmCursor {
    fn drop(&mut self) {
        self.finish();
    }
}

/// `AnyKV` のトランザクション。
pub enum AnyKVTransaction<'a> {
    /// MemoryKV のトランザクション。
    Memory(MemoryTransaction<'a>),
    /// LsmKV のトランザクション。
    Lsm(LsmTransaction<'a>),
    /// A temporary compatibility view over an owned transaction session.  Its owning session
    /// performs the one terminal commit or rollback.
    Owned(OwnedKVTransactionAdapter<'a>),
}

impl<'a> KVTransaction<'a> for AnyKVTransaction<'a> {
    fn id(&self) -> TxnId {
        match self {
            Self::Memory(tx) => tx.id(),
            Self::Lsm(tx) => tx.id(),
            Self::Owned(tx) => tx.id(),
        }
    }

    fn mode(&self) -> TxnMode {
        match self {
            Self::Memory(tx) => tx.mode(),
            Self::Lsm(tx) => tx.mode(),
            Self::Owned(tx) => tx.mode(),
        }
    }

    fn get(&mut self, key: &crate::types::Key) -> Result<Option<crate::types::Value>> {
        match self {
            Self::Memory(tx) => tx.get(key),
            Self::Lsm(tx) => tx.get(key),
            Self::Owned(tx) => tx.get(key),
        }
    }

    fn put(&mut self, key: crate::types::Key, value: crate::types::Value) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.put(key, value),
            Self::Lsm(tx) => tx.put(key, value),
            Self::Owned(tx) => tx.put(key, value),
        }
    }

    fn delete(&mut self, key: crate::types::Key) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.delete(key),
            Self::Lsm(tx) => tx.delete(key),
            Self::Owned(tx) => tx.delete(key),
        }
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (crate::types::Key, crate::types::Value)> + '_>> {
        match self {
            Self::Memory(tx) => tx.scan_prefix(prefix),
            Self::Lsm(tx) => tx.scan_prefix(prefix),
            Self::Owned(tx) => tx.scan_prefix(prefix),
        }
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (crate::types::Key, crate::types::Value)> + '_>> {
        match self {
            Self::Memory(tx) => tx.scan_range(start, end),
            Self::Lsm(tx) => tx.scan_range(start, end),
            Self::Owned(tx) => tx.scan_range(start, end),
        }
    }

    fn commit_self(self) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.commit_self(),
            Self::Lsm(tx) => tx.commit_self(),
            Self::Owned(tx) => tx.commit_self(),
        }
    }

    fn rollback_self(self) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.rollback_self(),
            Self::Lsm(tx) => tx.rollback_self(),
            Self::Owned(tx) => tx.rollback_self(),
        }
    }
}

impl<'a> AnyKVTransaction<'a> {
    /// トランザクションを消費せずにロールバックする。
    pub fn rollback_in_place(&mut self) -> Result<()> {
        match self {
            Self::Memory(tx) => tx.rollback_in_place(),
            Self::Lsm(tx) => tx.rollback_in_place(),
            Self::Owned(_) => Err(Error::InvalidParameter {
                param: "owned_transaction_adapter".to_owned(),
                reason: "terminal ownership belongs to OwnedTransactionSession".to_owned(),
            }),
        }
    }
}

/// `AnyKV` のトランザクションマネージャ（薄いラッパー）。
#[derive(Clone, Copy)]
pub enum AnyKVManager<'a> {
    /// MemoryKV のマネージャ参照。
    Memory(&'a MemoryTxnManager),
    /// LsmKV のマネージャ参照。
    Lsm(LsmTxnManagerRef<'a>),
}

impl<'a> TxnManager<'a, AnyKVTransaction<'a>> for AnyKVManager<'a> {
    fn begin(&'a self, mode: TxnMode) -> Result<AnyKVTransaction<'a>> {
        match self {
            Self::Memory(m) => Ok(AnyKVTransaction::Memory(m.begin(mode)?)),
            Self::Lsm(m) => Ok(AnyKVTransaction::Lsm(m.begin(mode)?)),
        }
    }

    fn commit(&'a self, txn: AnyKVTransaction<'a>) -> Result<()> {
        txn.commit_self()
    }

    fn rollback(&'a self, txn: AnyKVTransaction<'a>) -> Result<()> {
        txn.rollback_self()
    }
}

impl KVStore for AnyKV {
    type Transaction<'a>
        = AnyKVTransaction<'a>
    where
        Self: 'a;
    type Manager<'a>
        = AnyKVManager<'a>
    where
        Self: 'a;

    fn txn_manager(&self) -> Self::Manager<'_> {
        match self {
            Self::Memory(kv) => AnyKVManager::Memory(kv.txn_manager()),
            Self::Lsm(kv) => AnyKVManager::Lsm(kv.as_ref().txn_manager()),
            #[cfg(feature = "s3")]
            Self::S3(kv) => AnyKVManager::Lsm(kv.inner().txn_manager()),
        }
    }

    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        match self {
            Self::Memory(kv) => Ok(AnyKVTransaction::Memory(kv.begin(mode)?)),
            Self::Lsm(kv) => Ok(AnyKVTransaction::Lsm(kv.as_ref().begin(mode)?)),
            #[cfg(feature = "s3")]
            Self::S3(kv) => Ok(AnyKVTransaction::Lsm(kv.inner().begin(mode)?)),
        }
    }

    fn read_at_capability(&self) -> ReadAtCapability {
        AnyKV::read_at_capability(self)
    }

    fn begin_read_at(&self, point: &ReadAtPoint) -> ReadAtResult<Self::Transaction<'_>> {
        Err(self
            .read_at_capability()
            .unavailable_error(point, "AnyKV remote read-at is not enabled"))
    }

    fn runtime_stats(&self) -> Option<crate::kv::RuntimeStats> {
        match self {
            Self::Memory(kv) => kv.runtime_stats(),
            Self::Lsm(kv) => kv.runtime_stats(),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.inner().runtime_stats(),
        }
    }

    fn set_memory_limit_bytes(&self, limit: Option<usize>) -> Result<()> {
        match self {
            Self::Memory(kv) => kv.set_memory_limit_bytes(limit),
            Self::Lsm(kv) => kv.set_memory_limit_bytes(limit),
            #[cfg(feature = "s3")]
            Self::S3(kv) => kv.inner().set_memory_limit_bytes(limit),
        }
    }

    fn set_cache_capacity_bytes(&self, capacity: usize) -> Result<()> {
        match self {
            Self::Memory(kv) => <MemoryKV as KVStore>::set_cache_capacity_bytes(kv, capacity),
            Self::Lsm(kv) => <LsmKV as KVStore>::set_cache_capacity_bytes(kv, capacity),
            #[cfg(feature = "s3")]
            Self::S3(kv) => <LsmKV as KVStore>::set_cache_capacity_bytes(kv.inner(), capacity),
        }
    }

    fn clear_cache(&self) -> Result<usize> {
        match self {
            Self::Memory(kv) => <MemoryKV as KVStore>::clear_cache(kv),
            Self::Lsm(kv) => <LsmKV as KVStore>::clear_cache(kv),
            #[cfg(feature = "s3")]
            Self::S3(kv) => <LsmKV as KVStore>::clear_cache(kv.inner()),
        }
    }
}

// Every test here exercises disk-backed persistence across a restart, which
// wasm32 has no filesystem for; `tempfile` is also excluded from that target.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::AnyKV;
    use crate::kv::storage::{StorageFactory, StorageMode};
    use crate::kv::{OwnedReadOptions, OwnedSessionFactory};
    use crate::txn::OwnedLeaseOutcome;
    use crate::types::TxnMode;

    fn disk_store(path: &Path) -> Arc<AnyKV> {
        Arc::new(
            StorageFactory::create(StorageMode::Disk {
                path: path.to_path_buf(),
                config: None,
            })
            .unwrap(),
        )
    }

    #[test]
    fn file_format_storage_dir_distinguishes_memory_and_disk() {
        let memory = AnyKV::Memory(crate::kv::memory::MemoryKV::new());
        assert_eq!(memory.file_format_storage_dir(), None);

        let directory = tempfile::tempdir().unwrap();
        let disk = disk_store(directory.path());
        assert_eq!(disk.file_format_storage_dir(), Some(directory.path()));
    }

    #[test]
    fn owned_lsm_transaction_streams_prefix_and_persisted_range_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store = disk_store(directory.path());
        let transaction = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let lease = transaction.acquire_lease().unwrap();
        lease
            .with_transaction(|transaction| {
                transaction.put(b"p:1".to_vec(), b"one".to_vec())?;
                transaction.put(b"p:2".to_vec(), b"two".to_vec())?;
                transaction.put(b"q:1".to_vec(), b"other".to_vec())?;
                Ok(())
            })
            .unwrap();
        let mut cursor = lease
            .with_transaction(|transaction| transaction.scan_prefix(b"p:"))
            .unwrap();
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"p:1".to_vec(), b"one".to_vec()))
        );
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"p:2".to_vec(), b"two".to_vec()))
        );
        assert_eq!(cursor.next_entry().unwrap(), None);
        drop(cursor);
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        transaction.commit().unwrap();

        match store.as_ref() {
            AnyKV::Lsm(lsm) => lsm.checkpoint().unwrap(),
            _ => panic!("disk store must use LSM"),
        };
        drop(store);

        let reopened = disk_store(directory.path());
        let read = reopened
            .clone()
            .begin_owned_read(OwnedReadOptions::default())
            .unwrap();
        let lease = read.acquire_lease().unwrap();
        assert_eq!(
            lease
                .with_transaction(|transaction| transaction.get(&b"p:2".to_vec()))
                .unwrap(),
            Some(b"two".to_vec())
        );
        let mut cursor = lease
            .with_transaction(|transaction| transaction.scan_range(b"p:1", b"q:"))
            .unwrap();
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"p:1".to_vec(), b"one".to_vec()))
        );
        assert_eq!(
            cursor.next_entry().unwrap(),
            Some((b"p:2".to_vec(), b"two".to_vec()))
        );
        assert_eq!(cursor.next_entry().unwrap(), None);
        drop(cursor);
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
    }

    #[test]
    fn owned_lsm_rollback_does_not_survive_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store = disk_store(directory.path());
        let transaction = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let lease = transaction.acquire_lease().unwrap();
        lease
            .with_transaction(|transaction| transaction.put(b"discard".to_vec(), b"value".to_vec()))
            .unwrap();
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        transaction.rollback().unwrap();
        drop(store);

        let reopened = disk_store(directory.path());
        let read = reopened
            .clone()
            .begin_owned_read(OwnedReadOptions::default())
            .unwrap();
        let lease = read.acquire_lease().unwrap();
        assert_eq!(
            lease
                .with_transaction(|transaction| transaction.get(&b"discard".to_vec()))
                .unwrap(),
            None
        );
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
    }
}
