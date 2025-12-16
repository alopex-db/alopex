//! An in-memory key-value store implementation with Write-Ahead Logging
//! and Optimistic Concurrency Control for Snapshot Isolation.

use crate::error::{Error, Result};
#[cfg(feature = "test-hooks")]
use crate::kv::hooks::{CrashOperation, CrashSimulator, CrashTiming, IoHooks};
use crate::kv::{KVStore, KVTransaction};
use crate::log::wal::{WalReader, WalRecord, WalWriter};
use crate::storage::flush::write_empty_vector_segment;
use crate::storage::sstable::{SstableReader, SstableWriter};
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, TxnState, Value};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use tracing::warn;

/// メモリ使用量の統計（バイト単位）。
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// 全体のメモリ使用量。
    pub total_bytes: usize,
    /// KV データのメモリ使用量。
    pub kv_bytes: usize,
    /// 補助インデックスのメモリ使用量。
    pub index_bytes: usize,
}

/// An in-memory key-value store.
#[derive(Clone)]
pub struct MemoryKV {
    manager: Arc<MemoryTxnManager>,
}

impl MemoryKV {
    /// Creates a new, purely transient in-memory KV store.
    pub fn new() -> Self {
        Self {
            manager: Arc::new(MemoryTxnManager::new(None, None, None)),
        }
    }

    /// Returns current in-memory usage statistics.
    pub fn memory_stats(&self) -> MemoryStats {
        self.manager.memory_stats()
    }

    /// Creates a new in-memory KV store with an optional memory limit.
    pub fn new_with_limit(limit: Option<usize>) -> Self {
        Self {
            manager: Arc::new(MemoryTxnManager::new_with_limit(limit)),
        }
    }

    /// Opens a persistent in-memory KV store from a file path.
    pub fn open(path: &Path) -> Result<Self> {
        let wal_writer = WalWriter::new(path)?;
        let sstable_path = path.with_extension("sst");
        let manager = Arc::new(MemoryTxnManager::new(
            Some(wal_writer),
            Some(path.to_path_buf()),
            Some(sstable_path),
        ));
        manager.recover()?;
        Ok(Self { manager })
    }

    /// Opens a persistent KV store with I/O hooks (test only).
    #[cfg(feature = "test-hooks")]
    pub fn open_with_io_hooks(path: &Path, hooks: Arc<dyn IoHooks>) -> Result<Self> {
        let wal_writer = WalWriter::new(path)?;
        let sstable_path = path.with_extension("sst");
        let manager = Arc::new(MemoryTxnManager::new(
            Some(wal_writer),
            Some(path.to_path_buf()),
            Some(sstable_path),
        ));
        manager.set_io_hooks(Some(hooks));
        manager.recover()?;
        Ok(Self { manager })
    }

    /// Opens a persistent KV store with crash hooks (test only).
    #[cfg(feature = "test-hooks")]
    pub fn open_with_crash_hooks(path: &Path, crash_sim: Arc<CrashSimulator>) -> Result<Self> {
        let wal_writer = WalWriter::new(path)?;
        let sstable_path = path.with_extension("sst");
        let manager = Arc::new(MemoryTxnManager::new(
            Some(wal_writer),
            Some(path.to_path_buf()),
            Some(sstable_path),
        ));
        manager.set_crash_sim(Some(crash_sim));
        manager.recover()?;
        Ok(Self { manager })
    }

    /// Flushes the in-memory data to an SSTable.
    pub fn flush(&self) -> Result<()> {
        self.manager.flush()
    }
}

impl Default for MemoryKV {
    fn default() -> Self {
        Self::new()
    }
}

impl KVStore for MemoryKV {
    type Transaction<'a> = MemoryTransaction<'a>;
    type Manager<'a> = &'a MemoryTxnManager;

    fn txn_manager(&self) -> Self::Manager<'_> {
        &self.manager
    }

    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        self.manager.begin_internal(mode)
    }
}

// The internal value stored in the BTreeMap, containing the data and its version.
type VersionedValue = (Value, u64);

/// The underlying shared state for the in-memory store.
struct MemorySharedState {
    /// The main data store, mapping keys to versioned values.
    data: RwLock<BTreeMap<Key, VersionedValue>>,
    /// The next transaction ID to be allocated.
    next_txn_id: AtomicU64,
    /// The current commit version of the database. Incremented on every successful commit.
    commit_version: AtomicU64,
    /// The WAL writer. If None, the store is transient.
    wal_writer: Option<RwLock<WalWriter>>,
    /// Optional WAL path for replay on reopen.
    wal_path: Option<PathBuf>,
    /// Optional SSTable reader for read-through.
    sstable: RwLock<Option<SstableReader>>,
    /// Optional SSTable path for flush/reopen.
    sstable_path: Option<PathBuf>,
    /// Optional memory upper limit (bytes) for in-memory mode。
    memory_limit: RwLock<Option<usize>>,
    /// Current memory consumption (bytes) tracked across operations。
    current_memory: AtomicUsize,
    #[cfg(feature = "test-hooks")]
    /// Optional I/O hooks for fault injection (test only).
    io_hooks: RwLock<Option<Arc<dyn IoHooks>>>,
    #[cfg(feature = "test-hooks")]
    /// Optional crash simulator for test-only crash points.
    crash_sim: RwLock<Option<Arc<CrashSimulator>>>,
}

impl MemorySharedState {
    /// Check whether adding `additional` bytes would exceed the memory limit.
    fn check_memory_limit(&self, additional: usize) -> Result<()> {
        if let Some(limit) = *self.memory_limit.read().unwrap() {
            let current = self.current_memory.load(Ordering::Relaxed);
            let requested = current.saturating_add(additional);
            if requested > limit {
                return Err(Error::MemoryLimitExceeded { limit, requested });
            }
        }
        Ok(())
    }

    /// Return current memory usage statistics.
    fn memory_stats(&self) -> MemoryStats {
        let kv_bytes = self.current_memory.load(Ordering::Relaxed);
        MemoryStats {
            total_bytes: kv_bytes,
            kv_bytes,
            index_bytes: 0,
        }
    }

    /// Recompute tracked memory usage from existing data (used after recovery).
    fn recompute_current_memory(&self) {
        let data = self.data.read().unwrap();
        let mut total = 0usize;
        for (k, (v, _)) in data.iter() {
            total = total.saturating_add(k.len() + v.len());
        }
        self.current_memory.store(total, Ordering::Relaxed);
    }
}

/// A transaction manager backed by an in-memory map and optional WAL.
pub struct MemoryTxnManager {
    state: Arc<MemorySharedState>,
}

impl MemoryTxnManager {
    fn new_with_params(
        wal_writer: Option<WalWriter>,
        wal_path: Option<PathBuf>,
        sstable_path: Option<PathBuf>,
        memory_limit: Option<usize>,
    ) -> Self {
        Self {
            state: Arc::new(MemorySharedState {
                data: RwLock::new(BTreeMap::new()),
                next_txn_id: AtomicU64::new(1),
                commit_version: AtomicU64::new(0),
                wal_writer: wal_writer.map(RwLock::new),
                wal_path,
                sstable: RwLock::new(None),
                sstable_path,
                memory_limit: RwLock::new(memory_limit),
                current_memory: AtomicUsize::new(0),
                #[cfg(feature = "test-hooks")]
                io_hooks: RwLock::new(None),
                #[cfg(feature = "test-hooks")]
                crash_sim: RwLock::new(None),
            }),
        }
    }

    fn new(
        wal_writer: Option<WalWriter>,
        wal_path: Option<PathBuf>,
        sstable_path: Option<PathBuf>,
    ) -> Self {
        Self::new_with_params(wal_writer, wal_path, sstable_path, None)
    }

    /// Creates an in-memory manager with an optional memory limit.
    pub fn new_with_limit(limit: Option<usize>) -> Self {
        Self::new_with_params(None, None, None, limit)
    }

    #[cfg(feature = "test-hooks")]
    fn set_io_hooks(&self, hooks: Option<Arc<dyn IoHooks>>) {
        let mut guard = self.state.io_hooks.write().unwrap();
        *guard = hooks;
    }

    #[cfg(feature = "test-hooks")]
    fn set_crash_sim(&self, crash_sim: Option<Arc<CrashSimulator>>) {
        let mut guard = self.state.crash_sim.write().unwrap();
        *guard = crash_sim;
    }

    #[cfg(feature = "test-hooks")]
    fn io_hooks(&self) -> Option<Arc<dyn IoHooks>> {
        self.state.io_hooks.read().unwrap().clone()
    }

    #[cfg(feature = "test-hooks")]
    fn crash_sim(&self) -> Option<Arc<CrashSimulator>> {
        self.state.crash_sim.read().unwrap().clone()
    }

    /// Returns current memory usage statistics.
    pub fn memory_stats(&self) -> MemoryStats {
        self.state.memory_stats()
    }

    /// Update the configured memory limit at runtime.
    pub fn set_memory_limit(&self, limit: Option<usize>) {
        let mut guard = self.state.memory_limit.write().unwrap();
        *guard = limit;
    }

    /// Returns a snapshot clone of all key/value pairs.
    pub fn snapshot(&self) -> Vec<(Key, Value)> {
        let data = self.state.data.read().unwrap();
        data.iter()
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }

    /// Clears all data and resets memory accounting.
    pub fn clear_all(&self) {
        let mut data = self.state.data.write().unwrap();
        data.clear();
        drop(data);
        self.state.current_memory.store(0, Ordering::Relaxed);
        self.state.commit_version.store(0, Ordering::Relaxed);
    }

    /// Runs compaction if it can fit within the configured memory limit.
    /// Returns Ok(true) when compaction executed, Ok(false) when skipped.
    pub fn compact_with_limit<F>(
        &self,
        input_bytes: usize,
        output_bytes: usize,
        run: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> Result<()>,
    {
        if let Some(limit) = *self.state.memory_limit.read().unwrap() {
            let current = self.state.current_memory.load(Ordering::Relaxed);
            // predicted usage after compaction: current - input + output (clamped at 0)
            let prospective = current
                .saturating_sub(input_bytes)
                .saturating_add(output_bytes);
            if prospective > limit {
                warn!(
                    limit,
                    requested = prospective,
                    "compaction skipped due to memory limit"
                );
                return Ok(false);
            }
        }

        run()?;

        // Update tracked memory to reflect compaction result.
        let current = self.state.current_memory.load(Ordering::Relaxed);
        let new_usage = current
            .saturating_sub(input_bytes)
            .saturating_add(output_bytes);
        self.state
            .current_memory
            .store(new_usage, Ordering::Relaxed);
        Ok(true)
    }

    #[cfg(feature = "test-hooks")]
    fn trigger_crash(&self, operation: CrashOperation, timing: CrashTiming) {
        if let Some(sim) = self.crash_sim() {
            sim.check_crash(operation, timing);
        }
    }

    #[cfg(feature = "test-hooks")]
    fn notify_wal_hooks(&self, data: &[u8], timing: CrashTiming) -> Result<()> {
        match timing {
            CrashTiming::Before => {
                self.trigger_crash(CrashOperation::WalWrite, CrashTiming::Before);
                if let Some(hooks) = self.io_hooks() {
                    hooks.before_wal_write(data).map_err(Error::Io)?;
                    hooks.before_fsync().map_err(Error::Io)?;
                }
                self.trigger_crash(CrashOperation::WalFsync, CrashTiming::Before);
            }
            CrashTiming::During => {
                self.trigger_crash(CrashOperation::WalWrite, CrashTiming::During);
                self.trigger_crash(CrashOperation::WalFsync, CrashTiming::During);
            }
            CrashTiming::After => {
                self.trigger_crash(CrashOperation::WalWrite, CrashTiming::After);
                self.trigger_crash(CrashOperation::WalFsync, CrashTiming::After);
                if let Some(hooks) = self.io_hooks() {
                    hooks.after_wal_write(data).map_err(Error::Io)?;
                    hooks.after_fsync().map_err(Error::Io)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "test-hooks")]
    fn notify_compaction(&self, timing: CrashTiming) {
        self.trigger_crash(CrashOperation::Compaction, timing);
        if let Some(hooks) = self.io_hooks() {
            match timing {
                CrashTiming::Before => hooks.on_compaction_start(),
                CrashTiming::After => hooks.on_compaction_end(),
                CrashTiming::During => {}
            }
        }
    }

    fn append_wal_record(&self, wal: &mut WalWriter, record: &WalRecord) -> Result<()> {
        #[cfg(feature = "test-hooks")]
        {
            let data =
                bincode::serialize(record).map_err(|e| Error::Io(std::io::Error::other(e)))?;
            self.notify_wal_hooks(&data, CrashTiming::Before)?;
            self.notify_wal_hooks(&data, CrashTiming::During)?;
            wal.append(record)?;
            self.notify_wal_hooks(&data, CrashTiming::After)?;
            return Ok(());
        }

        #[cfg(not(feature = "test-hooks"))]
        {
            wal.append(record)
        }
    }

    fn write_wal(&self, txn_id: TxnId, writes: &BTreeMap<Key, Option<Value>>) -> Result<()> {
        if let Some(wal_lock) = &self.state.wal_writer {
            let mut wal = wal_lock.write().unwrap();
            self.append_wal_record(&mut wal, &WalRecord::Begin(txn_id))?;
            for (key, value) in writes {
                let record = match value {
                    Some(v) => WalRecord::Put(txn_id, key.clone(), v.clone()),
                    None => WalRecord::Delete(txn_id, key.clone()),
                };
                self.append_wal_record(&mut wal, &record)?;
            }
            self.append_wal_record(&mut wal, &WalRecord::Commit(txn_id))?;
        }
        Ok(())
    }

    /// In-memory compaction entrypoint that rebuilds the map while honoring memory limits.
    pub fn compact_in_memory(&self) -> Result<bool> {
        #[cfg(feature = "test-hooks")]
        self.notify_compaction(CrashTiming::Before);

        let snapshot_bytes = {
            let data = self.state.data.read().unwrap();
            let mut bytes = 0usize;
            for (k, (v, _)) in data.iter() {
                bytes = bytes.saturating_add(k.len() + v.len());
            }
            bytes
        };

        let executed = self.compact_with_limit(snapshot_bytes, snapshot_bytes, || {
            let data = self.state.data.read().unwrap();
            let mut rebuilt = BTreeMap::new();
            for (k, (v, version)) in data.iter() {
                rebuilt.insert(k.clone(), (v.clone(), *version));
            }
            drop(data);

            #[cfg(feature = "test-hooks")]
            self.notify_compaction(CrashTiming::During);

            let mut write_guard = self.state.data.write().unwrap();
            *write_guard = rebuilt;
            Ok(())
        })?;

        #[cfg(feature = "test-hooks")]
        self.notify_compaction(CrashTiming::After);

        Ok(executed)
    }

    /// Flushes the current in-memory data to an SSTable file.
    pub fn flush(&self) -> Result<()> {
        let path = self
            .state
            .sstable_path
            .as_ref()
            .ok_or_else(|| Error::InvalidFormat("sstable path is not configured".into()))?;

        #[cfg(feature = "test-hooks")]
        self.notify_compaction(CrashTiming::Before);

        let data = self.state.data.read().unwrap();
        let mut writer = SstableWriter::create(path)?;
        for (key, (value, _version)) in data.iter() {
            #[cfg(feature = "test-hooks")]
            {
                let mut record = Vec::with_capacity(key.len() + value.len());
                record.extend_from_slice(key);
                record.extend_from_slice(value);
                self.trigger_crash(CrashOperation::SstWrite, CrashTiming::Before);
                if let Some(hooks) = self.io_hooks() {
                    hooks.before_sst_write(&record).map_err(Error::Io)?;
                }
                self.trigger_crash(CrashOperation::SstWrite, CrashTiming::During);
            }

            writer.append(key, value)?;

            #[cfg(feature = "test-hooks")]
            self.trigger_crash(CrashOperation::SstWrite, CrashTiming::After);
        }
        drop(data);

        #[cfg(feature = "test-hooks")]
        self.trigger_crash(CrashOperation::SstFinalize, CrashTiming::Before);
        let _footer = writer.finish()?;
        #[cfg(feature = "test-hooks")]
        self.trigger_crash(CrashOperation::SstFinalize, CrashTiming::After);
        let reader = SstableReader::open(path)?;
        // Also emit a placeholder vector segment alongside SSTable for future vector recovery.
        let vec_path = path.with_extension("vec");
        write_empty_vector_segment(&vec_path)?;

        let mut slot = self.state.sstable.write().unwrap();
        *slot = Some(reader);

        #[cfg(feature = "test-hooks")]
        self.notify_compaction(CrashTiming::After);
        Ok(())
    }

    /// Replays the WAL to restore the state of the in-memory map.
    fn replay(&self) -> Result<()> {
        let path = match &self.state.wal_path {
            Some(p) => p,
            None => return Ok(()),
        };
        if !path.exists() || std::fs::metadata(path)?.len() == 0 {
            return Ok(());
        }

        let mut data = self.state.data.write().unwrap();
        let mut max_txn_id = 0;
        let mut max_version = self.state.commit_version.load(Ordering::Acquire);
        let reader = WalReader::new(path)?;
        let mut pending_txns: HashMap<TxnId, Vec<(Key, Option<Value>)>> = HashMap::new();

        for record_result in reader {
            match record_result? {
                WalRecord::Begin(txn_id) => {
                    max_txn_id = max_txn_id.max(txn_id.0);
                    pending_txns.entry(txn_id).or_default();
                }
                WalRecord::Put(txn_id, key, value) => {
                    max_txn_id = max_txn_id.max(txn_id.0);
                    pending_txns
                        .entry(txn_id)
                        .or_default()
                        .push((key, Some(value)));
                }
                WalRecord::Delete(txn_id, key) => {
                    max_txn_id = max_txn_id.max(txn_id.0);
                    pending_txns.entry(txn_id).or_default().push((key, None));
                }
                WalRecord::Commit(txn_id) => {
                    if let Some(writes) = pending_txns.remove(&txn_id) {
                        max_version += 1;
                        for (key, value) in writes {
                            if let Some(v) = value {
                                data.insert(key, (v, max_version));
                            } else {
                                data.remove(&key);
                            }
                        }
                    }
                }
            }
        }

        self.state
            .next_txn_id
            .store(max_txn_id + 1, Ordering::SeqCst);
        self.state
            .commit_version
            .store(max_version, Ordering::SeqCst);
        Ok(())
    }

    fn load_sstable(&self) -> Result<()> {
        let path = match &self.state.sstable_path {
            Some(p) => p,
            None => return Ok(()),
        };
        if !path.exists() {
            return Ok(());
        }

        let mut reader = SstableReader::open(path)?;
        let mut data = self.state.data.write().unwrap();
        let mut version = self.state.commit_version.load(Ordering::Acquire);

        let keys: Vec<Key> = reader
            .index()
            .iter()
            .map(|entry| entry.key.clone())
            .collect();

        for key in keys {
            if let Some(value) = reader.get(&key)? {
                version += 1;
                data.insert(key, (value, version));
            }
        }

        self.state.commit_version.store(version, Ordering::SeqCst);
        let mut slot = self.state.sstable.write().unwrap();
        *slot = Some(reader);
        Ok(())
    }

    /// Loads SSTable then replays WAL to restore state.
    fn recover(&self) -> Result<()> {
        self.load_sstable()?;
        self.replay()?;
        self.state.recompute_current_memory();
        Ok(())
    }

    fn sstable_get(&self, key: &Key) -> Result<Option<Value>> {
        let mut guard = self.state.sstable.write().unwrap();
        if let Some(reader) = guard.as_mut() {
            return reader.get(key);
        }
        Ok(None)
    }

    fn begin_internal(&self, mode: TxnMode) -> Result<MemoryTransaction<'_>> {
        let txn_id = self.state.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_version = self.state.commit_version.load(Ordering::Acquire);
        Ok(MemoryTransaction::new(
            self,
            TxnId(txn_id),
            mode,
            start_version,
        ))
    }
}

impl<'a> TxnManager<'a, MemoryTransaction<'a>> for &'a MemoryTxnManager {
    fn begin(&'a self, mode: TxnMode) -> Result<MemoryTransaction<'a>> {
        self.begin_internal(mode)
    }

    fn commit(&'a self, mut txn: MemoryTransaction<'a>) -> Result<()> {
        if txn.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        if txn.mode == TxnMode::ReadOnly || txn.writes.is_empty() {
            txn.state = TxnState::Committed;
            return Ok(());
        }

        let mut data = self.state.data.write().unwrap();

        for key in txn.read_set.keys() {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > txn.start_version {
                return Err(Error::TxnConflict);
            }
        }

        // Detect write-write conflicts even when the key was never read.
        for key in txn.writes.keys() {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > txn.start_version {
                return Err(Error::TxnConflict);
            }
        }

        // Compute prospective memory usage and enforce limits before mutating state.
        let mut delta: isize = 0;
        for (key, value) in &txn.writes {
            let current_size = data.get(key).map(|(v, _)| key.len() + v.len()).unwrap_or(0);
            let new_size = match value {
                Some(v) => key.len() + v.len(),
                None => 0,
            };
            delta += new_size as isize - current_size as isize;
        }

        let current_mem = self.state.current_memory.load(Ordering::Relaxed);
        let prospective = if delta >= 0 {
            current_mem.saturating_add(delta as usize)
        } else {
            current_mem.saturating_sub(delta.unsigned_abs())
        };

        if delta > 0 {
            self.state.check_memory_limit(delta as usize)?;
        }

        let commit_version = self.state.commit_version.fetch_add(1, Ordering::AcqRel) + 1;

        self.write_wal(txn.id, &txn.writes)?;

        for (key, value) in std::mem::take(&mut txn.writes) {
            if let Some(v) = value {
                data.insert(key, (v, commit_version));
            } else {
                data.remove(&key);
            }
        }

        self.state
            .current_memory
            .store(prospective, Ordering::Relaxed);

        txn.state = TxnState::Committed;
        Ok(())
    }

    fn rollback(&'a self, mut txn: MemoryTransaction<'a>) -> Result<()> {
        if txn.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        txn.state = TxnState::RolledBack;
        Ok(())
    }
}

/// An in-memory transaction that enforces snapshot isolation.
pub struct MemoryTransaction<'a> {
    manager: &'a MemoryTxnManager,
    id: TxnId,
    mode: TxnMode,
    state: TxnState,
    start_version: u64,
    writes: BTreeMap<Key, Option<Value>>,
    read_set: HashMap<Key, u64>,
}

impl<'a> MemoryTransaction<'a> {
    fn new(manager: &'a MemoryTxnManager, id: TxnId, mode: TxnMode, start_version: u64) -> Self {
        Self {
            manager,
            id,
            mode,
            state: TxnState::Active,
            start_version,
            writes: BTreeMap::new(),
            read_set: HashMap::new(),
        }
    }

    fn ensure_active(&self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        Ok(())
    }

    fn scan_range_internal(&mut self, start: &[u8], end: &[u8]) -> MergedScanIter<'_> {
        let start_vec = start.to_vec();
        let end_vec = end.to_vec();
        let data_guard = self.manager.state.data.read().unwrap();
        let data_ptr: *const BTreeMap<Key, VersionedValue> = &*data_guard;
        let data_iter = unsafe {
            // Safety: data_guard keeps the map alive for the lifetime of the iterator.
            (&*data_ptr).range((Included(start_vec.clone()), Excluded(end_vec.clone())))
        };
        let write_iter = self
            .writes
            .range((Included(start_vec.clone()), Excluded(end_vec.clone())));

        MergedScanIter::new(
            data_guard,
            data_iter,
            write_iter,
            None,
            Some(end_vec),
            self.start_version,
            &mut self.read_set,
        )
    }

    fn scan_prefix_internal(&mut self, prefix: &[u8]) -> MergedScanIter<'_> {
        let prefix_vec = prefix.to_vec();
        let data_guard = self.manager.state.data.read().unwrap();
        let data_ptr: *const BTreeMap<Key, VersionedValue> = &*data_guard;
        let data_iter = unsafe {
            // Safety: data_guard keeps the map alive for the lifetime of the iterator.
            (&*data_ptr).range(prefix_vec.clone()..)
        };
        let write_iter = self.writes.range(prefix_vec.clone()..);
        MergedScanIter::new(
            data_guard,
            data_iter,
            write_iter,
            Some(prefix_vec),
            None,
            self.start_version,
            &mut self.read_set,
        )
    }
}

impl<'a> KVTransaction<'a> for MemoryTransaction<'a> {
    fn id(&self) -> TxnId {
        self.id
    }

    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }

        if let Some(value) = self.writes.get(key) {
            return Ok(value.clone());
        }

        let result = {
            let data = self.manager.state.data.read().unwrap();
            data.get(key).cloned()
        };

        if let Some((v, version)) = result {
            self.read_set.insert(key.clone(), version);
            return Ok(Some(v));
        }

        // Read-through to SSTable if not found in memory.
        if let Some(value) = self.manager.sstable_get(key)? {
            let version = self.manager.state.commit_version.load(Ordering::Acquire);
            self.read_set.insert(key.clone(), version);
            return Ok(Some(value));
        }

        Ok(None)
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        if self.mode == TxnMode::ReadOnly {
            return Err(Error::TxnConflict);
        }
        self.writes.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        if self.mode == TxnMode::ReadOnly {
            return Err(Error::TxnConflict);
        }
        self.writes.insert(key, None);
        Ok(())
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.ensure_active()?;
        let iter = self
            .scan_prefix_internal(prefix)
            .filter_map(|(k, v)| v.map(|val| (k, val)));
        Ok(Box::new(iter))
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.ensure_active()?;
        let iter = self
            .scan_range_internal(start, end)
            .filter_map(|(k, v)| v.map(|val| (k, val)));
        Ok(Box::new(iter))
    }

    fn commit_self(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        if self.mode == TxnMode::ReadOnly || self.writes.is_empty() {
            self.state = TxnState::Committed;
            return Ok(());
        }

        let mut data = self.manager.state.data.write().unwrap();

        // Check read-set for conflicts
        for key in self.read_set.keys() {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > self.start_version {
                return Err(Error::TxnConflict);
            }
        }

        // Check write-write conflicts
        for key in self.writes.keys() {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > self.start_version {
                return Err(Error::TxnConflict);
            }
        }

        // Compute prospective memory usage
        let mut delta: isize = 0;
        for (key, value) in &self.writes {
            let current_size = data.get(key).map(|(v, _)| key.len() + v.len()).unwrap_or(0);
            let new_size = match value {
                Some(v) => key.len() + v.len(),
                None => 0,
            };
            delta += new_size as isize - current_size as isize;
        }

        let current_mem = self.manager.state.current_memory.load(Ordering::Relaxed);
        let prospective = if delta >= 0 {
            current_mem.saturating_add(delta as usize)
        } else {
            current_mem.saturating_sub(delta.unsigned_abs())
        };

        if delta > 0 {
            self.manager.state.check_memory_limit(delta as usize)?;
        }

        let commit_version = self
            .manager
            .state
            .commit_version
            .fetch_add(1, Ordering::AcqRel)
            + 1;

        // WAL write
        self.manager.write_wal(self.id, &self.writes)?;

        // Apply writes
        for (key, value) in std::mem::take(&mut self.writes) {
            if let Some(v) = value {
                data.insert(key, (v, commit_version));
            } else {
                data.remove(&key);
            }
        }

        self.manager
            .state
            .current_memory
            .store(prospective, Ordering::Relaxed);

        self.state = TxnState::Committed;
        Ok(())
    }

    fn rollback_self(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        self.state = TxnState::RolledBack;
        Ok(())
    }
}

/// Lazy merge iterator that overlays in-flight writes onto a snapshot guard.
struct MergedScanIter<'a> {
    _data_guard: RwLockReadGuard<'a, BTreeMap<Key, VersionedValue>>,
    data_iter: std::collections::btree_map::Range<'a, Key, VersionedValue>,
    write_iter: std::collections::btree_map::Range<'a, Key, Option<Value>>,
    data_peek: Option<(Key, (Value, u64))>,
    write_peek: Option<(Key, Option<Value>)>,
    prefix: Option<Vec<u8>>,
    end: Option<Key>,
    start_version: u64,
    read_set: &'a mut HashMap<Key, u64>,
}

impl<'a> MergedScanIter<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        data_guard: std::sync::RwLockReadGuard<'a, BTreeMap<Key, VersionedValue>>,
        data_iter: std::collections::btree_map::Range<'a, Key, VersionedValue>,
        write_iter: std::collections::btree_map::Range<'a, Key, Option<Value>>,
        prefix: Option<Vec<u8>>,
        end: Option<Key>,
        start_version: u64,
        read_set: &'a mut HashMap<Key, u64>,
    ) -> Self {
        let mut iter = Self {
            _data_guard: data_guard,
            data_iter,
            write_iter,
            data_peek: None,
            write_peek: None,
            prefix,
            end,
            start_version,
            read_set,
        };
        iter.advance_data();
        iter.advance_write();
        iter
    }

    fn advance_data(&mut self) {
        self.data_peek = None;
        while let Some((k, (v, ver))) = self.data_iter.next().map(|(k, v)| (k.clone(), v.clone())) {
            if let Some(end) = &self.end {
                if k >= *end {
                    return;
                }
            }
            if let Some(prefix) = &self.prefix {
                if !k.starts_with(prefix) {
                    return;
                }
            }
            if ver > self.start_version {
                continue;
            }
            self.data_peek = Some((k, (v, ver)));
            return;
        }
    }

    fn advance_write(&mut self) {
        self.write_peek = None;
        if let Some((k, v)) = self.write_iter.next().map(|(k, v)| (k.clone(), v.clone())) {
            if let Some(end) = &self.end {
                if k >= *end {
                    return;
                }
            }
            if let Some(prefix) = &self.prefix {
                if !k.starts_with(prefix) {
                    return;
                }
            }
            self.write_peek = Some((k, v));
        }
    }
}

impl<'a> Iterator for MergedScanIter<'a> {
    type Item = (Key, Option<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        let data_key = self.data_peek.as_ref().map(|(k, _)| k.clone());
        let write_key = self.write_peek.as_ref().map(|(k, _)| k.clone());

        match (data_key, write_key) {
            (Some(dk), Some(wk)) => {
                if dk == wk {
                    let (_, (_, ver)) = self.data_peek.take().unwrap();
                    let (_, write_val) = self.write_peek.take().unwrap();
                    self.read_set.insert(dk.clone(), ver);
                    self.advance_data();
                    self.advance_write();
                    Some((dk, write_val))
                } else if dk < wk {
                    let (k, (v, ver)) = self.data_peek.take().unwrap();
                    self.read_set.insert(k.clone(), ver);
                    self.advance_data();
                    Some((k, Some(v)))
                } else {
                    let (k, write_val) = self.write_peek.take().unwrap();
                    self.advance_write();
                    Some((k, write_val))
                }
            }
            (Some(_), None) => {
                let (k, (v, ver)) = self.data_peek.take().unwrap();
                self.read_set.insert(k.clone(), ver);
                self.advance_data();
                Some((k, Some(v)))
            }
            (None, Some(_)) => {
                let (k, write_val) = self.write_peek.take().unwrap();
                self.advance_write();
                Some((k, write_val))
            }
            (None, None) => None,
        }
    }
}

impl<'a> Drop for MemoryTransaction<'a> {
    fn drop(&mut self) {
        if self.state == TxnState::Active {
            self.state = TxnState::RolledBack;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KVTransaction, TxnManager};
    use tempfile::tempdir;
    use tracing::Level;

    fn key(s: &str) -> Key {
        s.as_bytes().to_vec()
    }

    fn value(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_put_and_get_transient() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("hello"), value("world")).unwrap();
        let val = txn.get(&key("hello")).unwrap();
        assert_eq!(val, Some(value("world")));
        manager.commit(txn).unwrap();

        let mut txn2 = manager.begin(TxnMode::ReadOnly).unwrap();
        let val2 = txn2.get(&key("hello")).unwrap();
        assert_eq!(val2, Some(value("world")));
    }

    #[test]
    fn test_occ_conflict() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let mut t1 = manager.begin(TxnMode::ReadWrite).unwrap();
        t1.get(&key("k1")).unwrap();

        let mut t2 = manager.begin(TxnMode::ReadWrite).unwrap();
        t2.put(key("k1"), value("v2")).unwrap();
        assert!(manager.commit(t2).is_ok());

        t1.put(key("k1"), value("v1")).unwrap();
        let result = manager.commit(t1);
        assert!(matches!(result, Err(Error::TxnConflict)));
    }

    #[test]
    fn test_blind_write_conflict() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let mut t1 = manager.begin(TxnMode::ReadWrite).unwrap();
        t1.put(key("k1"), value("v1")).unwrap();

        let mut t2 = manager.begin(TxnMode::ReadWrite).unwrap();
        t2.put(key("k1"), value("v2")).unwrap();
        assert!(manager.commit(t2).is_ok());

        let result = manager.commit(t1);
        assert!(matches!(result, Err(Error::TxnConflict)));
    }

    #[test]
    fn test_read_only_write_fails() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadOnly).unwrap();
        assert!(matches!(
            txn.put(key("k1"), value("v1")),
            Err(Error::TxnConflict)
        ));
        assert!(matches!(txn.delete(key("k1")), Err(Error::TxnConflict)));
    }

    #[test]
    fn test_txn_closed_error() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();
        let txn = manager.begin(TxnMode::ReadWrite).unwrap();
        manager.commit(txn).unwrap();

        // This is tricky to test because commit takes ownership.
        // We can test by creating a new txn and manually setting its state.
        let mut closed_txn = manager.begin(TxnMode::ReadWrite).unwrap();
        closed_txn.state = TxnState::Committed;
        assert!(matches!(closed_txn.get(&key("k1")), Err(Error::TxnClosed)));
        assert!(matches!(
            closed_txn.put(key("k1"), value("v1")),
            Err(Error::TxnClosed)
        ));
    }

    #[test]
    fn test_get_not_found() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadOnly).unwrap();
        let res = txn.get(&key("non-existent"));
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[test]
    fn flush_and_reopen_reads_from_sstable() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        {
            let store = MemoryKV::open(&wal_path).unwrap();
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
            txn.put(key("k1"), value("v1")).unwrap();
            manager.commit(txn).unwrap();
            store.flush().unwrap();
        }

        let reopened = MemoryKV::open(&wal_path).unwrap();
        let manager = reopened.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(txn.get(&key("k1")).unwrap(), Some(value("v1")));
    }

    #[test]
    fn wal_overlays_sstable_on_reopen() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        {
            let store = MemoryKV::open(&wal_path).unwrap();
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
            txn.put(key("k1"), value("v1")).unwrap();
            manager.commit(txn).unwrap();
            store.flush().unwrap();

            let mut txn2 = manager.begin(TxnMode::ReadWrite).unwrap();
            txn2.put(key("k1"), value("v2")).unwrap();
            manager.commit(txn2).unwrap();
        }

        let reopened = MemoryKV::open(&wal_path).unwrap();
        let manager = reopened.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(txn.get(&key("k1")).unwrap(), Some(value("v2")));
    }

    #[test]
    fn scan_prefix_merges_snapshot_and_writes() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let mut seed = manager.begin(TxnMode::ReadWrite).unwrap();
        seed.put(key("p:1"), value("old1")).unwrap();
        seed.put(key("p:2"), value("old2")).unwrap();
        seed.put(key("q:1"), value("other")).unwrap();
        manager.commit(seed).unwrap();

        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("p:1"), value("new1")).unwrap();
        txn.delete(key("p:2")).unwrap();
        txn.put(key("p:3"), value("new3")).unwrap();

        let results: Vec<_> = txn.scan_prefix(b"p:").unwrap().collect();
        assert_eq!(
            results,
            vec![(key("p:1"), value("new1")), (key("p:3"), value("new3"))]
        );
    }

    #[test]
    fn scan_range_skips_newer_versions() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let mut seed = manager.begin(TxnMode::ReadWrite).unwrap();
        seed.put(key("b"), value("v1")).unwrap();
        manager.commit(seed).unwrap();

        let mut txn1 = manager.begin(TxnMode::ReadWrite).unwrap();

        let mut txn2 = manager.begin(TxnMode::ReadWrite).unwrap();
        txn2.put(key("ba"), value("v2")).unwrap();
        manager.commit(txn2).unwrap();

        let results: Vec<_> = txn1.scan_range(b"b", b"c").unwrap().collect();
        assert_eq!(results, vec![(key("b"), value("v1"))]);
    }

    #[test]
    fn scan_range_records_reads_for_conflict_detection() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let mut seed = manager.begin(TxnMode::ReadWrite).unwrap();
        seed.put(key("k1"), value("v1")).unwrap();
        manager.commit(seed).unwrap();

        let mut t1 = manager.begin(TxnMode::ReadWrite).unwrap();
        let results: Vec<_> = t1.scan_range(b"k0", b"kz").unwrap().collect();
        assert_eq!(results, vec![(key("k1"), value("v1"))]);
        t1.put(key("k_new"), value("v_new")).unwrap();

        let mut t2 = manager.begin(TxnMode::ReadWrite).unwrap();
        t2.put(key("k1"), value("v2")).unwrap();
        manager.commit(t2).unwrap();

        let result = manager.commit(t1);
        assert!(matches!(result, Err(Error::TxnConflict)));
    }

    #[test]
    fn memory_stats_tracks_put_and_delete() {
        let store = MemoryKV::new();
        let manager = store.txn_manager();

        let stats = manager.memory_stats();
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.kv_bytes, 0);
        assert_eq!(stats.index_bytes, 0);

        // Insert a value and commit.
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(key("a"), value("1234")).unwrap(); // key=1, value=4 => 5 bytes
        manager.commit(txn).unwrap();

        let stats = manager.memory_stats();
        assert_eq!(stats.total_bytes, 5);
        assert_eq!(stats.kv_bytes, 5);
        assert_eq!(stats.index_bytes, 0);

        // Delete and ensure usage returns to zero.
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.delete(key("a")).unwrap();
        manager.commit(txn).unwrap();

        let stats = manager.memory_stats();
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.kv_bytes, 0);
    }

    #[test]
    fn memory_limit_error_does_not_break_reads() {
        let store = MemoryKV::new_with_limit(Some(10));
        let manager = store.txn_manager();

        // First insert within limit: key(2) + value(4) = 6.
        let mut txn = manager.begin_internal(TxnMode::ReadWrite).unwrap();
        txn.put(key("k1"), value("vvvv")).unwrap();
        manager.commit(txn).unwrap();

        // Next insert would exceed limit: key(2) + value(6) + existing(6) -> 14 > 10.
        let mut txn2 = manager.begin_internal(TxnMode::ReadWrite).unwrap();
        txn2.put(key("k2"), value("vvvvvv")).unwrap();
        let result = manager.commit(txn2);
        assert!(matches!(result, Err(Error::MemoryLimitExceeded { .. })));

        // Read still works and existing data intact.
        let mut read_txn = manager.begin_internal(TxnMode::ReadOnly).unwrap();
        let got = read_txn.get(&key("k1")).unwrap();
        assert_eq!(got, Some(value("vvvv")));

        // Memory usage stays at the previous successful commit.
        let stats = manager.memory_stats();
        assert_eq!(stats.total_bytes, 6);
    }

    struct VecWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn compaction_skips_when_over_limit_and_logs_warning() {
        let store = MemoryKV::new_with_limit(Some(12));
        let manager = store.txn_manager();

        // Populate data to track current memory: key(2)+val(6)=8 bytes.
        let mut txn = manager.begin_internal(TxnMode::ReadWrite).unwrap();
        txn.put(key("k1"), value("123456")).unwrap();
        manager.commit(txn).unwrap();

        // Prepare log capture.
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let make_writer = {
            let buf = buffer.clone();
            move || VecWriter(buf.clone())
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_writer(make_writer)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        // input=2 (assume one entry), output=10 => projected 8-2+10=16 > 12 -> skip.
        let ran = manager.compact_with_limit(2, 10, || Ok(())).unwrap();
        assert!(!ran);

        // Memory usage unchanged.
        assert_eq!(manager.memory_stats().total_bytes, 8);

        // Verify warning was logged.
        let log = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(
            log.contains("compaction skipped due to memory limit"),
            "expected warning log, got: {}",
            log
        );
    }
}
