//! An in-memory key-value store implementation with Write-Ahead Logging
//! and Optimistic Concurrency Control for Snapshot Isolation.

use crate::error::{Error, Result};
use crate::kv::{KVStore, KVTransaction};
use crate::log::wal::{WalReader, WalRecord, WalWriter};
use crate::storage::sstable::{SstableReader, SstableWriter};
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, TxnState, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
}

/// A transaction manager backed by an in-memory map and optional WAL.
pub struct MemoryTxnManager {
    state: Arc<MemorySharedState>,
}

impl MemoryTxnManager {
    fn new(
        wal_writer: Option<WalWriter>,
        wal_path: Option<PathBuf>,
        sstable_path: Option<PathBuf>,
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
            }),
        }
    }

    /// Flushes the current in-memory data to an SSTable file.
    pub fn flush(&self) -> Result<()> {
        let path = self
            .state
            .sstable_path
            .as_ref()
            .ok_or_else(|| Error::InvalidFormat("sstable path is not configured".into()))?;

        let data = self.state.data.read().unwrap();
        let mut writer = SstableWriter::create(path)?;
        for (key, (value, _version)) in data.iter() {
            writer.append(key, value)?;
        }
        drop(data);

        let _footer = writer.finish()?;
        let reader = SstableReader::open(path)?;
        let mut slot = self.state.sstable.write().unwrap();
        *slot = Some(reader);
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

        for (key, _read_version) in &txn.read_set {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > txn.start_version {
                return Err(Error::TxnConflict);
            }
        }

        // Detect write-write conflicts even when the key was never read.
        for (key, _) in &txn.writes {
            let current_version = data.get(key).map(|(_, v)| *v).unwrap_or(0);
            if current_version > txn.start_version {
                return Err(Error::TxnConflict);
            }
        }

        let commit_version = self.state.commit_version.fetch_add(1, Ordering::AcqRel) + 1;

        if let Some(wal_lock) = &self.state.wal_writer {
            let mut wal = wal_lock.write().unwrap();
            wal.append(&WalRecord::Begin(txn.id))?;
            for (key, value) in &txn.writes {
                let record = match value {
                    Some(v) => WalRecord::Put(txn.id, key.clone(), v.clone()),
                    None => WalRecord::Delete(txn.id, key.clone()),
                };
                wal.append(&record)?;
            }
            wal.append(&WalRecord::Commit(txn.id))?;
        }

        for (key, value) in std::mem::take(&mut txn.writes) {
            if let Some(v) = value {
                data.insert(key, (v, commit_version));
            } else {
                data.remove(&key);
            }
        }

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
}
