//! ディスク永続化向けの LSM-Tree 実装。
//!
//! このモジュールは「単一 `.alopex` ファイル」方針の Disk モード向けに、WAL / MemTable /
//! SSTable / Compaction を統合する `LsmKV` の土台を提供する。
//!
//! 仕様: `docs-internal/specs/lsm-tree-file-mode-spec.md`

pub mod buffer_pool;
pub mod free_space;
pub mod memtable;
pub mod sstable;
pub mod wal;

use std::collections::VecDeque;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::compaction::leveled::{LeveledCompactionConfig, SSTableMeta};
use crate::error::{Error, Result};
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::buffer_pool::{BufferPool, BufferPoolConfig};
use crate::lsm::memtable::{ImmutableMemTable, MemTable, MemTableConfig};
use crate::lsm::sstable::SSTableConfig;
use crate::lsm::wal::{SyncMode, WalConfig, WalEntryPayload, WalReader, WalWriter};
use crate::storage::format::WriteThrottleConfig;
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, TxnState, Value};

/// スレッドアクセスモード。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadMode {
    /// マルチスレッド同時アクセス（デフォルト）。
    MultiThread,
    /// シングルスレッド専有アクセス（ロックオーバーヘッド最小）。
    SingleThread,
}

/// LSM-Tree の設定。
#[derive(Debug, Clone)]
pub struct LsmKVConfig {
    /// WAL 設定。
    pub wal: WalConfig,
    /// MemTable 設定。
    pub memtable: MemTableConfig,
    /// SSTable 設定。
    pub sstable: SSTableConfig,
    /// Compaction 設定。
    pub compaction: LeveledCompactionConfig,
    /// バッファプール設定。
    pub buffer_pool: BufferPoolConfig,
    /// スレッドモード。
    pub thread_mode: ThreadMode,
    /// 書き込みスロットリング設定。
    pub write_throttle: WriteThrottleConfig,
}

impl Default for LsmKVConfig {
    fn default() -> Self {
        let wal = WalConfig {
            sync_mode: SyncMode::BatchSync {
                max_batch_size: 1024,
                max_wait_ms: 10,
            },
            ..WalConfig::default()
        };

        // 仕様書のデフォルトは LZ4 だが、機能フラグ未指定でもコンパイルできるように分岐する。
        #[cfg(feature = "compression-lz4")]
        let sstable = SSTableConfig {
            compression: crate::lsm::sstable::CompressionType::Lz4,
            ..SSTableConfig::default()
        };
        #[cfg(not(feature = "compression-lz4"))]
        let sstable = SSTableConfig::default();

        Self {
            wal,
            memtable: MemTableConfig::default(),
            sstable,
            compaction: LeveledCompactionConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
            thread_mode: ThreadMode::MultiThread,
            write_throttle: WriteThrottleConfig::default(),
        }
    }
}

/// タイムスタンプ生成器（単調増加）。
#[derive(Debug)]
pub struct TimestampOracle {
    next: AtomicU64,
}

impl TimestampOracle {
    /// 新しいオラクルを作成する。
    pub fn new(start: u64) -> Self {
        Self {
            next: AtomicU64::new(start),
        }
    }

    /// 新しいタイムスタンプを発行する。
    pub fn next_timestamp(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

/// LSM 用トランザクションマネージャ（詳細はタスク 3.3 で実装）。
#[derive(Debug)]
pub struct LsmTxnManager {
    next_txn_id: AtomicU64,
}

impl Default for LsmTxnManager {
    fn default() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
        }
    }
}

#[derive(Debug, Clone, Copy)]
/// `LsmKV` に紐づくトランザクションマネージャの参照。
pub struct LsmTxnManagerRef<'a> {
    store: &'a LsmKV,
}

impl<'a> LsmTxnManagerRef<'a> {
    fn allocate_txn_id(&self) -> TxnId {
        TxnId(
            self.store
                .txn_manager
                .next_txn_id
                .fetch_add(1, Ordering::Relaxed),
        )
    }
}

/// LSM-Tree ベースの KV ストア（Disk モード）。
///
/// 設計: 仕様書 §4.1
#[derive(Debug)]
pub struct LsmKV {
    /// 設定。
    pub config: LsmKVConfig,
    /// データディレクトリ。
    pub data_dir: PathBuf,
    /// WAL ファイルパス。
    pub wal_path: PathBuf,
    /// WAL Writer。
    pub wal: RwLock<WalWriter>,
    /// アクティブ MemTable。
    pub active_memtable: RwLock<MemTable>,
    /// Immutable MemTable キュー。
    pub immutable_memtables: RwLock<VecDeque<Arc<ImmutableMemTable>>>,
    /// レベル別 SSTable 一覧（コンパクションの単位）。
    pub levels: RwLock<Vec<Vec<SSTableMeta>>>,
    /// SSTable データブロックのバッファプール。
    pub buffer_pool: BufferPool,
    /// タイムスタンプオラクル。
    pub ts_oracle: TimestampOracle,
    /// トランザクションマネージャ。
    pub txn_manager: LsmTxnManager,
}

impl LsmKV {
    /// LsmKV をデフォルト設定で開く。
    ///
    /// `path` はデータディレクトリとして扱い、内部で WAL ファイルを作成/再利用する。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, LsmKVConfig::default())
    }

    /// 設定付きで LsmKV を開く。
    ///
    /// 既存 WAL がある場合は WAL をリプレイして MemTable を復元する（クラッシュリカバリ）。
    pub fn open_with_config(path: impl AsRef<Path>, config: LsmKVConfig) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        let wal_path = data_dir.join("lsm.wal");

        let (wal_writer, recovered, next_ts) = if wal_path.exists() {
            let mut reader = WalReader::open(&wal_path, config.wal.clone())?;
            let replay = reader.replay()?;
            let mut mem = MemTable::new();
            let last_lsn = apply_wal_replay(&mut mem, &replay.entries);
            let next = last_lsn.saturating_add(1).max(1);
            (WalWriter::open(&wal_path, config.wal.clone())?, mem, next)
        } else {
            (
                WalWriter::create(&wal_path, config.wal.clone(), 1, 1)?,
                MemTable::new(),
                1,
            )
        };

        let levels = vec![Vec::new(); config.compaction.max_levels];

        Ok(Self {
            wal: RwLock::new(wal_writer),
            active_memtable: RwLock::new(recovered),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(config.buffer_pool),
            ts_oracle: TimestampOracle::new(next_ts),
            txn_manager: LsmTxnManager::default(),
            data_dir,
            wal_path,
            config,
        })
    }

    /// MemTable をフラッシュする（手動）。
    ///
    /// 現段階では「Active MemTable を freeze して Immutable キューへ移す」までを行う。
    pub fn flush(&self) -> Result<()> {
        let old = {
            let mut guard = self
                .active_memtable
                .write()
                .expect("lsm active_memtable lock poisoned");
            std::mem::take(&mut *guard)
        };
        let imm = Arc::new(old.freeze());

        let mut queue = self
            .immutable_memtables
            .write()
            .expect("lsm immutable_memtables lock poisoned");
        queue.push_back(imm);

        while queue.len() > self.config.memtable.max_immutable_count {
            queue.pop_front();
        }
        Ok(())
    }

    /// コンパクションを実行する（手動）。
    ///
    /// 現段階ではメタデータ更新までの配線は未実装のため、no-op とする。
    pub fn compact(&self) -> Result<()> {
        Ok(())
    }

    /// メトリクスを取得する。
    pub fn metrics(&self) -> LsmMetrics {
        let wal_bytes = fs::metadata(&self.wal_path).map(|m| m.len()).unwrap_or(0);
        let active_memtable_bytes = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .memory_usage_bytes();

        let imm = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        let immutable_memtables = imm.len();
        let immutable_memtable_bytes = imm.iter().map(|t| t.memory_usage_bytes()).sum();

        LsmMetrics {
            wal_bytes,
            active_memtable_bytes,
            immutable_memtables,
            immutable_memtable_bytes,
            buffer_pool: self.buffer_pool.stats(),
        }
    }

    /// 推定ディスク使用量（バイト）を返す。
    pub fn disk_usage(&self) -> u64 {
        fs::metadata(&self.wal_path).map(|m| m.len()).unwrap_or(0)
    }

    fn get_visible_at(
        &self,
        key: &[u8],
        read_timestamp: u64,
    ) -> Option<crate::lsm::memtable::MemTableEntry> {
        if let Some(e) = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .get(key, read_timestamp)
        {
            return Some(e);
        }
        let imm = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        for t in imm.iter().rev() {
            if let Some(e) = t.get(key, read_timestamp) {
                return Some(e);
            }
        }
        None
    }

    fn latest_timestamp(&self, key: &[u8]) -> u64 {
        let mut best: Option<(u64, u64)> = None;
        if let Some(e) = self.get_visible_at(key, u64::MAX) {
            best = Some((e.timestamp, e.sequence));
        }
        match best {
            Some((ts, _seq)) => ts,
            None => 0,
        }
    }

    fn scan_prefix_visible(
        &self,
        prefix: &[u8],
        read_timestamp: u64,
    ) -> BTreeMap<Key, crate::lsm::memtable::MemTableEntry> {
        let mut out: BTreeMap<Key, crate::lsm::memtable::MemTableEntry> = BTreeMap::new();

        let active = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned");
        for (k, e) in active.scan_prefix(prefix, read_timestamp) {
            out.insert(k, e);
        }

        let imm = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        for t in imm.iter().rev() {
            for (k, e) in t.scan_prefix(prefix, read_timestamp) {
                match out.get(&k) {
                    None => {
                        out.insert(k, e);
                    }
                    Some(cur) => {
                        let better = (e.timestamp > cur.timestamp)
                            || (e.timestamp == cur.timestamp && e.sequence > cur.sequence);
                        if better {
                            out.insert(k, e);
                        }
                    }
                }
            }
        }

        out
    }

    fn scan_range_visible(
        &self,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> BTreeMap<Key, crate::lsm::memtable::MemTableEntry> {
        let mut out: BTreeMap<Key, crate::lsm::memtable::MemTableEntry> = BTreeMap::new();

        let active = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned");
        for (k, e) in active.scan_range(start, end, read_timestamp) {
            out.insert(k, e);
        }

        let imm = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        for t in imm.iter().rev() {
            for (k, e) in t.scan_range(start, end, read_timestamp) {
                match out.get(&k) {
                    None => {
                        out.insert(k, e);
                    }
                    Some(cur) => {
                        let better = (e.timestamp > cur.timestamp)
                            || (e.timestamp == cur.timestamp && e.sequence > cur.sequence);
                        if better {
                            out.insert(k, e);
                        }
                    }
                }
            }
        }

        out
    }
}

/// LsmKV のメトリクス（スナップショット）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmMetrics {
    /// WAL ファイルサイズ（バイト）。
    pub wal_bytes: u64,
    /// Active MemTable の推定メモリ使用量（バイト）。
    pub active_memtable_bytes: usize,
    /// Immutable MemTable の個数。
    pub immutable_memtables: usize,
    /// Immutable MemTable の推定メモリ使用量合計（バイト）。
    pub immutable_memtable_bytes: usize,
    /// バッファプール統計。
    pub buffer_pool: crate::lsm::buffer_pool::BufferPoolStatsSnapshot,
}

fn apply_wal_replay(mem: &mut MemTable, entries: &[crate::lsm::wal::WalEntry]) -> u64 {
    let mut last = 0u64;
    for e in entries {
        last = last.max(e.lsn);
        match &e.payload {
            WalEntryPayload::Put { key, value } => {
                mem.put(key.clone(), value.clone(), e.lsn, 0);
            }
            WalEntryPayload::Delete { key } => {
                mem.delete(key.clone(), e.lsn, 0);
            }
            WalEntryPayload::Batch(ops) => {
                let mut seq = 0u64;
                for op in ops {
                    match op.op_type {
                        crate::lsm::wal::WalOpType::Put => {
                            let val = op.value.clone().unwrap_or_default();
                            mem.put(op.key.clone(), val, e.lsn, seq);
                        }
                        crate::lsm::wal::WalOpType::Delete => {
                            mem.delete(op.key.clone(), e.lsn, seq);
                        }
                    }
                    seq = seq.wrapping_add(1);
                }
            }
        }
    }
    last
}

/// LSM 用トランザクション（スナップショット分離 + 書き込みバッファ）。
#[derive(Debug)]
pub struct LsmTransaction<'a> {
    manager: LsmTxnManagerRef<'a>,
    id: TxnId,
    mode: TxnMode,
    state: TxnState,
    read_timestamp: u64,
    writes: BTreeMap<Key, Option<Value>>,
    read_set: HashSet<Key>,
}

impl<'a> LsmTransaction<'a> {
    fn new(manager: LsmTxnManagerRef<'a>, id: TxnId, mode: TxnMode, read_timestamp: u64) -> Self {
        Self {
            manager,
            id,
            mode,
            state: TxnState::Active,
            read_timestamp,
            writes: BTreeMap::new(),
            read_set: HashSet::new(),
        }
    }

    fn ensure_active(&self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnClosed);
        }
        Ok(())
    }

    fn write_iter_prefix<'b>(
        &'b self,
        prefix: &'b [u8],
    ) -> impl Iterator<Item = (&'b Key, &'b Option<Value>)> + 'b {
        let prefix_vec = prefix.to_vec();
        self.writes
            .range(prefix_vec..)
            .take_while(move |(k, _)| k.starts_with(prefix))
    }
}

impl<'a> KVTransaction<'a> for LsmTransaction<'a> {
    fn id(&self) -> TxnId {
        self.id
    }

    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.ensure_active()?;

        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }

        self.read_set.insert(key.clone());
        let entry = self.manager.store.get_visible_at(key, self.read_timestamp);
        Ok(entry.and_then(|e| e.value))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.ensure_active()?;
        if self.mode == TxnMode::ReadOnly {
            return Err(Error::TxnConflict);
        }
        self.writes.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        self.ensure_active()?;
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
        let mut map: BTreeMap<Key, Option<Value>> = self
            .manager
            .store
            .scan_prefix_visible(prefix, self.read_timestamp)
            .into_iter()
            .map(|(k, e)| (k, e.value))
            .collect();

        // スナップショットで観測したキーは read_set に入れる（read-write conflict 検出の最低限）。
        self.read_set.extend(map.keys().cloned());

        let overlays: Vec<(Key, Option<Value>)> = self
            .write_iter_prefix(prefix)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, v) in overlays {
            self.read_set.insert(k.clone());
            match v {
                Some(val) => {
                    map.insert(k, Some(val));
                }
                None => {
                    map.remove(&k);
                }
            }
        }

        let iter = map.into_iter().filter_map(|(k, v)| v.map(|vv| (k, vv)));
        Ok(Box::new(iter))
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.ensure_active()?;

        let mut map: BTreeMap<Key, Option<Value>> = self
            .manager
            .store
            .scan_range_visible(start, end, self.read_timestamp)
            .into_iter()
            .map(|(k, e)| (k, e.value))
            .collect();

        // スナップショットで観測したキーは read_set に入れる（read-write conflict 検出の最低限）。
        self.read_set.extend(map.keys().cloned());

        let overlays: Vec<(Key, Option<Value>)> = self
            .writes
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, v) in overlays {
            self.read_set.insert(k.clone());
            match v {
                Some(val) => {
                    map.insert(k, Some(val));
                }
                None => {
                    map.remove(&k);
                }
            }
        }

        let iter = map.into_iter().filter_map(|(k, v)| v.map(|vv| (k, vv)));
        Ok(Box::new(iter))
    }

    fn commit_self(mut self) -> Result<()> {
        self.ensure_active()?;
        if self.mode == TxnMode::ReadOnly || self.writes.is_empty() {
            self.state = TxnState::Committed;
            return Ok(());
        }

        for key in self.read_set.iter() {
            if self.manager.store.latest_timestamp(key) > self.read_timestamp {
                return Err(Error::TxnConflict);
            }
        }
        for key in self.writes.keys() {
            if self.manager.store.latest_timestamp(key) > self.read_timestamp {
                return Err(Error::TxnConflict);
            }
        }

        let commit_ts = self.manager.store.ts_oracle.next_timestamp();
        let active = self
            .manager
            .store
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned");
        let mut seq = 1u64;
        for (k, v) in std::mem::take(&mut self.writes) {
            match v {
                Some(val) => active.put(k, val, commit_ts, seq),
                None => active.delete(k, commit_ts, seq),
            }
            seq = seq.wrapping_add(1);
        }

        self.state = TxnState::Committed;
        Ok(())
    }

    fn rollback_self(mut self) -> Result<()> {
        self.ensure_active()?;
        self.state = TxnState::RolledBack;
        Ok(())
    }
}

impl<'a> TxnManager<'a, LsmTransaction<'a>> for LsmTxnManagerRef<'a> {
    fn begin(&'a self, mode: TxnMode) -> Result<LsmTransaction<'a>> {
        let read_timestamp = self.store.ts_oracle.next_timestamp();
        Ok(LsmTransaction::new(
            *self,
            self.allocate_txn_id(),
            mode,
            read_timestamp,
        ))
    }

    fn commit(&'a self, txn: LsmTransaction<'a>) -> Result<()> {
        txn.commit_self()
    }

    fn rollback(&'a self, txn: LsmTransaction<'a>) -> Result<()> {
        txn.rollback_self()
    }
}

impl KVStore for LsmKV {
    type Transaction<'a>
        = LsmTransaction<'a>
    where
        Self: 'a;
    type Manager<'a>
        = LsmTxnManagerRef<'a>
    where
        Self: 'a;

    fn txn_manager(&self) -> Self::Manager<'_> {
        LsmTxnManagerRef { store: self }
    }

    fn begin(&self, mode: TxnMode) -> Result<Self::Transaction<'_>> {
        let manager = LsmTxnManagerRef { store: self };
        let read_timestamp = self.ts_oracle.next_timestamp();
        Ok(LsmTransaction::new(
            manager,
            manager.allocate_txn_id(),
            mode,
            read_timestamp,
        ))
    }
}

#[cfg(test)]
mod kv_store {
    use super::*;

    fn test_config() -> LsmKVConfig {
        let mut cfg = LsmKVConfig::default();
        cfg.wal = WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        };
        cfg
    }

    fn new_test_store() -> LsmKV {
        let cfg = test_config();
        let data_dir = tempfile::tempdir().expect("tempdir").keep();
        let wal_path = data_dir.join("lsm.wal");
        let wal = WalWriter::create(&wal_path, cfg.wal.clone(), 1, 1).expect("wal create");

        let levels = vec![Vec::new(); cfg.compaction.max_levels];
        LsmKV {
            config: cfg,
            data_dir,
            wal_path,
            wal: RwLock::new(wal),
            active_memtable: RwLock::new(MemTable::new()),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(BufferPoolConfig::default()),
            ts_oracle: TimestampOracle::new(1),
            txn_manager: LsmTxnManager::default(),
        }
    }

    #[test]
    fn commit_makes_writes_visible() {
        let store = new_test_store();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
        tx.commit_self().unwrap();

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn rollback_discards_writes() {
        let store = new_test_store();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.rollback_self().unwrap();

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), None);
    }

    #[test]
    fn read_only_rejects_writes() {
        let store = new_test_store();
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        assert!(tx.put(b"k".to_vec(), b"v".to_vec()).is_err());
    }

    #[test]
    fn detects_write_conflict() {
        let store = new_test_store();
        let mut a = store.begin(TxnMode::ReadWrite).unwrap();
        let mut b = store.begin(TxnMode::ReadWrite).unwrap();

        a.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        a.commit_self().unwrap();

        b.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        assert!(b.commit_self().is_err());
    }

    #[test]
    fn scan_populates_read_set_for_conflict_detection() {
        let store = new_test_store();

        let mut init = store.begin(TxnMode::ReadWrite).unwrap();
        init.put(b"p:a".to_vec(), b"v1".to_vec()).unwrap();
        init.commit_self().unwrap();

        let mut scan_tx = store.begin(TxnMode::ReadWrite).unwrap();
        let got: Vec<(Key, Value)> = scan_tx.scan_prefix(b"p:").unwrap().collect();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, b"p:a".to_vec());
        assert_eq!(got[0].1, b"v1".to_vec());

        let mut updater = store.begin(TxnMode::ReadWrite).unwrap();
        updater.put(b"p:a".to_vec(), b"v2".to_vec()).unwrap();
        updater.commit_self().unwrap();

        scan_tx.put(b"q:z".to_vec(), b"ok".to_vec()).unwrap();
        assert!(scan_tx.commit_self().is_err());
    }
}

#[cfg(test)]
mod methods {
    use super::*;

    fn test_config() -> LsmKVConfig {
        let mut cfg = LsmKVConfig::default();
        cfg.wal = WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        };
        cfg
    }

    #[test]
    fn open_creates_wal_and_returns_metrics() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LsmKV::open_with_config(dir.path(), test_config()).expect("open");
        let m = store.metrics();
        assert!(m.wal_bytes > 0);
        assert_eq!(m.immutable_memtables, 0);
        assert_eq!(store.disk_usage(), m.wal_bytes);
    }

    #[test]
    fn open_replays_wal_entries() {
        let dir = tempfile::tempdir().expect("tempdir");

        {
            let store = LsmKV::open_with_config(dir.path(), test_config()).expect("open");
            let mut wal = store.wal.write().unwrap();
            wal.append(&crate::lsm::wal::WalEntry::put(
                10,
                b"k".to_vec(),
                b"v".to_vec(),
            ))
            .unwrap();
        }

        let store = LsmKV::open_with_config(dir.path(), test_config()).expect("reopen");
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn flush_moves_active_to_immutable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LsmKV::open_with_config(dir.path(), test_config()).expect("open");

        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        store.flush().unwrap();

        let m = store.metrics();
        assert_eq!(m.immutable_memtables, 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }
}
