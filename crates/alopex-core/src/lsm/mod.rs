//! ディスク永続化向けの LSM-Tree 実装。
//!
//! このモジュールは「単一 `.alopex` ファイル」方針の Disk モード向けに、WAL / MemTable /
//! SSTable / Compaction を統合する `LsmKV` の土台を提供する。
//!
//! 仕様: `docs-internal/specs/lsm-tree-file-mode-spec.md`

pub mod buffer_pool;
pub mod checkpoint;
pub mod container;
pub mod free_space;
pub mod memtable;
pub mod metrics;
pub mod sstable;
pub mod wal;

use std::collections::VecDeque;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::compaction::leveled::{KeyRange, LeveledCompactionConfig, SSTableMeta};
use crate::error::{Error, Result};
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::buffer_pool::{BufferPool, BufferPoolConfig};
use crate::lsm::checkpoint::{load_checkpoint_meta, save_checkpoint_meta, CheckpointMeta};
pub use crate::lsm::container::{ConvergePolicy, ConvergeResult};
use crate::lsm::memtable::{ImmutableMemTable, MemTable, MemTableConfig, MemTableEntry};
use crate::lsm::metrics::{LsmMetrics, LsmMetricsSnapshot};
use crate::lsm::sstable::{
    SSTableConfig, SSTableCursor, SSTableEntry, SSTableReader, SSTableWriter,
};
use crate::lsm::wal::{
    detect_wal_format_version, SyncMode, WalBatchOp, WalConfig, WalEntry, WalEntryPayload,
    WalOpType, WalReader, WalWriter, WAL_FORMAT_VERSION,
};
use crate::storage::format::WriteThrottleConfig;
use crate::txn::TxnManager;
use crate::types::{Key, TxnId, TxnMode, Value};
use tracing::{info, warn};

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
    /// チェックポイント設定。
    pub checkpoint: CheckpointConfig,
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
    /// 単一 `.alopex` ファイルへの収束ポリシー。
    ///
    /// 既定の [`ConvergePolicy::SidecarOnly`] は `X.alopex.d` サイドカーだけを対象に
    /// するため、素のディレクトリ運用（`alopex-server` を含む）の挙動は変わらない。
    pub converge: ConvergePolicy,
    /// ハンドル解放時にサイドカー作業ディレクトリを削除するか。
    pub prune_sidecar_on_drop: bool,
}

/// チェックポイント設定。
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// WAL サイズ閾値（バイト）。
    pub wal_size_threshold: u64,
    /// 最小チェックポイント間隔（ms）。
    pub min_interval_ms: u64,
    /// 自動チェックポイントを有効にするか。
    pub auto_checkpoint: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            wal_size_threshold: 64 * 1024 * 1024,
            min_interval_ms: 60_000,
            auto_checkpoint: true,
        }
    }
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
            checkpoint: CheckpointConfig::default(),
            memtable: MemTableConfig::default(),
            sstable,
            compaction: LeveledCompactionConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
            thread_mode: ThreadMode::MultiThread,
            write_throttle: WriteThrottleConfig::default(),
            converge: ConvergePolicy::default(),
            prune_sidecar_on_drop: true,
        }
    }
}

/// WAL リカバリの診断結果。
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// リカバリで適用したエントリ数。
    pub entries_recovered: usize,
    /// 最後に適用した LSN。
    pub last_lsn: u64,
    /// 非致命的な警告メッセージ。
    pub warnings: Vec<String>,
    /// リカバリ停止理由（ある場合）。
    pub stop_reason: Option<String>,
    /// チェックポイント LSN（利用時のみ）。
    pub checkpoint_lsn: Option<u64>,
}

/// 収束が O(N) コストとして目に見えるサイズの閾値（1 GiB）。
#[cfg(not(target_arch = "wasm32"))]
const CONVERGE_WARN_BYTES: u64 = 1024 * 1024 * 1024;

/// 同時に開いたままにする SSTable リーダーの上限（ファイルディスクリプタ保護）。
const SSTABLE_READER_CACHE_LIMIT: usize = 256;

/// Cache of opened SSTable readers, keyed by file id.
///
/// [`SSTableReader::open`] re-reads the whole file to verify its CRC32, so
/// opening one per point lookup makes a single `get` cost the total size of every
/// live SSTable. Tables are immutable once written and file ids are never reused,
/// so a cached reader can never go stale; the only bound needed is on open file
/// descriptors.
#[derive(Default)]
struct SSTableReaderCache {
    readers: Mutex<HashMap<u64, Arc<Mutex<SSTableReader>>>>,
}

impl SSTableReaderCache {
    fn get_or_open(&self, file_id: u64, path: &Path) -> Result<Arc<Mutex<SSTableReader>>> {
        {
            let cache = self
                .readers
                .lock()
                .expect("lsm sstable reader cache poisoned");
            if let Some(reader) = cache.get(&file_id) {
                return Ok(Arc::clone(reader));
            }
        }

        let opened = Arc::new(Mutex::new(SSTableReader::open(path)?));
        let mut cache = self
            .readers
            .lock()
            .expect("lsm sstable reader cache poisoned");
        if cache.len() >= SSTABLE_READER_CACHE_LIMIT {
            cache.clear();
        }
        Ok(Arc::clone(cache.entry(file_id).or_insert(opened)))
    }
}

impl std::fmt::Debug for SSTableReaderCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open_readers = self.readers.lock().map(|cache| cache.len()).unwrap_or(0);
        f.debug_struct("SSTableReaderCache")
            .field("open_readers", &open_readers)
            .finish()
    }
}

/// チェックポイント実行結果。
#[derive(Debug, Clone)]
pub struct CheckpointResult {
    /// Checkpoint LSN captured during the run.
    pub checkpoint_lsn: u64,
    /// WAL bytes reclaimed by advancing the start offset.
    pub wal_bytes_reclaimed: u64,
    /// Total checkpoint duration in milliseconds.
    pub duration_ms: u64,
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

    /// Latest issued timestamp without incrementing.
    pub fn current_timestamp(&self) -> u64 {
        self.next.load(Ordering::Relaxed).saturating_sub(1)
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

/// Bounds shared by an owned LSM cursor and its per-SSTable cursors.
///
/// The type contains only key boundaries, not a materialized result set.  Its `end` is exclusive
/// when present; an all-`0xff` prefix has no finite lexical successor and therefore no end.
#[derive(Debug, Clone)]
pub(crate) struct OwnedLsmScanBounds {
    start: Key,
    end: Option<Key>,
    prefix: Option<Key>,
}

impl OwnedLsmScanBounds {
    /// Construct a prefix bound.
    pub(crate) fn prefix(prefix: Key) -> Self {
        let end = lexical_prefix_end(&prefix);
        Self {
            start: prefix.clone(),
            end,
            prefix: Some(prefix),
        }
    }

    /// Construct a half-open range bound.
    pub(crate) fn range(start: Key, end: Key) -> Self {
        Self {
            start,
            end: Some(end),
            prefix: None,
        }
    }

    /// Inclusive start key.
    pub(crate) fn start(&self) -> &Key {
        &self.start
    }

    /// Exclusive end key, when the range has a finite end.
    pub(crate) fn end(&self) -> Option<&Key> {
        self.end.as_ref()
    }

    /// Optional prefix constraint.
    pub(crate) fn prefix_constraint(&self) -> Option<&Key> {
        self.prefix.as_ref()
    }

    /// Whether `key` belongs to this cursor's result domain.
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice()
            && self.end.as_ref().is_none_or(|end| key < end.as_slice())
            && self
                .prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }
}

fn lexical_prefix_end(prefix: &[u8]) -> Option<Key> {
    if prefix.is_empty() {
        return None;
    }
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] = end[index].saturating_add(1);
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

/// Reader/writer gate that keeps an owned cursor's LSM source layout stable.
///
/// It is deliberately not an `RwLockReadGuard`: cursors own this small guard without borrowing
/// the store, while legacy and owned writers wait before replacing/flushing source structures.
#[derive(Debug, Default)]
struct OwnedLsmSnapshotGate {
    state: Mutex<OwnedLsmSnapshotGateState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct OwnedLsmSnapshotGateState {
    readers: usize,
    writer: bool,
}

impl OwnedLsmSnapshotGate {
    fn acquire_reader(self: &Arc<Self>) -> OwnedLsmSnapshotReader {
        let mut state = self.state.lock().expect("lsm snapshot gate mutex poisoned");
        while state.writer {
            state = self
                .changed
                .wait(state)
                .expect("lsm snapshot gate mutex poisoned");
        }
        state.readers = state.readers.saturating_add(1);
        OwnedLsmSnapshotReader {
            gate: self.clone(),
            released: false,
        }
    }

    fn acquire_writer(self: &Arc<Self>) -> OwnedLsmSnapshotWriter {
        let mut state = self.state.lock().expect("lsm snapshot gate mutex poisoned");
        while state.writer || state.readers != 0 {
            state = self
                .changed
                .wait(state)
                .expect("lsm snapshot gate mutex poisoned");
        }
        state.writer = true;
        OwnedLsmSnapshotWriter {
            gate: self.clone(),
            released: false,
        }
    }
}

/// Owned cursor's source-stability token.
pub(crate) struct OwnedLsmSnapshotReader {
    gate: Arc<OwnedLsmSnapshotGate>,
    released: bool,
}

impl Drop for OwnedLsmSnapshotReader {
    fn drop(&mut self) {
        if !self.released {
            let mut state = self
                .gate
                .state
                .lock()
                .expect("lsm snapshot gate mutex poisoned");
            state.readers = state.readers.saturating_sub(1);
            self.released = true;
            self.gate.changed.notify_all();
        }
    }
}

struct OwnedLsmSnapshotWriter {
    gate: Arc<OwnedLsmSnapshotGate>,
    released: bool,
}

impl Drop for OwnedLsmSnapshotWriter {
    fn drop(&mut self) {
        if !self.released {
            let mut state = self
                .gate
                .state
                .lock()
                .expect("lsm snapshot gate mutex poisoned");
            state.writer = false;
            self.released = true;
            self.gate.changed.notify_all();
        }
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
    /// SSTable ディレクトリ。
    pub sst_dir: PathBuf,
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
    /// 開いたままにする SSTable リーダー（file_id -> reader）。
    sstable_readers: SSTableReaderCache,
    /// メトリクス（Atomic カウンタ）。
    pub metrics: Arc<LsmMetrics>,
    /// タイムスタンプオラクル。
    pub ts_oracle: TimestampOracle,
    /// トランザクションマネージャ。
    pub txn_manager: LsmTxnManager,
    /// コミットの直列化ロック（OCC の検証ウィンドウを閉じる）。
    pub commit_lock: Mutex<()>,
    /// Source-layout stability gate for owned incremental cursors.
    owned_snapshot_gate: Arc<OwnedLsmSnapshotGate>,
    /// 次に割り当てる SSTable ID。
    pub next_sstable_id: AtomicU64,
    /// WAL の現在使用量（バイト）。
    pub wal_used_bytes: AtomicU64,
    /// 最終チェックポイント時刻（epoch ms）。
    pub last_checkpoint_ms: AtomicU64,
    /// 収束先の `.alopex` コンテナ（対象外なら `None`）。
    container_path: Option<PathBuf>,
    /// 直近の収束以降にコミットがあったか。
    dirty: AtomicBool,
    /// コンテナが現在の状態を完全に含んでいるか。
    container_current: AtomicBool,
    /// `close()` 済みか（多重 close と Drop 時の二重収束を防ぐ）。
    closed: AtomicBool,
}

impl LsmKV {
    /// Allocate a transaction id for an owned local session without borrowing this store.
    pub(crate) fn allocate_owned_transaction_id(&self) -> TxnId {
        TxnId(self.txn_manager.next_txn_id.fetch_add(1, Ordering::Relaxed))
    }

    /// LsmKV をデフォルト設定で開く。
    ///
    /// `path` はデータディレクトリとして扱い、内部で WAL ファイルを作成/再利用する。
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let (store, _recovery) = Self::open_with_config(path, LsmKVConfig::default())?;
        Ok(store)
    }

    /// 設定付きで LsmKV を開く。
    ///
    /// 既存 WAL がある場合は WAL をリプレイして MemTable を復元する（クラッシュリカバリ）。
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: LsmKVConfig,
    ) -> Result<(Self, RecoveryResult)> {
        let data_dir = path.as_ref().to_path_buf();
        let metrics = Arc::new(LsmMetrics::default());
        let container_path = Self::restore_from_container(&data_dir, &config, &metrics)?;

        fs::create_dir_all(&data_dir)?;
        let wal_path = data_dir.join(container::SIDECAR_WAL_FILE);
        let sst_dir = data_dir.join(container::SIDECAR_SST_DIR);
        fs::create_dir_all(&sst_dir)?;
        let checkpoint_path = data_dir.join(container::SIDECAR_CHECKPOINT_FILE);

        let (wal_writer, recovered, next_ts, recovery, last_checkpoint_ms) = if wal_path.exists() {
            let wal_version = detect_wal_format_version(&wal_path, &config.wal)?;
            if wal_version < WAL_FORMAT_VERSION {
                Self::migrate_legacy_wal(&wal_path, &checkpoint_path, &config.wal)?;
            }
            let start = Instant::now();
            let checkpoint = load_checkpoint_meta(&checkpoint_path)?;
            let checkpoint_lsn = checkpoint.as_ref().map(|meta| meta.checkpoint_lsn);
            let last_checkpoint_ms = checkpoint.as_ref().map(|meta| meta.created_at).unwrap_or(0);
            let mut reader = WalReader::open(&wal_path, config.wal.clone())?;
            let replay = reader.replay()?;
            let mut mem = MemTable::new();
            let entries: Vec<_> = if let Some(start_lsn) = checkpoint_lsn {
                replay
                    .entries
                    .into_iter()
                    .filter(|entry| entry.lsn > start_lsn)
                    .collect()
            } else {
                replay.entries
            };
            let mut last_lsn = apply_wal_replay(&mut mem, &entries);
            if let Some(start_lsn) = checkpoint_lsn {
                last_lsn = last_lsn.max(start_lsn);
            }
            let next = last_lsn.saturating_add(1).max(1);
            for warning in &replay.warnings {
                warn!(warning = %warning, "WAL recovery warning");
            }
            if let Some(reason) = &replay.stop_reason {
                warn!(stop_reason = %reason, "WAL recovery stopped early");
            }

            let stopped_at = replay.stopped_at;
            let recovery = RecoveryResult {
                entries_recovered: entries.len(),
                last_lsn,
                warnings: replay.warnings,
                stop_reason: replay.stop_reason,
                checkpoint_lsn,
            };
            let duration_ms = start.elapsed().as_millis() as u64;
            info!(
                entries_recovered = recovery.entries_recovered,
                checkpoint_lsn = ?recovery.checkpoint_lsn,
                duration_ms,
                "WAL recovery completed"
            );
            let mut wal_writer = WalWriter::open(&wal_path, config.wal.clone())?;
            if let Some(valid_end) = stopped_at {
                wal_writer.truncate_tail_to(valid_end)?;
            }
            (wal_writer, mem, next, recovery, last_checkpoint_ms)
        } else {
            // A missing WAL does not mean a fresh database: a checkpoint may have
            // persisted everything into SSTables already. Resuming the timestamp
            // oracle at 1 would hand out commit timestamps below the timestamps
            // already stored in those SSTables, which surfaces as spurious
            // `TxnConflict`s and reads that return stale values.
            let checkpoint = load_checkpoint_meta(&checkpoint_path)?;
            let checkpoint_lsn = checkpoint.as_ref().map(|meta| meta.checkpoint_lsn);
            let last_checkpoint_ms = checkpoint.as_ref().map(|meta| meta.created_at).unwrap_or(0);
            let next = checkpoint_lsn.unwrap_or(0).saturating_add(1).max(1);
            (
                WalWriter::create(&wal_path, config.wal.clone(), 1, next)?,
                MemTable::new(),
                next,
                RecoveryResult {
                    entries_recovered: 0,
                    last_lsn: checkpoint_lsn.unwrap_or(0),
                    warnings: Vec::new(),
                    stop_reason: None,
                    checkpoint_lsn,
                },
                last_checkpoint_ms,
            )
        };
        let wal_used_bytes = wal_writer.used_bytes();
        let next_sstable_id = next_sstable_id_from_dir(&sst_dir)?;
        let levels = load_sstable_levels(&sst_dir, config.compaction.max_levels)?;

        let store = Self {
            wal: RwLock::new(wal_writer),
            active_memtable: RwLock::new(recovered),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(config.buffer_pool),
            sstable_readers: SSTableReaderCache::default(),
            metrics: Arc::clone(&metrics),
            ts_oracle: TimestampOracle::new(next_ts),
            txn_manager: LsmTxnManager::default(),
            commit_lock: Mutex::new(()),
            owned_snapshot_gate: Arc::new(OwnedLsmSnapshotGate::default()),
            next_sstable_id: AtomicU64::new(next_sstable_id),
            wal_used_bytes: AtomicU64::new(wal_used_bytes),
            last_checkpoint_ms: AtomicU64::new(last_checkpoint_ms),
            container_path,
            dirty: AtomicBool::new(false),
            container_current: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            data_dir,
            sst_dir,
            wal_path,
            config,
        };
        store.refresh_memtable_size_metrics();
        Ok((store, recovery))
    }

    /// Decide between the sidecar working directory and the `.alopex` container,
    /// rebuilding the sidecar from the container when the container is the only
    /// authoritative copy left.
    ///
    /// The rule is one line (裁定 D7): **`X.alopex.d/lsm.wal` exists ⇒ the sidecar
    /// wins**. A container is consulted only when no live sidecar is present, so
    /// crash recovery behaves exactly as it did before this file existed.
    #[cfg(not(target_arch = "wasm32"))]
    fn restore_from_container(
        data_dir: &Path,
        config: &LsmKVConfig,
        metrics: &LsmMetrics,
    ) -> Result<Option<PathBuf>> {
        let Some(container_path) = container::container_path_for(data_dir, &config.converge) else {
            return Ok(None);
        };
        if container::sidecar_is_live(data_dir) {
            // Two-file steady state while running: the sidecar is authoritative.
            return Ok(Some(container_path));
        }
        if container::is_legacy_marker(&container_path) {
            // Either no container at all, or the v0.8.6 zero-byte marker. Both mean
            // "there is nothing to restore"; fall through to the normal open path.
            return Ok(Some(container_path));
        }
        if data_dir.exists() {
            // A sidecar without a WAL is the debris of an interrupted prune. The
            // container already supersedes it, so discard it before rebuilding.
            container::discard_dead_sidecar(data_dir);
        }
        fs::create_dir_all(data_dir)?;
        let outcome = container::rehydrate(&container_path, data_dir, &config.wal)?;
        metrics.add_rehydrate_bytes_read(outcome.bytes_read);
        info!(
            container = ?container_path,
            tables_restored = outcome.tables_restored,
            converged_lsn = outcome.converged_lsn,
            bytes_read = outcome.bytes_read,
            "Rehydrated a sidecar working directory from a single .alopex container"
        );
        Ok(Some(container_path))
    }

    /// WASM has no container writer, so nothing ever converges there.
    #[cfg(target_arch = "wasm32")]
    fn restore_from_container(
        _data_dir: &Path,
        _config: &LsmKVConfig,
        _metrics: &LsmMetrics,
    ) -> Result<Option<PathBuf>> {
        Ok(None)
    }

    /// The `.alopex` container this store converges into, if any.
    pub fn container_path(&self) -> Option<&Path> {
        self.container_path.as_deref()
    }

    fn migrate_legacy_wal(
        wal_path: &Path,
        checkpoint_path: &Path,
        config: &WalConfig,
    ) -> Result<()> {
        let mut reader = WalReader::open_allow_legacy(wal_path, config.clone())?;
        let replay = reader.replay()?;
        for warning in &replay.warnings {
            warn!(warning = %warning, "Legacy WAL replay warning during migration");
        }
        if let Some(reason) = &replay.stop_reason {
            warn!(
                stop_reason = %reason,
                "Legacy WAL replay stopped early during migration"
            );
        }

        let entries = replay.entries;
        let first_lsn = entries.first().map(|entry| entry.lsn).unwrap_or(1);
        let temp_path = wal_path.with_extension("wal.migrate");
        let backup_path = wal_path.with_extension("wal.bak");

        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }

        let mut writer = WalWriter::create(&temp_path, config.clone(), 1, first_lsn)?;
        for entry in &entries {
            writer.append(entry)?;
        }
        writer.force_sync()?;
        drop(writer);

        if backup_path.exists() {
            fs::remove_file(&backup_path)?;
        }
        fs::rename(wal_path, &backup_path)?;
        fs::rename(&temp_path, wal_path)?;

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = CheckpointMeta::new(0, created_at);
        save_checkpoint_meta(checkpoint_path, &meta)?;

        info!(
            entries_migrated = entries.len(),
            backup = ?backup_path,
            "Legacy WAL migration completed"
        );

        Ok(())
    }

    /// MemTable をフラッシュする（手動）。
    ///
    /// Active MemTable を freeze し、Immutable キューに溜まった分を SSTable として
    /// 実際にディスクへ書き出す。WAL の truncate は行わない（それは
    /// [`Self::checkpoint`] / [`Self::converge`] の仕事）。
    pub fn flush(&self) -> Result<()> {
        let _snapshot_writer = self.owned_snapshot_gate.acquire_writer();
        self.flush_inner()?;
        self.persist_immutable_memtables()
    }

    /// Flush while the caller already owns the snapshot writer gate.
    fn flush_inner(&self) -> Result<()> {
        let old = {
            let mut guard = self
                .active_memtable
                .write()
                .expect("lsm active_memtable lock poisoned");
            std::mem::take(&mut *guard)
        };
        let imm = Arc::new(old.freeze());

        {
            let mut queue = self
                .immutable_memtables
                .write()
                .expect("lsm immutable_memtables lock poisoned");
            queue.push_back(imm);
        }
        self.metrics.inc_memtable_flush_count();
        self.refresh_memtable_size_metrics();

        // Overflowing immutable MemTables are *persisted*, never dropped. The
        // previous `pop_front()` silently discarded unpersisted data as soon as
        // more than `max_immutable_count` tables piled up.
        let limit = self.config.memtable.max_immutable_count;
        self.drain_immutables_while(move |len| len > limit)
    }

    /// 明示的にチェックポイントを作成する。
    pub fn checkpoint(&self) -> Result<CheckpointResult> {
        let _snapshot_writer = self.owned_snapshot_gate.acquire_writer();
        let _guard = self.commit_lock.lock().expect("lsm commit_lock poisoned");
        let start = Instant::now();

        self.flush_inner()?;
        self.persist_immutable_memtables()?;

        let checkpoint_lsn = self.ts_oracle.current_timestamp();
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = CheckpointMeta::new(checkpoint_lsn, created_at);
        let checkpoint_path = self.data_dir.join("checkpoint.meta");
        save_checkpoint_meta(&checkpoint_path, &meta)?;

        let mut wal = self.wal.write().expect("lsm wal lock poisoned");
        let wal_bytes_reclaimed = wal.used_bytes();
        let end_offset = wal.end_offset();
        wal.advance_start(end_offset)?;
        self.wal_used_bytes
            .store(wal.used_bytes(), Ordering::Relaxed);
        self.last_checkpoint_ms.store(created_at, Ordering::Relaxed);

        Ok(CheckpointResult {
            checkpoint_lsn,
            wal_bytes_reclaimed,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// 単一 `.alopex` ファイルへ収束する。
    ///
    /// 「安定状態」= すべての MemTable が SSTable 化され、その全量と manifest が
    /// `.alopex` コンテナ 1 本に入り、WAL が空になった状態。収束後の `.alopex` は
    /// 単体でコピーしても完全に復元できる（`docs-public/tech/file-format-comparison.md`
    /// L67「安定後は `.alopex` 単体で完全状態」）。
    ///
    /// 収束対象外（素のディレクトリ運用や [`ConvergePolicy::Never`]）の場合は
    /// MemTable の SSTable 化だけを行い、WAL には触れない。
    pub fn converge(&self) -> Result<ConvergeResult> {
        let _snapshot_writer = self.owned_snapshot_gate.acquire_writer();
        let _guard = self.commit_lock.lock().expect("lsm commit_lock poisoned");
        self.converge_locked()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn converge_locked(&self) -> Result<ConvergeResult> {
        let Some(container_path) = self.container_path.as_ref() else {
            self.flush_inner()?;
            self.persist_immutable_memtables()?;
            self.dirty.store(false, Ordering::Relaxed);
            return Ok(ConvergeResult::skipped(self.ts_oracle.current_timestamp()));
        };

        if !self.dirty.load(Ordering::Relaxed) && self.container_current.load(Ordering::Relaxed) {
            // Nothing changed since the last converge: never rewrite the file.
            return Ok(ConvergeResult::skipped(self.ts_oracle.current_timestamp()));
        }

        // 1. Every MemTable becomes an SSTable. This is what "stable" means here.
        self.flush_inner()?;
        self.persist_immutable_memtables()?;

        let converged_lsn = self.ts_oracle.current_timestamp();
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // 2. Record the checkpoint before touching the container.
        let checkpoint_path = self.data_dir.join(container::SIDECAR_CHECKPOINT_FILE);
        save_checkpoint_meta(
            &checkpoint_path,
            &CheckpointMeta::new(converged_lsn, created_at),
        )?;

        // 3. Write the container atomically (tmp -> fsync -> rename -> fsync dir).
        let tables: Vec<SSTableMeta> = self
            .levels
            .read()
            .expect("lsm levels lock poisoned")
            .iter()
            .flatten()
            .cloned()
            .collect();
        let result =
            container::write_container(container_path, &self.sst_dir, &tables, converged_lsn)?;

        // 4. Only now may the WAL be reclaimed. Truncating before the container is
        //    durable would turn converge into a data-loss device.
        {
            let mut wal = self.wal.write().expect("lsm wal lock poisoned");
            let end_offset = wal.end_offset();
            wal.advance_start(end_offset)?;
            self.wal_used_bytes
                .store(wal.used_bytes(), Ordering::Relaxed);
        }
        self.last_checkpoint_ms.store(created_at, Ordering::Relaxed);
        self.dirty.store(false, Ordering::Relaxed);
        self.container_current.store(true, Ordering::Relaxed);
        self.metrics
            .record_converge(result.bytes_written, result.duration_ms);
        if result.bytes_written >= CONVERGE_WARN_BYTES {
            warn!(
                bytes_written = result.bytes_written,
                duration_ms = result.duration_ms,
                container = ?container_path,
                "Converging a large database copies every live SSTable into the container"
            );
        }
        Ok(result)
    }

    #[cfg(target_arch = "wasm32")]
    fn converge_locked(&self) -> Result<ConvergeResult> {
        self.flush_inner()?;
        self.persist_immutable_memtables()?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(ConvergeResult::skipped(self.ts_oracle.current_timestamp()))
    }

    /// Converge and mark the store closed. Idempotent; errors are visible.
    ///
    /// The sidecar is intentionally *not* pruned here (裁定 D8): live cursors and
    /// `get_visible_at` reopen `sst/<id>.sst` on demand, so deleting the working
    /// directory behind a usable handle would break reads. Pruning happens on drop.
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        match self.converge() {
            Ok(_) => Ok(()),
            Err(err) => {
                self.closed.store(false, Ordering::SeqCst);
                Err(err)
            }
        }
    }

    /// Whether every lock this store owns is usable (i.e. none is poisoned).
    ///
    /// `Drop` consults this before doing any I/O so a panic elsewhere can never be
    /// escalated into a panic inside a destructor.
    #[cfg(not(target_arch = "wasm32"))]
    fn locks_are_healthy(&self) -> bool {
        !self.active_memtable.is_poisoned()
            && !self.immutable_memtables.is_poisoned()
            && !self.levels.is_poisoned()
            && !self.wal.is_poisoned()
            && !self.commit_lock.is_poisoned()
            && !self.owned_snapshot_gate.state.is_poisoned()
    }

    /// Determine whether an auto-checkpoint should run based on size and time thresholds.
    pub fn should_checkpoint(&self) -> bool {
        if !self.config.checkpoint.auto_checkpoint {
            return false;
        }
        let wal_used = self.wal_used_bytes.load(Ordering::Relaxed);
        if wal_used <= self.config.checkpoint.wal_size_threshold {
            return false;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.last_checkpoint_ms.load(Ordering::Relaxed);
        now_ms.saturating_sub(last) >= self.config.checkpoint.min_interval_ms
    }

    /// コンパクションを実行する（手動）。
    ///
    /// 現段階ではメタデータ更新までの配線は未実装のため、no-op とする。
    pub fn compact(&self) -> Result<()> {
        Ok(())
    }

    /// メトリクスを取得する。
    pub fn metrics(&self) -> LsmMetricsSnapshot {
        // Atomic カウンタの値を取得し、スナップショットに補完情報を足す。
        let counters = self.metrics.counters_snapshot();
        let sstable_count_per_level = self
            .levels
            .read()
            .expect("lsm levels lock poisoned")
            .iter()
            .map(|l| l.len())
            .collect::<Vec<_>>();

        let bp_stats = self.buffer_pool.stats();
        let bp_total = bp_stats.hits + bp_stats.misses;
        let hit_rate = if bp_total == 0 {
            1.0
        } else {
            (bp_stats.hits as f64) / (bp_total as f64)
        };
        LsmMetricsSnapshot {
            wal_write_bytes: counters.wal_write_bytes,
            wal_sync_duration_ms: counters.wal_sync_duration_ms,
            memtable_size_bytes: counters.memtable_size_bytes,
            memtable_flush_count: counters.memtable_flush_count,
            sstable_read_bytes: counters.sstable_read_bytes,
            sstable_count_per_level,
            buffer_pool_hit_rate: hit_rate,
            buffer_pool_size_bytes: self.buffer_pool.current_size_bytes() as u64,
            compaction_bytes_written: counters.compaction_bytes_written,
            compaction_duration_ms: counters.compaction_duration_ms,
            converge_count: counters.converge_count,
            converge_bytes_written: counters.converge_bytes_written,
            converge_duration_ms: counters.converge_duration_ms,
            rehydrate_bytes_read: counters.rehydrate_bytes_read,
        }
    }

    /// Changes the LSM buffer-pool capacity in bytes.
    pub fn set_cache_capacity_bytes(&self, capacity: usize) {
        self.buffer_pool.set_capacity_bytes(capacity);
    }

    /// Clears the LSM buffer pool and returns removed bytes.
    pub fn clear_cache(&self) -> usize {
        self.buffer_pool.clear()
    }

    /// 推定ディスク使用量（バイト）を返す。
    pub fn disk_usage(&self) -> u64 {
        fs::metadata(&self.wal_path).map(|m| m.len()).unwrap_or(0)
    }

    fn sstable_path_for(&self, file_id: u64) -> PathBuf {
        self.sst_dir.join(format!("{file_id}.sst"))
    }

    /// Return a shared, already-validated reader for `file_id`.
    fn sstable_reader(&self, file_id: u64) -> Result<Arc<Mutex<SSTableReader>>> {
        self.sstable_readers
            .get_or_open(file_id, &self.sstable_path_for(file_id))
    }

    fn persist_immutable_memtables(&self) -> Result<()> {
        self.drain_immutables_while(|len| len > 0)
    }

    /// Persist immutable MemTables from the front of the queue while `keep_going`
    /// holds for the current queue length.
    ///
    /// Each table is written to an SSTable **before** it leaves the queue, so a
    /// failed write can never silently drop unpersisted data: the table stays
    /// readable in memory and the error propagates to the caller. During the short
    /// window where a table is both in the queue and in `levels`, reads simply see
    /// the same entries twice and pick the newest, which is already how the merge
    /// path resolves duplicates.
    fn drain_immutables_while(&self, keep_going: impl Fn(usize) -> bool) -> Result<()> {
        loop {
            let front = {
                let queue = self
                    .immutable_memtables
                    .read()
                    .expect("lsm immutable_memtables lock poisoned");
                if !keep_going(queue.len()) {
                    break;
                }
                queue.front().cloned()
            };
            let Some(front) = front else {
                break;
            };

            self.persist_immutable_table(&front)?;

            let mut queue = self
                .immutable_memtables
                .write()
                .expect("lsm immutable_memtables lock poisoned");
            match queue.front() {
                Some(head) if Arc::ptr_eq(head, &front) => {
                    queue.pop_front();
                }
                // Another writer already retired this table. Stop instead of
                // risking an unbounded loop; anything left is still readable.
                _ => break,
            }
        }
        self.refresh_memtable_size_metrics();
        Ok(())
    }

    /// Write one immutable MemTable to an SSTable and register it at level 0.
    fn persist_immutable_table(&self, mem: &ImmutableMemTable) -> Result<()> {
        let entries = mem.scan_prefix(b"", u64::MAX);
        if entries.is_empty() {
            return Ok(());
        }

        let file_id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);
        let path = self.sstable_path_for(file_id);
        let mut writer = SSTableWriter::create(&path, self.config.sstable)?;

        let mut first_key: Option<Key> = None;
        let mut last_key: Option<Key> = None;
        for (key, entry) in entries {
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key.clone());
            writer.append(SSTableEntry {
                key,
                value: entry.value,
                timestamp: entry.timestamp,
                sequence: entry.sequence,
            })?;
        }
        writer.finish()?;
        let size_bytes = fs::metadata(&path)?.len();

        let meta = SSTableMeta {
            id: file_id,
            level: 0,
            size_bytes,
            key_range: KeyRange {
                first_key: first_key.expect("non-empty table has a first key"),
                last_key: last_key.expect("non-empty table has a last key"),
            },
        };
        self.levels
            .write()
            .expect("lsm levels lock poisoned")
            .get_mut(0)
            .expect("lsm always has at least one level")
            .push(meta);
        Ok(())
    }

    fn refresh_memtable_size_metrics(&self) {
        let active_bytes = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .memory_usage_bytes() as u64;
        let imm_bytes = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned")
            .iter()
            .map(|t| t.memory_usage_bytes() as u64)
            .sum::<u64>();
        self.metrics
            .set_memtable_size_bytes(active_bytes.saturating_add(imm_bytes));
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

        // SSTable を探索（L0 → L1..Ln）。
        let levels = self.levels.read().expect("lsm levels lock poisoned");
        let mut best: Option<crate::lsm::memtable::MemTableEntry> = None;
        for level in levels.iter() {
            for meta in level.iter() {
                // Skip tables that provably cannot hold the key before paying for
                // an open, which validates the whole file.
                if !meta.key_range.contains(key) {
                    continue;
                }
                let Ok(reader) = self.sstable_reader(meta.id) else {
                    continue;
                };
                let Ok(mut reader) = reader.lock() else {
                    continue;
                };
                let Ok(found) = reader.get_with_buffer_pool(
                    &self.buffer_pool,
                    &self.metrics,
                    meta.id,
                    key,
                    read_timestamp,
                ) else {
                    continue;
                };
                let Some(found) = found else {
                    continue;
                };
                let candidate = crate::lsm::memtable::MemTableEntry {
                    value: found.value,
                    timestamp: found.timestamp,
                    sequence: found.sequence,
                };
                let better = match &best {
                    None => true,
                    Some(cur) => {
                        (candidate.timestamp > cur.timestamp)
                            || (candidate.timestamp == cur.timestamp
                                && candidate.sequence > cur.sequence)
                    }
                };
                if better {
                    best = Some(candidate);
                }
            }
        }
        best
    }

    /// Return a visible entry for an owned cursor, surfacing persisted-source errors instead of
    /// silently skipping a damaged SSTable.
    pub(crate) fn owned_visible_at(
        &self,
        key: &[u8],
        read_timestamp: u64,
    ) -> Result<Option<MemTableEntry>> {
        if let Some(entry) = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .get(key, read_timestamp)
        {
            return Ok(Some(entry));
        }
        let immutables = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        for table in immutables.iter().rev() {
            if let Some(entry) = table.get(key, read_timestamp) {
                return Ok(Some(entry));
            }
        }

        let levels = self.levels.read().expect("lsm levels lock poisoned");
        let mut best: Option<MemTableEntry> = None;
        for level in levels.iter() {
            for meta in level {
                if !meta.key_range.contains(key) {
                    continue;
                }
                let reader = self.sstable_reader(meta.id)?;
                let mut reader = reader.lock().expect("lsm sstable reader lock poisoned");
                let Some(found) = reader.get_with_buffer_pool(
                    &self.buffer_pool,
                    &self.metrics,
                    meta.id,
                    key,
                    read_timestamp,
                )?
                else {
                    continue;
                };
                let candidate = MemTableEntry {
                    value: found.value,
                    timestamp: found.timestamp,
                    sequence: found.sequence,
                };
                let better = match &best {
                    None => true,
                    Some(current) => {
                        candidate.timestamp > current.timestamp
                            || (candidate.timestamp == current.timestamp
                                && candidate.sequence > current.sequence)
                    }
                };
                if better {
                    best = Some(candidate);
                }
            }
        }
        Ok(best)
    }

    /// Acquire the source-stability token that an owned LSM cursor retains until close/drop.
    pub(crate) fn acquire_owned_snapshot_reader(&self) -> OwnedLsmSnapshotReader {
        self.owned_snapshot_gate.acquire_reader()
    }

    /// Open one incremental reader for every persisted table overlapping an owned cursor bound.
    pub(crate) fn open_owned_sstable_cursors(
        &self,
        bounds: &OwnedLsmScanBounds,
        read_timestamp: u64,
    ) -> Result<Vec<SSTableCursor>> {
        let tables = {
            let levels = self.levels.read().expect("lsm levels lock poisoned");
            levels
                .iter()
                .flatten()
                .map(|meta| {
                    (
                        meta.id,
                        self.sstable_path_for(meta.id),
                        meta.key_range.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut cursors = Vec::new();
        for (file_id, path, key_range) in tables {
            if key_range.last_key.as_slice() < bounds.start().as_slice()
                || bounds
                    .end()
                    .is_some_and(|end| key_range.first_key.as_slice() >= end.as_slice())
            {
                continue;
            }
            cursors.push(SSTableCursor::open(
                &path,
                file_id,
                bounds.start().clone(),
                bounds.end().cloned(),
                read_timestamp,
            )?);
        }
        Ok(cursors)
    }

    /// Find the least next visible key among active and immutable MemTables without collecting a
    /// whole scan result.
    pub(crate) fn owned_next_memtable_key_after(
        &self,
        after: Option<&Key>,
        bounds: &OwnedLsmScanBounds,
        read_timestamp: u64,
    ) -> Option<Key> {
        let mut candidate = self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .next_visible_after(
                after.map(Vec::as_slice),
                bounds.start(),
                bounds.end().map(Vec::as_slice),
                bounds.prefix_constraint().map(Vec::as_slice),
                read_timestamp,
            )
            .map(|(key, _)| key);

        let immutables = self
            .immutable_memtables
            .read()
            .expect("lsm immutable_memtables lock poisoned");
        for table in immutables.iter() {
            let Some((key, _)) = table.next_visible_after(
                after.map(Vec::as_slice),
                bounds.start(),
                bounds.end().map(Vec::as_slice),
                bounds.prefix_constraint().map(Vec::as_slice),
                read_timestamp,
            ) else {
                continue;
            };
            if candidate.as_ref().is_none_or(|current| key < *current) {
                candidate = Some(key);
            }
        }
        candidate
    }

    /// Commit a staged LSM write set while respecting owned-cursor source stability.
    ///
    /// Both legacy borrowed transactions and owned sessions call this one implementation so a
    /// cursor never observes a half-flushed source layout and WAL-before-MemTable ordering stays
    /// identical across the two APIs.
    pub(crate) fn commit_write_set(
        &self,
        start_ts: u64,
        read_set: &HashSet<Key>,
        write_set: BTreeMap<Key, Option<Value>>,
    ) -> Result<()> {
        if write_set.is_empty() {
            return Ok(());
        }

        let _snapshot_writer = self.owned_snapshot_gate.acquire_writer();
        let _commit_guard = self.commit_lock.lock().expect("lsm commit_lock poisoned");
        for key in read_set {
            if self.latest_timestamp(key) > start_ts {
                return Err(Error::TxnConflict);
            }
        }
        for key in write_set.keys() {
            if self.latest_timestamp(key) > start_ts {
                return Err(Error::TxnConflict);
            }
        }

        let commit_ts = self.ts_oracle.next_timestamp();
        let mut ops = Vec::with_capacity(write_set.len());
        for (key, value) in &write_set {
            ops.push(match value {
                Some(value) => WalBatchOp {
                    op_type: WalOpType::Put,
                    key: key.clone(),
                    value: Some(value.clone()),
                },
                None => WalBatchOp {
                    op_type: WalOpType::Delete,
                    key: key.clone(),
                    value: None,
                },
            });
        }
        {
            let entry = WalEntry::batch(commit_ts, ops);
            let mut wal = self.wal.write().expect("lsm wal lock poisoned");
            let stats = wal.append_with_stats(&entry)?;
            self.metrics.add_wal_write_bytes(stats.bytes_written);
            let sync_duration_ms = if stats.sync_duration_ms == 0
                && !matches!(self.config.wal.sync_mode, SyncMode::EveryWrite)
            {
                wal.force_sync()?
            } else {
                stats.sync_duration_ms
            };
            self.metrics.add_wal_sync_duration_ms(sync_duration_ms);
            self.wal_used_bytes
                .store(wal.used_bytes(), Ordering::Relaxed);
        }
        // The durable WAL record is what makes this commit real, so the container
        // is stale from this point on.
        self.dirty.store(true, Ordering::Relaxed);
        self.container_current.store(false, Ordering::Relaxed);

        {
            let active = self
                .active_memtable
                .read()
                .expect("lsm active_memtable lock poisoned");
            let mut sequence = 1u64;
            for (key, value) in write_set {
                match value {
                    Some(value) => active.put(key, value, commit_ts, sequence),
                    None => active.delete(key, commit_ts, sequence),
                }
                sequence = sequence.wrapping_add(1);
            }
        }
        self.refresh_memtable_size_metrics();

        if self
            .active_memtable
            .read()
            .expect("lsm active_memtable lock poisoned")
            .memory_usage_bytes()
            >= self.config.memtable.flush_threshold
        {
            self.flush_inner()?;
        }
        Ok(())
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

        // SSTable もマージ（L0→Ln、同一キーは新しい timestamp/sequence を採用）。
        let levels = self.levels.read().expect("lsm levels lock poisoned");
        for level in levels.iter() {
            for meta in level.iter() {
                let Ok(reader) = self.sstable_reader(meta.id) else {
                    continue;
                };
                let Ok(mut reader) = reader.lock() else {
                    continue;
                };
                let Ok(entries) = reader.scan_prefix_with_buffer_pool(
                    &self.buffer_pool,
                    &self.metrics,
                    meta.id,
                    prefix,
                    read_timestamp,
                ) else {
                    continue;
                };
                for e in entries {
                    let k = e.key.clone();
                    let candidate = crate::lsm::memtable::MemTableEntry {
                        value: e.value,
                        timestamp: e.timestamp,
                        sequence: e.sequence,
                    };
                    match out.get(&k) {
                        None => {
                            out.insert(k, candidate);
                        }
                        Some(cur) => {
                            let better = (candidate.timestamp > cur.timestamp)
                                || (candidate.timestamp == cur.timestamp
                                    && candidate.sequence > cur.sequence);
                            if better {
                                out.insert(k, candidate);
                            }
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

        let levels = self.levels.read().expect("lsm levels lock poisoned");
        for level in levels.iter() {
            for meta in level.iter() {
                let path = self.sstable_path_for(meta.id);
                let Ok(mut reader) = SSTableReader::open(&path) else {
                    continue;
                };
                let Ok(entries) = reader.scan_range_with_buffer_pool(
                    &self.buffer_pool,
                    &self.metrics,
                    meta.id,
                    start,
                    end,
                    read_timestamp,
                ) else {
                    continue;
                };
                for e in entries {
                    let k = e.key.clone();
                    let candidate = crate::lsm::memtable::MemTableEntry {
                        value: e.value,
                        timestamp: e.timestamp,
                        sequence: e.sequence,
                    };
                    match out.get(&k) {
                        None => {
                            out.insert(k, candidate);
                        }
                        Some(cur) => {
                            let better = (candidate.timestamp > cur.timestamp)
                                || (candidate.timestamp == cur.timestamp
                                    && candidate.sequence > cur.sequence);
                            if better {
                                out.insert(k, candidate);
                            }
                        }
                    }
                }
            }
        }

        out
    }
}

/// Best-effort convergence when a handle is released without an explicit close.
///
/// The issue this fixes (#178) reproduces through `open` → commit → normal exit,
/// which only ever passes through `Drop`, so converging here is mandatory. A
/// destructor cannot report errors, so [`LsmKV::close`] stays the error-visible
/// path and everything here is defensive:
///
/// * stores with no container (plain directories, `alopex-server`) return immediately,
///   leaving their on-disk layout byte-identical to previous releases;
/// * a panicking thread or any poisoned lock skips the work rather than risking a
///   panic inside a destructor;
/// * failures are logged, never propagated — the sidecar remains authoritative, so
///   a failed converge costs nothing but a slower next open.
#[cfg(not(target_arch = "wasm32"))]
impl Drop for LsmKV {
    fn drop(&mut self) {
        if self.container_path.is_none() {
            return;
        }
        if std::thread::panicking() || !self.locks_are_healthy() {
            warn!(
                data_dir = ?self.data_dir,
                "Skipping converge on drop: the store is unwinding or a lock is poisoned"
            );
            return;
        }
        if let Err(err) = self.converge() {
            warn!(
                error = %err,
                data_dir = ?self.data_dir,
                "Converge on drop failed; the sidecar working directory stays authoritative"
            );
            return;
        }
        if !self.config.prune_sidecar_on_drop || !self.container_current.load(Ordering::Relaxed) {
            return;
        }
        if let Err(err) = container::prune_sidecar(&self.data_dir) {
            warn!(
                error = %err,
                data_dir = ?self.data_dir,
                "Failed to prune the sidecar working directory after converge"
            );
        }
    }
}

fn next_sstable_id_from_dir(dir: &Path) -> Result<u64> {
    let mut max_id = 0u64;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("sst") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(id) = stem.parse::<u64>() else {
                continue;
            };
            max_id = max_id.max(id);
        }
    }
    Ok(max_id.saturating_add(1))
}

fn load_sstable_levels(dir: &Path, max_levels: usize) -> Result<Vec<Vec<SSTableMeta>>> {
    let mut levels = vec![Vec::new(); max_levels];
    if max_levels == 0 || !dir.exists() {
        return Ok(levels);
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sst") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(file_id) = stem.parse::<u64>() else {
            continue;
        };
        let reader = match SSTableReader::open(&path) {
            Ok(reader) => reader,
            Err(err) => {
                warn!(error = %err, path = ?path, "Skipping unreadable SSTable");
                continue;
            }
        };
        let Some((first_key, last_key)) = reader.key_range() else {
            continue;
        };
        let size_bytes = fs::metadata(&path)?.len();
        let meta = SSTableMeta {
            id: file_id,
            level: 0,
            size_bytes,
            key_range: KeyRange {
                first_key,
                last_key,
            },
        };
        levels[0].push(meta);
    }

    Ok(levels)
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

/// LSM 用トランザクション（Snapshot Isolation + OCC）。
///
/// - 開始時点の `start_ts` をスナップショットとして読み取る。
/// - `read_set` を記録し、コミット時に read-write conflict を検出する（ファントム検出は未対応）。
/// - 書き込みは `write_set` にバッファし、コミット時に WAL → MemTable の順で反映する。
#[derive(Debug)]
pub struct LsmTransaction<'a> {
    /// 開始タイムスタンプ。
    start_ts: u64,
    /// トランザクション ID。
    txn_id: TxnId,
    /// モード。
    mode: TxnMode,
    /// 読み取りセット。
    read_set: HashSet<Vec<u8>>,
    /// 書き込みセット（`None` は tombstone）。
    write_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// KV ストア参照。
    store: &'a LsmKV,
}

impl<'a> LsmTransaction<'a> {
    fn new(store: &'a LsmKV, txn_id: TxnId, mode: TxnMode, start_ts: u64) -> Self {
        Self {
            start_ts,
            txn_id,
            mode,
            read_set: HashSet::new(),
            write_set: BTreeMap::new(),
            store,
        }
    }

    /// トランザクションを消費せずにロールバックする。
    pub(crate) fn rollback_in_place(&mut self) -> Result<()> {
        self.read_set.clear();
        self.write_set.clear();
        Ok(())
    }

    fn write_iter_prefix<'b>(
        &'b self,
        prefix: &'b [u8],
    ) -> impl Iterator<Item = (&'b Key, &'b Option<Value>)> + 'b {
        let prefix_vec = prefix.to_vec();
        self.write_set
            .range(prefix_vec..)
            .take_while(move |(k, _)| k.starts_with(prefix))
    }
}

impl<'a> KVTransaction<'a> for LsmTransaction<'a> {
    fn id(&self) -> TxnId {
        self.txn_id
    }

    fn mode(&self) -> TxnMode {
        self.mode
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        if let Some(v) = self.write_set.get(key) {
            return Ok(v.clone());
        }

        self.read_set.insert(key.clone());
        let entry = self.store.get_visible_at(key, self.start_ts);
        Ok(entry.and_then(|e| e.value))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        if self.mode == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly);
        }
        self.write_set.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: Key) -> Result<()> {
        if self.mode == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly);
        }
        self.write_set.insert(key, None);
        Ok(())
    }

    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        let mut map: BTreeMap<Key, Option<Value>> = self
            .store
            .scan_prefix_visible(prefix, self.start_ts)
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
        let mut map: BTreeMap<Key, Option<Value>> = self
            .store
            .scan_range_visible(start, end, self.start_ts)
            .into_iter()
            .map(|(k, e)| (k, e.value))
            .collect();

        // スナップショットで観測したキーは read_set に入れる（read-write conflict 検出の最低限）。
        self.read_set.extend(map.keys().cloned());

        let overlays: Vec<(Key, Option<Value>)> = self
            .write_set
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
        if self.mode == TxnMode::ReadOnly || self.write_set.is_empty() {
            return Ok(());
        }
        self.store.commit_write_set(
            self.start_ts,
            &self.read_set,
            std::mem::take(&mut self.write_set),
        )
    }

    fn rollback_self(mut self) -> Result<()> {
        self.write_set.clear();
        Ok(())
    }
}

impl<'a> TxnManager<'a, LsmTransaction<'a>> for LsmTxnManagerRef<'a> {
    fn begin(&'a self, mode: TxnMode) -> Result<LsmTransaction<'a>> {
        let start_ts = self.store.ts_oracle.next_timestamp();
        Ok(LsmTransaction::new(
            self.store,
            self.allocate_txn_id(),
            mode,
            start_ts,
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
        let start_ts = self.ts_oracle.next_timestamp();
        Ok(LsmTransaction::new(
            self,
            manager.allocate_txn_id(),
            mode,
            start_ts,
        ))
    }

    fn runtime_stats(&self) -> Option<crate::kv::RuntimeStats> {
        Some(crate::kv::RuntimeStats::Lsm(self.metrics()))
    }

    fn set_cache_capacity_bytes(&self, capacity: usize) -> Result<()> {
        self.set_cache_capacity_bytes(capacity);
        Ok(())
    }

    fn clear_cache(&self) -> Result<usize> {
        Ok(self.clear_cache())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod kv_store {
    use super::*;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    fn new_test_store() -> LsmKV {
        let cfg = test_config();
        let data_dir = tempfile::tempdir().expect("tempdir").keep();
        let sst_dir = data_dir.join("sst");
        fs::create_dir_all(&sst_dir).expect("create sst dir");
        let wal_path = data_dir.join("lsm.wal");
        let wal = WalWriter::create(&wal_path, cfg.wal.clone(), 1, 1).expect("wal create");

        let levels = vec![Vec::new(); cfg.compaction.max_levels];
        LsmKV {
            config: cfg,
            data_dir,
            sst_dir,
            wal_path,
            wal: RwLock::new(wal),
            active_memtable: RwLock::new(MemTable::new()),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(BufferPoolConfig::default()),
            sstable_readers: SSTableReaderCache::default(),
            metrics: Arc::new(LsmMetrics::default()),
            ts_oracle: TimestampOracle::new(1),
            txn_manager: LsmTxnManager::default(),
            commit_lock: Mutex::new(()),
            owned_snapshot_gate: Arc::new(OwnedLsmSnapshotGate::default()),
            next_sstable_id: AtomicU64::new(1),
            wal_used_bytes: AtomicU64::new(0),
            last_checkpoint_ms: AtomicU64::new(0),
            container_path: None,
            dirty: AtomicBool::new(false),
            container_current: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn new_test_store_with_sync(sync_mode: SyncMode) -> LsmKV {
        let mut cfg = test_config();
        cfg.wal.sync_mode = sync_mode;
        let data_dir = tempfile::tempdir().expect("tempdir").keep();
        let sst_dir = data_dir.join("sst");
        fs::create_dir_all(&sst_dir).expect("create sst dir");
        let wal_path = data_dir.join("lsm.wal");
        let wal = WalWriter::create(&wal_path, cfg.wal.clone(), 1, 1).expect("wal create");

        let levels = vec![Vec::new(); cfg.compaction.max_levels];
        LsmKV {
            config: cfg,
            data_dir,
            sst_dir,
            wal_path,
            wal: RwLock::new(wal),
            active_memtable: RwLock::new(MemTable::new()),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(BufferPoolConfig::default()),
            sstable_readers: SSTableReaderCache::default(),
            metrics: Arc::new(LsmMetrics::default()),
            ts_oracle: TimestampOracle::new(1),
            txn_manager: LsmTxnManager::default(),
            commit_lock: Mutex::new(()),
            owned_snapshot_gate: Arc::new(OwnedLsmSnapshotGate::default()),
            next_sstable_id: AtomicU64::new(1),
            wal_used_bytes: AtomicU64::new(0),
            last_checkpoint_ms: AtomicU64::new(0),
            container_path: None,
            dirty: AtomicBool::new(false),
            container_current: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    #[test]
    fn commit_forces_fsync_across_sync_modes() {
        let modes = [
            SyncMode::EveryWrite,
            SyncMode::BatchSync {
                max_batch_size: 1024 * 1024,
                max_wait_ms: 60_000,
            },
            SyncMode::NoSync,
        ];

        for mode in modes {
            let store = new_test_store_with_sync(mode);
            crate::lsm::wal::reset_sync_calls();

            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
            tx.commit_self().unwrap();

            assert!(
                crate::lsm::wal::sync_calls() >= 1,
                "expected fsync during commit for sync mode"
            );
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod txn {
    use super::*;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    fn new_test_store() -> LsmKV {
        let cfg = test_config();
        let data_dir = tempfile::tempdir().expect("tempdir").keep();
        let sst_dir = data_dir.join("sst");
        fs::create_dir_all(&sst_dir).expect("create sst dir");
        let wal_path = data_dir.join("lsm.wal");
        let wal = WalWriter::create(&wal_path, cfg.wal.clone(), 1, 1).expect("wal create");

        let levels = vec![Vec::new(); cfg.compaction.max_levels];
        LsmKV {
            config: cfg,
            data_dir,
            sst_dir,
            wal_path,
            wal: RwLock::new(wal),
            active_memtable: RwLock::new(MemTable::new()),
            immutable_memtables: RwLock::new(VecDeque::new()),
            levels: RwLock::new(levels),
            buffer_pool: BufferPool::new(BufferPoolConfig::default()),
            sstable_readers: SSTableReaderCache::default(),
            metrics: Arc::new(LsmMetrics::default()),
            ts_oracle: TimestampOracle::new(1),
            txn_manager: LsmTxnManager::default(),
            commit_lock: Mutex::new(()),
            owned_snapshot_gate: Arc::new(OwnedLsmSnapshotGate::default()),
            next_sstable_id: AtomicU64::new(1),
            wal_used_bytes: AtomicU64::new(0),
            last_checkpoint_ms: AtomicU64::new(0),
            container_path: None,
            dirty: AtomicBool::new(false),
            container_current: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    #[test]
    fn detects_read_write_conflict_after_get() {
        let store = new_test_store();

        let mut init = store.begin(TxnMode::ReadWrite).unwrap();
        init.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        init.commit_self().unwrap();

        let mut a = store.begin(TxnMode::ReadWrite).unwrap();
        assert_eq!(a.get(&b"k".to_vec()).unwrap(), Some(b"v1".to_vec()));

        let mut b = store.begin(TxnMode::ReadWrite).unwrap();
        b.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        b.commit_self().unwrap();

        a.put(b"other".to_vec(), b"x".to_vec()).unwrap();
        assert!(a.commit_self().is_err());
    }

    #[test]
    fn rollback_discards_write_set() {
        let store = new_test_store();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.rollback_self().unwrap();

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), None);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod methods {
    use super::*;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    #[test]
    fn open_creates_wal_and_returns_metrics() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");
        let m = store.metrics();
        assert_eq!(m.wal_write_bytes, 0);
        assert_eq!(m.memtable_flush_count, 0);
        assert!(store.disk_usage() > 0);
    }

    #[test]
    fn open_replays_wal_entries() {
        let dir = tempfile::tempdir().expect("tempdir");

        {
            let (store, _recovery) =
                LsmKV::open_with_config(dir.path(), test_config()).expect("open");
            let mut wal = store.wal.write().unwrap();
            wal.append(&crate::lsm::wal::WalEntry::put(
                10,
                b"k".to_vec(),
                b"v".to_vec(),
            ))
            .unwrap();
        }

        let (store, _recovery) =
            LsmKV::open_with_config(dir.path(), test_config()).expect("reopen");
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn flush_moves_active_to_immutable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");

        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        store.flush().unwrap();

        let m = store.metrics();
        assert_eq!(m.memtable_flush_count, 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    /// `flush()` must actually produce an SSTable, not merely freeze the MemTable.
    /// Its documentation always claimed it did, but before v0.8.8 only
    /// `checkpoint()` ever wrote one, which is why embedded databases kept every
    /// row in the WAL and `sst/` stayed empty.
    #[test]
    fn flush_writes_an_sstable_to_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");

        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();
        store.flush().unwrap();

        let sst_files: Vec<_> = fs::read_dir(dir.path().join("sst"))
            .expect("read sst dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|e| e.to_str()) == Some("sst"))
            .collect();
        assert_eq!(sst_files.len(), 1, "flush must persist one SSTable");
        assert_eq!(store.metrics().sstable_count_per_level[0], 1);
    }

    /// Overflowing the immutable-MemTable queue used to drop the oldest table with
    /// `pop_front()`, silently losing every unpersisted row in it. With converge
    /// truncating the WAL at every close, that would have become deterministic data
    /// loss, so overflow now persists before it pops.
    #[test]
    fn immutable_queue_overflow_persists_instead_of_dropping_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LsmKVConfig {
            memtable: MemTableConfig {
                // Every commit freezes the active table, so the queue overflows
                // without any explicit flush draining it first.
                flush_threshold: 1,
                max_immutable_count: 2,
            },
            ..test_config()
        };
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), config).expect("open");

        // Six freezes against a queue capped at two: four tables must overflow.
        for index in 0..6u32 {
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            tx.put(
                format!("k{index}").into_bytes(),
                format!("v{index}").into_bytes(),
            )
            .unwrap();
            tx.commit_self().unwrap();
        }
        assert!(
            store.immutable_memtables.read().unwrap().len() <= 2,
            "the queue must stay capped"
        );
        store.checkpoint().unwrap();

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        for index in 0..6u32 {
            assert_eq!(
                ro.get(&format!("k{index}").into_bytes()).unwrap(),
                Some(format!("v{index}").into_bytes()),
                "row k{index} was dropped from the immutable queue"
            );
        }
    }

    /// A surviving `checkpoint.meta` must keep the MVCC clock ahead of the
    /// timestamps already written into SSTables. Resuming at 1 used to hand out
    /// commit timestamps below them, producing spurious conflicts and stale reads.
    #[test]
    fn missing_wal_resumes_the_clock_from_checkpoint_meta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path();
        fs::create_dir_all(data_dir.join("sst")).unwrap();
        save_checkpoint_meta(
            &data_dir.join("checkpoint.meta"),
            &CheckpointMeta::new(5000, 1),
        )
        .unwrap();

        let (store, recovery) = LsmKV::open_with_config(data_dir, test_config()).expect("open");
        assert_eq!(recovery.checkpoint_lsn, Some(5000));
        assert!(
            store.ts_oracle.current_timestamp() >= 5000,
            "timestamp oracle rewound to {}",
            store.ts_oracle.current_timestamp()
        );
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod write_path {
    use super::*;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            memtable: MemTableConfig {
                flush_threshold: 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn commit_appends_wal_and_reopen_replays() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (store, _recovery) =
                LsmKV::open_with_config(dir.path(), test_config()).expect("open");
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
            tx.commit_self().unwrap();
        }

        let (store, _recovery) =
            LsmKV::open_with_config(dir.path(), test_config()).expect("reopen");
        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn flush_trigger_moves_to_immutable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");

        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        // flush_threshold=1 のため commit 後に自動で flush される。
        let metrics = store.metrics();
        assert_eq!(metrics.memtable_flush_count, 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod recovery_tests {
    use super::*;
    use crate::lsm::checkpoint::{save_checkpoint_meta, CheckpointMeta};
    use std::io::{Read, Seek, SeekFrom, Write};

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    fn create_corrupted_tail_wal(dir: &Path, wal_cfg: WalConfig) {
        let wal_path = dir.join("lsm.wal");
        let mut writer = WalWriter::create(&wal_path, wal_cfg, 1, 1).unwrap();
        let e1 = WalEntry::put(1, b"a".to_vec(), b"1".to_vec());
        let e2 = WalEntry::put(2, b"b".to_vec(), b"2".to_vec());
        let _off1 = writer.append(&e1).unwrap();
        let off2 = writer.append(&e2).unwrap();
        let e2_bytes = e2.encode().unwrap();
        drop(writer);

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        let corrupt_offset = off2 + (e2_bytes.len() as u64).saturating_sub(1);
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf).unwrap();
        buf[0] ^= 0xFF;
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&buf).unwrap();
        file.flush().unwrap();
    }

    #[test]
    fn recovery_uses_checkpoint_lsn_when_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"before".to_vec(), b"1".to_vec()).unwrap();
        tx.commit_self().unwrap();

        let checkpoint_lsn = store.ts_oracle.current_timestamp();
        let meta = CheckpointMeta::new(checkpoint_lsn, 0);
        let checkpoint_path = dir.path().join("checkpoint.meta");
        save_checkpoint_meta(&checkpoint_path, &meta).unwrap();

        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"after".to_vec(), b"2".to_vec()).unwrap();
        tx.commit_self().unwrap();

        let (store, recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("reopen");
        assert!(recovery.checkpoint_lsn.is_some());
        assert_eq!(recovery.entries_recovered, 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"after".to_vec()).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn recovery_falls_back_when_checkpoint_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        let (store, recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("reopen");
        assert!(recovery.checkpoint_lsn.is_none());
        assert!(recovery.entries_recovered >= 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn recovery_stops_on_corrupted_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_cfg = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            sync_mode: SyncMode::NoSync,
        };
        create_corrupted_tail_wal(dir.path(), wal_cfg.clone());

        let cfg = LsmKVConfig {
            wal: wal_cfg,
            ..Default::default()
        };
        let (store, recovery) = LsmKV::open_with_config(dir.path(), cfg).expect("reopen");
        assert!(recovery.stop_reason.is_some());
        assert_eq!(recovery.entries_recovered, 1);
        assert_eq!(recovery.last_lsn, 1);

        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"a".to_vec()).unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn recovery_is_idempotent_across_reopens() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_cfg = WalConfig {
            segment_size: 4096,
            max_segments: 1,
            sync_mode: SyncMode::NoSync,
        };
        create_corrupted_tail_wal(dir.path(), wal_cfg.clone());

        let cfg = LsmKVConfig {
            wal: wal_cfg,
            ..Default::default()
        };

        let (first_recovery, first_data) = {
            let (store, recovery) =
                LsmKV::open_with_config(dir.path(), cfg.clone()).expect("first reopen");
            let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
            let data = (
                ro.get(&b"a".to_vec()).unwrap(),
                ro.get(&b"b".to_vec()).unwrap(),
            );
            (recovery, data)
        };

        let (second_recovery, second_data) = {
            let (store, recovery) =
                LsmKV::open_with_config(dir.path(), cfg).expect("second reopen");
            let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
            let data = (
                ro.get(&b"a".to_vec()).unwrap(),
                ro.get(&b"b".to_vec()).unwrap(),
            );
            (recovery, data)
        };

        assert_eq!(
            first_recovery.entries_recovered,
            second_recovery.entries_recovered
        );
        assert_eq!(first_recovery.last_lsn, second_recovery.last_lsn);
        assert_eq!(first_recovery.entries_recovered, 1);
        assert_eq!(first_recovery.last_lsn, 1);
        assert_eq!(first_data, second_data);
        assert_eq!(first_data, (Some(b"1".to_vec()), None));
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod read_path {
    use super::*;

    use crate::compaction::leveled::KeyRange;
    use crate::lsm::sstable::{SSTableEntry, SSTableWriter};

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        }
    }

    #[test]
    fn reads_from_sstable_via_buffer_pool() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) = LsmKV::open_with_config(dir.path(), test_config()).expect("open");

        // SSTable を1つ作成して L0 に登録する。
        let file_id = 1u64;
        let path = store.sstable_path_for(file_id);
        let mut writer = SSTableWriter::create(&path, store.config.sstable).expect("sst create");
        writer
            .append(SSTableEntry {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                timestamp: 0,
                sequence: 1,
            })
            .unwrap();
        writer.finish().unwrap();

        let size_bytes = fs::metadata(&path).unwrap().len();
        let meta = SSTableMeta {
            id: file_id,
            level: 0,
            size_bytes,
            key_range: KeyRange {
                first_key: b"k".to_vec(),
                last_key: b"k".to_vec(),
            },
        };
        store.levels.write().unwrap()[0].push(meta);

        let before = store.buffer_pool.stats();
        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
        assert_eq!(ro.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
        let after = store.buffer_pool.stats();

        assert!(after.misses > before.misses);
        assert!(after.hits > before.hits);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod integration;
