//! LSM-Tree（file-mode）のメトリクス。
//!
//! 設計書 `docs-internal/specs/lsm-tree-file-mode-spec.md` §11.1 に基づく。

use std::sync::atomic::{AtomicU64, Ordering};

/// LSM 用メトリクス（Atomic カウンタ）。
#[derive(Debug, Default)]
pub struct LsmMetrics {
    wal_write_bytes: AtomicU64,
    wal_sync_duration_ms: AtomicU64,
    memtable_size_bytes: AtomicU64,
    memtable_flush_count: AtomicU64,
    sstable_read_bytes: AtomicU64,
    compaction_bytes_written: AtomicU64,
    compaction_duration_ms: AtomicU64,
}

impl LsmMetrics {
    /// WAL 書き込みバイト数を加算する。
    pub fn add_wal_write_bytes(&self, bytes: u64) {
        self.wal_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// WAL fsync 所要時間（ms）を加算する。
    pub fn add_wal_sync_duration_ms(&self, ms: u64) {
        self.wal_sync_duration_ms.fetch_add(ms, Ordering::Relaxed);
    }

    /// MemTable サイズ（バイト）を更新する（推定値）。
    pub fn set_memtable_size_bytes(&self, bytes: u64) {
        self.memtable_size_bytes.store(bytes, Ordering::Relaxed);
    }

    /// MemTable フラッシュ回数を加算する。
    pub fn inc_memtable_flush_count(&self) {
        self.memtable_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// SSTable 読み取りバイト数を加算する。
    pub fn add_sstable_read_bytes(&self, bytes: u64) {
        self.sstable_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Compaction 書き込みバイト数を加算する。
    pub fn add_compaction_bytes_written(&self, bytes: u64) {
        self.compaction_bytes_written
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Compaction 所要時間（ms）を加算する。
    pub fn add_compaction_duration_ms(&self, ms: u64) {
        self.compaction_duration_ms.fetch_add(ms, Ordering::Relaxed);
    }

    /// 現在値のスナップショットを返す。
    pub fn counters_snapshot(&self) -> LsmMetricsCountersSnapshot {
        LsmMetricsCountersSnapshot {
            wal_write_bytes: self.wal_write_bytes.load(Ordering::Relaxed),
            wal_sync_duration_ms: self.wal_sync_duration_ms.load(Ordering::Relaxed),
            memtable_size_bytes: self.memtable_size_bytes.load(Ordering::Relaxed),
            memtable_flush_count: self.memtable_flush_count.load(Ordering::Relaxed),
            sstable_read_bytes: self.sstable_read_bytes.load(Ordering::Relaxed),
            compaction_bytes_written: self.compaction_bytes_written.load(Ordering::Relaxed),
            compaction_duration_ms: self.compaction_duration_ms.load(Ordering::Relaxed),
        }
    }
}

/// メトリクススナップショット（Atomic カウンタの値のみ）。
#[derive(Debug, Clone, PartialEq)]
pub struct LsmMetricsCountersSnapshot {
    /// WAL 書き込みバイト数。
    pub wal_write_bytes: u64,
    /// WAL fsync 所要時間（合計 ms）。
    pub wal_sync_duration_ms: u64,
    /// MemTable サイズ（推定、バイト）。
    pub memtable_size_bytes: u64,
    /// MemTable フラッシュ回数。
    pub memtable_flush_count: u64,
    /// SSTable 読み取りバイト数。
    pub sstable_read_bytes: u64,
    /// Compaction 書き込みバイト数。
    pub compaction_bytes_written: u64,
    /// Compaction 所要時間（合計 ms）。
    pub compaction_duration_ms: u64,
}

/// メトリクススナップショット（設計書 §11.1 に対応）。
#[derive(Debug, Clone, PartialEq)]
pub struct LsmMetricsSnapshot {
    /// WAL 書き込みバイト数。
    pub wal_write_bytes: u64,
    /// WAL fsync 所要時間（合計 ms）。
    pub wal_sync_duration_ms: u64,
    /// MemTable サイズ（推定、バイト）。
    pub memtable_size_bytes: u64,
    /// MemTable フラッシュ回数。
    pub memtable_flush_count: u64,
    /// SSTable 読み取りバイト数。
    pub sstable_read_bytes: u64,
    /// レベル別 SSTable 数（スナップショット時点）。
    pub sstable_count_per_level: Vec<usize>,
    /// バッファプールのヒット率（0.0..=1.0）。
    pub buffer_pool_hit_rate: f64,
    /// バッファプール使用メモリ（推定、バイト）。
    pub buffer_pool_size_bytes: u64,
    /// Compaction 書き込みバイト数。
    pub compaction_bytes_written: u64,
    /// Compaction 所要時間（合計 ms）。
    pub compaction_duration_ms: u64,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    use crate::kv::KVStore;
    use crate::kv::KVTransaction;
    use crate::lsm::buffer_pool::{BufferPool, BufferPoolConfig};
    use crate::lsm::sstable::{SSTableConfig, SSTableEntry, SSTableReader, SSTableWriter};
    use crate::lsm::wal::{SyncMode, WalConfig};
    use crate::lsm::{LsmKV, LsmKVConfig};
    use crate::types::TxnMode;

    #[test]
    fn wal_write_bytes_increments_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };
        let store = LsmKV::open_with_config(dir.path(), cfg).unwrap();

        let before = store.metrics().wal_write_bytes;
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        tx.commit_self().unwrap();

        let after = store.metrics().wal_write_bytes;
        assert!(after > before);
    }

    #[test]
    fn memtable_flush_count_increments_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = LsmKV::open(dir.path()).unwrap();
        let before = store.metrics().memtable_flush_count;
        store.flush().unwrap();
        let after = store.metrics().memtable_flush_count;
        assert_eq!(after, before + 1);
    }

    #[test]
    fn sstable_read_bytes_increments_on_reader_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");

        {
            let mut w = SSTableWriter::create(&path, SSTableConfig::default()).unwrap();
            w.append(SSTableEntry {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                timestamp: 10,
                sequence: 1,
            })
            .unwrap();
            w.finish().unwrap();
        }

        let metrics = LsmMetrics::default();
        let pool = BufferPool::new(BufferPoolConfig {
            capacity: 1024 * 1024,
            min_block_age_ms: 0,
        });

        let before = metrics.counters_snapshot().sstable_read_bytes;
        let mut r = SSTableReader::open(&path).unwrap();
        let got = r
            .get_with_buffer_pool(&pool, &metrics, 1, b"k", 10)
            .unwrap();
        assert!(got.is_some());
        let after = metrics.counters_snapshot().sstable_read_bytes;
        assert!(after > before);
    }

    #[test]
    fn snapshot_contains_level_counts_and_buffer_pool_values() {
        let dir = tempfile::tempdir().unwrap();
        let store = LsmKV::open(dir.path()).unwrap();
        let snap = store.metrics();
        assert!(!snap.sstable_count_per_level.is_empty());
        assert!(snap.buffer_pool_hit_rate >= 0.0 && snap.buffer_pool_hit_rate <= 1.0);
        assert_eq!(snap.buffer_pool_size_bytes, 0);
    }
}
