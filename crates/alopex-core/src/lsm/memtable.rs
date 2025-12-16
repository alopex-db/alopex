//! MemTable implementation for the LSM-tree storage engine.
//!
//! This module provides an in-memory, ordered map with MVCC-style versioning. Each write inserts a
//! new version identified by `(timestamp, sequence)`. Reads at a given `read_timestamp` return the
//! latest version whose timestamp is `<= read_timestamp`.
//!
//! Internally, versions are stored in a single `BTreeMap` using a composite key:
//! `user_key || 0x00 || (!timestamp as BE u64) || (!sequence as BE u64)`.
//! This keeps versions for the same user key contiguous while ordering newer versions first.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::types::{Key, Value};

/// MemTable entry (one MVCC version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    /// Value bytes; `None` indicates a tombstone (delete marker).
    pub value: Option<Value>,
    /// MVCC timestamp.
    pub timestamp: u64,
    /// Sequence number for tie-breaking within the same timestamp.
    pub sequence: u64,
}

/// MemTable configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemTableConfig {
    /// Flush threshold in bytes (default: 64MB).
    pub flush_threshold: usize,
    /// Maximum immutable MemTable count (default: 4).
    pub max_immutable_count: usize,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            flush_threshold: 64 * 1024 * 1024,
            max_immutable_count: 4,
        }
    }
}

fn encode_be_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn invert_u64(v: u64) -> u64 {
    u64::MAX - v
}

fn internal_key_prefix(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 1);
    out.extend_from_slice(user_key);
    out.push(0);
    out
}

fn internal_key(user_key: &[u8], timestamp: u64, sequence: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 1 + 16);
    out.extend_from_slice(user_key);
    out.push(0);
    out.extend_from_slice(&encode_be_u64(invert_u64(timestamp)));
    out.extend_from_slice(&encode_be_u64(invert_u64(sequence)));
    out
}

fn decode_user_key(internal_key: &[u8]) -> &[u8] {
    // internal_key = user_key || 0x00 || inv_ts(8) || inv_seq(8)
    // user_key length = len - 1 - 16
    let user_len = internal_key
        .len()
        .checked_sub(1 + 16)
        .expect("internal key has fixed trailer");
    &internal_key[..user_len]
}

fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut out = prefix.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != 0xFF {
            out[i] = out[i].wrapping_add(1);
            out.truncate(i + 1);
            return Some(out);
        }
    }
    None
}

fn update_min(atom: &AtomicU64, v: u64) {
    let mut cur = atom.load(Ordering::Relaxed);
    while v < cur {
        match atom.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => cur = next,
        }
    }
}

fn update_max(atom: &AtomicU64, v: u64) {
    let mut cur = atom.load(Ordering::Relaxed);
    while v > cur {
        match atom.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => cur = next,
        }
    }
}

/// In-memory, ordered map for LSM writes (MVCC).
#[derive(Debug)]
pub struct MemTable {
    /// Data store mapping internal composite keys to a versioned entry.
    data: RwLock<BTreeMap<Vec<u8>, MemTableEntry>>,
    /// Current memory usage (approx bytes).
    memory_usage: AtomicUsize,
    /// Minimum MVCC timestamp observed.
    min_timestamp: AtomicU64,
    /// Maximum MVCC timestamp observed.
    max_timestamp: AtomicU64,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    /// Create an empty MemTable.
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            memory_usage: AtomicUsize::new(0),
            min_timestamp: AtomicU64::new(u64::MAX),
            max_timestamp: AtomicU64::new(0),
        }
    }

    /// Current approximate memory usage in bytes.
    ///
    /// This is best-effort accounting intended for coarse thresholds (e.g. flush triggers).
    pub fn memory_usage_bytes(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Minimum timestamp inserted so far.
    pub fn min_timestamp(&self) -> Option<u64> {
        let v = self.min_timestamp.load(Ordering::Relaxed);
        if v == u64::MAX {
            None
        } else {
            Some(v)
        }
    }

    /// Maximum timestamp inserted so far.
    pub fn max_timestamp(&self) -> Option<u64> {
        let v = self.max_timestamp.load(Ordering::Relaxed);
        if self.memory_usage_bytes() == 0 {
            None
        } else {
            Some(v)
        }
    }

    fn insert_entry(&self, user_key: &[u8], entry: MemTableEntry) {
        let ikey = internal_key(user_key, entry.timestamp, entry.sequence);
        let value_len = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
        let approx_bytes = ikey.len().saturating_add(value_len);

        let mut data = self.data.write().expect("memtable lock poisoned");
        if let Some(old) = data.insert(ikey, entry.clone()) {
            let old_value_len = old.value.as_ref().map(|v| v.len()).unwrap_or(0);
            let old_key_len = internal_key(user_key, old.timestamp, old.sequence).len();
            let old_bytes = old_key_len.saturating_add(old_value_len);
            self.memory_usage
                .fetch_sub(old_bytes.min(self.memory_usage_bytes()), Ordering::Relaxed);
        }
        self.memory_usage.fetch_add(approx_bytes, Ordering::Relaxed);
        drop(data);

        update_min(&self.min_timestamp, entry.timestamp);
        update_max(&self.max_timestamp, entry.timestamp);
    }

    /// Insert a Put (value may be empty).
    pub fn put(&self, key: Key, value: Value, timestamp: u64, sequence: u64) {
        self.insert_entry(
            &key,
            MemTableEntry {
                value: Some(value),
                timestamp,
                sequence,
            },
        );
    }

    /// Insert a Delete tombstone.
    pub fn delete(&self, key: Key, timestamp: u64, sequence: u64) {
        self.insert_entry(
            &key,
            MemTableEntry {
                value: None,
                timestamp,
                sequence,
            },
        );
    }

    /// Get the latest visible entry for `key` at `read_timestamp`.
    pub fn get(&self, key: &[u8], read_timestamp: u64) -> Option<MemTableEntry> {
        let prefix = internal_key_prefix(key);
        let start = internal_key(key, read_timestamp, u64::MAX);
        let end = next_prefix(&prefix);

        let data = self.data.read().expect("memtable lock poisoned");
        let range = match end {
            Some(end_key) => data.range((Included(start), Excluded(end_key))),
            None => data.range((Included(start), Unbounded)),
        };
        for (k, entry) in range {
            if decode_user_key(k) != key {
                break;
            }
            if entry.timestamp <= read_timestamp {
                return Some(entry.clone());
            }
        }
        None
    }

    fn collect_scan(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        read_timestamp: u64,
    ) -> Vec<(Key, MemTableEntry)> {
        let data = self.data.read().expect("memtable lock poisoned");
        let mut out = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        for (k, entry) in data.range((start, end)) {
            let user_key = decode_user_key(k);
            if last_user_key.as_deref() == Some(user_key) {
                continue;
            }
            if entry.timestamp > read_timestamp {
                // Newer than the read snapshot; keep scanning within the same user key group.
                // Because versions are sorted newest-first, we can't set last_user_key yet.
                continue;
            }
            last_user_key = Some(user_key.to_vec());
            out.push((user_key.to_vec(), entry.clone()));
        }
        out
    }

    /// Scan keys with the given prefix, returning at most one visible version per user key.
    pub fn scan_prefix(&self, prefix: &[u8], read_timestamp: u64) -> Vec<(Key, MemTableEntry)> {
        // Use internal key space boundary to avoid accidentally starting in the middle of the
        // version trailer region for a user key.
        let start = Included(internal_key_prefix(prefix));
        let end = next_prefix(prefix).map(Excluded).unwrap_or(Unbounded);
        self.collect_scan(start, end, read_timestamp)
    }

    /// Scan keys in `[start, end)`, returning at most one visible version per user key.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Vec<(Key, MemTableEntry)> {
        self.collect_scan(
            Included(start.to_vec()),
            Excluded(end.to_vec()),
            read_timestamp,
        )
    }

    /// Convert this MemTable into an immutable snapshot.
    pub fn freeze(self) -> ImmutableMemTable {
        let min_timestamp = self.min_timestamp();
        let max_timestamp = self.max_timestamp();
        let memory_usage = self.memory_usage.load(Ordering::Relaxed);
        let data = self.data.into_inner().expect("memtable lock poisoned");
        ImmutableMemTable {
            data: Arc::new(data),
            memory_usage,
            min_timestamp,
            max_timestamp,
        }
    }
}

/// Read-only MemTable snapshot.
#[derive(Debug, Clone)]
pub struct ImmutableMemTable {
    data: Arc<BTreeMap<Vec<u8>, MemTableEntry>>,
    memory_usage: usize,
    min_timestamp: Option<u64>,
    max_timestamp: Option<u64>,
}

/// ImmutableMemTable のキャッシュ管理。
///
/// - `max_immutable_count` を超えないように、追加時にエビクションを行う。
/// - フラッシュ中（`flushing == true`）の MemTable はエビクトしない。
///
/// 注: この構造体はスレッドセーフではないため、呼び出し側で外側のロック（例: `RwLock`）を行うこと。
#[derive(Debug)]
pub struct ImmutableMemTableCache {
    max_immutable_count: usize,
    next_id: u64,
    entries: VecDeque<ImmutableMemTableCacheEntry>,
}

/// `ImmutableMemTableCache` 内で ImmutableMemTable を識別するための ID。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImmutableMemTableId(u64);

#[derive(Debug)]
struct ImmutableMemTableCacheEntry {
    id: ImmutableMemTableId,
    table: Arc<ImmutableMemTable>,
    flushing: bool,
}

/// `try_push` の結果。
#[derive(Debug)]
pub struct ImmutableMemTablePushOutcome {
    /// 追加されたテーブルの ID。
    pub id: ImmutableMemTableId,
    /// エビクトされたテーブル（0 件以上）。
    pub evicted: Vec<ImmutableMemTableEvicted>,
}

/// エビクトされた ImmutableMemTable。
#[derive(Debug)]
pub struct ImmutableMemTableEvicted {
    /// エビクトされたテーブルの ID。
    pub id: ImmutableMemTableId,
    /// エビクトされたテーブル。
    pub table: Arc<ImmutableMemTable>,
}

impl ImmutableMemTableCache {
    /// 新しいキャッシュを作成する。
    pub fn new(max_immutable_count: usize) -> Self {
        Self {
            max_immutable_count,
            next_id: 1,
            entries: VecDeque::new(),
        }
    }

    /// 保持している immutable MemTable 数。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 空かどうか。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `max_immutable_count` を返す。
    pub fn max_immutable_count(&self) -> usize {
        self.max_immutable_count
    }

    /// immutable MemTable を追加する。
    ///
    /// `max_immutable_count` に達している場合は、エビクションできる候補を選び、空きが作れた場合のみ追加する。
    /// フラッシュ中の候補しかない場合は `None` を返して追加しない。
    pub fn try_push(
        &mut self,
        table: Arc<ImmutableMemTable>,
    ) -> Option<ImmutableMemTablePushOutcome> {
        if self.max_immutable_count == 0 {
            return None;
        }

        let mut evicted = Vec::new();
        while self.entries.len() >= self.max_immutable_count {
            let victim_index = self.select_eviction_candidate_index()?;
            let victim = self
                .entries
                .remove(victim_index)
                .expect("candidate index is in range");
            evicted.push(ImmutableMemTableEvicted {
                id: victim.id,
                table: victim.table,
            });
        }

        let id = ImmutableMemTableId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.entries.push_back(ImmutableMemTableCacheEntry {
            id,
            table,
            flushing: false,
        });
        Some(ImmutableMemTablePushOutcome { id, evicted })
    }

    /// ID で immutable MemTable を取得する（保持していれば）。
    pub fn get(&self, id: ImmutableMemTableId) -> Option<Arc<ImmutableMemTable>> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| Arc::clone(&e.table))
    }

    /// フラッシュ中フラグを設定する（見つからなければ `false`）。
    pub fn set_flushing(&mut self, id: ImmutableMemTableId, flushing: bool) -> bool {
        let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) else {
            return false;
        };
        entry.flushing = flushing;
        true
    }

    /// ID 指定でキャッシュから取り除く（見つからなければ `None`）。
    pub fn remove(&mut self, id: ImmutableMemTableId) -> Option<Arc<ImmutableMemTable>> {
        let index = self.entries.iter().position(|e| e.id == id)?;
        let entry = self.entries.remove(index)?;
        Some(entry.table)
    }

    fn select_eviction_candidate_index(&self) -> Option<usize> {
        // 優先度:
        // 1) flushing ではないこと（必須）
        // 2) max_timestamp が小さい（古い）ものを優先
        // 3) 同一なら memory_usage が大きいものを優先（よりメモリを回収）
        // 4) 最後に ID が小さい（古い生成順）もの
        let mut best: Option<(usize, u64, usize, u64)> = None;
        for (idx, entry) in self.entries.iter().enumerate() {
            if entry.flushing {
                continue;
            }
            let ts = entry.table.max_timestamp().unwrap_or(0);
            let mem = entry.table.memory_usage_bytes();
            let id = entry.id.0;

            let key = (idx, ts, mem, id);
            best = match best {
                None => Some(key),
                Some((best_idx, best_ts, best_mem, best_id)) => {
                    let better = (ts < best_ts)
                        || (ts == best_ts && mem > best_mem)
                        || (ts == best_ts && mem == best_mem && id < best_id);
                    if better {
                        Some((idx, ts, mem, id))
                    } else {
                        Some((best_idx, best_ts, best_mem, best_id))
                    }
                }
            };
        }
        best.map(|(idx, _, _, _)| idx)
    }
}

impl ImmutableMemTable {
    /// Current approximate memory usage in bytes.
    ///
    /// This is best-effort accounting intended for coarse thresholds (e.g. flush triggers).
    pub fn memory_usage_bytes(&self) -> usize {
        self.memory_usage
    }

    /// Minimum timestamp observed in this snapshot (if any).
    pub fn min_timestamp(&self) -> Option<u64> {
        self.min_timestamp
    }

    /// Maximum timestamp observed in this snapshot (if any).
    pub fn max_timestamp(&self) -> Option<u64> {
        self.max_timestamp
    }

    /// Get the latest visible entry for `key` at `read_timestamp`.
    pub fn get(&self, key: &[u8], read_timestamp: u64) -> Option<MemTableEntry> {
        let prefix = internal_key_prefix(key);
        let start = internal_key(key, read_timestamp, u64::MAX);
        let end = next_prefix(&prefix);

        let range = match end {
            Some(end_key) => self.data.range((Included(start), Excluded(end_key))),
            None => self.data.range((Included(start), Unbounded)),
        };
        for (k, entry) in range {
            if decode_user_key(k) != key {
                break;
            }
            if entry.timestamp <= read_timestamp {
                return Some(entry.clone());
            }
        }
        None
    }

    fn collect_scan(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        read_timestamp: u64,
    ) -> Vec<(Key, MemTableEntry)> {
        let mut out = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        for (k, entry) in self.data.range((start, end)) {
            let user_key = decode_user_key(k);
            if last_user_key.as_deref() == Some(user_key) {
                continue;
            }
            if entry.timestamp > read_timestamp {
                continue;
            }
            last_user_key = Some(user_key.to_vec());
            out.push((user_key.to_vec(), entry.clone()));
        }
        out
    }

    /// Scan keys with the given prefix, returning at most one visible version per user key.
    pub fn scan_prefix(&self, prefix: &[u8], read_timestamp: u64) -> Vec<(Key, MemTableEntry)> {
        let start = Included(internal_key_prefix(prefix));
        let end = next_prefix(prefix).map(Excluded).unwrap_or(Unbounded);
        self.collect_scan(start, end, read_timestamp)
    }

    /// Scan keys in `[start, end)`, returning at most one visible version per user key.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Vec<(Key, MemTableEntry)> {
        self.collect_scan(
            Included(start.to_vec()),
            Excluded(end.to_vec()),
            read_timestamp,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_obeys_read_timestamp_and_sequence() {
        let mem = MemTable::new();
        mem.put(b"k".to_vec(), b"v1".to_vec(), 10, 1);
        mem.put(b"k".to_vec(), b"v2".to_vec(), 20, 1);
        mem.put(b"k".to_vec(), b"v2b".to_vec(), 20, 2);

        assert_eq!(mem.get(b"k", 9), None);
        assert_eq!(mem.get(b"k", 10).unwrap().value.unwrap(), b"v1".to_vec());
        assert_eq!(mem.get(b"k", 20).unwrap().value.unwrap(), b"v2b".to_vec());
        assert_eq!(mem.get(b"k", 999).unwrap().value.unwrap(), b"v2b".to_vec());
    }

    #[test]
    fn tombstone_is_visible() {
        let mem = MemTable::new();
        mem.put(b"k".to_vec(), b"v".to_vec(), 10, 1);
        mem.delete(b"k".to_vec(), 20, 1);

        let e = mem.get(b"k", 20).unwrap();
        assert_eq!(e.value, None);
    }

    #[test]
    fn scan_prefix_returns_latest_visible_per_key() {
        let mem = MemTable::new();
        mem.put(b"p:a".to_vec(), b"v1".to_vec(), 10, 1);
        mem.put(b"p:a".to_vec(), b"v2".to_vec(), 20, 1);
        mem.put(b"p:b".to_vec(), b"x".to_vec(), 15, 1);
        mem.delete(b"p:c".to_vec(), 12, 1);
        mem.put(b"q:z".to_vec(), b"no".to_vec(), 99, 1);

        let got = mem.scan_prefix(b"p:", 20);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, b"p:a".to_vec());
        assert_eq!(got[0].1.value.as_deref(), Some(b"v2".as_slice()));
        assert_eq!(got[1].0, b"p:b".to_vec());
        assert_eq!(got[2].0, b"p:c".to_vec());
        assert!(got[2].1.value.is_none());
    }

    #[test]
    fn scan_range_is_end_exclusive_and_obeys_read_timestamp() {
        let mem = MemTable::new();
        mem.put(b"a".to_vec(), b"1".to_vec(), 10, 1);
        mem.put(b"b".to_vec(), b"2_old".to_vec(), 10, 1);
        mem.put(b"b".to_vec(), b"2_new".to_vec(), 20, 1);
        mem.delete(b"c".to_vec(), 12, 1);
        mem.put(b"d".to_vec(), b"4".to_vec(), 40, 1);

        // [b, d) => b と c のみ（d は end で除外）
        let got = mem.scan_range(b"b", b"d", 15);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, b"b".to_vec());
        // ts=20 は見えないため古い版が返る
        assert_eq!(got[0].1.value.as_deref(), Some(b"2_old".as_slice()));
        assert_eq!(got[1].0, b"c".to_vec());
        // tombstone は返る（value == None）
        assert!(got[1].1.value.is_none());
    }

    #[test]
    fn freeze_produces_read_only_snapshot() {
        let mem = MemTable::new();
        mem.put(b"k".to_vec(), b"v".to_vec(), 10, 1);
        let imm = mem.freeze();
        assert_eq!(imm.get(b"k", 10).unwrap().value.unwrap(), b"v".to_vec());
    }
}

#[cfg(test)]
mod cache {
    use super::*;

    fn frozen_with_ts(ts: u64, mem: usize) -> Arc<ImmutableMemTable> {
        let memtable = MemTable::new();
        memtable.put(b"k".to_vec(), vec![0u8; mem], ts, 1);
        Arc::new(memtable.freeze())
    }

    #[test]
    fn evicts_oldest_non_flushing_when_full() {
        let mut cache = ImmutableMemTableCache::new(2);

        let a = cache.try_push(frozen_with_ts(10, 10)).unwrap().id;
        let _b = cache.try_push(frozen_with_ts(20, 10)).unwrap().id;
        let outcome = cache.try_push(frozen_with_ts(30, 10)).unwrap();

        assert_eq!(cache.len(), 2);
        assert_eq!(outcome.evicted.len(), 1);
        assert_eq!(outcome.evicted[0].id, a);
        assert!(cache.get(a).is_none());
    }

    #[test]
    fn does_not_evict_flushing_entries() {
        let mut cache = ImmutableMemTableCache::new(2);

        let a = cache.try_push(frozen_with_ts(10, 10)).unwrap().id;
        let b = cache.try_push(frozen_with_ts(20, 10)).unwrap().id;
        assert!(cache.set_flushing(a, true));
        assert!(cache.set_flushing(b, true));

        // どちらも flushing のため、追加できない（max を超えることはしない）
        assert!(cache.try_push(frozen_with_ts(30, 10)).is_none());
        assert_eq!(cache.len(), 2);
        assert!(cache.get(a).is_some());
        assert!(cache.get(b).is_some());
    }

    #[test]
    fn eviction_prefers_older_then_larger_memory() {
        let mut cache = ImmutableMemTableCache::new(2);

        // 同一 timestamp の場合はメモリが大きい方が先にエビクトされる。
        let a = cache.try_push(frozen_with_ts(10, 5)).unwrap().id;
        let b = cache.try_push(frozen_with_ts(10, 50)).unwrap().id;
        let outcome = cache.try_push(frozen_with_ts(11, 5)).unwrap();

        assert_eq!(outcome.evicted.len(), 1);
        assert_eq!(outcome.evicted[0].id, b);
        assert!(cache.get(a).is_some());
        assert!(cache.get(b).is_none());
    }
}
