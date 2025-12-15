//! SSTable データブロック向けのバッファプール（LRU キャッシュ）。
//!
//! 目的: SSTable のデータブロックをメモリに保持して、ディスク I/O を削減する（設計書 §3.5）。

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// バッファプール設定（設計書 §3.5.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolConfig {
    /// キャッシュ容量（バイト）。
    pub capacity: usize,
    /// 最小ブロック保持時間（ミリ秒）。
    ///
    /// 容量超過でエビクションが必要な場合、LRU の末尾からこの保持時間を満たす候補を探索する。
    /// ただし候補が見つからない場合は、容量制限を優先して LRU 末尾をエビクトする（ベストエフォート）。
    pub min_block_age_ms: u64,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            capacity: 128 * 1024 * 1024,
            min_block_age_ms: 0,
        }
    }
}

/// SSTable ブロックを一意に識別する ID（設計書 §3.5.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId {
    /// SSTable ファイル ID。
    pub file_id: u64,
    /// ブロックのファイル内オフセット。
    pub block_offset: u64,
}

/// キャッシュ対象のデータブロック（バイト列）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBlock {
    bytes: Vec<u8>,
}

impl DataBlock {
    /// 新しいデータブロックを作成する。
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// ブロックの長さ（バイト）。
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// ブロックが空かどうか。
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// ブロックのバイト列を参照する。
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
struct CacheEntry {
    block: Arc<DataBlock>,
    size: usize,
    inserted_at: Instant,
}

/// バッファプール統計（スレッドセーフ）。
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    hits: AtomicU64,
    misses: AtomicU64,
    puts: AtomicU64,
    evictions: AtomicU64,
    oversized_puts: AtomicU64,
}

/// `BufferPoolStats` のスナップショット。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolStatsSnapshot {
    /// キャッシュヒット数。
    pub hits: u64,
    /// キャッシュミス数。
    pub misses: u64,
    /// put 呼び出し回数（キャッシュ可能なサイズに限らない）。
    pub puts: u64,
    /// エビクション回数。
    pub evictions: u64,
    /// 容量を超えるためキャッシュしなかった put 回数。
    pub oversized_puts: u64,
}

impl BufferPoolStats {
    fn snapshot(&self) -> BufferPoolStatsSnapshot {
        BufferPoolStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            oversized_puts: self.oversized_puts.load(Ordering::Relaxed),
        }
    }
}

/// SSTable データブロック用の LRU バッファプール。
#[derive(Debug)]
pub struct BufferPool {
    cache: RwLock<LruCache<BlockId, CacheEntry>>,
    capacity: usize,
    min_block_age: Duration,
    current_size: AtomicUsize,
    stats: BufferPoolStats,
}

impl BufferPool {
    /// 新しい `BufferPool` を作成する。
    pub fn new(config: BufferPoolConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new()),
            capacity: config.capacity,
            min_block_age: Duration::from_millis(config.min_block_age_ms),
            current_size: AtomicUsize::new(0),
            stats: BufferPoolStats::default(),
        }
    }

    /// 現在の使用量（バイト、概算ではなく実測）。
    pub fn current_size_bytes(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    /// 容量（バイト）。
    pub fn capacity_bytes(&self) -> usize {
        self.capacity
    }

    /// 統計を取得する。
    pub fn stats(&self) -> BufferPoolStatsSnapshot {
        self.stats.snapshot()
    }

    /// ブロックを取得する（ヒット時は LRU を更新）。
    pub fn get(&self, id: &BlockId) -> Option<Arc<DataBlock>> {
        let mut cache = self.cache.write().expect("poisoned BufferPool lock");
        match cache.get(id) {
            Some(entry) => {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&entry.block))
            }
            None => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// ブロックを追加する（容量超過時は LRU でエビクション）。
    ///
    /// 戻り値:
    /// - `true`: キャッシュに格納した
    /// - `false`: ブロックが容量より大きく、キャッシュしなかった
    pub fn put(&self, id: BlockId, block: Arc<DataBlock>) -> bool {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);

        let size = block.len();
        if size > self.capacity {
            self.stats.oversized_puts.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let mut cache = self.cache.write().expect("poisoned BufferPool lock");
        let now = Instant::now();

        if let Some(old) = cache.put(
            id,
            CacheEntry {
                block,
                size,
                inserted_at: now,
            },
        ) {
            let old_size = old.size;
            match size.cmp(&old_size) {
                std::cmp::Ordering::Greater => {
                    self.current_size
                        .fetch_add(size - old_size, Ordering::Relaxed);
                }
                std::cmp::Ordering::Less => {
                    self.current_size
                        .fetch_sub(old_size - size, Ordering::Relaxed);
                }
                std::cmp::Ordering::Equal => {}
            }
        } else {
            self.current_size.fetch_add(size, Ordering::Relaxed);
        }

        self.evict_while_over_capacity_locked(&mut cache, now);
        true
    }

    /// 明示的に LRU で 1 件エビクトする（エビクトできなければ `None`）。
    pub fn evict_one(&self) -> Option<(BlockId, Arc<DataBlock>)> {
        let mut cache = self.cache.write().expect("poisoned BufferPool lock");
        let now = Instant::now();
        let evicted = self.evict_one_locked(&mut cache, now, self.min_block_age);
        if let Some((id, entry)) = evicted {
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            return Some((id, entry.block));
        }
        None
    }

    fn evict_while_over_capacity_locked(
        &self,
        cache: &mut LruCache<BlockId, CacheEntry>,
        now: Instant,
    ) {
        while self.current_size.load(Ordering::Relaxed) > self.capacity {
            let Some((_, entry)) = self.evict_one_locked(cache, now, self.min_block_age) else {
                break;
            };
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn evict_one_locked(
        &self,
        cache: &mut LruCache<BlockId, CacheEntry>,
        now: Instant,
        min_age: Duration,
    ) -> Option<(BlockId, CacheEntry)> {
        if cache.len() == 0 {
            return None;
        }

        if min_age == Duration::from_millis(0) {
            return cache.pop_lru();
        }

        let mut candidate = cache.tail_index();
        while let Some(index) = candidate {
            let entry = cache.value_at(index)?;
            if now.saturating_duration_since(entry.inserted_at) >= min_age {
                return cache.remove_at(index);
            }
            candidate = cache.prev_index(index);
        }

        // 候補が見つからなければ、容量制限を優先して LRU 末尾をエビクトする（ベストエフォート）。
        cache.pop_lru()
    }
}

#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Debug)]
struct LruCache<K, V> {
    map: HashMap<K, usize>,
    nodes: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Copy,
{
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn tail_index(&self) -> Option<usize> {
        self.tail
    }

    fn prev_index(&self, index: usize) -> Option<usize> {
        self.nodes
            .get(index)
            .and_then(|n| n.as_ref().and_then(|n| n.prev))
    }

    fn value_at(&self, index: usize) -> Option<&V> {
        self.nodes
            .get(index)
            .and_then(|n| n.as_ref().map(|n| &n.value))
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        let index = *self.map.get(key)?;
        self.move_to_front(index);
        self.value_at(index)
    }

    fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(&index) = self.map.get(&key) {
            let old = self
                .nodes
                .get_mut(index)
                .and_then(|n| n.as_mut())
                .map(|n| std::mem::replace(&mut n.value, value));
            self.move_to_front(index);
            return old;
        }

        let index = self.alloc_index();
        let node = Node {
            key,
            value,
            prev: None,
            next: None,
        };
        self.nodes[index] = Some(node);
        self.map.insert(key, index);
        self.len += 1;
        self.attach_front(index);
        None
    }

    fn pop_lru(&mut self) -> Option<(K, V)> {
        let index = self.tail?;
        self.remove_at(index)
    }

    fn remove_at(&mut self, index: usize) -> Option<(K, V)> {
        let node = self.nodes.get_mut(index)?.take()?;
        self.detach(index, node.prev, node.next);
        self.map.remove(&node.key);
        self.free.push(index);
        self.len -= 1;
        Some((node.key, node.value))
    }

    fn alloc_index(&mut self) -> usize {
        if let Some(index) = self.free.pop() {
            return index;
        }
        let index = self.nodes.len();
        self.nodes.push(None);
        index
    }

    fn move_to_front(&mut self, index: usize) {
        if Some(index) == self.head {
            return;
        }
        let (prev, next) = match self.nodes.get(index).and_then(|n| n.as_ref()) {
            Some(node) => (node.prev, node.next),
            None => return,
        };
        self.detach(index, prev, next);
        self.attach_front(index);
    }

    fn detach(&mut self, index: usize, prev: Option<usize>, next: Option<usize>) {
        if let Some(prev) = prev {
            if let Some(Some(node)) = self.nodes.get_mut(prev) {
                node.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next) = next {
            if let Some(Some(node)) = self.nodes.get_mut(next) {
                node.prev = prev;
            }
        } else {
            self.tail = prev;
        }

        if let Some(Some(node)) = self.nodes.get_mut(index) {
            node.prev = None;
            node.next = None;
        }
    }

    fn attach_front(&mut self, index: usize) {
        let old_head = self.head;
        self.head = Some(index);
        if let Some(old_head) = old_head {
            if let Some(Some(node)) = self.nodes.get_mut(old_head) {
                node.prev = Some(index);
            }
            if let Some(Some(node)) = self.nodes.get_mut(index) {
                node.next = Some(old_head);
                node.prev = None;
            }
        } else {
            self.tail = Some(index);
            if let Some(Some(node)) = self.nodes.get_mut(index) {
                node.prev = None;
                node.next = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: u8, len: usize) -> Arc<DataBlock> {
        Arc::new(DataBlock::new(vec![n; len]))
    }

    #[test]
    fn put_get_updates_stats() {
        let pool = BufferPool::new(BufferPoolConfig {
            capacity: 1024,
            min_block_age_ms: 0,
        });

        let id = BlockId {
            file_id: 1,
            block_offset: 0,
        };
        assert!(pool.put(id, block(7, 10)));

        assert!(pool.get(&id).is_some());
        assert!(pool
            .get(&BlockId {
                file_id: 1,
                block_offset: 999,
            })
            .is_none());

        let s = pool.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.puts, 1);
    }

    #[test]
    fn lru_evicts_least_recent() {
        let pool = BufferPool::new(BufferPoolConfig {
            capacity: 8,
            min_block_age_ms: 0,
        });

        let a = BlockId {
            file_id: 1,
            block_offset: 0,
        };
        let b = BlockId {
            file_id: 1,
            block_offset: 1,
        };
        let c = BlockId {
            file_id: 1,
            block_offset: 2,
        };

        assert!(pool.put(a, block(1, 4)));
        assert!(pool.put(b, block(2, 4)));

        // A を最近使ったことにする → 次の put では B が追い出される。
        assert!(pool.get(&a).is_some());
        assert!(pool.put(c, block(3, 4)));

        assert!(pool.get(&a).is_some());
        assert!(pool.get(&b).is_none());
        assert!(pool.get(&c).is_some());
    }

    #[test]
    fn oversized_block_is_not_cached() {
        let pool = BufferPool::new(BufferPoolConfig {
            capacity: 4,
            min_block_age_ms: 0,
        });

        let id = BlockId {
            file_id: 1,
            block_offset: 0,
        };
        assert!(!pool.put(id, block(1, 5)));
        assert_eq!(pool.current_size_bytes(), 0);

        let s = pool.stats();
        assert_eq!(s.puts, 1);
        assert_eq!(s.oversized_puts, 1);
    }
}
