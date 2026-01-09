//! Hierarchical Navigable Small World (HNSW) vector index module.

mod graph;
mod storage;
mod types;

pub(crate) use graph::HnswGraph;
pub(crate) use storage::HnswStorage;
pub(crate) use types::HnswMetadata;
pub use types::{HnswConfig, HnswSearchResult, HnswStats, InsertStats, SearchStats};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::kv::KVTransaction;
use crate::vector::CompactionResult;
use crate::{Error, Result};
use std::collections::HashSet;

/// コンパクション待ちのタイムアウト（長時間ブロックを避けるため）。
const COMPACTION_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
type SearchCallback = Box<dyn Fn(&SearchStats) + Send + Sync>;
type InsertCallback = Box<dyn Fn(&InsertStats) + Send + Sync>;

/// HNSW インデックスの公開 API。
///
/// - `create`/`load` で生成・復元
/// - `upsert`/`delete` で更新
/// - `search` で top-k 近傍検索
/// - `compact` で削除済みノードを物理除去
pub struct HnswIndex {
    name: String,
    graph: Arc<RwLock<HnswGraph>>,
    storage: HnswStorage,
    on_search: Option<SearchCallback>,
    on_insert: Option<InsertCallback>,
    stats_cache: HnswStats,
    compacting: AtomicBool,
    compact_condvar: Condvar,
    compact_mutex: Mutex<()>,
}

impl HnswIndex {
    /// 新規インデックスを作成する（未永続化）。
    pub fn create(name: &str, config: HnswConfig) -> Result<Self> {
        let graph = HnswGraph::new(config)?;
        Ok(Self::from_graph(name, graph))
    }

    /// 既存インデックスを永続化ストアからロードする。
    pub fn load<'a, T: KVTransaction<'a>>(name: &str, txn: &mut T) -> Result<Self> {
        let storage = HnswStorage::new(name);
        let graph = storage.load(txn)?;
        Ok(Self::from_parts(name, storage, graph))
    }

    /// インデックス名を返す。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// ベクトルを挿入または更新する（既存キーは上書き）。
    pub fn upsert(&mut self, key: &[u8], vector: &[f32], metadata: &[u8]) -> Result<()> {
        self.wait_for_compaction("upsert")?;
        let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
        let node_id = graph.upsert(key, vector, metadata)?;
        self.stats_cache = Self::compute_stats(&graph);

        if let Some(callback) = &self.on_insert {
            let insert_stats = self.build_insert_stats(&graph, node_id);
            callback(&insert_stats);
        }

        Ok(())
    }

    /// Top-K 検索を実行する（読み取りロックのみ）。
    ///
    /// 注: compact 実行中は write ロックにより待たされるため、v0.3 では短時間ブロックを許容。
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Result<(Vec<HnswSearchResult>, SearchStats)> {
        let start = Instant::now();
        let graph = self.graph.read().unwrap_or_else(|e| e.into_inner());
        let ef = ef_search.unwrap_or(k.max(50));
        let (results, mut stats) = graph.search(query, k, ef)?;
        stats.search_time_us = start.elapsed().as_micros() as u64;

        if let Some(callback) = &self.on_search {
            callback(&stats);
        }

        Ok((results, stats))
    }

    /// 指定キーを論理削除する。
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.wait_for_compaction("delete")?;
        let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
        let deleted = graph.delete(key)?;
        if deleted {
            self.stats_cache = Self::compute_stats(&graph);
        }
        Ok(deleted)
    }

    /// グラフを保存する（フル保存）。
    pub fn save<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<()> {
        let graph = self.graph.read().unwrap_or_else(|e| e.into_inner());
        self.storage.save(txn, &graph)
    }

    pub(crate) fn graph_handle(&self) -> Arc<RwLock<HnswGraph>> {
        Arc::clone(&self.graph)
    }

    /// インデックスデータをストアから削除する。
    pub fn drop<'a, T: KVTransaction<'a>>(self, txn: &mut T) -> Result<()> {
        self.storage.delete(txn)
    }

    /// 現在の統計情報を返す。
    pub fn stats(&self) -> HnswStats {
        self.stats_cache.clone()
    }

    /// コンパクションを実行する（論理削除されたノードを物理削除）。
    ///
    /// v0.3 方針: グラフをクローンしてロック外で compact を実行し、短時間のスワップのみ
    /// write ロックを取得することで search をほぼブロックしないようにする。
    ///
    /// TODO(v0.4): compacting フラグの set/reset を compact_mutex と併用して
    /// Condvar 連携を厳密化する。現行でも実質動作は問題ないが明示的に整合させる。
    /// TODO(v0.4): クローン方式は巨大グラフでメモリ増分・コピーコストが増えるため、
    /// 段階的/CoW コンパクションなどへの置き換えを検討する。
    pub fn compact(&mut self) -> Result<CompactionResult> {
        self.compacting.store(true, Ordering::Release);
        // 1) 読み取りロックでクローンを取得（search を止めない）
        let mut working_graph = {
            let graph = self.graph.read().unwrap_or_else(|e| e.into_inner());
            graph.clone()
        };
        // 2) ロック外でコンパクション実行
        let result = working_graph.compact()?;
        // 3) 短時間だけ write ロックで差し替え
        {
            let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
            *graph = working_graph;
            self.stats_cache = Self::compute_stats(&graph);
        }
        self.compacting.store(false, Ordering::Release);
        self.compact_condvar.notify_all();
        Ok(result)
    }

    /// トランザクション内での upsert（変更をステージング）。
    pub fn upsert_staged(
        &mut self,
        key: &[u8],
        vector: &[f32],
        metadata: &[u8],
        state: &mut HnswTransactionState,
    ) -> Result<()> {
        self.wait_for_compaction("upsert")?;
        let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
        state.ensure_snapshot(&graph);

        let existed = graph.find_node_id(key).is_some();
        let node_id = graph.upsert(key, vector, metadata)?;
        if existed {
            state.record_upsert(node_id, false, None);
        } else {
            state.record_upsert(node_id, true, None);
        }
        self.stats_cache = Self::compute_stats(&graph);
        Ok(())
    }

    /// トランザクション内での論理削除（変更をステージング）。
    pub fn delete_staged(&mut self, key: &[u8], state: &mut HnswTransactionState) -> Result<bool> {
        self.wait_for_compaction("delete")?;
        let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
        let Some(node_id) = graph.find_node_id(key) else {
            return Ok(false);
        };
        state.ensure_snapshot(&graph);
        let deleted = graph.delete(key)?;
        if deleted {
            state.record_delete(node_id);
            self.stats_cache = Self::compute_stats(&graph);
        }
        Ok(deleted)
    }

    /// ステージした変更を保存する。
    pub fn commit_staged<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
        state: &mut HnswTransactionState,
    ) -> Result<()> {
        let (modified, inserted, deleted_keys) = state.prepare_for_commit();
        let graph = self.graph.read().unwrap_or_else(|e| e.into_inner());
        self.storage
            .save_incremental(txn, &graph, &modified, &inserted, &deleted_keys)?;
        state.clear();
        Ok(())
    }

    /// ステージした変更を破棄し、スナップショットへ完全に戻す。
    pub fn rollback(&mut self, state: &mut HnswTransactionState) -> Result<()> {
        if let Some(snapshot) = state.snapshot.take() {
            let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
            *graph = snapshot;
            self.stats_cache = Self::compute_stats(&graph);
        }
        state.clear();
        Ok(())
    }

    /// 検索コールバックを設定する。
    pub fn on_search<F>(&mut self, callback: F)
    where
        F: Fn(&SearchStats) + Send + Sync + 'static,
    {
        self.on_search = Some(Box::new(callback));
    }

    /// 挿入コールバックを設定する。
    pub fn on_insert<F>(&mut self, callback: F)
    where
        F: Fn(&InsertStats) + Send + Sync + 'static,
    {
        self.on_insert = Some(Box::new(callback));
    }

    pub(crate) fn from_graph(name: &str, graph: HnswGraph) -> Self {
        let storage = HnswStorage::new(name);
        Self::from_parts(name, storage, graph)
    }

    pub(crate) fn from_parts(name: &str, storage: HnswStorage, graph: HnswGraph) -> Self {
        let stats_cache = Self::compute_stats(&graph);
        Self {
            name: name.to_string(),
            graph: Arc::new(RwLock::new(graph)),
            storage,
            on_search: None,
            on_insert: None,
            stats_cache,
            compacting: AtomicBool::new(false),
            compact_condvar: Condvar::new(),
            compact_mutex: Mutex::new(()),
        }
    }

    fn wait_for_compaction(&self, operation: &str) -> Result<()> {
        let mut guard = self.compact_mutex.lock().unwrap();
        if self.compacting.load(Ordering::Acquire) {
            let (g, timeout) = self
                .compact_condvar
                .wait_timeout_while(guard, COMPACTION_WAIT_TIMEOUT, |_| {
                    self.compacting.load(Ordering::Acquire)
                })
                .unwrap();
            guard = g;
            if timeout.timed_out() && self.compacting.load(Ordering::Acquire) {
                return Err(Error::IndexBusy {
                    operation: operation.to_string(),
                });
            }
        }
        drop(guard);
        Ok(())
    }

    fn compute_stats(graph: &HnswGraph) -> HnswStats {
        let mut level_distribution = vec![0u64; graph.max_level + 1];
        let mut total_edges = 0u64;
        let mut active_nodes = 0u64;

        for node in graph.nodes.iter().flatten() {
            if !node.deleted {
                active_nodes += 1;
                let node_level = node.neighbors.len().saturating_sub(1);
                if node_level < level_distribution.len() {
                    level_distribution[node_level] += 1;
                }
                for level_neighbors in &node.neighbors {
                    total_edges += level_neighbors.len() as u64;
                }
            }
        }

        let avg_edges_per_node = if active_nodes > 0 {
            total_edges as f64 / active_nodes as f64
        } else {
            0.0
        };

        HnswStats {
            node_count: active_nodes,
            deleted_count: graph.deleted_count,
            level_distribution,
            memory_bytes: Self::estimate_memory_bytes(graph),
            avg_edges_per_node,
        }
    }

    fn estimate_memory_bytes(graph: &HnswGraph) -> u64 {
        let mut total_bytes: u64 = 0;

        for node in graph.nodes.iter().flatten() {
            if !node.deleted {
                total_bytes += (node.vector.len() * 4) as u64;
                total_bytes += node.key.len() as u64;
                total_bytes += node.metadata.len() as u64;
                for level_neighbors in &node.neighbors {
                    total_bytes += (level_neighbors.len() * 4) as u64;
                    total_bytes += 24;
                }
                total_bytes += 104;
            }
        }

        total_bytes += (graph.nodes.capacity() * 8) as u64;
        total_bytes += (graph.free_list.capacity() * 4) as u64;

        total_bytes
    }

    fn build_insert_stats(&self, graph: &HnswGraph, node_id: u32) -> InsertStats {
        let level = graph
            .nodes
            .get(node_id as usize)
            .and_then(|n| n.as_ref())
            .map(|n| n.neighbors.len().saturating_sub(1))
            .unwrap_or(0);
        let connected_neighbors = graph
            .nodes
            .get(node_id as usize)
            .and_then(|n| n.as_ref())
            .and_then(|n| n.neighbors.first())
            .map(|v| v.len())
            .unwrap_or(0);

        InsertStats {
            node_id,
            level,
            connected_neighbors,
        }
    }
}

/// トランザクション内の HNSW 変更を追跡する。
#[derive(Default)]
pub struct HnswTransactionState {
    snapshot: Option<HnswGraph>,
    modified_nodes: HashSet<u32>,
    inserted_nodes: HashSet<u32>,
    deleted_nodes: HashSet<u32>,
    deleted_key_indices: Vec<Vec<u8>>,
}

impl HnswTransactionState {
    fn ensure_snapshot(&mut self, graph: &HnswGraph) {
        if self.snapshot.is_none() {
            self.snapshot = Some(graph.clone());
        }
    }

    fn record_upsert(&mut self, node_id: u32, is_new: bool, old_key: Option<Vec<u8>>) {
        if is_new {
            self.inserted_nodes.insert(node_id);
        } else {
            self.modified_nodes.insert(node_id);
        }
        if let Some(key) = old_key {
            self.deleted_key_indices.push(key);
        }
    }

    fn record_delete(&mut self, node_id: u32) {
        self.deleted_nodes.insert(node_id);
        self.modified_nodes.insert(node_id);
    }

    fn prepare_for_commit(&self) -> (Vec<u32>, Vec<u32>, Vec<Vec<u8>>) {
        let modified: Vec<u32> = self
            .modified_nodes
            .difference(&self.inserted_nodes)
            .copied()
            .collect();
        let inserted: Vec<u32> = self.inserted_nodes.iter().copied().collect();
        let deleted_keys = self.deleted_key_indices.clone();
        (modified, inserted, deleted_keys)
    }

    fn clear(&mut self) {
        self.snapshot = None;
        self.modified_nodes.clear();
        self.inserted_nodes.clear();
        self.deleted_nodes.clear();
        self.deleted_key_indices.clear();
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
