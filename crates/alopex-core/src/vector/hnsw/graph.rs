//! HNSW graph algorithms and data structure implementation.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

use rand::Rng;

use super::types::{HnswConfig, HnswNode, HnswSearchResult, HnswStats, InsertStats, SearchStats};
use crate::vector::simd::{select_kernel, DistanceKernel};
use crate::vector::{validate_dimensions, CompactionResult, Metric};
use crate::{Error, Result};

/// Core in-memory HNSW graph.
#[allow(dead_code)]
pub struct HnswGraph {
    pub(crate) nodes: Vec<Option<HnswNode>>,
    pub(crate) key_to_node: HashMap<Vec<u8>, u32>,
    pub(crate) entry_point: Option<u32>,
    pub(crate) max_level: usize,
    pub(crate) config: HnswConfig,
    pub(crate) kernel: Box<dyn DistanceKernel>,
    pub(crate) free_list: Vec<u32>,
    pub(crate) active_count: u64,
    pub(crate) deleted_count: u64,
}

#[allow(dead_code)]
impl HnswGraph {
    /// Creates a new, empty HNSW graph using the provided configuration.
    pub fn new(config: HnswConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            nodes: Vec::new(),
            key_to_node: HashMap::new(),
            entry_point: None,
            max_level: 0,
            kernel: select_kernel(),
            config,
            free_list: Vec::new(),
            active_count: 0,
            deleted_count: 0,
        })
    }

    /// Inserts a vector into the graph, returning the assigned node id.
    pub fn insert(&mut self, key: &[u8], vector: &[f32], metadata: &[u8]) -> Result<u32> {
        validate_dimensions(self.config.dimension, vector.len())?;

        if self.key_to_node.contains_key(key) {
            return Err(Error::InvalidParameter {
                param: "key".to_string(),
                reason: "duplicate key".to_string(),
            });
        }

        let level = self.random_level();
        let node_id = self.allocate_node_id();
        let neighbors = vec![Vec::new(); level + 1];
        let node = HnswNode {
            key: key.to_vec(),
            vector: vector.to_vec(),
            metadata: metadata.to_vec(),
            neighbors,
            deleted: false,
        };

        if let Some(slot) = self.nodes.get_mut(node_id as usize) {
            *slot = Some(node);
        } else {
            self.nodes.push(Some(node));
        }
        self.key_to_node.insert(key.to_vec(), node_id);

        // First node shortcut.
        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
            self.max_level = level;
            self.active_count = 1;
            return Ok(node_id);
        }

        // Remember the current entry/max before wiring to avoid isolating the new node.
        let prev_entry = self.entry_point.expect("entry point set above");
        let mut enter_point = prev_entry;

        // If current entry is deleted, retarget to a live entry (or initialize if none).
        if self.node(enter_point).is_none_or(|n| n.deleted) {
            if let Some(new_entry) = self.select_new_entry_point() {
                enter_point = new_entry;
                self.entry_point = Some(new_entry);
                self.max_level = self.calculate_max_level();
            } else {
                // Everything deleted; treat this insertion as first node initialization.
                self.entry_point = Some(node_id);
                self.max_level = level;
                self.active_count = 1;
                return Ok(node_id);
            }
        }

        // Baseline after potential retarget.
        let baseline_max_level = self.max_level;

        // Greedy search on upper layers until just above the node level.
        if baseline_max_level > level {
            for l in (level + 1..=baseline_max_level).rev() {
                enter_point = self.greedy_search(vector, enter_point, l);
            }
        }

        // Connect across layers down to 0.
        for l in (0..=level.min(self.max_level)).rev() {
            let candidates =
                self.search_layer(vector, enter_point, l, self.config.ef_construction, None);
            let max_conn = if l == 0 {
                self.config.m * 2
            } else {
                self.config.m
            };
            let selected = self.select_neighbors_heuristic(&candidates, max_conn);
            self.connect_new_node(node_id, &selected, l);

            // Prune neighbor lists to maintain degree constraints.
            for &n in &selected {
                let neighbor_max = if l == 0 {
                    self.config.m * 2
                } else {
                    self.config.m
                };
                self.prune_neighbors(n, l, neighbor_max);
            }

            if let Some(&first) = selected.first() {
                enter_point = first;
            }
        }

        self.active_count += 1;

        // Only after connections are made, update entry point if this node is now the highest level.
        if level > baseline_max_level {
            self.entry_point = Some(node_id);
            self.max_level = level;
        }

        // Hook for future callback usage (HnswIndex).
        let _ = InsertStats {
            node_id,
            level,
            connected_neighbors: self.nodes[node_id as usize]
                .as_ref()
                .map(|n| n.neighbors.first().map(|v| v.len()).unwrap_or(0))
                .unwrap_or(0),
        };

        Ok(node_id)
    }

    /// Executes a top-k search. Returns results and search statistics.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<(Vec<HnswSearchResult>, SearchStats)> {
        validate_dimensions(self.config.dimension, query.len())?;
        if k == 0 || self.entry_point.is_none() || self.active_count == 0 {
            return Ok((Vec::new(), SearchStats::default()));
        }

        let mut stats = SearchStats::default();
        let mut max_level = self.max_level;
        let mut enter_point = match self.entry_point {
            Some(ep) if self.node(ep).is_some_and(|n| !n.deleted) => ep,
            _ => match self.select_new_entry_point() {
                Some(ep) => {
                    // Use the recalculated max level for this search to avoid traversing stale layers.
                    max_level = self.calculate_max_level();
                    ep
                }
                None => return Ok((Vec::new(), stats)),
            },
        };

        // Greedy descent from the top layer to level 1.
        if max_level > 0 {
            for l in (1..=max_level).rev() {
                enter_point = self.greedy_search_with_stats(query, enter_point, l, &mut stats);
            }
        }

        let mut ef = ef_search;
        if ef < k {
            ef = k;
        }

        let candidates = self.search_layer(query, enter_point, 0, ef, Some(&mut stats));

        let mut results: Vec<HnswSearchResult> = candidates
            .into_iter()
            .filter_map(|c| self.node(c.node_id).map(|n| (c, n)))
            .filter(|(_, n)| !n.deleted)
            .map(|(c, n)| HnswSearchResult {
                key: n.key.clone(),
                distance: c.score,
                metadata: n.metadata.clone(),
            })
            .collect();

        results.sort_by(|a, b| {
            b.distance
                .total_cmp(&a.distance)
                .then_with(|| a.key.cmp(&b.key))
        });
        if results.len() > k {
            results.truncate(k);
        }

        Ok((results, stats))
    }

    /// Logically deletes a node by key. Returns true if a node was deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let Some(&node_id) = self.key_to_node.get(key) else {
            return Ok(false);
        };
        let Some(node) = self
            .nodes
            .get_mut(node_id as usize)
            .and_then(|n| n.as_mut())
        else {
            return Ok(false);
        };
        if node.deleted {
            return Ok(false);
        }

        node.deleted = true;
        self.active_count = self.active_count.saturating_sub(1);
        self.deleted_count += 1;
        Ok(true)
    }

    /// Returns the node id for the given key, if present.
    pub fn find_node_id(&self, key: &[u8]) -> Option<u32> {
        self.key_to_node.get(key).copied()
    }

    /// Physically removes deleted nodes, pruning neighbor references.
    pub fn compact(&mut self) -> Result<CompactionResult> {
        if self.deleted_count == 0 {
            return Ok(CompactionResult {
                old_segment_id: 0,
                new_segment_id: None,
                vectors_removed: 0,
                space_reclaimed: 0,
            });
        }

        let mut removed_ids = Vec::new();
        let mut removed_keys = Vec::new();
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if let Some(n) = node {
                if n.deleted {
                    removed_keys.push(n.key.clone());
                    removed_ids.push(idx as u32);
                    *node = None;
                }
            }
        }

        for key in removed_keys {
            self.key_to_node.remove(&key);
        }

        for id in &removed_ids {
            self.free_list.push(*id);
        }

        self.reconnect_edges_after_compaction(&removed_ids);
        self.entry_point = self.select_new_entry_point();
        self.max_level = self.calculate_max_level();
        let removed = removed_ids.len() as u64;
        self.deleted_count = 0;

        Ok(CompactionResult {
            old_segment_id: 0,
            new_segment_id: None,
            vectors_removed: removed,
            space_reclaimed: 0,
        })
    }

    /// Returns current statistics (counts only; size estimation is deferred).
    pub fn stats(&self) -> HnswStats {
        HnswStats {
            node_count: self.active_count,
            deleted_count: self.deleted_count,
            level_distribution: self.level_distribution(),
            memory_bytes: 0,
            avg_edges_per_node: self.average_edges(),
        }
    }

    fn allocate_node_id(&mut self) -> u32 {
        self.free_list.pop().unwrap_or(self.nodes.len() as u32)
    }

    fn random_level(&self) -> usize {
        if self.config.m <= 1 {
            return 0;
        }
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen();
        let ml = 1.0_f64 / (self.config.m as f64).ln();
        (-r.ln() * ml).floor() as usize
    }

    fn greedy_search(&self, target: &[f32], start: u32, level: usize) -> u32 {
        self.greedy_search_with_stats(target, start, level, &mut SearchStats::default())
    }

    fn greedy_search_with_stats(
        &self,
        target: &[f32],
        start: u32,
        level: usize,
        stats: &mut SearchStats,
    ) -> u32 {
        let mut current = start;
        let mut current_score = self.distance(target, current, stats);
        loop {
            let mut best = current;
            let mut best_score = current_score;
            if let Some(node) = self.node(current) {
                if let Some(neighbors) = node.neighbors.get(level) {
                    for &n in neighbors {
                        let s = self.distance(target, n, stats);
                        if s > best_score {
                            best = n;
                            best_score = s;
                        }
                    }
                }
            }
            if best == current {
                break;
            }
            current = best;
            current_score = best_score;
        }
        current
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_point: u32,
        level: usize,
        ef: usize,
        mut stats: Option<&mut SearchStats>,
    ) -> Vec<ScoredEntry> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut best: BinaryHeap<Reverse<ScoredEntry>> = BinaryHeap::new();

        let mut scratch = SearchStats::default();
        let stats_ref: &mut SearchStats = match stats {
            Some(ref mut s) => s,
            None => &mut scratch,
        };

        let entry_score = self.distance(query, entry_point, stats_ref);
        let entry = ScoredEntry {
            node_id: entry_point,
            score: entry_score,
        };
        visited.insert(entry_point);
        candidates.push(entry.clone());
        best.push(Reverse(entry));

        while let Some(candidate) = candidates.pop() {
            let worst_best = best.peek().map(|r| r.0.score).unwrap_or(f32::NEG_INFINITY);
            if best.len() >= ef && candidate.score <= worst_best {
                break;
            }

            if let Some(node) = self.node(candidate.node_id) {
                if let Some(neighbors) = node.neighbors.get(level) {
                    for &n in neighbors {
                        if !visited.insert(n) {
                            continue;
                        }
                        let s = self.distance(query, n, stats_ref);
                        let entry = ScoredEntry {
                            node_id: n,
                            score: s,
                        };
                        candidates.push(entry.clone());
                        best.push(Reverse(entry));
                        if best.len() > ef {
                            best.pop();
                        }
                    }
                }
            }
        }

        let mut results: Vec<ScoredEntry> = best.into_iter().map(|r| r.0).collect();
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| self.node_key(a.node_id).cmp(&self.node_key(b.node_id)))
        });
        results
    }

    fn select_neighbors_heuristic(&self, candidates: &[ScoredEntry], max: usize) -> Vec<u32> {
        let mut sorted = candidates.to_vec();
        sorted.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| self.node_key(a.node_id).cmp(&self.node_key(b.node_id)))
        });
        sorted.into_iter().take(max).map(|c| c.node_id).collect()
    }

    fn connect_new_node(&mut self, node_id: u32, neighbors: &[u32], level: usize) {
        if let Some(node) = self
            .nodes
            .get_mut(node_id as usize)
            .and_then(|n| n.as_mut())
        {
            if let Some(neigh) = node.neighbors.get_mut(level) {
                for &n in neighbors {
                    if !neigh.contains(&n) {
                        neigh.push(n);
                    }
                }
            }
        }

        for &n in neighbors {
            if let Some(node) = self.nodes.get_mut(n as usize).and_then(|n| n.as_mut()) {
                if level < node.neighbors.len() {
                    let neigh = &mut node.neighbors[level];
                    if !neigh.contains(&node_id) {
                        neigh.push(node_id);
                    }
                }
            }
        }
    }

    fn prune_neighbors(&mut self, node_id: u32, level: usize, max_degree: usize) {
        let (neighbors_snapshot, query_vec) = {
            let Some(node) = self.node(node_id) else {
                return;
            };
            if level >= node.neighbors.len() || node.neighbors[level].len() <= max_degree {
                return;
            }
            (node.neighbors[level].clone(), node.vector.clone())
        };

        let mut selected: Vec<ScoredEntry> = neighbors_snapshot
            .iter()
            .copied()
            .filter_map(|n| {
                self.node(n).map(|other| ScoredEntry {
                    node_id: n,
                    score: self.distance_raw(&query_vec, &other.vector),
                })
            })
            .collect();

        selected.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| self.node_key(a.node_id).cmp(&self.node_key(b.node_id)))
        });
        selected.truncate(max_degree);

        if let Some(node) = self
            .nodes
            .get_mut(node_id as usize)
            .and_then(|n| n.as_mut())
        {
            if level < node.neighbors.len() {
                node.neighbors[level] = selected.into_iter().map(|c| c.node_id).collect();
            }
        }
    }

    fn reconnect_edges_after_compaction(&mut self, removed_ids: &[u32]) {
        if removed_ids.is_empty() {
            return;
        }
        let removed: HashSet<u32> = removed_ids.iter().copied().collect();
        for node in self.nodes.iter_mut().flatten() {
            for neigh in node.neighbors.iter_mut() {
                neigh.retain(|id| !removed.contains(id));
            }
        }
    }

    fn select_new_entry_point(&self) -> Option<u32> {
        let mut best: Option<(u32, usize)> = None;
        for (id, node_opt) in self.nodes.iter().enumerate() {
            if let Some(node) = node_opt {
                if node.deleted {
                    continue;
                }
                let level = node.neighbors.len().saturating_sub(1);
                match best {
                    None => best = Some((id as u32, level)),
                    Some((_, l)) if level > l => best = Some((id as u32, level)),
                    _ => {}
                }
            }
        }
        best.map(|(id, _)| id)
    }

    fn calculate_max_level(&self) -> usize {
        self.nodes
            .iter()
            .filter_map(|n| n.as_ref())
            .filter(|n| !n.deleted)
            .map(|n| n.neighbors.len().saturating_sub(1))
            .max()
            .unwrap_or(0)
    }

    fn level_distribution(&self) -> Vec<u64> {
        let mut levels = Vec::new();
        for node in self.nodes.iter().flatten() {
            if node.deleted {
                continue;
            }
            let lvl = node.neighbors.len();
            if levels.len() < lvl {
                levels.resize(lvl, 0);
            }
            levels[lvl - 1] += 1;
        }
        levels
    }

    fn average_edges(&self) -> f64 {
        let mut total_edges = 0usize;
        let mut nodes = 0usize;
        for node in self.nodes.iter().flatten() {
            if node.deleted {
                continue;
            }
            total_edges += node.neighbors.iter().map(|n| n.len()).sum::<usize>();
            nodes += 1;
        }
        if nodes == 0 {
            0.0
        } else {
            total_edges as f64 / nodes as f64
        }
    }

    fn node(&self, id: u32) -> Option<&HnswNode> {
        self.nodes.get(id as usize).and_then(|n| n.as_ref())
    }

    fn node_key(&self, id: u32) -> Vec<u8> {
        self.node(id).map(|n| n.key.clone()).unwrap_or_default()
    }

    fn distance(&self, query: &[f32], node_id: u32, stats: &mut SearchStats) -> f32 {
        stats.nodes_visited = stats.nodes_visited.saturating_add(1);
        if let Some(node) = self.node(node_id) {
            stats.distance_computations = stats.distance_computations.saturating_add(1);
            return self.distance_raw(query, &node.vector);
        }
        f32::NEG_INFINITY
    }

    fn distance_raw(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            Metric::Cosine => self.kernel.cosine(a, b),
            Metric::L2 => self.kernel.l2(a, b),
            Metric::InnerProduct => self.kernel.inner_product(a, b),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct ScoredEntry {
    node_id: u32,
    score: f32,
}

impl PartialEq for ScoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredEntry {}

impl PartialOrd for ScoredEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}
