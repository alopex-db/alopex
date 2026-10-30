//! Incremental aggregate statistics; node payloads are scanned once after load.
use std::collections::HashMap;

use super::{graph::HnswGraph, types::HnswNode, HnswStats};

#[derive(Clone, Copy, Default)]
struct Contribution {
    level: Option<usize>,
    edges: u64,
    bytes: u64,
}

impl Contribution {
    fn of(node: Option<&HnswNode>) -> Self {
        let Some(node) = node.filter(|node| !node.deleted) else {
            return Self::default();
        };
        let edges = node.neighbors.iter().map(|level| level.len() as u64).sum();
        let bytes = (node.vector.len() * 4 + node.key.len() + node.metadata.len()) as u64
            + node
                .neighbors
                .iter()
                .map(|level| (level.len() * 4 + 24) as u64)
                .sum::<u64>()
            + std::mem::size_of::<HnswNode>() as u64;
        Self {
            level: Some(node.neighbors.len().saturating_sub(1)),
            edges,
            bytes,
        }
    }
}

#[derive(Clone, Default)]
struct Totals {
    nodes: u64,
    edges: u64,
    bytes: u64,
    levels: Vec<u64>,
}

impl Totals {
    fn add(&mut self, value: Contribution) {
        if let Some(level) = value.level {
            self.nodes += 1;
            self.levels.resize(self.levels.len().max(level + 1), 0);
            self.levels[level] += 1;
        }
        self.edges += value.edges;
        self.bytes += value.bytes;
    }

    fn remove(&mut self, value: Contribution) {
        if let Some(level) = value.level {
            self.nodes = self.nodes.checked_sub(1).expect("accounted active node");
            self.levels[level] = self.levels[level].checked_sub(1).expect("accounted level");
        }
        self.edges = self
            .edges
            .checked_sub(value.edges)
            .expect("accounted edges");
        self.bytes = self
            .bytes
            .checked_sub(value.bytes)
            .expect("accounted bytes");
    }
}

#[derive(Clone, Default)]
pub(crate) struct GraphStatistics {
    totals: Option<Totals>,
    // Only the FIRST before-image since the last refresh is subtracted.
    pending: HashMap<u32, Contribution>,
    #[cfg(test)]
    last_refresh_slots: usize,
}

impl GraphStatistics {
    pub(crate) fn record_before(&mut self, id: u32, node: Option<&HnswNode>) {
        if self.totals.is_some() {
            self.pending
                .entry(id)
                .or_insert_with(|| Contribution::of(node));
        }
    }
}

impl HnswGraph {
    pub(crate) fn refresh_statistics(&mut self) -> HnswStats {
        let totals = if let Some(mut totals) = self.statistics.totals.take() {
            #[cfg(test)]
            {
                self.statistics.last_refresh_slots = self.statistics.pending.len();
            }
            for (id, before) in self.statistics.pending.drain() {
                totals.remove(before);
                totals.add(Contribution::of(
                    self.nodes.get(id as usize).and_then(|node| node.as_deref()),
                ));
            }
            totals
        } else {
            #[cfg(test)]
            {
                self.statistics.last_refresh_slots = self.nodes.len();
            }
            let mut totals = Totals::default();
            for node in self.nodes.iter() {
                totals.add(Contribution::of(node.as_deref()));
            }
            self.statistics.pending.clear();
            totals
        };
        // The public distribution retains the existing max-level boundary,
        // including the empty level-zero bucket on an empty graph.
        let mut levels = totals.levels.clone();
        levels.resize(self.max_level + 1, 0);
        let result = HnswStats {
            node_count: totals.nodes,
            deleted_count: self.deleted_count,
            level_distribution: levels,
            memory_bytes: totals.bytes
                + (self.nodes.capacity() * 8 + self.free_list.capacity() * 4) as u64,
            avg_edges_per_node: if totals.nodes == 0 {
                0.0
            } else {
                totals.edges as f64 / totals.nodes as f64
            },
        };
        // Keep every level in the internal aggregate, even when metadata's
        // current max level is lower; a later refresh must subtract accurately.
        self.statistics.totals = Some(totals);
        result
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::super::HnswTransactionState;
    use super::*;
    use crate::kv::memory::MemoryKV;
    use crate::kv::KVStore;
    use crate::TxnMode;
    use crate::{HnswConfig, HnswIndex, Metric};

    fn graph() -> HnswGraph {
        HnswGraph::new(
            HnswConfig::default()
                .with_dimension(2)
                .with_metric(Metric::L2),
        )
        .unwrap()
    }

    fn verify(graph: &mut HnswGraph) {
        let expected = HnswIndex::compute_stats_full(graph);
        let actual = graph.refresh_statistics();
        assert_eq!(actual.node_count, expected.node_count);
        assert_eq!(actual.deleted_count, expected.deleted_count);
        assert_eq!(actual.level_distribution, expected.level_distribution);
        assert_eq!(actual.memory_bytes, expected.memory_bytes);
        assert_eq!(actual.avg_edges_per_node, expected.avg_edges_per_node);
    }

    #[test]
    fn repeated_changes_delete_revive_and_slot_reuse_match_full_scan() {
        let mut graph = graph();
        verify(&mut graph);
        for id in 0u8..6 {
            graph.upsert(&[id], &[id as f32, 1.0], &[id]).unwrap();
        }
        verify(&mut graph);
        for id in 0u8..6 {
            graph.upsert(&[id], &[2.0, id as f32], &[0; 7]).unwrap();
        }
        graph.delete(&[1]).unwrap();
        graph.delete(&[1]).unwrap();
        graph.upsert(&[1], &[4.0, 2.0], &[]).unwrap();
        graph.delete(&[3]).unwrap();
        verify(&mut graph);
        graph.compact().unwrap();
        verify(&mut graph);
        graph.upsert(&[9], &[9.0, 1.0], &[1, 2, 3]).unwrap();
        verify(&mut graph);
    }

    #[test]
    fn clean_fork_and_repeated_refresh_visit_no_node_slots() {
        let mut index = HnswIndex::create(
            "stats",
            HnswConfig::default()
                .with_dimension(2)
                .with_metric(Metric::L2),
        )
        .unwrap();
        for id in 0u8..16 {
            index.upsert(&[id], &[id as f32, 1.0], b"").unwrap();
        }
        let fork = index.fork_owned();
        let mut graph = fork.graph.write().unwrap();
        assert_eq!(graph.statistics.last_refresh_slots, 0);
        verify(&mut graph);
        assert_eq!(graph.statistics.last_refresh_slots, 0);
    }

    #[test]
    fn pending_fork_save_load_and_rollback_keep_independent_totals() {
        let store = MemoryKV::new();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut index = HnswIndex::create(
            "stats",
            HnswConfig::default()
                .with_dimension(2)
                .with_metric(Metric::L2),
        )
        .unwrap();
        index.upsert(b"base", &[1.0, 0.0], b"m").unwrap();
        index.save(&mut txn).unwrap();
        let mut state = HnswTransactionState::default();
        index
            .upsert_staged(b"new", &[2.0, 0.0], b"meta", &mut state)
            .unwrap();
        let fork = index.fork_owned();
        assert_eq!(fork.stats().node_count, 2);
        verify(&mut fork.graph.write().unwrap());
        index.commit_staged(&mut txn, &mut state).unwrap();
        let loaded = HnswIndex::load("stats", &mut txn).unwrap();
        verify(&mut loaded.graph.write().unwrap());
        assert_eq!(loaded.stats().memory_bytes, index.stats().memory_bytes);
        index.delete_staged(b"base", &mut state).unwrap();
        index.rollback(&mut state).unwrap();
        assert_eq!(index.stats().node_count, 2);
        verify(&mut index.graph.write().unwrap());
        assert_eq!(fork.stats().node_count, 2);
    }

    #[test]
    fn bounded_operation_traces_match_full_statistics_model() {
        let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
            cases: 32,
            failure_persistence: None,
            ..Default::default()
        });
        runner
            .run(
                &proptest::collection::vec((0u8..5, 0u8..8, 0u8..8), 0..32),
                |steps| {
                    let mut graph = graph();
                    verify(&mut graph);
                    for (operation, key, size) in steps {
                        match operation {
                            0 | 1 => {
                                graph
                                    .upsert(
                                        &[key],
                                        &[key as f32, size as f32],
                                        &vec![size; size as usize],
                                    )
                                    .unwrap();
                            }
                            2 => {
                                graph.delete(&[key]).unwrap();
                            }
                            3 => {
                                graph.compact().unwrap();
                            }
                            _ => {
                                let mut fork = graph.clone();
                                fork.upsert(&[99], &[1.0, 2.0], b"fork").unwrap();
                                verify(&mut fork);
                            }
                        }
                        // Exercise both a burst with one before-image per slot and
                        // incremental refreshes, without changing the reference model.
                        if size.is_multiple_of(2) {
                            verify(&mut graph);
                        }
                    }
                    verify(&mut graph);
                    Ok(())
                },
            )
            .unwrap();
    }
}
