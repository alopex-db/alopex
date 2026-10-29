//! Small graph persistence contracts; no scale or timing assertions.
use std::collections::{BTreeMap, HashSet};

use super::super::{HnswConfig, HnswIndex, HnswStorage, HnswTransactionState};
use crate::kv::{memory::MemoryKV, KVStore, KVTransaction};
use crate::txn::TxnManager;
use crate::{Metric, TxnMode};

fn index() -> HnswIndex {
    HnswIndex::create(
        "dirty",
        HnswConfig::default()
            .with_dimension(2)
            .with_metric(Metric::L2)
            .with_m(2)
            .with_ef_construction(8),
    )
    .unwrap()
}

fn save(index: &HnswIndex, kv: &MemoryKV) {
    let mut tx = kv.begin(TxnMode::ReadWrite).unwrap();
    index.save(&mut tx).unwrap();
    kv.txn_manager().commit(tx).unwrap();
}

fn expected(index: &HnswIndex) -> BTreeMap<Vec<u8>, Vec<u8>> {
    HnswStorage::new("dirty")
        .build_save_plan(&index.graph.read().unwrap())
        .unwrap()
        .into_iter()
        .collect()
}

fn assert_stored(index: &HnswIndex, kv: &MemoryKV) {
    let mut tx = kv.begin(TxnMode::ReadOnly).unwrap();
    let actual: BTreeMap<_, _> = tx.scan_prefix(b"hnsw:").unwrap().collect();
    assert_eq!(actual, expected(index));
    let loaded = HnswIndex::load("dirty", &mut tx).unwrap();
    assert_eq!(expected(&loaded), expected(index));
    assert!(loaded.graph.read().unwrap().dirty_nodes.is_empty());
}

fn commit(
    index: &mut HnswIndex,
    state: &mut HnswTransactionState,
    kv: &MemoryKV,
) -> HashSet<Vec<u8>> {
    let mut tx = kv.begin(TxnMode::ReadWrite).unwrap();
    index.commit_staged(&mut tx, state).unwrap();
    let mut writes = HashSet::new();
    assert!(tx.visit_pending_write_keys(&mut |key| {
        if key.starts_with(b"hnsw:node:") {
            writes.insert(key.to_vec());
        }
    }));
    kv.txn_manager().commit(tx).unwrap();
    assert!(index.graph.read().unwrap().dirty_nodes.is_empty());
    assert_stored(index, kv);
    writes
}

#[test]
fn insertion_saves_reverse_edges_without_rewriting_untouched_nodes() {
    let kv = MemoryKV::new();
    let mut index = index();
    for id in 0..32u8 {
        index.upsert(&[id], &[id as f32, 1.0], &[]).unwrap();
    }
    save(&index, &kv);
    let before = expected(&index);
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(&[99], &[8.5, 1.0], &[], &mut state)
        .unwrap();
    let after = expected(&index);
    let changed: HashSet<_> = after
        .iter()
        .filter(|(key, value)| key.starts_with(b"hnsw:node:") && before.get(*key) != Some(*value))
        .map(|(key, _)| key.clone())
        .collect();
    assert!(changed.len() > 1, "must exercise a reverse-edge update");
    let writes = commit(&mut index, &mut state, &kv);
    assert!(changed.is_subset(&writes));
    assert!(writes.len() < 33, "single insertion rewrote every node");
    // A subsequent flush with no mutations must not rewrite graph nodes.
    assert!(commit(&mut index, &mut state, &kv).is_empty());
}

#[test]
fn rollback_restores_graph_and_dirty_set_before_retry() {
    let kv = MemoryKV::new();
    let mut index = index();
    index.upsert(b"a", &[1.0, 0.0], &[]).unwrap();
    index.upsert(b"b", &[2.0, 0.0], &[]).unwrap();
    save(&index, &kv);
    let before = expected(&index);
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(b"c", &[1.5, 0.0], &[], &mut state)
        .unwrap();
    index.rollback(&mut state).unwrap();
    assert_eq!(expected(&index), before);
    assert!(index.graph.read().unwrap().dirty_nodes.is_empty());
    index.delete_staged(b"a", &mut state).unwrap();
    commit(&mut index, &mut state, &kv);
}

#[test]
fn compaction_and_slot_reuse_persist_removed_reverse_edges() {
    let kv = MemoryKV::new();
    let mut index = index();
    for id in 0..6u8 {
        index.upsert(&[id], &[id as f32, 0.0], &[]).unwrap();
    }
    save(&index, &kv);
    let mut state = HnswTransactionState::default();
    index.delete_staged(&[2], &mut state).unwrap();
    commit(&mut index, &mut state, &kv);
    index.compact().unwrap();
    commit(&mut index, &mut state, &kv);
    index
        .upsert_staged(&[9], &[2.5, 0.0], &[], &mut state)
        .unwrap();
    commit(&mut index, &mut state, &kv);
}

#[test]
fn failed_save_preserves_dirty_nodes_for_retry() {
    let kv = MemoryKV::new();
    let mut index = index();
    index.upsert(b"a", &[1.0, 0.0], &[]).unwrap();
    save(&index, &kv);
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(b"b", &[2.0, 0.0], &[], &mut state)
        .unwrap();
    let before = index.graph.read().unwrap().dirty_nodes.clone();
    let mut read_only = kv.begin(TxnMode::ReadOnly).unwrap();
    assert!(index.commit_staged(&mut read_only, &mut state).is_err());
    assert_eq!(index.graph.read().unwrap().dirty_nodes, before);
    drop(read_only);
    commit(&mut index, &mut state, &kv);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn bounded_mutations_match_full_format_v1_save(operations in proptest::collection::vec((0u8..4, 0u8..8, 0u8..16), 1..17)) {
        let kv = MemoryKV::new();
        let mut index = index();
        save(&index, &kv);
        for (operation, id, coordinate) in operations {
            let mut state = HnswTransactionState::default();
            match operation {
                0 | 1 => index.upsert_staged(&[id], &[coordinate as f32, 1.0], &[coordinate], &mut state).unwrap(),
                2 => { index.delete_staged(&[id], &mut state).unwrap(); },
                _ => { index.compact().unwrap(); },
            }
            commit(&mut index, &mut state, &kv);
        }
    }
}

#[test]
fn rolled_back_full_save_does_not_leave_unsaved_nodes_missing() {
    let kv = MemoryKV::new();
    let mut index = index();
    index.upsert(b"a", &[1.0, 0.0], &[]).unwrap();
    let mut tx = kv.begin(TxnMode::ReadWrite).unwrap();
    index.save(&mut tx).unwrap();
    tx.rollback_self().unwrap();
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(b"b", &[2.0, 0.0], &[], &mut state)
        .unwrap();
    commit(&mut index, &mut state, &kv);
}

#[test]
fn rolled_back_flush_and_different_store_require_full_save() {
    let kv = MemoryKV::new();
    let other = MemoryKV::new();
    let mut index = index();
    for id in 0..4u8 {
        index.upsert(&[id], &[id as f32, 0.0], &[]).unwrap();
    }
    save(&index, &kv);
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(&[5], &[1.5, 0.0], &[], &mut state)
        .unwrap();
    let mut tx = kv.begin(TxnMode::ReadWrite).unwrap();
    index.commit_staged(&mut tx, &mut state).unwrap();
    tx.rollback_self().unwrap();
    // The handle still contains its flushed graph, but the target has its old header.
    commit(&mut index, &mut state, &kv);
    commit(&mut index, &mut state, &other);
}

struct FailAfterPuts<T> {
    inner: T,
    remaining: Option<usize>,
}

impl<'a, T: KVTransaction<'a>> KVTransaction<'a> for FailAfterPuts<T> {
    fn id(&self) -> crate::TxnId {
        self.inner.id()
    }
    fn mode(&self) -> TxnMode {
        self.inner.mode()
    }
    fn get(&mut self, key: &crate::Key) -> crate::Result<Option<crate::Value>> {
        self.inner.get(key)
    }
    fn put(&mut self, key: crate::Key, value: crate::Value) -> crate::Result<()> {
        if let Some(remaining) = &mut self.remaining {
            if *remaining == 0 {
                return Err(crate::Error::TxnConflict);
            }
            *remaining -= 1;
        }
        self.inner.put(key, value)
    }
    fn delete(&mut self, key: crate::Key) -> crate::Result<()> {
        self.inner.delete(key)
    }
    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> crate::Result<Box<dyn Iterator<Item = (crate::Key, crate::Value)> + '_>> {
        self.inner.scan_prefix(prefix)
    }
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> crate::Result<Box<dyn Iterator<Item = (crate::Key, crate::Value)> + '_>> {
        self.inner.scan_range(start, end)
    }
    fn commit_self(self) -> crate::Result<()> {
        self.inner.commit_self()
    }
    fn rollback_self(self) -> crate::Result<()> {
        self.inner.rollback_self()
    }
}

#[test]
fn partial_node_write_failure_can_retry_in_same_transaction() {
    let kv = MemoryKV::new();
    let mut index = index();
    for id in 0..4u8 {
        index.upsert(&[id], &[id as f32, 0.0], &[]).unwrap();
    }
    save(&index, &kv);
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged(&[8], &[1.5, 0.0], &[], &mut state)
        .unwrap();
    let dirty = index.graph.read().unwrap().dirty_nodes.clone();
    let mut tx = FailAfterPuts {
        inner: kv.begin(TxnMode::ReadWrite).unwrap(),
        remaining: Some(2),
    };
    assert!(index.commit_staged(&mut tx, &mut state).is_err());
    assert_eq!(index.graph.read().unwrap().dirty_nodes, dirty);
    let mut pending = 0;
    assert!(tx.inner.visit_pending_write_keys(&mut |_| pending += 1));
    assert_eq!(pending, 2, "failure must follow actual buffered writes");
    tx.remaining = None;
    index.commit_staged(&mut tx, &mut state).unwrap();
    tx.commit_self().unwrap();
    assert_stored(&index, &kv);
}

#[test]
fn staged_batch_preserves_pruned_neighbors_after_reopen() {
    let kv = MemoryKV::new();
    let mut index = index();
    save(&index, &kv);
    let mut state = HnswTransactionState::default();
    let keys: Vec<_> = (0..8u8).map(|id| vec![id]).collect();
    let vectors: Vec<_> = (0..8).map(|id| vec![id as f32, 1.0]).collect();
    let entries: Vec<_> = keys
        .iter()
        .zip(&vectors)
        .map(|(k, v)| (k.as_slice(), v.as_slice(), b"".as_slice()))
        .collect();
    index.upsert_staged_batch(&entries, &mut state).unwrap();
    commit(&mut index, &mut state, &kv);
}
