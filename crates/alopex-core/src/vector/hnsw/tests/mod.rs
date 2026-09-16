mod config_tests;
mod graph_tests;
mod storage_tests;

use super::{HnswConfig, HnswIndex, HnswTransactionState};
use crate::vector::Metric;

#[test]
fn staged_batch_updates_stats_once_and_rollback_restores_the_graph() {
    let mut index = HnswIndex::create("staged-batch", HnswConfig::default().with_dimension(2))
        .expect("valid configuration creates an index");
    let entries = [
        (&b"a"[..], &[-1.0, 0.0][..], &b""[..]),
        (&b"b"[..], &[0.0, 1.0][..], &b""[..]),
        (&b"c"[..], &[1.0, 0.0][..], &b""[..]),
    ];
    let mut state = HnswTransactionState::default();

    index
        .upsert_staged_batch(entries, &mut state)
        .expect("a valid batch is staged");
    assert_eq!(index.stats().node_count, 3);

    index.rollback(&mut state).expect("rollback succeeds");
    assert_eq!(index.stats().node_count, 0);
}

#[test]
fn staged_existing_batch_rewires_moved_vectors() {
    const COUNT: usize = 64;
    let mut index = HnswIndex::create(
        "staged-existing",
        HnswConfig::default()
            .with_dimension(2)
            .with_metric(Metric::L2),
    )
    .expect("valid configuration creates an index");
    for value in 0..COUNT {
        index
            .upsert(&(value as u64).to_be_bytes(), &[value as f32, 0.0], b"")
            .expect("initial vector inserts");
    }
    let before = index
        .graph
        .read()
        .expect("graph lock is available")
        .nodes
        .iter()
        .map(|node| node.as_ref().map(|node| node.neighbors.clone()))
        .collect::<Vec<_>>();

    let keys: Vec<_> = (0..COUNT)
        .map(|value| (value as u64).to_be_bytes())
        .collect();
    let vectors: Vec<_> = (0..COUNT)
        .map(|value| [((COUNT - 1 - value) * 17) as f32, (value * value) as f32])
        .collect();
    let entries: Vec<_> = keys
        .iter()
        .zip(&vectors)
        .map(|(key, vector)| (&key[..], &vector[..], &b""[..]))
        .collect();
    let mut state = HnswTransactionState::default();
    index
        .upsert_staged_batch(entries, &mut state)
        .expect("existing vectors are updated in one batch");

    let after = index
        .graph
        .read()
        .expect("graph lock is available")
        .nodes
        .iter()
        .map(|node| node.as_ref().map(|node| node.neighbors.clone()))
        .collect::<Vec<_>>();
    assert_ne!(before, after, "moving vectors must repair HNSW links");
}
