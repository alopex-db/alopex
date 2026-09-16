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

#[test]
fn staged_existing_batch_retains_recall_after_repeated_updates() {
    const COUNT: usize = 128;
    let mut index = HnswIndex::create(
        "staged-recall",
        HnswConfig::default()
            .with_dimension(2)
            .with_metric(Metric::L2),
    )
    .expect("valid configuration creates an index");
    let keys: Vec<_> = (0..COUNT)
        .map(|value| (value as u64).to_be_bytes())
        .collect();

    for key in &keys {
        let value = u64::from_be_bytes(*key) as f32;
        index
            .upsert(key, &[value, 0.0], b"")
            .expect("initial vector inserts");
    }

    let mut updated_vectors = Vec::with_capacity(COUNT);
    for round in 0..2_u32 {
        updated_vectors = keys
            .iter()
            .map(|key| {
                let seed = u64::from_be_bytes(*key) as u32 + (round + 1) * 10_000;
                let x = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                let y = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                [
                    (x >> 16) as f32 / u16::MAX as f32,
                    (y >> 16) as f32 / u16::MAX as f32,
                ]
            })
            .collect();
        let entries = keys
            .iter()
            .zip(&updated_vectors)
            .map(|(key, vector)| (&key[..], &vector[..], &b""[..]));
        let mut state = HnswTransactionState::default();
        index
            .upsert_staged_batch(entries, &mut state)
            .expect("existing vectors are updated in one batch");
    }

    let mut correct = 0;
    for (expected_key, vector) in keys.iter().zip(&updated_vectors) {
        let query = [vector[0] + 0.001, vector[1] - 0.001];
        let expected = keys
            .iter()
            .zip(&updated_vectors)
            .min_by(|(_, left), (_, right)| {
                let left_distance = (query[0] - left[0]).powi(2) + (query[1] - left[1]).powi(2);
                let right_distance = (query[0] - right[0]).powi(2) + (query[1] - right[1]).powi(2);
                left_distance.total_cmp(&right_distance)
            })
            .map(|(key, _)| key)
            .expect("non-empty dataset");
        assert_eq!(
            expected, expected_key,
            "query must have a unique nearest key"
        );

        let (results, _) = index
            .search(&query, 1, Some(COUNT))
            .expect("search succeeds");
        correct += usize::from(results[0].key == *expected);
    }
    assert!(
        correct as f32 / COUNT as f32 >= 0.99,
        "recall@1 after updates is {correct}/{COUNT}"
    );
}
