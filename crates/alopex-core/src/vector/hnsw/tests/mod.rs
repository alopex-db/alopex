mod config_tests;
mod graph_tests;
mod storage_tests;

use super::{HnswConfig, HnswIndex, HnswTransactionState};

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
