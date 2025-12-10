use std::sync::{Arc, Mutex};

use alopex_core::{HnswConfig, HnswIndex, Metric, TxnMode};
use alopex_embedded::Database;

fn config() -> HnswConfig {
    HnswConfig::default()
        .with_dimension(2)
        .with_metric(Metric::L2)
        .with_m(8)
        .with_ef_construction(32)
}

#[test]
fn hnsw_lifecycle_via_embedded_api() {
    let db = Database::new();
    db.create_hnsw_index("vec_idx", config()).unwrap();

    // 挿入と検索
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_to_hnsw("vec_idx", b"a", &[0.0, 0.0], b"ma")
        .unwrap();
    txn.upsert_to_hnsw("vec_idx", b"b", &[1.0, 0.0], b"mb")
        .unwrap();
    txn.commit().unwrap();

    let (results, _) = db.search_hnsw("vec_idx", &[0.1, 0.0], 1, None).unwrap();
    assert_eq!(results[0].key, b"a");

    // 削除とコンパクション
    let mut del_txn = db.begin(TxnMode::ReadWrite).unwrap();
    assert!(del_txn.delete_from_hnsw("vec_idx", b"a").unwrap());
    del_txn.commit().unwrap();

    let stats = db.get_hnsw_stats("vec_idx").unwrap();
    assert_eq!(stats.node_count, 1);
    assert_eq!(stats.deleted_count, 1);

    db.compact_hnsw_index("vec_idx").unwrap();
    let stats_after = db.get_hnsw_stats("vec_idx").unwrap();
    assert_eq!(stats_after.node_count, 1);
    assert_eq!(stats_after.deleted_count, 0);

    // DROP で完全削除
    db.drop_hnsw_index("vec_idx").unwrap();
    let err = db.get_hnsw_stats("vec_idx").unwrap_err();
    match err {
        alopex_embedded::Error::Core(alopex_core::Error::IndexNotFound { .. }) => {}
        other => panic!("存在しないインデックスで異常終了すべき: {:?}", other),
    }
}

#[test]
fn transaction_commit_and_rollback_are_respected() {
    let db = Database::new();
    db.create_hnsw_index("vec_idx", config()).unwrap();

    // コミットされる挿入
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_to_hnsw("vec_idx", b"keep", &[0.0, 0.0], b"mk")
        .unwrap();
    txn.commit().unwrap();

    // ロールバックされる挿入
    let mut txn2 = db.begin(TxnMode::ReadWrite).unwrap();
    txn2.upsert_to_hnsw("vec_idx", b"rollback", &[10.0, 0.0], b"mr")
        .unwrap();
    txn2.rollback().unwrap();

    let (results, _) = db.search_hnsw("vec_idx", &[0.0, 0.0], 5, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, b"keep");
}

#[test]
fn callbacks_fire_on_core_index() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let searches = Arc::new(Mutex::new(Vec::new()));
    let mut index = HnswIndex::create("cb_idx", config()).unwrap();

    {
        let calls = calls.clone();
        index.on_insert(move |stats| {
            calls.lock().unwrap().push(stats.node_id);
        });
    }
    {
        let searches = searches.clone();
        index.on_search(move |stats| {
            searches.lock().unwrap().push(stats.nodes_visited);
        });
    }

    index
        .upsert(b"a", &[0.0, 0.0], b"ma")
        .expect("挿入に失敗しない");
    index
        .upsert(b"b", &[1.0, 0.0], b"mb")
        .expect("挿入に失敗しない");
    let (_r, _s) = index.search(&[0.0, 0.0], 1, None).unwrap();

    let insert_calls = calls.lock().unwrap();
    assert_eq!(insert_calls.len(), 2);

    let search_calls = searches.lock().unwrap();
    assert_eq!(search_calls.len(), 1);
    assert!(search_calls[0] > 0);
}
