use std::sync::{Arc, Mutex};

use alopex_core::{HnswConfig, HnswIndex, Metric, TxnMode};
use alopex_embedded::Database;
use tempfile::tempdir;

fn config() -> HnswConfig {
    HnswConfig::default()
        .with_dimension(2)
        .with_metric(Metric::L2)
        .with_m(8)
        .with_ef_construction(32)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_query_ef_changes_observed_search_work() {
    let db = Database::new();
    db.create_hnsw_index("breadth", config().with_m(2).with_ef_construction(16))
        .unwrap();
    let keys: Vec<_> = (0..128).map(|i| i.to_string().into_bytes()).collect();
    let vectors: Vec<_> = (0..128).map(|i| vec![i as f32, 1.0]).collect();
    let refs: Vec<_> = vectors.iter().map(Vec::as_slice).collect();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    assert_eq!(
        txn.upsert_to_hnsw_batch("breadth", &keys, &refs, None)
            .unwrap(),
        128
    );
    txn.commit().unwrap();

    let (narrow, narrow_stats) = db.search_hnsw("breadth", &vectors[64], 1, Some(1)).unwrap();
    let (wide, wide_stats) = db
        .search_hnsw("breadth", &vectors[64], 1, Some(128))
        .unwrap();
    assert_eq!(narrow.len(), 1);
    assert_eq!(wide.len(), 1);
    assert_eq!(wide[0].key, b"64");
    assert!(wide_stats.nodes_visited > narrow_stats.nodes_visited);
    assert!(narrow_stats.nodes_visited > 0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_batch_rejects_invalid_inputs_without_partial_writes() {
    let db = Database::new();
    db.create_hnsw_index("batch", config()).unwrap();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    let good = [0.0, 0.0];
    let other = [1.0, 0.0];
    let vectors = [good.as_slice(), other.as_slice()];
    let keys = [b"a".to_vec(), b"b".to_vec()];
    assert_eq!(
        txn.upsert_to_hnsw_batch("batch", &keys, &vectors, None)
            .unwrap(),
        2
    );

    let duplicate = [b"duplicate".to_vec(), b"duplicate".to_vec()];
    assert!(matches!(
        txn.upsert_to_hnsw_batch("batch", &duplicate, &vectors, None),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::InvalidParameter { .. }
        ))
    ));
    assert!(matches!(
        txn.upsert_to_hnsw_batch("batch", &[], &[], None),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::InvalidParameter { .. }
        ))
    ));
    assert!(matches!(
        txn.upsert_to_hnsw_batch("batch", &[b"length".to_vec()], &vectors, None),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::InvalidParameter { .. }
        ))
    ));
    assert!(matches!(
        txn.upsert_to_hnsw_batch("batch", &keys, &vectors, Some(&[None])),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::InvalidParameter { .. }
        ))
    ));
    let later_invalid = [good.as_slice(), &[2.0]];
    assert!(matches!(
        txn.upsert_to_hnsw_batch(
            "batch",
            &[b"must-not-appear".to_vec(), b"invalid".to_vec()],
            &later_invalid,
            None,
        ),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::DimensionMismatch { .. }
        ))
    ));
    txn.commit().unwrap();

    let (hits, _) = db.search_hnsw("batch", &good, 10, Some(32)).unwrap();
    let mut actual: Vec<_> = hits.into_iter().map(|hit| hit.key).collect();
    actual.sort();
    assert_eq!(actual, keys);
}

const MIXED_INDEX: &str = "idx_items_embedding";

fn seed_borrowed_sql_hnsw(db: &Database) -> (Vec<u8>, Vec<u8>) {
    db.execute_sql(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2));
        INSERT INTO items VALUES (1, [0,0]), (2, [5,0]);
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
    )
    .unwrap();
    let (hits, _) = db
        .search_hnsw(MIXED_INDEX, &[0.0, 0.0], 2, Some(16))
        .unwrap();
    assert_eq!(hits.len(), 2);
    (hits[0].key.clone(), hits[0].metadata.clone())
}

fn query_rows(result: alopex_sql::ExecutionResult) -> Vec<Vec<alopex_sql::storage::SqlValue>> {
    match result {
        alopex_sql::ExecutionResult::Query(result) => result.rows,
        other => panic!("expected query result, got {other:?}"),
    }
}

fn assert_borrowed_sql_hnsw_state(db: &Database, expected: &[(i32, f32)]) {
    use alopex_sql::storage::SqlValue;
    // Observe the warmed cache before another SQL call can invalidate it.
    let (hits, _) = db
        .search_hnsw(MIXED_INDEX, &[0.0, 0.0], 10, Some(16))
        .unwrap();
    assert_eq!(
        hits.len(),
        expected.len(),
        "direct graph must retain every SQL row"
    );
    for &(_, x) in expected {
        let (hits, _) = db.search_hnsw(MIXED_INDEX, &[x, 0.0], 1, Some(16)).unwrap();
        assert_eq!(hits[0].distance, 0.0, "graph lacks vector {x}");
    }
    assert_eq!(
        query_rows(
            db.execute_sql("SELECT id, embedding FROM items ORDER BY id")
                .unwrap()
        ),
        expected
            .iter()
            .map(|&(id, x)| vec![SqlValue::Integer(id), SqlValue::Vector(vec![x, 0.0])])
            .collect::<Vec<_>>()
    );
    let sql = format!("SELECT id FROM items ORDER BY vector_distance(embedding, [0,0], 'l2') ASC LIMIT {} WITH (enable_hnsw = true)", expected.len());
    let plan = query_rows(db.execute_sql(&format!("EXPLAIN {sql}")).unwrap());
    assert!(matches!(&plan[0][0], SqlValue::Text(plan) if plan.contains("HnswSearch")));
    let mut sorted = expected.to_vec();
    sorted.sort_by(|left, right| left.1.total_cmp(&right.1));
    assert_eq!(
        query_rows(db.execute_sql(&sql).unwrap()),
        sorted
            .iter()
            .map(|&(id, _)| vec![SqlValue::Integer(id)])
            .collect::<Vec<_>>()
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_sql_insert_commit_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        let (key, metadata) = seed_borrowed_sql_hnsw(&db);
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw(MIXED_INDEX, &key, &[0.0, 0.0], &metadata)
            .unwrap();
        txn.execute_sql("INSERT INTO items VALUES (3, [1,0])")
            .unwrap();
        txn.commit().unwrap();
        assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0), (3, 1.0)]);
    }
    let db = Database::open(dir.path()).unwrap();
    assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0), (3, 1.0)]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_sql_update_commit_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        let (key, metadata) = seed_borrowed_sql_hnsw(&db);
        let (second, _) = db
            .search_hnsw(MIXED_INDEX, &[5.0, 0.0], 1, Some(16))
            .unwrap();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw(MIXED_INDEX, &key, &[0.0, 0.0], &metadata)
            .unwrap();
        txn.execute_sql("UPDATE items SET embedding = [9,0] WHERE id = 1")
            .unwrap();
        // A later direct operation must reload SQL's latest graph, not resurrect
        // the graph from before the UPDATE.
        txn.upsert_to_hnsw(
            MIXED_INDEX,
            &second[0].key,
            &[5.0, 0.0],
            &second[0].metadata,
        )
        .unwrap();
        txn.execute_sql("SELECT 1").unwrap();
        txn.commit().unwrap();
        assert_borrowed_sql_hnsw_state(&db, &[(1, 9.0), (2, 5.0)]);
        let (hits, _) = db
            .search_hnsw(MIXED_INDEX, &[9.0, 0.0], 1, Some(16))
            .unwrap();
        assert_eq!((&hits[0].key, &hits[0].metadata), (&key, &metadata));
    }
    let db = Database::open(dir.path()).unwrap();
    assert_borrowed_sql_hnsw_state(&db, &[(1, 9.0), (2, 5.0)]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_sql_read_commit_refreshes_cache() {
    use alopex_sql::storage::SqlValue;
    let dir = tempdir().unwrap();
    let key;
    {
        let db = Database::open(dir.path()).unwrap();
        let (row_key, metadata) = seed_borrowed_sql_hnsw(&db);
        key = row_key;
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw(MIXED_INDEX, &key, &[9.0, 0.0], &metadata)
            .unwrap();
        assert_eq!(
            query_rows(txn.execute_sql("SELECT 1").unwrap()),
            vec![vec![SqlValue::Integer(1)]]
        );
        txn.commit().unwrap();
        let (hits, _) = db
            .search_hnsw(MIXED_INDEX, &[9.0, 0.0], 1, Some(16))
            .unwrap();
        assert_eq!((&hits[0].key, hits[0].distance), (&key, 0.0));
        // Direct graph writes do not update the relational row.
        assert_eq!(
            query_rows(
                db.execute_sql("SELECT embedding FROM items WHERE id = 1")
                    .unwrap()
            ),
            vec![vec![SqlValue::Vector(vec![0.0, 0.0])]]
        );
    }
    let db = Database::open(dir.path()).unwrap();
    let (hits, _) = db
        .search_hnsw(MIXED_INDEX, &[9.0, 0.0], 1, Some(16))
        .unwrap();
    assert_eq!((&hits[0].key, hits[0].distance), (&key, 0.0));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_sql_reads_direct_changes() {
    use alopex_sql::storage::SqlValue;
    let dir = tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        let (key, _) = seed_borrowed_sql_hnsw(&db);
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        assert!(txn.delete_from_hnsw(MIXED_INDEX, &key).unwrap());
        let sql = "SELECT id FROM items ORDER BY vector_distance(embedding, [0,0], 'l2') ASC LIMIT 1 WITH (enable_hnsw = true)";
        let plan = query_rows(txn.execute_sql(&format!("EXPLAIN {sql}")).unwrap());
        assert!(matches!(&plan[0][0], SqlValue::Text(plan) if plan.contains("HnswSearch")));
        assert_eq!(
            query_rows(txn.execute_sql(sql).unwrap()),
            vec![vec![SqlValue::Integer(2)]]
        );
        assert_eq!(
            query_rows(txn.execute_sql("SELECT id FROM items ORDER BY id").unwrap()),
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
        );
        txn.rollback().unwrap();
        assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0)]);
    }
    let db = Database::open(dir.path()).unwrap();
    assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0)]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn borrowed_hnsw_sql_rollback_preserves_committed_state() {
    let dir = tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        let (key, metadata) = seed_borrowed_sql_hnsw(&db);
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw(MIXED_INDEX, &key, &[9.0, 0.0], &metadata)
            .unwrap();
        txn.execute_sql(
            "INSERT INTO items VALUES (3, [1,0]); UPDATE items SET embedding = [7,0] WHERE id = 2;",
        )
        .unwrap();
        txn.rollback().unwrap();
        assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0)]);
    }
    let db = Database::open(dir.path()).unwrap();
    assert_borrowed_sql_hnsw_state(&db, &[(1, 0.0), (2, 5.0)]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn owned_direct_hnsw_update_then_sql_read_invalidates_shared_cache() {
    use alopex_sql::storage::SqlValue;
    use alopex_sql::ExecutionResult;

    let db = Arc::new(Database::open_in_memory().unwrap());
    db.create_hnsw_index("vec_idx", config()).unwrap();
    let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
    seed.upsert_to_hnsw("vec_idx", b"key", &[0.0, 0.0], b"metadata")
        .unwrap();
    seed.commit().unwrap();
    let (before, _) = db.search_hnsw("vec_idx", &[0.0, 0.0], 1, Some(8)).unwrap();
    assert_eq!(before[0].distance, 0.0);

    let mut transaction = Arc::clone(&db)
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    transaction
        .upsert_to_hnsw("vec_idx", b"key", &[9.0, 0.0], b"metadata")
        .unwrap();
    let ExecutionResult::Query(result) = transaction.execute_sql("SELECT 1").unwrap() else {
        panic!("expected SQL query result");
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Integer(1)]]);
    transaction.commit().unwrap();

    let (after, _) = db.search_hnsw("vec_idx", &[9.0, 0.0], 1, Some(8)).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, b"key");
    assert_eq!(after[0].metadata, b"metadata");
    assert_eq!(after[0].distance, 0.0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn owned_sql_update_after_direct_hnsw_noop_uses_latest_vector() {
    use alopex_sql::storage::SqlValue;
    use alopex_sql::ExecutionResult;

    let db = Arc::new(Database::open_in_memory().unwrap());
    db.execute_sql(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2));
         INSERT INTO items VALUES (1, [0.0, 0.0]);
         CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
    )
    .unwrap();

    // Use the name declared in SQL and resolve the opaque key through the public
    // search API. The direct
    // upsert preserves the vector and metadata, so only SQL changes the value.
    let index_name = "idx_items_embedding";
    let (before, _) = db.search_hnsw(index_name, &[0.0, 0.0], 1, Some(8)).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].distance, 0.0);
    let key = before[0].key.clone();
    let metadata = before[0].metadata.clone();

    let mut transaction = Arc::clone(&db)
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    transaction
        .upsert_to_hnsw(index_name, &key, &[0.0, 0.0], &metadata)
        .unwrap();
    transaction
        .execute_sql("UPDATE items SET embedding = [9.0, 0.0] WHERE id = 1")
        .unwrap();
    transaction.commit().unwrap();

    let ExecutionResult::Query(result) = db
        .execute_sql("SELECT embedding FROM items WHERE id = 1")
        .unwrap()
    else {
        panic!("expected SQL query result");
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Vector(vec![9.0, 0.0])]]);
    let (after, _) = db.search_hnsw(index_name, &[9.0, 0.0], 1, Some(8)).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].key, key);
    assert_eq!(after[0].metadata, metadata);
    assert_eq!(
        after[0].distance, 0.0,
        "HNSW must reflect the later SQL update"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

    let (results, search_stats) = db.search_hnsw("vec_idx", &[0.1, 0.0], 1, None).unwrap();
    assert_eq!(results[0].key, b"a");
    assert!(search_stats.nodes_visited > 0);
    assert!(search_stats.distance_computations > 0);

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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_index_persists_across_reopen() {
    let dir = tempdir().expect("tempdir");
    {
        let db = Database::open(dir.path()).expect("open db");
        db.create_hnsw_index("vec_idx", config()).unwrap();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_to_hnsw("vec_idx", b"a", &[0.0, 0.0], b"ma")
            .unwrap();
        txn.upsert_to_hnsw("vec_idx", b"b", &[1.0, 0.0], b"mb")
            .unwrap();
        txn.commit().unwrap();
    }

    let db = Database::open(dir.path()).expect("reopen db");
    let (results, _) = db.search_hnsw("vec_idx", &[0.0, 0.0], 2, None).unwrap();
    assert_eq!(results.len(), 2);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_upsert_reconnects_existing_key_without_duplicate_results() {
    let db = Database::new();
    db.create_hnsw_index("vec_idx", config()).unwrap();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_to_hnsw("vec_idx", b"a", &[0.0, 0.0], b"old")
        .unwrap();
    txn.upsert_to_hnsw("vec_idx", b"b", &[100.0, 0.0], b"b")
        .unwrap();
    for index in 0_u32..128 {
        let key = format!("node-{index}");
        txn.upsert_to_hnsw("vec_idx", key.as_bytes(), &[index as f32, 1.0], b"fixture")
            .unwrap();
    }
    txn.commit().unwrap();

    let mut update = db.begin(TxnMode::ReadWrite).unwrap();
    update
        .upsert_to_hnsw("vec_idx", b"b", &[0.1, 0.0], b"new")
        .unwrap();
    update.commit().unwrap();

    let mut delete = db.begin(TxnMode::ReadWrite).unwrap();
    delete.delete_from_hnsw("vec_idx", b"a").unwrap();
    delete.commit().unwrap();

    let (results, _) = db.search_hnsw("vec_idx", &[0.1, 0.0], 2, Some(8)).unwrap();
    assert_eq!(
        results.iter().filter(|result| result.key == b"b").count(),
        1
    );
    assert_eq!(results[0].key, b"b");
    assert!(results.iter().all(|result| result.key != b"a"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_existing_upsert_rollback_restores_previous_position() {
    let db = Database::new();
    db.create_hnsw_index("vec_idx", config()).unwrap();
    let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
    seed.upsert_to_hnsw("vec_idx", b"anchor", &[0.0, 0.0], b"")
        .unwrap();
    seed.upsert_to_hnsw("vec_idx", b"moving", &[100.0, 0.0], b"old")
        .unwrap();
    seed.commit().unwrap();

    let mut update = db.begin(TxnMode::ReadWrite).unwrap();
    update
        .upsert_to_hnsw("vec_idx", b"moving", &[0.1, 0.0], b"new")
        .unwrap();
    update.rollback().unwrap();

    let (results, _) = db
        .search_hnsw("vec_idx", &[100.0, 0.0], 1, Some(8))
        .unwrap();
    assert_eq!(results[0].key, b"moving");
    assert_eq!(results[0].metadata, b"old");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_deleted_node_reactivation_reconnects_at_new_position() {
    let db = Database::new();
    db.create_hnsw_index("vec_idx", config()).unwrap();
    let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
    seed.upsert_to_hnsw("vec_idx", b"anchor", &[0.0, 0.0], b"")
        .unwrap();
    seed.upsert_to_hnsw("vec_idx", b"moving", &[100.0, 0.0], b"old")
        .unwrap();
    seed.upsert_to_hnsw("vec_idx", b"old-neighbor", &[100.0, 1.0], b"")
        .unwrap();
    seed.commit().unwrap();

    let mut delete = db.begin(TxnMode::ReadWrite).unwrap();
    delete.delete_from_hnsw("vec_idx", b"moving").unwrap();
    delete.commit().unwrap();
    let mut reactivate = db.begin(TxnMode::ReadWrite).unwrap();
    reactivate
        .upsert_to_hnsw("vec_idx", b"moving", &[0.1, 0.0], b"new")
        .unwrap();
    reactivate.commit().unwrap();

    let (new_position, _) = db.search_hnsw("vec_idx", &[0.1, 0.0], 1, Some(8)).unwrap();
    let (old_position, _) = db
        .search_hnsw("vec_idx", &[100.0, 0.0], 1, Some(8))
        .unwrap();
    assert_eq!(new_position[0].key, b"moving");
    assert_eq!(new_position[0].metadata, b"new");
    assert_eq!(old_position[0].key, b"old-neighbor");
    assert_eq!(db.get_hnsw_stats("vec_idx").unwrap().deleted_count, 0);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
