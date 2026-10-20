use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};

const QUERY: &str = "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 3 WITH (enable_hnsw = true)";

fn seed(db: &Database) {
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, e VECTOR(2,L2))")
        .unwrap();
    db.execute_sql("INSERT INTO items VALUES (1,[1,0]),(2,[5,0])")
        .unwrap();
    db.execute_sql("CREATE INDEX ih ON items (e) USING HNSW")
        .unwrap();
    assert!(db.warm_hnsw_index("ih").unwrap() > 0);
}

fn ids(result: ExecutionResult) -> Vec<i32> {
    let ExecutionResult::Query(result) = result else {
        panic!("expected rows");
    };
    result
        .rows
        .into_iter()
        .map(|row| match row[0] {
            SqlValue::Integer(id) => id,
            _ => panic!("expected integer"),
        })
        .collect()
}

fn assert_index(db: &Database, expected: &[i32], nodes: u64, deleted: u64) {
    assert_eq!(ids(db.execute_sql(QUERY).unwrap()), expected);
    let stats = db.get_hnsw_stats("ih").unwrap();
    assert_eq!(stats.node_count, nodes);
    assert_eq!(stats.deleted_count, deleted);
}

#[test]
fn truncate_insert_commit_replaces_cached_graph_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        seed(&db);
        let old_bytes = db.get_hnsw_stats("ih").unwrap().memory_bytes;
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        assert_eq!(ids(tx.execute_sql(QUERY).unwrap()), [1, 2]);
        tx.execute_sql("TRUNCATE items").unwrap();
        assert!(ids(tx.execute_sql(QUERY).unwrap()).is_empty());
        tx.execute_sql("INSERT INTO items VALUES (3,[0,0])")
            .unwrap();
        assert_eq!(ids(tx.execute_sql(QUERY).unwrap()), [3]);
        tx.commit().unwrap();
        assert_index(&db, &[3], 1, 0);
        assert!(db.get_hnsw_stats("ih").unwrap().memory_bytes < old_bytes);
    }
    let db = Database::open(dir.path()).unwrap();
    assert_index(&db, &[3], 1, 0);
    db.execute_sql("INSERT INTO items VALUES (4,[2,0])")
        .unwrap();
    assert_index(&db, &[3, 4], 2, 0);
}

#[test]
fn truncate_rollback_preserves_original_graph_and_old_reader() {
    let db = Database::new();
    seed(&db);
    let old_bytes = db.get_hnsw_stats("ih").unwrap().memory_bytes;
    let mut old = db.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), [1, 2]);
    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    tx.execute_sql("TRUNCATE items").unwrap();
    tx.execute_sql("INSERT INTO items VALUES (3,[0,0])")
        .unwrap();
    assert_eq!(ids(tx.execute_sql(QUERY).unwrap()), [3]);
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), [1, 2]);
    tx.rollback().unwrap();
    old.rollback().unwrap();
    assert_index(&db, &[1, 2], 2, 0);
    assert_eq!(db.get_hnsw_stats("ih").unwrap().memory_bytes, old_bytes);
}

#[test]
fn truncate_with_delete_and_update_does_not_restore_old_nodes() {
    let db = Database::new();
    seed(&db);
    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    tx.execute_sql("UPDATE items SET e = [9,0] WHERE id = 1")
        .unwrap();
    tx.execute_sql("DELETE FROM items WHERE id = 2").unwrap();
    tx.execute_sql("TRUNCATE items").unwrap();
    tx.execute_sql("INSERT INTO items VALUES (3,[3,0]),(4,[4,0])")
        .unwrap();
    tx.execute_sql("UPDATE items SET e = [0,0] WHERE id = 4")
        .unwrap();
    tx.execute_sql("DELETE FROM items WHERE id = 3").unwrap();
    assert_eq!(ids(tx.execute_sql(QUERY).unwrap()), [4]);
    tx.commit().unwrap();
    // Only the new live node and its deleted peer remain after TRUNCATE.
    assert_index(&db, &[4], 1, 1);
}

#[test]
fn truncate_commit_without_insert_leaves_empty_index() {
    let db = Database::new();
    seed(&db);
    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    tx.execute_sql("TRUNCATE items").unwrap();
    tx.commit().unwrap();
    assert_index(&db, &[], 0, 0);
}

#[test]
fn truncate_savepoint_rollback_restores_original_graph() {
    let db = std::sync::Arc::new(Database::new());
    seed(&db);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    tx.create_savepoint("before").unwrap();
    tx.execute_sql("TRUNCATE items").unwrap();
    tx.execute_sql("INSERT INTO items VALUES (3,[0,0])")
        .unwrap();
    tx.rollback_to_savepoint("before").unwrap();
    assert_eq!(ids(tx.execute_sql(QUERY).unwrap()), [1, 2]);
    tx.commit().unwrap();
    assert_index(&db, &[1, 2], 2, 0);
}
