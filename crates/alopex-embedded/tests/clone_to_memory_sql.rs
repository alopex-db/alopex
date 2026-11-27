//! Regression tests ensuring `Database::clone_to_memory()` reloads the SQL catalog
//! (tables, rows, and HNSW indexes) and preserves isolation from the source database.

use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};

const HNSW_QUERY: &str =
    "SELECT id FROM items ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)";

fn populate_sql_and_hnsw(db: &Database) {
    for sql in [
        "CREATE TABLE items(id INTEGER PRIMARY KEY, e VECTOR(2,L2))",
        "INSERT INTO items VALUES (1, [1, 0]), (2, [5, 0])",
        "CREATE INDEX ih ON items(e) USING HNSW",
    ] {
        db.execute_sql(sql).expect("setup SQL must succeed");
    }
}

fn query_ids(db: &Database, sql: &str) -> Vec<i32> {
    let ExecutionResult::Query(result) = db.execute_sql(sql).expect("query must succeed") else {
        panic!("expected query execution result for: {sql}");
    };
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [SqlValue::Integer(id)] => *id,
            other => panic!("unexpected row shape: {other:?}"),
        })
        .collect()
}

fn assert_cloned_sql_hnsw_and_isolation(source: &Database) {
    let cloned = source
        .clone_to_memory()
        .expect("clone_to_memory must reload SQL catalog");

    assert_eq!(
        query_ids(&cloned, "SELECT id FROM items ORDER BY id"),
        vec![1, 2]
    );
    assert_eq!(query_ids(&cloned, HNSW_QUERY), vec![1]);

    let warmed_bytes = cloned
        .warm_hnsw_index("ih")
        .expect("cloned HNSW index must be loadable");
    assert!(warmed_bytes > 0);
    assert_eq!(cloned.hnsw_cache_stats().entries, 1);

    // Mutate clone and verify source is unaffected.
    cloned
        .execute_sql("INSERT INTO items VALUES (3, [0.1, 0])")
        .expect("insert into cloned SQL table must succeed");
    assert_eq!(
        query_ids(&cloned, "SELECT id FROM items ORDER BY id"),
        vec![1, 2, 3]
    );
    assert_eq!(query_ids(&cloned, HNSW_QUERY), vec![3]);
    assert_eq!(
        query_ids(source, "SELECT id FROM items ORDER BY id"),
        vec![1, 2]
    );
    assert_eq!(query_ids(source, HNSW_QUERY), vec![1]);

    // Mutate source and verify clone is unaffected.
    source
        .execute_sql("INSERT INTO items VALUES (4, [0.05, 0])")
        .expect("insert into source SQL table must succeed");
    assert_eq!(
        query_ids(source, "SELECT id FROM items ORDER BY id"),
        vec![1, 2, 4]
    );
    assert_eq!(query_ids(source, HNSW_QUERY), vec![4]);
    assert_eq!(
        query_ids(&cloned, "SELECT id FROM items ORDER BY id"),
        vec![1, 2, 3]
    );
    assert_eq!(query_ids(&cloned, HNSW_QUERY), vec![3]);
}

#[test]
fn clone_to_memory_reloads_sql_catalog_and_hnsw_from_disk_db() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = Database::open(dir.path()).expect("open disk db");
    populate_sql_and_hnsw(&source);
    source.flush().expect("flush disk db");

    assert_cloned_sql_hnsw_and_isolation(&source);
}

#[test]
fn clone_to_memory_reloads_sql_catalog_and_hnsw_from_in_memory_db() {
    let source = Database::open_in_memory().expect("open in-memory db");
    populate_sql_and_hnsw(&source);

    assert_cloned_sql_hnsw_and_isolation(&source);
}

#[test]
fn clone_to_memory_handles_empty_and_kv_only_databases() {
    let empty = Database::open_in_memory().expect("empty in-memory db");
    let cloned_empty = empty.clone_to_memory().expect("clone empty db");
    cloned_empty
        .execute_sql("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .expect("DDL on cloned empty db");
    cloned_empty
        .execute_sql("INSERT INTO t VALUES (42)")
        .expect("DML on cloned empty db");
    assert_eq!(query_ids(&cloned_empty, "SELECT id FROM t"), vec![42]);

    let kv_only = Database::open_in_memory().expect("kv-only db");
    {
        let mut txn = kv_only.begin(TxnMode::ReadWrite).expect("rw txn");
        txn.put(b"plain:key", b"plain:value").expect("put");
        txn.commit().expect("commit");
    }
    let cloned_kv = kv_only.clone_to_memory().expect("clone kv-only db");
    {
        let mut txn = cloned_kv.begin(TxnMode::ReadOnly).expect("ro txn");
        assert_eq!(
            txn.get(b"plain:key").expect("get"),
            Some(b"plain:value".to_vec())
        );
    }
}
