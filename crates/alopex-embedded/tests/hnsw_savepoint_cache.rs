//! Savepoint cache contracts use two rows and two dimensions per index.
use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};
use std::{path::Path, sync::Arc};

const QUERY: &str =
    "SELECT id FROM items ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)";

fn database(path: &Path, persistent: bool, cold: bool) -> Arc<Database> {
    let db = Arc::new(if persistent {
        Database::open(path).unwrap()
    } else {
        Database::open_in_memory().unwrap()
    });
    for sql in [
        "CREATE TABLE items(id INTEGER PRIMARY KEY,e VECTOR(2,L2))",
        "INSERT INTO items VALUES (1,[1,0]),(2,[5,0])",
        "CREATE INDEX ih ON items(e) USING HNSW",
    ] {
        db.execute_sql(sql).unwrap();
    }
    assert!(db.warm_hnsw_index("ih").unwrap() > 0);
    if cold {
        db.set_hnsw_cache_limit(0);
        db.set_hnsw_cache_limit(1_000_000);
    }
    db
}

fn nearest(result: ExecutionResult, expected: i32) {
    let ExecutionResult::Query(result) = result else {
        panic!("query expected")
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::Integer(expected));
}

fn reuse(persistent: bool, cold: bool) {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), persistent, cold);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.create_savepoint("s").unwrap();
    tx.execute_sql("UPDATE items SET e=[9,0] WHERE id=1")
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 2);
    tx.rollback_to_savepoint("s").unwrap();
    let misses = db.hnsw_cache_stats().misses;
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    assert_eq!(db.hnsw_cache_stats().misses, misses);
    tx.rollback().unwrap();
    // A restored unmodified cold load remains publishable; shared warm entries
    // remain globally retained. Neither may be confused with a mutated graph.
    assert_eq!(db.hnsw_cache_stats().entries, 1);
    nearest(db.execute_sql(QUERY).unwrap(), 1);
}

#[test]
fn memory_warm_reuse() {
    reuse(false, false);
}
#[test]
fn memory_cold_reuse() {
    reuse(false, true);
}
#[test]
fn lsm_warm_reuse() {
    reuse(true, false);
}
#[test]
fn lsm_cold_reuse() {
    reuse(true, true);
}

fn nested(persistent: bool) {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), persistent, true);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.create_savepoint("outer").unwrap();
    tx.execute_sql("UPDATE items SET e=[9,0] WHERE id=1")
        .unwrap();
    tx.create_savepoint("inner").unwrap();
    for _ in 0..2 {
        tx.execute_sql("UPDATE items SET e=[11,0] WHERE id=2")
            .unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        tx.rollback_to_savepoint("inner").unwrap();
        let misses = db.hnsw_cache_stats().misses;
        nearest(tx.execute_sql(QUERY).unwrap(), 2);
        assert_eq!(db.hnsw_cache_stats().misses, misses);
    }
    tx.rollback_to_savepoint("outer").unwrap();
    assert!(tx.rollback_to_savepoint("inner").is_err());
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.commit().unwrap();
    nearest(db.execute_sql(QUERY).unwrap(), 1);
}
#[test]
fn memory_nested_repeated_restore() {
    nested(false);
}
#[test]
fn lsm_nested_repeated_restore() {
    nested(true);
}

fn restore_after(sql: &str, fails: bool) {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), false, true);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.create_savepoint("s").unwrap();
    assert_eq!(tx.execute_sql(sql).is_err(), fails);
    tx.rollback_to_savepoint("s").unwrap();
    let misses = db.hnsw_cache_stats().misses;
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    assert_eq!(db.hnsw_cache_stats().misses, misses);
    tx.commit().unwrap();
    nearest(db.execute_sql(QUERY).unwrap(), 1);
}
#[test]
fn restore_after_drop_index() {
    restore_after("DROP INDEX ih", false);
}
#[test]
fn restore_after_truncate() {
    restore_after("TRUNCATE TABLE items", false);
}
#[test]
fn restore_after_null_update() {
    restore_after("UPDATE items SET e=NULL WHERE id=1", false);
}
#[test]
fn restore_after_sql_failure() {
    restore_after("SELECT * FROM missing_table", true);
}

#[test]
fn mutated_before_savepoint_is_not_published_after_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), false, true);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    tx.execute_sql("UPDATE items SET e=[9,0] WHERE id=1")
        .unwrap();
    tx.create_savepoint("s").unwrap();
    tx.execute_sql("UPDATE items SET e=[11,0] WHERE id=2")
        .unwrap();
    tx.rollback_to_savepoint("s").unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 2);
    tx.rollback().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    nearest(db.execute_sql(QUERY).unwrap(), 1);
}

#[test]
fn restore_obeys_lowered_cache_limit() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), false, true);
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.create_savepoint("s").unwrap();
    db.set_hnsw_cache_limit(0);
    tx.rollback_to_savepoint("s").unwrap();
    let misses = db.hnsw_cache_stats().misses;
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    assert_eq!(db.hnsw_cache_stats().misses, misses + 1);
    tx.release_savepoint("s").unwrap();
    assert!(tx.rollback_to_savepoint("s").is_err());
    tx.rollback().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
}

#[test]
fn graphs_loaded_after_savepoint_are_not_restored() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(directory.path(), false, true);
    for sql in [
        "CREATE TABLE other(id INTEGER PRIMARY KEY,e VECTOR(2,L2))",
        "INSERT INTO other VALUES (3,[2,0])",
        "CREATE INDEX oh ON other(e) USING HNSW",
    ] {
        db.execute_sql(sql).unwrap();
    }
    db.set_hnsw_cache_limit(0);
    db.set_hnsw_cache_limit(1_000_000);
    let other_query = QUERY.replace("items", "other");
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.create_savepoint("s").unwrap();
    nearest(tx.execute_sql(&other_query).unwrap(), 3);
    tx.rollback_to_savepoint("s").unwrap();
    let misses = db.hnsw_cache_stats().misses;
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    assert_eq!(db.hnsw_cache_stats().misses, misses);
    nearest(tx.execute_sql(&other_query).unwrap(), 3);
    assert_eq!(db.hnsw_cache_stats().misses, misses + 1);
    tx.rollback().unwrap();
}
