//! Transaction cache correctness with two rows, separate from manual scale work.
use std::sync::Arc;

use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};

const QUERY: &str =
    "SELECT id FROM items ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)";

fn database() -> Arc<Database> {
    let db = Arc::new(Database::open_in_memory().unwrap());
    for sql in [
        "CREATE TABLE items(id INTEGER PRIMARY KEY,e VECTOR(2,L2))",
        "INSERT INTO items VALUES (1,[1,0]),(2,[5,0])",
        "CREATE INDEX ih ON items(e) USING HNSW",
    ] {
        db.execute_sql(sql).unwrap();
    }
    assert!(db.warm_hnsw_index("ih").unwrap() > 0);
    db
}

fn ids(result: ExecutionResult) -> Vec<i32> {
    let ExecutionResult::Query(result) = result else {
        panic!("query expected")
    };
    result
        .rows
        .into_iter()
        .map(|row| match row[0] {
            SqlValue::Integer(id) => id,
            _ => panic!("id expected"),
        })
        .collect()
}

#[test]
fn transaction_delete_rollback_preserves_old_and_new_readers() {
    let db = database();
    let mut old = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut writer = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    writer.execute_sql("DELETE FROM items WHERE id=1").unwrap();
    assert_eq!(ids(writer.execute_sql(QUERY).unwrap()), vec![2]);
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), vec![1]);
    writer.rollback().unwrap();
    assert_eq!(ids(db.execute_sql(QUERY).unwrap()), vec![1]);
    old.rollback().unwrap();
}

#[test]
fn transaction_null_update_commit_preserves_old_snapshot() {
    let db = database();
    let mut old = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut writer = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    writer
        .execute_sql("UPDATE items SET e=NULL WHERE id=1")
        .unwrap();
    assert_eq!(ids(writer.execute_sql(QUERY).unwrap()), vec![2]);
    writer.commit().unwrap();
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), vec![1]);
    assert_eq!(ids(db.execute_sql(QUERY).unwrap()), vec![2]);
    old.rollback().unwrap();
}

fn truncate(commit: bool) {
    let db = database();
    let mut old = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadOnly)
        .unwrap();
    let mut writer = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    writer.execute_sql("TRUNCATE TABLE items").unwrap();
    assert!(ids(writer.execute_sql(QUERY).unwrap()).is_empty());
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), vec![1]);
    if commit {
        writer.commit().unwrap()
    } else {
        writer.rollback().unwrap()
    }
    assert_eq!(ids(old.execute_sql(QUERY).unwrap()), vec![1]);
    assert_eq!(
        ids(db.execute_sql(QUERY).unwrap()),
        if commit { vec![] } else { vec![1] }
    );
    old.rollback().unwrap();
}

#[test]
fn transaction_truncate_rollback_preserves_cached_graph() {
    truncate(false)
}

#[test]
fn transaction_truncate_commit_replaces_cached_graph() {
    truncate(true)
}

#[test]
fn conflicting_sql_writer_cannot_publish_its_graph() {
    let db = database();
    let mut first = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    let mut second = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    first
        .execute_sql("UPDATE items SET e=[10,0] WHERE id=1")
        .unwrap();
    second
        .execute_sql("UPDATE items SET e=[0,0] WHERE id=1")
        .unwrap();
    assert_eq!(ids(first.execute_sql(QUERY).unwrap()), vec![2]);
    assert_eq!(ids(second.execute_sql(QUERY).unwrap()), vec![1]);
    first.commit().unwrap();
    let before = db.hnsw_cache_stats();
    assert!(matches!(
        second.commit(),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::TxnConflict
        ))
    ));
    assert_eq!(db.hnsw_cache_stats().invalidations, before.invalidations);
    assert_eq!(ids(db.execute_sql(QUERY).unwrap()), vec![2]);
}
