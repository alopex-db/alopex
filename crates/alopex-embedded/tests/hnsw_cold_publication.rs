//! Two-row cache lifecycle checks; scale measurements live in the manual runner.
use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};
use std::sync::Arc;

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
    db.set_hnsw_cache_limit(0);
    db.set_hnsw_cache_limit(1_000_000);
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    db
}

fn nearest(result: ExecutionResult, expected: i32) {
    let ExecutionResult::Query(result) = result else {
        panic!("query expected")
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::Integer(expected));
}

fn reuse(owned: bool, mode: TxnMode, commit: bool) {
    let db = database();
    if owned {
        let mut tx = db.clone().begin_owned_embedded_transaction(mode).unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        assert_eq!(db.hnsw_cache_stats().entries, 0);
        if commit {
            tx.commit().unwrap()
        } else {
            tx.rollback().unwrap()
        }
        let before = db.hnsw_cache_stats();
        assert!(tx.rollback().is_err());
        assert_eq!(db.hnsw_cache_stats().entries, before.entries);
    } else {
        let mut tx = db.begin(mode).unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        assert_eq!(db.hnsw_cache_stats().entries, 0);
        if commit {
            tx.commit().unwrap()
        } else {
            tx.rollback().unwrap()
        }
    }
    assert_eq!(db.hnsw_cache_stats().entries, 1);
    let misses = db.hnsw_cache_stats().misses;
    // Use the same explicit API again: the second transaction must hit the cache.
    if owned {
        let mut tx = db.clone().begin_owned_embedded_transaction(mode).unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        tx.rollback().unwrap();
    } else {
        let mut tx = db.begin(mode).unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        tx.rollback().unwrap();
    }
    assert_eq!(db.hnsw_cache_stats().misses, misses);
}

macro_rules! reuse_case {
    ($name:ident, $owned:expr, $mode:ident, $commit:expr) => {
        #[test]
        fn $name() {
            reuse($owned, TxnMode::$mode, $commit);
        }
    };
}
reuse_case!(borrowed_read_only_commit_reuses, false, ReadOnly, true);
reuse_case!(borrowed_read_only_rollback_reuses, false, ReadOnly, false);
reuse_case!(borrowed_read_write_commit_reuses, false, ReadWrite, true);
reuse_case!(borrowed_read_write_rollback_reuses, false, ReadWrite, false);
reuse_case!(owned_read_only_commit_reuses, true, ReadOnly, true);
reuse_case!(owned_read_only_rollback_reuses, true, ReadOnly, false);
reuse_case!(owned_read_write_commit_reuses, true, ReadWrite, true);
reuse_case!(owned_read_write_rollback_reuses, true, ReadWrite, false);

fn stale(owned: bool, commit: bool) {
    let db = database();
    if owned {
        let mut old = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)
            .unwrap();
        nearest(old.execute_sql(QUERY).unwrap(), 1);
        db.execute_sql("UPDATE items SET e=[10,0] WHERE id=1")
            .unwrap();
        nearest(old.execute_sql(QUERY).unwrap(), 1);
        if commit {
            let _ = old.commit();
        } else {
            old.rollback().unwrap();
        }
    } else {
        let mut old = db.begin(TxnMode::ReadOnly).unwrap();
        nearest(old.execute_sql(QUERY).unwrap(), 1);
        db.execute_sql("UPDATE items SET e=[10,0] WHERE id=1")
            .unwrap();
        nearest(old.execute_sql(QUERY).unwrap(), 1);
        if commit {
            let _ = old.commit();
        } else {
            old.rollback().unwrap();
        }
    }
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    nearest(db.execute_sql(QUERY).unwrap(), 2);
}
#[test]
fn borrowed_stale_commit_does_not_publish() {
    stale(false, true);
}
#[test]
fn borrowed_stale_rollback_does_not_publish() {
    stale(false, false);
}
#[test]
fn owned_stale_commit_does_not_publish() {
    stale(true, true);
}
#[test]
fn owned_stale_rollback_does_not_publish() {
    stale(true, false);
}

fn write(owned: bool, commit: bool) {
    let db = database();
    if owned {
        let mut tx = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadWrite)
            .unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        tx.execute_sql("UPDATE items SET e=[10,0] WHERE id=1")
            .unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 2);
        if commit {
            tx.commit().unwrap()
        } else {
            tx.rollback().unwrap()
        }
    } else {
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 1);
        tx.execute_sql("UPDATE items SET e=[10,0] WHERE id=1")
            .unwrap();
        nearest(tx.execute_sql(QUERY).unwrap(), 2);
        if commit {
            tx.commit().unwrap()
        } else {
            tx.rollback().unwrap()
        }
    }
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    nearest(db.execute_sql(QUERY).unwrap(), if commit { 2 } else { 1 });
}
#[test]
fn borrowed_write_commit_does_not_publish() {
    write(false, true);
}
#[test]
fn borrowed_write_rollback_does_not_publish() {
    write(false, false);
}
#[test]
fn owned_write_commit_does_not_publish() {
    write(true, true);
}
#[test]
fn owned_write_rollback_does_not_publish() {
    write(true, false);
}

#[test]
fn borrowed_rollback_in_place_publishes_once() {
    let db = database();
    let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.rollback_in_place().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 1);
    assert!(tx.rollback_in_place().is_err());
}

#[test]
fn disabled_cache_rejects_terminal_publication() {
    let db = database();
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadOnly)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    db.set_hnsw_cache_limit(0);
    tx.commit().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
}

#[test]
fn oversized_graph_rejects_terminal_publication() {
    let db = database();
    let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    db.set_hnsw_cache_limit(1);
    tx.rollback().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
}

#[test]
fn unrelated_borrowed_write_conservatively_prevents_publication() {
    let db = database();
    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.put(b"unrelated", b"value").unwrap();
    tx.rollback().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
}

#[test]
fn unrelated_owned_write_conservatively_prevents_publication() {
    let db = database();
    let mut tx = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    nearest(tx.execute_sql(QUERY).unwrap(), 1);
    tx.put(b"unrelated", b"value").unwrap();
    tx.commit().unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
}
