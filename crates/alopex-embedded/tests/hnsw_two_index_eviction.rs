//! Byte-budget eviction through public SQL transaction APIs, with four rows total.
use alopex_core::TxnMode;
use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};
use std::sync::Arc;

const QUERIES: [&str; 2] = [
    "SELECT id FROM a ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)",
    "SELECT id FROM b ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)",
];

fn nearest(result: ExecutionResult, index: usize) {
    let ExecutionResult::Query(result) = result else {
        panic!("query expected")
    };
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(if index == 0 { 1 } else { 3 })]]
    );
}

fn database() -> (Arc<Database>, [String; 2], u64) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    for sql in [
        "CREATE TABLE a(id INTEGER PRIMARY KEY,e VECTOR(2,L2))",
        "CREATE TABLE b(id INTEGER PRIMARY KEY,e VECTOR(2,L2))",
        "INSERT INTO a VALUES (1,[1,0]),(2,[5,0])",
        "INSERT INTO b VALUES (3,[2,0]),(4,[6,0])",
        "CREATE INDEX ia ON a(e) USING HNSW",
        "CREATE INDEX ib ON b(e) USING HNSW",
    ] {
        db.execute_sql(sql).unwrap();
    }
    let mut names = [String::new(), String::new()];
    let mut bytes = [0; 2];
    for index in 0..2 {
        db.set_hnsw_cache_limit(0);
        db.set_hnsw_cache_limits(1_000_000, Some(2)).unwrap();
        nearest(db.execute_sql(QUERIES[index]).unwrap(), index);
        let stats = db.hnsw_cache_stats();
        assert_eq!(stats.entries, 1);
        names[index] = stats.indexes[0].name.clone();
        bytes[index] = stats.used_bytes;
        assert!(bytes[index] > 0);
    }
    assert_ne!(names[0], names[1]);
    // Either graph fits; both together do not. The entry limit permits both,
    // so only the byte budget can cause the eviction under test.
    let limit = bytes[0].max(bytes[1]);
    assert!(bytes[0] + bytes[1] > limit);
    db.set_hnsw_cache_limit(0);
    db.set_hnsw_cache_limits(limit, Some(2)).unwrap();
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    (db, names, limit)
}

fn read_twice(db: &Database, index: usize, mut query: impl FnMut(&str) -> ExecutionResult) {
    let before = db.hnsw_cache_stats();
    nearest(query(QUERIES[index]), index);
    let loaded = db.hnsw_cache_stats();
    assert_eq!(loaded.misses, before.misses + 1);
    nearest(query(QUERIES[index]), index);
    let reused = db.hnsw_cache_stats();
    assert_eq!(reused.misses, loaded.misses);
    assert!(reused.hits > loaded.hits);
    assert_eq!(reused.oversize_skips, before.oversize_skips);
    assert_eq!(reused.invalidations, before.invalidations);
}

fn across_transactions(owned: bool, commit: bool) {
    let (db, names, limit) = database();
    let initial = db.hnsw_cache_stats();
    for (round, index) in [0, 1, 0, 1].into_iter().enumerate() {
        if owned {
            let mut tx = db
                .clone()
                .begin_owned_embedded_transaction(TxnMode::ReadOnly)
                .unwrap();
            read_twice(&db, index, |sql| tx.execute_sql(sql).unwrap());
            if commit {
                tx.commit().unwrap();
            } else {
                tx.rollback().unwrap();
            }
        } else {
            let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
            read_twice(&db, index, |sql| tx.execute_sql(sql).unwrap());
            if commit {
                tx.commit().unwrap();
            } else {
                tx.rollback().unwrap();
            }
        }
        let after = db.hnsw_cache_stats();
        assert_eq!(after.entries, 1);
        assert_eq!(after.indexes[0].name, names[index]);
        assert_eq!(after.used_bytes, after.indexes[0].bytes);
        assert!(after.used_bytes <= limit);
        assert_eq!(after.evictions, initial.evictions + round as u64);
        assert_eq!(after.invalidations, initial.invalidations);
        assert_eq!(after.oversize_skips, initial.oversize_skips);
    }
}

fn within_transaction(owned: bool) {
    let (db, names, limit) = database();
    let initial = db.hnsw_cache_stats();
    let read_all = |query: &mut dyn FnMut(&str) -> ExecutionResult| {
        for index in [0, 1, 0, 1] {
            read_twice(&db, index, &mut *query);
            assert_eq!(db.hnsw_cache_stats().entries, 0);
        }
    };
    if owned {
        let mut tx = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)
            .unwrap();
        read_all(&mut |sql| tx.execute_sql(sql).unwrap());
        tx.rollback().unwrap();
    } else {
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        read_all(&mut |sql| tx.execute_sql(sql).unwrap());
        tx.rollback().unwrap();
    }
    let after = db.hnsw_cache_stats();
    assert_eq!(after.misses, initial.misses + 4);
    assert_eq!(after.entries, 1);
    assert_eq!(after.indexes[0].name, names[1]);
    assert!(after.used_bytes <= limit);
    assert_eq!(after.invalidations, initial.invalidations);
    assert_eq!(after.oversize_skips, initial.oversize_skips);
    // The remaining graph is published and reusable by a new transaction.
    if owned {
        let mut tx = db
            .clone()
            .begin_owned_embedded_transaction(TxnMode::ReadOnly)
            .unwrap();
        nearest(tx.execute_sql(QUERIES[1]).unwrap(), 1);
        tx.rollback().unwrap();
    } else {
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        nearest(tx.execute_sql(QUERIES[1]).unwrap(), 1);
        tx.rollback().unwrap();
    }
    assert_eq!(db.hnsw_cache_stats().misses, after.misses);
}

#[test]
fn borrowed_commit_alternates_two_indexes_within_byte_budget() {
    across_transactions(false, true);
}
#[test]
fn borrowed_rollback_alternates_two_indexes_within_byte_budget() {
    across_transactions(false, false);
}
#[test]
fn owned_commit_alternates_two_indexes_within_byte_budget() {
    across_transactions(true, true);
}
#[test]
fn owned_rollback_alternates_two_indexes_within_byte_budget() {
    across_transactions(true, false);
}
#[test]
fn borrowed_transaction_evicts_and_reloads_local_graphs() {
    within_transaction(false);
}
#[test]
fn owned_transaction_evicts_and_reloads_local_graphs() {
    within_transaction(true);
}
