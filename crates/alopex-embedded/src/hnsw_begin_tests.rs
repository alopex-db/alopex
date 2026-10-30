//! Two-row snapshot contracts for contended transaction startup.
use super::*;
use alopex_sql::{ExecutionResult, SqlValue};
use std::sync::mpsc;
use std::time::Duration;

const QUERY: &str =
    "SELECT id FROM items ORDER BY vector_distance(e,[0,0],'l2') LIMIT 1 WITH (enable_hnsw=true)";

fn database(persistent: bool, path: &Path) -> Arc<Database> {
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
    db
}

fn nearest(result: ExecutionResult, expected: i32) {
    let ExecutionResult::Query(result) = result else {
        panic!("query expected")
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::Integer(expected));
}

fn contended_begin(owned: bool, mode: TxnMode, persistent: bool) {
    let directory = tempfile::tempdir().unwrap();
    let db = database(persistent, directory.path());
    let (begun_tx, begun_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    // The writer gate is deliberately held until begin reports completion.
    // The timeout is only a deadlock guard, not a latency/performance assertion.
    let gate = db.hnsw_cache_gate.write().unwrap();
    let (started, update) = std::thread::scope(|scope| {
        let reader = Arc::clone(&db);
        scope.spawn(move || {
            if owned {
                let mut tx = reader.begin_owned_embedded_transaction(mode).unwrap();
                begun_tx.send(tx.sql_hnsw.is_empty()).unwrap();
                resume_rx.recv().unwrap();
                nearest(tx.execute_sql(QUERY).unwrap(), 1);
                tx.rollback().unwrap();
            } else {
                let mut tx = reader.begin(mode).unwrap();
                begun_tx.send(tx.sql_hnsw.is_empty()).unwrap();
                resume_rx.recv().unwrap();
                nearest(tx.execute_sql(QUERY).unwrap(), 1);
                tx.rollback().unwrap();
            }
        });
        let started = begun_rx.recv_timeout(Duration::from_secs(5));
        // Always release the gate and worker even on regression, so a failing
        // assertion cannot leave a scoped thread deadlocked.
        drop(gate);
        let update = if started.is_ok() {
            db.execute_sql("UPDATE items SET e=[9,0] WHERE id=1")
                .map(|_| ())
        } else {
            Ok(())
        };
        resume_tx.send(()).unwrap();
        (started, update)
    });
    update.unwrap();
    assert!(started.unwrap(), "contended begin must skip shared graphs");
    // The old transaction must not republish its old graph after the commit.
    assert_eq!(db.hnsw_cache_stats().entries, 0);
    nearest(db.execute_sql(QUERY).unwrap(), 2);
}

macro_rules! begin_case {
    ($name:ident, $owned:expr, $mode:ident, $persistent:expr) => {
        #[test]
        fn $name() {
            contended_begin($owned, TxnMode::$mode, $persistent);
        }
    };
}
begin_case!(borrowed_memory_read, false, ReadOnly, false);
begin_case!(borrowed_memory_write, false, ReadWrite, false);
begin_case!(owned_memory_read, true, ReadOnly, false);
begin_case!(owned_memory_write, true, ReadWrite, false);
begin_case!(borrowed_lsm_read, false, ReadOnly, true);
begin_case!(borrowed_lsm_write, false, ReadWrite, true);
begin_case!(owned_lsm_read, true, ReadOnly, true);
begin_case!(owned_lsm_write, true, ReadWrite, true);

#[test]
fn uncontended_begin_reuses_cached_graphs() {
    let directory = tempfile::tempdir().unwrap();
    let db = database(false, directory.path());
    let misses = db.hnsw_cache_stats().misses;
    let mut borrowed = db.begin(TxnMode::ReadOnly).unwrap();
    assert!(borrowed.sql_hnsw.contains_key("ih"));
    nearest(borrowed.execute_sql(QUERY).unwrap(), 1);
    borrowed.rollback().unwrap();
    let mut owned = db
        .clone()
        .begin_owned_embedded_transaction(TxnMode::ReadOnly)
        .unwrap();
    assert!(owned.sql_hnsw.contains_key("ih"));
    nearest(owned.execute_sql(QUERY).unwrap(), 1);
    owned.rollback().unwrap();
    assert_eq!(db.hnsw_cache_stats().misses, misses);
}

#[test]
fn contended_begin_captures_epoch_before_backend_snapshot() {
    let db = Database::new();
    let _gate = db.hnsw_cache_gate.write().unwrap();
    let before = db.hnsw_cache_epoch.load(Ordering::Acquire);
    let (_, (epoch, cached)) = db
        .begin_with_hnsw_cache(|| {
            // Model a writer committing between epoch sampling and backend begin.
            db.hnsw_cache_epoch.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
        .unwrap();
    assert_eq!(epoch, before);
    assert!(cached.is_empty());
    assert_ne!(epoch, db.hnsw_cache_epoch.load(Ordering::Acquire));
}

#[test]
fn begin_propagates_backend_error_with_and_without_contention() {
    let db = Database::new();
    assert!(matches!(
        db.begin_with_hnsw_cache(|| Err::<(), _>(Error::TxnFailed)),
        Err(Error::TxnFailed)
    ));
    let _gate = db.hnsw_cache_gate.write().unwrap();
    assert!(matches!(
        db.begin_with_hnsw_cache(|| Err::<(), _>(Error::TxnFailed)),
        Err(Error::TxnFailed)
    ));
}
