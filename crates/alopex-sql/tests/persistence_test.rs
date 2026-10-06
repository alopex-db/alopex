use std::io::Write;
use std::sync::{Arc, RwLock};

use alopex_core::KVStore;
use alopex_core::KVTransaction;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_sql::AlopexDialect;
use alopex_sql::Catalog;
use alopex_sql::Parser;
use alopex_sql::Planner;
use alopex_sql::SqlValue;
use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
use alopex_sql::executor::{ConstraintViolation, ExecutionResult, Executor, ExecutorError};
use alopex_sql::storage::{BorrowedSqlTransaction, SqlTxn, TxnBridge};

fn run_sql_in_txn(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    mode: TxnMode,
    sql: &str,
) -> ExecutionResult {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).expect("parse");
    assert!(!stmts.is_empty(), "sql must contain at least one statement");

    let mut txn = store.begin(mode).expect("begin");
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = TxnBridge::<MemoryKV>::wrap_external(&mut txn, mode, &mut overlay);
    let mut executor: Executor<_, _> = Executor::new(store.clone(), catalog.clone());

    let mut last = ExecutionResult::Success;
    for stmt in &stmts {
        let plan = {
            let catalog_guard = catalog.read().expect("catalog lock poisoned");
            let (_, overlay) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*catalog_guard, &*overlay);
            let planner = Planner::new(&view);
            planner.plan(stmt).expect("plan")
        };

        last = executor
            .execute_in_txn(plan, &mut borrowed)
            .expect("execute_in_txn");
    }

    drop(borrowed);
    txn.commit_self().expect("commit");

    if mode == TxnMode::ReadWrite {
        let mut catalog_guard = catalog.write().expect("catalog lock poisoned");
        catalog_guard.apply_overlay(overlay);
    }

    last
}

fn postload_unique_csv(contents: &str) -> (tempfile::NamedTempFile, String) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    let sql = format!(
        "COPY events FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
        file.path().to_str().unwrap().replace('\'', "''")
    );
    (file, sql)
}

fn postload_kv<'a>(txn: &mut impl KVTransaction<'a>) -> Vec<(Vec<u8>, Vec<u8>)> {
    txn.scan_prefix(&[]).unwrap().collect()
}

fn postload_execute_borrowed(
    executor: &mut Executor<MemoryKV, PersistentCatalog<MemoryKV>>,
    catalog: &Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    borrowed: &mut BorrowedSqlTransaction<'_, '_, '_, MemoryKV>,
    sql: &str,
) -> Result<ExecutionResult, ExecutorError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    assert_eq!(statements.len(), 1);
    let plan = {
        let guard = catalog.read().unwrap();
        let (_, overlay) = borrowed.split_parts();
        Planner::new(&TxnCatalogView::new(&*guard, overlay))
            .plan(&statements[0])
            .unwrap()
    };
    executor.execute_in_txn(plan, borrowed)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_columnar_unique_rejection_preserves_borrowed_prior_write() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadWrite,
        "CREATE TABLE events (id INT) WITH (storage='columnar'); \
         CREATE TABLE keeper (id INT PRIMARY KEY, note TEXT);",
    );
    let (_csv, copy_sql) = postload_unique_csv("id\n1\n1\n");
    assert_eq!(
        run_sql_in_txn(
            store.clone(),
            catalog.clone(),
            TxnMode::ReadWrite,
            &copy_sql
        ),
        ExecutionResult::RowsAffected(2)
    );
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let committed_before = postload_kv(&mut txn);
    let mut overlay = CatalogOverlay::new();
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let pending;
    {
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        assert_eq!(
            postload_execute_borrowed(
                &mut executor,
                &catalog,
                &mut borrowed,
                "INSERT INTO keeper VALUES (7, 'prior')",
            )
            .unwrap(),
            ExecutionResult::RowsAffected(1)
        );
        pending = {
            let (mut sql_txn, _) = borrowed.split_parts();
            postload_kv(sql_txn.inner_mut())
        };
        assert_ne!(pending, committed_before, "prior write must be pending");
        let result = postload_execute_borrowed(
            &mut executor,
            &catalog,
            &mut borrowed,
            "CREATE UNIQUE INDEX uq_events ON events(id)",
        );
        eprintln!("borrowed CREATE UNIQUE result={result:?}");
        assert!(matches!(
            &result,
            Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique {
                index_name, columns, ..
            })) if index_name == "uq_events" && columns == &["id"]
        ));
        let (mut sql_txn, overlay) = borrowed.split_parts();
        assert_eq!(postload_kv(sql_txn.inner_mut()), pending);
        let guard = catalog.read().unwrap();
        assert!(
            TxnCatalogView::new(&*guard, overlay)
                .get_index("uq_events")
                .is_none()
        );
        assert!(guard.get_index("uq_events").is_none());
    }
    // The caller commits its earlier INSERT instead of rolling back the transaction.
    txn.commit_self().unwrap();
    catalog.write().unwrap().apply_overlay(overlay);
    let reloaded = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert!(reloaded.read().unwrap().get_index("uq_events").is_none());
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(postload_kv(&mut read), pending);
    read.rollback_self().unwrap();
    let ExecutionResult::Query(keeper) = run_sql_in_txn(
        store.clone(),
        reloaded.clone(),
        TxnMode::ReadOnly,
        "SELECT id, note FROM keeper ORDER BY id",
    ) else {
        panic!("expected committed prior write");
    };
    assert_eq!(
        keeper.rows,
        vec![vec![SqlValue::Integer(7), SqlValue::Text("prior".into())]]
    );
    let ExecutionResult::Query(events) = run_sql_in_txn(
        store,
        reloaded,
        TxnMode::ReadOnly,
        "SELECT id FROM events ORDER BY id",
    ) else {
        panic!("expected preserved columnar rows");
    };
    assert_eq!(
        events.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(1)]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_columnar_unique_reload_rejects_duplicate_copy() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("columnar_unique.wal");
    let expected_index_id = {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store.clone(),
            catalog.clone(),
            TxnMode::ReadWrite,
            "CREATE TABLE events (id INT) WITH (storage='columnar')",
        );
        let (_csv, copy_sql) = postload_unique_csv("id\n1\n2\n");
        assert_eq!(
            run_sql_in_txn(
                store.clone(),
                catalog.clone(),
                TxnMode::ReadWrite,
                &copy_sql
            ),
            ExecutionResult::RowsAffected(2)
        );
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        let index_id;
        {
            let mut borrowed =
                TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
            let mut executor = Executor::new(store.clone(), catalog.clone());
            assert_eq!(
                postload_execute_borrowed(
                    &mut executor,
                    &catalog,
                    &mut borrowed,
                    "CREATE UNIQUE INDEX uq_events ON events(id)",
                )
                .unwrap(),
                ExecutionResult::Success
            );
            let guard = catalog.read().unwrap();
            assert!(guard.get_index("uq_events").is_none());
            let (_, overlay) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*guard, overlay);
            let index = view.get_index("uq_events").unwrap();
            assert!(index.unique);
            index_id = index.index_id;
        }
        txn.commit_self().unwrap();
        catalog.write().unwrap().apply_overlay(overlay);
        index_id
    };

    // Reopen the real WAL and reconstruct catalog metadata; do not reuse its cache.
    let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    {
        let guard = catalog.read().unwrap();
        let index = guard.get_index("uq_events").unwrap();
        assert!(index.unique);
        assert_eq!(index.index_id, expected_index_id);
        assert_eq!(index.table, "events");
        assert_eq!(index.columns, vec!["id"]);
        assert_eq!(index.column_indices, vec![0]);
    }
    let (_csv, copy_sql) = postload_unique_csv("id\n2\n3\n");
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let before = postload_kv(&mut txn);
    let mut overlay = CatalogOverlay::new();
    {
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let result = postload_execute_borrowed(&mut executor, &catalog, &mut borrowed, &copy_sql);
        eprintln!("reloaded UNIQUE duplicate COPY result={result:?}");
        assert!(matches!(
            &result,
            Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique {
                index_name, columns, ..
            })) if index_name == "uq_events" && columns == &["id"]
        ));
        let (mut sql_txn, overlay) = borrowed.split_parts();
        assert_eq!(postload_kv(sql_txn.inner_mut()), before);
        let guard = catalog.read().unwrap();
        assert!(
            TxnCatalogView::new(&*guard, overlay)
                .get_index("uq_events")
                .unwrap()
                .unique
        );
    }
    txn.commit_self().unwrap();
    catalog.write().unwrap().apply_overlay(overlay);
    let mut read = store.begin(TxnMode::ReadOnly).unwrap();
    assert_eq!(postload_kv(&mut read), before);
    read.rollback_self().unwrap();
    let ExecutionResult::Query(events) = run_sql_in_txn(
        store,
        catalog,
        TxnMode::ReadOnly,
        "SELECT id FROM events ORDER BY id",
    ) else {
        panic!("expected unchanged columnar rows after rejected COPY");
    };
    assert_eq!(
        events.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_catalog_survives_restart_with_flush() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("catalog_flush.wal");

    // 1st run
    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));

        run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadWrite,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        );
        store.flush().unwrap();
    }

    // restart
    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = PersistentCatalog::load(store.clone()).unwrap();
        assert!(catalog.table_exists("users"));
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_data_survives_restart_with_flush() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("data_flush.wal");

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadWrite,
            r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'alice');
            "#,
        );
        store.flush().unwrap();
    }

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        let result = run_sql_in_txn(
            store,
            catalog,
            TxnMode::ReadOnly,
            "SELECT id, name FROM users ORDER BY id;",
        );
        match result {
            ExecutionResult::Query(q) => {
                assert_eq!(q.rows.len(), 1);
                assert_eq!(q.rows[0][0], SqlValue::Integer(1));
                assert_eq!(q.rows[0][1], SqlValue::Text("alice".into()));
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_default_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("default_restart.wal");

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadWrite,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL DEFAULT 0, \
             created_at TIMESTAMP NOT NULL DEFAULT NOW()); \
             INSERT INTO users (id) VALUES (1);",
        );
        store.flush().unwrap();
    }

    let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadWrite,
        "INSERT INTO users (id) VALUES (2);",
    );
    let result = run_sql_in_txn(
        store,
        catalog,
        TxnMode::ReadOnly,
        "SELECT id, qty, created_at FROM users ORDER BY id;",
    );

    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(query.rows.len(), 2);
    assert_eq!(query.rows[0][0], SqlValue::Integer(1));
    assert_eq!(query.rows[0][1], SqlValue::Integer(0));
    assert_eq!(query.rows[1][0], SqlValue::Integer(2));
    assert_eq!(query.rows[1][1], SqlValue::Integer(0));
    assert!(matches!(query.rows[0][2], SqlValue::Timestamp(_)));
    assert!(matches!(query.rows[1][2], SqlValue::Timestamp(_)));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_catalog_survives_restart_wal_only() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("catalog_wal_only.wal");

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store,
            catalog,
            TxnMode::ReadWrite,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        );
    }

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = PersistentCatalog::load(store.clone()).unwrap();
        assert!(catalog.table_exists("users"));
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_id_counter_consistent_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("id_counter.wal");

    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadWrite,
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY);",
        );
        store.flush().unwrap();
    }

    let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    let t1_id = catalog.read().unwrap().get_table("t1").unwrap().table_id;

    run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadWrite,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY);",
    );

    let t2_id = catalog.read().unwrap().get_table("t2").unwrap().table_id;
    assert!(t2_id > t1_id);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistence_test_wal_truncation_recovery_without_hooks() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("truncate.wal");

    // Phase 1: flush 済みの状態を作る（SSTable 相当の永続化）。
    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store.clone(),
            catalog,
            TxnMode::ReadWrite,
            r#"
            CREATE TABLE t1 (id INTEGER PRIMARY KEY);
            INSERT INTO t1 (id) VALUES (1);
            "#,
        );
        store.flush().unwrap();
    }

    // Phase 2: flush せず WAL にのみ残る変更を作る（最後のレコードを壊す想定）。
    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        run_sql_in_txn(
            store,
            catalog,
            TxnMode::ReadWrite,
            "CREATE TABLE t2 (id INTEGER PRIMARY KEY);",
        );
    }

    // WAL を途中で切り詰め（クラッシュ模擬）。
    //
    // 「末尾 N バイト」だと WAL レコード形式の変更でフレークし得るため、
    // 最終レコードのボディを 1 バイト欠落させる形で確実に破損させる。
    {
        let bytes = std::fs::read(&wal_path).unwrap();
        let mut pos = 0usize;
        let mut last_start = None::<usize>;
        let mut last_len = 0usize;

        while pos + 8 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let record_total = 8usize.saturating_add(len);
            if pos + record_total > bytes.len() {
                break;
            }
            last_start = Some(pos);
            last_len = len;
            pos += record_total;
        }

        let last_start = last_start.expect("wal must contain at least one full record");
        assert!(last_len > 0, "wal record body must not be empty");
        let new_len = (last_start + 8 + last_len - 1) as u64;

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(new_len).unwrap();
    }

    // Phase 3: 回復確認（t1 はアクセス可能、t2 は存在しないことを期待）。
    {
        let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        let result = run_sql_in_txn(
            store,
            catalog.clone(),
            TxnMode::ReadOnly,
            "SELECT id FROM t1;",
        );
        match result {
            ExecutionResult::Query(q) => assert_eq!(q.rows.len(), 1),
            other => panic!("expected query result, got {other:?}"),
        }

        let catalog_guard = catalog.read().unwrap();
        assert!(!catalog_guard.table_exists("t2"));
    }
}
