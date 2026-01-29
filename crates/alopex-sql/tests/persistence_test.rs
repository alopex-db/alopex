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
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::storage::TxnBridge;

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
