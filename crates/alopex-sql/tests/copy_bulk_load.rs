use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Catalog;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::TxnBridge;

fn create_table(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
) {
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='columnar');",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    executor.execute(plan).unwrap();
}

fn write_csv(path: &Path) {
    let mut f = File::create(path).unwrap();
    writeln!(f, "id,name").unwrap();
    writeln!(f, "1,alice").unwrap();
    writeln!(f, "2,bob").unwrap();
}

#[test]
fn copy_csv_success_and_query() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_table(&mut executor, &catalog);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        let res = execute_copy(
            &mut txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(res, ExecutionResult::RowsAffected(2));
    }

    let stmt = Parser::parse_sql(&AlopexDialect, "SELECT name FROM users ORDER BY id")
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    match executor.execute(plan).unwrap() {
        ExecutionResult::Query(q) => {
            assert_eq!(
                q.rows,
                vec![
                    vec![alopex_sql::storage::SqlValue::Text("alice".into())],
                    vec![alopex_sql::storage::SqlValue::Text("bob".into())],
                ]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn copy_schema_mismatch_rolls_back() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_table(&mut executor, &catalog);

    // bad CSV: missing column
    let bad_file = tempfile::NamedTempFile::new().unwrap();
    {
        let mut f = File::create(bad_file.path()).unwrap();
        writeln!(f, "id").unwrap();
        writeln!(f, "1").unwrap();
    }

    let err = {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        let res = execute_copy(
            &mut txn,
            &*guard,
            "users",
            bad_file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        );
        let err = res.unwrap_err();
        txn.rollback().unwrap();
        err
    };
    assert!(matches!(
        err,
        ExecutorError::SchemaMismatch { .. } | ExecutorError::BulkLoad(_)
    ));

    // Ensure no rows were written
    // Ensure no rows were written by scanning storage directly.
    let mut verify = bridge.begin_read().unwrap();
    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();
    let count = verify.table_storage(&stored).scan().unwrap().count();
    verify.commit().unwrap();
    assert_eq!(count, 0);
}
