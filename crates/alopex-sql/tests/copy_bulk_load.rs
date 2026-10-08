use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_sql::Catalog;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::{ConstraintViolation, ExecutionResult, Executor, ExecutorError};
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_rejects_existing_primary_key() {
    // The same public SQL/auto-transaction boundary owns the row control and columnar case.
    for (storage_name, storage_type) in [
        ("row", alopex_sql::catalog::StorageType::Row),
        ("columnar", alopex_sql::catalog::StorageType::Columnar),
    ] {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let plan_sql = |sql: &str| {
            let statement = Parser::parse_sql(&AlopexDialect, sql)
                .unwrap()
                .pop()
                .unwrap();
            Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap()
        };
        executor
            .execute(plan_sql(&format!(
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='{storage_name}')"
            )))
            .unwrap();
        let table_id = {
            let guard = catalog.read().unwrap();
            let table = guard.get_table("users").unwrap();
            assert_eq!(table.storage_options.storage_type, storage_type);
            assert_eq!(
                table.primary_key.as_deref(),
                Some(["id".to_owned()].as_slice())
            );
            assert!(guard.get_indexes_for_table("users").iter().any(|index| {
                index.unique && index.name.starts_with("__pk_") && index.columns == ["id"]
            }));
            table.table_id
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        write_csv(file.path());
        let copy_sql = format!(
            "COPY users FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
            file.path().to_str().unwrap().replace('\'', "''")
        );
        let snapshot = || {
            let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
            let entries = txn.scan_prefix(&[]).unwrap().collect::<Vec<_>>();
            txn.rollback_self().unwrap();
            entries
        };
        let query_rows = |result| {
            let ExecutionResult::Query(query) = result else {
                panic!("expected query result");
            };
            query.rows
        };

        assert_eq!(
            executor.execute(plan_sql(&copy_sql)).unwrap(),
            ExecutionResult::RowsAffected(2),
            "{storage_name}: the first COPY must succeed"
        );
        let expected = vec![
            vec![
                alopex_sql::storage::SqlValue::Integer(1),
                alopex_sql::storage::SqlValue::Text("alice".into()),
            ],
            vec![
                alopex_sql::storage::SqlValue::Integer(2),
                alopex_sql::storage::SqlValue::Text("bob".into()),
            ],
        ];
        assert_eq!(
            query_rows(
                executor
                    .execute(plan_sql("SELECT id, name FROM users ORDER BY id"))
                    .unwrap()
            ),
            expected
        );
        let before = snapshot();
        if storage_type == alopex_sql::catalog::StorageType::Columnar {
            let segment_index =
                alopex_core::columnar::kvs_bridge::key_layout::segment_index_key(table_id);
            assert!(before.iter().any(|(key, _)| key == &segment_index));
        }

        let duplicate = executor.execute(plan_sql(&copy_sql));
        // Observe the state even on RED. Auto-transaction rollback is this public entry's duty.
        let after = query_rows(
            executor
                .execute(plan_sql("SELECT id, name FROM users ORDER BY id"))
                .unwrap(),
        );
        let after_kv = snapshot();
        eprintln!(
            "{storage_name}: duplicate={duplicate:?}, rows_after={after:?}, all_kv_preserved={}",
            after_kv == before
        );
        assert!(
            matches!(duplicate, Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. })) if columns == ["id"]),
            "{storage_name}: COPY must reject the existing PK with the PRIMARY KEY error"
        );
        assert_eq!(after, expected, "{storage_name}: existing rows changed");
        assert_eq!(
            after_kv, before,
            "{storage_name}: failed COPY changed KV state"
        );
        eprintln!("{storage_name}: existing-PK contract PASS");
    }
}
