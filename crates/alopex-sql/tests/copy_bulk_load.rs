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
use alopex_sql::storage::{SqlTxn, SqlValue, TxnBridge};

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

fn constraint_execute(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<ExecutionResult, ExecutorError> {
    let statement = Parser::parse_sql(&AlopexDialect, sql)
        .unwrap()
        .pop()
        .unwrap();
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    executor.execute(plan)
}

fn constraint_fixture(
    columns: &str,
) -> (
    Arc<MemoryKV>,
    Arc<RwLock<MemoryCatalog>>,
    Executor<MemoryKV, MemoryCatalog>,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    constraint_execute(
        &mut executor,
        &catalog,
        &format!("CREATE TABLE users ({columns}) WITH (storage='columnar')"),
    )
    .unwrap();
    assert_eq!(
        catalog
            .read()
            .unwrap()
            .get_table("users")
            .unwrap()
            .storage_options
            .storage_type,
        alopex_sql::catalog::StorageType::Columnar
    );
    (store, catalog, executor)
}

fn constraint_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn constraint_copy_sql(file: &tempfile::NamedTempFile) -> String {
    format!(
        "COPY users FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
        file.path().to_str().unwrap().replace('\'', "''")
    )
}

fn constraint_snapshot(store: &MemoryKV) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let entries = txn.scan_prefix(&[]).unwrap().collect();
    txn.rollback_self().unwrap();
    entries
}

fn assert_columnar_null_rejected(columns: &str, csv: &str, expected_column: &str) {
    let (store, catalog, mut executor) = constraint_fixture(columns);
    let before = constraint_snapshot(&store);
    let file = constraint_csv(csv);
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file));
    let after = constraint_snapshot(&store);
    eprintln!(
        "NULL({expected_column}): runtime={result:?}, all_kv_unchanged={}",
        before == after
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::NotNull { column })) if column == expected_column),
        "expected NotNull({expected_column}), got {result:?}"
    );
    assert_eq!(after, before, "rejected NULL changed the complete KV state");
    eprintln!("NULL({expected_column}): strict error and complete KV oracle reached");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_inline_pk_null_is_rejected() {
    assert_columnar_null_rejected("id INT PRIMARY KEY, name TEXT", "id,name\nNULL,bad\n", "id");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_not_null_only_is_rejected() {
    assert_columnar_null_rejected("id INT, name TEXT NOT NULL", "id,name\n1,NULL\n", "name");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_table_pk_null_is_rejected() {
    assert_columnar_null_rejected(
        "id INT, name TEXT, PRIMARY KEY (id)",
        "id,name\nNULL,bad\n",
        "id",
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_pk_first_null_is_rejected() {
    assert_columnar_null_rejected("a INT, b INT, PRIMARY KEY (a, b)", "a,b\nNULL,1\n", "a");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_pk_second_null_is_rejected() {
    assert_columnar_null_rejected("a INT, b INT, PRIMARY KEY (a, b)", "a,b\n1,NULL\n", "b");
}

fn assert_nullable_composite_unique(csv: &str, expected_row: Vec<SqlValue>) {
    let (_, catalog, mut executor) =
        constraint_fixture("a INT, b INT, CONSTRAINT uq_pair UNIQUE (a, b)");
    let file = constraint_csv(csv);
    // Each COPY covers an input duplicate; the second also covers existing segments.
    for copy in 1..=2 {
        assert_eq!(
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
            ExecutionResult::RowsAffected(2),
            "nullable UNIQUE COPY {copy} must accept both rows"
        );
        let ExecutionResult::Query(query) =
            constraint_execute(&mut executor, &catalog, "SELECT a, b FROM users").unwrap()
        else {
            panic!("expected query result");
        };
        assert_eq!(query.rows, vec![expected_row.clone(); 2 * copy]);
        eprintln!("nullable UNIQUE COPY {copy}: complete row values reached");
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_first_null_is_allowed() {
    assert_nullable_composite_unique(
        "a,b\nNULL,7\nNULL,7\n",
        vec![SqlValue::Null, SqlValue::Integer(7)],
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_second_null_is_allowed() {
    assert_nullable_composite_unique(
        "a,b\n7,NULL\n7,NULL\n",
        vec![SqlValue::Integer(7), SqlValue::Null],
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_non_null_duplicate_is_rejected() {
    let (store, catalog, mut executor) =
        constraint_fixture("a INT, b INT, CONSTRAINT uq_pair UNIQUE (a, b)");
    let first = constraint_csv("a,b\n1,7\n");
    assert_eq!(
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&first)).unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let before = constraint_snapshot(&store);
    let conflicting = constraint_csv("a,b\n2,8\n1,7\n");
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&conflicting));
    let after = constraint_snapshot(&store);
    eprintln!(
        "non-NULL UNIQUE: runtime={result:?}, all_kv_unchanged={}",
        before == after
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
            if index_name == "uq_pair" && columns == &["a", "b"]),
        "expected the named composite UNIQUE error, got {result:?}"
    );
    assert_eq!(after, before);
    let ExecutionResult::Query(query) =
        constraint_execute(&mut executor, &catalog, "SELECT a, b FROM users").unwrap()
    else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(7)]]
    );
}

fn assert_borrowed_copy_failure_preserves_prior_write(csv: &str, null_error: bool) {
    let (store, catalog, mut executor) = constraint_fixture("id INT, name TEXT, PRIMARY KEY (id)");
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let first = constraint_csv("id,name\n1,prior\n");
    let failing = constraint_csv(csv);
    let guard = catalog.read().unwrap();
    let committed_before = constraint_snapshot(&store);
    assert_eq!(
        execute_copy(
            &mut txn,
            &*guard,
            "users",
            first.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default()
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    assert_ne!(
        before, committed_before,
        "the first COPY must be pending in this transaction"
    );
    let result = execute_copy(
        &mut txn,
        &*guard,
        "users",
        failing.path().to_str().unwrap(),
        FileFormat::Csv,
        CopyOptions { header: true },
        &CopySecurityConfig::default(),
    );
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    drop(guard);
    // Observe all outcomes even on RED; never rollback to make the equality oracle pass.
    let commit_result = txn.commit();
    let committed_after = constraint_snapshot(&store);
    let query_result = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id, name FROM users ORDER BY id",
    );
    eprintln!(
        "borrowed COPY: runtime={result:?}, all_kv_unchanged={}, commit={commit_result:?}, query={query_result:?}",
        before == after
    );
    let correct_error = if null_error {
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::NotNull { column })) if column == "id")
    } else {
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. })) if columns == &["id"])
    };
    assert!(correct_error, "unexpected borrowed COPY result: {result:?}");
    assert_eq!(
        after, before,
        "failed COPY changed the pending complete KV state"
    );
    commit_result.unwrap();
    assert_eq!(
        committed_after, before,
        "commit did not preserve exactly the prior write"
    );
    let ExecutionResult::Query(query) = query_result.unwrap() else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("prior".into())]]
    );
    eprintln!("borrowed COPY: strict error, full KV equality, commit and prior values reached");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_borrowed_late_null_preserves_prior_write() {
    assert_borrowed_copy_failure_preserves_prior_write("id,name\n2,new\nNULL,bad\n", true);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_borrowed_late_pk_duplicate_preserves_prior_write() {
    assert_borrowed_copy_failure_preserves_prior_write("id,name\n2,new\n1,duplicate\n", false);
}
