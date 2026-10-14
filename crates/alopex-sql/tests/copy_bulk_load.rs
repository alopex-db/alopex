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

fn assert_postload_columnar_unique_rejects_duplicates(public_sql: bool) {
    use alopex_sql::catalog::IndexMetadata;
    use alopex_sql::planner::LogicalPlan;

    let (store, catalog, mut executor) = constraint_fixture("id INT");
    let file = constraint_csv("id\n1\n1\n");
    assert_eq!(
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    let expected = vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(1)]];
    let ExecutionResult::Query(before_rows) =
        constraint_execute(&mut executor, &catalog, "SELECT id FROM users ORDER BY id").unwrap()
    else {
        panic!("expected existing columnar rows");
    };
    assert_eq!(before_rows.rows, expected);
    let before = constraint_snapshot(&store);
    let table_before = catalog.read().unwrap().get_table("users").unwrap().clone();
    let plan = if public_sql {
        let statement =
            Parser::parse_sql(&AlopexDialect, "CREATE UNIQUE INDEX uq_users ON users(id)")
                .unwrap()
                .pop()
                .unwrap();
        assert!(matches!(
            &statement.kind,
            alopex_sql::ast::StatementKind::CreateIndex(index) if index.unique
        ));
        Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap()
    } else {
        LogicalPlan::CreateIndex {
            index: IndexMetadata::new(0, "uq_users", "users", vec!["id".into()]).with_unique(true),
            if_not_exists: false,
        }
    };
    assert!(matches!(&plan, LogicalPlan::CreateIndex { index, .. } if index.unique));
    let result = executor.execute(plan);
    let after = constraint_snapshot(&store);
    let index_absent = catalog.read().unwrap().get_index("uq_users").is_none();
    let table_after = catalog.read().unwrap().get_table("users").unwrap().clone();
    let ExecutionResult::Query(after_rows) =
        constraint_execute(&mut executor, &catalog, "SELECT id FROM users ORDER BY id").unwrap()
    else {
        panic!("expected preserved columnar rows");
    };
    eprintln!(
        "public_sql={public_sql} result={result:?} all_kv_unchanged={} index_absent={index_absent} rows={:?}",
        before == after,
        after_rows.rows
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
            if index_name == "uq_users" && columns == &["id"]),
        "existing duplicate columnar rows must reject UNIQUE creation: {result:?}"
    );
    assert_eq!(after, before);
    assert!(index_absent);
    assert_eq!(table_after.table_id, table_before.table_id);
    assert_eq!(table_after.name, table_before.name);
    assert_eq!(table_after.primary_key, table_before.primary_key);
    assert_eq!(table_after.properties, table_before.properties);
    assert_eq!(table_after.storage_options, table_before.storage_options);
    assert_eq!(after_rows.rows, expected);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_postload_unique_sql_rejects_existing_duplicates() {
    assert_postload_columnar_unique_rejects_duplicates(true);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_postload_unique_typed_plan_rejects_existing_duplicates() {
    assert_postload_columnar_unique_rejects_duplicates(false);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_unique_preserves_nulls_and_checks_future_copy() {
    let (store, catalog, mut executor) = constraint_fixture("id INT");
    for csv in ["id\n1\nNULL\n", "id\n2\nNULL\n"] {
        let file = constraint_csv(csv);
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
    }
    constraint_execute(
        &mut executor,
        &catalog,
        "CREATE UNIQUE INDEX uq_users ON users(id)",
    )
    .unwrap();
    let index = catalog
        .read()
        .unwrap()
        .get_index("uq_users")
        .unwrap()
        .clone();
    assert!(index.unique);
    assert_eq!(index.column_indices, vec![0]);
    let valid = constraint_csv("id\n3\nNULL\n");
    constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&valid)).unwrap();
    let before = constraint_snapshot(&store);
    let duplicate = constraint_csv("id\n4\n1\n");
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&duplicate));
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, .. })) if index_name == "uq_users"),
        "{result:?}"
    );
    assert_eq!(constraint_snapshot(&store), before);
    let ExecutionResult::Query(query) = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id FROM users ORDER BY id NULLS LAST",
    )
    .unwrap() else {
        panic!("expected query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Null],
            vec![SqlValue::Null],
            vec![SqlValue::Null]
        ]
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_composite_unique_checks_complete_keys() {
    for duplicate in [false, true] {
        let (store, catalog, mut executor) = constraint_fixture("a INT, b INT");
        for csv in [
            "a,b\n1,10\nNULL,10\n1,NULL\n",
            "a,b\n1,11\nNULL,10\n1,NULL\n",
        ] {
            let file = constraint_csv(csv);
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
        }
        if duplicate {
            let file = constraint_csv("a,b\n1,10\n");
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
        }
        let before = constraint_snapshot(&store);
        let ExecutionResult::Query(rows_before) = constraint_execute(
            &mut executor,
            &catalog,
            "SELECT a, b FROM users ORDER BY a NULLS LAST, b NULLS LAST",
        )
        .unwrap() else {
            panic!("expected composite rows");
        };
        let result = executor.execute(alopex_sql::planner::LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "uq_pair",
                "users",
                vec!["a".into(), "b".into()],
            )
            .with_unique(true),
            if_not_exists: false,
        });
        let ExecutionResult::Query(rows_after) = constraint_execute(
            &mut executor,
            &catalog,
            "SELECT a, b FROM users ORDER BY a NULLS LAST, b NULLS LAST",
        )
        .unwrap() else {
            panic!("expected preserved composite rows");
        };
        assert_eq!(rows_after.rows, rows_before.rows);
        if duplicate {
            assert!(
                matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. })) if index_name == "uq_pair" && columns == &["a", "b"]),
                "{result:?}"
            );
            assert_eq!(constraint_snapshot(&store), before);
            assert!(catalog.read().unwrap().get_index("uq_pair").is_none());
        } else {
            result.unwrap();
            assert_eq!(
                catalog
                    .read()
                    .unwrap()
                    .get_index("uq_pair")
                    .unwrap()
                    .column_indices,
                vec![0, 1]
            );
            let file = constraint_csv("a,b\n2,10\nNULL,10\n1,NULL\n");
            assert_eq!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
                ExecutionResult::RowsAffected(3)
            );
            let before = constraint_snapshot(&store);
            let file = constraint_csv("a,b\n1,11\n");
            assert!(matches!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)),
                Err(ExecutorError::ConstraintViolation(
                    ConstraintViolation::Unique { .. }
                ))
            ));
            assert_eq!(constraint_snapshot(&store), before);
        }
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_unique_rejects_unindexable_type_before_null_skip() {
    for (declaration, kind) in [
        ("JSON", "Json"),
        ("VECTOR(2)", "Vector"),
        ("INTERVAL", "Interval"),
        ("DECIMAL(10,2)", "Decimal"),
    ] {
        let (store, catalog, mut executor) = constraint_fixture(&format!("payload {declaration}"));
        // Fixed-encoding COPY has a separate known selector boundary on this
        // base. Keep those cases at the empty-table DDL boundary, not a codec test.
        if kind == "Json" {
            let file = constraint_csv("payload\nNULL\n");
            assert_eq!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
                ExecutionResult::RowsAffected(1)
            );
        }
        let before = constraint_snapshot(&store);
        let result = executor.execute(alopex_sql::planner::LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "uq_json",
                "users",
                vec!["payload".into()],
            )
            .with_unique(true),
            if_not_exists: false,
        });
        if kind == "Json" {
            assert!(
                matches!(&result, Err(ExecutorError::InvalidOperation { operation, reason })
            if operation == "CREATE INDEX" && reason == "Alopex does not define a JSON sort order"),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(&result, Err(ExecutorError::Storage(alopex_sql::storage::StorageError::TypeMismatch { expected, actual })) if expected == "indexable scalar type" && actual == kind),
                "{result:?}"
            );
        }
        assert_eq!(constraint_snapshot(&store), before);
        assert!(catalog.read().unwrap().get_index("uq_json").is_none());
    }
}
