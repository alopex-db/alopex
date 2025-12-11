use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Catalog;
use alopex_sql::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TxnBridge};

fn create_executor() -> (
    Arc<MemoryKV>,
    TxnBridge<MemoryKV>,
    Arc<RwLock<MemoryCatalog>>,
    Executor<MemoryKV, MemoryCatalog>,
) {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let executor = Executor::new(store.clone(), catalog.clone());
    (store, bridge, catalog, executor)
}

fn create_table(executor: &mut Executor<MemoryKV, MemoryCatalog>) {
    let table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
            ColumnMetadata::new("name", ResolvedType::Text),
        ],
    );
    executor
        .execute(LogicalPlan::CreateTable {
            table,
            if_not_exists: false,
            with_options: vec![
                ("storage".into(), "columnar".into()),
                ("row_group_size".into(), "1000".into()),
            ],
        })
        .unwrap();
}

fn write_csv(path: &Path) {
    let mut f = File::create(path).unwrap();
    writeln!(f, "id,name").unwrap();
    writeln!(f, "1,alpha").unwrap();
    writeln!(f, "2,beta").unwrap();
    writeln!(f, "3,gamma").unwrap();
    writeln!(f, "4,delta").unwrap();
}

#[test]
fn copy_and_select_columnar_with_pruning_and_projection() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table(&mut executor);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        let result = execute_copy(
            &mut copy_txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
        assert_eq!(result, ExecutionResult::RowsAffected(4));
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();

    // Row storage should remain untouched for columnar tables.
    let mut verify_txn = bridge.begin_read().unwrap();
    let row_count = verify_txn.table_storage(&stored).scan().unwrap().count();
    verify_txn.commit().unwrap();
    assert_eq!(row_count, 0);

    // Query with filter should return rows from pruned row groups.
    let sql = "SELECT name FROM users WHERE id >= 3";
    let stmt = Parser::parse_sql(&AlopexDialect::default(), sql)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let query_result = executor.execute(plan).unwrap();

    match query_result {
        ExecutionResult::Query(q) => {
            assert_eq!(
                q.rows,
                vec![
                    vec![SqlValue::Text("gamma".into())],
                    vec![SqlValue::Text("delta".into())],
                ]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn columnar_scan_falls_back_when_statistics_missing() {
    let (_store, bridge, catalog, mut executor) = create_executor();
    create_table(&mut executor);
    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut copy_txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut copy_txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        copy_txn.commit().unwrap();
    }

    let stored = catalog.read().unwrap().get_table("users").unwrap().clone();

    // Remove RowGroup statistics to force fallback.
    let mut prefix = vec![key_layout::PREFIX_ROW_GROUP];
    prefix.extend_from_slice(&stored.table_id.to_le_bytes());
    bridge
        .with_write_txn(|txn| {
            txn.delete_prefix(&prefix)?;
            Ok(())
        })
        .unwrap();

    let sql = "SELECT name FROM users WHERE id = 2";
    let stmt = Parser::parse_sql(&AlopexDialect::default(), sql)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let result = executor.execute(plan).unwrap();

    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows, vec![vec![SqlValue::Text("beta".into())]]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}
