use std::io::Write;
use std::sync::{Arc, RwLock};

use alopex_core::columnar::kvs_bridge::key_layout;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, CatalogOverlay, PersistentCatalog};
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::storage::{SqlValue, TxnBridge};
use alopex_sql::{AlopexDialect, Parser, Planner};

type TestExecutor = Executor<MemoryKV, PersistentCatalog<MemoryKV>>;
type CatalogHandle = Arc<RwLock<PersistentCatalog<MemoryKV>>>;

fn run(executor: &mut TestExecutor, catalog: &CatalogHandle, sql: &str) -> ExecutionResult {
    let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    executor.execute(plan).unwrap()
}

fn rows(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
    match result {
        ExecutionResult::Query(result) => result.rows,
        other => panic!("expected query, got {other:?}"),
    }
}

fn snapshot(store: &MemoryKV) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    txn.scan_prefix(&[]).unwrap().collect()
}

fn data_key(key: &[u8], table_id: u32) -> bool {
    key.len() >= 5 && (0x11..=0x15).contains(&key[0]) && key[1..5] == table_id.to_le_bytes()
}

fn copy(executor: &mut TestExecutor, catalog: &CatalogHandle, table: &str, csv: &str) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(csv.as_bytes()).unwrap();
    assert!(matches!(
        run(
            executor,
            catalog,
            &format!(
                "COPY {table} FROM '{}' WITH (FORMAT CSV)",
                file.path().display()
            )
        ),
        ExecutionResult::RowsAffected(_)
    ));
}

fn setup() -> (Arc<MemoryKV>, CatalogHandle, TestExecutor, u32, u32) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    for table in ["items", "neighbor"] {
        run(
            &mut executor,
            &catalog,
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, price DOUBLE) WITH (storage='columnar', row_group_size=1000)"
            ),
        );
        copy(&mut executor, &catalog, table, "1,1.5\n2,2.5\n");
        copy(&mut executor, &catalog, table, "3,3.5\n");
    }
    let id = catalog.read().unwrap().get_table("items").unwrap().table_id;
    let neighbor = catalog
        .read()
        .unwrap()
        .get_table("neighbor")
        .unwrap()
        .table_id;
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    for table_id in [id, neighbor] {
        // V08 has no writer here: sentinels prove key ownership, not reader validity.
        for key in [
            key_layout::v08_header_key(table_id, 9),
            key_layout::v08_schema_key(table_id, 9),
            key_layout::v08_directory_key(table_id, 9),
            key_layout::v08_chunk_key(table_id, 9, 0, 0),
        ] {
            txn.put(key, vec![7]).unwrap();
        }
        let mut reserved = vec![key_layout::PREFIX_TABLE_META];
        reserved.extend_from_slice(&table_id.to_le_bytes());
        txn.put(reserved, vec![8]).unwrap();
    }
    txn.commit_self().unwrap();
    (store, catalog, executor, id, neighbor)
}

fn expected() -> Vec<Vec<SqlValue>> {
    (1..=3)
        .map(|id| vec![SqlValue::Integer(id), SqlValue::Double(f64::from(id) + 0.5)])
        .collect()
}

#[test]
fn columnar_truncate_removes_owned_data_preserves_metadata_and_recopies() {
    let (store, catalog, mut executor, id, neighbor) = setup();
    assert_eq!(
        rows(run(
            &mut executor,
            &catalog,
            "SELECT id, price FROM items ORDER BY id"
        )),
        expected()
    );
    let before = snapshot(&store);
    for prefix in 0x11..=0x15 {
        assert!(
            before
                .iter()
                .any(|(key, _)| data_key(key, id) && key[0] == prefix)
        );
    }
    let table = catalog.read().unwrap().get_table("items").unwrap().clone();
    let index = catalog
        .read()
        .unwrap()
        .get_index("__pk_items")
        .unwrap()
        .clone();
    run(&mut executor, &catalog, "TRUNCATE items");
    assert!(
        rows(run(
            &mut executor,
            &catalog,
            "SELECT id, price FROM items ORDER BY id"
        ))
        .is_empty()
    );
    let after = snapshot(&store);
    assert!(!after.iter().any(|(key, _)| data_key(key, id)));
    let retained: Vec<_> = before
        .into_iter()
        .filter(|(key, _)| !data_key(key, id))
        .collect();
    assert_eq!(
        after, retained,
        "catalog, reserved metadata, and neighboring keyspaces must be unchanged"
    );
    assert!(after.iter().any(|(key, _)| data_key(key, neighbor)));
    {
        let guard = catalog.read().unwrap();
        let actual = guard.get_table("items").unwrap();
        assert_eq!(actual.table_id, table.table_id);
        assert_eq!(actual.primary_key, table.primary_key);
        assert_eq!(actual.columns.len(), table.columns.len());
        for (actual, expected) in actual.columns.iter().zip(&table.columns) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.data_type, expected.data_type);
        }
        let actual = guard.get_index("__pk_items").unwrap();
        assert_eq!(actual.index_id, index.index_id);
        assert_eq!(actual.columns, index.columns);
        assert_eq!(actual.column_indices, index.column_indices);
        assert_eq!(actual.unique, index.unique);
    }
    assert_eq!(
        rows(run(
            &mut executor,
            &catalog,
            "SELECT id, price FROM neighbor ORDER BY id"
        )),
        expected()
    );
    copy(&mut executor, &catalog, "items", "9,9.5\n");
    assert_eq!(
        rows(run(&mut executor, &catalog, "SELECT id, price FROM items")),
        vec![vec![SqlValue::Integer(9), SqlValue::Double(9.5)]]
    );
}

#[test]
fn columnar_truncate_rollback_restores_all_raw_entries() {
    let (store, catalog, mut executor, _, _) = setup();
    let before = snapshot(&store);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    {
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        for sql in ["TRUNCATE items", "SELECT id, price FROM items"] {
            let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
            let plan = Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap();
            let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
            if sql.starts_with("SELECT") {
                assert!(rows(result).is_empty());
            }
        }
    }
    txn.rollback_self().unwrap();
    assert_eq!(snapshot(&store), before);
    assert_eq!(
        rows(run(
            &mut executor,
            &catalog,
            "SELECT id, price FROM items ORDER BY id"
        )),
        expected()
    );
}
