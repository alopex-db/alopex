//! Standalone producer linked ONLY against the exact v0.8.15 core/SQL libraries.
//! This file is not an integration test module and never edits catalog bytes.
use std::fs::OpenOptions;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_core::{KVStore, KVTransaction};
use alopex_sql::catalog::persistent::{INDEXES_PREFIX, PersistedIndexMeta};
use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::storage::TxnBridge;
use alopex_sql::{AlopexDialect, Parser, Planner, SqlValue};

#[path = "legacy_unique_expected.rs"]
mod expected;

fn query(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    sql: &str,
) -> Vec<Vec<Option<i32>>> {
    let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);
    let result = Executor::new(store.clone(), catalog)
        .execute_in_txn(plan, &mut borrowed)
        .unwrap();
    drop(borrowed);
    txn.rollback_self().unwrap();
    match result {
        ExecutionResult::Query(result) => result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| match value {
                        SqlValue::Integer(value) => Some(value),
                        SqlValue::Null => None,
                        other => panic!("unexpected old SELECT value: {other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected old SELECT rows: {other:?}"),
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: old-unique-producer CASE NEW_OUTPUT.json"
    );
    let name = args[1].as_str();
    let (columns, rows, duplicate) = match name {
        "column" => ("a INTEGER UNIQUE, b INTEGER", "(1,10,20)", false),
        "named" => (
            "a INTEGER, b INTEGER, CONSTRAINT uq_a UNIQUE(a)",
            "(1,10,20)",
            false,
        ),
        "composite" => ("a INTEGER, b INTEGER, UNIQUE(a,b)", "(1,10,20)", false),
        "nullable" => (
            "a INTEGER, b INTEGER, UNIQUE(a,b)",
            "(1,NULL,20),(2,NULL,20),(3,10,NULL)",
            false,
        ),
        "duplicate" => ("a INTEGER UNIQUE, b INTEGER", "(1,10,20),(2,10,21)", true),
        "named_duplicate" => (
            "a INTEGER, b INTEGER, CONSTRAINT uq_a UNIQUE(a)",
            "(1,10,20),(2,10,21)",
            true,
        ),
        "composite_duplicate" => (
            "a INTEGER, b INTEGER, UNIQUE(a,b)",
            "(1,10,20),(2,10,20)",
            true,
        ),
        _ => panic!("unknown producer case"),
    };
    let sql = format!(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, {columns}); INSERT INTO items VALUES {rows}; CREATE TABLE neighbor (id INTEGER PRIMARY KEY); INSERT INTO neighbor VALUES (7);"
    );
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    let mut executor = Executor::new(store.clone(), catalog.clone());
    for statement in Parser::parse_sql(&AlopexDialect, &sql).unwrap() {
        let plan = {
            let guard = catalog.read().unwrap();
            let (_, overlay) = borrowed.split_parts();
            Planner::new(&TxnCatalogView::new(&*guard, overlay))
                .plan(&statement)
                .unwrap()
        };
        executor
            .execute_in_txn(plan, &mut borrowed)
            .expect("old SQL producer must execute without raw injection");
    }
    drop(borrowed);
    txn.commit_self().unwrap();
    catalog.write().unwrap().apply_overlay(overlay);
    let observed_rows = query(
        store.clone(),
        catalog.clone(),
        "SELECT id,a,b FROM items ORDER BY id",
    );
    assert_eq!(observed_rows, expected::rows(name));
    let observed_neighbor = query(
        store.clone(),
        catalog.clone(),
        "SELECT id FROM neighbor ORDER BY id",
    );
    assert_eq!(observed_neighbor, vec![vec![Some(7)]]);
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut entries: Vec<_> = txn.scan_prefix(&[]).unwrap().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let indexes: Vec<_> = txn
        .scan_prefix(INDEXES_PREFIX)
        .unwrap()
        .map(|(_, bytes)| {
            bincode::deserialize::<PersistedIndexMeta>(&bytes)
                .unwrap()
                .name
        })
        .collect();
    assert!(
        indexes
            .iter()
            .all(|name| !name.starts_with("__uq_") && name != "uq_a"),
        "old producer unexpectedly created UNIQUE index metadata"
    );
    txn.rollback_self().unwrap();
    let fixture = serde_json::json!({
        "schema": "alopex-sql-kv-fixture/v1",
        "source_commit": "d3917e1fa21d41097742a971e14094c0d6ae397e",
        "parser_contract": "0.25.0",
        "case": name, "sql": sql, "duplicate": duplicate,
        "indexes": indexes,
        "observed_rows": observed_rows, "observed_neighbor": observed_neighbor,
        "entries": entries.into_iter().map(|(key, value)| [hex::encode(key), hex::encode(value)]).collect::<Vec<_>>()
    });
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[2])
        .unwrap();
    serde_json::to_writer_pretty(file, &fixture).unwrap();
}
