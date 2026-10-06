//! Exact v0.8.15 producer; observes constraint outcomes without editing KV bytes.
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_core::{KVStore, KVTransaction};
use alopex_sql::catalog::{Catalog, CatalogOverlay, PersistentCatalog, TxnCatalogView};
use alopex_sql::executor::{ConstraintViolation, ExecutionResult, Executor, ExecutorError};
use alopex_sql::planner::PlannerError;
use alopex_sql::storage::TxnBridge;
use alopex_sql::{AlopexDialect, Parser, Planner, SqlValue};
use std::fs::OpenOptions;
use std::sync::{Arc, RwLock};

#[path = "legacy_pk_cases.rs"]
mod cases;

fn execute(
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    sql: &str,
    mode: TxnMode,
) -> Result<ExecutionResult, ExecutorError> {
    let mut txn = store.begin(mode).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = TxnBridge::<MemoryKV>::wrap_external(&mut txn, mode, &mut overlay);
    let result = (|| {
        let mut result = None;
        for statement in Parser::parse_sql(&AlopexDialect, sql).unwrap() {
            let plan = {
                let guard = catalog.read().unwrap();
                let (_, overlay) = borrowed.split_parts();
                Planner::new(&TxnCatalogView::new(&*guard, overlay))
                    .plan(&statement)
                    .map_err(ExecutorError::from)?
            };
            result = Some(
                Executor::new(store.clone(), catalog.clone())
                    .execute_in_txn(plan, &mut borrowed)?,
            );
        }
        Ok(result.expect("nonempty SQL"))
    })();
    drop(borrowed);
    if result.is_ok() && mode == TxnMode::ReadWrite {
        txn.commit_self().unwrap();
        catalog.write().unwrap().apply_overlay(overlay);
    } else {
        txn.rollback_self().unwrap();
    }
    result
}

fn rows(result: ExecutionResult) -> Vec<Vec<Option<i32>>> {
    match result {
        ExecutionResult::Query(result) => result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        SqlValue::Integer(v) => Some(v),
                        SqlValue::Null => None,
                        other => panic!("unexpected SELECT value: {other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected SELECT: {other:?}"),
    }
}

fn snapshot(store: &MemoryKV) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut entries: Vec<_> = txn.scan_prefix(&[]).unwrap().collect();
    txn.rollback_self().unwrap();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: old-unique-producer PK_CASE NEW_OUTPUT.json"
    );
    let name = args[1].as_str();
    let sql = cases::setup(name);
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    execute(store.clone(), catalog.clone(), &sql, TxnMode::ReadWrite).unwrap();
    let before_rows = rows(
        execute(
            store.clone(),
            catalog.clone(),
            "SELECT id,value FROM items ORDER BY value",
            TxnMode::ReadOnly,
        )
        .unwrap(),
    );
    assert_eq!(before_rows, cases::rows(name, "valid"));
    let before = snapshot(&store);
    let (outcome, error) = match cases::attempt(name) {
        None => ("valid", None),
        Some(attempt) => match execute(store.clone(), catalog.clone(), attempt, TxnMode::ReadWrite)
        {
            Ok(_) => ("accepted", None),
            Err(error) => {
                // An unrelated parser/storage failure must not masquerade as constraint rejection.
                assert!(matches!(
                    &error,
                    ExecutorError::Planner(PlannerError::NullConstraintViolation { .. })
                        | ExecutorError::ConstraintViolation(
                            ConstraintViolation::NotNull { .. }
                                | ConstraintViolation::PrimaryKey { .. }
                                | ConstraintViolation::Unique { .. }
                        )
                ));
                assert_eq!(snapshot(&store), before, "rejected attempt must roll back");
                ("rejected", Some(format!("{error:?}")))
            }
        },
    };
    let observed_rows = rows(
        execute(
            store.clone(),
            catalog.clone(),
            "SELECT id,value FROM items ORDER BY value",
            TxnMode::ReadOnly,
        )
        .unwrap(),
    );
    assert_eq!(observed_rows, cases::rows(name, outcome));
    let neighbor = rows(
        execute(
            store.clone(),
            catalog.clone(),
            "SELECT id FROM neighbor ORDER BY id",
            TxnMode::ReadOnly,
        )
        .unwrap(),
    );
    assert_eq!(neighbor, vec![vec![Some(7)]]);
    let guard = catalog.read().unwrap();
    let table = guard.get_table("items").unwrap();
    let metadata = serde_json::json!({
        "primary_key": table.primary_key,
        "columns": table.columns.iter().map(|column| serde_json::json!({
            "name": column.name, "not_null": column.not_null, "primary_key": column.primary_key
        })).collect::<Vec<_>>(),
        "constraints": table.constraints,
        "indexes": guard.get_indexes_for_table("items").iter().map(|index| serde_json::json!({
            "name": index.name, "columns": index.columns, "unique": index.unique
        })).collect::<Vec<_>>()
    });
    drop(guard);
    let fixture = serde_json::json!({
        "schema": "alopex-sql-kv-fixture/v1",
        "source_commit": "d3917e1fa21d41097742a971e14094c0d6ae397e",
        "parser_contract": "0.25.0", "case": name, "sql": sql,
        "attempt_sql": cases::attempt(name), "outcome": outcome, "error": error,
        "before_rows": before_rows, "observed_rows": observed_rows,
        "observed_neighbor": neighbor, "observed_metadata": metadata,
        "entries": snapshot(&store).into_iter().map(|(k,v)| [hex::encode(k),hex::encode(v)]).collect::<Vec<_>>()
    });
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[2])
        .unwrap();
    serde_json::to_writer_pretty(file, &fixture).unwrap();
}
