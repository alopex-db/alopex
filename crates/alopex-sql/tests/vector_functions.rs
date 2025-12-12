use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn run_sql(sql: &str) -> Vec<ExecutionResult> {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    let dialect = AlopexDialect::default();
    let stmts = Parser::parse_sql(&dialect, sql).expect("parse sql");
    let mut results = Vec::new();
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().unwrap();
            Planner::new(&*guard).plan(&stmt).expect("plan")
        };
        results.push(executor.execute(plan).expect("execute"));
    }
    results
}

#[test]
fn vector_similarity_and_distance_end_to_end() {
    let sql = r#"
        CREATE TABLE dummy (id INT);
        INSERT INTO dummy (id) VALUES (1);
        SELECT vector_similarity([1.0, 0.0], [0.0, 1.0], 'cosine') AS cos_sim FROM dummy;
        SELECT vector_distance([1.0, 0.0], [2.0, 0.0], 'l2') AS l2_dist FROM dummy;
        SELECT vector_similarity([1.0, 0.0], [2.0, 0.0], 'inner') AS inner_prod FROM dummy;
    "#;
    let results = run_sql(sql);
    match &results[2] {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows[0][0], alopex_sql::storage::SqlValue::Double(0.0));
        }
        other => panic!("unexpected {other:?}"),
    }
    match &results[3] {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows[0][0], alopex_sql::storage::SqlValue::Double(1.0));
        }
        other => panic!("unexpected {other:?}"),
    }
    match &results[4] {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows[0][0], alopex_sql::storage::SqlValue::Double(2.0));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn vector_function_invalid_args_return_error() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    let dialect = AlopexDialect::default();

    // prepare table
    let create = Parser::parse_sql(&dialect, "CREATE TABLE dummy (id INT);")
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&create).unwrap()
    };
    executor.execute(plan).unwrap();

    // invalid vector length
    let bad = Parser::parse_sql(
        &dialect,
        "SELECT vector_similarity([1.0], [2.0, 3.0], 'l2') FROM dummy;",
    )
    .unwrap()
    .pop()
    .unwrap();
    let _err = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&bad).unwrap_err()
    };
}
