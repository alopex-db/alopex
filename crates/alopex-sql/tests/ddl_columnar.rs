use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn plan_and_exec(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    let config = ExecutionConfig::default();
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).expect("parse sql");
    let mut results = Vec::new();
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().unwrap();
            Planner::new(&*guard).plan(&stmt).expect("plan")
        };
        results.push(executor.execute(plan, &config)?);
    }
    Ok(results)
}

#[test]
fn create_table_with_columnar_options() {
    let sql = r#"
        CREATE TABLE docs (
            id INT PRIMARY KEY,
            name TEXT
        ) WITH (storage='columnar', compression='lz4', row_group_size=5000, rowid_mode='direct');
    "#;
    let res = plan_and_exec(sql).expect("execute ddl");
    assert_eq!(res.len(), 1);
}

#[test]
fn create_table_with_invalid_option_fails() {
    let sql = r#"
        CREATE TABLE bad (
            id INT
        ) WITH (storage='columnar', unknown_option='x');
    "#;
    let err = plan_and_exec(sql).unwrap_err();
    assert!(matches!(err, ExecutorError::UnknownTableOption(_)));
}
