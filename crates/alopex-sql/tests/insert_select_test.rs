use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());

    statements
        .iter()
        .map(|statement| {
            let catalog = catalog.read().expect("catalog lock");
            let plan = Planner::new(&*catalog).plan(statement)?;
            drop(catalog);
            executor.execute(plan)
        })
        .collect()
}

#[test]
fn insert_select_inserts_rows_with_and_without_explicit_columns() {
    let results = execute_sql(
        "
        CREATE TABLE t (id INT, name TEXT);
        INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob');
        INSERT INTO t SELECT id, name FROM t;
        SELECT id, name FROM t ORDER BY id;
        ",
    )
    .expect("INSERT INTO ... SELECT executes");

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
        ]
    );

    let explicit_columns = execute_sql(
        "
        CREATE TABLE t (id INT, name TEXT);
        INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob');
        INSERT INTO t (name, id) SELECT name, id FROM t;
        SELECT id, name FROM t ORDER BY id;
        ",
    )
    .expect("INSERT INTO columns ... SELECT executes");

    let ExecutionResult::Query(query) = explicit_columns.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
        ]
    );
}
