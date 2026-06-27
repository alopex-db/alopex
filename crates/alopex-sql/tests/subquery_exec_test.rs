use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let dialect = AlopexDialect;
    let statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut results = Vec::new();
    for stmt in statements {
        let guard = catalog.read().unwrap();
        let plan = Planner::new(&*guard).plan(&stmt)?;
        drop(guard);
        results.push(executor.execute(plan)?);
    }
    Ok(results)
}

fn last_query(sql: &str) -> alopex_sql::executor::QueryResult {
    execute_sql(sql)
        .expect("execute sql")
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

fn setup_sql(select: &str) -> String {
    format!(
        r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20);
        {select};
        "#
    )
}

#[test]
fn scalar_and_correlated_exists_subqueries_execute() {
    let query = last_query(&setup_sql(
        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id",
    ));
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );

    let exists = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id",
    ));
    assert_eq!(
        exists.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn in_any_all_and_derived_subqueries_execute() {
    let in_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(
        in_query.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );

    let any_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id = ANY (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(any_query.rows, in_query.rows);

    let all_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id < ALL (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert!(all_query.rows.is_empty());

    let derived = last_query(&setup_sql(
        "SELECT active_users.name FROM (SELECT users.id, users.name FROM users WHERE users.id < 3) AS active_users ORDER BY active_users.id",
    ));
    assert_eq!(
        derived.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn scalar_subquery_rejects_multiple_rows() {
    let err = execute_sql(&setup_sql(
        "SELECT (SELECT orders.total FROM orders) AS total FROM users",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("multiple rows"));
}
