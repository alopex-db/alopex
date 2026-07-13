//! Streaming-path subquery execution tests (GitHub issues #23 / #24).
//!
//! The CLI and async storage layers execute SELECT via the streaming pipeline
//! (`execute_query_streaming` / `build_streaming_pipeline`), which historically
//! could not evaluate subqueries even though the non-streaming path
//! (`execute_query`, covered by `subquery_exec_test.rs`) could.
//!
//! These tests pin the streaming path to the same results as the
//! non-streaming path, using the same data set as `subquery_exec_test.rs`.

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::execute_query_streaming;
use alopex_sql::executor::{Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlValue, TxnBridge};

fn setup() -> (Arc<MemoryKV>, Arc<RwLock<MemoryCatalog>>) {
    let dialect = AlopexDialect;
    let setup_sql = r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20);
    "#;
    let statements = Parser::parse_sql(&dialect, setup_sql).expect("parse setup sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store.clone(), catalog.clone());
    for stmt in statements {
        let guard = catalog.read().unwrap();
        let plan = Planner::new(&*guard).plan(&stmt).expect("plan setup stmt");
        drop(guard);
        executor.execute(plan).expect("execute setup stmt");
    }
    (store, catalog)
}

/// Execute a single SELECT through the streaming pipeline and materialize
/// columns + rows. Errors from `next_row` are propagated, never swallowed.
fn run_streaming(
    store: &Arc<MemoryKV>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    select: &str,
) -> Result<(Vec<String>, Vec<Vec<SqlValue>>), ExecutorError> {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, select).expect("parse select");
    assert_eq!(stmts.len(), 1, "expected single SELECT statement");
    let guard = catalog.read().unwrap();
    let plan = Planner::new(&*guard).plan(&stmts[0]).expect("plan select");
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_read().expect("begin read txn");
    let mut iter = execute_query_streaming(&mut txn, &*guard, plan)?;
    let columns: Vec<String> = iter.columns().iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();
    while let Some(row) = iter.next_row()? {
        rows.push(row);
    }
    Ok((columns, rows))
}

#[test]
fn streaming_where_in_subquery_executes() {
    let (store, catalog) = setup();
    let (columns, rows) = run_streaming(
        &store,
        &catalog,
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id",
    )
    .expect("streaming IN subquery must execute");
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn streaming_scalar_subquery_in_select_list_executes() {
    let (store, catalog) = setup();
    let (columns, rows) = run_streaming(
        &store,
        &catalog,
        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id",
    )
    .expect("streaming scalar subquery must execute");
    assert_eq!(columns, vec!["name".to_string(), "order_count".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );
}

#[test]
fn streaming_correlated_exists_subquery_executes() {
    let (store, catalog) = setup();
    let (columns, rows) = run_streaming(
        &store,
        &catalog,
        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id",
    )
    .expect("streaming EXISTS subquery must execute");
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

/// Execute a single SELECT through the non-streaming path (`execute_query`)
/// for parity comparison.
fn run_non_streaming(
    store: &Arc<MemoryKV>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    select: &str,
) -> Result<Vec<Vec<SqlValue>>, ExecutorError> {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, select).expect("parse select");
    assert_eq!(stmts.len(), 1, "expected single SELECT statement");
    let guard = catalog.read().unwrap();
    let plan = Planner::new(&*guard).plan(&stmts[0]).expect("plan select");
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_read().expect("begin read txn");
    match alopex_sql::executor::query::execute_query(&mut txn, &*guard, plan)? {
        alopex_sql::executor::ExecutionResult::Query(q) => Ok(q.rows),
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn streaming_not_in_subquery_executes() {
    let (store, catalog) = setup();
    // {1, 1, 2} contains no NULL: NOT IN behaves two-valued and returns carol.
    let (_, rows) = run_streaming(
        &store,
        &catalog,
        "SELECT users.name FROM users WHERE users.id NOT IN (SELECT orders.user_id FROM orders) ORDER BY users.id",
    )
    .expect("streaming NOT IN subquery must execute");
    assert_eq!(rows, vec![vec![SqlValue::Text("carol".into())]]);
}

#[test]
fn streaming_not_in_subquery_with_null_returns_empty() {
    let (store, catalog) = setup();
    {
        // Add an order whose user_id is NULL: the subquery result becomes
        // {1, 1, 2, NULL}.
        let dialect = AlopexDialect;
        let stmts = Parser::parse_sql(
            &dialect,
            "INSERT INTO orders (id, user_id, total) VALUES (13, NULL, 5)",
        )
        .expect("parse insert");
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let guard = catalog.read().unwrap();
        let plan = Planner::new(&*guard).plan(&stmts[0]).expect("plan insert");
        drop(guard);
        executor.execute(plan).expect("insert NULL user_id");
    }

    // Three-valued logic: `id NOT IN (..., NULL)` is FALSE for matching ids
    // and UNKNOWN otherwise, so no row may be returned.
    let sql = "SELECT users.name FROM users WHERE users.id NOT IN (SELECT orders.user_id FROM orders) ORDER BY users.id";
    let (_, streaming_rows) =
        run_streaming(&store, &catalog, sql).expect("streaming NOT IN with NULL must execute");
    assert!(
        streaming_rows.is_empty(),
        "NOT IN over a NULL-containing subquery result must return no rows, got {streaming_rows:?}"
    );

    // Parity: the non-streaming path must agree.
    let non_streaming_rows =
        run_non_streaming(&store, &catalog, sql).expect("non-streaming NOT IN with NULL");
    assert_eq!(streaming_rows, non_streaming_rows);
}

#[test]
fn streaming_quantified_any_subquery_executes() {
    let (store, catalog) = setup();
    let (_, rows) = run_streaming(
        &store,
        &catalog,
        "SELECT users.name FROM users WHERE users.id = ANY (SELECT orders.user_id FROM orders) ORDER BY users.id",
    )
    .expect("streaming ANY subquery must execute");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}
