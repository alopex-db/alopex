use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{Planner, PlannerError};
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut results = Vec::new();

    for statement in statements {
        let guard = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*guard).plan(&statement)?;
        drop(guard);
        results.push(executor.execute(plan)?);
    }

    Ok(results)
}

fn last_query(sql: &str) -> QueryResult {
    execute_sql(sql)
        .expect("execute SQL")
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

#[test]
fn single_cte_executes() {
    let query = last_query(
        "CREATE TABLE t (id INT);\
         INSERT INTO t VALUES (1), (2);\
         WITH c AS (SELECT id FROM t) SELECT id FROM c ORDER BY id;",
    );

    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
}

#[test]
fn multiple_ctes_execute_in_one_from_clause() {
    let query = last_query(
        "CREATE TABLE t (id INT);\
         INSERT INTO t VALUES (1), (2), (3);\
         WITH a AS (SELECT id FROM t WHERE id <= 2),\
              b AS (SELECT id FROM t WHERE id >= 2)\
         SELECT a.id, b.id FROM a, b ORDER BY a.id, b.id;",
    );

    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
            vec![SqlValue::Integer(1), SqlValue::Integer(3)],
            vec![SqlValue::Integer(2), SqlValue::Integer(2)],
            vec![SqlValue::Integer(2), SqlValue::Integer(3)],
        ]
    );
}

#[test]
fn cte_can_filter_and_aggregate() {
    let query = last_query(
        "CREATE TABLE t (id INT, category TEXT);\
         INSERT INTO t VALUES (1, 'skip'), (2, 'x'), (3, 'x'), (4, 'y');\
         WITH summary AS (\
             SELECT category, COUNT(*) AS item_count \
             FROM t WHERE id >= 2 GROUP BY category\
         )\
         SELECT category, item_count FROM summary ORDER BY category;",
    );

    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("x".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("y".into()), SqlValue::BigInt(1)],
        ]
    );
}

#[test]
fn cte_can_be_used_in_a_join() {
    let query = last_query(
        "CREATE TABLE users (id INT, name TEXT);\
         CREATE TABLE orders (user_id INT, total INT);\
         INSERT INTO users VALUES (1, 'alice'), (2, 'bob');\
         INSERT INTO orders VALUES (1, 40), (1, 75), (2, 20);\
         WITH large_orders AS (SELECT user_id, total FROM orders WHERE total >= 50)\
         SELECT users.name, large_orders.total \
         FROM users JOIN large_orders ON users.id = large_orders.user_id;",
    );

    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Text("alice".into()), SqlValue::Integer(75),]]
    );
}

#[test]
fn cte_name_shadows_a_base_table() {
    let query = last_query(
        "CREATE TABLE c (id INT);\
         CREATE TABLE source (id INT);\
         INSERT INTO c VALUES (99);\
         INSERT INTO source VALUES (1);\
         WITH c AS (SELECT id FROM source) SELECT id FROM c;",
    );

    assert_eq!(query.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn with_recursive_is_explicitly_unsupported() {
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "WITH RECURSIVE c AS (SELECT 1 AS id) SELECT id FROM c",
    )
    .expect("WITH RECURSIVE should parse so the planner can reject it")
    .remove(0);
    let catalog = MemoryCatalog::new();

    let error = Planner::new(&catalog)
        .plan(&statement)
        .expect_err("recursive CTE must be rejected");

    assert!(
        matches!(
            &error,
            PlannerError::UnsupportedFeature { feature, .. }
                if feature.contains("recursive common table expression")
        ),
        "expected an explicit unsupported_feature error, got: {error}"
    );
}

#[test]
fn undefined_cte_reference_is_an_error() {
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "WITH defined AS (SELECT 1 AS id) SELECT id FROM missing",
    )
    .expect("parse CTE query")
    .remove(0);
    let catalog = MemoryCatalog::new();

    let error = Planner::new(&catalog)
        .plan(&statement)
        .expect_err("an undefined CTE reference must fail");

    assert!(
        matches!(
            &error,
            PlannerError::TableNotFound { name, .. } if name == "missing"
        ),
        "expected the missing CTE name in the error, got: {error}"
    );
}
