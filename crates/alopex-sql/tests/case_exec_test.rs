use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute(sql: &str) -> Vec<ExecutionResult> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse CASE SQL");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, Arc::clone(&catalog));
    statements
        .into_iter()
        .map(|statement| {
            let plan = Planner::new(&*catalog.read().expect("catalog read"))
                .plan(&statement)
                .expect("plan CASE SQL");
            executor.execute(plan).expect("execute CASE SQL")
        })
        .collect()
}

fn query(sql: &str) -> QueryResult {
    match execute(sql)
        .into_iter()
        .rev()
        .find(|result| matches!(result, ExecutionResult::Query(_)))
        .expect("query result")
    {
        ExecutionResult::Query(query) => query,
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn case_executes_lazily_with_simple_and_searched_semantics() {
    let searched = query("SELECT CASE WHEN TRUE THEN 7 ELSE 1 / 0 END");
    assert_eq!(searched.rows, vec![vec![SqlValue::Integer(7)]]);

    let simple = query("SELECT CASE 2 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END");
    assert_eq!(simple.rows, vec![vec![SqlValue::Integer(20)]]);

    let else_branch = query("SELECT CASE WHEN FALSE THEN 1 / 0 ELSE 9 END");
    assert_eq!(else_branch.rows, vec![vec![SqlValue::Integer(9)]]);

    let aggregate = query(
        "CREATE TABLE scores (value INT); \
         INSERT INTO scores VALUES (1), (2); \
         SELECT CASE WHEN COUNT(*) > 1 THEN 10 ELSE 20 END FROM scores",
    );
    assert_eq!(aggregate.rows, vec![vec![SqlValue::Integer(10)]]);

    let having = query(
        "CREATE TABLE checks (value INT); \
         INSERT INTO checks VALUES (1), (2); \
         SELECT COUNT(*) FROM checks \
         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END",
    );
    assert_eq!(having.rows, vec![vec![SqlValue::BigInt(2)]]);

    let dml = query(
        "CREATE TABLE items (id INT PRIMARY KEY, value INT); \
         INSERT INTO items (id, value) VALUES (1, CASE WHEN TRUE THEN 10 ELSE 0 END); \
         UPDATE items SET value = CASE value WHEN 10 THEN 20 ELSE 0 END; \
         SELECT value FROM items",
    );
    assert_eq!(dml.rows, vec![vec![SqlValue::Integer(20)]]);

    let ordered = query(
        "CREATE TABLE rankings (id INT PRIMARY KEY); \
         INSERT INTO rankings VALUES (1), (2); \
         SELECT id FROM rankings ORDER BY CASE id WHEN 1 THEN 2 ELSE 1 END",
    );
    assert_eq!(
        ordered.rows,
        vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(1)]]
    );

    let subquery = query(
        "CREATE TABLE values_for_case (value INT); \
         INSERT INTO values_for_case VALUES (1), (2); \
         SELECT CASE WHEN FALSE THEN (SELECT value FROM values_for_case) ELSE 9 END",
    );
    assert_eq!(subquery.rows, vec![vec![SqlValue::Integer(9)]]);
}
