use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn last_query(sql: &str) -> alopex_sql::executor::QueryResult {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());

    let mut results = Vec::new();
    for statement in statements {
        let catalog = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*catalog).plan(&statement).expect("plan sql");
        drop(catalog);
        results.push(executor.execute(plan).expect("execute sql"));
    }
    results
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

#[test]
fn qualified_wildcards_expand_the_named_table_only() {
    let single = last_query(
        "
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
        SELECT users.* FROM users ORDER BY users.id;
        ",
    );
    assert_eq!(
        single.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
        ]
    );

    let joined = last_query(
        "
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y TEXT);
        INSERT INTO a (id, x) VALUES (1, 'left');
        INSERT INTO b (id, y) VALUES (1, 'right');
        SELECT a.*, b.y FROM a JOIN b ON a.id = b.id;
        ",
    );
    assert_eq!(
        joined.rows,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("left".into()),
            SqlValue::Text("right".into()),
        ]]
    );
}
