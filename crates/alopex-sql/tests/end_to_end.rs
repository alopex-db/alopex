use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use std::sync::{Arc, RwLock};

#[test]
fn parser_planner_executor_pipeline() {
    let sql = r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            age INT
        );
        INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 20);
        UPDATE users SET age = 21 WHERE id = 2;
        DELETE FROM users WHERE id = 1;
        SELECT id, name FROM users WHERE age > 20 ORDER BY id DESC LIMIT 1;
    "#;

    // Parse statements
    let dialect = AlopexDialect::default();
    let statements = Parser::parse_sql(&dialect, sql).expect("parse sql");

    // Shared catalog for planning+execution
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));

    // Executor
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());

    let mut last_query = None;
    for stmt in statements {
        let guard = catalog.read().unwrap();
        let planner = Planner::new(&*guard);
        let plan = planner.plan(&stmt).expect("plan");
        drop(guard);

        match executor.execute(plan).expect("execute") {
            ExecutionResult::Query(q) => last_query = Some(q),
            _ => {}
        }
    }

    let query = last_query.expect("query result");
    assert_eq!(query.rows.len(), 1);
    assert_eq!(
        query.rows[0],
        vec![
            alopex_sql::storage::SqlValue::Integer(2),
            alopex_sql::storage::SqlValue::Text("bob".into())
        ]
    );
}
