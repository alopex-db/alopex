use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn run_sql(
    sql: &str,
) -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    let dialect = AlopexDialect::default();
    let stmts = Parser::parse_sql(&dialect, sql).expect("parse sql");
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().unwrap();
            Planner::new(&*guard).plan(&stmt).expect("plan")
        };
        let _ = executor.execute(plan).expect("execute");
    }
    (executor, catalog)
}

#[test]
fn knn_optimization_without_index() {
    let sql = r#"
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        INSERT INTO items (id, embedding) VALUES
            (1, [0.0, 0.0]),
            (2, [1.0, 0.0]),
            (3, [2.0, 0.0]);
    "#;
    let (mut executor, catalog) = run_sql(sql);

    let query =
        "SELECT id FROM items ORDER BY vector_similarity(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2";
    let stmt = Parser::parse_sql(&AlopexDialect::default(), query)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    match executor.execute(plan).unwrap() {
        ExecutionResult::Query(q) => {
            let mut ids: Vec<i32> = q
                .rows
                .iter()
                .map(|r| match &r[0] {
                    alopex_sql::storage::SqlValue::Integer(v) => *v,
                    other => panic!("unexpected {other:?}"),
                })
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2]);
        }
        other => panic!("unexpected {other:?}"),
    }
}
