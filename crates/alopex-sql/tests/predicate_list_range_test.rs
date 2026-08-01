use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

#[test]
fn row_scan_evaluates_in_list_and_between_including_negated_forms() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "
        CREATE TABLE numbers (id INT PRIMARY KEY);
        INSERT INTO numbers (id) VALUES (1), (2), (3), (4);
        SELECT id FROM numbers WHERE id IN (1, 3) ORDER BY id;
        SELECT id FROM numbers WHERE id NOT IN (1, 3) ORDER BY id;
        SELECT id FROM numbers WHERE id BETWEEN 2 AND 3 ORDER BY id;
        SELECT id FROM numbers WHERE id NOT BETWEEN 2 AND 3 ORDER BY id;
        ",
    )
    .expect("parse predicates");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut queries = Vec::new();

    for statement in statements {
        let catalog = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*catalog)
            .plan(&statement)
            .expect("plan predicates");
        drop(catalog);
        if let ExecutionResult::Query(query) = executor.execute(plan).expect("execute predicates") {
            queries.push(query.rows);
        }
    }

    assert_eq!(
        queries,
        vec![
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]],
            vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(4)]],
            vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(3)]],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(4)]],
        ]
    );
}
