use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Vec<ExecutionResult> {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog, sql).expect("execute SQL")
}

fn execute_sql_on(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<Vec<ExecutionResult>, alopex_sql::executor::ExecutorError> {
    Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse SQL")
        .into_iter()
        .map(|statement| {
            let plan = Planner::new(&*catalog.read().expect("catalog read"))
                .plan(&statement)
                .expect("plan SQL");
            executor.execute(plan)
        })
        .collect()
}

#[test]
fn multi_row_upsert_uses_excluded_values() {
    let results = execute_sql(
        "
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);
        INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0])
          ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;
        SELECT id, embedding FROM items ORDER BY id;
        ",
    );

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![1.0, 0.0])],
            vec![SqlValue::Integer(2), SqlValue::Vector(vec![0.0, 1.0])],
        ]
    );
}

#[test]
fn duplicate_ids_in_one_upsert_are_rejected_atomically() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));\
         INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);",
    )
    .expect("seed SQL");

    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0]), (1, [0.0, 1.0])\
         ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;",
        )
        .is_err()
    );

    let results = execute_sql_on(&mut executor, &catalog, "SELECT id, embedding FROM items;")
        .expect("verify SQL");
    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Vector(vec![0.0, 0.0]),]]
    );
}

#[test]
fn unique_index_enforces_conflicts_and_allows_multiple_nulls() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "
        CREATE TABLE users (id INT PRIMARY KEY, code TEXT);
        INSERT INTO users (id, code) VALUES (1, 'a'), (2, NULL);
        CREATE UNIQUE INDEX users_code_key ON users (code);
        INSERT INTO users (id, code) VALUES (3, NULL);
        ",
    )
    .expect("create unique index");

    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "INSERT INTO users (id, code) VALUES (4, 'a');",
        )
        .is_err()
    );
    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "UPDATE users SET code = 'a' WHERE id = 2;",
        )
        .is_err()
    );

    let results = execute_sql_on(
        &mut executor,
        &catalog,
        "
        INSERT INTO users (id, code) VALUES (5, 'a') ON CONFLICT (code) DO NOTHING;
        SELECT id, code FROM users ORDER BY id;
        ",
    )
    .expect("execute ON CONFLICT");
    assert_eq!(results[0], ExecutionResult::RowsAffected(0));
    let ExecutionResult::Query(query) = &results[1] else {
        panic!("expected users query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Null],
            vec![SqlValue::Integer(3), SqlValue::Null],
        ]
    );
}
