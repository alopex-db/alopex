use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

fn execute_sql(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Vec<ExecutionResult> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
    statements
        .into_iter()
        .map(|statement| {
            let plan = {
                let catalog = catalog.read().expect("catalog read");
                Planner::new(&*catalog).plan(&statement).expect("plan SQL")
            };
            executor.execute(plan).expect("execute SQL")
        })
        .collect()
}

fn query_sql(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> QueryResult {
    execute_sql(executor, catalog, sql)
        .into_iter()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

fn setup() -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    (executor, catalog)
}

fn unix_epoch_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_micros()
        .try_into()
        .expect("current microseconds fit i64")
}

#[test]
fn select_now_returns_timestamp_and_is_fixed_within_one_statement() {
    let (mut executor, catalog) = setup();

    let query = query_sql(&mut executor, &catalog, "SELECT NOW(), NOW()");

    assert_eq!(
        query
            .columns
            .iter()
            .map(|column| &column.data_type)
            .collect::<Vec<_>>(),
        vec![&ResolvedType::Timestamp, &ResolvedType::Timestamp]
    );
    assert_eq!(query.rows.len(), 1);
    assert!(matches!(query.rows[0][0], SqlValue::Timestamp(_)));
    assert_eq!(query.rows[0][0], query.rows[0][1]);
}

#[test]
fn select_now_is_fixed_across_multiple_rows_in_one_statement() {
    let (mut executor, catalog) = setup();
    execute_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE source (id INTEGER); INSERT INTO source VALUES (1), (2), (3);",
    );

    let query = query_sql(
        &mut executor,
        &catalog,
        "SELECT NOW() FROM source ORDER BY id",
    );

    assert_eq!(query.rows.len(), 3);
    let first = query.rows[0][0].clone();
    assert!(matches!(first, SqlValue::Timestamp(_)));
    assert!(query.rows.iter().all(|row| row[0] == first));
}

#[test]
fn insert_uses_timestamp_default_now() {
    let (mut executor, catalog) = setup();
    execute_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE w (id INTEGER, created_at TIMESTAMP DEFAULT NOW());",
    );
    let before_insert = unix_epoch_micros();
    execute_sql(&mut executor, &catalog, "INSERT INTO w (id) VALUES (1);");
    let after_insert = unix_epoch_micros();

    let query = query_sql(&mut executor, &catalog, "SELECT created_at FROM w");

    assert_eq!(query.columns[0].data_type, ResolvedType::Timestamp);
    let SqlValue::Timestamp(value) = query.rows[0][0] else {
        panic!("DEFAULT NOW() must store a timestamp");
    };
    assert!((before_insert..=after_insert).contains(&value));
}
