use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{DecimalValue, RowCodec, SqlValue};

fn decimal(coefficient: i128, scale: u8) -> SqlValue {
    SqlValue::Decimal(DecimalValue::new(coefficient, scale))
}

fn run(sql: &str) -> Result<Option<QueryResult>, String> {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, Arc::clone(&catalog));
    let mut last = None;
    for statement in Parser::parse_sql(&AlopexDialect, sql).map_err(|error| error.to_string())? {
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .map_err(|error| error.to_string())?;
        if let ExecutionResult::Query(result) =
            executor.execute(plan).map_err(|error| error.to_string())?
        {
            last = Some(result);
        }
    }
    Ok(last)
}

#[test]
fn decimal_ddl_cast_rounding_and_storage_are_exact() {
    let result = run(
        "CREATE TABLE amounts (id INTEGER PRIMARY KEY, amount DECIMAL(10,2));
         INSERT INTO amounts VALUES (1, DECIMAL '12.345'), (2, CAST('-0.005' AS NUMERIC(10,2)));
         SELECT amount FROM amounts ORDER BY id",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![decimal(1235, 2)], vec![decimal(-1, 2)]]
    );

    let encoded = RowCodec::encode(&result.rows[0]);
    assert_eq!(RowCodec::decode(&encoded).unwrap(), result.rows[0]);
}

#[test]
fn decimal_arithmetic_and_comparison_preserve_scale() {
    let result = run("SELECT DECIMAL '1.20' + DECIMAL '2.3' AS added,
                DECIMAL '1.20' * DECIMAL '2.3' AS multiplied,
                DECIMAL '1.20' = DECIMAL '1.2' AS equal_value")
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            decimal(350, 2),
            decimal(2760, 3),
            SqlValue::Boolean(true)
        ]]
    );
}

#[test]
fn decimal_aggregates_remain_exact() {
    let result = run(
        "SELECT SUM(v) AS total, AVG(v) AS average, MIN(v) AS minimum, MAX(v) AS maximum
         FROM (VALUES (DECIMAL '1.00'), (DECIMAL '2.00')) AS t(v)",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            decimal(300, 2),
            decimal(1500000, 6),
            decimal(100, 2),
            decimal(200, 2),
        ]]
    );
}

#[test]
fn decimal_precision_overflow_is_rejected() {
    let error = run("SELECT CAST('1000' AS DECIMAL(3,0))").unwrap_err();
    assert!(
        error.contains("precision overflow"),
        "unexpected error: {error}"
    );
}

#[test]
fn decimal_cast_unary_division_and_wide_comparison_are_exact() {
    let result = run("SELECT -DECIMAL '1.20' AS negative,
                CAST(DECIMAL '12.99' AS INTEGER) AS integral,
                CAST(DECIMAL '1.25' AS TEXT) AS rendered,
                DECIMAL '1' / DECIMAL '8' AS quotient,
                DECIMAL '99999999999999999999999999999999999999' >
                    DECIMAL '0.0000000000000000000000000000000000001' AS ordered")
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            decimal(-120, 2),
            SqlValue::Integer(12),
            SqlValue::Text("1.25".into()),
            decimal(125000, 6),
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn decimal_literal_accepts_the_maximum_scale() {
    let result = run("SELECT DECIMAL '0.00000000000000000000000000000000000001'")
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![decimal(1, 38)]]);
}
