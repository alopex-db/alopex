use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::StatementKind;
use alopex_sql::ast::ddl::DataType;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

struct SqlHarness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl SqlHarness {
    fn new() -> Self {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Self {
            executor: Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog)),
            catalog,
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Vec<ExecutionResult> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
        statements
            .into_iter()
            .map(|statement| {
                let plan = {
                    let catalog = self.catalog.read().expect("catalog read");
                    Planner::new(&*catalog).plan(&statement).expect("plan SQL")
                };
                self.executor.execute(plan).expect("execute SQL")
            })
            .collect()
    }

    fn query_sql(&mut self, sql: &str) -> QueryResult {
        self.execute_sql(sql)
            .into_iter()
            .rev()
            .find_map(|result| match result {
                ExecutionResult::Query(query) => Some(query),
                _ => None,
            })
            .expect("query result")
    }
}

fn parsed_column_type(sql: &str) -> DataType {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse CREATE TABLE");
    let StatementKind::CreateTable(table) = &statements[0].kind else {
        panic!("expected CREATE TABLE");
    };
    table.columns[0].data_type.clone()
}

#[test]
fn real_aliases_float_and_pg_typeof_reports_real() {
    assert!(matches!(
        parsed_column_type("CREATE TABLE t (x REAL)"),
        DataType::Float
    ));
    assert!(matches!(
        parsed_column_type("CREATE TABLE t (x FLOAT)"),
        DataType::Float
    ));

    let mut harness = SqlHarness::new();
    harness.execute_sql("CREATE TABLE values_real (x REAL); INSERT INTO values_real VALUES (1.5);");
    assert_eq!(
        harness
            .query_sql("SELECT pg_typeof(x) FROM values_real")
            .rows,
        vec![vec![SqlValue::Text("real".into())]]
    );
}

#[test]
fn double_integer_arithmetic_promotes_and_preserves_existing_numeric_queries() {
    let mut harness = SqlHarness::new();
    harness.execute_sql("CREATE TABLE t (v DOUBLE, i INTEGER); INSERT INTO t VALUES (1.5, 10);");

    assert_eq!(
        harness.query_sql("SELECT v * 2 FROM t").rows,
        vec![vec![SqlValue::Double(3.0)]]
    );
    assert_eq!(
        harness.query_sql("SELECT SUM(v * i) FROM t").rows,
        vec![vec![SqlValue::Double(15.0)]]
    );
    assert_eq!(
        harness.query_sql("SELECT SUM(v * 2) FROM t").rows,
        vec![vec![SqlValue::Double(3.0)]]
    );

    assert_eq!(
        harness.query_sql("SELECT v * 2.0 FROM t").rows,
        vec![vec![SqlValue::Double(3.0)]]
    );
    assert_eq!(
        harness.query_sql("SELECT i * 2 FROM t").rows,
        vec![vec![SqlValue::Integer(20)]]
    );
    assert_eq!(
        harness.query_sql("SELECT SUM(v + v) FROM t").rows,
        vec![vec![SqlValue::Double(3.0)]]
    );
    assert!(
        harness
            .query_sql("SELECT v FROM t WHERE v > 2")
            .rows
            .is_empty()
    );
}

#[test]
fn sum_integer_preserves_integer_while_total_and_avg_remain_floating_point() {
    let mut harness = SqlHarness::new();
    harness.execute_sql("CREATE TABLE n (i INTEGER); INSERT INTO n VALUES (10), (20);");

    assert_eq!(
        harness
            .query_sql("SELECT SUM(i), TOTAL(i), AVG(i), MIN(i), MAX(i), COUNT(i) FROM n")
            .rows,
        vec![vec![
            // SUM widens INTEGER to BIGINT so the accumulator cannot overflow.
            SqlValue::BigInt(30),
            SqlValue::Double(30.0),
            SqlValue::Double(15.0),
            SqlValue::Integer(10),
            SqlValue::Integer(20),
            SqlValue::BigInt(2),
        ]]
    );
}

#[test]
fn timestamp_columns_accept_canonical_text_and_epoch_micros() {
    assert!(ResolvedType::Text.can_cast_to(&ResolvedType::Timestamp));
    assert!(ResolvedType::Integer.can_cast_to(&ResolvedType::Timestamp));

    let mut harness = SqlHarness::new();
    harness.execute_sql("CREATE TABLE ts (x TIMESTAMP);");
    harness.execute_sql(
        "INSERT INTO ts VALUES \
         ('2025-01-15 10:30:00'), \
         ('2025-01-15 10:30:00.123456'), \
         (0), \
         (0.0), \
         (1736937000000000);",
    );

    assert_eq!(
        harness.query_sql("SELECT x FROM ts").rows,
        vec![
            vec![SqlValue::Timestamp(1_736_937_000_000_000)],
            vec![SqlValue::Timestamp(1_736_937_000_123_456)],
            vec![SqlValue::Timestamp(0)],
            vec![SqlValue::Timestamp(0)],
            vec![SqlValue::Timestamp(1_736_937_000_000_000)],
        ]
    );
}

/// SUM over INTEGER must accumulate in a wider type. PostgreSQL sums int4 into
/// int8 and DuckDB widens to hugeint precisely because a 32-bit accumulator
/// overflows on ordinary data: two rows near i32::MAX are enough.
#[test]
fn sum_over_integer_accumulates_without_overflowing_the_column_type() {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE wide (i INTEGER); INSERT INTO wide VALUES (2000000000), (2000000000);",
    );

    assert_eq!(
        harness.query_sql("SELECT SUM(i) FROM wide").rows,
        vec![vec![SqlValue::BigInt(4_000_000_000)]]
    );
}

/// FLOAT and INTEGER mix in arithmetic. A 32-bit float cannot represent the whole
/// i32 range, so the result promotes to DOUBLE rather than losing magnitude.
#[test]
fn integer_and_float_arithmetic_promotes_to_double() {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE mixed (i INTEGER, f FLOAT); INSERT INTO mixed VALUES (2000000001, 1.5);",
    );

    assert_eq!(
        harness.query_sql("SELECT i * f FROM mixed").rows,
        vec![vec![SqlValue::Double(3_000_000_001.5)]]
    );
}

#[test]
fn dml_normalizes_values_to_the_declared_numeric_storage_type() {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE normalized (f FLOAT, d DOUBLE, b BIGINT); \
         INSERT INTO normalized VALUES (1.5, 2, 3);",
    );

    assert_eq!(
        harness.query_sql("SELECT f, d, b FROM normalized").rows,
        vec![vec![
            SqlValue::Float(1.5),
            SqlValue::Double(2.0),
            SqlValue::BigInt(3),
        ]]
    );

    harness.execute_sql("UPDATE normalized SET f = 2.5, d = 4, b = 5;");
    assert_eq!(
        harness.query_sql("SELECT f, d, b FROM normalized").rows,
        vec![vec![
            SqlValue::Float(2.5),
            SqlValue::Double(4.0),
            SqlValue::BigInt(5),
        ]]
    );
}

#[test]
fn insert_select_normalizes_values_to_the_target_numeric_storage_type() {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE source_values (v DOUBLE); \
         CREATE TABLE target_values (v FLOAT); \
         INSERT INTO source_values VALUES (1.5), (2.5); \
         INSERT INTO target_values SELECT v FROM source_values;",
    );

    assert_eq!(
        harness
            .query_sql("SELECT v FROM target_values ORDER BY v")
            .rows,
        vec![vec![SqlValue::Float(1.5)], vec![SqlValue::Float(2.5)]]
    );
}

#[test]
fn assignment_normalization_preserves_compatible_vector_values() {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE vector_values (id INTEGER PRIMARY KEY, v VECTOR(2, L2)); \
         INSERT INTO vector_values VALUES (1, [1.0, 0.0]); \
         UPDATE vector_values SET v = [0.0, 1.0] WHERE id = 1;",
    );

    assert_eq!(
        harness.query_sql("SELECT v FROM vector_values").rows,
        vec![vec![SqlValue::Vector(vec![0.0, 1.0])]]
    );
}
