use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
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
            SqlValue::Integer(30),
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
