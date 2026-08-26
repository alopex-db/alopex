use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

struct Harness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Harness {
    fn seeded() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, Arc::clone(&catalog));
        let mut harness = Self { executor, catalog };
        harness.run(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, m INTEGER, \
             label TEXT, k INTEGER, bits INTEGER, flag BOOLEAN); \
             INSERT INTO t VALUES \
             (1, 1, 2, 1, 'a', 2, 7, TRUE), \
             (2, 2, 4, 2, 'b', 1, 3, TRUE), \
             (3, 3, 5, 2, 'c', 1, 1, FALSE), \
             (4, 4, 8, 4, 'd', 3, NULL, NULL)",
        );
        harness
    }

    fn run(&mut self, sql: &str) -> Option<QueryResult> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
        let mut last = None;
        for statement in statements {
            let plan = Planner::new(&*self.catalog.read().expect("catalog"))
                .plan(&statement)
                .expect("plan SQL");
            if let ExecutionResult::Query(result) =
                self.executor.execute(plan).expect("execute SQL")
            {
                last = Some(result);
            }
        }
        last
    }

    fn query(&mut self, sql: &str) -> QueryResult {
        self.run(sql).expect("query result")
    }
}

fn double(value: &SqlValue) -> f64 {
    match value {
        SqlValue::Double(value) => *value,
        other => panic!("expected DOUBLE, got {other:?}"),
    }
}

fn assert_close(actual: &SqlValue, expected: f64) {
    let error = (double(actual) - expected).abs();
    assert!(error <= 1e-12, "{actual:?} != {expected} (error {error})");
}

#[test]
fn statistics_percentiles_and_regression_share_the_aggregate_surface() {
    let row = Harness::seeded()
        .query(
            "SELECT VARIANCE(x), VAR_SAMP(x), VAR_POP(x), \
             STDDEV(x), STDDEV_SAMP(x), STDDEV_POP(x), \
             COVAR_SAMP(y, x), COVAR_POP(y, x), CORR(y, x), \
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x), \
             PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x), \
             MEDIAN(x), MODE(m), QUANTILE_CONT(x, 0.25), \
             REGR_COUNT(y, x), REGR_AVGX(y, x), REGR_AVGY(y, x), \
             REGR_SXX(y, x), REGR_SYY(y, x), REGR_SXY(y, x), \
             REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x), \
             PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY x DESC) FROM t",
        )
        .rows
        .remove(0);

    assert_close(&row[0], 5.0 / 3.0);
    assert_close(&row[1], 5.0 / 3.0);
    assert_close(&row[2], 1.25);
    assert_close(&row[3], (5.0_f64 / 3.0).sqrt());
    assert_close(&row[4], (5.0_f64 / 3.0).sqrt());
    assert_close(&row[5], 1.25_f64.sqrt());
    assert_close(&row[6], 9.5 / 3.0);
    assert_close(&row[7], 9.5 / 4.0);
    assert_close(&row[8], 9.5 / (5.0_f64 * 18.75).sqrt());
    assert_close(&row[9], 2.5);
    assert_eq!(row[10], SqlValue::Integer(2));
    assert_close(&row[11], 2.5);
    assert_eq!(row[12], SqlValue::Integer(2));
    assert_close(&row[13], 1.75);
    assert_eq!(row[14], SqlValue::BigInt(4));
    assert_close(&row[15], 2.5);
    assert_close(&row[16], 4.75);
    assert_close(&row[17], 5.0);
    assert_close(&row[18], 18.75);
    assert_close(&row[19], 9.5);
    assert_close(&row[20], 1.9);
    assert_close(&row[21], 0.0);
    assert_close(&row[22], 90.25 / 93.75);
    assert_close(&row[23], 3.25);
}

#[test]
fn value_bitwise_and_boolean_aggregates_define_null_and_tie_behavior() {
    let rows = Harness::seeded()
        .query(
            "SELECT ANY_VALUE(label), FIRST(label ORDER BY k, id), \
             LAST(label ORDER BY k, id), ARG_MIN(label, k), MIN_BY(label, k), \
             ARG_MAX(label, k), MAX_BY(label, k), BIT_AND(bits), BIT_OR(bits), \
             BIT_XOR(bits), BOOL_AND(flag), BOOL_OR(flag) FROM t",
        )
        .rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::Text("a".into()),
            SqlValue::Text("b".into()),
            SqlValue::Text("d".into()),
            SqlValue::Text("b".into()),
            SqlValue::Text("b".into()),
            SqlValue::Text("d".into()),
            SqlValue::Text("d".into()),
            SqlValue::Integer(1),
            SqlValue::Integer(7),
            SqlValue::Integer(5),
            SqlValue::Boolean(false),
            SqlValue::Boolean(true),
        ]]
    );

    let empty = Harness::seeded()
        .query(
            "SELECT VAR_SAMP(x), VAR_POP(x), REGR_COUNT(y, x), BIT_AND(bits), \
             BOOL_AND(flag), ANY_VALUE(label) FROM t WHERE FALSE",
        )
        .rows;
    assert_eq!(
        empty,
        vec![vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::BigInt(0),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );
}

#[test]
fn new_aggregates_execute_as_windows() {
    let rows = Harness::seeded()
        .query(
            "SELECT id, VAR_POP(x) OVER (), REGR_SLOPE(y, x) OVER (), \
             MEDIAN(x) OVER (), BIT_OR(bits) OVER (), BOOL_AND(flag) OVER (), \
             FIRST(label) OVER (ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING \
               AND UNBOUNDED FOLLOWING), \
             LAST(label) OVER (ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING \
               AND UNBOUNDED FOLLOWING) FROM t ORDER BY id",
        )
        .rows;

    assert_eq!(rows.len(), 4);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row[0], SqlValue::Integer((index + 1) as i32));
        assert_close(&row[1], 1.25);
        assert_close(&row[2], 1.9);
        assert_close(&row[3], 2.5);
        assert_eq!(row[4], SqlValue::Integer(7));
        assert_eq!(row[5], SqlValue::Boolean(false));
        assert_eq!(row[6], SqlValue::Text("b".into()));
        assert_eq!(row[7], SqlValue::Text("d".into()));
    }
}
