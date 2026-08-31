use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

struct Harness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Harness {
    fn new() -> Self {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Self {
            executor: Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog)),
            catalog,
        }
    }

    fn query(&mut self, sql: &str) -> QueryResult {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL");
        statements
            .into_iter()
            .filter_map(|statement| {
                let plan = Planner::new(&*self.catalog.read().expect("catalog read"))
                    .plan(&statement)
                    .unwrap_or_else(|error| panic!("plan {statement:?}: {error:?}"));
                match self.executor.execute(plan).expect("execute SQL") {
                    ExecutionResult::Query(query) => Some(query),
                    _ => None,
                }
            })
            .last()
            .expect("query result")
    }
}

#[test]
fn date_time_interval_casts_are_typed_and_strict() {
    let mut harness = Harness::new();
    let query = harness.query(
        "SELECT CAST('2024-02-29' AS DATE), \
                CAST('23:59:59.123456' AS TIME), \
                INTERVAL '-1 month 2 days 03:04:05.000006'",
    );

    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Date(19_782),
            SqlValue::Time(86_399_123_456),
            SqlValue::Interval {
                months: -1,
                days: 2,
                micros: 11_045_000_006,
            },
        ]]
    );

    assert_eq!(
        harness.query("SELECT TRY_CAST('2023-02-29' AS DATE)").rows,
        vec![vec![SqlValue::Null]]
    );
    assert_eq!(
        harness
            .query("SELECT TRY_CAST('12:34:56.1234567' AS TIME)")
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn portable_temporal_functions_cover_calendar_boundaries() {
    let mut harness = Harness::new();
    let query = harness.query(
        "SELECT MAKE_DATE(2024, 2, 29), \
                MAKE_TIME(23, 59, 59.123456), \
                MAKE_TIMESTAMP(1970, 1, 1, 0, 0, 1.5), \
                MAKE_INTERVAL(0, 1, 0, -2, 3, 4, 5.000006), \
                DATE_ADD(DATE '2024-01-31', INTERVAL '1 month'), \
                DATE_SUB(DATE '2024-03-31', INTERVAL '1 month'), \
                TO_DATE('29/02/2024', 'DD/MM/YYYY'), \
                AGE(DATE '2024-03-01', DATE '2024-02-28'), \
                DATETIME(TIMESTAMP '2024-02-28 12:00:00', '+1 day'), \
                DATE(TIMESTAMP '1970-01-01 00:00:01.5'), \
                TIME(TIMESTAMP '1970-01-01 00:00:01.5'), \
                TIME '23:00:00' + INTERVAL '1 day 2 hours'",
    );

    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Date(19_782),
            SqlValue::Time(86_399_123_456),
            SqlValue::Timestamp(1_500_000),
            SqlValue::Interval {
                months: 1,
                days: -2,
                micros: 11_045_000_006,
            },
            SqlValue::Date(19_782),
            SqlValue::Date(19_782),
            SqlValue::Date(19_782),
            SqlValue::Interval {
                months: 0,
                days: 2,
                micros: 0,
            },
            SqlValue::Timestamp(1_709_208_000_000_000),
            SqlValue::Date(0),
            SqlValue::Time(1_500_000),
            SqlValue::Time(3_600_000_000),
        ]]
    );

    let current = harness.query("SELECT CURRENT_DATE, CURRENT_TIME");
    assert!(matches!(current.rows[0][0], SqlValue::Date(_)));
    assert!(matches!(current.rows[0][1], SqlValue::Time(_)));
    assert!(matches!(
        harness.query("SELECT AGE(DATE '2024-01-01')").rows[0][0],
        SqlValue::Interval { .. }
    ));
}

#[test]
fn temporal_storage_comparison_and_month_end_arithmetic_round_trip() {
    assert!(ResolvedType::Text.can_cast_to(&ResolvedType::Date));
    let mut harness = Harness::new();
    let query = harness.query(
        "CREATE TABLE events (d DATE, t TIME, gap INTERVAL); \
         INSERT INTO events VALUES ('2024-01-31', '12:30:00', INTERVAL '1 month'); \
         SELECT d, t, gap, d + gap, d < CAST('2024-02-01' AS DATE), \
                gap > INTERVAL '40 days' FROM events",
    );

    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Date(19_753),
            SqlValue::Time(45_000_000_000),
            SqlValue::Interval {
                months: 1,
                days: 0,
                micros: 0,
            },
            SqlValue::Date(19_782),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
        ]]
    );
}
