use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{
    EvaluationError, ExecutionResult, Executor, ExecutorError, QueryResult,
};
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
fn analysis_date_differences_are_numeric_and_interval_units_are_sql_standard() {
    let mut harness = Harness::new();
    let query = harness.query(
        "SELECT TIMESTAMP '2017-01-01 00:00:00' + INTERVAL '5' DAY, \
                DATE_DIFF('day', TIMESTAMP '2017-01-01', TIMESTAMP '2017-01-09'), \
                EXTRACT(EPOCH FROM TIMESTAMP '2017-01-09' - TIMESTAMP '2017-01-01')",
    );

    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Timestamp(1_483_660_800_000_000),
            SqlValue::BigInt(8),
            SqlValue::Double(691_200.0),
        ]]
    );
}

#[test]
fn analysis_date_differences_preserve_sign_day_boundaries_and_nulls() {
    let mut harness = Harness::new();
    let query = harness.query(
        "SELECT DATE_DIFF('day', TIMESTAMP '2017-01-09', TIMESTAMP '2017-01-01'),
                DATE_DIFF('day', TIMESTAMP '1969-12-31 23:59:59', TIMESTAMP '1970-01-01'),
                DATE_DIFF('day', TIMESTAMP '2017-01-01', CAST(NULL AS TIMESTAMP)),
                EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01' - TIMESTAMP '1970-01-01 00:00:00.5'),
                TIMESTAMP '1970-01-02' + INTERVAL '-1' DAY",
    );
    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::BigInt(-8),
            SqlValue::BigInt(1),
            SqlValue::Null,
            SqlValue::Double(-0.5),
            SqlValue::Timestamp(0),
        ]]
    );
}

#[test]
fn interval_epoch_positive_year_uses_year_days() {
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query("SELECT EXTRACT(EPOCH FROM INTERVAL '1' YEAR)")
            .rows,
        vec![vec![SqlValue::Double(31_557_600.0)]]
    );
}

// DuckDB v1.4.0 date_diff.cpp: TIMESTAMP specializations count floor-boundaries,
// rather than dividing an elapsed duration; microseconds use checked subtraction.
fn assert_fixed_date_diff(unit: &str, start: &str, end: &str, same_bucket_end: &str) {
    let mut harness = Harness::new();
    let before_epoch = "1969-12-31 23:59:59.999999";
    let epoch = "1970-01-01 00:00:00";
    for (label, part, left, right, expected) in [
        ("boundary", unit.to_owned(), start, end, 1),
        ("reverse", unit.to_owned(), end, start, -1),
        (
            "negative epoch floor",
            unit.to_owned(),
            before_epoch,
            epoch,
            1,
        ),
        (
            "negative epoch reverse",
            unit.to_owned(),
            epoch,
            before_epoch,
            -1,
        ),
        ("same bucket", unit.to_owned(), epoch, same_bucket_end, 0),
        (
            "uppercase plural",
            format!("{}S", unit.to_ascii_uppercase()),
            start,
            end,
            1,
        ),
    ] {
        let sql = format!("SELECT DATE_DIFF('{part}', TIMESTAMP '{left}', TIMESTAMP '{right}')");
        assert_eq!(
            harness.query(&sql).rows,
            vec![vec![SqlValue::BigInt(expected)]],
            "{unit}: {label}: {sql}"
        );
    }
    let sql = format!(
        "SELECT DATE_DIFF('{unit}', CAST(NULL AS TIMESTAMP), TIMESTAMP '{end}'), \
                DATE_DIFF('{unit}', TIMESTAMP '{start}', CAST(NULL AS TIMESTAMP))"
    );
    assert_eq!(
        harness.query(&sql).rows,
        vec![vec![SqlValue::Null, SqlValue::Null]],
        "{unit}: typed NULL"
    );
}

#[test]
fn date_diff_fixed_hour_boundaries() {
    assert_fixed_date_diff(
        "hour",
        "1970-01-01 00:59:59.999999",
        "1970-01-01 01:00:00",
        "1970-01-01 00:00:00.000001",
    );
    // The answer fits even though subtracting raw microseconds first would overflow.
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query(
                "SELECT DATE_DIFF('hour', \
                CAST(CAST('-9000000000000000000' AS BIGINT) AS TIMESTAMP), \
                CAST(CAST('9000000000000000000' AS BIGINT) AS TIMESTAMP))"
            )
            .rows,
        vec![vec![SqlValue::BigInt(5_000_000_000)]]
    );
}

#[test]
fn date_diff_fixed_minute_boundaries() {
    assert_fixed_date_diff(
        "minute",
        "1970-01-01 00:00:59.999999",
        "1970-01-01 00:01:00",
        "1970-01-01 00:00:00.000001",
    );
}

#[test]
fn date_diff_fixed_second_boundaries() {
    assert_fixed_date_diff(
        "second",
        "1970-01-01 00:00:00.999999",
        "1970-01-01 00:00:01",
        "1970-01-01 00:00:00.000001",
    );
}

#[test]
fn date_diff_fixed_millisecond_boundaries() {
    assert_fixed_date_diff(
        "millisecond",
        "1970-01-01 00:00:00.000999",
        "1970-01-01 00:00:00.001000",
        "1970-01-01 00:00:00.000001",
    );
}

#[test]
fn date_diff_fixed_microsecond_boundaries() {
    assert_fixed_date_diff(
        "microsecond",
        "1970-01-01 00:00:00.000000",
        "1970-01-01 00:00:00.000001",
        "1970-01-01 00:00:00",
    );
}

fn assert_microsecond_overflow(start: &str, end: &str) {
    let mut harness = Harness::new();
    // These are finite timestamps, not the i64 extrema used for infinity by DuckDB.
    let sql = format!(
        "SELECT DATE_DIFF('microsecond', \
            CAST(CAST('{start}' AS BIGINT) AS TIMESTAMP), \
            CAST(CAST('{end}' AS BIGINT) AS TIMESTAMP))"
    );
    let statement = Parser::parse_sql(&AlopexDialect, &sql)
        .expect("parse SQL")
        .pop()
        .expect("one statement");
    let plan = Planner::new(&*harness.catalog.read().expect("catalog read"))
        .plan(&statement)
        .expect("plan SQL");
    let error = harness
        .executor
        .execute(plan)
        .expect_err("microsecond overflow");
    assert!(
        matches!(error, ExecutorError::Evaluation(EvaluationError::Overflow)),
        "expected numeric overflow, got {error:?}"
    );
}

#[test]
fn date_diff_fixed_microsecond_positive_overflow() {
    assert_microsecond_overflow("-9000000000000000000", "9000000000000000000");
}

#[test]
fn date_diff_fixed_microsecond_negative_overflow() {
    assert_microsecond_overflow("9000000000000000000", "-9000000000000000000");
}

#[test]
fn interval_epoch_negative_year_preserves_sign() {
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query("SELECT EXTRACT(EPOCH FROM INTERVAL '-1' YEAR)")
            .rows,
        vec![vec![SqlValue::Double(-31_557_600.0)]]
    );
}

#[test]
fn interval_epoch_positive_thirteen_months_splits_year_and_month() {
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query("SELECT EXTRACT(EPOCH FROM INTERVAL '13' MONTH)")
            .rows,
        vec![vec![SqlValue::Double(34_149_600.0)]]
    );
}

#[test]
fn interval_epoch_negative_thirteen_months_truncates_toward_zero() {
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query("SELECT EXTRACT(EPOCH FROM INTERVAL '-13' MONTH)")
            .rows,
        vec![vec![SqlValue::Double(-34_149_600.0)]]
    );
}

#[test]
fn interval_epoch_single_month_keeps_thirty_days() {
    let mut harness = Harness::new();
    assert_eq!(
        harness
            .query("SELECT EXTRACT(EPOCH FROM INTERVAL '1' MONTH)")
            .rows,
        vec![vec![SqlValue::Double(2_592_000.0)]]
    );
}

#[test]
fn interval_epoch_mixed_sign_preserves_fraction_and_null() {
    let mut harness = Harness::new();
    let query = harness.query(
        "SELECT EXTRACT(EPOCH FROM INTERVAL '-1 month 2 days 03:04:05.000006'), \
                EXTRACT(EPOCH FROM CAST(NULL AS INTERVAL))",
    );
    assert_eq!(query.rows.len(), 1);
    assert_eq!(query.rows[0].len(), 2);
    let SqlValue::Double(actual) = query.rows[0][0] else {
        panic!("expected Double epoch, got {:?}", query.rows[0][0]);
    };
    // REL_18_0 interval_part_common: years use 365.25 days, remaining months 30.
    // Allow only floating-point rounding, not loss of the microsecond fraction.
    assert!((actual - (-2_408_154.999_994)).abs() <= 1e-9, "{actual}");
    assert_eq!(query.rows[0][1], SqlValue::Null);
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
