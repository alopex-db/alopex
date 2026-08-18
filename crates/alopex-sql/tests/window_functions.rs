//! Window functions / `OVER` clause (issues #128 and #141, v0.8.x).
//!
//! Implemented scope:
//!   * `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()`
//!   * positional `LAG()` / `LEAD()` with optional offset and default
//!   * aggregate `OVER`: `SUM` / `COUNT` / `AVG` / `MIN` / `MAX`
//!   * `PARTITION BY`
//!   * window-local `ORDER BY`
//!   * implicit frames: bare `OVER ()` spans the whole partition, while an
//!     `ORDER BY` inside `OVER` makes the frame cumulative.
//!
//!   * explicit `ROWS` physical-row frames and `RANGE` value/peer frames

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;
use std::sync::{Arc, RwLock};

/// Fixture table `sales`:
///
/// | id | region | amount | qty | bonus |
/// |----|--------|--------|-----|-------|
/// |  1 | east   |  100.0 |   3 |  10.0 |
/// |  2 | east   |  200.0 |   1 |  NULL |
/// |  3 | west   |  150.0 |   5 |  20.0 |
/// |  4 | west   |  150.0 |   2 |  NULL |
/// |  5 | north  |   50.0 |   0 |   5.0 |
///
/// The shape of this data is load-bearing:
///   * ids 3 and 4 share `amount = 150.0`. Without that tie, `RANK` and
///     `DENSE_RANK` produce identical output and an implementation that
///     aliases one to the other would go undetected.
///   * ids 2 and 4 have a NULL `bonus`, so window aggregates must be shown to
///     skip NULLs rather than treat them as 0 or propagate them.
///   * `qty = 0` on id 5 is a boundary value.
///
/// `REAL` is not accepted by the parser's type vocabulary at this commit (the
/// FFI AST contract lists `Float`/`Double` but no `Real`), so the float columns
/// are declared `FLOAT`.
const FIXTURE: &str = r#"
    CREATE TABLE sales (
      id INTEGER PRIMARY KEY,
      region TEXT,
      amount FLOAT,
      qty INTEGER,
      bonus FLOAT
    );
    INSERT INTO sales VALUES (1, 'east',  100.0, 3, 10.0);
    INSERT INTO sales VALUES (2, 'east',  200.0, 1, NULL);
    INSERT INTO sales VALUES (3, 'west',  150.0, 5, 20.0);
    INSERT INTO sales VALUES (4, 'west',  150.0, 2, NULL);
    INSERT INTO sales VALUES (5, 'north',  50.0, 0, 5.0);
"#;

struct Harness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Harness {
    fn new() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, Arc::clone(&catalog));
        let mut harness = Self { executor, catalog };
        harness.run_ok(FIXTURE);
        harness
    }

    /// Run one or more statements, panicking on any parse/plan/exec failure.
    fn run_ok(&mut self, sql: &str) -> Option<QueryResult> {
        match self.run(sql) {
            Ok(result) => result,
            Err(err) => panic!("expected `{}` to succeed, got: {err}", sql.trim()),
        }
    }

    fn run(&mut self, sql: &str) -> Result<Option<QueryResult>, String> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(|e| format!("parse: {e}"))?;
        let mut last = None;
        for stmt in statements {
            let plan = {
                let guard = self.catalog.read().unwrap();
                Planner::new(&*guard)
                    .plan(&stmt)
                    .map_err(|e| format!("{e}"))?
            };
            if let ExecutionResult::Query(q) =
                self.executor.execute(plan).map_err(|e| format!("{e}"))?
            {
                last = Some(q);
            }
        }
        Ok(last)
    }

    /// Run a query expected to fail, returning the rendered error string.
    fn run_err(&mut self, sql: &str) -> String {
        match self.run(sql) {
            Err(err) => err,
            Ok(_) => panic!("expected `{}` to fail, but it succeeded", sql.trim()),
        }
    }
}

fn query(harness: &mut Harness, sql: &str) -> QueryResult {
    harness
        .run_ok(sql)
        .unwrap_or_else(|| panic!("`{}` produced no query result", sql.trim()))
}

fn column_names(result: &QueryResult) -> Vec<String> {
    result.columns.iter().map(|c| c.name.clone()).collect()
}

/// Extract an integer column as `i64`.
///
/// `INTEGER` columns arrive as `SqlValue::Integer(i32)` while `COUNT` /
/// `ROW_NUMBER` / `RANK` widen to `SqlValue::BigInt(i64)`; both are accepted so
/// the assertions compare values rather than storage widths.
fn int_column(result: &QueryResult, index: usize) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match &row[index] {
            SqlValue::Integer(v) => i64::from(*v),
            SqlValue::BigInt(v) => *v,
            other => panic!("expected an integer at column {index}, got {other:?}"),
        })
        .collect()
}

/// Extract a floating-point column as `f64`.
///
/// `FLOAT` columns arrive as `SqlValue::Float(f32)`; `SUM`/`AVG` may widen to
/// `SqlValue::Double(f64)`. Integers are accepted too so that an implementation
/// returning an exact integral value is not failed on its storage width alone.
fn float_column(result: &QueryResult, index: usize) -> Vec<f64> {
    result
        .rows
        .iter()
        .map(|row| match &row[index] {
            SqlValue::Float(v) => f64::from(*v),
            SqlValue::Double(v) => *v,
            SqlValue::Integer(v) => f64::from(*v),
            SqlValue::BigInt(v) => *v as f64,
            other => panic!("expected a float at column {index}, got {other:?}"),
        })
        .collect()
}

fn optional_float_column(result: &QueryResult, index: usize) -> Vec<Option<f64>> {
    result
        .rows
        .iter()
        .map(|row| match &row[index] {
            SqlValue::Null => None,
            SqlValue::Float(v) => Some(f64::from(*v)),
            SqlValue::Double(v) => Some(*v),
            SqlValue::Integer(v) => Some(f64::from(*v)),
            SqlValue::BigInt(v) => Some(*v as f64),
            other => panic!("expected a float or NULL at column {index}, got {other:?}"),
        })
        .collect()
}

fn text_column(result: &QueryResult, index: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row[index] {
            SqlValue::Text(v) => v.clone(),
            other => panic!("expected text at column {index}, got {other:?}"),
        })
        .collect()
}

/// Compare float vectors with a tolerance, reporting the whole vector on
/// mismatch so a RED run shows what the implementation actually produced.
#[track_caller]
fn assert_floats_eq(actual: &[f64], expected: &[f64]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "row count mismatch: got {actual:?}, expected {expected:?}"
    );
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            (a - e).abs() < 1e-6,
            "value at row {i} differs: got {a}, expected {e} (full: {actual:?} vs {expected:?})"
        );
    }
}

// ---------------------------------------------------------------------------
// Aggregate OVER: row preservation and partitioning
// ---------------------------------------------------------------------------

/// A bare `OVER ()` aggregates the entire input yet must preserve every input
/// row. If the implementation collapses the query into a GROUP BY-style
/// aggregation, the result would be a single row and the row-count assertion
/// below catches it.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn empty_over_aggregates_all_rows_without_collapsing_them() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(amount) OVER () AS grand FROM sales ORDER BY id",
    );

    assert_eq!(
        column_names(&result),
        vec!["id".to_string(), "grand".to_string()]
    );
    // The row count is the whole point of this test.
    assert_eq!(
        result.rows.len(),
        5,
        "OVER () must not collapse rows; got {} row(s)",
        result.rows.len()
    );
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    // 100 + 200 + 150 + 150 + 50 = 650, repeated on every row.
    assert_floats_eq(&float_column(&result, 1), &[650.0; 5]);
}

/// `PARTITION BY` must split the input into independent windows while still
/// emitting one output row per input row.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn partition_by_scopes_the_aggregate_to_each_partition() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, region, SUM(amount) OVER (PARTITION BY region) AS rt \
         FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_eq!(
        text_column(&result, 1),
        vec!["east", "east", "west", "west", "north"]
    );
    // east: 100 + 200 = 300; west: 150 + 150 = 300; north: 50.
    assert_floats_eq(
        &float_column(&result, 2),
        &[300.0, 300.0, 300.0, 300.0, 50.0],
    );
}

/// An `ORDER BY` inside `OVER` changes the implicit frame from "whole
/// partition" to "cumulative up to the current row". An implementation that
/// ignores the frame distinction and always aggregates the full partition would
/// return 650.0 on every row instead of the running total.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_inside_over_produces_a_running_total() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(amount) OVER (ORDER BY id) AS running FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_floats_eq(
        &float_column(&result, 1),
        &[100.0, 300.0, 450.0, 600.0, 650.0],
    );
}

/// SQL's implicit frame for an ordered aggregate window is
/// `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Rows sharing the same
/// complete sort key are peers, so every aggregate must observe the whole peer
/// group rather than the executor's incidental row-by-row traversal order.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn ordered_window_aggregates_include_the_complete_peer_group() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, \
                SUM(amount) OVER (ORDER BY amount) AS s, \
                COUNT(amount) OVER (ORDER BY amount) AS c, \
                AVG(amount) OVER (ORDER BY amount) AS a, \
                MIN(amount) OVER (ORDER BY amount) AS lo, \
                MAX(amount) OVER (ORDER BY amount) AS hi \
         FROM sales ORDER BY id",
    );

    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_floats_eq(
        &float_column(&result, 1),
        &[150.0, 650.0, 450.0, 450.0, 50.0],
    );
    assert_eq!(int_column(&result, 2), vec![2, 5, 4, 4, 1]);
    assert_floats_eq(
        &float_column(&result, 3),
        &[75.0, 130.0, 112.5, 112.5, 50.0],
    );
    assert_floats_eq(&float_column(&result, 4), &[50.0, 50.0, 50.0, 50.0, 50.0]);
    assert_floats_eq(
        &float_column(&result, 5),
        &[100.0, 200.0, 150.0, 150.0, 50.0],
    );
}

/// Peer grouping follows the complete window sort key and is independent of
/// sort direction. Adding `id` breaks the amount tie, while descending amount
/// alone keeps the tied rows in one implicit RANGE frame.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn implicit_range_uses_direction_and_the_complete_sort_key() {
    let mut h = Harness::new();
    let descending = query(
        &mut h,
        "SELECT id, SUM(amount) OVER (ORDER BY amount DESC) AS s \
         FROM sales ORDER BY id",
    );
    assert_floats_eq(
        &float_column(&descending, 1),
        &[600.0, 200.0, 500.0, 500.0, 650.0],
    );

    let tie_broken = query(
        &mut h,
        "SELECT id, SUM(amount) OVER (ORDER BY amount, id) AS s \
         FROM sales ORDER BY id",
    );
    assert_floats_eq(
        &float_column(&tie_broken, 1),
        &[150.0, 650.0, 300.0, 450.0, 50.0],
    );
}

/// NULL ordering changes where a peer group appears, not whether NULL sort
/// keys are peers. The two NULL `bonus` rows therefore receive the same count
/// with either explicit placement.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn implicit_range_groups_null_sort_keys_for_both_null_placements() {
    let mut h = Harness::new();
    let nulls_first = query(
        &mut h,
        "SELECT id, COUNT(*) OVER (ORDER BY bonus NULLS FIRST) AS c \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&nulls_first, 1), vec![4, 2, 5, 2, 3]);

    let nulls_last = query(
        &mut h,
        "SELECT id, COUNT(*) OVER (ORDER BY bonus NULLS LAST) AS c \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&nulls_last, 1), vec![2, 5, 3, 5, 1]);
}

// ---------------------------------------------------------------------------
// Ranking functions
// ---------------------------------------------------------------------------

/// `ROW_NUMBER()` numbers rows within its own partition using the window's own
/// `ORDER BY`, independently of the outer `ORDER BY`. The tie on
/// `amount = 150.0` (ids 3 and 4) is broken deterministically by adding `id` as
/// a secondary window sort key, so the expected values are unambiguous.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn row_number_uses_window_local_ordering_independent_of_outer_order_by() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) AS rn \
         FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    // east ordered by amount DESC: id2 (200) => 1, id1 (100) => 2.
    // west ordered by amount DESC then id: id3 => 1, id4 => 2.
    // north has a single row: id5 => 1.
    assert_eq!(int_column(&result, 1), vec![2, 1, 1, 2, 1]);
}

/// `RANK` leaves a gap after a tie while `DENSE_RANK` does not. Evaluating both
/// in one query over the tied `amount = 150.0` pair pins down the difference:
/// an implementation that maps one function onto the other cannot satisfy both
/// assertions at once.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn rank_leaves_gaps_after_ties_while_dense_rank_does_not() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, RANK() OVER (ORDER BY amount) AS r, \
                DENSE_RANK() OVER (ORDER BY amount) AS dr \
         FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    // amounts ascending: 50 (id5), 100 (id1), 150 (id3), 150 (id4), 200 (id2).
    // RANK:       id5=1, id1=2, id3=3, id4=3, id2=5  <- 4 is skipped
    // DENSE_RANK: id5=1, id1=2, id3=3, id4=3, id2=4  <- no gap
    assert_eq!(int_column(&result, 1), vec![2, 5, 3, 3, 1]);
    assert_eq!(int_column(&result, 2), vec![2, 4, 3, 3, 1]);
}

// ---------------------------------------------------------------------------
// Multiple windows and NULL handling
// ---------------------------------------------------------------------------

/// Two window aggregates over the same window specification must coexist in one
/// SELECT list and be evaluated independently of each other.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn multiple_window_functions_coexist_in_one_select() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, COUNT(*) OVER (PARTITION BY region) AS c, \
                AVG(amount) OVER (PARTITION BY region) AS a \
         FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    // east 2 rows, west 2 rows, north 1 row.
    assert_eq!(int_column(&result, 1), vec![2, 2, 2, 2, 1]);
    // east avg = (100+200)/2 = 150; west avg = (150+150)/2 = 150; north = 50.
    assert_floats_eq(
        &float_column(&result, 2),
        &[150.0, 150.0, 150.0, 150.0, 50.0],
    );
}

/// Window aggregates must skip NULL inputs, matching plain aggregate semantics.
/// `bonus` is NULL for ids 2 and 4, so each partition sums only its non-NULL
/// bonus. Treating NULL as 0 would coincidentally give the same sums here, but
/// propagating NULL (yielding NULL for east and west) would not — the float
/// extraction below panics on `SqlValue::Null`, which is the failure we want.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn window_aggregate_skips_null_inputs() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(bonus) OVER (PARTITION BY region) AS b FROM sales ORDER BY id",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    // east: only id1 contributes 10.0; west: only id3 contributes 20.0;
    // north: id5 contributes 5.0.
    assert_floats_eq(&float_column(&result, 1), &[10.0, 10.0, 20.0, 20.0, 5.0]);
}

// ---------------------------------------------------------------------------
// Interaction with projection alias scope (issue #122)
// ---------------------------------------------------------------------------

/// The alias bound to a window function must be resolvable from the outer
/// `ORDER BY`, exactly as #122 established for ordinary and aggregate
/// projections. This is the crossing point between the two features: window
/// output is produced after projection, so the alias scope built by #122 must
/// also cover window expressions.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn outer_order_by_resolves_window_function_alias() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn \
         FROM sales ORDER BY region, rn",
    );

    assert_eq!(
        column_names(&result),
        vec!["region".to_string(), "rn".to_string()]
    );
    assert_eq!(result.rows.len(), 5);
    // Regions ascending: east, east, north, west, west; `rn` ascending inside
    // each region.
    assert_eq!(
        text_column(&result, 0),
        vec!["east", "east", "north", "west", "west"]
    );
    assert_eq!(int_column(&result, 1), vec![1, 2, 1, 1, 2]);
}

// ---------------------------------------------------------------------------
// Positional value functions
// ---------------------------------------------------------------------------

/// The one-argument forms default to offset 1 and NULL outside the partition.
/// LEAD deliberately reads beyond the aggregate window's implicit
/// `RANGE ... CURRENT ROW` frame: positional value functions address the whole
/// partition and are not constrained by aggregate frame semantics.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_default_to_one_row_and_null() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, LAG(amount) OVER (ORDER BY id) AS previous, \
                LEAD(amount) OVER (ORDER BY id) AS following \
         FROM sales ORDER BY id",
    );

    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_eq!(
        optional_float_column(&result, 1),
        vec![None, Some(100.0), Some(200.0), Some(150.0), Some(150.0)]
    );
    assert_eq!(
        optional_float_column(&result, 2),
        vec![Some(200.0), Some(150.0), Some(150.0), Some(50.0), None]
    );
}

/// Explicit offsets and defaults are evaluated relative to the current row.
/// Numeric defaults are coerced to a common result type with the value.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_support_offsets_and_current_row_defaults() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, LAG(amount, 2, -1) OVER (ORDER BY id) AS previous, \
                LEAD(amount, 99, qty) OVER (ORDER BY id) AS following, \
                LAG(amount, 0) OVER (ORDER BY id) AS current_value, \
                LAG(id, qty, -1) OVER (ORDER BY id) AS dynamic_offset, \
                amount - LAG(amount, 1, amount) OVER (ORDER BY id) AS delta \
         FROM sales ORDER BY id",
    );

    assert_floats_eq(
        &float_column(&result, 1),
        &[-1.0, -1.0, 100.0, 200.0, 150.0],
    );
    assert_floats_eq(&float_column(&result, 2), &[3.0, 1.0, 5.0, 2.0, 0.0]);
    assert_floats_eq(
        &float_column(&result, 3),
        &[100.0, 200.0, 150.0, 150.0, 50.0],
    );
    assert_eq!(int_column(&result, 4), vec![-1, 1, -1, 2, 5]);
    assert_floats_eq(&float_column(&result, 5), &[0.0, 100.0, -50.0, 0.0, -100.0]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_do_not_cross_partition_boundaries() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, LAG(amount, 1, -1) OVER (PARTITION BY region ORDER BY id) AS previous, \
                LEAD(amount, 1, -1) OVER (PARTITION BY region ORDER BY id) AS following \
         FROM sales ORDER BY id",
    );

    assert_floats_eq(&float_column(&result, 1), &[-1.0, 100.0, -1.0, 150.0, -1.0]);
    assert_floats_eq(&float_column(&result, 2), &[200.0, -1.0, 150.0, -1.0, -1.0]);
}

/// NULL values are respected rather than skipped. The default is used only
/// when the requested position is outside the partition.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_preserve_null_values() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, LAG(bonus, 1, -1) OVER (ORDER BY id) AS previous, \
                LEAD(bonus, 1, -1) OVER (ORDER BY id) AS following \
         FROM sales ORDER BY id",
    );

    assert_eq!(
        optional_float_column(&result, 1),
        vec![Some(-1.0), Some(10.0), None, Some(20.0), None]
    );
    assert_eq!(
        optional_float_column(&result, 2),
        vec![None, Some(20.0), None, Some(5.0), Some(-1.0)]
    );
}

/// Equal ORDER BY keys retain their upstream order as a deterministic
/// tie-breaker. The outer ORDER BY only changes presentation order.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_use_stable_upstream_order_for_peer_ties() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, LAG(id) OVER (ORDER BY amount) AS previous, \
                LEAD(id) OVER (ORDER BY amount) AS following \
         FROM sales ORDER BY id DESC",
    );

    assert_eq!(int_column(&result, 0), vec![5, 4, 3, 2, 1]);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![
            SqlValue::Null,
            SqlValue::Integer(3),
            SqlValue::Integer(1),
            SqlValue::Integer(4),
            SqlValue::Integer(5),
        ]
    );
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[2].clone())
            .collect::<Vec<_>>(),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(4),
            SqlValue::Null,
            SqlValue::Integer(3),
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_validate_arity_and_argument_types() {
    let mut h = Harness::new();
    for (sql, expected) in [
        ("SELECT LAG() OVER (ORDER BY id) FROM sales", "1 to 3"),
        (
            "SELECT LEAD(amount, 1, 0, 99) OVER (ORDER BY id) FROM sales",
            "1 to 3",
        ),
        (
            "SELECT LAG(amount, region) OVER (ORDER BY id) FROM sales",
            "offset",
        ),
        (
            "SELECT LEAD(amount, 1, region) OVER (ORDER BY id) FROM sales",
            "type mismatch",
        ),
        (
            "SELECT LAG(DISTINCT amount) OVER (ORDER BY id) FROM sales",
            "DISTINCT",
        ),
        ("SELECT LEAD(*) OVER (ORDER BY id) FROM sales", "star"),
    ] {
        let err = h.run_err(sql);
        assert!(
            err.to_ascii_lowercase()
                .contains(&expected.to_ascii_lowercase()),
            "error for `{sql}` must contain `{expected}`, got: {err}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn lag_and_lead_reject_negative_offsets_and_propagate_null_offsets() {
    let mut h = Harness::new();
    let err = h.run_err("SELECT LAG(amount, -1) OVER (ORDER BY id) FROM sales");
    assert!(
        err.to_ascii_lowercase().contains("non-negative"),
        "negative offset error must explain the constraint, got: {err}"
    );

    let result = query(
        &mut h,
        "SELECT id, LEAD(amount, NULL, -1) OVER (ORDER BY id) AS following \
         FROM sales ORDER BY id",
    );
    assert_eq!(optional_float_column(&result, 1), vec![None; 5]);
}

// ---------------------------------------------------------------------------
// Explicit ROWS / RANGE frames (issue #140)
// ---------------------------------------------------------------------------

/// ROWS addresses physical positions in the window-local order. The one-row
/// neighbors differ from the existing cumulative implicit frame on every row.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn rows_between_uses_physical_row_boundaries() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_eq!(int_column(&result, 1), vec![4, 9, 8, 7, 2]);
}

/// RANGE addresses order-key values and expands each endpoint to the complete
/// peer group. The tied amount=150 rows therefore share a frame.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn range_between_uses_value_boundaries_and_peers() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(qty) OVER (ORDER BY amount \
                    RANGE BETWEEN 50 PRECEDING AND CURRENT ROW) AS s \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&result, 0), vec![1, 2, 3, 4, 5]);
    assert_eq!(int_column(&result, 1), vec![3, 8, 10, 10, 0]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explicit_default_range_matches_the_implicit_ordered_frame() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, \
                SUM(qty) OVER (ORDER BY amount) AS implicit, \
                SUM(qty) OVER (ORDER BY amount \
                  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS explicit \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&result, 1), int_column(&result, 2));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explicit_default_range_keeps_large_input_parity_with_implicit_frame() {
    let mut h = Harness::new();
    let values = (1..=1001)
        .map(|value| format!("({value}, {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    h.run_ok(&format!(
        "CREATE TABLE large_frame (id INTEGER PRIMARY KEY, qty INTEGER); \
         INSERT INTO large_frame VALUES {values};"
    ));

    let result = query(
        &mut h,
        "SELECT id, \
                SUM(qty) OVER (ORDER BY id) AS implicit, \
                SUM(qty) OVER (ORDER BY id \
                  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS explicit \
         FROM large_frame ORDER BY id",
    );

    assert_eq!(int_column(&result, 1), int_column(&result, 2));
    assert_eq!(int_column(&result, 2).last(), Some(&501_501));

    let bounded = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id \
           RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
         FROM large_frame",
    );
    assert!(
        bounded.contains("explicit RANGE frame requires more than"),
        "large generic RANGE frame must fail closed: {bounded}"
    );

    let visit_bounded = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id \
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
         FROM large_frame",
    );
    assert!(
        visit_bounded.contains("aggregate input visits"),
        "large generic ROWS frame must fail closed: {visit_bounded}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn rows_frames_stop_at_partition_boundaries_and_can_be_empty() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, \
                SUM(qty) OVER (PARTITION BY region ORDER BY id \
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running, \
                COUNT(*) OVER (PARTITION BY region ORDER BY id \
                  ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) AS empty_count, \
                SUM(qty) OVER (PARTITION BY region ORDER BY id \
                  ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) AS empty_sum \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&result, 1), vec![3, 4, 5, 7, 0]);
    assert_eq!(int_column(&result, 2), vec![0, 0, 0, 0, 0]);
    assert!(result.rows.iter().all(|row| row[3] == SqlValue::Null));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn range_respects_descending_direction_and_null_peers() {
    let mut h = Harness::new();
    let descending = query(
        &mut h,
        "SELECT id, SUM(qty) OVER (ORDER BY amount DESC \
                    RANGE BETWEEN 50 PRECEDING AND CURRENT ROW) AS s \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&descending, 1), vec![10, 1, 8, 8, 3]);

    let nulls = query(
        &mut h,
        "SELECT id, COUNT(*) OVER (ORDER BY bonus NULLS FIRST RANGE CURRENT ROW) AS c \
         FROM sales ORDER BY id",
    );
    assert_eq!(int_column(&nulls, 1), vec![1, 2, 1, 2, 1]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn invalid_frame_shapes_and_types_have_deterministic_errors() {
    let mut h = Harness::new();
    let no_order = h.run_err("SELECT SUM(qty) OVER (ROWS CURRENT ROW) FROM sales");
    assert!(no_order.contains("require ORDER BY"), "{no_order}");

    let reversed = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) \
         FROM sales",
    );
    assert!(reversed.contains("bounds are reversed"), "{reversed}");

    let multiple_keys =
        h.run_err("SELECT SUM(qty) OVER (ORDER BY amount, id RANGE 1 PRECEDING) FROM sales");
    assert!(
        multiple_keys.contains("exactly one ORDER BY"),
        "{multiple_keys}"
    );

    let wrong_type =
        h.run_err("SELECT SUM(qty) OVER (ORDER BY region RANGE 1 PRECEDING) FROM sales");
    assert!(wrong_type.contains("must be numeric"), "{wrong_type}");

    let value_function =
        h.run_err("SELECT LAG(qty) OVER (ORDER BY id ROWS CURRENT ROW) FROM sales");
    assert!(
        value_function.contains("only supported for aggregate functions"),
        "{value_function}"
    );

    let ranking_function =
        h.run_err("SELECT RANK() OVER (ORDER BY id ROWS CURRENT ROW) FROM sales");
    assert!(
        ranking_function.contains("only supported for aggregate functions"),
        "{ranking_function}"
    );

    let invalid_start = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id \
           ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) \
         FROM sales",
    );
    assert!(
        invalid_start.contains("start cannot be UNBOUNDED FOLLOWING"),
        "{invalid_start}"
    );

    let invalid_end = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id \
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) \
         FROM sales",
    );
    assert!(
        invalid_end.contains("end cannot be UNBOUNDED PRECEDING"),
        "{invalid_end}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn frame_offset_overflow_is_rejected_without_execution() {
    let mut h = Harness::new();
    let overflow = h.run_err(
        "SELECT SUM(qty) OVER (ORDER BY id ROWS 18446744073709551616 PRECEDING) FROM sales",
    );
    assert!(
        overflow.to_ascii_lowercase().contains("range")
            || overflow.to_ascii_lowercase().contains("overflow"),
        "{overflow}"
    );
}
