//! Projection alias visibility in ORDER BY / HAVING (issue #122).
//!
//! SQL standard: aliases introduced by the SELECT list are visible to
//! ORDER BY and HAVING, but NOT to WHERE / GROUP BY (which are logically
//! evaluated before the projection).
//!
//! The planner currently builds a single `expr_scope` from the FROM-derived
//! base relations only, and reuses it for every clause, so alias references
//! fail with `error[ALOPEX-C003]: column '...' not found`.

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;
use std::sync::{Arc, RwLock};

/// Fixture table `t`:
///
/// | id | n | val |
/// |----|---|-----|
/// |  1 | 5 |  20 |
/// |  2 | 3 |   7 |
/// |  3 | 9 |  40 |
///
/// `id` is the primary key, so each group has exactly one row and
/// SUM(val) = {1 => 20, 2 => 7, 3 => 40}.
/// `HAVING total > 15` keeps ids {1, 3}.
const FIXTURE: &str = r#"
    CREATE TABLE t (id INT PRIMARY KEY, n INT, val INT);
    INSERT INTO t (id, n, val) VALUES (2, 3, 7), (1, 5, 20), (3, 9, 40);
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
    /// Returns the last query result produced, if any.
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
/// `INT` columns come back as `SqlValue::Integer(i32)` while `SUM(...)` widens
/// to `SqlValue::BigInt(i64)`; both are accepted so that the assertions below
/// compare exact values rather than storage widths.
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

// ---------------------------------------------------------------------------
// Aliases MUST be visible to ORDER BY (non-aggregate path)
// ---------------------------------------------------------------------------

/// Proves a bare column alias is resolvable from ORDER BY and that the
/// resulting order is the ascending order of the aliased column.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_resolves_simple_projection_alias() {
    let mut h = Harness::new();
    let result = query(&mut h, "SELECT id AS ident FROM t ORDER BY ident");

    assert_eq!(column_names(&result), vec!["ident".to_string()]);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3]);
}

/// Proves an alias bound to a computed expression (not a base column) is
/// resolvable from ORDER BY and sorts by the computed value.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_resolves_expression_projection_alias() {
    let mut h = Harness::new();
    let result = query(&mut h, "SELECT n * 2 AS doubled FROM t ORDER BY doubled");

    assert_eq!(column_names(&result), vec!["doubled".to_string()]);
    // n = {3, 5, 9} => doubled = {6, 10, 18}
    assert_eq!(int_column(&result, 0), vec![6, 10, 18]);
}

/// Proves DESC direction is honoured when sorting by an alias, so the alias is
/// resolved to the projected expression rather than silently ignored.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_alias_honours_descending_direction() {
    let mut h = Harness::new();
    let result = query(&mut h, "SELECT id AS ident FROM t ORDER BY ident DESC");

    assert_eq!(int_column(&result, 0), vec![3, 2, 1]);
}

// ---------------------------------------------------------------------------
// Aliases MUST be visible to ORDER BY / HAVING (aggregate path)
// ---------------------------------------------------------------------------

/// Proves an aggregate alias is resolvable from ORDER BY on the GROUP BY path,
/// and that it sorts by the aggregate value (40, 20, 7) rather than by `id`.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_resolves_aggregate_alias() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(val) AS total FROM t GROUP BY id ORDER BY total DESC",
    );

    assert_eq!(
        column_names(&result),
        vec!["id".to_string(), "total".to_string()]
    );
    assert_eq!(int_column(&result, 0), vec![3, 1, 2]);
    assert_eq!(int_column(&result, 1), vec![40, 20, 7]);
}

/// Proves an aggregate alias is resolvable from HAVING and that the predicate
/// filters on the aggregate value, keeping only ids 1 and 3.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn having_resolves_aggregate_alias() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(val) AS total FROM t GROUP BY id HAVING total > 15 ORDER BY id",
    );

    assert_eq!(int_column(&result, 0), vec![1, 3]);
    assert_eq!(int_column(&result, 1), vec![20, 40]);
}

/// Proves alias resolution in HAVING and ORDER BY compose in a single query.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn having_and_order_by_resolve_same_alias() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(val) AS total FROM t GROUP BY id HAVING total > 15 ORDER BY total ASC",
    );

    assert_eq!(int_column(&result, 1), vec![20, 40]);
}

// ---------------------------------------------------------------------------
// Alias vs. base column precedence
// ---------------------------------------------------------------------------

/// When an alias shadows a different base column, ORDER BY must bind to the
/// projection alias (SQL standard precedence), not the base column.
///
/// `SELECT id AS n FROM t ORDER BY n` must sort by `id` (1, 2, 3), whereas
/// binding to the base column `n` would yield the order of n = {5, 3, 9},
/// i.e. ids (2, 1, 3). The two orderings are deliberately distinguishable.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_prefers_projection_alias_over_shadowed_base_column() {
    let mut h = Harness::new();
    let result = query(&mut h, "SELECT id AS n FROM t ORDER BY n");

    assert_eq!(column_names(&result), vec!["n".to_string()]);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3]);
}

// ---------------------------------------------------------------------------
// Regression guards: behaviour that already works must keep working
// ---------------------------------------------------------------------------

/// Base-column references in ORDER BY must keep working while the projected
/// column still carries the alias in the result metadata.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn order_by_base_column_with_aliased_projection_still_works() {
    let mut h = Harness::new();
    let result = query(&mut h, "SELECT id AS ident FROM t ORDER BY id");

    assert_eq!(column_names(&result), vec!["ident".to_string()]);
    assert_eq!(int_column(&result, 0), vec![1, 2, 3]);
}

/// HAVING over the raw aggregate expression must keep working.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn having_aggregate_expression_without_alias_still_works() {
    let mut h = Harness::new();
    let result = query(
        &mut h,
        "SELECT id, SUM(val) AS total FROM t GROUP BY id HAVING SUM(val) > 15 ORDER BY id",
    );

    assert_eq!(int_column(&result, 0), vec![1, 3]);
    assert_eq!(int_column(&result, 1), vec![20, 40]);
}

// ---------------------------------------------------------------------------
// Aliases MUST NOT leak into WHERE / GROUP BY
// ---------------------------------------------------------------------------

/// WHERE is logically evaluated before the projection, so a projection alias
/// must remain unresolvable there and report ALOPEX-C003.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn where_does_not_see_projection_alias() {
    let mut h = Harness::new();
    let err = h.run_err("SELECT id AS ident FROM t WHERE ident > 1");

    assert!(
        err.contains("ALOPEX-C003") && err.contains("'ident'"),
        "expected column-not-found for alias in WHERE, got: {err}"
    );
}

/// GROUP BY is likewise evaluated before the projection; an alias reference
/// there must remain an ALOPEX-C003 error.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_does_not_see_projection_alias() {
    let mut h = Harness::new();
    let err = h.run_err("SELECT id AS ident, SUM(val) FROM t GROUP BY ident");

    assert!(
        err.contains("ALOPEX-C003") && err.contains("'ident'"),
        "expected column-not-found for alias in GROUP BY, got: {err}"
    );
}
