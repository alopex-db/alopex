//! Aggregate FILTER, aggregate-local ORDER BY, and WITHIN GROUP ordered-set
//! aggregates (issue #148). Decisions D1-D12 are documented in
//! docs/sql-aggregate-filter-within-group.md.

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::{build_streaming_pipeline_with_policy, project_row_values};
use alopex_sql::executor::{ExecutionResult, Executor, MemoryPolicy, QueryResult, SpillPolicy};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlValue, TxnBridge};

const SETUP: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER, name TEXT); \
     INSERT INTO t VALUES \
     (1, 'a', 5, 'x'), \
     (2, 'a', 20, 'y'), \
     (3, 'a', NULL, 'z'), \
     (4, 'b', 30, 'w'), \
     (5, 'b', NULL, NULL), \
     (6, 'b', 20, 'y')";

struct Harness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Harness {
    fn new() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, Arc::clone(&catalog));
        Self { executor, catalog }
    }

    fn seeded() -> Self {
        let mut harness = Self::new();
        harness.run(SETUP).expect("seed table");
        harness
    }

    fn run(&mut self, sql: &str) -> Result<Option<QueryResult>, String> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(|error| format!("parse: {error}"))?;
        let mut last = None;
        for statement in statements {
            let plan = {
                let catalog = self.catalog.read().expect("catalog read");
                Planner::new(&*catalog)
                    .plan(&statement)
                    .map_err(|error| error.to_string())?
            };
            if let ExecutionResult::Query(result) = self
                .executor
                .execute(plan)
                .map_err(|error| error.to_string())?
            {
                last = Some(result);
            }
        }
        Ok(last)
    }

    fn query(&mut self, sql: &str) -> QueryResult {
        self.run(sql)
            .unwrap_or_else(|error| panic!("expected `{}` to succeed: {error}", sql.trim()))
            .unwrap_or_else(|| panic!("expected `{}` to return rows", sql.trim()))
    }

    fn expect_error(&mut self, sql: &str) -> String {
        self.run(sql)
            .expect_err(&format!("expected `{}` to fail", sql.trim()))
    }
}

#[test]
fn filter_counts_only_true_rows_per_group() {
    let result = Harness::seeded().query(
        "SELECT g, COUNT(*) FILTER (WHERE v > 10) AS big, COUNT(*) AS total \
         FROM t GROUP BY g ORDER BY g",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Text("a".into()),
                SqlValue::BigInt(1),
                SqlValue::BigInt(3),
            ],
            vec![
                SqlValue::Text("b".into()),
                SqlValue::BigInt(2),
                SqlValue::BigInt(3),
            ],
        ]
    );
}

#[test]
fn filter_excludes_unknown_predicates_and_empty_sets_match_sql_semantics() {
    // NULL v makes `v > 10` UNKNOWN, which is excluded exactly like FALSE.
    let mut harness = Harness::seeded();
    let result = harness.query(
        "SELECT SUM(v) FILTER (WHERE FALSE), COUNT(*) FILTER (WHERE FALSE), \
         AVG(v) FILTER (WHERE v > 100) FROM t",
    );
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Null, SqlValue::BigInt(0), SqlValue::Null]]
    );

    harness.run("DELETE FROM t").expect("clear table");
    let empty =
        harness.query("SELECT SUM(v) FILTER (WHERE FALSE), COUNT(*) FILTER (WHERE FALSE) FROM t");
    assert_eq!(empty.rows, vec![vec![SqlValue::Null, SqlValue::BigInt(0)]]);
}

#[test]
fn filter_applies_before_distinct() {
    // v duplicates across groups: 20 appears in 'a' and 'b'. Filtering to 'a'
    // first leaves {5, 20}; a distinct-then-filter order would see 'b' rows.
    let result = Harness::seeded()
        .query("SELECT COUNT(DISTINCT v) FILTER (WHERE g = 'a'), COUNT(DISTINCT v) FROM t");
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::BigInt(2), SqlValue::BigInt(3)]]
    );
}

#[test]
fn aggregates_differing_only_in_filter_are_distinct_slots() {
    // Regression for AggregateSignature.filter_key (D10): without it, the two
    // aggregates silently merge and report the same value.
    let result = Harness::seeded().query("SELECT COUNT(v), COUNT(v) FILTER (WHERE v > 10) FROM t");
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::BigInt(4), SqlValue::BigInt(3)]]
    );
}

#[test]
fn having_matches_filtered_aggregates() {
    let result = Harness::seeded().query(
        "SELECT g FROM t GROUP BY g \
         HAVING COUNT(*) FILTER (WHERE v > 10) >= 1 ORDER BY g",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Text("a".into())],
            vec![SqlValue::Text("b".into())]
        ]
    );

    let strict = Harness::seeded().query(
        "SELECT g FROM t GROUP BY g \
         HAVING COUNT(*) FILTER (WHERE v > 25) >= 1 ORDER BY g",
    );
    assert_eq!(strict.rows, vec![vec![SqlValue::Text("b".into())]]);
}

#[test]
fn ordered_string_agg_orders_values_and_breaks_ties_deterministically() {
    let result = Harness::seeded().query(
        "SELECT g, STRING_AGG(name, ',' ORDER BY v DESC, name ASC) \
         FROM t GROUP BY g ORDER BY g",
    );
    // Group a: (20,y), (5,x), (NULL,z last by default NULLS LAST) -> y,x,z
    // Group b: (30,w), (20,y); the NULL-name row is skipped by STRING_AGG.
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Text("a".into()), SqlValue::Text("y,x,z".into())],
            vec![SqlValue::Text("b".into()), SqlValue::Text("w,y".into())],
        ]
    );
}

#[test]
fn ordered_group_concat_supports_nulls_first() {
    let result = Harness::seeded().query(
        "SELECT g, GROUP_CONCAT(name ORDER BY v ASC NULLS FIRST, name ASC) \
         FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Text("a".into()), SqlValue::Text("z,x,y".into())],
            vec![SqlValue::Text("b".into()), SqlValue::Text("y,w".into())],
        ]
    );
}

#[test]
fn order_by_on_order_insensitive_aggregates_is_validated_then_discarded() {
    // D3: identical result, and both spellings share one aggregate slot.
    let ordered = Harness::seeded().query("SELECT SUM(v ORDER BY name), SUM(v) FROM t");
    assert_eq!(
        ordered.rows,
        vec![vec![SqlValue::BigInt(75), SqlValue::BigInt(75)]]
    );

    // The sort key is still name-resolved: an unknown column must fail.
    let error = Harness::seeded().expect_error("SELECT SUM(v ORDER BY missing) FROM t");
    assert!(error.contains("missing"), "unexpected error: {error}");
}

#[test]
fn distinct_with_order_by_requires_order_keys_in_arguments() {
    // D4 (PostgreSQL rule).
    let result =
        Harness::seeded().query("SELECT STRING_AGG(DISTINCT name, ',' ORDER BY name DESC) FROM t");
    assert_eq!(result.rows, vec![vec![SqlValue::Text("z,y,x,w".into())]]);

    let error =
        Harness::seeded().expect_error("SELECT STRING_AGG(DISTINCT name, ',' ORDER BY v) FROM t");
    assert!(
        error.contains("ORDER BY expressions must appear in the argument list"),
        "unexpected error: {error}"
    );
}

#[test]
fn percentile_disc_selects_discrete_values() {
    let mut harness = Harness::new();
    harness
        .run(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER); \
             INSERT INTO p VALUES (1, 1), (2, 2), (3, 2), (4, 3)",
        )
        .expect("seed percentile table");

    let result = harness.query(
        "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v), \
         PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY v), \
         PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY v), \
         PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY v), \
         PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY v) FROM p",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(3),
            SqlValue::Integer(1),
            SqlValue::Integer(2),
        ]]
    );

    // Fractions that differ must occupy distinct aggregate slots.
    let fractions = harness.query(
        "SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY v), \
         PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY v) FROM p",
    );
    assert_eq!(
        fractions.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(3)]]
    );
}

#[test]
fn percentile_disc_handles_nulls_groups_descending_and_filter() {
    let mut harness = Harness::seeded();

    // NULL sort values are excluded; per-group evaluation.
    let grouped = harness.query(
        "SELECT g, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) \
         FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(
        grouped.rows,
        vec![
            vec![SqlValue::Text("a".into()), SqlValue::Integer(5)],
            vec![SqlValue::Text("b".into()), SqlValue::Integer(20)],
        ]
    );

    // DESC reverses the cumulative distribution.
    let descending =
        harness.query("SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY v DESC) FROM t");
    assert_eq!(descending.rows, vec![vec![SqlValue::Integer(30)]]);

    // All rows excluded -> NULL.
    let empty =
        harness.query("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) FROM t WHERE v > 100");
    assert_eq!(empty.rows, vec![vec![SqlValue::Null]]);

    // FILTER applies before the ordered set is formed.
    let filtered = harness.query(
        "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE g = 'a') FROM t",
    );
    assert_eq!(filtered.rows, vec![vec![SqlValue::Integer(5)]]);
}

#[test]
fn misuse_is_rejected_with_stable_planner_errors() {
    let cases: &[(&str, &str)] = &[
        (
            "SELECT SUM(v) WITHIN GROUP (ORDER BY v) FROM t",
            "WITHIN GROUP is only valid for ordered-set aggregate functions",
        ),
        (
            "SELECT PERCENTILE_DISC(0.5) FROM t",
            "WITHIN GROUP (ORDER BY ...) is required for PERCENTILE_DISC",
        ),
        (
            "SELECT PERCENTILE_DISC(2.0) WITHIN GROUP (ORDER BY v) FROM t",
            "PERCENTILE_DISC fraction must be between 0 and 1",
        ),
        (
            "SELECT PERCENTILE_DISC(v) WITHIN GROUP (ORDER BY v) FROM t",
            "PERCENTILE_DISC fraction must be a numeric literal",
        ),
        (
            "SELECT COUNT(*) FILTER (WHERE SUM(v) > 1) FROM t",
            "aggregate functions are not allowed in FILTER",
        ),
        (
            "SELECT ABS(v) FILTER (WHERE v > 0) FROM t",
            "FILTER (WHERE ...) is only valid for aggregate functions",
        ),
        (
            "SELECT COUNT(*) FILTER (WHERE v) FROM t",
            "BOOLEAN FILTER predicate",
        ),
        (
            "SELECT SUM(v) FILTER (WHERE v > 0) OVER () FROM t",
            "FILTER on a window function call",
        ),
        (
            "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) OVER () FROM t",
            "WITHIN GROUP cannot be combined with OVER",
        ),
        (
            "SELECT SUM(v ORDER BY v) OVER () FROM t",
            "aggregate ORDER BY cannot be combined with OVER",
        ),
        (
            "SELECT PERCENTILE_DISC(DISTINCT 0.5) WITHIN GROUP (ORDER BY v) FROM t",
            "DISTINCT is not supported with WITHIN GROUP",
        ),
        (
            "SELECT ABS(v ORDER BY v) FROM t",
            "ORDER BY in the argument list is only valid for aggregate functions",
        ),
        (
            "SELECT SUM(COUNT(v)) FROM t",
            "nested aggregate functions are not supported",
        ),
    ];
    for (sql, expected) in cases {
        let error = Harness::seeded().expect_error(sql);
        assert!(
            error.contains(expected),
            "`{sql}` produced unexpected error: {error}"
        );
    }
}

fn parity_rows(sql: &str, spill: bool) -> Vec<Vec<SqlValue>> {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
    for statement in Parser::parse_sql(&AlopexDialect, SETUP).expect("parse setup") {
        let plan = {
            let guard = catalog.read().expect("catalog read");
            Planner::new(&*guard).plan(&statement).expect("plan setup")
        };
        executor.execute(plan).expect("execute setup");
    }

    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse query");
    let plan = {
        let guard = catalog.read().expect("catalog read");
        Planner::new(&*guard)
            .plan(&statements[0])
            .expect("plan query")
    };

    let spill_dir = tempfile::tempdir().expect("spill dir");
    let policy = spill.then(|| {
        MemoryPolicy::new(
            None,
            SpillPolicy::SpillToDisk {
                directory: spill_dir.path().to_path_buf(),
            },
        )
    });

    let bridge = TxnBridge::new(store);
    let mut txn = bridge.begin_read().expect("begin read");
    let guard = catalog.read().expect("catalog read");
    let (mut iterator, projection, schema) =
        build_streaming_pipeline_with_policy(&mut txn, &*guard, plan, policy.as_ref())
            .expect("build pipeline");
    let mut rows = Vec::new();
    while let Some(row) = iterator.next_row() {
        let row = row.expect("read row");
        rows.push(project_row_values(&row, &projection, &schema).expect("project row"));
    }
    rows
}

#[test]
fn materialized_streaming_and_spill_paths_agree() {
    for sql in [
        "SELECT g, COUNT(*) FILTER (WHERE v > 10), SUM(v) FILTER (WHERE g = 'a'), \
         COUNT(DISTINCT v) FILTER (WHERE v > 4) \
         FROM t GROUP BY g ORDER BY g",
        "SELECT g, STRING_AGG(name, ',' ORDER BY v DESC, name ASC) \
         FROM t GROUP BY g ORDER BY g",
        "SELECT g, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) \
         FROM t GROUP BY g ORDER BY g",
    ] {
        let expected = Harness::seeded().query(sql).rows;
        assert_eq!(parity_rows(sql, false), expected, "streaming parity: {sql}");
        assert_eq!(parity_rows(sql, true), expected, "spill parity: {sql}");
    }
}

fn reference_value(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Boolean(value) => serde_json::json!(value),
        SqlValue::Integer(value) => serde_json::json!(value),
        SqlValue::BigInt(value) => serde_json::json!(value),
        SqlValue::Float(value) => serde_json::json!(value),
        SqlValue::Double(value) => serde_json::json!(value),
        SqlValue::Text(value) => serde_json::json!(value),
        other => panic!("reference fixture does not use {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn portable_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/aggregate_filter_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["reference_versions"]["duckdb"], "1.5.5");
    assert_eq!(reference["reference_versions"]["datafusion"], "54.0.0");
    assert_eq!(reference["reference_versions"]["postgresql"], "16.14");

    let mut harness = Harness::seeded();
    for case in reference["cases"].as_array().expect("reference cases") {
        let result = harness.query(case["sql"].as_str().expect("reference SQL"));
        let rows = result
            .rows
            .iter()
            .map(|row| row.iter().map(reference_value).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::json!(rows),
            case["rows"],
            "{} rows",
            case["name"]
        );
    }
}
