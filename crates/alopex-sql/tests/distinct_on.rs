//! SELECT DISTINCT ON (expr, ...) deterministic first-row deduplication
//! (issue #150, v0.8.8). Semantics follow PostgreSQL 16 with an Alopex
//! determinism extension; decisions are documented in
//! docs/sql-distinct-on.md (D1..D13).

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
    fn new() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, Arc::clone(&catalog));
        Self { executor, catalog }
    }

    /// Shared fixture: two `west` rows tie on amount (id 3 and 4) and the
    /// NULL region group has two rows, so tie handling is always exercised.
    fn with_sales() -> Self {
        let mut harness = Self::new();
        harness.query_ok(
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER, note TEXT); \
             INSERT INTO sales (id, region, amount, note) VALUES \
             (1, 'east', 100, 'a'), (2, 'east', 50, 'b'), (3, 'west', 75, 'c'), \
             (4, 'west', 75, 'd'), (5, NULL, 10, 'e'), (6, NULL, 5, 'f');",
        );
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

    fn query_ok(&mut self, sql: &str) {
        self.run(sql)
            .unwrap_or_else(|error| panic!("expected `{}` to succeed: {error}", sql.trim()));
    }

    fn query(&mut self, sql: &str) -> QueryResult {
        self.run(sql)
            .unwrap_or_else(|error| panic!("expected `{}` to succeed: {error}", sql.trim()))
            .unwrap_or_else(|| panic!("expected `{}` to return rows", sql.trim()))
    }

    fn error(&mut self, sql: &str) -> String {
        match self.run(sql) {
            Err(error) => error,
            Ok(_) => panic!("expected `{}` to fail", sql.trim()),
        }
    }
}

fn integer(value: i32) -> SqlValue {
    SqlValue::Integer(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_string())
}

// T1: basic single-key DISTINCT ON with a NULL key group (D5: one NULL row,
// NULLS LAST by default).
#[test]
fn distinct_on_single_key_keeps_the_first_row_per_group() {
    let mut harness = Harness::with_sales();
    let result = harness
        .query("SELECT DISTINCT ON (region) region, amount FROM sales ORDER BY region, amount");
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(50)],
            vec![text("west"), integer(75)],
            vec![SqlValue::Null, integer(5)],
        ]
    );
}

// T2: the ORDER BY tail picks the winner (amount DESC).
#[test]
fn distinct_on_respects_a_descending_tail() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT DISTINCT ON (region) region, amount FROM sales ORDER BY region, amount DESC",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(100)],
            vec![text("west"), integer(75)],
            vec![SqlValue::Null, integer(10)],
        ]
    );
}

// T3: multiple keys form composite groups.
#[test]
fn distinct_on_multiple_keys() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT DISTINCT ON (region, amount) region, amount, id FROM sales \
         ORDER BY region, amount, id",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(50), integer(2)],
            vec![text("east"), integer(100), integer(1)],
            vec![text("west"), integer(75), integer(3)],
            vec![SqlValue::Null, integer(5), integer(6)],
            vec![SqlValue::Null, integer(10), integer(5)],
        ]
    );
}

// T4: tie contract (D4) — when the user ORDER BY leaves a tie, the all-column
// tie-breaker decides, and the same rows win regardless of insertion order.
#[test]
fn distinct_on_tie_winner_does_not_depend_on_physical_order() {
    let mut harness = Harness::with_sales();
    let sql = "SELECT DISTINCT ON (region) id FROM sales WHERE region = 'west' \
               ORDER BY region, amount";
    let forward = harness.query(sql);
    // amount ties at 75 for id 3 and 4; the schema-order tie-breaker starts
    // at column `id` ASC, so id 3 wins.
    assert_eq!(forward.rows, vec![vec![integer(3)]]);

    // The same data inserted in reverse order must elect the same winner.
    harness.query_ok(
        "CREATE TABLE sales_reversed (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER, note TEXT); \
         INSERT INTO sales_reversed (id, region, amount, note) VALUES \
         (6, NULL, 5, 'f'), (5, NULL, 10, 'e'), (4, 'west', 75, 'd'), \
         (3, 'west', 75, 'c'), (2, 'east', 50, 'b'), (1, 'east', 100, 'a');",
    );
    let reversed = harness.query(
        "SELECT DISTINCT ON (region) id FROM sales_reversed WHERE region = 'west' \
         ORDER BY region, amount",
    );
    assert_eq!(reversed.rows, forward.rows);
}

// T5: select-list aliases resolve inside DISTINCT ON keys (D6).
#[test]
fn distinct_on_resolves_projection_aliases() {
    let mut harness = Harness::with_sales();
    let result = harness
        .query("SELECT DISTINCT ON (parity) id % 2 AS parity, id FROM sales ORDER BY parity, id");
    assert_eq!(
        result.rows,
        vec![vec![integer(0), integer(2)], vec![integer(1), integer(1)],]
    );
}

// D2: a compound key expression written literally in both the ON list and
// ORDER BY matches structurally even though the two occurrences carry
// different source spans.
#[test]
fn distinct_on_matches_compound_expressions_across_clauses() {
    let mut harness = Harness::with_sales();
    let result = harness
        .query("SELECT DISTINCT ON (id % 2) id % 2 AS parity, id FROM sales ORDER BY id % 2, id");
    assert_eq!(
        result.rows,
        vec![vec![integer(0), integer(2)], vec![integer(1), integer(1)],]
    );
}

// T6: ORDER BY omitted (D3) — keys sort ASC NULLS LAST and the all-column
// tie-breaker elects a deterministic winner per group.
#[test]
fn distinct_on_without_order_by_is_deterministic() {
    let mut harness = Harness::with_sales();
    let result = harness.query("SELECT DISTINCT ON (region) region, amount, id FROM sales");
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(100), integer(1)],
            vec![text("west"), integer(75), integer(3)],
            vec![SqlValue::Null, integer(10), integer(5)],
        ]
    );
}

// T7: the ORDER BY prefix may permute the ON keys (D2).
#[test]
fn distinct_on_accepts_a_permuted_order_by_prefix() {
    let mut harness = Harness::with_sales();
    let result = harness
        .query("SELECT DISTINCT ON (region, amount) id FROM sales ORDER BY amount, region, id");
    assert_eq!(result.rows.len(), 5);
}

// T8: ORDER BY starting with a non-key expression is rejected (D2, 42P10).
#[test]
fn distinct_on_rejects_a_non_matching_order_by_prefix() {
    let mut harness = Harness::with_sales();
    let message = harness.error("SELECT DISTINCT ON (region) region FROM sales ORDER BY amount");
    assert!(
        message.contains("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("ALOPEX-T014"),
        "unexpected error: {message}"
    );
}

// T9: an unconsumed key with a non-key tail present is rejected (D2).
#[test]
fn distinct_on_rejects_an_unconsumed_key_before_a_tail() {
    let mut harness = Harness::with_sales();
    let message =
        harness.error("SELECT DISTINCT ON (region, amount) id FROM sales ORDER BY region, id");
    assert!(
        message.contains("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
        "unexpected error: {message}"
    );
}

// T10: ORDER BY covering only a subset of the keys appends the rest as
// implicit ASC NULLS LAST keys (D2/D3).
#[test]
fn distinct_on_appends_unreached_keys_implicitly() {
    let mut harness = Harness::with_sales();
    let result = harness
        .query("SELECT DISTINCT ON (region, amount) region, amount FROM sales ORDER BY region");
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(50)],
            vec![text("east"), integer(100)],
            vec![text("west"), integer(75)],
            vec![SqlValue::Null, integer(5)],
            vec![SqlValue::Null, integer(10)],
        ]
    );
}

// T11: v1 combination limits (D7) and key expression limits (D6).
#[test]
fn distinct_on_rejects_unsupported_combinations() {
    let mut harness = Harness::with_sales();
    let group_by =
        harness.error("SELECT DISTINCT ON (region) region, COUNT(*) FROM sales GROUP BY region");
    assert!(group_by.contains("not supported"), "got: {group_by}");

    let window = harness.error("SELECT DISTINCT ON (region) ROW_NUMBER() OVER () FROM sales");
    assert!(window.contains("not supported"), "got: {window}");

    let set_operation =
        harness.error("SELECT DISTINCT ON (region) region FROM sales UNION SELECT 'x'");
    assert!(
        set_operation.contains("not supported"),
        "got: {set_operation}"
    );

    let aggregate_key = harness.error("SELECT DISTINCT ON (SUM(amount)) region FROM sales");
    assert!(aggregate_key.contains("aggregate"), "got: {aggregate_key}");

    let having = harness.error("SELECT DISTINCT ON (region) region FROM sales HAVING 1 = 1");
    assert!(
        having.contains("not supported") || having.contains("HAVING"),
        "got: {having}"
    );
}

// T12: equivalence with plain DISTINCT when the keys cover every column.
#[test]
fn distinct_on_over_all_columns_equals_distinct() {
    let mut harness = Harness::with_sales();
    let distinct_on = harness.query(
        "SELECT DISTINCT ON (region, amount, id, note) region, amount, id, note FROM sales \
         ORDER BY region, amount, id, note",
    );
    let distinct = harness.query("SELECT DISTINCT region, amount, id, note FROM sales");
    let mut distinct_rows = distinct.rows.clone();
    distinct_rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    let mut distinct_on_rows = distinct_on.rows.clone();
    distinct_on_rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    assert_eq!(distinct_on_rows, distinct_rows);
}

// T13: equivalence with the QUALIFY ROW_NUMBER() = 1 rewrite when both sides
// carry the unique tie-breaker (id) explicitly.
#[test]
fn distinct_on_matches_the_row_number_rewrite() {
    let mut harness = Harness::with_sales();
    let distinct_on = harness
        .query("SELECT DISTINCT ON (region) region, amount FROM sales ORDER BY region, amount, id");
    let rewrite = harness.query(
        "SELECT region, amount FROM sales \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount, id) = 1 \
         ORDER BY region, amount",
    );
    assert_eq!(distinct_on.rows, rewrite.rows);
}

// T14: empty inputs produce empty outputs.
#[test]
fn distinct_on_on_empty_input() {
    let mut harness = Harness::with_sales();
    let filtered = harness.query("SELECT DISTINCT ON (region) region FROM sales WHERE 1 = 0");
    assert!(filtered.rows.is_empty());

    harness.query_ok("CREATE TABLE empty_sales (id INTEGER PRIMARY KEY, region TEXT);");
    let empty = harness.query("SELECT DISTINCT ON (region) region FROM empty_sales");
    assert!(empty.rows.is_empty());
}

// T15: LIMIT/OFFSET applies after deduplication (D8).
#[test]
fn distinct_on_applies_limit_after_deduplication() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT DISTINCT ON (region) region, amount FROM sales \
         ORDER BY region, amount LIMIT 1 OFFSET 1",
    );
    assert_eq!(result.rows, vec![vec![text("west"), integer(75)]]);
}

// T16: DISTINCT ON inside a CTE and a derived table (D7 scope).
#[test]
fn distinct_on_works_inside_cte_and_derived_table() {
    let mut harness = Harness::with_sales();
    let cte = harness.query(
        "WITH t AS (SELECT DISTINCT ON (region) region, amount FROM sales \
         ORDER BY region, amount) SELECT COUNT(*) FROM t",
    );
    assert_eq!(cte.rows, vec![vec![SqlValue::BigInt(3)]]);

    let derived =
        harness.query("SELECT COUNT(*) FROM (SELECT DISTINCT ON (region) region FROM sales) AS d");
    assert_eq!(derived.rows, vec![vec![SqlValue::BigInt(3)]]);
}

// Duplicate ON keys deduplicate structurally (D2).
#[test]
fn distinct_on_deduplicates_repeated_keys() {
    let mut harness = Harness::with_sales();
    let result =
        harness.query("SELECT DISTINCT ON (region, region) region FROM sales ORDER BY region");
    assert_eq!(result.rows.len(), 3);
}

// DISTINCT ON on the right operand of a set operation stays supported (D7).
#[test]
fn distinct_on_as_a_set_operation_operand() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT 'x' UNION ALL SELECT DISTINCT ON (region) region FROM sales WHERE region = 'east'",
    );
    assert_eq!(result.rows.len(), 2);
}

// D13: DISTINCT ON plans no Sort node of its own, so WITH TIES has to read its
// peer specification from the user ORDER BY the DistinctOn node carries. It
// used to be rejected with the factually wrong "requires ORDER BY".
#[test]
fn distinct_on_supports_fetch_with_ties() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT DISTINCT ON (region) region, amount FROM sales \
         ORDER BY region, amount FETCH FIRST 1 ROW WITH TIES",
    );
    // 'east'/50, 'west'/75 and NULL/5 survive deduplication; no two of them
    // tie on (region, amount), so WITH TIES keeps exactly the first row.
    assert_eq!(result.rows, vec![vec![text("east"), integer(50)]]);
}

// D13: the peer specification must be the *user* ORDER BY only. The effective
// specification also carries implicit ON keys and an all-column tie-breaker
// tail, which make every surviving row unique — using those would silently
// degrade WITH TIES to a plain LIMIT.
#[test]
fn distinct_on_with_ties_uses_only_the_user_order_by() {
    let mut harness = Harness::with_sales();
    // The ON keys (region, amount) exceed the user ORDER BY (region), so
    // `amount` becomes an implicit key (D3). Deduplication leaves
    // ('east', 50), ('east', 100) and ('west', 75); the two 'east' rows are
    // peers under the user ORDER BY, so WITH TIES keeps both.
    let result = harness.query(
        "SELECT DISTINCT ON (region, amount) region, amount FROM sales \
         WHERE region IS NOT NULL ORDER BY region FETCH FIRST 1 ROW WITH TIES",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), integer(50)],
            vec![text("east"), integer(100)]
        ]
    );
}

// D3/D13: WITH TIES without any ORDER BY is still the PostgreSQL 42P20 error,
// and the message is now accurate for DISTINCT ON too.
#[test]
fn distinct_on_with_ties_without_order_by_is_rejected() {
    let mut harness = Harness::with_sales();
    let error =
        harness.error("SELECT DISTINCT ON (region) region FROM sales FETCH FIRST 1 ROW WITH TIES");
    assert!(
        error.contains("FETCH ... WITH TIES requires ORDER BY"),
        "{error}"
    );
}

// Reference fixture pinned to PostgreSQL/DuckDB semantics.
fn reference_value(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(value) => serde_json::json!(value),
        SqlValue::Text(value) => serde_json::json!(value),
        other => panic!("unexpected reference value {other:?}"),
    }
}

#[test]
fn distinct_on_matches_the_pinned_reference_fixture() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/distinct_on_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["reference_versions"]["postgresql"], "16.14");
    assert_eq!(reference["reference_versions"]["duckdb"], "1.5.5");

    let mut harness = Harness::with_sales();
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
