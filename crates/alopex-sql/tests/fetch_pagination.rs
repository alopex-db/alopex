//! FETCH FIRST/NEXT, OFFSET n ROWS, WITH TIES, and expression pagination
//! (issue #152, v0.8.8). Semantics follow PostgreSQL 16; grammar decisions
//! are documented in docs/sql-fetch-pagination.md (D1..D16).

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

    fn with_scores() -> Self {
        let mut harness = Self::new();
        harness.query_ok(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER); \
             INSERT INTO t (id, score) VALUES \
             (1, 10), (2, 20), (3, 20), (4, 20), (5, 30), (6, NULL);",
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

fn ids(result: &QueryResult) -> Vec<i32> {
    result
        .rows
        .iter()
        .map(|row| match row[0] {
            SqlValue::Integer(id) => id,
            ref other => panic!("expected integer id, got {other:?}"),
        })
        .collect()
}

#[test]
fn offset_rows_with_fetch_next_only_matches_limit_offset() {
    let mut harness = Harness::with_scores();
    let fetched =
        harness.query("SELECT id FROM t ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY");
    assert_eq!(ids(&fetched), vec![3, 4]);

    let limited = harness.query("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2");
    assert_eq!(fetched.rows, limited.rows);

    // OFFSET may stand alone and may precede LIMIT (D2).
    let bare = harness.query("SELECT id FROM t ORDER BY id OFFSET 4");
    assert_eq!(ids(&bare), vec![5, 6]);
    let swapped = harness.query("SELECT id FROM t ORDER BY id OFFSET 1 LIMIT 2");
    assert_eq!(ids(&swapped), vec![2, 3]);
}

#[test]
fn fetch_count_defaults_to_one_row() {
    let mut harness = Harness::with_scores();
    let result = harness.query("SELECT id FROM t ORDER BY id FETCH NEXT ROW ONLY");
    assert_eq!(ids(&result), vec![1]);
}

#[test]
fn with_ties_keeps_every_peer_of_the_boundary_row() {
    let mut harness = Harness::with_scores();
    let result =
        harness.query("SELECT id, score FROM t ORDER BY score FETCH FIRST 2 ROWS WITH TIES");
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            vec![SqlValue::Integer(3), SqlValue::Integer(20)],
            vec![SqlValue::Integer(4), SqlValue::Integer(20)],
        ]
    );
}

#[test]
fn with_ties_uses_every_order_by_key_for_peers() {
    let mut harness = Harness::with_scores();
    // The second key (id) breaks the score tie, so no extra rows appear.
    let result = harness.query(
        "SELECT id, score FROM t ORDER BY score DESC NULLS LAST, id ASC \
         FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(5), SqlValue::Integer(30)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
        ]
    );
}

#[test]
fn with_ties_treats_nulls_as_peers() {
    let mut harness = Harness::with_scores();
    let single =
        harness.query("SELECT id FROM t ORDER BY score NULLS FIRST FETCH FIRST 1 ROW WITH TIES");
    assert_eq!(ids(&single), vec![6]);

    harness.query_ok("INSERT INTO t (id, score) VALUES (7, NULL)");
    let peers =
        harness.query("SELECT id FROM t ORDER BY score NULLS FIRST FETCH FIRST 1 ROW WITH TIES");
    assert_eq!(ids(&peers), vec![6, 7]);
}

#[test]
fn with_ties_never_revives_rows_discarded_by_offset() {
    let mut harness = Harness::with_scores();
    let result =
        harness.query("SELECT id FROM t ORDER BY score OFFSET 2 ROWS FETCH FIRST 1 ROW WITH TIES");
    // Sorted by score: 10(1), 20(2), 20(3), 20(4), 30(5), NULL(6).
    // OFFSET 2 discards ids 1 and 2; the boundary row is id 3 and only its
    // remaining peer id 4 follows. Discarded id 2 stays discarded.
    assert_eq!(ids(&result), vec![3, 4]);
}

#[test]
fn with_ties_requires_order_by() {
    let mut harness = Harness::with_scores();
    let error = harness.error("SELECT id FROM t FETCH FIRST 2 ROWS WITH TIES");
    assert!(
        error.contains("FETCH ... WITH TIES requires ORDER BY"),
        "{error}"
    );
}

#[test]
fn with_ties_on_empty_input_and_zero_limit_returns_no_rows() {
    let mut harness = Harness::with_scores();
    let empty = harness
        .query("SELECT id FROM t WHERE score > 1000 ORDER BY score FETCH FIRST 2 ROWS WITH TIES");
    assert!(empty.rows.is_empty());

    let zero = harness.query("SELECT id FROM t ORDER BY score FETCH FIRST 0 ROWS WITH TIES");
    assert!(zero.rows.is_empty());
}

#[test]
fn constant_expressions_are_folded_at_plan_time() {
    let mut harness = Harness::with_scores();
    assert_eq!(
        harness
            .query("SELECT id FROM t ORDER BY id LIMIT 1 + 1")
            .rows
            .len(),
        2
    );
    assert_eq!(
        harness
            .query("SELECT id FROM t ORDER BY id LIMIT CAST('3' AS INTEGER)")
            .rows
            .len(),
        3
    );
    assert_eq!(
        harness
            .query("SELECT id FROM t ORDER BY id OFFSET 10 / 2")
            .rows
            .len(),
        1
    );
    assert_eq!(
        harness
            .query("SELECT id FROM t ORDER BY id FETCH FIRST 1 + 1 ROWS ONLY")
            .rows
            .len(),
        2
    );
}

#[test]
fn null_and_all_mean_unlimited() {
    let mut harness = Harness::with_scores();
    for sql in [
        "SELECT id FROM t ORDER BY id LIMIT NULL",
        "SELECT id FROM t ORDER BY id LIMIT ALL",
        "SELECT id FROM t ORDER BY id OFFSET NULL",
        "SELECT id FROM t ORDER BY id FETCH FIRST NULL ROWS ONLY",
    ] {
        assert_eq!(harness.query(sql).rows.len(), 6, "{sql}");
    }
}

#[test]
fn invalid_counts_are_rejected_at_plan_time() {
    let mut harness = Harness::with_scores();

    let negative_limit = harness.error("SELECT id FROM t LIMIT -1");
    assert!(
        negative_limit.contains("LIMIT must not be negative"),
        "{negative_limit}"
    );

    let negative_offset = harness.error("SELECT id FROM t OFFSET -1");
    assert!(
        negative_offset.contains("OFFSET must not be negative"),
        "{negative_offset}"
    );

    let fractional = harness.error("SELECT id FROM t LIMIT 2.5");
    assert!(fractional.contains("BIGINT"), "{fractional}");

    let textual = harness.error("SELECT id FROM t LIMIT 'a'");
    assert!(textual.contains("BIGINT"), "{textual}");

    // A literal beyond i64 is rejected by the parser before planning.
    let literal_overflow = harness.error("SELECT id FROM t LIMIT 9223372036854775808");
    assert!(
        literal_overflow.contains("outside of valid range"),
        "{literal_overflow}"
    );

    // An arithmetic overflow in a constant count fails at plan time.
    let expression_overflow = harness.error("SELECT id FROM t LIMIT 9223372036854775807 + 1");
    assert!(
        expression_overflow.contains("LIMIT"),
        "{expression_overflow}"
    );

    let subquery = harness.error("SELECT id FROM t LIMIT (SELECT 1)");
    assert!(subquery.contains("subquery in LIMIT"), "{subquery}");

    let column = harness.error("SELECT id FROM t LIMIT score");
    assert!(column.contains("score"), "{column}");

    let aggregate = harness.error("SELECT id FROM t LIMIT COUNT(*)");
    assert!(
        aggregate.contains("aggregate functions are not allowed in LIMIT"),
        "{aggregate}"
    );
}

#[test]
fn unordered_pagination_returns_the_requested_row_count() {
    // Without ORDER BY the selected rows are an arbitrary-n contract (D10):
    // only the row count is guaranteed.
    let mut harness = Harness::with_scores();
    assert_eq!(harness.query("SELECT id FROM t LIMIT 3").rows.len(), 3);
    assert_eq!(
        harness
            .query("SELECT id FROM t FETCH FIRST 3 ROWS ONLY")
            .rows
            .len(),
        3
    );
}

#[test]
fn values_tail_supports_fetch() {
    let mut harness = Harness::new();
    let result =
        harness.query("VALUES (1), (2), (3) ORDER BY column1 DESC FETCH FIRST 2 ROWS ONLY");
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(2)]]
    );
}

#[test]
fn set_operation_tail_preserves_with_ties() {
    let mut harness = Harness::new();
    let result = harness.query(
        "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 2 \
         ORDER BY v DESC FETCH FIRST 1 ROW WITH TIES",
    );
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(2)]]
    );
}

#[test]
fn recursive_cte_bodies_reject_fetch() {
    let mut harness = Harness::new();
    let error = harness.error(
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c \
         FETCH FIRST 1 ROW ONLY) SELECT n FROM c",
    );
    assert!(
        error.contains("inside a recursive common table expression"),
        "{error}"
    );
}

fn reference_value(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Boolean(value) => serde_json::json!(value),
        SqlValue::Integer(value) => serde_json::json!(value),
        SqlValue::BigInt(value) => serde_json::json!(value),
        SqlValue::Text(value) => serde_json::json!(value),
        other => panic!("reference fixture does not use {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn postgresql_and_duckdb_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/fetch_pagination_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["reference_versions"]["postgresql"], "16.14");
    assert_eq!(reference["reference_versions"]["duckdb"], "1.5.5");

    let mut harness = Harness::with_scores();
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

// D16: the five pagination keywords stay usable as identifiers end to end.
// Reserving them in the lexer broke DDL/DML on any existing schema that used
// `row`, `next`, `only`, `ties` or `fetch` as a table or column name.
#[test]
fn pagination_keywords_remain_usable_as_identifiers() {
    let mut harness = Harness::new();
    harness.query_ok(
        "CREATE TABLE row (id INTEGER PRIMARY KEY, next INTEGER, only INTEGER, \
         ties INTEGER, fetch INTEGER);",
    );
    harness.query_ok("INSERT INTO row (id, next, only, ties, fetch) VALUES (1, 2, 3, 4, 5);");
    harness.query_ok("UPDATE row SET next = 9 WHERE id = 1;");

    let result =
        harness.query("SELECT r.next, r.only, r.ties, r.fetch FROM row AS r ORDER BY r.next");
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(9),
            SqlValue::Integer(3),
            SqlValue::Integer(4),
            SqlValue::Integer(5),
        ]]
    );

    // An explicit alias works for all five; an implicit one for all but
    // `fetch`, which starts the pagination tail in that position.
    let aliased = harness.query("SELECT id AS fetch, id row FROM row");
    assert_eq!(
        aliased
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["fetch", "row"]
    );
}

// D7: a literal beyond the i64 range never reaches the planner; only a
// constant *expression* produces the planner's bounded diagnostic.
#[test]
fn an_out_of_range_literal_count_is_a_parse_error_not_a_planner_error() {
    let mut harness = Harness::with_scores();

    let literal = harness.error("SELECT id FROM t ORDER BY id LIMIT 9223372036854775808");
    assert!(literal.contains("outside of valid range"), "{literal}");
    assert!(
        !literal.contains("LIMIT expression is invalid"),
        "{literal}"
    );

    let expression = harness.error("SELECT id FROM t ORDER BY id LIMIT 9223372036854775807 + 1");
    assert!(
        expression.contains("LIMIT expression is invalid"),
        "{expression}"
    );
}
