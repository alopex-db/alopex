//! Standard predicates and row-value comparison behavior (issue #146, v0.8.8).

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::{build_streaming_pipeline, project_row_values};
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlValue, TxnBridge};

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
}

#[test]
fn truth_predicates_are_total_over_boolean_and_null() {
    let result = Harness::new().query(
        "SELECT value, value IS TRUE, value IS NOT TRUE, value IS FALSE, \
         value IS NOT FALSE, value IS UNKNOWN, value IS NOT UNKNOWN \
         FROM (VALUES (TRUE), (FALSE), (NULL)) AS truth(value)",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Boolean(true),
                SqlValue::Boolean(true),
                SqlValue::Boolean(false),
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
            ],
            vec![
                SqlValue::Boolean(false),
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
                SqlValue::Boolean(true),
                SqlValue::Boolean(false),
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
            ],
            vec![
                SqlValue::Null,
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
                SqlValue::Boolean(false),
                SqlValue::Boolean(true),
                SqlValue::Boolean(true),
                SqlValue::Boolean(false),
            ],
        ]
    );
}

#[test]
fn distinct_predicates_are_null_safe() {
    let result = Harness::new().query(
        "SELECT NULL IS DISTINCT FROM NULL, NULL IS DISTINCT FROM 1, \
         NULL IS NOT DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 2",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Boolean(false),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Boolean(false),
        ]]
    );
}

#[test]
fn row_equality_and_ordering_follow_three_valued_logic() {
    let result = Harness::new().query(
        "SELECT (1, 2) = (1, 2), (1, NULL) = (2, NULL), \
         (1, NULL) = (1, NULL), (1, NULL) < (2, 0), \
         (1, NULL) < (1, 0)",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Boolean(true),
            SqlValue::Boolean(false),
            SqlValue::Null,
            SqlValue::Boolean(true),
            SqlValue::Null,
        ]]
    );
}

#[test]
fn row_comparison_and_negated_predicate_dispatch_is_complete() {
    let result = Harness::new().query(
        "SELECT (1, 2) <> (1, 3), (1, 2) <= (1, 2), \
         (2, 0) > (1, 9), (2, 0) >= (2, 0), \
         (NULL, 1) = (NULL, 2), (NULL, 1) <> (NULL, 2), \
         (1, 2) NOT IN ((3, 4), (5, 6)), \
         (1, NULL) NOT IN ((1, 2), (2, 3)), \
         (2, 3) NOT BETWEEN (1, 9) AND (3, 0), \
         (4, 0) NOT BETWEEN (1, 9) AND (3, 0)",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Boolean(false),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Null,
            SqlValue::Boolean(false),
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn row_distinct_from_compares_fields_null_safely() {
    let result = Harness::new().query(
        "SELECT (1, NULL) IS DISTINCT FROM (1, NULL), \
         (1, NULL) IS DISTINCT FROM (1, 2), \
         (1, NULL) IS NOT DISTINCT FROM (1, NULL)",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Boolean(false),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn row_values_work_with_in_and_between() {
    let result = Harness::new().query(
        "SELECT (1, 2) IN ((3, 4), (1, 2)), \
         (1, NULL) IN ((1, 2), (1, NULL)), \
         (2, 3) BETWEEN (1, 9) AND (3, 0)",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Boolean(true),
            SqlValue::Null,
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn row_predicates_resolve_columns_and_filter_rows() {
    let result = Harness::new().query(
        "CREATE TABLE pairs (id INTEGER, a INTEGER, b INTEGER); \
         INSERT INTO pairs VALUES (1, 1, 2), (2, 1, NULL), (3, 2, 0); \
         SELECT id FROM pairs \
         WHERE (a, b) >= (1, 2) AND (a, b) IS DISTINCT FROM (1, NULL) \
         ORDER BY id",
    );

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]
    );
}

#[test]
fn standard_predicates_have_materialized_and_streaming_parity() {
    let sql = "SELECT TRUE IS TRUE AS truth_value, \
               NULL IS DISTINCT FROM 1 AS distinct_null, \
               (1, NULL) < (2, 0) AS row_less, \
               (1, NULL) = (1, NULL) AS row_unknown";
    let expected = Harness::new().query(sql).rows;
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse predicate query");
    let catalog = MemoryCatalog::new();
    let plan = Planner::new(&catalog)
        .plan(&statements[0])
        .expect("plan predicate query");
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut txn = bridge.begin_read().expect("begin read");
    let (mut iterator, projection, schema) =
        build_streaming_pipeline(&mut txn, &catalog, plan).expect("build predicate pipeline");
    let mut actual = Vec::new();
    while let Some(row) = iterator.next_row() {
        let row = row.expect("read predicate row");
        actual.push(project_row_values(&row, &projection, &schema).expect("project predicate row"));
    }

    assert_eq!(actual, expected);
}

#[test]
fn row_arity_and_type_errors_are_stable() {
    for sql in [
        "SELECT (1, 2) = (1, 2, 3)",
        "SELECT (1, 2) IN ((1, 2), (1, 2, 3))",
        "SELECT (1, 2) BETWEEN (1) AND (2, 3)",
    ] {
        let arity = Harness::new()
            .run(sql)
            .expect_err("different row arities must fail");
        assert!(arity.contains("ALOPEX-T013"), "unexpected error: {arity}");
    }

    let types = Harness::new()
        .run("SELECT (1, 'x') < (1, 2)")
        .expect_err("incompatible row fields must fail");
    assert!(types.contains("ALOPEX-T001"), "unexpected error: {types}");

    let non_boolean = Harness::new()
        .run("SELECT 1 IS TRUE")
        .expect_err("truth predicates require BOOLEAN");
    assert!(
        non_boolean.contains("ALOPEX-T001"),
        "unexpected error: {non_boolean}"
    );

    let standalone = Harness::new()
        .run("SELECT (1, 2)")
        .expect_err("standalone rows are not persisted scalar values");
    assert!(
        standalone.contains("ALOPEX-F001"),
        "unexpected error: {standalone}"
    );
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
        serde_json::from_str(include_str!("fixtures/standard_predicates_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["reference_versions"]["duckdb"], "1.5.5");
    assert_eq!(reference["reference_versions"]["datafusion"], "54.0.0");
    assert_eq!(reference["reference_versions"]["postgresql"], "16.14");

    let mut harness = Harness::new();
    for case in reference["cases"].as_array().expect("reference cases") {
        let result = harness.query(case["sql"].as_str().expect("reference SQL"));
        let columns = result
            .columns
            .iter()
            .map(|column| serde_json::json!(column.name))
            .collect::<Vec<_>>();
        let rows = result
            .rows
            .iter()
            .map(|row| row.iter().map(reference_value).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::Value::Array(columns),
            case["columns"],
            "{} columns",
            case["name"]
        );
        assert_eq!(
            serde_json::json!(rows),
            case["rows"],
            "{} rows",
            case["name"]
        );
    }
}
