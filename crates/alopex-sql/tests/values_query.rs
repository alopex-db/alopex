//! VALUES query and table-constructor public behavior (issue #145, v0.8.8).

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::build_streaming_pipeline;
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
fn top_level_values_evaluates_expressions_and_infers_null_columns() {
    let result = Harness::new().query("VALUES (1 + 2, 'alpha', NULL), (4, 'beta', 6)");

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Integer(3),
                SqlValue::Text("alpha".into()),
                SqlValue::Null,
            ],
            vec![
                SqlValue::Integer(4),
                SqlValue::Text("beta".into()),
                SqlValue::Integer(6),
            ],
        ]
    );
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["column1", "column2", "column3"]
    );
}

#[test]
fn derived_values_uses_table_and_column_aliases() {
    let result = Harness::new()
        .query("SELECT id, label FROM (VALUES (2, 'b'), (1, 'a')) AS t(id, label) ORDER BY id");

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );
}

#[test]
fn values_can_be_a_cte_body_and_insert_source() {
    let result = Harness::new().query(
        "CREATE TABLE copied (id INTEGER, label TEXT); \
         INSERT INTO copied VALUES (9, 'existing'); \
         WITH v(id, label) AS (VALUES (2, 'b'), (1, 'a')) \
         SELECT id, label FROM v \
         UNION ALL SELECT id, label FROM copied \
         ORDER BY id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
            vec![SqlValue::Integer(9), SqlValue::Text("existing".into())],
        ]
    );
}

#[test]
fn values_query_can_be_an_insert_source_after_with() {
    let result = Harness::new().query(
        "CREATE TABLE copied (id INTEGER, label TEXT); \
         INSERT INTO copied \
         WITH seed(dummy) AS (VALUES (1)) \
         VALUES (2, 'b'), (1, 'a'); \
         SELECT id, label FROM copied ORDER BY id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );
}

#[test]
fn values_composes_with_set_operations_order_by_and_limit() {
    let result = Harness::new()
        .query("VALUES (3), (1) UNION ALL SELECT 2 UNION ALL VALUES (4) ORDER BY column1 LIMIT 3");

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
        ]
    );
}

#[test]
fn values_numeric_columns_use_a_common_widened_type() {
    let result = Harness::new().query("VALUES (1), (2147483648), (3.5)");

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Double(1.0)],
            vec![SqlValue::Double(2_147_483_648.0)],
            vec![SqlValue::Double(3.5)],
        ]
    );
}

#[test]
fn values_rejects_row_width_and_type_mismatches_with_stable_codes() {
    let width = Harness::new()
        .run("VALUES (1, 'a'), (2)")
        .expect_err("different row widths must fail");
    assert!(width.contains("ALOPEX-T011"), "unexpected error: {width}");

    let types = Harness::new()
        .run("VALUES (1), ('not-an-integer')")
        .expect_err("incompatible column types must fail");
    assert!(types.contains("ALOPEX-T001"), "unexpected error: {types}");
}

#[test]
fn values_rejects_empty_constructor_and_bad_alias_width() {
    for sql in ["VALUES", "VALUES ()"] {
        let error = Harness::new()
            .run(sql)
            .expect_err("an empty VALUES constructor must fail");
        assert!(error.contains("ALOPEX-P001"), "unexpected error: {error}");
    }

    let alias = Harness::new()
        .run("SELECT * FROM (VALUES (1, 2)) AS t(only_one)")
        .expect_err("alias width mismatch must fail");
    assert!(alias.contains("ALOPEX-T012"), "unexpected error: {alias}");
}

#[test]
fn values_has_materialized_and_streaming_parity() {
    let sql = "VALUES (1 + 1, 'two'), (3, 'three')";
    let expected = Harness::new().query(sql).rows;
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse VALUES query");
    let catalog = MemoryCatalog::new();
    let plan = Planner::new(&catalog)
        .plan(&statements[0])
        .expect("plan VALUES query");
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut txn = bridge.begin_read().expect("begin read");
    let (mut iterator, _, _) =
        build_streaming_pipeline(&mut txn, &catalog, plan).expect("build VALUES pipeline");
    let mut actual = Vec::new();
    while let Some(row) = iterator.next_row() {
        actual.push(row.expect("read VALUES row").values);
    }

    assert_eq!(actual, expected);
}

#[test]
fn values_works_in_scalar_and_membership_subqueries() {
    let result = Harness::new().query(
        "SELECT (VALUES (42)) AS answer, 2 IN (VALUES (1), (2)) AS present, \
         EXISTS (VALUES (NULL)) AS exists_row",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(42),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn values_subquery_can_reference_the_outer_row() {
    let result = Harness::new().query(
        "CREATE TABLE inputs (id INTEGER); \
         INSERT INTO inputs VALUES (2), (1); \
         SELECT id, (VALUES (id + 10)) AS shifted FROM inputs ORDER BY id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(11)],
            vec![SqlValue::Integer(2), SqlValue::Integer(12)],
        ]
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
fn duckdb_and_datafusion_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/values_query_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["documented_against"]["duckdb"], "1.5.5");
    assert_eq!(reference["documented_against"]["datafusion"], "54.0.0");

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
