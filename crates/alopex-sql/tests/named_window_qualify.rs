//! Named WINDOW and QUALIFY public behavior (issue #144, v0.8.8).

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{LogicalPlan, Planner};
use alopex_sql::storage::SqlValue;
use std::sync::{Arc, RwLock};

const FIXTURE: &str = r#"
    CREATE TABLE sales (
      id INTEGER PRIMARY KEY,
      region TEXT,
      amount INTEGER
    );
    INSERT INTO sales VALUES
      (1, 'east', 100),
      (2, 'east', 200),
      (3, 'west', 150),
      (4, 'west', 150),
      (5, 'north', 50);
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

    fn run(&mut self, sql: &str) -> Result<Option<QueryResult>, String> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(|error| format!("parse: {error}"))?;
        let mut last = None;
        for statement in statements {
            let plan = {
                let catalog = self.catalog.read().unwrap();
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

    fn run_ok(&mut self, sql: &str) -> Option<QueryResult> {
        self.run(sql)
            .unwrap_or_else(|error| panic!("expected `{}` to succeed: {error}", sql.trim()))
    }

    fn run_err(&mut self, sql: &str) -> String {
        self.run(sql)
            .expect_err(&format!("expected `{}` to fail", sql.trim()))
    }
}

fn query(harness: &mut Harness, sql: &str) -> QueryResult {
    harness
        .run_ok(sql)
        .unwrap_or_else(|| panic!("expected `{}` to return rows", sql.trim()))
}

fn integer_rows(result: &QueryResult) -> Vec<Vec<i64>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    SqlValue::Integer(value) => i64::from(*value),
                    SqlValue::BigInt(value) => *value,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn named_windows_support_direct_reference_and_inheritance() {
    let mut harness = Harness::new();
    let result = query(
        &mut harness,
        "SELECT id, \
                ROW_NUMBER() OVER ordered AS row_number, \
                SUM(amount) OVER running AS running_total \
         FROM sales \
         WINDOW ordered AS (base ORDER BY amount DESC, id), \
                running AS (base ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
                base AS (PARTITION BY region) \
         ORDER BY id",
    );

    assert_eq!(
        integer_rows(&result),
        vec![
            vec![1, 2, 100],
            vec![2, 1, 300],
            vec![3, 1, 150],
            vec![4, 2, 300],
            vec![5, 1, 50],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn qualify_filters_window_results_and_resolves_projection_aliases() {
    let mut harness = Harness::new();
    let result = query(
        &mut harness,
        "SELECT id, ROW_NUMBER() OVER by_region AS row_number \
         FROM sales \
         WINDOW by_region AS (PARTITION BY region ORDER BY amount DESC, id) \
         QUALIFY row_number = 1 \
         ORDER BY id",
    );

    assert_eq!(
        integer_rows(&result),
        vec![vec![2, 1], vec![3, 1], vec![5, 1]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn qualify_matches_an_equivalent_subquery_rewrite() {
    let mut harness = Harness::new();
    let qualify = query(
        &mut harness,
        "SELECT id, region, ROW_NUMBER() OVER w AS row_number \
         FROM sales WINDOW w AS (PARTITION BY region ORDER BY amount DESC, id) \
         QUALIFY row_number = 1 ORDER BY id",
    );
    let rewritten = query(
        &mut harness,
        "SELECT id, region, row_number FROM ( \
           SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) AS row_number \
           FROM sales \
         ) ranked WHERE row_number = 1 ORDER BY id",
    );

    assert_eq!(qualify.columns, rewritten.columns);
    assert_eq!(qualify.rows, rewritten.rows);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn qualify_runs_after_group_and_having_but_before_distinct_and_order() {
    let mut harness = Harness::new();
    let result = query(
        &mut harness,
        "SELECT DISTINCT SUM(amount) AS total, \
                DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank \
         FROM sales \
         GROUP BY region \
         HAVING SUM(amount) >= 50 \
         QUALIFY sales_rank = 1 \
         ORDER BY total",
    );

    assert_eq!(integer_rows(&result), vec![vec![300, 1]]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn named_window_validation_is_deterministic() {
    let mut harness = Harness::new();
    for (sql, expected) in [
        ("SELECT ROW_NUMBER() OVER missing FROM sales", "not defined"),
        (
            "SELECT ROW_NUMBER() OVER w FROM sales WINDOW w AS (other), other AS (w)",
            "cycle",
        ),
        (
            "SELECT ROW_NUMBER() OVER w FROM sales WINDOW w AS (ORDER BY id), W AS (ORDER BY amount)",
            "defined more than once",
        ),
        (
            "SELECT ROW_NUMBER() OVER (base PARTITION BY region) FROM sales WINDOW base AS (ORDER BY id)",
            "PARTITION BY",
        ),
        (
            "SELECT ROW_NUMBER() OVER (base ORDER BY amount) FROM sales WINDOW base AS (ORDER BY id)",
            "ORDER BY",
        ),
        (
            "SELECT ROW_NUMBER() OVER (base ROWS BETWEEN CURRENT ROW AND CURRENT ROW) \
             FROM sales WINDOW base AS (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
            "frame",
        ),
        (
            "SELECT ROW_NUMBER() OVER ok FROM sales \
             WINDOW ok AS (ORDER BY id), first_bad AS (missing), \
                    loop_a AS (loop_b), loop_b AS (loop_a)",
            "missing",
        ),
    ] {
        let error = harness.run_err(sql);
        assert!(
            error
                .to_ascii_lowercase()
                .contains(&expected.to_ascii_lowercase()),
            "expected {expected:?} from {sql:?}, got {error}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn named_window_scope_stops_at_each_query_block() {
    let mut harness = Harness::new();
    let result = query(
        &mut harness,
        "SELECT id FROM ( \
           SELECT id, ROW_NUMBER() OVER inner_w AS row_number \
           FROM sales WINDOW inner_w AS (ORDER BY id) \
         ) ranked WHERE row_number = 1",
    );
    assert_eq!(integer_rows(&result), vec![vec![1]]);

    let error = harness.run_err(
        "SELECT id FROM ( \
           SELECT id, ROW_NUMBER() OVER outer_w AS row_number FROM sales \
         ) ranked WINDOW outer_w AS (ORDER BY id)",
    );
    assert!(
        error.to_ascii_lowercase().contains("not defined"),
        "{error}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn qualify_requires_boolean_and_a_window_in_the_query_block() {
    let mut harness = Harness::new();
    let qualify_only = query(
        &mut harness,
        "SELECT id FROM sales \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) = 1 \
         ORDER BY id",
    );
    assert_eq!(integer_rows(&qualify_only), vec![vec![2], vec![3], vec![5]]);

    let non_boolean = harness.run_err(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_number \
         FROM sales QUALIFY amount",
    );
    assert!(non_boolean.contains("Boolean"), "{non_boolean}");

    let without_window = harness.run_err("SELECT id FROM sales QUALIFY amount > 100");
    assert!(
        without_window
            .to_ascii_lowercase()
            .contains("requires at least one window"),
        "{without_window}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn logical_plan_places_qualify_between_window_and_projection() {
    let harness = Harness::new();
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "SELECT DISTINCT SUM(amount) AS total, \
                DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank \
         FROM sales GROUP BY region HAVING SUM(amount) >= 50 \
         QUALIFY sales_rank = 1 ORDER BY total",
    )
    .unwrap()
    .remove(0);
    let plan = {
        let catalog = harness.catalog.read().unwrap();
        Planner::new(&*catalog).plan(&statement).unwrap()
    };

    let LogicalPlan::Sort { input, .. } = plan else {
        panic!("expected ORDER BY Sort");
    };
    let LogicalPlan::Aggregate { input, .. } = *input else {
        panic!("expected DISTINCT Aggregate below Sort");
    };
    let LogicalPlan::Project { input, .. } = *input else {
        panic!("expected visible Project below DISTINCT");
    };
    let LogicalPlan::Filter { input, .. } = *input else {
        panic!("expected QUALIFY Filter below Project");
    };
    let LogicalPlan::Window { input, .. } = *input else {
        panic!("expected Window below QUALIFY");
    };
    assert!(
        matches!(*input, LogicalPlan::Aggregate { .. }),
        "expected grouped Aggregate/HAVING below Window"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn duckdb_and_datafusion_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/named_window_qualify_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["documented_against"]["duckdb"], "1.5.5");
    assert_eq!(reference["documented_against"]["datafusion"], "54.0.0");

    let mut harness = Harness::new();
    for case in reference["cases"].as_array().expect("reference cases") {
        let result = query(&mut harness, case["sql"].as_str().expect("reference SQL"));
        let columns = result
            .columns
            .iter()
            .map(|column| serde_json::json!(column.name))
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::Value::Array(columns),
            case["columns"],
            "{} columns",
            case["name"]
        );
        assert_eq!(
            serde_json::json!(integer_rows(&result)),
            case["rows"],
            "{} rows",
            case["name"]
        );
    }
}
