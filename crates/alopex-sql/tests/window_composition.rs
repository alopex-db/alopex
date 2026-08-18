//! Composition of grouped aggregation and window functions (issue #142).
//!
//! The contract under test is the SQL logical evaluation order:
//! aggregate -> HAVING -> window -> projection -> DISTINCT -> ORDER BY.
//! The integrated suite also verifies that explicit `ROWS` / `RANGE` metadata
//! survives aggregate-expression rewriting into the window stage.

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::{execute_query, execute_query_streaming};
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{LogicalPlan, Planner};
use alopex_sql::storage::{SqlValue, TxnBridge};

const FIXTURE: &str = r#"
    CREATE TABLE sales (
      id INTEGER PRIMARY KEY,
      region TEXT,
      amount INTEGER
    );
    INSERT INTO sales VALUES (1, 'east', 100);
    INSERT INTO sales VALUES (2, 'east', 200);
    INSERT INTO sales VALUES (3, 'west', 150);
    INSERT INTO sales VALUES (4, 'west', 150);
    INSERT INTO sales VALUES (5, 'north', 50);
    INSERT INTO sales VALUES (6, 'south', 400);
"#;

const COMPOSED_SQL: &str = "\
    SELECT region, SUM(amount) AS total, \
           RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank, \
           SUM(SUM(amount)) OVER () AS retained_total \
    FROM sales \
    GROUP BY region \
    HAVING SUM(amount) >= 300 \
    ORDER BY sales_rank, region";

fn setup() -> (Arc<MemoryKV>, Arc<RwLock<MemoryCatalog>>) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
    for statement in Parser::parse_sql(&AlopexDialect, FIXTURE).expect("parse fixture") {
        let plan = {
            let guard = catalog.read().expect("catalog read");
            Planner::new(&*guard)
                .plan(&statement)
                .expect("plan fixture statement")
        };
        executor.execute(plan).expect("execute fixture statement");
    }
    (store, catalog)
}

fn plan(catalog: &Arc<RwLock<MemoryCatalog>>, sql: &str) -> LogicalPlan {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse query");
    assert_eq!(statements.len(), 1, "expected one query");
    let guard = catalog.read().expect("catalog read");
    Planner::new(&*guard)
        .plan(&statements[0])
        .expect("plan query")
}

fn materialized(
    store: &Arc<MemoryKV>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> QueryResult {
    let plan = plan(catalog, sql);
    let bridge = TxnBridge::new(Arc::clone(store));
    let mut txn = bridge.begin_read().expect("begin materialized read");
    let guard = catalog.read().expect("catalog read");
    match execute_query(&mut txn, &*guard, plan).expect("execute materialized query") {
        ExecutionResult::Query(result) => result,
        other => panic!("expected query result, got {other:?}"),
    }
}

fn streaming(
    store: &Arc<MemoryKV>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> QueryResult {
    let plan = plan(catalog, sql);
    let bridge = TxnBridge::new(Arc::clone(store));
    let mut txn = bridge.begin_read().expect("begin streaming read");
    let guard = catalog.read().expect("catalog read");
    let mut stream =
        execute_query_streaming(&mut txn, &*guard, plan).expect("execute streaming query");
    let columns = stream.columns().to_vec();
    let mut rows = Vec::new();
    while let Some(row) = stream.next_row().expect("read streaming row") {
        rows.push(row);
    }
    QueryResult { columns, rows }
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_string())
}

fn value_as_json(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(value) => serde_json::json!(value),
        SqlValue::BigInt(value) => serde_json::json!(value),
        SqlValue::Float(value) => serde_json::json!(value),
        SqlValue::Double(value) => serde_json::json!(value),
        SqlValue::Text(value) => serde_json::json!(value),
        SqlValue::Boolean(value) => serde_json::json!(value),
        other => panic!("reference fixture does not use {other:?}"),
    }
}

fn assert_composed_rows(result: &QueryResult) {
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["region", "total", "sales_rank", "retained_total"]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                text("south"),
                SqlValue::BigInt(400),
                SqlValue::BigInt(1),
                SqlValue::BigInt(1_000),
            ],
            vec![
                text("east"),
                SqlValue::BigInt(300),
                SqlValue::BigInt(2),
                SqlValue::BigInt(1_000),
            ],
            vec![
                text("west"),
                SqlValue::BigInt(300),
                SqlValue::BigInt(2),
                SqlValue::BigInt(1_000),
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn plan_orders_aggregate_having_window_projection_distinct_and_sort() {
    let (_, catalog) = setup();
    let plan = plan(
        &catalog,
        "SELECT DISTINCT region, SUM(amount) AS total, \
                RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank \
         FROM sales GROUP BY region HAVING SUM(amount) >= 300 \
         ORDER BY sales_rank, region",
    );

    let LogicalPlan::Sort { input, .. } = plan else {
        panic!("ORDER BY must be the outermost relational stage");
    };
    let LogicalPlan::Aggregate {
        input,
        aggregates,
        having,
        ..
    } = *input
    else {
        panic!("DISTINCT must run after projection");
    };
    assert!(
        aggregates.is_empty(),
        "DISTINCT is not an ordinary aggregate"
    );
    assert!(having.is_none(), "DISTINCT must not own HAVING");
    let LogicalPlan::Project { input, .. } = *input else {
        panic!("projection must run after window and before DISTINCT");
    };
    let LogicalPlan::Window { input, .. } = *input else {
        panic!("window must run after grouped aggregation");
    };
    let LogicalPlan::Aggregate { having, .. } = *input else {
        panic!("grouped aggregation must feed the window stage");
    };
    assert!(
        having.is_some(),
        "HAVING must be owned by grouped aggregation"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn grouped_aggregates_feed_window_order_and_window_aggregate_after_having() {
    let (store, catalog) = setup();
    assert_composed_rows(&materialized(&store, &catalog, COMPOSED_SQL));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn grouped_aggregate_window_rewrite_preserves_explicit_frame() {
    let (store, catalog) = setup();
    let result = materialized(
        &store,
        &catalog,
        "SELECT region, SUM(amount) AS total, \
                SUM(SUM(amount)) OVER (ORDER BY SUM(amount), region \
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling_total \
         FROM sales GROUP BY region ORDER BY total, region",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![text("north"), SqlValue::BigInt(50), SqlValue::BigInt(50)],
            vec![text("east"), SqlValue::BigInt(300), SqlValue::BigInt(350)],
            vec![text("west"), SqlValue::BigInt(300), SqlValue::BigInt(600)],
            vec![text("south"), SqlValue::BigInt(400), SqlValue::BigInt(700)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn distinct_collapses_equal_rows_after_window_evaluation() {
    let (store, catalog) = setup();
    let result = materialized(
        &store,
        &catalog,
        "SELECT DISTINCT SUM(amount) AS total, \
                DENSE_RANK() OVER (ORDER BY SUM(amount)) AS sales_rank \
         FROM sales GROUP BY region ORDER BY total",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::BigInt(50), SqlValue::BigInt(1)],
            vec![SqlValue::BigInt(300), SqlValue::BigInt(2)],
            vec![SqlValue::BigInt(400), SqlValue::BigInt(3)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn duckdb_and_datafusion_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/window_composition_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["verified_with"]["duckdb"], "1.5.5");
    assert_eq!(reference["verified_with"]["datafusion"], "54.0.0");

    let (store, catalog) = setup();
    for case in reference["cases"].as_array().expect("reference cases") {
        let result = materialized(
            &store,
            &catalog,
            case["sql"].as_str().expect("reference SQL"),
        );
        let columns = result
            .columns
            .iter()
            .map(|column| serde_json::json!(column.name))
            .collect::<Vec<_>>();
        let rows = result
            .rows
            .iter()
            .map(|row| row.iter().map(value_as_json).collect::<Vec<_>>())
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn window_alias_is_visible_to_outer_order_by() {
    let (store, catalog) = setup();
    let result = materialized(
        &store,
        &catalog,
        "SELECT region, ROW_NUMBER() OVER (ORDER BY SUM(amount), region) AS rn \
         FROM sales GROUP BY region ORDER BY rn DESC",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![text("south"), SqlValue::BigInt(4)],
            vec![text("west"), SqlValue::BigInt(3)],
            vec![text("east"), SqlValue::BigInt(2)],
            vec![text("north"), SqlValue::BigInt(1)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn non_projected_outer_sort_keys_remain_available_after_window_projection() {
    let (store, catalog) = setup();
    let result = materialized(
        &store,
        &catalog,
        "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn \
         FROM sales ORDER BY amount DESC, id",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::BigInt(6)],
            vec![SqlValue::BigInt(2)],
            vec![SqlValue::BigInt(3)],
            vec![SqlValue::BigInt(4)],
            vec![SqlValue::BigInt(1)],
            vec![SqlValue::BigInt(5)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_and_materialized_composition_are_exactly_equal() {
    let (store, catalog) = setup();
    let materialized = materialized(&store, &catalog, COMPOSED_SQL);
    let streaming = streaming(&store, &catalog, COMPOSED_SQL);
    assert_eq!(streaming.columns, materialized.columns);
    assert_eq!(streaming.rows, materialized.rows);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn illegal_aggregate_and_window_nesting_is_rejected_during_planning() {
    let (_, catalog) = setup();
    for (sql, expected) in [
        (
            "SELECT SUM(ROW_NUMBER() OVER (ORDER BY id)) FROM sales",
            "aggregate functions cannot contain window functions",
        ),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY ROW_NUMBER() OVER (ORDER BY id)) FROM sales",
            "nested window functions",
        ),
        (
            "SELECT SUM(SUM(amount)) FROM sales",
            "nested aggregate functions",
        ),
        (
            "SELECT SUM(amount) FROM sales HAVING ROW_NUMBER() OVER () > 0",
            "HAVING cannot contain window functions",
        ),
        (
            "SELECT SUM(amount), ROW_NUMBER() OVER () AS rn FROM sales HAVING rn > 0",
            "HAVING cannot contain window functions",
        ),
    ] {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse invalid query");
        let guard = catalog.read().expect("catalog read");
        let error = Planner::new(&*guard)
            .plan(&statements[0])
            .expect_err("invalid nesting must fail during planning")
            .to_string();
        assert!(
            error.contains(expected),
            "`{sql}` must report `{expected}`, got: {error}"
        );
    }
}
