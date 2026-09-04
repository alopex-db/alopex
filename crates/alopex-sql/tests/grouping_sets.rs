//! GROUPING SETS / ROLLUP / CUBE public behavior (issue #149, contract 0.13.0).

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

    /// Harness with the shared four-row sales dataset. Row 4 carries a real
    /// NULL region so placeholder NULLs can be distinguished (D7).
    fn with_sales() -> Self {
        let mut harness = Self::new();
        harness
            .run(
                "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, product TEXT, \
                 amount INTEGER); \
                 INSERT INTO sales VALUES (1, 'east', 'a', 10), (2, 'east', 'b', 20), \
                 (3, 'west', 'a', 30), (4, NULL, 'b', 40)",
            )
            .expect("create sales dataset");
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
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.into())
}

#[test]
fn rollup_distinguishes_placeholder_null_from_data_null() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS total, GROUPING(region) AS g FROM sales \
         GROUP BY ROLLUP(region) ORDER BY g, region NULLS FIRST",
    );

    // The real-NULL region row keeps g = 0; only the grand total row has
    // g = 1 even though both print region as NULL (D7).
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(40), SqlValue::BigInt(0)],
            vec![text("east"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
            vec![text("west"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
            vec![SqlValue::Null, SqlValue::BigInt(100), SqlValue::BigInt(1)],
        ]
    );
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["region", "total", "g"]
    );
}

#[test]
fn cube_emits_all_subsets_with_multi_argument_grouping() {
    let result = Harness::with_sales().query(
        "SELECT region, product, SUM(amount) AS total, GROUPING(region, product) AS gid \
         FROM sales GROUP BY CUBE(region, product) \
         ORDER BY gid, region NULLS FIRST, product NULLS FIRST",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Null,
                text("b"),
                SqlValue::BigInt(40),
                SqlValue::BigInt(0)
            ],
            vec![
                text("east"),
                text("a"),
                SqlValue::BigInt(10),
                SqlValue::BigInt(0)
            ],
            vec![
                text("east"),
                text("b"),
                SqlValue::BigInt(20),
                SqlValue::BigInt(0)
            ],
            vec![
                text("west"),
                text("a"),
                SqlValue::BigInt(30),
                SqlValue::BigInt(0)
            ],
            vec![
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::BigInt(40),
                SqlValue::BigInt(1)
            ],
            vec![
                text("east"),
                SqlValue::Null,
                SqlValue::BigInt(30),
                SqlValue::BigInt(1)
            ],
            vec![
                text("west"),
                SqlValue::Null,
                SqlValue::BigInt(30),
                SqlValue::BigInt(1)
            ],
            vec![
                SqlValue::Null,
                text("a"),
                SqlValue::BigInt(40),
                SqlValue::BigInt(2)
            ],
            vec![
                SqlValue::Null,
                text("b"),
                SqlValue::BigInt(60),
                SqlValue::BigInt(2)
            ],
            vec![
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::BigInt(100),
                SqlValue::BigInt(3)
            ],
        ]
    );
}

#[test]
fn grouping_id_is_an_accepted_alias_of_grouping() {
    let mut harness = Harness::with_sales();
    let with_grouping = harness.query(
        "SELECT region, product, GROUPING(region, product) AS gid FROM sales \
         GROUP BY CUBE(region, product) \
         ORDER BY gid, region NULLS FIRST, product NULLS FIRST",
    );
    let with_grouping_id = harness.query(
        "SELECT region, product, GROUPING_ID(region, product) AS gid FROM sales \
         GROUP BY CUBE(region, product) \
         ORDER BY gid, region NULLS FIRST, product NULLS FIRST",
    );

    assert_eq!(with_grouping.rows, with_grouping_id.rows);
}

#[test]
fn grouping_sets_include_the_empty_set() {
    let result = Harness::with_sales().query(
        "SELECT region, product, COUNT(*) AS c, GROUPING(region, product) AS gid \
         FROM sales GROUP BY GROUPING SETS ((region), (product), ()) \
         ORDER BY gid, region NULLS FIRST, product NULLS FIRST",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::BigInt(1),
                SqlValue::BigInt(1)
            ],
            vec![
                text("east"),
                SqlValue::Null,
                SqlValue::BigInt(2),
                SqlValue::BigInt(1)
            ],
            vec![
                text("west"),
                SqlValue::Null,
                SqlValue::BigInt(1),
                SqlValue::BigInt(1)
            ],
            vec![
                SqlValue::Null,
                text("a"),
                SqlValue::BigInt(2),
                SqlValue::BigInt(2)
            ],
            vec![
                SqlValue::Null,
                text("b"),
                SqlValue::BigInt(2),
                SqlValue::BigInt(2)
            ],
            vec![
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::BigInt(4),
                SqlValue::BigInt(3)
            ],
        ]
    );
}

#[test]
fn ordinary_group_by_cross_products_with_rollup() {
    let result = Harness::with_sales().query(
        "SELECT region, product, SUM(amount) AS total FROM sales \
         GROUP BY region, ROLLUP(product) \
         ORDER BY region NULLS FIRST, GROUPING(product), product NULLS FIRST",
    );

    // D2: GROUP BY region, ROLLUP(product) = {region} x {(product), ()},
    // so each region gains one subtotal row with a product placeholder.
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, text("b"), SqlValue::BigInt(40)],
            vec![SqlValue::Null, SqlValue::Null, SqlValue::BigInt(40)],
            vec![text("east"), text("a"), SqlValue::BigInt(10)],
            vec![text("east"), text("b"), SqlValue::BigInt(20)],
            vec![text("east"), SqlValue::Null, SqlValue::BigInt(30)],
            vec![text("west"), text("a"), SqlValue::BigInt(30)],
            vec![text("west"), SqlValue::Null, SqlValue::BigInt(30)],
        ]
    );
}

#[test]
fn duplicate_grouping_sets_emit_duplicate_rows() {
    let result = Harness::with_sales().query(
        "SELECT region, COUNT(*) AS c FROM sales \
         GROUP BY GROUPING SETS ((region), (region)) ORDER BY region NULLS FIRST",
    );

    // D3: each listed set produces rows independently; no deduplication.
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(1)],
            vec![SqlValue::Null, SqlValue::BigInt(1)],
            vec![text("east"), SqlValue::BigInt(2)],
            vec![text("east"), SqlValue::BigInt(2)],
            vec![text("west"), SqlValue::BigInt(1)],
            vec![text("west"), SqlValue::BigInt(1)],
        ]
    );
}

#[test]
fn having_composes_with_grouping() {
    let mut harness = Harness::with_sales();

    let totals_only = harness.query(
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY ROLLUP(region) \
         HAVING GROUPING(region) = 1",
    );
    assert_eq!(
        totals_only.rows,
        vec![vec![SqlValue::Null, SqlValue::BigInt(100)]]
    );

    let details = harness.query(
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY ROLLUP(region) \
         HAVING GROUPING(region) = 0 AND SUM(amount) >= 30 \
         ORDER BY region NULLS FIRST",
    );
    assert_eq!(
        details.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(40)],
            vec![text("east"), SqlValue::BigInt(30)],
            vec![text("west"), SqlValue::BigInt(30)],
        ]
    );
}

#[test]
fn order_by_breaks_aggregate_ties_deterministically() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS t, GROUPING(region) AS g FROM sales \
         GROUP BY ROLLUP(region) ORDER BY t DESC, g, region NULLS FIRST",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(100), SqlValue::BigInt(1)],
            vec![SqlValue::Null, SqlValue::BigInt(40), SqlValue::BigInt(0)],
            vec![text("east"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
            vec![text("west"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
        ]
    );
}

#[test]
fn window_functions_compose_with_rollup() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS t, RANK() OVER (ORDER BY SUM(amount) DESC) AS r \
         FROM sales GROUP BY ROLLUP(region) ORDER BY r, region NULLS FIRST",
    );

    // D11: the grand-total row participates in the window like any group row.
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(100), SqlValue::BigInt(1)],
            vec![SqlValue::Null, SqlValue::BigInt(40), SqlValue::BigInt(2)],
            vec![text("east"), SqlValue::BigInt(30), SqlValue::BigInt(3)],
            vec![text("west"), SqlValue::BigInt(30), SqlValue::BigInt(3)],
        ]
    );
}

#[test]
fn grouping_composes_with_window_order_by() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS t, \
         RANK() OVER (ORDER BY GROUPING(region), SUM(amount) DESC) AS r \
         FROM sales GROUP BY ROLLUP(region) ORDER BY r, region NULLS FIRST",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(40), SqlValue::BigInt(1)],
            vec![text("east"), SqlValue::BigInt(30), SqlValue::BigInt(2)],
            vec![text("west"), SqlValue::BigInt(30), SqlValue::BigInt(2)],
            vec![SqlValue::Null, SqlValue::BigInt(100), SqlValue::BigInt(4)],
        ]
    );
}

#[test]
fn empty_grouping_set_aggregates_all_rows() {
    let mut harness = Harness::with_sales();

    let bare = harness.query("SELECT COUNT(*) AS c, SUM(amount) AS total FROM sales GROUP BY ()");
    assert_eq!(
        bare.rows,
        vec![vec![SqlValue::BigInt(4), SqlValue::BigInt(100)]]
    );

    let spelled = harness
        .query("SELECT COUNT(*) AS c, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS (())");
    assert_eq!(spelled.rows, bare.rows);
}

#[test]
fn global_sets_emit_one_row_for_empty_input() {
    let mut harness = Harness::with_sales();
    let result = harness.query(
        "SELECT region, COUNT(*) AS c, GROUPING(region) AS g FROM sales \
         WHERE amount > 1000 GROUP BY ROLLUP(region) ORDER BY g",
    );

    // Only the () set groups over no key, so an empty input still yields
    // exactly its one global row (PostgreSQL/DuckDB semantics).
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Null,
            SqlValue::BigInt(0),
            SqlValue::BigInt(1)
        ]]
    );
}

#[test]
fn grouping_placement_errors_are_stable() {
    let ungrouped = Harness::with_sales()
        .run("SELECT GROUPING(region) FROM sales")
        .expect_err("GROUPING outside a grouped query must fail");
    assert!(
        ungrouped.contains("GROUPING is only allowed in grouped queries"),
        "unexpected error: {ungrouped}"
    );

    let non_key = Harness::with_sales()
        .run("SELECT region, GROUPING(amount) AS g FROM sales GROUP BY region")
        .expect_err("GROUPING of a non-key column must fail");
    assert!(
        non_key.contains("arguments to GROUPING must be grouping expressions"),
        "unexpected error: {non_key}"
    );

    let in_where = Harness::with_sales()
        .run("SELECT region FROM sales WHERE GROUPING(region) = 0 GROUP BY region")
        .expect_err("GROUPING in WHERE must fail");
    assert!(
        in_where.contains("GROUPING is not allowed in WHERE"),
        "unexpected error: {in_where}"
    );

    let in_group_by = Harness::with_sales()
        .run("SELECT region FROM sales GROUP BY region, GROUPING(region)")
        .expect_err("GROUPING inside GROUP BY must fail");
    assert!(
        in_group_by.contains("GROUPING is not allowed in GROUP BY"),
        "unexpected error: {in_group_by}"
    );

    let in_aggregate = Harness::with_sales()
        .run("SELECT region, SUM(GROUPING(region)) FROM sales GROUP BY ROLLUP(region)")
        .expect_err("GROUPING inside an aggregate argument must fail");
    assert!(
        in_aggregate.contains("GROUPING cannot appear inside aggregate function arguments"),
        "unexpected error: {in_aggregate}"
    );
}

#[test]
fn grouping_set_expansion_is_bounded() {
    let wide_cube = Harness::with_sales()
        .run(
            "SELECT COUNT(*) FROM sales GROUP BY CUBE(id, region, product, amount, id, region, \
             product, amount, id, region, product, amount, id)",
        )
        .expect_err("a 13-column CUBE must exceed the grouping-set bound");
    assert!(
        wide_cube.contains("too many grouping sets (max 4096)"),
        "unexpected error: {wide_cube}"
    );

    let cross_product = Harness::with_sales()
        .run(
            "SELECT COUNT(*) FROM sales GROUP BY CUBE(id, region, product, amount), \
             CUBE(id, region, product, amount), CUBE(id, region, product, amount), ROLLUP(id)",
        )
        .expect_err("a cross product beyond 4096 sets must fail");
    assert!(
        cross_product.contains("too many grouping sets (max 4096)"),
        "unexpected error: {cross_product}"
    );
}

#[test]
fn plain_group_by_stays_free_of_grouping_id_columns() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region \
         ORDER BY region NULLS FIRST",
    );

    // D12: the unmodified GROUP BY path is unchanged — no hidden column
    // leaks into the output schema and GROUPING folds to constant 0.
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["region", "total"]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(40)],
            vec![text("east"), SqlValue::BigInt(30)],
            vec![text("west"), SqlValue::BigInt(30)],
        ]
    );

    let constant_grouping = Harness::with_sales().query(
        "SELECT region, GROUPING(region) AS g FROM sales GROUP BY region \
         ORDER BY region NULLS FIRST",
    );
    assert_eq!(
        constant_grouping.rows,
        vec![
            vec![SqlValue::Null, SqlValue::BigInt(0)],
            vec![text("east"), SqlValue::BigInt(0)],
            vec![text("west"), SqlValue::BigInt(0)],
        ]
    );
}

#[test]
fn rollup_composes_with_limit_and_offset() {
    let result = Harness::with_sales().query(
        "SELECT region, SUM(amount) AS total, GROUPING(region) AS g FROM sales \
         GROUP BY ROLLUP(region) ORDER BY g, region NULLS FIRST LIMIT 2 OFFSET 1",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![text("east"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
            vec![text("west"), SqlValue::BigInt(30), SqlValue::BigInt(0)],
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
        serde_json::from_str(include_str!("fixtures/grouping_sets_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["documented_against"]["duckdb"], "1.5.5");
    assert_eq!(reference["documented_against"]["datafusion"], "54.0.0");

    let mut harness = Harness::with_sales();
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
