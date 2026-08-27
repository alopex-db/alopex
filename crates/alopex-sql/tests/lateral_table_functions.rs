//! LATERAL joins, FROM-clause table functions, and relation alias column
//! lists (issue #151, contract 0.14.0).

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

    /// `t` has two rows; `s` holds one child row for `t.id = 1` and none for
    /// `t.id = 2`, which is what makes the LEFT JOIN LATERAL padding visible.
    fn with_parent_child() -> Self {
        let mut harness = Self::new();
        harness
            .run(
                "CREATE TABLE t (id INT PRIMARY KEY); \
                 CREATE TABLE s (id INT PRIMARY KEY, t_id INT, val INT); \
                 INSERT INTO t VALUES (1), (2); \
                 INSERT INTO s VALUES (10, 1, 100), (11, 1, 200)",
            )
            .expect("create parent/child dataset");
        harness
    }

    fn with_vectors() -> Self {
        let mut harness = Self::new();
        harness
            .run(
                "CREATE TABLE d (id INT PRIMARY KEY, emb VECTOR(2)); \
                 INSERT INTO d VALUES (1, [1.0, 2.0]), (2, [3.0, 4.0])",
            )
            .expect("create vector dataset");
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

    fn plan(&mut self, sql: &str) -> alopex_sql::planner::logical_plan::LogicalPlan {
        let statement = Parser::parse_sql(&AlopexDialect, sql)
            .expect("parse")
            .pop()
            .expect("one statement");
        let catalog = self.catalog.read().expect("catalog read");
        Planner::new(&*catalog).plan(&statement).expect("plan")
    }

    fn error(&mut self, sql: &str) -> String {
        match self.run(sql) {
            Ok(_) => panic!("expected `{}` to fail", sql.trim()),
            Err(error) => error,
        }
    }
}

fn column_names(result: &QueryResult) -> Vec<&str> {
    result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

fn float(value: f32) -> SqlValue {
    SqlValue::Float(value)
}

fn int(value: i32) -> SqlValue {
    SqlValue::Integer(value)
}

// === Table functions =====================================================

#[test]
fn unnest_expands_a_vector_literal_into_one_row_per_element() {
    let result = Harness::new().query("SELECT u.unnest FROM UNNEST([1.0, 2.0, 3.0]) AS u");

    assert_eq!(
        result.rows,
        vec![vec![float(1.0)], vec![float(2.0)], vec![float(3.0)]]
    );
    assert_eq!(column_names(&result), vec!["unnest"]);
}

#[test]
fn a_table_function_without_an_alias_keeps_its_function_name() {
    let result = Harness::new().query("SELECT unnest FROM UNNEST([5.0])");

    assert_eq!(result.rows, vec![vec![float(5.0)]]);
    assert_eq!(column_names(&result), vec!["unnest"]);
}

#[test]
fn unnest_of_null_produces_no_rows() {
    let result = Harness::new().query("SELECT x FROM UNNEST(NULL) AS t(x)");

    assert!(result.rows.is_empty());
    assert_eq!(column_names(&result), vec!["x"]);
}

#[test]
fn an_unknown_table_function_is_rejected_by_name() {
    let message = Harness::new().error("SELECT * FROM frobnicate(1) AS f");

    assert!(
        message.contains("frobnicate"),
        "unexpected message: {message}"
    );
}

#[test]
fn unnest_requires_a_vector_argument() {
    let message = Harness::new().error("SELECT * FROM UNNEST(1) AS u");

    assert!(message.contains("VECTOR"), "unexpected message: {message}");
}

#[test]
fn unnest_takes_exactly_one_argument() {
    let message = Harness::new().error("SELECT * FROM UNNEST([1.0], [2.0]) AS u");

    assert!(message.contains("UNNEST"), "unexpected message: {message}");
}

#[test]
#[cfg(feature = "generate_series")]
fn generate_series_supports_inclusive_positive_and_negative_integer_ranges() {
    let ascending = Harness::new().query("SELECT * FROM GENERATE_SERIES(1, 3) AS g");
    assert_eq!(
        ascending.rows,
        vec![vec![int(1)], vec![int(2)], vec![int(3)]]
    );

    let descending = Harness::new().query("SELECT n FROM GENERATE_SERIES(3, 1, -1) AS g(n)");
    assert_eq!(
        descending.rows,
        vec![vec![int(3)], vec![int(2)], vec![int(1)]]
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn generate_series_returns_empty_when_step_moves_away_from_stop() {
    assert!(
        Harness::new()
            .query("SELECT * FROM GENERATE_SERIES(3, 1)")
            .rows
            .is_empty()
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn generate_series_rejects_zero_step_and_bounded_output() {
    let zero = Harness::new().error("SELECT * FROM GENERATE_SERIES(1, 3, 0)");
    assert!(
        zero.contains("step must not be zero"),
        "unexpected message: {zero}"
    );

    let too_many = Harness::new().error("SELECT * FROM GENERATE_SERIES(1, 100002)");
    assert!(
        too_many.contains("100000 rows"),
        "unexpected message: {too_many}"
    );

    let overflow = Harness::new()
        .error("SELECT * FROM GENERATE_SERIES(9223372036854775806, 9223372036854775807, 2)");
    assert!(
        overflow.contains("integer overflow"),
        "unexpected message: {overflow}"
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn generate_series_composes_with_lateral_cte_join_and_window() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE bounds (id INT PRIMARY KEY, stop INT); INSERT INTO bounds VALUES (1, 2)")
        .expect("create bounds");

    let result = harness.query(
        "WITH expanded AS (\
             SELECT b.id, g.n FROM bounds AS b \
             CROSS JOIN LATERAL GENERATE_SERIES(1, b.stop) AS g(n)\
         ) \
         SELECT e.n, ROW_NUMBER() OVER (ORDER BY e.n) AS rn \
         FROM expanded AS e JOIN bounds AS b ON e.id = b.id ORDER BY e.n",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), SqlValue::BigInt(1)],
            vec![int(2), SqlValue::BigInt(2)],
        ]
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn generate_series_supports_inclusive_timestamp_ranges() {
    let ascending = Harness::new().query(
        "SELECT g.ts FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00', \
             INTERVAL '1 day') AS g(ts)",
    );
    assert_eq!(
        ascending.rows,
        vec![
            vec![SqlValue::Timestamp(1_704_067_200_000_000)],
            vec![SqlValue::Timestamp(1_704_153_600_000_000)],
            vec![SqlValue::Timestamp(1_704_240_000_000_000)],
        ]
    );

    let descending = Harness::new().query(
        "SELECT g.ts FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-03 00:00:00', TIMESTAMP '2024-01-01 00:00:00', \
             INTERVAL '-1 day') AS g(ts)",
    );
    assert_eq!(
        descending.rows,
        ascending.rows.into_iter().rev().collect::<Vec<_>>()
    );

    let single = Harness::new().query(
        "SELECT * FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00', \
             INTERVAL '1 month')",
    );
    assert_eq!(
        single.rows,
        vec![vec![SqlValue::Timestamp(1_704_067_200_000_000)]]
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn timestamp_generate_series_rejects_zero_and_bounded_output() {
    let zero = Harness::new().error(
        "SELECT * FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', \
             INTERVAL '0 seconds')",
    );
    assert!(
        zero.contains("step must not be zero"),
        "unexpected message: {zero}"
    );

    let empty = Harness::new().query(
        "SELECT * FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-03 00:00:00', TIMESTAMP '2024-01-01 00:00:00', \
             INTERVAL '1 day')",
    );
    assert!(empty.rows.is_empty());

    let too_many = Harness::new().error(
        "SELECT * FROM GENERATE_SERIES(\
             TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00', \
             INTERVAL '1 second')",
    );
    assert!(
        too_many.contains("100000 rows"),
        "unexpected message: {too_many}"
    );
}

#[test]
#[cfg(feature = "generate_series")]
fn timestamp_generate_series_composes_with_lateral_cte_join_and_window() {
    let mut harness = Harness::new();
    harness
        .run(
            "CREATE TABLE bounds (id INT PRIMARY KEY, start_ts TIMESTAMP, stop_ts TIMESTAMP); \
             INSERT INTO bounds VALUES (1, TIMESTAMP '2024-02-28 00:00:00', \
                                            TIMESTAMP '2024-03-01 00:00:00')",
        )
        .expect("create timestamp bounds");

    let result = harness.query(
        "WITH expanded AS (\
             SELECT b.id, g.ts FROM bounds AS b \
             CROSS JOIN LATERAL GENERATE_SERIES(\
                 b.start_ts, b.stop_ts, INTERVAL '1 day') AS g(ts)\
         ) \
         SELECT e.ts, ROW_NUMBER() OVER (ORDER BY e.ts) AS rn \
         FROM expanded AS e JOIN bounds AS b ON e.id = b.id ORDER BY e.ts",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Timestamp(1_709_078_400_000_000),
                SqlValue::BigInt(1),
            ],
            vec![
                SqlValue::Timestamp(1_709_164_800_000_000),
                SqlValue::BigInt(2),
            ],
            vec![
                SqlValue::Timestamp(1_709_251_200_000_000),
                SqlValue::BigInt(3),
            ],
        ]
    );
}

// === Alias column lists ==================================================

#[test]
fn an_alias_column_list_renames_table_function_output() {
    let result = Harness::new().query("SELECT t.x FROM UNNEST([1.0, 2.0]) AS t(x)");

    assert_eq!(result.rows, vec![vec![float(1.0)], vec![float(2.0)]]);
    assert_eq!(column_names(&result), vec!["x"]);
}

#[test]
fn an_alias_column_list_renames_base_table_columns() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE o (id INT PRIMARY KEY, v INT); INSERT INTO o VALUES (1, 10), (2, 20)")
        .expect("create o");

    let result = harness.query("SELECT p.a, p.b FROM o AS p(a, b) WHERE p.a = 1");

    assert_eq!(result.rows, vec![vec![int(1), int(10)]]);
    assert_eq!(column_names(&result), vec!["a", "b"]);
}

#[test]
fn a_base_table_alias_list_must_name_every_column() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE o (id INT PRIMARY KEY, v INT)")
        .expect("create o");

    let message = harness.error("SELECT * FROM o AS p(a)");

    assert!(
        message.contains("ALOPEX-T012"),
        "unexpected message: {message}"
    );
}

#[test]
fn a_table_function_alias_list_must_name_every_column() {
    let message = Harness::new().error("SELECT * FROM UNNEST([1.0]) AS t(x, y)");

    assert!(
        message.contains("ALOPEX-T012"),
        "unexpected message: {message}"
    );
}

#[test]
fn an_alias_column_list_cannot_repeat_a_name() {
    let message = Harness::new().error("SELECT * FROM (SELECT 1 AS a, 2 AS b) AS d(c, c)");

    assert!(message.contains("'c'"), "unexpected message: {message}");
}

// === LATERAL =============================================================

#[test]
fn a_table_function_argument_sees_the_preceding_from_item() {
    let result = Harness::with_vectors()
        .query("SELECT d.id, u.unnest FROM d, UNNEST(d.emb) AS u ORDER BY d.id, u.unnest");

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), float(1.0)],
            vec![int(1), float(2.0)],
            vec![int(2), float(3.0)],
            vec![int(2), float(4.0)],
        ]
    );
}

#[test]
fn a_table_function_over_an_empty_relation_produces_no_rows() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE d (id INT PRIMARY KEY, emb VECTOR(2))")
        .expect("create d");

    let result = harness.query("SELECT d.id, u.unnest FROM d, UNNEST(d.emb) AS u");

    assert!(result.rows.is_empty());
    assert_eq!(column_names(&result), vec!["id", "unnest"]);
}

#[test]
fn cross_join_lateral_evaluates_the_subquery_per_left_row() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE o (id INT PRIMARY KEY, v INT); INSERT INTO o VALUES (1, 10), (2, 20)")
        .expect("create o");

    let result = harness.query(
        "SELECT o.id, s.w FROM o CROSS JOIN LATERAL (SELECT o.v + 1 AS w) AS s ORDER BY o.id",
    );

    assert_eq!(
        result.rows,
        vec![vec![int(1), int(11)], vec![int(2), int(21)]]
    );
}

#[test]
fn left_join_lateral_pads_a_left_row_without_matches() {
    let result = Harness::with_parent_child().query(
        "SELECT t.id, l.val FROM t LEFT JOIN LATERAL \
         (SELECT s.val FROM s WHERE s.t_id = t.id) AS l ON TRUE ORDER BY t.id, l.val",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), int(100)],
            vec![int(1), int(200)],
            vec![int(2), SqlValue::Null],
        ]
    );
}

#[test]
fn a_lateral_subquery_can_take_the_top_row_per_left_row() {
    let result = Harness::with_parent_child().query(
        "SELECT t.id, l.val FROM t CROSS JOIN LATERAL \
         (SELECT s.val FROM s WHERE s.t_id = t.id ORDER BY s.val DESC LIMIT 1) AS l \
         ORDER BY t.id",
    );

    assert_eq!(result.rows, vec![vec![int(1), int(200)]]);
}

#[test]
fn a_lateral_aggregate_returns_one_row_per_left_row() {
    let result = Harness::with_parent_child().query(
        "SELECT t.id, m.mx FROM t CROSS JOIN LATERAL \
         (SELECT MAX(s.val) AS mx FROM s WHERE s.t_id = t.id) AS m ORDER BY t.id",
    );

    assert_eq!(
        result.rows,
        vec![vec![int(1), int(200)], vec![int(2), SqlValue::Null]]
    );
}

#[test]
fn a_lateral_join_condition_filters_the_correlated_rows() {
    let result = Harness::with_parent_child().query(
        "SELECT t.id, l.val FROM t LEFT JOIN LATERAL \
         (SELECT s.val FROM s WHERE s.t_id = t.id) AS l ON l.val > 150 ORDER BY t.id",
    );

    assert_eq!(
        result.rows,
        vec![vec![int(1), int(200)], vec![int(2), SqlValue::Null]]
    );
}

#[test]
fn lateral_items_chain_left_to_right() {
    let result = Harness::new().query(
        "SELECT a.x, b.y FROM UNNEST([1.0]) AS a(x) CROSS JOIN LATERAL (SELECT a.x + 1.0 AS y) AS b",
    );

    // UNNEST yields FLOAT elements; adding a DOUBLE literal promotes the sum,
    // which is the ordinary scalar rule and not specific to LATERAL.
    assert_eq!(result.rows, vec![vec![float(1.0), SqlValue::Double(2.0)]]);
}

#[test]
fn a_lateral_join_using_merges_the_common_column() {
    let mut harness = Harness::with_parent_child();

    // USING over a LATERAL right side merges the common column exactly as it
    // does for an ordinary join: `id` stays one unqualified name.
    let result = harness.query(
        "SELECT id, l.val FROM t JOIN LATERAL \
         (SELECT s.t_id AS id, s.val FROM s WHERE s.t_id = t.id) AS l USING (id) \
         ORDER BY id, l.val",
    );

    assert_eq!(
        result.rows,
        vec![vec![int(1), int(100)], vec![int(1), int(200)]]
    );
}

#[test]
fn a_derived_table_without_lateral_still_cannot_see_the_enclosing_from() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE o (id INT PRIMARY KEY, v INT)")
        .expect("create o");

    let message = harness.error("SELECT o.id FROM o JOIN (SELECT o.v AS w) AS dd ON TRUE");

    assert!(message.contains("'o'"), "unexpected message: {message}");
}

#[test]
fn lateral_cannot_look_forward_to_a_later_from_item() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE t (id INT PRIMARY KEY)")
        .expect("create t");

    let message = harness.error("SELECT * FROM LATERAL (SELECT z.id AS x) AS l, t AS z");

    assert!(message.contains("'z'"), "unexpected message: {message}");
}

#[test]
fn right_and_full_lateral_joins_are_rejected() {
    let mut harness = Harness::with_parent_child();

    let right = harness.error("SELECT * FROM t RIGHT JOIN LATERAL (SELECT t.id AS x) AS l ON TRUE");
    assert!(right.contains("LATERAL"), "unexpected message: {right}");

    let full = harness.error("SELECT * FROM t FULL JOIN LATERAL (SELECT t.id AS x) AS l ON TRUE");
    assert!(full.contains("LATERAL"), "unexpected message: {full}");
}

#[test]
fn lateral_stays_usable_as_an_ordinary_identifier() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE lateral (id INT PRIMARY KEY); INSERT INTO lateral VALUES (7)")
        .expect("create a table named lateral");

    let result = harness.query("SELECT l.id FROM lateral AS l");

    assert_eq!(result.rows, vec![vec![int(7)]]);
}

// === Reference parity ====================================================

fn reference_value(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Boolean(value) => serde_json::json!(value),
        SqlValue::Integer(value) => serde_json::json!(value),
        SqlValue::BigInt(value) => serde_json::json!(value),
        SqlValue::Float(value) => serde_json::json!(value),
        SqlValue::Double(value) => serde_json::json!(value),
        SqlValue::Text(value) => serde_json::json!(value),
        SqlValue::Timestamp(value) => serde_json::json!(value),
        other => panic!("reference fixture does not use {other:?}"),
    }
}

#[test]
fn postgresql_and_duckdb_reference_fixture_matches_exact_rows() {
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/lateral_reference.json"))
            .expect("parse reference fixture");
    assert_eq!(reference["reference"]["postgresql"], "17");
    assert_eq!(reference["reference"]["duckdb"], "1.5.5");

    let mut harness = Harness::new();
    harness
        .run(reference["setup"].as_str().expect("reference setup"))
        .expect("apply reference setup");

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

#[test]
fn a_wildcard_over_a_renamed_relation_uses_the_alias_columns() {
    let mut harness = Harness::new();
    harness
        .run("CREATE TABLE o (id INT PRIMARY KEY, v INT); INSERT INTO o VALUES (1, 10)")
        .expect("create o");

    let result = harness.query("SELECT * FROM o AS p(a, b)");

    assert_eq!(result.rows, vec![vec![int(1), int(10)]]);
    assert_eq!(column_names(&result), vec!["a", "b"]);
}

#[test]
fn a_lateral_join_is_a_distinct_plan_node_that_pins_its_right_schema() {
    use alopex_sql::planner::JoinType;
    use alopex_sql::planner::logical_plan::LogicalPlan;
    use alopex_sql::planner::typed_expr::TypedExprKind;

    let mut harness = Harness::with_parent_child();
    let plan = harness.plan(
        "SELECT t.id, l.val FROM t LEFT JOIN LATERAL \
         (SELECT s.val FROM s WHERE s.t_id = t.id) AS l ON TRUE",
    );

    // The FROM relation sits under the SELECT projection.
    let LogicalPlan::Project { input, .. } = plan else {
        panic!("expected a projection over the FROM relation");
    };
    let LogicalPlan::LateralJoin {
        join_type,
        right,
        right_schema,
        ..
    } = *input
    else {
        panic!("expected a LateralJoin node, not a plain Join");
    };
    assert_eq!(join_type, JoinType::Left);
    // Kept on the node so a LEFT join can pad without executing the right side.
    assert_eq!(right_schema.len(), 1);

    // D9: inside the correlated side, `t.id` addresses the outer row at
    // `inner width + position in the left row`. `s` is three columns wide and
    // `t.id` is the left row's first column.
    fn correlated_index(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Filter { predicate, input } => match &predicate.kind {
                TypedExprKind::BinaryOp { right, .. } => match &right.kind {
                    TypedExprKind::ColumnRef { column_index, .. } => Some(*column_index),
                    _ => correlated_index(input),
                },
                _ => correlated_index(input),
            },
            LogicalPlan::Project { input, .. } => correlated_index(input),
            _ => None,
        }
    }
    assert_eq!(correlated_index(&right), Some(3));
}

// === D16: correlated references in operators that build their own context ===

/// Fixture for the operators that evaluate expressions over their input rows.
///
/// `w` is three columns wide, so an outer reference from a correlated side
/// resolves at index `3 + <position in the outer row>`.
fn with_scores() -> Harness {
    let mut harness = Harness::new();
    harness
        .run(
            "CREATE TABLE w (id INT PRIMARY KEY, g TEXT, v INT); \
             INSERT INTO w VALUES (1, 'a', 5), (2, 'a', 20), (3, 'b', 20)",
        )
        .expect("create score dataset");
    harness
}

// An aggregate FILTER that references the outer row used to escape as an
// internal ALOPEX-E999 "invalid column reference" from the Aggregate operator,
// while the same reference in WHERE worked.
#[test]
fn a_lateral_aggregate_filter_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let result = harness.query(
        "SELECT w.id, l.c FROM w CROSS JOIN LATERAL \
         (SELECT COUNT(*) FILTER (WHERE z.v > w.v) AS c FROM w AS z) AS l ORDER BY w.id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), SqlValue::BigInt(2)],
            vec![int(2), SqlValue::BigInt(0)],
            vec![int(3), SqlValue::BigInt(0)],
        ]
    );
}

// Aggregate-local ORDER BY keys are evaluated by the same operator.
#[test]
fn a_lateral_ordered_aggregate_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let result = harness.query(
        "SELECT w.id, l.s FROM w CROSS JOIN LATERAL \
         (SELECT GROUP_CONCAT(z.g ORDER BY ABS(z.v - w.v), z.id) AS s FROM w AS z) AS l \
         ORDER BY w.id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), SqlValue::Text("a,a,b".into())],
            vec![int(2), SqlValue::Text("a,b,a".into())],
            vec![int(3), SqlValue::Text("a,b,a".into())],
        ]
    );
}

// Plain ORDER BY inside the correlated side is evaluated by the Sort operator.
#[test]
fn a_lateral_order_by_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let result = harness.query(
        "SELECT w.id, l.v FROM w CROSS JOIN LATERAL \
         (SELECT z.v FROM w AS z ORDER BY ABS(z.v - w.v), z.id LIMIT 1) AS l ORDER BY w.id",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), int(5)],
            vec![int(2), int(20)],
            vec![int(3), int(20)],
        ]
    );
}

// DISTINCT ON sorts and deduplicates in one operator, and FETCH ... WITH TIES
// evaluates its peer keys in the Limit operator.
#[test]
fn a_lateral_distinct_on_and_with_ties_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let distinct_on = harness.query(
        "SELECT w.id, l.v FROM w CROSS JOIN LATERAL \
         (SELECT DISTINCT ON (z.g) z.v FROM w AS z ORDER BY z.g, ABS(z.v - w.v), z.id) AS l \
         ORDER BY w.id, l.v",
    );
    assert_eq!(
        distinct_on.rows,
        vec![
            vec![int(1), int(5)],
            vec![int(1), int(20)],
            vec![int(2), int(20)],
            vec![int(2), int(20)],
            vec![int(3), int(20)],
            vec![int(3), int(20)],
        ]
    );

    let with_ties = harness.query(
        "SELECT w.id, l.v FROM w CROSS JOIN LATERAL \
         (SELECT z.v FROM w AS z ORDER BY ABS(z.v - w.v) FETCH FIRST 1 ROW WITH TIES) AS l \
         ORDER BY w.id, l.v",
    );
    assert_eq!(
        with_ties.rows,
        vec![
            vec![int(1), int(5)],
            vec![int(2), int(20)],
            vec![int(2), int(20)],
            vec![int(3), int(20)],
            vec![int(3), int(20)],
        ]
    );
}

// A window ORDER BY key reaching the outer row must not shift the window
// results: the outer values are cut back out from the middle of the row.
#[test]
fn a_lateral_window_order_by_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let result = harness.query(
        "SELECT w.id, l.r, l.v FROM w CROSS JOIN LATERAL \
         (SELECT ROW_NUMBER() OVER (ORDER BY ABS(z.v - w.v), z.id) AS r, z.v FROM w AS z) AS l \
         ORDER BY w.id, l.r",
    );

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), SqlValue::BigInt(1), int(5)],
            vec![int(1), SqlValue::BigInt(2), int(20)],
            vec![int(1), SqlValue::BigInt(3), int(20)],
            vec![int(2), SqlValue::BigInt(1), int(20)],
            vec![int(2), SqlValue::BigInt(2), int(20)],
            vec![int(2), SqlValue::BigInt(3), int(5)],
            vec![int(3), SqlValue::BigInt(1), int(20)],
            vec![int(3), SqlValue::BigInt(2), int(20)],
            vec![int(3), SqlValue::BigInt(3), int(5)],
        ]
    );
}

// The same gap in a plain correlated scalar subquery (no LATERAL).
#[test]
fn a_correlated_scalar_subquery_aggregate_can_reference_the_outer_row() {
    let mut harness = with_scores();
    let result =
        harness.query("SELECT w.id, (SELECT SUM(z.v + w.v) FROM w AS z) AS s FROM w ORDER BY w.id");

    assert_eq!(
        result.rows,
        vec![
            vec![int(1), SqlValue::BigInt(60)],
            vec![int(2), SqlValue::BigInt(105)],
            vec![int(3), SqlValue::BigInt(105)],
        ]
    );
}
