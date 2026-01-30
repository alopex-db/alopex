use alopex_sql::catalog::ColumnMetadata;
use alopex_sql::executor::ExecutorError;
use alopex_sql::executor::Row;
use alopex_sql::executor::RowIterator;
use alopex_sql::executor::query::aggregate::AggregateIterator;
use alopex_sql::executor::query::iterator::VecIterator;
use alopex_sql::planner::PlannerError;
use alopex_sql::planner::aggregate_expr::AggregateExpr;
use alopex_sql::planner::typed_expr::{TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

use super::TestHarness;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn aggregates_handle_empty_table() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        r#"
        CREATE TABLE metrics (
            value DOUBLE
        );
        "#,
    );

    let result = harness.query_sql("SELECT COUNT(*), SUM(value), AVG(value) FROM metrics");
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0], SqlValue::BigInt(0));
    assert_eq!(row[1], SqlValue::Null);
    assert_eq!(row[2], SqlValue::Null);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn aggregates_handle_all_nulls() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        r#"
        CREATE TABLE metrics (
            value DOUBLE,
            label TEXT
        );
        INSERT INTO metrics (value, label) VALUES
            (NULL, NULL),
            (NULL, NULL);
        "#,
    );

    let result = harness.query_sql(
        "SELECT COUNT(label), SUM(value), AVG(value), MIN(value), MAX(value), GROUP_CONCAT(label) FROM metrics",
    );
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0], SqlValue::BigInt(0));
    assert_eq!(row[1], SqlValue::Null);
    assert_eq!(row[2], SqlValue::Null);
    assert_eq!(row[3], SqlValue::Null);
    assert_eq!(row[4], SqlValue::Null);
    assert_eq!(row[5], SqlValue::Null);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn invalid_group_by_column_returns_error() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (id INT);");

    let stmt = "SELECT missing, COUNT(*) FROM items GROUP BY missing";
    let err = {
        let dialect = alopex_sql::dialect::AlopexDialect;
        let statements = alopex_sql::parser::Parser::parse_sql(&dialect, stmt).unwrap();
        let guard = harness.catalog().read().unwrap();
        let planner = alopex_sql::planner::Planner::new(&*guard);
        planner.plan(&statements[0]).unwrap_err()
    };
    assert!(matches!(err, PlannerError::ColumnNotFound { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_expression_not_supported() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT);");

    let stmt = "SELECT value, COUNT(*) FROM items GROUP BY value + 1";
    let err = {
        let dialect = alopex_sql::dialect::AlopexDialect;
        let statements = alopex_sql::parser::Parser::parse_sql(&dialect, stmt).unwrap();
        let guard = harness.catalog().read().unwrap();
        let planner = alopex_sql::planner::Planner::new(&*guard);
        planner.plan(&statements[0]).unwrap_err()
    };
    assert!(matches!(err, PlannerError::InvalidExpression { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn invalid_having_non_grouped_column() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        r#"
        CREATE TABLE items (category TEXT, value INT);
        INSERT INTO items (category, value) VALUES ('a', 1), ('a', 2);
        "#,
    );

    let stmt = "SELECT category, COUNT(*) FROM items GROUP BY category HAVING value > 1";
    let err = {
        let dialect = alopex_sql::dialect::AlopexDialect;
        let statements = alopex_sql::parser::Parser::parse_sql(&dialect, stmt).unwrap();
        let guard = harness.catalog().read().unwrap();
        let planner = alopex_sql::planner::Planner::new(&*guard);
        planner.plan(&statements[0]).unwrap_err()
    };
    assert!(matches!(err, PlannerError::InvalidExpression { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn invalid_select_non_grouped_column() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        r#"
        CREATE TABLE items (category TEXT, value INT);
        INSERT INTO items (category, value) VALUES ('a', 1), ('a', 2);
        "#,
    );

    let stmt = "SELECT category, value FROM items GROUP BY category";
    let err = {
        let dialect = alopex_sql::dialect::AlopexDialect;
        let statements = alopex_sql::parser::Parser::parse_sql(&dialect, stmt).unwrap();
        let guard = harness.catalog().read().unwrap();
        let planner = alopex_sql::planner::Planner::new(&*guard);
        planner.plan(&statements[0]).unwrap_err()
    };
    assert!(matches!(err, PlannerError::InvalidExpression { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sum_on_text_returns_type_mismatch() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (label TEXT);");

    let stmt = "SELECT SUM(label) FROM items";
    let err = {
        let dialect = alopex_sql::dialect::AlopexDialect;
        let statements = alopex_sql::parser::Parser::parse_sql(&dialect, stmt).unwrap();
        let guard = harness.catalog().read().unwrap();
        let planner = alopex_sql::planner::Planner::new(&*guard);
        planner.plan(&statements[0]).unwrap_err()
    };
    assert!(matches!(err, PlannerError::TypeMismatch { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_resource_limit_exceeded() {
    let schema = vec![ColumnMetadata::new("category", ResolvedType::Text)];
    let rows = vec![
        Row::new(0, vec![SqlValue::Text("a".into())]),
        Row::new(1, vec![SqlValue::Text("b".into())]),
        Row::new(2, vec![SqlValue::Text("c".into())]),
    ];
    let input = Box::new(VecIterator::new(rows, schema.clone()));
    let group_key = TypedExpr {
        kind: TypedExprKind::ColumnRef {
            table: "items".into(),
            column: "category".into(),
            column_index: 0,
        },
        resolved_type: ResolvedType::Text,
        span: alopex_sql::Span::default(),
    };
    let aggregates = vec![AggregateExpr::count_star()];

    let mut iter = AggregateIterator::new(input, vec![group_key], aggregates, None, schema)
        .with_group_limit(2);

    let err = iter.next_row().unwrap().unwrap_err();
    assert!(matches!(err, ExecutorError::ResourceExhausted { .. }));
}
