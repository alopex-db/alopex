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
fn group_by_supports_expressions_aliases_and_positions() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT); INSERT INTO items VALUES (1), (1), (2);");

    for group_by in ["value + 1", "bucket", "1"] {
        let result = harness.query_sql(&format!(
            "SELECT value + 1 AS bucket, COUNT(*) FROM items GROUP BY {group_by} ORDER BY bucket"
        ));
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Integer(2), SqlValue::BigInt(2)],
                vec![SqlValue::Integer(3), SqlValue::BigInt(1)],
            ]
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_rejects_invalid_positions_and_aggregate_keys() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT);");
    for key in ["0", "3", "2", "COUNT(*)", "total"] {
        let sql = format!("SELECT value, COUNT(*) AS total FROM items GROUP BY {key}");
        let statements =
            alopex_sql::parser::Parser::parse_sql(&alopex_sql::dialect::AlopexDialect, &sql)
                .unwrap();
        let guard = harness.catalog().read().unwrap();
        let err = alopex_sql::planner::Planner::new(&*guard)
            .plan(&statements[0])
            .unwrap_err();
        assert!(
            matches!(err, PlannerError::InvalidExpression { .. }),
            "{sql}: {err}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_rejects_nonintegral_negative_and_overflow_positions() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT);");

    for key in ["1.5", "-1"] {
        let sql = format!("SELECT COUNT(*) FROM items GROUP BY {key}");
        let statements =
            alopex_sql::parser::Parser::parse_sql(&alopex_sql::dialect::AlopexDialect, &sql)
                .unwrap();
        let guard = harness.catalog().read().unwrap();
        let err = alopex_sql::planner::Planner::new(&*guard)
            .plan(&statements[0])
            .unwrap_err();
        assert!(
            matches!(err, PlannerError::InvalidExpression { .. }),
            "{sql}: {err}"
        );
        assert!(
            err.to_string().contains("GROUP BY position"),
            "{sql}: {err}"
        );
    }

    let sql = "SELECT COUNT(*) FROM items GROUP BY 9999999999999999999999999999999999999999999";
    assert!(
        alopex_sql::parser::Parser::parse_sql(&alopex_sql::dialect::AlopexDialect, sql).is_err(),
        "out-of-range integer position must be rejected before planning"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_rejects_window_projection_position() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT);");
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY value) AS rn FROM items GROUP BY 1";
    let statements =
        alopex_sql::parser::Parser::parse_sql(&alopex_sql::dialect::AlopexDialect, sql).unwrap();
    let guard = harness.catalog().read().unwrap();
    let err = alopex_sql::planner::Planner::new(&*guard)
        .plan(&statements[0])
        .unwrap_err();
    assert!(
        matches!(err, PlannerError::InvalidExpression { .. }),
        "{sql}: {err}"
    );
    assert!(err.to_string().contains("window functions"), "{sql}: {err}");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_allows_negative_column_expression() {
    let mut harness = TestHarness::new();
    harness.execute_sql("CREATE TABLE items (value INT); INSERT INTO items VALUES (1), (1), (2);");
    let result =
        harness.query_sql("SELECT -value AS neg, COUNT(*) FROM items GROUP BY -value ORDER BY neg");
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(-2), SqlValue::BigInt(1)],
            vec![SqlValue::Integer(-1), SqlValue::BigInt(2)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_expression_preserves_null_groups_and_having() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        "CREATE TABLE items (value INT); INSERT INTO items VALUES (NULL), (NULL), (1), (2);",
    );
    let result = harness.query_sql(
        "SELECT value % 2 AS bucket, COUNT(*) FROM items GROUP BY value % 2 HAVING COUNT(*) = 2",
    );
    assert_eq!(result.rows, vec![vec![SqlValue::Null, SqlValue::BigInt(2)]]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_supports_case_and_date_trunc() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        "CREATE TABLE events (value INT, happened_at TEXT); \
         INSERT INTO events VALUES \
         (1, '2025-01-15 10:30:45'), (2, '2025-01-15 12:00:00'), (3, '2025-01-16 09:00:00');",
    );

    assert_eq!(
        harness
            .query_sql(
                "SELECT CASE WHEN value < 3 THEN 0 ELSE 1 END AS bucket, COUNT(*) \
                 FROM events GROUP BY CASE WHEN value < 3 THEN 0 ELSE 1 END ORDER BY bucket",
            )
            .rows,
        vec![
            vec![SqlValue::Integer(0), SqlValue::BigInt(2)],
            vec![SqlValue::Integer(1), SqlValue::BigInt(1)],
        ]
    );

    assert_eq!(
        harness
            .query_sql(
                "SELECT DATE_TRUNC('day', CAST(happened_at AS TIMESTAMP)) AS day, COUNT(*) \
                 FROM events \
                 GROUP BY DATE_TRUNC('day', CAST(happened_at AS TIMESTAMP)) ORDER BY day",
            )
            .rows,
        vec![
            vec![
                SqlValue::Timestamp(1_736_899_200_000_000),
                SqlValue::BigInt(2)
            ],
            vec![
                SqlValue::Timestamp(1_736_985_600_000_000),
                SqlValue::BigInt(1)
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn group_by_month_expression_alias_and_position_match() {
    let mut harness = TestHarness::new();
    harness.execute_sql(
        "CREATE TABLE orders (ts TIMESTAMP); \
         INSERT INTO orders VALUES \
         (TIMESTAMP '2025-01-15 10:00:00'), \
         (TIMESTAMP '2025-01-20 11:00:00'), \
         (TIMESTAMP '2025-02-01 00:00:00');",
    );

    for group_by in ["DATE_TRUNC('month', ts)", "m", "1"] {
        let result = harness.query_sql(&format!(
            "SELECT DATE_TRUNC('month', ts) AS m, COUNT(*) \
             FROM orders GROUP BY {group_by} ORDER BY m"
        ));
        assert_eq!(
            result.rows,
            vec![
                vec![
                    SqlValue::Timestamp(1_735_689_600_000_000),
                    SqlValue::BigInt(2)
                ],
                vec![
                    SqlValue::Timestamp(1_738_368_000_000_000),
                    SqlValue::BigInt(1)
                ],
            ],
            "GROUP BY {group_by}"
        );
    }
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
