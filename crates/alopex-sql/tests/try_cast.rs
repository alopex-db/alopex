//! TRY_CAST conversion and failure contract (issue #147, v0.8.8).

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::SqlError;
use alopex_sql::ast::{ExprKind, SelectItem, StatementKind};
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedExprKind};
use alopex_sql::planner::{Planner, ResolvedType};
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

    fn run(&mut self, sql: &str) -> Result<Option<QueryResult>, SqlError> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).map_err(SqlError::from)?;
        let mut last = None;
        for statement in statements {
            let plan = {
                let catalog = self.catalog.read().expect("catalog read");
                Planner::new(&*catalog)
                    .plan(&statement)
                    .map_err(SqlError::from)?
            };
            if let ExecutionResult::Query(result) =
                self.executor.execute(plan).map_err(SqlError::from)?
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
fn try_cast_has_dedicated_ast_and_typed_expression() {
    let statement = Parser::parse_sql(&AlopexDialect, "SELECT TRY_CAST('42' AS INTEGER)")
        .expect("parse TRY_CAST")
        .remove(0);
    let StatementKind::Select(select) = &statement.kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    assert!(matches!(
        expr.kind,
        ExprKind::TryCast {
            target_type: alopex_sql::ast::ddl::DataType::Integer,
            ..
        }
    ));

    let plan = Planner::new(&MemoryCatalog::new())
        .plan(&statement)
        .expect("plan TRY_CAST");
    let LogicalPlan::Scan {
        projection: Projection::Columns(columns),
        ..
    } = plan
    else {
        panic!("expected literal SELECT scan");
    };
    assert!(matches!(
        columns[0].expr.kind,
        TypedExprKind::TryCast {
            target_type: ResolvedType::Integer,
            ..
        }
    ));
}

#[test]
fn try_cast_success_matrix_preserves_target_values() {
    let result = Harness::new().query(
        "SELECT TRY_CAST('42' AS INTEGER), TRY_CAST(42 AS TEXT), \
         TRY_CAST('yes' AS BOOLEAN), \
         TRY_CAST('2025-01-15 10:30:00.123456' AS TIMESTAMP), \
         TRY_CAST([1.0, 2.0] AS VECTOR(2)), TRY_CAST('hi' AS BLOB)",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(42),
            SqlValue::Text("42".into()),
            SqlValue::Boolean(true),
            SqlValue::Timestamp(1_736_937_000_123_456),
            SqlValue::Vector(vec![1.0, 2.0]),
            SqlValue::Blob(b"hi".to_vec()),
        ]]
    );
}

#[test]
fn try_cast_failure_matrix_returns_null() {
    let result = Harness::new().query(
        "SELECT TRY_CAST('not-an-int' AS INTEGER), \
         TRY_CAST('2147483648' AS INTEGER), \
         TRY_CAST('NaN' AS INTEGER), TRY_CAST('Infinity' AS INTEGER), \
         TRY_CAST('NaN' AS DOUBLE), TRY_CAST('Infinity' AS DOUBLE), \
         TRY_CAST('3.5e38' AS FLOAT), \
         TRY_CAST('not-a-timestamp' AS TIMESTAMP), \
         TRY_CAST([1.0, 2.0] AS VECTOR(3)), \
         TRY_CAST(unhex('ff') AS TEXT)",
    );

    assert_eq!(result.rows, vec![vec![SqlValue::Null; 10]]);
}

#[test]
fn cast_keeps_hard_errors_with_stable_public_vocabulary() {
    for sql in [
        "SELECT CAST('not-an-int' AS INTEGER)",
        "SELECT CAST('2147483648' AS INTEGER)",
        "SELECT CAST([1.0, 2.0] AS VECTOR(3))",
        "SELECT CAST(unhex('ff') AS TEXT)",
    ] {
        let error = Harness::new().run(sql).expect_err("CAST must fail");
        assert_eq!(error.code(), "ALOPEX-E004", "{error}");
        let rendered = error.to_string();
        assert!(rendered.contains("cannot cast"), "{rendered}");
        for internal in ["ExprKind", "TypedExpr", "MessagePack", "__alopex"] {
            assert!(!rendered.contains(internal), "{rendered}");
        }
    }
}

#[test]
fn literal_and_column_paths_share_conversion_semantics() {
    let literal =
        Harness::new().query("SELECT TRY_CAST('42' AS INTEGER), TRY_CAST('bad' AS INTEGER)");
    let mut runtime = Harness::new();
    let rows = runtime.query(
        "CREATE TABLE raw_values (id INTEGER, raw TEXT); \
         INSERT INTO raw_values VALUES (1, '42'), (2, 'bad'); \
         SELECT TRY_CAST(raw AS INTEGER) FROM raw_values ORDER BY id",
    );

    assert_eq!(literal.rows[0][0], rows.rows[0][0]);
    assert_eq!(literal.rows[0][1], rows.rows[1][0]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Integer(42)], vec![SqlValue::Null]]
    );
}

#[test]
fn try_cast_does_not_hide_source_expression_errors() {
    let error = Harness::new()
        .run("SELECT TRY_CAST(1 / 0 AS INTEGER)")
        .expect_err("source evaluation errors remain errors");
    assert!(error.to_string().contains("division by zero"), "{error}");
}

#[test]
fn cast_and_try_cast_preserve_scalar_subquery_conversion_semantics() {
    let result = Harness::new().query(
        "SELECT CAST((SELECT '42') AS INTEGER), \
         TRY_CAST((SELECT '42') AS INTEGER), \
         TRY_CAST((SELECT 'bad') AS INTEGER)",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(42),
            SqlValue::Integer(42),
            SqlValue::Null,
        ]]
    );
}
