use alopex_sql::ast::ddl::DataType;
use alopex_sql::ast::{ExprKind, SelectItem, StatementKind};
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::parser::Parser;
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedExprKind};
use alopex_sql::planner::{Planner, ResolvedType};

fn parsed_cast_type(sql: &str) -> DataType {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse CAST statement");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression projection");
    };
    let ExprKind::Cast { target_type, .. } = &expr.kind else {
        panic!("expected CAST expression");
    };
    target_type.clone()
}

#[test]
fn cast_parses_every_documented_data_type_and_rejects_colon_casts() {
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS INTEGER)"),
        DataType::Integer
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS BIGINT)"),
        DataType::BigInt
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS FLOAT)"),
        DataType::Float
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS DOUBLE)"),
        DataType::Double
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS TEXT)"),
        DataType::Text
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS BLOB)"),
        DataType::Blob
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS BOOLEAN)"),
        DataType::Boolean
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS TIMESTAMP)"),
        DataType::Timestamp
    ));
    assert!(matches!(
        parsed_cast_type("SELECT CAST(1 AS VECTOR(3))"),
        DataType::Vector { dimension: 3, .. }
    ));
    assert!(Parser::parse_sql(&AlopexDialect, "SELECT 1::INTEGER").is_err());
}

#[test]
fn cast_is_wired_to_the_existing_typed_cast_expression() {
    let statement = Parser::parse_sql(&AlopexDialect, "SELECT CAST(1 AS BIGINT)")
        .expect("parse CAST statement")
        .remove(0);
    let plan = Planner::new(&MemoryCatalog::new())
        .plan(&statement)
        .expect("plan CAST statement");

    let LogicalPlan::Scan {
        projection: Projection::Columns(columns),
        ..
    } = plan
    else {
        panic!("expected literal SELECT scan with expression projection");
    };
    assert!(matches!(
        columns[0].expr.kind,
        TypedExprKind::Cast {
            target_type: ResolvedType::BigInt,
            ..
        }
    ));
}
