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

/// CAST must survive planning *and* execution. The parser-level tests above only
/// prove the grammar and the typed expression; they passed while every CAST still
/// failed at runtime with `unsupported expression: cast to ...`.
#[test]
fn cast_evaluates_at_execution_for_column_and_literal_inputs() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "
        CREATE TABLE casts (id INT PRIMARY KEY, v DOUBLE, s TEXT);
        INSERT INTO casts (id, v, s) VALUES (1, 1.5, '123');
        SELECT CAST(v AS INTEGER) FROM casts;
        SELECT CAST(s AS INTEGER) FROM casts;
        SELECT CAST(id AS DOUBLE) FROM casts;
        SELECT CAST(v AS TEXT) FROM casts;
        SELECT CAST('123' AS INTEGER) FROM casts;
        ",
    )
    .expect("parse casts");

    let catalog = std::sync::Arc::new(std::sync::RwLock::new(MemoryCatalog::new()));
    let store = std::sync::Arc::new(alopex_core::kv::memory::MemoryKV::new());
    let mut executor = alopex_sql::executor::Executor::new(store, catalog.clone());
    let mut queries = Vec::new();

    for statement in statements {
        let guard = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*guard).plan(&statement).expect("plan casts");
        drop(guard);
        if let alopex_sql::executor::ExecutionResult::Query(query) =
            executor.execute(plan).expect("execute casts")
        {
            queries.push(query.rows);
        }
    }

    use alopex_sql::storage::SqlValue;
    assert_eq!(
        queries,
        vec![
            vec![vec![SqlValue::Integer(1)]],
            vec![vec![SqlValue::Integer(123)]],
            vec![vec![SqlValue::Double(1.0)]],
            vec![vec![SqlValue::Text("1.5".into())]],
            vec![vec![SqlValue::Integer(123)]],
        ]
    );
}
