//! Integration tests for the Planner module.
//!
//! Tests cover:
//! - DDL statements (CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX)
//! - SELECT statements (projection, WHERE, ORDER BY, LIMIT)
//! - INSERT/UPDATE/DELETE statements
//! - Error cases (table not found, column not found, type mismatch, etc.)

use super::*;
use crate::ast::ddl::{
    ColumnConstraint, ColumnDef, CreateIndex, CreateTable, DataType, DropIndex, DropTable,
    IndexMethod,
};
use crate::ast::dml::{
    Assignment, Delete, Insert, OrderByExpr, Select, SelectItem, TableRef, Update,
};
use crate::ast::expr::{BinaryOp, Expr, ExprKind, Literal};
use crate::ast::span::Span;
use crate::ast::{Statement, StatementKind};
use crate::catalog::{ColumnMetadata, IndexMetadata, MemoryCatalog, TableMetadata};
use crate::{DataSourceFormat, TableType};

// ============================================================
// Test Helpers
// ============================================================

/// Create a test catalog with sample tables.
fn create_test_catalog() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();

    // users table
    let users = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("age", ResolvedType::Integer),
            ColumnMetadata::new("email", ResolvedType::Text),
        ],
    )
    .with_primary_key(vec!["id".to_string()]);
    catalog.create_table(users).unwrap();

    // products table
    let products = TableMetadata::new(
        "products",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("price", ResolvedType::Double),
        ],
    );
    catalog.create_table(products).unwrap();

    catalog
}

/// Create a default span for testing.
fn span() -> Span {
    Span::default()
}

/// Create a Statement wrapper for a StatementKind.
fn stmt(kind: StatementKind) -> Statement {
    Statement { kind, span: span() }
}

/// Create an integer literal expression.
fn int_lit(value: i64) -> Expr {
    Expr {
        kind: ExprKind::Literal(Literal::Number(value.to_string())),
        span: span(),
    }
}

/// Create a string literal expression.
fn str_lit(value: &str) -> Expr {
    Expr {
        kind: ExprKind::Literal(Literal::String(value.to_string())),
        span: span(),
    }
}

/// Create a NULL literal expression.
fn null_lit() -> Expr {
    Expr {
        kind: ExprKind::Literal(Literal::Null),
        span: span(),
    }
}

/// Create a column reference expression.
fn col_ref(table: Option<&str>, column: &str) -> Expr {
    Expr {
        kind: ExprKind::ColumnRef {
            table: table.map(String::from),
            column: column.to_string(),
        },
        span: span(),
    }
}

/// Create a binary operation expression.
fn binary_op(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        span: span(),
    }
}

// ============================================================
// DDL Tests
// ============================================================

#[test]
fn test_plan_create_table() {
    let catalog = MemoryCatalog::new();
    let planner = Planner::new(&catalog);

    let create = CreateTable {
        if_not_exists: false,
        name: "new_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey, ColumnConstraint::NotNull],
                span: span(),
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                constraints: vec![ColumnConstraint::NotNull],
                span: span(),
            },
        ],
        constraints: vec![],
        with_options: vec![],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::CreateTable(create)));
    assert!(result.is_ok());

    if let LogicalPlan::CreateTable {
        table,
        if_not_exists,
        with_options,
    } = result.unwrap()
    {
        assert_eq!(table.name, "new_table");
        assert_eq!(table.columns.len(), 2);
        assert!(!if_not_exists);
        assert!(with_options.is_empty());
        assert!(table.columns[0].primary_key);
        assert!(table.columns[0].not_null);
        assert!(table.columns[1].not_null);
        assert_eq!(table.catalog_name, "default");
        assert_eq!(table.namespace_name, "default");
        assert_eq!(table.table_type, TableType::Managed);
        assert_eq!(table.data_source_format, DataSourceFormat::Alopex);
        assert!(table.properties.is_empty());
    } else {
        panic!("Expected CreateTable plan");
    }
}

#[test]
fn test_plan_create_table_already_exists() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let create = CreateTable {
        if_not_exists: false,
        name: "users".to_string(),
        columns: vec![],
        constraints: vec![],
        with_options: vec![],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::CreateTable(create)));
    assert!(matches!(
        result,
        Err(PlannerError::TableAlreadyExists { name }) if name == "users"
    ));
}

#[test]
fn test_plan_create_table_if_not_exists() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let create = CreateTable {
        if_not_exists: true,
        name: "users".to_string(),
        columns: vec![],
        constraints: vec![],
        with_options: vec![],
        span: span(),
    };

    // Should succeed with IF NOT EXISTS
    let result = planner.plan(&stmt(StatementKind::CreateTable(create)));
    assert!(result.is_ok());
}

#[test]
fn test_plan_drop_table() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let drop = DropTable {
        if_exists: false,
        name: "users".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropTable(drop)));
    assert!(result.is_ok());

    if let LogicalPlan::DropTable { name, if_exists } = result.unwrap() {
        assert_eq!(name, "users");
        assert!(!if_exists);
    } else {
        panic!("Expected DropTable plan");
    }
}

#[test]
fn test_plan_drop_table_not_found() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let drop = DropTable {
        if_exists: false,
        name: "nonexistent".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropTable(drop)));
    assert!(matches!(
        result,
        Err(PlannerError::TableNotFound { name, .. }) if name == "nonexistent"
    ));
}

#[test]
fn test_plan_drop_table_ignores_non_default_namespace() {
    let mut catalog = MemoryCatalog::new();
    let mut table = TableMetadata::new(
        "events",
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );
    table.catalog_name = "main".to_string();
    table.namespace_name = "analytics".to_string();
    catalog.create_table(table).unwrap();

    let planner = Planner::new(&catalog);
    let drop = DropTable {
        if_exists: false,
        name: "events".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropTable(drop)));
    assert!(matches!(
        result,
        Err(PlannerError::TableNotFound { name, .. }) if name == "events"
    ));
}

#[test]
fn test_plan_create_index() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let create = CreateIndex {
        if_not_exists: false,
        name: "idx_users_name".to_string(),
        table: "users".to_string(),
        column: "name".to_string(),
        method: Some(IndexMethod::BTree),
        options: vec![],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::CreateIndex(create)));
    assert!(result.is_ok());

    if let LogicalPlan::CreateIndex {
        index,
        if_not_exists,
    } = result.unwrap()
    {
        assert_eq!(index.name, "idx_users_name");
        assert_eq!(index.table, "users");
        assert_eq!(index.first_column(), Some("name"));
        assert_eq!(index.method, Some(IndexMethod::BTree));
        assert!(!if_not_exists);
    } else {
        panic!("Expected CreateIndex plan");
    }
}

#[test]
fn test_plan_create_index_column_not_found() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let create = CreateIndex {
        if_not_exists: false,
        name: "idx_users_foo".to_string(),
        table: "users".to_string(),
        column: "nonexistent".to_string(),
        method: None,
        options: vec![],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::CreateIndex(create)));
    assert!(matches!(
        result,
        Err(PlannerError::ColumnNotFound { column, table, .. })
            if column == "nonexistent" && table == "users"
    ));
}

#[test]
fn test_plan_drop_index() {
    let mut catalog = create_test_catalog();
    let index = crate::catalog::IndexMetadata::new(0, "idx_test", "users", vec!["name".into()]);
    catalog.create_index(index).unwrap();

    let planner = Planner::new(&catalog);

    let drop = DropIndex {
        if_exists: false,
        name: "idx_test".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropIndex(drop)));
    assert!(result.is_ok());
}

#[test]
fn test_plan_drop_index_ignores_non_default_namespace() {
    let mut catalog = MemoryCatalog::new();
    let mut table = TableMetadata::new(
        "users",
        vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
    )
    .with_primary_key(vec!["id".to_string()]);
    table.catalog_name = "main".to_string();
    table.namespace_name = "analytics".to_string();
    catalog.create_table(table).unwrap();

    let mut index = IndexMetadata::new(0, "idx_users_id", "users", vec!["id".into()]);
    index.catalog_name = "main".to_string();
    index.namespace_name = "analytics".to_string();
    catalog.create_index(index).unwrap();

    let planner = Planner::new(&catalog);

    let drop = DropIndex {
        if_exists: false,
        name: "idx_users_id".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropIndex(drop)));
    assert!(matches!(
        result,
        Err(PlannerError::IndexNotFound { name }) if name == "idx_users_id"
    ));
}

#[test]
fn test_plan_drop_index_if_exists_allows_non_default_namespace() {
    let mut catalog = MemoryCatalog::new();
    let mut table = TableMetadata::new(
        "users",
        vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
    )
    .with_primary_key(vec!["id".to_string()]);
    table.catalog_name = "main".to_string();
    table.namespace_name = "analytics".to_string();
    catalog.create_table(table).unwrap();

    let mut index = IndexMetadata::new(0, "idx_users_id", "users", vec!["id".into()]);
    index.catalog_name = "main".to_string();
    index.namespace_name = "analytics".to_string();
    catalog.create_index(index).unwrap();

    let planner = Planner::new(&catalog);

    let drop = DropIndex {
        if_exists: true,
        name: "idx_users_id".to_string(),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::DropIndex(drop)));
    assert!(result.is_ok());
}

// ============================================================
// SELECT Tests
// ============================================================

#[test]
fn test_plan_select_wildcard() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    if let LogicalPlan::Scan { table, projection } = result.unwrap() {
        assert_eq!(table, "users");
        if let Projection::All(cols) = projection {
            assert_eq!(cols, vec!["id", "name", "age", "email"]);
        } else {
            panic!("Expected Projection::All");
        }
    } else {
        panic!("Expected Scan plan");
    }
}

#[test]
fn test_plan_select_columns() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![
            SelectItem::Expr {
                expr: col_ref(None, "id"),
                alias: None,
                span: span(),
            },
            SelectItem::Expr {
                expr: col_ref(None, "name"),
                alias: Some("user_name".to_string()),
                span: span(),
            },
        ],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    if let LogicalPlan::Scan { projection, .. } = result.unwrap() {
        if let Projection::Columns(cols) = projection {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].output_name(), Some("id"));
            assert_eq!(cols[1].output_name(), Some("user_name"));
        } else {
            panic!("Expected Projection::Columns");
        }
    } else {
        panic!("Expected Scan plan");
    }
}

#[test]
fn test_plan_select_with_where() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: Some(binary_op(col_ref(None, "age"), BinaryOp::Gt, int_lit(18))),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    if let LogicalPlan::Filter { input, predicate } = result.unwrap() {
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
        assert_eq!(predicate.resolved_type, ResolvedType::Boolean);
    } else {
        panic!("Expected Filter plan");
    }
}

#[test]
fn test_plan_select_with_order_by() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![
            OrderByExpr {
                expr: col_ref(None, "name"),
                asc: Some(true),
                nulls_first: None,
                span: span(),
            },
            OrderByExpr {
                expr: col_ref(None, "age"),
                asc: Some(false),
                nulls_first: Some(true),
                span: span(),
            },
        ],
        limit: None,
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    if let LogicalPlan::Sort { input, order_by } = result.unwrap() {
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
        assert_eq!(order_by.len(), 2);
        assert!(order_by[0].asc);
        assert!(!order_by[0].nulls_first);
        assert!(!order_by[1].asc);
        assert!(order_by[1].nulls_first);
    } else {
        panic!("Expected Sort plan");
    }
}

#[test]
fn test_plan_select_with_limit() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: Some(int_lit(10)),
        offset: Some(int_lit(5)),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    if let LogicalPlan::Limit {
        input,
        limit,
        offset,
    } = result.unwrap()
    {
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
        assert_eq!(limit, Some(10));
        assert_eq!(offset, Some(5));
    } else {
        panic!("Expected Limit plan");
    }
}

#[test]
fn test_plan_select_combined() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    // SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 10
    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "users".to_string(),
            alias: None,
            span: span(),
        },
        selection: Some(binary_op(col_ref(None, "age"), BinaryOp::Gt, int_lit(18))),
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: col_ref(None, "name"),
            asc: Some(true),
            nulls_first: None,
            span: span(),
        }],
        limit: Some(int_lit(10)),
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(result.is_ok());

    // Verify nested structure: Limit -> Sort -> Filter -> Scan
    if let LogicalPlan::Limit { input, limit, .. } = result.unwrap() {
        assert_eq!(limit, Some(10));
        if let LogicalPlan::Sort { input, .. } = *input {
            if let LogicalPlan::Filter { input, .. } = *input {
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
            } else {
                panic!("Expected Filter plan");
            }
        } else {
            panic!("Expected Sort plan");
        }
    } else {
        panic!("Expected Limit plan");
    }
}

#[test]
fn test_plan_select_table_not_found() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span() }],
        from: TableRef {
            name: "nonexistent".to_string(),
            alias: None,
            span: span(),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Select(select)));
    assert!(matches!(
        result,
        Err(PlannerError::TableNotFound { name, .. }) if name == "nonexistent"
    ));
}

// ============================================================
// INSERT Tests
// ============================================================

#[test]
fn test_plan_insert_with_columns() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let insert = Insert {
        table: "users".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![int_lit(1), str_lit("Alice")]],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(result.is_ok());

    if let LogicalPlan::Insert {
        table,
        columns,
        values,
    } = result.unwrap()
    {
        assert_eq!(table, "users");
        assert_eq!(columns, vec!["id", "name"]);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].len(), 2);
    } else {
        panic!("Expected Insert plan");
    }
}

#[test]
fn test_plan_insert_without_columns() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let insert = Insert {
        table: "products".to_string(),
        columns: None,
        values: vec![vec![int_lit(1), str_lit("Widget"), int_lit(100)]],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(result.is_ok());

    if let LogicalPlan::Insert { columns, .. } = result.unwrap() {
        // Should use table definition order
        assert_eq!(columns, vec!["id", "name", "price"]);
    } else {
        panic!("Expected Insert plan");
    }
}

#[test]
fn test_plan_insert_multiple_rows() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let insert = Insert {
        table: "products".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![
            vec![int_lit(1), str_lit("Widget")],
            vec![int_lit(2), str_lit("Gadget")],
            vec![int_lit(3), str_lit("Gizmo")],
        ],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(result.is_ok());

    if let LogicalPlan::Insert { values, .. } = result.unwrap() {
        assert_eq!(values.len(), 3);
    } else {
        panic!("Expected Insert plan");
    }
}

#[test]
fn test_plan_insert_column_count_mismatch() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let insert = Insert {
        table: "users".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![int_lit(1)]], // Missing value
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(matches!(
        result,
        Err(PlannerError::ColumnValueCountMismatch {
            columns: 2,
            values: 1,
            ..
        })
    ));
}

#[test]
fn test_plan_insert_null_constraint_violation() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let insert = Insert {
        table: "users".to_string(),
        columns: Some(vec!["id".to_string(), "name".to_string()]),
        values: vec![vec![int_lit(1), null_lit()]], // name is NOT NULL
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(matches!(
        result,
        Err(PlannerError::NullConstraintViolation { column, .. }) if column == "name"
    ));
}

// ============================================================
// UPDATE Tests
// ============================================================

#[test]
fn test_plan_update() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let update = Update {
        table: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: str_lit("Bob"),
            span: span(),
        }],
        selection: Some(binary_op(col_ref(None, "id"), BinaryOp::Eq, int_lit(1))),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Update(update)));
    assert!(result.is_ok());

    if let LogicalPlan::Update {
        table,
        assignments,
        filter,
    } = result.unwrap()
    {
        assert_eq!(table, "users");
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].column, "name");
        assert!(filter.is_some());
    } else {
        panic!("Expected Update plan");
    }
}

#[test]
fn test_plan_update_without_where() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let update = Update {
        table: "users".to_string(),
        assignments: vec![Assignment {
            column: "age".to_string(),
            value: int_lit(25),
            span: span(),
        }],
        selection: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Update(update)));
    assert!(result.is_ok());

    if let LogicalPlan::Update { filter, .. } = result.unwrap() {
        assert!(filter.is_none());
    } else {
        panic!("Expected Update plan");
    }
}

#[test]
fn test_plan_update_null_constraint_violation() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let update = Update {
        table: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: null_lit(), // name is NOT NULL
            span: span(),
        }],
        selection: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Update(update)));
    assert!(matches!(
        result,
        Err(PlannerError::NullConstraintViolation { column, .. }) if column == "name"
    ));
}

// ============================================================
// DELETE Tests
// ============================================================

#[test]
fn test_plan_delete() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let delete = Delete {
        table: "users".to_string(),
        selection: Some(binary_op(col_ref(None, "id"), BinaryOp::Eq, int_lit(1))),
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Delete(delete)));
    assert!(result.is_ok());

    if let LogicalPlan::Delete { table, filter } = result.unwrap() {
        assert_eq!(table, "users");
        assert!(filter.is_some());
    } else {
        panic!("Expected Delete plan");
    }
}

#[test]
fn test_plan_delete_without_where() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let delete = Delete {
        table: "users".to_string(),
        selection: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Delete(delete)));
    assert!(result.is_ok());

    if let LogicalPlan::Delete { filter, .. } = result.unwrap() {
        assert!(filter.is_none());
    } else {
        panic!("Expected Delete plan");
    }
}

#[test]
fn test_plan_delete_table_not_found() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    let delete = Delete {
        table: "nonexistent".to_string(),
        selection: None,
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Delete(delete)));
    assert!(matches!(
        result,
        Err(PlannerError::TableNotFound { name, .. }) if name == "nonexistent"
    ));
}

// ============================================================
// Type Compatibility Tests
// ============================================================

#[test]
fn test_plan_insert_type_compatible() {
    let catalog = create_test_catalog();
    let planner = Planner::new(&catalog);

    // Integer can be inserted into Double column (price)
    let insert = Insert {
        table: "products".to_string(),
        columns: Some(vec![
            "id".to_string(),
            "name".to_string(),
            "price".to_string(),
        ]),
        values: vec![vec![int_lit(1), str_lit("Widget"), int_lit(100)]],
        span: span(),
    };

    let result = planner.plan(&stmt(StatementKind::Insert(insert)));
    assert!(result.is_ok());
}
