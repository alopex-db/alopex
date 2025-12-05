//! Unit tests for NameResolver.

use super::*;
use crate::ast::expr::{BinaryOp, Expr, ExprKind, Literal};
use crate::ast::span::{Location, Span};
use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use crate::planner::types::ResolvedType;

/// Create a test span for testing purposes.
fn test_span() -> Span {
    Span::new(Location::new(1, 1), Location::new(1, 10))
}

/// Create a span at a specific location.
fn span_at(line: u64, column: u64) -> Span {
    Span::new(Location::new(line, column), Location::new(line, column + 5))
}

/// Create a test catalog with a users table.
fn create_test_catalog() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();

    let users_table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("email", ResolvedType::Text),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ],
    )
    .with_primary_key(vec!["id".to_string()]);

    catalog.create_table(users_table).unwrap();

    catalog
}

/// Create a test catalog with multiple tables for scope testing.
fn create_multi_table_catalog() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();

    let users_table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text),
        ],
    );

    let orders_table = TableMetadata::new(
        "orders",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("user_id", ResolvedType::Integer),
            ColumnMetadata::new("total", ResolvedType::Double),
        ],
    );

    catalog.create_table(users_table).unwrap();
    catalog.create_table(orders_table).unwrap();

    catalog
}

// =============================================================================
// resolve_table tests
// =============================================================================

#[test]
fn test_resolve_table_success() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();
    assert_eq!(table.name, "users");
    assert_eq!(table.columns.len(), 4);
}

#[test]
fn test_resolve_table_not_found() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let span = span_at(5, 10);
    let result = resolver.resolve_table("unknown", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::TableNotFound { name, line, column } => {
            assert_eq!(name, "unknown");
            assert_eq!(line, 5);
            assert_eq!(column, 10);
        }
        e => panic!("Expected TableNotFound, got {:?}", e),
    }
}

#[test]
fn test_resolve_table_empty_catalog() {
    let catalog = MemoryCatalog::new();
    let resolver = NameResolver::new(&catalog);

    let result = resolver.resolve_table("any_table", test_span());
    assert!(result.is_err());
}

// =============================================================================
// resolve_column tests
// =============================================================================

#[test]
fn test_resolve_column_success() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();
    let column = resolver.resolve_column(table, "name", test_span()).unwrap();

    assert_eq!(column.name, "name");
    assert_eq!(column.data_type, ResolvedType::Text);
}

#[test]
fn test_resolve_column_all_columns() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // Test all columns exist
    for col_name in ["id", "name", "email", "age"] {
        let column = resolver.resolve_column(table, col_name, test_span());
        assert!(column.is_ok(), "Column {} should exist", col_name);
    }
}

#[test]
fn test_resolve_column_not_found() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();
    let span = span_at(3, 15);
    let result = resolver.resolve_column(table, "unknown_column", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::ColumnNotFound {
            column,
            table,
            line,
            col,
        } => {
            assert_eq!(column, "unknown_column");
            assert_eq!(table, "users");
            assert_eq!(line, 3);
            assert_eq!(col, 15);
        }
        e => panic!("Expected ColumnNotFound, got {:?}", e),
    }
}

#[test]
fn test_resolve_column_case_sensitive() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // Column names are case-sensitive
    let result_lower = resolver.resolve_column(table, "name", test_span());
    let result_upper = resolver.resolve_column(table, "NAME", test_span());

    assert!(result_lower.is_ok());
    assert!(result_upper.is_err());
}

// =============================================================================
// resolve_column_with_scope tests
// =============================================================================

#[test]
fn test_resolve_column_with_scope_single_table() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = catalog.get_table("users").unwrap();
    let tables = vec![table];

    let resolved = resolver
        .resolve_column_with_scope(&tables, None, "id", test_span())
        .unwrap();

    assert_eq!(resolved.table_name, "users");
    assert_eq!(resolved.column_name, "id");
    assert_eq!(resolved.column_index, 0);
    assert_eq!(resolved.resolved_type, ResolvedType::Integer);
}

#[test]
fn test_resolve_column_with_scope_with_qualifier() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = catalog.get_table("users").unwrap();
    let tables = vec![table];

    let resolved = resolver
        .resolve_column_with_scope(&tables, Some("users"), "name", test_span())
        .unwrap();

    assert_eq!(resolved.table_name, "users");
    assert_eq!(resolved.column_name, "name");
    assert_eq!(resolved.column_index, 1);
    assert_eq!(resolved.resolved_type, ResolvedType::Text);
}

#[test]
fn test_resolve_column_with_scope_wrong_qualifier() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = catalog.get_table("users").unwrap();
    let tables = vec![table];

    let span = span_at(2, 5);
    let result = resolver.resolve_column_with_scope(&tables, Some("orders"), "id", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::TableNotFound { name, line, column } => {
            assert_eq!(name, "orders");
            assert_eq!(line, 2);
            assert_eq!(column, 5);
        }
        e => panic!("Expected TableNotFound, got {:?}", e),
    }
}

#[test]
fn test_resolve_column_with_scope_ambiguous() {
    let catalog = create_multi_table_catalog();
    let resolver = NameResolver::new(&catalog);

    let users_table = catalog.get_table("users").unwrap();
    let orders_table = catalog.get_table("orders").unwrap();
    let tables = vec![users_table, orders_table];

    // 'id' exists in both tables - should be ambiguous
    let span = span_at(1, 20);
    let result = resolver.resolve_column_with_scope(&tables, None, "id", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::AmbiguousColumn {
            column,
            tables,
            line,
            col,
        } => {
            assert_eq!(column, "id");
            assert!(tables.contains(&"users".to_string()));
            assert!(tables.contains(&"orders".to_string()));
            assert_eq!(line, 1);
            assert_eq!(col, 20);
        }
        e => panic!("Expected AmbiguousColumn, got {:?}", e),
    }
}

#[test]
fn test_resolve_column_with_scope_unique_in_multi_table() {
    let catalog = create_multi_table_catalog();
    let resolver = NameResolver::new(&catalog);

    let users_table = catalog.get_table("users").unwrap();
    let orders_table = catalog.get_table("orders").unwrap();
    let tables = vec![users_table, orders_table];

    // 'total' only exists in orders table
    let resolved = resolver
        .resolve_column_with_scope(&tables, None, "total", test_span())
        .unwrap();

    assert_eq!(resolved.table_name, "orders");
    assert_eq!(resolved.column_name, "total");
    assert_eq!(resolved.resolved_type, ResolvedType::Double);
}

#[test]
fn test_resolve_column_with_scope_qualified_in_multi_table() {
    let catalog = create_multi_table_catalog();
    let resolver = NameResolver::new(&catalog);

    let users_table = catalog.get_table("users").unwrap();
    let orders_table = catalog.get_table("orders").unwrap();
    let tables = vec![users_table, orders_table];

    // 'users.id' should resolve to users table
    let resolved = resolver
        .resolve_column_with_scope(&tables, Some("users"), "id", test_span())
        .unwrap();

    assert_eq!(resolved.table_name, "users");
    assert_eq!(resolved.column_index, 0);

    // 'orders.id' should resolve to orders table
    let resolved = resolver
        .resolve_column_with_scope(&tables, Some("orders"), "id", test_span())
        .unwrap();

    assert_eq!(resolved.table_name, "orders");
    assert_eq!(resolved.column_index, 0);
}

#[test]
fn test_resolve_column_with_scope_column_not_found() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = catalog.get_table("users").unwrap();
    let tables = vec![table];

    let span = span_at(4, 12);
    let result = resolver.resolve_column_with_scope(&tables, None, "nonexistent", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::ColumnNotFound { column, .. } => {
            assert_eq!(column, "nonexistent");
        }
        e => panic!("Expected ColumnNotFound, got {:?}", e),
    }
}

// =============================================================================
// expand_wildcard tests
// =============================================================================

#[test]
fn test_expand_wildcard() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();
    let columns = resolver.expand_wildcard(table);

    assert_eq!(columns, vec!["id", "name", "email", "age"]);
}

#[test]
fn test_expand_wildcard_order_preserved() {
    let mut catalog = MemoryCatalog::new();

    // Create table with specific column order
    let table = TableMetadata::new(
        "test",
        vec![
            ColumnMetadata::new("z_col", ResolvedType::Text),
            ColumnMetadata::new("a_col", ResolvedType::Integer),
            ColumnMetadata::new("m_col", ResolvedType::Boolean),
        ],
    );
    catalog.create_table(table).unwrap();

    let resolver = NameResolver::new(&catalog);
    let table = resolver.resolve_table("test", test_span()).unwrap();
    let columns = resolver.expand_wildcard(table);

    // Should maintain definition order, not alphabetical
    assert_eq!(columns, vec!["z_col", "a_col", "m_col"]);
}

#[test]
fn test_expand_wildcard_empty_table() {
    let mut catalog = MemoryCatalog::new();

    // Create table with no columns (edge case)
    let table = TableMetadata::new("empty", vec![]);
    catalog.create_table(table).unwrap();

    let resolver = NameResolver::new(&catalog);
    let table = resolver.resolve_table("empty", test_span()).unwrap();
    let columns = resolver.expand_wildcard(table);

    assert!(columns.is_empty());
}

// =============================================================================
// resolve_expr tests
// =============================================================================

#[test]
fn test_resolve_expr_column_ref() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: "name".to_string(),
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_column_ref_with_qualifier() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_column_ref_wrong_qualifier() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: Some("other_table".to_string()),
            column: "id".to_string(),
        },
        span: span_at(2, 8),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::TableNotFound { name, .. } => {
            assert_eq!(name, "other_table");
        }
        e => panic!("Expected TableNotFound, got {:?}", e),
    }
}

#[test]
fn test_resolve_expr_column_ref_not_found() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: "unknown".to_string(),
        },
        span: span_at(3, 12),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::ColumnNotFound { column, .. } => {
            assert_eq!(column, "unknown");
        }
        e => panic!("Expected ColumnNotFound, got {:?}", e),
    }
}

#[test]
fn test_resolve_expr_literal() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Number("42".to_string())),
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_binary_op() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // age > 18
    let expr = Expr {
        kind: ExprKind::BinaryOp {
            left: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                },
                span: test_span(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr {
                kind: ExprKind::Literal(Literal::Number("18".to_string())),
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_binary_op_with_invalid_column() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // invalid_column > 18
    let expr = Expr {
        kind: ExprKind::BinaryOp {
            left: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "invalid_column".to_string(),
                },
                span: span_at(1, 5),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr {
                kind: ExprKind::Literal(Literal::Number("18".to_string())),
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_err());
}

#[test]
fn test_resolve_expr_function_call() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // UPPER(name)
    let expr = Expr {
        kind: ExprKind::FunctionCall {
            name: "UPPER".to_string(),
            args: vec![Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                span: test_span(),
            }],
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_between() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // age BETWEEN 18 AND 65
    let expr = Expr {
        kind: ExprKind::Between {
            expr: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                },
                span: test_span(),
            }),
            low: Box::new(Expr {
                kind: ExprKind::Literal(Literal::Number("18".to_string())),
                span: test_span(),
            }),
            high: Box::new(Expr {
                kind: ExprKind::Literal(Literal::Number("65".to_string())),
                span: test_span(),
            }),
            negated: false,
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_in_list() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // id IN (1, 2, 3)
    let expr = Expr {
        kind: ExprKind::InList {
            expr: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                },
                span: test_span(),
            }),
            list: vec![
                Expr {
                    kind: ExprKind::Literal(Literal::Number("1".to_string())),
                    span: test_span(),
                },
                Expr {
                    kind: ExprKind::Literal(Literal::Number("2".to_string())),
                    span: test_span(),
                },
                Expr {
                    kind: ExprKind::Literal(Literal::Number("3".to_string())),
                    span: test_span(),
                },
            ],
            negated: false,
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_is_null() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // email IS NULL
    let expr = Expr {
        kind: ExprKind::IsNull {
            expr: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "email".to_string(),
                },
                span: test_span(),
            }),
            negated: false,
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_like() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    // name LIKE 'A%'
    let expr = Expr {
        kind: ExprKind::Like {
            expr: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                span: test_span(),
            }),
            pattern: Box::new(Expr {
                kind: ExprKind::Literal(Literal::String("A%".to_string())),
                span: test_span(),
            }),
            escape: None,
            negated: false,
        },
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

#[test]
fn test_resolve_expr_vector_literal() {
    let catalog = create_test_catalog();
    let resolver = NameResolver::new(&catalog);

    let table = resolver.resolve_table("users", test_span()).unwrap();

    let expr = Expr {
        kind: ExprKind::VectorLiteral(vec![1.0, 2.0, 3.0]),
        span: test_span(),
    };

    let result = resolver.resolve_expr(&expr, table);
    assert!(result.is_ok());
}

// =============================================================================
// ResolvedColumn tests
// =============================================================================

#[test]
fn test_resolved_column_equality() {
    let col1 = ResolvedColumn {
        table_name: "users".to_string(),
        column_name: "id".to_string(),
        column_index: 0,
        resolved_type: ResolvedType::Integer,
    };

    let col2 = ResolvedColumn {
        table_name: "users".to_string(),
        column_name: "id".to_string(),
        column_index: 0,
        resolved_type: ResolvedType::Integer,
    };

    let col3 = ResolvedColumn {
        table_name: "users".to_string(),
        column_name: "name".to_string(),
        column_index: 1,
        resolved_type: ResolvedType::Text,
    };

    assert_eq!(col1, col2);
    assert_ne!(col1, col3);
}

#[test]
fn test_resolved_column_clone() {
    let col = ResolvedColumn {
        table_name: "users".to_string(),
        column_name: "id".to_string(),
        column_index: 0,
        resolved_type: ResolvedType::Integer,
    };

    let cloned = col.clone();
    assert_eq!(col, cloned);
}
