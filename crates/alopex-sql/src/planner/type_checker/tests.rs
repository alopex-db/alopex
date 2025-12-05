//! Unit tests for TypeChecker.

use super::*;
use crate::ast::ddl::VectorMetric;
use crate::ast::expr::{BinaryOp, Expr, ExprKind, Literal, UnaryOp};
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
            ColumnMetadata::new("score", ResolvedType::Double),
            ColumnMetadata::new("active", ResolvedType::Boolean),
        ],
    )
    .with_primary_key(vec!["id".to_string()]);

    catalog.create_table(users_table).unwrap();

    catalog
}

/// Create a test catalog with a vector table.
fn create_vector_catalog() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();

    let embeddings_table = TableMetadata::new(
        "embeddings",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("text", ResolvedType::Text),
            ColumnMetadata::new(
                "embedding",
                ResolvedType::Vector {
                    dimension: 128,
                    metric: VectorMetric::Cosine,
                },
            )
            .with_not_null(true),
        ],
    );

    catalog.create_table(embeddings_table).unwrap();

    catalog
}

// =============================================================================
// infer_type tests - Literals
// =============================================================================

#[test]
fn test_infer_literal_integer() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Number("42".to_string())),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Integer);
}

#[test]
fn test_infer_literal_bigint() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // Number larger than i32::MAX
    let expr = Expr {
        kind: ExprKind::Literal(Literal::Number("9999999999".to_string())),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::BigInt);
}

#[test]
fn test_infer_literal_double() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Number("3.14".to_string())),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Double);
}

#[test]
fn test_infer_literal_double_scientific() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Number("1e10".to_string())),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Double);
}

#[test]
fn test_infer_literal_string() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::String("hello".to_string())),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Text);
}

#[test]
fn test_infer_literal_boolean() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Boolean(true)),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_literal_null() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::Literal(Literal::Null),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Null);
}

// =============================================================================
// infer_type tests - Column References
// =============================================================================

#[test]
fn test_infer_column_ref_integer() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: "id".to_string(),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Integer);
}

#[test]
fn test_infer_column_ref_text() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: "name".to_string(),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Text);
}

#[test]
fn test_infer_column_ref_not_found() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let span = span_at(3, 15);
    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: None,
            column: "unknown".to_string(),
        },
        span,
    };

    let result = type_checker.infer_type(&expr, table);
    assert!(result.is_err());

    match result.unwrap_err() {
        PlannerError::ColumnNotFound {
            column,
            table,
            line,
            col,
        } => {
            assert_eq!(column, "unknown");
            assert_eq!(table, "users");
            assert_eq!(line, 3);
            assert_eq!(col, 15);
        }
        e => panic!("Expected ColumnNotFound, got {:?}", e),
    }
}

// =============================================================================
// check_binary_op tests - Arithmetic
// =============================================================================

#[test]
fn test_check_binary_op_integer_add() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Add,
            &ResolvedType::Integer,
            &ResolvedType::Integer,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Integer);
}

#[test]
fn test_check_binary_op_integer_bigint_add() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Add,
            &ResolvedType::Integer,
            &ResolvedType::BigInt,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::BigInt);
}

#[test]
fn test_check_binary_op_integer_double_mul() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Mul,
            &ResolvedType::Integer,
            &ResolvedType::Double,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Double);
}

#[test]
fn test_check_binary_op_arithmetic_with_null() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Add,
            &ResolvedType::Integer,
            &ResolvedType::Null,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Null);
}

#[test]
fn test_check_binary_op_arithmetic_invalid_types() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker.check_binary_op(
        BinaryOp::Add,
        &ResolvedType::Text,
        &ResolvedType::Integer,
        test_span(),
    );

    assert!(result.is_err());
}

// =============================================================================
// check_binary_op tests - Comparison
// =============================================================================

#[test]
fn test_check_binary_op_comparison_same_type() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Eq,
            &ResolvedType::Integer,
            &ResolvedType::Integer,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Boolean);
}

#[test]
fn test_check_binary_op_comparison_numeric_types() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Lt,
            &ResolvedType::Integer,
            &ResolvedType::Double,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Boolean);
}

#[test]
fn test_check_binary_op_comparison_with_null() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Eq,
            &ResolvedType::Integer,
            &ResolvedType::Null,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Boolean);
}

#[test]
fn test_check_binary_op_comparison_incompatible() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker.check_binary_op(
        BinaryOp::Eq,
        &ResolvedType::Text,
        &ResolvedType::Integer,
        test_span(),
    );

    assert!(result.is_err());
}

// =============================================================================
// check_binary_op tests - Logical
// =============================================================================

#[test]
fn test_check_binary_op_logical_and() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::And,
            &ResolvedType::Boolean,
            &ResolvedType::Boolean,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Boolean);
}

#[test]
fn test_check_binary_op_logical_with_null() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::Or,
            &ResolvedType::Boolean,
            &ResolvedType::Null,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Boolean);
}

#[test]
fn test_check_binary_op_logical_invalid() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker.check_binary_op(
        BinaryOp::And,
        &ResolvedType::Integer,
        &ResolvedType::Boolean,
        test_span(),
    );

    assert!(result.is_err());
}

// =============================================================================
// check_binary_op tests - String Concatenation
// =============================================================================

#[test]
fn test_check_binary_op_string_concat() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::StringConcat,
            &ResolvedType::Text,
            &ResolvedType::Text,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Text);
}

#[test]
fn test_check_binary_op_string_concat_with_null() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker
        .check_binary_op(
            BinaryOp::StringConcat,
            &ResolvedType::Text,
            &ResolvedType::Null,
            test_span(),
        )
        .unwrap();

    assert_eq!(result, ResolvedType::Text);
}

#[test]
fn test_check_binary_op_string_concat_invalid() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker.check_binary_op(
        BinaryOp::StringConcat,
        &ResolvedType::Text,
        &ResolvedType::Integer,
        test_span(),
    );

    assert!(result.is_err());
}

// =============================================================================
// Unary operator tests
// =============================================================================

#[test]
fn test_infer_unary_not() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // NOT active
    let expr = Expr {
        kind: ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "active".to_string(),
                },
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_unary_not_invalid() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // NOT age (integer)
    let expr = Expr {
        kind: ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                },
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table);
    assert!(result.is_err());
}

#[test]
fn test_infer_unary_minus() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // -age
    let expr = Expr {
        kind: ExprKind::UnaryOp {
            op: UnaryOp::Minus,
            operand: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                },
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Integer);
}

#[test]
fn test_infer_unary_minus_invalid() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // -name (text)
    let expr = Expr {
        kind: ExprKind::UnaryOp {
            op: UnaryOp::Minus,
            operand: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table);
    assert!(result.is_err());
}

// =============================================================================
// normalize_metric tests
// =============================================================================

#[test]
fn test_normalize_metric_cosine() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    // Test various cases
    assert_eq!(
        type_checker.normalize_metric("cosine", test_span()).unwrap(),
        VectorMetric::Cosine
    );
    assert_eq!(
        type_checker.normalize_metric("COSINE", test_span()).unwrap(),
        VectorMetric::Cosine
    );
    assert_eq!(
        type_checker.normalize_metric("Cosine", test_span()).unwrap(),
        VectorMetric::Cosine
    );
}

#[test]
fn test_normalize_metric_l2() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    assert_eq!(
        type_checker.normalize_metric("l2", test_span()).unwrap(),
        VectorMetric::L2
    );
    assert_eq!(
        type_checker.normalize_metric("L2", test_span()).unwrap(),
        VectorMetric::L2
    );
}

#[test]
fn test_normalize_metric_inner() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    assert_eq!(
        type_checker.normalize_metric("inner", test_span()).unwrap(),
        VectorMetric::Inner
    );
    assert_eq!(
        type_checker.normalize_metric("INNER", test_span()).unwrap(),
        VectorMetric::Inner
    );
}

#[test]
fn test_normalize_metric_invalid() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let span = span_at(5, 20);
    let result = type_checker.normalize_metric("euclidean", span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::InvalidMetric { value, line, column } => {
            assert_eq!(value, "euclidean");
            assert_eq!(line, 5);
            assert_eq!(column, 20);
        }
        e => panic!("Expected InvalidMetric, got {:?}", e),
    }
}

// =============================================================================
// Vector type tests
// =============================================================================

#[test]
fn test_infer_vector_literal() {
    let catalog = create_vector_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("embeddings").unwrap();

    let expr = Expr {
        kind: ExprKind::VectorLiteral(vec![1.0, 2.0, 3.0, 4.0]),
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();

    match result.resolved_type {
        ResolvedType::Vector { dimension, metric } => {
            assert_eq!(dimension, 4);
            assert_eq!(metric, VectorMetric::Cosine);
        }
        _ => panic!("Expected Vector type"),
    }
}

#[test]
fn test_check_vector_dimension_match() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let result = type_checker.check_vector_dimension(128, 128, test_span());
    assert!(result.is_ok());
}

#[test]
fn test_check_vector_dimension_mismatch() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let span = span_at(4, 8);
    let result = type_checker.check_vector_dimension(128, 256, span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::VectorDimensionMismatch {
            expected,
            found,
            line,
            column,
        } => {
            assert_eq!(expected, 128);
            assert_eq!(found, 256);
            assert_eq!(line, 4);
            assert_eq!(column, 8);
        }
        e => panic!("Expected VectorDimensionMismatch, got {:?}", e),
    }
}

// =============================================================================
// check_insert_values tests
// =============================================================================

#[test]
fn test_check_insert_values_success() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![vec![
        Expr {
            kind: ExprKind::Literal(Literal::Number("1".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("Alice".to_string())),
            span: test_span(),
        },
    ]];

    let result = type_checker.check_insert_values(table, &columns, &values, test_span());
    assert!(result.is_ok());

    let typed_rows = result.unwrap();
    assert_eq!(typed_rows.len(), 1);
    assert_eq!(typed_rows[0].len(), 2);
}

#[test]
fn test_check_insert_values_without_columns() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // Insert without column list uses definition order
    let columns: Vec<String> = vec![];
    let values = vec![vec![
        Expr {
            kind: ExprKind::Literal(Literal::Number("1".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("Alice".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("alice@example.com".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::Number("30".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::Number("95.5".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::Boolean(true)),
            span: test_span(),
        },
    ]];

    let result = type_checker.check_insert_values(table, &columns, &values, test_span());
    assert!(result.is_ok());
}

#[test]
fn test_check_insert_values_column_count_mismatch() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![vec![
        // Only one value for two columns
        Expr {
            kind: ExprKind::Literal(Literal::Number("1".to_string())),
            span: test_span(),
        },
    ]];

    let span = span_at(2, 5);
    let result = type_checker.check_insert_values(table, &columns, &values, span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::ColumnValueCountMismatch {
            columns,
            values,
            line,
            column,
        } => {
            assert_eq!(columns, 2);
            assert_eq!(values, 1);
            assert_eq!(line, 2);
            assert_eq!(column, 5);
        }
        e => panic!("Expected ColumnValueCountMismatch, got {:?}", e),
    }
}

#[test]
fn test_check_insert_values_type_mismatch() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![vec![
        Expr {
            kind: ExprKind::Literal(Literal::String("not_a_number".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("Alice".to_string())),
            span: test_span(),
        },
    ]];

    let result = type_checker.check_insert_values(table, &columns, &values, test_span());

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::TypeMismatch { expected, found, .. } => {
            assert_eq!(expected, "Integer");
            assert_eq!(found, "Text");
        }
        e => panic!("Expected TypeMismatch, got {:?}", e),
    }
}

#[test]
fn test_check_insert_values_null_constraint_violation() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // id is NOT NULL
    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![vec![
        Expr {
            kind: ExprKind::Literal(Literal::Null),
            span: span_at(3, 10),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("Alice".to_string())),
            span: test_span(),
        },
    ]];

    let result = type_checker.check_insert_values(table, &columns, &values, test_span());

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::NullConstraintViolation { column, line, col } => {
            assert_eq!(column, "id");
            assert_eq!(line, 3);
            assert_eq!(col, 10);
        }
        e => panic!("Expected NullConstraintViolation, got {:?}", e),
    }
}

#[test]
fn test_check_insert_values_implicit_conversion() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // Insert Integer into Double column (score)
    let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
    let values = vec![vec![
        Expr {
            kind: ExprKind::Literal(Literal::Number("1".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::String("Alice".to_string())),
            span: test_span(),
        },
        Expr {
            kind: ExprKind::Literal(Literal::Number("90".to_string())), // Integer into Double
            span: test_span(),
        },
    ]];

    let result = type_checker.check_insert_values(table, &columns, &values, test_span());
    assert!(result.is_ok());
}

// =============================================================================
// check_assignment tests
// =============================================================================

#[test]
fn test_check_assignment_success() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let value = Expr {
        kind: ExprKind::Literal(Literal::String("Bob".to_string())),
        span: test_span(),
    };

    let result = type_checker.check_assignment(table, "name", &value, test_span());
    assert!(result.is_ok());

    let typed = result.unwrap();
    assert_eq!(typed.resolved_type, ResolvedType::Text);
}

#[test]
fn test_check_assignment_column_not_found() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let span = span_at(4, 12);
    let value = Expr {
        kind: ExprKind::Literal(Literal::String("value".to_string())),
        span: test_span(),
    };

    let result = type_checker.check_assignment(table, "unknown", &value, span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::ColumnNotFound { column, .. } => {
            assert_eq!(column, "unknown");
        }
        e => panic!("Expected ColumnNotFound, got {:?}", e),
    }
}

#[test]
fn test_check_assignment_type_mismatch() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let value = Expr {
        kind: ExprKind::Literal(Literal::String("not_a_number".to_string())),
        span: test_span(),
    };

    let result = type_checker.check_assignment(table, "age", &value, test_span());

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::TypeMismatch { expected, found, .. } => {
            assert_eq!(expected, "Integer");
            assert_eq!(found, "Text");
        }
        e => panic!("Expected TypeMismatch, got {:?}", e),
    }
}

#[test]
fn test_check_assignment_null_constraint_violation() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    let value = Expr {
        kind: ExprKind::Literal(Literal::Null),
        span: span_at(5, 15),
    };

    // name is NOT NULL
    let result = type_checker.check_assignment(table, "name", &value, test_span());

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::NullConstraintViolation { column, .. } => {
            assert_eq!(column, "name");
        }
        e => panic!("Expected NullConstraintViolation, got {:?}", e),
    }
}

// =============================================================================
// check_null_constraint tests
// =============================================================================

#[test]
fn test_check_null_constraint_ok_non_null_value() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let column = ColumnMetadata::new("test", ResolvedType::Integer).with_not_null(true);

    let value = TypedExpr {
        kind: TypedExprKind::Literal(Literal::Number("42".to_string())),
        resolved_type: ResolvedType::Integer,
        span: test_span(),
    };

    let result = type_checker.check_null_constraint(&column, &value, test_span());
    assert!(result.is_ok());
}

#[test]
fn test_check_null_constraint_ok_nullable_column() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    // Column allows NULL
    let column = ColumnMetadata::new("test", ResolvedType::Integer);

    let value = TypedExpr {
        kind: TypedExprKind::Literal(Literal::Null),
        resolved_type: ResolvedType::Null,
        span: test_span(),
    };

    let result = type_checker.check_null_constraint(&column, &value, test_span());
    assert!(result.is_ok());
}

#[test]
fn test_check_null_constraint_violation() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let column = ColumnMetadata::new("test_col", ResolvedType::Integer).with_not_null(true);

    let value = TypedExpr {
        kind: TypedExprKind::Literal(Literal::Null),
        resolved_type: ResolvedType::Null,
        span: test_span(),
    };

    let span = span_at(2, 8);
    let result = type_checker.check_null_constraint(&column, &value, span);

    assert!(result.is_err());
    match result.unwrap_err() {
        PlannerError::NullConstraintViolation { column, line, col } => {
            assert_eq!(column, "test_col");
            assert_eq!(line, 2);
            assert_eq!(col, 8);
        }
        e => panic!("Expected NullConstraintViolation, got {:?}", e),
    }
}

// =============================================================================
// Vector function tests
// =============================================================================

#[test]
fn test_check_vector_distance_success() {
    let catalog = create_vector_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let args = vec![
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "embeddings".to_string(),
                column: "embedding".to_string(),
                column_index: 2,
            },
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        TypedExpr {
            kind: TypedExprKind::VectorLiteral(vec![0.0; 128]),
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        TypedExpr {
            kind: TypedExprKind::Literal(Literal::String("cosine".to_string())),
            resolved_type: ResolvedType::Text,
            span: test_span(),
        },
    ];

    let result = type_checker.check_vector_distance(&args, test_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ResolvedType::Double);
}

#[test]
fn test_check_vector_distance_wrong_arg_count() {
    let catalog = create_vector_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let args = vec![
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "embeddings".to_string(),
                column: "embedding".to_string(),
                column_index: 2,
            },
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        // Missing second and third argument
    ];

    let result = type_checker.check_vector_distance(&args, test_span());
    assert!(result.is_err());
}

#[test]
fn test_check_vector_distance_dimension_mismatch() {
    let catalog = create_vector_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let args = vec![
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "embeddings".to_string(),
                column: "embedding".to_string(),
                column_index: 2,
            },
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        TypedExpr {
            kind: TypedExprKind::VectorLiteral(vec![0.0; 256]), // Wrong dimension
            resolved_type: ResolvedType::Vector {
                dimension: 256,
                metric: VectorMetric::Cosine,
            },
            span: span_at(3, 12),
        },
        TypedExpr {
            kind: TypedExprKind::Literal(Literal::String("cosine".to_string())),
            resolved_type: ResolvedType::Text,
            span: test_span(),
        },
    ];

    let result = type_checker.check_vector_distance(&args, test_span());
    assert!(result.is_err());

    match result.unwrap_err() {
        PlannerError::VectorDimensionMismatch {
            expected, found, ..
        } => {
            assert_eq!(expected, 128);
            assert_eq!(found, 256);
        }
        e => panic!("Expected VectorDimensionMismatch, got {:?}", e),
    }
}

#[test]
fn test_check_vector_distance_invalid_metric() {
    let catalog = create_vector_catalog();
    let type_checker = TypeChecker::new(&catalog);

    let args = vec![
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "embeddings".to_string(),
                column: "embedding".to_string(),
                column_index: 2,
            },
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        TypedExpr {
            kind: TypedExprKind::VectorLiteral(vec![0.0; 128]),
            resolved_type: ResolvedType::Vector {
                dimension: 128,
                metric: VectorMetric::Cosine,
            },
            span: test_span(),
        },
        TypedExpr {
            kind: TypedExprKind::Literal(Literal::String("invalid_metric".to_string())),
            resolved_type: ResolvedType::Text,
            span: span_at(4, 20),
        },
    ];

    let result = type_checker.check_vector_distance(&args, test_span());
    assert!(result.is_err());

    match result.unwrap_err() {
        PlannerError::InvalidMetric { value, .. } => {
            assert_eq!(value, "invalid_metric");
        }
        e => panic!("Expected InvalidMetric, got {:?}", e),
    }
}

// =============================================================================
// Complex expression tests
// =============================================================================

#[test]
fn test_infer_complex_expression() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // age + 10 > 25 AND active
    let expr = Expr {
        kind: ExprKind::BinaryOp {
            left: Box::new(Expr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(Expr {
                        kind: ExprKind::BinaryOp {
                            left: Box::new(Expr {
                                kind: ExprKind::ColumnRef {
                                    table: None,
                                    column: "age".to_string(),
                                },
                                span: test_span(),
                            }),
                            op: BinaryOp::Add,
                            right: Box::new(Expr {
                                kind: ExprKind::Literal(Literal::Number("10".to_string())),
                                span: test_span(),
                            }),
                        },
                        span: test_span(),
                    }),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr {
                        kind: ExprKind::Literal(Literal::Number("25".to_string())),
                        span: test_span(),
                    }),
                },
                span: test_span(),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "active".to_string(),
                },
                span: test_span(),
            }),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_between_expression() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

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

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_in_list_expression() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

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

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_like_expression() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

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

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

#[test]
fn test_infer_is_null_expression() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

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

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Boolean);
}

// ============================================================
// Table Qualifier Tests
// ============================================================

#[test]
fn test_infer_column_ref_with_valid_qualifier() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // users.name - correct table qualifier
    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: Some("users".to_string()),
            column: "name".to_string(),
        },
        span: test_span(),
    };

    let result = type_checker.infer_type(&expr, table).unwrap();
    assert_eq!(result.resolved_type, ResolvedType::Text);
}

#[test]
fn test_infer_column_ref_with_invalid_qualifier() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // other_table.name - incorrect table qualifier
    let expr = Expr {
        kind: ExprKind::ColumnRef {
            table: Some("other_table".to_string()),
            column: "name".to_string(),
        },
        span: span_at(5, 10),
    };

    let result = type_checker.infer_type(&expr, table);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, PlannerError::TableNotFound { name, .. } if name == "other_table"));
}

// ============================================================
// Unknown Function Tests
// ============================================================

#[test]
fn test_unknown_function_error() {
    let catalog = create_test_catalog();
    let type_checker = TypeChecker::new(&catalog);
    let table = catalog.get_table("users").unwrap();

    // unknown_func(id)
    let expr = Expr {
        kind: ExprKind::FunctionCall {
            name: "unknown_func".to_string(),
            args: vec![Expr {
                kind: ExprKind::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                },
                span: test_span(),
            }],
        },
        span: span_at(3, 5),
    };

    let result = type_checker.infer_type(&expr, table);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, PlannerError::UnsupportedFeature { feature, .. } if feature.contains("unknown_func"))
    );
}
