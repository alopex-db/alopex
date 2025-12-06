//! Type checking module for the Alopex SQL dialect.
//!
//! This module provides type inference and validation for SQL expressions.
//! It checks that expressions are well-typed and that operations are valid
//! for the types involved.

use crate::ast::Span;
use crate::ast::ddl::VectorMetric;
use crate::ast::expr::{BinaryOp, Expr, ExprKind, Literal, UnaryOp};
use crate::catalog::{Catalog, TableMetadata};
use crate::planner::error::PlannerError;
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::planner::types::ResolvedType;

/// Type checker for SQL expressions.
///
/// Performs type inference and validation for expressions, ensuring that
/// operations are valid for the types involved and that constraints are met.
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::MemoryCatalog;
/// use alopex_sql::planner::type_checker::TypeChecker;
///
/// let catalog = MemoryCatalog::new();
/// let type_checker = TypeChecker::new(&catalog);
/// ```
pub struct TypeChecker<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> TypeChecker<'a, C> {
    /// Create a new TypeChecker with the given catalog.
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &'a C {
        self.catalog
    }

    /// Infer the type of an expression within a table context.
    ///
    /// Recursively analyzes the expression to determine its type, resolving
    /// column references against the provided table metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A column reference cannot be resolved
    /// - A binary operation is invalid for the operand types
    /// - A function call has invalid arguments
    pub fn infer_type(
        &self,
        expr: &Expr,
        table: &TableMetadata,
    ) -> Result<TypedExpr, PlannerError> {
        let span = expr.span;
        match &expr.kind {
            ExprKind::Literal(lit) => self.infer_literal_type(lit, span),

            ExprKind::ColumnRef {
                table: table_qualifier,
                column,
            } => {
                // If table qualifier is present, verify it matches the current table
                if let Some(qualifier) = table_qualifier
                    && qualifier != &table.name
                {
                    return Err(PlannerError::TableNotFound {
                        name: qualifier.clone(),
                        line: span.start.line,
                        column: span.start.column,
                    });
                }
                self.infer_column_ref_type(table, column, span)
            }

            ExprKind::BinaryOp { left, op, right } => {
                self.infer_binary_op_type(left, *op, right, table, span)
            }

            ExprKind::UnaryOp { op, operand } => {
                self.infer_unary_op_type(*op, operand, table, span)
            }

            ExprKind::FunctionCall { name, args } => {
                self.infer_function_call_type(name, args, table, span)
            }

            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => self.infer_between_type(expr, low, high, *negated, table, span),

            ExprKind::Like {
                expr,
                pattern,
                escape,
                negated,
            } => self.infer_like_type(expr, pattern, escape.as_deref(), *negated, table, span),

            ExprKind::InList {
                expr,
                list,
                negated,
            } => self.infer_in_list_type(expr, list, *negated, table, span),

            ExprKind::IsNull { expr, negated } => {
                self.infer_is_null_type(expr, *negated, table, span)
            }

            ExprKind::VectorLiteral(values) => self.infer_vector_literal_type(values, span),
        }
    }

    /// Infer the type of a literal value.
    fn infer_literal_type(&self, lit: &Literal, span: Span) -> Result<TypedExpr, PlannerError> {
        let (kind, resolved_type) = match lit {
            Literal::Number(s) => {
                // Determine if it's integer or floating point
                let resolved_type = if s.contains('.') || s.contains('e') || s.contains('E') {
                    ResolvedType::Double
                } else {
                    // Check if it fits in i32 or needs i64
                    if s.parse::<i32>().is_ok() {
                        ResolvedType::Integer
                    } else {
                        ResolvedType::BigInt
                    }
                };
                (TypedExprKind::Literal(lit.clone()), resolved_type)
            }
            Literal::String(_) => (TypedExprKind::Literal(lit.clone()), ResolvedType::Text),
            Literal::Boolean(_) => (TypedExprKind::Literal(lit.clone()), ResolvedType::Boolean),
            Literal::Null => (TypedExprKind::Literal(lit.clone()), ResolvedType::Null),
        };

        Ok(TypedExpr {
            kind,
            resolved_type,
            span,
        })
    }

    /// Infer the type of a column reference.
    fn infer_column_ref_type(
        &self,
        table: &TableMetadata,
        column_name: &str,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        // Find the column in the table
        let (column_index, column) = table
            .columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == column_name)
            .ok_or_else(|| PlannerError::ColumnNotFound {
                column: column_name.to_string(),
                table: table.name.clone(),
                line: span.start.line,
                col: span.start.column,
            })?;

        Ok(TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: table.name.clone(),
                column: column_name.to_string(),
                column_index,
            },
            resolved_type: column.data_type.clone(),
            span,
        })
    }

    /// Infer the type of a binary operation.
    fn infer_binary_op_type(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let left_typed = self.infer_type(left, table)?;
        let right_typed = self.infer_type(right, table)?;

        let result_type = self.check_binary_op(
            op,
            &left_typed.resolved_type,
            &right_typed.resolved_type,
            span,
        )?;

        Ok(TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(left_typed),
                op,
                right: Box::new(right_typed),
            },
            resolved_type: result_type,
            span,
        })
    }

    /// Check binary operation and return the result type.
    ///
    /// Validates that the operator is valid for the given operand types
    /// and returns the result type.
    ///
    /// # Type Rules
    ///
    /// - Arithmetic operators (+, -, *, /, %): Require numeric operands
    /// - Comparison operators (=, <>, <, >, <=, >=): Require compatible types
    /// - Logical operators (AND, OR): Require boolean operands
    /// - String concatenation (||): Requires text operands
    pub fn check_binary_op(
        &self,
        op: BinaryOp,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        use BinaryOp::*;
        use ResolvedType::*;

        match op {
            // Arithmetic operators: require numeric types
            Add | Sub | Mul | Div | Mod => {
                let result = self.check_arithmetic_op(left, right, span)?;
                Ok(result)
            }

            // Comparison operators: require compatible types, return boolean
            Eq | Neq | Lt | Gt | LtEq | GtEq => {
                self.check_comparison_op(left, right, span)?;
                Ok(Boolean)
            }

            // Logical operators: require boolean types
            And | Or => {
                self.check_logical_op(left, right, span)?;
                Ok(Boolean)
            }

            // String concatenation: requires text types
            StringConcat => {
                self.check_string_concat_op(left, right, span)?;
                Ok(Text)
            }
        }
    }

    /// Check arithmetic operation and return the result type.
    fn check_arithmetic_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        use ResolvedType::*;

        // Handle NULL propagation
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Determine result type based on numeric type hierarchy
        match (left, right) {
            // Integer operations
            (Integer, Integer) => Ok(Integer),
            (Integer, BigInt) | (BigInt, Integer) | (BigInt, BigInt) => Ok(BigInt),
            (Integer, Float) | (Float, Integer) | (Float, Float) => Ok(Float),
            (Integer, Double)
            | (Double, Integer)
            | (BigInt, Float)
            | (Float, BigInt)
            | (BigInt, Double)
            | (Double, BigInt)
            | (Float, Double)
            | (Double, Float)
            | (Double, Double) => Ok(Double),

            _ => Err(PlannerError::InvalidOperator {
                op: "arithmetic".to_string(),
                type_name: format!("{} and {}", left.type_name(), right.type_name()),
                line: span.start.line,
                column: span.start.column,
            }),
        }
    }

    /// Check comparison operation for compatible types.
    fn check_comparison_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<(), PlannerError> {
        use ResolvedType::*;

        // NULL can be compared with anything
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(());
        }

        // Check type compatibility
        let compatible = match (left, right) {
            // Same types are always comparable
            (a, b) if a == b => true,

            // Numeric types are comparable with each other
            (Integer | BigInt | Float | Double, Integer | BigInt | Float | Double) => true,

            // Text types
            (Text, Text) => true,

            // Boolean types
            (Boolean, Boolean) => true,

            // Timestamp types
            (Timestamp, Timestamp) => true,

            // Vector types (for equality only, dimension must match)
            (Vector { dimension: d1, .. }, Vector { dimension: d2, .. }) => d1 == d2,

            _ => false,
        };

        if compatible {
            Ok(())
        } else {
            Err(PlannerError::TypeMismatch {
                expected: left.type_name().to_string(),
                found: right.type_name().to_string(),
                line: span.start.line,
                column: span.start.column,
            })
        }
    }

    /// Check logical operation for boolean types.
    fn check_logical_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<(), PlannerError> {
        use ResolvedType::*;

        // NULL is allowed (three-valued logic)
        let left_ok = matches!(left, Boolean | Null);
        let right_ok = matches!(right, Boolean | Null);

        if !left_ok {
            return Err(PlannerError::TypeMismatch {
                expected: "Boolean".to_string(),
                found: left.type_name().to_string(),
                line: span.start.line,
                column: span.start.column,
            });
        }

        if !right_ok {
            return Err(PlannerError::TypeMismatch {
                expected: "Boolean".to_string(),
                found: right.type_name().to_string(),
                line: span.start.line,
                column: span.start.column,
            });
        }

        Ok(())
    }

    /// Check string concatenation operation.
    fn check_string_concat_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<(), PlannerError> {
        use ResolvedType::*;

        // NULL is allowed
        let left_ok = matches!(left, Text | Null);
        let right_ok = matches!(right, Text | Null);

        if !left_ok {
            return Err(PlannerError::TypeMismatch {
                expected: "Text".to_string(),
                found: left.type_name().to_string(),
                line: span.start.line,
                column: span.start.column,
            });
        }

        if !right_ok {
            return Err(PlannerError::TypeMismatch {
                expected: "Text".to_string(),
                found: right.type_name().to_string(),
                line: span.start.line,
                column: span.start.column,
            });
        }

        Ok(())
    }

    /// Infer the type of a unary operation.
    fn infer_unary_op_type(
        &self,
        op: UnaryOp,
        operand: &Expr,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let operand_typed = self.infer_type(operand, table)?;

        let result_type = match op {
            UnaryOp::Not => {
                // NOT requires boolean operand
                if !matches!(
                    operand_typed.resolved_type,
                    ResolvedType::Boolean | ResolvedType::Null
                ) {
                    return Err(PlannerError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        found: operand_typed.resolved_type.type_name().to_string(),
                        line: span.start.line,
                        column: span.start.column,
                    });
                }
                ResolvedType::Boolean
            }
            UnaryOp::Minus => {
                // Unary minus requires numeric operand
                match &operand_typed.resolved_type {
                    ResolvedType::Integer => ResolvedType::Integer,
                    ResolvedType::BigInt => ResolvedType::BigInt,
                    ResolvedType::Float => ResolvedType::Float,
                    ResolvedType::Double => ResolvedType::Double,
                    ResolvedType::Null => ResolvedType::Null,
                    other => {
                        return Err(PlannerError::InvalidOperator {
                            op: "unary minus".to_string(),
                            type_name: other.type_name().to_string(),
                            line: span.start.line,
                            column: span.start.column,
                        });
                    }
                }
            }
        };

        Ok(TypedExpr {
            kind: TypedExprKind::UnaryOp {
                op,
                operand: Box::new(operand_typed),
            },
            resolved_type: result_type,
            span,
        })
    }

    /// Infer the type of a function call.
    fn infer_function_call_type(
        &self,
        name: &str,
        args: &[Expr],
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        // Type-check all arguments first
        let typed_args: Vec<TypedExpr> = args
            .iter()
            .map(|arg| self.infer_type(arg, table))
            .collect::<Result<Vec<_>, _>>()?;

        // Delegate to check_function_call for validation and return type
        let result_type = self.check_function_call(name, &typed_args, span)?;

        Ok(TypedExpr {
            kind: TypedExprKind::FunctionCall {
                name: name.to_string(),
                args: typed_args,
            },
            resolved_type: result_type,
            span,
        })
    }

    /// Infer the type of a BETWEEN expression.
    fn infer_between_type(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type(expr, table)?;
        let low_typed = self.infer_type(low, table)?;
        let high_typed = self.infer_type(high, table)?;

        // Check that all three expressions have compatible types
        self.check_comparison_op(&expr_typed.resolved_type, &low_typed.resolved_type, span)?;
        self.check_comparison_op(&expr_typed.resolved_type, &high_typed.resolved_type, span)?;

        Ok(TypedExpr {
            kind: TypedExprKind::Between {
                expr: Box::new(expr_typed),
                low: Box::new(low_typed),
                high: Box::new(high_typed),
                negated,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    /// Infer the type of a LIKE expression.
    fn infer_like_type(
        &self,
        expr: &Expr,
        pattern: &Expr,
        escape: Option<&Expr>,
        negated: bool,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type(expr, table)?;
        let pattern_typed = self.infer_type(pattern, table)?;

        // Expression must be text
        if !matches!(
            expr_typed.resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::TypeMismatch {
                expected: "Text".to_string(),
                found: expr_typed.resolved_type.type_name().to_string(),
                line: expr.span.start.line,
                column: expr.span.start.column,
            });
        }

        // Pattern must be text
        if !matches!(
            pattern_typed.resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::TypeMismatch {
                expected: "Text".to_string(),
                found: pattern_typed.resolved_type.type_name().to_string(),
                line: pattern.span.start.line,
                column: pattern.span.start.column,
            });
        }

        let escape_typed = if let Some(esc) = escape {
            let typed = self.infer_type(esc, table)?;
            if !matches!(typed.resolved_type, ResolvedType::Text | ResolvedType::Null) {
                return Err(PlannerError::TypeMismatch {
                    expected: "Text".to_string(),
                    found: typed.resolved_type.type_name().to_string(),
                    line: esc.span.start.line,
                    column: esc.span.start.column,
                });
            }
            Some(Box::new(typed))
        } else {
            None
        };

        Ok(TypedExpr {
            kind: TypedExprKind::Like {
                expr: Box::new(expr_typed),
                pattern: Box::new(pattern_typed),
                escape: escape_typed,
                negated,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    /// Infer the type of an IN list expression.
    fn infer_in_list_type(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type(expr, table)?;

        let typed_list: Vec<TypedExpr> = list
            .iter()
            .map(|item| {
                let typed = self.infer_type(item, table)?;
                // Check each item is compatible with the expression
                self.check_comparison_op(
                    &expr_typed.resolved_type,
                    &typed.resolved_type,
                    item.span,
                )?;
                Ok(typed)
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;

        Ok(TypedExpr {
            kind: TypedExprKind::InList {
                expr: Box::new(expr_typed),
                list: typed_list,
                negated,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    /// Infer the type of an IS NULL expression.
    fn infer_is_null_type(
        &self,
        expr: &Expr,
        negated: bool,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type(expr, table)?;

        Ok(TypedExpr {
            kind: TypedExprKind::IsNull {
                expr: Box::new(expr_typed),
                negated,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    /// Infer the type of a vector literal.
    fn infer_vector_literal_type(
        &self,
        values: &[f64],
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        Ok(TypedExpr {
            kind: TypedExprKind::VectorLiteral(values.to_vec()),
            resolved_type: ResolvedType::Vector {
                dimension: values.len() as u32,
                metric: VectorMetric::Cosine, // Default metric for literals
            },
            span,
        })
    }

    /// Normalize a metric string to VectorMetric enum (case-insensitive).
    ///
    /// # Valid Values
    ///
    /// - "cosine" (case-insensitive) → `VectorMetric::Cosine`
    /// - "l2" (case-insensitive) → `VectorMetric::L2`
    /// - "inner" (case-insensitive) → `VectorMetric::Inner`
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::InvalidMetric` if the value is not recognized.
    pub fn normalize_metric(&self, metric: &str, span: Span) -> Result<VectorMetric, PlannerError> {
        match metric.to_lowercase().as_str() {
            "cosine" => Ok(VectorMetric::Cosine),
            "l2" => Ok(VectorMetric::L2),
            "inner" => Ok(VectorMetric::Inner),
            _ => Err(PlannerError::InvalidMetric {
                value: metric.to_string(),
                line: span.start.line,
                column: span.start.column,
            }),
        }
    }

    /// Check function call and return the result type.
    ///
    /// Validates that the function arguments have correct types and returns
    /// the result type.
    pub fn check_function_call(
        &self,
        name: &str,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        let lower_name = name.to_lowercase();

        match lower_name.as_str() {
            "vector_distance" => self.check_vector_distance(args, span),
            "vector_similarity" => self.check_vector_similarity(args, span),
            // Add more built-in functions here as needed
            _ => {
                // Unknown function is an error
                Err(PlannerError::UnsupportedFeature {
                    feature: format!("function '{}'", name),
                    version: "future".to_string(),
                    line: span.start.line,
                    column: span.start.column,
                })
            }
        }
    }

    /// Check vector_distance function arguments.
    ///
    /// Signature: `vector_distance(column: Vector, vector: Vector, metric: Text) -> Double`
    ///
    /// # Requirements
    ///
    /// - First argument must be a Vector type (column reference)
    /// - Second argument must be a Vector type (vector literal)
    /// - Third argument must be a Text type (metric string)
    /// - Vector dimensions must match
    pub fn check_vector_distance(
        &self,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if args.len() != 3 {
            return Err(PlannerError::TypeMismatch {
                expected: "3 arguments".to_string(),
                found: format!("{} arguments", args.len()),
                line: span.start.line,
                column: span.start.column,
            });
        }

        // First argument: Vector column
        let col_dim = match &args[0].resolved_type {
            ResolvedType::Vector { dimension, .. } => *dimension,
            other => {
                return Err(PlannerError::TypeMismatch {
                    expected: "Vector".to_string(),
                    found: other.type_name().to_string(),
                    line: args[0].span.start.line,
                    column: args[0].span.start.column,
                });
            }
        };

        // Second argument: Vector literal
        let vec_dim = match &args[1].resolved_type {
            ResolvedType::Vector { dimension, .. } => *dimension,
            other => {
                return Err(PlannerError::TypeMismatch {
                    expected: "Vector".to_string(),
                    found: other.type_name().to_string(),
                    line: args[1].span.start.line,
                    column: args[1].span.start.column,
                });
            }
        };

        // Check dimension match
        self.check_vector_dimension(col_dim, vec_dim, args[1].span)?;

        // Third argument: Metric string
        match &args[2].resolved_type {
            ResolvedType::Text => {
                // Validate metric value if it's a literal
                if let TypedExprKind::Literal(Literal::String(s)) = &args[2].kind {
                    self.normalize_metric(s, args[2].span)?;
                }
            }
            ResolvedType::Null => {
                // NULL metric is not allowed
                return Err(PlannerError::TypeMismatch {
                    expected: "Text (metric)".to_string(),
                    found: "Null".to_string(),
                    line: args[2].span.start.line,
                    column: args[2].span.start.column,
                });
            }
            other => {
                return Err(PlannerError::TypeMismatch {
                    expected: "Text (metric)".to_string(),
                    found: other.type_name().to_string(),
                    line: args[2].span.start.line,
                    column: args[2].span.start.column,
                });
            }
        }

        Ok(ResolvedType::Double)
    }

    /// Check vector_similarity function arguments.
    ///
    /// Signature: `vector_similarity(column: Vector, vector: Vector, metric: Text) -> Double`
    ///
    /// Same validation rules as vector_distance.
    pub fn check_vector_similarity(
        &self,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        // Same validation as vector_distance
        self.check_vector_distance(args, span)
    }

    /// Check that two vector dimensions match.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::VectorDimensionMismatch` if dimensions don't match.
    pub fn check_vector_dimension(
        &self,
        expected: u32,
        found: u32,
        span: Span,
    ) -> Result<(), PlannerError> {
        if expected != found {
            Err(PlannerError::VectorDimensionMismatch {
                expected,
                found,
                line: span.start.line,
                column: span.start.column,
            })
        } else {
            Ok(())
        }
    }

    // ============================================================
    // INSERT/UPDATE Type Checking Methods (Task 13)
    // ============================================================

    /// Check INSERT values against table columns.
    ///
    /// Validates that:
    /// - The number of values matches the number of columns
    /// - Each value's type is compatible with the column type
    /// - NOT NULL constraints are satisfied
    /// - Vector dimensions match for vector columns
    ///
    /// # Column Order
    ///
    /// If `columns` is empty, uses `TableMetadata.column_names()` order (definition order).
    ///
    /// # Errors
    ///
    /// - `ColumnValueCountMismatch`: Number of values doesn't match columns
    /// - `TypeMismatch`: Value type incompatible with column type
    /// - `NullConstraintViolation`: NULL value for NOT NULL column
    /// - `VectorDimensionMismatch`: Vector dimension mismatch
    pub fn check_insert_values(
        &self,
        table: &TableMetadata,
        columns: &[String],
        values: &[Vec<Expr>],
        span: Span,
    ) -> Result<Vec<Vec<TypedExpr>>, PlannerError> {
        // Determine the target columns
        let target_columns: Vec<&str> = if columns.is_empty() {
            table.column_names()
        } else {
            columns.iter().map(|s| s.as_str()).collect()
        };

        let mut typed_rows = Vec::with_capacity(values.len());

        for row in values {
            // Check value count matches column count
            if row.len() != target_columns.len() {
                return Err(PlannerError::ColumnValueCountMismatch {
                    columns: target_columns.len(),
                    values: row.len(),
                    line: span.start.line,
                    column: span.start.column,
                });
            }

            let mut typed_values = Vec::with_capacity(row.len());

            for (value, col_name) in row.iter().zip(target_columns.iter()) {
                // Get column metadata
                let col_meta =
                    table
                        .get_column(col_name)
                        .ok_or_else(|| PlannerError::ColumnNotFound {
                            column: col_name.to_string(),
                            table: table.name.clone(),
                            line: span.start.line,
                            col: span.start.column,
                        })?;

                // Type-check the value expression
                let typed_value = self.infer_type(value, table)?;

                // Check NOT NULL constraint
                self.check_null_constraint(col_meta, &typed_value, value.span)?;

                // Check type compatibility
                self.check_type_compatibility(
                    &col_meta.data_type,
                    &typed_value.resolved_type,
                    value.span,
                )?;

                // For vector types, also check dimension
                if let (
                    ResolvedType::Vector {
                        dimension: expected_dim,
                        ..
                    },
                    ResolvedType::Vector {
                        dimension: actual_dim,
                        ..
                    },
                ) = (&col_meta.data_type, &typed_value.resolved_type)
                {
                    self.check_vector_dimension(*expected_dim, *actual_dim, value.span)?;
                }

                typed_values.push(typed_value);
            }

            typed_rows.push(typed_values);
        }

        Ok(typed_rows)
    }

    /// Check UPDATE assignment type compatibility.
    ///
    /// Validates that the value's type is compatible with the column type.
    ///
    /// # Errors
    ///
    /// - `ColumnNotFound`: Column doesn't exist
    /// - `TypeMismatch`: Value type incompatible with column type
    /// - `NullConstraintViolation`: NULL value for NOT NULL column
    /// - `VectorDimensionMismatch`: Vector dimension mismatch
    pub fn check_assignment(
        &self,
        table: &TableMetadata,
        column: &str,
        value: &Expr,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        // Get column metadata
        let col_meta = table
            .get_column(column)
            .ok_or_else(|| PlannerError::ColumnNotFound {
                column: column.to_string(),
                table: table.name.clone(),
                line: span.start.line,
                col: span.start.column,
            })?;

        // Type-check the value expression
        let typed_value = self.infer_type(value, table)?;

        // Check NOT NULL constraint
        self.check_null_constraint(col_meta, &typed_value, value.span)?;

        // Check type compatibility
        self.check_type_compatibility(&col_meta.data_type, &typed_value.resolved_type, value.span)?;

        // For vector types, also check dimension
        if let (
            ResolvedType::Vector {
                dimension: expected_dim,
                ..
            },
            ResolvedType::Vector {
                dimension: actual_dim,
                ..
            },
        ) = (&col_meta.data_type, &typed_value.resolved_type)
        {
            self.check_vector_dimension(*expected_dim, *actual_dim, value.span)?;
        }

        Ok(typed_value)
    }

    /// Check NOT NULL constraint for a value.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::NullConstraintViolation` if the column has NOT NULL
    /// constraint and the value is NULL.
    pub fn check_null_constraint(
        &self,
        column: &crate::catalog::ColumnMetadata,
        value: &TypedExpr,
        span: Span,
    ) -> Result<(), PlannerError> {
        if column.not_null && matches!(value.resolved_type, ResolvedType::Null) {
            Err(PlannerError::NullConstraintViolation {
                column: column.name.clone(),
                line: span.start.line,
                col: span.start.column,
            })
        } else {
            Ok(())
        }
    }

    /// Check type compatibility between expected and actual types.
    ///
    /// Uses implicit type conversion rules defined in `ResolvedType::can_cast_to`.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::TypeMismatch` if types are incompatible.
    fn check_type_compatibility(
        &self,
        expected: &ResolvedType,
        actual: &ResolvedType,
        span: Span,
    ) -> Result<(), PlannerError> {
        // Same type is always compatible
        if expected == actual {
            return Ok(());
        }

        // Check if implicit cast is allowed
        if actual.can_cast_to(expected) {
            return Ok(());
        }

        // Special case: Vector types with same dimension but different metric are compatible
        // (the column's metric is used)
        if let (
            ResolvedType::Vector {
                dimension: d1,
                metric: _,
            },
            ResolvedType::Vector {
                dimension: d2,
                metric: _,
            },
        ) = (expected, actual)
        {
            // Dimensions must match for vector compatibility
            if *d1 == *d2 {
                return Ok(());
            }
            // Different dimensions will fall through to TypeMismatch error
        }

        Err(PlannerError::TypeMismatch {
            expected: expected.type_name().to_string(),
            found: actual.type_name().to_string(),
            line: span.start.line,
            column: span.start.column,
        })
    }
}

// Tests are in type_checker/tests.rs
#[cfg(test)]
#[path = "type_checker/tests.rs"]
mod tests;
