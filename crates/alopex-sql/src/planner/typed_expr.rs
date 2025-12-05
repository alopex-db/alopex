//! Type-checked expression types for the planner.
//!
//! This module defines [`TypedExpr`] and related types that represent
//! expressions after type checking. These types carry resolved type
//! information and are used in [`crate::planner::LogicalPlan`] construction.
//!
//! # Overview
//!
//! - [`TypedExpr`]: A type-checked expression with resolved type and span
//! - [`TypedExprKind`]: The kind of typed expression (literals, column refs, operators, etc.)
//! - [`SortExpr`]: A sort expression for ORDER BY clauses
//! - [`TypedAssignment`]: A typed assignment for UPDATE SET clauses
//! - [`ProjectedColumn`]: A projected column for SELECT clauses
//! - [`Projection`]: The projection specification for SELECT

use crate::ast::expr::{BinaryOp, Literal, UnaryOp};
use crate::ast::span::Span;
use crate::planner::types::ResolvedType;

/// A type-checked expression with resolved type information.
///
/// This struct represents an expression that has been validated by the type checker.
/// It contains the expression kind, the resolved type, and the source span for
/// error reporting.
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::typed_expr::{TypedExpr, TypedExprKind};
/// use alopex_sql::planner::types::ResolvedType;
/// use alopex_sql::ast::expr::Literal;
/// use alopex_sql::Span;
///
/// let expr = TypedExpr {
///     kind: TypedExprKind::Literal(Literal::Number("42".to_string())),
///     resolved_type: ResolvedType::Integer,
///     span: Span::default(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct TypedExpr {
    /// The kind of expression.
    pub kind: TypedExprKind,
    /// The resolved type of this expression.
    pub resolved_type: ResolvedType,
    /// Source span for error reporting.
    pub span: Span,
}

/// The kind of a typed expression.
///
/// Each variant corresponds to a different expression type that has been
/// type-checked. Unlike [`ExprKind`](crate::ast::expr::ExprKind), column
/// references include the resolved column index for efficient access.
#[derive(Debug, Clone)]
pub enum TypedExprKind {
    /// A literal value.
    Literal(Literal),

    /// A column reference with resolved table and column index.
    ColumnRef {
        /// The table name (resolved, never None after name resolution).
        table: String,
        /// The column name.
        column: String,
        /// The column index in the table's column list (0-based).
        /// This allows efficient column access during execution.
        column_index: usize,
    },

    /// A binary operation.
    BinaryOp {
        /// Left operand.
        left: Box<TypedExpr>,
        /// The operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<TypedExpr>,
    },

    /// A unary operation.
    UnaryOp {
        /// The operator.
        op: UnaryOp,
        /// The operand.
        operand: Box<TypedExpr>,
    },

    /// A function call.
    FunctionCall {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<TypedExpr>,
    },

    /// An explicit type cast.
    Cast {
        /// Expression to cast.
        expr: Box<TypedExpr>,
        /// Target type.
        target_type: ResolvedType,
    },

    /// A BETWEEN expression.
    Between {
        /// Expression to test.
        expr: Box<TypedExpr>,
        /// Lower bound.
        low: Box<TypedExpr>,
        /// Upper bound.
        high: Box<TypedExpr>,
        /// Whether the expression is negated (NOT BETWEEN).
        negated: bool,
    },

    /// A LIKE pattern match expression.
    Like {
        /// Expression to match.
        expr: Box<TypedExpr>,
        /// Pattern to match against.
        pattern: Box<TypedExpr>,
        /// Optional escape character.
        escape: Option<Box<TypedExpr>>,
        /// Whether the expression is negated (NOT LIKE).
        negated: bool,
    },

    /// An IN list expression.
    InList {
        /// Expression to test.
        expr: Box<TypedExpr>,
        /// List of values to check against.
        list: Vec<TypedExpr>,
        /// Whether the expression is negated (NOT IN).
        negated: bool,
    },

    /// An IS NULL expression.
    IsNull {
        /// Expression to test.
        expr: Box<TypedExpr>,
        /// Whether the expression is negated (IS NOT NULL).
        negated: bool,
    },

    /// A vector literal.
    VectorLiteral(Vec<f64>),
}

/// A sort expression for ORDER BY clauses.
///
/// Contains a typed expression and sort direction information.
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::typed_expr::{SortExpr, TypedExpr, TypedExprKind};
/// use alopex_sql::planner::types::ResolvedType;
/// use alopex_sql::Span;
///
/// let sort_expr = SortExpr {
///     expr: TypedExpr {
///         kind: TypedExprKind::ColumnRef {
///             table: "users".to_string(),
///             column: "name".to_string(),
///             column_index: 1,
///         },
///         resolved_type: ResolvedType::Text,
///         span: Span::default(),
///     },
///     asc: true,
///     nulls_first: false,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct SortExpr {
    /// The expression to sort by.
    pub expr: TypedExpr,
    /// Sort in ascending order (true) or descending (false).
    pub asc: bool,
    /// Place NULLs first (true) or last (false).
    pub nulls_first: bool,
}

/// A typed assignment for UPDATE SET clauses.
///
/// Contains the column name, index, and the typed value expression.
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::typed_expr::{TypedAssignment, TypedExpr, TypedExprKind};
/// use alopex_sql::planner::types::ResolvedType;
/// use alopex_sql::ast::expr::Literal;
/// use alopex_sql::Span;
///
/// let assignment = TypedAssignment {
///     column: "name".to_string(),
///     column_index: 1,
///     value: TypedExpr {
///         kind: TypedExprKind::Literal(Literal::String("Bob".to_string())),
///         resolved_type: ResolvedType::Text,
///         span: Span::default(),
///     },
/// };
/// ```
#[derive(Debug, Clone)]
pub struct TypedAssignment {
    /// The column name being assigned.
    pub column: String,
    /// The column index in the table's column list (0-based).
    pub column_index: usize,
    /// The value expression (type-checked against the column type).
    pub value: TypedExpr,
}

/// A projected column for SELECT clauses.
///
/// Contains a typed expression and an optional alias.
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::typed_expr::{ProjectedColumn, TypedExpr, TypedExprKind};
/// use alopex_sql::planner::types::ResolvedType;
/// use alopex_sql::Span;
///
/// // SELECT name AS user_name
/// let projected = ProjectedColumn {
///     expr: TypedExpr {
///         kind: TypedExprKind::ColumnRef {
///             table: "users".to_string(),
///             column: "name".to_string(),
///             column_index: 1,
///         },
///         resolved_type: ResolvedType::Text,
///         span: Span::default(),
///     },
///     alias: Some("user_name".to_string()),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ProjectedColumn {
    /// The projected expression.
    pub expr: TypedExpr,
    /// Optional alias (AS name).
    pub alias: Option<String>,
}

/// Projection specification for SELECT clauses.
///
/// Represents either all columns (after wildcard expansion) or specific columns.
#[derive(Debug, Clone)]
pub enum Projection {
    /// All columns (expanded from `*`).
    /// Contains the list of column names in definition order.
    All(Vec<String>),

    /// Specific columns/expressions.
    Columns(Vec<ProjectedColumn>),
}

impl TypedExpr {
    /// Creates a new typed expression.
    pub fn new(kind: TypedExprKind, resolved_type: ResolvedType, span: Span) -> Self {
        Self {
            kind,
            resolved_type,
            span,
        }
    }

    /// Creates a typed literal expression.
    pub fn literal(lit: Literal, resolved_type: ResolvedType, span: Span) -> Self {
        Self::new(TypedExprKind::Literal(lit), resolved_type, span)
    }

    /// Creates a typed column reference.
    pub fn column_ref(
        table: String,
        column: String,
        column_index: usize,
        resolved_type: ResolvedType,
        span: Span,
    ) -> Self {
        Self::new(
            TypedExprKind::ColumnRef {
                table,
                column,
                column_index,
            },
            resolved_type,
            span,
        )
    }

    /// Creates a typed binary operation.
    pub fn binary_op(
        left: TypedExpr,
        op: BinaryOp,
        right: TypedExpr,
        resolved_type: ResolvedType,
        span: Span,
    ) -> Self {
        Self::new(
            TypedExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            resolved_type,
            span,
        )
    }

    /// Creates a typed unary operation.
    pub fn unary_op(op: UnaryOp, operand: TypedExpr, resolved_type: ResolvedType, span: Span) -> Self {
        Self::new(
            TypedExprKind::UnaryOp {
                op,
                operand: Box::new(operand),
            },
            resolved_type,
            span,
        )
    }

    /// Creates a typed function call.
    pub fn function_call(
        name: String,
        args: Vec<TypedExpr>,
        resolved_type: ResolvedType,
        span: Span,
    ) -> Self {
        Self::new(
            TypedExprKind::FunctionCall { name, args },
            resolved_type,
            span,
        )
    }

    /// Creates a typed cast expression.
    pub fn cast(expr: TypedExpr, target_type: ResolvedType, span: Span) -> Self {
        Self::new(
            TypedExprKind::Cast {
                expr: Box::new(expr),
                target_type: target_type.clone(),
            },
            target_type,
            span,
        )
    }

    /// Creates a typed vector literal.
    pub fn vector_literal(values: Vec<f64>, dimension: u32, span: Span) -> Self {
        use crate::ast::ddl::VectorMetric;
        Self::new(
            TypedExprKind::VectorLiteral(values),
            ResolvedType::Vector {
                dimension,
                metric: VectorMetric::Cosine,
            },
            span,
        )
    }
}

impl SortExpr {
    /// Creates a new sort expression with ascending order.
    pub fn asc(expr: TypedExpr) -> Self {
        Self {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    /// Creates a new sort expression with descending order.
    ///
    /// Note: `nulls_first` defaults to `false` (NULLS LAST) for consistency.
    /// Use [`SortExpr::new`] for explicit NULLS ordering.
    pub fn desc(expr: TypedExpr) -> Self {
        Self {
            expr,
            asc: false,
            nulls_first: false,
        }
    }

    /// Creates a new sort expression with custom settings.
    pub fn new(expr: TypedExpr, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }
}

impl TypedAssignment {
    /// Creates a new typed assignment.
    pub fn new(column: String, column_index: usize, value: TypedExpr) -> Self {
        Self {
            column,
            column_index,
            value,
        }
    }
}

impl ProjectedColumn {
    /// Creates a new projected column without an alias.
    pub fn new(expr: TypedExpr) -> Self {
        Self { expr, alias: None }
    }

    /// Creates a new projected column with an alias.
    pub fn with_alias(expr: TypedExpr, alias: String) -> Self {
        Self {
            expr,
            alias: Some(alias),
        }
    }

    /// Returns the output name (alias if present, otherwise derived from expression).
    ///
    /// Returns:
    /// - The alias if one was specified (e.g., `SELECT name AS user_name`)
    /// - The column name for simple column references (e.g., `SELECT name`)
    /// - `None` for complex expressions without an alias (e.g., `SELECT 1 + 2`)
    ///
    /// Complex expressions (function calls, literals, binary operations) return `None`
    /// because they don't have a natural name. Use [`with_alias`](Self::with_alias)
    /// to give them an output name.
    pub fn output_name(&self) -> Option<&str> {
        if let Some(ref alias) = self.alias {
            return Some(alias);
        }
        // For column references, return the column name
        if let TypedExprKind::ColumnRef { ref column, .. } = self.expr.kind {
            return Some(column);
        }
        None
    }
}

impl Projection {
    /// Returns the number of columns in the projection.
    pub fn len(&self) -> usize {
        match self {
            Projection::All(cols) => cols.len(),
            Projection::Columns(cols) => cols.len(),
        }
    }

    /// Returns true if the projection has no columns.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the column names in the projection.
    ///
    /// For [`Projection::All`], all names are present (from the wildcard expansion).
    /// For [`Projection::Columns`], names may be `None` for complex expressions
    /// without aliases. See [`ProjectedColumn::output_name`] for details.
    pub fn column_names(&self) -> Vec<Option<&str>> {
        match self {
            Projection::All(cols) => cols.iter().map(|s| Some(s.as_str())).collect(),
            Projection::Columns(cols) => cols.iter().map(|c| c.output_name()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ddl::VectorMetric;

    #[test]
    fn test_typed_expr_literal() {
        let expr = TypedExpr::literal(
            Literal::Number("42".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );

        assert!(matches!(expr.kind, TypedExprKind::Literal(Literal::Number(_))));
        assert_eq!(expr.resolved_type, ResolvedType::Integer);
    }

    #[test]
    fn test_typed_expr_column_ref() {
        let expr = TypedExpr::column_ref(
            "users".to_string(),
            "id".to_string(),
            0,
            ResolvedType::Integer,
            Span::default(),
        );

        if let TypedExprKind::ColumnRef {
            table,
            column,
            column_index,
        } = &expr.kind
        {
            assert_eq!(table, "users");
            assert_eq!(column, "id");
            assert_eq!(*column_index, 0);
        } else {
            panic!("Expected ColumnRef");
        }
    }

    #[test]
    fn test_typed_expr_binary_op() {
        let left = TypedExpr::literal(
            Literal::Number("1".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );
        let right = TypedExpr::literal(
            Literal::Number("2".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );

        let expr = TypedExpr::binary_op(
            left,
            BinaryOp::Add,
            right,
            ResolvedType::Integer,
            Span::default(),
        );

        assert!(matches!(expr.kind, TypedExprKind::BinaryOp { .. }));
        assert_eq!(expr.resolved_type, ResolvedType::Integer);
    }

    #[test]
    fn test_typed_expr_vector_literal() {
        let values = vec![1.0, 2.0, 3.0];
        let expr = TypedExpr::vector_literal(values.clone(), 3, Span::default());

        if let TypedExprKind::VectorLiteral(v) = &expr.kind {
            assert_eq!(v, &values);
        } else {
            panic!("Expected VectorLiteral");
        }

        if let ResolvedType::Vector { dimension, metric } = &expr.resolved_type {
            assert_eq!(*dimension, 3);
            assert_eq!(*metric, VectorMetric::Cosine);
        } else {
            panic!("Expected Vector type");
        }
    }

    #[test]
    fn test_sort_expr_asc() {
        let col = TypedExpr::column_ref(
            "users".to_string(),
            "name".to_string(),
            1,
            ResolvedType::Text,
            Span::default(),
        );
        let sort = SortExpr::asc(col);

        assert!(sort.asc);
        assert!(!sort.nulls_first);
    }

    #[test]
    fn test_sort_expr_desc() {
        let col = TypedExpr::column_ref(
            "users".to_string(),
            "name".to_string(),
            1,
            ResolvedType::Text,
            Span::default(),
        );
        let sort = SortExpr::desc(col);

        assert!(!sort.asc);
        // NULLS LAST is the consistent default for both ASC and DESC
        assert!(!sort.nulls_first);
    }

    #[test]
    fn test_typed_assignment() {
        let value = TypedExpr::literal(
            Literal::String("Alice".to_string()),
            ResolvedType::Text,
            Span::default(),
        );
        let assignment = TypedAssignment::new("name".to_string(), 1, value);

        assert_eq!(assignment.column, "name");
        assert_eq!(assignment.column_index, 1);
    }

    #[test]
    fn test_projected_column_output_name() {
        let col = TypedExpr::column_ref(
            "users".to_string(),
            "name".to_string(),
            1,
            ResolvedType::Text,
            Span::default(),
        );

        // Without alias, output name is the column name
        let proj1 = ProjectedColumn::new(col.clone());
        assert_eq!(proj1.output_name(), Some("name"));

        // With alias, output name is the alias
        let proj2 = ProjectedColumn::with_alias(col, "user_name".to_string());
        assert_eq!(proj2.output_name(), Some("user_name"));
    }

    #[test]
    fn test_projection_all() {
        let columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        let proj = Projection::All(columns);

        assert_eq!(proj.len(), 3);
        assert!(!proj.is_empty());

        let names: Vec<_> = proj.column_names();
        assert_eq!(names, vec![Some("id"), Some("name"), Some("email")]);
    }

    #[test]
    fn test_projection_columns() {
        let col1 = ProjectedColumn::new(TypedExpr::column_ref(
            "users".to_string(),
            "id".to_string(),
            0,
            ResolvedType::Integer,
            Span::default(),
        ));
        let col2 = ProjectedColumn::with_alias(
            TypedExpr::column_ref(
                "users".to_string(),
                "name".to_string(),
                1,
                ResolvedType::Text,
                Span::default(),
            ),
            "user_name".to_string(),
        );

        let proj = Projection::Columns(vec![col1, col2]);

        assert_eq!(proj.len(), 2);
        let names: Vec<_> = proj.column_names();
        assert_eq!(names, vec![Some("id"), Some("user_name")]);
    }

    #[test]
    fn test_typed_expr_cast() {
        let inner = TypedExpr::literal(
            Literal::Number("42".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );
        let expr = TypedExpr::cast(inner, ResolvedType::Double, Span::default());

        assert!(matches!(expr.kind, TypedExprKind::Cast { .. }));
        assert_eq!(expr.resolved_type, ResolvedType::Double);
    }

    #[test]
    fn test_typed_expr_kind_between() {
        let expr_kind = TypedExprKind::Between {
            expr: Box::new(TypedExpr::column_ref(
                "t".to_string(),
                "x".to_string(),
                0,
                ResolvedType::Integer,
                Span::default(),
            )),
            low: Box::new(TypedExpr::literal(
                Literal::Number("1".to_string()),
                ResolvedType::Integer,
                Span::default(),
            )),
            high: Box::new(TypedExpr::literal(
                Literal::Number("10".to_string()),
                ResolvedType::Integer,
                Span::default(),
            )),
            negated: false,
        };

        assert!(matches!(expr_kind, TypedExprKind::Between { negated: false, .. }));
    }

    #[test]
    fn test_typed_expr_kind_is_null() {
        let expr_kind = TypedExprKind::IsNull {
            expr: Box::new(TypedExpr::column_ref(
                "t".to_string(),
                "x".to_string(),
                0,
                ResolvedType::Integer,
                Span::default(),
            )),
            negated: true,
        };

        assert!(matches!(expr_kind, TypedExprKind::IsNull { negated: true, .. }));
    }
}
