//! Type checking module for the Alopex SQL dialect.
//!
//! This module provides type inference and validation for SQL expressions.
//! It checks that expressions are well-typed and that operations are valid
//! for the types involved.

use crate::ast::Span;
use crate::ast::Statement;
use crate::ast::ddl::VectorMetric;
use crate::ast::expr::{
    BinaryOp, Expr, ExprKind, Literal, PatternMatchKind, Quantifier as AstQuantifier, TruthValue,
    UnaryOp, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowSpec,
};
use crate::ast::expr::{
    INTERNAL_ROW_BETWEEN, INTERNAL_ROW_DISTINCT, INTERNAL_ROW_EQ, INTERNAL_ROW_GT,
    INTERNAL_ROW_GTEQ, INTERNAL_ROW_IN, INTERNAL_ROW_LT, INTERNAL_ROW_LTEQ, INTERNAL_ROW_NEQ,
    INTERNAL_TRUTH_FALSE, INTERNAL_TRUTH_TRUE, INTERNAL_TRUTH_UNKNOWN,
};
use crate::catalog::{Catalog, ColumnMetadata, TableMetadata};
use crate::planner::aggregate_expr::{AggregateExpr, AggregateFunction};
use crate::planner::error::PlannerError;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{
    Quantifier, SortExpr, TypedCaseWhen, TypedExpr, TypedExprKind, TypedWindowSpec,
};
use crate::planner::types::ResolvedType;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// A table visible to expression name resolution.
///
/// The metadata is shared rather than owned: every enclosing scope is copied
/// into each nested scope, and copying whole schemas there made resolution cost
/// grow with the square of the nesting depth.
#[derive(Debug, Clone)]
pub struct ScopedTable {
    pub table: Arc<TableMetadata>,
    pub start_index: usize,
    /// Lexical nesting level; zero is the current SELECT and larger values
    /// are successively enclosing SELECT scopes.
    pub scope_level: usize,
    /// Columns coalesced by a JOIN ... USING or NATURAL JOIN. They remain
    /// addressable by a qualified right-hand reference, but are not candidates
    /// for an unqualified reference because the merged output column owns
    /// the name.
    pub hidden_unqualified_columns: HashSet<String>,
    /// For a column merged by USING or NATURAL, the output indexes of every
    /// other side. An unqualified reference to a merged name resolves to
    /// `COALESCE(left, right, ...)` so that RIGHT and FULL joins report the key
    /// from whichever joined input is present.
    pub merged_column_partners: HashMap<String, Vec<usize>>,
    /// Column name to position in `table.columns`, built once when the table
    /// enters scope. Resolution looks a name up once per reference, so scanning
    /// the column list made a wide projection cost the square of its width.
    /// Shared alongside the metadata it indexes so the two cannot drift apart.
    ///
    /// `None` for narrow tables, where building the map costs more than the
    /// scans it saves; see [`COLUMN_INDEX_THRESHOLD`].
    column_index: Option<Arc<HashMap<String, usize>>>,
}

/// Column count above which a scoped table gets a hash index.
///
/// Below this a linear scan of the column list wins: the map allocation is paid
/// once per table per scope, and measurement showed narrow tables getting 10-15%
/// slower when every table was indexed unconditionally.
const COLUMN_INDEX_THRESHOLD: usize = 32;

impl ScopedTable {
    pub fn new(table: impl Into<Arc<TableMetadata>>, start_index: usize) -> Self {
        let table = table.into();
        let column_index = (table.columns.len() > COLUMN_INDEX_THRESHOLD).then(|| {
            // On a duplicate name the first position wins, matching the linear
            // scan this replaces.
            let mut index = HashMap::with_capacity(table.columns.len());
            for (position, column) in table.columns.iter().enumerate() {
                index.entry(column.name.clone()).or_insert(position);
            }
            Arc::new(index)
        });
        Self {
            table,
            start_index,
            scope_level: 0,
            hidden_unqualified_columns: HashSet::new(),
            merged_column_partners: HashMap::new(),
            column_index,
        }
    }

    /// Position of `column` in this table, or `None` if it has no such column.
    pub fn column_position(&self, column: &str) -> Option<usize> {
        match &self.column_index {
            Some(index) => index.get(column).copied(),
            None => self.table.get_column_index(column),
        }
    }

    pub fn hide_unqualified_columns(&mut self, columns: &[String]) {
        self.hidden_unqualified_columns
            .extend(columns.iter().cloned());
    }

    /// Record that `column` is merged with the output column at `partner_index`.
    pub fn merge_column_with(&mut self, column: &str, partner_index: usize) {
        let partners = self
            .merged_column_partners
            .entry(column.to_string())
            .or_default();
        if !partners.contains(&partner_index) {
            partners.push(partner_index);
        }
    }
}

pub type SubqueryPlanner<'p> = dyn Fn(&Statement, &[ScopedTable]) -> Result<(LogicalPlan, Vec<ColumnMetadata>), PlannerError>
    + 'p;

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
pub struct TypeChecker<'a, C: Catalog + ?Sized> {
    catalog: &'a C,
}

impl<'a, C: Catalog + ?Sized> TypeChecker<'a, C> {
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
        let scope = [ScopedTable::new(table.clone(), 0)];
        self.infer_type_with_scope(expr, &scope, &|stmt, _outer| {
            let planner = crate::planner::Planner::new(self.catalog);
            let plan = planner.plan(stmt)?;
            Ok((plan, Vec::new()))
        })
    }

    pub fn infer_type_with_scope(
        &self,
        expr: &Expr,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
    ) -> Result<TypedExpr, PlannerError> {
        let span = expr.span;
        match &expr.kind {
            ExprKind::Literal { literal: lit } => self.infer_literal_type(lit, span),

            ExprKind::ColumnRef {
                table: table_qualifier,
                column,
            } => self.infer_column_ref_type_with_scope(
                scope,
                table_qualifier.as_deref(),
                column,
                span,
            ),

            ExprKind::BinaryOp { left, op, right } => {
                self.infer_binary_op_type_with_scope(left, *op, right, scope, plan_subquery, span)
            }

            ExprKind::UnaryOp { op, operand } => {
                self.infer_unary_op_type_with_scope(*op, operand, scope, plan_subquery, span)
            }

            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => self.infer_case_type_with_scope(
                operand.as_deref(),
                branches,
                else_expr.as_deref(),
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::FunctionCall {
                name,
                args,
                distinct,
                star,
                order_by,
                within_group,
                filter,
                over,
            } => self.infer_function_call_type_with_scope(
                name,
                args,
                *distinct,
                *star,
                order_by,
                within_group,
                filter.as_deref(),
                over.as_ref(),
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::Cast { expr, target_type } => {
                let typed_expr = self.infer_type_with_scope(expr, scope, plan_subquery)?;
                Ok(TypedExpr::cast(
                    typed_expr,
                    ResolvedType::from_ast(target_type),
                    span,
                ))
            }

            ExprKind::TryCast { expr, target_type } => {
                let typed_expr = self.infer_type_with_scope(expr, scope, plan_subquery)?;
                Ok(TypedExpr::try_cast(
                    typed_expr,
                    ResolvedType::from_ast(target_type),
                    span,
                ))
            }

            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => self.infer_between_type_with_scope(
                expr,
                low,
                high,
                *negated,
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::Like {
                expr,
                pattern,
                escape,
                negated,
                kind,
            } => self.infer_like_type_with_scope(
                expr,
                pattern,
                escape.as_deref(),
                *negated,
                *kind,
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::InList {
                expr,
                list,
                negated,
            } => {
                self.infer_in_list_type_with_scope(expr, list, *negated, scope, plan_subquery, span)
            }

            ExprKind::IsNull { expr, negated } => {
                self.infer_is_null_type_with_scope(expr, *negated, scope, plan_subquery, span)
            }

            ExprKind::Row { .. } => Err(PlannerError::unsupported_feature(
                "standalone row constructor",
                "v0.8.8 predicate context",
                span,
            )),

            ExprKind::TruthPredicate {
                expr,
                value,
                negated,
            } => self.infer_truth_predicate_with_scope(
                expr,
                *value,
                *negated,
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::IsDistinctFrom {
                left,
                right,
                negated,
            } => self.infer_distinct_predicate_with_scope(
                left,
                right,
                *negated,
                scope,
                plan_subquery,
                span,
            ),

            ExprKind::VectorLiteral { values } => self.infer_vector_literal_type(values, span),

            ExprKind::ScalarSubquery { subquery } => {
                let (plan, schema) = plan_subquery(subquery, scope)?;
                let value_type = single_column_type(&schema, span)?;
                Ok(TypedExpr {
                    kind: TypedExprKind::ScalarSubquery(Box::new(plan)),
                    resolved_type: value_type,
                    span,
                })
            }
            ExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;
                let (plan, schema) = plan_subquery(subquery, scope)?;
                let value_type = single_column_type(&schema, span)?;
                self.check_comparison_op(&expr_typed.resolved_type, &value_type, span)?;
                Ok(TypedExpr {
                    kind: TypedExprKind::InSubquery {
                        expr: Box::new(expr_typed),
                        subquery: Box::new(plan),
                        negated: *negated,
                    },
                    resolved_type: ResolvedType::Boolean,
                    span,
                })
            }
            ExprKind::Exists { subquery, negated } => {
                let (plan, _schema) = plan_subquery(subquery, scope)?;
                Ok(TypedExpr {
                    kind: TypedExprKind::Exists {
                        subquery: Box::new(plan),
                        negated: *negated,
                    },
                    resolved_type: ResolvedType::Boolean,
                    span,
                })
            }
            ExprKind::Quantified {
                expr,
                op,
                quantifier,
                subquery,
            } => {
                let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;
                let (plan, schema) = plan_subquery(subquery, scope)?;
                let value_type = single_column_type(&schema, span)?;
                self.check_binary_op(*op, &expr_typed.resolved_type, &value_type, span)?;
                Ok(TypedExpr {
                    kind: TypedExprKind::Quantified {
                        expr: Box::new(expr_typed),
                        op: *op,
                        quantifier: match quantifier {
                            AstQuantifier::Any => Quantifier::Any,
                            AstQuantifier::All => Quantifier::All,
                        },
                        subquery: Box::new(plan),
                    },
                    resolved_type: ResolvedType::Boolean,
                    span,
                })
            }
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
            Literal::Interval(_) => (TypedExprKind::Literal(lit.clone()), ResolvedType::Interval),
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
    #[allow(dead_code)]
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

    fn infer_column_ref_type_with_scope(
        &self,
        scope: &[ScopedTable],
        table_qualifier: Option<&str>,
        column_name: &str,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let levels = scope
            .iter()
            .map(|table| table.scope_level)
            .collect::<BTreeSet<_>>();
        let mut qualifier_found = false;

        for level in levels {
            let candidates = scope
                .iter()
                .filter(|table| table.scope_level == level)
                .filter(|table| {
                    table_qualifier.is_some()
                        || !table.hidden_unqualified_columns.contains(column_name)
                })
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                continue;
            }
            if let Some(qualifier) = table_qualifier {
                let qualified = candidates
                    .iter()
                    .filter(|table| table.table.name == qualifier)
                    .collect::<Vec<_>>();
                match qualified.len() {
                    0 => continue,
                    1 => qualifier_found = true,
                    _ => {
                        return Err(PlannerError::ambiguous_column(
                            column_name,
                            qualified
                                .iter()
                                .map(|table| table.table.name.clone())
                                .collect(),
                            span,
                        ));
                    }
                }
            }

            // Resolution happens through each table's own column index rather
            // than by scanning its column list, because this runs once per
            // column reference and the scan made a wide projection quadratic.
            let mut matches = candidates.iter().filter(|table| {
                table_qualifier.is_none_or(|qualifier| table.table.name == qualifier)
                    && table.column_position(column_name).is_some()
            });
            let found = matches.next();
            let second = matches.next();

            match (found, second) {
                (Some(_), Some(_)) => {
                    return Err(PlannerError::ambiguous_column(
                        column_name,
                        candidates
                            .iter()
                            .filter(|table| table.column_position(column_name).is_some())
                            .map(|table| table.table.name.clone())
                            .collect(),
                        span,
                    ));
                }
                (None, _) => {
                    if table_qualifier.is_some() {
                        // A qualified name that the named table does not have is
                        // an error here; it cannot be a correlated reference.
                        return Err(PlannerError::column_not_found(
                            column_name,
                            candidates
                                .first()
                                .map(|table| table.table.name.as_str())
                                .unwrap_or("unknown"),
                            span,
                        ));
                    }
                    // A missing local name may be a correlated reference. Only
                    // this case falls back to the next enclosing scope.
                    continue;
                }
                (Some(scoped), None) => {
                    let column_index = scoped
                        .column_position(column_name)
                        .expect("filtered on the column being present");
                    let column = &scoped.table.columns[column_index];
                    let own_ref = TypedExpr {
                        kind: TypedExprKind::ColumnRef {
                            table: scoped.table.name.clone(),
                            column: column_name.to_string(),
                            column_index: scoped.start_index + column_index,
                        },
                        resolved_type: column.data_type.clone(),
                        span,
                    };

                    // A USING/NATURAL common column is one output column formed
                    // from both inputs. An unqualified reference must see the
                    // merged value, otherwise a RIGHT or FULL join reports the
                    // left side's NULL for rows that only exist on the right.
                    if table_qualifier.is_none()
                        && let Some(partner_indices) =
                            scoped.merged_column_partners.get(column_name)
                    {
                        let mut args = Vec::with_capacity(partner_indices.len() + 1);
                        args.push(own_ref);
                        args.extend(partner_indices.iter().map(|&partner_index| TypedExpr {
                            kind: TypedExprKind::ColumnRef {
                                table: scoped.table.name.clone(),
                                column: column_name.to_string(),
                                column_index: partner_index,
                            },
                            resolved_type: column.data_type.clone(),
                            span,
                        }));
                        return Ok(TypedExpr {
                            kind: TypedExprKind::FunctionCall {
                                name: "coalesce".to_string(),
                                args,
                                distinct: false,
                                star: false,
                                filter: None,
                                order_by: Vec::new(),
                                over: None,
                            },
                            resolved_type: column.data_type.clone(),
                            span,
                        });
                    }

                    return Ok(own_ref);
                }
            }
        }

        let table = scope
            .iter()
            .min_by_key(|table| table.scope_level)
            .map(|table| table.table.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        if let Some(qualifier) = table_qualifier
            && !qualifier_found
        {
            return Err(PlannerError::table_not_found(qualifier, span));
        }
        Err(PlannerError::column_not_found(column_name, table, span))
    }

    /// Infer the type of a binary operation.
    #[allow(dead_code)]
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

        if let Some(folded) =
            fold_integral_binary(&left_typed, op, &right_typed, &result_type, span)
        {
            return Ok(folded);
        }

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

    fn infer_binary_op_type_with_scope(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        if row_items(left).is_some() || row_items(right).is_some() {
            let internal = match op {
                BinaryOp::Eq => INTERNAL_ROW_EQ,
                BinaryOp::Neq => INTERNAL_ROW_NEQ,
                BinaryOp::Lt => INTERNAL_ROW_LT,
                BinaryOp::LtEq => INTERNAL_ROW_LTEQ,
                BinaryOp::Gt => INTERNAL_ROW_GT,
                BinaryOp::GtEq => INTERNAL_ROW_GTEQ,
                _ => {
                    return Err(PlannerError::invalid_operator(
                        format!("{op:?}"),
                        "Row",
                        span,
                    ));
                }
            };
            let (mut left, right, width) =
                self.infer_row_pair_with_scope(left, right, scope, plan_subquery, span)?;
            left.extend(right);
            return Ok(internal_predicate(
                format!("{internal}:{width}"),
                left,
                span,
            ));
        }

        let left_typed = self.infer_type_with_scope(left, scope, plan_subquery)?;
        let right_typed = self.infer_type_with_scope(right, scope, plan_subquery)?;

        let result_type = self.check_binary_op(
            op,
            &left_typed.resolved_type,
            &right_typed.resolved_type,
            span,
        )?;

        if let Some(folded) =
            fold_integral_binary(&left_typed, op, &right_typed, &result_type, span)
        {
            return Ok(folded);
        }

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

    fn infer_case_type_with_scope(
        &self,
        operand: Option<&Expr>,
        branches: &[crate::ast::expr::CaseWhen],
        else_expr: Option<&Expr>,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        if branches.is_empty() {
            return Err(PlannerError::invalid_expression(
                "CASE expression requires at least one WHEN branch",
            ));
        }
        let typed_operand = operand
            .map(|expr| self.infer_type_with_scope(expr, scope, plan_subquery))
            .transpose()?;
        let mut typed_branches = Vec::with_capacity(branches.len());
        let mut result_type = ResolvedType::Null;

        for branch in branches {
            let condition = self.infer_type_with_scope(&branch.when, scope, plan_subquery)?;
            if let Some(operand) = &typed_operand {
                self.check_comparison_op(
                    &operand.resolved_type,
                    &condition.resolved_type,
                    condition.span,
                )?;
            } else if !matches!(
                condition.resolved_type,
                ResolvedType::Boolean | ResolvedType::Null
            ) {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    condition.resolved_type.type_name(),
                    condition.span,
                ));
            }

            let result = self.infer_type_with_scope(&branch.then, scope, plan_subquery)?;
            result_type =
                self.common_case_result_type(&result_type, &result.resolved_type, result.span)?;
            typed_branches.push(TypedCaseWhen {
                when: condition,
                then: result,
            });
        }

        let mut typed_else = else_expr
            .map(|expr| self.infer_type_with_scope(expr, scope, plan_subquery))
            .transpose()?;
        if let Some(else_expr) = &typed_else {
            result_type = self.common_case_result_type(
                &result_type,
                &else_expr.resolved_type,
                else_expr.span,
            )?;
        }

        for branch in &mut typed_branches {
            coerce_case_result(&mut branch.then, &result_type);
        }
        if let Some(else_expr) = &mut typed_else {
            coerce_case_result(else_expr, &result_type);
        }

        Ok(TypedExpr {
            kind: TypedExprKind::Case {
                operand: typed_operand.map(Box::new),
                branches: typed_branches,
                else_expr: typed_else.map(Box::new),
            },
            resolved_type: result_type,
            span,
        })
    }

    fn common_case_result_type(
        &self,
        current: &ResolvedType,
        next: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if matches!(current, ResolvedType::Null) {
            return Ok(next.clone());
        }
        if matches!(next, ResolvedType::Null) || current == next {
            return Ok(current.clone());
        }
        if is_numeric_type(current) && is_numeric_type(next) {
            return self.check_arithmetic_op(current, next, span);
        }
        Err(PlannerError::type_mismatch(
            current.type_name(),
            next.type_name(),
            span,
        ))
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
            Add | Sub | Mul | Div => {
                if let Some(result) = Self::temporal_arithmetic_type(op, left, right) {
                    return Ok(result);
                }
                if let Some(result) = decimal_arithmetic_type(op, left, right) {
                    return Ok(result);
                }
                let result = self.check_arithmetic_op(left, right, span)?;
                Ok(result)
            }

            // Remainder is defined only for integral operands.
            Mod => self.check_modulo_op(left, right, span),

            BitAnd | BitOr | BitXor | ShiftLeft | ShiftRight => {
                self.check_integral_op(left, right, span)
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
            (Float, Float) => Ok(Float),
            // f32 has 24 bits of mantissa and cannot hold the whole i32 range,
            // so an INTEGER mixed with FLOAT widens to DOUBLE.
            (Integer, Float)
            | (Float, Integer)
            | (Integer, Double)
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

    fn temporal_arithmetic_type(
        op: BinaryOp,
        left: &ResolvedType,
        right: &ResolvedType,
    ) -> Option<ResolvedType> {
        use ResolvedType::*;
        match (op, left, right) {
            (BinaryOp::Add | BinaryOp::Sub, Date, Interval) => Some(Date),
            (BinaryOp::Add | BinaryOp::Sub, Timestamp, Interval) => Some(Timestamp),
            (BinaryOp::Add | BinaryOp::Sub, Time, Interval) => Some(Time),
            (BinaryOp::Add, Interval, Date) => Some(Date),
            (BinaryOp::Add, Interval, Timestamp) => Some(Timestamp),
            (BinaryOp::Add, Interval, Time) => Some(Time),
            (BinaryOp::Sub, Date, Date)
            | (BinaryOp::Sub, Timestamp, Timestamp)
            | (BinaryOp::Sub, Time, Time)
            | (BinaryOp::Add | BinaryOp::Sub, Interval, Interval) => Some(Interval),
            _ => None,
        }
    }

    /// Check remainder operands and return the integral result type.
    fn check_modulo_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        use ResolvedType::*;

        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        match (left, right) {
            (Integer, Integer) => Ok(Integer),
            (Integer, BigInt) | (BigInt, Integer) | (BigInt, BigInt) => Ok(BigInt),
            _ => Err(PlannerError::InvalidOperator {
                op: "modulo".to_string(),
                type_name: format!("{} and {}", left.type_name(), right.type_name()),
                line: span.start.line,
                column: span.start.column,
            }),
        }
    }

    fn check_integral_op(
        &self,
        left: &ResolvedType,
        right: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        use ResolvedType::*;
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }
        match (left, right) {
            (Integer, Integer) => Ok(Integer),
            (Integer | BigInt, Integer | BigInt) => Ok(BigInt),
            _ => Err(PlannerError::invalid_operator(
                "bitwise",
                format!("{} and {}", left.type_name(), right.type_name()),
                span,
            )),
        }
    }

    /// Check comparison operation for compatible types.
    pub(crate) fn check_comparison_op(
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
            (
                Integer | BigInt | Float | Double | Decimal { .. },
                Integer | BigInt | Float | Double | Decimal { .. },
            ) => true,

            // Text types
            (Text, Text) => true,

            // Boolean types
            (Boolean, Boolean) => true,

            // Timestamp types
            (Timestamp, Timestamp) => true,
            (Date, Date) | (Time, Time) | (Interval, Interval) => true,

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
    #[allow(dead_code)]
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
                    ResolvedType::Decimal { precision, scale } => ResolvedType::Decimal {
                        precision: *precision,
                        scale: *scale,
                    },
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
            UnaryOp::BitNot => match &operand_typed.resolved_type {
                ResolvedType::Integer => ResolvedType::Integer,
                ResolvedType::BigInt => ResolvedType::BigInt,
                ResolvedType::Null => ResolvedType::Null,
                other => {
                    return Err(PlannerError::InvalidOperator {
                        op: "bitwise not".to_string(),
                        type_name: other.type_name().to_string(),
                        line: span.start.line,
                        column: span.start.column,
                    });
                }
            },
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

    fn infer_unary_op_type_with_scope(
        &self,
        op: UnaryOp,
        operand: &Expr,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let operand_typed = self.infer_type_with_scope(operand, scope, plan_subquery)?;

        let result_type = match op {
            UnaryOp::Not => {
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
            UnaryOp::Minus => match &operand_typed.resolved_type {
                ResolvedType::Integer => ResolvedType::Integer,
                ResolvedType::BigInt => ResolvedType::BigInt,
                ResolvedType::Float => ResolvedType::Float,
                ResolvedType::Double => ResolvedType::Double,
                ResolvedType::Decimal { precision, scale } => ResolvedType::Decimal {
                    precision: *precision,
                    scale: *scale,
                },
                ResolvedType::Null => ResolvedType::Null,
                other => {
                    return Err(PlannerError::InvalidOperator {
                        op: "unary minus".to_string(),
                        type_name: other.type_name().to_string(),
                        line: span.start.line,
                        column: span.start.column,
                    });
                }
            },
            UnaryOp::BitNot => match &operand_typed.resolved_type {
                ResolvedType::Integer => ResolvedType::Integer,
                ResolvedType::BigInt => ResolvedType::BigInt,
                ResolvedType::Null => ResolvedType::Null,
                other => {
                    return Err(PlannerError::InvalidOperator {
                        op: "bitwise not".to_string(),
                        type_name: other.type_name().to_string(),
                        line: span.start.line,
                        column: span.start.column,
                    });
                }
            },
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
    #[allow(dead_code)]
    fn infer_function_call_type(
        &self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        star: bool,
        table: &TableMetadata,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        // Type-check all arguments first
        let typed_args: Vec<TypedExpr> = args
            .iter()
            .map(|arg| self.infer_type(arg, table))
            .collect::<Result<Vec<_>, _>>()?;

        // Delegate to check_function_call for validation and return type
        let result_type = self.check_function_call(name, &typed_args, distinct, star, span)?;

        Ok(TypedExpr {
            kind: TypedExprKind::FunctionCall {
                name: name.to_string(),
                args: typed_args,
                distinct,
                star,
                filter: None,
                order_by: Vec::new(),
                over: None,
            },
            resolved_type: result_type,
            span,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn infer_function_call_type_with_scope(
        &self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        star: bool,
        order_by: &[crate::ast::dml::OrderByExpr],
        within_group: &[crate::ast::dml::OrderByExpr],
        filter: Option<&Expr>,
        over: Option<&WindowSpec>,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let lower_name = name.to_ascii_lowercase();
        self.validate_aggregate_clause_placement(
            &lower_name,
            distinct,
            order_by,
            within_group,
            filter,
            over.is_some(),
            span,
        )?;
        if over.is_some() {
            match lower_name.as_str() {
                "lag" | "lead" => {
                    validate_offset_window_call(name, args.len(), distinct, star)?;
                }
                "first_value" | "last_value" | "ntile" => {
                    validate_exact_window_call(name, args.len(), 1, distinct, star)?;
                }
                "nth_value" => {
                    validate_exact_window_call(name, args.len(), 2, distinct, star)?;
                }
                "percent_rank" | "cume_dist" => {
                    validate_exact_window_call(name, args.len(), 0, distinct, star)?;
                }
                _ => {}
            }
        }

        let mut typed_args: Vec<TypedExpr> = args
            .iter()
            .map(|arg| self.infer_type_with_scope(arg, scope, plan_subquery))
            .collect::<Result<Vec<_>, _>>()?;

        // WITHIN GROUP normalizes onto the same aggregate-local ordering as an
        // in-argument ORDER BY; the parser rejects supplying both at once.
        let order_by_source = if within_group.is_empty() {
            order_by
        } else {
            within_group
        };
        let typed_order_by = order_by_source
            .iter()
            .map(|order| {
                let expr = self.infer_type_with_scope(&order.expr, scope, plan_subquery)?;
                if super::typed_expr_contains_aggregate(&expr) {
                    return Err(PlannerError::invalid_expression(
                        "aggregate functions are not allowed in aggregate ORDER BY".to_string(),
                    ));
                }
                if super::typed_expr_contains_window(&expr) {
                    return Err(PlannerError::invalid_expression(
                        "window functions are not allowed in aggregate ORDER BY".to_string(),
                    ));
                }
                Ok(SortExpr::new(
                    expr,
                    order.asc.unwrap_or(true),
                    order.nulls_first.unwrap_or(false),
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;

        let typed_filter = filter
            .map(|predicate| {
                let typed = self.infer_type_with_scope(predicate, scope, plan_subquery)?;
                if super::typed_expr_contains_aggregate(&typed) {
                    return Err(PlannerError::invalid_expression(
                        "aggregate functions are not allowed in FILTER".to_string(),
                    ));
                }
                if super::typed_expr_contains_window(&typed) {
                    return Err(PlannerError::invalid_expression(
                        "window functions are not allowed in FILTER".to_string(),
                    ));
                }
                if !matches!(
                    typed.resolved_type,
                    ResolvedType::Boolean | ResolvedType::Null
                ) {
                    return Err(PlannerError::type_mismatch(
                        "BOOLEAN FILTER predicate",
                        typed.resolved_type.type_name(),
                        typed.span,
                    ));
                }
                Ok(Box::new(typed))
            })
            .transpose()?;

        // D4 (PostgreSQL rule): with DISTINCT, every aggregate ORDER BY
        // expression must appear in the argument list, otherwise the sort key
        // is undefined after deduplication.
        if distinct && !typed_order_by.is_empty() {
            for sort in &typed_order_by {
                let key = super::distinct_on_expr_signature(&sort.expr);
                let appears = typed_args
                    .iter()
                    .any(|arg| super::distinct_on_expr_signature(arg) == key);
                if !appears {
                    return Err(PlannerError::invalid_expression(
                        "in an aggregate with DISTINCT, ORDER BY expressions must appear in \
                         the argument list"
                            .to_string(),
                    ));
                }
            }
        }

        let result_type = if over.is_some() {
            match lower_name.as_str() {
                "lag" | "lead" => self.infer_offset_window_result_type(name, &mut typed_args)?,
                "first_value" | "last_value" => typed_args[0].resolved_type.clone(),
                "nth_value" => {
                    validate_positive_integer_argument(name, &typed_args[1])?;
                    typed_args[0].resolved_type.clone()
                }
                "ntile" => {
                    validate_positive_integer_argument(name, &typed_args[0])?;
                    ResolvedType::BigInt
                }
                "percent_rank" | "cume_dist" => ResolvedType::Double,
                "row_number" | "rank" | "dense_rank" => {
                    if !typed_args.is_empty() || distinct || star {
                        return Err(PlannerError::invalid_expression(format!(
                            "{}() window function takes no arguments",
                            name.to_ascii_uppercase()
                        )));
                    }
                    ResolvedType::BigInt
                }
                name if is_aggregate_name(name) => {
                    self.check_function_call(name, &typed_args, distinct, star, span)?
                }
                _ => {
                    return Err(PlannerError::unsupported_feature(
                        format!("function '{}' with OVER", name),
                        "future",
                        span,
                    ));
                }
            }
        } else if matches!(lower_name.as_str(), "percentile_disc" | "percentile_cont") {
            self.check_percentile(&lower_name, &typed_args, &typed_order_by, span)?
        } else if lower_name == "mode" && !within_group.is_empty() {
            if !typed_args.is_empty() || typed_order_by.len() != 1 {
                return Err(PlannerError::invalid_expression(
                    "MODE requires no arguments and exactly one WITHIN GROUP sort expression"
                        .to_string(),
                ));
            }
            typed_order_by[0].expr.resolved_type.clone()
        } else {
            self.check_function_call(name, &typed_args, distinct, star, span)?
        };

        let typed_over = over
            .map(|window| {
                if let Some(base) = &window.base {
                    return Err(PlannerError::invalid_expression(format!(
                        "named window '{base}' was not resolved in its query block"
                    )));
                }
                let partition_by = window
                    .partition_by
                    .iter()
                    .map(|expr| self.infer_type_with_scope(expr, scope, plan_subquery))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = window
                    .order_by
                    .iter()
                    .map(|order| {
                        let expr = self.infer_type_with_scope(&order.expr, scope, plan_subquery)?;
                        Ok(SortExpr::new(
                            expr,
                            order.asc.unwrap_or(true),
                            order.nulls_first.unwrap_or(false),
                        ))
                    })
                    .collect::<Result<Vec<_>, PlannerError>>()?;
                if let Some(frame) = &window.frame {
                    validate_window_frame(&lower_name, frame, &order_by)?;
                }
                Ok(TypedWindowSpec {
                    partition_by,
                    order_by,
                    frame: window.frame.clone(),
                })
            })
            .transpose()?;

        Ok(TypedExpr {
            kind: TypedExprKind::FunctionCall {
                name: name.to_string(),
                args: typed_args,
                distinct,
                star,
                filter: typed_filter,
                order_by: typed_order_by,
                over: typed_over,
            },
            resolved_type: result_type,
            span,
        })
    }

    /// Placement rules for FILTER / WITHIN GROUP / aggregate ORDER BY that do
    /// not require typed arguments (issue #148, D2/D6/D7).
    #[allow(clippy::too_many_arguments)]
    fn validate_aggregate_clause_placement(
        &self,
        lower_name: &str,
        distinct: bool,
        order_by: &[crate::ast::dml::OrderByExpr],
        within_group: &[crate::ast::dml::OrderByExpr],
        filter: Option<&Expr>,
        has_over: bool,
        span: Span,
    ) -> Result<(), PlannerError> {
        let is_ordered_set = is_ordered_set_aggregate_name(lower_name);
        let is_aggregate = is_aggregate_name(lower_name);

        if let Some(filter) = filter {
            if has_over {
                // PostgreSQL allows FILTER on window-aggregates; the Alopex
                // window executor frame path does not implement it yet, so the
                // boundary is a stable explicit error (D2).
                return Err(PlannerError::unsupported_feature(
                    "FILTER on a window function call",
                    "future",
                    span,
                ));
            }
            if !is_aggregate {
                return Err(PlannerError::invalid_expression(format!(
                    "FILTER (WHERE ...) is only valid for aggregate functions, not '{lower_name}'"
                )));
            }
            if super::expr_contains_subquery(filter) {
                return Err(PlannerError::unsupported_feature(
                    "subquery in aggregate FILTER",
                    "future",
                    filter.span,
                ));
            }
        }

        if !within_group.is_empty() {
            if has_over {
                // PostgreSQL: ordered-set aggregates cannot be window calls.
                return Err(PlannerError::invalid_expression(
                    "WITHIN GROUP cannot be combined with OVER".to_string(),
                ));
            }
            if !is_ordered_set {
                return Err(PlannerError::invalid_expression(format!(
                    "WITHIN GROUP is only valid for ordered-set aggregate functions, \
                     not '{lower_name}'"
                )));
            }
            if distinct {
                return Err(PlannerError::invalid_expression(
                    "DISTINCT is not supported with WITHIN GROUP".to_string(),
                ));
            }
            if within_group
                .iter()
                .any(|order| super::expr_contains_subquery(&order.expr))
            {
                return Err(PlannerError::unsupported_feature(
                    "subquery in aggregate ORDER BY",
                    "future",
                    span,
                ));
            }
        }

        if !order_by.is_empty() {
            if has_over {
                // PostgreSQL: "aggregate ORDER BY is not implemented for
                // window functions".
                return Err(PlannerError::invalid_expression(
                    "aggregate ORDER BY cannot be combined with OVER".to_string(),
                ));
            }
            if !is_aggregate || is_ordered_set {
                return Err(PlannerError::invalid_expression(format!(
                    "ORDER BY in the argument list is only valid for aggregate functions, \
                     not '{lower_name}'"
                )));
            }
            if order_by
                .iter()
                .any(|order| super::expr_contains_subquery(&order.expr))
            {
                return Err(PlannerError::unsupported_feature(
                    "subquery in aggregate ORDER BY",
                    "future",
                    span,
                ));
            }
        }

        if matches!(lower_name, "percentile_disc" | "percentile_cont") && within_group.is_empty() {
            return Err(PlannerError::invalid_expression(format!(
                "WITHIN GROUP (ORDER BY ...) is required for {}",
                lower_name.to_ascii_uppercase()
            )));
        }

        Ok(())
    }

    /// Argument and ordering rules for `PERCENTILE_DISC(fraction) WITHIN
    /// GROUP (ORDER BY sort_expr)` (issue #148, D5). The result type is the
    /// sort expression's type; PostgreSQL 16 behaves identically.
    fn check_percentile(
        &self,
        name: &str,
        args: &[TypedExpr],
        order_by: &[SortExpr],
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if args.len() != 1 {
            return Err(PlannerError::type_mismatch(
                "1 argument",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        let _ = percentile_fraction_named(name, &args[0])?;
        if order_by.len() != 1 {
            return Err(PlannerError::invalid_expression(format!(
                "{} requires WITHIN GROUP (ORDER BY ...) with exactly one sort expression",
                name.to_ascii_uppercase()
            )));
        }
        Ok(order_by[0].expr.resolved_type.clone())
    }

    fn infer_offset_window_result_type(
        &self,
        name: &str,
        args: &mut [TypedExpr],
    ) -> Result<ResolvedType, PlannerError> {
        if let Some(offset) = args.get(1)
            && !matches!(
                offset.resolved_type,
                ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
            )
        {
            return Err(PlannerError::type_mismatch(
                "INTEGER offset",
                offset.resolved_type.type_name(),
                offset.span,
            ));
        }

        let value_type = args
            .first()
            .map(|arg| arg.resolved_type.clone())
            .ok_or_else(|| {
                PlannerError::invalid_expression(format!(
                    "{}() window function expects 1 to 3 arguments",
                    name.to_ascii_uppercase()
                ))
            })?;
        let result_type = if let Some(default) = args.get(2) {
            self.common_compatible_result_type(&value_type, &default.resolved_type, default.span)?
        } else {
            value_type
        };

        coerce_compatible_result(&mut args[0], &result_type);
        if let Some(default) = args.get_mut(2) {
            coerce_compatible_result(default, &result_type);
        }

        Ok(result_type)
    }

    fn common_compatible_result_type(
        &self,
        current: &ResolvedType,
        next: &ResolvedType,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if matches!(current, ResolvedType::Null) {
            return Ok(next.clone());
        }
        if matches!(next, ResolvedType::Null) || current == next {
            return Ok(current.clone());
        }
        if is_numeric_type(current) && is_numeric_type(next) {
            return self.check_arithmetic_op(current, next, span);
        }
        if next.can_cast_to(current) {
            return Ok(current.clone());
        }
        if current.can_cast_to(next) {
            return Ok(next.clone());
        }
        Err(PlannerError::type_mismatch(
            current.type_name(),
            next.type_name(),
            span,
        ))
    }

    /// Infer the type of a BETWEEN expression.
    #[allow(dead_code)]
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

    #[allow(clippy::too_many_arguments)]
    fn infer_between_type_with_scope(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        if row_items(expr).is_some() || row_items(low).is_some() || row_items(high).is_some() {
            let expr_typed = self.infer_row_operand_with_scope(expr, scope, plan_subquery)?;
            let low_typed = self.infer_row_operand_with_scope(low, scope, plan_subquery)?;
            let high_typed = self.infer_row_operand_with_scope(high, scope, plan_subquery)?;
            let width = expr_typed.len();
            self.check_row_arity(width, low_typed.len(), span)?;
            self.check_row_arity(width, high_typed.len(), span)?;
            self.check_row_types(&expr_typed, &low_typed, span)?;
            self.check_row_types(&expr_typed, &high_typed, span)?;
            let mut args = expr_typed;
            args.extend(low_typed);
            args.extend(high_typed);
            return Ok(internal_predicate(
                format!("{INTERNAL_ROW_BETWEEN}:{width}:{}", u8::from(negated)),
                args,
                span,
            ));
        }

        let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;
        let low_typed = self.infer_type_with_scope(low, scope, plan_subquery)?;
        let high_typed = self.infer_type_with_scope(high, scope, plan_subquery)?;
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
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    fn infer_like_type(
        &self,
        expr: &Expr,
        pattern: &Expr,
        escape: Option<&Expr>,
        negated: bool,
        kind: PatternMatchKind,
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
                kind,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn infer_like_type_with_scope(
        &self,
        expr: &Expr,
        pattern: &Expr,
        escape: Option<&Expr>,
        negated: bool,
        kind: PatternMatchKind,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;
        let pattern_typed = self.infer_type_with_scope(pattern, scope, plan_subquery)?;

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
            let typed = self.infer_type_with_scope(esc, scope, plan_subquery)?;
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
                kind,
            },
            resolved_type: ResolvedType::Boolean,
            span,
        })
    }

    /// Infer the type of an IN list expression.
    #[allow(dead_code)]
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

    fn infer_in_list_type_with_scope(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        if row_items(expr).is_some() || list.iter().any(|item| row_items(item).is_some()) {
            let mut args = self.infer_row_operand_with_scope(expr, scope, plan_subquery)?;
            let width = args.len();
            for item in list {
                let typed = self.infer_row_operand_with_scope(item, scope, plan_subquery)?;
                self.check_row_arity(width, typed.len(), item.span)?;
                self.check_row_types(&args[..width], &typed, item.span)?;
                args.extend(typed);
            }
            return Ok(internal_predicate(
                format!("{INTERNAL_ROW_IN}:{width}:{}", u8::from(negated)),
                args,
                span,
            ));
        }

        let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;

        let typed_list: Vec<TypedExpr> = list
            .iter()
            .map(|item| {
                let typed = self.infer_type_with_scope(item, scope, plan_subquery)?;
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

    fn infer_truth_predicate_with_scope(
        &self,
        expr: &Expr,
        value: TruthValue,
        negated: bool,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;
        if !matches!(
            typed.resolved_type,
            ResolvedType::Boolean | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Boolean",
                typed.resolved_type.type_name(),
                expr.span,
            ));
        }
        let name = match value {
            TruthValue::True => INTERNAL_TRUTH_TRUE,
            TruthValue::False => INTERNAL_TRUTH_FALSE,
            TruthValue::Unknown => INTERNAL_TRUTH_UNKNOWN,
        };
        Ok(internal_predicate(
            format!("{name}:{}", u8::from(negated)),
            vec![typed],
            span,
        ))
    }

    fn infer_distinct_predicate_with_scope(
        &self,
        left: &Expr,
        right: &Expr,
        negated: bool,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let (mut left, right, width) =
            self.infer_row_pair_with_scope(left, right, scope, plan_subquery, span)?;
        left.extend(right);
        Ok(internal_predicate(
            format!("{INTERNAL_ROW_DISTINCT}:{width}:{}", u8::from(negated)),
            left,
            span,
        ))
    }

    fn infer_row_pair_with_scope(
        &self,
        left: &Expr,
        right: &Expr,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<(Vec<TypedExpr>, Vec<TypedExpr>, usize), PlannerError> {
        let left = self.infer_row_operand_with_scope(left, scope, plan_subquery)?;
        let right = self.infer_row_operand_with_scope(right, scope, plan_subquery)?;
        let width = left.len();
        self.check_row_arity(width, right.len(), span)?;
        self.check_row_types(&left, &right, span)?;
        Ok((left, right, width))
    }

    fn infer_row_operand_with_scope(
        &self,
        expr: &Expr,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
    ) -> Result<Vec<TypedExpr>, PlannerError> {
        match row_items(expr) {
            Some(items) => items
                .iter()
                .map(|item| self.infer_type_with_scope(item, scope, plan_subquery))
                .collect(),
            None => Ok(vec![self.infer_type_with_scope(
                expr,
                scope,
                plan_subquery,
            )?]),
        }
    }

    fn check_row_arity(
        &self,
        expected: usize,
        actual: usize,
        span: Span,
    ) -> Result<(), PlannerError> {
        if expected == actual {
            Ok(())
        } else {
            Err(PlannerError::RowArityMismatch {
                expected,
                actual,
                line: span.start.line,
                column: span.start.column,
            })
        }
    }

    fn check_row_types(
        &self,
        left: &[TypedExpr],
        right: &[TypedExpr],
        span: Span,
    ) -> Result<(), PlannerError> {
        for (left, right) in left.iter().zip(right) {
            self.check_comparison_op(&left.resolved_type, &right.resolved_type, span)?;
        }
        Ok(())
    }

    /// Infer the type of an IS NULL expression.
    #[allow(dead_code)]
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

    fn infer_is_null_type_with_scope(
        &self,
        expr: &Expr,
        negated: bool,
        scope: &[ScopedTable],
        plan_subquery: &SubqueryPlanner<'_>,
        span: Span,
    ) -> Result<TypedExpr, PlannerError> {
        let expr_typed = self.infer_type_with_scope(expr, scope, plan_subquery)?;

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
        distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        let lower_name = name.to_ascii_lowercase();

        match lower_name.as_str() {
            "count" => self.check_count(args, distinct, star, span),
            "sum" => self.check_sum(args, distinct, star, span),
            "total" => self.check_total(args, distinct, star, span),
            "avg" => self.check_avg(args, distinct, star, span),
            "min" => self.check_min_max(args, distinct, star, span),
            "max" => self.check_min_max(args, distinct, star, span),
            "group_concat" => self.check_group_concat(args, distinct, star, span),
            "string_agg" => self.check_string_agg(args, distinct, star, span),
            "json_group_array" => self.check_json_group_array(args, star, span),
            "json_group_object" => self.check_json_group_object(args, star, span),
            "jsonb_agg" => self
                .check_json_group_array(args, star, span)
                .map(|_| ResolvedType::Json),
            "jsonb_object_agg" => self
                .check_json_group_object(args, star, span)
                .map(|_| ResolvedType::Json),
            name if is_portable_aggregate_name(name) => {
                check_portable_aggregate(name, args, distinct, star, span)
            }
            // GROUPING/GROUPING_ID distinguish grouping-set placeholder NULLs
            // from data NULLs (issue #149, D4). Placement and argument
            // validation happen in the planner; the result is a BIGINT
            // bitmask, so at most 63 arguments are accepted.
            "grouping" | "grouping_id" => {
                if distinct || star {
                    return Err(PlannerError::invalid_expression(
                        "GROUPING does not support DISTINCT or *".to_string(),
                    ));
                }
                if args.is_empty() {
                    return Err(PlannerError::invalid_expression(
                        "GROUPING requires at least one argument".to_string(),
                    ));
                }
                if args.len() > 63 {
                    return Err(PlannerError::invalid_expression(
                        "GROUPING accepts at most 63 arguments".to_string(),
                    ));
                }
                Ok(ResolvedType::BigInt)
            }
            _ => {
                let Some(signature) = crate::scalar::signature(&lower_name) else {
                    return Err(PlannerError::unsupported_feature(
                        format!("function '{name}'"),
                        "future",
                        span,
                    ));
                };
                if distinct || star {
                    return Err(PlannerError::invalid_expression(format!(
                        "scalar function '{name}' does not support DISTINCT or *"
                    )));
                }
                signature.arity.validate(name, args.len(), span)?;
                (signature.check)(args)?;
                let types: Vec<_> = args.iter().map(|arg| arg.resolved_type.clone()).collect();
                match &signature.ret {
                    crate::scalar::ReturnRule::Fixed(ty) => Ok(ty.clone()),
                    crate::scalar::ReturnRule::FromArgs(rule) => rule(&types),
                }
            }
        }
    }

    pub fn validate_having_expr(
        &self,
        expr: &TypedExpr,
        group_keys: &[TypedExpr],
        aggregates: &[AggregateExpr],
    ) -> Result<(), PlannerError> {
        use std::collections::HashSet;

        let group_key_indices: HashSet<usize> = group_keys
            .iter()
            .filter_map(|expr| match &expr.kind {
                TypedExprKind::ColumnRef { column_index, .. } => Some(*column_index),
                _ => None,
            })
            .collect();

        let aggregate_signatures: HashSet<AggregateSignature> = aggregates
            .iter()
            .map(aggregate_signature_from_expr)
            .collect();

        fn walk(
            expr: &TypedExpr,
            group_key_indices: &HashSet<usize>,
            aggregate_signatures: &HashSet<AggregateSignature>,
        ) -> Result<(), PlannerError> {
            match &expr.kind {
                TypedExprKind::ColumnRef { column_index, .. } => {
                    if group_key_indices.contains(column_index) {
                        Ok(())
                    } else {
                        Err(PlannerError::invalid_expression(
                            "column in HAVING must be in GROUP BY or be aggregated".to_string(),
                        ))
                    }
                }
                TypedExprKind::FunctionCall { name, args, .. }
                    if name.eq_ignore_ascii_case("grouping")
                        || name.eq_ignore_ascii_case("grouping_id") =>
                {
                    // GROUPING in HAVING is valid when every argument is a
                    // grouping expression (issue #149, D5); the planner
                    // rewrites the call onto __grouping_id afterwards.
                    for arg in args {
                        match &arg.kind {
                            TypedExprKind::ColumnRef { column_index, .. }
                                if group_key_indices.contains(column_index) => {}
                            _ => {
                                return Err(PlannerError::invalid_expression(
                                    "arguments to GROUPING must be grouping expressions \
                                     of the query"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Ok(())
                }
                TypedExprKind::FunctionCall {
                    name,
                    args,
                    distinct,
                    star,
                    filter,
                    order_by,
                    over: _,
                } if is_aggregate_name(name) => {
                    let signature = aggregate_signature_from_call(
                        name,
                        args,
                        *distinct,
                        *star,
                        filter.as_deref(),
                        order_by,
                    )?;
                    if aggregate_signatures.contains(&signature) {
                        Ok(())
                    } else {
                        Err(PlannerError::invalid_expression(
                            "aggregate in HAVING must appear in plan".to_string(),
                        ))
                    }
                }
                TypedExprKind::BinaryOp { left, right, .. } => {
                    walk(left, group_key_indices, aggregate_signatures)?;
                    walk(right, group_key_indices, aggregate_signatures)
                }
                TypedExprKind::UnaryOp { operand, .. } => {
                    walk(operand, group_key_indices, aggregate_signatures)
                }
                TypedExprKind::Case {
                    operand,
                    branches,
                    else_expr,
                } => {
                    if let Some(operand) = operand {
                        walk(operand, group_key_indices, aggregate_signatures)?;
                    }
                    for branch in branches {
                        walk(&branch.when, group_key_indices, aggregate_signatures)?;
                        walk(&branch.then, group_key_indices, aggregate_signatures)?;
                    }
                    if let Some(else_expr) = else_expr {
                        walk(else_expr, group_key_indices, aggregate_signatures)?;
                    }
                    Ok(())
                }
                TypedExprKind::FunctionCall { args, .. } => {
                    for arg in args {
                        walk(arg, group_key_indices, aggregate_signatures)?;
                    }
                    Ok(())
                }
                TypedExprKind::Between {
                    expr, low, high, ..
                } => {
                    walk(expr, group_key_indices, aggregate_signatures)?;
                    walk(low, group_key_indices, aggregate_signatures)?;
                    walk(high, group_key_indices, aggregate_signatures)
                }
                TypedExprKind::Like {
                    expr,
                    pattern,
                    escape,
                    ..
                } => {
                    walk(expr, group_key_indices, aggregate_signatures)?;
                    walk(pattern, group_key_indices, aggregate_signatures)?;
                    if let Some(esc) = escape {
                        walk(esc, group_key_indices, aggregate_signatures)?;
                    }
                    Ok(())
                }
                TypedExprKind::InList { expr, list, .. } => {
                    walk(expr, group_key_indices, aggregate_signatures)?;
                    for item in list {
                        walk(item, group_key_indices, aggregate_signatures)?;
                    }
                    Ok(())
                }
                TypedExprKind::IsNull { expr, .. } => {
                    walk(expr, group_key_indices, aggregate_signatures)
                }
                _ => Ok(()),
            }
        }

        walk(expr, &group_key_indices, &aggregate_signatures)
    }

    fn check_count(
        &self,
        args: &[TypedExpr],
        distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            if distinct {
                return Err(PlannerError::unsupported_feature(
                    "COUNT(DISTINCT *)",
                    "future",
                    span,
                ));
            }
            if !args.is_empty() {
                return Err(PlannerError::type_mismatch(
                    "no arguments with COUNT(*)",
                    format!("{} arguments", args.len()),
                    span,
                ));
            }
            return Ok(ResolvedType::BigInt);
        }

        if args.len() != 1 {
            return Err(PlannerError::type_mismatch(
                "1 argument",
                format!("{} arguments", args.len()),
                span,
            ));
        }

        if distinct {
            return Ok(ResolvedType::BigInt);
        }

        Ok(ResolvedType::BigInt)
    }

    fn check_sum(
        &self,
        args: &[TypedExpr],
        _distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "numeric argument",
                "COUNT(*) style",
                span,
            ));
        }
        let arg = self.require_single_arg(args, span)?;
        if !is_numeric_type(&arg.resolved_type) && arg.resolved_type != ResolvedType::Null {
            return Err(PlannerError::type_mismatch(
                "numeric",
                arg.resolved_type.type_name().to_string(),
                arg.span,
            ));
        }
        Ok(crate::planner::aggregate_expr::sum_result_type(
            &arg.resolved_type,
        ))
    }

    fn check_total(
        &self,
        args: &[TypedExpr],
        distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "numeric argument",
                "COUNT(*) style",
                span,
            ));
        }
        if distinct {
            return Err(PlannerError::unsupported_feature(
                "TOTAL(DISTINCT ...)",
                "future",
                span,
            ));
        }
        let arg = self.require_single_arg(args, span)?;
        if !is_numeric_type(&arg.resolved_type) && arg.resolved_type != ResolvedType::Null {
            return Err(PlannerError::type_mismatch(
                "numeric",
                arg.resolved_type.type_name().to_string(),
                arg.span,
            ));
        }
        Ok(ResolvedType::Double)
    }

    fn check_avg(
        &self,
        args: &[TypedExpr],
        _distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "numeric argument",
                "COUNT(*) style",
                span,
            ));
        }
        let arg = self.require_single_arg(args, span)?;
        if !is_numeric_type(&arg.resolved_type) && arg.resolved_type != ResolvedType::Null {
            return Err(PlannerError::type_mismatch(
                "numeric",
                arg.resolved_type.type_name().to_string(),
                arg.span,
            ));
        }
        Ok(ResolvedType::Double)
    }

    fn check_min_max(
        &self,
        args: &[TypedExpr],
        _distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "argument",
                "COUNT(*) style",
                span,
            ));
        }
        let arg = self.require_single_arg(args, span)?;
        if matches!(arg.resolved_type, ResolvedType::Vector { .. }) {
            return Err(PlannerError::type_mismatch(
                "comparable",
                arg.resolved_type.type_name().to_string(),
                arg.span,
            ));
        }
        Ok(arg.resolved_type.clone())
    }

    fn check_group_concat(
        &self,
        args: &[TypedExpr],
        _distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "text argument",
                "COUNT(*) style",
                span,
            ));
        }
        if args.is_empty() || args.len() > 2 {
            return Err(PlannerError::type_mismatch(
                "1 or 2 arguments",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        if !matches!(
            args[0].resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Text",
                args[0].resolved_type.type_name().to_string(),
                args[0].span,
            ));
        }
        if args.len() == 2
            && !matches!(
                args[1].resolved_type,
                ResolvedType::Text | ResolvedType::Null
            )
        {
            return Err(PlannerError::type_mismatch(
                "Text",
                args[1].resolved_type.type_name().to_string(),
                args[1].span,
            ));
        }
        Ok(ResolvedType::Text)
    }

    fn check_string_agg(
        &self,
        args: &[TypedExpr],
        _distinct: bool,
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star {
            return Err(PlannerError::type_mismatch(
                "text argument",
                "COUNT(*) style",
                span,
            ));
        }
        if args.len() != 2 {
            return Err(PlannerError::type_mismatch(
                "2 arguments",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        if !matches!(
            args[0].resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Text",
                args[0].resolved_type.type_name().to_string(),
                args[0].span,
            ));
        }
        if !matches!(
            args[1].resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Text",
                args[1].resolved_type.type_name().to_string(),
                args[1].span,
            ));
        }
        Ok(ResolvedType::Text)
    }

    fn check_json_group_array(
        &self,
        args: &[TypedExpr],
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star || args.len() != 1 {
            return Err(PlannerError::type_mismatch(
                "1 argument",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        Ok(ResolvedType::Text)
    }

    fn check_json_group_object(
        &self,
        args: &[TypedExpr],
        star: bool,
        span: Span,
    ) -> Result<ResolvedType, PlannerError> {
        if star || args.len() != 2 {
            return Err(PlannerError::type_mismatch(
                "2 arguments",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        if !matches!(
            args[0].resolved_type,
            ResolvedType::Text | ResolvedType::Null
        ) {
            return Err(PlannerError::type_mismatch(
                "Text",
                args[0].resolved_type.type_name(),
                args[0].span,
            ));
        }
        Ok(ResolvedType::Text)
    }

    fn require_single_arg<'b>(
        &self,
        args: &'b [TypedExpr],
        span: Span,
    ) -> Result<&'b TypedExpr, PlannerError> {
        if args.len() != 1 {
            return Err(PlannerError::type_mismatch(
                "1 argument",
                format!("{} arguments", args.len()),
                span,
            ));
        }
        Ok(&args[0])
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

                let typed_value =
                    self.coerce_column_value(&col_meta.data_type, typed_value, value.span);

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

        let typed_value = self.coerce_column_value(&col_meta.data_type, typed_value, value.span);

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

    /// Insert an execution-time coercion where a column accepts a value whose
    /// source representation differs from its storage representation.
    fn coerce_column_value(
        &self,
        expected: &ResolvedType,
        value: TypedExpr,
        span: Span,
    ) -> TypedExpr {
        if value.resolved_type != *expected
            && value.resolved_type != ResolvedType::Null
            && matches!(
                expected,
                ResolvedType::Integer
                    | ResolvedType::BigInt
                    | ResolvedType::Float
                    | ResolvedType::Double
                    | ResolvedType::Timestamp
                    | ResolvedType::Date
                    | ResolvedType::Time
                    | ResolvedType::Interval
                    | ResolvedType::Decimal { .. }
            )
        {
            TypedExpr::cast(value, expected.clone(), span)
        } else {
            value
        }
    }
}

fn fold_integral_binary(
    left: &TypedExpr,
    op: BinaryOp,
    right: &TypedExpr,
    result_type: &ResolvedType,
    span: Span,
) -> Option<TypedExpr> {
    let value = |expr: &TypedExpr| match &expr.kind {
        TypedExprKind::Literal(Literal::Number(value)) => value.parse::<i64>().ok(),
        _ => None,
    };
    let left = value(left)?;
    let right = value(right)?;
    let folded = match op {
        BinaryOp::BitAnd => left & right,
        BinaryOp::BitOr => left | right,
        BinaryOp::BitXor => left ^ right,
        BinaryOp::ShiftLeft | BinaryOp::ShiftRight => {
            let width = if matches!(result_type, ResolvedType::BigInt) {
                64
            } else {
                32
            };
            if !(0..width).contains(&right) {
                return None;
            }
            if op == BinaryOp::ShiftRight {
                left >> right as u32
            } else {
                let shifted = i128::from(left) * (1_i128 << right as u32);
                if matches!(result_type, ResolvedType::BigInt) {
                    i64::try_from(shifted).ok()?
                } else {
                    i64::from(i32::try_from(shifted).ok()?)
                }
            }
        }
        _ => return None,
    };
    Some(TypedExpr::literal(
        Literal::Number(folded.to_string()),
        result_type.clone(),
        span,
    ))
}

fn is_numeric_type(ty: &ResolvedType) -> bool {
    matches!(
        ty,
        ResolvedType::Integer
            | ResolvedType::BigInt
            | ResolvedType::Float
            | ResolvedType::Double
            | ResolvedType::Decimal { .. }
    )
}

fn decimal_arithmetic_type(
    op: BinaryOp,
    left: &ResolvedType,
    right: &ResolvedType,
) -> Option<ResolvedType> {
    let parts = |ty: &ResolvedType| match ty {
        ResolvedType::Decimal { precision, scale } => Some((*precision, *scale)),
        ResolvedType::Integer => Some((10, 0)),
        ResolvedType::BigInt => Some((19, 0)),
        _ => None,
    };
    if !matches!(left, ResolvedType::Decimal { .. })
        && !matches!(right, ResolvedType::Decimal { .. })
    {
        return None;
    }
    let (lp, ls) = parts(left)?;
    let (rp, rs) = parts(right)?;
    let (raw_precision, raw_scale) = match op {
        BinaryOp::Add | BinaryOp::Sub => ((lp - ls).max(rp - rs) + ls.max(rs) + 1, ls.max(rs)),
        BinaryOp::Mul => (
            lp.saturating_add(rp).saturating_add(1),
            ls.saturating_add(rs),
        ),
        BinaryOp::Div => {
            let scale = 6_u8.max(ls.saturating_add(rp).saturating_add(1));
            ((lp - ls).saturating_add(rs).saturating_add(scale), scale)
        }
        _ => return None,
    };
    let reduction = raw_precision.saturating_sub(38);
    Some(ResolvedType::Decimal {
        precision: raw_precision.min(38),
        scale: raw_scale.saturating_sub(reduction),
    })
}

fn validate_window_frame(
    function_name: &str,
    frame: &WindowFrame,
    order_by: &[SortExpr],
) -> Result<(), PlannerError> {
    if !is_aggregate_name(function_name)
        && !matches!(function_name, "first_value" | "last_value" | "nth_value")
    {
        return Err(PlannerError::invalid_expression(format!(
            "explicit window frames are only supported for aggregate functions and \
             FIRST_VALUE/LAST_VALUE/NTH_VALUE, not {}()",
            function_name.to_ascii_uppercase()
        )));
    }
    if order_by.is_empty() {
        return Err(PlannerError::invalid_expression(
            "explicit ROWS/RANGE window frames require ORDER BY for deterministic evaluation",
        ));
    }
    if matches!(frame.start_bound, WindowFrameBound::UnboundedFollowing) {
        return Err(PlannerError::invalid_expression(
            "window frame start cannot be UNBOUNDED FOLLOWING",
        ));
    }
    if matches!(frame.end_bound, WindowFrameBound::UnboundedPreceding) {
        return Err(PlannerError::invalid_expression(
            "window frame end cannot be UNBOUNDED PRECEDING",
        ));
    }
    if (matches!(frame.start_bound, WindowFrameBound::CurrentRow)
        && matches!(frame.end_bound, WindowFrameBound::Preceding(_)))
        || (matches!(frame.start_bound, WindowFrameBound::Following(_))
            && matches!(
                frame.end_bound,
                WindowFrameBound::Preceding(_) | WindowFrameBound::CurrentRow
            ))
    {
        return Err(PlannerError::invalid_expression(
            "window frame bounds are reversed",
        ));
    }

    let has_offset = matches!(
        frame.start_bound,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    ) || matches!(
        frame.end_bound,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    );
    if frame.units == WindowFrameUnits::Range && has_offset {
        if order_by.len() != 1 {
            return Err(PlannerError::invalid_expression(
                "RANGE offset frames require exactly one ORDER BY expression",
            ));
        }
        if !is_numeric_type(&order_by[0].expr.resolved_type) {
            return Err(PlannerError::invalid_expression(format!(
                "RANGE offset ORDER BY expression must be numeric, found {:?}",
                order_by[0].expr.resolved_type
            )));
        }
    }
    Ok(())
}

fn validate_offset_window_call(
    name: &str,
    arg_count: usize,
    distinct: bool,
    star: bool,
) -> Result<(), PlannerError> {
    let display_name = name.to_ascii_uppercase();
    if distinct {
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function does not accept DISTINCT"
        )));
    }
    if star {
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function does not accept a star argument"
        )));
    }
    if !(1..=3).contains(&arg_count) {
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function expects 1 to 3 arguments"
        )));
    }
    Ok(())
}

fn validate_exact_window_call(
    name: &str,
    arg_count: usize,
    expected: usize,
    distinct: bool,
    star: bool,
) -> Result<(), PlannerError> {
    let display_name = name.to_ascii_uppercase();
    if distinct {
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function does not support DISTINCT"
        )));
    }
    if star {
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function does not support a star argument"
        )));
    }
    if arg_count != expected {
        let signature = match expected {
            0 => "no arguments",
            1 => "one argument",
            2 => "two arguments",
            _ => unreachable!("window signatures are bounded above"),
        };
        return Err(PlannerError::invalid_expression(format!(
            "{display_name}() window function takes {signature}"
        )));
    }
    Ok(())
}

fn validate_positive_integer_argument(
    name: &str,
    argument: &TypedExpr,
) -> Result<(), PlannerError> {
    if matches!(
        argument.resolved_type,
        ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
    ) {
        return Ok(());
    }
    Err(PlannerError::type_mismatch(
        format!("positive INTEGER {} argument", name.to_ascii_uppercase()),
        argument.resolved_type.type_name(),
        argument.span,
    ))
}

fn coerce_compatible_result(expr: &mut TypedExpr, target: &ResolvedType) {
    if expr.resolved_type == *target || matches!(expr.resolved_type, ResolvedType::Null) {
        return;
    }
    let span = expr.span;
    *expr = TypedExpr::cast(expr.clone(), target.clone(), span);
}

fn coerce_case_result(expr: &mut TypedExpr, target: &ResolvedType) {
    if expr.resolved_type == *target || matches!(expr.resolved_type, ResolvedType::Null) {
        return;
    }
    let span = expr.span;
    *expr = TypedExpr::cast(expr.clone(), target.clone(), span);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateSignature {
    name: String,
    distinct: bool,
    star: bool,
    arg_key: Option<String>,
    extra_arg_keys: Vec<String>,
    separator: Option<String>,
    /// FILTER (WHERE ...) predicate identity; aggregates that differ only in
    /// their filter are distinct physical aggregates (issue #148, D10).
    filter_key: Option<String>,
    /// Aggregate-local ordering identity. Populated only for order-sensitive
    /// aggregates so that a validated-then-discarded ORDER BY (D3) still
    /// deduplicates with the unordered call.
    order_key: Option<String>,
}

pub(crate) fn is_portable_aggregate_name(name: &str) -> bool {
    matches!(
        name,
        "variance"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "median"
            | "mode"
            | "quantile_cont"
            | "regr_count"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_syy"
            | "regr_sxy"
            | "regr_slope"
            | "regr_intercept"
            | "regr_r2"
            | "any_value"
            | "first"
            | "last"
            | "arg_min"
            | "min_by"
            | "arg_max"
            | "max_by"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "bool_and"
            | "bool_or"
    )
}

fn canonical_aggregate_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "variance" | "var_samp" => "var_samp".into(),
        "stddev" | "stddev_samp" => "stddev_samp".into(),
        "min_by" => "arg_min".into(),
        "max_by" => "arg_max".into(),
        lower => lower.into(),
    }
}

fn check_portable_aggregate(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    star: bool,
    span: Span,
) -> Result<ResolvedType, PlannerError> {
    if distinct || star {
        return Err(PlannerError::invalid_expression(format!(
            "{} does not support DISTINCT or *",
            name.to_ascii_uppercase()
        )));
    }
    let expected = if matches!(
        name,
        "covar_samp"
            | "covar_pop"
            | "corr"
            | "quantile_cont"
            | "regr_count"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_syy"
            | "regr_sxy"
            | "regr_slope"
            | "regr_intercept"
            | "regr_r2"
            | "arg_min"
            | "min_by"
            | "arg_max"
            | "max_by"
    ) {
        2
    } else {
        1
    };
    if args.len() != expected {
        return Err(PlannerError::type_mismatch(
            format!("{expected} argument(s)"),
            format!("{} arguments", args.len()),
            span,
        ));
    }

    let numeric = |arg: &TypedExpr| {
        matches!(
            arg.resolved_type,
            ResolvedType::Integer
                | ResolvedType::BigInt
                | ResolvedType::Float
                | ResolvedType::Double
                | ResolvedType::Null
        )
    };
    if matches!(
        name,
        "variance"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "median"
            | "quantile_cont"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "regr_count"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_syy"
            | "regr_sxy"
            | "regr_slope"
            | "regr_intercept"
            | "regr_r2"
    ) && !args.iter().all(numeric)
    {
        return Err(PlannerError::type_mismatch(
            "numeric aggregate argument",
            args.iter()
                .find(|arg| !numeric(arg))
                .expect("non-numeric argument")
                .resolved_type
                .type_name(),
            span,
        ));
    }
    if name == "quantile_cont" {
        let _ = percentile_fraction_named(name, &args[1])?;
    }
    if matches!(name, "bit_and" | "bit_or" | "bit_xor")
        && !matches!(
            args[0].resolved_type,
            ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null
        )
    {
        return Err(PlannerError::type_mismatch(
            "INTEGER or BIGINT",
            args[0].resolved_type.type_name(),
            span,
        ));
    }
    if matches!(name, "bool_and" | "bool_or")
        && !matches!(
            args[0].resolved_type,
            ResolvedType::Boolean | ResolvedType::Null
        )
    {
        return Err(PlannerError::type_mismatch(
            "BOOLEAN",
            args[0].resolved_type.type_name(),
            span,
        ));
    }

    Ok(match name {
        "regr_count" => ResolvedType::BigInt,
        "any_value" | "first" | "last" | "arg_min" | "min_by" | "arg_max" | "max_by" | "mode" => {
            args[0].resolved_type.clone()
        }
        "bit_and" | "bit_or" | "bit_xor" => args[0].resolved_type.clone(),
        "bool_and" | "bool_or" => ResolvedType::Boolean,
        _ => ResolvedType::Double,
    })
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "total"
            | "avg"
            | "min"
            | "max"
            | "group_concat"
            | "string_agg"
            | "json_group_array"
            | "json_group_object"
            | "jsonb_agg"
            | "jsonb_object_agg"
            | "percentile_disc"
            | "percentile_cont"
            | "variance"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "median"
            | "mode"
            | "quantile_cont"
            | "regr_count"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_syy"
            | "regr_sxy"
            | "regr_slope"
            | "regr_intercept"
            | "regr_r2"
            | "any_value"
            | "first"
            | "last"
            | "arg_min"
            | "min_by"
            | "arg_max"
            | "max_by"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "bool_and"
            | "bool_or"
    )
}

fn is_ordered_set_aggregate_name(name: &str) -> bool {
    matches!(name, "percentile_disc" | "percentile_cont" | "mode")
}

/// Order identity participates in the signature only where ordering changes
/// the result (D3): order-insensitive aggregates discard their validated
/// ORDER BY, and their signature must match the unordered spelling.
fn is_order_sensitive_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "group_concat"
            | "string_agg"
            | "percentile_disc"
            | "percentile_cont"
            | "mode"
            | "first"
            | "last"
    )
}

/// Extract and validate the `PERCENTILE_DISC` fraction literal (D5): a
/// numeric literal (optionally negated) inside `[0, 1]`.
pub(crate) fn percentile_fraction(arg: &TypedExpr) -> Result<f64, PlannerError> {
    percentile_fraction_named("percentile_disc", arg)
}

pub(crate) fn percentile_fraction_named(name: &str, arg: &TypedExpr) -> Result<f64, PlannerError> {
    let literal = match &arg.kind {
        TypedExprKind::Literal(Literal::Number(text)) => text.parse::<f64>().ok(),
        TypedExprKind::UnaryOp {
            op: crate::ast::expr::UnaryOp::Minus,
            operand,
        } => match &operand.kind {
            TypedExprKind::Literal(Literal::Number(text)) => {
                text.parse::<f64>().ok().map(|value| -value)
            }
            _ => None,
        },
        _ => None,
    };
    let Some(value) = literal else {
        return Err(PlannerError::invalid_expression(format!(
            "{} fraction must be a numeric literal",
            name.to_ascii_uppercase()
        )));
    };
    if !(0.0..=1.0).contains(&value) {
        return Err(PlannerError::invalid_expression(format!(
            "{} fraction must be between 0 and 1",
            name.to_ascii_uppercase()
        )));
    }
    Ok(value)
}

fn typed_sort_signature(order_by: &[SortExpr]) -> Option<String> {
    if order_by.is_empty() {
        return None;
    }
    Some(
        order_by
            .iter()
            .map(|sort| {
                format!(
                    "{}|{}|{}",
                    typed_expr_signature(&sort.expr),
                    sort.asc,
                    sort.nulls_first
                )
            })
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn aggregate_signature_from_expr(expr: &AggregateExpr) -> AggregateSignature {
    let (name, separator, star, arg) = match &expr.function {
        AggregateFunction::Count => (
            "count".to_string(),
            None,
            expr.arg.is_none(),
            expr.arg.as_ref(),
        ),
        AggregateFunction::Sum => ("sum".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::Total => ("total".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::Avg => ("avg".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::Min => ("min".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::Max => ("max".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::GroupConcat { separator } => (
            "group_concat".to_string(),
            separator.clone(),
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::StringAgg { separator } => (
            "string_agg".to_string(),
            separator.clone(),
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::JsonGroupArray => (
            "json_group_array".to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::JsonGroupObject => (
            "json_group_object".to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::JsonbAgg => ("jsonb_agg".to_string(), None, false, expr.arg.as_ref()),
        AggregateFunction::JsonbObjectAgg => (
            "jsonb_object_agg".to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        // The sort value lives in `order_key`; the fraction rides the
        // separator slot so both signature constructions stay symmetric.
        AggregateFunction::PercentileDisc { fraction } => (
            "percentile_disc".to_string(),
            Some(format!("{fraction:?}")),
            false,
            None,
        ),
        AggregateFunction::PercentileCont { fraction } => (
            "percentile_cont".to_string(),
            Some(format!("{fraction:?}")),
            false,
            None,
        ),
        AggregateFunction::QuantileCont { fraction } => (
            "quantile_cont".to_string(),
            Some(format!("{fraction:?}")),
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::Variance { sample } => (
            if *sample { "var_samp" } else { "var_pop" }.to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::Stddev { sample } => (
            if *sample { "stddev_samp" } else { "stddev_pop" }.to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::Covariance { sample } => (
            if *sample { "covar_samp" } else { "covar_pop" }.to_string(),
            None,
            false,
            expr.arg.as_ref(),
        ),
        AggregateFunction::Corr => ("corr".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::Median => ("median".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::Mode => (
            "mode".into(),
            None,
            false,
            expr.order_by
                .is_empty()
                .then_some(())
                .and(expr.arg.as_ref()),
        ),
        AggregateFunction::RegrCount => ("regr_count".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrAvgX => ("regr_avgx".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrAvgY => ("regr_avgy".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrSxx => ("regr_sxx".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrSyy => ("regr_syy".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrSxy => ("regr_sxy".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrSlope => ("regr_slope".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::RegrIntercept => {
            ("regr_intercept".into(), None, false, expr.arg.as_ref())
        }
        AggregateFunction::RegrR2 => ("regr_r2".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::AnyValue => ("any_value".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::First => ("first".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::Last => ("last".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::ArgMin => ("arg_min".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::ArgMax => ("arg_max".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::BitAnd => ("bit_and".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::BitOr => ("bit_or".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::BitXor => ("bit_xor".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::BoolAnd => ("bool_and".into(), None, false, expr.arg.as_ref()),
        AggregateFunction::BoolOr => ("bool_or".into(), None, false, expr.arg.as_ref()),
    };
    AggregateSignature {
        name,
        distinct: expr.distinct,
        star,
        arg_key: arg.map(typed_expr_signature),
        extra_arg_keys: expr.extra_args.iter().map(typed_expr_signature).collect(),
        separator,
        filter_key: expr.filter.as_ref().map(typed_expr_signature),
        order_key: typed_sort_signature(&expr.order_by),
    }
}

fn aggregate_signature_from_call(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    star: bool,
    filter: Option<&TypedExpr>,
    order_by: &[SortExpr],
) -> Result<AggregateSignature, PlannerError> {
    let lower = name.to_ascii_lowercase();
    let is_percentile = matches!(lower.as_str(), "percentile_disc" | "percentile_cont");
    let separator = if name.eq_ignore_ascii_case("group_concat") && args.len() == 2 {
        if let TypedExprKind::Literal(Literal::String(value)) = &args[1].kind {
            Some(value.clone())
        } else {
            return Err(PlannerError::invalid_expression(
                "GROUP_CONCAT separator must be a string literal".to_string(),
            ));
        }
    } else if name.eq_ignore_ascii_case("string_agg") && args.len() == 2 {
        if let TypedExprKind::Literal(Literal::String(value)) = &args[1].kind {
            Some(value.clone())
        } else {
            return Err(PlannerError::invalid_expression(
                "STRING_AGG separator must be a string literal".to_string(),
            ));
        }
    } else if is_percentile && args.len() == 1 {
        Some(format!(
            "{:?}",
            percentile_fraction_named(&lower, &args[0])?
        ))
    } else if lower == "quantile_cont" && args.len() == 2 {
        Some(format!(
            "{:?}",
            percentile_fraction_named(&lower, &args[1])?
        ))
    } else {
        None
    };
    Ok(AggregateSignature {
        name: canonical_aggregate_name(name),
        distinct,
        star,
        arg_key: if is_percentile {
            None
        } else {
            args.first().map(typed_expr_signature)
        },
        extra_arg_keys: if matches!(
            lower.as_str(),
            "group_concat" | "string_agg" | "percentile_disc" | "percentile_cont" | "quantile_cont"
        ) {
            Vec::new()
        } else {
            args.iter().skip(1).map(typed_expr_signature).collect()
        },
        separator,
        filter_key: filter.map(typed_expr_signature),
        order_key: if is_order_sensitive_aggregate_name(name) {
            typed_sort_signature(order_by)
        } else {
            None
        },
    })
}

fn typed_expr_signature(expr: &TypedExpr) -> String {
    format!("{:?}", expr.kind)
}

fn single_column_type(schema: &[ColumnMetadata], span: Span) -> Result<ResolvedType, PlannerError> {
    match schema {
        [column] => Ok(column.data_type.clone()),
        [] => Err(PlannerError::type_mismatch(
            "one-column subquery",
            "zero-column subquery",
            span,
        )),
        _ => Err(PlannerError::type_mismatch(
            "one-column subquery",
            format!("{} columns", schema.len()),
            span,
        )),
    }
}

fn row_items(expr: &Expr) -> Option<&[Expr]> {
    match &expr.kind {
        ExprKind::Row { items } => Some(items),
        _ => None,
    }
}

fn internal_predicate(name: String, args: Vec<TypedExpr>, span: Span) -> TypedExpr {
    TypedExpr::function_call(name, args, false, false, ResolvedType::Boolean, span)
}

// Tests are in type_checker/tests.rs
#[cfg(test)]
#[path = "type_checker/tests.rs"]
mod tests;
