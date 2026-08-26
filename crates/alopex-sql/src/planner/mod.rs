//! Query planning module for the Alopex SQL dialect.
//!
//! This module provides:
//! - [`PlannerError`]: Error types for planning phase
//! - [`ResolvedType`]: Normalized type information for type checking
//! - [`TypedExpr`]: Type-checked expressions with resolved types
//! - [`LogicalPlan`]: Logical query plan representation
//! - [`NameResolver`]: Table and column reference resolution
//! - [`TypeChecker`]: Expression type inference and validation
//! - [`Planner`]: Main entry point for converting AST to LogicalPlan

pub mod aggregate_expr;
mod error;
pub mod knn_optimizer;
pub mod logical_plan;
pub mod name_resolver;
mod named_window;
pub mod type_checker;
pub mod typed_expr;
pub mod types;

#[cfg(test)]
mod planner_tests;

pub use aggregate_expr::{AggregateExpr, AggregateFunction};
pub use error::PlannerError;
pub use knn_optimizer::{KnnPattern, SortDirection, detect_knn_pattern};
pub use logical_plan::{
    JoinType, LogicalPlan, OffsetWindowFunction, RecursiveCteLimits, SetOperator,
    TableFunctionKind, ValueWindowFunction, WindowExpr, WindowFunction,
};
pub use name_resolver::{NameResolver, ResolvedColumn};
pub use type_checker::{ScopedTable, TypeChecker};
pub use typed_expr::{
    ProjectedColumn, Projection, SortExpr, TypedAssignment, TypedCaseWhen, TypedExpr, TypedExprKind,
};
pub use types::ResolvedType;

use crate::ast::ddl::{
    ColumnConstraint, ColumnDef, CreateIndex, CreateTable, DropIndex, DropTable,
};
use crate::ast::dml::{
    Delete, FromItem, GroupByItem, Insert, InsertSource, LITERAL_TABLE, OrderByExpr, QueryBody,
    Select, SelectItem, SetOperation as AstSetOperation, SetOperator as AstSetOperator, Update,
    Values,
};
use crate::ast::expr::{Expr, ExprKind, Literal};
use crate::ast::{PragmaValue, Spanned, Statement, StatementKind};
use crate::catalog::{Catalog, ColumnMetadata, IndexMetadata, TableMetadata};
use crate::{AlopexDialect, DataSourceFormat, Parser, SqlError, TableType};
use named_window::resolve_named_windows;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
struct PlannedRelation {
    plan: LogicalPlan,
    schema: Vec<ColumnMetadata>,
    scope: Vec<ScopedTable>,
}

struct WindowSelectStages {
    plan: LogicalPlan,
    windows: Vec<WindowExpr>,
    qualify: Option<TypedExpr>,
    projection: Projection,
    order_by: Vec<SortExpr>,
    window_schema: Vec<ColumnMetadata>,
}

type CtePlans = HashMap<String, PlannedRelation>;

fn direct_from_reference_count(items: &[FromItem], name: &str) -> usize {
    items
        .iter()
        .map(|item| match item {
            FromItem::Table {
                name: table_name, ..
            } => usize::from(table_name == name),
            FromItem::Join { left, right, .. } => {
                direct_from_item_reference_count(left, name)
                    + direct_from_item_reference_count(right, name)
            }
            FromItem::Derived { .. } | FromItem::Function { .. } => 0,
        })
        .sum()
}

fn direct_from_item_reference_count(item: &FromItem, name: &str) -> usize {
    match item {
        FromItem::Table {
            name: table_name, ..
        } => usize::from(table_name == name),
        FromItem::Join { left, right, .. } => {
            direct_from_item_reference_count(left, name)
                + direct_from_item_reference_count(right, name)
        }
        FromItem::Derived { .. } | FromItem::Function { .. } => 0,
    }
}

fn select_table_reference_count(select: &Select, name: &str) -> usize {
    fn from_item_count(item: &FromItem, name: &str) -> usize {
        match item {
            FromItem::Table {
                name: table_name, ..
            } => usize::from(table_name == name),
            FromItem::Join { left, right, .. } => {
                from_item_count(left, name) + from_item_count(right, name)
            }
            FromItem::Derived { subquery, .. } => query_body_table_reference_count(subquery, name),
            // A table-function argument is an expression; expression-level
            // subqueries are outside this FROM-shape count, exactly as they
            // are for WHERE and the projection.
            FromItem::Function { .. } => 0,
        }
    }

    let from_count = select
        .from
        .iter()
        .map(|item| from_item_count(item, name))
        .sum::<usize>();
    let set_count = select
        .set_operations
        .iter()
        .map(|operation| query_body_table_reference_count(&operation.right, name))
        .sum::<usize>();
    let with_count = select.with.as_ref().map_or(0, |with| {
        with.ctes
            .iter()
            .map(|cte| query_body_table_reference_count(&cte.query, name))
            .sum()
    });
    from_count + set_count + with_count
}

fn values_table_reference_count(values: &Values, name: &str) -> usize {
    let set_count = values
        .set_operations
        .iter()
        .map(|operation| query_body_table_reference_count(&operation.right, name))
        .sum::<usize>();
    let with_count = values.with.as_ref().map_or(0, |with| {
        with.ctes
            .iter()
            .map(|cte| query_body_table_reference_count(&cte.query, name))
            .sum()
    });
    set_count + with_count
}

fn query_body_table_reference_count(body: &QueryBody, name: &str) -> usize {
    match body {
        QueryBody::Select(select) => select_table_reference_count(select, name),
        QueryBody::Values(values) => values_table_reference_count(values, name),
    }
}

fn cte_dependency_cycle(with: &crate::ast::WithClause) -> bool {
    fn visit(node: usize, dependencies: &[Vec<usize>], state: &mut [u8]) -> bool {
        state[node] = 1;
        for &dependency in &dependencies[node] {
            if state[dependency] == 1
                || (state[dependency] == 0 && visit(dependency, dependencies, state))
            {
                return true;
            }
        }
        state[node] = 2;
        false
    }

    let dependencies = with
        .ctes
        .iter()
        .map(|cte| {
            with.ctes
                .iter()
                .enumerate()
                .filter_map(|(dependency, candidate)| {
                    (query_body_table_reference_count(&cte.query, &candidate.name) > 0)
                        .then_some(dependency)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut state = vec![0; dependencies.len()];
    (0..dependencies.len()).any(|node| state[node] == 0 && visit(node, &dependencies, &mut state))
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Literal { .. } | ExprKind::ColumnRef { .. } | ExprKind::VectorLiteral { .. } => {
            false
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        ExprKind::UnaryOp { operand, .. } => expr_contains_subquery(operand),
        ExprKind::TruthPredicate { expr, .. } => expr_contains_subquery(expr),
        ExprKind::IsDistinctFrom { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        ExprKind::Row { items } => items.iter().any(expr_contains_subquery),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_subquery)
                || branches.iter().any(|branch| {
                    expr_contains_subquery(&branch.when) || expr_contains_subquery(&branch.then)
                })
                || else_expr.as_deref().is_some_and(expr_contains_subquery)
        }
        ExprKind::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            args.iter().any(expr_contains_subquery)
                || order_by
                    .iter()
                    .any(|order| expr_contains_subquery(&order.expr))
                || within_group
                    .iter()
                    .any(|order| expr_contains_subquery(&order.expr))
                || filter.as_deref().is_some_and(expr_contains_subquery)
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(expr_contains_subquery)
                        || window
                            .order_by
                            .iter()
                            .any(|order| expr_contains_subquery(&order.expr))
                })
        }
        ExprKind::Cast { expr, .. }
        | ExprKind::TryCast { expr, .. }
        | ExprKind::IsNull { expr, .. } => expr_contains_subquery(expr),
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(pattern)
                || escape.as_deref().is_some_and(expr_contains_subquery)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
        ExprKind::ScalarSubquery { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::Exists { .. }
        | ExprKind::Quantified { .. } => true,
    }
}

fn from_item_contains_subquery(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => false,
        // A table function is a correlated relation source, so it is treated
        // as a subquery boundary wherever one is rejected (issue #151).
        FromItem::Derived { .. } | FromItem::Function { .. } => true,
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            from_item_contains_subquery(left)
                || from_item_contains_subquery(right)
                || condition.as_ref().is_some_and(expr_contains_subquery)
        }
    }
}

fn select_contains_subquery(select: &Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_subquery(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    }) || select.from.iter().any(from_item_contains_subquery)
        || select
            .selection
            .as_ref()
            .is_some_and(expr_contains_subquery)
        || select.group_by.as_ref().is_some_and(|group| {
            group
                .iter()
                .flat_map(GroupByItem::exprs)
                .any(expr_contains_subquery)
        })
        || select.having.as_ref().is_some_and(expr_contains_subquery)
        || select
            .set_operations
            .iter()
            .any(|operation| query_body_contains_subquery(&operation.right))
        || select
            .order_by
            .iter()
            .any(|order| expr_contains_subquery(&order.expr))
        || select.limit.as_ref().is_some_and(expr_contains_subquery)
        || select.offset.as_ref().is_some_and(expr_contains_subquery)
        || select.with.as_ref().is_some_and(|with| {
            with.ctes
                .iter()
                .any(|cte| query_body_contains_subquery(&cte.query))
        })
}

fn values_contains_subquery(values: &Values) -> bool {
    values.rows.iter().flatten().any(expr_contains_subquery)
        || values
            .set_operations
            .iter()
            .any(|operation| query_body_contains_subquery(&operation.right))
        || values
            .order_by
            .iter()
            .any(|order| expr_contains_subquery(&order.expr))
        || values.limit.as_ref().is_some_and(expr_contains_subquery)
        || values.offset.as_ref().is_some_and(expr_contains_subquery)
        || values.with.as_ref().is_some_and(|with| {
            with.ctes
                .iter()
                .any(|cte| query_body_contains_subquery(&cte.query))
        })
}

fn query_body_contains_subquery(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => select_contains_subquery(select),
        QueryBody::Values(values) => values_contains_subquery(values),
    }
}

fn common_values_type(
    current: &ResolvedType,
    next: &ResolvedType,
    span: crate::ast::Span,
) -> Result<ResolvedType, PlannerError> {
    use ResolvedType::{BigInt, Double, Float, Integer, Null};

    if *current == Null {
        return Ok(next.clone());
    }
    if *next == Null || current == next {
        return Ok(current.clone());
    }
    match (current, next) {
        (Integer, BigInt) | (BigInt, Integer) => Ok(BigInt),
        (Integer | BigInt | Float | Double, Integer | BigInt | Float | Double) => Ok(Double),
        _ => Err(PlannerError::type_mismatch(
            current.type_name(),
            next.type_name(),
            span,
        )),
    }
}

/// Planning output used by server-side routing analysis.
///
/// This is intentionally owned by `alopex-sql` and contains no
/// `alopex-cluster` types. Cluster routing layers can translate this DTO into
/// their own routing model without making SQL depend on cluster metadata.
#[derive(Debug, Clone)]
pub struct PlannedStatement {
    /// Logical plan produced by the regular SQL planner.
    pub plan: LogicalPlan,
    /// SQL-owned routing input derived during planning.
    pub routing_input: RoutingInput,
}

impl PlannedStatement {
    /// Statement kind associated with this plan.
    pub fn statement_kind(&self) -> &StatementKind {
        &self.routing_input.statement_kind
    }

    /// Table references extracted for routing analysis.
    pub fn table_references(&self) -> &[TableReference] {
        &self.routing_input.table_references
    }

    /// Planning diagnostics available for routing layers to attach to their
    /// own decision diagnostics.
    pub fn diagnostics(&self) -> &[PlanningDiagnostic] {
        &self.routing_input.diagnostics
    }
}

/// SQL-owned input for routing decision composition.
#[derive(Debug, Clone)]
pub struct RoutingInput {
    /// Original statement kind. Consumers should match on variants rather than
    /// reparsing SQL.
    pub statement_kind: StatementKind,
    /// Conservative table references extracted from the planned statement.
    pub table_references: Vec<TableReference>,
    /// Diagnostics produced while preparing routing input.
    pub diagnostics: Vec<PlanningDiagnostic>,
}

/// A table reference visible at the SQL planning boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReference {
    /// Table name as resolved by the current planner/catalog view.
    pub table_name: String,
    /// Access class requested by the statement.
    pub access: TableReferenceAccess,
    /// Extraction source for diagnostics and future extractor expansion.
    pub source: TableReferenceSource,
}

impl TableReference {
    pub fn new(
        table_name: impl Into<String>,
        access: TableReferenceAccess,
        source: TableReferenceSource,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            access,
            source,
        }
    }
}

/// Access class for a table reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableReferenceAccess {
    /// Read-only scan/reference.
    Read,
    /// Data mutation against an existing table.
    Write,
    /// Table creation.
    Create,
    /// Table drop/removal.
    Drop,
    /// Metadata operation related to a table, such as CREATE INDEX.
    Metadata,
}

/// Where a table reference was extracted from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableReferenceSource {
    /// The existing `LogicalPlan::table_name()` single-table helper.
    TopLevelPlanTableName,
    /// A physical table scan in a logical plan tree.
    LogicalPlanScan,
    /// A DML target table.
    LogicalPlanMutationTarget,
    /// A DDL target table.
    LogicalPlanDdlTarget,
    /// A table referenced by index metadata.
    LogicalPlanIndexTarget,
    /// A table reached through a typed subquery expression.
    TypedExprSubquery,
}

/// Severity for planning diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningDiagnosticSeverity {
    Info,
    Warning,
}

/// SQL planning diagnostic attachment point for routing layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanningDiagnostic {
    /// Stable machine-readable diagnostic code.
    pub code: &'static str,
    /// Diagnostic severity.
    pub severity: PlanningDiagnosticSeverity,
    /// Human-readable context.
    pub message: String,
}

impl PlanningDiagnostic {
    pub fn info(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: PlanningDiagnosticSeverity::Info,
            message: message.into(),
        }
    }

    pub fn warning(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: PlanningDiagnosticSeverity::Warning,
            message: message.into(),
        }
    }
}

/// Parse and plan SQL without executing it, returning SQL-owned routing input.
pub fn plan_sql_for_routing<C: Catalog + ?Sized>(
    catalog: &C,
    sql: &str,
) -> Result<Vec<PlannedStatement>, SqlError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).map_err(SqlError::from)?;
    statements
        .iter()
        .map(|statement| plan_statement_for_routing(catalog, statement).map_err(SqlError::from))
        .collect()
}

/// Plan a parsed statement without executing it, returning SQL-owned routing input.
pub fn plan_statement_for_routing<C: Catalog + ?Sized>(
    catalog: &C,
    statement: &Statement,
) -> Result<PlannedStatement, PlannerError> {
    let planner = Planner::new(catalog);
    let plan = planner.plan(statement)?;
    let routing_input = routing_input_for_plan(statement, &plan)?;
    Ok(PlannedStatement {
        plan,
        routing_input,
    })
}

fn routing_input_for_plan(
    statement: &Statement,
    plan: &LogicalPlan,
) -> Result<RoutingInput, PlannerError> {
    let mut diagnostics = Vec::new();
    let extractor = TableReferenceExtractor::new();
    let table_references = extractor.extract_from_logical_plan(
        plan,
        table_reference_access(statement)?,
        &mut diagnostics,
    );

    Ok(RoutingInput {
        statement_kind: statement.kind.clone(),
        table_references,
        diagnostics,
    })
}

/// Extracts physical table references from SQL-owned planner structures.
#[derive(Debug, Default, Clone, Copy)]
pub struct TableReferenceExtractor;

impl TableReferenceExtractor {
    pub fn new() -> Self {
        Self
    }

    /// Extract references from a logical plan tree. `root_access` is applied to
    /// the top-level statement target; nested typed subqueries are read-only.
    pub fn extract_from_logical_plan(
        &self,
        plan: &LogicalPlan,
        root_access: TableReferenceAccess,
        diagnostics: &mut Vec<PlanningDiagnostic>,
    ) -> Vec<TableReference> {
        let mut references = Vec::new();
        self.extract_plan(
            plan,
            root_access,
            TableReferenceSource::LogicalPlanScan,
            diagnostics,
            &mut references,
        );
        if references.is_empty() {
            diagnostics.push(PlanningDiagnostic::info(
                "ALOPEX-PLAN-ROUTE-001",
                "statement has no physical table reference",
            ));
        }
        references
    }

    /// Extract references from a typed subquery plan embedded in an expression.
    pub fn extract_from_subquery_context(
        &self,
        plan: &LogicalPlan,
        diagnostics: &mut Vec<PlanningDiagnostic>,
    ) -> Vec<TableReference> {
        let mut references = Vec::new();
        self.extract_plan(
            plan,
            TableReferenceAccess::Read,
            TableReferenceSource::TypedExprSubquery,
            diagnostics,
            &mut references,
        );
        references
    }

    fn extract_plan(
        &self,
        plan: &LogicalPlan,
        root_access: TableReferenceAccess,
        scan_source: TableReferenceSource,
        diagnostics: &mut Vec<PlanningDiagnostic>,
        references: &mut Vec<TableReference>,
    ) {
        match plan {
            LogicalPlan::Scan { table, projection } => {
                if table != LITERAL_TABLE {
                    push_table_reference(
                        references,
                        table,
                        TableReferenceAccess::Read,
                        scan_source,
                    );
                }
                self.extract_projection(projection, diagnostics, references);
            }
            LogicalPlan::Values { rows, .. } => {
                for row in rows {
                    for value in row {
                        self.extract_typed_expr(value, diagnostics, references);
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                self.extract_typed_expr(predicate, diagnostics, references);
            }
            LogicalPlan::Project { input, projection } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                self.extract_projection(projection, diagnostics, references);
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            }
            | LogicalPlan::LateralJoin {
                left,
                right,
                condition,
                ..
            } => {
                self.extract_plan(
                    left,
                    TableReferenceAccess::Read,
                    scan_source,
                    diagnostics,
                    references,
                );
                self.extract_plan(
                    right,
                    TableReferenceAccess::Read,
                    scan_source,
                    diagnostics,
                    references,
                );
                if let Some(condition) = condition {
                    self.extract_typed_expr(condition, diagnostics, references);
                }
            }
            LogicalPlan::TableFunction { args, .. } => {
                for arg in args {
                    self.extract_typed_expr(arg, diagnostics, references);
                }
            }
            LogicalPlan::Aggregate {
                input,
                group_keys,
                aggregates,
                having,
                projection,
                grouping_sets: _,
            } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                for expr in group_keys {
                    self.extract_typed_expr(expr, diagnostics, references);
                }
                for aggregate in aggregates {
                    if let Some(arg) = &aggregate.arg {
                        self.extract_typed_expr(arg, diagnostics, references);
                    }
                }
                if let Some(having) = having {
                    self.extract_typed_expr(having, diagnostics, references);
                }
                self.extract_projection(projection, diagnostics, references);
            }
            LogicalPlan::Window { input, windows } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                for window in windows {
                    for expr in &window.partition_by {
                        self.extract_typed_expr(expr, diagnostics, references);
                    }
                    for sort_expr in &window.order_by {
                        self.extract_typed_expr(&sort_expr.expr, diagnostics, references);
                    }
                    match &window.function {
                        WindowFunction::Aggregate(aggregate) => {
                            if let Some(arg) = &aggregate.arg {
                                self.extract_typed_expr(arg, diagnostics, references);
                            }
                        }
                        WindowFunction::Lag(function) | WindowFunction::Lead(function) => {
                            self.extract_typed_expr(&function.value, diagnostics, references);
                            if let Some(offset) = &function.offset {
                                self.extract_typed_expr(offset, diagnostics, references);
                            }
                            if let Some(default) = &function.default {
                                self.extract_typed_expr(default, diagnostics, references);
                            }
                        }
                        WindowFunction::Ntile(argument) => {
                            self.extract_typed_expr(argument, diagnostics, references);
                        }
                        WindowFunction::Value(function) => match function {
                            ValueWindowFunction::FirstValue(value)
                            | ValueWindowFunction::LastValue(value) => {
                                self.extract_typed_expr(value, diagnostics, references);
                            }
                            ValueWindowFunction::NthValue { value, nth } => {
                                self.extract_typed_expr(value, diagnostics, references);
                                self.extract_typed_expr(nth, diagnostics, references);
                            }
                        },
                        WindowFunction::RowNumber
                        | WindowFunction::Rank
                        | WindowFunction::DenseRank
                        | WindowFunction::PercentRank
                        | WindowFunction::CumeDist => {}
                    }
                }
            }
            LogicalPlan::SetOperation { left, right, .. } => {
                self.extract_plan(left, root_access, scan_source, diagnostics, references);
                self.extract_plan(right, root_access, scan_source, diagnostics, references);
            }
            LogicalPlan::RecursiveCte {
                anchor,
                recursive_term,
                ..
            } => {
                self.extract_plan(anchor, root_access, scan_source, diagnostics, references);
                self.extract_plan(
                    recursive_term,
                    root_access,
                    scan_source,
                    diagnostics,
                    references,
                );
            }
            LogicalPlan::RecursiveReference { .. } => {}
            LogicalPlan::Sort { input, order_by } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                for sort_expr in order_by {
                    self.extract_typed_expr(&sort_expr.expr, diagnostics, references);
                }
            }
            LogicalPlan::DistinctOn {
                input, order_by, ..
            } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
                for sort_expr in order_by {
                    self.extract_typed_expr(&sort_expr.expr, diagnostics, references);
                }
            }
            LogicalPlan::Limit { input, .. } => {
                self.extract_plan(input, root_access, scan_source, diagnostics, references);
            }
            LogicalPlan::Insert { table, values, .. } => {
                push_table_reference(
                    references,
                    table,
                    root_access,
                    TableReferenceSource::LogicalPlanMutationTarget,
                );
                for row in values {
                    for value in row {
                        self.extract_typed_expr(value, diagnostics, references);
                    }
                }
            }
            LogicalPlan::InsertSelect { table, source, .. } => {
                push_table_reference(
                    references,
                    table,
                    root_access,
                    TableReferenceSource::LogicalPlanMutationTarget,
                );
                self.extract_plan(
                    source,
                    TableReferenceAccess::Read,
                    scan_source,
                    diagnostics,
                    references,
                );
            }
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                push_table_reference(
                    references,
                    table,
                    root_access,
                    TableReferenceSource::LogicalPlanMutationTarget,
                );
                for assignment in assignments {
                    self.extract_typed_expr(&assignment.value, diagnostics, references);
                }
                if let Some(filter) = filter {
                    self.extract_typed_expr(filter, diagnostics, references);
                }
            }
            LogicalPlan::Delete { table, filter } => {
                push_table_reference(
                    references,
                    table,
                    root_access,
                    TableReferenceSource::LogicalPlanMutationTarget,
                );
                if let Some(filter) = filter {
                    self.extract_typed_expr(filter, diagnostics, references);
                }
            }
            LogicalPlan::CreateTable { table, .. } => push_table_reference(
                references,
                &table.name,
                root_access,
                TableReferenceSource::LogicalPlanDdlTarget,
            ),
            LogicalPlan::DropTable { name, .. } => push_table_reference(
                references,
                name,
                root_access,
                TableReferenceSource::LogicalPlanDdlTarget,
            ),
            LogicalPlan::CreateIndex { index, .. } => push_table_reference(
                references,
                &index.table,
                root_access,
                TableReferenceSource::LogicalPlanIndexTarget,
            ),
            LogicalPlan::DropIndex { name, .. } => diagnostics.push(PlanningDiagnostic::warning(
                "ALOPEX-PLAN-ROUTE-003",
                format!(
                    "DROP INDEX {name} does not expose a target table in the current logical plan"
                ),
            )),
            LogicalPlan::Pragma { .. } => {}
        }
    }

    fn extract_projection(
        &self,
        projection: &Projection,
        diagnostics: &mut Vec<PlanningDiagnostic>,
        references: &mut Vec<TableReference>,
    ) {
        if let Projection::Columns(columns) = projection {
            for column in columns {
                self.extract_typed_expr(&column.expr, diagnostics, references);
            }
        }
    }

    fn extract_typed_expr(
        &self,
        expr: &TypedExpr,
        diagnostics: &mut Vec<PlanningDiagnostic>,
        references: &mut Vec<TableReference>,
    ) {
        match &expr.kind {
            TypedExprKind::Literal(_)
            | TypedExprKind::ColumnRef { .. }
            | TypedExprKind::VectorLiteral(_) => {}
            TypedExprKind::BinaryOp { left, right, .. } => {
                self.extract_typed_expr(left, diagnostics, references);
                self.extract_typed_expr(right, diagnostics, references);
            }
            TypedExprKind::UnaryOp { operand, .. }
            | TypedExprKind::Cast { expr: operand, .. }
            | TypedExprKind::TryCast { expr: operand, .. }
            | TypedExprKind::IsNull { expr: operand, .. } => {
                self.extract_typed_expr(operand, diagnostics, references);
            }
            TypedExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    self.extract_typed_expr(operand, diagnostics, references);
                }
                for branch in branches {
                    self.extract_typed_expr(&branch.when, diagnostics, references);
                    self.extract_typed_expr(&branch.then, diagnostics, references);
                }
                if let Some(else_expr) = else_expr {
                    self.extract_typed_expr(else_expr, diagnostics, references);
                }
            }
            TypedExprKind::FunctionCall {
                args,
                filter,
                order_by,
                ..
            } => {
                for arg in args {
                    self.extract_typed_expr(arg, diagnostics, references);
                }
                if let Some(filter) = filter {
                    self.extract_typed_expr(filter, diagnostics, references);
                }
                for sort in order_by {
                    self.extract_typed_expr(&sort.expr, diagnostics, references);
                }
            }
            TypedExprKind::Between {
                expr, low, high, ..
            } => {
                self.extract_typed_expr(expr, diagnostics, references);
                self.extract_typed_expr(low, diagnostics, references);
                self.extract_typed_expr(high, diagnostics, references);
            }
            TypedExprKind::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.extract_typed_expr(expr, diagnostics, references);
                self.extract_typed_expr(pattern, diagnostics, references);
                if let Some(escape) = escape {
                    self.extract_typed_expr(escape, diagnostics, references);
                }
            }
            TypedExprKind::InList { expr, list, .. } => {
                self.extract_typed_expr(expr, diagnostics, references);
                for item in list {
                    self.extract_typed_expr(item, diagnostics, references);
                }
            }
            TypedExprKind::ScalarSubquery(subquery) => self.extract_plan(
                subquery,
                TableReferenceAccess::Read,
                TableReferenceSource::TypedExprSubquery,
                diagnostics,
                references,
            ),
            TypedExprKind::InSubquery { expr, subquery, .. } => {
                self.extract_typed_expr(expr, diagnostics, references);
                self.extract_plan(
                    subquery,
                    TableReferenceAccess::Read,
                    TableReferenceSource::TypedExprSubquery,
                    diagnostics,
                    references,
                );
            }
            TypedExprKind::Exists { subquery, .. } => self.extract_plan(
                subquery,
                TableReferenceAccess::Read,
                TableReferenceSource::TypedExprSubquery,
                diagnostics,
                references,
            ),
            TypedExprKind::Quantified { expr, subquery, .. } => {
                self.extract_typed_expr(expr, diagnostics, references);
                self.extract_plan(
                    subquery,
                    TableReferenceAccess::Read,
                    TableReferenceSource::TypedExprSubquery,
                    diagnostics,
                    references,
                );
            }
        }
    }
}

fn push_table_reference(
    references: &mut Vec<TableReference>,
    table_name: &str,
    access: TableReferenceAccess,
    source: TableReferenceSource,
) {
    if !references.iter().any(|reference| {
        reference.table_name == table_name
            && reference.access == access
            && reference.source == source
    }) {
        references.push(TableReference::new(table_name, access, source));
    }
}

#[derive(Debug)]
enum GenericHostStatement<'a> {
    CreateTable(&'a CreateTable),
    DropTable(&'a DropTable),
    CreateIndex(&'a CreateIndex),
    DropIndex(&'a DropIndex),
    Pragma {
        name: &'a str,
        value: &'a Option<PragmaValue>,
    },
    Select(&'a Select),
    Values(&'a Values),
    Insert(&'a Insert),
    Update(&'a Update),
    Delete(&'a Delete),
    Unsupported,
}

fn classify_generic_host_statement(statement_kind: &StatementKind) -> GenericHostStatement<'_> {
    // The fallback is intentionally unreachable for the current enum. It
    // becomes the safe route before a future statement-specific host is added.
    #[allow(unreachable_patterns)]
    match statement_kind {
        StatementKind::CreateTable(statement) => GenericHostStatement::CreateTable(statement),
        StatementKind::DropTable(statement) => GenericHostStatement::DropTable(statement),
        StatementKind::CreateIndex(statement) => GenericHostStatement::CreateIndex(statement),
        StatementKind::DropIndex(statement) => GenericHostStatement::DropIndex(statement),
        StatementKind::Pragma { name, value } => GenericHostStatement::Pragma { name, value },
        StatementKind::Select(statement) => GenericHostStatement::Select(statement),
        StatementKind::Values(statement) => GenericHostStatement::Values(statement),
        StatementKind::Insert(statement) => GenericHostStatement::Insert(statement),
        StatementKind::Update(statement) => GenericHostStatement::Update(statement),
        StatementKind::Delete(statement) => GenericHostStatement::Delete(statement),
        _ => GenericHostStatement::Unsupported,
    }
}

fn unsupported_generic_statement(statement: &Statement) -> PlannerError {
    PlannerError::unsupported_feature(
        "statement kind for the generic SQL planner",
        "a statement-specific planner",
        statement.span,
    )
}

fn table_reference_access(statement: &Statement) -> Result<TableReferenceAccess, PlannerError> {
    table_reference_access_for_classified(
        statement,
        classify_generic_host_statement(&statement.kind),
    )
}

fn table_reference_access_for_classified(
    statement: &Statement,
    classified: GenericHostStatement<'_>,
) -> Result<TableReferenceAccess, PlannerError> {
    match classified {
        GenericHostStatement::Select(_) | GenericHostStatement::Values(_) => {
            Ok(TableReferenceAccess::Read)
        }
        GenericHostStatement::Insert(_)
        | GenericHostStatement::Update(_)
        | GenericHostStatement::Delete(_) => Ok(TableReferenceAccess::Write),
        GenericHostStatement::CreateTable(_) => Ok(TableReferenceAccess::Create),
        GenericHostStatement::DropTable(_) => Ok(TableReferenceAccess::Drop),
        GenericHostStatement::CreateIndex(_)
        | GenericHostStatement::DropIndex(_)
        | GenericHostStatement::Pragma { .. } => Ok(TableReferenceAccess::Metadata),
        GenericHostStatement::Unsupported => Err(unsupported_generic_statement(statement)),
    }
}

/// The SQL query planner.
///
/// The planner converts AST statements into logical plans. It performs:
/// - Name resolution: Validates table and column references
/// - Type checking: Infers and validates expression types
/// - Plan construction: Builds the logical plan tree
///
/// # Design Notes
///
/// - The planner uses an immutable reference to the catalog (`&C`)
/// - DDL statements produce plans but don't modify the catalog
/// - The executor is responsible for applying catalog changes
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::MemoryCatalog;
/// use alopex_sql::planner::Planner;
///
/// let catalog = MemoryCatalog::new();
/// let planner = Planner::new(&catalog);
///
/// // Parse and plan a statement
/// // let stmt = parser.parse("SELECT * FROM users")?;
/// // let plan = planner.plan(&stmt)?;
/// ```
pub struct Planner<'a, C: Catalog + ?Sized> {
    catalog: &'a C,
    name_resolver: NameResolver<'a, C>,
    type_checker: TypeChecker<'a, C>,
}

impl<'a, C: Catalog + ?Sized> Planner<'a, C> {
    /// Create a new planner with the given catalog.
    pub fn new(catalog: &'a C) -> Self {
        Self {
            catalog,
            name_resolver: NameResolver::new(catalog),
            type_checker: TypeChecker::new(catalog),
        }
    }

    /// Plan a SQL statement.
    ///
    /// This is the main entry point for converting an AST statement into a logical plan.
    ///
    /// # Errors
    ///
    /// Returns a `PlannerError` if:
    /// - Referenced tables or columns don't exist
    /// - Type checking fails
    /// - DDL validation fails (e.g., table already exists for CREATE TABLE)
    pub fn plan(&self, stmt: &Statement) -> Result<LogicalPlan, PlannerError> {
        self.plan_classified_statement(stmt, classify_generic_host_statement(&stmt.kind))
    }

    fn plan_classified_statement(
        &self,
        stmt: &Statement,
        classified: GenericHostStatement<'_>,
    ) -> Result<LogicalPlan, PlannerError> {
        match classified {
            // DDL statements
            GenericHostStatement::CreateTable(statement) => self.plan_create_table(statement),
            GenericHostStatement::DropTable(statement) => self.plan_drop_table(statement),
            GenericHostStatement::CreateIndex(statement) => self.plan_create_index(statement),
            GenericHostStatement::DropIndex(statement) => self.plan_drop_index(statement),
            GenericHostStatement::Pragma { name, value } => self.plan_pragma(name, value),

            // DML statements
            GenericHostStatement::Select(statement) => self.plan_select(statement),
            GenericHostStatement::Values(statement) => self.plan_values(statement),
            GenericHostStatement::Insert(statement) => self.plan_insert(statement),
            GenericHostStatement::Update(statement) => self.plan_update(statement),
            GenericHostStatement::Delete(statement) => self.plan_delete(statement),
            GenericHostStatement::Unsupported => Err(unsupported_generic_statement(stmt)),
        }
    }

    fn plan_pragma(
        &self,
        raw_name: &str,
        value: &Option<PragmaValue>,
    ) -> Result<LogicalPlan, PlannerError> {
        let name = raw_name.to_ascii_lowercase();
        if !matches!(name.as_str(), "cache_size" | "memory_limit" | "io_stats") {
            return Err(PlannerError::InvalidPragma {
                name,
                reason: "supported names are cache_size, memory_limit, and io_stats".to_string(),
            });
        }
        match name.as_str() {
            "cache_size" => match value {
                Some(PragmaValue::Int(v)) if *v > 0 => {}
                Some(PragmaValue::Int(_)) => {
                    return Err(PlannerError::InvalidPragma {
                        name,
                        reason: "cache_size must be a positive page count".to_string(),
                    });
                }
                Some(PragmaValue::Text(_)) => {
                    return Err(PlannerError::InvalidPragma {
                        name,
                        reason: "cache_size requires an integer page count".to_string(),
                    });
                }
                None => {}
            },
            "memory_limit" => {
                if let Some(PragmaValue::Int(v)) = value
                    && *v < 0
                {
                    return Err(PlannerError::InvalidPragma {
                        name,
                        reason: "memory_limit cannot be negative".to_string(),
                    });
                }
            }
            "io_stats" => {
                if value.is_some() {
                    return Err(PlannerError::InvalidPragma {
                        name,
                        reason: "io_stats does not accept a value".to_string(),
                    });
                }
            }
            _ => unreachable!(),
        }
        Ok(LogicalPlan::Pragma {
            name,
            value: value.clone(),
        })
    }

    // ============================================================
    // DDL Planning Methods (Task 16)
    // ============================================================

    /// Plan a CREATE TABLE statement.
    ///
    /// Validates that the table doesn't already exist (unless IF NOT EXISTS is specified),
    /// and converts the AST column definitions to catalog metadata.
    fn plan_create_table(&self, stmt: &CreateTable) -> Result<LogicalPlan, PlannerError> {
        // Check if table already exists
        if !stmt.if_not_exists && self.catalog.table_exists(&stmt.name) {
            return Err(PlannerError::table_already_exists(&stmt.name));
        }

        // Convert column definitions to metadata
        let columns: Vec<ColumnMetadata> = stmt
            .columns
            .iter()
            .map(|col| self.convert_column_def(col))
            .collect();

        // Collect primary key from table constraints
        let primary_key = Self::extract_primary_key(stmt);

        // Build table metadata
        // Note: table_id defaults to 0 as placeholder; Executor assigns the actual ID
        let mut table = TableMetadata::new(stmt.name.clone(), columns);
        if let Some(pk) = primary_key {
            table = table.with_primary_key(pk);
        }
        table.catalog_name = "default".to_string();
        table.namespace_name = "default".to_string();
        table.table_type = TableType::Managed;
        table.data_source_format = DataSourceFormat::Alopex;
        table.properties = HashMap::new();

        Ok(LogicalPlan::CreateTable {
            table,
            if_not_exists: stmt.if_not_exists,
            with_options: stmt
                .with_options
                .iter()
                .map(|opt| (opt.key.clone(), opt.value.clone()))
                .collect(),
        })
    }

    /// Convert an AST column definition to catalog column metadata.
    fn convert_column_def(&self, col: &ColumnDef) -> ColumnMetadata {
        let data_type = ResolvedType::from_ast(&col.data_type);
        let mut meta = ColumnMetadata::new(col.name.clone(), data_type);

        // Process constraints
        for constraint in &col.constraints {
            meta = Self::apply_column_constraint(meta, constraint);
        }

        meta
    }

    /// Apply a column constraint to column metadata.
    fn apply_column_constraint(
        mut meta: ColumnMetadata,
        constraint: &ColumnConstraint,
    ) -> ColumnMetadata {
        match constraint {
            ColumnConstraint::NotNull { .. } => {
                meta.not_null = true;
            }
            ColumnConstraint::PrimaryKey { .. } => {
                meta.primary_key = true;
                meta.not_null = true; // PRIMARY KEY implies NOT NULL
            }
            ColumnConstraint::Unique { .. } => {
                meta.unique = true;
            }
            ColumnConstraint::Default { value: expr, .. } => {
                meta.default = Some(expr.clone());
            }
        }
        meta
    }

    /// Extract primary key columns from table constraints.
    fn extract_primary_key(stmt: &CreateTable) -> Option<Vec<String>> {
        use crate::ast::ddl::TableConstraint;

        // First check table-level constraints
        // Note: Currently only PrimaryKey variant exists; when more variants are added,
        // this should iterate to find the first PrimaryKey constraint
        if let Some(TableConstraint::PrimaryKey { columns, .. }) = stmt.constraints.first() {
            return Some(columns.clone());
        }

        // Then check column-level PRIMARY KEY constraints
        let pk_columns: Vec<String> = stmt
            .columns
            .iter()
            .filter(|col| col.constraints.iter().any(Self::is_primary_key_constraint))
            .map(|col| col.name.clone())
            .collect();

        if pk_columns.is_empty() {
            None
        } else {
            Some(pk_columns)
        }
    }

    /// Check if a column constraint is a PRIMARY KEY constraint.
    fn is_primary_key_constraint(constraint: &ColumnConstraint) -> bool {
        matches!(constraint, ColumnConstraint::PrimaryKey { .. })
    }

    /// Plan a DROP TABLE statement.
    ///
    /// Validates that the table exists (unless IF EXISTS is specified).
    fn plan_drop_table(&self, stmt: &DropTable) -> Result<LogicalPlan, PlannerError> {
        // Check if table exists
        if !stmt.if_exists && !self.table_exists_in_default(&stmt.name) {
            return Err(PlannerError::TableNotFound {
                name: stmt.name.clone(),
                line: stmt.span.start.line,
                column: stmt.span.start.column,
            });
        }

        Ok(LogicalPlan::DropTable {
            name: stmt.name.clone(),
            if_exists: stmt.if_exists,
        })
    }

    fn table_exists_in_default(&self, name: &str) -> bool {
        match self.catalog.get_table(name) {
            Some(table) => table.catalog_name == "default" && table.namespace_name == "default",
            None => false,
        }
    }

    /// Plan a CREATE INDEX statement.
    ///
    /// Validates that:
    /// - The index doesn't already exist (unless IF NOT EXISTS is specified)
    /// - The target table exists
    /// - The target column exists in the table
    fn plan_create_index(&self, stmt: &CreateIndex) -> Result<LogicalPlan, PlannerError> {
        // Check if index already exists
        if !stmt.if_not_exists && self.catalog.index_exists(&stmt.name) {
            return Err(PlannerError::index_already_exists(&stmt.name));
        }

        // Validate table exists
        let table = self.name_resolver.resolve_table(&stmt.table, stmt.span)?;

        // Validate column exists
        self.name_resolver
            .resolve_column(table, &stmt.column, stmt.span)?;

        // Build index metadata
        // Note: index_id is set to 0 as placeholder; Executor assigns the actual ID
        // Note: column_indices will be resolved by Executor when table schema is available
        let mut index = IndexMetadata::new(
            0,
            stmt.name.clone(),
            stmt.table.clone(),
            vec![stmt.column.clone()],
        );

        if let Some(method) = stmt.method {
            index = index.with_method(method);
        }

        let options: Vec<(String, String)> = stmt
            .options
            .iter()
            .map(|opt| (opt.key.clone(), opt.value.clone()))
            .collect();
        if !options.is_empty() {
            index = index.with_options(options);
        }

        Ok(LogicalPlan::CreateIndex {
            index,
            if_not_exists: stmt.if_not_exists,
        })
    }

    /// Plan a DROP INDEX statement.
    ///
    /// Validates that the index exists (unless IF EXISTS is specified).
    fn plan_drop_index(&self, stmt: &DropIndex) -> Result<LogicalPlan, PlannerError> {
        // Check if index exists
        if !stmt.if_exists && !self.index_exists_in_default(&stmt.name) {
            return Err(PlannerError::index_not_found(&stmt.name));
        }

        Ok(LogicalPlan::DropIndex {
            name: stmt.name.clone(),
            if_exists: stmt.if_exists,
        })
    }

    fn index_exists_in_default(&self, name: &str) -> bool {
        match self.catalog.get_index(name) {
            Some(index) => index.catalog_name == "default" && index.namespace_name == "default",
            None => false,
        }
    }

    // ============================================================
    // DML Planning Methods (Task 17 & 18)
    // ============================================================

    /// Plan a SELECT statement.
    ///
    /// Builds a logical plan tree: Scan -> Filter -> Sort -> Limit
    /// Each layer is optional and only added if the corresponding clause is present.
    fn plan_select(&self, stmt: &Select) -> Result<LogicalPlan, PlannerError> {
        self.plan_select_relation(stmt, &[], &CtePlans::new())
            .map(|relation| relation.plan)
    }

    fn plan_values(&self, stmt: &Values) -> Result<LogicalPlan, PlannerError> {
        self.plan_values_relation(stmt, &[], &CtePlans::new())
            .map(|relation| relation.plan)
    }

    fn plan_query_body_relation(
        &self,
        body: &QueryBody,
        outer_scope: &[ScopedTable],
        enclosing_ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        match body {
            QueryBody::Select(select) => {
                self.plan_select_relation(select, outer_scope, enclosing_ctes)
            }
            QueryBody::Values(values) => {
                self.plan_values_relation(values, outer_scope, enclosing_ctes)
            }
        }
    }

    fn plan_ctes(
        &self,
        stmt: &Select,
        enclosing_ctes: &CtePlans,
    ) -> Result<CtePlans, PlannerError> {
        self.plan_with_clause(stmt.with.as_ref(), enclosing_ctes)
    }

    fn plan_with_clause(
        &self,
        with: Option<&crate::ast::WithClause>,
        enclosing_ctes: &CtePlans,
    ) -> Result<CtePlans, PlannerError> {
        let Some(with) = with else {
            return Ok(enclosing_ctes.clone());
        };
        let mut declared_names = HashSet::new();
        for cte in &with.ctes {
            if !declared_names.insert(&cte.name) {
                return Err(PlannerError::invalid_expression(format!(
                    "common table expression '{}' is defined more than once",
                    cte.name
                )));
            }
        }
        if with.recursive && cte_dependency_cycle(with) {
            return self.plan_recursive_cte(with, enclosing_ctes);
        }

        let mut plans = enclosing_ctes.clone();
        let mut local_names = HashSet::new();
        for cte in &with.ctes {
            if !local_names.insert(cte.name.clone()) {
                return Err(PlannerError::invalid_expression(format!(
                    "common table expression '{}' is defined more than once",
                    cte.name
                )));
            }
            let mut relation = self.plan_query_body_relation(&cte.query, &[], &plans)?;
            if !cte.columns.is_empty() {
                if cte.columns.len() != relation.schema.len() {
                    return Err(PlannerError::cte_column_count_mismatch(
                        &cte.name,
                        cte.columns.len(),
                        relation.schema.len(),
                        cte.span,
                    ));
                }

                let mut column_names = HashSet::new();
                for column_name in &cte.columns {
                    if !column_names.insert(column_name) {
                        return Err(PlannerError::duplicate_cte_column(
                            &cte.name,
                            column_name,
                            cte.span,
                        ));
                    }
                }

                for (column, column_name) in relation.schema.iter_mut().zip(&cte.columns) {
                    column.name.clone_from(column_name);
                }
            }
            plans.insert(cte.name.clone(), relation);
        }
        Ok(plans)
    }

    fn plan_recursive_cte(
        &self,
        with: &crate::ast::WithClause,
        enclosing_ctes: &CtePlans,
    ) -> Result<CtePlans, PlannerError> {
        if with.ctes.len() != 1 {
            return Err(PlannerError::unsupported_feature(
                "recursive WITH containing anything other than exactly one common table expression",
                "a future version",
                with.span,
            ));
        }

        let cte = &with.ctes[0];
        let mut column_names = HashSet::new();
        for column_name in &cte.columns {
            if !column_names.insert(column_name) {
                return Err(PlannerError::duplicate_cte_column(
                    &cte.name,
                    column_name,
                    cte.span,
                ));
            }
        }

        let QueryBody::Select(body) = cte.query.as_ref() else {
            return Err(PlannerError::unsupported_feature(
                "recursive common table expression whose outer body is not SELECT",
                "a future version",
                cte.span,
            ));
        };
        if body.set_operations.len() != 1 {
            return Err(PlannerError::unsupported_feature(
                "recursive common table expression without exactly one UNION or UNION ALL",
                "a future version",
                cte.span,
            ));
        }
        if !body.order_by.is_empty()
            || body.limit.is_some()
            || body.offset.is_some()
            || body.limit_with_ties
        {
            return Err(PlannerError::unsupported_feature(
                "ORDER BY, LIMIT, OFFSET, or FETCH inside a recursive common table expression",
                "a future version",
                body.span,
            ));
        }

        let operation = &body.set_operations[0];
        if operation.operator != AstSetOperator::Union {
            return Err(PlannerError::unsupported_feature(
                "recursive common table expression using an operator other than UNION or UNION ALL",
                "a future version",
                operation.span,
            ));
        }

        let mut anchor = body.clone();
        anchor.with = None;
        anchor.set_operations.clear();
        if select_table_reference_count(&anchor, &cte.name) != 0 {
            return Err(PlannerError::unsupported_feature(
                "recursive common table expression without an anchor term that does not reference itself",
                "a future version",
                anchor.span,
            ));
        }

        let QueryBody::Select(recursive_term) = operation.right.as_ref() else {
            return Err(PlannerError::unsupported_feature(
                "recursive common table expression whose recursive term is not SELECT",
                "a future version",
                operation.span,
            ));
        };
        if select_contains_subquery(recursive_term) {
            return Err(PlannerError::unsupported_feature(
                "subquery in a recursive term",
                "a future version",
                recursive_term.span,
            ));
        }
        let total_references = select_table_reference_count(recursive_term, &cte.name);
        let direct_references = direct_from_reference_count(&recursive_term.from, &cte.name);
        if total_references != 1 || direct_references != 1 {
            return Err(PlannerError::unsupported_feature(
                "recursive term without exactly one direct self-reference",
                "a future version",
                recursive_term.span,
            ));
        }
        if recursive_term.with.is_some() || !recursive_term.set_operations.is_empty() {
            return Err(PlannerError::unsupported_feature(
                "nested WITH or set operation in a recursive term",
                "a future version",
                recursive_term.span,
            ));
        }

        let mut anchor_relation = self.plan_select_relation(&anchor, &[], enclosing_ctes)?;
        if cte.columns.is_empty() {
            let mut anchor_names = HashSet::new();
            for column in &anchor_relation.schema {
                if !anchor_names.insert(&column.name) {
                    return Err(PlannerError::duplicate_cte_column(
                        &cte.name,
                        &column.name,
                        cte.span,
                    ));
                }
            }
        } else {
            if cte.columns.len() != anchor_relation.schema.len() {
                return Err(PlannerError::cte_column_count_mismatch(
                    &cte.name,
                    cte.columns.len(),
                    anchor_relation.schema.len(),
                    cte.span,
                ));
            }
            for (column, name) in anchor_relation.schema.iter_mut().zip(&cte.columns) {
                column.name.clone_from(name);
            }
        }

        let mut recursive_scope = enclosing_ctes.clone();
        recursive_scope.insert(
            cte.name.clone(),
            PlannedRelation {
                plan: LogicalPlan::RecursiveReference {
                    name: cte.name.clone(),
                    schema: anchor_relation.schema.clone(),
                },
                schema: anchor_relation.schema.clone(),
                scope: vec![ScopedTable::new(
                    TableMetadata::new(&cte.name, anchor_relation.schema.clone()),
                    0,
                )],
            },
        );
        let recursive_relation =
            self.plan_select_relation(recursive_term, &[], &recursive_scope)?;
        if recursive_relation.schema.len() != anchor_relation.schema.len() {
            return Err(PlannerError::set_operation_column_count_mismatch(
                anchor_relation.schema.len(),
                recursive_relation.schema.len(),
                operation.span,
            ));
        }
        for (anchor_column, recursive_column) in anchor_relation
            .schema
            .iter()
            .zip(&recursive_relation.schema)
        {
            if anchor_column.data_type != recursive_column.data_type {
                return Err(PlannerError::type_mismatch(
                    anchor_column.data_type.type_name(),
                    recursive_column.data_type.type_name(),
                    operation.span,
                ));
            }
        }

        let schema = anchor_relation.schema.clone();
        let relation = PlannedRelation {
            plan: LogicalPlan::RecursiveCte {
                name: cte.name.clone(),
                anchor: Box::new(anchor_relation.plan),
                recursive_term: Box::new(recursive_relation.plan),
                union_all: operation.all,
                schema: schema.clone(),
                limits: RecursiveCteLimits::default(),
            },
            schema: schema.clone(),
            scope: vec![ScopedTable::new(TableMetadata::new(&cte.name, schema), 0)],
        };
        let mut plans = enclosing_ctes.clone();
        plans.insert(cte.name.clone(), relation);
        Ok(plans)
    }

    fn plan_values_relation(
        &self,
        stmt: &Values,
        outer_scope: &[ScopedTable],
        enclosing_ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        let ctes = self.plan_with_clause(stmt.with.as_ref(), enclosing_ctes)?;
        let relation = self.plan_values_core(stmt, outer_scope, &ctes)?;
        self.apply_set_operations_and_tail(
            relation,
            &stmt.set_operations,
            &stmt.order_by,
            &stmt.limit,
            &stmt.offset,
            stmt.limit_with_ties,
            outer_scope,
            &ctes,
        )
    }

    fn plan_values_core(
        &self,
        stmt: &Values,
        outer_scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        if stmt.rows.is_empty() {
            let schema = Vec::new();
            return Ok(PlannedRelation {
                plan: LogicalPlan::Values {
                    rows: Vec::new(),
                    schema: schema.clone(),
                },
                schema: schema.clone(),
                scope: vec![ScopedTable::new(
                    TableMetadata::new(LITERAL_TABLE, schema),
                    0,
                )],
            });
        }

        let width = stmt.rows[0].len();
        if width == 0 {
            return Err(PlannerError::values_column_count_mismatch(
                1, 1, 0, stmt.span,
            ));
        }

        let mut typed_rows = Vec::with_capacity(stmt.rows.len());
        for (row_index, row) in stmt.rows.iter().enumerate() {
            if row.len() != width {
                return Err(PlannerError::values_column_count_mismatch(
                    row_index + 1,
                    width,
                    row.len(),
                    stmt.span,
                ));
            }
            let mut typed_row = Vec::with_capacity(width);
            for expr in row {
                if expr_contains_subquery(expr) {
                    return Err(PlannerError::invalid_expression(
                        "VALUES expressions cannot contain subqueries".to_string(),
                    ));
                }
                let typed = self.infer_expr_with_scope(expr, outer_scope, ctes)?;
                if typed_expr_contains_aggregate(&typed) || typed_expr_contains_window(&typed) {
                    return Err(PlannerError::invalid_expression(
                        "VALUES expressions must be scalar".to_string(),
                    ));
                }
                typed_row.push(typed);
            }
            typed_rows.push(typed_row);
        }

        let mut common_types = vec![ResolvedType::Null; width];
        for row in &typed_rows {
            for (column_index, value) in row.iter().enumerate() {
                common_types[column_index] = common_values_type(
                    &common_types[column_index],
                    &value.resolved_type,
                    value.span,
                )?;
            }
        }
        for row in &mut typed_rows {
            for (value, target) in row.iter_mut().zip(&common_types) {
                if value.resolved_type != *target && value.resolved_type != ResolvedType::Null {
                    *value = TypedExpr::cast(value.clone(), target.clone(), value.span);
                }
            }
        }

        let schema = common_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| {
                ColumnMetadata::new(format!("column{}", index + 1), data_type)
            })
            .collect::<Vec<_>>();
        Ok(PlannedRelation {
            plan: LogicalPlan::Values {
                rows: typed_rows,
                schema: schema.clone(),
            },
            schema: schema.clone(),
            scope: vec![ScopedTable::new(
                TableMetadata::new(LITERAL_TABLE, schema),
                0,
            )],
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_set_operations_and_tail(
        &self,
        mut relation: PlannedRelation,
        operations: &[AstSetOperation],
        order_by: &[OrderByExpr],
        limit: &Option<Expr>,
        offset: &Option<Expr>,
        limit_with_ties: bool,
        outer_scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        for operation in operations {
            let right = self.plan_query_body_relation(&operation.right, outer_scope, ctes)?;
            if relation.schema.len() != right.schema.len() {
                return Err(PlannerError::set_operation_column_count_mismatch(
                    relation.schema.len(),
                    right.schema.len(),
                    operation.span,
                ));
            }
            for (left_column, right_column) in relation.schema.iter().zip(&right.schema) {
                if left_column.data_type != right_column.data_type {
                    return Err(PlannerError::type_mismatch(
                        left_column.data_type.type_name(),
                        right_column.data_type.type_name(),
                        operation.span,
                    ));
                }
            }

            relation.plan = LogicalPlan::SetOperation {
                left: Box::new(relation.plan),
                right: Box::new(right.plan),
                operator: match operation.operator {
                    AstSetOperator::Union => SetOperator::Union,
                    AstSetOperator::Intersect => SetOperator::Intersect,
                    AstSetOperator::Except => SetOperator::Except,
                },
                all: operation.all,
            };
        }

        relation.scope = vec![ScopedTable::new(
            TableMetadata::new(LITERAL_TABLE, relation.schema.clone()),
            0,
        )];
        if !order_by.is_empty() {
            let order_by =
                self.build_sort_exprs_with_scope(order_by, &relation.scope, &HashMap::new(), ctes)?;
            relation.plan = LogicalPlan::Sort {
                input: Box::new(relation.plan),
                order_by,
            };
        }
        relation.plan = self.apply_pagination(relation.plan, limit, offset, limit_with_ties)?;
        Ok(relation)
    }

    fn plan_select_relation(
        &self,
        stmt: &Select,
        outer_scope: &[ScopedTable],
        enclosing_ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        let resolved_stmt = resolve_named_windows(stmt)?;
        let stmt = &resolved_stmt;
        let ctes = self.plan_ctes(stmt, enclosing_ctes)?;
        if !stmt.set_operations.is_empty() {
            // D7: the trailing ORDER BY belongs to the whole set operation,
            // so the DISTINCT ON prefix contract has no owner here. A nested
            // operand SELECT with its own DISTINCT ON remains supported.
            if !stmt.distinct_on.is_empty() {
                return Err(PlannerError::unsupported_feature(
                    "DISTINCT ON combined with UNION, INTERSECT, or EXCEPT",
                    "a future version",
                    stmt.span,
                ));
            }
            let mut left_select = stmt.clone();
            left_select.with = None;
            left_select.set_operations.clear();
            left_select.order_by.clear();
            left_select.limit = None;
            left_select.offset = None;
            left_select.limit_with_ties = false;
            let relation = self.plan_select_relation(&left_select, outer_scope, &ctes)?;

            return self.apply_set_operations_and_tail(
                relation,
                &stmt.set_operations,
                &stmt.order_by,
                &stmt.limit,
                &stmt.offset,
                stmt.limit_with_ties,
                outer_scope,
                &ctes,
            );
        }

        let mut relation = self.plan_from_items(&stmt.from, stmt.span, outer_scope, &ctes)?;
        let expr_scope = relation
            .scope
            .iter()
            .cloned()
            .chain(offset_scope(outer_scope, relation.schema.len()))
            .collect::<Vec<_>>();

        let has_group_by = stmt
            .group_by
            .as_ref()
            .is_some_and(|items| !items.is_empty());
        let has_aggregate = self.select_contains_aggregate(stmt);
        let has_window = select_contains_window(stmt);
        if stmt.qualify.is_some() && !has_window {
            return Err(PlannerError::invalid_expression(
                "QUALIFY requires at least one window function in the query block".to_string(),
            ));
        }
        let distinct_only =
            stmt.distinct && !has_group_by && !has_aggregate && stmt.having.is_none();

        let has_distinct_on = !stmt.distinct_on.is_empty();
        if has_distinct_on {
            // D10: the grammar cannot produce both, so a hand-built AST with
            // both set is a defect of the producer, not a user error.
            if stmt.distinct {
                return Err(PlannerError::invalid_expression(
                    "DISTINCT and DISTINCT ON cannot be combined".to_string(),
                ));
            }
            // D6: DISTINCT ON keys are scalar sort keys.
            for key in &stmt.distinct_on {
                if expr_contains_aggregate(key) || expr_contains_window(key) {
                    return Err(PlannerError::invalid_expression(
                        "DISTINCT ON expressions cannot contain aggregate or window functions"
                            .to_string(),
                    ));
                }
                if expr_contains_subquery(key) {
                    return Err(PlannerError::invalid_expression(
                        "DISTINCT ON expressions cannot contain subqueries".to_string(),
                    ));
                }
            }
            // D7: v1 rejects combinations whose deduplication order is not
            // covered by the DistinctOn sort contract.
            if has_group_by || has_aggregate || stmt.having.is_some() {
                return Err(PlannerError::unsupported_feature(
                    "DISTINCT ON with GROUP BY, aggregate functions, or HAVING",
                    "a future version",
                    stmt.span,
                ));
            }
            if has_window || stmt.qualify.is_some() {
                return Err(PlannerError::unsupported_feature(
                    "DISTINCT ON with window functions or QUALIFY",
                    "a future version",
                    stmt.span,
                ));
            }
        }

        if stmt.having.as_ref().is_some_and(expr_contains_window) {
            return Err(PlannerError::invalid_expression(
                "HAVING cannot contain window functions".to_string(),
            ));
        }
        if stmt.group_by.as_ref().is_some_and(|items| {
            items
                .iter()
                .flat_map(GroupByItem::exprs)
                .any(expr_contains_window)
        }) {
            return Err(PlannerError::invalid_expression(
                "GROUP BY cannot contain window functions".to_string(),
            ));
        }
        // D5: GROUPING/GROUPING_ID are meaningful only over aggregate output.
        if stmt.group_by.as_ref().is_some_and(|items| {
            items
                .iter()
                .flat_map(GroupByItem::exprs)
                .any(expr_contains_grouping)
        }) {
            return Err(PlannerError::invalid_expression(
                "GROUPING is not allowed in GROUP BY".to_string(),
            ));
        }
        if stmt.selection.as_ref().is_some_and(expr_contains_grouping) {
            return Err(PlannerError::invalid_expression(
                "GROUPING is not allowed in WHERE".to_string(),
            ));
        }
        if !(has_group_by || has_aggregate || stmt.having.is_some()) {
            let grouping_present = stmt.projection.iter().any(|item| match item {
                SelectItem::Expr { expr, .. } => expr_contains_grouping(expr),
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
            }) || stmt
                .order_by
                .iter()
                .any(|order| expr_contains_grouping(&order.expr))
                || stmt.qualify.as_ref().is_some_and(expr_contains_grouping);
            if grouping_present {
                return Err(PlannerError::invalid_expression(
                    "GROUPING is only allowed in grouped queries".to_string(),
                ));
            }
        }

        // SELECT-list aliases are visible to HAVING, QUALIFY, and ORDER BY.
        // `expr_scope` above stays alias-free so that WHERE and GROUP BY keep
        // resolving against the FROM-derived base relations.
        let projection_aliases = collect_projection_aliases(&stmt.projection);

        let final_projection = self.build_projection_with_scope(
            &stmt.projection,
            &relation.schema,
            &expr_scope,
            &ctes,
        )?;
        if !has_window {
            install_base_projection(&mut relation.plan, &final_projection);
        }
        let needs_project_boundary = !matches!(relation.plan, LogicalPlan::Scan { .. });
        let base_schema = relation.schema.clone();
        let mut plan = relation.plan;

        // 3. Add Filter if WHERE clause is present
        if let Some(ref selection) = stmt.selection {
            if expr_contains_window(selection) {
                return Err(PlannerError::invalid_expression(
                    "WHERE cannot contain window functions".to_string(),
                ));
            }
            let predicate = self.infer_expr_with_scope(selection, &expr_scope, &ctes)?;

            // Verify predicate returns Boolean
            if predicate.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    predicate.resolved_type.to_string(),
                    selection.span,
                ));
            }

            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        if has_window && (has_group_by || has_aggregate || stmt.having.is_some()) {
            return self.plan_grouped_window_select(
                stmt,
                &ctes,
                &expr_scope,
                &projection_aliases,
                plan,
            );
        }

        if has_window {
            let mut windows = Vec::new();
            let mut window_map = HashMap::new();
            if let Projection::Columns(columns) = &final_projection {
                for column in columns {
                    self.collect_windows_from_typed_expr(
                        &column.expr,
                        &mut windows,
                        &mut window_map,
                    )?;
                }
            }

            let mut outer_order_by = Vec::new();
            for order_expr in &stmt.order_by {
                let sort_source =
                    substitute_projection_aliases(&order_expr.expr, &projection_aliases);
                let typed = self.infer_expr_with_scope(&sort_source, &expr_scope, &ctes)?;
                self.collect_windows_from_typed_expr(&typed, &mut windows, &mut window_map)?;
                outer_order_by.push(SortExpr::new(
                    typed,
                    order_expr.asc.unwrap_or(true),
                    order_expr.nulls_first.unwrap_or(false),
                ));
            }

            let qualify = if let Some(qualify) = &stmt.qualify {
                let source = substitute_projection_aliases(qualify, &projection_aliases);
                let typed = self.infer_expr_with_scope(&source, &expr_scope, &ctes)?;
                if typed.resolved_type != ResolvedType::Boolean {
                    return Err(PlannerError::type_mismatch(
                        "Boolean",
                        typed.resolved_type.type_name().to_string(),
                        typed.span,
                    ));
                }
                self.collect_windows_from_typed_expr(&typed, &mut windows, &mut window_map)?;
                Some(typed)
            } else {
                None
            };

            let window_names = (0..windows.len())
                .map(|idx| format!("__window_{idx}"))
                .collect::<Vec<_>>();
            let mut window_schema = base_schema;
            window_schema.extend(windows.iter().enumerate().map(|(idx, window)| {
                ColumnMetadata::new(window_names[idx].clone(), window.result_type.clone())
            }));

            let projection = rewrite_projection_for_windows(
                &final_projection,
                &window_map,
                relation.schema.len(),
                &window_names,
            )?;
            let order_by = outer_order_by
                .into_iter()
                .map(|sort| {
                    Ok(SortExpr::new(
                        rewrite_expr_for_windows(
                            &sort.expr,
                            &window_map,
                            relation.schema.len(),
                            &window_names,
                        )?,
                        sort.asc,
                        sort.nulls_first,
                    ))
                })
                .collect::<Result<Vec<_>, PlannerError>>()?;
            let qualify = qualify
                .as_ref()
                .map(|expr| {
                    rewrite_expr_for_windows(
                        expr,
                        &window_map,
                        relation.schema.len(),
                        &window_names,
                    )
                })
                .transpose()?;

            return self.finish_window_select(
                stmt,
                WindowSelectStages {
                    plan,
                    windows,
                    qualify,
                    projection,
                    order_by,
                    window_schema,
                },
            );
        }

        if has_group_by || has_aggregate || stmt.having.is_some() || stmt.distinct {
            if !has_group_by && !has_aggregate && stmt.having.is_some() {
                return Err(PlannerError::invalid_expression(
                    "HAVING requires GROUP BY or aggregate functions".to_string(),
                ));
            }

            let (group_keys, grouping_sets, projected) = if distinct_only {
                let projected = self.build_projected_columns_for_distinct_with_scope(
                    &stmt.projection,
                    &relation.schema,
                    &expr_scope,
                    &ctes,
                )?;
                let group_keys = projected.iter().map(|col| col.expr.clone()).collect();
                (group_keys, None, projected)
            } else {
                let expanded = self.expand_group_by_items(stmt, &expr_scope, &ctes)?;
                let projected = self.build_projected_columns_for_aggregate_with_scope(
                    &stmt.projection,
                    &expr_scope,
                    &ctes,
                )?;
                (expanded.group_keys, expanded.grouping_sets, projected)
            };
            let mut aggregates = Vec::new();
            let mut agg_map = HashMap::new();

            for col in &projected {
                self.collect_aggregates_from_typed_expr(&col.expr, &mut aggregates, &mut agg_map)?;
            }

            let having_typed = if let Some(having) = &stmt.having {
                let having = substitute_projection_aliases(having, &projection_aliases);
                let typed = self.infer_expr_with_scope(&having, &expr_scope, &ctes)?;
                if typed.resolved_type != ResolvedType::Boolean {
                    return Err(PlannerError::type_mismatch(
                        "Boolean",
                        typed.resolved_type.type_name().to_string(),
                        typed.span,
                    ));
                }
                self.collect_aggregates_from_typed_expr(&typed, &mut aggregates, &mut agg_map)?;
                Some(typed)
            } else {
                None
            };

            let mut order_by = Vec::new();
            if !stmt.order_by.is_empty() {
                for order_expr in &stmt.order_by {
                    let sort_source =
                        substitute_projection_aliases(&order_expr.expr, &projection_aliases);
                    let typed = self.infer_expr_with_scope(&sort_source, &expr_scope, &ctes)?;
                    self.collect_aggregates_from_typed_expr(&typed, &mut aggregates, &mut agg_map)?;
                    let asc = order_expr.asc.unwrap_or(true);
                    let nulls_first = order_expr.nulls_first.unwrap_or(false);
                    order_by.push(SortExpr::new(typed, asc, nulls_first));
                }
            }

            if let Some(ref having) = having_typed {
                self.type_checker
                    .validate_having_expr(having, &group_keys, &aggregates)?;
            }

            let mut output_schema = build_aggregate_schema(&group_keys, &aggregates);
            // GROUPING rewrites resolve indexes against the key+aggregate
            // names only; the trailing __grouping_id column is appended to
            // the schema afterwards so the group-key arithmetic inside
            // rewrite_expr_with_maps stays exact (issue #149).
            let output_names: Vec<String> = output_schema.iter().map(|c| c.name.clone()).collect();
            let grouping_rewrite = GroupingRewrite::new(&group_keys, &aggregates, &grouping_sets);
            if grouping_sets.is_some() {
                output_schema.push(ColumnMetadata::new(
                    GROUPING_ID_COLUMN,
                    ResolvedType::BigInt,
                ));
            }

            let projection = self.build_aggregate_projection(
                projected,
                &group_keys,
                &aggregates,
                &output_names,
                Some(&grouping_rewrite),
            )?;

            let having = if let Some(having) = having_typed {
                Some(self.rewrite_expr_for_aggregate(
                    &having,
                    &group_keys,
                    &aggregates,
                    &output_names,
                    Some(&grouping_rewrite),
                )?)
            } else {
                None
            };

            let order_by = order_by
                .into_iter()
                .map(|expr| {
                    let rewritten = self.rewrite_expr_for_aggregate(
                        &expr.expr,
                        &group_keys,
                        &aggregates,
                        &output_names,
                        Some(&grouping_rewrite),
                    )?;
                    Ok(SortExpr::new(rewritten, expr.asc, expr.nulls_first))
                })
                .collect::<Result<Vec<_>, PlannerError>>()?;

            let schema = projection_schema(&projection, &output_schema);
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_keys,
                aggregates,
                having,
                projection,
                grouping_sets,
            };

            if !order_by.is_empty() {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by,
                };
            }

            plan = self.apply_pagination(plan, &stmt.limit, &stmt.offset, stmt.limit_with_ties)?;

            return Ok(PlannedRelation {
                plan,
                schema: schema.clone(),
                scope: vec![ScopedTable::new(
                    TableMetadata::new(LITERAL_TABLE, schema),
                    0,
                )],
            });
        }

        // Non-aggregate path: ORDER BY + LIMIT/OFFSET
        let mut distinct_on_tie_keys: Option<Vec<SortExpr>> = None;
        if has_distinct_on {
            // D2/D3/D4: type the deduplicated ON keys and the user ORDER BY,
            // verify the prefix contract, and synthesize the complete
            // deterministic sort specification. The DistinctOn node emits
            // rows already ordered by that specification, so no separate
            // Sort node is planned (D8). LIMIT/OFFSET applies after
            // deduplication.
            let mut key_exprs: Vec<TypedExpr> = Vec::new();
            let mut key_signatures = HashSet::new();
            for key in &stmt.distinct_on {
                let source = substitute_projection_aliases(key, &projection_aliases);
                let typed = self.infer_expr_with_scope(&source, &expr_scope, &ctes)?;
                if key_signatures.insert(distinct_on_expr_signature(&typed)) {
                    key_exprs.push(typed);
                }
            }
            let user_order_by = self.build_sort_exprs_with_scope(
                &stmt.order_by,
                &expr_scope,
                &projection_aliases,
                &ctes,
            )?;
            // D13: `build_distinct_on_sort_spec` places the user's ORDER BY
            // items first, so the leading `user_order_by_len` entries of the
            // effective specification are exactly that ORDER BY. WITH TIES
            // must read its peer groups from those entries alone: the
            // synthesized implicit ON keys and the all-column tie-breaker
            // tail make every surviving row unique, which would silently
            // degrade WITH TIES to a plain LIMIT.
            let user_order_by_len = user_order_by.len();
            let (key_count, order_by) =
                build_distinct_on_sort_spec(key_exprs, user_order_by, &base_schema, stmt.span)?;
            distinct_on_tie_keys = Some(order_by[..user_order_by_len.min(order_by.len())].to_vec());
            plan = LogicalPlan::DistinctOn {
                input: Box::new(plan),
                key_count,
                order_by,
            };
        } else if !stmt.order_by.is_empty() {
            let order_by = self.build_sort_exprs_with_scope(
                &stmt.order_by,
                &expr_scope,
                &projection_aliases,
                &ctes,
            )?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        plan = self.apply_pagination_with_tie_keys(
            plan,
            &stmt.limit,
            &stmt.offset,
            stmt.limit_with_ties,
            distinct_on_tie_keys.as_deref(),
        )?;

        let output_schema = projection_schema(&final_projection, &relation.schema);
        if needs_project_boundary {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projection: final_projection,
            };
        }
        Ok(PlannedRelation {
            plan,
            schema: output_schema.clone(),
            scope: vec![ScopedTable::new(
                TableMetadata::new(LITERAL_TABLE, output_schema),
                0,
            )],
        })
    }

    /// Plan the aggregate/HAVING stages that feed a window query.
    ///
    /// Grouped aggregation deliberately exposes its internal group-key plus
    /// aggregate schema to the window stage. The user-facing projection stays
    /// above the window so aggregate results can participate in window
    /// arguments and ordering without being evaluated against base rows.
    fn plan_grouped_window_select(
        &self,
        stmt: &Select,
        ctes: &CtePlans,
        expr_scope: &[ScopedTable],
        projection_aliases: &HashMap<String, crate::ast::expr::Expr>,
        mut plan: LogicalPlan,
    ) -> Result<PlannedRelation, PlannerError> {
        let expanded = self.expand_group_by_items(stmt, expr_scope, ctes)?;
        let group_keys = expanded.group_keys;
        let grouping_sets = expanded.grouping_sets;
        let projected = self.build_projected_columns_for_aggregate_with_scope(
            &stmt.projection,
            expr_scope,
            ctes,
        )?;
        let mut aggregates = Vec::new();
        let mut aggregate_map = HashMap::new();
        for column in &projected {
            self.collect_aggregates_from_typed_expr(
                &column.expr,
                &mut aggregates,
                &mut aggregate_map,
            )?;
        }

        let having_typed = if let Some(having) = &stmt.having {
            let having = substitute_projection_aliases(having, projection_aliases);
            if expr_contains_window(&having) {
                return Err(PlannerError::invalid_expression(
                    "HAVING cannot contain window functions".to_string(),
                ));
            }
            let typed = self.infer_expr_with_scope(&having, expr_scope, ctes)?;
            if typed.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    typed.resolved_type.type_name().to_string(),
                    typed.span,
                ));
            }
            self.collect_aggregates_from_typed_expr(&typed, &mut aggregates, &mut aggregate_map)?;
            Some(typed)
        } else {
            None
        };

        let qualify_typed = if let Some(qualify) = &stmt.qualify {
            let qualify = substitute_projection_aliases(qualify, projection_aliases);
            let typed = self.infer_expr_with_scope(&qualify, expr_scope, ctes)?;
            if typed.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    typed.resolved_type.type_name().to_string(),
                    typed.span,
                ));
            }
            self.collect_aggregates_from_typed_expr(&typed, &mut aggregates, &mut aggregate_map)?;
            Some(typed)
        } else {
            None
        };

        let mut outer_order_by = Vec::new();
        for order_expr in &stmt.order_by {
            let source = substitute_projection_aliases(&order_expr.expr, projection_aliases);
            let typed = self.infer_expr_with_scope(&source, expr_scope, ctes)?;
            self.collect_aggregates_from_typed_expr(&typed, &mut aggregates, &mut aggregate_map)?;
            outer_order_by.push(SortExpr::new(
                typed,
                order_expr.asc.unwrap_or(true),
                order_expr.nulls_first.unwrap_or(false),
            ));
        }

        if let Some(having) = &having_typed {
            self.type_checker
                .validate_having_expr(having, &group_keys, &aggregates)?;
        }

        let mut aggregate_schema = build_aggregate_schema(&group_keys, &aggregates);
        // GROUPING rewrites resolve indexes against key+aggregate names only;
        // the __grouping_id column joins the schema afterwards (issue #149).
        let rewrite_names = aggregate_schema
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let grouping_rewrite = GroupingRewrite::new(&group_keys, &aggregates, &grouping_sets);
        if grouping_sets.is_some() {
            aggregate_schema.push(ColumnMetadata::new(
                GROUPING_ID_COLUMN,
                ResolvedType::BigInt,
            ));
        }
        let aggregate_names = aggregate_schema
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let projection = self.build_aggregate_projection(
            projected,
            &group_keys,
            &aggregates,
            &rewrite_names,
            Some(&grouping_rewrite),
        )?;
        let having = having_typed
            .as_ref()
            .map(|expr| {
                self.rewrite_expr_for_aggregate(
                    expr,
                    &group_keys,
                    &aggregates,
                    &rewrite_names,
                    Some(&grouping_rewrite),
                )
            })
            .transpose()?;
        let outer_order_by = outer_order_by
            .into_iter()
            .map(|sort| {
                Ok(SortExpr::new(
                    self.rewrite_expr_for_aggregate(
                        &sort.expr,
                        &group_keys,
                        &aggregates,
                        &rewrite_names,
                        Some(&grouping_rewrite),
                    )?,
                    sort.asc,
                    sort.nulls_first,
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;
        let qualify = qualify_typed
            .as_ref()
            .map(|expr| {
                self.rewrite_expr_for_aggregate(
                    expr,
                    &group_keys,
                    &aggregates,
                    &rewrite_names,
                    Some(&grouping_rewrite),
                )
            })
            .transpose()?;

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
            having,
            projection: Projection::All(aggregate_names),
            grouping_sets,
        };

        let mut windows = Vec::new();
        let mut window_map = HashMap::new();
        if let Projection::Columns(columns) = &projection {
            for column in columns {
                self.collect_windows_from_typed_expr(&column.expr, &mut windows, &mut window_map)?;
            }
        }
        for sort in &outer_order_by {
            self.collect_windows_from_typed_expr(&sort.expr, &mut windows, &mut window_map)?;
        }
        if let Some(qualify) = &qualify {
            self.collect_windows_from_typed_expr(qualify, &mut windows, &mut window_map)?;
        }

        let window_names = (0..windows.len())
            .map(|index| format!("__window_{index}"))
            .collect::<Vec<_>>();
        let mut window_schema = aggregate_schema;
        window_schema.extend(windows.iter().enumerate().map(|(index, window)| {
            ColumnMetadata::new(window_names[index].clone(), window.result_type.clone())
        }));
        let projection = rewrite_projection_for_windows(
            &projection,
            &window_map,
            window_schema.len() - windows.len(),
            &window_names,
        )?;
        let outer_order_by = outer_order_by
            .into_iter()
            .map(|sort| {
                Ok(SortExpr::new(
                    rewrite_expr_for_windows(
                        &sort.expr,
                        &window_map,
                        window_schema.len() - windows.len(),
                        &window_names,
                    )?,
                    sort.asc,
                    sort.nulls_first,
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;
        let qualify = qualify
            .as_ref()
            .map(|expr| {
                rewrite_expr_for_windows(
                    expr,
                    &window_map,
                    window_schema.len() - windows.len(),
                    &window_names,
                )
            })
            .transpose()?;

        self.finish_window_select(
            stmt,
            WindowSelectStages {
                plan,
                windows,
                qualify,
                projection,
                order_by: outer_order_by,
                window_schema,
            },
        )
    }

    /// Finish the stages shared by base-row and grouped window queries.
    fn finish_window_select(
        &self,
        stmt: &Select,
        stages: WindowSelectStages,
    ) -> Result<PlannedRelation, PlannerError> {
        let WindowSelectStages {
            mut plan,
            windows,
            qualify,
            projection,
            order_by,
            window_schema,
        } = stages;
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            windows,
        };
        if let Some(predicate) = qualify {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let output_schema = projection_schema(&projection, &window_schema);
        let mut hidden_order_keys = Vec::new();
        let mut projected_order_by = Vec::with_capacity(order_by.len());
        for sort in order_by {
            let expr =
                match rewrite_expr_for_projected_output(&sort.expr, &projection, &output_schema) {
                    Ok(expr) => expr,
                    Err(_) if !stmt.distinct => {
                        let hidden_index = hidden_order_keys.len();
                        let name = format!("__order_{hidden_index}");
                        let output_index = output_schema.len() + hidden_index;
                        hidden_order_keys.push(ProjectedColumn {
                            expr: sort.expr.clone(),
                            alias: Some(name.clone()),
                        });
                        TypedExpr::column_ref(
                            "__project__".to_string(),
                            name,
                            output_index,
                            sort.expr.resolved_type.clone(),
                            sort.expr.span,
                        )
                    }
                    Err(error) => return Err(error),
                };
            projected_order_by.push(SortExpr::new(expr, sort.asc, sort.nulls_first));
        }
        let has_hidden_order_keys = !hidden_order_keys.is_empty();
        let projection = if has_hidden_order_keys {
            let mut columns = match projection {
                Projection::Columns(columns) => columns,
                Projection::All(_) => output_schema
                    .iter()
                    .enumerate()
                    .map(|(index, column)| ProjectedColumn {
                        expr: TypedExpr::column_ref(
                            "__window__".to_string(),
                            column.name.clone(),
                            index,
                            column.data_type.clone(),
                            stmt.span,
                        ),
                        alias: None,
                    })
                    .collect(),
            };
            columns.extend(hidden_order_keys);
            Projection::Columns(columns)
        } else {
            projection
        };
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            projection,
        };

        if stmt.distinct {
            let group_keys = output_schema
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    TypedExpr::column_ref(
                        LITERAL_TABLE.to_string(),
                        column.name.clone(),
                        index,
                        column.data_type.clone(),
                        stmt.span,
                    )
                })
                .collect();
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_keys,
                aggregates: Vec::new(),
                having: None,
                projection: Projection::All(
                    output_schema
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                ),
                grouping_sets: None,
            };
        }
        if !projected_order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: projected_order_by,
            };
        }
        plan = self.apply_pagination(plan, &stmt.limit, &stmt.offset, stmt.limit_with_ties)?;
        if has_hidden_order_keys {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                projection: Projection::Columns(
                    output_schema
                        .iter()
                        .enumerate()
                        .map(|(index, column)| ProjectedColumn {
                            expr: TypedExpr::column_ref(
                                "__ordered__".to_string(),
                                column.name.clone(),
                                index,
                                column.data_type.clone(),
                                stmt.span,
                            ),
                            alias: Some(column.name.clone()),
                        })
                        .collect(),
                ),
            };
        }

        Ok(PlannedRelation {
            plan,
            schema: output_schema.clone(),
            scope: vec![ScopedTable::new(
                TableMetadata::new(LITERAL_TABLE, output_schema),
                0,
            )],
        })
    }

    /// Build the projection for a SELECT statement.
    ///
    /// Handles wildcard expansion and expression type checking.
    fn plan_from_items(
        &self,
        items: &[FromItem],
        select_span: crate::ast::Span,
        outer_scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        match items {
            [] => {
                let schema = Vec::new();
                Ok(PlannedRelation {
                    plan: LogicalPlan::Scan {
                        table: LITERAL_TABLE.to_string(),
                        projection: Projection::All(Vec::new()),
                    },
                    schema: schema.clone(),
                    scope: vec![ScopedTable::new(
                        TableMetadata::new(LITERAL_TABLE, schema),
                        0,
                    )],
                })
            }
            [single] => self.plan_from_item(single, 0, outer_scope, outer_scope, ctes),
            [first, rest @ ..] => {
                let mut relation = self.plan_from_item(first, 0, outer_scope, outer_scope, ctes)?;
                for item in rest {
                    // Comma-separated items are an implicit cross join, so a
                    // later item may be LATERAL over everything to its left.
                    let left_width = relation.schema.len();
                    let lateral_scope =
                        lateral_outer_scope(&relation.scope, 0, left_width, outer_scope);
                    let right =
                        self.plan_from_item(item, left_width, outer_scope, &lateral_scope, ctes)?;
                    relation = if from_item_is_lateral(item) {
                        self.combine_lateral_join_relation(
                            relation,
                            right,
                            JoinType::Cross,
                            None,
                            None,
                            select_span,
                        )?
                    } else {
                        self.combine_join_relation(
                            relation,
                            right,
                            JoinType::Cross,
                            None,
                            None,
                            select_span,
                        )?
                    };
                }
                Ok(relation)
            }
        }
    }

    fn plan_from_item(
        &self,
        item: &FromItem,
        start_index: usize,
        outer_scope: &[ScopedTable],
        lateral_scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        match item {
            FromItem::Table {
                name,
                alias,
                columns,
                span,
            } => {
                if let Some(cte) = ctes.get(name) {
                    let mut relation = cte.clone();
                    relation.plan = LogicalPlan::Project {
                        input: Box::new(relation.plan),
                        projection: Projection::All(
                            relation.schema.iter().map(|col| col.name.clone()).collect(),
                        ),
                    };
                    let relation_name = alias.clone().unwrap_or_else(|| name.clone());
                    apply_alias_columns(&relation_name, columns, &mut relation.schema, *span)?;
                    relation.scope = vec![ScopedTable::new(
                        TableMetadata::new(relation_name, relation.schema.clone()),
                        start_index,
                    )];
                    return Ok(relation);
                }
                let table = self.name_resolver.resolve_table(name, *span)?.clone();
                let mut scope_table = table.clone();
                if let Some(alias) = alias {
                    scope_table.name = alias.clone();
                }
                let mut schema = table.columns.clone();
                // The physical scan keeps the stored column names; only the
                // relation the query sees is renamed (issue #151, D8).
                let projection =
                    Projection::All(schema.iter().map(|col| col.name.clone()).collect());
                apply_alias_columns(&scope_table.name, columns, &mut schema, *span)?;
                scope_table.columns = schema.clone();
                Ok(PlannedRelation {
                    plan: LogicalPlan::Scan {
                        table: name.clone(),
                        projection,
                    },
                    schema,
                    scope: vec![ScopedTable::new(scope_table, start_index)],
                })
            }
            FromItem::Function {
                name,
                args,
                alias,
                columns,
                span,
                ..
            } => self.plan_table_function(
                name,
                args,
                alias.as_deref(),
                columns,
                *span,
                start_index,
                lateral_scope,
                ctes,
            ),
            FromItem::Join {
                left,
                right,
                join_type,
                condition,
                using,
                natural,
                span,
            } => {
                let right_lateral = from_item_is_lateral(right);
                if right_lateral {
                    // PostgreSQL rejects the same shape: a correlated relation
                    // cannot be the null-supplying side of the join (D3).
                    let unsupported = match join_type {
                        crate::ast::dml::JoinType::Right => Some("RIGHT"),
                        crate::ast::dml::JoinType::Full => Some("FULL"),
                        _ => None,
                    };
                    if let Some(kind) = unsupported {
                        return Err(PlannerError::lateral_join_type_unsupported(kind, *span));
                    }
                }
                let left_relation =
                    self.plan_from_item(left, start_index, outer_scope, lateral_scope, ctes)?;
                // A LATERAL right side is planned against the left row, which
                // is the row the executor supplies as its outer row. An
                // ordinary right side never reads this scope, so the rebase is
                // only paid where it is used.
                let right_lateral_scope = right_lateral.then(|| {
                    lateral_outer_scope(
                        &left_relation.scope,
                        start_index,
                        left_relation.schema.len(),
                        outer_scope,
                    )
                });
                let right_relation = self.plan_from_item(
                    right,
                    start_index + left_relation.schema.len(),
                    outer_scope,
                    right_lateral_scope.as_deref().unwrap_or(lateral_scope),
                    ctes,
                )?;
                let expr_scope = left_relation
                    .scope
                    .iter()
                    .cloned()
                    .chain(right_relation.scope.iter().cloned())
                    .chain(offset_scope(
                        outer_scope,
                        left_relation.schema.len() + right_relation.schema.len(),
                    ))
                    .collect::<Vec<_>>();
                let using = if *natural {
                    Some(natural_join_columns(
                        &left_relation.schema,
                        &right_relation.schema,
                    ))
                } else {
                    using.clone()
                };
                let typed_condition = if let Some(expr) = condition {
                    let typed = self.infer_expr_with_scope(expr, &expr_scope, ctes)?;
                    if typed.resolved_type != ResolvedType::Boolean {
                        return Err(PlannerError::type_mismatch(
                            "Boolean",
                            typed.resolved_type.to_string(),
                            expr.span,
                        ));
                    }
                    Some(typed)
                } else {
                    self.build_using_condition(
                        using.as_deref(),
                        &left_relation,
                        &right_relation,
                        *span,
                    )?
                };
                if right_lateral {
                    self.combine_lateral_join_relation(
                        left_relation,
                        right_relation,
                        map_join_type(*join_type),
                        typed_condition,
                        using.as_deref(),
                        *span,
                    )
                } else {
                    self.combine_join_relation(
                        left_relation,
                        right_relation,
                        map_join_type(*join_type),
                        typed_condition,
                        using,
                        *span,
                    )
                }
            }
            FromItem::Derived {
                subquery,
                alias,
                columns,
                lateral,
                span,
            } => {
                // A derived table is evaluated independently of the query it
                // sits in, so nothing from the enclosing scopes is visible
                // inside it (D4). LATERAL is what lifts that restriction:
                // passing the scope through unconditionally would resolve an
                // outer name into a correlated reference the user never wrote.
                let visible: &[ScopedTable] = if *lateral { lateral_scope } else { &[] };
                let mut relation = self.plan_query_body_relation(subquery, visible, ctes)?;
                let alias = alias.clone().ok_or_else(|| {
                    PlannerError::invalid_expression("derived table requires an alias".to_string())
                })?;
                let source_names = relation
                    .schema
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                relation.plan = LogicalPlan::Project {
                    input: Box::new(relation.plan),
                    projection: Projection::All(source_names),
                };
                apply_alias_columns(&alias, columns, &mut relation.schema, *span)?;
                relation.scope = vec![ScopedTable::new(
                    TableMetadata::new(alias, relation.schema.clone()),
                    start_index,
                )];
                Ok(relation)
            }
        }
    }

    /// Plan a FROM-clause table function (issue #151).
    ///
    /// Arguments are typed against `lateral_scope`, which addresses the row the
    /// executor supplies as the outer row: the preceding FROM items followed by
    /// the enclosing query's own outer row. That holds whether or not LATERAL
    /// was written, because table-function arguments are implicitly lateral
    /// (D2).
    #[allow(clippy::too_many_arguments)]
    fn plan_table_function(
        &self,
        name: &str,
        args: &[Expr],
        alias: Option<&str>,
        columns: &[String],
        span: crate::ast::Span,
        start_index: usize,
        lateral_scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<PlannedRelation, PlannerError> {
        let Some(function) = TableFunctionKind::from_name(name) else {
            return Err(PlannerError::unknown_table_function(name, span));
        };
        let (typed_args, output_type) = match function {
            TableFunctionKind::Unnest => {
                if args.len() != 1 {
                    return Err(PlannerError::invalid_expression(format!(
                        "table function UNNEST takes exactly 1 argument, found {}",
                        args.len()
                    )));
                }
                let typed = self.infer_expr_with_scope(&args[0], lateral_scope, ctes)?;
                if !matches!(
                    typed.resolved_type,
                    ResolvedType::Vector { .. } | ResolvedType::Null
                ) {
                    return Err(PlannerError::type_mismatch(
                        "VECTOR",
                        typed.resolved_type.to_string(),
                        args[0].span,
                    ));
                }
                (vec![typed], ResolvedType::Float)
            }
            TableFunctionKind::GenerateSeries => {
                if !(2..=3).contains(&args.len()) {
                    return Err(PlannerError::invalid_expression(format!(
                        "table function GENERATE_SERIES takes 2 or 3 arguments, found {}",
                        args.len()
                    )));
                }
                let mut typed = Vec::with_capacity(args.len());
                let mut output_type = ResolvedType::Integer;
                for arg in args {
                    let value = self.infer_expr_with_scope(arg, lateral_scope, ctes)?;
                    match value.resolved_type {
                        ResolvedType::BigInt => output_type = ResolvedType::BigInt,
                        ResolvedType::Integer | ResolvedType::Null => {}
                        _ => {
                            return Err(PlannerError::type_mismatch(
                                "INTEGER or BIGINT",
                                value.resolved_type.to_string(),
                                arg.span,
                            ));
                        }
                    }
                    typed.push(value);
                }
                (typed, output_type)
            }
        };

        let mut schema = vec![ColumnMetadata::new(
            function.default_relation_name(),
            output_type,
        )];
        let relation_name = alias
            .map(str::to_string)
            .unwrap_or_else(|| function.default_relation_name().to_string());
        apply_alias_columns(&relation_name, columns, &mut schema, span)?;

        Ok(PlannedRelation {
            plan: LogicalPlan::TableFunction {
                function,
                args: typed_args,
                schema: schema.clone(),
            },
            schema: schema.clone(),
            scope: vec![ScopedTable::new(
                TableMetadata::new(relation_name, schema),
                start_index,
            )],
        })
    }

    fn combine_lateral_join_relation(
        &self,
        left: PlannedRelation,
        right: PlannedRelation,
        join_type: JoinType,
        condition: Option<TypedExpr>,
        using: Option<&[String]>,
        _span: crate::ast::Span,
    ) -> Result<PlannedRelation, PlannerError> {
        // USING/NATURAL merges the common columns the same way it does for an
        // ordinary join; only the execution strategy differs. The merged
        // equality already lives in `condition`, so the node itself does not
        // carry the column list.
        let (schema, scope) = combine_join_shape(&left, &right, using);
        Ok(PlannedRelation {
            plan: LogicalPlan::LateralJoin {
                left: Box::new(left.plan),
                right: Box::new(right.plan),
                join_type,
                condition,
                right_schema: right.schema,
            },
            schema,
            scope,
        })
    }

    fn combine_join_relation(
        &self,
        left: PlannedRelation,
        right: PlannedRelation,
        join_type: JoinType,
        condition: Option<TypedExpr>,
        using: Option<Vec<String>>,
        _span: crate::ast::Span,
    ) -> Result<PlannedRelation, PlannerError> {
        let (schema, scope) = combine_join_shape(&left, &right, using.as_deref());
        Ok(PlannedRelation {
            plan: LogicalPlan::Join {
                left: Box::new(left.plan),
                right: Box::new(right.plan),
                join_type,
                condition,
                using,
            },
            schema,
            scope,
        })
    }

    fn build_using_condition(
        &self,
        using: Option<&[String]>,
        left: &PlannedRelation,
        right: &PlannedRelation,
        span: crate::ast::Span,
    ) -> Result<Option<TypedExpr>, PlannerError> {
        let Some(columns) = using else {
            return Ok(None);
        };
        let mut condition = None;
        for column in columns {
            let left_col = find_scoped_column(&left.scope, column, span)?;
            let right_col = find_scoped_column(&right.scope, column, span)?;
            let left_expr = merged_scoped_column_expr(&left_col, column, span);
            let right_expr = merged_scoped_column_expr(&right_col, column, span);
            self.type_checker
                .check_comparison_op(&left_col.ty, &right_col.ty, span)?;
            let eq = TypedExpr::binary_op(
                left_expr,
                crate::ast::expr::BinaryOp::Eq,
                right_expr,
                ResolvedType::Boolean,
                span,
            );
            condition = Some(match condition {
                Some(prev) => TypedExpr::binary_op(
                    prev,
                    crate::ast::expr::BinaryOp::And,
                    eq,
                    ResolvedType::Boolean,
                    span,
                ),
                None => eq,
            });
        }
        Ok(condition)
    }

    fn infer_expr_with_scope(
        &self,
        expr: &crate::ast::expr::Expr,
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<TypedExpr, PlannerError> {
        self.type_checker
            .infer_type_with_scope(expr, scope, &|stmt, outer_scope| {
                let relation = match &stmt.kind {
                    StatementKind::Select(select) => {
                        self.plan_select_relation(select, outer_scope, ctes)?
                    }
                    StatementKind::Values(values) => {
                        self.plan_values_relation(values, outer_scope, ctes)?
                    }
                    _ => {
                        return Err(PlannerError::unsupported_feature(
                            "non-query subquery",
                            "a future version",
                            stmt.span(),
                        ));
                    }
                };
                Ok((relation.plan, relation.schema))
            })
    }

    #[allow(dead_code)]
    fn build_projection(
        &self,
        items: &[SelectItem],
        table: &TableMetadata,
    ) -> Result<Projection, PlannerError> {
        // Check for wildcard - if present, expand it
        if items.len() == 1 && matches!(&items[0], SelectItem::Wildcard { .. }) {
            let columns = self.name_resolver.expand_wildcard(table);
            return Ok(Projection::All(columns));
        }

        // Process each select item
        let mut projected_columns = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard { span } => {
                    // Wildcard mixed with other items - expand inline
                    for col in &table.columns {
                        let column_index = table.get_column_index(&col.name).unwrap();
                        let typed_expr = TypedExpr::column_ref(
                            table.name.clone(),
                            col.name.clone(),
                            column_index,
                            col.data_type.clone(),
                            *span,
                        );
                        projected_columns.push(ProjectedColumn::new(typed_expr));
                    }
                }
                SelectItem::QualifiedWildcard {
                    table: qualifier,
                    span,
                } => {
                    if qualifier != &table.name {
                        return Err(PlannerError::invalid_expression(format!(
                            "table '{qualifier}' is not available for wildcard projection"
                        )));
                    }
                    for col in &table.columns {
                        let column_index = table.get_column_index(&col.name).unwrap();
                        projected_columns.push(ProjectedColumn::new(TypedExpr::column_ref(
                            table.name.clone(),
                            col.name.clone(),
                            column_index,
                            col.data_type.clone(),
                            *span,
                        )));
                    }
                }
                SelectItem::Expr { expr, alias, .. } => {
                    let typed_expr = self.type_checker.infer_type(expr, table)?;
                    let projected = if let Some(alias) = alias {
                        ProjectedColumn::with_alias(typed_expr, alias.clone())
                    } else {
                        ProjectedColumn::new(typed_expr)
                    };
                    projected_columns.push(projected);
                }
            }
        }

        Ok(Projection::Columns(projected_columns))
    }

    fn build_projection_with_scope(
        &self,
        items: &[SelectItem],
        schema: &[ColumnMetadata],
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<Projection, PlannerError> {
        if items.len() == 1 && matches!(&items[0], SelectItem::Wildcard { .. }) {
            return Ok(Projection::All(visible_wildcard_columns(schema, scope)));
        }

        let mut projected_columns = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard { span } => {
                    for scoped in scope {
                        for (local_idx, col) in scoped.table.columns.iter().enumerate() {
                            projected_columns.push(ProjectedColumn::new(TypedExpr::column_ref(
                                scoped.table.name.clone(),
                                col.name.clone(),
                                scoped.start_index + local_idx,
                                col.data_type.clone(),
                                *span,
                            )));
                        }
                    }
                }
                SelectItem::QualifiedWildcard { table, span } => {
                    let scoped = scope
                        .iter()
                        .filter(|scoped| scoped.table.name == *table)
                        .collect::<Vec<_>>();
                    match scoped.as_slice() {
                        [] => {
                            return Err(PlannerError::invalid_expression(format!(
                                "table '{table}' is not available for wildcard projection"
                            )));
                        }
                        [scoped] => {
                            for (local_idx, col) in scoped.table.columns.iter().enumerate() {
                                projected_columns.push(ProjectedColumn::new(
                                    TypedExpr::column_ref(
                                        scoped.table.name.clone(),
                                        col.name.clone(),
                                        scoped.start_index + local_idx,
                                        col.data_type.clone(),
                                        *span,
                                    ),
                                ));
                            }
                        }
                        _ => {
                            return Err(PlannerError::ambiguous_column(
                                table,
                                scoped
                                    .iter()
                                    .map(|scoped| scoped.table.name.clone())
                                    .collect(),
                                *span,
                            ));
                        }
                    }
                }
                SelectItem::Expr { expr, alias, .. } => {
                    let typed_expr = self.infer_expr_with_scope(expr, scope, ctes)?;
                    let projected = if let Some(alias) = alias {
                        ProjectedColumn::with_alias(typed_expr, alias.clone())
                    } else {
                        ProjectedColumn::new(typed_expr)
                    };
                    projected_columns.push(projected);
                }
            }
        }

        Ok(Projection::Columns(projected_columns))
    }

    /// Build sort expressions from ORDER BY clause.
    #[allow(dead_code)]
    fn build_sort_exprs(
        &self,
        order_by: &[OrderByExpr],
        table: &TableMetadata,
    ) -> Result<Vec<SortExpr>, PlannerError> {
        let mut sort_exprs = Vec::new();

        for order_expr in order_by {
            let typed_expr = self.type_checker.infer_type(&order_expr.expr, table)?;

            // Determine sort direction (default: ASC)
            let asc = order_expr.asc.unwrap_or(true);

            // Determine NULLS ordering (default: NULLS LAST for both ASC and DESC)
            let nulls_first = order_expr.nulls_first.unwrap_or(false);

            sort_exprs.push(SortExpr::new(typed_expr, asc, nulls_first));
        }

        Ok(sort_exprs)
    }

    fn build_sort_exprs_with_scope(
        &self,
        order_by: &[OrderByExpr],
        scope: &[ScopedTable],
        projection_aliases: &HashMap<String, crate::ast::expr::Expr>,
        ctes: &CtePlans,
    ) -> Result<Vec<SortExpr>, PlannerError> {
        let mut sort_exprs = Vec::new();
        for order_expr in order_by {
            let sort_source = substitute_projection_aliases(&order_expr.expr, projection_aliases);
            let typed_expr = self.infer_expr_with_scope(&sort_source, scope, ctes)?;
            let asc = order_expr.asc.unwrap_or(true);
            let nulls_first = order_expr.nulls_first.unwrap_or(false);
            sort_exprs.push(SortExpr::new(typed_expr, asc, nulls_first));
        }
        Ok(sort_exprs)
    }

    fn select_contains_aggregate(&self, stmt: &Select) -> bool {
        stmt.projection.iter().any(|item| match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
            SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
        }) || stmt
            .group_by
            .as_ref()
            .map(|items| {
                items
                    .iter()
                    .flat_map(GroupByItem::exprs)
                    .any(expr_contains_aggregate)
            })
            .unwrap_or(false)
            || stmt
                .having
                .as_ref()
                .map(expr_contains_aggregate)
                .unwrap_or(false)
            || stmt
                .qualify
                .as_ref()
                .map(expr_contains_aggregate)
                .unwrap_or(false)
            || stmt
                .order_by
                .iter()
                .any(|order| expr_contains_aggregate(&order.expr))
    }

    #[allow(dead_code)]
    fn build_group_keys(
        &self,
        stmt: &Select,
        table: &TableMetadata,
    ) -> Result<Vec<TypedExpr>, PlannerError> {
        let mut keys = Vec::new();
        if let Some(items) = &stmt.group_by {
            for expr in items.iter().flat_map(GroupByItem::exprs) {
                let typed = self.type_checker.infer_type(expr, table)?;
                if typed_expr_contains_aggregate(&typed) {
                    return Err(PlannerError::invalid_expression(
                        "GROUP BY cannot contain aggregate functions".to_string(),
                    ));
                }
                if !matches!(typed.kind, TypedExprKind::ColumnRef { .. }) {
                    return Err(PlannerError::invalid_expression(
                        "GROUP BY expressions must be column references".to_string(),
                    ));
                }
                keys.push(typed);
            }
        }
        Ok(keys)
    }

    /// Type one grouping key with the shared GROUP BY constraints.
    fn type_group_key_with_scope(
        &self,
        expr: &Expr,
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<TypedExpr, PlannerError> {
        let typed = self.infer_expr_with_scope(expr, scope, ctes)?;
        if typed_expr_contains_aggregate(&typed) {
            return Err(PlannerError::invalid_expression(
                "GROUP BY cannot contain aggregate functions".to_string(),
            ));
        }
        if !matches!(typed.kind, TypedExprKind::ColumnRef { .. }) {
            return Err(PlannerError::invalid_expression(
                "GROUP BY expressions must be column references".to_string(),
            ));
        }
        Ok(typed)
    }

    /// Expand GROUP BY items into a flat key list plus grouping-set masks.
    ///
    /// Without ROLLUP/CUBE/GROUPING SETS the result is the pre-existing key
    /// list with `grouping_sets: None`, keeping the legacy single-set plan
    /// byte-for-byte identical (issue #149, D12). With modifiers, keys are
    /// unioned by expression identity in first-appearance order and every
    /// item contributes a set list that is combined by cross product (D2).
    fn expand_group_by_items(
        &self,
        stmt: &Select,
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<ExpandedGroupBy, PlannerError> {
        let Some(items) = &stmt.group_by else {
            return Ok(ExpandedGroupBy {
                group_keys: Vec::new(),
                grouping_sets: None,
            });
        };

        if items
            .iter()
            .all(|item| matches!(item, GroupByItem::Expr { .. }))
        {
            // Legacy path: no dedup, no masks (D12).
            let mut keys = Vec::new();
            for item in items {
                if let GroupByItem::Expr { expr } = item {
                    keys.push(self.type_group_key_with_scope(expr, scope, ctes)?);
                }
            }
            return Ok(ExpandedGroupBy {
                group_keys: keys,
                grouping_sets: None,
            });
        }

        let mut keys: Vec<TypedExpr> = Vec::new();
        let mut key_index: HashMap<String, usize> = HashMap::new();
        let mut add_key = |planner: &Self, expr: &Expr| -> Result<usize, PlannerError> {
            let typed = planner.type_group_key_with_scope(expr, scope, ctes)?;
            let signature = expr_key(&typed);
            if let Some(&index) = key_index.get(&signature) {
                return Ok(index);
            }
            let index = keys.len();
            keys.push(typed);
            key_index.insert(signature, index);
            Ok(index)
        };

        // Cross product of per-item set lists (D2); each set is a list of
        // union-key indexes.
        let mut sets: Vec<Vec<usize>> = vec![Vec::new()];
        for item in items {
            let item_sets: Vec<Vec<usize>> = match item {
                GroupByItem::Expr { expr } => vec![vec![add_key(self, expr)?]],
                GroupByItem::Rollup { exprs } => {
                    if exprs.is_empty() {
                        return Err(PlannerError::invalid_expression(
                            "ROLLUP requires at least one expression".to_string(),
                        ));
                    }
                    let indexes = exprs
                        .iter()
                        .map(|expr| add_key(self, expr))
                        .collect::<Result<Vec<_>, _>>()?;
                    (0..=indexes.len())
                        .rev()
                        .map(|len| indexes[..len].to_vec())
                        .collect()
                }
                GroupByItem::Cube { exprs } => {
                    if exprs.is_empty() {
                        return Err(PlannerError::invalid_expression(
                            "CUBE requires at least one expression".to_string(),
                        ));
                    }
                    if exprs.len() > MAX_CUBE_COLUMNS {
                        return Err(PlannerError::invalid_expression(format!(
                            "too many grouping sets (max {MAX_GROUPING_SETS})"
                        )));
                    }
                    let indexes = exprs
                        .iter()
                        .map(|expr| add_key(self, expr))
                        .collect::<Result<Vec<_>, _>>()?;
                    let n = indexes.len();
                    (0..(1usize << n))
                        .rev()
                        .map(|included| {
                            indexes
                                .iter()
                                .enumerate()
                                .filter(|(position, _)| (included >> (n - 1 - position)) & 1 == 1)
                                .map(|(_, &index)| index)
                                .collect()
                        })
                        .collect()
                }
                GroupByItem::GroupingSets { sets: listed } => {
                    if listed.is_empty() {
                        return Err(PlannerError::invalid_expression(
                            "GROUPING SETS requires at least one grouping set".to_string(),
                        ));
                    }
                    listed
                        .iter()
                        .map(|set| {
                            set.iter()
                                .map(|expr| add_key(self, expr))
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                }
            };

            let mut combined = Vec::with_capacity(sets.len().saturating_mul(item_sets.len()));
            for base in &sets {
                for item_set in &item_sets {
                    if combined.len() >= MAX_GROUPING_SETS {
                        return Err(PlannerError::invalid_expression(format!(
                            "too many grouping sets (max {MAX_GROUPING_SETS})"
                        )));
                    }
                    let mut set = base.clone();
                    set.extend(item_set.iter().copied());
                    combined.push(set);
                }
            }
            sets = combined;
        }

        if keys.len() > MAX_GROUPING_KEYS {
            return Err(PlannerError::invalid_expression(format!(
                "too many grouping columns (max {MAX_GROUPING_KEYS})"
            )));
        }

        let key_count = keys.len();
        let full_mask = grouping_full_mask(key_count);
        let masks = sets
            .iter()
            .map(|set| {
                let mut mask = full_mask;
                for &index in set {
                    mask &= !(1u64 << (key_count - 1 - index));
                }
                mask
            })
            .collect();

        Ok(ExpandedGroupBy {
            group_keys: keys,
            grouping_sets: Some(masks),
        })
    }

    #[allow(dead_code)]
    fn build_projected_columns_for_aggregate(
        &self,
        items: &[SelectItem],
        table: &TableMetadata,
    ) -> Result<Vec<ProjectedColumn>, PlannerError> {
        let mut projected = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                    return Err(PlannerError::invalid_expression(
                        "wildcard projection not supported with GROUP BY/aggregate".to_string(),
                    ));
                }
                SelectItem::Expr { expr, alias, .. } => {
                    let typed = self.type_checker.infer_type(expr, table)?;
                    projected.push(ProjectedColumn {
                        expr: typed,
                        alias: alias.clone(),
                    });
                }
            }
        }
        Ok(projected)
    }

    fn build_projected_columns_for_aggregate_with_scope(
        &self,
        items: &[SelectItem],
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<Vec<ProjectedColumn>, PlannerError> {
        let mut projected = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                    return Err(PlannerError::invalid_expression(
                        "wildcard projection not supported with GROUP BY/aggregate".to_string(),
                    ));
                }
                SelectItem::Expr { expr, alias, .. } => {
                    let typed = self.infer_expr_with_scope(expr, scope, ctes)?;
                    projected.push(ProjectedColumn {
                        expr: typed,
                        alias: alias.clone(),
                    });
                }
            }
        }
        Ok(projected)
    }

    #[allow(dead_code)]
    fn build_projected_columns_for_distinct(
        &self,
        items: &[SelectItem],
        table: &TableMetadata,
    ) -> Result<Vec<ProjectedColumn>, PlannerError> {
        let projection = self.build_projection(items, table)?;
        match projection {
            Projection::All(columns) => {
                let mut projected = Vec::with_capacity(columns.len());
                for column in columns {
                    let column_index = table.get_column_index(&column).ok_or_else(|| {
                        PlannerError::invalid_expression(format!(
                            "column '{column}' not found for DISTINCT projection"
                        ))
                    })?;
                    let column_meta = table.get_column(&column).ok_or_else(|| {
                        PlannerError::invalid_expression(format!(
                            "column '{column}' not found for DISTINCT projection"
                        ))
                    })?;
                    let typed_expr = TypedExpr::column_ref(
                        table.name.clone(),
                        column.clone(),
                        column_index,
                        column_meta.data_type.clone(),
                        crate::ast::Span::default(),
                    );
                    projected.push(ProjectedColumn::new(typed_expr));
                }
                Ok(projected)
            }
            Projection::Columns(columns) => Ok(columns),
        }
    }

    fn build_projected_columns_for_distinct_with_scope(
        &self,
        items: &[SelectItem],
        schema: &[ColumnMetadata],
        scope: &[ScopedTable],
        ctes: &CtePlans,
    ) -> Result<Vec<ProjectedColumn>, PlannerError> {
        let projection = self.build_projection_with_scope(items, schema, scope, ctes)?;
        match projection {
            Projection::All(columns) => {
                let mut projected = Vec::with_capacity(columns.len());
                for (idx, column) in columns.into_iter().enumerate() {
                    let column_meta = schema.get(idx).ok_or_else(|| {
                        PlannerError::invalid_expression(format!(
                            "column '{column}' not found for DISTINCT projection"
                        ))
                    })?;
                    projected.push(ProjectedColumn::new(TypedExpr::column_ref(
                        LITERAL_TABLE.to_string(),
                        column,
                        idx,
                        column_meta.data_type.clone(),
                        crate::ast::Span::default(),
                    )));
                }
                Ok(projected)
            }
            Projection::Columns(columns) => Ok(columns),
        }
    }

    fn collect_aggregates_from_typed_expr(
        &self,
        expr: &TypedExpr,
        aggregates: &mut Vec<AggregateExpr>,
        aggregate_map: &mut HashMap<AggregateSignature, usize>,
    ) -> Result<(), PlannerError> {
        match &expr.kind {
            TypedExprKind::FunctionCall {
                name,
                args,
                distinct,
                star,
                filter,
                order_by,
                over: None,
            } if is_aggregate_function(name) => {
                if args.iter().any(typed_expr_contains_window) {
                    return Err(PlannerError::invalid_expression(
                        "aggregate functions cannot contain window functions".to_string(),
                    ));
                }
                for arg in args {
                    if typed_expr_contains_aggregate(arg) {
                        return Err(PlannerError::invalid_expression(
                            "nested aggregate functions are not supported".to_string(),
                        ));
                    }
                }
                // The type checker rejects aggregates and window functions in
                // FILTER / aggregate ORDER BY; keep a defensive re-check so a
                // future construction path cannot smuggle them through.
                if let Some(filter) = filter {
                    if typed_expr_contains_aggregate(filter) {
                        return Err(PlannerError::invalid_expression(
                            "aggregate functions are not allowed in FILTER".to_string(),
                        ));
                    }
                    if typed_expr_contains_window(filter) {
                        return Err(PlannerError::invalid_expression(
                            "window functions are not allowed in FILTER".to_string(),
                        ));
                    }
                }
                for sort in order_by {
                    if typed_expr_contains_aggregate(&sort.expr) {
                        return Err(PlannerError::invalid_expression(
                            "aggregate functions are not allowed in aggregate ORDER BY".to_string(),
                        ));
                    }
                    if typed_expr_contains_window(&sort.expr) {
                        return Err(PlannerError::invalid_expression(
                            "window functions are not allowed in aggregate ORDER BY".to_string(),
                        ));
                    }
                }
                let (agg, signature) = self.build_aggregate_expr_from_typed(
                    expr,
                    name,
                    args,
                    *distinct,
                    *star,
                    filter.as_deref(),
                    order_by,
                )?;
                aggregate_map.entry(signature).or_insert_with(|| {
                    aggregates.push(agg);
                    aggregates.len() - 1
                });
                Ok(())
            }
            TypedExprKind::BinaryOp { left, right, .. } => {
                self.collect_aggregates_from_typed_expr(left, aggregates, aggregate_map)?;
                self.collect_aggregates_from_typed_expr(right, aggregates, aggregate_map)?;
                Ok(())
            }
            TypedExprKind::UnaryOp { operand, .. } => {
                self.collect_aggregates_from_typed_expr(operand, aggregates, aggregate_map)
            }
            TypedExprKind::Cast { expr, .. } | TypedExprKind::TryCast { expr, .. } => {
                self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)
            }
            TypedExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    self.collect_aggregates_from_typed_expr(operand, aggregates, aggregate_map)?;
                }
                for branch in branches {
                    self.collect_aggregates_from_typed_expr(
                        &branch.when,
                        aggregates,
                        aggregate_map,
                    )?;
                    self.collect_aggregates_from_typed_expr(
                        &branch.then,
                        aggregates,
                        aggregate_map,
                    )?;
                }
                if let Some(else_expr) = else_expr {
                    self.collect_aggregates_from_typed_expr(else_expr, aggregates, aggregate_map)?;
                }
                Ok(())
            }
            TypedExprKind::FunctionCall { args, over, .. } => {
                for arg in args {
                    self.collect_aggregates_from_typed_expr(arg, aggregates, aggregate_map)?;
                }
                if let Some(window) = over {
                    for expr in &window.partition_by {
                        self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)?;
                    }
                    for sort in &window.order_by {
                        self.collect_aggregates_from_typed_expr(
                            &sort.expr,
                            aggregates,
                            aggregate_map,
                        )?;
                    }
                }
                Ok(())
            }
            TypedExprKind::Between {
                expr, low, high, ..
            } => {
                self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)?;
                self.collect_aggregates_from_typed_expr(low, aggregates, aggregate_map)?;
                self.collect_aggregates_from_typed_expr(high, aggregates, aggregate_map)?;
                Ok(())
            }
            TypedExprKind::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)?;
                self.collect_aggregates_from_typed_expr(pattern, aggregates, aggregate_map)?;
                if let Some(esc) = escape {
                    self.collect_aggregates_from_typed_expr(esc, aggregates, aggregate_map)?;
                }
                Ok(())
            }
            TypedExprKind::InList { expr, list, .. } => {
                self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)?;
                for item in list {
                    self.collect_aggregates_from_typed_expr(item, aggregates, aggregate_map)?;
                }
                Ok(())
            }
            TypedExprKind::IsNull { expr, .. } => {
                self.collect_aggregates_from_typed_expr(expr, aggregates, aggregate_map)
            }
            _ => Ok(()),
        }
    }

    fn collect_windows_from_typed_expr(
        &self,
        expr: &TypedExpr,
        windows: &mut Vec<WindowExpr>,
        window_map: &mut HashMap<String, usize>,
    ) -> Result<(), PlannerError> {
        match &expr.kind {
            TypedExprKind::FunctionCall {
                name,
                args,
                distinct,
                star,
                filter,
                order_by,
                over: Some(over),
            } => {
                if filter.is_some() || !order_by.is_empty() {
                    // The type checker rejects these combinations (D2); this
                    // guard keeps the window planner from silently ignoring a
                    // filter if a future path forgets that validation.
                    return Err(PlannerError::invalid_expression(
                        "FILTER and aggregate ORDER BY cannot be combined with OVER".to_string(),
                    ));
                }
                if args.iter().any(typed_expr_contains_window)
                    || over.partition_by.iter().any(typed_expr_contains_window)
                    || over
                        .order_by
                        .iter()
                        .any(|sort| typed_expr_contains_window(&sort.expr))
                {
                    return Err(PlannerError::invalid_expression(
                        "nested window functions are not supported".to_string(),
                    ));
                }

                let key = expr_key(expr);
                if window_map.contains_key(&key) {
                    return Ok(());
                }
                let function = match name.to_ascii_lowercase().as_str() {
                    "row_number" => WindowFunction::RowNumber,
                    "rank" => WindowFunction::Rank,
                    "dense_rank" => WindowFunction::DenseRank,
                    "percent_rank" => WindowFunction::PercentRank,
                    "cume_dist" => WindowFunction::CumeDist,
                    "ntile" => WindowFunction::Ntile(args[0].clone()),
                    "first_value" => {
                        WindowFunction::Value(ValueWindowFunction::FirstValue(args[0].clone()))
                    }
                    "last_value" => {
                        WindowFunction::Value(ValueWindowFunction::LastValue(args[0].clone()))
                    }
                    "nth_value" => WindowFunction::Value(ValueWindowFunction::NthValue {
                        value: args[0].clone(),
                        nth: args[1].clone(),
                    }),
                    "sum" | "count" | "avg" | "min" | "max" => {
                        let (aggregate, _) = self.build_aggregate_expr_from_typed(
                            expr,
                            name,
                            args,
                            *distinct,
                            *star,
                            None,
                            &[],
                        )?;
                        WindowFunction::Aggregate(aggregate)
                    }
                    "lag" | "lead" => {
                        let positional = build_offset_window_function(name, args)?;
                        if name.eq_ignore_ascii_case("lag") {
                            WindowFunction::Lag(positional)
                        } else {
                            WindowFunction::Lead(positional)
                        }
                    }
                    _ => {
                        return Err(PlannerError::unsupported_feature(
                            format!("function '{}' with OVER", name),
                            "future",
                            expr.span,
                        ));
                    }
                };
                let index = windows.len();
                windows.push(WindowExpr {
                    function,
                    partition_by: over.partition_by.clone(),
                    order_by: over.order_by.clone(),
                    frame: over.frame.clone(),
                    result_type: expr.resolved_type.clone(),
                });
                window_map.insert(key, index);
                Ok(())
            }
            TypedExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_windows_from_typed_expr(arg, windows, window_map)?;
                }
                Ok(())
            }
            TypedExprKind::BinaryOp { left, right, .. } => {
                self.collect_windows_from_typed_expr(left, windows, window_map)?;
                self.collect_windows_from_typed_expr(right, windows, window_map)
            }
            TypedExprKind::UnaryOp { operand, .. } => {
                self.collect_windows_from_typed_expr(operand, windows, window_map)
            }
            TypedExprKind::Cast { expr, .. }
            | TypedExprKind::TryCast { expr, .. }
            | TypedExprKind::IsNull { expr, .. } => {
                self.collect_windows_from_typed_expr(expr, windows, window_map)
            }
            TypedExprKind::Between {
                expr, low, high, ..
            } => {
                self.collect_windows_from_typed_expr(expr, windows, window_map)?;
                self.collect_windows_from_typed_expr(low, windows, window_map)?;
                self.collect_windows_from_typed_expr(high, windows, window_map)
            }
            TypedExprKind::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_windows_from_typed_expr(expr, windows, window_map)?;
                self.collect_windows_from_typed_expr(pattern, windows, window_map)?;
                if let Some(escape) = escape {
                    self.collect_windows_from_typed_expr(escape, windows, window_map)?;
                }
                Ok(())
            }
            TypedExprKind::InList { expr, list, .. } => {
                self.collect_windows_from_typed_expr(expr, windows, window_map)?;
                for item in list {
                    self.collect_windows_from_typed_expr(item, windows, window_map)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_aggregate_expr_from_typed(
        &self,
        expr: &TypedExpr,
        name: &str,
        args: &[TypedExpr],
        distinct: bool,
        star: bool,
        filter: Option<&TypedExpr>,
        order_by: &[SortExpr],
    ) -> Result<(AggregateExpr, AggregateSignature), PlannerError> {
        let lower = name.to_lowercase();
        let filter_owned = filter.cloned();
        // D3: order-insensitive aggregates validate their ORDER BY (names and
        // types) and then discard it — the result is order-independent, so
        // the sort cost is avoided and the signature matches the unordered
        // spelling. Order-sensitive aggregates keep the ordering.
        let retained_order_by: Vec<SortExpr> = if is_order_sensitive_aggregate(&lower) {
            order_by.to_vec()
        } else {
            Vec::new()
        };
        match lower.as_str() {
            "count" => {
                if star {
                    let mut agg = AggregateExpr::count_star();
                    agg.filter = filter_owned;
                    let signature = aggregate_signature(
                        name,
                        distinct,
                        star,
                        None,
                        None,
                        expr,
                        filter,
                        &retained_order_by,
                    );
                    return Ok((agg, signature));
                }
                if args.len() != 1 {
                    return Err(PlannerError::type_mismatch(
                        "1 argument",
                        format!("{} arguments", args.len()),
                        expr.span,
                    ));
                }
                let agg = AggregateExpr {
                    function: AggregateFunction::Count,
                    arg: Some(args[0].clone()),
                    distinct,
                    result_type: ResolvedType::BigInt,
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(&args[0]),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "sum" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Sum,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: crate::planner::aggregate_expr::sum_result_type(
                        &arg.resolved_type,
                    ),
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "total" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Total,
                    arg: Some(arg.clone()),
                    distinct: false,
                    result_type: ResolvedType::Double,
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    false,
                    star,
                    Some(arg),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "avg" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Avg,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: ResolvedType::Double,
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "min" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Min,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: arg.resolved_type.clone(),
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "max" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Max,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: arg.resolved_type.clone(),
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    None,
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "group_concat" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(PlannerError::type_mismatch(
                        "1 or 2 arguments",
                        format!("{} arguments", args.len()),
                        expr.span,
                    ));
                }
                let arg = &args[0];
                let mut separator = None;
                if args.len() == 2 {
                    if let TypedExprKind::Literal(Literal::String(value)) = &args[1].kind {
                        separator = Some(value.clone());
                    } else {
                        return Err(PlannerError::invalid_expression(
                            "GROUP_CONCAT separator must be a string literal".to_string(),
                        ));
                    }
                }
                let agg = AggregateExpr {
                    function: AggregateFunction::GroupConcat { separator },
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: ResolvedType::Text,
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    match &agg.function {
                        AggregateFunction::GroupConcat { separator } => separator.as_ref(),
                        _ => None,
                    },
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "string_agg" => {
                if args.len() != 2 {
                    return Err(PlannerError::type_mismatch(
                        "2 arguments",
                        format!("{} arguments", args.len()),
                        expr.span,
                    ));
                }
                let arg = &args[0];
                let separator =
                    if let TypedExprKind::Literal(Literal::String(value)) = &args[1].kind {
                        Some(value.clone())
                    } else {
                        return Err(PlannerError::invalid_expression(
                            "STRING_AGG separator must be a string literal".to_string(),
                        ));
                    };
                let agg = AggregateExpr {
                    function: AggregateFunction::StringAgg { separator },
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: ResolvedType::Text,
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                let signature = aggregate_signature(
                    name,
                    distinct,
                    star,
                    Some(arg),
                    match &agg.function {
                        AggregateFunction::StringAgg { separator } => separator.as_ref(),
                        _ => None,
                    },
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            "percentile_disc" => {
                if args.len() != 1 {
                    return Err(PlannerError::type_mismatch(
                        "1 argument",
                        format!("{} arguments", args.len()),
                        expr.span,
                    ));
                }
                let fraction = type_checker::percentile_fraction(&args[0])?;
                if retained_order_by.len() != 1 {
                    return Err(PlannerError::invalid_expression(
                        "PERCENTILE_DISC requires WITHIN GROUP (ORDER BY ...) with exactly \
                         one sort expression"
                            .to_string(),
                    ));
                }
                let sort = &retained_order_by[0];
                let agg = AggregateExpr {
                    function: AggregateFunction::PercentileDisc { fraction },
                    arg: Some(sort.expr.clone()),
                    distinct: false,
                    result_type: sort.expr.resolved_type.clone(),
                    filter: filter_owned,
                    order_by: retained_order_by.clone(),
                };
                // The fraction rides the separator slot; the sort value's
                // identity lives in the order key (see AggregateSignature).
                let fraction_key = format!("{fraction:?}");
                let signature = aggregate_signature(
                    name,
                    false,
                    star,
                    None,
                    Some(&fraction_key),
                    expr,
                    filter,
                    &retained_order_by,
                );
                Ok((agg, signature))
            }
            _ => Err(PlannerError::unsupported_feature(
                format!("function '{}'", name),
                "future",
                expr.span,
            )),
        }
    }
    fn require_single_aggregate_arg<'b>(
        &self,
        args: &'b [TypedExpr],
        span: crate::ast::Span,
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

    fn build_aggregate_projection(
        &self,
        projected: Vec<ProjectedColumn>,
        group_keys: &[TypedExpr],
        aggregates: &[AggregateExpr],
        output_names: &[String],
        grouping: Option<&GroupingRewrite>,
    ) -> Result<Projection, PlannerError> {
        let mut columns = Vec::new();
        for col in projected {
            let rewritten = self.rewrite_expr_for_aggregate(
                &col.expr,
                group_keys,
                aggregates,
                output_names,
                grouping,
            )?;
            columns.push(ProjectedColumn {
                expr: rewritten,
                alias: col.alias,
            });
        }
        Ok(Projection::Columns(columns))
    }

    /// Rewrite an aggregate-context expression onto the aggregate output.
    ///
    /// `output_names` must name the group keys and aggregates only, never a
    /// trailing `__grouping_id` column: `rewrite_expr_with_maps` derives the
    /// group-key count from `output_names.len() - aggregate_map.len()`.
    /// GROUPING/GROUPING_ID calls are lowered onto `__grouping_id` first via
    /// the `grouping` context (issue #149, D4/D5).
    fn rewrite_expr_for_aggregate(
        &self,
        expr: &TypedExpr,
        group_keys: &[TypedExpr],
        aggregates: &[AggregateExpr],
        output_names: &[String],
        grouping: Option<&GroupingRewrite>,
    ) -> Result<TypedExpr, PlannerError> {
        let group_key_map = build_group_key_map(group_keys);
        let aggregate_map = build_aggregate_map(aggregates);

        let expr = match grouping {
            Some(context) => rewrite_grouping_calls(expr, context)?,
            None => expr.clone(),
        };
        rewrite_expr_with_maps(&expr, &group_key_map, &aggregate_map, output_names)
    }

    /// Resolve a LIMIT/OFFSET/FETCH count expression to a concrete value.
    ///
    /// The expression must be a constant scalar of an integer type
    /// (issue #152, D4/D5): literals, arithmetic, CAST, CASE, and
    /// deterministic scalar functions over constants are accepted and
    /// const-folded at plan time; column references, subqueries, and
    /// aggregate/window functions are rejected. NULL means "no limit"
    /// (`LIMIT NULL` / `FETCH FIRST NULL ROWS`) or "no offset"
    /// (`OFFSET NULL`), matching PostgreSQL (D6). Negative values are
    /// rejected (D7).
    fn resolve_pagination_count(
        &self,
        expr: &Option<crate::ast::expr::Expr>,
        clause: &str,
    ) -> Result<Option<u64>, PlannerError> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        if expr_contains_subquery(expr) {
            return Err(PlannerError::unsupported_feature(
                format!("subquery in {clause}"),
                "a future version",
                expr.span,
            ));
        }
        // Empty scope: any column reference fails name resolution here.
        let typed = self
            .type_checker
            .infer_type_with_scope(expr, &[], &|stmt, _outer| {
                Err(PlannerError::unsupported_feature(
                    format!("subquery in {clause}"),
                    "a future version",
                    stmt.span(),
                ))
            })?;
        if typed_expr_contains_aggregate(&typed) {
            return Err(PlannerError::invalid_expression(format!(
                "aggregate functions are not allowed in {clause}"
            )));
        }
        if typed_expr_contains_window(&typed) {
            return Err(PlannerError::invalid_expression(format!(
                "window functions are not allowed in {clause}"
            )));
        }
        match typed.resolved_type {
            ResolvedType::Integer | ResolvedType::BigInt | ResolvedType::Null => {}
            ref other => {
                return Err(PlannerError::type_mismatch(
                    "BIGINT",
                    other.type_name().to_string(),
                    expr.span,
                ));
            }
        }
        // Constant-fold with an empty row context; the plan carries the
        // concrete value, so distributed execution never re-evaluates it.
        let context = crate::executor::evaluator::EvalContext::new(&[]);
        let value = crate::executor::evaluator::evaluate(&typed, &context).map_err(|error| {
            PlannerError::invalid_expression(format!("{clause} expression is invalid: {error}"))
        })?;
        let count = match value {
            crate::storage::SqlValue::Null => return Ok(None),
            crate::storage::SqlValue::Integer(value) => i64::from(value),
            crate::storage::SqlValue::BigInt(value) => value,
            other => {
                return Err(PlannerError::type_mismatch(
                    "BIGINT",
                    format!("{other:?}"),
                    expr.span,
                ));
            }
        };
        if count < 0 {
            return Err(PlannerError::invalid_expression(format!(
                "{clause} must not be negative"
            )));
        }
        Ok(Some(count as u64))
    }

    /// Apply the LIMIT/OFFSET/FETCH tail to a plan (issue #152).
    ///
    /// WITH TIES copies the sort keys from the `Sort` node directly beneath
    /// the Limit; without ORDER BY it is rejected (D3, PostgreSQL 42P20).
    fn apply_pagination(
        &self,
        plan: LogicalPlan,
        limit: &Option<Expr>,
        offset: &Option<Expr>,
        with_ties: bool,
    ) -> Result<LogicalPlan, PlannerError> {
        self.apply_pagination_with_tie_keys(plan, limit, offset, with_ties, None)
    }

    /// Apply the pagination tail with an explicit WITH TIES peer specification.
    ///
    /// `tie_keys` is `None` for every ordinary query block, where the peer
    /// specification is the `Sort` node directly beneath the Limit. The
    /// DISTINCT ON path plans no `Sort` node of its own (sql-distinct-on.md
    /// D8), so it supplies the user's ORDER BY explicitly (D13); an empty
    /// slice there means the query has no ORDER BY and WITH TIES is rejected.
    fn apply_pagination_with_tie_keys(
        &self,
        plan: LogicalPlan,
        limit: &Option<Expr>,
        offset: &Option<Expr>,
        with_ties: bool,
        tie_keys: Option<&[SortExpr]>,
    ) -> Result<LogicalPlan, PlannerError> {
        if limit.is_none() && offset.is_none() && !with_ties {
            return Ok(plan);
        }
        let ties = if with_ties {
            let keys = match tie_keys {
                Some(keys) => keys.to_vec(),
                None => match &plan {
                    LogicalPlan::Sort { order_by, .. } => order_by.clone(),
                    _ => Vec::new(),
                },
            };
            if keys.is_empty() {
                return Err(PlannerError::invalid_expression(
                    "FETCH ... WITH TIES requires ORDER BY".to_string(),
                ));
            }
            Some(keys)
        } else {
            None
        };
        Ok(LogicalPlan::Limit {
            input: Box::new(plan),
            limit: self.resolve_pagination_count(limit, "LIMIT")?,
            offset: self.resolve_pagination_count(offset, "OFFSET")?,
            ties,
        })
    }

    /// Plan an INSERT statement.
    ///
    /// Handles column list specification or implicit column ordering.
    /// When columns are omitted, uses table definition order from TableMetadata.
    fn plan_insert(&self, stmt: &Insert) -> Result<LogicalPlan, PlannerError> {
        // Resolve the target table
        let table = self.name_resolver.resolve_table(&stmt.table, stmt.span)?;

        // Determine the column list
        let columns: Vec<String> = if let Some(ref cols) = stmt.columns {
            // Explicit column list - validate each column exists
            for col in cols {
                self.name_resolver.resolve_column(table, col, stmt.span)?;
            }
            cols.clone()
        } else {
            // Implicit - use all columns in table definition order
            table.column_names().into_iter().map(String::from).collect()
        };

        match &stmt.source {
            InsertSource::Values { values } => {
                let mut typed_values: Vec<Vec<TypedExpr>> = Vec::new();

                for row in values {
                    if row.len() != columns.len() {
                        return Err(PlannerError::column_value_count_mismatch(
                            columns.len(),
                            row.len(),
                            stmt.span,
                        ));
                    }

                    typed_values.push(self.type_check_insert_values(row, &columns, table)?);
                }

                Ok(LogicalPlan::Insert {
                    table: table.name.clone(),
                    columns,
                    values: typed_values,
                })
            }
            InsertSource::Select { select } => {
                let source = self.plan_select_relation(select, &[], &CtePlans::new())?;
                self.finish_insert_query(stmt, table, columns, source)
            }
            InsertSource::Query { query } => {
                let source = self.plan_query_body_relation(query, &[], &CtePlans::new())?;
                self.finish_insert_query(stmt, table, columns, source)
            }
        }
    }

    fn finish_insert_query(
        &self,
        stmt: &Insert,
        table: &TableMetadata,
        columns: Vec<String>,
        source: PlannedRelation,
    ) -> Result<LogicalPlan, PlannerError> {
        if source.schema.len() != columns.len() {
            return Err(PlannerError::column_value_count_mismatch(
                columns.len(),
                source.schema.len(),
                stmt.span,
            ));
        }

        for (source_column, target_column) in source.schema.iter().zip(&columns) {
            let target = table
                .get_column(target_column)
                .expect("validated target column");
            if target.not_null && source_column.data_type == ResolvedType::Null {
                return Err(PlannerError::null_constraint_violation(
                    target_column,
                    stmt.span,
                ));
            }
            self.validate_resolved_type_assignment(
                &source_column.data_type,
                &target.data_type,
                stmt.span,
            )?;
        }

        Ok(LogicalPlan::InsertSelect {
            table: table.name.clone(),
            columns,
            source: Box::new(source.plan),
        })
    }

    /// Type-check INSERT values against column definitions.
    fn type_check_insert_values(
        &self,
        values: &[crate::ast::expr::Expr],
        columns: &[String],
        table: &TableMetadata,
    ) -> Result<Vec<TypedExpr>, PlannerError> {
        let mut typed_values = Vec::new();

        for (i, value) in values.iter().enumerate() {
            let column_name = &columns[i];
            let column_meta = table.get_column(column_name).ok_or_else(|| {
                PlannerError::column_not_found(column_name, &table.name, value.span)
            })?;

            // Type-check the value expression
            let typed_value = self.type_checker.infer_type(value, table)?;

            // Check for NOT NULL constraint violation (except for NULL literal which is allowed if nullable)
            if column_meta.not_null
                && matches!(&typed_value.kind, TypedExprKind::Literal(Literal::Null))
            {
                return Err(PlannerError::null_constraint_violation(
                    column_name,
                    value.span,
                ));
            }

            // Validate type compatibility
            self.validate_type_assignment(&typed_value, &column_meta.data_type, value.span)?;

            let typed_value =
                self.coerce_assignment_value(typed_value, &column_meta.data_type, value.span);

            typed_values.push(typed_value);
        }

        Ok(typed_values)
    }

    /// Validate that a value type can be assigned to a column type.
    fn validate_type_assignment(
        &self,
        value: &TypedExpr,
        target_type: &ResolvedType,
        span: crate::ast::Span,
    ) -> Result<(), PlannerError> {
        self.validate_resolved_type_assignment(&value.resolved_type, target_type, span)
    }

    fn validate_resolved_type_assignment(
        &self,
        source_type: &ResolvedType,
        target_type: &ResolvedType,
        span: crate::ast::Span,
    ) -> Result<(), PlannerError> {
        // NULL can be assigned to any nullable column
        if *source_type == ResolvedType::Null {
            return Ok(());
        }

        // Check for exact match or implicit conversion compatibility
        if self.types_compatible(source_type, target_type) {
            return Ok(());
        }

        Err(PlannerError::type_mismatch(
            target_type.to_string(),
            source_type.to_string(),
            span,
        ))
    }

    /// Check if two types are compatible for assignment.
    fn types_compatible(&self, source: &ResolvedType, target: &ResolvedType) -> bool {
        use ResolvedType::*;

        // Same type is always compatible
        if source == target {
            return true;
        }

        // Numeric promotions
        match (source, target) {
            // Integer can be assigned to BigInt, Float, Double
            (Integer, BigInt) | (Integer, Float) | (Integer, Double) => true,
            // BigInt can be assigned to Float, Double
            (BigInt, Float) | (BigInt, Double) => true,
            // Float can be assigned to Double
            (Float, Double) => true,
            // A decimal literal is typed DOUBLE, so a FLOAT column needs this
            // narrowing; the value is rounded to f32 at execution time.
            (Double, Float) => true,
            // TIMESTAMP is stored as microseconds; text and numeric input is
            // converted by the assignment expression at execution time.
            (Text | Integer | BigInt | Float | Double, Timestamp) => true,
            // Vector dimensions must match
            (Vector { dimension: d1, .. }, Vector { dimension: d2, .. }) => d1 == d2,
            _ => false,
        }
    }

    fn coerce_assignment_value(
        &self,
        value: TypedExpr,
        target_type: &ResolvedType,
        span: crate::ast::Span,
    ) -> TypedExpr {
        if value.resolved_type != *target_type
            && value.resolved_type != ResolvedType::Null
            && matches!(
                target_type,
                ResolvedType::Integer
                    | ResolvedType::BigInt
                    | ResolvedType::Float
                    | ResolvedType::Double
                    | ResolvedType::Timestamp
            )
        {
            TypedExpr::cast(value, target_type.clone(), span)
        } else {
            value
        }
    }

    /// Plan an UPDATE statement.
    ///
    /// Validates assignments and optional WHERE clause.
    fn plan_update(&self, stmt: &Update) -> Result<LogicalPlan, PlannerError> {
        // Resolve the target table
        let table = self.name_resolver.resolve_table(&stmt.table, stmt.span)?;

        // Process assignments
        let mut typed_assignments = Vec::new();

        for assignment in &stmt.assignments {
            // Resolve the column
            let column_meta =
                self.name_resolver
                    .resolve_column(table, &assignment.column, assignment.span)?;
            let column_index = table.get_column_index(&assignment.column).unwrap();

            // Type-check the value expression
            let typed_value = self.type_checker.infer_type(&assignment.value, table)?;

            // Check NOT NULL constraint
            if column_meta.not_null
                && matches!(&typed_value.kind, TypedExprKind::Literal(Literal::Null))
            {
                return Err(PlannerError::null_constraint_violation(
                    &assignment.column,
                    assignment.value.span,
                ));
            }

            // Validate type compatibility
            self.validate_type_assignment(
                &typed_value,
                &column_meta.data_type,
                assignment.value.span,
            )?;

            let typed_value = self.coerce_assignment_value(
                typed_value,
                &column_meta.data_type,
                assignment.value.span,
            );

            typed_assignments.push(TypedAssignment::new(
                assignment.column.clone(),
                column_index,
                typed_value,
            ));
        }

        // Process optional WHERE clause
        let filter = if let Some(ref selection) = stmt.selection {
            let predicate = self.type_checker.infer_type(selection, table)?;

            // Verify predicate returns Boolean
            if predicate.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    predicate.resolved_type.to_string(),
                    selection.span,
                ));
            }

            Some(predicate)
        } else {
            None
        };

        Ok(LogicalPlan::Update {
            table: table.name.clone(),
            assignments: typed_assignments,
            filter,
        })
    }

    /// Plan a DELETE statement.
    ///
    /// Validates optional WHERE clause.
    fn plan_delete(&self, stmt: &Delete) -> Result<LogicalPlan, PlannerError> {
        // Resolve the target table
        let table = self.name_resolver.resolve_table(&stmt.table, stmt.span)?;

        // Process optional WHERE clause
        let filter = if let Some(ref selection) = stmt.selection {
            let predicate = self.type_checker.infer_type(selection, table)?;

            // Verify predicate returns Boolean
            if predicate.resolved_type != ResolvedType::Boolean {
                return Err(PlannerError::type_mismatch(
                    "Boolean",
                    predicate.resolved_type.to_string(),
                    selection.span,
                ));
            }

            Some(predicate)
        } else {
            None
        };

        Ok(LogicalPlan::Delete {
            table: table.name.clone(),
            filter,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateSignature {
    name: String,
    distinct: bool,
    star: bool,
    arg_key: Option<String>,
    separator: Option<String>,
    /// FILTER (WHERE ...) identity: aggregates that differ only in their
    /// filter are distinct physical aggregates (issue #148, D10).
    filter_key: Option<String>,
    /// Aggregate-local ordering identity; populated only for order-sensitive
    /// aggregates so a discarded ORDER BY (D3) still deduplicates with the
    /// unordered spelling.
    order_key: Option<String>,
}

/// Collect the SELECT-list aliases that ORDER BY / HAVING may reference.
///
/// Per the SQL standard, aliases introduced by the projection are visible to
/// HAVING and ORDER BY (which are logically evaluated after the projection),
/// but not to WHERE / GROUP BY. Only `SelectItem::Expr` carries an alias;
/// wildcards contribute nothing.
///
/// When the same alias is declared twice the first declaration wins, which
/// keeps the substitution deterministic instead of depending on map ordering.
fn collect_projection_aliases(items: &[SelectItem]) -> HashMap<String, crate::ast::expr::Expr> {
    let mut aliases = HashMap::new();
    for item in items {
        if let SelectItem::Expr {
            expr,
            alias: Some(alias),
            ..
        } = item
        {
            aliases.entry(alias.clone()).or_insert_with(|| expr.clone());
        }
    }
    aliases
}

/// Substitute projection aliases inside an ORDER BY / HAVING expression.
///
/// An unqualified `ColumnRef` whose name matches a projection alias is replaced
/// by the aliased source expression, so everything downstream (type inference,
/// aggregate collection, `validate_having_expr`, and the aggregate output
/// rewrite) observes the very expression the projection already produced.
///
/// Substitution rules:
/// - Only unqualified references are eligible; `t.total` always means the base
///   column `total` of table `t`, never an alias.
/// - An alias takes precedence over a base column of the same name, per the
///   SQL standard. `order_by_prefers_projection_alias_over_shadowed_base_column`
///   pins this behaviour.
/// - The substituted expression keeps the *reference* site's span so that any
///   resulting diagnostic still points at the ORDER BY / HAVING clause.
/// - Subqueries are not descended into: an inner SELECT establishes its own
///   projection scope, so the outer alias must not leak inside it.
fn substitute_projection_aliases(
    expr: &crate::ast::expr::Expr,
    aliases: &HashMap<String, crate::ast::expr::Expr>,
) -> crate::ast::expr::Expr {
    use crate::ast::expr::ExprKind;

    if aliases.is_empty() {
        return expr.clone();
    }

    let recurse = |e: &crate::ast::expr::Expr| substitute_projection_aliases(e, aliases);

    let kind = match &expr.kind {
        ExprKind::ColumnRef {
            table: None,
            column,
        } => match aliases.get(column) {
            Some(source) => {
                let mut replacement = source.clone();
                replacement.span = expr.span;
                return replacement;
            }
            None => return expr.clone(),
        },
        ExprKind::BinaryOp { left, op, right } => ExprKind::BinaryOp {
            left: Box::new(recurse(left)),
            op: *op,
            right: Box::new(recurse(right)),
        },
        ExprKind::UnaryOp { op, operand } => ExprKind::UnaryOp {
            op: *op,
            operand: Box::new(recurse(operand)),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
            order_by,
            within_group,
            filter,
            over,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(recurse).collect(),
            distinct: *distinct,
            star: *star,
            order_by: order_by
                .iter()
                .map(|order| OrderByExpr {
                    expr: recurse(&order.expr),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                    span: order.span,
                })
                .collect(),
            within_group: within_group
                .iter()
                .map(|order| OrderByExpr {
                    expr: recurse(&order.expr),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                    span: order.span,
                })
                .collect(),
            filter: filter
                .as_deref()
                .map(|predicate| Box::new(recurse(predicate))),
            over: over.as_ref().map(|window| crate::ast::expr::WindowSpec {
                base: window.base.clone(),
                partition_by: window.partition_by.iter().map(recurse).collect(),
                order_by: window
                    .order_by
                    .iter()
                    .map(|order| OrderByExpr {
                        expr: recurse(&order.expr),
                        asc: order.asc,
                        nulls_first: order.nulls_first,
                        span: order.span,
                    })
                    .collect(),
                frame: window.frame.clone(),
            }),
        },
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => ExprKind::Case {
            operand: operand.as_deref().map(|e| Box::new(recurse(e))),
            branches: branches
                .iter()
                .map(|branch| crate::ast::expr::CaseWhen {
                    when: recurse(&branch.when),
                    then: recurse(&branch.then),
                })
                .collect(),
            else_expr: else_expr.as_deref().map(|e| Box::new(recurse(e))),
        },
        ExprKind::Cast { expr, target_type } => ExprKind::Cast {
            expr: Box::new(recurse(expr)),
            target_type: target_type.clone(),
        },
        ExprKind::TryCast { expr, target_type } => ExprKind::TryCast {
            expr: Box::new(recurse(expr)),
            target_type: target_type.clone(),
        },
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(recurse(expr)),
            low: Box::new(recurse(low)),
            high: Box::new(recurse(high)),
            negated: *negated,
        },
        ExprKind::Like {
            expr,
            pattern,
            escape,
            negated,
            kind,
        } => ExprKind::Like {
            expr: Box::new(recurse(expr)),
            pattern: Box::new(recurse(pattern)),
            escape: escape.as_deref().map(|e| Box::new(recurse(e))),
            negated: *negated,
            kind: *kind,
        },
        ExprKind::InList {
            expr,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(recurse(expr)),
            list: list.iter().map(recurse).collect(),
            negated: *negated,
        },
        ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
            expr: Box::new(recurse(expr)),
            negated: *negated,
        },
        ExprKind::TruthPredicate {
            expr,
            value,
            negated,
        } => ExprKind::TruthPredicate {
            expr: Box::new(recurse(expr)),
            value: *value,
            negated: *negated,
        },
        ExprKind::IsDistinctFrom {
            left,
            right,
            negated,
        } => ExprKind::IsDistinctFrom {
            left: Box::new(recurse(left)),
            right: Box::new(recurse(right)),
            negated: *negated,
        },
        ExprKind::Row { items } => ExprKind::Row {
            items: items.iter().map(recurse).collect(),
        },
        // Qualified column refs, literals, and subquery-bearing expressions are
        // left untouched (see the subquery note above).
        ExprKind::ColumnRef { .. }
        | ExprKind::Literal { .. }
        | ExprKind::VectorLiteral { .. }
        | ExprKind::ScalarSubquery { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::Exists { .. }
        | ExprKind::Quantified { .. } => return expr.clone(),
    };

    crate::ast::expr::Expr {
        kind,
        span: expr.span,
    }
}

fn build_offset_window_function(
    name: &str,
    args: &[TypedExpr],
) -> Result<OffsetWindowFunction, PlannerError> {
    let value = args.first().cloned().ok_or_else(|| {
        PlannerError::invalid_expression(format!(
            "{}() window function expects 1 to 3 arguments",
            name.to_ascii_uppercase()
        ))
    })?;
    if args.len() > 3 {
        return Err(PlannerError::invalid_expression(format!(
            "{}() window function expects 1 to 3 arguments",
            name.to_ascii_uppercase()
        )));
    }
    Ok(OffsetWindowFunction {
        value,
        offset: args.get(1).cloned(),
        default: args.get(2).cloned(),
    })
}

fn expr_contains_aggregate(expr: &crate::ast::expr::Expr) -> bool {
    use crate::ast::expr::ExprKind;

    match &expr.kind {
        ExprKind::FunctionCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            if over.is_none() && is_aggregate_function(name) {
                return true;
            }
            args.iter().any(expr_contains_aggregate)
                || order_by
                    .iter()
                    .any(|sort| expr_contains_aggregate(&sort.expr))
                || within_group
                    .iter()
                    .any(|sort| expr_contains_aggregate(&sort.expr))
                || filter.as_deref().is_some_and(expr_contains_aggregate)
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(expr_contains_aggregate)
                        || window
                            .order_by
                            .iter()
                            .any(|sort| expr_contains_aggregate(&sort.expr))
                })
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        ExprKind::UnaryOp { operand, .. } => expr_contains_aggregate(operand),
        ExprKind::TruthPredicate { expr, .. } => expr_contains_aggregate(expr),
        ExprKind::IsDistinctFrom { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        ExprKind::Row { items } => items.iter().any(expr_contains_aggregate),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_aggregate)
                || branches.iter().any(|branch| {
                    expr_contains_aggregate(&branch.when) || expr_contains_aggregate(&branch.then)
                })
                || else_expr.as_deref().is_some_and(expr_contains_aggregate)
        }
        ExprKind::Cast { expr, .. } | ExprKind::TryCast { expr, .. } => {
            expr_contains_aggregate(expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(pattern)
                || escape.as_deref().is_some_and(expr_contains_aggregate)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        ExprKind::IsNull { expr, .. } => expr_contains_aggregate(expr),
        ExprKind::ScalarSubquery { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::Exists { .. }
        | ExprKind::Quantified { .. }
        | ExprKind::Literal { .. }
        | ExprKind::VectorLiteral { .. }
        | ExprKind::ColumnRef { .. } => false,
    }
}

fn typed_expr_contains_aggregate(expr: &TypedExpr) -> bool {
    match &expr.kind {
        TypedExprKind::FunctionCall {
            name,
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            if over.is_none() && is_aggregate_function(name) {
                return true;
            }
            args.iter().any(typed_expr_contains_aggregate)
                || filter.as_deref().is_some_and(typed_expr_contains_aggregate)
                || order_by
                    .iter()
                    .any(|sort| typed_expr_contains_aggregate(&sort.expr))
                || over.as_ref().is_some_and(|window| {
                    window
                        .partition_by
                        .iter()
                        .any(typed_expr_contains_aggregate)
                        || window
                            .order_by
                            .iter()
                            .any(|sort| typed_expr_contains_aggregate(&sort.expr))
                })
        }
        TypedExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_aggregate(left) || typed_expr_contains_aggregate(right)
        }
        TypedExprKind::UnaryOp { operand, .. } => typed_expr_contains_aggregate(operand),
        TypedExprKind::Cast { expr, .. } | TypedExprKind::TryCast { expr, .. } => {
            typed_expr_contains_aggregate(expr)
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(typed_expr_contains_aggregate)
                || branches.iter().any(|branch| {
                    typed_expr_contains_aggregate(&branch.when)
                        || typed_expr_contains_aggregate(&branch.then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(typed_expr_contains_aggregate)
        }
        TypedExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_aggregate(expr)
                || typed_expr_contains_aggregate(low)
                || typed_expr_contains_aggregate(high)
        }
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            typed_expr_contains_aggregate(expr)
                || typed_expr_contains_aggregate(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|inner| typed_expr_contains_aggregate(inner))
        }
        TypedExprKind::InList { expr, list, .. } => {
            typed_expr_contains_aggregate(expr) || list.iter().any(typed_expr_contains_aggregate)
        }
        TypedExprKind::IsNull { expr, .. } => typed_expr_contains_aggregate(expr),
        TypedExprKind::InSubquery { expr, .. } => typed_expr_contains_aggregate(expr),
        TypedExprKind::Quantified { expr, .. } => typed_expr_contains_aggregate(expr),
        TypedExprKind::ScalarSubquery(_) | TypedExprKind::Exists { .. } => false,
        _ => false,
    }
}

fn select_contains_window(stmt: &Select) -> bool {
    stmt.projection.iter().any(|item| match item {
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
        SelectItem::Expr { expr, .. } => expr_contains_window(expr),
    }) || stmt.qualify.as_ref().is_some_and(expr_contains_window)
        || stmt
            .order_by
            .iter()
            .any(|order| expr_contains_window(&order.expr))
}

fn expr_contains_window(expr: &crate::ast::expr::Expr) -> bool {
    match &expr.kind {
        crate::ast::expr::ExprKind::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            over.is_some()
                || args.iter().any(expr_contains_window)
                || order_by.iter().any(|sort| expr_contains_window(&sort.expr))
                || within_group
                    .iter()
                    .any(|sort| expr_contains_window(&sort.expr))
                || filter.as_deref().is_some_and(expr_contains_window)
        }
        crate::ast::expr::ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        crate::ast::expr::ExprKind::UnaryOp { operand, .. }
        | crate::ast::expr::ExprKind::Cast { expr: operand, .. }
        | crate::ast::expr::ExprKind::TryCast { expr: operand, .. }
        | crate::ast::expr::ExprKind::IsNull { expr: operand, .. } => expr_contains_window(operand),
        crate::ast::expr::ExprKind::Between {
            expr, low, high, ..
        } => expr_contains_window(expr) || expr_contains_window(low) || expr_contains_window(high),
        crate::ast::expr::ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_window(expr)
                || expr_contains_window(pattern)
                || escape.as_deref().is_some_and(expr_contains_window)
        }
        crate::ast::expr::ExprKind::InList { expr, list, .. } => {
            expr_contains_window(expr) || list.iter().any(expr_contains_window)
        }
        _ => false,
    }
}

fn typed_expr_contains_window(expr: &TypedExpr) -> bool {
    match &expr.kind {
        TypedExprKind::FunctionCall {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            over.is_some()
                || args.iter().any(typed_expr_contains_window)
                || filter.as_deref().is_some_and(typed_expr_contains_window)
                || order_by
                    .iter()
                    .any(|sort| typed_expr_contains_window(&sort.expr))
        }
        TypedExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_window(left) || typed_expr_contains_window(right)
        }
        TypedExprKind::UnaryOp { operand, .. }
        | TypedExprKind::Cast { expr: operand, .. }
        | TypedExprKind::TryCast { expr: operand, .. }
        | TypedExprKind::IsNull { expr: operand, .. } => typed_expr_contains_window(operand),
        TypedExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_window(expr)
                || typed_expr_contains_window(low)
                || typed_expr_contains_window(high)
        }
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            typed_expr_contains_window(expr)
                || typed_expr_contains_window(pattern)
                || escape.as_deref().is_some_and(typed_expr_contains_window)
        }
        TypedExprKind::InList { expr, list, .. } => {
            typed_expr_contains_window(expr) || list.iter().any(typed_expr_contains_window)
        }
        _ => false,
    }
}

fn rewrite_projection_for_windows(
    projection: &Projection,
    window_map: &HashMap<String, usize>,
    base_width: usize,
    window_names: &[String],
) -> Result<Projection, PlannerError> {
    match projection {
        Projection::All(names) => Ok(Projection::All(names.clone())),
        Projection::Columns(columns) => Ok(Projection::Columns(
            columns
                .iter()
                .map(|column| {
                    Ok(ProjectedColumn {
                        expr: rewrite_expr_for_windows(
                            &column.expr,
                            window_map,
                            base_width,
                            window_names,
                        )?,
                        alias: column.alias.clone(),
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?,
        )),
    }
}

fn rewrite_expr_for_windows(
    expr: &TypedExpr,
    window_map: &HashMap<String, usize>,
    base_width: usize,
    window_names: &[String],
) -> Result<TypedExpr, PlannerError> {
    if let Some(index) = window_map.get(&expr_key(expr)) {
        return Ok(TypedExpr::column_ref(
            "__window__".to_string(),
            window_names
                .get(*index)
                .cloned()
                .unwrap_or_else(|| format!("__window_{index}")),
            base_width + index,
            expr.resolved_type.clone(),
            expr.span,
        ));
    }

    let rewrite =
        |inner: &TypedExpr| rewrite_expr_for_windows(inner, window_map, base_width, window_names);
    let kind = match &expr.kind {
        TypedExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
            filter,
            order_by,
            over,
        } => {
            if over.is_some() {
                return Err(PlannerError::invalid_expression(
                    "window expression is not part of the window plan".to_string(),
                ));
            }
            TypedExprKind::FunctionCall {
                name: name.clone(),
                args: args.iter().map(rewrite).collect::<Result<Vec<_>, _>>()?,
                distinct: *distinct,
                star: *star,
                filter: filter.as_deref().map(rewrite).transpose()?.map(Box::new),
                order_by: order_by
                    .iter()
                    .map(|sort| {
                        Ok(SortExpr::new(
                            rewrite(&sort.expr)?,
                            sort.asc,
                            sort.nulls_first,
                        ))
                    })
                    .collect::<Result<Vec<_>, PlannerError>>()?,
                over: None,
            }
        }
        TypedExprKind::BinaryOp { left, op, right } => TypedExprKind::BinaryOp {
            left: Box::new(rewrite(left)?),
            op: *op,
            right: Box::new(rewrite(right)?),
        },
        TypedExprKind::UnaryOp { op, operand } => TypedExprKind::UnaryOp {
            op: *op,
            operand: Box::new(rewrite(operand)?),
        },
        TypedExprKind::Cast {
            expr: inner,
            target_type,
        } => TypedExprKind::Cast {
            expr: Box::new(rewrite(inner)?),
            target_type: target_type.clone(),
        },
        TypedExprKind::TryCast {
            expr: inner,
            target_type,
        } => TypedExprKind::TryCast {
            expr: Box::new(rewrite(inner)?),
            target_type: target_type.clone(),
        },
        TypedExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExprKind::Between {
            expr: Box::new(rewrite(inner)?),
            low: Box::new(rewrite(low)?),
            high: Box::new(rewrite(high)?),
            negated: *negated,
        },
        TypedExprKind::Like {
            expr: inner,
            pattern,
            escape,
            negated,
            kind,
        } => TypedExprKind::Like {
            expr: Box::new(rewrite(inner)?),
            pattern: Box::new(rewrite(pattern)?),
            escape: escape.as_deref().map(rewrite).transpose()?.map(Box::new),
            negated: *negated,
            kind: *kind,
        },
        TypedExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExprKind::InList {
            expr: Box::new(rewrite(inner)?),
            list: list.iter().map(rewrite).collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        TypedExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExprKind::IsNull {
            expr: Box::new(rewrite(inner)?),
            negated: *negated,
        },
        _ => return Ok(expr.clone()),
    };
    Ok(TypedExpr {
        kind,
        resolved_type: expr.resolved_type.clone(),
        span: expr.span,
    })
}

/// Rebind an outer ORDER BY expression to the visible projection schema.
///
/// Projection aliases have already been substituted before type inference, so
/// expression identity is enough to map both aliases and repeated expressions
/// without making aliases visible to WHERE/GROUP BY/window specifications.
fn rewrite_expr_for_projected_output(
    expr: &TypedExpr,
    projection: &Projection,
    output_schema: &[ColumnMetadata],
) -> Result<TypedExpr, PlannerError> {
    let index = match projection {
        Projection::Columns(columns) => columns
            .iter()
            .position(|column| expr_key(&column.expr) == expr_key(expr)),
        Projection::All(_) => match &expr.kind {
            TypedExprKind::ColumnRef { column_index, .. }
                if *column_index < output_schema.len() =>
            {
                Some(*column_index)
            }
            _ => None,
        },
    };
    let Some(index) = index else {
        return Err(PlannerError::invalid_expression(
            "ORDER BY expression must appear in the SELECT projection for window queries"
                .to_string(),
        ));
    };
    let column = output_schema.get(index).ok_or_else(|| {
        PlannerError::invalid_expression(
            "ORDER BY projection index is outside the output schema".to_string(),
        )
    })?;
    Ok(TypedExpr::column_ref(
        "__project__".to_string(),
        column.name.clone(),
        index,
        column.data_type.clone(),
        expr.span,
    ))
}

fn map_join_type(join_type: crate::ast::dml::JoinType) -> JoinType {
    match join_type {
        crate::ast::dml::JoinType::Inner => JoinType::Inner,
        crate::ast::dml::JoinType::Left => JoinType::Left,
        crate::ast::dml::JoinType::Right => JoinType::Right,
        crate::ast::dml::JoinType::Full => JoinType::Full,
        crate::ast::dml::JoinType::Cross => JoinType::Cross,
    }
}

struct FoundScopedColumn {
    table: String,
    index: usize,
    ty: ResolvedType,
    partner_indices: Vec<usize>,
}

fn find_scoped_column(
    scope: &[ScopedTable],
    column: &str,
    span: crate::ast::Span,
) -> Result<FoundScopedColumn, PlannerError> {
    let mut matches = Vec::new();
    for table in scope {
        if table.hidden_unqualified_columns.contains(column) {
            continue;
        }
        if let Some(local_idx) = table.table.get_column_index(column) {
            let meta = &table.table.columns[local_idx];
            matches.push(FoundScopedColumn {
                table: table.table.name.clone(),
                index: table.start_index + local_idx,
                ty: meta.data_type.clone(),
                partner_indices: table
                    .merged_column_partners
                    .get(column)
                    .cloned()
                    .unwrap_or_default(),
            });
        }
    }
    match matches.len() {
        0 => Err(PlannerError::column_not_found(column, "JOIN input", span)),
        1 => Ok(matches.remove(0)),
        _ => Err(PlannerError::ambiguous_column(
            column,
            scope.iter().map(|s| s.table.name.clone()).collect(),
            span,
        )),
    }
}

fn merged_scoped_column_expr(
    found: &FoundScopedColumn,
    column: &str,
    span: crate::ast::Span,
) -> TypedExpr {
    let own = TypedExpr::column_ref(
        found.table.clone(),
        column.to_string(),
        found.index,
        found.ty.clone(),
        span,
    );
    if found.partner_indices.is_empty() {
        return own;
    }

    let mut args = Vec::with_capacity(found.partner_indices.len() + 1);
    args.push(own);
    args.extend(found.partner_indices.iter().map(|&index| {
        TypedExpr::column_ref(
            found.table.clone(),
            column.to_string(),
            index,
            found.ty.clone(),
            span,
        )
    }));
    TypedExpr {
        kind: TypedExprKind::FunctionCall {
            name: "coalesce".to_string(),
            args,
            distinct: false,
            star: false,
            filter: None,
            order_by: Vec::new(),
            over: None,
        },
        resolved_type: found.ty.clone(),
        span,
    }
}

fn projection_schema(
    projection: &Projection,
    input_schema: &[ColumnMetadata],
) -> Vec<ColumnMetadata> {
    match projection {
        Projection::All(names) => names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let ty = (names.len() == input_schema.len())
                    .then(|| input_schema.get(idx))
                    .flatten()
                    .or_else(|| input_schema.iter().find(|col| &col.name == name))
                    .map(|col| col.data_type.clone())
                    .unwrap_or(ResolvedType::Null);
                ColumnMetadata::new(name.clone(), ty)
            })
            .collect(),
        Projection::Columns(columns) => columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let name = col
                    .alias
                    .clone()
                    .or_else(|| match &col.expr.kind {
                        TypedExprKind::ColumnRef { column, .. } => Some(column.clone()),
                        // A USING/NATURAL common column is planned as
                        // COALESCE(left, right); it still names the merged
                        // column, not an anonymous expression.
                        TypedExprKind::FunctionCall { name, args, .. }
                            if name == "coalesce" && !args.is_empty() =>
                        {
                            let first_column = match &args[0].kind {
                                TypedExprKind::ColumnRef { column, .. } => Some(column),
                                _ => None,
                            };
                            first_column
                                .filter(|column| {
                                    args.iter().all(|arg| {
                                        matches!(
                                            &arg.kind,
                                            TypedExprKind::ColumnRef { column: other, .. }
                                                if other == *column
                                        )
                                    })
                                })
                                .cloned()
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| format!("col_{idx}"));
                ColumnMetadata::new(name, col.expr.resolved_type.clone())
            })
            .collect(),
    }
}

fn visible_wildcard_columns(schema: &[ColumnMetadata], scope: &[ScopedTable]) -> Vec<String> {
    schema
        .iter()
        .enumerate()
        .filter(|(index, column)| {
            !scope.iter().any(|table| {
                *index >= table.start_index
                    && *index < table.start_index + table.table.columns.len()
                    && table.hidden_unqualified_columns.contains(&column.name)
            })
        })
        .map(|(_, column)| column.name.clone())
        .collect()
}

/// Output schema and name scope of a join over `left` and `right`.
///
/// Shared by the plain and LATERAL join builders so both expose the same
/// USING/NATURAL column merging.
fn combine_join_shape(
    left: &PlannedRelation,
    right: &PlannedRelation,
    using: Option<&[String]>,
) -> (Vec<ColumnMetadata>, Vec<ScopedTable>) {
    let mut schema = left.schema.clone();
    schema.extend(right.schema.clone());
    let mut scope = left.scope.clone();
    let mut right_scope = right.scope.clone();
    if let Some(columns) = using {
        // The right-hand copy of a common column stops being an unqualified
        // candidate, and the surviving left-hand column records where its
        // partner lives so that an unqualified reference can merge the two.
        for column in columns {
            let right_index = right_scope.iter().find_map(|table| {
                table
                    .table
                    .get_column_index(column)
                    .map(|index| table.start_index + index)
            });
            let Some(right_index) = right_index else {
                continue;
            };
            for table in &mut scope {
                if table.table.get_column_index(column).is_some() {
                    table.merge_column_with(column, right_index);
                }
            }
        }
        for table in &mut right_scope {
            table.hide_unqualified_columns(columns);
        }
    }
    scope.extend(right_scope);
    (schema, scope)
}

/// Whether this FROM item is evaluated once per row of everything to its left.
///
/// An explicit `LATERAL` marks a derived table; a table function is implicitly
/// lateral because its arguments may reference the preceding items (D2).
fn from_item_is_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Derived { lateral, .. } => *lateral,
        FromItem::Function { .. } => true,
        FromItem::Table { .. } | FromItem::Join { .. } => false,
    }
}

/// Scope a LATERAL item sees, addressed against the outer row the executor
/// builds for it: the left join row followed by the enclosing outer row.
///
/// `base` is the output offset the left relation was planned at, so its scope
/// is rebased to 0; the enclosing scope shifts past the left row. Neither side
/// changes `scope_level` here, because planning the lateral relation applies
/// [`offset_scope`] once and that is the single level it is nested by.
fn lateral_outer_scope(
    left_scope: &[ScopedTable],
    base: usize,
    left_width: usize,
    outer_scope: &[ScopedTable],
) -> Vec<ScopedTable> {
    debug_assert!(
        left_scope.iter().all(|table| table.start_index >= base),
        "a FROM item's left sibling scope must start at the join's own base"
    );
    left_scope
        .iter()
        .cloned()
        .map(|mut table| {
            table.start_index -= base;
            table
        })
        .chain(outer_scope.iter().cloned().map(|mut table| {
            table.start_index += left_width;
            table
        }))
        .collect()
}

/// Apply a relation alias column-name list to `schema` in place.
///
/// Exact arity is required for every relation kind, and a repeated name is
/// rejected (issue #151, D8).
fn apply_alias_columns(
    alias: &str,
    columns: &[String],
    schema: &mut [ColumnMetadata],
    span: crate::ast::Span,
) -> Result<(), PlannerError> {
    if columns.is_empty() {
        return Ok(());
    }
    if columns.len() != schema.len() {
        return Err(PlannerError::table_alias_column_count_mismatch(
            alias,
            columns.len(),
            schema.len(),
            span,
        ));
    }
    let mut names = HashSet::new();
    for name in columns {
        if !names.insert(name) {
            return Err(PlannerError::invalid_expression(format!(
                "relation alias '{alias}' declares column '{name}' more than once"
            )));
        }
    }
    for (column, name) in schema.iter_mut().zip(columns) {
        column.name.clone_from(name);
    }
    Ok(())
}

fn offset_scope(scope: &[ScopedTable], offset: usize) -> Vec<ScopedTable> {
    scope
        .iter()
        .cloned()
        .map(|mut table| {
            table.start_index += offset;
            table.scope_level += 1;
            table
        })
        .collect()
}

fn natural_join_columns(
    left_schema: &[ColumnMetadata],
    right_schema: &[ColumnMetadata],
) -> Vec<String> {
    // Pairing every left column against every right column is quadratic in the
    // join width, so the right side is hashed once. Iteration stays over the
    // left schema because the common columns keep the left table's order.
    let right_names = right_schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    left_schema
        .iter()
        .filter(|left| right_names.contains(left.name.as_str()))
        .map(|column| column.name.clone())
        .collect()
}

fn install_base_projection(plan: &mut LogicalPlan, projection: &Projection) {
    match plan {
        LogicalPlan::Scan {
            projection: scan_projection,
            ..
        } => *scan_projection = projection.clone(),
        LogicalPlan::Filter { input, .. } => install_base_projection(input, projection),
        _ => {}
    }
}

fn is_aggregate_function(name: &str) -> bool {
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
            | "percentile_disc"
    )
}

fn expr_key(expr: &TypedExpr) -> String {
    format!("{:?}", expr.kind)
}

/// Structural signature for DISTINCT ON key matching (D2).
///
/// `expr_key` embeds the source spans of nested sub-expressions, so the same
/// compound expression written once in the ON list and once in ORDER BY would
/// never compare equal. This signature erases every rendered
/// `span: Span { .. }` segment first. The eraser only rewrites segments that
/// match the exact derived-Debug shape (digits and fixed punctuation), so a
/// string literal that happens to contain the marker text is left untouched
/// and still compares consistently on both sides.
fn distinct_on_expr_signature(expr: &TypedExpr) -> String {
    const MARKER: &str = "span: Span { start: Location { line: ";
    let rendered = format!("{:?}", expr.kind);
    let mut result = String::with_capacity(rendered.len());
    let mut rest = rendered.as_str();
    while let Some(position) = rest.find(MARKER) {
        let after = &rest[position + MARKER.len()..];
        match debug_span_tail_length(after) {
            Some(consumed) => {
                result.push_str(&rest[..position]);
                result.push_str("span: _");
                rest = &after[consumed..];
            }
            None => {
                let keep = position + MARKER.len();
                result.push_str(&rest[..keep]);
                rest = &rest[keep..];
            }
        }
    }
    result.push_str(rest);
    result
}

/// Length of `<digits>, column: <digits> }, end: Location { line: <digits>,
/// column: <digits> } }` at the start of `input`, or `None` when the text does
/// not match that exact derived-Debug shape.
fn debug_span_tail_length(input: &str) -> Option<usize> {
    fn digits(input: &str, offset: &mut usize) -> bool {
        let start = *offset;
        while input
            .as_bytes()
            .get(*offset)
            .is_some_and(u8::is_ascii_digit)
        {
            *offset += 1;
        }
        *offset > start
    }
    fn literal(input: &str, offset: &mut usize, expected: &str) -> bool {
        if input[*offset..].starts_with(expected) {
            *offset += expected.len();
            true
        } else {
            false
        }
    }
    let mut offset = 0;
    (digits(input, &mut offset)
        && literal(input, &mut offset, ", column: ")
        && digits(input, &mut offset)
        && literal(input, &mut offset, " }, end: Location { line: ")
        && digits(input, &mut offset)
        && literal(input, &mut offset, ", column: ")
        && digits(input, &mut offset)
        && literal(input, &mut offset, " } }"))
    .then_some(offset)
}

/// Verify the DISTINCT ON / ORDER BY prefix contract (D2) and synthesize the
/// complete deterministic sort specification for [`LogicalPlan::DistinctOn`].
///
/// Returns `(key_count, order_by)` where the leading `key_count` entries cover
/// every deduplicated ON key: the user's matching ORDER BY prefix (any
/// permutation, keeping the user's direction), then any keys the user ORDER BY
/// did not reach as implicit ASC NULLS LAST (D3). The user's non-key tail
/// follows, and every input column is appended in schema order as an ASC NULLS
/// LAST tie-breaker (D4) so the surviving row of each key group never depends
/// on the physical input order.
///
/// Invariant relied on by `FETCH ... WITH TIES` (D13): the leading
/// `user_order_by.len()` entries of the returned specification are exactly the
/// user's ORDER BY, in the user's order. Implicit ON keys are only synthesized
/// when the user ORDER BY has no non-key tail (a tail plus an unreached key is
/// a D2 error), so the two groups can never interleave.
fn build_distinct_on_sort_spec(
    key_exprs: Vec<TypedExpr>,
    user_order_by: Vec<SortExpr>,
    base_schema: &[ColumnMetadata],
    fallback_span: crate::ast::Span,
) -> Result<(usize, Vec<SortExpr>), PlannerError> {
    let key_signatures: Vec<String> = key_exprs.iter().map(distinct_on_expr_signature).collect();
    let mut consumed = vec![false; key_exprs.len()];
    let mut prefix: Vec<SortExpr> = Vec::new();
    let mut tail: Vec<SortExpr> = Vec::new();
    let mut prefix_ended = false;
    for sort in user_order_by {
        let signature = distinct_on_expr_signature(&sort.expr);
        if let Some(index) = key_signatures
            .iter()
            .position(|candidate| candidate == &signature)
        {
            if prefix_ended {
                // D2: an ON key reappears after a non-key ORDER BY item
                // already ended the prefix (PostgreSQL 42P10).
                return Err(PlannerError::distinct_on_order_by_mismatch(sort.expr.span));
            }
            consumed[index] = true;
            prefix.push(sort);
        } else {
            prefix_ended = true;
            tail.push(sort);
        }
    }
    let mut implicit: Vec<SortExpr> = Vec::new();
    for (index, key) in key_exprs.into_iter().enumerate() {
        if consumed[index] {
            continue;
        }
        if prefix_ended {
            // D2: with non-key tail items present, an ON key the prefix never
            // reached leaves the deduplication order ambiguous.
            return Err(PlannerError::distinct_on_order_by_mismatch(key.span));
        }
        implicit.push(SortExpr::new(key, true, false));
    }
    let key_count = prefix.len() + implicit.len();
    let mut order_by = prefix;
    order_by.append(&mut implicit);
    order_by.append(&mut tail);
    let mut seen_columns: HashSet<usize> = order_by
        .iter()
        .filter_map(|sort| match &sort.expr.kind {
            TypedExprKind::ColumnRef { column_index, .. } => Some(*column_index),
            _ => None,
        })
        .collect();
    for (index, column) in base_schema.iter().enumerate() {
        if seen_columns.insert(index) {
            order_by.push(SortExpr::new(
                TypedExpr::column_ref(
                    String::new(),
                    column.name.clone(),
                    index,
                    column.data_type.clone(),
                    fallback_span,
                ),
                true,
                false,
            ));
        }
    }
    Ok((key_count, order_by))
}

/// Ordering changes the result only for these aggregates (issue #148, D3).
fn is_order_sensitive_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "group_concat" | "string_agg" | "percentile_disc"
    )
}

fn sort_exprs_key(order_by: &[SortExpr]) -> Option<String> {
    if order_by.is_empty() {
        return None;
    }
    Some(
        order_by
            .iter()
            .map(|sort| format!("{}|{}|{}", expr_key(&sort.expr), sort.asc, sort.nulls_first))
            .collect::<Vec<_>>()
            .join(","),
    )
}

#[allow(clippy::too_many_arguments)]
fn aggregate_signature(
    name: &str,
    distinct: bool,
    star: bool,
    arg: Option<&TypedExpr>,
    separator: Option<&String>,
    _expr: &TypedExpr,
    filter: Option<&TypedExpr>,
    order_by: &[SortExpr],
) -> AggregateSignature {
    AggregateSignature {
        name: name.to_ascii_lowercase(),
        distinct,
        star,
        arg_key: arg.map(expr_key),
        separator: separator.cloned(),
        filter_key: filter.map(expr_key),
        order_key: sort_exprs_key(order_by),
    }
}

fn build_group_key_map(group_keys: &[TypedExpr]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (idx, key) in group_keys.iter().enumerate() {
        map.insert(expr_key(key), idx);
    }
    map
}

fn build_aggregate_map(aggregates: &[AggregateExpr]) -> HashMap<AggregateSignature, usize> {
    let mut map = HashMap::new();
    for (idx, agg) in aggregates.iter().enumerate() {
        let (name, separator, star, arg) = match &agg.function {
            AggregateFunction::Count => (
                "count".to_string(),
                None,
                agg.arg.is_none(),
                agg.arg.as_ref(),
            ),
            AggregateFunction::Sum => ("sum".to_string(), None, false, agg.arg.as_ref()),
            AggregateFunction::Total => ("total".to_string(), None, false, agg.arg.as_ref()),
            AggregateFunction::Avg => ("avg".to_string(), None, false, agg.arg.as_ref()),
            AggregateFunction::Min => ("min".to_string(), None, false, agg.arg.as_ref()),
            AggregateFunction::Max => ("max".to_string(), None, false, agg.arg.as_ref()),
            AggregateFunction::GroupConcat { separator } => (
                "group_concat".to_string(),
                separator.clone(),
                false,
                agg.arg.as_ref(),
            ),
            AggregateFunction::StringAgg { separator } => (
                "string_agg".to_string(),
                separator.clone(),
                false,
                agg.arg.as_ref(),
            ),
            AggregateFunction::PercentileDisc { fraction } => (
                "percentile_disc".to_string(),
                Some(format!("{fraction:?}")),
                false,
                None,
            ),
        };
        let signature = AggregateSignature {
            name,
            distinct: agg.distinct,
            star,
            arg_key: arg.map(expr_key),
            separator,
            filter_key: agg.filter.as_ref().map(expr_key),
            order_key: sort_exprs_key(&agg.order_by),
        };
        map.insert(signature, idx);
    }
    map
}

/// Hidden aggregate output column carrying the grouping-set mask (issue #149).
pub(crate) const GROUPING_ID_COLUMN: &str = "__grouping_id";
/// PostgreSQL-compatible bound on expanded grouping sets (D6).
const MAX_GROUPING_SETS: usize = 4096;
/// CUBE with more than 12 columns always exceeds `MAX_GROUPING_SETS`.
const MAX_CUBE_COLUMNS: usize = 12;
/// The grouping-id mask is a BIGINT, so 63 keys/arguments at most (D4).
const MAX_GROUPING_KEYS: usize = 63;

/// GROUP BY expansion result (issue #149).
struct ExpandedGroupBy {
    group_keys: Vec<TypedExpr>,
    grouping_sets: Option<Vec<u64>>,
}

fn grouping_full_mask(key_count: usize) -> u64 {
    if key_count == 0 {
        0
    } else {
        (1u64 << key_count) - 1
    }
}

fn is_grouping_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("grouping") || name.eq_ignore_ascii_case("grouping_id")
}

/// Context for lowering GROUPING/GROUPING_ID onto `__grouping_id` (D4/D5).
struct GroupingRewrite {
    /// Group-key expression identity -> union key position.
    key_index: HashMap<String, usize>,
    key_count: usize,
    /// Output position of `__grouping_id` (after keys and aggregates).
    gid_index: usize,
    /// Whether the plan actually carries grouping sets; a plain GROUP BY
    /// still accepts GROUPING but every call folds to constant 0.
    sets_present: bool,
}

impl GroupingRewrite {
    fn new(
        group_keys: &[TypedExpr],
        aggregates: &[AggregateExpr],
        grouping_sets: &Option<Vec<u64>>,
    ) -> Self {
        let key_index = group_keys
            .iter()
            .enumerate()
            .map(|(index, key)| (expr_key(key), index))
            .collect();
        Self {
            key_index,
            key_count: group_keys.len(),
            gid_index: group_keys.len() + aggregates.len(),
            sets_present: grouping_sets.is_some(),
        }
    }
}

/// AST-level detection of GROUPING/GROUPING_ID calls (D5 placement rules).
fn expr_contains_grouping(expr: &crate::ast::expr::Expr) -> bool {
    use crate::ast::expr::ExprKind;

    match &expr.kind {
        ExprKind::FunctionCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            is_grouping_function(name)
                || args.iter().any(expr_contains_grouping)
                || order_by
                    .iter()
                    .any(|sort| expr_contains_grouping(&sort.expr))
                || within_group
                    .iter()
                    .any(|sort| expr_contains_grouping(&sort.expr))
                || filter.as_deref().is_some_and(expr_contains_grouping)
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(expr_contains_grouping)
                        || window
                            .order_by
                            .iter()
                            .any(|sort| expr_contains_grouping(&sort.expr))
                })
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_grouping(left) || expr_contains_grouping(right)
        }
        ExprKind::UnaryOp { operand, .. } => expr_contains_grouping(operand),
        ExprKind::TruthPredicate { expr, .. } => expr_contains_grouping(expr),
        ExprKind::IsDistinctFrom { left, right, .. } => {
            expr_contains_grouping(left) || expr_contains_grouping(right)
        }
        ExprKind::Row { items } => items.iter().any(expr_contains_grouping),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_grouping)
                || branches.iter().any(|branch| {
                    expr_contains_grouping(&branch.when) || expr_contains_grouping(&branch.then)
                })
                || else_expr.as_deref().is_some_and(expr_contains_grouping)
        }
        ExprKind::Cast { expr, .. } | ExprKind::TryCast { expr, .. } => {
            expr_contains_grouping(expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_contains_grouping(expr)
                || expr_contains_grouping(low)
                || expr_contains_grouping(high)
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_grouping(expr)
                || expr_contains_grouping(pattern)
                || escape.as_deref().is_some_and(expr_contains_grouping)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_contains_grouping(expr) || list.iter().any(expr_contains_grouping)
        }
        ExprKind::IsNull { expr, .. } => expr_contains_grouping(expr),
        ExprKind::ScalarSubquery { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::Exists { .. }
        | ExprKind::Quantified { .. }
        | ExprKind::Literal { .. }
        | ExprKind::VectorLiteral { .. }
        | ExprKind::ColumnRef { .. } => false,
    }
}

fn typed_expr_contains_grouping(expr: &TypedExpr) -> bool {
    match &expr.kind {
        TypedExprKind::FunctionCall {
            name,
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            is_grouping_function(name)
                || args.iter().any(typed_expr_contains_grouping)
                || filter.as_deref().is_some_and(typed_expr_contains_grouping)
                || order_by
                    .iter()
                    .any(|sort| typed_expr_contains_grouping(&sort.expr))
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(typed_expr_contains_grouping)
                        || window
                            .order_by
                            .iter()
                            .any(|sort| typed_expr_contains_grouping(&sort.expr))
                })
        }
        TypedExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_grouping(left) || typed_expr_contains_grouping(right)
        }
        TypedExprKind::UnaryOp { operand, .. } => typed_expr_contains_grouping(operand),
        TypedExprKind::Cast { expr, .. } | TypedExprKind::TryCast { expr, .. } => {
            typed_expr_contains_grouping(expr)
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(typed_expr_contains_grouping)
                || branches.iter().any(|branch| {
                    typed_expr_contains_grouping(&branch.when)
                        || typed_expr_contains_grouping(&branch.then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(typed_expr_contains_grouping)
        }
        TypedExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_grouping(expr)
                || typed_expr_contains_grouping(low)
                || typed_expr_contains_grouping(high)
        }
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            typed_expr_contains_grouping(expr)
                || typed_expr_contains_grouping(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|inner| typed_expr_contains_grouping(inner))
        }
        TypedExprKind::InList { expr, list, .. } => {
            typed_expr_contains_grouping(expr) || list.iter().any(typed_expr_contains_grouping)
        }
        TypedExprKind::IsNull { expr, .. } => typed_expr_contains_grouping(expr),
        TypedExprKind::InSubquery { expr, .. } => typed_expr_contains_grouping(expr),
        TypedExprKind::Quantified { expr, .. } => typed_expr_contains_grouping(expr),
        TypedExprKind::Literal(_)
        | TypedExprKind::VectorLiteral(_)
        | TypedExprKind::ColumnRef { .. }
        | TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::Exists { .. } => false,
    }
}

fn bigint_literal(value: u64, span: crate::ast::Span) -> TypedExpr {
    TypedExpr {
        kind: TypedExprKind::Literal(Literal::Number(value.to_string())),
        resolved_type: ResolvedType::BigInt,
        span,
    }
}

fn bigint_binary_op(
    left: TypedExpr,
    op: crate::ast::expr::BinaryOp,
    right: TypedExpr,
    span: crate::ast::Span,
) -> TypedExpr {
    TypedExpr {
        kind: TypedExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        resolved_type: ResolvedType::BigInt,
        span,
    }
}

/// Lower a `GROUPING(e1, ..., en)` call to integer arithmetic over the
/// hidden `__grouping_id` output column (D4).
///
/// Argument `j` (0-based, leftmost = most significant result bit) whose key
/// occupies union position `i` contributes
/// `((__grouping_id / 2^(K-1-i)) % 2) * 2^(n-1-j)`; the divisor is a power
/// of two, so no division by zero is possible at runtime.
fn lower_grouping_call(
    args: &[TypedExpr],
    context: &GroupingRewrite,
    span: crate::ast::Span,
) -> Result<TypedExpr, PlannerError> {
    use crate::ast::expr::BinaryOp as AstBinaryOp;

    if args.is_empty() {
        return Err(PlannerError::invalid_expression(
            "GROUPING requires at least one argument".to_string(),
        ));
    }
    if args.len() > MAX_GROUPING_KEYS {
        return Err(PlannerError::invalid_expression(format!(
            "GROUPING accepts at most {MAX_GROUPING_KEYS} arguments"
        )));
    }
    let mut key_positions = Vec::with_capacity(args.len());
    for arg in args {
        let Some(&position) = context.key_index.get(&expr_key(arg)) else {
            return Err(PlannerError::invalid_expression(
                "arguments to GROUPING must be grouping expressions of the query".to_string(),
            ));
        };
        key_positions.push(position);
    }

    if !context.sets_present {
        // Plain GROUP BY has exactly one grouping set: every key is present.
        return Ok(bigint_literal(0, span));
    }

    let argument_count = key_positions.len();
    let mut sum: Option<TypedExpr> = None;
    for (argument, key_position) in key_positions.into_iter().enumerate() {
        let gid_ref = TypedExpr::column_ref(
            "__agg__".to_string(),
            GROUPING_ID_COLUMN.to_string(),
            context.gid_index,
            ResolvedType::BigInt,
            span,
        );
        let excluded_shift = (context.key_count - 1 - key_position) as u32;
        let bit = bigint_binary_op(
            bigint_binary_op(
                gid_ref,
                AstBinaryOp::Div,
                bigint_literal(1u64 << excluded_shift, span),
                span,
            ),
            AstBinaryOp::Mod,
            bigint_literal(2, span),
            span,
        );
        let weight = 1u64 << (argument_count - 1 - argument);
        let term = if weight == 1 {
            bit
        } else {
            bigint_binary_op(bit, AstBinaryOp::Mul, bigint_literal(weight, span), span)
        };
        sum = Some(match sum {
            None => term,
            Some(current) => bigint_binary_op(current, AstBinaryOp::Add, term, span),
        });
    }
    Ok(sum.expect("GROUPING argument list is non-empty"))
}

/// Pre-pass over aggregate-context expressions: replace GROUPING calls and
/// validate their placement before `rewrite_expr_with_maps` runs (D5).
///
/// Aggregate calls are returned unchanged (their signature must keep matching
/// the collected plan aggregates), but GROUPING inside their arguments is a
/// planning error because aggregate arguments evaluate against input rows.
fn rewrite_grouping_calls(
    expr: &TypedExpr,
    context: &GroupingRewrite,
) -> Result<TypedExpr, PlannerError> {
    let rebuild = |inner: &TypedExpr| rewrite_grouping_calls(inner, context);
    let rebuild_box = |inner: &TypedExpr| -> Result<Box<TypedExpr>, PlannerError> {
        Ok(Box::new(rewrite_grouping_calls(inner, context)?))
    };
    let kind = match &expr.kind {
        TypedExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
            filter,
            order_by,
            over,
        } => {
            if is_grouping_function(name) {
                if over.is_some() {
                    return Err(PlannerError::invalid_expression(
                        "GROUPING cannot be used as a window function".to_string(),
                    ));
                }
                return lower_grouping_call(args, context, expr.span);
            }
            if over.is_none() && is_aggregate_function(name) {
                if args.iter().any(typed_expr_contains_grouping)
                    || filter.as_deref().is_some_and(typed_expr_contains_grouping)
                    || order_by
                        .iter()
                        .any(|sort| typed_expr_contains_grouping(&sort.expr))
                {
                    return Err(PlannerError::invalid_expression(
                        "GROUPING cannot appear inside aggregate function arguments".to_string(),
                    ));
                }
                return Ok(expr.clone());
            }
            TypedExprKind::FunctionCall {
                name: name.clone(),
                args: args.iter().map(rebuild).collect::<Result<Vec<_>, _>>()?,
                distinct: *distinct,
                star: *star,
                filter: filter.as_deref().map(rebuild_box).transpose()?,
                order_by: order_by
                    .iter()
                    .map(|sort| {
                        Ok(SortExpr::new(
                            rebuild(&sort.expr)?,
                            sort.asc,
                            sort.nulls_first,
                        ))
                    })
                    .collect::<Result<Vec<_>, PlannerError>>()?,
                over: over
                    .as_ref()
                    .map(|window| {
                        Ok(typed_expr::TypedWindowSpec {
                            partition_by: window
                                .partition_by
                                .iter()
                                .map(rebuild)
                                .collect::<Result<Vec<_>, _>>()?,
                            order_by: window
                                .order_by
                                .iter()
                                .map(|sort| {
                                    Ok(SortExpr::new(
                                        rebuild(&sort.expr)?,
                                        sort.asc,
                                        sort.nulls_first,
                                    ))
                                })
                                .collect::<Result<Vec<_>, PlannerError>>()?,
                            frame: window.frame.clone(),
                        })
                    })
                    .transpose()
                    .map_err(|error: PlannerError| error)?,
            }
        }
        TypedExprKind::BinaryOp { left, op, right } => TypedExprKind::BinaryOp {
            left: rebuild_box(left)?,
            op: *op,
            right: rebuild_box(right)?,
        },
        TypedExprKind::UnaryOp { op, operand } => TypedExprKind::UnaryOp {
            op: *op,
            operand: rebuild_box(operand)?,
        },
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => TypedExprKind::Case {
            operand: operand.as_deref().map(rebuild_box).transpose()?,
            branches: branches
                .iter()
                .map(|branch| {
                    Ok(TypedCaseWhen {
                        when: rebuild(&branch.when)?,
                        then: rebuild(&branch.then)?,
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?,
            else_expr: else_expr.as_deref().map(rebuild_box).transpose()?,
        },
        TypedExprKind::Cast {
            expr: inner,
            target_type,
        } => TypedExprKind::Cast {
            expr: rebuild_box(inner)?,
            target_type: target_type.clone(),
        },
        TypedExprKind::TryCast {
            expr: inner,
            target_type,
        } => TypedExprKind::TryCast {
            expr: rebuild_box(inner)?,
            target_type: target_type.clone(),
        },
        TypedExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExprKind::Between {
            expr: rebuild_box(inner)?,
            low: rebuild_box(low)?,
            high: rebuild_box(high)?,
            negated: *negated,
        },
        TypedExprKind::Like {
            expr: inner,
            pattern,
            escape,
            negated,
            kind,
        } => TypedExprKind::Like {
            expr: rebuild_box(inner)?,
            pattern: rebuild_box(pattern)?,
            escape: escape.as_deref().map(rebuild_box).transpose()?,
            negated: *negated,
            kind: *kind,
        },
        TypedExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExprKind::InList {
            expr: rebuild_box(inner)?,
            list: list.iter().map(rebuild).collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        TypedExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExprKind::IsNull {
            expr: rebuild_box(inner)?,
            negated: *negated,
        },
        TypedExprKind::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => TypedExprKind::InSubquery {
            expr: rebuild_box(inner)?,
            subquery: subquery.clone(),
            negated: *negated,
        },
        TypedExprKind::Quantified {
            expr: inner,
            op,
            quantifier,
            subquery,
        } => TypedExprKind::Quantified {
            expr: rebuild_box(inner)?,
            op: *op,
            quantifier: *quantifier,
            subquery: subquery.clone(),
        },
        TypedExprKind::Literal(_)
        | TypedExprKind::VectorLiteral(_)
        | TypedExprKind::ColumnRef { .. }
        | TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::Exists { .. } => return Ok(expr.clone()),
    };
    Ok(TypedExpr {
        kind,
        resolved_type: expr.resolved_type.clone(),
        span: expr.span,
    })
}

fn build_aggregate_schema(
    group_keys: &[TypedExpr],
    aggregates: &[AggregateExpr],
) -> Vec<ColumnMetadata> {
    let mut schema = Vec::new();
    for (idx, key) in group_keys.iter().enumerate() {
        let name = match &key.kind {
            TypedExprKind::ColumnRef { column, .. } => column.clone(),
            _ => format!("group_{idx}"),
        };
        schema.push(ColumnMetadata::new(name, key.resolved_type.clone()));
    }
    for (idx, agg) in aggregates.iter().enumerate() {
        let name = match &agg.function {
            AggregateFunction::Count => format!("count_{idx}"),
            AggregateFunction::Sum => format!("sum_{idx}"),
            AggregateFunction::Total => format!("total_{idx}"),
            AggregateFunction::Avg => format!("avg_{idx}"),
            AggregateFunction::Min => format!("min_{idx}"),
            AggregateFunction::Max => format!("max_{idx}"),
            AggregateFunction::GroupConcat { .. } => format!("group_concat_{idx}"),
            AggregateFunction::StringAgg { .. } => format!("string_agg_{idx}"),
            AggregateFunction::PercentileDisc { .. } => format!("percentile_disc_{idx}"),
        };
        schema.push(ColumnMetadata::new(name, agg.result_type.clone()));
    }
    schema
}

fn rewrite_expr_with_maps(
    expr: &TypedExpr,
    group_key_map: &HashMap<String, usize>,
    aggregate_map: &HashMap<AggregateSignature, usize>,
    output_names: &[String],
) -> Result<TypedExpr, PlannerError> {
    let group_key_count = output_names.len().saturating_sub(aggregate_map.len());
    let key = expr_key(expr);
    if let Some(idx) = group_key_map.get(&key) {
        return Ok(make_output_column_ref(
            *idx,
            output_names,
            expr.resolved_type.clone(),
            expr.span,
        ));
    }

    match &expr.kind {
        TypedExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
            filter,
            order_by,
            over: None,
        } if is_aggregate_function(name) => {
            let is_percentile = name.eq_ignore_ascii_case("percentile_disc");
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
                    type_checker::percentile_fraction(&args[0])?
                ))
            } else {
                None
            };
            let signature = AggregateSignature {
                name: name.to_ascii_lowercase(),
                distinct: *distinct,
                star: *star,
                arg_key: if is_percentile {
                    None
                } else {
                    args.first().map(expr_key)
                },
                separator,
                filter_key: filter.as_deref().map(expr_key),
                order_key: if is_order_sensitive_aggregate(name) {
                    sort_exprs_key(order_by)
                } else {
                    None
                },
            };
            let idx = aggregate_map.get(&signature).ok_or_else(|| {
                PlannerError::invalid_expression(
                    "aggregate in expression is not part of plan".to_string(),
                )
            })?;
            let output_index = group_key_count + idx;
            Ok(make_output_column_ref(
                output_index,
                output_names,
                expr.resolved_type.clone(),
                expr.span,
            ))
        }
        TypedExprKind::FunctionCall {
            name,
            args,
            distinct,
            star,
            filter,
            order_by,
            over,
        } => {
            if over.is_none() && (*distinct || *star) {
                return Err(PlannerError::invalid_expression(
                    "DISTINCT/STAR modifiers are only supported for aggregates".to_string(),
                ));
            }
            if filter.is_some() || !order_by.is_empty() {
                // Non-aggregate calls never carry these clauses (rejected by
                // the type checker), so reaching here means the aggregate
                // above did not match the plan.
                return Err(PlannerError::invalid_expression(
                    "aggregate in expression is not part of plan".to_string(),
                ));
            }
            let mut rewritten_args = Vec::with_capacity(args.len());
            for arg in args {
                rewritten_args.push(rewrite_expr_with_maps(
                    arg,
                    group_key_map,
                    aggregate_map,
                    output_names,
                )?);
            }
            let over = over
                .as_ref()
                .map(|window| {
                    let partition_by = window
                        .partition_by
                        .iter()
                        .map(|expr| {
                            rewrite_expr_with_maps(expr, group_key_map, aggregate_map, output_names)
                        })
                        .collect::<Result<Vec<_>, PlannerError>>()?;
                    let order_by = window
                        .order_by
                        .iter()
                        .map(|sort| {
                            Ok(SortExpr::new(
                                rewrite_expr_with_maps(
                                    &sort.expr,
                                    group_key_map,
                                    aggregate_map,
                                    output_names,
                                )?,
                                sort.asc,
                                sort.nulls_first,
                            ))
                        })
                        .collect::<Result<Vec<_>, PlannerError>>()?;
                    Ok(crate::planner::typed_expr::TypedWindowSpec {
                        partition_by,
                        order_by,
                        frame: window.frame.clone(),
                    })
                })
                .transpose()?;
            Ok(TypedExpr {
                kind: TypedExprKind::FunctionCall {
                    name: name.clone(),
                    args: rewritten_args,
                    distinct: *distinct,
                    star: *star,
                    filter: None,
                    order_by: Vec::new(),
                    over,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::BinaryOp { left, op, right } => {
            let left = rewrite_expr_with_maps(left, group_key_map, aggregate_map, output_names)?;
            let right = rewrite_expr_with_maps(right, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::BinaryOp {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::UnaryOp { op, operand } => {
            let operand =
                rewrite_expr_with_maps(operand, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::UnaryOp {
                    op: *op,
                    operand: Box::new(operand),
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            let operand = operand
                .as_deref()
                .map(|operand| {
                    rewrite_expr_with_maps(operand, group_key_map, aggregate_map, output_names)
                        .map(Box::new)
                })
                .transpose()?;
            let mut rewritten_branches = Vec::with_capacity(branches.len());
            for branch in branches {
                rewritten_branches.push(TypedCaseWhen {
                    when: rewrite_expr_with_maps(
                        &branch.when,
                        group_key_map,
                        aggregate_map,
                        output_names,
                    )?,
                    then: rewrite_expr_with_maps(
                        &branch.then,
                        group_key_map,
                        aggregate_map,
                        output_names,
                    )?,
                });
            }
            let else_expr = else_expr
                .as_deref()
                .map(|else_expr| {
                    rewrite_expr_with_maps(else_expr, group_key_map, aggregate_map, output_names)
                        .map(Box::new)
                })
                .transpose()?;
            Ok(TypedExpr {
                kind: TypedExprKind::Case {
                    operand,
                    branches: rewritten_branches,
                    else_expr,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            let low = rewrite_expr_with_maps(low, group_key_map, aggregate_map, output_names)?;
            let high = rewrite_expr_with_maps(high, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::Between {
                    expr: Box::new(inner),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: *negated,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::Like {
            expr: inner,
            pattern,
            escape,
            negated,
            kind,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            let pattern =
                rewrite_expr_with_maps(pattern, group_key_map, aggregate_map, output_names)?;
            let escape = if let Some(esc) = escape {
                Some(Box::new(rewrite_expr_with_maps(
                    esc,
                    group_key_map,
                    aggregate_map,
                    output_names,
                )?))
            } else {
                None
            };
            Ok(TypedExpr {
                kind: TypedExprKind::Like {
                    expr: Box::new(inner),
                    pattern: Box::new(pattern),
                    escape,
                    negated: *negated,
                    kind: *kind,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::InList {
            expr: inner,
            list,
            negated,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            let mut rewritten_list = Vec::with_capacity(list.len());
            for item in list {
                rewritten_list.push(rewrite_expr_with_maps(
                    item,
                    group_key_map,
                    aggregate_map,
                    output_names,
                )?);
            }
            Ok(TypedExpr {
                kind: TypedExprKind::InList {
                    expr: Box::new(inner),
                    list: rewritten_list,
                    negated: *negated,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::IsNull {
                    expr: Box::new(inner),
                    negated: *negated,
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::Literal(_) | TypedExprKind::VectorLiteral(_) => Ok(expr.clone()),
        // References the GROUPING pre-pass already resolved onto the
        // aggregate output (the hidden __grouping_id column) pass through.
        TypedExprKind::ColumnRef { table, .. } if table == "__agg__" => Ok(expr.clone()),
        TypedExprKind::ColumnRef { .. } => Err(PlannerError::invalid_expression(
            "column reference must appear in GROUP BY or be aggregated".to_string(),
        )),
        TypedExprKind::Cast {
            expr: inner,
            target_type,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::Cast {
                    expr: Box::new(inner),
                    target_type: target_type.clone(),
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::TryCast {
            expr: inner,
            target_type,
        } => {
            let inner = rewrite_expr_with_maps(inner, group_key_map, aggregate_map, output_names)?;
            Ok(TypedExpr {
                kind: TypedExprKind::TryCast {
                    expr: Box::new(inner),
                    target_type: target_type.clone(),
                },
                resolved_type: expr.resolved_type.clone(),
                span: expr.span,
            })
        }
        TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::InSubquery { .. }
        | TypedExprKind::Exists { .. }
        | TypedExprKind::Quantified { .. } => Ok(expr.clone()),
    }
}

fn make_output_column_ref(
    index: usize,
    output_names: &[String],
    resolved_type: ResolvedType,
    span: crate::ast::Span,
) -> TypedExpr {
    let name = output_names
        .get(index)
        .cloned()
        .unwrap_or_else(|| format!("col_{index}"));
    TypedExpr::column_ref("__agg__".to_string(), name, index, resolved_type, span)
}
