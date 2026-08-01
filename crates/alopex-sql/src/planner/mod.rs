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
pub mod type_checker;
pub mod typed_expr;
pub mod types;

#[cfg(test)]
mod planner_tests;

pub use aggregate_expr::{AggregateExpr, AggregateFunction};
pub use error::PlannerError;
pub use knn_optimizer::{KnnPattern, SortDirection, detect_knn_pattern};
pub use logical_plan::{JoinType, LogicalPlan};
pub use name_resolver::{NameResolver, ResolvedColumn};
pub use type_checker::{ScopedTable, TypeChecker};
pub use typed_expr::{
    ProjectedColumn, Projection, SortExpr, TypedAssignment, TypedExpr, TypedExprKind,
};
pub use types::ResolvedType;

use crate::ast::ddl::{
    ColumnConstraint, ColumnDef, CreateIndex, CreateTable, DropIndex, DropTable,
};
use crate::ast::dml::{
    Delete, FromItem, Insert, InsertSource, LITERAL_TABLE, OrderByExpr, Select, SelectItem, Update,
};
use crate::ast::expr::Literal;
use crate::ast::{PragmaValue, Spanned, Statement, StatementKind};
use crate::catalog::{Catalog, ColumnMetadata, IndexMetadata, TableMetadata};
use crate::{AlopexDialect, DataSourceFormat, Parser, SqlError, TableType};
use std::collections::HashMap;

struct PlannedRelation {
    plan: LogicalPlan,
    schema: Vec<ColumnMetadata>,
    scope: Vec<ScopedTable>,
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
    let routing_input = routing_input_for_plan(&statement.kind, &plan);
    Ok(PlannedStatement {
        plan,
        routing_input,
    })
}

fn routing_input_for_plan(statement_kind: &StatementKind, plan: &LogicalPlan) -> RoutingInput {
    let mut diagnostics = Vec::new();
    let extractor = TableReferenceExtractor::new();
    let table_references = extractor.extract_from_logical_plan(
        plan,
        table_reference_access(statement_kind),
        &mut diagnostics,
    );

    RoutingInput {
        statement_kind: statement_kind.clone(),
        table_references,
        diagnostics,
    }
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
            LogicalPlan::Aggregate {
                input,
                group_keys,
                aggregates,
                having,
                projection,
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
            LogicalPlan::Sort { input, order_by } => {
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
            | TypedExprKind::IsNull { expr: operand, .. } => {
                self.extract_typed_expr(operand, diagnostics, references);
            }
            TypedExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    self.extract_typed_expr(arg, diagnostics, references);
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

fn table_reference_access(statement_kind: &StatementKind) -> TableReferenceAccess {
    match statement_kind {
        StatementKind::Select(_) => TableReferenceAccess::Read,
        StatementKind::Insert(_) | StatementKind::Update(_) | StatementKind::Delete(_) => {
            TableReferenceAccess::Write
        }
        StatementKind::CreateTable(_) => TableReferenceAccess::Create,
        StatementKind::DropTable(_) => TableReferenceAccess::Drop,
        StatementKind::CreateIndex(_) | StatementKind::DropIndex(_) => {
            TableReferenceAccess::Metadata
        }
        StatementKind::Pragma { .. } => TableReferenceAccess::Metadata,
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
        match &stmt.kind {
            // DDL statements
            StatementKind::CreateTable(ct) => self.plan_create_table(ct),
            StatementKind::DropTable(dt) => self.plan_drop_table(dt),
            StatementKind::CreateIndex(ci) => self.plan_create_index(ci),
            StatementKind::DropIndex(di) => self.plan_drop_index(di),
            StatementKind::Pragma { name, value } => self.plan_pragma(name, value),

            // DML statements
            StatementKind::Select(sel) => self.plan_select(sel),
            StatementKind::Insert(ins) => self.plan_insert(ins),
            StatementKind::Update(upd) => self.plan_update(upd),
            StatementKind::Delete(del) => self.plan_delete(del),
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
        self.plan_select_relation(stmt, &[])
            .map(|relation| relation.plan)
    }

    fn plan_select_relation(
        &self,
        stmt: &Select,
        outer_scope: &[ScopedTable],
    ) -> Result<PlannedRelation, PlannerError> {
        let mut relation = self.plan_from_items(&stmt.from, stmt.span, outer_scope)?;
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
        let distinct_only =
            stmt.distinct && !has_group_by && !has_aggregate && stmt.having.is_none();

        let final_projection =
            self.build_projection_with_scope(&stmt.projection, &relation.schema, &expr_scope)?;
        install_base_projection(&mut relation.plan, &final_projection);
        let needs_project_boundary = !matches!(relation.plan, LogicalPlan::Scan { .. });
        let mut plan = relation.plan;

        // 3. Add Filter if WHERE clause is present
        if let Some(ref selection) = stmt.selection {
            let predicate = self.infer_expr_with_scope(selection, &expr_scope)?;

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

        if has_group_by || has_aggregate || stmt.having.is_some() || stmt.distinct {
            if !has_group_by && !has_aggregate && stmt.having.is_some() {
                return Err(PlannerError::invalid_expression(
                    "HAVING requires GROUP BY or aggregate functions".to_string(),
                ));
            }

            let (group_keys, projected) = if distinct_only {
                let projected = self.build_projected_columns_for_distinct_with_scope(
                    &stmt.projection,
                    &relation.schema,
                    &expr_scope,
                )?;
                let group_keys = projected.iter().map(|col| col.expr.clone()).collect();
                (group_keys, projected)
            } else {
                let group_keys = self.build_group_keys_with_scope(stmt, &expr_scope)?;
                let projected = self.build_projected_columns_for_aggregate_with_scope(
                    &stmt.projection,
                    &expr_scope,
                )?;
                (group_keys, projected)
            };
            let mut aggregates = Vec::new();
            let mut agg_map = HashMap::new();

            for col in &projected {
                self.collect_aggregates_from_typed_expr(&col.expr, &mut aggregates, &mut agg_map)?;
            }

            let having_typed = if let Some(having) = &stmt.having {
                let typed = self.infer_expr_with_scope(having, &expr_scope)?;
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
                    let typed = self.infer_expr_with_scope(&order_expr.expr, &expr_scope)?;
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

            let output_schema = build_aggregate_schema(&group_keys, &aggregates);
            let output_names: Vec<String> = output_schema.iter().map(|c| c.name.clone()).collect();

            let projection = self.build_aggregate_projection(
                projected,
                &group_keys,
                &aggregates,
                &output_names,
            )?;

            let having = if let Some(having) = having_typed {
                Some(self.rewrite_expr_for_aggregate(
                    &having,
                    &group_keys,
                    &aggregates,
                    &output_names,
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
            };

            if !order_by.is_empty() {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by,
                };
            }

            if stmt.limit.is_some() || stmt.offset.is_some() {
                let limit = self.extract_limit_value(&stmt.limit, stmt.span)?;
                let offset = self.extract_limit_value(&stmt.offset, stmt.span)?;
                plan = LogicalPlan::Limit {
                    input: Box::new(plan),
                    limit,
                    offset,
                };
            }

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
        if !stmt.order_by.is_empty() {
            let order_by = self.build_sort_exprs_with_scope(&stmt.order_by, &expr_scope)?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = self.extract_limit_value(&stmt.limit, stmt.span)?;
            let offset = self.extract_limit_value(&stmt.offset, stmt.span)?;
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

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

    /// Build the projection for a SELECT statement.
    ///
    /// Handles wildcard expansion and expression type checking.
    fn plan_from_items(
        &self,
        items: &[FromItem],
        select_span: crate::ast::Span,
        outer_scope: &[ScopedTable],
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
            [single] => self.plan_from_item(single, 0, outer_scope),
            [first, rest @ ..] => {
                let mut relation = self.plan_from_item(first, 0, outer_scope)?;
                for item in rest {
                    let right = self.plan_from_item(item, relation.schema.len(), outer_scope)?;
                    relation = self.combine_join_relation(
                        relation,
                        right,
                        JoinType::Cross,
                        None,
                        None,
                        select_span,
                    )?;
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
    ) -> Result<PlannedRelation, PlannerError> {
        match item {
            FromItem::Table { name, alias, span } => {
                let table = self.name_resolver.resolve_table(name, *span)?.clone();
                let mut scope_table = table.clone();
                if let Some(alias) = alias {
                    scope_table.name = alias.clone();
                }
                let schema = table.columns.clone();
                Ok(PlannedRelation {
                    plan: LogicalPlan::Scan {
                        table: name.clone(),
                        projection: Projection::All(
                            schema.iter().map(|col| col.name.clone()).collect(),
                        ),
                    },
                    schema,
                    scope: vec![ScopedTable::new(scope_table, start_index)],
                })
            }
            FromItem::Join {
                left,
                right,
                join_type,
                condition,
                using,
                natural,
                span,
            } => {
                let left_relation = self.plan_from_item(left, start_index, outer_scope)?;
                let right_relation = self.plan_from_item(
                    right,
                    start_index + left_relation.schema.len(),
                    outer_scope,
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
                    let typed = self.infer_expr_with_scope(expr, &expr_scope)?;
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
                self.combine_join_relation(
                    left_relation,
                    right_relation,
                    map_join_type(*join_type),
                    typed_condition,
                    using,
                    *span,
                )
            }
            FromItem::Derived {
                subquery,
                alias,
                span,
            } => {
                let crate::ast::StatementKind::Select(select) = &subquery.kind else {
                    return Err(PlannerError::unsupported_feature(
                        "non-SELECT derived table",
                        "v0.6.0-subquery Phase 6",
                        *span,
                    ));
                };
                let mut relation = self.plan_select_relation(select, outer_scope)?;
                let alias = alias.clone().ok_or_else(|| {
                    PlannerError::invalid_expression("derived table requires an alias".to_string())
                })?;
                relation.plan = LogicalPlan::Project {
                    input: Box::new(relation.plan),
                    projection: Projection::All(
                        relation.schema.iter().map(|col| col.name.clone()).collect(),
                    ),
                };
                relation.scope = vec![ScopedTable::new(
                    TableMetadata::new(alias, relation.schema.clone()),
                    start_index,
                )];
                Ok(relation)
            }
        }
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
        let mut schema = left.schema.clone();
        schema.extend(right.schema.clone());
        let mut scope = left.scope.clone();
        let mut right_scope = right.scope.clone();
        if let Some(columns) = &using {
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
    ) -> Result<TypedExpr, PlannerError> {
        self.type_checker
            .infer_type_with_scope(expr, scope, &|stmt, outer_scope| {
                let crate::ast::StatementKind::Select(select) = &stmt.kind else {
                    return Err(PlannerError::unsupported_feature(
                        "non-SELECT subquery",
                        "v0.6.0-subquery Phase 6",
                        stmt.span(),
                    ));
                };
                let relation = self.plan_select_relation(select, outer_scope)?;
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
                    let typed_expr = self.infer_expr_with_scope(expr, scope)?;
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
    ) -> Result<Vec<SortExpr>, PlannerError> {
        let mut sort_exprs = Vec::new();
        for order_expr in order_by {
            let typed_expr = self.infer_expr_with_scope(&order_expr.expr, scope)?;
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
            .map(|items| items.iter().any(expr_contains_aggregate))
            .unwrap_or(false)
            || stmt
                .having
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
            for expr in items {
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

    fn build_group_keys_with_scope(
        &self,
        stmt: &Select,
        scope: &[ScopedTable],
    ) -> Result<Vec<TypedExpr>, PlannerError> {
        let mut keys = Vec::new();
        if let Some(items) = &stmt.group_by {
            for expr in items {
                let typed = self.infer_expr_with_scope(expr, scope)?;
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
                    let typed = self.infer_expr_with_scope(expr, scope)?;
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
    ) -> Result<Vec<ProjectedColumn>, PlannerError> {
        let projection = self.build_projection_with_scope(items, schema, scope)?;
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
            } if is_aggregate_function(name) => {
                for arg in args {
                    if typed_expr_contains_aggregate(arg) {
                        return Err(PlannerError::invalid_expression(
                            "nested aggregate functions are not supported".to_string(),
                        ));
                    }
                }
                let (agg, signature) =
                    self.build_aggregate_expr_from_typed(expr, name, args, *distinct, *star)?;
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
            TypedExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_aggregates_from_typed_expr(arg, aggregates, aggregate_map)?;
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

    fn build_aggregate_expr_from_typed(
        &self,
        expr: &TypedExpr,
        name: &str,
        args: &[TypedExpr],
        distinct: bool,
        star: bool,
    ) -> Result<(AggregateExpr, AggregateSignature), PlannerError> {
        let lower = name.to_lowercase();
        match lower.as_str() {
            "count" => {
                if star {
                    let agg = AggregateExpr::count_star();
                    let signature = aggregate_signature(name, distinct, star, None, None, expr);
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
                };
                let signature =
                    aggregate_signature(name, distinct, star, Some(&args[0]), None, expr);
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
                };
                let signature = aggregate_signature(name, distinct, star, Some(arg), None, expr);
                Ok((agg, signature))
            }
            "total" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Total,
                    arg: Some(arg.clone()),
                    distinct: false,
                    result_type: ResolvedType::Double,
                };
                let signature = aggregate_signature(name, false, star, Some(arg), None, expr);
                Ok((agg, signature))
            }
            "avg" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Avg,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: ResolvedType::Double,
                };
                let signature = aggregate_signature(name, distinct, star, Some(arg), None, expr);
                Ok((agg, signature))
            }
            "min" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Min,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: arg.resolved_type.clone(),
                };
                let signature = aggregate_signature(name, distinct, star, Some(arg), None, expr);
                Ok((agg, signature))
            }
            "max" => {
                let arg = self.require_single_aggregate_arg(args, expr.span)?;
                let agg = AggregateExpr {
                    function: AggregateFunction::Max,
                    arg: Some(arg.clone()),
                    distinct,
                    result_type: arg.resolved_type.clone(),
                };
                let signature = aggregate_signature(name, distinct, star, Some(arg), None, expr);
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
    ) -> Result<Projection, PlannerError> {
        let mut columns = Vec::new();
        for col in projected {
            let rewritten =
                self.rewrite_expr_for_aggregate(&col.expr, group_keys, aggregates, output_names)?;
            columns.push(ProjectedColumn {
                expr: rewritten,
                alias: col.alias,
            });
        }
        Ok(Projection::Columns(columns))
    }

    fn rewrite_expr_for_aggregate(
        &self,
        expr: &TypedExpr,
        group_keys: &[TypedExpr],
        aggregates: &[AggregateExpr],
        output_names: &[String],
    ) -> Result<TypedExpr, PlannerError> {
        let group_key_map = build_group_key_map(group_keys);
        let aggregate_map = build_aggregate_map(aggregates);

        rewrite_expr_with_maps(expr, &group_key_map, &aggregate_map, output_names)
    }

    /// Extract a numeric value from a LIMIT or OFFSET expression.
    ///
    /// Currently only supports literal integer values.
    fn extract_limit_value(
        &self,
        expr: &Option<crate::ast::expr::Expr>,
        stmt_span: crate::ast::Span,
    ) -> Result<Option<u64>, PlannerError> {
        match expr {
            None => Ok(None),
            Some(e) => {
                // For now, only support literal integers
                if let crate::ast::expr::ExprKind::Literal {
                    literal: Literal::Number(s),
                } = &e.kind
                {
                    s.parse::<u64>().map(Some).map_err(|_| {
                        PlannerError::type_mismatch("unsigned integer", s.clone(), e.span)
                    })
                } else {
                    Err(PlannerError::unsupported_feature(
                        "non-literal LIMIT/OFFSET",
                        "v0.3.0+",
                        stmt_span,
                    ))
                }
            }
        }
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
                let source = self.plan_select_relation(select, &[])?;
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
        }
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
        if matches!(target_type, ResolvedType::Timestamp)
            && !matches!(
                value.resolved_type,
                ResolvedType::Timestamp | ResolvedType::Null
            )
        {
            TypedExpr::cast(value, ResolvedType::Timestamp, span)
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
}

fn expr_contains_aggregate(expr: &crate::ast::expr::Expr) -> bool {
    use crate::ast::expr::ExprKind;

    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            if is_aggregate_function(name) {
                return true;
            }
            args.iter().any(expr_contains_aggregate)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        ExprKind::UnaryOp { operand, .. } => expr_contains_aggregate(operand),
        ExprKind::Cast { expr, .. } => expr_contains_aggregate(expr),
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
        TypedExprKind::FunctionCall { name, args, .. } => {
            if is_aggregate_function(name) {
                return true;
            }
            args.iter().any(typed_expr_contains_aggregate)
        }
        TypedExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_aggregate(left) || typed_expr_contains_aggregate(right)
        }
        TypedExprKind::UnaryOp { operand, .. } => typed_expr_contains_aggregate(operand),
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
                            first_column.filter(|column| {
                                args.iter().all(|arg| {
                                    matches!(
                                        &arg.kind,
                                        TypedExprKind::ColumnRef { column: other, .. }
                                            if other == *column
                                    )
                                })
                            }).cloned()
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
    left_schema
        .iter()
        .filter(|left| right_schema.iter().any(|right| right.name == left.name))
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
        "count" | "sum" | "total" | "avg" | "min" | "max" | "group_concat" | "string_agg"
    )
}

fn expr_key(expr: &TypedExpr) -> String {
    format!("{:?}", expr.kind)
}

fn aggregate_signature(
    name: &str,
    distinct: bool,
    star: bool,
    arg: Option<&TypedExpr>,
    separator: Option<&String>,
    _expr: &TypedExpr,
) -> AggregateSignature {
    AggregateSignature {
        name: name.to_ascii_lowercase(),
        distinct,
        star,
        arg_key: arg.map(expr_key),
        separator: separator.cloned(),
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
        };
        let signature = AggregateSignature {
            name,
            distinct: agg.distinct,
            star,
            arg_key: arg.map(expr_key),
            separator,
        };
        map.insert(signature, idx);
    }
    map
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
        } if is_aggregate_function(name) => {
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
            } else {
                None
            };
            let signature = AggregateSignature {
                name: name.to_ascii_lowercase(),
                distinct: *distinct,
                star: *star,
                arg_key: args.first().map(expr_key),
                separator,
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
        } => {
            if *distinct || *star {
                return Err(PlannerError::invalid_expression(
                    "DISTINCT/STAR modifiers are only supported for aggregates".to_string(),
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
            Ok(TypedExpr {
                kind: TypedExprKind::FunctionCall {
                    name: name.clone(),
                    args: rewritten_args,
                    distinct: false,
                    star: false,
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
