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
mod select;
pub mod type_checker;
pub mod typed_expr;
pub mod types;

#[cfg(test)]
mod planner_tests;

pub use aggregate_expr::{AggregateExpr, AggregateFunction};
pub use error::PlannerError;
pub use knn_optimizer::{KnnPattern, SortDirection, detect_knn_pattern};
pub use logical_plan::LogicalPlan;
pub use name_resolver::{NameResolver, ResolvedColumn};
pub use type_checker::TypeChecker;
pub use typed_expr::{
    ProjectedColumn, Projection, SortExpr, TypedAssignment, TypedExpr, TypedExprKind,
};
pub use types::ResolvedType;

use crate::ast::ddl::{
    ColumnConstraint, ColumnDef, CreateIndex, CreateTable, DropIndex, DropTable,
};
use crate::ast::dml::{Delete, Insert, OrderByExpr, Select, SelectItem, Update};
use crate::ast::expr::Literal;
use crate::ast::{Statement, StatementKind};
use crate::catalog::{Catalog, ColumnMetadata, IndexMetadata, TableMetadata};
use crate::{DataSourceFormat, TableType};
use std::collections::HashMap;

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
pub struct Planner<'a, C: Catalog> {
    catalog: &'a C,
    name_resolver: NameResolver<'a, C>,
    type_checker: TypeChecker<'a, C>,
}

impl<'a, C: Catalog> Planner<'a, C> {
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

            // DML statements
            StatementKind::Select(sel) => self.plan_select(sel),
            StatementKind::Insert(ins) => self.plan_insert(ins),
            StatementKind::Update(upd) => self.plan_update(upd),
            StatementKind::Delete(del) => self.plan_delete(del),
        }
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
            with_options: stmt.with_options.clone(),
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
            ColumnConstraint::NotNull => {
                meta.not_null = true;
            }
            ColumnConstraint::Null => {
                meta.not_null = false;
            }
            ColumnConstraint::PrimaryKey => {
                meta.primary_key = true;
                meta.not_null = true; // PRIMARY KEY implies NOT NULL
            }
            ColumnConstraint::Unique => {
                meta.unique = true;
            }
            ColumnConstraint::Default(expr) => {
                meta.default = Some(expr.clone());
            }
            ColumnConstraint::WithSpan { kind, .. } => {
                meta = Self::apply_column_constraint(meta, kind);
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
        match constraint {
            ColumnConstraint::PrimaryKey => true,
            ColumnConstraint::WithSpan { kind, .. } => Self::is_primary_key_constraint(kind),
            _ => false,
        }
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
        self.validate_group_by_semantics(
            &stmt.projection,
            &stmt.group_by,
            &stmt.having,
            &stmt.selection,
        )?;

        let has_grouping =
            !stmt.group_by.is_empty() || stmt.having.is_some() || self.has_aggregates(stmt);
        if has_grouping {
            return self.plan_aggregate_select(stmt);
        }

        // 1. Resolve the FROM table
        let table = self
            .name_resolver
            .resolve_table(&stmt.from.name, stmt.from.span)?;

        // 2. Build the projection
        let projection = self.build_projection(&stmt.projection, table)?;

        // 3. Create the base Scan plan
        let mut plan = LogicalPlan::Scan {
            table: table.name.clone(),
            projection,
        };

        // 4. Add Filter if WHERE clause is present
        if let Some(ref selection) = stmt.selection {
            let predicate = self.type_checker.infer_type(selection, table)?;

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

        // 5. Add Sort if ORDER BY clause is present
        if !stmt.order_by.is_empty() {
            let order_by = self.build_sort_exprs(&stmt.order_by, table)?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }

        // 6. Add Limit if LIMIT/OFFSET is present
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = self.extract_limit_value(&stmt.limit, stmt.span)?;
            let offset = self.extract_limit_value(&stmt.offset, stmt.span)?;
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    /// Build the projection for a SELECT statement.
    ///
    /// Handles wildcard expansion and expression type checking.
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

    /// Build sort expressions from ORDER BY clause.
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
                if let crate::ast::expr::ExprKind::Literal(Literal::Number(s)) = &e.kind {
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

        // Validate and type-check each row of values
        let mut typed_values: Vec<Vec<TypedExpr>> = Vec::new();

        for row in &stmt.values {
            // Check column count matches
            if row.len() != columns.len() {
                return Err(PlannerError::column_value_count_mismatch(
                    columns.len(),
                    row.len(),
                    stmt.span,
                ));
            }

            // Type-check each value
            let typed_row = self.type_check_insert_values(row, &columns, table)?;
            typed_values.push(typed_row);
        }

        Ok(LogicalPlan::Insert {
            table: table.name.clone(),
            columns,
            values: typed_values,
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
        // NULL can be assigned to any nullable column
        if value.resolved_type == ResolvedType::Null {
            return Ok(());
        }

        // Check for exact match or implicit conversion compatibility
        if self.types_compatible(&value.resolved_type, target_type) {
            return Ok(());
        }

        Err(PlannerError::type_mismatch(
            target_type.to_string(),
            value.resolved_type.to_string(),
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
            // Vector dimensions must match
            (Vector { dimension: d1, .. }, Vector { dimension: d2, .. }) => d1 == d2,
            _ => false,
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
