//! Name resolution for SQL queries.
//!
//! This module provides functionality for resolving table and column references
//! in SQL statements, validating their existence against the catalog, and
//! expanding wildcards in SELECT statements.

use crate::ast::Span;
use crate::ast::expr::{Expr, ExprKind};
use crate::catalog::{Catalog, TableMetadata};
use crate::planner::error::PlannerError;
use crate::planner::types::ResolvedType;

/// A resolved column reference with full metadata.
///
/// Contains all information needed to access a column after name resolution,
/// including the table name, column name, column index (for efficient access),
/// and the resolved type.
///
/// # Examples
///
/// ```
/// use alopex_sql::planner::name_resolver::ResolvedColumn;
/// use alopex_sql::planner::types::ResolvedType;
///
/// let resolved = ResolvedColumn {
///     table_name: "users".to_string(),
///     column_name: "id".to_string(),
///     column_index: 0,
///     resolved_type: ResolvedType::Integer,
/// };
///
/// assert_eq!(resolved.table_name, "users");
/// assert_eq!(resolved.column_index, 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    /// The table name containing this column.
    pub table_name: String,
    /// The column name.
    pub column_name: String,
    /// The index of the column in the table's column list (0-based).
    pub column_index: usize,
    /// The resolved type of the column.
    pub resolved_type: ResolvedType,
}

/// Name resolver for validating table and column references.
///
/// The `NameResolver` validates SQL identifiers against the catalog,
/// ensuring that referenced tables and columns exist. It also handles
/// wildcard expansion for `SELECT *` queries.
///
/// # Design Notes
///
/// - In v0.1.1, only single-table operations are supported (no JOINs).
/// - The `resolve_column_with_scope` method is prepared for future JOIN support (v0.3.0+).
/// - Wildcard expansion returns columns in table definition order.
///
/// # Examples
///
/// ```ignore
/// use alopex_sql::catalog::MemoryCatalog;
/// use alopex_sql::planner::name_resolver::NameResolver;
///
/// let catalog = MemoryCatalog::new();
/// let resolver = NameResolver::new(&catalog);
///
/// // Resolve a table reference
/// let table = resolver.resolve_table("users", span)?;
/// ```
pub struct NameResolver<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> NameResolver<'a, C> {
    /// Create a new name resolver with the given catalog.
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    /// Resolve a table reference.
    ///
    /// Validates that the table exists in the catalog and returns its metadata.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::TableNotFound` if the table doesn't exist.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let table = resolver.resolve_table("users", span)?;
    /// assert_eq!(table.name, "users");
    /// ```
    pub fn resolve_table(&self, name: &str, span: Span) -> Result<&TableMetadata, PlannerError> {
        self.catalog
            .get_table(name)
            .ok_or_else(|| PlannerError::TableNotFound {
                name: name.to_string(),
                line: span.start.line,
                column: span.start.column,
            })
    }

    /// Resolve a column reference within a single table context.
    ///
    /// Validates that the column exists in the given table and returns its metadata.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::ColumnNotFound` if the column doesn't exist in the table.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let table = resolver.resolve_table("users", span)?;
    /// let column = resolver.resolve_column(table, "id", span)?;
    /// assert_eq!(column.name, "id");
    /// ```
    pub fn resolve_column<'t>(
        &self,
        table: &'t TableMetadata,
        column: &str,
        span: Span,
    ) -> Result<&'t crate::catalog::ColumnMetadata, PlannerError> {
        table
            .get_column(column)
            .ok_or_else(|| PlannerError::ColumnNotFound {
                column: column.to_string(),
                table: table.name.clone(),
                line: span.start.line,
                col: span.start.column,
            })
    }

    /// Resolve a column reference with scope for multiple tables.
    ///
    /// This method supports resolution in contexts with multiple tables (for future JOIN support).
    /// In v0.1.1, `tables` should always contain exactly one element.
    ///
    /// # Resolution Rules
    ///
    /// 1. If `table_qualifier` is specified (e.g., `users.id`), resolve from that table only.
    /// 2. If `table_qualifier` is `None`:
    ///    - If the column exists in exactly one table, resolve it.
    ///    - If the column exists in multiple tables, return `AmbiguousColumn` error.
    ///    - If the column doesn't exist in any table, return `ColumnNotFound` error.
    ///
    /// # Errors
    ///
    /// - `PlannerError::TableNotFound` if the specified table qualifier doesn't match any table.
    /// - `PlannerError::ColumnNotFound` if the column doesn't exist in the resolved table(s).
    /// - `PlannerError::AmbiguousColumn` if the column exists in multiple tables without qualification.
    pub fn resolve_column_with_scope(
        &self,
        tables: &[&TableMetadata],
        table_qualifier: Option<&str>,
        column: &str,
        span: Span,
    ) -> Result<ResolvedColumn, PlannerError> {
        if let Some(qualifier) = table_qualifier {
            // Explicit table qualifier - find the matching table
            let table = tables.iter().find(|t| t.name == qualifier).ok_or_else(|| {
                PlannerError::TableNotFound {
                    name: qualifier.to_string(),
                    line: span.start.line,
                    column: span.start.column,
                }
            })?;

            let column_index =
                table
                    .get_column_index(column)
                    .ok_or_else(|| PlannerError::ColumnNotFound {
                        column: column.to_string(),
                        table: table.name.clone(),
                        line: span.start.line,
                        col: span.start.column,
                    })?;

            let column_meta = &table.columns[column_index];
            Ok(ResolvedColumn {
                table_name: table.name.clone(),
                column_name: column.to_string(),
                column_index,
                resolved_type: column_meta.data_type.clone(),
            })
        } else {
            // No qualifier - search all tables
            let mut matches: Vec<(&TableMetadata, usize)> = Vec::new();

            for table in tables {
                if let Some(idx) = table.get_column_index(column) {
                    matches.push((table, idx));
                }
            }

            match matches.len() {
                0 => {
                    // Column not found in any table
                    let table_name = tables.first().map(|t| t.name.as_str()).unwrap_or("unknown");
                    Err(PlannerError::ColumnNotFound {
                        column: column.to_string(),
                        table: table_name.to_string(),
                        line: span.start.line,
                        col: span.start.column,
                    })
                }
                1 => {
                    // Unique match
                    let (table, column_index) = matches[0];
                    let column_meta = &table.columns[column_index];
                    Ok(ResolvedColumn {
                        table_name: table.name.clone(),
                        column_name: column.to_string(),
                        column_index,
                        resolved_type: column_meta.data_type.clone(),
                    })
                }
                _ => {
                    // Ambiguous - column exists in multiple tables
                    let table_names: Vec<String> =
                        matches.iter().map(|(t, _)| t.name.clone()).collect();
                    Err(PlannerError::AmbiguousColumn {
                        column: column.to_string(),
                        tables: table_names,
                        line: span.start.line,
                        col: span.start.column,
                    })
                }
            }
        }
    }

    /// Expand a wildcard (`*`) to all column names in definition order.
    ///
    /// Returns a vector of column names from the table in the order they were defined.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let table = resolver.resolve_table("users", span)?;
    /// let columns = resolver.expand_wildcard(table);
    /// // Returns ["id", "name", "email"] in definition order
    /// ```
    pub fn expand_wildcard(&self, table: &TableMetadata) -> Vec<String> {
        table.column_names().into_iter().map(String::from).collect()
    }

    /// Validate all column references within an expression.
    ///
    /// Recursively traverses the expression tree and validates that all column
    /// references exist in the given table.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::ColumnNotFound` if any column reference in the
    /// expression doesn't exist in the table.
    pub fn resolve_expr(&self, expr: &Expr, table: &TableMetadata) -> Result<(), PlannerError> {
        self.resolve_expr_recursive(expr, table)
    }

    /// Internal recursive implementation for expression resolution.
    fn resolve_expr_recursive(
        &self,
        expr: &Expr,
        table: &TableMetadata,
    ) -> Result<(), PlannerError> {
        match &expr.kind {
            ExprKind::ColumnRef {
                table: table_qualifier,
                column,
            } => {
                // If a table qualifier is provided, verify it matches
                if let Some(qualifier) = table_qualifier
                    && qualifier != &table.name
                {
                    return Err(PlannerError::TableNotFound {
                        name: qualifier.clone(),
                        line: expr.span.start.line,
                        column: expr.span.start.column,
                    });
                }

                // Verify the column exists
                self.resolve_column(table, column, expr.span)?;
                Ok(())
            }

            ExprKind::Literal(_) | ExprKind::VectorLiteral(_) => {
                // Literals don't need resolution
                Ok(())
            }

            ExprKind::BinaryOp { left, right, .. } => {
                self.resolve_expr_recursive(left, table)?;
                self.resolve_expr_recursive(right, table)?;
                Ok(())
            }

            ExprKind::UnaryOp { operand, .. } => self.resolve_expr_recursive(operand, table),

            ExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    self.resolve_expr_recursive(arg, table)?;
                }
                Ok(())
            }

            ExprKind::Between {
                expr: e, low, high, ..
            } => {
                self.resolve_expr_recursive(e, table)?;
                self.resolve_expr_recursive(low, table)?;
                self.resolve_expr_recursive(high, table)?;
                Ok(())
            }

            ExprKind::Like {
                expr: e,
                pattern,
                escape,
                ..
            } => {
                self.resolve_expr_recursive(e, table)?;
                self.resolve_expr_recursive(pattern, table)?;
                if let Some(esc) = escape {
                    self.resolve_expr_recursive(esc, table)?;
                }
                Ok(())
            }

            ExprKind::InList { expr: e, list, .. } => {
                self.resolve_expr_recursive(e, table)?;
                for item in list {
                    self.resolve_expr_recursive(item, table)?;
                }
                Ok(())
            }

            ExprKind::IsNull { expr: e, .. } => self.resolve_expr_recursive(e, table),
        }
    }
}

#[cfg(test)]
mod tests;
