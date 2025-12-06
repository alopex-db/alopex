//! Logical plan representation for query execution.
//!
//! This module defines [`LogicalPlan`], which represents the logical structure
//! of a query after parsing and semantic analysis. The logical plan is used
//! by the executor to produce query results.
//!
//! # Plan Structure
//!
//! Logical plans form a tree structure where:
//! - Leaf nodes are typically scans or DDL operations
//! - Internal nodes represent transformations (filter, sort, limit)
//! - DML operations (insert, update, delete) are also represented
//!
//! # Examples
//!
//! ```
//! use alopex_sql::planner::logical_plan::LogicalPlan;
//! use alopex_sql::planner::{Projection, TypedExpr, TypedExprKind, SortExpr};
//! use alopex_sql::planner::types::ResolvedType;
//! use alopex_sql::Span;
//!
//! // SELECT * FROM users ORDER BY name LIMIT 10
//! let scan = LogicalPlan::Scan {
//!     table: "users".to_string(),
//!     projection: Projection::All(vec!["id".to_string(), "name".to_string()]),
//! };
//!
//! let sort = LogicalPlan::Sort {
//!     input: Box::new(scan),
//!     order_by: vec![SortExpr::asc(TypedExpr::column_ref(
//!         "users".to_string(),
//!         "name".to_string(),
//!         1,
//!         ResolvedType::Text,
//!         Span::default(),
//!     ))],
//! };
//!
//! let limit = LogicalPlan::Limit {
//!     input: Box::new(sort),
//!     limit: Some(10),
//!     offset: None,
//! };
//! ```

use crate::catalog::{IndexMetadata, TableMetadata};
use crate::planner::typed_expr::{Projection, SortExpr, TypedAssignment, TypedExpr};

/// Logical query plan representation.
///
/// This enum represents all possible logical operations that can be performed.
/// Plans are organized into three categories:
///
/// 1. **Query Plans**: Read operations (Scan, Filter, Sort, Limit)
/// 2. **DML Plans**: Data modification (Insert, Update, Delete)
/// 3. **DDL Plans**: Schema modification (CreateTable, DropTable, CreateIndex, DropIndex)
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    // === Query Plans ===
    /// Table scan operation.
    ///
    /// Scans all rows from a table with the specified projection.
    /// This is typically the leaf node of query plans.
    Scan {
        /// Table name to scan.
        table: String,
        /// Columns to project (after wildcard expansion).
        projection: Projection,
    },

    /// Filter operation (WHERE clause).
    ///
    /// Filters rows from the input plan based on a predicate.
    Filter {
        /// Input plan to filter.
        input: Box<LogicalPlan>,
        /// Filter predicate (must evaluate to Boolean).
        predicate: TypedExpr,
    },

    /// Sort operation (ORDER BY clause).
    ///
    /// Sorts rows from the input plan based on sort expressions.
    Sort {
        /// Input plan to sort.
        input: Box<LogicalPlan>,
        /// Sort expressions with direction.
        order_by: Vec<SortExpr>,
    },

    /// Limit operation (LIMIT/OFFSET clause).
    ///
    /// Limits the number of rows from the input plan.
    Limit {
        /// Input plan to limit.
        input: Box<LogicalPlan>,
        /// Maximum number of rows to return.
        limit: Option<u64>,
        /// Number of rows to skip.
        offset: Option<u64>,
    },

    // === DML Plans ===
    /// INSERT operation.
    ///
    /// Inserts one or more rows into a table.
    /// When columns are omitted in the SQL statement, the Planner fills in
    /// all columns from TableMetadata in definition order.
    Insert {
        /// Target table name.
        table: String,
        /// Column names (always populated, never empty).
        /// If omitted in SQL, filled from TableMetadata.column_names().
        columns: Vec<String>,
        /// Values to insert (one Vec per row, each value corresponds to a column).
        values: Vec<Vec<TypedExpr>>,
    },

    /// UPDATE operation.
    ///
    /// Updates rows in a table that match an optional filter.
    Update {
        /// Target table name.
        table: String,
        /// Assignments (SET column = value).
        assignments: Vec<TypedAssignment>,
        /// Optional filter predicate (WHERE clause).
        filter: Option<TypedExpr>,
    },

    /// DELETE operation.
    ///
    /// Deletes rows from a table that match an optional filter.
    Delete {
        /// Target table name.
        table: String,
        /// Optional filter predicate (WHERE clause).
        filter: Option<TypedExpr>,
    },

    // === DDL Plans ===
    /// CREATE TABLE operation.
    ///
    /// Creates a new table with the specified metadata.
    CreateTable {
        /// Table metadata (name, columns, constraints).
        table: TableMetadata,
        /// If true, don't error if table already exists.
        if_not_exists: bool,
    },

    /// DROP TABLE operation.
    ///
    /// Drops an existing table.
    DropTable {
        /// Table name to drop.
        name: String,
        /// If true, don't error if table doesn't exist.
        if_exists: bool,
    },

    /// CREATE INDEX operation.
    ///
    /// Creates a new index on a table column.
    CreateIndex {
        /// Index metadata (name, table, column, method, options).
        index: IndexMetadata,
        /// If true, don't error if index already exists.
        if_not_exists: bool,
    },

    /// DROP INDEX operation.
    ///
    /// Drops an existing index.
    DropIndex {
        /// Index name to drop.
        name: String,
        /// If true, don't error if index doesn't exist.
        if_exists: bool,
    },
}

impl LogicalPlan {
    /// Creates a new Scan plan.
    pub fn scan(table: String, projection: Projection) -> Self {
        LogicalPlan::Scan { table, projection }
    }

    /// Creates a new Filter plan.
    pub fn filter(input: LogicalPlan, predicate: TypedExpr) -> Self {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    /// Creates a new Sort plan.
    pub fn sort(input: LogicalPlan, order_by: Vec<SortExpr>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(input),
            order_by,
        }
    }

    /// Creates a new Limit plan.
    pub fn limit(input: LogicalPlan, limit: Option<u64>, offset: Option<u64>) -> Self {
        LogicalPlan::Limit {
            input: Box::new(input),
            limit,
            offset,
        }
    }

    /// Creates a new Insert plan.
    pub fn insert(table: String, columns: Vec<String>, values: Vec<Vec<TypedExpr>>) -> Self {
        LogicalPlan::Insert {
            table,
            columns,
            values,
        }
    }

    /// Creates a new Update plan.
    pub fn update(
        table: String,
        assignments: Vec<TypedAssignment>,
        filter: Option<TypedExpr>,
    ) -> Self {
        LogicalPlan::Update {
            table,
            assignments,
            filter,
        }
    }

    /// Creates a new Delete plan.
    pub fn delete(table: String, filter: Option<TypedExpr>) -> Self {
        LogicalPlan::Delete { table, filter }
    }

    /// Creates a new CreateTable plan.
    pub fn create_table(table: TableMetadata, if_not_exists: bool) -> Self {
        LogicalPlan::CreateTable {
            table,
            if_not_exists,
        }
    }

    /// Creates a new DropTable plan.
    pub fn drop_table(name: String, if_exists: bool) -> Self {
        LogicalPlan::DropTable { name, if_exists }
    }

    /// Creates a new CreateIndex plan.
    pub fn create_index(index: IndexMetadata, if_not_exists: bool) -> Self {
        LogicalPlan::CreateIndex {
            index,
            if_not_exists,
        }
    }

    /// Creates a new DropIndex plan.
    pub fn drop_index(name: String, if_exists: bool) -> Self {
        LogicalPlan::DropIndex { name, if_exists }
    }

    /// Returns the name of this plan variant.
    pub fn name(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Insert { .. } => "Insert",
            LogicalPlan::Update { .. } => "Update",
            LogicalPlan::Delete { .. } => "Delete",
            LogicalPlan::CreateTable { .. } => "CreateTable",
            LogicalPlan::DropTable { .. } => "DropTable",
            LogicalPlan::CreateIndex { .. } => "CreateIndex",
            LogicalPlan::DropIndex { .. } => "DropIndex",
        }
    }

    /// Returns true if this is a query plan (Scan, Filter, Sort, Limit).
    pub fn is_query(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Scan { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Limit { .. }
        )
    }

    /// Returns true if this is a DML plan (Insert, Update, Delete).
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        )
    }

    /// Returns true if this is a DDL plan (CreateTable, DropTable, CreateIndex, DropIndex).
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropIndex { .. }
        )
    }

    /// Returns the input plan if this is a transformation (Filter, Sort, Limit).
    pub fn input(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => Some(input),
            _ => None,
        }
    }

    /// Returns the table name if this plan operates on a single table.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            LogicalPlan::Scan { table, .. }
            | LogicalPlan::Insert { table, .. }
            | LogicalPlan::Update { table, .. }
            | LogicalPlan::Delete { table, .. } => Some(table),
            LogicalPlan::CreateTable { table, .. } => Some(&table.name),
            LogicalPlan::DropTable { name, .. } => Some(name),
            LogicalPlan::CreateIndex { index, .. } => Some(&index.table),
            LogicalPlan::DropIndex { .. } => None,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.table_name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::Literal;
    use crate::ast::span::Span;
    use crate::catalog::ColumnMetadata;
    use crate::planner::typed_expr::ProjectedColumn;
    use crate::planner::types::ResolvedType;

    fn create_test_table_metadata() -> TableMetadata {
        TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true)
                    .with_not_null(true),
                ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
                ColumnMetadata::new("email", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".to_string()])
    }

    #[test]
    fn test_scan_plan() {
        let plan = LogicalPlan::scan(
            "users".to_string(),
            Projection::All(vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
            ]),
        );

        assert_eq!(plan.name(), "Scan");
        assert!(plan.is_query());
        assert!(!plan.is_dml());
        assert!(!plan.is_ddl());
        assert_eq!(plan.table_name(), Some("users"));
        assert!(plan.input().is_none());
    }

    #[test]
    fn test_filter_plan() {
        let scan = LogicalPlan::scan("users".to_string(), Projection::All(vec![]));
        let predicate = TypedExpr::column_ref(
            "users".to_string(),
            "id".to_string(),
            0,
            ResolvedType::Integer,
            Span::default(),
        );

        let plan = LogicalPlan::filter(scan, predicate);

        assert_eq!(plan.name(), "Filter");
        assert!(plan.is_query());
        assert!(plan.input().is_some());
        assert_eq!(plan.table_name(), Some("users"));
    }

    #[test]
    fn test_sort_plan() {
        let scan = LogicalPlan::scan("users".to_string(), Projection::All(vec![]));
        let sort_expr = SortExpr::asc(TypedExpr::column_ref(
            "users".to_string(),
            "name".to_string(),
            1,
            ResolvedType::Text,
            Span::default(),
        ));

        let plan = LogicalPlan::sort(scan, vec![sort_expr]);

        assert_eq!(plan.name(), "Sort");
        assert!(plan.is_query());
    }

    #[test]
    fn test_limit_plan() {
        let scan = LogicalPlan::scan("users".to_string(), Projection::All(vec![]));
        let plan = LogicalPlan::limit(scan, Some(10), Some(5));

        assert_eq!(plan.name(), "Limit");
        assert!(plan.is_query());

        if let LogicalPlan::Limit { limit, offset, .. } = &plan {
            assert_eq!(*limit, Some(10));
            assert_eq!(*offset, Some(5));
        } else {
            panic!("Expected Limit plan");
        }
    }

    #[test]
    fn test_nested_query_plan() {
        // SELECT * FROM users WHERE id > 5 ORDER BY name LIMIT 10
        let scan = LogicalPlan::scan(
            "users".to_string(),
            Projection::All(vec!["id".to_string(), "name".to_string()]),
        );

        let predicate = TypedExpr::literal(
            Literal::Boolean(true),
            ResolvedType::Boolean,
            Span::default(),
        );
        let filter = LogicalPlan::filter(scan, predicate);

        let sort_expr = SortExpr::asc(TypedExpr::column_ref(
            "users".to_string(),
            "name".to_string(),
            1,
            ResolvedType::Text,
            Span::default(),
        ));
        let sort = LogicalPlan::sort(filter, vec![sort_expr]);

        let limit = LogicalPlan::limit(sort, Some(10), None);

        // Verify the plan tree
        assert_eq!(limit.name(), "Limit");
        assert_eq!(limit.table_name(), Some("users"));

        let sort_plan = limit.input().unwrap();
        assert_eq!(sort_plan.name(), "Sort");

        let filter_plan = sort_plan.input().unwrap();
        assert_eq!(filter_plan.name(), "Filter");

        let scan_plan = filter_plan.input().unwrap();
        assert_eq!(scan_plan.name(), "Scan");
        assert!(scan_plan.input().is_none());
    }

    #[test]
    fn test_insert_plan() {
        let value1 = TypedExpr::literal(
            Literal::Number("1".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );
        let value2 = TypedExpr::literal(
            Literal::String("Alice".to_string()),
            ResolvedType::Text,
            Span::default(),
        );

        let plan = LogicalPlan::insert(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            vec![vec![value1, value2]],
        );

        assert_eq!(plan.name(), "Insert");
        assert!(plan.is_dml());
        assert!(!plan.is_query());
        assert!(!plan.is_ddl());
        assert_eq!(plan.table_name(), Some("users"));

        if let LogicalPlan::Insert {
            table,
            columns,
            values,
        } = &plan
        {
            assert_eq!(table, "users");
            assert_eq!(columns, &vec!["id".to_string(), "name".to_string()]);
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 2);
        } else {
            panic!("Expected Insert plan");
        }
    }

    #[test]
    fn test_update_plan() {
        let assignment = TypedAssignment::new(
            "name".to_string(),
            1,
            TypedExpr::literal(
                Literal::String("Bob".to_string()),
                ResolvedType::Text,
                Span::default(),
            ),
        );

        let filter = TypedExpr::literal(
            Literal::Boolean(true),
            ResolvedType::Boolean,
            Span::default(),
        );

        let plan = LogicalPlan::update("users".to_string(), vec![assignment], Some(filter));

        assert_eq!(plan.name(), "Update");
        assert!(plan.is_dml());
        assert_eq!(plan.table_name(), Some("users"));
    }

    #[test]
    fn test_delete_plan() {
        let filter = TypedExpr::column_ref(
            "users".to_string(),
            "id".to_string(),
            0,
            ResolvedType::Integer,
            Span::default(),
        );

        let plan = LogicalPlan::delete("users".to_string(), Some(filter));

        assert_eq!(plan.name(), "Delete");
        assert!(plan.is_dml());
        assert_eq!(plan.table_name(), Some("users"));
    }

    #[test]
    fn test_create_table_plan() {
        let table = create_test_table_metadata();
        let plan = LogicalPlan::create_table(table, false);

        assert_eq!(plan.name(), "CreateTable");
        assert!(plan.is_ddl());
        assert!(!plan.is_dml());
        assert!(!plan.is_query());
        assert_eq!(plan.table_name(), Some("users"));
    }

    #[test]
    fn test_drop_table_plan() {
        let plan = LogicalPlan::drop_table("users".to_string(), true);

        assert_eq!(plan.name(), "DropTable");
        assert!(plan.is_ddl());
        assert_eq!(plan.table_name(), Some("users"));

        if let LogicalPlan::DropTable { name, if_exists } = &plan {
            assert_eq!(name, "users");
            assert!(*if_exists);
        } else {
            panic!("Expected DropTable plan");
        }
    }

    #[test]
    fn test_create_index_plan() {
        let index = IndexMetadata::new("idx_users_name", "users", "name");
        let plan = LogicalPlan::create_index(index, false);

        assert_eq!(plan.name(), "CreateIndex");
        assert!(plan.is_ddl());
        assert_eq!(plan.table_name(), Some("users"));
    }

    #[test]
    fn test_drop_index_plan() {
        let plan = LogicalPlan::drop_index("idx_users_name".to_string(), false);

        assert_eq!(plan.name(), "DropIndex");
        assert!(plan.is_ddl());
        // DropIndex doesn't have table_name directly
        assert!(plan.table_name().is_none());
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

        let plan = LogicalPlan::scan("users".to_string(), Projection::Columns(vec![col1, col2]));

        if let LogicalPlan::Scan { projection, .. } = &plan {
            assert_eq!(projection.len(), 2);
        } else {
            panic!("Expected Scan plan");
        }
    }
}
