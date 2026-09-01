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
//!     ties: None,
//! };
//! ```

use crate::ast::expr::WindowFrame;
use crate::catalog::{IndexMetadata, TableMetadata};
use crate::planner::aggregate_expr::AggregateExpr;
use crate::planner::typed_expr::{Projection, SortExpr, TypedAssignment, TypedExpr};

/// Function evaluated by a window operator.
#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile(TypedExpr),
    Aggregate(AggregateExpr),
    Value(ValueWindowFunction),
    /// Value at an offset before the current row in the whole partition.
    Lag(OffsetWindowFunction),
    /// Value at an offset after the current row in the whole partition.
    Lead(OffsetWindowFunction),
}

/// Value selected from the current row's effective window frame.
#[derive(Debug, Clone)]
// `TypedExpr` grew with the issue #148 aggregate clauses; every variant holds
// at least one TypedExpr, so boxing `nth` alone buys nothing structural.
#[allow(clippy::large_enum_variant)]
pub enum ValueWindowFunction {
    FirstValue(TypedExpr),
    LastValue(TypedExpr),
    NthValue { value: TypedExpr, nth: TypedExpr },
}

/// Arguments shared by the positional `LAG` and `LEAD` window functions.
///
/// Offset and default expressions are evaluated against the current row. The
/// value expression is evaluated against the addressed partition row. Unlike
/// aggregate windows, these functions do not restrict lookup to the current
/// aggregate frame.
#[derive(Debug, Clone)]
pub struct OffsetWindowFunction {
    pub value: TypedExpr,
    pub offset: Option<TypedExpr>,
    pub default: Option<TypedExpr>,
}

/// A planned window expression and its partition/order specification.
#[derive(Debug, Clone)]
pub struct WindowExpr {
    pub function: WindowFunction,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortExpr>,
    pub frame: Option<WindowFrame>,
    pub result_type: crate::planner::types::ResolvedType,
}

/// JOIN type for logical and physical execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Set operation applied to two query inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

/// Hard execution bounds for a recursive common table expression.
///
/// Recursive evaluation is deliberately bounded even when the SQL uses
/// `UNION ALL`, where a repeated row is semantically significant and cannot
/// be used as an implicit convergence signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursiveCteLimits {
    pub max_iterations: usize,
    pub max_rows: usize,
}

impl Default for RecursiveCteLimits {
    fn default() -> Self {
        Self {
            max_iterations: 1_000,
            max_rows: 100_000,
        }
    }
}

/// FROM-clause table functions Alopex can plan (issue #151).
///
/// The registry is closed: a name outside this set is a planning error rather
/// than a call into an open function namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFunctionKind {
    /// `UNNEST(vector)` — one row per element.
    Unnest,
    /// `UNNEST(array) WITH ORDINALITY` — element and one-based position.
    UnnestWithOrdinality,
    /// `GENERATE_SERIES(start, stop [, step])` over integers.
    GenerateSeries,
    /// `JSON_EACH(json [, path])` — immediate children of JSON TEXT.
    JsonEach,
    /// `JSON_TREE(json [, path])` — recursive JSON TEXT traversal.
    JsonTree,
    /// `FTS_SEARCH(table, column, query [, config])` — ranked full-text matches.
    FtsSearch,
}

impl TableFunctionKind {
    /// Resolve a FROM-clause function name, case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "UNNEST" => Some(Self::Unnest),
            "GENERATE_SERIES" => Some(Self::GenerateSeries),
            "JSON_EACH" => Some(Self::JsonEach),
            "JSON_TREE" => Some(Self::JsonTree),
            "FTS_SEARCH" => Some(Self::FtsSearch),
            _ => None,
        }
    }

    /// Canonical uppercase spelling used in messages and plan output.
    pub fn name(self) -> &'static str {
        match self {
            Self::Unnest | Self::UnnestWithOrdinality => "UNNEST",
            Self::GenerateSeries => "GENERATE_SERIES",
            Self::JsonEach => "JSON_EACH",
            Self::JsonTree => "JSON_TREE",
            Self::FtsSearch => "FTS_SEARCH",
        }
    }

    /// Default relation name when the item carries no alias (PostgreSQL uses
    /// the lowercase function name).
    pub fn default_relation_name(self) -> &'static str {
        match self {
            Self::Unnest | Self::UnnestWithOrdinality => "unnest",
            Self::GenerateSeries => "generate_series",
            Self::JsonEach => "json_each",
            Self::JsonTree => "json_tree",
            Self::FtsSearch => "fts_search",
        }
    }
}

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
    /// Introspection wrapper around the plan that the executor actually uses.
    Explain {
        /// Execute the wrapped plan and include runtime metrics.
        analyze: bool,
        /// Requested output encoding.
        format: crate::ast::ExplainFormat,
        /// Planned statement being described.
        input: Box<LogicalPlan>,
    },
    /// Runtime configuration or statistics operation.
    Pragma {
        /// PRAGMA name.
        name: String,
        /// Optional assignment value.
        value: Option<crate::ast::PragmaValue>,
    },

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

    /// Inline VALUES rows evaluated once per output row.
    Values {
        /// Type-checked expressions for each row.
        rows: Vec<Vec<TypedExpr>>,
        /// Common output schema inferred column-by-column across all rows.
        schema: Vec<crate::catalog::ColumnMetadata>,
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

    /// Projection boundary.
    ///
    /// Scan keeps the legacy single-table projection path; this node is used
    /// when a relation-producing input such as JOIN or a derived table must be
    /// materialized before being consumed by a parent query.
    Project {
        /// Input plan to project.
        input: Box<LogicalPlan>,
        /// Projection to apply.
        projection: Projection,
    },

    /// JOIN operation.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join type.
        join_type: JoinType,
        /// Optional ON condition.
        condition: Option<TypedExpr>,
        /// Optional USING columns.
        using: Option<Vec<String>>,
    },

    /// LATERAL join (issue #151).
    ///
    /// The right input is a correlated relation: it is planned against the left
    /// row and re-executed once per left row, so it cannot be reordered with or
    /// hoisted above the left side the way [`LogicalPlan::Join`] can.
    LateralJoin {
        /// Left input, executed once.
        left: Box<LogicalPlan>,
        /// Correlated right input, executed once per left row with the left
        /// row supplied as the outer row.
        right: Box<LogicalPlan>,
        /// `Inner`, `Left`, or `Cross`; RIGHT and FULL are rejected in planning.
        join_type: JoinType,
        /// Optional ON condition over the concatenated (left, right) row.
        condition: Option<TypedExpr>,
        /// Output schema of the right input, kept so a LEFT join can pad even
        /// when the left input is empty.
        right_schema: Vec<crate::catalog::ColumnMetadata>,
    },

    /// FROM-clause table function (issue #151).
    TableFunction {
        /// Which function this node evaluates.
        function: TableFunctionKind,
        /// Argument expressions, evaluated against the outer row.
        args: Vec<TypedExpr>,
        /// Output schema.
        schema: Vec<crate::catalog::ColumnMetadata>,
    },

    /// Aggregate operation (GROUP BY / aggregation).
    ///
    /// Aggregates rows from the input plan using group keys and aggregate expressions.
    Aggregate {
        /// Input plan to aggregate.
        input: Box<LogicalPlan>,
        /// Group-by key expressions (empty for global aggregation).
        group_keys: Vec<TypedExpr>,
        /// Aggregate expressions to compute.
        aggregates: Vec<AggregateExpr>,
        /// HAVING filter applied after aggregation.
        having: Option<TypedExpr>,
        /// Projection to apply after aggregation.
        projection: Projection,
        /// Expanded GROUPING SETS masks over `group_keys` (issue #149).
        ///
        /// `None` keeps the pre-grouping-sets single-set behavior. Each mask
        /// covers `group_keys` with key 0 at the most significant of the low
        /// `group_keys.len()` bits; a 1 bit marks the key as excluded from
        /// that grouping set (NULL placeholder in the output). When present,
        /// the aggregate output schema gains a trailing `__grouping_id`
        /// BIGINT column carrying the mask of the producing set.
        grouping_sets: Option<Vec<u64>>,
    },

    /// Window operation preserving every input row and appending one result
    /// column per window expression.
    Window {
        input: Box<LogicalPlan>,
        windows: Vec<WindowExpr>,
    },

    /// UNION, INTERSECT, or EXCEPT over two projection-compatible queries.
    SetOperation {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        operator: SetOperator,
        all: bool,
    },

    /// Materialized fixed-point evaluation for one directly self-recursive
    /// common table expression.
    RecursiveCte {
        name: String,
        anchor: Box<LogicalPlan>,
        recursive_term: Box<LogicalPlan>,
        union_all: bool,
        schema: Vec<crate::catalog::ColumnMetadata>,
        limits: RecursiveCteLimits,
    },

    /// Read the current working-table delta of an enclosing `RecursiveCte`.
    /// The executor resolves this through an explicit per-query context.
    RecursiveReference {
        name: String,
        schema: Vec<crate::catalog::ColumnMetadata>,
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

    /// SELECT DISTINCT ON (expr, ...) deduplication (issue #150).
    ///
    /// Sorts the input by the complete effective sort specification and emits
    /// only the first row of each group of rows whose leading `key_count`
    /// sort keys compare equal (NULL keys compare equal to NULL, D5).
    ///
    /// Invariants established by the planner (docs/sql-distinct-on.md):
    /// - `order_by[..key_count]` covers every deduplicated DISTINCT ON key
    ///   (the user's matching ORDER BY prefix plus implicit ASC NULLS LAST
    ///   keys, D2/D3).
    /// - `order_by[key_count..]` carries the user's ORDER BY tail followed by
    ///   every input column as an ASC NULLS LAST tie-breaker, so the surviving
    ///   row of each group never depends on physical input order (D4).
    /// - The node emits rows already ordered by the effective specification,
    ///   so no additional Sort node is planned above it (D8).
    DistinctOn {
        /// Input plan to deduplicate.
        input: Box<LogicalPlan>,
        /// Number of leading `order_by` entries that form the distinctness key.
        key_count: usize,
        /// Complete effective sort specification (keys, tail, tie-breakers).
        order_by: Vec<SortExpr>,
    },

    /// Limit operation (LIMIT/OFFSET/FETCH clause).
    ///
    /// Limits the number of rows from the input plan. `limit` and `offset`
    /// are concrete values resolved at plan time, so the node can be carried
    /// by the distributed plan contract without re-evaluating expressions.
    Limit {
        /// Input plan to limit.
        input: Box<LogicalPlan>,
        /// Maximum number of rows to return.
        limit: Option<u64>,
        /// Number of rows to skip.
        offset: Option<u64>,
        /// FETCH ... WITH TIES: after `limit` rows, keep emitting rows whose
        /// ORDER BY sort key equals the final emitted row's key (peer rows).
        /// The keys are a copy of the `Sort` node directly beneath this
        /// Limit; `None` means plain ONLY/LIMIT semantics.
        ties: Option<Vec<SortExpr>>,
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

    /// INSERT rows produced by a SELECT query.
    InsertSelect {
        /// Target table name.
        table: String,
        /// Column names (always populated, never empty).
        columns: Vec<String>,
        /// Query that produces one row per inserted row.
        source: Box<LogicalPlan>,
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
        /// Raw WITH options to be validated during execution.
        with_options: Vec<(String, String)>,
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
    /// Render the stable, secret-free machine-readable explain contract.
    pub fn explain_json(
        &self,
        analyze: bool,
        elapsed_ns: Option<u64>,
        rows: Option<u64>,
    ) -> String {
        let knn_pattern_applied = super::knn_optimizer::detect_knn_pattern(self).is_some();
        serde_json::json!({
            "schema": "alopex.explain",
            "version": 1,
            "analyze": analyze,
            "logical_plan": self.explain_node(),
            "physical_plan": {
                "engine": "logical-direct",
                "root": self.explain_node(),
            },
            "distributed_plan": {
                "mode": "single-node",
                "fragments": [{"id": 0, "placement": "local", "root": self.name()}],
            },
            "optimizer_rules": [{
                "name": "knn_pattern_detection",
                "status": "integrated",
                "applied": knn_pattern_applied,
            }],
            "metrics": {
                "elapsed_ns": elapsed_ns,
                "rows": rows,
            },
        })
        .to_string()
    }

    /// Render the human-readable explain tree. This format is not stable across releases.
    pub fn explain_text(&self, elapsed_ns: Option<u64>, rows: Option<u64>) -> String {
        fn write_tree(plan: &LogicalPlan, depth: usize, output: &mut String) {
            output.push_str(&"  ".repeat(depth));
            output.push_str(plan.name());
            if let Some(table) = plan.table_name() {
                output.push_str(" table=");
                output.push_str(table);
            }
            output.push('\n');
            for child in plan.explain_children() {
                write_tree(child, depth + 1, output);
            }
        }

        let mut output = String::new();
        write_tree(self, 0, &mut output);
        if let Some(value) = elapsed_ns {
            output.push_str(&format!("elapsed_ns={value}\n"));
        }
        if let Some(value) = rows {
            output.push_str(&format!("rows={value}\n"));
        }
        output
    }

    fn explain_node(&self) -> serde_json::Value {
        serde_json::json!({
            "node": self.name(),
            "table": self.table_name(),
            "children": self.explain_children().into_iter().map(Self::explain_node).collect::<Vec<_>>(),
        })
    }

    fn explain_children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Explain { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::DistinctOn { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::InsertSelect { source: input, .. } => vec![input],
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::LateralJoin { left, right, .. }
            | LogicalPlan::SetOperation { left, right, .. } => vec![left, right],
            LogicalPlan::RecursiveCte {
                anchor,
                recursive_term,
                ..
            } => vec![anchor, recursive_term],
            _ => Vec::new(),
        }
    }

    pub fn operation_name(&self) -> &'static str {
        match self {
            LogicalPlan::Explain { .. } => "EXPLAIN",
            LogicalPlan::Pragma { .. } => "PRAGMA",
            LogicalPlan::Scan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::TableFunction { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::RecursiveCte { .. }
            | LogicalPlan::RecursiveReference { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::DistinctOn { .. }
            | LogicalPlan::Limit { .. } => "SELECT",
            LogicalPlan::Insert { .. } => "INSERT",
            LogicalPlan::InsertSelect { .. } => "INSERT",
            LogicalPlan::Update { .. } => "UPDATE",
            LogicalPlan::Delete { .. } => "DELETE",
            LogicalPlan::CreateTable { .. } => "CREATE TABLE",
            LogicalPlan::DropTable { .. } => "DROP TABLE",
            LogicalPlan::CreateIndex { .. } => "CREATE INDEX",
            LogicalPlan::DropIndex { .. } => "DROP INDEX",
        }
    }

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

    /// Creates a new Project plan.
    pub fn project(input: LogicalPlan, projection: Projection) -> Self {
        LogicalPlan::Project {
            input: Box::new(input),
            projection,
        }
    }

    /// Creates a new Join plan.
    pub fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        condition: Option<TypedExpr>,
        using: Option<Vec<String>>,
    ) -> Self {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            using,
        }
    }

    /// Creates a new Aggregate plan without grouping sets.
    pub fn aggregate(
        input: LogicalPlan,
        group_keys: Vec<TypedExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<TypedExpr>,
        projection: Projection,
    ) -> Self {
        LogicalPlan::Aggregate {
            input: Box::new(input),
            group_keys,
            aggregates,
            having,
            projection,
            grouping_sets: None,
        }
    }

    /// Creates a new Sort plan.
    pub fn sort(input: LogicalPlan, order_by: Vec<SortExpr>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(input),
            order_by,
        }
    }

    /// Creates a new DistinctOn plan.
    pub fn distinct_on(input: LogicalPlan, key_count: usize, order_by: Vec<SortExpr>) -> Self {
        LogicalPlan::DistinctOn {
            input: Box::new(input),
            key_count,
            order_by,
        }
    }

    /// Creates a new Limit plan (plain ONLY/LIMIT semantics, no ties).
    pub fn limit(input: LogicalPlan, limit: Option<u64>, offset: Option<u64>) -> Self {
        LogicalPlan::Limit {
            input: Box::new(input),
            limit,
            offset,
            ties: None,
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
    pub fn create_table(
        table: TableMetadata,
        if_not_exists: bool,
        with_options: Vec<(String, String)>,
    ) -> Self {
        LogicalPlan::CreateTable {
            table,
            if_not_exists,
            with_options,
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
            LogicalPlan::Explain { .. } => "Explain",
            LogicalPlan::Pragma { .. } => "Pragma",
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Values { .. } => "Values",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::LateralJoin { .. } => "LateralJoin",
            LogicalPlan::TableFunction { .. } => "TableFunction",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Window { .. } => "Window",
            LogicalPlan::SetOperation { .. } => "SetOperation",
            LogicalPlan::RecursiveCte { .. } => "RecursiveCte",
            LogicalPlan::RecursiveReference { .. } => "RecursiveReference",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::DistinctOn { .. } => "DistinctOn",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Insert { .. } => "Insert",
            LogicalPlan::InsertSelect { .. } => "InsertSelect",
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
            LogicalPlan::Explain { .. }
                | LogicalPlan::Scan { .. }
                | LogicalPlan::Values { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Project { .. }
                | LogicalPlan::Join { .. }
                | LogicalPlan::LateralJoin { .. }
                | LogicalPlan::TableFunction { .. }
                | LogicalPlan::Aggregate { .. }
                | LogicalPlan::Window { .. }
                | LogicalPlan::SetOperation { .. }
                | LogicalPlan::RecursiveCte { .. }
                | LogicalPlan::RecursiveReference { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::DistinctOn { .. }
                | LogicalPlan::Limit { .. }
        )
    }

    /// Returns true if this is a DML plan (Insert, Update, Delete).
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Insert { .. }
                | LogicalPlan::InsertSelect { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
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
                | LogicalPlan::Pragma { .. }
        )
    }

    /// Returns the input plan if this is a transformation (Filter, Aggregate, Sort, Limit).
    pub fn input(&self) -> Option<&LogicalPlan> {
        match self {
            LogicalPlan::Explain { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::DistinctOn { input, .. }
            | LogicalPlan::Limit { input, .. } => Some(input),
            LogicalPlan::Join { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::TableFunction { .. }
            | LogicalPlan::Values { .. } => None,
            LogicalPlan::SetOperation { .. } => None,
            LogicalPlan::RecursiveCte { .. } | LogicalPlan::RecursiveReference { .. } => None,
            _ => None,
        }
    }

    /// Returns the table name if this plan operates on a single table.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            LogicalPlan::Explain { input, .. } => input.table_name(),
            LogicalPlan::Scan { table, .. }
            | LogicalPlan::Insert { table, .. }
            | LogicalPlan::InsertSelect { table, .. }
            | LogicalPlan::Update { table, .. }
            | LogicalPlan::Delete { table, .. } => Some(table),
            LogicalPlan::CreateTable { table, .. } => Some(&table.name),
            LogicalPlan::DropTable { name, .. } => Some(name),
            LogicalPlan::CreateIndex { index, .. } => Some(&index.table),
            LogicalPlan::DropIndex { .. } => None,
            LogicalPlan::Pragma { .. } => None,
            LogicalPlan::Values { .. } => None,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::DistinctOn { input, .. }
            | LogicalPlan::Limit { input, .. } => input.table_name(),
            LogicalPlan::Join { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::TableFunction { .. } => None,
            LogicalPlan::SetOperation { left, right, .. } => left
                .table_name()
                .filter(|name| right.table_name() == Some(*name)),
            LogicalPlan::RecursiveCte { .. } | LogicalPlan::RecursiveReference { .. } => None,
        }
    }

    /// Returns whether this plan tree contains a JOIN boundary.
    ///
    /// The normal local planner/executor continues to support JOIN.  Consumers
    /// with a deliberately closed execution catalog (such as distributed
    /// reads) can use this structural fact to reject it before any transport is
    /// opened rather than trying to infer it from a table name.
    pub fn contains_join(&self) -> bool {
        match self {
            LogicalPlan::Explain { input, .. } => input.contains_join(),
            LogicalPlan::Join { .. } | LogicalPlan::LateralJoin { .. } => true,
            LogicalPlan::SetOperation { left, right, .. } => {
                left.contains_join() || right.contains_join()
            }
            LogicalPlan::RecursiveCte {
                anchor,
                recursive_term,
                ..
            } => anchor.contains_join() || recursive_term.contains_join(),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::DistinctOn { input, .. }
            | LogicalPlan::Limit { input, .. } => input.contains_join(),
            _ => false,
        }
    }

    /// Returns whether this plan tree contains a set-operation boundary.
    pub fn contains_set_operation(&self) -> bool {
        match self {
            LogicalPlan::Explain { input, .. } => input.contains_set_operation(),
            LogicalPlan::SetOperation { .. } | LogicalPlan::RecursiveCte { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::DistinctOn { input, .. }
            | LogicalPlan::Limit { input, .. } => input.contains_set_operation(),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::LateralJoin { left, right, .. } => {
                left.contains_set_operation() || right.contains_set_operation()
            }
            _ => false,
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
        let plan = LogicalPlan::create_table(table, false, vec![]);

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
        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
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
