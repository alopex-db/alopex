//! Versioned, closed classifier for the v0.8 distributed-read SQL surface.
//!
//! Classification is intentionally conservative.  It records only the public
//! shape that is eligible for a future normalized wire descriptor; it never
//! serializes a [`LogicalPlan`](crate::planner::LogicalPlan) or uses a
//! permissive "is query" predicate.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::planner::{
    AggregateFunction, LogicalPlan, Projection, TableReference, TableReferenceAccess, TypedExpr,
    TypedExprKind,
};

/// Version identifier embedded in all accepted remote-read descriptors.
pub const REMOTE_READ_CATALOG_VERSION: &str = "v0.8";

/// Scalar function identities explicitly admitted to the v0.8 remote catalog.
/// Adding a scalar signature elsewhere does not extend remote support until it
/// is deliberately added here and appears in the public coverage matrix.
pub const REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS: &[&str] = &[
    "abs",
    "sign",
    "round",
    "floor",
    "ceil",
    "ceiling",
    "trunc",
    "mod",
    "power",
    "pow",
    "sqrt",
    "exp",
    "ln",
    "log",
    "log10",
    "extract",
    "date_part",
    "date_trunc",
    "to_char",
    "to_timestamp",
    "strftime",
    "julianday",
    "unixepoch",
    "cbrt",
    "cot",
    "log2",
    "acosh",
    "asinh",
    "atanh",
    "cosh",
    "sinh",
    "tanh",
    "isnan",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "degrees",
    "radians",
    "pi",
    "sha256",
    "md5",
    "simhash",
    "hamming_distance",
    "hex",
    "unhex",
    "encode",
    "decode",
    "length",
    "char_length",
    "octet_length",
    "ascii",
    "chr",
    "bit_length",
    "starts_with",
    "ends_with",
    "translate",
    "levenshtein",
    "upper",
    "lower",
    "initcap",
    "substr",
    "left",
    "right",
    "trim",
    "ltrim",
    "rtrim",
    "replace",
    "instr",
    "strpos",
    "concat",
    "concat_ws",
    "repeat",
    "reverse",
    "lpad",
    "rpad",
    "split_part",
    "regexp_replace",
    "regexp_match",
    "regexp_matches",
    "regexp_like",
    "coalesce",
    "nullif",
    "ifnull",
    "iif",
    "greatest",
    "least",
    "typeof",
    "pg_typeof",
    "quote",
];

/// Registered scalar identities intentionally excluded from remote execution.
pub const REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS: &[&str] = &[
    "vector_similarity",
    "vector_distance",
    "vector_dims",
    "vector_norm",
    "random",
    "now",
    "current_timestamp",
    "gen_random_uuid",
    "uuidv7",
    "memory_stats",
    "io_stats",
    "clear_cache",
];

/// A complete pre-routing classification for a planned SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteReadClassification {
    /// The plan belongs to the explicitly supported v0.8 catalog.
    Supported(RemoteReadDescriptor),
    /// The statement remains valid for the legacy local executor, but is not
    /// eligible for remote/multi-range execution.
    LocalOnly(RemoteReadRejection),
    /// A cluster read request must fail before opening a transport session.
    UnsupportedRemote(RemoteReadRejection),
}

/// Bounded descriptor metadata derived from an accepted logical plan.
///
/// This is deliberately not executable SQL and carries no private planner
/// tree.  The later transport task expands it into an expression codec with a
/// separate compatibility test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReadDescriptor {
    pub catalog_version: String,
    pub table: String,
    pub shape: RemoteReadShape,
    pub operators: RemoteReadOperators,
}

/// The closed high-level result shape accepted by the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteReadShape {
    Rows,
    Aggregate { aggregates: Vec<RemoteAggregate> },
}

/// Aggregate identities available to the v0.8 catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAggregate {
    Count,
    Sum,
    Total,
    Avg,
    Min,
    Max,
    GroupConcat,
    StringAgg,
}

/// Closed modifiers which a later normalized descriptor must preserve.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReadOperators {
    pub filter: bool,
    pub projection: bool,
    pub order_by: bool,
    pub limit: bool,
    pub offset: bool,
    pub group_by: bool,
    pub having: bool,
    pub deterministic_scalar: bool,
    pub aggregate_distinct: bool,
}

/// Stable explanation for a non-supported remote classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReadRejection {
    pub code: String,
    pub reason: String,
}

/// Public support status emitted by the coverage matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteReadCoverageStatus {
    RemoteSupported,
    LocalOnly,
    PreExecutionRejection,
}

/// One stable public category row in the v0.8 coverage matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemoteReadCoverageEntry {
    pub id: &'static str,
    pub public_surface: &'static str,
    #[serde(skip_serializing)]
    pub identities: &'static [&'static str],
    pub remote_status: RemoteReadCoverageStatus,
    pub prerequisite: &'static str,
    pub normal_outcome: &'static str,
    pub failure_outcome: &'static str,
}

impl RemoteReadRejection {
    fn local_only(code: &str, reason: &str) -> RemoteReadClassification {
        RemoteReadClassification::LocalOnly(Self {
            code: code.to_string(),
            reason: reason.to_string(),
        })
    }

    fn unsupported(code: &str, reason: &str) -> RemoteReadClassification {
        RemoteReadClassification::UnsupportedRemote(Self {
            code: code.to_string(),
            reason: reason.to_string(),
        })
    }
}

/// Closed v0.8 remote-read catalog.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteReadCatalogV0_8;

impl RemoteReadCatalogV0_8 {
    /// Classifies a fully planned statement before routing or transport.
    pub fn classify(
        &self,
        plan: &LogicalPlan,
        table_references: &[TableReference],
    ) -> RemoteReadClassification {
        classify(plan, table_references)
    }

    /// Returns every public SQL category from the same closed catalog used by
    /// the classifier.  No local feature is inferred as remotely supported.
    pub fn coverage_entries(&self) -> Vec<RemoteReadCoverageEntry> {
        coverage_entries()
    }
}

/// Returns every public SQL category from the v0.8 closed catalog.
pub fn coverage_entries() -> Vec<RemoteReadCoverageEntry> {
    use RemoteReadCoverageStatus::{LocalOnly, PreExecutionRejection, RemoteSupported};

    vec![
        RemoteReadCoverageEntry {
            id: "select.one_table.read_only",
            public_surface: "one logical table SELECT with projection, WHERE, ORDER BY, LIMIT, OFFSET",
            identities: &[
                "select",
                "projection",
                "where",
                "order_by",
                "limit",
                "offset",
            ],
            remote_status: RemoteSupported,
            prerequisite: "closed catalog, fenced retained read point, authorized range targets",
            normal_outcome: "prepared globally equivalent result",
            failure_outcome: "classified pre-execution or routed-read failure; no local fallback",
        },
        RemoteReadCoverageEntry {
            id: "select.aggregate.basic",
            public_surface: "one-table COUNT, SUM, TOTAL, AVG, MIN, MAX with GROUP BY/HAVING/DISTINCT",
            identities: &[
                "count", "sum", "total", "avg", "min", "max", "group_by", "having", "distinct",
            ],
            remote_status: RemoteSupported,
            prerequisite: "closed aggregate descriptor and global finalization budget",
            normal_outcome: "prepared globally equivalent aggregate result",
            failure_outcome: "classified pre-execution or global preparation failure",
        },
        RemoteReadCoverageEntry {
            id: "select.aggregate.ordered_string",
            public_surface: "one-table GROUP_CONCAT and STRING_AGG with global ordered replay",
            identities: &["group_concat", "string_agg"],
            remote_status: RemoteSupported,
            prerequisite: "closed aggregate descriptor and ordered raw-value finalization budget",
            normal_outcome: "prepared globally ordered aggregate result",
            failure_outcome: "classified pre-execution or global preparation failure",
        },
        RemoteReadCoverageEntry {
            id: "scalar.deterministic",
            public_surface: "explicit deterministic scalar function list in RemoteReadCatalogV0_8",
            identities: REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS,
            remote_status: RemoteSupported,
            prerequisite: "one-table SELECT and each function identity listed by the v0.8 catalog",
            normal_outcome: "evaluated as part of a prepared remotely supported read",
            failure_outcome: "unlisted function is rejected before transport",
        },
        RemoteReadCoverageEntry {
            id: "scalar.local_only",
            public_surface: "vector, random/UUID, statistics, and cache-control scalar functions",
            identities: REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS,
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "remote profile receives an explicit local-only classification",
        },
        RemoteReadCoverageEntry {
            id: "statement.ddl",
            public_surface: "CREATE/DROP TABLE and CREATE/DROP INDEX",
            identities: &["create_table", "drop_table", "create_index", "drop_index"],
            remote_status: PreExecutionRejection,
            prerequisite: "local schema-management workflow",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "ddl_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "statement.dml",
            public_surface: "INSERT, UPDATE, DELETE",
            identities: &["insert", "update", "delete"],
            remote_status: PreExecutionRejection,
            prerequisite: "local transaction workflow",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "dml_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "statement.pragma",
            public_surface: "PRAGMA",
            identities: &["pragma"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "pragma_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.join",
            public_surface: "JOIN",
            identities: &[
                "inner_join",
                "left_join",
                "right_join",
                "full_join",
                "cross_join",
            ],
            remote_status: PreExecutionRejection,
            prerequisite: "local execution profile",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "join_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.subquery",
            public_surface: "scalar, IN, EXISTS, and quantified subqueries",
            identities: &["scalar_subquery", "in_subquery", "exists", "quantified"],
            remote_status: PreExecutionRejection,
            prerequisite: "local execution profile",
            normal_outcome: "v0.7.4 local SQL behavior",
            failure_outcome: "subquery_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.compound_window",
            public_surface: "compound and window query forms",
            identities: &["compound_query", "window_expression"],
            remote_status: PreExecutionRejection,
            prerequisite: "a future remote catalog version",
            normal_outcome: "not a v0.8 remote-read form",
            failure_outcome: "function_not_in_remote_catalog before transport",
        },
        RemoteReadCoverageEntry {
            id: "scalar.standard_predicate",
            public_surface: "truth, distinctness, and row-value predicates",
            identities: &["truth_predicate", "is_distinct_from", "row_comparison"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "three-valued evaluation by the local executor",
            failure_outcome: "standard_predicate_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "scalar.try_cast",
            public_surface: "TRY_CAST safe conversion",
            identities: &["try_cast"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "NULL-on-conversion-failure evaluation by the local executor",
            failure_outcome: "try_cast_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "pagination.fetch_with_ties",
            public_surface: "FETCH ... WITH TIES peer-preserving row limits",
            identities: &["fetch_with_ties"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "peer-preserving limit evaluation by the local executor",
            failure_outcome: "fetch_with_ties_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.distinct_on",
            public_surface: "SELECT DISTINCT ON deterministic first-row deduplication",
            identities: &["distinct_on"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "deterministic per-key first-row evaluation by the local executor",
            failure_outcome: "distinct_on_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "scalar.aggregate_filter",
            public_surface: "aggregate FILTER (WHERE ...) per-aggregate row filtering",
            identities: &["aggregate_filter"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "per-aggregate predicate filtering by the local executor",
            failure_outcome: "aggregate_filter_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "scalar.ordered_aggregate",
            public_surface: "aggregate-local ORDER BY and WITHIN GROUP ordered-set aggregates",
            identities: &["aggregate_order_by", "within_group", "percentile_disc"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "ordered aggregate evaluation by the local executor",
            failure_outcome: "ordered_aggregate_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "aggregate.grouping_sets",
            public_surface: "GROUPING SETS / ROLLUP / CUBE multi-set aggregation",
            identities: &["rollup", "cube", "grouping_sets", "grouping", "grouping_id"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "single-pass multi-set aggregation by the local executor",
            failure_outcome: "grouping_sets_local_only before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.lateral_join",
            public_surface: "LATERAL joins over a correlated relation",
            identities: &["lateral", "cross_join_lateral", "left_join_lateral"],
            remote_status: PreExecutionRejection,
            prerequisite: "local execution profile",
            normal_outcome: "per-left-row correlated evaluation by the local executor",
            failure_outcome: "lateral_join_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.table_function",
            public_surface: "FROM-clause table functions",
            identities: &["unnest", "generate_series"],
            remote_status: LocalOnly,
            prerequisite: "local execution profile",
            normal_outcome: "row generation by the local executor",
            failure_outcome: "table_function_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "relation.recursive_cte",
            public_surface: "recursive common table expressions",
            identities: &["with_recursive"],
            remote_status: PreExecutionRejection,
            prerequisite: "local execution profile",
            normal_outcome: "bounded fixed-point evaluation by the local executor",
            failure_outcome: "recursive_cte_not_supported_remote before transport",
        },
        RemoteReadCoverageEntry {
            id: "transaction.multi_statement",
            public_surface: "existing multi-statement Transaction API workflow",
            identities: &["transaction_api"],
            remote_status: LocalOnly,
            prerequisite: "local transaction workflow",
            normal_outcome: "v0.7.4 local transaction behavior",
            failure_outcome: "remote profile receives an explicit pre-execution classification",
        },
    ]
}

/// Classifies a fully planned statement before routing or transport.
pub fn classify(
    plan: &LogicalPlan,
    table_references: &[TableReference],
) -> RemoteReadClassification {
    if let Some(classification) = table_boundary(table_references) {
        return classification;
    }
    if plan.contains_join() {
        return RemoteReadRejection::unsupported(
            "join_not_supported_remote",
            "JOIN is outside the v0.8 remote-read catalog",
        );
    }

    let mut analysis = Analysis::default();
    if let Err(rejection) = validate_plan(plan, &mut analysis) {
        return rejection;
    }
    let table = match single_table(table_references) {
        Some(table) => table,
        None => {
            return RemoteReadRejection::unsupported(
                "single_logical_table_required",
                "remote reads require exactly one physical logical table",
            );
        }
    };
    if analysis.scan_count != 1 {
        return RemoteReadRejection::unsupported(
            "single_logical_table_required",
            "remote reads require exactly one table scan",
        );
    }
    if analysis
        .scan_tables
        .first()
        .is_none_or(|scan_table| scan_table != &table)
    {
        return RemoteReadRejection::unsupported(
            "table_reference_mismatch",
            "planned scan table does not match the routing table reference",
        );
    }

    RemoteReadClassification::Supported(RemoteReadDescriptor {
        catalog_version: REMOTE_READ_CATALOG_VERSION.to_string(),
        table,
        shape: if analysis.aggregates.is_empty() {
            RemoteReadShape::Rows
        } else {
            RemoteReadShape::Aggregate {
                aggregates: analysis.aggregates,
            }
        },
        operators: analysis.operators,
    })
}

fn table_boundary(table_references: &[TableReference]) -> Option<RemoteReadClassification> {
    if table_references
        .iter()
        .any(|reference| reference.access != TableReferenceAccess::Read)
    {
        return Some(RemoteReadRejection::unsupported(
            "read_only_select_required",
            "remote reads require a read-only SELECT",
        ));
    }
    let tables: BTreeSet<_> = table_references
        .iter()
        .map(|reference| reference.table_name.as_str())
        .collect();
    if tables.len() > 1 {
        return Some(RemoteReadRejection::unsupported(
            "single_logical_table_required",
            "remote reads cannot span multiple logical tables",
        ));
    }
    None
}

fn single_table(table_references: &[TableReference]) -> Option<String> {
    table_references
        .first()
        .map(|reference| reference.table_name.clone())
}

#[derive(Debug, Default)]
struct Analysis {
    scan_count: usize,
    scan_tables: Vec<String>,
    aggregates: Vec<RemoteAggregate>,
    operators: RemoteReadOperators,
}

fn validate_plan(
    plan: &LogicalPlan,
    analysis: &mut Analysis,
) -> Result<(), RemoteReadClassification> {
    match plan {
        LogicalPlan::Pragma { .. } => Err(RemoteReadRejection::local_only(
            "pragma_local_only",
            "PRAGMA remains available only to the local executor",
        )),
        LogicalPlan::Insert { .. }
        | LogicalPlan::InsertSelect { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. } => Err(RemoteReadRejection::unsupported(
            "dml_not_supported_remote",
            "DML is outside the read-only remote-read catalog",
        )),
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. } => Err(RemoteReadRejection::unsupported(
            "ddl_not_supported_remote",
            "DDL is outside the read-only remote-read catalog",
        )),
        LogicalPlan::Join { .. } => Err(RemoteReadRejection::unsupported(
            "join_not_supported_remote",
            "JOIN is outside the v0.8 remote-read catalog",
        )),
        LogicalPlan::LateralJoin { .. } => Err(RemoteReadRejection::unsupported(
            "lateral_join_not_supported_remote",
            "LATERAL joins are outside the v0.8 remote-read catalog",
        )),
        LogicalPlan::TableFunction { .. } => Err(RemoteReadRejection::local_only(
            "table_function_not_supported_remote",
            "FROM-clause table functions are evaluated by the local executor",
        )),
        LogicalPlan::Window { .. } => Err(RemoteReadRejection::unsupported(
            "window_not_supported_remote",
            "window functions are outside the v0.8 remote-read catalog",
        )),
        LogicalPlan::SetOperation { .. } => Err(RemoteReadRejection::unsupported(
            "set_operation_not_supported_remote",
            "set operations are outside the v0.8 remote-read catalog",
        )),
        LogicalPlan::Values { .. } => Err(RemoteReadRejection::local_only(
            "values_local_only",
            "VALUES relations are evaluated by the local executor",
        )),
        LogicalPlan::RecursiveCte { .. } | LogicalPlan::RecursiveReference { .. } => {
            Err(RemoteReadRejection::unsupported(
                "recursive_cte_not_supported_remote",
                "recursive common table expressions are outside the v0.8 remote-read catalog",
            ))
        }
        LogicalPlan::Scan { table, projection } => {
            analysis.scan_count += 1;
            analysis.scan_tables.push(table.clone());
            validate_projection(projection, false, analysis)
        }
        LogicalPlan::Filter { input, predicate } => {
            analysis.operators.filter = true;
            validate_expr(predicate, false, analysis)?;
            validate_plan(input, analysis)
        }
        LogicalPlan::Project { input, projection } => {
            validate_plan(input, analysis)?;
            analysis.operators.projection = true;
            validate_projection(projection, !analysis.aggregates.is_empty(), analysis)
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
            projection,
            grouping_sets,
        } => {
            if grouping_sets.is_some() {
                return Err(RemoteReadRejection::local_only(
                    "grouping_sets_local_only",
                    "GROUPING SETS/ROLLUP/CUBE aggregation is not in the v0.8 remote-read catalog",
                ));
            }
            analysis.operators.group_by = !group_keys.is_empty();
            analysis.operators.having = having.is_some();
            for group_key in group_keys {
                validate_expr(group_key, false, analysis)?;
            }
            for aggregate in aggregates {
                if aggregate.filter.is_some() {
                    return Err(RemoteReadRejection::local_only(
                        "aggregate_filter_local_only",
                        "aggregate FILTER (WHERE ...) is not in the v0.8 remote-read catalog",
                    ));
                }
                let Some(aggregate_name) = remote_aggregate(&aggregate.function) else {
                    return Err(RemoteReadRejection::local_only(
                        "ordered_aggregate_local_only",
                        "ordered-set aggregates are not in the v0.8 remote-read catalog",
                    ));
                };
                if !aggregate.order_by.is_empty() {
                    return Err(RemoteReadRejection::local_only(
                        "ordered_aggregate_local_only",
                        "aggregate-local ORDER BY is not in the v0.8 remote-read catalog",
                    ));
                }
                analysis.operators.aggregate_distinct |= aggregate.distinct;
                if let Some(argument) = &aggregate.arg {
                    validate_expr(argument, false, analysis)?;
                }
                analysis.aggregates.push(aggregate_name);
            }
            if let Some(having) = having {
                validate_expr(having, true, analysis)?;
            }
            validate_projection(projection, true, analysis)?;
            validate_plan(input, analysis)
        }
        LogicalPlan::Sort { input, order_by } => {
            validate_plan(input, analysis)?;
            analysis.operators.order_by = true;
            for sort in order_by {
                validate_expr(sort.expr(), !analysis.aggregates.is_empty(), analysis)?;
            }
            Ok(())
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
            ties,
        } => {
            if ties.is_some() {
                return Err(RemoteReadRejection::local_only(
                    "fetch_with_ties_local_only",
                    "FETCH ... WITH TIES is not in the v0.8 remote-read catalog",
                ));
            }
            analysis.operators.limit |= limit.is_some();
            analysis.operators.offset |= offset.is_some();
            validate_plan(input, analysis)
        }
        LogicalPlan::DistinctOn { .. } => Err(RemoteReadRejection::local_only(
            "distinct_on_local_only",
            "SELECT DISTINCT ON is not in the v0.8 remote-read catalog",
        )),
    }
}

fn validate_projection(
    projection: &Projection,
    allow_aggregate: bool,
    analysis: &mut Analysis,
) -> Result<(), RemoteReadClassification> {
    if let Projection::Columns(columns) = projection {
        analysis.operators.projection = true;
        for column in columns {
            validate_expr(&column.expr, allow_aggregate, analysis)?;
        }
    }
    Ok(())
}

fn validate_expr(
    expr: &TypedExpr,
    allow_aggregate: bool,
    analysis: &mut Analysis,
) -> Result<(), RemoteReadClassification> {
    match &expr.kind {
        TypedExprKind::Literal(_) | TypedExprKind::ColumnRef { .. } => Ok(()),
        TypedExprKind::VectorLiteral(_) => Err(RemoteReadRejection::local_only(
            "vector_sql_local_only",
            "vector SQL is not in the v0.8 remote-read catalog",
        )),
        TypedExprKind::TryCast { .. } => Err(RemoteReadRejection::local_only(
            "try_cast_local_only",
            "TRY_CAST is not in the v0.8 remote-read catalog",
        )),
        TypedExprKind::BinaryOp { left, right, .. } => {
            validate_expr(left, allow_aggregate, analysis)?;
            validate_expr(right, allow_aggregate, analysis)
        }
        TypedExprKind::UnaryOp { operand, .. }
        | TypedExprKind::Cast { expr: operand, .. }
        | TypedExprKind::IsNull { expr: operand, .. } => {
            validate_expr(operand, allow_aggregate, analysis)
        }
        TypedExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_expr(operand, allow_aggregate, analysis)?;
            }
            for branch in branches {
                validate_expr(&branch.when, allow_aggregate, analysis)?;
                validate_expr(&branch.then, allow_aggregate, analysis)?;
            }
            if let Some(else_expr) = else_expr {
                validate_expr(else_expr, allow_aggregate, analysis)?;
            }
            Ok(())
        }
        TypedExprKind::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr, allow_aggregate, analysis)?;
            validate_expr(low, allow_aggregate, analysis)?;
            validate_expr(high, allow_aggregate, analysis)
        }
        TypedExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_expr(expr, allow_aggregate, analysis)?;
            validate_expr(pattern, allow_aggregate, analysis)?;
            if let Some(escape) = escape {
                validate_expr(escape, allow_aggregate, analysis)?;
            }
            Ok(())
        }
        TypedExprKind::InList { expr, list, .. } => {
            validate_expr(expr, allow_aggregate, analysis)?;
            for item in list {
                validate_expr(item, allow_aggregate, analysis)?;
            }
            Ok(())
        }
        TypedExprKind::FunctionCall { name, args, .. } => {
            if allow_aggregate && aggregate_function_name(name) {
                for argument in args {
                    validate_expr(argument, false, analysis)?;
                }
                return Ok(());
            }
            let normalized = name.to_ascii_lowercase();
            if normalized.starts_with("__alopex_truth_") || normalized.starts_with("__alopex_row_")
            {
                return Err(RemoteReadRejection::local_only(
                    "standard_predicate_local_only",
                    "standard predicates are not in the v0.8 remote-read catalog",
                ));
            }
            if REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS.contains(&normalized.as_str()) {
                return Err(RemoteReadRejection::local_only(
                    "stateful_function_local_only",
                    "the requested scalar function remains local-only",
                ));
            }
            if !REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS.contains(&normalized.as_str()) {
                return Err(RemoteReadRejection::unsupported(
                    "function_not_in_remote_catalog",
                    "function is not explicitly listed in the remote-read catalog",
                ));
            }
            analysis.operators.deterministic_scalar = true;
            for argument in args {
                validate_expr(argument, false, analysis)?;
            }
            Ok(())
        }
        TypedExprKind::ScalarSubquery(_)
        | TypedExprKind::InSubquery { .. }
        | TypedExprKind::Exists { .. }
        | TypedExprKind::Quantified { .. } => Err(RemoteReadRejection::unsupported(
            "subquery_not_supported_remote",
            "subqueries are outside the v0.8 remote-read catalog",
        )),
    }
}

/// Aggregate identities admitted to the closed v0.8 catalog. Ordered-set
/// aggregates (PERCENTILE_DISC, issue #148) return `None` and classify as
/// `ordered_aggregate_local_only`.
fn remote_aggregate(function: &AggregateFunction) -> Option<RemoteAggregate> {
    match function {
        AggregateFunction::Count => Some(RemoteAggregate::Count),
        AggregateFunction::Sum => Some(RemoteAggregate::Sum),
        AggregateFunction::Total => Some(RemoteAggregate::Total),
        AggregateFunction::Avg => Some(RemoteAggregate::Avg),
        AggregateFunction::Min => Some(RemoteAggregate::Min),
        AggregateFunction::Max => Some(RemoteAggregate::Max),
        AggregateFunction::GroupConcat { .. } => Some(RemoteAggregate::GroupConcat),
        AggregateFunction::StringAgg { .. } => Some(RemoteAggregate::StringAgg),
        AggregateFunction::PercentileDisc { .. } => None,
        _ => None,
    }
}

fn aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "total" | "avg" | "min" | "max" | "group_concat" | "string_agg"
    )
}

trait SortExprExt {
    fn expr(&self) -> &TypedExpr;
}

impl SortExprExt for crate::planner::SortExpr {
    fn expr(&self) -> &TypedExpr {
        &self.expr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;
    use crate::ast::expr::Literal;
    use crate::catalog::ColumnMetadata;
    use crate::planner::{Projection, RecursiveCteLimits, ResolvedType, SortExpr, TypedExpr};

    fn references() -> Vec<TableReference> {
        vec![TableReference::new(
            "users",
            TableReferenceAccess::Read,
            crate::planner::TableReferenceSource::LogicalPlanScan,
        )]
    }

    fn scan() -> LogicalPlan {
        LogicalPlan::scan("users".to_string(), Projection::All(vec!["id".to_string()]))
    }

    fn column() -> TypedExpr {
        TypedExpr::column_ref(
            "users".to_string(),
            "id".to_string(),
            0,
            ResolvedType::Integer,
            Span::default(),
        )
    }

    #[test]
    fn classifies_closed_single_table_read_shape() {
        let plan = LogicalPlan::limit(
            LogicalPlan::sort(
                LogicalPlan::filter(scan(), column()),
                vec![SortExpr::asc(column())],
            ),
            Some(10),
            Some(3),
        );

        let RemoteReadClassification::Supported(descriptor) = classify(&plan, &references()) else {
            panic!("single-table deterministic read must be accepted");
        };
        assert_eq!(descriptor.catalog_version, REMOTE_READ_CATALOG_VERSION);
        assert_eq!(descriptor.table, "users");
        assert_eq!(descriptor.shape, RemoteReadShape::Rows);
        assert!(descriptor.operators.filter);
        assert!(descriptor.operators.order_by);
        assert!(descriptor.operators.limit);
        assert!(descriptor.operators.offset);
    }

    #[test]
    fn rejects_join_and_dml_before_transport() {
        let join = LogicalPlan::join(scan(), scan(), crate::planner::JoinType::Inner, None, None);
        assert!(matches!(
            classify(&join, &references()),
            RemoteReadClassification::UnsupportedRemote(RemoteReadRejection { code, .. })
                if code == "join_not_supported_remote"
        ));

        let insert = LogicalPlan::insert("users".to_string(), vec!["id".to_string()], vec![]);
        assert!(matches!(
            classify(&insert, &references()),
            RemoteReadClassification::UnsupportedRemote(RemoteReadRejection { code, .. })
                if code == "dml_not_supported_remote"
        ));
    }

    #[test]
    fn rejects_ddl_and_pragma_before_a_remote_session_exists() {
        let ddl = LogicalPlan::drop_table("users".to_string(), false);
        assert!(matches!(
            classify(&ddl, &[]),
            RemoteReadClassification::UnsupportedRemote(RemoteReadRejection { code, .. })
                if code == "ddl_not_supported_remote"
        ));

        let pragma = LogicalPlan::Pragma {
            name: "cache_size".to_string(),
            value: None,
        };
        assert!(matches!(
            classify(&pragma, &[]),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "pragma_local_only"
        ));
    }

    #[test]
    fn values_relations_remain_local_only() {
        let plan = LogicalPlan::Values {
            rows: vec![vec![TypedExpr::literal(
                Literal::Number("1".to_string()),
                ResolvedType::Integer,
                Span::default(),
            )]],
            schema: vec![ColumnMetadata::new("column1", ResolvedType::Integer)],
        };
        assert!(matches!(
            classify(&plan, &[]),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "values_local_only"
        ));
    }

    #[test]
    fn recursive_cte_is_rejected_before_remote_transport() {
        let schema = vec![ColumnMetadata::new("id", ResolvedType::Integer)];
        let reference = LogicalPlan::RecursiveReference {
            name: "counter".to_string(),
            schema: schema.clone(),
        };
        let recursive = LogicalPlan::RecursiveCte {
            name: "counter".to_string(),
            anchor: Box::new(scan()),
            recursive_term: Box::new(reference.clone()),
            union_all: true,
            schema,
            limits: RecursiveCteLimits::default(),
        };

        for plan in [&recursive, &reference] {
            assert!(matches!(
                classify(plan, &references()),
                RemoteReadClassification::UnsupportedRemote(RemoteReadRejection { code, reason })
                    if code == "recursive_cte_not_supported_remote"
                        && reason.contains("outside the v0.8 remote-read catalog")
            ));
        }
    }

    #[test]
    fn stateful_and_vector_expressions_remain_local_only() {
        let random = TypedExpr::function_call(
            "random".to_string(),
            vec![],
            false,
            false,
            ResolvedType::Double,
            Span::default(),
        );
        let random_plan = LogicalPlan::filter(scan(), random);
        assert!(matches!(
            classify(&random_plan, &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "stateful_function_local_only"
        ));

        let vector_plan = LogicalPlan::filter(
            scan(),
            TypedExpr::vector_literal(vec![1.0, 2.0], 2, Span::default()),
        );
        assert!(matches!(
            classify(&vector_plan, &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "vector_sql_local_only"
        ));
    }

    #[test]
    fn standard_predicates_remain_local_only() {
        let predicate = TypedExpr::function_call(
            "__alopex_truth_true:0".to_string(),
            vec![TypedExpr::literal(
                Literal::Boolean(true),
                ResolvedType::Boolean,
                Span::default(),
            )],
            false,
            false,
            ResolvedType::Boolean,
            Span::default(),
        );
        assert!(matches!(
            classify(&LogicalPlan::filter(scan(), predicate), &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "standard_predicate_local_only"
        ));
    }

    #[test]
    fn try_cast_remains_local_only() {
        let expression = TypedExpr::try_cast(
            TypedExpr::literal(
                Literal::String("42".to_string()),
                ResolvedType::Text,
                Span::default(),
            ),
            ResolvedType::Integer,
            Span::default(),
        );
        assert!(matches!(
            classify(&LogicalPlan::filter(scan(), expression), &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "try_cast_local_only"
        ));
    }

    #[test]
    fn grouping_sets_remain_local_only() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: vec![column()],
            aggregates: Vec::new(),
            having: None,
            projection: Projection::All(vec!["value".to_string()]),
            grouping_sets: Some(vec![0b0, 0b1]),
        };
        assert!(matches!(
            classify(&plan, &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "grouping_sets_local_only"
        ));
    }

    #[test]
    fn fetch_with_ties_remains_local_only() {
        let sorted = LogicalPlan::sort(scan(), vec![SortExpr::asc(column())]);
        let ties = if let LogicalPlan::Sort { order_by, .. } = &sorted {
            Some(order_by.clone())
        } else {
            unreachable!("sort constructed above")
        };
        let plan = LogicalPlan::Limit {
            input: Box::new(sorted),
            limit: Some(2),
            offset: None,
            ties,
        };
        assert!(matches!(
            classify(&plan, &references()),
            RemoteReadClassification::LocalOnly(RemoteReadRejection { code, .. })
                if code == "fetch_with_ties_local_only"
        ));

        // Plain FETCH ... ONLY desugars to limit/offset and stays remote-supported.
        let desugared = LogicalPlan::limit(
            LogicalPlan::sort(scan(), vec![SortExpr::asc(column())]),
            Some(2),
            Some(1),
        );
        assert!(matches!(
            classify(&desugared, &references()),
            RemoteReadClassification::Supported(_)
        ));

        assert!(
            coverage_entries()
                .iter()
                .any(|entry| entry.id == "pagination.fetch_with_ties"
                    && matches!(entry.remote_status, RemoteReadCoverageStatus::LocalOnly))
        );
    }

    #[test]
    fn subqueries_are_rejected_and_descriptor_never_contains_plan() {
        let subquery = TypedExpr::new(
            TypedExprKind::ScalarSubquery(Box::new(scan())),
            ResolvedType::Integer,
            Span::default(),
        );
        assert!(matches!(
            classify(&LogicalPlan::filter(scan(), subquery), &references()),
            RemoteReadClassification::UnsupportedRemote(RemoteReadRejection { code, .. })
                if code == "subquery_not_supported_remote"
        ));

        let encoded = serde_json::to_string(&classify(&scan(), &references())).unwrap();
        assert!(!encoded.contains("LogicalPlan"));
        assert!(!encoded.contains("column_index"));
    }

    #[test]
    fn aggregate_catalog_includes_string_aggregates() {
        let aggregate = crate::planner::AggregateExpr {
            function: AggregateFunction::StringAgg {
                separator: Some(",".to_string()),
            },
            arg: Some(column()),
            extra_args: Vec::new(),
            distinct: true,
            result_type: ResolvedType::Text,
            filter: None,
            order_by: Vec::new(),
        };
        let plan = LogicalPlan::aggregate(
            scan(),
            vec![column()],
            vec![aggregate],
            Some(TypedExpr::literal(
                Literal::Boolean(true),
                ResolvedType::Boolean,
                Span::default(),
            )),
            Projection::All(vec![]),
        );
        let RemoteReadClassification::Supported(descriptor) = classify(&plan, &references()) else {
            panic!("listed aggregate must be accepted");
        };
        assert_eq!(
            descriptor.shape,
            RemoteReadShape::Aggregate {
                aggregates: vec![RemoteAggregate::StringAgg]
            }
        );
        assert!(descriptor.operators.group_by);
        assert!(descriptor.operators.having);
        assert!(descriptor.operators.aggregate_distinct);
    }
}
