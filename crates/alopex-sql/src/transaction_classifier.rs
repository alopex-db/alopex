//! Closed preflight classifier for the Phase 4 distributed-transaction SQL surface.
//!
//! The local parser and planner keep their v0.8 behavior.  A caller selecting
//! distributed execution must invoke this classifier before it opens a range
//! participant or delegates to the normal planner.  The classifier deliberately
//! records the approved matrix row for every decision instead of treating a
//! broad "query" predicate as distributed-capable.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ast::{
    BinaryOp, Expr, ExprKind, FromItem, IndexMethod, Select, SelectItem, Statement, StatementKind,
};
use crate::catalog::Catalog;
use crate::dialect::AlopexDialect;
use crate::distributed_read::{
    REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS, REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS,
    RemoteReadCatalogV0_8, RemoteReadClassification,
};
use crate::error::ParserError;
use crate::parser::Parser;
use crate::planner::{PlannedStatement, PlannerError};

/// Fixed execution status for one approved SQL transaction matrix row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransactionSqlStatus {
    /// The operation may enlist more than one range when a cluster profile is explicit.
    Distributed,
    /// The operation is valid only when the adapter proves one target range.
    SingleRange,
    /// The operation retains its v0.8 local behavior and is never broadcast.
    LocalOnly,
    /// The adapter must reject before opening a transaction participant.
    PreExecutionReject,
}

/// One row from the approved SQL-TXN-01 through SQL-TXN-51 register.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionSqlRow {
    Begin,
    Commit,
    Rollback,
    Abort,
    SelectLiteral,
    SelectSingleTable,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    CreateIndexBtree,
    CreateIndexHnsw,
    DropIndex,
    Where,
    Comparison,
    Arithmetic,
    Boolean,
    Pattern,
    IsNull,
    IsNotNull,
    InList,
    NotInList,
    Between,
    NotBetween,
    Case,
    OrderBy,
    Limit,
    Offset,
    Distinct,
    GroupBy,
    Having,
    JoinInner,
    JoinLeft,
    JoinRight,
    JoinFull,
    JoinSemi,
    JoinAnti,
    ScalarSubquery,
    InSubquery,
    ExistsSubquery,
    QuantifiedSubquery,
    Union,
    Intersect,
    Except,
    Window,
    CopyCsv,
    CopyParquet,
    PragmaCacheSize,
    PragmaMemoryLimit,
    PragmaIoStats,
}

impl TransactionSqlRow {
    /// Stable design/evidence identity for this row.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Begin => "SQL-TXN-01",
            Self::Commit => "SQL-TXN-02",
            Self::Rollback => "SQL-TXN-03",
            Self::Abort => "SQL-TXN-04",
            Self::SelectLiteral => "SQL-TXN-05",
            Self::SelectSingleTable => "SQL-TXN-06",
            Self::Insert => "SQL-TXN-07",
            Self::Update => "SQL-TXN-08",
            Self::Delete => "SQL-TXN-09",
            Self::CreateTable => "SQL-TXN-10",
            Self::DropTable => "SQL-TXN-11",
            Self::CreateIndexBtree => "SQL-TXN-12",
            Self::CreateIndexHnsw => "SQL-TXN-13",
            Self::DropIndex => "SQL-TXN-14",
            Self::Where => "SQL-TXN-15",
            Self::Comparison => "SQL-TXN-16",
            Self::Arithmetic => "SQL-TXN-17",
            Self::Boolean => "SQL-TXN-18",
            Self::Pattern => "SQL-TXN-19",
            Self::IsNull => "SQL-TXN-20",
            Self::IsNotNull => "SQL-TXN-21",
            Self::InList => "SQL-TXN-22",
            Self::NotInList => "SQL-TXN-23",
            Self::Between => "SQL-TXN-24",
            Self::NotBetween => "SQL-TXN-25",
            Self::Case => "SQL-TXN-26",
            Self::OrderBy => "SQL-TXN-27",
            Self::Limit => "SQL-TXN-28",
            Self::Offset => "SQL-TXN-29",
            Self::Distinct => "SQL-TXN-30",
            Self::GroupBy => "SQL-TXN-31",
            Self::Having => "SQL-TXN-32",
            Self::JoinInner => "SQL-TXN-33",
            Self::JoinLeft => "SQL-TXN-34",
            Self::JoinRight => "SQL-TXN-35",
            Self::JoinFull => "SQL-TXN-36",
            Self::JoinSemi => "SQL-TXN-37",
            Self::JoinAnti => "SQL-TXN-38",
            Self::ScalarSubquery => "SQL-TXN-39",
            Self::InSubquery => "SQL-TXN-40",
            Self::ExistsSubquery => "SQL-TXN-41",
            Self::QuantifiedSubquery => "SQL-TXN-42",
            Self::Union => "SQL-TXN-43",
            Self::Intersect => "SQL-TXN-44",
            Self::Except => "SQL-TXN-45",
            Self::Window => "SQL-TXN-46",
            Self::CopyCsv => "SQL-TXN-47",
            Self::CopyParquet => "SQL-TXN-48",
            Self::PragmaCacheSize => "SQL-TXN-49",
            Self::PragmaMemoryLimit => "SQL-TXN-50",
            Self::PragmaIoStats => "SQL-TXN-51",
        }
    }

    /// The approved fixed status, independent of one request's runtime fence.
    pub const fn status(self) -> TransactionSqlStatus {
        match self {
            Self::CreateTable
            | Self::DropTable
            | Self::CreateIndexBtree
            | Self::CreateIndexHnsw
            | Self::DropIndex
            | Self::JoinInner
            | Self::JoinLeft
            | Self::JoinRight
            | Self::JoinFull
            | Self::JoinSemi
            | Self::JoinAnti
            | Self::ScalarSubquery
            | Self::InSubquery
            | Self::ExistsSubquery
            | Self::QuantifiedSubquery
            | Self::Union
            | Self::Intersect
            | Self::Except
            | Self::Window => TransactionSqlStatus::PreExecutionReject,
            Self::CopyCsv | Self::CopyParquet => TransactionSqlStatus::SingleRange,
            Self::PragmaCacheSize | Self::PragmaMemoryLimit | Self::PragmaIoStats => {
                TransactionSqlStatus::LocalOnly
            }
            _ => TransactionSqlStatus::Distributed,
        }
    }

    /// Public operation identity without exposing a planner implementation.
    pub const fn operation(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            Self::Abort => "ABORT",
            Self::SelectLiteral => "SELECT literal",
            Self::SelectSingleTable => "SELECT one table",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::CreateTable => "CREATE TABLE",
            Self::DropTable => "DROP TABLE",
            Self::CreateIndexBtree => "CREATE INDEX USING BTREE",
            Self::CreateIndexHnsw => "CREATE INDEX USING HNSW",
            Self::DropIndex => "DROP INDEX",
            Self::Where => "WHERE",
            Self::Comparison => "comparison operator",
            Self::Arithmetic => "arithmetic operator",
            Self::Boolean => "boolean operator",
            Self::Pattern => "LIKE/pattern predicate",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
            Self::InList => "IN list",
            Self::NotInList => "NOT IN list",
            Self::Between => "BETWEEN",
            Self::NotBetween => "NOT BETWEEN",
            Self::Case => "CASE",
            Self::OrderBy => "ORDER BY",
            Self::Limit => "LIMIT",
            Self::Offset => "OFFSET",
            Self::Distinct => "DISTINCT",
            Self::GroupBy => "GROUP BY",
            Self::Having => "HAVING",
            Self::JoinInner => "JOIN INNER",
            Self::JoinLeft => "JOIN LEFT",
            Self::JoinRight => "JOIN RIGHT",
            Self::JoinFull => "JOIN FULL",
            Self::JoinSemi => "JOIN SEMI",
            Self::JoinAnti => "JOIN ANTI",
            Self::ScalarSubquery => "scalar subquery",
            Self::InSubquery => "IN subquery",
            Self::ExistsSubquery => "EXISTS subquery",
            Self::QuantifiedSubquery => "quantified subquery",
            Self::Union => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
            Self::Window => "window expression",
            Self::CopyCsv => "COPY CSV",
            Self::CopyParquet => "COPY Parquet",
            Self::PragmaCacheSize => "PRAGMA cache_size",
            Self::PragmaMemoryLimit => "PRAGMA memory_limit",
            Self::PragmaIoStats => "PRAGMA io_stats",
        }
    }

    /// Stable observable behavior declared by the approved matrix.
    pub const fn behavior(self) -> &'static str {
        match self {
            Self::Begin => {
                "fixed snapshot/read point and transaction identity; missing capability is blocked/prerequisite_missing"
            }
            Self::Commit => {
                "all enlisted ranges commit or recovery_pending; partial success is never success"
            }
            Self::Rollback | Self::Abort => "all enlisted ranges abort; no committed writes remain",
            Self::SelectLiteral => "snapshot/read-your-writes and closed read catalog semantics",
            Self::SelectSingleTable => {
                "fixed read point, range routing, and global result assembly"
            }
            Self::Insert => "row-key routing and all-or-none write set",
            Self::Update => "target-range enlistment and all-or-none write set",
            Self::Delete => "target-range enlistment and all-or-none delete",
            Self::Where
            | Self::IsNull
            | Self::IsNotNull
            | Self::InList
            | Self::NotInList
            | Self::Between
            | Self::NotBetween
            | Self::Case => "deterministic expression with local null/type semantics",
            Self::Comparison | Self::Arithmetic | Self::Boolean => {
                "deterministic expression with exact local type/error mapping"
            }
            Self::Pattern => "deterministic pattern semantics",
            Self::OrderBy => "global ordering is assembled before success",
            Self::Limit | Self::Offset | Self::Distinct => "applied after global result assembly",
            Self::GroupBy | Self::Having => "closed one-table aggregate shape only",
            Self::CopyCsv | Self::CopyParquet => {
                "one target range only; multi-range ingest is rejected before execution"
            }
            Self::PragmaCacheSize => {
                "local cache side effect; never part of a distributed decision"
            }
            Self::PragmaMemoryLimit => "local resource side effect; never broadcast",
            Self::PragmaIoStats => "local runtime statistics; never broadcast",
            _ => {
                "outside the approved distributed transaction catalog; reject before participant open"
            }
        }
    }

    /// Observable null and type behavior.  The classifier does not coerce
    /// values: accepted expressions retain the local planner/executor rules.
    pub const fn null_type_behavior(self) -> &'static str {
        match self {
            Self::IsNull | Self::IsNotNull => "local SQL null predicate semantics",
            Self::InList | Self::NotInList => "local SQL list and null semantics",
            Self::Case => "local SQL branch, type resolution, and null semantics",
            Self::Comparison | Self::Arithmetic | Self::Boolean | Self::Pattern => {
                "local SQL expression type/error and null semantics"
            }
            _ => "local SQL type/error and null semantics are preserved",
        }
    }

    /// Observable ordering behavior for this matrix row.
    pub const fn order_behavior(self) -> &'static str {
        match self {
            Self::OrderBy => "globally ordered before success",
            Self::Limit | Self::Offset => "applied after global result assembly",
            Self::Distinct => "global duplicate elimination before success",
            Self::GroupBy | Self::Having => "global group finalization before success",
            _ => "no additional ordering contract",
        }
    }

    /// Transaction SQL rows are deterministic; volatile scalar identities are
    /// classified by the separate scalar matrix in tasks 4.7–4.8.
    pub const fn volatile(self) -> bool {
        false
    }

    /// Side effects are explicit rather than inferred from broad statement
    /// families, so an adapter can refuse the rejected/local-only cases.
    pub const fn side_effect(self) -> &'static str {
        match self {
            Self::Insert | Self::Update | Self::Delete => {
                "transactional write; visible only after commit"
            }
            Self::Begin | Self::Commit | Self::Rollback | Self::Abort => {
                "transaction lifecycle decision"
            }
            Self::CopyCsv | Self::CopyParquet => "transactional ingest to one resolved range",
            Self::PragmaCacheSize | Self::PragmaMemoryLimit | Self::PragmaIoStats => {
                "local-only runtime side effect"
            }
            Self::CreateTable
            | Self::DropTable
            | Self::CreateIndexBtree
            | Self::CreateIndexHnsw
            | Self::DropIndex => "rejected before schema side effect",
            _ => "no side effect",
        }
    }

    /// Converts the closed row to its complete public metadata record.
    pub const fn metadata(self) -> TransactionSqlRowMetadata {
        TransactionSqlRowMetadata {
            row: self,
            id: self.id(),
            operation: self.operation(),
            status: self.status(),
            isolation: "snapshot",
            visibility: "read-your-writes; no external visibility before committed; aborted writes remain invisible",
            null_type_behavior: self.null_type_behavior(),
            order_behavior: self.order_behavior(),
            volatile: self.volatile(),
            side_effect: self.side_effect(),
            behavior: self.behavior(),
            adapter_mapping: "transaction classifier -> server/HTTP/gRPC/CLI/Python adapter -> Phase 4 evidence manifest",
            evidence_id: self.id(),
        }
    }
}

/// Complete observable metadata for one SQL-TXN row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TransactionSqlRowMetadata {
    pub row: TransactionSqlRow,
    pub id: &'static str,
    pub operation: &'static str,
    pub status: TransactionSqlStatus,
    pub isolation: &'static str,
    pub visibility: &'static str,
    pub null_type_behavior: &'static str,
    pub order_behavior: &'static str,
    pub volatile: bool,
    pub side_effect: &'static str,
    pub behavior: &'static str,
    pub adapter_mapping: &'static str,
    pub evidence_id: &'static str,
}

/// Static source of truth for every approved SQL-TXN row.  The Phase 4
/// verifier consumes this table instead of inferring support from parser or
/// planner variants.
pub static TRANSACTION_SQL_STATEMENT_MATRIX: [TransactionSqlRowMetadata; 51] = [
    TransactionSqlRow::Begin.metadata(),
    TransactionSqlRow::Commit.metadata(),
    TransactionSqlRow::Rollback.metadata(),
    TransactionSqlRow::Abort.metadata(),
    TransactionSqlRow::SelectLiteral.metadata(),
    TransactionSqlRow::SelectSingleTable.metadata(),
    TransactionSqlRow::Insert.metadata(),
    TransactionSqlRow::Update.metadata(),
    TransactionSqlRow::Delete.metadata(),
    TransactionSqlRow::CreateTable.metadata(),
    TransactionSqlRow::DropTable.metadata(),
    TransactionSqlRow::CreateIndexBtree.metadata(),
    TransactionSqlRow::CreateIndexHnsw.metadata(),
    TransactionSqlRow::DropIndex.metadata(),
    TransactionSqlRow::Where.metadata(),
    TransactionSqlRow::Comparison.metadata(),
    TransactionSqlRow::Arithmetic.metadata(),
    TransactionSqlRow::Boolean.metadata(),
    TransactionSqlRow::Pattern.metadata(),
    TransactionSqlRow::IsNull.metadata(),
    TransactionSqlRow::IsNotNull.metadata(),
    TransactionSqlRow::InList.metadata(),
    TransactionSqlRow::NotInList.metadata(),
    TransactionSqlRow::Between.metadata(),
    TransactionSqlRow::NotBetween.metadata(),
    TransactionSqlRow::Case.metadata(),
    TransactionSqlRow::OrderBy.metadata(),
    TransactionSqlRow::Limit.metadata(),
    TransactionSqlRow::Offset.metadata(),
    TransactionSqlRow::Distinct.metadata(),
    TransactionSqlRow::GroupBy.metadata(),
    TransactionSqlRow::Having.metadata(),
    TransactionSqlRow::JoinInner.metadata(),
    TransactionSqlRow::JoinLeft.metadata(),
    TransactionSqlRow::JoinRight.metadata(),
    TransactionSqlRow::JoinFull.metadata(),
    TransactionSqlRow::JoinSemi.metadata(),
    TransactionSqlRow::JoinAnti.metadata(),
    TransactionSqlRow::ScalarSubquery.metadata(),
    TransactionSqlRow::InSubquery.metadata(),
    TransactionSqlRow::ExistsSubquery.metadata(),
    TransactionSqlRow::QuantifiedSubquery.metadata(),
    TransactionSqlRow::Union.metadata(),
    TransactionSqlRow::Intersect.metadata(),
    TransactionSqlRow::Except.metadata(),
    TransactionSqlRow::Window.metadata(),
    TransactionSqlRow::CopyCsv.metadata(),
    TransactionSqlRow::CopyParquet.metadata(),
    TransactionSqlRow::PragmaCacheSize.metadata(),
    TransactionSqlRow::PragmaMemoryLimit.metadata(),
    TransactionSqlRow::PragmaIoStats.metadata(),
];

/// Returns the complete, closed SQL-TXN register in design-row order.
pub fn transaction_sql_statement_matrix() -> &'static [TransactionSqlRowMetadata; 51] {
    &TRANSACTION_SQL_STATEMENT_MATRIX
}

/// Versioned, closed v0.9 transaction SQL catalog.
///
/// It deliberately does not modify [`crate::distributed_read::RemoteReadCatalogV0_8`]:
/// distributed reads and distributed transaction DML have different approved
/// surfaces and compatibility contracts.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransactionSqlCatalogV0_9;

impl TransactionSqlCatalogV0_9 {
    /// Returns the static SQL-TXN-01–51 source of truth.
    pub const fn entries(&self) -> &'static [TransactionSqlRowMetadata; 51] {
        &TRANSACTION_SQL_STATEMENT_MATRIX
    }

    /// Classifies a parsed statement without planner, storage, or participant I/O.
    pub fn classify_statement(&self, statement: &Statement) -> TransactionSqlClassification {
        classify_transaction_statement(statement)
    }

    /// Classifies an adapter-level transaction control operation.
    pub fn classify_control(&self, control: TransactionSqlControl) -> TransactionSqlClassification {
        classify_transaction_control(control)
    }

    /// Classifies COPY after its target ranges have been resolved.
    pub fn classify_copy(
        &self,
        format: TransactionCopyFormat,
        target_range_count: usize,
    ) -> TransactionSqlClassification {
        classify_transaction_copy(format, target_range_count)
    }
}

/// Control operations handled by a transaction/session adapter rather than the
/// current local SQL AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionSqlControl {
    Begin,
    Commit,
    Rollback,
    Abort,
}

/// COPY format whose target range count is determined by an adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionCopyFormat {
    Csv,
    Parquet,
}

/// Parser/planner shapes that must be rejected before a distributed
/// participant is opened.  Semi/anti and compound forms are represented here
/// because the current v0.8 AST deliberately does not expose all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionUnsupportedConstruct {
    JoinInner,
    JoinLeft,
    JoinRight,
    JoinFull,
    JoinSemi,
    JoinAnti,
    ScalarSubquery,
    InSubquery,
    ExistsSubquery,
    QuantifiedSubquery,
    Union,
    Intersect,
    Except,
    Window,
}

impl TransactionUnsupportedConstruct {
    const fn row(self) -> TransactionSqlRow {
        match self {
            Self::JoinInner => TransactionSqlRow::JoinInner,
            Self::JoinLeft => TransactionSqlRow::JoinLeft,
            Self::JoinRight => TransactionSqlRow::JoinRight,
            Self::JoinFull => TransactionSqlRow::JoinFull,
            Self::JoinSemi => TransactionSqlRow::JoinSemi,
            Self::JoinAnti => TransactionSqlRow::JoinAnti,
            Self::ScalarSubquery => TransactionSqlRow::ScalarSubquery,
            Self::InSubquery => TransactionSqlRow::InSubquery,
            Self::ExistsSubquery => TransactionSqlRow::ExistsSubquery,
            Self::QuantifiedSubquery => TransactionSqlRow::QuantifiedSubquery,
            Self::Union => TransactionSqlRow::Union,
            Self::Intersect => TransactionSqlRow::Intersect,
            Self::Except => TransactionSqlRow::Except,
            Self::Window => TransactionSqlRow::Window,
        }
    }
}

/// Stable non-success result returned before a participant or planner is used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code}: {reason}")]
pub struct TransactionSqlPreflightError {
    pub code: String,
    pub reason: String,
}

/// Structured failure at the distributed-transaction admission boundary.
///
/// The original classification is retained for rejected and local-only forms
/// so adapters can return the exact SQL-TXN row without opening a participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Error)]
#[error("{error}")]
pub struct TransactionSqlPreflightFailure {
    pub classification: TransactionSqlClassification,
    pub error: TransactionSqlPreflightError,
}

/// Result used at the admission boundary.  The structured failure is boxed so
/// callers do not carry a full evidence matrix in every successful result.
pub type TransactionSqlPreflightResult<T> = Result<T, Box<TransactionSqlPreflightFailure>>;

/// Result of classifying one transaction operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionSqlClassification {
    pub status: TransactionSqlStatus,
    /// The first exact matrix row that determined the statement boundary.
    /// Additional rows describe its admitted clauses and expressions.
    pub primary_row: Option<TransactionSqlRowMetadata>,
    pub rows: Vec<TransactionSqlRowMetadata>,
    pub preflight_rejection: Option<TransactionSqlPreflightError>,
}

impl TransactionSqlClassification {
    /// True only when this is not a rejected or local-only form.  A
    /// `SingleRange` result still requires its adapter-specific range fence.
    pub fn is_preflight_accepted(&self) -> bool {
        self.preflight_rejection.is_none() && self.status != TransactionSqlStatus::LocalOnly
    }
}

/// A parsed statement plus the classification that was accepted before
/// planning.  This keeps local [`crate::planner::PlannedStatement`] behavior
/// unchanged while giving distributed callers a single safe entry point.
#[derive(Debug, Clone)]
pub struct TransactionPlannedStatement {
    pub classification: TransactionSqlClassification,
    pub planned: PlannedStatement,
}

/// One parsed distributed-transaction statement whose classifier result was
/// accepted before routing or participant acquisition.
#[derive(Debug, Clone)]
pub struct TransactionParsedStatement {
    pub statement: Statement,
    pub classification: TransactionSqlClassification,
}

/// Failure returned by the parser/planner transaction preflight boundary.
#[derive(Debug, Error)]
pub enum TransactionSqlPlanningError {
    #[error(transparent)]
    Preflight(#[from] Box<TransactionSqlPreflightFailure>),
    #[error(transparent)]
    Planner(#[from] PlannerError),
}

/// Failure at the parser-to-transaction-classifier boundary.
#[derive(Debug, Error)]
pub enum TransactionSqlParseError {
    #[error(transparent)]
    Parser(#[from] ParserError),
    #[error("distributed transaction SQL accepts exactly one statement, got {count}")]
    StatementCount { count: usize },
    #[error(transparent)]
    Preflight(#[from] Box<TransactionSqlPreflightFailure>),
}

/// Classifies a control operation handled outside the current local AST.
pub fn classify_transaction_control(
    control: TransactionSqlControl,
) -> TransactionSqlClassification {
    let row = match control {
        TransactionSqlControl::Begin => TransactionSqlRow::Begin,
        TransactionSqlControl::Commit => TransactionSqlRow::Commit,
        TransactionSqlControl::Rollback => TransactionSqlRow::Rollback,
        TransactionSqlControl::Abort => TransactionSqlRow::Abort,
    };
    accepted([row])
}

/// Classifies one raw SQL input before parser/planner routing.
///
/// The small lexer ignores quoted text and comments and exists only for
/// adapter-level operations absent from the local statement AST.  Regular SQL
/// still flows through the Nim parser so an identifier is never misread as a
/// compound-query or window keyword.
pub fn classify_transaction_sql(sql: &str) -> TransactionSqlClassification {
    let scan = match scan_transaction_sql(sql) {
        Ok(scan) => scan,
        Err(reason) => return rejected([], "transaction_sql_lexical_error", &reason),
    };
    if scan.statement_count != 1 {
        return rejected(
            [],
            "distributed_transaction_statement_count_unsupported",
            "a distributed transaction adapter accepts exactly one SQL statement at a time",
        );
    }
    let Some(first) = scan.keywords.first().map(String::as_str) else {
        return rejected(
            [],
            "transaction_sql_empty",
            "an empty SQL input is not an approved transaction matrix row",
        );
    };

    let control = match first {
        "BEGIN" => Some(TransactionSqlControl::Begin),
        "COMMIT" => Some(TransactionSqlControl::Commit),
        "ROLLBACK" => Some(TransactionSqlControl::Rollback),
        "ABORT" => Some(TransactionSqlControl::Abort),
        _ => None,
    };
    if let Some(control) = control {
        return classify_transaction_control(control);
    }

    if first == "COPY" {
        let row = if scan.contains("CSV") {
            Some(TransactionSqlRow::CopyCsv)
        } else if scan.contains("PARQUET") {
            Some(TransactionSqlRow::CopyParquet)
        } else {
            None
        };
        return match row {
            Some(row) => accepted([row]),
            None => rejected(
                [],
                "copy_format_not_in_transaction_matrix",
                "only COPY CSV and COPY Parquet are approved transaction rows",
            ),
        };
    }

    if let Some(construct) = unsupported_construct_from_sql(&scan) {
        return classify_unsupported_transaction_construct(construct);
    }
    match Parser::parse_sql(&AlopexDialect, sql) {
        Ok(statements) if statements.len() == 1 => classify_transaction_statement(&statements[0]),
        Ok(_) => rejected(
            [],
            "distributed_transaction_statement_count_unsupported",
            "a distributed transaction adapter accepts exactly one SQL statement at a time",
        ),
        Err(error) => rejected(
            [],
            "transaction_sql_parse_rejected",
            &format!("the existing SQL parser rejected this transaction input: {error}"),
        ),
    }
}

/// Checks the raw-SQL admission boundary before a parser/planner caller opens
/// a participant.  COPY remains subject to its resolved-range check through
/// [`preflight_transaction_copy`].
pub fn preflight_transaction_sql(
    sql: &str,
) -> TransactionSqlPreflightResult<TransactionSqlClassification> {
    let classification = classify_transaction_sql(sql);
    if classification.status == TransactionSqlStatus::SingleRange {
        return Err(Box::new(TransactionSqlPreflightFailure {
            classification,
            error: TransactionSqlPreflightError {
                code: "transaction_sql_single_range_required".to_string(),
                reason: "COPY requires a resolved target range before distributed execution"
                    .to_string(),
            },
        }));
    }
    preflight_classification(classification)
}

/// Parses exactly one existing SQL statement and runs the v0.9 transaction
/// classifier before a caller can route or obtain a participant.  Transaction
/// control and COPY retain their separate adapter APIs because the existing
/// Nim statement AST intentionally does not represent them.
pub fn parse_and_preflight_transaction_statement(
    sql: &str,
) -> Result<TransactionParsedStatement, TransactionSqlParseError> {
    preflight_transaction_sql(sql)?;
    let statements = Parser::parse_sql(&AlopexDialect, sql)?;
    if statements.len() != 1 {
        return Err(TransactionSqlParseError::StatementCount {
            count: statements.len(),
        });
    }
    let statement = statements
        .into_iter()
        .next()
        .expect("statement length was checked");
    let classification = preflight_transaction_statement(&statement)?;
    Ok(TransactionParsedStatement {
        statement,
        classification,
    })
}

/// Classifies COPY after the adapter has resolved its target range count.
pub fn classify_transaction_copy(
    format: TransactionCopyFormat,
    target_range_count: usize,
) -> TransactionSqlClassification {
    let row = match format {
        TransactionCopyFormat::Csv => TransactionSqlRow::CopyCsv,
        TransactionCopyFormat::Parquet => TransactionSqlRow::CopyParquet,
    };
    if target_range_count == 1 {
        accepted([row])
    } else {
        rejected(
            [row],
            "copy_requires_single_range",
            "COPY in a distributed transaction requires exactly one resolved target range",
        )
    }
}

/// Enforces the COPY single-range fence after an adapter resolves the target.
pub fn preflight_transaction_copy(
    format: TransactionCopyFormat,
    target_range_count: usize,
) -> TransactionSqlPreflightResult<TransactionSqlClassification> {
    let classification = classify_transaction_copy(format, target_range_count);
    match classification.preflight_rejection.clone() {
        Some(error) => Err(Box::new(TransactionSqlPreflightFailure {
            classification,
            error,
        })),
        None => Ok(classification),
    }
}

/// Classifies an unsupported parser/planner construct without opening a
/// participant.  This entry point covers exact matrix rows not represented by
/// the current v0.8 AST.
pub fn classify_unsupported_transaction_construct(
    construct: TransactionUnsupportedConstruct,
) -> TransactionSqlClassification {
    let row = construct.row();
    rejected(
        [row],
        "distributed_transaction_shape_unsupported",
        row.behavior(),
    )
}

#[derive(Debug, Default)]
struct TransactionSqlTextScan {
    keywords: Vec<String>,
    statement_count: usize,
}

impl TransactionSqlTextScan {
    fn contains(&self, keyword: &str) -> bool {
        self.keywords.iter().any(|item| item == keyword)
    }

    fn contains_sequence(&self, sequence: &[&str]) -> bool {
        self.keywords.windows(sequence.len()).any(|window| {
            window
                .iter()
                .map(String::as_str)
                .eq(sequence.iter().copied())
        })
    }
}

fn scan_transaction_sql(sql: &str) -> Result<TransactionSqlTextScan, String> {
    let bytes = sql.as_bytes();
    let mut scan = TransactionSqlTextScan::default();
    let mut index = 0;
    let mut statement_has_keyword = false;

    while index < bytes.len() {
        match bytes[index] {
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                let start = index - 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                if index + 1 == bytes.len() {
                    return Err(format!(
                        "unterminated block comment beginning at byte {start}"
                    ));
                }
                index += 2;
            }
            b'\'' | b'"' => {
                let quote = bytes[index];
                let start = index;
                index += 1;
                let mut closed = false;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                        } else {
                            index += 1;
                            closed = true;
                            break;
                        }
                    } else if bytes[index] == b'\\' && index + 1 < bytes.len() {
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                if !closed {
                    return Err(format!(
                        "unterminated quoted token beginning at byte {start}"
                    ));
                }
            }
            b';' => {
                if statement_has_keyword {
                    scan.statement_count += 1;
                    statement_has_keyword = false;
                }
                index += 1;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                scan.keywords.push(sql[start..index].to_ascii_uppercase());
                statement_has_keyword = true;
            }
            _ => index += 1,
        }
    }
    if statement_has_keyword {
        scan.statement_count += 1;
    }
    Ok(scan)
}

fn unsupported_construct_from_sql(
    scan: &TransactionSqlTextScan,
) -> Option<TransactionUnsupportedConstruct> {
    if scan.contains_sequence(&["SEMI", "JOIN"]) {
        return Some(TransactionUnsupportedConstruct::JoinSemi);
    }
    if scan.contains_sequence(&["ANTI", "JOIN"]) {
        return Some(TransactionUnsupportedConstruct::JoinAnti);
    }
    if scan.contains_sequence(&["UNION", "SELECT"])
        || scan.contains_sequence(&["UNION", "ALL", "SELECT"])
        || scan.contains_sequence(&["UNION", "DISTINCT", "SELECT"])
    {
        return Some(TransactionUnsupportedConstruct::Union);
    }
    if scan.contains_sequence(&["INTERSECT", "SELECT"]) {
        return Some(TransactionUnsupportedConstruct::Intersect);
    }
    if scan.contains_sequence(&["EXCEPT", "SELECT"]) {
        return Some(TransactionUnsupportedConstruct::Except);
    }
    None
}

/// Classifies one parsed SQL statement for a distributed transaction request.
///
/// The function has no storage, network, participant, or executor dependency;
/// callers must use [`preflight_transaction_statement`] before opening those
/// resources.
pub fn classify_transaction_statement(statement: &Statement) -> TransactionSqlClassification {
    match &statement.kind {
        StatementKind::Select(select) => classify_select(select),
        StatementKind::Insert(insert) => {
            let mut rows = BTreeSet::from([TransactionSqlRow::Insert]);
            let mut rejection = None;
            for values in &insert.values {
                for expression in values {
                    collect_expression_rows(expression, &mut rows, &mut rejection);
                }
            }
            finish(rows, rejection)
        }
        StatementKind::Update(update) => {
            let mut rows = BTreeSet::from([TransactionSqlRow::Update]);
            let mut rejection = None;
            if let Some(selection) = &update.selection {
                rows.insert(TransactionSqlRow::Where);
                collect_expression_rows(selection, &mut rows, &mut rejection);
            }
            for assignment in &update.assignments {
                collect_expression_rows(&assignment.value, &mut rows, &mut rejection);
            }
            finish(rows, rejection)
        }
        StatementKind::Delete(delete) => {
            let mut rows = BTreeSet::from([TransactionSqlRow::Delete]);
            let mut rejection = None;
            if let Some(selection) = &delete.selection {
                rows.insert(TransactionSqlRow::Where);
                collect_expression_rows(selection, &mut rows, &mut rejection);
            }
            finish(rows, rejection)
        }
        StatementKind::CreateTable(_) => rejected(
            [TransactionSqlRow::CreateTable],
            "distributed_transaction_ddl_unsupported",
            TransactionSqlRow::CreateTable.behavior(),
        ),
        StatementKind::DropTable(_) => rejected(
            [TransactionSqlRow::DropTable],
            "distributed_transaction_ddl_unsupported",
            TransactionSqlRow::DropTable.behavior(),
        ),
        StatementKind::CreateIndex(index) => match index.method {
            Some(IndexMethod::Hnsw) => rejected(
                [TransactionSqlRow::CreateIndexHnsw],
                "distributed_transaction_ddl_unsupported",
                TransactionSqlRow::CreateIndexHnsw.behavior(),
            ),
            Some(IndexMethod::BTree) => rejected(
                [TransactionSqlRow::CreateIndexBtree],
                "distributed_transaction_ddl_unsupported",
                TransactionSqlRow::CreateIndexBtree.behavior(),
            ),
            None => rejected(
                [],
                "unregistered_distributed_transaction_sql_form",
                "CREATE INDEX without an explicit approved USING method is not in the transaction matrix",
            ),
        },
        StatementKind::DropIndex(_) => rejected(
            [TransactionSqlRow::DropIndex],
            "distributed_transaction_ddl_unsupported",
            TransactionSqlRow::DropIndex.behavior(),
        ),
        StatementKind::Pragma { name, .. } => match name.to_ascii_lowercase().as_str() {
            "cache_size" => accepted([TransactionSqlRow::PragmaCacheSize]),
            "memory_limit" => accepted([TransactionSqlRow::PragmaMemoryLimit]),
            "io_stats" => accepted([TransactionSqlRow::PragmaIoStats]),
            _ => rejected(
                [],
                "pragma_not_in_transaction_matrix",
                "only PRAGMA cache_size, memory_limit, and io_stats have an approved local-only transaction row",
            ),
        },
    }
}

/// Returns the classification or the stable preflight rejection.  The caller
/// can therefore reject an unsupported distributed request before invoking the
/// normal planner or obtaining a participant handle.
pub fn preflight_transaction_statement(
    statement: &Statement,
) -> TransactionSqlPreflightResult<TransactionSqlClassification> {
    let classification = classify_transaction_statement(statement);
    preflight_classification(classification)
}

fn preflight_classification(
    classification: TransactionSqlClassification,
) -> TransactionSqlPreflightResult<TransactionSqlClassification> {
    if let Some(error) = classification.preflight_rejection.clone() {
        return Err(Box::new(TransactionSqlPreflightFailure {
            classification,
            error,
        }));
    }
    if classification.status == TransactionSqlStatus::LocalOnly {
        return Err(Box::new(TransactionSqlPreflightFailure {
            classification,
            error: TransactionSqlPreflightError {
                code: "transaction_sql_local_only".to_string(),
                reason: "this SQL form retains its local-only behavior and cannot open a distributed participant"
                    .to_string(),
            },
        }));
    }
    Ok(classification)
}

/// Classifies first and only then invokes the existing planner/routing-input
/// builder.  Local callers continue to call
/// [`crate::planner::plan_statement_for_routing`] directly and retain v0.8
/// behavior; distributed callers use this wrapper to guarantee preflight.
pub fn plan_transaction_statement_for_routing<C: Catalog + ?Sized>(
    catalog: &C,
    statement: &Statement,
) -> Result<TransactionPlannedStatement, TransactionSqlPlanningError> {
    let classification = preflight_transaction_statement(statement)?;
    let planned = crate::planner::plan_statement_for_routing(catalog, statement)?;
    if matches!(&statement.kind, StatementKind::Select(_))
        && classification
            .rows
            .iter()
            .any(|entry| entry.row == TransactionSqlRow::SelectSingleTable)
    {
        validate_planned_transaction_select(&classification, &planned)?;
    }
    Ok(TransactionPlannedStatement {
        classification,
        planned,
    })
}

fn validate_planned_transaction_select(
    classification: &TransactionSqlClassification,
    planned: &PlannedStatement,
) -> Result<(), TransactionSqlPlanningError> {
    let catalog = RemoteReadCatalogV0_8;
    match catalog.classify(&planned.plan, planned.table_references()) {
        RemoteReadClassification::Supported(_) => Ok(()),
        RemoteReadClassification::LocalOnly(rejection) => Err(remote_catalog_failure(
            classification,
            rejection,
            TransactionSqlStatus::LocalOnly,
        )),
        RemoteReadClassification::UnsupportedRemote(rejection) => Err(remote_catalog_failure(
            classification,
            rejection,
            TransactionSqlStatus::PreExecutionReject,
        )),
    }
}

fn remote_catalog_failure(
    classification: &TransactionSqlClassification,
    rejection: crate::distributed_read::RemoteReadRejection,
    status: TransactionSqlStatus,
) -> TransactionSqlPlanningError {
    let mut structured = classification.clone();
    structured.status = status;
    structured.preflight_rejection = Some(TransactionSqlPreflightError {
        code: rejection.code.clone(),
        reason: rejection.reason.clone(),
    });
    TransactionSqlPlanningError::Preflight(Box::new(TransactionSqlPreflightFailure {
        classification: structured,
        error: TransactionSqlPreflightError {
            code: rejection.code,
            reason: rejection.reason,
        },
    }))
}

fn classify_select(select: &Select) -> TransactionSqlClassification {
    let mut rows = BTreeSet::new();
    let mut rejection = None;

    if select.from.is_empty() {
        rows.insert(TransactionSqlRow::SelectLiteral);
    } else if select.from.len() == 1 {
        collect_from_rows(&select.from[0], &mut rows, &mut rejection);
    } else {
        set_rejection(
            &mut rejection,
            "distributed_transaction_multi_from_unsupported",
            "the approved transaction catalog permits exactly one table source",
        );
    }

    if select.distinct {
        rows.insert(TransactionSqlRow::Distinct);
    }
    if let Some(selection) = &select.selection {
        rows.insert(TransactionSqlRow::Where);
        collect_expression_rows(selection, &mut rows, &mut rejection);
    }
    if let Some(group_by) = &select.group_by {
        rows.insert(TransactionSqlRow::GroupBy);
        for expression in group_by {
            collect_expression_rows(expression, &mut rows, &mut rejection);
        }
    }
    if let Some(having) = &select.having {
        rows.insert(TransactionSqlRow::Having);
        collect_expression_rows(having, &mut rows, &mut rejection);
    }
    if !select.order_by.is_empty() {
        rows.insert(TransactionSqlRow::OrderBy);
        for order in &select.order_by {
            collect_expression_rows(&order.expr, &mut rows, &mut rejection);
        }
    }
    if let Some(limit) = &select.limit {
        rows.insert(TransactionSqlRow::Limit);
        collect_expression_rows(limit, &mut rows, &mut rejection);
    }
    if let Some(offset) = &select.offset {
        rows.insert(TransactionSqlRow::Offset);
        collect_expression_rows(offset, &mut rows, &mut rejection);
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expression_rows(expr, &mut rows, &mut rejection);
        }
    }

    finish(rows, rejection)
}

fn collect_from_rows(
    item: &FromItem,
    rows: &mut BTreeSet<TransactionSqlRow>,
    rejection: &mut Option<TransactionSqlPreflightError>,
) {
    match item {
        FromItem::Table { .. } => {
            rows.insert(TransactionSqlRow::SelectSingleTable);
        }
        FromItem::Derived { .. } => {
            set_rejection(
                rejection,
                "unregistered_distributed_transaction_sql_form",
                "derived table sources are not an approved transaction matrix row",
            );
        }
        FromItem::Join { join_type, .. } => {
            match join_type {
                crate::ast::dml::JoinType::Inner => rows.insert(TransactionSqlRow::JoinInner),
                crate::ast::dml::JoinType::Left => rows.insert(TransactionSqlRow::JoinLeft),
                crate::ast::dml::JoinType::Right => rows.insert(TransactionSqlRow::JoinRight),
                crate::ast::dml::JoinType::Full => rows.insert(TransactionSqlRow::JoinFull),
                crate::ast::dml::JoinType::Cross => false,
            };
            let (code, reason) = if matches!(join_type, crate::ast::dml::JoinType::Cross) {
                (
                    "unregistered_distributed_transaction_sql_form",
                    "CROSS JOIN is not an approved transaction matrix row",
                )
            } else {
                (
                    "distributed_transaction_join_unsupported",
                    "joins are outside the approved one-table distributed transaction catalog",
                )
            };
            set_rejection(rejection, code, reason);
        }
    }
}

fn collect_expression_rows(
    expression: &Expr,
    rows: &mut BTreeSet<TransactionSqlRow>,
    rejection: &mut Option<TransactionSqlPreflightError>,
) {
    match &expression.kind {
        ExprKind::Literal { .. } | ExprKind::ColumnRef { .. } => {}
        ExprKind::VectorLiteral { .. } => set_rejection(
            rejection,
            "transaction_sql_vector_local_only",
            "vector SQL remains local-only and cannot open a distributed transaction participant",
        ),
        ExprKind::BinaryOp { left, op, right } => {
            let row = match op {
                BinaryOp::Eq
                | BinaryOp::Neq
                | BinaryOp::Lt
                | BinaryOp::Gt
                | BinaryOp::LtEq
                | BinaryOp::GtEq => TransactionSqlRow::Comparison,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    TransactionSqlRow::Arithmetic
                }
                BinaryOp::And | BinaryOp::Or => TransactionSqlRow::Boolean,
                BinaryOp::StringConcat => {
                    set_rejection(
                        rejection,
                        "unregistered_distributed_transaction_sql_form",
                        "string concatenation is not an approved transaction matrix row",
                    );
                    collect_expression_rows(left, rows, rejection);
                    collect_expression_rows(right, rows, rejection);
                    return;
                }
            };
            rows.insert(row);
            collect_expression_rows(left, rows, rejection);
            collect_expression_rows(right, rows, rejection);
        }
        ExprKind::UnaryOp { operand, op } => {
            rows.insert(match op {
                crate::ast::UnaryOp::Not => TransactionSqlRow::Boolean,
                crate::ast::UnaryOp::Minus => TransactionSqlRow::Arithmetic,
            });
            collect_expression_rows(operand, rows, rejection);
        }
        ExprKind::FunctionCall { name, args, .. } => {
            let normalized = name.to_ascii_lowercase();
            if transaction_aggregate_function(&normalized) || normalized == "cast" {
                // The typed v0.8 remote catalog validates aggregate placement
                // and CAST operands after planner normalization.
            } else if REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS.contains(&normalized.as_str()) {
                set_rejection(
                    rejection,
                    "transaction_sql_scalar_local_only",
                    "a local-only or volatile scalar cannot be evaluated by a distributed transaction",
                );
            } else if !REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS.contains(&normalized.as_str()) {
                set_rejection(
                    rejection,
                    "transaction_sql_scalar_not_in_closed_catalog",
                    "the scalar identity is not registered in the closed distributed transaction catalog",
                );
            }
            for argument in args {
                collect_expression_rows(argument, rows, rejection);
            }
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            rows.insert(if *negated {
                TransactionSqlRow::NotBetween
            } else {
                TransactionSqlRow::Between
            });
            collect_expression_rows(expr, rows, rejection);
            collect_expression_rows(low, rows, rejection);
            collect_expression_rows(high, rows, rejection);
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            rows.insert(TransactionSqlRow::Pattern);
            collect_expression_rows(expr, rows, rejection);
            collect_expression_rows(pattern, rows, rejection);
            if let Some(escape) = escape {
                collect_expression_rows(escape, rows, rejection);
            }
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            rows.insert(if *negated {
                TransactionSqlRow::NotInList
            } else {
                TransactionSqlRow::InList
            });
            collect_expression_rows(expr, rows, rejection);
            for value in list {
                collect_expression_rows(value, rows, rejection);
            }
        }
        ExprKind::IsNull { expr, negated } => {
            rows.insert(if *negated {
                TransactionSqlRow::IsNotNull
            } else {
                TransactionSqlRow::IsNull
            });
            collect_expression_rows(expr, rows, rejection);
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            rows.insert(TransactionSqlRow::Case);
            if let Some(operand) = operand {
                collect_expression_rows(operand, rows, rejection);
            }
            for branch in branches {
                collect_expression_rows(&branch.when, rows, rejection);
                collect_expression_rows(&branch.then, rows, rejection);
            }
            if let Some(else_expr) = else_expr {
                collect_expression_rows(else_expr, rows, rejection);
            }
        }
        ExprKind::ScalarSubquery { .. } => reject_expression(
            rows,
            rejection,
            TransactionSqlRow::ScalarSubquery,
            "distributed_transaction_subquery_unsupported",
        ),
        ExprKind::InSubquery { expr, .. } => {
            collect_expression_rows(expr, rows, rejection);
            reject_expression(
                rows,
                rejection,
                TransactionSqlRow::InSubquery,
                "distributed_transaction_subquery_unsupported",
            );
        }
        ExprKind::Exists { .. } => reject_expression(
            rows,
            rejection,
            TransactionSqlRow::ExistsSubquery,
            "distributed_transaction_subquery_unsupported",
        ),
        ExprKind::Quantified { expr, .. } => {
            collect_expression_rows(expr, rows, rejection);
            reject_expression(
                rows,
                rejection,
                TransactionSqlRow::QuantifiedSubquery,
                "distributed_transaction_subquery_unsupported",
            );
        }
    }
}

fn reject_expression(
    rows: &mut BTreeSet<TransactionSqlRow>,
    rejection: &mut Option<TransactionSqlPreflightError>,
    row: TransactionSqlRow,
    code: &str,
) {
    rows.insert(row);
    set_rejection(rejection, code, row.behavior());
}

fn transaction_aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "count" | "sum" | "total" | "avg" | "min" | "max" | "group_concat" | "string_agg"
    )
}

fn accepted(rows: impl IntoIterator<Item = TransactionSqlRow>) -> TransactionSqlClassification {
    finish(rows.into_iter().collect(), None)
}

fn rejected(
    rows: impl IntoIterator<Item = TransactionSqlRow>,
    code: &str,
    reason: &str,
) -> TransactionSqlClassification {
    let mut classification = finish(
        rows.into_iter().collect(),
        Some(TransactionSqlPreflightError {
            code: code.to_string(),
            reason: reason.to_string(),
        }),
    );
    classification.status = TransactionSqlStatus::PreExecutionReject;
    classification
}

fn finish(
    rows: BTreeSet<TransactionSqlRow>,
    rejection: Option<TransactionSqlPreflightError>,
) -> TransactionSqlClassification {
    let status = if rejection.is_some() {
        TransactionSqlStatus::PreExecutionReject
    } else if rows
        .iter()
        .any(|row| row.status() == TransactionSqlStatus::LocalOnly)
    {
        TransactionSqlStatus::LocalOnly
    } else if rows
        .iter()
        .any(|row| row.status() == TransactionSqlStatus::SingleRange)
    {
        TransactionSqlStatus::SingleRange
    } else {
        TransactionSqlStatus::Distributed
    };
    TransactionSqlClassification {
        status,
        primary_row: rows.iter().next().copied().map(TransactionSqlRow::metadata),
        rows: rows.into_iter().map(TransactionSqlRow::metadata).collect(),
        preflight_rejection: rejection,
    }
}

fn set_rejection(rejection: &mut Option<TransactionSqlPreflightError>, code: &str, reason: &str) {
    rejection.get_or_insert_with(|| TransactionSqlPreflightError {
        code: code.to_string(),
        reason: reason.to_string(),
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::ast::{
        BinaryOp, Expr, ExprKind, FromItem, JoinType, Literal, Select, SelectItem, Span, Statement,
        StatementKind,
    };
    use crate::catalog::MemoryCatalog;

    use super::{
        TransactionCopyFormat, TransactionSqlControl, TransactionSqlRow, TransactionSqlStatus,
        TransactionUnsupportedConstruct, classify_transaction_control, classify_transaction_copy,
        classify_transaction_sql, classify_transaction_statement,
        classify_unsupported_transaction_construct, parse_and_preflight_transaction_statement,
        plan_transaction_statement_for_routing, preflight_transaction_copy,
        preflight_transaction_statement, transaction_sql_statement_matrix,
    };

    fn statement(kind: StatementKind) -> Statement {
        Statement {
            kind,
            span: Span::default(),
        }
    }

    fn literal(value: Literal) -> Expr {
        Expr::new(ExprKind::Literal { literal: value }, Span::default())
    }

    fn select(from: Vec<FromItem>) -> Statement {
        statement(StatementKind::Select(Select {
            distinct: false,
            projection: vec![SelectItem::Wildcard {
                span: Span::default(),
            }],
            from,
            selection: None,
            group_by: None,
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::default(),
        }))
    }

    fn literal_select(expr: Expr) -> Statement {
        statement(StatementKind::Select(Select {
            distinct: false,
            projection: vec![SelectItem::Expr {
                expr,
                alias: None,
                span: Span::default(),
            }],
            from: Vec::new(),
            selection: None,
            group_by: None,
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::default(),
        }))
    }

    #[test]
    fn matrix_registers_every_sql_txn_row_exactly_once() {
        let matrix = transaction_sql_statement_matrix();
        assert_eq!(matrix.len(), 51);
        let ids = matrix.iter().map(|entry| entry.id).collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 51);
        assert_eq!(matrix.first().unwrap().id, "SQL-TXN-01");
        assert_eq!(matrix.last().unwrap().id, "SQL-TXN-51");
        assert!(matrix.iter().all(|entry| !entry.behavior.is_empty()));
        assert!(matrix.iter().all(|entry| !entry.adapter_mapping.is_empty()));
        assert!(
            matrix
                .iter()
                .all(|entry| !entry.null_type_behavior.is_empty())
        );
        assert!(matrix.iter().all(|entry| !entry.order_behavior.is_empty()));
        assert!(matrix.iter().all(|entry| !entry.side_effect.is_empty()));
        assert!(matrix.iter().all(|entry| !entry.volatile));
    }

    #[test]
    fn supported_select_keeps_clause_metadata() {
        let mut select = match select(vec![FromItem::Table {
            name: "items".to_string(),
            alias: None,
            span: Span::default(),
        }])
        .kind
        {
            StatementKind::Select(select) => select,
            _ => unreachable!(),
        };
        select.distinct = true;
        select.selection = Some(Expr::new(
            ExprKind::BinaryOp {
                left: Box::new(Expr::new(
                    ExprKind::ColumnRef {
                        table: None,
                        column: "id".to_string(),
                    },
                    Span::default(),
                )),
                op: BinaryOp::Gt,
                right: Box::new(literal(Literal::Number("3".to_string()))),
            },
            Span::default(),
        ));
        select.limit = Some(literal(Literal::Number("4".to_string())));
        let classification =
            classify_transaction_statement(&statement(StatementKind::Select(select)));

        assert_eq!(classification.status, TransactionSqlStatus::Distributed);
        assert!(classification.is_preflight_accepted());
        let ids = classification
            .rows
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "SQL-TXN-06",
                "SQL-TXN-15",
                "SQL-TXN-16",
                "SQL-TXN-28",
                "SQL-TXN-30"
            ]
        );
    }

    #[test]
    fn ddl_and_join_are_rejected_before_planning() {
        let create = statement(StatementKind::DropTable(crate::ast::DropTable {
            if_exists: false,
            name: "items".to_string(),
            span: Span::default(),
        }));
        let error =
            plan_transaction_statement_for_routing(&MemoryCatalog::new(), &create).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("distributed_transaction_ddl_unsupported")
        );

        let join = select(vec![FromItem::Join {
            left: Box::new(FromItem::Table {
                name: "left".to_string(),
                alias: None,
                span: Span::default(),
            }),
            right: Box::new(FromItem::Table {
                name: "right".to_string(),
                alias: None,
                span: Span::default(),
            }),
            join_type: JoinType::Inner,
            condition: None,
            using: None,
            span: Span::default(),
        }]);
        let classification = classify_transaction_statement(&join);
        assert_eq!(
            classification.status,
            TransactionSqlStatus::PreExecutionReject
        );
        assert!(
            classification
                .rows
                .iter()
                .any(|entry| entry.row == TransactionSqlRow::JoinInner)
        );
    }

    #[test]
    fn control_copy_and_local_pragma_use_fixed_boundaries() {
        assert_eq!(
            classify_transaction_control(TransactionSqlControl::Begin).status,
            TransactionSqlStatus::Distributed
        );
        assert_eq!(
            classify_transaction_copy(TransactionCopyFormat::Csv, 1).status,
            TransactionSqlStatus::SingleRange
        );
        assert_eq!(
            classify_transaction_copy(TransactionCopyFormat::Parquet, 2).status,
            TransactionSqlStatus::PreExecutionReject
        );
        assert_eq!(
            classify_unsupported_transaction_construct(TransactionUnsupportedConstruct::Window)
                .status,
            TransactionSqlStatus::PreExecutionReject
        );

        let pragma = statement(StatementKind::Pragma {
            name: "cache_size".to_string(),
            value: None,
        });
        assert_eq!(
            classify_transaction_statement(&pragma).status,
            TransactionSqlStatus::LocalOnly
        );
        let failure = preflight_transaction_statement(&pragma).unwrap_err();
        assert_eq!(
            failure.classification.status,
            TransactionSqlStatus::LocalOnly
        );
        assert_eq!(failure.error.code, "transaction_sql_local_only");
        assert!(preflight_transaction_copy(TransactionCopyFormat::Parquet, 1).is_ok());
    }

    #[test]
    fn parser_boundary_preflights_before_planning() {
        let parsed = parse_and_preflight_transaction_statement("SELECT 1").unwrap();
        assert_eq!(
            parsed.classification.status,
            TransactionSqlStatus::Distributed
        );

        let error = parse_and_preflight_transaction_statement("DROP TABLE t").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("distributed_transaction_ddl_unsupported")
        );
    }

    #[test]
    fn raw_sql_classifier_reaches_non_ast_matrix_rows_safely() {
        let begin = classify_transaction_sql("-- BEGIN in a comment\nBEGIN");
        assert_eq!(begin.status, TransactionSqlStatus::Distributed);
        assert_eq!(begin.primary_row.unwrap().row, TransactionSqlRow::Begin);

        let copy = classify_transaction_sql("COPY records FROM 'records.csv' CSV");
        assert_eq!(copy.status, TransactionSqlStatus::SingleRange);
        assert_eq!(copy.primary_row.unwrap().row, TransactionSqlRow::CopyCsv);

        let case = classify_transaction_sql("SELECT CASE WHEN true THEN 1 ELSE 0 END");
        assert_eq!(case.status, TransactionSqlStatus::Distributed);
        assert_eq!(
            case.primary_row.unwrap().row,
            TransactionSqlRow::SelectLiteral
        );
        assert!(
            case.rows
                .iter()
                .any(|entry| entry.row == TransactionSqlRow::Case)
        );
        assert!(case.preflight_rejection.is_none());

        let parsed = parse_and_preflight_transaction_statement(
            "SELECT CASE 2 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END",
        )
        .unwrap();
        assert!(matches!(
            &parsed.statement.kind,
            StatementKind::Select(Select {
                projection,
                ..
            }) if matches!(
                &projection[0],
                SelectItem::Expr {
                    expr: Expr { kind: ExprKind::Case { operand: Some(_), branches, else_expr: Some(_) }, .. },
                    ..
                } if branches.len() == 2
            )
        ));
        assert!(
            plan_transaction_statement_for_routing(&MemoryCatalog::new(), &parsed.statement)
                .is_ok()
        );

        for (sql, expected_code) in [
            (
                "SELECT CASE WHEN true THEN random() ELSE 0 END",
                "transaction_sql_scalar_local_only",
            ),
            (
                "SELECT CASE WHEN true THEN [1.0, 2.0] ELSE [3.0, 4.0] END",
                "transaction_sql_vector_local_only",
            ),
            (
                "SELECT CASE WHEN true THEN (SELECT 1) ELSE 0 END",
                "distributed_transaction_subquery_unsupported",
            ),
            (
                "SELECT CASE WHEN true THEN unregistered_case_scalar() ELSE 0 END",
                "transaction_sql_scalar_not_in_closed_catalog",
            ),
            (
                "SELECT 1 LIMIT CASE WHEN true THEN random() ELSE 1 END",
                "transaction_sql_scalar_local_only",
            ),
            (
                "SELECT 1 LIMIT CASE WHEN true THEN [1.0, 2.0] ELSE [3.0, 4.0] END",
                "transaction_sql_vector_local_only",
            ),
            (
                "SELECT 1 LIMIT CASE WHEN true THEN (SELECT 1) ELSE 1 END",
                "distributed_transaction_subquery_unsupported",
            ),
            (
                "SELECT 1 LIMIT CASE WHEN true THEN unregistered_case_scalar() ELSE 1 END",
                "transaction_sql_scalar_not_in_closed_catalog",
            ),
            (
                "SELECT 1 LIMIT 1 OFFSET CASE WHEN true THEN random() ELSE 1 END",
                "transaction_sql_scalar_local_only",
            ),
            (
                "SELECT 1 LIMIT 1 OFFSET CASE WHEN true THEN [1.0, 2.0] ELSE [3.0, 4.0] END",
                "transaction_sql_vector_local_only",
            ),
            (
                "SELECT 1 LIMIT 1 OFFSET CASE WHEN true THEN (SELECT 1) ELSE 1 END",
                "distributed_transaction_subquery_unsupported",
            ),
            (
                "SELECT 1 LIMIT 1 OFFSET CASE WHEN true THEN unregistered_case_scalar() ELSE 1 END",
                "transaction_sql_scalar_not_in_closed_catalog",
            ),
        ] {
            let classification = classify_transaction_sql(sql);
            assert_eq!(
                classification.status,
                TransactionSqlStatus::PreExecutionReject
            );
            assert_eq!(
                classification
                    .preflight_rejection
                    .as_ref()
                    .map(|rejection| rejection.code.as_str()),
                Some(expected_code)
            );
            assert!(
                classification
                    .rows
                    .iter()
                    .any(|entry| entry.row == TransactionSqlRow::Case)
            );
        }

        let union = classify_transaction_sql("SELECT 1 UNION SELECT 2");
        assert_eq!(union.status, TransactionSqlStatus::PreExecutionReject);
        assert_eq!(union.primary_row.unwrap().row, TransactionSqlRow::Union);

        let literal = classify_transaction_sql("SELECT 'UNION' AS value");
        assert_eq!(literal.status, TransactionSqlStatus::Distributed);

        let identifier = classify_transaction_sql("SELECT union FROM records");
        assert_eq!(identifier.status, TransactionSqlStatus::Distributed);
    }

    #[test]
    fn scalar_catalog_is_fail_closed_before_routing() {
        let random = statement(StatementKind::Select(Select {
            distinct: false,
            projection: vec![SelectItem::Expr {
                expr: Expr::new(
                    ExprKind::FunctionCall {
                        name: "random".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        star: false,
                    },
                    Span::default(),
                ),
                alias: None,
                span: Span::default(),
            }],
            from: Vec::new(),
            selection: None,
            group_by: None,
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::default(),
        }));
        let classification = classify_transaction_statement(&random);
        assert_eq!(
            classification.status,
            TransactionSqlStatus::PreExecutionReject
        );
        assert_eq!(
            classification.preflight_rejection.unwrap().code,
            "transaction_sql_scalar_local_only"
        );

        let aggregate = literal_select(Expr::new(
            ExprKind::FunctionCall {
                name: "count".to_string(),
                args: Vec::new(),
                distinct: false,
                star: true,
            },
            Span::default(),
        ));
        assert_eq!(
            classify_transaction_statement(&aggregate).status,
            TransactionSqlStatus::Distributed
        );

        let vector = literal_select(Expr::new(
            ExprKind::VectorLiteral {
                values: vec![1.0, 2.0],
            },
            Span::default(),
        ));
        assert_eq!(
            classify_transaction_statement(&vector).status,
            TransactionSqlStatus::PreExecutionReject
        );
    }
}
