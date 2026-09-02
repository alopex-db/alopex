pub mod ddl;
pub mod dml;
pub mod expr;
pub mod span;

pub use ddl::*;
pub use dml::*;
pub use expr::*;
use serde::{Deserialize, Serialize};
pub use span::{Location, Span, Spanned};

/// Top-level SQL statement wrapper with span information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    pub kind: StatementKind,
    pub span: Span,
}

/// Value supplied to a PRAGMA statement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PragmaValue {
    /// Integer setting.
    Int(i64),
    /// Text setting.
    Text(String),
}

/// SQL transaction isolation names accepted by the parser.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// SQL transaction access mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// Output encoding requested by an `EXPLAIN` statement.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExplainFormat {
    /// Stable human-readable tree. This format may change between releases.
    Text,
    /// Versioned machine-readable JSON document.
    Json,
}

impl Spanned for Statement {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
#[allow(clippy::large_enum_variant)]
pub enum StatementKind {
    /// Describe a nested statement, optionally executing it for runtime metrics.
    Explain {
        /// Execute the nested statement and collect timing and row counts.
        analyze: bool,
        /// Requested output encoding.
        format: ExplainFormat,
        /// Statement being described.
        statement: Box<Statement>,
    },
    // DDL
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateView(CreateView),
    DropView(DropView),
    AlterTable(AlterTable),
    Truncate(Truncate),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateSequence(CreateSequence),
    AlterSequence(AlterSequence),
    DropSequence(DropSequence),
    /// A Skulk-owned continuous aggregate definition carried but not executed by Alopex.
    CreateContinuousAggregate(CreateContinuousAggregate),

    /// Runtime configuration or statistics statement.
    Pragma {
        /// PRAGMA name.
        name: String,
        /// Optional assignment value.
        value: Option<PragmaValue>,
    },

    /// Begin an explicit SQL transaction (`BEGIN` or `START TRANSACTION`).
    Begin {
        isolation_level: Option<TransactionIsolationLevel>,
        access_mode: Option<TransactionAccessMode>,
    },
    /// Set characteristics before the active transaction executes work.
    SetTransaction {
        isolation_level: Option<TransactionIsolationLevel>,
        access_mode: Option<TransactionAccessMode>,
    },
    /// Commit the active explicit SQL transaction.
    Commit,
    /// Roll back the active explicit SQL transaction.
    Rollback,
    /// Create a named savepoint in the active transaction.
    Savepoint {
        name: String,
    },
    /// Roll back to a named savepoint while keeping it active.
    RollbackToSavepoint {
        name: String,
    },
    /// Release a named savepoint and every savepoint nested after it.
    ReleaseSavepoint {
        name: String,
    },

    // DML
    Select(Select),
    Values(Values),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Merge(Merge),
    Copy(CopyStatement),
}

impl StatementKind {
    /// Returns whether this statement produces a query result.
    pub fn is_query(&self) -> bool {
        match self {
            Self::Explain { .. } | Self::Select(_) | Self::Values(_) => true,
            Self::Pragma { name, .. } => {
                matches!(name.as_str(), "show_tables" | "show_indexes" | "describe")
            }
            _ => false,
        }
    }
}
