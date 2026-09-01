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

impl Spanned for Statement {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
#[allow(clippy::large_enum_variant)]
pub enum StatementKind {
    // DDL
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
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
    Begin,
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
}

impl StatementKind {
    /// Returns whether this statement produces a query result without mutating data.
    pub const fn is_query(&self) -> bool {
        matches!(self, Self::Select(_) | Self::Values(_))
    }
}
