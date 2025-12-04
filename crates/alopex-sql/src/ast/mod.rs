pub mod ddl;
pub mod dml;
pub mod expr;
pub mod span;

pub use ddl::*;
pub use dml::*;
pub use expr::*;
pub use span::{Location, Span, Spanned};

/// Top-level SQL statement wrapper with span information.
#[derive(Debug, Clone)]
pub struct Statement {
    pub kind: StatementKind,
    pub span: Span,
}

impl Spanned for Statement {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone)]
pub enum StatementKind {
    // DDL
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),

    // DML
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}
