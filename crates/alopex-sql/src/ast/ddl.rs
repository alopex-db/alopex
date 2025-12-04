use super::expr::Expr;
use super::span::{Span, Spanned};

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Int,
    BigInt,
    Float,
    Double,
    Text,
    Blob,
    Boolean,
    Bool,
    Timestamp,
    Vector {
        dimension: u32,
        metric: Option<VectorMetric>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum VectorMetric {
    Cosine,
    L2,
    Inner,
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    Default(Expr),
    /// Span for the constraint keyword/location.
    WithSpan {
        kind: Box<ColumnConstraint>,
        span: Span,
    },
}

#[derive(Debug, Clone)]
pub enum TableConstraint {
    PrimaryKey { columns: Vec<String>, span: Span },
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub if_exists: bool,
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub if_not_exists: bool,
    pub name: String,
    pub table: String,
    pub column: String,
    pub method: Option<IndexMethod>,
    pub options: Vec<IndexOption>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub enum IndexMethod {
    BTree,
    Hnsw,
}

#[derive(Debug, Clone)]
pub struct IndexOption {
    pub key: String,
    pub value: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub if_exists: bool,
    pub name: String,
    pub span: Span,
}

impl Spanned for CreateTable {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for ColumnDef {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for ColumnConstraint {
    fn span(&self) -> Span {
        match self {
            ColumnConstraint::WithSpan { span, .. } => *span,
            _ => Span::empty(),
        }
    }
}

impl Spanned for TableConstraint {
    fn span(&self) -> Span {
        match self {
            TableConstraint::PrimaryKey { span, .. } => *span,
        }
    }
}

impl Spanned for DropTable {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for CreateIndex {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for IndexOption {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for DropIndex {
    fn span(&self) -> Span {
        self.span
    }
}
