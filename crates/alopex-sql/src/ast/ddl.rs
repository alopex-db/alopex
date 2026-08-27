use super::dml::Select;
use super::expr::Expr;
use super::span::{Span, Spanned};
use serde::{Deserialize, Serialize};

/// A Skulk-owned continuous aggregate definition carried by the SQL parser.
///
/// Alopex owns the wire representation but does not execute this statement.
/// The generic-host behavior is added separately after every host boundary is
/// extension-safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateContinuousAggregate {
    pub name: String,
    pub name_span: Span,
    #[serde(with = "crate::nim_bridge::continuous_aggregate_select_wire")]
    pub query: Select,
    pub options: Vec<ContinuousAggregateOption>,
    pub span: Span,
}

/// One ordered option in a continuous aggregate definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousAggregateOption {
    pub key: String,
    pub key_span: Span,
    pub value: String,
    pub value_span: Span,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub with_options: Vec<IndexOption>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
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
    Date,
    Time,
    Interval,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Json,
    Vector {
        dimension: u32,
        metric: Option<VectorMetric>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    Cosine,
    L2,
    Inner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
// `Expr` grew with the issue #148 aggregate clauses; boxing `Default.value`
// would churn every construction/pattern site of a rarely-instantiated
// variant for no measurable gain.
#[allow(clippy::large_enum_variant)]
pub enum ColumnConstraint {
    NotNull { span: Span },
    PrimaryKey { span: Span },
    Unique { span: Span },
    Default { value: Expr, span: Span },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
pub enum TableConstraint {
    PrimaryKey { columns: Vec<String>, span: Span },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTable {
    pub if_exists: bool,
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIndex {
    pub if_not_exists: bool,
    pub name: String,
    pub table: String,
    pub column: String,
    pub method: Option<IndexMethod>,
    pub options: Vec<IndexOption>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexMethod {
    BTree,
    Hnsw,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexOption {
    pub key: String,
    pub value: String,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Spanned for CreateContinuousAggregate {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for ContinuousAggregateOption {
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
            ColumnConstraint::NotNull { span }
            | ColumnConstraint::PrimaryKey { span }
            | ColumnConstraint::Unique { span }
            | ColumnConstraint::Default { span, .. } => *span,
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
