use super::Statement;
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
    #[serde(default)]
    pub temporary: bool,
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
    SmallSerial,
    Serial,
    BigSerial,
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
    Array {
        element: Box<DataType>,
    },
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },
    Struct {
        fields: Vec<StructField>,
    },
    Vector {
        dimension: u32,
        metric: Option<VectorMetric>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
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
    NotNull {
        #[serde(default)]
        name: Option<String>,
        span: Span,
    },
    PrimaryKey {
        #[serde(default)]
        name: Option<String>,
        span: Span,
    },
    Unique {
        #[serde(default)]
        name: Option<String>,
        span: Span,
    },
    Default {
        #[serde(default)]
        name: Option<String>,
        value: Expr,
        span: Span,
    },
    Check {
        #[serde(default)]
        name: Option<String>,
        expression: Box<Expr>,
        span: Span,
    },
    References {
        #[serde(default)]
        name: Option<String>,
        table: String,
        #[serde(default)]
        columns: Vec<String>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
        deferrable: bool,
        initially_deferred: bool,
        span: Span,
    },
    Identity {
        #[serde(default)]
        name: Option<String>,
        generation: IdentityGeneration,
        options: SequenceOptions,
        span: Span,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityGeneration {
    Always,
    ByDefault,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
pub enum TableConstraint {
    PrimaryKey {
        #[serde(default)]
        name: Option<String>,
        columns: Vec<String>,
        span: Span,
    },
    Unique {
        #[serde(default)]
        name: Option<String>,
        columns: Vec<String>,
        span: Span,
    },
    Check {
        #[serde(default)]
        name: Option<String>,
        expression: Box<Expr>,
        span: Span,
    },
    ForeignKey {
        #[serde(default)]
        name: Option<String>,
        columns: Vec<String>,
        referenced_table: String,
        #[serde(default)]
        referenced_columns: Vec<String>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
        deferrable: bool,
        initially_deferred: bool,
        span: Span,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTable {
    pub if_exists: bool,
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateView {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<Statement>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropView {
    pub if_exists: bool,
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTable {
    pub if_exists: bool,
    pub name: String,
    pub action: AlterTableAction,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
pub enum AlterTableAction {
    AddColumn {
        if_not_exists: bool,
        column: ColumnDef,
    },
    DropColumn {
        if_exists: bool,
        name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
    AlterColumn {
        name: String,
        action: AlterColumnAction,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
pub enum AlterColumnAction {
    SetDataType { data_type: DataType },
    SetDefault { value: Box<Expr> },
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Truncate {
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
    Fts,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SequenceOptions {
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<u64>,
    pub cycle: Option<bool>,
    pub restart: Option<i64>,
    pub restart_default: bool,
    pub owned_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSequence {
    pub if_not_exists: bool,
    pub name: String,
    pub options: SequenceOptions,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterSequence {
    pub if_exists: bool,
    pub name: String,
    pub options: SequenceOptions,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropSequence {
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
            ColumnConstraint::NotNull { span, .. }
            | ColumnConstraint::PrimaryKey { span, .. }
            | ColumnConstraint::Unique { span, .. }
            | ColumnConstraint::Check { span, .. }
            | ColumnConstraint::References { span, .. }
            | ColumnConstraint::Identity { span, .. }
            | ColumnConstraint::Default { span, .. } => *span,
        }
    }
}

impl Spanned for TableConstraint {
    fn span(&self) -> Span {
        match self {
            TableConstraint::PrimaryKey { span, .. }
            | TableConstraint::Unique { span, .. }
            | TableConstraint::Check { span, .. }
            | TableConstraint::ForeignKey { span, .. } => *span,
        }
    }
}

impl Spanned for DropTable {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for CreateView {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for DropView {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for AlterTable {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Truncate {
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

impl Spanned for CreateSequence {
    fn span(&self) -> Span {
        self.span
    }
}
impl Spanned for AlterSequence {
    fn span(&self) -> Span {
        self.span
    }
}
impl Spanned for DropSequence {
    fn span(&self) -> Span {
        self.span
    }
}
