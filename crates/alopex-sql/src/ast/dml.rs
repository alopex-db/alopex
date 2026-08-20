use super::expr::Expr;
use super::span::{Span, Spanned};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    #[serde(default)]
    pub with: Option<WithClause>,
    pub distinct: bool,
    /// SELECT DISTINCT ON (expr, ...) key expressions in source order.
    /// Empty when the clause is absent (issue #150, contract 0.11.0).
    /// Mutually exclusive with `distinct` by grammar.
    #[serde(default)]
    pub distinct_on: Vec<Expr>,
    pub projection: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub selection: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    #[serde(default)]
    pub windows: Vec<NamedWindow>,
    #[serde(default)]
    pub qualify: Option<Expr>,
    #[serde(default)]
    pub set_operations: Vec<SetOperation>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    /// FETCH ... WITH TIES: the limit keeps every peer of the final row
    /// under the ORDER BY sort key (issue #152, contract 0.10.0).
    #[serde(default)]
    pub limit_with_ties: bool,
    #[serde(default)]
    pub span: Span,
}

/// A VALUES query body with the same set/order/limit tail as SELECT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Values {
    #[serde(default)]
    pub with: Option<WithClause>,
    pub rows: Vec<Vec<Expr>>,
    #[serde(default)]
    pub set_operations: Vec<SetOperation>,
    #[serde(default)]
    pub order_by: Vec<OrderByExpr>,
    #[serde(default)]
    pub limit: Option<Expr>,
    #[serde(default)]
    pub offset: Option<Expr>,
    /// FETCH ... WITH TIES on a VALUES tail (issue #152, contract 0.10.0).
    #[serde(default)]
    pub limit_with_ties: bool,
    #[serde(default)]
    pub span: Span,
}

/// A relational query body accepted by nested query positions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
#[allow(clippy::large_enum_variant)]
pub enum QueryBody {
    Select(Select),
    Values(Values),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedWindow {
    pub name: String,
    pub spec: super::expr::WindowSpec,
    #[serde(default)]
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetOperation {
    pub operator: SetOperator,
    pub all: bool,
    pub right: Box<QueryBody>,
    #[serde(default)]
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<CommonTableExpr>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonTableExpr {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    pub query: Box<QueryBody>,
    pub span: Span,
}

pub const LITERAL_TABLE: &str = "__literal__";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
// `Expr` grew with the issue #148 aggregate clauses; almost every SelectItem
// is the Expr variant, so boxing it would add a pointless allocation to the
// hot projection path.
#[allow(clippy::large_enum_variant)]
pub enum SelectItem {
    Wildcard {
        span: Span,
    },
    QualifiedWildcard {
        table: String,
        span: Span,
    },
    Expr {
        expr: Expr,
        alias: Option<String>,
        span: Span,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
#[allow(clippy::large_enum_variant)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
        span: Span,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        condition: Option<Expr>,
        using: Option<Vec<String>>,
        #[serde(default)]
        natural: bool,
        span: Span,
    },
    Derived {
        subquery: Box<QueryBody>,
        alias: Option<String>,
        #[serde(default)]
        columns: Vec<String>,
        span: Span,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub span: Span,
}

/// The row source for an INSERT statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
pub enum InsertSource {
    Values { values: Vec<Vec<Expr>> },
    Select { select: Box<Select> },
    Query { query: Box<QueryBody> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub selection: Option<Expr>,
    pub span: Span,
}

impl Spanned for Select {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Values {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for QueryBody {
    fn span(&self) -> Span {
        match self {
            QueryBody::Select(select) => select.span,
            QueryBody::Values(values) => values.span,
        }
    }
}

impl Spanned for SetOperation {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for SelectItem {
    fn span(&self) -> Span {
        match self {
            SelectItem::Wildcard { span } | SelectItem::QualifiedWildcard { span, .. } => *span,
            SelectItem::Expr { span, .. } => *span,
        }
    }
}

impl Spanned for FromItem {
    fn span(&self) -> Span {
        match self {
            FromItem::Table { span, .. }
            | FromItem::Join { span, .. }
            | FromItem::Derived { span, .. } => *span,
        }
    }
}

impl Spanned for OrderByExpr {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Insert {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Update {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Assignment {
    fn span(&self) -> Span {
        self.span
    }
}

impl Spanned for Delete {
    fn span(&self) -> Span {
        self.span
    }
}
