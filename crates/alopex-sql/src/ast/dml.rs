use super::expr::Expr;
use super::span::{Span, Spanned};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub selection: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    #[serde(default)]
    pub span: Span,
}

pub const LITERAL_TABLE: &str = "__literal__";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
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
        span: Span,
    },
    Derived {
        subquery: Box<super::Statement>,
        alias: Option<String>,
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
