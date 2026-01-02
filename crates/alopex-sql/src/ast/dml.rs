use super::expr::Expr;
use super::span::{Span, Spanned};

#[derive(Debug, Clone)]
pub struct Select {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: TableRef,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard {
        span: Span,
    },
    Expr {
        expr: Expr,
        alias: Option<String>,
        span: Span,
    },
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
    pub span: Span,
}

#[derive(Debug, Clone)]
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
            SelectItem::Wildcard { span } => *span,
            SelectItem::Expr { span, .. } => *span,
        }
    }
}

impl Spanned for TableRef {
    fn span(&self) -> Span {
        self.span
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
