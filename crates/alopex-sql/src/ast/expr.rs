use super::span::{Span, Spanned};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expr {
    pub kind: ExprKind,
    pub span: Span,
}

impl Expr {
    pub fn new(kind: ExprKind, span: Span) -> Self {
        Self { kind, span }
    }
}

impl Spanned for Expr {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant")]
#[allow(clippy::large_enum_variant)]
pub enum ExprKind {
    Literal {
        literal: Literal,
    },
    ColumnRef {
        table: Option<String>,
        column: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        star: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    VectorLiteral {
        values: Vec<f64>,
    },
    ScalarSubquery {
        subquery: Box<super::Statement>,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<super::Statement>,
        negated: bool,
    },
    Exists {
        subquery: Box<super::Statement>,
        negated: bool,
    },
    Quantified {
        expr: Box<Expr>,
        op: BinaryOp,
        quantifier: Quantifier,
        subquery: Box<super::Statement>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant", content = "value")]
pub enum Literal {
    Number(String),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    StringConcat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Quantifier {
    Any,
    All,
}
