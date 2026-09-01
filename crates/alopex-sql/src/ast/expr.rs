use super::ddl::DataType;
use super::dml::OrderByExpr;
use super::span::{Span, Spanned};
use serde::{Deserialize, Serialize};

pub(crate) const INTERNAL_TRUTH_TRUE: &str = "__alopex_truth_true";
pub(crate) const INTERNAL_TRUTH_FALSE: &str = "__alopex_truth_false";
pub(crate) const INTERNAL_TRUTH_UNKNOWN: &str = "__alopex_truth_unknown";
pub(crate) const INTERNAL_ROW_EQ: &str = "__alopex_row_eq";
pub(crate) const INTERNAL_ROW_NEQ: &str = "__alopex_row_neq";
pub(crate) const INTERNAL_ROW_LT: &str = "__alopex_row_lt";
pub(crate) const INTERNAL_ROW_LTEQ: &str = "__alopex_row_lteq";
pub(crate) const INTERNAL_ROW_GT: &str = "__alopex_row_gt";
pub(crate) const INTERNAL_ROW_GTEQ: &str = "__alopex_row_gteq";
pub(crate) const INTERNAL_ROW_DISTINCT: &str = "__alopex_row_distinct";
pub(crate) const INTERNAL_ROW_BETWEEN: &str = "__alopex_row_between";
pub(crate) const INTERNAL_ROW_IN: &str = "__alopex_row_in";

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
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<CaseWhen>,
        else_expr: Option<Box<Expr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        star: bool,
        /// Aggregate-local ordering from `agg(expr ORDER BY ...)` (issue #148).
        #[serde(default)]
        order_by: Vec<OrderByExpr>,
        /// Ordered-set aggregate ordering from `WITHIN GROUP (ORDER BY ...)`.
        #[serde(default)]
        within_group: Vec<OrderByExpr>,
        /// Aggregate row filter from `FILTER (WHERE predicate)`.
        #[serde(default)]
        filter: Option<Box<Expr>>,
        #[serde(default)]
        over: Option<WindowSpec>,
    },
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },
    /// A cast that returns NULL when conversion is impossible.
    TryCast {
        expr: Box<Expr>,
        target_type: DataType,
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
        #[serde(default)]
        kind: PatternMatchKind,
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
    /// A parenthesized row-value constructor used by row predicates.
    Row {
        items: Vec<Expr>,
    },
    /// `IS [NOT] TRUE/FALSE/UNKNOWN`.
    TruthPredicate {
        expr: Box<Expr>,
        value: TruthValue,
        negated: bool,
    },
    /// Null-safe scalar or row equality.
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
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
    /// One-based positional `?` bind parameter.
    ///
    /// Appended to preserve existing bincode discriminants.
    Parameter {
        index: usize,
    },
}

/// A window specification attached to a function call through `OVER name` or
/// `OVER (...)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    /// Optional named specification inherited by this window.
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub partition_by: Vec<Expr>,
    #[serde(default)]
    pub order_by: Vec<OrderByExpr>,
    /// Optional explicit frame. `None` selects the SQL implicit frame.
    #[serde(default)]
    pub frame: Option<WindowFrame>,
}

/// An explicit SQL window frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFrameUnits {
    Rows,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "variant", content = "value")]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseWhen {
    pub when: Expr,
    pub then: Expr,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatternMatchKind {
    #[default]
    Like,
    ILike,
    Glob,
    SimilarTo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "variant", content = "value")]
pub enum Literal {
    Number(String),
    String(String),
    Boolean(bool),
    Null,
    /// SQL-TS interval text, preserved for a downstream semantic layer.
    ///
    /// Appended to preserve existing bincode discriminants.
    Interval(String),
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
    BitAnd,
    BitOr,
    BitXor,
    ShiftLeft,
    ShiftRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Minus,
    BitNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Quantifier {
    Any,
    All,
}
