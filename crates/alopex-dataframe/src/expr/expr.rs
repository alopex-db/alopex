/// Expression AST used by `DataFrame` and `LazyFrame`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference.
    Column(String),
    /// Literal scalar value.
    Literal(Scalar),
    /// Binary operator expression.
    BinaryOp {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    /// Unary operator expression.
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// Aggregation expression (only valid under `group_by().agg()`).
    Agg { func: AggFunc, expr: Box<Expr> },
    /// Expression alias (renames the resulting column).
    Alias { expr: Box<Expr>, name: String },
    /// Wildcard (`*`) that expands to all columns in projections.
    Wildcard,
}

/// Supported binary operators.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
    /// Equality.
    Eq,
    /// Inequality.
    Neq,
    /// Greater-than.
    Gt,
    /// Less-than.
    Lt,
    /// Greater-than-or-equal.
    Ge,
    /// Less-than-or-equal.
    Le,
    /// Boolean AND.
    And,
    /// Boolean OR.
    Or,
}

/// Supported unary operators.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Boolean NOT.
    Not,
}

/// Supported aggregation functions.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggFunc {
    /// Sum of non-null values.
    Sum,
    /// Mean of non-null values.
    Mean,
    /// Count of non-null values.
    Count,
    /// Minimum of non-null values.
    Min,
    /// Maximum of non-null values.
    Max,
}

/// Scalar literal values.
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    /// Null literal.
    Null,
    /// Boolean literal.
    Boolean(bool),
    /// 64-bit integer literal.
    Int64(i64),
    /// 64-bit float literal.
    Float64(f64),
    /// UTF-8 string literal.
    Utf8(String),
}

impl From<()> for Scalar {
    fn from(_: ()) -> Self {
        Scalar::Null
    }
}

impl From<bool> for Scalar {
    fn from(v: bool) -> Self {
        Scalar::Boolean(v)
    }
}

impl From<i64> for Scalar {
    fn from(v: i64) -> Self {
        Scalar::Int64(v)
    }
}

impl From<f64> for Scalar {
    fn from(v: f64) -> Self {
        Scalar::Float64(v)
    }
}

impl From<String> for Scalar {
    fn from(v: String) -> Self {
        Scalar::Utf8(v)
    }
}

impl From<&str> for Scalar {
    fn from(v: &str) -> Self {
        Scalar::Utf8(v.to_string())
    }
}

impl Expr {
    /// Alias this expression (used to name output columns).
    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias {
            expr: Box::new(self),
            name: name.into(),
        }
    }

    /// Build an addition expression.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Add,
            right: Box::new(rhs),
        }
    }

    /// Build a subtraction expression.
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Sub,
            right: Box::new(rhs),
        }
    }

    /// Build a multiplication expression.
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Mul,
            right: Box::new(rhs),
        }
    }

    /// Build a division expression.
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Div,
            right: Box::new(rhs),
        }
    }

    /// Build an equality predicate.
    pub fn eq(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Eq,
            right: Box::new(rhs),
        }
    }

    /// Build an inequality predicate.
    pub fn neq(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Neq,
            right: Box::new(rhs),
        }
    }

    /// Build a greater-than predicate.
    pub fn gt(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Gt,
            right: Box::new(rhs),
        }
    }

    /// Build a less-than predicate.
    pub fn lt(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Lt,
            right: Box::new(rhs),
        }
    }

    /// Build a greater-than-or-equal predicate.
    pub fn ge(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Ge,
            right: Box::new(rhs),
        }
    }

    /// Build a less-than-or-equal predicate.
    pub fn le(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Le,
            right: Box::new(rhs),
        }
    }

    /// Build a boolean AND predicate.
    pub fn and_(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::And,
            right: Box::new(rhs),
        }
    }

    /// Build a boolean OR predicate.
    pub fn or_(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Or,
            right: Box::new(rhs),
        }
    }

    /// Build a boolean NOT predicate.
    pub fn not_(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(self),
        }
    }

    /// Build a `sum` aggregation.
    pub fn sum(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Sum,
            expr: Box::new(self),
        }
    }

    /// Build a `mean` aggregation.
    pub fn mean(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Mean,
            expr: Box::new(self),
        }
    }

    /// Build a `count` aggregation (nulls excluded).
    pub fn count(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Count,
            expr: Box::new(self),
        }
    }

    /// Build a `min` aggregation.
    pub fn min(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Min,
            expr: Box::new(self),
        }
    }

    /// Build a `max` aggregation.
    pub fn max(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Max,
            expr: Box::new(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AggFunc, Expr, Operator, Scalar, UnaryOperator};
    use crate::expr::{col, lit};

    #[test]
    fn builder_and_chaining_works() {
        let expr = col("a").add(lit(1_i64)).alias("b");
        assert_eq!(
            expr,
            Expr::Alias {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("a".to_string())),
                    op: Operator::Add,
                    right: Box::new(Expr::Literal(Scalar::Int64(1))),
                }),
                name: "b".to_string(),
            }
        );
    }

    #[test]
    fn logical_and_agg_works() {
        let expr = col("x")
            .gt(lit(1_i64))
            .and_(col("y").lt(lit(10_i64)).not_())
            .alias("p");

        assert!(matches!(
            expr,
            Expr::Alias {
                expr: _,
                name
            } if name == "p"
        ));

        let agg = col("v").sum();
        assert_eq!(
            agg,
            Expr::Agg {
                func: AggFunc::Sum,
                expr: Box::new(Expr::Column("v".to_string()))
            }
        );

        let u = Expr::Column("a".to_string()).not_();
        assert_eq!(
            u,
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Column("a".to_string()))
            }
        );
    }
}
