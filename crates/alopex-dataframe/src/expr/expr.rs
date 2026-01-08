#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Scalar),
    BinaryOp {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Agg {
        func: AggFunc,
        expr: Box<Expr>,
    },
    Alias {
        expr: Box<Expr>,
        name: String,
    },
    Wildcard,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Operator {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Mean,
    Count,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
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
    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias {
            expr: Box::new(self),
            name: name.into(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Add,
            right: Box::new(rhs),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Sub,
            right: Box::new(rhs),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Mul,
            right: Box::new(rhs),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn div(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Div,
            right: Box::new(rhs),
        }
    }

    pub fn eq(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Eq,
            right: Box::new(rhs),
        }
    }

    pub fn neq(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Neq,
            right: Box::new(rhs),
        }
    }

    pub fn gt(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Gt,
            right: Box::new(rhs),
        }
    }

    pub fn lt(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Lt,
            right: Box::new(rhs),
        }
    }

    pub fn ge(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Ge,
            right: Box::new(rhs),
        }
    }

    pub fn le(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Le,
            right: Box::new(rhs),
        }
    }

    pub fn and_(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::And,
            right: Box::new(rhs),
        }
    }

    pub fn or_(self, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: Operator::Or,
            right: Box::new(rhs),
        }
    }

    pub fn not_(self) -> Expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(self),
        }
    }

    pub fn sum(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Sum,
            expr: Box::new(self),
        }
    }

    pub fn mean(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Mean,
            expr: Box::new(self),
        }
    }

    pub fn count(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Count,
            expr: Box::new(self),
        }
    }

    pub fn min(self) -> Expr {
        Expr::Agg {
            func: AggFunc::Min,
            expr: Box::new(self),
        }
    }

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
