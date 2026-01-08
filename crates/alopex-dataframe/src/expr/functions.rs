use crate::expr::expr::Scalar;
use crate::Expr;

pub fn col(_name: &str) -> Expr {
    Expr::Column(_name.to_string())
}

pub fn lit<T>(value: T) -> Expr
where
    T: Into<Scalar>,
{
    Expr::Literal(value.into())
}

pub fn all() -> Expr {
    Expr::Wildcard
}
