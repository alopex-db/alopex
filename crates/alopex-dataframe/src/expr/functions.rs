use crate::expr::expr::Scalar;
use crate::Expr;

/// Create an expression that refers to a column by name (case-sensitive).
pub fn col(_name: &str) -> Expr {
    Expr::Column(_name.to_string())
}

/// Create a literal expression from a scalar value.
pub fn lit<T>(value: T) -> Expr
where
    T: Into<Scalar>,
{
    Expr::Literal(value.into())
}

/// Create a wildcard expression that expands to all columns in projections.
pub fn all() -> Expr {
    Expr::Wildcard
}
