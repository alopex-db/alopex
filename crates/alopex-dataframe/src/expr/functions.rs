use crate::Expr;

pub fn col(_name: &str) -> Expr {
    Expr::new()
}

pub fn lit<T>(_value: T) -> Expr {
    Expr::new()
}

pub fn all() -> Expr {
    Expr::new()
}
