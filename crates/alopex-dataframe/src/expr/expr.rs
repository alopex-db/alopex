#[derive(Debug, Clone)]
pub struct Expr {
    pub(crate) _private: (),
}

impl Expr {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}
