use crate::ast::{Expr, Statement};
use crate::dialect::Dialect;
use crate::error::Result;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct Parser<'a> {
    _dialect: PhantomData<&'a dyn Dialect>,
}

impl<'a> Parser<'a> {
    pub fn parse_sql(_dialect: &'a dyn Dialect, sql: &str) -> Result<Vec<Statement>> {
        crate::nim_bridge::parse_sql(sql)
    }

    pub fn parse_expression_sql(_dialect: &'a dyn Dialect, sql: &str) -> Result<Expr> {
        crate::nim_bridge::parse_expression_sql(sql)
    }
}
