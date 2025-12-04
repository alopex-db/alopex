use crate::ast::{Expr, Statement};
use crate::error::Result;
use crate::parser::Parser;
use crate::parser::precedence::Precedence;

/// SQL方言ごとのカスタマイズポイントを提供するトrait。
pub trait Dialect: std::fmt::Debug {
    /// 識別子の先頭として利用可能な文字かを判定する。
    fn is_identifier_start(&self, ch: char) -> bool;

    /// 識別子の残りの文字として利用可能な文字かを判定する。
    fn is_identifier_part(&self, ch: char) -> bool;

    /// 方言固有のステートメントパースを行う。対応しない場合は`None`を返す。
    fn parse_statement(&self, parser: &mut Parser<'_>) -> Option<Result<Statement>>;

    /// 方言固有のプレフィックスパース（例: ベクトルリテラル）を行う。
    fn parse_prefix(&self, parser: &mut Parser<'_>) -> Option<Result<Expr>>;

    /// 優先順位の数値を返す。方言で演算子優先順位を調整する場合に使う。
    fn prec_value(&self, prec: Precedence) -> u8;
}

/// Alopex標準のSQL方言。
#[derive(Debug, Default, Clone)]
pub struct AlopexDialect;

impl Dialect for AlopexDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphanumeric()
    }

    fn parse_statement(&self, _parser: &mut Parser<'_>) -> Option<Result<Statement>> {
        None
    }

    fn parse_prefix(&self, parser: &mut Parser<'_>) -> Option<Result<Expr>> {
        if matches!(
            parser.peek().token,
            crate::tokenizer::token::Token::LBracket
        ) {
            return Some(parser.parse_vector_literal());
        }
        None
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        prec.value()
    }
}
