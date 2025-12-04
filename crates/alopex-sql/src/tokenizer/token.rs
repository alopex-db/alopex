use super::keyword::Keyword;
use crate::ast::span::Span;

/// Alopex SQL token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    EOF,

    /// Keywords or identifiers.
    Word(Word),

    /// Literals.
    Number(String),
    SingleQuotedString(String),

    /// Operators and punctuation.
    Comma,
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Period,
    Colon,
    SemiColon,
    StringConcat,
}

/// Word token (identifier or keyword).
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    pub value: String,
    pub quote_style: Option<char>, // Not used in v0.1 (no quoted identifiers)
    pub keyword: Keyword,
}

/// Token with attached span information.
#[derive(Debug, Clone, PartialEq)]
pub struct TokenWithSpan {
    pub token: Token,
    pub span: Span,
}
