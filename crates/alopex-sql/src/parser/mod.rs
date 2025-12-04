pub mod ddl;
pub mod expr;
pub mod precedence;
pub mod recursion;

use crate::Span;
use crate::ast::Expr;
use crate::dialect::Dialect;
use crate::error::{ParserError, Result};
use crate::tokenizer::token::{Token, TokenWithSpan, Word};
use precedence::Precedence;
use recursion::{DEFAULT_RECURSION_LIMIT, RecursionCounter};

pub struct Parser<'a> {
    tokens: Vec<TokenWithSpan>,
    pos: usize,
    pub(crate) recursion: RecursionCounter,
    _dialect: &'a dyn Dialect,
}

impl<'a> Parser<'a> {
    pub fn new(dialect: &'a dyn Dialect, tokens: Vec<TokenWithSpan>) -> Self {
        Self {
            tokens,
            pos: 0,
            recursion: RecursionCounter::new(DEFAULT_RECURSION_LIMIT),
            _dialect: dialect,
        }
    }

    pub fn with_recursion_limit(
        dialect: &'a dyn Dialect,
        tokens: Vec<TokenWithSpan>,
        limit: usize,
    ) -> Self {
        Self {
            tokens,
            pos: 0,
            recursion: RecursionCounter::new(limit),
            _dialect: dialect,
        }
    }

    /// Convenience entrypoint: parse a single expression from SQL input.
    pub fn parse_expression_sql(dialect: &'a dyn Dialect, sql: &str) -> Result<Expr> {
        let tokens = crate::tokenizer::Tokenizer::new(dialect, sql).tokenize()?;
        let mut parser = Parser::new(dialect, tokens);
        let expr = parser.parse_expr()?;
        if !matches!(parser.peek().token, Token::EOF) {
            let tok = parser.peek().clone();
            return Err(ParserError::UnexpectedToken {
                line: tok.span.start.line,
                column: tok.span.start.column,
                expected: "end of input".into(),
                found: format!("{:?}", tok.token),
            });
        }
        Ok(expr)
    }

    /// Parse a single expression from the current token stream.
    pub fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_subexpr(precedence::PREC_UNKNOWN)
    }

    pub(crate) fn peek(&self) -> &TokenWithSpan {
        self.tokens
            .get(self.pos)
            .unwrap_or_else(|| self.tokens.last().expect("token stream not empty"))
    }

    pub(crate) fn advance(&mut self) -> TokenWithSpan {
        let tok = self.peek().clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    pub(crate) fn prev(&self) -> Option<&TokenWithSpan> {
        if self.pos == 0 {
            None
        } else {
            self.tokens.get(self.pos - 1)
        }
    }

    pub(crate) fn expect_token<F>(
        &mut self,
        expected: &str,
        mut predicate: F,
    ) -> Result<TokenWithSpan>
    where
        F: FnMut(&Token) -> bool,
    {
        let tok = self.peek().clone();
        if predicate(&tok.token) {
            self.advance();
            Ok(tok)
        } else {
            Err(ParserError::ExpectedToken {
                line: tok.span.start.line,
                column: tok.span.start.column,
                expected: expected.to_string(),
                found: format!("{:?}", tok.token),
            })
        }
    }

    pub(crate) fn consume_keyword(&mut self, keyword: crate::tokenizer::keyword::Keyword) -> bool {
        if let Token::Word(Word { keyword: kw, .. }) = &self.peek().token {
            if *kw == keyword {
                self.advance();
                return true;
            }
        }
        false
    }

    pub(crate) fn parse_identifier(&mut self) -> Result<(String, Span)> {
        let tok = self.expect_token("identifier", |t| {
            matches!(
                t,
                Token::Word(Word {
                    keyword: crate::tokenizer::keyword::Keyword::NoKeyword,
                    ..
                })
            )
        })?;
        if let Token::Word(Word { value, .. }) = tok.token {
            Ok((value, tok.span))
        } else {
            unreachable!()
        }
    }

    pub(crate) fn expect_keyword(
        &mut self,
        expected: &str,
        kw: crate::tokenizer::keyword::Keyword,
    ) -> Result<Span> {
        let tok = self.peek().clone();
        if let Token::Word(Word { keyword, .. }) = tok.token {
            if keyword == kw {
                self.advance();
                return Ok(tok.span);
            }
        }
        Err(ParserError::ExpectedToken {
            line: tok.span.start.line,
            column: tok.span.start.column,
            expected: expected.to_string(),
            found: format!("{:?}", tok.token),
        })
    }

    pub(crate) fn next_precedence(&self) -> u8 {
        match &self.peek().token {
            Token::Plus | Token::Minus => Precedence::PlusMinus.value(),
            Token::Mul | Token::Div | Token::Mod => Precedence::MulDivMod.value(),
            Token::StringConcat => Precedence::StringConcat.value(),
            Token::Eq | Token::Neq | Token::Lt | Token::Gt | Token::LtEq | Token::GtEq => {
                Precedence::Comparison.value()
            }
            Token::Word(Word { keyword, .. }) => match keyword {
                crate::tokenizer::keyword::Keyword::AND => Precedence::And.value(),
                crate::tokenizer::keyword::Keyword::OR => Precedence::Or.value(),
                crate::tokenizer::keyword::Keyword::BETWEEN => Precedence::Between.value(),
                crate::tokenizer::keyword::Keyword::LIKE => Precedence::Like.value(),
                crate::tokenizer::keyword::Keyword::IN => Precedence::Comparison.value(),
                crate::tokenizer::keyword::Keyword::IS => Precedence::Is.value(),
                crate::tokenizer::keyword::Keyword::NOT => {
                    // NOT can introduce NOT BETWEEN/LIKE/IN
                    if let Some(next_kw) = self.peek_keyword_ahead(1) {
                        match next_kw {
                            crate::tokenizer::keyword::Keyword::BETWEEN => {
                                Precedence::Between.value()
                            }
                            crate::tokenizer::keyword::Keyword::LIKE => Precedence::Like.value(),
                            crate::tokenizer::keyword::Keyword::IN => {
                                Precedence::Comparison.value()
                            }
                            _ => precedence::PREC_UNKNOWN,
                        }
                    } else {
                        precedence::PREC_UNKNOWN
                    }
                }
                _ => precedence::PREC_UNKNOWN,
            },
            _ => precedence::PREC_UNKNOWN,
        }
    }

    fn peek_keyword_ahead(&self, offset: usize) -> Option<crate::tokenizer::keyword::Keyword> {
        self.tokens.get(self.pos + offset).and_then(|tw| {
            if let Token::Word(Word { keyword, .. }) = &tw.token {
                Some(*keyword)
            } else {
                None
            }
        })
    }
}
