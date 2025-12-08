pub mod ddl;
pub mod dml;
pub mod expr;
pub mod precedence;
pub mod recursion;

use crate::Span;
use crate::ast::span::Spanned;
use crate::ast::{Expr, Statement, StatementKind};
use crate::dialect::Dialect;
use crate::error::{ParserError, Result};
use crate::tokenizer::token::{Token, TokenWithSpan, Word};
use precedence::Precedence;
use recursion::{DEFAULT_RECURSION_LIMIT, RecursionCounter};

/// トークン列をSQL ASTへ変換するパーサ。
#[derive(Debug, Clone)]
pub struct Parser<'a> {
    tokens: Vec<TokenWithSpan>,
    pos: usize,
    pub(crate) recursion: RecursionCounter,
    dialect: &'a dyn Dialect,
}

impl<'a> Parser<'a> {
    pub fn new(dialect: &'a dyn Dialect, tokens: Vec<TokenWithSpan>) -> Self {
        Self {
            tokens,
            pos: 0,
            recursion: RecursionCounter::new(DEFAULT_RECURSION_LIMIT),
            dialect,
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
            dialect,
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

    /// SQL文字列全体をパースし、ステートメント列を返す。
    pub fn parse_sql(dialect: &'a dyn Dialect, sql: &str) -> Result<Vec<Statement>> {
        let tokens = crate::tokenizer::Tokenizer::new(dialect, sql).tokenize()?;
        let mut parser = Parser::new(dialect, tokens);
        parser.parse_statements()
    }

    fn parse_statements(&mut self) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();
        loop {
            match &self.peek().token {
                Token::EOF => break,
                Token::SemiColon => {
                    self.advance();
                    continue;
                }
                _ => {
                    let stmt = self.parse_statement()?;
                    statements.push(stmt);
                    if matches!(self.peek().token, Token::SemiColon) {
                        self.advance();
                    }
                }
            }
        }
        Ok(statements)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        if let Some(result) = self.dialect.parse_statement(self) {
            return result;
        }

        let tok = self.peek().clone();
        match &tok.token {
            Token::Word(Word { keyword, .. }) => match keyword {
                crate::tokenizer::keyword::Keyword::SELECT => {
                    let select = self.parse_select()?;
                    Ok(Statement {
                        span: select.span(),
                        kind: StatementKind::Select(select),
                    })
                }
                crate::tokenizer::keyword::Keyword::INSERT => {
                    let insert = self.parse_insert()?;
                    Ok(Statement {
                        span: insert.span(),
                        kind: StatementKind::Insert(insert),
                    })
                }
                crate::tokenizer::keyword::Keyword::UPDATE => {
                    let update = self.parse_update()?;
                    Ok(Statement {
                        span: update.span(),
                        kind: StatementKind::Update(update),
                    })
                }
                crate::tokenizer::keyword::Keyword::DELETE => {
                    let delete = self.parse_delete()?;
                    Ok(Statement {
                        span: delete.span(),
                        kind: StatementKind::Delete(delete),
                    })
                }
                crate::tokenizer::keyword::Keyword::CREATE => match self.peek_keyword_ahead(1) {
                    Some(crate::tokenizer::keyword::Keyword::TABLE) => {
                        let create_table = self.parse_create_table()?;
                        Ok(Statement {
                            span: create_table.span(),
                            kind: StatementKind::CreateTable(create_table),
                        })
                    }
                    Some(crate::tokenizer::keyword::Keyword::INDEX) => {
                        let create_index = self.parse_create_index()?;
                        Ok(Statement {
                            span: create_index.span(),
                            kind: StatementKind::CreateIndex(create_index),
                        })
                    }
                    _ => Err(ParserError::UnexpectedToken {
                        line: tok.span.start.line,
                        column: tok.span.start.column,
                        expected: "CREATE TABLE or CREATE INDEX".into(),
                        found: format!("{:?}", tok.token),
                    }),
                },
                crate::tokenizer::keyword::Keyword::DROP => match self.peek_keyword_ahead(1) {
                    Some(crate::tokenizer::keyword::Keyword::INDEX) => {
                        let drop_index = self.parse_drop_index()?;
                        Ok(Statement {
                            span: drop_index.span(),
                            kind: StatementKind::DropIndex(drop_index),
                        })
                    }
                    Some(crate::tokenizer::keyword::Keyword::TABLE) => {
                        let drop_table = self.parse_drop_table()?;
                        Ok(Statement {
                            span: drop_table.span(),
                            kind: StatementKind::DropTable(drop_table),
                        })
                    }
                    _ => Err(ParserError::UnexpectedToken {
                        line: tok.span.start.line,
                        column: tok.span.start.column,
                        expected: "DROP TABLE or DROP INDEX".into(),
                        found: format!("{:?}", tok.token),
                    }),
                },
                _ => Err(ParserError::UnexpectedToken {
                    line: tok.span.start.line,
                    column: tok.span.start.column,
                    expected: "statement".into(),
                    found: format!("{:?}", tok.token),
                }),
            },
            _ => Err(ParserError::UnexpectedToken {
                line: tok.span.start.line,
                column: tok.span.start.column,
                expected: "statement".into(),
                found: format!("{:?}", tok.token),
            }),
        }
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
        if let Token::Word(Word { keyword: kw, .. }) = &self.peek().token
            && *kw == keyword
        {
            self.advance();
            return true;
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
        if let Token::Word(Word { keyword, .. }) = tok.token
            && keyword == kw
        {
            self.advance();
            return Ok(tok.span);
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
            Token::Plus | Token::Minus => self.dialect.prec_value(Precedence::PlusMinus),
            Token::Mul | Token::Div | Token::Mod => self.dialect.prec_value(Precedence::MulDivMod),
            Token::StringConcat => self.dialect.prec_value(Precedence::StringConcat),
            Token::Eq | Token::Neq | Token::Lt | Token::Gt | Token::LtEq | Token::GtEq => {
                self.dialect.prec_value(Precedence::Comparison)
            }
            Token::Word(Word { keyword, .. }) => match keyword {
                crate::tokenizer::keyword::Keyword::AND => self.dialect.prec_value(Precedence::And),
                crate::tokenizer::keyword::Keyword::OR => self.dialect.prec_value(Precedence::Or),
                crate::tokenizer::keyword::Keyword::BETWEEN => {
                    self.dialect.prec_value(Precedence::Between)
                }
                crate::tokenizer::keyword::Keyword::LIKE => {
                    self.dialect.prec_value(Precedence::Like)
                }
                crate::tokenizer::keyword::Keyword::IN => {
                    self.dialect.prec_value(Precedence::Comparison)
                }
                crate::tokenizer::keyword::Keyword::IS => self.dialect.prec_value(Precedence::Is),
                crate::tokenizer::keyword::Keyword::NOT => {
                    // NOT can introduce NOT BETWEEN/LIKE/IN
                    if let Some(next_kw) = self.peek_keyword_ahead(1) {
                        match next_kw {
                            crate::tokenizer::keyword::Keyword::BETWEEN => {
                                self.dialect.prec_value(Precedence::Between)
                            }
                            crate::tokenizer::keyword::Keyword::LIKE => {
                                self.dialect.prec_value(Precedence::Like)
                            }
                            crate::tokenizer::keyword::Keyword::IN => {
                                self.dialect.prec_value(Precedence::Comparison)
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
