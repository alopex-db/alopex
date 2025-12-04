use crate::ast::ddl::{
    ColumnConstraint, ColumnDef, CreateIndex, CreateTable, DataType, DropIndex, DropTable,
    IndexMethod, IndexOption, TableConstraint, VectorMetric,
};
use crate::ast::span::{Span, Spanned};
use crate::error::{ParserError, Result};
use crate::tokenizer::keyword::Keyword;
use crate::tokenizer::token::{Token, Word};

use super::Parser;

impl<'a> Parser<'a> {
    pub fn parse_create_table(&mut self) -> Result<CreateTable> {
        let start_span = self.expect_keyword("CREATE", Keyword::CREATE)?;
        self.expect_keyword("TABLE", Keyword::TABLE)?;
        let if_not_exists = if self.consume_keyword(Keyword::IF) {
            self.expect_keyword("NOT", Keyword::NOT)?;
            self.expect_keyword("EXISTS", Keyword::EXISTS)?;
            true
        } else {
            false
        };

        let (name, _name_span) = self.parse_identifier()?;
        self.expect_token("'('", |t| matches!(t, Token::LParen))?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            if self.consume_keyword(Keyword::PRIMARY) {
                self.expect_keyword("KEY", Keyword::KEY)?;
                self.expect_token("'('", |t| matches!(t, Token::LParen))?;
                let mut cols = Vec::new();
                loop {
                    let (col, _) = self.parse_identifier()?;
                    cols.push(col);
                    if matches!(self.peek().token, Token::Comma) {
                        self.advance();
                        continue;
                    }
                    break;
                }
                let pk_end = self
                    .expect_token("')'", |t| matches!(t, Token::RParen))?
                    .span;
                constraints.push(TableConstraint::PrimaryKey {
                    columns: cols,
                    span: start_span.union(&pk_end),
                });
            } else {
                let (col_name, col_span) = self.parse_identifier()?;
                let (data_type, dt_span) = self.parse_data_type()?;
                let mut col_constraints = Vec::new();
                while let Token::Word(Word { keyword, .. }) = &self.peek().token {
                    let constraint = match keyword {
                        Keyword::NOT => {
                            let start = self.peek().span;
                            self.advance();
                            self.expect_keyword("NULL", Keyword::NULL)?;
                            ColumnConstraint::WithSpan {
                                kind: Box::new(ColumnConstraint::NotNull),
                                span: start.union(&self.prev().unwrap().span),
                            }
                        }
                        Keyword::NULL => {
                            let s = self.advance().span;
                            ColumnConstraint::WithSpan {
                                kind: Box::new(ColumnConstraint::Null),
                                span: s,
                            }
                        }
                        Keyword::PRIMARY => {
                            let start = self.advance().span;
                            self.expect_keyword("KEY", Keyword::KEY)?;
                            ColumnConstraint::WithSpan {
                                kind: Box::new(ColumnConstraint::PrimaryKey),
                                span: start.union(&self.prev().unwrap().span),
                            }
                        }
                        Keyword::UNIQUE => {
                            let s = self.advance().span;
                            ColumnConstraint::WithSpan {
                                kind: Box::new(ColumnConstraint::Unique),
                                span: s,
                            }
                        }
                        Keyword::DEFAULT => {
                            let start = self.advance().span;
                            let expr = self.parse_expr()?;
                            let span = start.union(&expr.span());
                            ColumnConstraint::WithSpan {
                                kind: Box::new(ColumnConstraint::Default(expr)),
                                span,
                            }
                        }
                        _ => break,
                    };
                    col_constraints.push(constraint);
                }

                let end_span = if let Some(last) = col_constraints.last() {
                    last.span()
                } else {
                    dt_span
                };

                columns.push(ColumnDef {
                    name: col_name,
                    data_type,
                    constraints: col_constraints,
                    span: col_span.union(&end_span),
                });
            }

            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        let end_span = self
            .expect_token("')'", |t| matches!(t, Token::RParen))?
            .span;
        let span = start_span.union(&end_span);
        Ok(CreateTable {
            if_not_exists,
            name,
            columns,
            constraints,
            span,
        })
    }

    pub fn parse_drop_table(&mut self) -> Result<DropTable> {
        let start_span = self.expect_keyword("DROP", Keyword::DROP)?;
        self.expect_keyword("TABLE", Keyword::TABLE)?;
        let if_exists = if self.consume_keyword(Keyword::IF) {
            self.expect_keyword("EXISTS", Keyword::EXISTS)?;
            true
        } else {
            false
        };
        let (name, name_span) = self.parse_identifier()?;
        let span = start_span.union(&name_span);
        Ok(DropTable {
            if_exists,
            name,
            span,
        })
    }

    pub fn parse_create_index(&mut self) -> Result<CreateIndex> {
        let start_span = self.expect_keyword("CREATE", Keyword::CREATE)?;
        self.expect_keyword("INDEX", Keyword::INDEX)?;
        let if_not_exists = if self.consume_keyword(Keyword::IF) {
            self.expect_keyword("NOT", Keyword::NOT)?;
            self.expect_keyword("EXISTS", Keyword::EXISTS)?;
            true
        } else {
            false
        };

        let (name, _name_span) = self.parse_identifier()?;
        self.expect_keyword("ON", Keyword::ON)?;
        let (table, _table_span) = self.parse_identifier()?;
        self.expect_token("'('", |t| matches!(t, Token::LParen))?;
        let (column, _col_span) = self.parse_identifier()?;
        let end_paren = self
            .expect_token("')'", |t| matches!(t, Token::RParen))?
            .span;

        let mut method = None;
        if self.consume_keyword(Keyword::USING) {
            let meth = self
                .expect_keyword("index method", Keyword::HNSW)
                .or_else(|_| self.expect_keyword("index method", Keyword::BTREE))?;
            method = Some(match self.prev().unwrap().token {
                Token::Word(Word {
                    keyword: Keyword::HNSW,
                    ..
                }) => IndexMethod::Hnsw,
                _ => IndexMethod::BTree,
            });
            // Use meth span to extend end
            let _ = meth;
        }

        let mut options = Vec::new();
        if self.consume_keyword(Keyword::WITH) {
            self.expect_token("'('", |t| matches!(t, Token::LParen))?;
            loop {
                let (key, key_span) = self.parse_identifier()?;
                self.expect_token("'='", |t| matches!(t, Token::Eq))?;
                let val_tok = self.expect_token("option value", |t| {
                    matches!(
                        t,
                        Token::Number(_) | Token::Word(_) | Token::SingleQuotedString(_)
                    )
                })?;
                let value = match val_tok.token {
                    Token::Number(n) => n,
                    Token::Word(Word { value, .. }) => value,
                    Token::SingleQuotedString(s) => s,
                    _ => unreachable!(),
                };
                let span = key_span.union(&val_tok.span);
                options.push(IndexOption { key, value, span });

                if matches!(self.peek().token, Token::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
            let _ = self.expect_token("')'", |t| matches!(t, Token::RParen))?;
        }

        let span = start_span.union(&end_paren);
        Ok(CreateIndex {
            if_not_exists,
            name,
            table,
            column,
            method,
            options,
            span,
        })
    }

    pub fn parse_drop_index(&mut self) -> Result<DropIndex> {
        let start_span = self.expect_keyword("DROP", Keyword::DROP)?;
        self.expect_keyword("INDEX", Keyword::INDEX)?;
        let if_exists = if self.consume_keyword(Keyword::IF) {
            self.expect_keyword("EXISTS", Keyword::EXISTS)?;
            true
        } else {
            false
        };
        let (name, name_span) = self.parse_identifier()?;
        let span = start_span.union(&name_span);
        Ok(DropIndex {
            if_exists,
            name,
            span,
        })
    }

    fn parse_data_type(&mut self) -> Result<(DataType, Span)> {
        let tok = self.peek().clone();
        let (dtype, end_span) = match &tok.token {
            Token::Word(Word { keyword, .. }) => match keyword {
                Keyword::INTEGER => {
                    self.advance();
                    (DataType::Integer, tok.span)
                }
                Keyword::INT => {
                    self.advance();
                    (DataType::Int, tok.span)
                }
                Keyword::BIGINT => {
                    self.advance();
                    (DataType::BigInt, tok.span)
                }
                Keyword::FLOAT => {
                    self.advance();
                    (DataType::Float, tok.span)
                }
                Keyword::DOUBLE => {
                    self.advance();
                    (DataType::Double, tok.span)
                }
                Keyword::TEXT => {
                    self.advance();
                    (DataType::Text, tok.span)
                }
                Keyword::BLOB => {
                    self.advance();
                    (DataType::Blob, tok.span)
                }
                Keyword::BOOLEAN => {
                    self.advance();
                    (DataType::Boolean, tok.span)
                }
                Keyword::BOOL => {
                    self.advance();
                    (DataType::Bool, tok.span)
                }
                Keyword::TIMESTAMP => {
                    self.advance();
                    (DataType::Timestamp, tok.span)
                }
                Keyword::VECTOR => {
                    self.advance();
                    self.expect_token("'('", |t| matches!(t, Token::LParen))?;
                    let dim_tok =
                        self.expect_token("dimension", |t| matches!(t, Token::Number(_)))?;
                    let dimension: u32 = match dim_tok.token {
                        Token::Number(ref n) => {
                            n.parse().map_err(|_| ParserError::InvalidVector {
                                line: dim_tok.span.start.line,
                                column: dim_tok.span.start.column,
                            })?
                        }
                        _ => unreachable!(),
                    };
                    let mut metric = None;
                    let mut last_span = dim_tok.span;
                    if matches!(self.peek().token, Token::Comma) {
                        self.advance();
                        let m_tok = self.expect_token("metric", |t| {
                            matches!(
                                t,
                                Token::Word(Word {
                                    keyword: Keyword::COSINE | Keyword::L2 | Keyword::INNER,
                                    ..
                                })
                            )
                        })?;
                        metric = Some(match m_tok.token {
                            Token::Word(Word {
                                keyword: Keyword::COSINE,
                                ..
                            }) => VectorMetric::Cosine,
                            Token::Word(Word {
                                keyword: Keyword::L2,
                                ..
                            }) => VectorMetric::L2,
                            _ => VectorMetric::Inner,
                        });
                        last_span = m_tok.span;
                    }
                    let end = self
                        .expect_token("')'", |t| matches!(t, Token::RParen))?
                        .span;
                    let span = tok.span.union(&end);
                    (
                        DataType::Vector { dimension, metric },
                        span.union(&last_span),
                    )
                }
                _ => {
                    return Err(ParserError::UnexpectedToken {
                        line: tok.span.start.line,
                        column: tok.span.start.column,
                        expected: "data type".into(),
                        found: format!("{:?}", tok.token),
                    });
                }
            },
            _ => {
                return Err(ParserError::UnexpectedToken {
                    line: tok.span.start.line,
                    column: tok.span.start.column,
                    expected: "data type".into(),
                    found: format!("{:?}", tok.token),
                });
            }
        };

        Ok((dtype, end_span))
    }
}
