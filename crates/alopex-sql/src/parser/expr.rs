use crate::ast::{BinaryOp, Expr, ExprKind, Literal, Spanned, UnaryOp};
use crate::error::{ParserError, Result};
use crate::tokenizer::keyword::Keyword;
use crate::tokenizer::token::{Token, Word};

use super::Parser;
use super::precedence::{PREC_UNKNOWN, Precedence};

impl<'a> Parser<'a> {
    pub(crate) fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr> {
        let _guard = self.recursion.try_decrease()?;

        let mut expr = self.parse_prefix()?;

        loop {
            let next_prec = self.next_precedence();
            if precedence >= next_prec || next_prec == PREC_UNKNOWN {
                break;
            }
            expr = self.parse_infix(expr, next_prec)?;
        }

        Ok(expr)
    }

    fn parse_prefix(&mut self) -> Result<Expr> {
        let token = self.peek().clone();
        match &token.token {
            Token::Number(n) => {
                self.advance();
                Ok(Expr::new(
                    ExprKind::Literal(Literal::Number(n.clone())),
                    token.span,
                ))
            }
            Token::SingleQuotedString(s) => {
                self.advance();
                Ok(Expr::new(
                    ExprKind::Literal(Literal::String(s.clone())),
                    token.span,
                ))
            }
            Token::Word(Word { keyword, value, .. }) => match keyword {
                Keyword::TRUE => {
                    self.advance();
                    Ok(Expr::new(
                        ExprKind::Literal(Literal::Boolean(true)),
                        token.span,
                    ))
                }
                Keyword::FALSE => {
                    self.advance();
                    Ok(Expr::new(
                        ExprKind::Literal(Literal::Boolean(false)),
                        token.span,
                    ))
                }
                Keyword::NULL => {
                    self.advance();
                    Ok(Expr::new(ExprKind::Literal(Literal::Null), token.span))
                }
                Keyword::NOT => {
                    self.advance();
                    let operand = self.parse_subexpr(Precedence::UnaryNot.value())?;
                    let span = token.span.union(&operand.span());
                    Ok(Expr::new(
                        ExprKind::UnaryOp {
                            op: UnaryOp::Not,
                            operand: Box::new(operand),
                        },
                        span,
                    ))
                }
                Keyword::NoKeyword => {
                    // Identifier or function call
                    self.advance();
                    let ident_span = token.span;
                    if let Token::LParen = self.peek().token {
                        self.advance(); // consume '('
                        let mut args = Vec::new();
                        if !matches!(self.peek().token, Token::RParen) {
                            loop {
                                args.push(self.parse_subexpr(PREC_UNKNOWN)?);
                                if matches!(self.peek().token, Token::Comma) {
                                    self.advance();
                                    continue;
                                }
                                break;
                            }
                        }
                        self.expect_token("')'", |t| matches!(t, Token::RParen))?;
                        let end_span = self.prev().map(|t| t.span).unwrap_or(ident_span);
                        Ok(Expr::new(
                            ExprKind::FunctionCall {
                                name: value.clone(),
                                args,
                            },
                            ident_span.union(&end_span),
                        ))
                    } else if matches!(self.peek().token, Token::Period) {
                        self.advance(); // consume '.'
                        let second = self.expect_token("identifier", |t| {
                            matches!(
                                t,
                                Token::Word(Word {
                                    keyword: Keyword::NoKeyword,
                                    ..
                                })
                            )
                        })?;
                        let end_span = second.span;
                        let col = match second.token {
                            Token::Word(Word { value, .. }) => value,
                            _ => unreachable!(),
                        };
                        Ok(Expr::new(
                            ExprKind::ColumnRef {
                                table: Some(value.clone()),
                                column: col,
                            },
                            ident_span.union(&end_span),
                        ))
                    } else {
                        Ok(Expr::new(
                            ExprKind::ColumnRef {
                                table: None,
                                column: value.clone(),
                            },
                            ident_span,
                        ))
                    }
                }
                _ => Err(ParserError::UnexpectedToken {
                    line: token.span.start.line,
                    column: token.span.start.column,
                    expected: "identifier or literal".into(),
                    found: format!("{:?}", token.token),
                }),
            },
            Token::LParen => {
                self.advance();
                let expr = self.parse_subexpr(PREC_UNKNOWN)?;
                self.expect_token("')'", |t| matches!(t, Token::RParen))?;
                let end_span = self.prev().map(|t| t.span).unwrap_or(expr.span());
                Ok(Expr::new(expr.kind.clone(), expr.span().union(&end_span)))
            }
            Token::Minus => {
                self.advance();
                let operand = self.parse_subexpr(Precedence::UnaryNot.value())?;
                let span = token.span.union(&operand.span());
                Ok(Expr::new(
                    ExprKind::UnaryOp {
                        op: UnaryOp::Minus,
                        operand: Box::new(operand),
                    },
                    span,
                ))
            }
            Token::LBracket => self.parse_vector_literal(),
            _ => Err(ParserError::UnexpectedToken {
                line: token.span.start.line,
                column: token.span.start.column,
                expected: "expression".into(),
                found: format!("{:?}", token.token),
            }),
        }
    }

    fn parse_vector_literal(&mut self) -> Result<Expr> {
        let start = self.peek().span;
        self.advance(); // consume '['

        let mut values = Vec::new();
        if matches!(self.peek().token, Token::RBracket) {
            return Err(ParserError::InvalidVector {
                line: start.start.line,
                column: start.start.column,
            });
        }
        loop {
            let sign = if matches!(self.peek().token, Token::Minus) {
                self.advance();
                -1.0
            } else {
                1.0
            };

            let tok = self.expect_token("number", |t| matches!(t, Token::Number(_)))?;
            let num = match tok.token {
                Token::Number(n) => n.parse::<f64>().map_err(|_| ParserError::InvalidVector {
                    line: tok.span.start.line,
                    column: tok.span.start.column,
                })?,
                _ => unreachable!(),
            } * sign;
            values.push(num);
            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }
        let end = self
            .expect_token("']'", |t| matches!(t, Token::RBracket))?
            .span;

        Ok(Expr::new(
            ExprKind::VectorLiteral(values),
            start.union(&end),
        ))
    }

    fn parse_infix(&mut self, left: Expr, precedence: u8) -> Result<Expr> {
        let op_token = self.peek().clone();
        match &op_token.token {
            Token::Plus
            | Token::Minus
            | Token::Mul
            | Token::Div
            | Token::Mod
            | Token::StringConcat => {
                self.advance();
                let op = match op_token.token {
                    Token::Plus => BinaryOp::Add,
                    Token::Minus => BinaryOp::Sub,
                    Token::Mul => BinaryOp::Mul,
                    Token::Div => BinaryOp::Div,
                    Token::Mod => BinaryOp::Mod,
                    Token::StringConcat => BinaryOp::StringConcat,
                    _ => unreachable!(),
                };
                let right = self.parse_subexpr(precedence)?;
                let span = left.span().union(&right.span());
                Ok(Expr::new(
                    ExprKind::BinaryOp {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                    span,
                ))
            }
            Token::Eq | Token::Neq | Token::Lt | Token::Gt | Token::LtEq | Token::GtEq => {
                self.advance();
                let op = match op_token.token {
                    Token::Eq => BinaryOp::Eq,
                    Token::Neq => BinaryOp::Neq,
                    Token::Lt => BinaryOp::Lt,
                    Token::Gt => BinaryOp::Gt,
                    Token::LtEq => BinaryOp::LtEq,
                    Token::GtEq => BinaryOp::GtEq,
                    _ => unreachable!(),
                };
                let right = self.parse_subexpr(precedence)?;
                let span = left.span().union(&right.span());
                Ok(Expr::new(
                    ExprKind::BinaryOp {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                    span,
                ))
            }
            Token::Word(Word { keyword, .. }) => match keyword {
                Keyword::AND | Keyword::OR => {
                    self.advance();
                    let op = if *keyword == Keyword::AND {
                        BinaryOp::And
                    } else {
                        BinaryOp::Or
                    };
                    let right = self.parse_subexpr(precedence)?;
                    let span = left.span().union(&right.span());
                    Ok(Expr::new(
                        ExprKind::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                        span,
                    ))
                }
                Keyword::BETWEEN => {
                    self.advance();
                    let low = self.parse_subexpr(Precedence::Between.value())?;
                    self.expect_keyword("AND", Keyword::AND)?;
                    let high = self.parse_subexpr(Precedence::Between.value())?;
                    let span = left.span().union(&high.span());
                    Ok(Expr::new(
                        ExprKind::Between {
                            expr: Box::new(left),
                            low: Box::new(low),
                            high: Box::new(high),
                            negated: false,
                        },
                        span,
                    ))
                }
                Keyword::LIKE => {
                    self.advance();
                    let pattern = self.parse_subexpr(Precedence::Like.value())?;
                    let escape = if self.consume_keyword(Keyword::ESCAPE) {
                        Some(Box::new(self.parse_subexpr(Precedence::Like.value())?))
                    } else {
                        None
                    };
                    let span = left.span().union(&pattern.span());
                    let span = if let Some(ref esc) = escape {
                        span.union(&esc.span())
                    } else {
                        span
                    };
                    Ok(Expr::new(
                        ExprKind::Like {
                            expr: Box::new(left),
                            pattern: Box::new(pattern),
                            escape,
                            negated: false,
                        },
                        span,
                    ))
                }
                Keyword::IN => {
                    self.advance();
                    self.expect_token("'('", |t| matches!(t, Token::LParen))?;
                    let mut list = Vec::new();
                    if !matches!(self.peek().token, Token::RParen) {
                        loop {
                            list.push(self.parse_subexpr(PREC_UNKNOWN)?);
                            if matches!(self.peek().token, Token::Comma) {
                                self.advance();
                                continue;
                            }
                            break;
                        }
                    }
                    let end_span = self
                        .expect_token("')'", |t| matches!(t, Token::RParen))?
                        .span;
                    let span = left.span().union(&end_span);
                    Ok(Expr::new(
                        ExprKind::InList {
                            expr: Box::new(left),
                            list,
                            negated: false,
                        },
                        span,
                    ))
                }
                Keyword::IS => {
                    self.advance();
                    let negated = self.consume_keyword(Keyword::NOT);
                    self.expect_keyword("NULL", Keyword::NULL)?;
                    let span = left.span().union(&self.prev().unwrap().span);
                    Ok(Expr::new(
                        ExprKind::IsNull {
                            expr: Box::new(left),
                            negated,
                        },
                        span,
                    ))
                }
                Keyword::NOT => {
                    // NOT BETWEEN / NOT LIKE / NOT IN
                    if let Some(next_kw) = self.peek_keyword_ahead(1) {
                        match next_kw {
                            Keyword::BETWEEN => {
                                self.advance(); // NOT
                                self.advance(); // BETWEEN
                                let low = self.parse_subexpr(Precedence::Between.value())?;
                                self.expect_keyword("AND", Keyword::AND)?;
                                let high = self.parse_subexpr(Precedence::Between.value())?;
                                let span = left.span().union(&high.span());
                                return Ok(Expr::new(
                                    ExprKind::Between {
                                        expr: Box::new(left),
                                        low: Box::new(low),
                                        high: Box::new(high),
                                        negated: true,
                                    },
                                    span,
                                ));
                            }
                            Keyword::LIKE => {
                                self.advance(); // NOT
                                self.advance(); // LIKE
                                let pattern = self.parse_subexpr(Precedence::Like.value())?;
                                let escape = if self.consume_keyword(Keyword::ESCAPE) {
                                    Some(Box::new(self.parse_subexpr(Precedence::Like.value())?))
                                } else {
                                    None
                                };
                                let span = left.span().union(&pattern.span());
                                let span = if let Some(ref esc) = escape {
                                    span.union(&esc.span())
                                } else {
                                    span
                                };
                                return Ok(Expr::new(
                                    ExprKind::Like {
                                        expr: Box::new(left),
                                        pattern: Box::new(pattern),
                                        escape,
                                        negated: true,
                                    },
                                    span,
                                ));
                            }
                            Keyword::IN => {
                                self.advance(); // NOT
                                self.advance(); // IN
                                self.expect_token("'('", |t| matches!(t, Token::LParen))?;
                                let mut list = Vec::new();
                                if !matches!(self.peek().token, Token::RParen) {
                                    loop {
                                        list.push(self.parse_subexpr(PREC_UNKNOWN)?);
                                        if matches!(self.peek().token, Token::Comma) {
                                            self.advance();
                                            continue;
                                        }
                                        break;
                                    }
                                }
                                let end_span = self
                                    .expect_token("')'", |t| matches!(t, Token::RParen))?
                                    .span;
                                let span = left.span().union(&end_span);
                                return Ok(Expr::new(
                                    ExprKind::InList {
                                        expr: Box::new(left),
                                        list,
                                        negated: true,
                                    },
                                    span,
                                ));
                            }
                            _ => {}
                        }
                    }
                    Err(ParserError::UnexpectedToken {
                        line: op_token.span.start.line,
                        column: op_token.span.start.column,
                        expected: "BETWEEN, LIKE, IN".into(),
                        found: "NOT".into(),
                    })
                }
                _ => Err(ParserError::UnexpectedToken {
                    line: op_token.span.start.line,
                    column: op_token.span.start.column,
                    expected: "operator".into(),
                    found: format!("{:?}", op_token.token),
                }),
            },
            _ => Err(ParserError::UnexpectedToken {
                line: op_token.span.start.line,
                column: op_token.span.start.column,
                expected: "operator".into(),
                found: format!("{:?}", op_token.token),
            }),
        }
    }

    fn expect_keyword(&mut self, expected: &str, kw: Keyword) -> Result<()> {
        let tok = self.peek().clone();
        if let Token::Word(Word { keyword, .. }) = tok.token {
            if keyword == kw {
                self.advance();
                return Ok(());
            }
        }
        Err(ParserError::ExpectedToken {
            line: tok.span.start.line,
            column: tok.span.start.column,
            expected: expected.to_string(),
            found: format!("{:?}", tok.token),
        })
    }
}
