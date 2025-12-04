use crate::ast::span::Spanned;
use crate::ast::{Assignment, Delete, Insert, OrderByExpr, Select, SelectItem, TableRef, Update};
use crate::error::Result;
use crate::tokenizer::keyword::Keyword;
use crate::tokenizer::token::{Token, Word};

use super::Parser;

impl<'a> Parser<'a> {
    pub fn parse_select(&mut self) -> Result<Select> {
        let start_span = self.expect_keyword("SELECT", Keyword::SELECT)?;
        let distinct = self.consume_keyword(Keyword::DISTINCT);

        let projection = self.parse_projection_list()?;

        self.expect_keyword("FROM", Keyword::FROM)?;
        let from = self.parse_table_ref()?;
        let mut end_span = from.span;

        let selection = if self.consume_keyword(Keyword::WHERE) {
            let expr = self.parse_expr()?;
            end_span = end_span.union(&expr.span());
            Some(expr)
        } else {
            None
        };

        let order_by = if self.consume_keyword(Keyword::ORDER) {
            self.expect_keyword("BY", Keyword::BY)?;
            let items = self.parse_order_by()?;
            if let Some(last) = items.last() {
                end_span = end_span.union(&last.span);
            }
            items
        } else {
            Vec::new()
        };

        let mut limit = None;
        let mut offset = None;
        if self.consume_keyword(Keyword::LIMIT) {
            let lim = self.parse_expr()?;
            end_span = end_span.union(&lim.span());
            limit = Some(lim);

            if self.consume_keyword(Keyword::OFFSET) {
                let off = self.parse_expr()?;
                end_span = end_span.union(&off.span());
                offset = Some(off);
            }
        }

        let span = start_span.union(&end_span);
        Ok(Select {
            distinct,
            projection,
            from,
            selection,
            order_by,
            limit,
            offset,
            span,
        })
    }

    fn parse_projection_list(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_select_item()?);
            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        let tok = self.peek().clone();
        if matches!(tok.token, Token::Mul) {
            self.advance();
            return Ok(SelectItem::Wildcard { span: tok.span });
        }

        let expr = self.parse_expr()?;
        let mut span = expr.span();
        let mut alias = None;

        if self.consume_keyword(Keyword::AS) {
            let (name, alias_span) = self.parse_identifier()?;
            span = span.union(&alias_span);
            alias = Some(name);
        } else if let Token::Word(Word {
            keyword: Keyword::NoKeyword,
            ..
        }) = &self.peek().token
        {
            let alias_tok = self.advance();
            if let Token::Word(Word { value, .. }) = alias_tok.token {
                span = span.union(&alias_tok.span);
                alias = Some(value);
            }
        }

        Ok(SelectItem::Expr { expr, alias, span })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let (name, name_span) = self.parse_identifier()?;
        let mut alias = None;
        let mut span = name_span;

        if self.consume_keyword(Keyword::AS) {
            let (a, alias_span) = self.parse_identifier()?;
            alias = Some(a);
            span = span.union(&alias_span);
        } else if let Token::Word(Word {
            keyword: Keyword::NoKeyword,
            ..
        }) = &self.peek().token
        {
            let alias_tok = self.advance();
            if let Token::Word(Word { value, .. }) = alias_tok.token {
                span = span.union(&alias_tok.span);
                alias = Some(value);
            }
        }

        Ok(TableRef { name, alias, span })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let mut span = expr.span();
            let mut asc = None;
            let mut nulls_first = None;

            if let Token::Word(Word { keyword, .. }) = &self.peek().token {
                match keyword {
                    Keyword::ASC => {
                        let s = self.advance().span;
                        span = span.union(&s);
                        asc = Some(true);
                    }
                    Keyword::DESC => {
                        let s = self.advance().span;
                        span = span.union(&s);
                        asc = Some(false);
                    }
                    _ => {}
                }
            }

            if let Token::Word(Word {
                keyword: Keyword::NULLS,
                ..
            }) = &self.peek().token
            {
                let nulls_tok = self.advance();
                let dir_tok = self.expect_token("FIRST or LAST", |t| {
                    matches!(
                        t,
                        Token::Word(Word {
                            keyword: Keyword::FIRST | Keyword::LAST,
                            ..
                        })
                    )
                })?;
                nulls_first = Some(matches!(
                    dir_tok.token,
                    Token::Word(Word {
                        keyword: Keyword::FIRST,
                        ..
                    })
                ));
                span = span.union(&nulls_tok.span).union(&dir_tok.span);
            }

            items.push(OrderByExpr {
                expr,
                asc,
                nulls_first,
                span,
            });

            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        Ok(items)
    }

    pub fn parse_insert(&mut self) -> Result<Insert> {
        let start_span = self.expect_keyword("INSERT", Keyword::INSERT)?;
        self.expect_keyword("INTO", Keyword::INTO)?;
        let (table, table_span) = self.parse_identifier()?;
        let mut end_span = table_span;
        let mut columns = None;

        if matches!(self.peek().token, Token::LParen) {
            self.advance();
            let mut cols = Vec::new();
            loop {
                let (col, col_span) = self.parse_identifier()?;
                end_span = end_span.union(&col_span);
                cols.push(col);
                if matches!(self.peek().token, Token::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
            let close = self
                .expect_token("')'", |t| matches!(t, Token::RParen))?
                .span;
            end_span = end_span.union(&close);
            columns = Some(cols);
        }

        self.expect_keyword("VALUES", Keyword::VALUES)?;
        let mut values = Vec::new();
        loop {
            self.expect_token("'('", |t| matches!(t, Token::LParen))?;
            let mut row = Vec::new();
            row.push(self.parse_expr()?);
            while matches!(self.peek().token, Token::Comma) {
                self.advance();
                row.push(self.parse_expr()?);
            }
            let row_end = self
                .expect_token("')'", |t| matches!(t, Token::RParen))?
                .span;
            end_span = end_span.union(&row_end);
            values.push(row);

            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        let span = start_span.union(&end_span);
        Ok(Insert {
            table,
            columns,
            values,
            span,
        })
    }

    pub fn parse_update(&mut self) -> Result<Update> {
        let start_span = self.expect_keyword("UPDATE", Keyword::UPDATE)?;
        let (table, table_span) = self.parse_identifier()?;
        self.expect_keyword("SET", Keyword::SET)?;

        let mut assignments = Vec::new();
        loop {
            let (column, col_span) = self.parse_identifier()?;
            self.expect_token("'='", |t| matches!(t, Token::Eq))?;
            let value = self.parse_expr()?;
            let span = col_span.union(&value.span());
            assignments.push(Assignment {
                column,
                value,
                span,
            });

            if matches!(self.peek().token, Token::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        let mut end_span = assignments.last().map(|a| a.span).unwrap_or(table_span);

        let selection = if self.consume_keyword(Keyword::WHERE) {
            let expr = self.parse_expr()?;
            end_span = end_span.union(&expr.span());
            Some(expr)
        } else {
            None
        };

        let span = start_span.union(&end_span);
        Ok(Update {
            table,
            assignments,
            selection,
            span,
        })
    }

    pub fn parse_delete(&mut self) -> Result<Delete> {
        let start_span = self.expect_keyword("DELETE", Keyword::DELETE)?;
        self.expect_keyword("FROM", Keyword::FROM)?;
        let (table, table_span) = self.parse_identifier()?;
        let mut end_span = table_span;

        let selection = if self.consume_keyword(Keyword::WHERE) {
            let expr = self.parse_expr()?;
            end_span = end_span.union(&expr.span());
            Some(expr)
        } else {
            None
        };

        let span = start_span.union(&end_span);
        Ok(Delete {
            table,
            selection,
            span,
        })
    }
}
