pub mod keyword;
pub mod token;

use crate::ast::span::{Location, Span};
use crate::dialect::Dialect;
use crate::error::{ParserError, Result};
use keyword::Keyword;
use token::{Token, TokenWithSpan, Word};

pub struct Tokenizer<'a> {
    _dialect: &'a dyn Dialect,
    input: &'a str,
    pos: usize,
    line: u64,
    column: u64,
}

impl<'a> Tokenizer<'a> {
    pub fn new(dialect: &'a dyn Dialect, input: &'a str) -> Self {
        Self {
            _dialect: dialect,
            input,
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<TokenWithSpan>> {
        let mut tokens = Vec::new();

        loop {
            self.skip_ignored();

            if self.peek_char().is_none() {
                let loc = Location::new(self.line, self.column);
                tokens.push(TokenWithSpan {
                    token: Token::EOF,
                    span: Span::new(loc, loc),
                });
                break;
            }

            let token = match self.peek_char().unwrap() {
                c if Self::is_identifier_start(c) => self.lex_word()?,
                c if c.is_ascii_digit() => self.lex_number()?,
                '\'' => self.lex_string()?,
                ',' => self.single_char_token(Token::Comma),
                '=' => self.single_char_token(Token::Eq),
                '<' => self.lex_lt_related()?,
                '>' => self.lex_gt_related()?,
                '!' => self.lex_bang_related()?,
                '+' => self.single_char_token(Token::Plus),
                '-' => self.single_char_token(Token::Minus),
                '*' => self.single_char_token(Token::Mul),
                '/' => self.single_char_token(Token::Div),
                '%' => self.single_char_token(Token::Mod),
                '(' => self.single_char_token(Token::LParen),
                ')' => self.single_char_token(Token::RParen),
                '[' => self.single_char_token(Token::LBracket),
                ']' => self.single_char_token(Token::RBracket),
                '.' => self.single_char_token(Token::Period),
                ':' => self.single_char_token(Token::Colon),
                ';' => self.single_char_token(Token::SemiColon),
                '|' => self.lex_pipe_related()?,
                other => {
                    return Err(ParserError::UnexpectedToken {
                        line: self.line,
                        column: self.column,
                        expected: "valid token".into(),
                        found: other.to_string(),
                    });
                }
            };

            tokens.push(token);
        }

        Ok(tokens)
    }

    fn skip_ignored(&mut self) {
        loop {
            // Whitespace
            while let Some(ch) = self.peek_char() {
                if ch.is_whitespace() {
                    self.next_char();
                } else {
                    break;
                }
            }

            // Single-line comment: --
            let mut skip_comment = false;
            if self.peek_char() == Some('-') && self.peek_next_char() == Some('-') {
                // consume the two dashes
                self.next_char();
                self.next_char();
                skip_comment = true;
                while let Some((ch, _)) = self.next_char() {
                    if ch == '\n' {
                        break;
                    }
                }
            }

            if !skip_comment {
                break;
            }
        }
    }

    fn lex_word(&mut self) -> Result<TokenWithSpan> {
        let start_pos = self.pos;
        let (_, start_loc) = self.next_char().expect("peek ensured Some");
        let mut last_loc = start_loc;

        while let Some(ch) = self.peek_char() {
            if Self::is_identifier_part(ch) {
                let (_, loc) = self.next_char().unwrap();
                last_loc = loc;
            } else {
                break;
            }
        }

        let value = &self.input[start_pos..self.pos];
        let keyword = Keyword::from_str(value);
        let word = Word {
            value: value.to_string(),
            quote_style: None,
            keyword,
        };

        Ok(TokenWithSpan {
            token: Token::Word(word),
            span: Span::new(start_loc, last_loc),
        })
    }

    fn lex_number(&mut self) -> Result<TokenWithSpan> {
        let start_pos = self.pos;
        let (_, start_loc) = self.next_char().expect("peek ensured Some");
        let mut last_loc = start_loc;
        let mut seen_dot = false;
        let mut seen_exp = false;

        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                let (_, loc) = self.next_char().unwrap();
                last_loc = loc;
                continue;
            }

            if ch == '.' {
                if seen_dot {
                    let mut value = self.input[start_pos..self.pos].to_string();
                    let _ = self.next_char().unwrap(); // consume the second '.'
                    value.push('.');
                    while let Some(c) = self.peek_char() {
                        if c.is_ascii_digit() {
                            let (d, _) = self.next_char().unwrap();
                            value.push(d);
                        } else {
                            break;
                        }
                    }
                    return Err(ParserError::InvalidNumber {
                        line: start_loc.line,
                        column: start_loc.column,
                        value,
                    });
                }

                // Only treat as float if next char is digit.
                if self
                    .peek_next_char()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
                {
                    let (_, loc) = self.next_char().unwrap();
                    last_loc = loc;
                    seen_dot = true;
                    continue;
                } else {
                    break;
                }
            }

            if ch == 'e' || ch == 'E' {
                if seen_exp {
                    break;
                }
                seen_exp = true;
                let (_, exp_loc) = self.next_char().unwrap();
                last_loc = exp_loc;

                // Optional sign
                if let Some(sign @ ('+' | '-')) = self.peek_char() {
                    let (_, loc) = self.next_char().unwrap();
                    last_loc = loc;
                    // record sign but no need to store
                    let _ = sign;
                }

                if !self
                    .peek_char()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
                {
                    let value = self.input[start_pos..self.pos].to_string();
                    return Err(ParserError::InvalidNumber {
                        line: start_loc.line,
                        column: start_loc.column,
                        value,
                    });
                }

                while let Some(c) = self.peek_char() {
                    if c.is_ascii_digit() {
                        let (_, loc) = self.next_char().unwrap();
                        last_loc = loc;
                    } else {
                        break;
                    }
                }
                continue;
            }

            break;
        }

        let value = self.input[start_pos..self.pos].to_string();
        Ok(TokenWithSpan {
            token: Token::Number(value),
            span: Span::new(start_loc, last_loc),
        })
    }

    #[allow(unused_assignments)]
    fn lex_string(&mut self) -> Result<TokenWithSpan> {
        let (_, start_loc) = self.next_char().expect("peek ensured Some"); // opening quote
        let mut last_loc = start_loc;
        let mut content = String::new();

        loop {
            let Some((ch, loc)) = self.next_char() else {
                return Err(ParserError::UnterminatedString {
                    line: start_loc.line,
                    column: start_loc.column,
                });
            };
            last_loc = loc;

            if ch == '\'' {
                if self.peek_char() == Some('\'') {
                    let _ = self.next_char().unwrap();
                    content.push('\'');
                    continue;
                } else {
                    break; // end of string
                }
            } else {
                content.push(ch);
            }
        }

        Ok(TokenWithSpan {
            token: Token::SingleQuotedString(content),
            span: Span::new(start_loc, last_loc),
        })
    }

    fn lex_lt_related(&mut self) -> Result<TokenWithSpan> {
        let (_, start_loc) = self.next_char().unwrap();
        let mut last_loc = start_loc;

        let token = match self.peek_char() {
            Some('=') => {
                let (_, loc) = self.next_char().unwrap();
                last_loc = loc;
                Token::LtEq
            }
            Some('>') => {
                let (_, loc) = self.next_char().unwrap();
                last_loc = loc;
                Token::Neq
            }
            _ => Token::Lt,
        };

        Ok(TokenWithSpan {
            token,
            span: Span::new(start_loc, last_loc),
        })
    }

    fn lex_gt_related(&mut self) -> Result<TokenWithSpan> {
        let (_, start_loc) = self.next_char().unwrap();
        let mut last_loc = start_loc;

        let token = match self.peek_char() {
            Some('=') => {
                let (_, loc) = self.next_char().unwrap();
                last_loc = loc;
                Token::GtEq
            }
            _ => Token::Gt,
        };

        Ok(TokenWithSpan {
            token,
            span: Span::new(start_loc, last_loc),
        })
    }

    fn lex_bang_related(&mut self) -> Result<TokenWithSpan> {
        let (_, start_loc) = self.next_char().unwrap();

        if self.peek_char() == Some('=') {
            let (_, loc) = self.next_char().unwrap();
            return Ok(TokenWithSpan {
                token: Token::Neq,
                span: Span::new(start_loc, loc),
            });
        }

        Err(ParserError::UnexpectedToken {
            line: start_loc.line,
            column: start_loc.column,
            expected: "valid operator".into(),
            found: "!".into(),
        })
    }

    fn lex_pipe_related(&mut self) -> Result<TokenWithSpan> {
        let (_, start_loc) = self.next_char().unwrap();

        if self.peek_char() == Some('|') {
            let (_, loc) = self.next_char().unwrap();
            Ok(TokenWithSpan {
                token: Token::StringConcat,
                span: Span::new(start_loc, loc),
            })
        } else {
            Err(ParserError::UnexpectedToken {
                line: start_loc.line,
                column: start_loc.column,
                expected: "||".into(),
                found: "|".into(),
            })
        }
    }

    fn single_char_token(&mut self, token: Token) -> TokenWithSpan {
        let (_, start_loc) = self.next_char().unwrap();
        TokenWithSpan {
            token,
            span: Span::new(start_loc, start_loc),
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_next_char(&self) -> Option<char> {
        let mut iter = self.input[self.pos..].chars();
        iter.next();
        iter.next()
    }

    fn next_char(&mut self) -> Option<(char, Location)> {
        let ch = self.peek_char()?;
        let loc = Location::new(self.line, self.column);
        self.pos += ch.len_utf8();
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some((ch, loc))
    }

    fn is_identifier_start(ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphabetic()
    }

    fn is_identifier_part(ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphanumeric()
    }
}
