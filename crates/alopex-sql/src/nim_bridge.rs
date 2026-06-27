use crate::ast::{Statement, StatementKind};
use crate::error::{ParserError, Result};
use crate::nim_ffi::{self, OwnedBuffer, ParseResultKind};

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let result = nim_ffi::parse_sql(sql);
    match result.kind {
        ParseResultKind::Ok => {
            let buffer = OwnedBuffer::new(result.buffer_ptr, result.buffer_len);
            rmp_serde::from_slice::<Vec<Statement>>(buffer.as_slice()).map_err(|err| {
                ParserError::UnexpectedToken {
                    line: 0,
                    column: 0,
                    expected: "MessagePack AST matching docs/ffi-ast-contract.md".to_string(),
                    found: err.to_string(),
                }
            })
        }
        ParseResultKind::Error => {
            let buffer = OwnedBuffer::new(result.error_ptr.cast(), result.error_len);
            Err(parser_error_from_nim(
                String::from_utf8_lossy(buffer.as_slice()).as_ref(),
            ))
        }
    }
}

pub fn parse_expression_sql(sql: &str) -> Result<crate::ast::Expr> {
    let wrapped = format!("SELECT {sql}");
    let statements = parse_sql(&wrapped)?;
    let Some(statement) = statements.into_iter().next() else {
        return Err(empty_expression_error());
    };
    let StatementKind::Select(select) = statement.kind else {
        return Err(empty_expression_error());
    };
    let Some(crate::ast::SelectItem::Expr { expr, .. }) = select.projection.into_iter().next()
    else {
        return Err(empty_expression_error());
    };
    Ok(expr)
}

fn empty_expression_error() -> ParserError {
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: "expression".to_string(),
        found: "empty parser result".to_string(),
    }
}

fn parser_error_from_nim(message: &str) -> ParserError {
    let (line, column) = parse_nim_line_col(message).unwrap_or((0, 0));
    ParserError::UnexpectedToken {
        line,
        column,
        expected: "valid SQL".to_string(),
        found: message.to_string(),
    }
}

fn parse_nim_line_col(message: &str) -> Option<(u64, u64)> {
    let after_line = message.strip_prefix("Parse error at line ")?;
    let (line, rest) = after_line.split_once(", col ")?;
    let (col, _) = rest.split_once(':')?;
    Some((line.parse().ok()?, col.parse().ok()?))
}
