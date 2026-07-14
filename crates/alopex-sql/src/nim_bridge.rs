use crate::ast::{Statement, StatementKind};
use crate::error::{ParserError, Result};
use crate::nim_ffi::{self, OwnedBuffer, ParseResultKind};

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    if sql.as_bytes().contains(&0) {
        return Err(ParserError::UnexpectedToken {
            line: 0,
            column: 0,
            expected: "valid SQL without interior NUL bytes".to_string(),
            found: "interior NUL byte".to_string(),
        });
    }

    let result = nim_ffi::parse_sql(sql);
    match result.kind {
        ParseResultKind::Ok => {
            let buffer = OwnedBuffer::new(result.buffer_ptr, result.buffer_len);
            // 正常時の payload は最低でも MessagePack の配列ヘッダ 1 バイトを
            // 含む。空 payload はゼロ初期化された CParseResult、つまり Nim 側
            // から例外が漏れた事故 (issue #40 の desync 経路) を意味するため、
            // 汎用の decode エラーではなく原因が特定できるエラーにする。
            if buffer.as_slice().is_empty() {
                return Err(ParserError::UnexpectedToken {
                    line: 0,
                    column: 0,
                    expected: "MessagePack AST matching docs/ffi-ast-contract.md".to_string(),
                    found: "empty payload from Nim parser (leaked exception at FFI boundary; \
                            see issue #40)"
                        .to_string(),
                });
            }
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

// nim-sql-parser/src/alopex_sql_parser.nim の `internalDefectPrefix` と
// 一致させる。Nim 側の `except Defect` 節が付与する接頭辞で、パーサー
// 内部の不変条件違反 (通常の構文エラーではない) を機械的に区別するための
// マーカー。ワイヤ契約 (MessagePack AST) には影響しない、エラー文言のみの
// 合意。
const INTERNAL_DEFECT_PREFIX: &str =
    "internal parser defect (this is a parser bug, not invalid SQL): ";

fn parser_error_from_nim(message: &str) -> ParserError {
    if let Some(defect_message) = message.strip_prefix(INTERNAL_DEFECT_PREFIX) {
        return ParserError::InternalParserDefect {
            message: defect_message.to_string(),
        };
    }
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
