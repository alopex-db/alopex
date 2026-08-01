use crate::ast::dml::{FromItem, Select, SelectItem};
use crate::ast::expr::{Expr, ExprKind};
use crate::ast::{Statement, StatementKind};
use crate::error::{ParserError, Result};
use crate::nim_ffi::{self, OwnedBuffer, ParseResultKind};

/// Return the SQL/PromQL MessagePack wire contract version exported by Nim.
pub fn parser_contract_version() -> String {
    nim_ffi::parser_contract_version()
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    if sql.as_bytes().contains(&0) {
        return Err(ParserError::UnexpectedToken {
            line: 0,
            column: 0,
            expected: "valid SQL without interior NUL bytes".to_string(),
            found: "interior NUL byte".to_string(),
        });
    }

    let natural_join_markers = natural_join_markers(sql);
    // Option (a): double-quoted tokens are identifiers under SQL standard and
    // PostgreSQL rules. The currently deployed Nim lexer predates that contract
    // and emits them as string literals, so normalize the FFI input until every
    // parser binary has the corrected token kind.
    let normalized_sql = normalize_quoted_identifiers(sql);
    let result = nim_ffi::parse_sql(&normalized_sql);
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
            let mut statements = rmp_serde::from_slice::<Vec<Statement>>(buffer.as_slice())
                .map_err(|err| ParserError::UnexpectedToken {
                    line: 0,
                    column: 0,
                    expected: "MessagePack AST matching docs/ffi-ast-contract.md".to_string(),
                    found: err.to_string(),
                })?;
            annotate_natural_joins(&mut statements, natural_join_markers);
            Ok(statements)
        }
        ParseResultKind::Error => {
            let buffer = OwnedBuffer::new(result.error_ptr.cast(), result.error_len);
            Err(parser_error_from_nim(
                String::from_utf8_lossy(buffer.as_slice()).as_ref(),
            ))
        }
    }
}

fn normalize_quoted_identifiers(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                normalized.push(ch);
                while let Some(string_ch) = chars.next() {
                    normalized.push(string_ch);
                    if string_ch == '\'' {
                        if chars.peek() == Some(&'\'') {
                            normalized.push(chars.next().expect("peeked quote"));
                        } else {
                            break;
                        }
                    }
                }
            }
            '"' => {
                while let Some(identifier_ch) = chars.next() {
                    if identifier_ch == '"' {
                        if chars.peek() == Some(&'"') {
                            normalized.push(chars.next().expect("peeked quote"));
                        } else {
                            break;
                        }
                    } else {
                        normalized.push(identifier_ch);
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                normalized.push(ch);
                normalized.push(chars.next().expect("peeked comment dash"));
                for comment_ch in chars.by_ref() {
                    normalized.push(comment_ch);
                    if comment_ch == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                normalized.push(ch);
                normalized.push(chars.next().expect("peeked comment star"));
                let mut previous = '\0';
                for comment_ch in chars.by_ref() {
                    normalized.push(comment_ch);
                    if previous == '*' && comment_ch == '/' {
                        break;
                    }
                    previous = comment_ch;
                }
            }
            ch if ch.is_ascii_alphabetic() || ch == '_' => {
                let mut identifier = String::from(ch);
                while chars
                    .peek()
                    .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
                {
                    identifier.push(chars.next().expect("peeked identifier character"));
                }
                // PostgreSQL folds bare identifiers to lowercase. Delimited
                // identifiers take the `\"` branch above and keep their exact
                // spelling for case-sensitive resolution.
                normalized.push_str(&identifier.to_ascii_lowercase());
            }
            _ => normalized.push(ch),
        }
    }
    normalized
}

fn natural_join_markers(sql: &str) -> Vec<bool> {
    let mut markers = Vec::new();
    let mut saw_natural = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' | '"' => skip_quoted(&mut chars, ch),
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for comment_ch in chars.by_ref() {
                    if comment_ch == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut previous = '\0';
                for comment_ch in chars.by_ref() {
                    if previous == '*' && comment_ch == '/' {
                        break;
                    }
                    previous = comment_ch;
                }
            }
            ';' => saw_natural = false,
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut word = String::from(c);
                while chars
                    .peek()
                    .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
                {
                    word.push(chars.next().expect("peeked identifier character"));
                }
                match word.to_ascii_lowercase().as_str() {
                    "natural" => saw_natural = true,
                    "join" => {
                        markers.push(saw_natural);
                        saw_natural = false;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    markers
}

fn skip_quoted(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, quote: char) {
    while let Some(ch) = chars.next() {
        if ch == quote {
            if chars.peek() == Some(&quote) {
                chars.next();
            } else {
                break;
            }
        }
    }
}

fn annotate_natural_joins(statements: &mut [Statement], natural_markers: Vec<bool>) {
    let mut natural_markers = natural_markers.into_iter();
    for statement in statements {
        if let StatementKind::Select(select) = &mut statement.kind {
            annotate_select_natural_joins(select, &mut natural_markers);
        }
    }
}

fn annotate_select_natural_joins(
    select: &mut Select,
    natural_markers: &mut impl Iterator<Item = bool>,
) {
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            annotate_expr_natural_joins(expr, natural_markers);
        }
    }
    for from in &mut select.from {
        annotate_from_natural_joins(from, natural_markers);
    }
    if let Some(selection) = &mut select.selection {
        annotate_expr_natural_joins(selection, natural_markers);
    }
    if let Some(group_by) = &mut select.group_by {
        for expression in group_by {
            annotate_expr_natural_joins(expression, natural_markers);
        }
    }
    if let Some(having) = &mut select.having {
        annotate_expr_natural_joins(having, natural_markers);
    }
    for order_by in &mut select.order_by {
        annotate_expr_natural_joins(&mut order_by.expr, natural_markers);
    }
    if let Some(limit) = &mut select.limit {
        annotate_expr_natural_joins(limit, natural_markers);
    }
    if let Some(offset) = &mut select.offset {
        annotate_expr_natural_joins(offset, natural_markers);
    }
}

fn annotate_from_natural_joins(
    from: &mut FromItem,
    natural_markers: &mut impl Iterator<Item = bool>,
) {
    match from {
        FromItem::Join {
            left,
            right,
            natural,
            ..
        } => {
            annotate_from_natural_joins(left, natural_markers);
            if let Some(marker) = natural_markers.next() {
                *natural |= marker;
            }
            annotate_from_natural_joins(right, natural_markers);
        }
        FromItem::Derived { subquery, .. } => {
            if let StatementKind::Select(select) = &mut subquery.kind {
                annotate_select_natural_joins(select, natural_markers);
            }
        }
        FromItem::Table { .. } => {}
    }
}

fn annotate_expr_natural_joins(expr: &mut Expr, natural_markers: &mut impl Iterator<Item = bool>) {
    match &mut expr.kind {
        ExprKind::ScalarSubquery { subquery } | ExprKind::Exists { subquery, .. } => {
            if let StatementKind::Select(select) = &mut subquery.kind {
                annotate_select_natural_joins(select, natural_markers);
            }
        }
        ExprKind::InSubquery { expr, subquery, .. }
        | ExprKind::Quantified { expr, subquery, .. } => {
            annotate_expr_natural_joins(expr, natural_markers);
            if let StatementKind::Select(select) = &mut subquery.kind {
                annotate_select_natural_joins(select, natural_markers);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            annotate_expr_natural_joins(left, natural_markers);
            annotate_expr_natural_joins(right, natural_markers);
        }
        ExprKind::UnaryOp { operand, .. } | ExprKind::IsNull { expr: operand, .. } => {
            annotate_expr_natural_joins(operand, natural_markers);
        }
        ExprKind::FunctionCall { args, .. } => {
            for argument in args {
                annotate_expr_natural_joins(argument, natural_markers);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            annotate_expr_natural_joins(expr, natural_markers);
            annotate_expr_natural_joins(low, natural_markers);
            annotate_expr_natural_joins(high, natural_markers);
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            annotate_expr_natural_joins(expr, natural_markers);
            annotate_expr_natural_joins(pattern, natural_markers);
            if let Some(escape) = escape {
                annotate_expr_natural_joins(escape, natural_markers);
            }
        }
        ExprKind::InList { expr, list, .. } => {
            annotate_expr_natural_joins(expr, natural_markers);
            for item in list {
                annotate_expr_natural_joins(item, natural_markers);
            }
        }
        ExprKind::Cast { expr, .. } => {
            annotate_expr_natural_joins(expr, natural_markers);
        }
        ExprKind::Literal { .. } | ExprKind::ColumnRef { .. } | ExprKind::VectorLiteral { .. } => {}
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
