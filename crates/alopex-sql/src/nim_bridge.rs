use crate::ast::ddl::CreateContinuousAggregate;
use crate::ast::dml::{FromItem, QueryBody, Select, SelectItem, SetOperation, SetOperator, Values};
use crate::ast::expr::{Expr, ExprKind};
use crate::ast::{Location, Span, Statement, StatementKind};
use crate::error::{ParserError, Result};
use crate::nim_ffi::{self, OwnedBuffer, ParseResultKind};
use serde::Deserialize;

const MAX_SQL_INPUT_BYTES: usize = 1_048_576;
const MAX_MESSAGEPACK_PAYLOAD_BYTES: usize = 1_048_576;
const MAX_MESSAGEPACK_DEPTH: usize = 128;
const MAX_MESSAGEPACK_VALUES: usize = 65_536;
const SELECT_WRAPPER_PREFIX: &str = "SELECT ";
const PARSER_CONTRACT_DESCRIPTOR: &str = include_str!("../nim-sql-parser/PARSER_CONTRACT_VERSION");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputPreflightError {
    TooLarge,
    LengthOverflow,
    InteriorNul,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessagePackPreflightError {
    TooLarge,
    TooDeep,
    TooManyValues,
    Truncated,
    ReservedMarker,
    TrailingBytes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedContinuousAggregateStatement {
    kind: StagedContinuousAggregateKind,
    span: Span,
}

#[derive(Deserialize)]
#[serde(tag = "variant")]
enum StagedContinuousAggregateKind {
    CreateContinuousAggregate(CreateContinuousAggregate),
}

/// Exact Select wire adapter for the continuous-aggregate payload.
///
/// Existing top-level Select statements encode their variant through
/// `StatementKind`. The nested continuous-aggregate query is a named Select
/// payload in its own right, so it carries and validates an explicit
/// `variant: Select` field.
pub(crate) mod continuous_aggregate_select_wire {
    use crate::ast::{
        Expr, FromItem, OrderByExpr, Select, SelectItem, SetOperation, Span, WithClause,
    };
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize)]
    struct SelectWireRef<'a> {
        variant: &'static str,
        distinct: bool,
        projection: &'a [SelectItem],
        from: &'a [FromItem],
        selection: &'a Option<Expr>,
        group_by: &'a Option<Vec<Expr>>,
        having: &'a Option<Expr>,
        set_operations: &'a [SetOperation],
        order_by: &'a [OrderByExpr],
        limit: &'a Option<Expr>,
        offset: &'a Option<Expr>,
        span: Span,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SelectWire {
        variant: String,
        #[serde(default)]
        with: Option<WithClause>,
        distinct: bool,
        projection: Vec<SelectItem>,
        from: Vec<FromItem>,
        selection: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        #[serde(default)]
        set_operations: Vec<SetOperation>,
        order_by: Vec<OrderByExpr>,
        limit: Option<Expr>,
        offset: Option<Expr>,
        span: Span,
    }

    pub(crate) fn serialize<S>(
        select: &Select,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SelectWireRef {
            variant: "Select",
            distinct: select.distinct,
            projection: &select.projection,
            from: &select.from,
            selection: &select.selection,
            group_by: &select.group_by,
            having: &select.having,
            set_operations: &select.set_operations,
            order_by: &select.order_by,
            limit: &select.limit,
            offset: &select.offset,
            span: select.span,
        }
        .serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Select, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SelectWire::deserialize(deserializer)?;
        if wire.variant != "Select" {
            return Err(D::Error::custom(format!(
                "expected nested query variant `Select`, found `{}`",
                wire.variant
            )));
        }
        Ok(Select {
            with: wire.with,
            distinct: wire.distinct,
            // The staged wire payload is frozen and cannot carry DISTINCT ON;
            // the Nim staging validator rejects it before encoding.
            distinct_on: Vec::new(),
            projection: wire.projection,
            from: wire.from,
            selection: wire.selection,
            group_by: wire.group_by,
            having: wire.having,
            windows: Vec::new(),
            qualify: None,
            set_operations: wire.set_operations,
            order_by: wire.order_by,
            limit: wire.limit,
            offset: wire.offset,
            // The staged wire payload is frozen and cannot carry WITH TIES;
            // the Nim staging validator rejects it before encoding.
            limit_with_ties: false,
            span: wire.span,
        })
    }
}

impl CreateContinuousAggregate {
    /// Decode the staged continuous-aggregate wire shape.
    ///
    /// The linked-version check is the same one used by the production parser
    /// path and always runs before payload preflight.
    #[doc(hidden)]
    pub fn decode_staged_messagepack(linked_parser_contract: &str, payload: &[u8]) -> Result<Self> {
        ensure_linked_parser_contract(linked_parser_contract)?;
        validate_bounded_messagepack(payload).map_err(messagepack_preflight_error)?;
        let decoded = rmp_serde::from_slice::<StagedContinuousAggregateStatement>(payload)
            .map_err(messagepack_decode_error)?;
        let StagedContinuousAggregateKind::CreateContinuousAggregate(statement) = decoded.kind;
        if decoded.span != statement.span {
            return Err(ParserError::UnexpectedToken {
                line: 0,
                column: 0,
                expected: "matching outer and kind spans in MessagePack AST".to_string(),
                found: "continuous aggregate outer span differs from kind span".to_string(),
            });
        }
        Ok(statement)
    }
}

/// Return the SQL/PromQL MessagePack wire contract version exported by Nim.
pub fn parser_contract_version() -> String {
    nim_ffi::parser_contract_version()
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    preflight_input(sql, 0).map_err(parser_error_from_preflight)?;
    parse_sql_preflighted(sql)
}

fn parse_sql_preflighted(sql: &str) -> Result<Vec<Statement>> {
    let tokens = scan_top_level_tokens(sql);
    let ranges = top_level_statement_ranges(sql, &tokens);
    let contains_set_operation = ranges.iter().any(|(start, end)| {
        tokens.iter().any(|token| {
            token.start >= *start
                && token.end <= *end
                && matches!(token.kind, TopLevelTokenKind::Word)
                && set_operator(&sql[token.start..token.end]).is_some()
        })
    });
    if contains_set_operation
        && let Ok(statements) = parse_sql_via_ffi(sql)
        && statements.len() == ranges.len()
    {
        return Ok(statements);
    }
    if let Some(statements) = parse_set_operation_batch(sql)? {
        return Ok(statements);
    }
    parse_sql_via_ffi(sql)
}

fn parse_sql_via_ffi(sql: &str) -> Result<Vec<Statement>> {
    ensure_linked_parser_contract(&nim_ffi::parser_contract_version())?;
    let natural_join_markers = natural_join_markers(sql);
    // Option (a): double-quoted tokens are identifiers under SQL standard and
    // PostgreSQL rules. The currently deployed Nim lexer predates that contract
    // and emits them as string literals, so normalize the FFI input until every
    // parser binary has the corrected token kind.
    let normalized_sql = normalize_quoted_identifiers(sql);
    let result = nim_ffi::parse_sql(&normalized_sql).map_err(parser_error_from_ffi_input)?;
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
            validate_bounded_messagepack(buffer.as_slice()).map_err(messagepack_preflight_error)?;
            let mut statements = rmp_serde::from_slice::<Vec<Statement>>(buffer.as_slice())
                .map_err(messagepack_decode_error)?;
            annotate_natural_joins(&mut statements, natural_join_markers)?;
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

#[derive(Debug, Clone, Copy)]
enum TopLevelTokenKind {
    Word,
    Semicolon,
}

#[derive(Debug, Clone, Copy)]
struct TopLevelToken {
    kind: TopLevelTokenKind,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone, Copy)]
struct SetOperationSpec {
    operator: SetOperator,
    all: bool,
    span: Span,
}

fn parse_set_operation_batch(sql: &str) -> Result<Option<Vec<Statement>>> {
    let tokens = scan_top_level_tokens(sql);
    let ranges = top_level_statement_ranges(sql, &tokens);

    let contains_set_operation = ranges.iter().any(|(start, end)| {
        tokens.iter().any(|token| {
            token.start >= *start
                && token.end <= *end
                && matches!(token.kind, TopLevelTokenKind::Word)
                && set_operator(&sql[token.start..token.end]).is_some()
        })
    });
    if !contains_set_operation {
        return Ok(None);
    }

    let mut statements = Vec::new();
    for (start, end) in ranges {
        let words = tokens
            .iter()
            .copied()
            .filter(|token| {
                token.start >= start
                    && token.end <= end
                    && matches!(token.kind, TopLevelTokenKind::Word)
            })
            .collect::<Vec<_>>();
        if words
            .iter()
            .any(|word| set_operator(&sql[word.start..word.end]).is_some())
        {
            statements.push(parse_set_operation_statement(sql, start, end, &words)?);
        } else {
            statements.extend(parse_sql_via_ffi(&sql[start..end])?);
        }
    }
    Ok(Some(statements))
}

fn top_level_statement_ranges(sql: &str, tokens: &[TopLevelToken]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    for token in tokens {
        if matches!(token.kind, TopLevelTokenKind::Semicolon) {
            if !sql[start..token.start].trim().is_empty() {
                ranges.push((start, token.start));
            }
            start = token.end;
        }
    }
    if !sql[start..].trim().is_empty() {
        ranges.push((start, sql.len()));
    }
    ranges
}

fn parse_set_operation_statement(
    sql: &str,
    statement_start: usize,
    statement_end: usize,
    words: &[TopLevelToken],
) -> Result<Statement> {
    let mut branch_ranges = Vec::new();
    let mut operations = Vec::new();
    let mut branch_start = statement_start;

    for (index, word) in words.iter().enumerate() {
        let Some(operator) = set_operator(&sql[word.start..word.end]) else {
            continue;
        };
        branch_ranges.push((branch_start, word.start));
        let all_word = words
            .get(index + 1)
            .filter(|next| sql[next.start..next.end].eq_ignore_ascii_case("all"));
        let all = all_word.is_some();
        branch_start = all_word.map_or(word.end, |token| token.end);
        operations.push(SetOperationSpec {
            operator,
            all,
            span: span_for_offsets(sql, word.start, word.end),
        });
    }
    branch_ranges.push((branch_start, statement_end));

    if branch_ranges.len() != operations.len() + 1 {
        return Err(set_operation_parser_error(
            "a SELECT query on both sides of every set operator",
            "malformed set-operation chain",
        ));
    }

    let mut branches = branch_ranges
        .into_iter()
        .map(|(start, end)| parse_query_body_fragment(&sql[start..end]))
        .collect::<Result<Vec<_>>>()?;
    let final_branch = branches.last_mut().ok_or_else(|| {
        set_operation_parser_error("at least one query body", "empty set-operation chain")
    })?;
    let tail = take_query_tail(final_branch);

    let mut terms = Vec::new();
    let mut outer_operations = Vec::new();
    let mut current = branches.remove(0);
    for (operation, right) in operations.into_iter().zip(branches) {
        if operation.operator == SetOperator::Intersect {
            query_set_operations_mut(&mut current).push(SetOperation {
                operator: operation.operator,
                all: operation.all,
                right: Box::new(right),
                span: operation.span,
            });
        } else {
            terms.push(current);
            outer_operations.push(operation);
            current = right;
        }
    }
    terms.push(current);

    let mut root = terms.remove(0);
    for (operation, right) in outer_operations.into_iter().zip(terms) {
        query_set_operations_mut(&mut root).push(SetOperation {
            operator: operation.operator,
            all: operation.all,
            right: Box::new(right),
            span: operation.span,
        });
    }
    set_query_tail(&mut root, tail);
    let span = query_body_span(&root);
    Ok(Statement {
        kind: match root {
            QueryBody::Select(select) => StatementKind::Select(select),
            QueryBody::Values(values) => StatementKind::Values(values),
        },
        span,
    })
}

fn parse_query_body_fragment(sql: &str) -> Result<QueryBody> {
    let mut statements = parse_sql_via_ffi(sql.trim())?;
    if statements.len() != 1 {
        return Err(set_operation_parser_error(
            "exactly one query body per set-operation input",
            format!("{} statements", statements.len()),
        ));
    }
    match statements.remove(0).kind {
        StatementKind::Select(select) => Ok(QueryBody::Select(select)),
        StatementKind::Values(values) => Ok(QueryBody::Values(values)),
        _ => Err(set_operation_parser_error(
            "SELECT or VALUES query in set operation",
            "non-query statement",
        )),
    }
}

fn query_set_operations_mut(body: &mut QueryBody) -> &mut Vec<SetOperation> {
    match body {
        QueryBody::Select(select) => &mut select.set_operations,
        QueryBody::Values(values) => &mut values.set_operations,
    }
}

type QueryTail = (
    Vec<crate::ast::OrderByExpr>,
    Option<Expr>,
    Option<Expr>,
    bool,
);

fn take_query_tail(body: &mut QueryBody) -> QueryTail {
    match body {
        QueryBody::Select(select) => (
            std::mem::take(&mut select.order_by),
            select.limit.take(),
            select.offset.take(),
            std::mem::take(&mut select.limit_with_ties),
        ),
        QueryBody::Values(values) => (
            std::mem::take(&mut values.order_by),
            values.limit.take(),
            values.offset.take(),
            std::mem::take(&mut values.limit_with_ties),
        ),
    }
}

fn set_query_tail(body: &mut QueryBody, tail: QueryTail) {
    let (order_by, limit, offset, limit_with_ties) = tail;
    match body {
        QueryBody::Select(select) => {
            select.order_by = order_by;
            select.limit = limit;
            select.offset = offset;
            select.limit_with_ties = limit_with_ties;
        }
        QueryBody::Values(values) => {
            values.order_by = order_by;
            values.limit = limit;
            values.offset = offset;
            values.limit_with_ties = limit_with_ties;
        }
    }
}

fn query_body_span(body: &QueryBody) -> Span {
    match body {
        QueryBody::Select(select) => select.span,
        QueryBody::Values(values) => values.span,
    }
}

fn set_operator(operator: &str) -> Option<SetOperator> {
    if operator.eq_ignore_ascii_case("union") {
        Some(SetOperator::Union)
    } else if operator.eq_ignore_ascii_case("intersect") {
        Some(SetOperator::Intersect)
    } else if operator.eq_ignore_ascii_case("except") {
        Some(SetOperator::Except)
    } else {
        None
    }
}

fn set_operation_parser_error(
    expected: impl Into<String>,
    found: impl Into<String>,
) -> ParserError {
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: expected.into(),
        found: found.into(),
    }
}

fn span_for_offsets(sql: &str, start: usize, end: usize) -> Span {
    fn location(sql: &str, offset: usize) -> Location {
        let prefix = &sql[..offset.min(sql.len())];
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() as u64 + 1;
        let column = prefix
            .rsplit_once('\n')
            .map_or(prefix.len(), |(_, tail)| tail.len()) as u64
            + 1;
        Location::new(line, column)
    }
    Span::new(location(sql, start), location(sql, end.saturating_sub(1)))
}

fn scan_top_level_tokens(sql: &str) -> Vec<TopLevelToken> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut depth = 0_u32;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            b';' if depth == 0 => {
                tokens.push(TopLevelToken {
                    kind: TopLevelTokenKind::Semicolon,
                    start: index,
                    end: index + 1,
                });
                index += 1;
            }
            byte if depth == 0 && (byte.is_ascii_alphabetic() || byte == b'_') => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(TopLevelToken {
                    kind: TopLevelTokenKind::Word,
                    start,
                    end: index,
                });
            }
            _ => index += 1,
        }
    }
    tokens
}

fn expected_parser_contract() -> &'static str {
    PARSER_CONTRACT_DESCRIPTOR.trim()
}

fn ensure_linked_parser_contract(linked_parser_contract: &str) -> Result<()> {
    ensure_parser_contract(expected_parser_contract(), linked_parser_contract)
}

fn ensure_parser_contract(expected: &str, linked_parser_contract: &str) -> Result<()> {
    if linked_parser_contract == expected {
        return Ok(());
    }
    Err(ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: format!("linked Nim parser contract {expected}"),
        found: format!("linked Nim parser contract {linked_parser_contract}"),
    })
}

fn messagepack_decode_error(error: rmp_serde::decode::Error) -> ParserError {
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: "bounded MessagePack AST matching docs/ffi-ast-contract.md".to_string(),
        found: error.to_string(),
    }
}

fn messagepack_preflight_error(error: MessagePackPreflightError) -> ParserError {
    let found = match error {
        MessagePackPreflightError::TooLarge => {
            format!("MessagePack payload exceeds {MAX_MESSAGEPACK_PAYLOAD_BYTES} bytes")
        }
        MessagePackPreflightError::TooDeep => {
            format!("MessagePack nesting exceeds {MAX_MESSAGEPACK_DEPTH} levels")
        }
        MessagePackPreflightError::TooManyValues => {
            format!("MessagePack collection limit of {MAX_MESSAGEPACK_VALUES} values exceeded")
        }
        MessagePackPreflightError::Truncated => "truncated MessagePack payload".to_string(),
        MessagePackPreflightError::ReservedMarker => "reserved MessagePack marker 0xc1".to_string(),
        MessagePackPreflightError::TrailingBytes => {
            "trailing bytes after MessagePack payload".to_string()
        }
    };
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: "bounded MessagePack AST matching docs/ffi-ast-contract.md".to_string(),
        found,
    }
}

fn validate_bounded_messagepack(
    payload: &[u8],
) -> std::result::Result<(), MessagePackPreflightError> {
    if payload.len() > MAX_MESSAGEPACK_PAYLOAD_BYTES {
        return Err(MessagePackPreflightError::TooLarge);
    }
    let mut scanner = MessagePackScanner {
        payload,
        position: 0,
        values: 0,
    };
    scanner.scan_value(1)?;
    if scanner.position != payload.len() {
        return Err(MessagePackPreflightError::TrailingBytes);
    }
    Ok(())
}

struct MessagePackScanner<'a> {
    payload: &'a [u8],
    position: usize,
    values: usize,
}

impl MessagePackScanner<'_> {
    fn scan_value(&mut self, depth: usize) -> std::result::Result<(), MessagePackPreflightError> {
        if depth > MAX_MESSAGEPACK_DEPTH {
            return Err(MessagePackPreflightError::TooDeep);
        }
        self.values = self
            .values
            .checked_add(1)
            .ok_or(MessagePackPreflightError::TooManyValues)?;
        if self.values > MAX_MESSAGEPACK_VALUES {
            return Err(MessagePackPreflightError::TooManyValues);
        }

        let marker = self.read_byte()?;
        match marker {
            0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => Ok(()),
            0x80..=0x8f => self.scan_map(usize::from(marker & 0x0f), depth),
            0x90..=0x9f => self.scan_children(usize::from(marker & 0x0f), depth),
            0xa0..=0xbf => self.skip(usize::from(marker & 0x1f)),
            0xc1 => Err(MessagePackPreflightError::ReservedMarker),
            0xc4 | 0xd9 => {
                let length = usize::from(self.read_byte()?);
                self.skip(length)
            }
            0xc5 | 0xda => {
                let length = usize::from(self.read_u16()?);
                self.skip(length)
            }
            0xc6 | 0xdb => {
                let length = usize::try_from(self.read_u32()?)
                    .map_err(|_| MessagePackPreflightError::TooLarge)?;
                self.skip(length)
            }
            0xc7 => {
                let length = usize::from(self.read_byte()?);
                self.skip_ext(length)
            }
            0xc8 => {
                let length = usize::from(self.read_u16()?);
                self.skip_ext(length)
            }
            0xc9 => {
                let length = usize::try_from(self.read_u32()?)
                    .map_err(|_| MessagePackPreflightError::TooLarge)?;
                self.skip_ext(length)
            }
            0xca => self.skip(4),
            0xcb => self.skip(8),
            0xcc | 0xd0 => self.skip(1),
            0xcd | 0xd1 => self.skip(2),
            0xce | 0xd2 => self.skip(4),
            0xcf | 0xd3 => self.skip(8),
            0xd4 => self.skip_ext(1),
            0xd5 => self.skip_ext(2),
            0xd6 => self.skip_ext(4),
            0xd7 => self.skip_ext(8),
            0xd8 => self.skip_ext(16),
            0xdc => {
                let count = usize::from(self.read_u16()?);
                self.scan_children(count, depth)
            }
            0xdd => {
                let count = usize::try_from(self.read_u32()?)
                    .map_err(|_| MessagePackPreflightError::TooManyValues)?;
                self.scan_children(count, depth)
            }
            0xde => {
                let count = usize::from(self.read_u16()?);
                self.scan_map(count, depth)
            }
            0xdf => {
                let count = usize::try_from(self.read_u32()?)
                    .map_err(|_| MessagePackPreflightError::TooManyValues)?;
                self.scan_map(count, depth)
            }
        }
    }

    fn scan_map(
        &mut self,
        entries: usize,
        depth: usize,
    ) -> std::result::Result<(), MessagePackPreflightError> {
        let children = entries
            .checked_mul(2)
            .ok_or(MessagePackPreflightError::TooManyValues)?;
        self.scan_children(children, depth)
    }

    fn scan_children(
        &mut self,
        children: usize,
        depth: usize,
    ) -> std::result::Result<(), MessagePackPreflightError> {
        if children > MAX_MESSAGEPACK_VALUES {
            return Err(MessagePackPreflightError::TooManyValues);
        }
        for _ in 0..children {
            self.scan_value(depth + 1)?;
        }
        Ok(())
    }

    fn skip_ext(
        &mut self,
        payload_length: usize,
    ) -> std::result::Result<(), MessagePackPreflightError> {
        let total = payload_length
            .checked_add(1)
            .ok_or(MessagePackPreflightError::TooLarge)?;
        self.skip(total)
    }

    fn read_byte(&mut self) -> std::result::Result<u8, MessagePackPreflightError> {
        let byte = *self
            .payload
            .get(self.position)
            .ok_or(MessagePackPreflightError::Truncated)?;
        self.position += 1;
        Ok(byte)
    }

    fn read_u16(&mut self) -> std::result::Result<u16, MessagePackPreflightError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> std::result::Result<u32, MessagePackPreflightError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn skip(&mut self, length: usize) -> std::result::Result<(), MessagePackPreflightError> {
        self.take(length).map(|_| ())
    }

    fn take(&mut self, length: usize) -> std::result::Result<&[u8], MessagePackPreflightError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(MessagePackPreflightError::Truncated)?;
        let bytes = self
            .payload
            .get(self.position..end)
            .ok_or(MessagePackPreflightError::Truncated)?;
        self.position = end;
        Ok(bytes)
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
                // Replace each quote with a space rather than removing it, so
                // every later token keeps its original offset and diagnostics
                // point into the SQL the caller actually wrote.
                normalized.push(' ');
                while let Some(identifier_ch) = chars.next() {
                    if identifier_ch == '"' {
                        if chars.peek() == Some(&'"') {
                            // An escaped quote is two characters in the input
                            // and one in the identifier; pad to keep the width.
                            normalized.push(chars.next().expect("peeked quote"));
                            normalized.push(' ');
                        } else {
                            normalized.push(' ');
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

/// Apply the parser's NATURAL markers to the joins they belong to.
///
/// The markers arrive as a flat list alongside the AST, so they only line up
/// while both sides walk the joins in the same order. A mismatch used to leave
/// the remaining joins as plain joins, turning `NATURAL JOIN` into a cross
/// product without any diagnostic. Treat it as the contract violation it is.
fn annotate_natural_joins(statements: &mut [Statement], natural_markers: Vec<bool>) -> Result<()> {
    let supplied = natural_markers.len();
    let mut natural_markers = natural_markers.into_iter();
    let mut consumed = 0usize;
    for statement in statements {
        match &mut statement.kind {
            StatementKind::Select(select) => {
                annotate_select_natural_joins(select, &mut natural_markers, &mut consumed);
            }
            StatementKind::Values(values) => {
                annotate_values_natural_joins(values, &mut natural_markers, &mut consumed);
            }
            _ => {}
        }
    }

    if consumed != supplied {
        return Err(ParserError::UnexpectedToken {
            line: 0,
            column: 0,
            expected: format!("{supplied} NATURAL join markers, one per join"),
            found: format!("{consumed} joins in the AST"),
        });
    }
    Ok(())
}

fn annotate_select_natural_joins(
    select: &mut Select,
    natural_markers: &mut impl Iterator<Item = bool>,
    consumed: &mut usize,
) {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            annotate_query_body_natural_joins(&mut cte.query, natural_markers, consumed);
        }
    }
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
        }
    }
    for from in &mut select.from {
        annotate_from_natural_joins(from, natural_markers, consumed);
    }
    if let Some(selection) = &mut select.selection {
        annotate_expr_natural_joins(selection, natural_markers, consumed);
    }
    if let Some(group_by) = &mut select.group_by {
        for expression in group_by {
            annotate_expr_natural_joins(expression, natural_markers, consumed);
        }
    }
    if let Some(having) = &mut select.having {
        annotate_expr_natural_joins(having, natural_markers, consumed);
    }
    for operation in &mut select.set_operations {
        annotate_query_body_natural_joins(&mut operation.right, natural_markers, consumed);
    }
    for order_by in &mut select.order_by {
        annotate_expr_natural_joins(&mut order_by.expr, natural_markers, consumed);
    }
    if let Some(limit) = &mut select.limit {
        annotate_expr_natural_joins(limit, natural_markers, consumed);
    }
    if let Some(offset) = &mut select.offset {
        annotate_expr_natural_joins(offset, natural_markers, consumed);
    }
}

fn annotate_values_natural_joins(
    values: &mut Values,
    natural_markers: &mut impl Iterator<Item = bool>,
    consumed: &mut usize,
) {
    if let Some(with) = &mut values.with {
        for cte in &mut with.ctes {
            annotate_query_body_natural_joins(&mut cte.query, natural_markers, consumed);
        }
    }
    for row in &mut values.rows {
        for expr in row {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
        }
    }
    for operation in &mut values.set_operations {
        annotate_query_body_natural_joins(&mut operation.right, natural_markers, consumed);
    }
    for order_by in &mut values.order_by {
        annotate_expr_natural_joins(&mut order_by.expr, natural_markers, consumed);
    }
    if let Some(limit) = &mut values.limit {
        annotate_expr_natural_joins(limit, natural_markers, consumed);
    }
    if let Some(offset) = &mut values.offset {
        annotate_expr_natural_joins(offset, natural_markers, consumed);
    }
}

fn annotate_query_body_natural_joins(
    body: &mut QueryBody,
    natural_markers: &mut impl Iterator<Item = bool>,
    consumed: &mut usize,
) {
    match body {
        QueryBody::Select(select) => {
            annotate_select_natural_joins(select, natural_markers, consumed);
        }
        QueryBody::Values(values) => {
            annotate_values_natural_joins(values, natural_markers, consumed);
        }
    }
}

fn annotate_from_natural_joins(
    from: &mut FromItem,
    natural_markers: &mut impl Iterator<Item = bool>,
    consumed: &mut usize,
) {
    match from {
        FromItem::Join {
            left,
            right,
            natural,
            ..
        } => {
            annotate_from_natural_joins(left, natural_markers, consumed);
            if let Some(marker) = natural_markers.next() {
                *natural |= marker;
                *consumed += 1;
            }
            annotate_from_natural_joins(right, natural_markers, consumed);
        }
        FromItem::Derived { subquery, .. } => {
            annotate_query_body_natural_joins(subquery, natural_markers, consumed);
        }
        FromItem::Table { .. } => {}
    }
}

fn annotate_expr_natural_joins(
    expr: &mut Expr,
    natural_markers: &mut impl Iterator<Item = bool>,
    consumed: &mut usize,
) {
    match &mut expr.kind {
        ExprKind::ScalarSubquery { subquery } | ExprKind::Exists { subquery, .. } => {
            if let StatementKind::Select(select) = &mut subquery.kind {
                annotate_select_natural_joins(select, natural_markers, consumed);
            }
        }
        ExprKind::InSubquery { expr, subquery, .. }
        | ExprKind::Quantified { expr, subquery, .. } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
            if let StatementKind::Select(select) = &mut subquery.kind {
                annotate_select_natural_joins(select, natural_markers, consumed);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            annotate_expr_natural_joins(left, natural_markers, consumed);
            annotate_expr_natural_joins(right, natural_markers, consumed);
        }
        ExprKind::UnaryOp { operand, .. } | ExprKind::IsNull { expr: operand, .. } => {
            annotate_expr_natural_joins(operand, natural_markers, consumed);
        }
        ExprKind::TruthPredicate { expr, .. } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
        }
        ExprKind::IsDistinctFrom { left, right, .. } => {
            annotate_expr_natural_joins(left, natural_markers, consumed);
            annotate_expr_natural_joins(right, natural_markers, consumed);
        }
        ExprKind::Row { items } => {
            for item in items {
                annotate_expr_natural_joins(item, natural_markers, consumed);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                annotate_expr_natural_joins(operand, natural_markers, consumed);
            }
            for branch in branches {
                annotate_expr_natural_joins(&mut branch.when, natural_markers, consumed);
                annotate_expr_natural_joins(&mut branch.then, natural_markers, consumed);
            }
            if let Some(else_expr) = else_expr {
                annotate_expr_natural_joins(else_expr, natural_markers, consumed);
            }
        }
        ExprKind::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            for argument in args {
                annotate_expr_natural_joins(argument, natural_markers, consumed);
            }
            for order in order_by {
                annotate_expr_natural_joins(&mut order.expr, natural_markers, consumed);
            }
            for order in within_group {
                annotate_expr_natural_joins(&mut order.expr, natural_markers, consumed);
            }
            if let Some(filter) = filter {
                annotate_expr_natural_joins(filter, natural_markers, consumed);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
            annotate_expr_natural_joins(low, natural_markers, consumed);
            annotate_expr_natural_joins(high, natural_markers, consumed);
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
            annotate_expr_natural_joins(pattern, natural_markers, consumed);
            if let Some(escape) = escape {
                annotate_expr_natural_joins(escape, natural_markers, consumed);
            }
        }
        ExprKind::InList { expr, list, .. } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
            for item in list {
                annotate_expr_natural_joins(item, natural_markers, consumed);
            }
        }
        ExprKind::Cast { expr, .. } | ExprKind::TryCast { expr, .. } => {
            annotate_expr_natural_joins(expr, natural_markers, consumed);
        }
        ExprKind::Literal { .. } | ExprKind::ColumnRef { .. } | ExprKind::VectorLiteral { .. } => {}
    }
}

pub fn parse_expression_sql(sql: &str) -> Result<crate::ast::Expr> {
    let wrapped_len =
        preflight_input(sql, SELECT_WRAPPER_PREFIX.len()).map_err(parser_error_from_preflight)?;
    let mut wrapped = String::with_capacity(wrapped_len);
    wrapped.push_str(SELECT_WRAPPER_PREFIX);
    wrapped.push_str(sql);
    let statements = parse_sql_preflighted(&wrapped)?;
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

fn checked_total_input_len(
    input_len: usize,
    wrapper_len: usize,
) -> std::result::Result<usize, InputPreflightError> {
    let total_len = input_len
        .checked_add(wrapper_len)
        .ok_or(InputPreflightError::LengthOverflow)?;
    if total_len > MAX_SQL_INPUT_BYTES {
        return Err(InputPreflightError::TooLarge);
    }
    Ok(total_len)
}

fn preflight_input(
    sql: &str,
    wrapper_len: usize,
) -> std::result::Result<usize, InputPreflightError> {
    let total_len = checked_total_input_len(sql.len(), wrapper_len)?;
    if sql.as_bytes().contains(&0) {
        return Err(InputPreflightError::InteriorNul);
    }
    Ok(total_len)
}

fn parser_error_from_preflight(error: InputPreflightError) -> ParserError {
    match error {
        InputPreflightError::TooLarge | InputPreflightError::LengthOverflow => {
            input_too_large_error()
        }
        InputPreflightError::InteriorNul => interior_nul_error(),
    }
}

fn parser_error_from_ffi_input(error: nim_ffi::ParseInputError) -> ParserError {
    match error {
        nim_ffi::ParseInputError::LengthOutOfRange => input_too_large_error(),
        nim_ffi::ParseInputError::InteriorNul => interior_nul_error(),
    }
}

fn input_too_large_error() -> ParserError {
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: "SQL input at most 1048576 UTF-8 bytes".to_string(),
        found: "SQL input exceeds byte limit".to_string(),
    }
}

fn interior_nul_error() -> ParserError {
    ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: "valid SQL without interior NUL bytes".to_string(),
        found: "interior NUL byte".to_string(),
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

#[cfg(test)]
mod input_preflight_tests {
    use super::*;

    #[test]
    fn relabeled_v040_parser_is_rejected_by_exported_contract_before_decode() {
        // The value checked here is the library export, not CONTRACT_VERSION.
        // Rewriting a sidecar therefore cannot change this runtime decision.
        let error = ensure_linked_parser_contract("0.4.0")
            .expect_err("sidecar labels cannot make a pre-frame producer compatible");
        let rendered = error.to_string();

        assert!(rendered.contains("linked Nim parser contract 0.12.0"));
        assert!(rendered.contains("linked Nim parser contract 0.4.0"));
    }

    #[test]
    fn relabeled_v050_parser_is_rejected_by_exported_contract_before_decode() {
        let error = ensure_linked_parser_contract("0.5.0")
            .expect_err("a 0.5.0 producer cannot satisfy the current named-window contract");
        let rendered = error.to_string();

        assert!(rendered.contains("linked Nim parser contract 0.12.0"));
        assert!(rendered.contains("linked Nim parser contract 0.5.0"));
    }

    #[test]
    fn legacy_v040_consumer_rejects_the_current_producer_before_decode() {
        let linked_producer_contract = nim_ffi::parser_contract_version();
        assert_eq!(linked_producer_contract, "0.12.0");
        let error = ensure_parser_contract("0.4.0", &linked_producer_contract)
            .expect_err("legacy consumer must reject a producer with frame semantics");
        let rendered = error.to_string();

        assert!(rendered.contains("linked Nim parser contract 0.4.0"));
        assert!(rendered.contains("linked Nim parser contract 0.12.0"));
    }

    #[test]
    fn legacy_v050_consumer_rejects_a_v060_named_window_producer_before_decode() {
        let linked_producer_contract = nim_ffi::parser_contract_version();
        assert_eq!(linked_producer_contract, "0.12.0");
        let error = ensure_parser_contract("0.5.0", &linked_producer_contract)
            .expect_err("legacy consumer must not ignore QUALIFY or named-window fields");
        let rendered = error.to_string();

        assert!(rendered.contains("linked Nim parser contract 0.5.0"));
        assert!(rendered.contains("linked Nim parser contract 0.12.0"));
    }

    #[test]
    fn raw_sql_guard_accepts_boundary_minus_and_exact_but_rejects_plus() {
        assert_eq!(
            checked_total_input_len(MAX_SQL_INPUT_BYTES - 1, 0),
            Ok(MAX_SQL_INPUT_BYTES - 1)
        );
        assert_eq!(
            checked_total_input_len(MAX_SQL_INPUT_BYTES, 0),
            Ok(MAX_SQL_INPUT_BYTES)
        );
        assert_eq!(
            checked_total_input_len(MAX_SQL_INPUT_BYTES + 1, 0),
            Err(InputPreflightError::TooLarge)
        );
    }

    #[test]
    fn guard_counts_utf8_bytes_instead_of_characters() {
        let exact = "é".repeat(MAX_SQL_INPUT_BYTES / "é".len());
        assert_eq!(exact.chars().count(), MAX_SQL_INPUT_BYTES / 2);
        assert_eq!(exact.len(), MAX_SQL_INPUT_BYTES);
        assert_eq!(preflight_input(&exact, 0), Ok(MAX_SQL_INPUT_BYTES));

        let plus = format!("{exact}é");
        assert_eq!(
            preflight_input(&plus, 0),
            Err(InputPreflightError::TooLarge)
        );
    }

    #[test]
    fn expression_guard_includes_wrapper_and_detects_length_overflow() {
        assert_eq!(
            checked_total_input_len(
                MAX_SQL_INPUT_BYTES - SELECT_WRAPPER_PREFIX.len(),
                SELECT_WRAPPER_PREFIX.len(),
            ),
            Ok(MAX_SQL_INPUT_BYTES)
        );
        assert_eq!(
            checked_total_input_len(
                MAX_SQL_INPUT_BYTES - SELECT_WRAPPER_PREFIX.len() + 1,
                SELECT_WRAPPER_PREFIX.len(),
            ),
            Err(InputPreflightError::TooLarge)
        );
        assert_eq!(
            checked_total_input_len(usize::MAX, SELECT_WRAPPER_PREFIX.len()),
            Err(InputPreflightError::LengthOverflow)
        );
    }

    #[test]
    fn preflight_rejects_nul_before_any_ffi_work() {
        assert_eq!(
            preflight_input("SELECT \0 1", 0),
            Err(InputPreflightError::InteriorNul)
        );
        assert_eq!(
            preflight_input("1 \0 2", SELECT_WRAPPER_PREFIX.len()),
            Err(InputPreflightError::InteriorNul)
        );
    }
}
