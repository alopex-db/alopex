#![cfg(target_os = "linux")]

use alopex_sql::{AlopexDialect, Parser, ParserError};

#[test]
fn invalid_sql_returns_parser_error_with_span() {
    let err = Parser::parse_sql(&AlopexDialect, "SELECT FROM").expect_err("invalid SQL");
    match err {
        ParserError::UnexpectedToken {
            line,
            column,
            found,
            ..
        } => {
            assert_eq!(line, 1);
            assert!(column > 0);
            assert!(found.contains("Parse error"));
        }
        other => panic!("expected ParserError::UnexpectedToken, got {other:?}"),
    }
}
