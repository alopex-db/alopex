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

#[test]
fn nul_byte_input_returns_parser_error_without_panic() {
    let err = Parser::parse_sql(&AlopexDialect, "\0\x03J").expect_err("invalid SQL");
    match err {
        ParserError::UnexpectedToken {
            expected, found, ..
        } => {
            assert!(expected.contains("without interior NUL"));
            assert_eq!(found, "interior NUL byte");
        }
        other => panic!("expected ParserError::UnexpectedToken, got {other:?}"),
    }
}
