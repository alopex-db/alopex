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
fn parse_failure_does_not_poison_subsequent_parses() {
    // issue #40: 非 ParseError 例外 (ここでは parseBiggestInt の桁あふれによる
    // ValueError) が FFI から漏れると、--exceptions:goto のスレッドローカルな
    // エラーフラグが立ったままになり、同一スレッドの後続呼び出しがゼロ初期化の
    // CParseResult (prkOk + 空バッファ = "failed to fill whole buffer") で
    // 巻き込まれて失敗する (desync)。
    let err = Parser::parse_sql(&AlopexDialect, "SELECT 99999999999999999999999999")
        .expect_err("oversized integer literal should fail");
    // ValueError は Nim 側の `except CatchableError` で捕捉される通常の
    // prkError 経路であり、nim_bridge.rs の「空 payload 検知」(防御的
    // フォールバック、"empty payload from Nim parser" を含むメッセージ) を
    // 経由してはならない。空 payload 経路を通っていたら、それは Nim 側の
    // except 節が機能していない (＝この修正が壊れている) ことを意味する。
    let message = err.to_string();
    assert!(
        !message.contains("empty payload from Nim parser"),
        "oversized integer literal must produce a clean prkError from Nim, \
         not fall through the Rust-side empty-payload defensive path; got: {message}"
    );
    for i in 0..3 {
        Parser::parse_sql(&AlopexDialect, "SELECT 1")
            .unwrap_or_else(|err| panic!("parse #{i} after failure must succeed, got {err}"));
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
