use alopex_sql::{AlopexDialect, ExprKind, Parser, ParserError, StatementKind};

#[test]
fn parses_multiple_statements() {
    let sql = "CREATE TABLE docs (id INT); INSERT INTO docs (id) VALUES (1); SELECT * FROM docs;";
    let stmts = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0].kind, StatementKind::CreateTable(_)));
    assert!(matches!(stmts[1].kind, StatementKind::Insert(_)));
    assert!(matches!(stmts[2].kind, StatementKind::Select(_)));
}

#[test]
fn vector_literal_parses_via_dialect_prefix() {
    let sql = "INSERT INTO docs VALUES ([0.1, -0.2])";
    let stmts = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    match &stmts[0].kind {
        StatementKind::Insert(ins) => {
            assert_eq!(ins.values.len(), 1);
            match ins.values[0][0].kind {
                ExprKind::VectorLiteral(ref v) => assert_eq!(v, &vec![0.1, -0.2]),
                ref other => panic!("expected vector literal, got {:?}", other),
            }
        }
        other => panic!("expected insert, got {:?}", other),
    }
}

#[test]
fn reports_error_positions() {
    let err = Parser::parse_sql(&AlopexDialect, "SELECT 1").unwrap_err();
    match err {
        ParserError::ExpectedToken {
            expected,
            line,
            column,
            ..
        } => {
            assert_eq!(expected, "FROM");
            assert_eq!(line, 1);
            assert_eq!(column, 9);
        }
        other => panic!("unexpected error {:?}", other),
    }
}
