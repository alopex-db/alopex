use alopex_sql::{AlopexDialect, ExprKind, LITERAL_TABLE, Parser, StatementKind};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parses_multiple_statements() {
    let sql = "CREATE TABLE docs (id INT); INSERT INTO docs (id) VALUES (1); SELECT * FROM docs;";
    let stmts = Parser::parse_sql(&AlopexDialect, sql).unwrap();
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0].kind, StatementKind::CreateTable(_)));
    assert!(matches!(stmts[1].kind, StatementKind::Insert(_)));
    assert!(matches!(stmts[2].kind, StatementKind::Select(_)));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parses_select_without_from_single_literal() {
    let stmts = Parser::parse_sql(&AlopexDialect, "SELECT 1").unwrap();
    match &stmts[0].kind {
        StatementKind::Select(select) => {
            assert_eq!(select.from.name, LITERAL_TABLE);
            assert_eq!(select.projection.len(), 1);
        }
        other => panic!("expected select, got {:?}", other),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parses_select_without_from_multiple_literals() {
    let stmts = Parser::parse_sql(&AlopexDialect, "SELECT 1, 'ok'").unwrap();
    match &stmts[0].kind {
        StatementKind::Select(select) => {
            assert_eq!(select.from.name, LITERAL_TABLE);
            assert_eq!(select.projection.len(), 2);
        }
        other => panic!("expected select, got {:?}", other),
    }
}
