#![cfg(target_os = "linux")]

use alopex_sql::{
    AlopexDialect, DataType, ExprKind, FromItem, InsertSource, JoinType, Literal, Parser,
    ParserError, SelectItem, StatementKind, VectorMetric, parser_contract_version,
};

const MAX_SQL_INPUT_BYTES: usize = 1_048_576;

fn padded_sql(total_bytes: usize) -> String {
    const STATEMENT: &str = "SELECT 1";
    assert!(total_bytes >= STATEMENT.len());
    format!("{}{STATEMENT}", " ".repeat(total_bytes - STATEMENT.len()))
}

fn padded_expression(total_bytes: usize) -> String {
    const EXPRESSION: &str = "1";
    assert!(total_bytes >= EXPRESSION.len());
    format!("{}{EXPRESSION}", " ".repeat(total_bytes - EXPRESSION.len()))
}

fn assert_input_too_large(error: ParserError) {
    let ParserError::UnexpectedToken {
        line,
        column,
        expected,
        found,
    } = error
    else {
        panic!("expected bounded input error, got {error:?}");
    };
    assert_eq!((line, column), (0, 0));
    assert_eq!(expected, "SQL input at most 1048576 UTF-8 bytes");
    assert_eq!(found, "SQL input exceeds byte limit");
}

#[test]
fn public_sql_boundary_accepts_minus_and_exact_then_rejects_plus() {
    for total_bytes in [MAX_SQL_INPUT_BYTES - 1, MAX_SQL_INPUT_BYTES] {
        let sql = padded_sql(total_bytes);
        assert_eq!(sql.len(), total_bytes);
        let statements = Parser::parse_sql(&AlopexDialect, &sql)
            .unwrap_or_else(|error| panic!("{total_bytes}-byte SQL should pass guard: {error}"));
        assert_eq!(statements.len(), 1);
    }

    let plus = padded_sql(MAX_SQL_INPUT_BYTES + 1);
    assert_input_too_large(
        Parser::parse_sql(&AlopexDialect, &plus).expect_err("limit plus one must be rejected"),
    );
}

#[test]
fn public_expression_boundary_accounts_for_select_wrapper_before_allocation() {
    const WRAPPER_BYTES: usize = "SELECT ".len();
    let exact = padded_expression(MAX_SQL_INPUT_BYTES - WRAPPER_BYTES);
    assert_eq!(exact.len() + WRAPPER_BYTES, MAX_SQL_INPUT_BYTES);
    let expression = Parser::parse_expression_sql(&AlopexDialect, &exact)
        .expect("wrapped exact-limit expression should pass guard and parse");
    assert!(matches!(
        expression.kind,
        ExprKind::Literal {
            literal: Literal::Number(ref value)
        } if value == "1"
    ));

    let plus = padded_expression(MAX_SQL_INPUT_BYTES - WRAPPER_BYTES + 1);
    assert_input_too_large(
        Parser::parse_expression_sql(&AlopexDialect, &plus)
            .expect_err("wrapper-adjusted limit plus one must be rejected"),
    );
}

#[test]
fn public_nul_guard_and_normal_sql_behavior_are_stable() {
    let nul = Parser::parse_sql(&AlopexDialect, "SELECT \0 1")
        .expect_err("interior NUL must be rejected without calling FFI");
    let ParserError::UnexpectedToken {
        expected, found, ..
    } = nul
    else {
        panic!("expected bounded NUL error, got {nul:?}");
    };
    assert_eq!(expected, "valid SQL without interior NUL bytes");
    assert_eq!(found, "interior NUL byte");

    let statements =
        Parser::parse_sql(&AlopexDialect, "SELECT 1").expect("ordinary SQL must remain unchanged");
    assert_eq!(statements.len(), 1);
}

#[test]
fn exposes_the_nim_wire_contract_version() {
    assert_eq!(parser_contract_version(), "0.3.0");
}

#[test]
fn parses_sql_ts_interval_as_a_distinct_literal() {
    let statements = Parser::parse_sql(&AlopexDialect, "SELECT NOW() - INTERVAL '24 hours'")
        .expect("SQL-TS INTERVAL should cross the Nim MessagePack boundary");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected Select");
    };
    assert!(matches!(
        &select.projection[0],
        SelectItem::Expr {
            expr:
                alopex_sql::Expr {
                    kind: ExprKind::BinaryOp { right, .. },
                    ..
                },
            ..
        } if matches!(
            &right.kind,
            ExprKind::Literal {
                literal: Literal::Interval(value)
            } if value == "24 hours"
        )
    ));
}

#[test]
fn parse_join_subquery_and_vector_variants_from_nim() {
    let sql = "\
        CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR(3, COSINE));\
        SELECT d.id, [0.1, 0.2, 0.3] FROM docs d INNER JOIN tags t ON d.id = t.doc_id \
        WHERE EXISTS (SELECT 1 FROM tags) AND d.id IN (SELECT doc_id FROM tags) \
        ORDER BY d.id NULLS LAST LIMIT 10";

    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("Nim parser should parse SQL");
    assert_eq!(statements.len(), 2);

    match &statements[0].kind {
        StatementKind::CreateTable(table) => {
            assert_eq!(table.name, "docs");
            assert!(matches!(
                table.columns[1].data_type,
                DataType::Vector {
                    dimension: 3,
                    metric: Some(VectorMetric::Cosine)
                }
            ));
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }

    let StatementKind::Select(select) = &statements[1].kind else {
        panic!("expected Select");
    };
    assert!(matches!(
        &select.from[0],
        FromItem::Join {
            join_type: JoinType::Inner,
            condition: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &select.projection[1],
        SelectItem::Expr {
            expr:
                alopex_sql::Expr {
                    kind: ExprKind::VectorLiteral { values },
                    ..
                },
            ..
        } if values == &vec![0.1, 0.2, 0.3]
    ));
    assert!(matches!(
        select.selection.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::BinaryOp { .. })
    ));
    assert!(matches!(
        select.limit.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::Literal {
            literal: Literal::Number(value)
        }) if value == "10"
    ));
}

#[test]
fn parse_multi_row_insert_without_column_list_from_nim() {
    // issue #40: 「カラムリスト省略 × 多行 VALUES」で先頭行が列リストと
    // 誤判別され、FieldDefect が FFI から漏れて ALOPEX-P001 になっていた。
    let statements = Parser::parse_sql(&AlopexDialect, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
        .expect("multi-row INSERT without column list should parse");
    assert_eq!(statements.len(), 1);
    let StatementKind::Insert(insert) = &statements[0].kind else {
        panic!("expected Insert, got {:?}", statements[0].kind);
    };
    assert_eq!(insert.table, "t1");
    assert!(insert.columns.is_none());
    let InsertSource::Values { values } = &insert.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].len(), 2);
    assert!(matches!(
        &values[0][0].kind,
        ExprKind::Literal {
            literal: Literal::Number(value)
        } if value == "1"
    ));
    assert!(matches!(
        &values[1][1].kind,
        ExprKind::Literal {
            literal: Literal::String(value)
        } if value == "b"
    ));
}

#[test]
fn parse_multi_row_all_string_insert_without_column_list_from_nim() {
    // issue #40: 先頭行が全て文字列だと例外にならず、先頭行が列リストへ
    // 静かに誤変換される (columns = ["a","b"], values = [["c","d"]])。
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "INSERT INTO t1 VALUES ('a', 'b'), ('c', 'd')",
    )
    .expect("all-string multi-row INSERT should parse");
    let StatementKind::Insert(insert) = &statements[0].kind else {
        panic!("expected Insert, got {:?}", statements[0].kind);
    };
    assert!(insert.columns.is_none());
    let InsertSource::Values { values } = &insert.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(values.len(), 2);
    assert!(matches!(
        &values[0][0].kind,
        ExprKind::Literal {
            literal: Literal::String(value)
        } if value == "a"
    ));
}

#[test]
fn parse_derived_and_quantified_subqueries_from_nim() {
    let sql = "\
        SELECT x.id FROM (SELECT id FROM docs) x \
        WHERE x.id > ANY (SELECT doc_id FROM tags)";
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("Nim parser should parse SQL");
    let StatementKind::Select(select) = &statements[0].kind else {
        panic!("expected Select");
    };
    assert!(
        matches!(&select.from[0], FromItem::Derived { alias: Some(alias), .. } if alias == "x")
    );
    assert!(matches!(
        select.selection.as_ref().map(|expr| &expr.kind),
        Some(ExprKind::Quantified { .. })
    ));
}
