#![cfg(target_os = "linux")]

use alopex_sql::{
    AlopexDialect, DataType, ExprKind, FromItem, JoinType, Literal, Parser, SelectItem,
    StatementKind, VectorMetric,
};

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
