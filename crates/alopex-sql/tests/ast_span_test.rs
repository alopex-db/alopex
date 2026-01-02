use alopex_sql::{
    Assignment, CreateIndex, CreateTable, Delete, Insert, OrderByExpr, Select, SelectItem, Span,
    Spanned, Statement, StatementKind, TableRef, Update,
};

fn span(line: u64, col: u64) -> Span {
    Span::new(
        alopex_sql::Location::new(line, col),
        alopex_sql::Location::new(line, col + 1),
    )
}

#[test]
fn ddl_nodes_carry_spans() {
    let tbl = CreateTable {
        if_not_exists: true,
        name: "t".into(),
        columns: vec![],
        constraints: vec![],
        with_options: vec![],
        span: span(1, 1),
    };
    assert_eq!(tbl.span().start.line, 1);

    let idx = CreateIndex {
        if_not_exists: false,
        name: "idx".into(),
        table: "t".into(),
        column: "c".into(),
        method: None,
        options: vec![],
        span: span(2, 1),
    };
    assert_eq!(idx.span().start.line, 2);
}

#[test]
fn dml_nodes_carry_spans() {
    let select = Select {
        distinct: false,
        projection: vec![SelectItem::Wildcard { span: span(1, 1) }],
        from: TableRef {
            name: "t".into(),
            alias: None,
            span: span(1, 10),
        },
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: alopex_sql::Expr::new(
                alopex_sql::ExprKind::Literal(alopex_sql::Literal::Number("1".into())),
                span(1, 20),
            ),
            asc: Some(true),
            nulls_first: None,
            span: span(1, 20),
        }],
        limit: None,
        offset: None,
        span: span(1, 1),
    };
    assert_eq!(select.span().start.column, 1);

    let insert = Insert {
        table: "t".into(),
        columns: None,
        values: vec![],
        span: span(2, 1),
    };
    assert_eq!(insert.span().start.line, 2);

    let update = Update {
        table: "t".into(),
        assignments: vec![Assignment {
            column: "c".into(),
            value: alopex_sql::Expr::new(
                alopex_sql::ExprKind::Literal(alopex_sql::Literal::Null),
                span(3, 10),
            ),
            span: span(3, 5),
        }],
        selection: None,
        span: span(3, 1),
    };
    assert_eq!(update.span().start.line, 3);

    let delete = Delete {
        table: "t".into(),
        selection: None,
        span: span(4, 1),
    };
    assert_eq!(delete.span().start.line, 4);

    let stmt = Statement {
        kind: StatementKind::Select(select),
        span: span(1, 1),
    };
    assert_eq!(stmt.span().start.line, 1);
}
