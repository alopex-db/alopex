use alopex_sql::{AlopexDialect, ExprKind, ParserError, SelectItem, Tokenizer, parser::Parser};

fn parse_with<F, T>(sql: &str, f: F) -> T
where
    F: FnOnce(&mut Parser<'_>) -> T,
{
    let dialect = AlopexDialect;
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
    let mut parser = Parser::new(&dialect, tokens);
    f(&mut parser)
}

#[test]
fn parse_select_with_clauses() {
    let select = parse_with(
        "SELECT DISTINCT id, name AS user_name, score FROM users u WHERE score > 10 ORDER BY created_at DESC NULLS LAST, id ASC LIMIT 5 OFFSET 10",
        |p| p.parse_select().unwrap(),
    );

    assert!(select.distinct);
    assert_eq!(select.projection.len(), 3);
    match &select.projection[1] {
        SelectItem::Expr { alias, .. } => assert_eq!(alias.as_deref(), Some("user_name")),
        other => panic!("unexpected projection {:?}", other),
    }
    assert_eq!(select.from.name, "users");
    assert_eq!(select.from.alias.as_deref(), Some("u"));
    assert!(select.selection.is_some());
    assert_eq!(select.order_by.len(), 2);
    assert_eq!(select.order_by[0].asc, Some(false));
    assert_eq!(select.order_by[0].nulls_first, Some(false));
    assert_eq!(select.order_by[1].asc, Some(true));
    assert_eq!(select.order_by[1].nulls_first, None);
    assert!(select.limit.is_some());
    assert!(select.offset.is_some());
}

#[test]
fn select_requires_from() {
    let err = parse_with("SELECT id", |p| p.parse_select().unwrap_err());
    match err {
        ParserError::ExpectedToken { expected, .. } => assert_eq!(expected, "FROM"),
        other => panic!("unexpected error {:?}", other),
    }
}

#[test]
fn parse_select_wildcard_and_alias_without_as() {
    let select = parse_with("SELECT *, score s FROM docs", |p| p.parse_select().unwrap());
    assert!(matches!(select.projection[0], SelectItem::Wildcard { .. }));
    match &select.projection[1] {
        SelectItem::Expr { alias, .. } => assert_eq!(alias.as_deref(), Some("s")),
        other => panic!("unexpected projection {:?}", other),
    }
}

#[test]
fn parse_insert_with_vectors_and_multiple_rows() {
    let insert = parse_with(
        "INSERT INTO documents (id, title, embedding) VALUES (1, 'doc', [0.1, 0.2]), (2, 'doc2', [0.3, -0.4])",
        |p| p.parse_insert().unwrap(),
    );

    assert_eq!(insert.table, "documents");
    assert_eq!(
        insert.columns.as_ref().unwrap(),
        &vec![
            "id".to_string(),
            "title".to_string(),
            "embedding".to_string()
        ]
    );
    assert_eq!(insert.values.len(), 2);
    assert_eq!(insert.values[0].len(), 3);
    assert!(matches!(
        insert.values[0][2].kind,
        ExprKind::VectorLiteral(_)
    ));
}

#[test]
fn parse_update_with_multiple_assignments() {
    let update = parse_with(
        "UPDATE users SET name = 'Alice', score = score + 1 WHERE id = 1",
        |p| p.parse_update().unwrap(),
    );

    assert_eq!(update.table, "users");
    assert_eq!(update.assignments.len(), 2);
    assert!(update.selection.is_some());
}

#[test]
fn parse_delete_with_and_without_where() {
    let delete_with = parse_with("DELETE FROM sessions WHERE expires_at < 100", |p| {
        p.parse_delete().unwrap()
    });
    assert_eq!(delete_with.table, "sessions");
    assert!(delete_with.selection.is_some());

    let delete_all = parse_with("DELETE FROM logs", |p| p.parse_delete().unwrap());
    assert_eq!(delete_all.table, "logs");
    assert!(delete_all.selection.is_none());
}

#[test]
fn insert_values_cannot_be_empty() {
    let err = parse_with("INSERT INTO t VALUES ()", |p| p.parse_insert().unwrap_err());
    match err {
        ParserError::ExpectedToken { expected, .. } => assert_eq!(expected, "expression"),
        ParserError::UnexpectedToken { .. } => {}
        other => panic!("unexpected error {:?}", other),
    }
}
