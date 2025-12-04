use alopex_sql::{
    AlopexDialect, BinaryOp, Expr, ExprKind, Literal, ParserError, UnaryOp, parser::Parser,
};

fn parse(input: &str) -> Expr {
    Parser::parse_expression_sql(&AlopexDialect, input).expect("parse should succeed")
}

fn parse_err(input: &str) -> ParserError {
    Parser::parse_expression_sql(&AlopexDialect, input).expect_err("parse should fail")
}

#[test]
fn parses_literals_and_unary() {
    let num = parse("123");
    assert!(matches!(num.kind, ExprKind::Literal(Literal::Number(ref n)) if n == "123"));

    let boolean = parse("TRUE");
    assert!(matches!(
        boolean.kind,
        ExprKind::Literal(Literal::Boolean(true))
    ));

    let null = parse("NULL");
    assert!(matches!(null.kind, ExprKind::Literal(Literal::Null)));

    let neg = parse("-1");
    match neg.kind {
        ExprKind::UnaryOp {
            op: UnaryOp::Minus, ..
        } => {}
        other => panic!("expected unary minus, got {:?}", other),
    }
}

#[test]
fn respects_precedence_and_parentheses() {
    let expr = parse("1 + 2 * 3");
    match expr.kind {
        ExprKind::BinaryOp {
            op: BinaryOp::Add,
            right,
            ..
        } => match right.kind {
            ExprKind::BinaryOp {
                op: BinaryOp::Mul, ..
            } => {}
            other => panic!("expected multiply on right, got {:?}", other),
        },
        other => panic!("expected add root, got {:?}", other),
    }

    let grouped = parse("(1 + 2) * 3");
    match grouped.kind {
        ExprKind::BinaryOp {
            op: BinaryOp::Mul,
            left,
            ..
        } => match left.kind {
            ExprKind::BinaryOp {
                op: BinaryOp::Add, ..
            } => {}
            other => panic!("expected add inside parens, got {:?}", other),
        },
        other => panic!("expected mul root, got {:?}", other),
    }
}

#[test]
fn parses_between_like_in_and_isnull() {
    let between = parse("a BETWEEN 1 AND 2");
    match between.kind {
        ExprKind::Between { negated, .. } => assert!(!negated),
        other => panic!("expected between, got {:?}", other),
    }

    let not_between = parse("a NOT BETWEEN 1 AND 2");
    match not_between.kind {
        ExprKind::Between { negated, .. } => assert!(negated),
        other => panic!("expected not between, got {:?}", other),
    }

    let like = parse("a LIKE 'x' ESCAPE '!'");
    match like.kind {
        ExprKind::Like {
            negated, escape, ..
        } => {
            assert!(!negated);
            assert!(escape.is_some());
        }
        other => panic!("expected like, got {:?}", other),
    }

    let in_list = parse("a IN (1, 2)");
    match in_list.kind {
        ExprKind::InList {
            negated, ref list, ..
        } => {
            assert!(!negated);
            assert_eq!(list.len(), 2);
        }
        other => panic!("expected in list, got {:?}", other),
    }

    let not_in = parse("a NOT IN (1)");
    match not_in.kind {
        ExprKind::InList { negated, .. } => assert!(negated),
        other => panic!("expected not in, got {:?}", other),
    }

    let is_null = parse("a IS NULL");
    match is_null.kind {
        ExprKind::IsNull { negated, .. } => assert!(!negated),
        other => panic!("expected is null, got {:?}", other),
    }

    let is_not_null = parse("a IS NOT NULL");
    match is_not_null.kind {
        ExprKind::IsNull { negated, .. } => assert!(negated),
        other => panic!("expected is not null, got {:?}", other),
    }
}

#[test]
fn parses_function_and_vector_literal() {
    let func = parse("foo(1, 2)");
    match func.kind {
        ExprKind::FunctionCall { ref name, ref args } => {
            assert_eq!(name, "foo");
            assert_eq!(args.len(), 2);
        }
        other => panic!("expected function call, got {:?}", other),
    }

    let vec = parse("[1.0, 2.0]");
    match vec.kind {
        ExprKind::VectorLiteral(ref v) => {
            assert_eq!(v, &vec![1.0, 2.0]);
        }
        other => panic!("expected vector literal, got {:?}", other),
    }
}

#[test]
fn vector_literal_allows_negative_numbers() {
    let vec = parse("[-1.0, 2.0]");
    match vec.kind {
        ExprKind::VectorLiteral(ref v) => {
            assert_eq!(v, &vec![-1.0, 2.0]);
        }
        other => panic!("expected vector literal, got {:?}", other),
    }
}

#[test]
fn reserved_keywords_are_not_identifiers() {
    let err = parse_err("SELECT");
    match err {
        ParserError::UnexpectedToken { .. } => {}
        other => panic!("expected unexpected token, got {:?}", other),
    }
}

#[test]
fn leftover_tokens_are_rejected() {
    let err = Parser::parse_expression_sql(&AlopexDialect, "1 2").unwrap_err();
    match err {
        ParserError::UnexpectedToken { .. } | ParserError::ExpectedToken { .. } => {}
        other => panic!("expected token error, got {:?}", other),
    }
}

#[test]
fn recursion_limit_errors() {
    // Build a deeply nested expression to exceed a small recursion limit.
    let dialect = AlopexDialect;
    let sql = "((((((((((((((((((((((((((((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))))))))))))))))))))))))))))";
    let tokens = alopex_sql::Tokenizer::new(&dialect, sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::with_recursion_limit(&dialect, tokens, 5);
    let err = parser.parse_expr().unwrap_err();
    match err {
        // 深さは「上限+1」(溢れた時点の実際の深さ)を報告することを期待する
        ParserError::RecursionLimitExceeded { depth } => assert_eq!(depth, 6),
        other => panic!("expected recursion limit error, got {:?}", other),
    }
}
