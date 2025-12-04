use alopex_sql::{AlopexDialect, Keyword, Location, ParserError, Token, TokenWithSpan, Tokenizer};

fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
    let dialect = AlopexDialect;
    Tokenizer::new(&dialect, sql).tokenize()
}

#[test]
fn keywords_are_case_insensitive() {
    let tokens = tokenize(
        "select FROM Where hnsw BTREE constraint ESCAPE CAST Case WHEN then ELSE end now int bool",
    )
    .unwrap();
    let expected = [
        Keyword::SELECT,
        Keyword::FROM,
        Keyword::WHERE,
        Keyword::HNSW,
        Keyword::BTREE,
        Keyword::CONSTRAINT,
        Keyword::ESCAPE,
        Keyword::CAST,
        Keyword::CASE,
        Keyword::WHEN,
        Keyword::THEN,
        Keyword::ELSE,
        Keyword::END,
        Keyword::NOW,
        Keyword::INT,
        Keyword::BOOL,
    ];

    for (idx, kw) in expected.iter().enumerate() {
        let word = match &tokens[idx].token {
            Token::Word(w) => w,
            _ => panic!("expected word"),
        };
        assert_eq!(word.keyword, *kw);
    }
    assert!(matches!(tokens.last().unwrap().token, Token::EOF));
}

#[test]
fn identifiers_preserve_original_case() {
    let tokens = tokenize("foo _Bar1").unwrap();
    let first = match &tokens[0].token {
        Token::Word(w) => w,
        _ => panic!("expected word"),
    };
    assert_eq!(first.value, "foo");
    assert_eq!(first.keyword, Keyword::NoKeyword);

    let second = match &tokens[1].token {
        Token::Word(w) => w,
        _ => panic!("expected word"),
    };
    assert_eq!(second.value, "_Bar1");
    assert_eq!(second.keyword, Keyword::NoKeyword);
}

#[test]
fn numbers_and_floats_and_exponents() {
    let tokens = tokenize("123 45.67 1.2e3").unwrap();
    assert_eq!(tokens[0].token, Token::Number("123".into()));
    assert_eq!(tokens[1].token, Token::Number("45.67".into()));
    assert_eq!(tokens[2].token, Token::Number("1.2e3".into()));
}

#[test]
fn invalid_number_reports_error() {
    let err = tokenize("1.2.3").unwrap_err();
    match err {
        ParserError::InvalidNumber {
            value,
            line,
            column,
        } => {
            assert_eq!(value, "1.2.3");
            assert_eq!((line, column), (1, 1));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn strings_handle_escaping_and_unterminated() {
    let tokens = tokenize("'it''s'").unwrap();
    assert_eq!(tokens[0].token, Token::SingleQuotedString("it's".into()));

    let err = tokenize("'unterminated").unwrap_err();
    assert!(matches!(
        err,
        ParserError::UnterminatedString { line: 1, column: 1 }
    ));
}

#[test]
fn operators_and_punctuation() {
    let tokens = tokenize("+ - * / % = <> <= >= < > != || ( ) [ ] , ; .").unwrap();
    let kinds: Vec<&Token> = tokens.iter().map(|t| &t.token).collect();
    assert_eq!(
        kinds[..18],
        [
            &Token::Plus,
            &Token::Minus,
            &Token::Mul,
            &Token::Div,
            &Token::Mod,
            &Token::Eq,
            &Token::Neq,
            &Token::LtEq,
            &Token::GtEq,
            &Token::Lt,
            &Token::Gt,
            &Token::Neq,
            &Token::StringConcat,
            &Token::LParen,
            &Token::RParen,
            &Token::LBracket,
            &Token::RBracket,
            &Token::Comma
        ]
    );
    assert_eq!(kinds[18], &Token::SemiColon);
    assert_eq!(kinds[19], &Token::Period);
}

#[test]
fn spans_track_positions_with_comments_and_newlines() {
    let tokens = tokenize("SELECT\n-- comment\nFROM").unwrap();

    let select_span = &tokens[0].span;
    assert_eq!(select_span.start, Location::new(1, 1));
    assert_eq!(select_span.end, Location::new(1, 6));

    let from_span = &tokens[1].span;
    assert_eq!(from_span.start, Location::new(3, 1));
    assert_eq!(from_span.end, Location::new(3, 4));
}
