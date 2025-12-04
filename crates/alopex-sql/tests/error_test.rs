use alopex_sql::ParserError;

#[test]
fn unexpected_token_display_matches_spec() {
    let err = ParserError::UnexpectedToken {
        line: 1,
        column: 2,
        expected: "identifier".into(),
        found: "(".into(),
    };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P001]: unexpected token at line 1, column 2: expected identifier, found ("
    );
}

#[test]
fn expected_token_display_matches_spec() {
    let err = ParserError::ExpectedToken {
        line: 3,
        column: 4,
        expected: "SELECT".into(),
        found: "EOF".into(),
    };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P002]: expected SELECT but found EOF at line 3, column 4"
    );
}

#[test]
fn unterminated_string_display_matches_spec() {
    let err = ParserError::UnterminatedString { line: 5, column: 6 };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P003]: unterminated string literal starting at line 5, column 6"
    );
}

#[test]
fn invalid_number_display_matches_spec() {
    let err = ParserError::InvalidNumber {
        line: 7,
        column: 8,
        value: "1.2.3".into(),
    };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P004]: invalid number literal '1.2.3' at line 7, column 8"
    );
}

#[test]
fn invalid_vector_display_matches_spec() {
    let err = ParserError::InvalidVector {
        line: 9,
        column: 10,
    };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P005]: invalid vector literal at line 9, column 10"
    );
}

#[test]
fn recursion_limit_exceeded_display_matches_spec() {
    let err = ParserError::RecursionLimitExceeded { depth: 42 };

    assert_eq!(
        err.to_string(),
        "error[ALOPEX-P006]: recursion limit exceeded (depth: 42)"
    );
}
