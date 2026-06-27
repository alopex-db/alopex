module []

## SQL Parser Tests for Alopex DB (Roc implementation)
## Ported from the Rust test suite.
##
## Covers:
##   - Tokenizer: keywords, identifiers, numbers, floats, string escaping, operators
##   - Expressions: literals, unary minus, precedence, parentheses, BETWEEN,
##                  IN, IS NULL / IS NOT NULL, function calls, column references
##   - DML: SELECT (WHERE, ORDER BY, LIMIT), INSERT, UPDATE, DELETE

import Lexer exposing [tokenize, Token, TokenKind]
import Parser exposing [parse]
import Ast exposing [SqlNode, BinaryOp, UnaryOp]

# ===========================================================================
# Helper functions
# ===========================================================================

## Extract just the token kinds from a token list (drop EOF sentinel).
tokenKinds : List Token -> List TokenKind
tokenKinds = |tokens|
    tokens
    |> List.drop_last(1)    # remove TkEof
    |> List.map(|t| t.kind)

## Extract just the token values from a token list (drop EOF sentinel).
tokenValues : List Token -> List Str
tokenValues = |tokens|
    tokens
    |> List.drop_last(1)
    |> List.map(|t| t.value)

## Tokenize and expect success; panics (expect failure) on lex error.
mustTokenize : Str -> List Token
mustTokenize = |sql|
    when tokenize(sql) is
        Ok(tokens) -> tokens
        Err(_) -> []    # unreachable in well-formed tests; expect will fail below

## Parse and expect success.
mustParse : Str -> SqlNode
mustParse = |sql|
    when tokenize(sql) is
        Ok(tokens) ->
            when parse(tokens) is
                Ok(node) -> node
                Err(_) -> NullLit   # unreachable; expect will fail
        Err(_) -> NullLit

# ===========================================================================
# TOKENIZER TESTS
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. Keywords are case-insensitive
# ---------------------------------------------------------------------------

## "select" and "SELECT" and "Select" all produce TkSelect
expect
    lower = mustTokenize("select")
    tokenKinds(lower) == [TkSelect]

expect
    upper = mustTokenize("SELECT")
    tokenKinds(upper) == [TkSelect]

expect
    mixed = mustTokenize("Select")
    tokenKinds(mixed) == [TkSelect]

expect
    lower2 = mustTokenize("from")
    tokenKinds(lower2) == [TkFrom]

expect
    upper2 = mustTokenize("FROM")
    tokenKinds(upper2) == [TkFrom]

expect
    kws = mustTokenize("WHERE AND OR NOT INSERT INTO VALUES UPDATE SET DELETE")
    tokenKinds(kws) == [TkWhere, TkAnd, TkOr, TkNot, TkInsert, TkInto, TkValues, TkUpdate, TkSet, TkDelete]

# ---------------------------------------------------------------------------
# 2. Identifiers preserve original case
# ---------------------------------------------------------------------------

expect
    toks = mustTokenize("myTable")
    tokenKinds(toks) == [TkIdent]

expect
    toks = mustTokenize("myTable")
    tokenValues(toks) == ["myTable"]

expect
    toks = mustTokenize("MyCol")
    tokenValues(toks) == ["MyCol"]

expect
    toks = mustTokenize("USER_ID")
    tokenValues(toks) == ["USER_ID"]

# ---------------------------------------------------------------------------
# 3. Numbers and floats
# ---------------------------------------------------------------------------

expect
    toks = mustTokenize("42")
    tokenKinds(toks) == [TkInteger]

expect
    toks = mustTokenize("42")
    tokenValues(toks) == ["42"]

expect
    toks = mustTokenize("3.14")
    tokenKinds(toks) == [TkFloat]

expect
    toks = mustTokenize("3.14")
    tokenValues(toks) == ["3.14"]

expect
    toks = mustTokenize("0")
    tokenKinds(toks) == [TkInteger]

expect
    toks = mustTokenize("100.0")
    tokenKinds(toks) == [TkFloat]

# ---------------------------------------------------------------------------
# 4. String escaping: 'it''s' -> it's
# ---------------------------------------------------------------------------

expect
    toks = mustTokenize("'it''s'")
    tokenKinds(toks) == [TkString]

expect
    toks = mustTokenize("'it''s'")
    tokenValues(toks) == ["it's"]

expect
    toks = mustTokenize("'hello'")
    tokenValues(toks) == ["hello"]

expect
    toks = mustTokenize("''")
    tokenValues(toks) == [""]

# ---------------------------------------------------------------------------
# 5. All operators
# ---------------------------------------------------------------------------

expect
    toks = mustTokenize("=")
    tokenKinds(toks) == [TkEq]

expect
    toks = mustTokenize("<>")
    tokenKinds(toks) == [TkNeq]

expect
    toks = mustTokenize("!=")
    tokenKinds(toks) == [TkNeq]

expect
    toks = mustTokenize("<")
    tokenKinds(toks) == [TkLt]

expect
    toks = mustTokenize("<=")
    tokenKinds(toks) == [TkLe]

expect
    toks = mustTokenize(">")
    tokenKinds(toks) == [TkGt]

expect
    toks = mustTokenize(">=")
    tokenKinds(toks) == [TkGe]

expect
    toks = mustTokenize("+ - * / %")
    tokenKinds(toks) == [TkPlus, TkMinus, TkStar, TkSlash, TkPercent]

expect
    toks = mustTokenize(", . ; ( )")
    tokenKinds(toks) == [TkComma, TkDot, TkSemicolon, TkLParen, TkRParen]

# ===========================================================================
# EXPRESSION TESTS
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. Integer literal
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT 42")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(IntLit(42)) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 2. String literal
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT 'hello'")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(StrLit("hello")) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 3. Boolean literals: TRUE and FALSE
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT TRUE")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(BoolLit(Bool.true)) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

expect
    node = mustParse("SELECT FALSE")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(BoolLit(Bool.false)) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 4. NULL literal
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT NULL")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(NullLit) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 5. Unary minus
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT -1")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(UnOp({ op: Neg, operand: IntLit(1) })) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 6. Operator precedence: 1 + 2 * 3 = (1 + (2 * 3))
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT 1 + 2 * 3")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(BinOp({ op: Add, left: IntLit(1), right: BinOp({ op: Mul, left: IntLit(2), right: IntLit(3) }) })) ->
                    Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 7. Parentheses override precedence: (1 + 2) * 3
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT (1 + 2) * 3")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(BinOp({ op: Mul, left: BinOp({ op: Add, left: IntLit(1), right: IntLit(2) }), right: IntLit(3) })) ->
                    Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 8. BETWEEN (simplified): x BETWEEN 1 AND 10
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT x FROM t WHERE x BETWEEN 1 AND 10")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when whereExpr is
                BinOp({ op: Between, left: Ident("x"), right: NodeList([IntLit(1), IntLit(10)]) }) ->
                    Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 9. IN list: x IN (1, 2, 3)
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT x FROM t WHERE x IN (1, 2, 3)")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when whereExpr is
                BinOp({ op: In, left: Ident("x"), right: NodeList(items) }) ->
                    items == [IntLit(1), IntLit(2), IntLit(3)]
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 10. IS NULL
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT x FROM t WHERE x IS NULL")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when whereExpr is
                UnOp({ op: IsNull, operand: Ident("x") }) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 11. IS NOT NULL
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT x FROM t WHERE x IS NOT NULL")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when whereExpr is
                UnOp({ op: IsNotNull, operand: Ident("x") }) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 12. Function calls
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT COUNT(*) FROM t")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(FnCall({ name: "COUNT", args: [StarLit] })) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

expect
    node = mustParse("SELECT MAX(age) FROM t")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(FnCall({ name: "MAX", args: [Ident("age")] })) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

expect
    node = mustParse("SELECT COALESCE(a, b) FROM t")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(FnCall({ name: "COALESCE", args: [Ident("a"), Ident("b")] })) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 13. Column references (table.column)
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT users.id FROM users")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(ColRef({ table: "users", column: "id" })) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ===========================================================================
# DML TESTS
# ===========================================================================

# ---------------------------------------------------------------------------
# 1. SELECT with WHERE, ORDER BY, LIMIT
# ---------------------------------------------------------------------------

expect
    node = mustParse("SELECT id, name FROM users WHERE age > 18")
    when node is
        SelectStmt({ columns, from, where: Some(_), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            List.len(columns) == 2 && List.len(from) == 1
        _ -> Bool.false

expect
    node = mustParse("SELECT id, name FROM users WHERE age > 18")
    when node is
        SelectStmt({ columns: _, from, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            fromOk = when List.first(from) is
                Ok(Ident("users")) -> Bool.true
                _ -> Bool.false
            whereOk = when whereExpr is
                BinOp({ op: Gt, left: Ident("age"), right: IntLit(18) }) -> Bool.true
                _ -> Bool.false
            fromOk && whereOk
        _ -> Bool.false

expect
    node = mustParse("SELECT id FROM t ORDER BY id")
    when node is
        SelectStmt({ columns: _, from: _, where: _, orderBy, groupBy: _, having: _, limit: _ }) ->
            orderBy == [Ident("id")]
        _ -> Bool.false

expect
    node = mustParse("SELECT id FROM t LIMIT 10")
    when node is
        SelectStmt({ columns: _, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: Some(IntLit(10)) }) ->
            Bool.true
        _ -> Bool.false

expect
    node = mustParse("SELECT id FROM t WHERE active = TRUE ORDER BY id LIMIT 5")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(_), orderBy, groupBy: _, having: _, limit: Some(IntLit(5)) }) ->
            orderBy == [Ident("id")]
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 2. INSERT with columns and VALUES
# ---------------------------------------------------------------------------

expect
    node = mustParse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    when node is
        InsertStmt({ table, columns, values }) ->
            table == "users" && columns == ["id", "name"] && List.len(values) == 2
        _ -> Bool.false

expect
    node = mustParse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    when node is
        InsertStmt({ table: _, columns: _, values }) ->
            when values is
                [IntLit(1), StrLit("Alice")] -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# INSERT without explicit column list
expect
    node = mustParse("INSERT INTO t VALUES (42)")
    when node is
        InsertStmt({ table: "t", columns: [], values: [IntLit(42)] }) -> Bool.true
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 3. UPDATE with SET and WHERE
# ---------------------------------------------------------------------------

expect
    node = mustParse("UPDATE users SET name = 'Bob' WHERE id = 1")
    when node is
        UpdateStmt({ table, sets, where: Some(_) }) ->
            table == "users" && List.len(sets) == 1
        _ -> Bool.false

expect
    node = mustParse("UPDATE users SET name = 'Bob' WHERE id = 1")
    when node is
        UpdateStmt({ table: _, sets, where: Some(whereExpr) }) ->
            setOk =
                when List.first(sets) is
                    Ok({ col: "name", val: StrLit("Bob") }) -> Bool.true
                    _ -> Bool.false
            whereOk =
                when whereExpr is
                    BinOp({ op: Eq, left: Ident("id"), right: IntLit(1) }) -> Bool.true
                    _ -> Bool.false
            setOk && whereOk
        _ -> Bool.false

# UPDATE with multiple SET assignments
expect
    node = mustParse("UPDATE t SET a = 1, b = 2 WHERE id = 3")
    when node is
        UpdateStmt({ table: "t", sets, where: Some(_) }) ->
            List.len(sets) == 2
        _ -> Bool.false

# UPDATE without WHERE
expect
    node = mustParse("UPDATE t SET x = 0")
    when node is
        UpdateStmt({ table: "t", sets, where: None }) ->
            when List.first(sets) is
                Ok({ col: "x", val: IntLit(0) }) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ---------------------------------------------------------------------------
# 4. DELETE with and without WHERE
# ---------------------------------------------------------------------------

expect
    node = mustParse("DELETE FROM users WHERE id = 1")
    when node is
        DeleteStmt({ table: "users", where: Some(whereExpr) }) ->
            when whereExpr is
                BinOp({ op: Eq, left: Ident("id"), right: IntLit(1) }) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

expect
    node = mustParse("DELETE FROM users")
    when node is
        DeleteStmt({ table: "users", where: None }) -> Bool.true
        _ -> Bool.false

# DELETE with complex WHERE
expect
    node = mustParse("DELETE FROM logs WHERE level = 'debug' AND created < 0")
    when node is
        DeleteStmt({ table: "logs", where: Some(whereExpr) }) ->
            when whereExpr is
                BinOp({ op: And, left: BinOp({ op: Eq, left: Ident("level"), right: StrLit("debug") }), right: BinOp({ op: Lt, left: Ident("created"), right: _ }) }) ->
                    Bool.true
                _ -> Bool.false
        _ -> Bool.false

# ===========================================================================
# Additional integration tests
# ===========================================================================

# Float literal parsed correctly
expect
    node = mustParse("SELECT 3.14")
    when node is
        SelectStmt({ columns, from: _, where: _, orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when List.first(columns) is
                Ok(FloatLit(_)) -> Bool.true
                _ -> Bool.false
        _ -> Bool.false

# Escaped string in INSERT
expect
    node = mustParse("INSERT INTO t (msg) VALUES ('it''s fine')")
    when node is
        InsertStmt({ table: "t", columns: ["msg"], values: [StrLit("it's fine")] }) -> Bool.true
        _ -> Bool.false

# AND / OR precedence: a OR b AND c = a OR (b AND c)
expect
    node = mustParse("SELECT x FROM t WHERE a OR b AND c")
    when node is
        SelectStmt({ columns: _, from: _, where: Some(whereExpr), orderBy: _, groupBy: _, having: _, limit: _ }) ->
            when whereExpr is
                BinOp({ op: Or, left: Ident("a"), right: BinOp({ op: And, left: Ident("b"), right: Ident("c") }) }) ->
                    Bool.true
                _ -> Bool.false
        _ -> Bool.false

# SELECT * FROM table
expect
    node = mustParse("SELECT * FROM products")
    when node is
        SelectStmt({ columns: [StarLit], from: [Ident("products")], where: None, orderBy: [], groupBy: [], having: None, limit: None }) ->
            Bool.true
        _ -> Bool.false
