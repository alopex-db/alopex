## Comprehensive tests for Alopex SQL Parser
## Covers tokenizer, expression, DML, and DDL tests ported from the Rust test suite.

import std/[sequtils, strutils, unittest]
import ../src/[ast, lexer, parser]

proc parseErrorMessage(sql: string): string =
  try:
    discard parseSql(sql)
  except ParseError as error:
    result = error.msg

# ---------------------------------------------------------------------------
# Tokenizer tests
# ---------------------------------------------------------------------------

suite "Tokenizer":

  test "PRAGMA keyword is recognized":
    var lex = initLexer("PRAGMA")
    let tok = lex.nextToken()
    check tok.kind == tkPragma

  test "keywords are case-insensitive":
    # All of these should produce keyword tokens regardless of case
    var lex = initLexer("SELECT select Select FROM from WHERE where")
    check lex.nextToken().kind == tkSelect
    check lex.nextToken().kind == tkSelect
    check lex.nextToken().kind == tkSelect
    check lex.nextToken().kind == tkFrom
    check lex.nextToken().kind == tkFrom
    check lex.nextToken().kind == tkWhere

  test "set-operation keywords are recognized":
    var lex = initLexer("UNION ALL INTERSECT EXCEPT")
    check lex.nextToken().kind == tkUnion
    check lex.nextToken().kind == tkAll
    check lex.nextToken().kind == tkIntersect
    check lex.nextToken().kind == tkExcept

  test "CASE expression keywords are recognized":
    var lex = initLexer("CASE WHEN THEN ELSE END")
    check lex.nextToken().kind == tkCase
    check lex.nextToken().kind == tkWhen
    check lex.nextToken().kind == tkThen
    check lex.nextToken().kind == tkElse
    check lex.nextToken().kind == tkEnd

  test "identifiers preserve original case":
    var lex = initLexer("foo _Bar1")
    let t1 = lex.nextToken()
    check t1.kind == tkIdent
    check t1.value == "foo"
    let t2 = lex.nextToken()
    check t2.kind == tkIdent
    check t2.value == "_Bar1"

  test "integer literal":
    var lex = initLexer("123")
    let tok = lex.nextToken()
    check tok.kind == tkInteger
    check tok.value == "123"

  test "float literal":
    var lex = initLexer("45.67")
    let tok = lex.nextToken()
    check tok.kind == tkFloat
    check tok.value == "45.67"

  test "string with escaped single quote":
    var lex = initLexer("'it''s'")
    let tok = lex.nextToken()
    check tok.kind == tkString
    check tok.value == "it's"

  test "tokens preserve inclusive raw lexical end locations":
    var lex = initLexer("foo * '7d' 'it''s' 'a\nb'")

    let ident = lex.nextToken()
    check ident.line == 1
    check ident.col == 1
    check ident.endLine == 1
    check ident.endCol == 3

    let star = lex.nextToken()
    check star.line == 1
    check star.col == 5
    check star.endLine == 1
    check star.endCol == 5

    let quoted = lex.nextToken()
    check quoted.value == "7d"
    check quoted.line == 1
    check quoted.col == 7
    check quoted.endLine == 1
    check quoted.endCol == 10

    let escaped = lex.nextToken()
    check escaped.value == "it's"
    check escaped.line == 1
    check escaped.col == 12
    check escaped.endLine == 1
    check escaped.endCol == 18

    let multiline = lex.nextToken()
    check multiline.value == "a\nb"
    check multiline.line == 1
    check multiline.col == 20
    check multiline.endLine == 2
    check multiline.endCol == 2

    let eof = lex.nextToken()
    check eof.line == 2
    check eof.col == 3
    check eof.endLine == 2
    check eof.endCol == 3

  test "operators and punctuation":
    var lex = initLexer("= <> != < <= > >= + - * / % , . ; ( )")
    check lex.nextToken().kind == tkEq
    check lex.nextToken().kind == tkNeq
    check lex.nextToken().kind == tkNeq
    check lex.nextToken().kind == tkLt
    check lex.nextToken().kind == tkLe
    check lex.nextToken().kind == tkGt
    check lex.nextToken().kind == tkGe
    check lex.nextToken().kind == tkPlus
    check lex.nextToken().kind == tkMinus
    check lex.nextToken().kind == tkStar
    check lex.nextToken().kind == tkSlash
    check lex.nextToken().kind == tkPercent
    check lex.nextToken().kind == tkComma
    check lex.nextToken().kind == tkDot
    check lex.nextToken().kind == tkSemicolon
    check lex.nextToken().kind == tkLParen
    check lex.nextToken().kind == tkRParen
    check lex.nextToken().kind == tkEof

  test "line comment is skipped":
    var lex = initLexer("SELECT -- this is a comment\nid")
    check lex.nextToken().kind == tkSelect
    let t = lex.nextToken()
    check t.kind == tkIdent
    check t.value == "id"
    check lex.nextToken().kind == tkEof

  test "block comment is skipped":
    var lex = initLexer("SELECT /* block\ncomment */ id")
    check lex.nextToken().kind == tkSelect
    let t = lex.nextToken()
    check t.kind == tkIdent
    check t.value == "id"
    check lex.nextToken().kind == tkEof

# ---------------------------------------------------------------------------
# Expression tests
# ---------------------------------------------------------------------------

suite "Expressions — literals":

  test "SQL-TS INTERVAL literal is preserved":
    let ast = parseSql("SELECT NOW() - INTERVAL '24 hours'")
    let expr = ast.children[0].children[0]
    check expr.kind == nkBinaryOp
    check expr.binRight.kind == nkIntervalLit
    check expr.binRight.strVal == "24 hours"

  test "SQL-TS reserved names are accepted as functions and columns":
    let ast = parseSql(
      "SELECT TIME_BUCKET(INTERVAL '1 hour', time) AS bucket, " &
      "FIRST(value, time), LAST(value, time), RATE(value), DELTA(value), " &
      "DERIVATIVE(value), HISTOGRAM_QUANTILE(0.95, value) " &
      "FROM cpu WHERE time > NOW() - INTERVAL '24 hours' " &
      "GROUP BY bucket ORDER BY bucket LIMIT 24"
    )
    check ast.kind == nkSelect
    check ast.children[0].children.len == 7

  test "qualified time column is accepted":
    let ast = parseSql("SELECT samples.time FROM samples WHERE samples.time > NOW()")
    check ast.kind == nkSelect
    check ast.children[0].children[0].kind == nkColumnRef

  test "PRAGMA accepts integer and string values":
    let integerPragma = parseSql("PRAGMA cache_size = 16")
    check integerPragma.kind == nkPragma
    check integerPragma.children[0].strVal == "cache_size"
    check integerPragma.children[1].intVal == 16
    let textPragma = parseSql("PRAGMA memory_limit = '100MB'")
    check textPragma.kind == nkPragma
    check textPragma.children[1].strVal == "100MB"

  test "integer literal":
    let ast = parseSql("SELECT 42")
    check ast.kind == nkSelect
    let cols = ast.children[0]  # nkExprList
    check cols.children[0].kind == nkIntLit
    check cols.children[0].intVal == 42

  test "float literal":
    let ast = parseSql("SELECT 3.14")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.children[0].kind == nkFloatLit

  test "string literal":
    let ast = parseSql("SELECT 'hello'")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.children[0].kind == nkStringLit
    check cols.children[0].strVal == "hello"

  test "TRUE literal":
    let ast = parseSql("SELECT TRUE")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.children[0].kind == nkBoolLit
    check cols.children[0].boolVal == true

  test "FALSE literal":
    let ast = parseSql("SELECT FALSE")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.children[0].kind == nkBoolLit
    check cols.children[0].boolVal == false

  test "NULL literal":
    let ast = parseSql("SELECT NULL")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.children[0].kind == nkNull

suite "Expressions — unary operators":

  test "NOT operator":
    let ast = parseSql("SELECT NOT TRUE")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkUnaryOp
    check col.unOp == opNot
    check col.unOperand.kind == nkBoolLit

  test "unary minus":
    let ast = parseSql("SELECT -1")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkUnaryOp
    check col.unOp == opNeg
    check col.unOperand.kind == nkIntLit

suite "Expressions — operator precedence":

  test "1 + 2 * 3 is Add(1, Mul(2, 3))":
    # precedence: * binds tighter than +
    let ast = parseSql("SELECT 1 + 2 * 3")
    let expr = ast.children[0].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opAdd
    check expr.binLeft.kind == nkIntLit
    check expr.binLeft.intVal == 1
    check expr.binRight.kind == nkBinaryOp
    check expr.binRight.binOp == opMul
    check expr.binRight.binLeft.intVal == 2
    check expr.binRight.binRight.intVal == 3

  test "(1 + 2) * 3 is Mul(Add(1, 2), 3)":
    # parentheses override precedence
    let ast = parseSql("SELECT (1 + 2) * 3")
    let expr = ast.children[0].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opMul
    check expr.binLeft.kind == nkBinaryOp
    check expr.binLeft.binOp == opAdd
    check expr.binLeft.binLeft.intVal == 1
    check expr.binLeft.binRight.intVal == 2
    check expr.binRight.intVal == 3

suite "Set operations":

  test "UNION ALL records the right input and duplicate policy":
    let ast = parseSql("SELECT 1 UNION ALL SELECT 2 ORDER BY 1")
    check ast.kind == nkSelect
    check ast.children[1].kind == nkSetOperation
    check ast.children[1].setOp == soUnion
    check ast.children[1].setAll
    check ast.children[1].setRight.kind == nkSelect
    check ast.children[^1].kind == nkOrderByClause

  test "INTERSECT binds more tightly than UNION":
    let ast = parseSql("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2")
    let unionNode = ast.children[1]
    check unionNode.kind == nkSetOperation
    check unionNode.setOp == soUnion
    let right = unionNode.setRight
    check right.children[1].kind == nkSetOperation
    check right.children[1].setOp == soIntersect

suite "Expressions — BETWEEN / NOT BETWEEN":

  test "BETWEEN":
    let ast = parseSql("SELECT * FROM t WHERE age BETWEEN 18 AND 65")
    check ast.kind == nkSelect
    let where = ast.children[^1]  # last child is WHERE
    check where.kind == nkWhereClause
    let expr = where.children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opBetween

  test "NOT BETWEEN":
    let ast = parseSql("SELECT * FROM t WHERE age NOT BETWEEN 18 AND 65")
    check ast.kind == nkSelect
    let where = ast.children[^1]
    check where.kind == nkWhereClause
    let expr = where.children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opNotBetween

suite "Expressions — LIKE / NOT LIKE":

  test "LIKE":
    let ast = parseSql("SELECT * FROM t WHERE name LIKE '%alice%'")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opLike

  test "LIKE with ESCAPE":
    let ast = parseSql("SELECT * FROM t WHERE name LIKE '%10!%%' ESCAPE '!'")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opLike
    # right-hand side is nkExprList(pattern, escape_char)
    check expr.binRight.kind == nkExprList
    check expr.binRight.children.len == 2

suite "Expressions — standard pattern operators":

  test "ILIKE and NOT ILIKE":
    let ast = parseSql("SELECT * FROM t WHERE name ILIKE 'alice%'")
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opILike

    let negated = parseSql("SELECT * FROM t WHERE name NOT ILIKE 'alice%'")
    check negated.children[^1].children[0].binOp == opNotILike

  test "GLOB and NOT GLOB":
    let ast = parseSql("SELECT * FROM t WHERE name GLOB '*.sql'")
    check ast.children[^1].children[0].binOp == opGlob

    let negated = parseSql("SELECT * FROM t WHERE name NOT GLOB '*.sql'")
    check negated.children[^1].children[0].binOp == opNotGlob

  test "SIMILAR TO and NOT SIMILAR TO":
    let ast = parseSql("SELECT * FROM t WHERE name SIMILAR TO '(alice|bob)%'")
    check ast.children[^1].children[0].binOp == opSimilarTo

    let negated = parseSql("SELECT * FROM t WHERE name NOT SIMILAR TO '(alice|bob)%'")
    check negated.children[^1].children[0].binOp == opNotSimilarTo

suite "Expressions — standard function syntax":

  test "SUBSTRING normalizes to SUBSTR":
    let ast = parseSql("SELECT SUBSTRING(name FROM 2 FOR 3)")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.children[0].strVal == "SUBSTR"
    check call.children.len == 4

  test "POSITION normalizes to STRPOS":
    let ast = parseSql("SELECT POSITION('x' IN name)")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.children[0].strVal == "STRPOS"
    check call.children.len == 3

  test "TRIM supports FROM syntax":
    let ast = parseSql("SELECT TRIM('x' FROM name)")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.children[0].strVal == "TRIM"
    check call.children.len == 3

  test "NOT LIKE":
    let ast = parseSql("SELECT * FROM t WHERE name NOT LIKE '%bob%'")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opNotLike

  test "NOT LIKE with ESCAPE":
    let ast = parseSql("SELECT * FROM t WHERE name NOT LIKE '%10!%%' ESCAPE '!'")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opNotLike
    check expr.binRight.kind == nkExprList
    check expr.binRight.children.len == 2

suite "Window frames":

  test "named WINDOW definitions, inheritance, and QUALIFY preserve structure":
    let ast = parseSql(
      "SELECT ROW_NUMBER() OVER ranked FROM sales " &
      "WINDOW base AS (PARTITION BY region), " &
      "ranked AS (base ORDER BY amount DESC) " &
      "QUALIFY ROW_NUMBER() OVER ranked = 1"
    )
    let call = ast.children[0].children[0]
    let direct = call.children[^1]
    check direct.kind == nkWindowSpec
    check direct.children.len == 1
    check direct.children[0].kind == nkIdentifier
    check direct.children[0].strVal == "ranked"

    let windowClause = ast.children[^2]
    check windowClause.kind == nkWindowClause
    check windowClause.children.len == 2
    check windowClause.children[0].kind == nkNamedWindow
    check windowClause.children[0].children[0].strVal == "base"
    let inherited = windowClause.children[1].children[1]
    check inherited.kind == nkWindowSpec
    check inherited.children[0].kind == nkIdentifier
    check inherited.children[0].strVal == "base"
    check inherited.children[1].kind == nkOrderByClause

    let qualify = ast.children[^1]
    check qualify.kind == nkQualifyClause
    check qualify.children.len == 1
    check qualify.children[0].kind == nkBinaryOp

  test "ROWS BETWEEN preserves physical bounds":
    let ast = parseSql(
      "SELECT SUM(qty) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM sales"
    )
    let call = ast.children[0].children[0]
    let window = call.children[^1]
    check window.kind == nkWindowSpec
    let frame = window.children[^1]
    check frame.kind == nkWindowFrame
    check frame.frameUnit == wfuRows
    check frame.frameStart.frameBoundKind == wfbPreceding
    check frame.frameStart.frameOffset == 2'u64
    check frame.frameEnd.frameBoundKind == wfbFollowing
    check frame.frameEnd.frameOffset == 1'u64

  test "RANGE shorthand ends at CURRENT ROW":
    let ast = parseSql(
      "SELECT SUM(qty) OVER (ORDER BY amount DESC RANGE 50 PRECEDING) FROM sales"
    )
    let frame = ast.children[0].children[0].children[^1].children[^1]
    check frame.frameUnit == wfuRange
    check frame.frameStart.frameBoundKind == wfbPreceding
    check frame.frameStart.frameOffset == 50'u64
    check frame.frameEnd.frameBoundKind == wfbCurrentRow

  test "all unbounded and current bounds parse":
    for sql in [
      "SELECT SUM(qty) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales",
      "SELECT SUM(qty) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM sales",
    ]:
      check parseSql(sql).kind == nkSelect

  test "offset overflow is deterministic":
    expect ParseError:
      discard parseSql(
        "SELECT SUM(qty) OVER (ORDER BY id ROWS 18446744073709551616 PRECEDING) FROM sales"
      )

suite "Expressions — IN / NOT IN":

  test "IN list":
    let ast = parseSql("SELECT * FROM t WHERE id IN (1, 2, 3)")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opIn
    check expr.binRight.kind == nkExprList
    check expr.binRight.children.len == 3

  test "NOT IN list":
    let ast = parseSql("SELECT * FROM t WHERE id NOT IN (1, 2, 3)")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkBinaryOp
    check expr.binOp == opNotIn
    check expr.binRight.kind == nkExprList
    check expr.binRight.children.len == 3

  test "empty IN list is rejected":
    expect ParseError:
      discard parseSql("SELECT * FROM t WHERE id IN ()")

suite "Expressions — IS NULL / IS NOT NULL":

  test "IS NULL":
    let ast = parseSql("SELECT * FROM t WHERE email IS NULL")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkUnaryOp
    check expr.unOp == opIsNull

  test "IS NOT NULL":
    let ast = parseSql("SELECT * FROM t WHERE email IS NOT NULL")
    check ast.kind == nkSelect
    let expr = ast.children[^1].children[0]
    check expr.kind == nkUnaryOp
    check expr.unOp == opIsNotNull

  test "truth predicates":
    let ast = parseSql("SELECT flag IS TRUE, flag IS NOT FALSE, flag IS UNKNOWN FROM t")
    let columns = ast.children[0]
    check columns.children.len == 3
    check columns.children[0].kind == nkTruthPredicate
    check columns.children[0].children[1].strVal == "TRUE"
    check columns.children[0].negated == false
    check columns.children[1].kind == nkTruthPredicate
    check columns.children[1].children[1].strVal == "FALSE"
    check columns.children[1].negated == true
    check columns.children[2].kind == nkTruthPredicate
    check columns.children[2].children[1].strVal == "UNKNOWN"

  test "UNKNOWN remains contextual outside truth predicates":
    check parseSql("SELECT unknown FROM t").kind == nkSelect
    check parseSql(
      "CREATE INDEX idx ON t (embedding) USING HNSW WITH (unknown = 1)"
    ).kind == nkCreateIndex

  test "IS DISTINCT FROM and row constructors":
    let ast = parseSql("SELECT (a, b) IS NOT DISTINCT FROM (c, d) FROM t")
    let predicate = ast.children[0].children[0]
    check predicate.kind == nkIsDistinctFrom
    check predicate.negated == true
    check predicate.children[0].kind == nkRowConstructor
    check predicate.children[0].children.len == 2
    check predicate.children[1].kind == nkRowConstructor
    check predicate.children[1].children.len == 2

  test "row comparison, IN and BETWEEN retain row boundaries":
    let ast = parseSql(
      "SELECT (a, b) < (c, d), (a, b) IN ((1, 2), (3, 4)), " &
      "(a, b) BETWEEN (1, 2) AND (3, 4) FROM t")
    let columns = ast.children[0]
    check columns.children[0].kind == nkBinaryOp
    check columns.children[0].binLeft.kind == nkRowConstructor
    check columns.children[0].binRight.kind == nkRowConstructor
    check columns.children[1].kind == nkBinaryOp
    check columns.children[1].binOp == opIn
    check columns.children[1].binRight.children[0].kind == nkRowConstructor
    check columns.children[2].kind == nkBinaryOp
    check columns.children[2].binOp == opBetween
    check columns.children[2].binLeft.kind == nkRowConstructor
    check columns.children[2].binRight.children[0].kind == nkRowConstructor

suite "Expressions — function calls":

  test "COUNT(*)":
    let ast = parseSql("SELECT COUNT(*) FROM t")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkFunctionCall
    check col.children[0].kind == nkIdentifier
    check col.children[0].strVal == "COUNT"
    check col.children[1].kind == nkStar

  test "function with arguments":
    let ast = parseSql("SELECT COALESCE(a, b, 0) FROM t")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkFunctionCall
    check col.children[0].strVal == "COALESCE"
    check col.children.len == 4  # name + 3 args

suite "Expressions — CASE":

  test "searched CASE with multiple branches and ELSE":
    let ast = parseSql("SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t")
    let caseExpr = ast.children[0].children[0]
    check caseExpr.kind == nkCase
    check caseExpr.caseOperand == nil
    check caseExpr.caseBranches.len == 2
    check caseExpr.caseBranches[0].caseWhen.kind == nkBinaryOp
    check caseExpr.caseBranches[0].caseThen.kind == nkStringLit
    check caseExpr.caseElse.kind == nkStringLit

  test "simple CASE without ELSE":
    let ast = parseSql("SELECT CASE a WHEN 1 THEN 'one' END FROM t")
    let caseExpr = ast.children[0].children[0]
    check caseExpr.kind == nkCase
    check caseExpr.caseOperand.kind == nkIdentifier
    check caseExpr.caseBranches.len == 1
    check caseExpr.caseElse == nil

  test "nested CASE":
    let ast = parseSql("SELECT CASE WHEN a THEN CASE b WHEN 1 THEN 2 END ELSE 3 END FROM t")
    let caseExpr = ast.children[0].children[0]
    check caseExpr.kind == nkCase
    check caseExpr.caseBranches[0].caseThen.kind == nkCase

  test "CASE branch span reaches the final THEN expression token":
    let ast = parseSql("SELECT CASE WHEN a = 1 THEN 1 + 2 ELSE 0 END")
    let branch = ast.children[0].children[0].caseBranches[0]
    check branch.span.start.column == 13
    check branch.span.`end`.column == 33

  test "malformed CASE expressions report the missing grammar element":
    check parseErrorMessage("SELECT CASE 1 END").contains("expected at least one WHEN")
    check parseErrorMessage("SELECT CASE WHEN TRUE 1 END").contains("expected tkThen")
    check parseErrorMessage("SELECT CASE WHEN TRUE THEN 1").contains("expected tkEnd")

suite "Expressions — column references":

  test "table.column":
    let ast = parseSql("SELECT t.col FROM t")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkColumnRef
    check col.children[0].strVal == "t"
    check col.children[1].strVal == "col"

# ---------------------------------------------------------------------------
# DML tests — SELECT
# ---------------------------------------------------------------------------

suite "DML — SELECT":

  test "SELECT *":
    let ast = parseSql("SELECT * FROM users")
    check ast.kind == nkSelect

  test "SELECT DISTINCT":
    let ast = parseSql("SELECT DISTINCT name FROM users")
    check ast.kind == nkSelect
    # DISTINCT marker is first child before the select list
    check ast.children[0].kind == nkIdentifier
    check ast.children[0].strVal == "DISTINCT"

  test "SELECT DISTINCT ON keeps its key expressions (issue #150)":
    let ast = parseSql(
      "SELECT DISTINCT ON (a, b % 2) a FROM t ORDER BY a, b % 2")
    check ast.kind == nkSelect
    check ast.children[0].kind == nkDistinctOnClause
    check ast.children[0].children.len == 2
    check ast.children[0].children[0].kind == nkIdentifier
    check ast.children[0].children[0].strVal == "a"
    check ast.children[0].children[1].kind == nkBinaryOp
    # The select list follows the DISTINCT ON clause.
    check ast.children[1].kind == nkExprList

  test "SELECT DISTINCT ON with a single key":
    let ast = parseSql("SELECT DISTINCT ON (region) region FROM sales")
    check ast.children[0].kind == nkDistinctOnClause
    check ast.children[0].children.len == 1

  test "SELECT DISTINCT without ON keeps the legacy marker":
    let ast = parseSql("SELECT DISTINCT on_hand FROM stock")
    check ast.children[0].kind == nkIdentifier
    check ast.children[0].strVal == "DISTINCT"

  test "DISTINCT ON requires parentheses":
    expect ParseError:
      discard parseSql("SELECT DISTINCT ON region FROM sales")

  test "DISTINCT ON rejects an empty key list":
    expect ParseError:
      discard parseSql("SELECT DISTINCT ON () region FROM sales")

  test "DISTINCT ON rejects a missing closing parenthesis":
    expect ParseError:
      discard parseSql("SELECT DISTINCT ON (region region FROM sales")

  test "SELECT with WHERE":
    let ast = parseSql("SELECT * FROM users WHERE id = 1")
    check ast.kind == nkSelect

  test "SELECT with column alias using AS":
    let ast = parseSql("SELECT name AS user_name FROM users")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkAlias
    check col.aliasName == "user_name"

  test "SELECT accepts reserved expression name as an explicit alias":
    let ast = parseSql("SELECT value AS time FROM metrics")
    let col = ast.children[0].children[0]
    check col.kind == nkAlias
    check col.aliasName == "time"

  test "SELECT with implicit column alias":
    let ast = parseSql("SELECT name user_name FROM users")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkAlias
    check col.aliasName == "user_name"

  test "SELECT with table alias using AS":
    let ast = parseSql("SELECT u.name FROM users AS u")
    check ast.kind == nkSelect
    let fromClause = ast.children[1]
    check fromClause.kind == nkFromClause
    check fromClause.children[0].kind == nkAlias
    check fromClause.children[0].aliasName == "u"

  test "SELECT with implicit table alias":
    let ast = parseSql("SELECT u.name FROM users u")
    check ast.kind == nkSelect
    let fromClause = ast.children[1]
    check fromClause.kind == nkFromClause
    check fromClause.children[0].kind == nkAlias
    check fromClause.children[0].aliasName == "u"

  test "SELECT with ORDER BY ASC":
    let ast = parseSql("SELECT * FROM users ORDER BY name ASC")
    check ast.kind == nkSelect
    # Find ORDER BY clause
    var found = false
    for child in ast.children:
      if child.kind == nkOrderByClause:
        found = true
        check child.children[0].kind == nkAlias
        check child.children[0].aliasName == "ASC"
    check found

  test "SELECT with ORDER BY DESC":
    let ast = parseSql("SELECT * FROM users ORDER BY created_at DESC")
    check ast.kind == nkSelect
    var found = false
    for child in ast.children:
      if child.kind == nkOrderByClause:
        found = true
        check child.children[0].aliasName == "DESC"
    check found

  test "SELECT with LIMIT":
    let ast = parseSql("SELECT * FROM users LIMIT 10")
    check ast.kind == nkSelect
    var found = false
    for child in ast.children:
      if child.kind == nkLimitClause:
        found = true
        check child.children[0].kind == nkIntLit
        check child.children[0].intVal == 10
    check found

  test "SELECT with LIMIT and OFFSET":
    let ast = parseSql("SELECT * FROM users LIMIT 10 OFFSET 20")
    check ast.kind == nkSelect
    var foundLimit = false
    var foundOffset = false
    for child in ast.children:
      if child.kind == nkLimitClause:
        foundLimit = true
        check child.children.len == 1
        check child.children[0].intVal == 10
        check child.limitWithTies == false
      elif child.kind == nkOffsetClause:
        foundOffset = true
        check child.children.len == 1
        check child.children[0].intVal == 20
    check foundLimit
    check foundOffset

  test "SELECT with multiple columns":
    let ast = parseSql("SELECT id, name, email FROM users")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.kind == nkExprList
    check cols.children.len == 3

  test "WITH common table expression keeps its column name list":
    let ast = parseSql(
      "WITH c(identifier, label) AS (SELECT 1, 'one') SELECT identifier FROM c")
    let withClause = ast.children[0]
    let cte = withClause.children[0]

    check withClause.kind == nkWithClause
    check cte.kind == nkCommonTableExpr
    check cte.children[0].strVal == "c"
    check cte.children[1].kind == nkCteColumnList
    check cte.children[1].children[0].strVal == "identifier"
    check cte.children[1].children[1].strVal == "label"
    check cte.children[2].kind == nkSelect

  test "WITH common table expression accepts a nested WITH query":
    let ast = parseSql(
      "WITH outer_cte(value) AS (WITH inner_cte(source) AS (SELECT 7) " &
      "SELECT source FROM inner_cte) SELECT value FROM outer_cte")
    let outerQuery = ast.children[0].children[0].children[2]

    check outerQuery.kind == nkSelect
    check outerQuery.children[0].kind == nkWithClause
    check outerQuery.children[0].children[0].children[1].kind == nkCteColumnList

  test "trailing semicolon":
    let ast = parseSql("SELECT 1;")
    check ast.kind == nkSelect

suite "VALUES query and table constructor":

  test "top-level VALUES keeps one or many expression rows":
    let ast = parseSql("VALUES (1 + 2, 'a', NULL), (4, 'b', 6)")
    check ast.kind == nkValues
    check ast.children.len == 2
    check ast.children[0].kind == nkExprList
    check ast.children[0].children.len == 3
    check ast.children[0].children[0].kind == nkBinaryOp
    check ast.children[1].children[2].kind == nkIntLit

  test "VALUES composes with SELECT set operands and query tail":
    let ast = parseSql(
      "VALUES (3), (1) UNION ALL SELECT 2 UNION ALL VALUES (4) " &
      "ORDER BY column1 LIMIT 3")
    check ast.kind == nkValues
    check ast.children.filterIt(it.kind == nkSetOperation).len == 2
    check ast.children.filterIt(it.kind == nkOrderByClause).len == 1
    check ast.children.filterIt(it.kind == nkLimitClause).len == 1
    let operations = ast.children.filterIt(it.kind == nkSetOperation)
    check operations[0].setRight.kind == nkSelect
    check operations[1].setRight.kind == nkValues

  test "derived VALUES keeps table and column aliases":
    let ast = parseSql(
      "SELECT id, label FROM (VALUES (2, 'b'), (1, 'a')) AS t(id, label)")
    let fromItem = ast.children.filterIt(it.kind == nkFromClause)[0].children[0]
    check fromItem.kind == nkAlias
    check fromItem.aliasExpr.kind == nkFromDerived
    check fromItem.aliasExpr.children[0].kind == nkValues
    check fromItem.aliasName == "t"
    check fromItem.aliasColumns == @["id", "label"]

  test "WITH accepts VALUES as a CTE body and final query":
    let selectQuery = parseSql(
      "WITH v(id, label) AS (VALUES (1, 'a')) SELECT id, label FROM v")
    let cte = selectQuery.children[0].children[0]
    check cte.children[^1].kind == nkValues

    let valuesQuery = parseSql("WITH v(id) AS (SELECT 1) VALUES (2)")
    check valuesQuery.kind == nkValues
    check valuesQuery.children[0].kind == nkWithClause

  test "empty VALUES constructors fail closed":
    check "expected tkLParen" in parseErrorMessage("VALUES")
    check "expected expression" in parseErrorMessage("VALUES ()")

# ---------------------------------------------------------------------------
# DML tests — INSERT
# ---------------------------------------------------------------------------

suite "DML — INSERT":

  test "INSERT with column list and VALUES":
    let ast = parseSql("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    check ast.kind == nkInsert
    # children: table, col-list, values-row
    check ast.children[0].kind == nkIdentifier
    check ast.children[0].strVal == "users"
    check ast.children[1].kind == nkColumnList  # column list
    check ast.children[1].children.len == 2
    check ast.children[2].kind == nkExprList  # values row
    check ast.children[2].children.len == 2

  test "INSERT without column list":
    let ast = parseSql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
    check ast.kind == nkInsert
    # children: table, values-row (no column list)
    check ast.children[0].strVal == "users"
    check ast.children[1].kind == nkExprList
    check ast.children[1].children.len == 3

  test "INSERT with multiple value rows":
    let ast = parseSql("""
      INSERT INTO users (name, age)
      VALUES ('Alice', 30), ('Bob', 25), ('Carol', 28)
    """)
    check ast.kind == nkInsert
    # children: table, col-list, row1, row2, row3
    check ast.children[0].strVal == "users"
    check ast.children[1].kind == nkColumnList  # col list
    check ast.children[2].kind == nkExprList  # row 1
    check ast.children[3].kind == nkExprList  # row 2
    check ast.children[4].kind == nkExprList  # row 3

  test "INSERT multi-row without column list (issue #40)":
    let ast = parseSql("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
    check ast.kind == nkInsert
    # children: table, row1, row2 — the first row must NOT look like a column list
    check ast.children.len == 3
    check ast.children[0].strVal == "t1"
    check ast.children[1].kind == nkExprList  # row 1
    check ast.children[1].children.len == 2
    check ast.children[1].children[0].kind == nkIntLit
    check ast.children[2].kind == nkExprList  # row 2
    check ast.children[2].children.len == 2

# ---------------------------------------------------------------------------
# DML tests — UPDATE
# ---------------------------------------------------------------------------

suite "DML — UPDATE":

  test "UPDATE with SET and WHERE":
    let ast = parseSql("UPDATE users SET name = 'Bob' WHERE id = 1")
    check ast.kind == nkUpdate
    check ast.children[0].strVal == "users"
    let setList = ast.children[1]
    check setList.kind == nkExprList
    check setList.children.len == 1
    check setList.children[0].kind == nkBinaryOp
    check setList.children[0].binOp == opEq

  test "UPDATE with multiple SET assignments":
    let ast = parseSql("UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1")
    check ast.kind == nkUpdate
    let setList = ast.children[1]
    check setList.kind == nkExprList
    check setList.children.len == 2

  test "UPDATE without WHERE":
    let ast = parseSql("UPDATE users SET active = FALSE")
    check ast.kind == nkUpdate

# ---------------------------------------------------------------------------
# DML tests — DELETE
# ---------------------------------------------------------------------------

suite "DML — DELETE":

  test "DELETE with WHERE":
    let ast = parseSql("DELETE FROM users WHERE id = 1")
    check ast.kind == nkDelete
    check ast.children[0].strVal == "users"
    # WHERE is second child
    check ast.children[1].kind == nkWhereClause

  test "DELETE without WHERE":
    let ast = parseSql("DELETE FROM users")
    check ast.kind == nkDelete
    check ast.children.len == 1
    check ast.children[0].strVal == "users"

# ---------------------------------------------------------------------------
# DDL tests — CREATE TABLE
# ---------------------------------------------------------------------------

suite "DDL — CREATE TABLE":

  test "CREATE TABLE with common constraints":
    let ast = parseSql("""
      CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email TEXT UNIQUE,
        age INT DEFAULT 0
      )
    """)
    check ast.kind == nkCreateTable
    # First non-flag child is the table name, then column defs
    # Find table name (nkIdentifier with value "users")
    var tableName = ""
    var colCount = 0
    for child in ast.children:
      if child.kind == nkIdentifier and child.strVal == "users":
        tableName = child.strVal
      elif child.kind == nkColumnDef:
        inc colCount
    check tableName == "users"
    check colCount == 4

  test "CREATE TABLE IF NOT EXISTS":
    let ast = parseSql("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
    check ast.kind == nkCreateTable
    # First child is IF NOT EXISTS marker
    check ast.children[0].kind == nkIdentifier
    check ast.children[0].strVal == "IF NOT EXISTS"

  test "CREATE TABLE with multiple data types":
    let ast = parseSql("""
      CREATE TABLE records (
        id BIGINT PRIMARY KEY,
        label VARCHAR(100) NOT NULL,
        score DECIMAL(10, 2),
        flag BOOLEAN DEFAULT FALSE,
        note TEXT
      )
    """)
    check ast.kind == nkCreateTable
    var colCount = 0
    for child in ast.children:
      if child.kind == nkColumnDef:
        inc colCount
    check colCount == 5

  test "column PRIMARY KEY constraint":
    let ast = parseSql("CREATE TABLE t (id INT PRIMARY KEY)")
    check ast.kind == nkCreateTable
    var pkFound = false
    for child in ast.children:
      if child.kind == nkColumnDef:
        for c in child.colConstraints:
          if c.children[0].strVal == "PRIMARY KEY":
            pkFound = true
    check pkFound

  test "column NOT NULL constraint":
    let ast = parseSql("CREATE TABLE t (name TEXT NOT NULL)")
    check ast.kind == nkCreateTable
    var nnFound = false
    for child in ast.children:
      if child.kind == nkColumnDef:
        for c in child.colConstraints:
          if c.children[0].strVal == "NOT NULL":
            nnFound = true
    check nnFound

  test "column UNIQUE constraint":
    let ast = parseSql("CREATE TABLE t (email TEXT UNIQUE)")
    check ast.kind == nkCreateTable
    var uFound = false
    for child in ast.children:
      if child.kind == nkColumnDef:
        for c in child.colConstraints:
          if c.children[0].strVal == "UNIQUE":
            uFound = true
    check uFound

  test "column DEFAULT constraint":
    let ast = parseSql("CREATE TABLE t (active BOOLEAN DEFAULT TRUE)")
    check ast.kind == nkCreateTable
    var defFound = false
    for child in ast.children:
      if child.kind == nkColumnDef:
        for c in child.colConstraints:
          if c.children[0].strVal == "DEFAULT":
            defFound = true
    check defFound

# ---------------------------------------------------------------------------
# DDL tests — CREATE CONTINUOUS AGGREGATE
# ---------------------------------------------------------------------------

suite "DDL — CREATE CONTINUOUS AGGREGATE":

  test "canonical statement reuses Select AST and preserves named options":
    let ast = parseSql("""
CREATE CONTINUOUS AGGREGATE cpu_hourly
AS
SELECT
  TIME_BUCKET(INTERVAL '1 hour', time) AS time,
  host,
  AVG(usage_user) AS usage_user_avg
FROM cpu_metrics
GROUP BY TIME_BUCKET(INTERVAL '1 hour', time), host
WITH (
  retention = '30d',
  refresh_interval = '1h'
);
""")
    check ast.kind == nkCreateContinuousAggregate
    check ast.children.len == 3
    check ast.children[0].kind == nkIdentifier
    check ast.children[0].strVal == "cpu_hourly"
    check ast.children[1].kind == nkSelect
    check ast.children[1].children[0].kind == nkExprList
    check ast.children[1].children[0].children[0].kind == nkAlias
    check ast.children[1].children[0].children[0].aliasName == "time"
    check ast.children[1].children[0].children[0].aliasExpr.kind ==
      nkFunctionCall
    check ast.children[2].kind == nkWithOptions
    check ast.children[2].children.len == 2
    check ast.children[2].children[0].children[0].strVal == "retention"
    check ast.children[2].children[0].children[1].kind == nkStringLit
    check ast.children[2].children[0].children[1].strVal == "30d"
    check ast.children[2].children[1].children[0].strVal ==
      "refresh_interval"
    check ast.children[2].children[1].children[1].strVal == "1h"

  test "contextual words and option names are case-insensitive":
    let ast = parseSql(
      "cReAtE cOnTiNuOuS aGgReGaTe CpuHourly AS " &
      "SeLeCt time, continuous, aggregate FrOm Metrics " &
      "WiTh (ReTeNtIoN = '30d', ReFrEsH_InTeRvAl = '1h')"
    )
    check ast.kind == nkCreateContinuousAggregate
    check ast.children[0].strVal == "CpuHourly"
    check ast.children[1].kind == nkSelect
    check ast.children[2].children[0].children[0].strVal == "ReTeNtIoN"
    check ast.children[2].children[1].children[0].strVal ==
      "ReFrEsH_InTeRvAl"

  test "CONTINUOUS and AGGREGATE remain ordinary identifiers elsewhere":
    var lex = initLexer("CONTINUOUS aggregate")
    check lex.nextToken().kind == tkIdent
    check lex.nextToken().kind == tkIdent

    let selected = parseSql(
      "SELECT continuous, aggregate FROM measurements"
    )
    check selected.kind == nkSelect
    check selected.children[0].children[0].strVal == "continuous"
    check selected.children[0].children[1].strVal == "aggregate"

    let table = parseSql(
      "CREATE TABLE continuous (aggregate TEXT)"
    )
    check table.kind == nkCreateTable
    check table.children[0].strVal == "continuous"
    check table.children[1].colName == "aggregate"

  test "statement, name, query, options, keys, and values retain nearest spans":
    let ast = parseSql("""CREATE CONTINUOUS AGGREGATE hourly
AS SELECT time, AVG(value) AS avg_value
FROM samples
WITH (
  retention = '7d',
  refresh_interval = '1h'
)""")
    check ast.span.start == Location(line: 1, column: 1)
    check ast.span.`end` == Location(line: 7, column: 1)
    check ast.children[0].span.start == Location(line: 1, column: 29)
    check ast.children[1].span.start == Location(line: 2, column: 4)
    check ast.children[1].span.`end` == Location(line: 3, column: 12)
    check ast.children[2].span.start == Location(line: 4, column: 1)
    check ast.children[2].span.`end` == Location(line: 7, column: 1)
    check ast.children[2].children[0].span.start ==
      Location(line: 5, column: 3)
    check ast.children[2].children[0].span.`end` ==
      Location(line: 5, column: 18)
    check ast.children[2].children[0].children[1].span.`end` ==
      Location(line: 5, column: 18)
    check ast.children[2].children[1].children[1].span.start ==
      Location(line: 6, column: 22)

  test "escaped and multiline option strings use exact raw lexical ends":
    let escaped = parseSql("""CREATE CONTINUOUS AGGREGATE hourly
AS SELECT time FROM samples
WITH (
  retention = '7''d',
  refresh_interval = '1h'
)""")
    let escapedValue = escaped.children[2].children[0].children[1]
    check escapedValue.strVal == "7'd"
    check escapedValue.span.start == Location(line: 4, column: 15)
    check escapedValue.span.`end` == Location(line: 4, column: 20)
    check escaped.children[2].children[0].span.`end` ==
      Location(line: 4, column: 20)

    let multiline = parseSql("""CREATE CONTINUOUS AGGREGATE hourly
AS SELECT time FROM samples
WITH (
  retention = '7
d',
  refresh_interval = '1h'
)""")
    let multilineValue = multiline.children[2].children[0].children[1]
    check multilineValue.strVal == "7\nd"
    check multilineValue.span.start == Location(line: 4, column: 15)
    check multilineValue.span.`end` == Location(line: 5, column: 2)
    check multiline.children[2].children[0].span.`end` ==
      Location(line: 5, column: 2)

  test "required clauses occur exactly in canonical order":
    let malformed = [
      "CREATE AGGREGATE CONTINUOUS hourly AS SELECT * FROM samples " &
        "WITH (retention = '7d', refresh_interval = '1h')",
      "CREATE CONTINUOUS hourly AS SELECT * FROM samples " &
        "WITH (retention = '7d', refresh_interval = '1h')",
      "CREATE CONTINUOUS AGGREGATE hourly SELECT * FROM samples " &
        "WITH (retention = '7d', refresh_interval = '1h')",
      "CREATE CONTINUOUS AGGREGATE hourly AS INSERT INTO samples VALUES (1) " &
        "WITH (retention = '7d', refresh_interval = '1h')",
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples",
      "CREATE CONTINUOUS AGGREGATE hourly WITH " &
        "(retention = '7d', refresh_interval = '1h') AS SELECT * FROM samples"
    ]
    for sql in malformed:
      check parseErrorMessage(sql).contains("Parse error at line")

  test "options must end the statement unless separated by a semicolon":
    let statement =
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples " &
      "WITH (retention = '7d', refresh_interval = '1h')"

    let adjacent = parseErrorMessage(statement & " SELECT 1")
    check adjacent.contains("unexpected token after continuous aggregate options")
    check adjacent.contains("got tkSelect 'SELECT'")

    let trailing = parseErrorMessage(statement & " ORDER BY time")
    check trailing.contains("unexpected token after continuous aggregate options")
    check trailing.contains("got tkOrder 'ORDER'")

    let separated = parseSql(statement & "; SELECT 1")
    check separated.kind == nkStatementList
    check separated.children.len == 2
    check separated.children[0].kind == nkCreateContinuousAggregate
    check separated.children[1].kind == nkSelect

  test "missing, duplicate, unknown, and out-of-order options are rejected":
    let prefix =
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples WITH "
    let malformed = [
      prefix & "(retention = '7d')",
      prefix & "(refresh_interval = '1h')",
      prefix & "(retention = '7d', retention = '8d', " &
        "refresh_interval = '1h')",
      prefix & "(retention = '7d', refresh_interval = '1h', bogus = 'x')",
      prefix & "(refresh_interval = '1h', retention = '7d')"
    ]
    for sql in malformed:
      check parseErrorMessage(sql).contains("Parse error at line")

  test "option values are non-empty strings and preserve mapper input":
    let prefix =
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples WITH "
    let malformed = [
      prefix & "(retention = 7, refresh_interval = '1h')",
      prefix & "(retention = '7d', refresh_interval = 1)",
      prefix & "(retention = '', refresh_interval = '1h')",
      prefix & "(retention = '7d', refresh_interval = '')"
    ]
    for sql in malformed:
      check parseErrorMessage(sql).contains("Parse error at line")

  test "option errors report the nearest offending token":
    let unknown = parseErrorMessage(
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples " &
      "WITH (retention = '7d', bogus = '1h')"
    )
    check unknown.contains("col 85")
    check unknown.contains("unknown continuous aggregate option")

    let malformed = parseErrorMessage(
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT * FROM samples " &
      "WITH (retention = '', refresh_interval = '1h')"
    )
    check malformed.contains("col 79")
    check malformed.contains("non-empty duration string")

  test "single measurement sources are structural and may be aliased":
    let aliased = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS SELECT s.time FROM samples AS s " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    check aliased.children[1].children[1].children[0].kind == nkAlias
    check aliased.children[1].children[1].children[0].aliasExpr.kind ==
      nkIdentifier

    let prefix = "CREATE CONTINUOUS AGGREGATE hourly AS "
    let suffix = " WITH (retention = '7d', refresh_interval = '1h')"
    let malformed = [
      prefix & "SELECT 1" & suffix,
      prefix & "SELECT * FROM a JOIN b ON a.id = b.id" & suffix,
      prefix & "SELECT * FROM a, b" & suffix,
      prefix & "SELECT * FROM (SELECT * FROM samples) nested" & suffix
    ]
    for sql in malformed:
      check parseErrorMessage(sql).contains(
        "continuous aggregate query requires one source measurement")

  test "query parsing preserves existing Select clauses for semantic mapping":
    let ast = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS " &
      "SELECT host, AVG(value) FROM samples GROUP BY host " &
      "HAVING AVG(value) > 0 ORDER BY host LIMIT 1 " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    let query = ast.children[1]
    check query.kind == nkSelect
    check query.children[2].kind == nkGroupByClause
    check query.children[3].kind == nkHavingClause
    check query.children[4].kind == nkOrderByClause
    check query.children[5].kind == nkLimitClause

# ---------------------------------------------------------------------------
# DDL tests — DROP TABLE
# ---------------------------------------------------------------------------

suite "DDL — DROP TABLE":

  test "DROP TABLE":
    let ast = parseSql("DROP TABLE users")
    check ast.kind == nkDropTable
    check ast.children[0].strVal == "users"

  test "DROP TABLE IF EXISTS":
    let ast = parseSql("DROP TABLE IF EXISTS users")
    check ast.kind == nkDropTable
    check ast.children[0].strVal == "IF EXISTS"
    check ast.children[1].strVal == "users"

# ---------------------------------------------------------------------------
# Roadmap coverage tests — Phase 2
# ---------------------------------------------------------------------------

suite "Roadmap — lexer keywords and symbols":

  test "roadmap reserved keywords and symbols":
    var lex = initLexer("USING NATURAL ANY SOME ALL CAST NOW HNSW BTREE COSINE L2 ESCAPE WITH NULLS FIRST LAST || [ ]")
    check lex.nextToken().kind == tkUsing
    check lex.nextToken().kind == tkNatural
    check lex.nextToken().kind == tkAny
    check lex.nextToken().kind == tkSome
    check lex.nextToken().kind == tkAll
    check lex.nextToken().kind == tkCast
    check lex.nextToken().kind == tkNow
    check lex.nextToken().kind == tkHnsw
    check lex.nextToken().kind == tkBtree
    check lex.nextToken().kind == tkCosine
    check lex.nextToken().kind == tkL2
    check lex.nextToken().kind == tkEscape
    check lex.nextToken().kind == tkWith
    check lex.nextToken().kind == tkNulls
    check lex.nextToken().kind == tkFirst
    check lex.nextToken().kind == tkLast
    check lex.nextToken().kind == tkPipePipe
    check lex.nextToken().kind == tkLBracket
    check lex.nextToken().kind == tkRBracket

suite "Roadmap — DDL and Vector":

  test "CREATE INDEX and DROP INDEX":
    let createIdx = parseSql("CREATE INDEX idx_doc_embedding ON documents (embedding) USING HNSW WITH (m = 16, ef_construction = 200)")
    check createIdx.kind == nkCreateIndex
    check createIdx.children[0].strVal == "idx_doc_embedding"
    check createIdx.children[1].strVal == "documents"
    check createIdx.children[2].strVal == "embedding"
    check createIdx.children[3].strVal == "HNSW"
    check createIdx.children[4].kind == nkWithOptions
    check createIdx.children[4].children.len == 2

    let dropIdx = parseSql("DROP INDEX IF EXISTS idx_doc_embedding")
    check dropIdx.kind == nkDropIndex
    check dropIdx.children[0].strVal == "IF EXISTS"
    check dropIdx.children[1].strVal == "idx_doc_embedding"

  test "VECTOR type, vector literal, CAST and NOW":
    let table = parseSql("CREATE TABLE items (id INT, embedding VECTOR(3, COSINE))")
    check table.kind == nkCreateTable
    check table.children[2].kind == nkColumnDef
    check table.children[2].colType.children[0].strVal == "VECTOR"
    check table.children[2].colType.children[1].intVal == 3
    check table.children[2].colType.children[2].strVal == "COSINE"

    let select = parseSql("SELECT [1.0, -2.0, 3.5], CAST(id AS TEXT), NOW() FROM items")
    let cols = select.children[0]
    check cols.children[0].kind == nkVectorLiteral
    check cols.children[0].children.len == 3
    check cols.children[0].children[1].floatVal == -2.0
    let castExpr = cols.children[1]
    check castExpr.kind == nkCast
    check castExpr.children.len == 2
    check castExpr.children[0].kind == nkIdentifier
    check castExpr.children[0].strVal == "id"
    check castExpr.children[1].kind == nkTypeName
    check castExpr.children[1].children.len == 1
    check castExpr.children[1].children[0].kind == nkIdentifier
    check castExpr.children[1].children[0].strVal == "TEXT"
    check not castExpr.span.isEmpty
    check not castExpr.children[0].span.isEmpty
    check not castExpr.children[1].span.isEmpty
    check castExpr.span.start == Location(line: 1, column: 26)
    check castExpr.children[0].span.start == Location(line: 1, column: 31)
    check castExpr.children[1].span.start == Location(line: 1, column: 37)
    check cols.children[2].children[0].strVal == "NOW"

  test "TRY_CAST is a dedicated expression and remains a contextual identifier":
    let select = parseSql(
      "SELECT TRY_CAST(raw_value AS INTEGER), try_cast FROM samples"
    )
    let cols = select.children[0]
    let tryCastExpr = cols.children[0]
    check tryCastExpr.kind == nkTryCast
    check tryCastExpr.children.len == 2
    check tryCastExpr.children[0].kind == nkIdentifier
    check tryCastExpr.children[0].strVal == "raw_value"
    check tryCastExpr.children[1].kind == nkTypeName
    check tryCastExpr.children[1].children[0].strVal == "INTEGER"
    check cols.children[1].kind == nkIdentifier
    check cols.children[1].strVal == "try_cast"

suite "Roadmap — aggregation":

  test "COUNT DISTINCT, COUNT star, aggregates and string concat":
    let ast = parseSql("""
      SELECT COUNT(DISTINCT user_id), COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount),
             first_name || ' ' || last_name
      FROM orders
      GROUP BY user_id, first_name, last_name
      HAVING COUNT(*) > 1
    """)
    let cols = ast.children[0]
    check cols.children[0].kind == nkFunctionCall
    check cols.children[0].funcDistinct == true
    check cols.children[1].funcStar == true
    check cols.children[6].kind == nkBinaryOp
    check cols.children[6].binOp == opStringConcat
    var groupFound = false
    var havingFound = false
    for child in ast.children:
      if child.kind == nkGroupByClause:
        groupFound = true
        check child.children.len == 3
      if child.kind == nkHavingClause:
        havingFound = true
    check groupFound
    check havingFound

suite "Roadmap — JOIN":

  test "JOIN USING and NATURAL":
    let usingAst = parseSql("SELECT * FROM users JOIN orders USING (user_id)")
    let usingFrom = usingAst.children[1]
    let usingJoin = usingFrom.children[0]
    check usingJoin.kind == nkJoin
    check usingJoin.joinKind == jkInner
    check usingJoin.joinUsing == @["user_id"]

    let naturalAst = parseSql("SELECT * FROM users NATURAL LEFT JOIN profiles")
    let naturalJoin = naturalAst.children[1].children[0]
    check naturalJoin.kind == nkJoin
    check naturalJoin.joinKind == jkLeft
    check naturalJoin.natural == true

  test "implicit comma join is a FromItem join tree":
    let ast = parseSql("SELECT * FROM users u, orders o WHERE u.id = o.user_id")
    let join = ast.children[1].children[0]
    check join.kind == nkJoin
    check join.joinKind == jkCross
    check join.joinLeft.kind == nkAlias
    check join.joinRight.kind == nkAlias

suite "Roadmap — Subquery":

  test "scalar subquery and FROM derived table":
    let scalar = parseSql("SELECT (SELECT COUNT(*) FROM orders) AS order_count FROM users")
    let item = scalar.children[0].children[0]
    check item.kind == nkAlias
    check item.aliasExpr.kind == nkScalarSubquery
    check item.aliasExpr.children[0].kind == nkSelect

    let derived = parseSql("SELECT * FROM (SELECT id, name FROM users WHERE active = TRUE) AS active_users")
    let fromItem = derived.children[1].children[0]
    check fromItem.kind == nkAlias
    check fromItem.aliasExpr.kind == nkFromDerived

  test "IN and EXISTS subqueries":
    let inAst = parseSql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
    let inExpr = inAst.children[2].children[0]
    check inExpr.kind == nkInSubquery
    check inExpr.negated == false

    let existsAst = parseSql("SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)")
    let existsExpr = existsAst.children[2].children[0]
    check existsExpr.kind == nkExists
    check existsExpr.negated == false

  test "ANY SOME and ALL quantified subqueries":
    let anyAst = parseSql("SELECT * FROM scores WHERE score > ANY (SELECT score FROM baseline)")
    let anyExpr = anyAst.children[2].children[0]
    check anyExpr.kind == nkQuantified
    check anyExpr.quantifier == qkAny

    let allAst = parseSql("SELECT * FROM scores WHERE score >= ALL (SELECT score FROM baseline)")
    let allExpr = allAst.children[2].children[0]
    check allExpr.kind == nkQuantified
    check allExpr.quantifier == qkAll

suite "Roadmap — ordering, multi statements, spans":

  test "ORDER BY NULLS FIRST and LAST":
    let ast = parseSql("SELECT * FROM users ORDER BY name DESC NULLS LAST, id ASC NULLS FIRST")
    let order = ast.children[2]
    check order.kind == nkOrderByClause
    check order.children[0].aliasName == "DESC"
    check order.children[0].nullsFirst == 0
    check order.children[1].aliasName == "ASC"
    check order.children[1].nullsFirst == 1

  test "multiple statements parse to statement list":
    let ast = parseSql("SELECT 1; SELECT 2;")
    check ast.kind == nkStatementList
    check ast.children.len == 2
    check ast.children[0].kind == nkSelect
    check ast.children[1].kind == nkSelect

  test "all nodes receive spans":
    let ast = parseSql("SELECT id, name FROM users WHERE id = 1")
    check ast.span.start.line == 1
    check ast.children[0].span.start.line == 1
    check ast.children[0].children[0].span.start.line == 1
    check ast.children[1].span.start.line == 1
    check ast.children[2].children[0].span.start.line == 1

# ---------------------------------------------------------------------------
# FETCH FIRST/NEXT, OFFSET n ROWS, WITH TIES, bind-parameter token (issue #152)
# ---------------------------------------------------------------------------

proc findClause(ast: SqlNode; kind: SqlNodeKind): SqlNode =
  for child in ast.children:
    if child.kind == kind:
      return child
  nil

suite "FETCH pagination (issue #152)":

  test "OFFSET n ROWS followed by FETCH NEXT n ROWS ONLY":
    let ast = parseSql("SELECT id FROM t OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY")
    check ast.kind == nkSelect
    let offsetNode = ast.findClause(nkOffsetClause)
    check offsetNode != nil
    check offsetNode.children[0].kind == nkIntLit
    check offsetNode.children[0].intVal == 2
    let limitNode = ast.findClause(nkLimitClause)
    check limitNode != nil
    check limitNode.children[0].intVal == 3
    check limitNode.limitWithTies == false

  test "FETCH NEXT ROW ONLY synthesizes count 1":
    let ast = parseSql("SELECT id FROM t FETCH NEXT ROW ONLY")
    let limitNode = ast.findClause(nkLimitClause)
    check limitNode != nil
    check limitNode.children[0].kind == nkIntLit
    check limitNode.children[0].intVal == 1
    check limitNode.limitWithTies == false

  test "FETCH FIRST 2 ROWS WITH TIES sets the ties flag":
    let ast = parseSql(
      "SELECT id FROM t ORDER BY id FETCH FIRST 2 ROWS WITH TIES")
    let limitNode = ast.findClause(nkLimitClause)
    check limitNode != nil
    check limitNode.children[0].intVal == 2
    check limitNode.limitWithTies == true

  test "OFFSET without LIMIT and OFFSET before LIMIT are accepted":
    let bare = parseSql("SELECT id FROM t OFFSET 4")
    check bare.findClause(nkOffsetClause) != nil
    check bare.findClause(nkLimitClause) == nil

    let swapped = parseSql("SELECT id FROM t OFFSET 4 LIMIT 2")
    check swapped.findClause(nkOffsetClause).children[0].intVal == 4
    check swapped.findClause(nkLimitClause).children[0].intVal == 2

  test "LIMIT ALL parses as no limit":
    let ast = parseSql("SELECT id FROM t LIMIT ALL OFFSET 1")
    check ast.findClause(nkLimitClause) == nil
    check ast.findClause(nkOffsetClause).children[0].intVal == 1

  test "FETCH count accepts constant expressions":
    let ast = parseSql("SELECT id FROM t FETCH FIRST 1 + 1 ROWS ONLY")
    let limitNode = ast.findClause(nkLimitClause)
    check limitNode.children[0].kind == nkBinaryOp

  test "duplicate limit-setting clauses are rejected":
    check parseErrorMessage(
      "SELECT 1 LIMIT 1 FETCH FIRST 1 ROWS ONLY").contains(
      "multiple LIMIT clauses")
    check parseErrorMessage("SELECT 1 LIMIT 1 LIMIT 2").contains(
      "multiple LIMIT clauses")
    check parseErrorMessage("SELECT 1 OFFSET 1 OFFSET 2").contains(
      "multiple OFFSET clauses")

  test "FETCH grammar errors are explicit":
    check parseErrorMessage("SELECT 1 FETCH 1 ROWS ONLY").contains(
      "expected FIRST or NEXT after FETCH")
    check parseErrorMessage("SELECT 1 FETCH FIRST 1 ROWS").contains(
      "expected ONLY or WITH TIES")
    check parseErrorMessage("SELECT 1 FETCH FIRST 1 ROWS WITH 2").contains(
      "expected TIES after WITH")
    check parseErrorMessage(
      "SELECT 1 FETCH FIRST 10 PERCENT ROWS ONLY").contains(
      "FETCH ... PERCENT is not supported")

  test "bind parameter placeholder reports a dedicated error":
    let message = parseErrorMessage("SELECT ? FROM t")
    check message.contains("bind parameters are not yet supported")
    check parseErrorMessage("SELECT id FROM t LIMIT ?").contains(
      "bind parameters are not yet supported")

  test "fetch keywords stay usable as expression identifiers":
    let ast = parseSql("SELECT fetch, next, ties, only, row FROM t")
    let items = ast.children[0]
    check items.kind == nkExprList
    check items.children.len == 5
    for i, expected in ["fetch", "next", "ties", "only", "row"]:
      check items.children[i].kind == nkIdentifier
      check items.children[i].strVal == expected

  test "window frame CURRENT ROW still parses with the ROW keyword":
    let ast = parseSql(
      "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t")
    check ast.kind == nkSelect

  test "VALUES tail accepts FETCH":
    let ast = parseSql("VALUES (1), (2), (3) ORDER BY 1 DESC FETCH FIRST 2 ROWS ONLY")
    check ast.kind == nkValues
    check ast.findClause(nkLimitClause).children[0].intVal == 2

  test "set-operation tail keeps WITH TIES":
    let ast = parseSql(
      "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 DESC FETCH FIRST 1 ROW WITH TIES")
    check ast.findClause(nkLimitClause).limitWithTies == true

suite "Aggregate FILTER / WITHIN GROUP / ORDER BY (issue #148)":

  test "FILTER (WHERE ...) attaches an nkAggFilterClause":
    let ast = parseSql("SELECT COUNT(*) FILTER (WHERE v > 10) FROM t")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.funcStar == true
    check call.children[^1].kind == nkAggFilterClause
    check call.children[^1].children.len == 1
    check call.children[^1].children[0].kind == nkBinaryOp

  test "FILTER combines with DISTINCT arguments and OVER stays last":
    let ast = parseSql(
      "SELECT SUM(DISTINCT v) FILTER (WHERE v > 0) OVER () FROM t")
    let call = ast.children[0].children[0]
    check call.funcDistinct == true
    check call.children[^2].kind == nkAggFilterClause
    check call.children[^1].kind == nkWindowSpec

  test "aggregate-local ORDER BY attaches an nkOrderByClause child":
    let ast = parseSql(
      "SELECT STRING_AGG(name, ',' ORDER BY v DESC, name ASC) FROM t")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.children.len == 4
    let orderBy = call.children[^1]
    check orderBy.kind == nkOrderByClause
    check orderBy.children.len == 2
    check orderBy.children[0].orderAsc == 0
    check orderBy.children[1].orderAsc == 1

  test "WITHIN GROUP attaches an nkWithinGroupClause child":
    let ast = parseSql(
      "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) FROM t")
    let call = ast.children[0].children[0]
    check call.kind == nkFunctionCall
    check call.children.len == 3
    let within = call.children[^1]
    check within.kind == nkWithinGroupClause
    check within.children.len == 1

  test "WITHIN GROUP followed by FILTER keeps clause order":
    let ast = parseSql(
      "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) " &
      "FILTER (WHERE g = 'a') FROM t")
    let call = ast.children[0].children[0]
    check call.children[^2].kind == nkWithinGroupClause
    check call.children[^1].kind == nkAggFilterClause

  test "filter and within stay usable as implicit aliases":
    let aliased = parseSql("SELECT COUNT(*) filter FROM t")
    check aliased.children[0].children[0].kind == nkAlias
    check aliased.children[0].children[0].aliasName == "filter"

    let explicit = parseSql("SELECT COUNT(*) AS filter FROM t")
    check explicit.children[0].children[0].aliasName == "filter"

    let within = parseSql("SELECT COUNT(*) within FROM t")
    check within.children[0].children[0].aliasName == "within"

  test "FILTER without WHERE is a parse error":
    check parseErrorMessage("SELECT COUNT(*) FILTER (v > 10) FROM t").contains(
      "expected tkWhere")

  test "WITHIN GROUP without ORDER BY is a parse error":
    check parseErrorMessage(
      "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (v) FROM t").contains(
      "expected tkOrder")

  test "argument ORDER BY cannot combine with WITHIN GROUP":
    check parseErrorMessage(
      "SELECT PERCENTILE_DISC(0.5 ORDER BY v) WITHIN GROUP (ORDER BY v) FROM t"
    ).contains("cannot combine an aggregate ORDER BY argument with WITHIN GROUP")
