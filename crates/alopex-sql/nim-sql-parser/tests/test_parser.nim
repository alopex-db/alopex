## Comprehensive tests for Alopex SQL Parser
## Covers tokenizer, expression, DML, and DDL tests ported from the Rust test suite.

import unittest
import ../src/[ast, lexer, parser]

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
    check lex.nextToken().kind == tkWhere

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

  test "SELECT with WHERE":
    let ast = parseSql("SELECT * FROM users WHERE id = 1")
    check ast.kind == nkSelect

  test "SELECT with column alias using AS":
    let ast = parseSql("SELECT name AS user_name FROM users")
    check ast.kind == nkSelect
    let col = ast.children[0].children[0]
    check col.kind == nkAlias
    check col.aliasName == "user_name"

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
    var found = false
    for child in ast.children:
      if child.kind == nkLimitClause:
        found = true
        check child.children.len == 2
        check child.children[0].intVal == 10
        check child.children[1].intVal == 20
    check found

  test "SELECT with multiple columns":
    let ast = parseSql("SELECT id, name, email FROM users")
    check ast.kind == nkSelect
    let cols = ast.children[0]
    check cols.kind == nkExprList
    check cols.children.len == 3

  test "trailing semicolon":
    let ast = parseSql("SELECT 1;")
    check ast.kind == nkSelect

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
    check cols.children[1].kind == nkFunctionCall
    check cols.children[1].children[0].strVal == "CAST"
    check cols.children[2].children[0].strVal == "NOW"

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
