## Comprehensive tests for Alopex SQL Parser
## Covers tokenizer, expression, DML, and DDL tests ported from the Rust test suite.

import unittest
import ../src/[ast, lexer, parser]

# ---------------------------------------------------------------------------
# Tokenizer tests
# ---------------------------------------------------------------------------

suite "Tokenizer":

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
    check ast.children[1].kind == nkExprList  # column list
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
    check ast.children[1].kind == nkExprList  # col list
    check ast.children[2].kind == nkExprList  # row 1
    check ast.children[3].kind == nkExprList  # row 2
    check ast.children[4].kind == nkExprList  # row 3

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
