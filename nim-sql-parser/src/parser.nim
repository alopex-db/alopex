## SQL Parser for Alopex DB
##
## Recursive-descent parser that converts token stream into AST.
## Supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE.

import std/[strutils, tables]
import lexer, ast

type
  Parser* = object
    lex: Lexer
    current: Token
    errors*: seq[string]

  ParseError* = object of CatchableError

proc initParser*(input: string): Parser =
  result.lex = initLexer(input)
  result.current = result.lex.nextToken()

proc error(p: var Parser, msg: string) =
  let errMsg = "Parse error at line " & $p.current.line & ", col " & $p.current.col &
    ": " & msg & " (got " & $p.current.kind & " '" & p.current.value & "')"
  p.errors.add(errMsg)
  raise newException(ParseError, errMsg)

proc advance(p: var Parser): Token =
  result = p.current
  p.current = p.lex.nextToken()

proc expect(p: var Parser, kind: TokenKind): Token =
  if p.current.kind != kind:
    p.error("expected " & $kind)
  result = p.advance()

proc check(p: Parser, kind: TokenKind): bool =
  p.current.kind == kind

proc checkKeyword(p: Parser, kinds: set[TokenKind]): bool =
  p.current.kind in kinds

# Forward declarations
proc parseExpr(p: var Parser): SqlNode
proc parseSelectStmt(p: var Parser): SqlNode
proc parseInsertStmt(p: var Parser): SqlNode
proc parseUpdateStmt(p: var Parser): SqlNode
proc parseDeleteStmt(p: var Parser): SqlNode
proc parseCreateTableStmt(p: var Parser): SqlNode
proc parseDropTableStmt(p: var Parser): SqlNode

# --- Expression parsing (precedence climbing) ---

proc parsePrimary(p: var Parser): SqlNode =
  case p.current.kind
  of tkInteger:
    let tok = p.advance()
    result = newIntLit(parseBiggestInt(tok.value))
  of tkFloat:
    let tok = p.advance()
    result = newFloatLit(parseFloat(tok.value))
  of tkString:
    let tok = p.advance()
    result = newStringLit(tok.value)
  of tkTrue:
    discard p.advance()
    result = newBoolLit(true)
  of tkFalse:
    discard p.advance()
    result = newBoolLit(false)
  of tkNull:
    discard p.advance()
    result = newNull()
  of tkStar:
    discard p.advance()
    result = newStar()
  of tkLParen:
    discard p.advance()
    result = p.parseExpr()
    discard p.expect(tkRParen)
  of tkNot:
    discard p.advance()
    result = newUnaryOp(opNot, p.parsePrimary())
  of tkMinus:
    discard p.advance()
    result = newUnaryOp(opNeg, p.parsePrimary())
  of tkIdent:
    let tok = p.advance()
    if p.check(tkLParen):
      # Function call: func(args...)
      discard p.advance() # (
      result = newNode(nkFunctionCall)
      result.children.add(newIdent(tok.value))
      if not p.check(tkRParen):
        result.children.add(p.parseExpr())
        while p.check(tkComma):
          discard p.advance()
          result.children.add(p.parseExpr())
      discard p.expect(tkRParen)
    elif p.check(tkDot):
      # table.column
      discard p.advance()
      let col = p.expect(tkIdent)
      result = newNode(nkColumnRef)
      result.children.add(newIdent(tok.value))
      result.children.add(newIdent(col.value))
    else:
      result = newIdent(tok.value)
  else:
    p.error("unexpected token in expression")

proc parseMulDiv(p: var Parser): SqlNode =
  result = p.parsePrimary()
  while p.current.kind in {tkStar, tkSlash, tkPercent}:
    let op = case p.current.kind
      of tkStar:    opMul
      of tkSlash:   opDiv
      of tkPercent: opMod
      else: opMul  # unreachable
    discard p.advance()
    result = newBinaryOp(op, result, p.parsePrimary())

proc parseAddSub(p: var Parser): SqlNode =
  result = p.parseMulDiv()
  while p.current.kind in {tkPlus, tkMinus}:
    let op = if p.current.kind == tkPlus: opAdd else: opSub
    discard p.advance()
    result = newBinaryOp(op, result, p.parseMulDiv())

proc parseComparison(p: var Parser): SqlNode =
  result = p.parseAddSub()

  # Handle NOT BETWEEN / NOT LIKE / NOT IN before IS
  if p.check(tkNot):
    discard p.advance()
    if p.check(tkBetween):
      discard p.advance()
      let low = p.parseAddSub()
      discard p.expect(tkAnd)
      let high = p.parseAddSub()
      let range = newNode(nkExprList)
      range.children.add(low)
      range.children.add(high)
      result = newBinaryOp(opNotBetween, result, range)
    elif p.check(tkLike):
      discard p.advance()
      let pattern = p.parseAddSub()
      if p.current.kind == tkIdent and p.current.value.toLowerAscii() == "escape":
        discard p.advance()
        let esc = p.parseAddSub()
        let pair = newNode(nkExprList)
        pair.children.add(pattern)
        pair.children.add(esc)
        result = newBinaryOp(opNotLike, result, pair)
      else:
        result = newBinaryOp(opNotLike, result, pattern)
    elif p.check(tkIn):
      discard p.advance()
      discard p.expect(tkLParen)
      let list = newNode(nkExprList)
      list.children.add(p.parseExpr())
      while p.check(tkComma):
        discard p.advance()
        list.children.add(p.parseExpr())
      discard p.expect(tkRParen)
      result = newBinaryOp(opNotIn, result, list)
    else:
      # bare NOT — wrap operand
      result = newUnaryOp(opNot, result)
    return

  # Handle IS NULL / IS NOT NULL
  if p.check(tkIs):
    discard p.advance()
    if p.check(tkNot):
      discard p.advance()
      discard p.expect(tkNull)
      result = newUnaryOp(opIsNotNull, result)
    else:
      discard p.expect(tkNull)
      result = newUnaryOp(opIsNull, result)
    return

  let opMap = {
    tkEq: opEq, tkNeq: opNeq,
    tkLt: opLt, tkLe: opLe,
    tkGt: opGt, tkGe: opGe,
  }.toTable

  if p.current.kind in opMap:
    let op = opMap[p.current.kind]
    discard p.advance()
    let right = p.parseAddSub()
    result = newBinaryOp(op, result, right)
  elif p.check(tkLike):
    discard p.advance()
    let pattern = p.parseAddSub()
    if p.current.kind == tkIdent and p.current.value.toLowerAscii() == "escape":
      discard p.advance()
      let esc = p.parseAddSub()
      let pair = newNode(nkExprList)
      pair.children.add(pattern)
      pair.children.add(esc)
      result = newBinaryOp(opLike, result, pair)
    else:
      result = newBinaryOp(opLike, result, pattern)
  elif p.check(tkIn):
    discard p.advance()
    discard p.expect(tkLParen)
    let list = newNode(nkExprList)
    list.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      list.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    result = newBinaryOp(opIn, result, list)
  elif p.check(tkBetween):
    discard p.advance()
    let low = p.parseAddSub()
    discard p.expect(tkAnd)
    let high = p.parseAddSub()
    let range = newNode(nkExprList)
    range.children.add(low)
    range.children.add(high)
    result = newBinaryOp(opBetween, result, range)

proc parseAndExpr(p: var Parser): SqlNode =
  result = p.parseComparison()
  while p.check(tkAnd):
    discard p.advance()
    result = newBinaryOp(opAnd, result, p.parseComparison())

proc parseExpr(p: var Parser): SqlNode =
  result = p.parseAndExpr()
  while p.check(tkOr):
    discard p.advance()
    result = newBinaryOp(opOr, result, p.parseAndExpr())

# --- Statement parsing ---

proc parseSelectItem(p: var Parser): SqlNode =
  result = p.parseExpr()
  if p.check(tkAs):
    discard p.advance()
    let alias = p.expect(tkIdent)
    result = SqlNode(kind: nkAlias, aliasExpr: result, aliasName: alias.value)
  elif p.check(tkIdent):
    # Implicit column alias: expression followed by a bare identifier
    # that is not a reserved keyword terminating the select list
    let alias = p.advance()
    result = SqlNode(kind: nkAlias, aliasExpr: result, aliasName: alias.value)

proc parseSelectList(p: var Parser): seq[SqlNode] =
  result.add(p.parseSelectItem())
  while p.check(tkComma):
    discard p.advance()
    result.add(p.parseSelectItem())

proc parseTableRef(p: var Parser): SqlNode =
  let name = p.expect(tkIdent)
  result = newIdent(name.value)
  if p.check(tkAs):
    discard p.advance()
    let alias = p.expect(tkIdent)
    result = SqlNode(kind: nkAlias, aliasExpr: result, aliasName: alias.value)
  elif p.check(tkIdent) and p.current.kind != tkOn and
       p.current.kind != tkWhere and p.current.kind != tkOrder and
       p.current.kind != tkGroup and p.current.kind != tkLimit:
    # Implicit alias
    let alias = p.advance()
    result = SqlNode(kind: nkAlias, aliasExpr: result, aliasName: alias.value)

proc parseFromClause(p: var Parser): SqlNode =
  result = newNode(nkFromClause)
  result.children.add(p.parseTableRef())

  # JOIN clauses
  while p.current.kind in {tkJoin, tkInner, tkLeft, tkRight, tkFull, tkCross}:
    var jk: JoinKind
    case p.current.kind
    of tkInner:
      jk = jkInner
      discard p.advance()
      discard p.expect(tkJoin)
    of tkLeft:
      jk = jkLeft
      discard p.advance()
      if p.check(tkOuter): discard p.advance()
      discard p.expect(tkJoin)
    of tkRight:
      jk = jkRight
      discard p.advance()
      if p.check(tkOuter): discard p.advance()
      discard p.expect(tkJoin)
    of tkFull:
      jk = jkFull
      discard p.advance()
      if p.check(tkOuter): discard p.advance()
      discard p.expect(tkJoin)
    of tkCross:
      jk = jkCross
      discard p.advance()
      discard p.expect(tkJoin)
    of tkJoin:
      jk = jkInner
      discard p.advance()
    else:
      break

    let table = p.parseTableRef()
    var cond: SqlNode = nil
    if jk != jkCross and p.check(tkOn):
      discard p.advance()
      cond = p.parseExpr()

    let joinNode = SqlNode(kind: nkJoin, joinKind: jk,
                           joinLeft: result.children[^1],
                           joinRight: table, joinCond: cond)
    result.children[^1] = joinNode

  # Additional comma-separated tables
  while p.check(tkComma):
    discard p.advance()
    result.children.add(p.parseTableRef())

proc parseSelectStmt(p: var Parser): SqlNode =
  discard p.expect(tkSelect)
  result = newNode(nkSelect)

  # DISTINCT
  if p.check(tkDistinct):
    discard p.advance()
    result.children.add(newIdent("DISTINCT"))

  # Select list
  let selectList = newNode(nkExprList)
  selectList.children = p.parseSelectList()
  result.children.add(selectList)

  # FROM
  if p.check(tkFrom):
    discard p.advance()
    result.children.add(p.parseFromClause())

  # WHERE
  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

  # GROUP BY
  if p.check(tkGroup):
    discard p.advance()
    discard p.expect(tkBy)
    let groupBy = newNode(nkGroupByClause)
    groupBy.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      groupBy.children.add(p.parseExpr())
    result.children.add(groupBy)

    # HAVING
    if p.check(tkHaving):
      discard p.advance()
      let having = newNode(nkHavingClause)
      having.children.add(p.parseExpr())
      result.children.add(having)

  # ORDER BY
  if p.check(tkOrder):
    discard p.advance()
    discard p.expect(tkBy)
    let orderBy = newNode(nkOrderByClause)
    let expr = p.parseExpr()
    if p.check(tkAsc):
      discard p.advance()
      let alias = SqlNode(kind: nkAlias, aliasExpr: expr, aliasName: "ASC")
      orderBy.children.add(alias)
    elif p.check(tkDesc):
      discard p.advance()
      let alias = SqlNode(kind: nkAlias, aliasExpr: expr, aliasName: "DESC")
      orderBy.children.add(alias)
    else:
      orderBy.children.add(expr)
    while p.check(tkComma):
      discard p.advance()
      let expr2 = p.parseExpr()
      if p.check(tkAsc):
        discard p.advance()
        orderBy.children.add(SqlNode(kind: nkAlias, aliasExpr: expr2, aliasName: "ASC"))
      elif p.check(tkDesc):
        discard p.advance()
        orderBy.children.add(SqlNode(kind: nkAlias, aliasExpr: expr2, aliasName: "DESC"))
      else:
        orderBy.children.add(expr2)
    result.children.add(orderBy)

  # LIMIT
  if p.check(tkLimit):
    discard p.advance()
    let limitNode = newNode(nkLimitClause)
    limitNode.children.add(p.parseExpr())
    if p.check(tkOffset):
      discard p.advance()
      limitNode.children.add(p.parseExpr())
    result.children.add(limitNode)

proc parseInsertStmt(p: var Parser): SqlNode =
  discard p.expect(tkInsert)
  discard p.expect(tkInto)
  result = newNode(nkInsert)

  let table = p.expect(tkIdent)
  result.children.add(newIdent(table.value))

  # Column list (optional)
  if p.check(tkLParen):
    discard p.advance()
    let cols = newNode(nkExprList)
    cols.children.add(newIdent(p.expect(tkIdent).value))
    while p.check(tkComma):
      discard p.advance()
      cols.children.add(newIdent(p.expect(tkIdent).value))
    discard p.expect(tkRParen)
    result.children.add(cols)

  # VALUES — one or more rows
  discard p.expect(tkValues)
  # Parse first row
  discard p.expect(tkLParen)
  let firstRow = newNode(nkExprList)
  firstRow.children.add(p.parseExpr())
  while p.check(tkComma):
    discard p.advance()
    firstRow.children.add(p.parseExpr())
  discard p.expect(tkRParen)
  result.children.add(firstRow)
  # Additional rows
  while p.check(tkComma):
    discard p.advance()
    discard p.expect(tkLParen)
    let row = newNode(nkExprList)
    row.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      row.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    result.children.add(row)

proc parseUpdateStmt(p: var Parser): SqlNode =
  discard p.expect(tkUpdate)
  result = newNode(nkUpdate)

  let table = p.expect(tkIdent)
  result.children.add(newIdent(table.value))

  discard p.expect(tkSet)
  let setList = newNode(nkExprList)
  # col = expr
  let col1 = p.expect(tkIdent)
  discard p.expect(tkEq)
  let val1 = p.parseExpr()
  setList.children.add(newBinaryOp(opEq, newIdent(col1.value), val1))
  while p.check(tkComma):
    discard p.advance()
    let col = p.expect(tkIdent)
    discard p.expect(tkEq)
    let val = p.parseExpr()
    setList.children.add(newBinaryOp(opEq, newIdent(col.value), val))
  result.children.add(setList)

  # WHERE
  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

proc parseDeleteStmt(p: var Parser): SqlNode =
  discard p.expect(tkDelete)
  discard p.expect(tkFrom)
  result = newNode(nkDelete)

  let table = p.expect(tkIdent)
  result.children.add(newIdent(table.value))

  # WHERE
  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

proc parseTypeName(p: var Parser): SqlNode =
  result = newNode(nkTypeName)
  let typeTok = p.advance()
  result.children.add(newIdent(typeTok.value))
  # VARCHAR(255) etc.
  if p.check(tkLParen):
    discard p.advance()
    result.children.add(p.parseExpr())
    if p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
    discard p.expect(tkRParen)

proc parseColumnDef(p: var Parser): SqlNode =
  let name = p.expect(tkIdent)
  let typeName = p.parseTypeName()
  result = SqlNode(kind: nkColumnDef, colName: name.value, colType: typeName,
                   colConstraints: @[])

  # Constraints
  while true:
    if p.check(tkPrimary):
      discard p.advance()
      discard p.expect(tkKey)
      let c = newNode(nkConstraint)
      c.children.add(newIdent("PRIMARY KEY"))
      result.colConstraints.add(c)
    elif p.check(tkNot):
      discard p.advance()
      discard p.expect(tkNull)
      let c = newNode(nkConstraint)
      c.children.add(newIdent("NOT NULL"))
      result.colConstraints.add(c)
    elif p.check(tkUnique):
      discard p.advance()
      let c = newNode(nkConstraint)
      c.children.add(newIdent("UNIQUE"))
      result.colConstraints.add(c)
    elif p.check(tkDefault):
      discard p.advance()
      let c = newNode(nkConstraint)
      c.children.add(newIdent("DEFAULT"))
      c.children.add(p.parseExpr())
      result.colConstraints.add(c)
    else:
      break

proc parseCreateTableStmt(p: var Parser): SqlNode =
  discard p.expect(tkCreate)
  discard p.expect(tkTable)
  result = newNode(nkCreateTable)

  # IF NOT EXISTS
  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkNot)
    # "EXISTS" is a keyword
    discard p.expect(tkExists)
    result.children.add(newIdent("IF NOT EXISTS"))

  let table = p.expect(tkIdent)
  result.children.add(newIdent(table.value))

  discard p.expect(tkLParen)
  result.children.add(p.parseColumnDef())
  while p.check(tkComma):
    discard p.advance()
    # Check for table-level constraints
    if p.check(tkPrimary) or p.check(tkUnique) or p.check(tkForeign) or p.check(tkConstraint):
      let c = newNode(nkConstraint)
      c.children.add(newIdent(p.advance().value))
      # Simplified: skip to closing paren of constraint
      if p.check(tkKey): discard p.advance()
      if p.check(tkLParen):
        discard p.advance()
        c.children.add(newIdent(p.expect(tkIdent).value))
        while p.check(tkComma):
          discard p.advance()
          c.children.add(newIdent(p.expect(tkIdent).value))
        discard p.expect(tkRParen)
      result.children.add(c)
    else:
      result.children.add(p.parseColumnDef())
  discard p.expect(tkRParen)

proc parseDropTableStmt(p: var Parser): SqlNode =
  discard p.expect(tkDrop)
  discard p.expect(tkTable)
  result = newNode(nkDropTable)

  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkExists)
    result.children.add(newIdent("IF EXISTS"))

  let table = p.expect(tkIdent)
  result.children.add(newIdent(table.value))

proc parseStatement*(p: var Parser): SqlNode =
  case p.current.kind
  of tkSelect:
    result = p.parseSelectStmt()
  of tkInsert:
    result = p.parseInsertStmt()
  of tkUpdate:
    result = p.parseUpdateStmt()
  of tkDelete:
    result = p.parseDeleteStmt()
  of tkCreate:
    result = p.parseCreateTableStmt()
  of tkDrop:
    result = p.parseDropTableStmt()
  else:
    p.error("expected SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP)")

  # Optional semicolon
  if p.check(tkSemicolon):
    discard p.advance()

proc parseSql*(input: string): SqlNode =
  ## Parse a single SQL statement from string
  var p = initParser(input)
  result = p.parseStatement()
