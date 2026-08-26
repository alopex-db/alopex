## SQL Parser for Alopex DB
##
## Recursive-descent parser that converts token stream into the Nim AST.

import std/strutils
import lexer, ast

type
  Parser* = object
    lex: Lexer
    current: Token
    previous: Token
    errors*: seq[string]
    nestingDepth: int

  ParseError* = object of CatchableError

const
  MaxSyntacticNesting = 128
  NestingLimitError =
    "maximum SQL syntactic nesting depth of 128 exceeded"
  ClauseTerminators = {tkWhere, tkOrder, tkGroup, tkHaving, tkLimit, tkOffset,
                       tkJoin, tkInner, tkLeft, tkRight, tkFull, tkCross,
                       tkNatural, tkUsing, tkOn}
  TypeTokens = {tkInt, tkBigint, tkSmallint, tkFloatType, tkReal, tkDouble, tkDecimal,
                tkVarchar, tkChar, tkText, tkBlob, tkBoolean, tkBool,
                tkTimestamp, tkDate, tkTime, tkVector}
  OptionValueTokens = {tkIdent, tkString, tkInteger, tkFloat, tkHnsw, tkBtree,
                       tkCosine, tkL2, tkInner, tkText, tkBoolean, tkBool}
  # FETCH pagination (issue #152) reserves FETCH/NEXT/TIES/ONLY/ROW in the
  # lexer, but they stay legal identifiers wherever an identifier is
  # *mandatory* and no clause can start (issue #152 D16). Every `expectIdent`
  # call site is such a position: a table/column/alias/index/window/CTE name
  # the grammar requires, so accepting these tokens cannot make a parse
  # ambiguous.
  PaginationIdentTokens = {tkFetch, tkNext, tkTies, tkOnly, tkRow}
  # An *implicit* (bare) alias is optional, so the token must not be able to
  # start the clause that follows. `FETCH` starts the pagination tail there
  # and therefore stays reserved; the other four cannot begin any clause.
  ImplicitAliasTokens = {tkIdent, tkNext, tkTies, tkOnly, tkRow}

proc initParser*(input: string): Parser =
  result.lex = initLexer(input)
  result.current = result.lex.nextToken()

proc tokenSpan(tok: Token): Span =
  Span(start: Location(line: tok.line, column: tok.col),
       `end`: Location(line: tok.endLine, column: tok.endCol))

proc currentSpan(p: Parser): Span =
  tokenSpan(p.current)

proc error(p: var Parser, msg: string) =
  let errMsg = "Parse error at line " & $p.current.line & ", col " & $p.current.col &
    ": " & msg & " (got " & $p.current.kind & " '" & p.current.value & "')"
  p.errors.add(errMsg)
  raise newException(ParseError, errMsg)

proc enterNesting(p: var Parser) =
  if p.nestingDepth >= MaxSyntacticNesting:
    raise newException(ParseError, NestingLimitError)
  inc p.nestingDepth

proc leaveNesting(p: var Parser) =
  dec p.nestingDepth

proc advance(p: var Parser): Token =
  result = p.current
  p.previous = result
  p.current = p.lex.nextToken()

proc expect(p: var Parser, kind: TokenKind): Token =
  if p.current.kind != kind:
    p.error("expected " & $kind)
  result = p.advance()

proc check(p: Parser, kind: TokenKind): bool =
  p.current.kind == kind

proc checkContextual(p: Parser; value: string): bool =
  p.current.kind == tkIdent and p.current.value.cmpIgnoreCase(value) == 0

proc peekNext(p: Parser): TokenKind =
  ## One-token lookahead. The lexer is a value object, so advancing a copy
  ## never disturbs the live token stream. The input string is copied with
  ## the lexer, so call this only after a cheap contextual-keyword check.
  var lookahead = p.lex
  lookahead.nextToken().kind

proc peekNextIsContextual(p: Parser; value: string): bool =
  ## One-token lookahead for a contextual keyword (issue #149: the SETS in
  ## GROUPING SETS). Same copy-the-lexer discipline as peekNext.
  var lookahead = p.lex
  let tok = lookahead.nextToken()
  tok.kind == tkIdent and tok.value.cmpIgnoreCase(value) == 0

proc peekSecond(p: Parser): TokenKind =
  ## Two-token lookahead. LATERAL stays a contextual keyword (issue #151), so
  ## telling `FROM lateral (SELECT ...)` from a relation named `lateral` needs
  ## the token past the parenthesis. Same copy-the-lexer discipline as peekNext.
  var lookahead = p.lex
  discard lookahead.nextToken()
  lookahead.nextToken().kind

proc expectContextual(p: var Parser; value: string): Token =
  if not p.checkContextual(value):
    p.error("expected " & value)
  result = p.advance()

proc spanThrough(first, last: Span): Span =
  Span(start: first.start, `end`: last.`end`)

proc expectIdent(p: var Parser; context = "identifier"): Token =
  ## A mandatory identifier position. FETCH/NEXT/TIES/ONLY/ROW are lexer
  ## keywords for the FETCH pagination tail (issue #152) but remain legal
  ## names here, so `CREATE TABLE t (row INTEGER)`, `INSERT INTO t (next)`,
  ## `UPDATE t SET only = 1` and `... AS ties` keep parsing (issue #152 D16).
  if p.current.kind notin {tkIdent} + PaginationIdentTokens:
    p.error("expected " & context)
  result = p.advance()

proc expectExprIdent(p: var Parser; context = "identifier"): Token =
  ## FIRST/LAST/time are reserved by ORDER BY and type grammar, and
  ## FETCH/NEXT/TIES/ONLY/ROW by FETCH pagination (issue #152), but SQL-TS
  ## also uses them as ordinary function/column identifiers.
  if p.current.kind notin {tkIdent, tkFirst, tkLast, tkTime} +
                          PaginationIdentTokens:
    p.error("expected " & context)
  result = p.advance()

proc expectOptionValue(p: var Parser): Token =
  if p.current.kind notin OptionValueTokens:
    p.error("expected option value")
  result = p.advance()

proc makeAlias(expr: SqlNode, alias: string; span: Span = emptySpan();
               columns: seq[string] = @[]): SqlNode =
  SqlNode(kind: nkAlias, aliasExpr: expr, aliasName: alias,
          aliasColumns: columns, span: span,
          orderAsc: -1, nullsFirst: -1)

# Forward declarations
proc parseExpr(p: var Parser): SqlNode
proc parseConcat(p: var Parser): SqlNode
proc parseSelectStmt(p: var Parser): SqlNode
proc parseWithQueryStmt(p: var Parser): SqlNode
proc parseQueryStmt(p: var Parser): SqlNode
proc parseInsertStmt(p: var Parser): SqlNode
proc parseUpdateStmt(p: var Parser): SqlNode
proc parseDeleteStmt(p: var Parser): SqlNode
proc parseCreateStmt(p: var Parser): SqlNode
proc parseDropStmt(p: var Parser): SqlNode
proc parsePragmaStmt(p: var Parser): SqlNode
proc parseTypeName(p: var Parser): SqlNode
proc parseOrderByItem(p: var Parser): SqlNode

# --- Expression parsing (precedence climbing) ---

proc parseVectorLiteral(p: var Parser): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  let start = p.expect(tkLBracket)
  result = newNode(nkVectorLiteral, tokenSpan(start))
  var sawValue = false
  while not p.check(tkRBracket):
    var sign = 1.0
    if p.check(tkMinus):
      discard p.advance()
      sign = -1.0
    if p.current.kind notin {tkInteger, tkFloat}:
      p.error("expected vector numeric literal")
    let tok = p.advance()
    let value = if tok.kind == tkFloat: parseFloat(tok.value) else: parseFloat(tok.value)
    result.children.add(newFloatLit(value * sign, tokenSpan(tok)))
    sawValue = true
    if p.check(tkComma):
      discard p.advance()
    elif not p.check(tkRBracket):
      p.error("expected ',' or ']' in vector literal")
  if not sawValue:
    p.error("vector literal cannot be empty")
  discard p.expect(tkRBracket)

proc parseSubqueryInParens(p: var Parser): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  discard p.expect(tkLParen)
  if p.current.kind notin {tkSelect, tkValues, tkWith}:
    p.error("expected SELECT, VALUES, or WITH subquery")
  result = p.parseQueryStmt()
  discard p.expect(tkRParen)

proc parseExistsExpr(p: var Parser; negated: bool): SqlNode =
  let tok = p.expect(tkExists)
  result = newNode(nkExists, tokenSpan(tok))
  result.negated = negated
  result.children.add(p.parseSubqueryInParens())

proc parseWindowFrameBound(p: var Parser): SqlNode =
  let startTok = p.current
  if p.checkContextual("unbounded"):
    discard p.advance()
    if p.checkContextual("preceding"):
      let endTok = p.advance()
      result = SqlNode(kind: nkWindowFrameBound,
        frameBoundKind: wfbUnboundedPreceding, frameOffset: 0,
        span: Span(start: tokenSpan(startTok).start, `end`: tokenSpan(endTok).`end`),
        orderAsc: -1, nullsFirst: -1)
    elif p.checkContextual("following"):
      let endTok = p.advance()
      result = SqlNode(kind: nkWindowFrameBound,
        frameBoundKind: wfbUnboundedFollowing, frameOffset: 0,
        span: Span(start: tokenSpan(startTok).start, `end`: tokenSpan(endTok).`end`),
        orderAsc: -1, nullsFirst: -1)
    else:
      p.error("expected PRECEDING or FOLLOWING after UNBOUNDED")
  elif p.checkContextual("current"):
    discard p.advance()
    if not p.check(tkRow):
      p.error("expected row")
    let endTok = p.advance()
    result = SqlNode(kind: nkWindowFrameBound,
      frameBoundKind: wfbCurrentRow, frameOffset: 0,
      span: Span(start: tokenSpan(startTok).start, `end`: tokenSpan(endTok).`end`),
      orderAsc: -1, nullsFirst: -1)
  elif p.check(tkInteger):
    let offsetTok = p.advance()
    var offset: uint64
    try:
      offset = uint64(parseBiggestUInt(offsetTok.value))
    except ValueError:
      p.error("window frame offset exceeds unsigned 64-bit range")
    if p.checkContextual("preceding"):
      let endTok = p.advance()
      result = SqlNode(kind: nkWindowFrameBound,
        frameBoundKind: wfbPreceding, frameOffset: offset,
        span: Span(start: tokenSpan(startTok).start, `end`: tokenSpan(endTok).`end`),
        orderAsc: -1, nullsFirst: -1)
    elif p.checkContextual("following"):
      let endTok = p.advance()
      result = SqlNode(kind: nkWindowFrameBound,
        frameBoundKind: wfbFollowing, frameOffset: offset,
        span: Span(start: tokenSpan(startTok).start, `end`: tokenSpan(endTok).`end`),
        orderAsc: -1, nullsFirst: -1)
    else:
      p.error("expected PRECEDING or FOLLOWING after window frame offset")
  else:
    p.error("expected window frame bound")

proc parseWindowFrame(p: var Parser): SqlNode =
  let unitTok = p.advance()
  let unit = if unitTok.kind == tkRows: wfuRows else: wfuRange
  var startBound: SqlNode
  var endBound: SqlNode
  if p.check(tkBetween):
    discard p.advance()
    startBound = p.parseWindowFrameBound()
    discard p.expect(tkAnd)
    endBound = p.parseWindowFrameBound()
  else:
    startBound = p.parseWindowFrameBound()
    endBound = SqlNode(kind: nkWindowFrameBound,
      frameBoundKind: wfbCurrentRow, frameOffset: 0,
      span: startBound.span, orderAsc: -1, nullsFirst: -1)
  result = SqlNode(kind: nkWindowFrame, frameUnit: unit,
    frameStart: startBound, frameEnd: endBound,
    span: Span(start: tokenSpan(unitTok).start, `end`: endBound.span.`end`),
    orderAsc: -1, nullsFirst: -1)

proc parseWindowSpecContents(p: var Parser; window: SqlNode) =
  # A base window name, if present, is an identifier; the pagination keywords
  # stay usable as window names so `WINDOW row AS (...)` and `OVER (row ...)`
  # agree with the widened `expectIdent` (issue #152 D16).
  if p.current.kind in {tkIdent} + PaginationIdentTokens:
    let base = p.advance()
    window.children.add(newIdent(base.value, tokenSpan(base)))

  if p.check(tkPartition):
    let partitionTok = p.advance()
    discard p.expect(tkBy)
    let partitionBy = newNode(nkPartitionByClause, tokenSpan(partitionTok))
    partitionBy.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      partitionBy.children.add(p.parseExpr())
    window.children.add(partitionBy)

  if p.check(tkOrder):
    let orderTok = p.advance()
    discard p.expect(tkBy)
    let orderBy = newNode(nkOrderByClause, tokenSpan(orderTok))
    orderBy.children.add(p.parseOrderByItem())
    while p.check(tkComma):
      discard p.advance()
      orderBy.children.add(p.parseOrderByItem())
    window.children.add(orderBy)

  if p.current.kind in {tkRows, tkRange}:
    window.children.add(p.parseWindowFrame())

proc parseWindowSpecBody(p: var Parser): SqlNode =
  let openTok = p.expect(tkLParen)
  result = newNode(nkWindowSpec, tokenSpan(openTok))
  p.parseWindowSpecContents(result)
  let closeTok = p.expect(tkRParen)
  result.span = Span(start: tokenSpan(openTok).start,
                     `end`: tokenSpan(closeTok).`end`)

proc parseWindowSpec(p: var Parser): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  let overTok = p.expect(tkOver)
  if p.current.kind in {tkIdent} + PaginationIdentTokens:
    let base = p.advance()
    result = newNode(nkWindowSpec,
      Span(start: tokenSpan(overTok).start, `end`: tokenSpan(base).`end`))
    result.children.add(newIdent(base.value, tokenSpan(base)))
    return

  result = p.parseWindowSpecBody()
  result.span = Span(start: tokenSpan(overTok).start,
                     `end`: result.span.`end`)

proc parseOptionalOver(p: var Parser; functionCall: SqlNode) =
  if p.check(tkOver):
    functionCall.children.add(p.parseWindowSpec())

proc parseAggregateTail(p: var Parser; functionCall: SqlNode) =
  ## Shared post-`)` tail of every function call:
  ## [WITHIN GROUP (ORDER BY ...)] [FILTER (WHERE expr)] [OVER ...].
  ## FILTER and WITHIN are contextual identifiers (issue #148): they start a
  ## clause only when followed by `(` / GROUP, so `SELECT count(x) filter`
  ## still parses as an implicit alias.
  if p.checkContextual("within") and p.peekNext() == tkGroup:
    discard p.advance()
    discard p.expect(tkGroup)
    discard p.expect(tkLParen)
    let orderTok = p.expect(tkOrder)
    discard p.expect(tkBy)
    let clause = newNode(nkWithinGroupClause, tokenSpan(orderTok))
    clause.children.add(p.parseOrderByItem())
    while p.check(tkComma):
      discard p.advance()
      clause.children.add(p.parseOrderByItem())
    let closeTok = p.expect(tkRParen)
    clause.span = spanThrough(tokenSpan(orderTok), tokenSpan(closeTok))
    for child in functionCall.children:
      if child.kind == nkOrderByClause:
        p.error("cannot combine an aggregate ORDER BY argument with WITHIN GROUP")
    functionCall.children.add(clause)
  if p.checkContextual("filter") and p.peekNext() == tkLParen:
    let filterTok = p.advance()
    discard p.expect(tkLParen)
    discard p.expect(tkWhere)
    let clause = newNode(nkAggFilterClause, tokenSpan(filterTok))
    clause.children.add(p.parseExpr())
    let closeTok = p.expect(tkRParen)
    clause.span = spanThrough(tokenSpan(filterTok), tokenSpan(closeTok))
    functionCall.children.add(clause)
  p.parseOptionalOver(functionCall)

proc parseFunctionCall(p: var Parser; nameTok: Token): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  result = newNode(nkFunctionCall, tokenSpan(nameTok))
  discard p.expect(tkLParen)
  let normalizedName = case nameTok.value.toLowerAscii()
    of "substring": "SUBSTR"
    of "position": "STRPOS"
    else: nameTok.value
  result.children.add(newIdent(normalizedName, tokenSpan(nameTok)))
  if nameTok.value.toLowerAscii() == "substring" and not p.check(tkRParen):
    result.children.add(p.parseExpr())
    if p.check(tkFrom):
      discard p.advance()
      result.children.add(p.parseExpr())
      if p.check(tkFor):
        discard p.advance()
        result.children.add(p.parseExpr())
      discard p.expect(tkRParen)
      p.parseAggregateTail(result)
      return
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    p.parseAggregateTail(result)
    return
  if nameTok.value.toLowerAscii() == "position" and not p.check(tkRParen):
    let searched = p.parseConcat()
    if p.check(tkIn):
      discard p.advance()
      let source = p.parseExpr()
      result.children.add(source)
      result.children.add(searched)
      discard p.expect(tkRParen)
      p.parseAggregateTail(result)
      return
    result.children.add(searched)
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    p.parseAggregateTail(result)
    return
  if nameTok.value.toLowerAscii() == "trim" and not p.check(tkRParen):
    result.children.add(p.parseExpr())
    if p.check(tkFrom):
      discard p.advance()
      result.children.add(p.parseExpr())
      discard p.expect(tkRParen)
      p.parseAggregateTail(result)
      return
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    p.parseAggregateTail(result)
    return
  if not p.check(tkRParen):
    if p.check(tkStar):
      result.funcStar = true
      result.children.add(newStar(tokenSpan(p.advance())))
    else:
      if p.check(tkDistinct):
        result.funcDistinct = true
        discard p.advance()
      result.children.add(p.parseExpr())
      while p.check(tkComma):
        discard p.advance()
        result.children.add(p.parseExpr())
      if p.check(tkOrder):
        # Aggregate-local ordering: agg(expr [, ...] ORDER BY key [, ...]).
        let orderTok = p.advance()
        discard p.expect(tkBy)
        let orderBy = newNode(nkOrderByClause, tokenSpan(orderTok))
        orderBy.children.add(p.parseOrderByItem())
        while p.check(tkComma):
          discard p.advance()
          orderBy.children.add(p.parseOrderByItem())
        result.children.add(orderBy)
  discard p.expect(tkRParen)
  p.parseAggregateTail(result)

proc parseCastBody(p: var Parser; tok: Token; kind: SqlNodeKind): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  result = newNode(kind, tokenSpan(tok))
  discard p.expect(tkLParen)
  result.children.add(p.parseExpr())
  discard p.expect(tkAs)
  result.children.add(p.parseTypeName())
  discard p.expect(tkRParen)

proc parseCastExpr(p: var Parser): SqlNode =
  let tok = p.expect(tkCast)
  result = p.parseCastBody(tok, nkCast)

proc parseCaseExpr(p: var Parser): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  let start = p.expect(tkCase)
  result = newNode(nkCase, tokenSpan(start))

  if not p.check(tkWhen):
    result.caseOperand = p.parseExpr()

  if not p.check(tkWhen):
    p.error("expected at least one WHEN in CASE expression")

  while p.check(tkWhen):
    let whenTok = p.advance()
    let condition = p.parseExpr()
    discard p.expect(tkThen)
    let branchResult = p.parseExpr()
    let branchEnd = tokenSpan(p.previous)
    let branch = newNode(nkCaseWhen,
      spanThrough(tokenSpan(whenTok), branchEnd))
    branch.caseWhen = condition
    branch.caseThen = branchResult
    result.caseBranches.add(branch)

  if p.check(tkElse):
    discard p.advance()
    result.caseElse = p.parseExpr()

  let endTok = p.expect(tkEnd)
  result.span = spanThrough(tokenSpan(start), tokenSpan(endTok))

proc parsePrimary(p: var Parser): SqlNode =
  case p.current.kind
  of tkInteger:
    let tok = p.advance()
    result = newIntLit(parseBiggestInt(tok.value), tokenSpan(tok))
  of tkFloat:
    let tok = p.advance()
    result = newFloatLit(parseFloat(tok.value), tokenSpan(tok))
  of tkString:
    let tok = p.advance()
    result = newStringLit(tok.value, tokenSpan(tok))
  of tkInterval:
    let intervalTok = p.advance()
    let valueTok = p.expect(tkString)
    result = newIntervalLit(valueTok.value,
      Span(start: tokenSpan(intervalTok).start, `end`: tokenSpan(valueTok).`end`))
  of tkTrue:
    let tok = p.advance()
    result = newBoolLit(true, tokenSpan(tok))
  of tkFalse:
    let tok = p.advance()
    result = newBoolLit(false, tokenSpan(tok))
  of tkNull:
    let tok = p.advance()
    result = newNull(tokenSpan(tok))
  of tkStar:
    let tok = p.advance()
    result = newStar(tokenSpan(tok))
  of tkLBracket:
    result = p.parseVectorLiteral()
  of tkLParen:
    p.enterNesting()
    defer: p.leaveNesting()
    let start = p.advance()
    if p.current.kind in {tkSelect, tkValues, tkWith}:
      let subquery = p.parseQueryStmt()
      discard p.expect(tkRParen)
      result = newNode(nkScalarSubquery, tokenSpan(start))
      result.children.add(subquery)
    else:
      let first = p.parseExpr()
      if p.check(tkComma):
        result = newNode(nkRowConstructor, tokenSpan(start))
        result.children.add(first)
        while p.check(tkComma):
          discard p.advance()
          result.children.add(p.parseExpr())
      else:
        result = first
      discard p.expect(tkRParen)
  of tkExists:
    result = p.parseExistsExpr(false)
  of tkCast:
    result = p.parseCastExpr()
  of tkCase:
    result = p.parseCaseExpr()
  of tkNot:
    discard p.advance()
    if p.check(tkExists):
      result = p.parseExistsExpr(true)
    else:
      p.enterNesting()
      defer: p.leaveNesting()
      result = newUnaryOp(opNot, p.parsePrimary())
  of tkMinus:
    p.enterNesting()
    defer: p.leaveNesting()
    let tok = p.advance()
    result = newUnaryOp(opNeg, p.parsePrimary(), tokenSpan(tok))
  of tkQuestion:
    p.error("bind parameters are not yet supported; pass literal values " &
      "instead (prepared statements are tracked by issue #166)")
  of tkIdent, tkFirst, tkLast, tkTime, tkFetch, tkNext, tkTies, tkOnly, tkRow:
    let tok = p.advance()
    if tok.value.cmpIgnoreCase("try_cast") == 0 and p.check(tkLParen):
      result = p.parseCastBody(tok, nkTryCast)
    elif p.check(tkLParen):
      result = p.parseFunctionCall(tok)
    elif p.check(tkDot):
      discard p.advance()
      if p.check(tkStar):
        discard p.advance()
        result = newNode(nkQualifiedStar, tokenSpan(tok))
        result.children.add(newIdent(tok.value, tokenSpan(tok)))
      else:
        let col = p.expectExprIdent("column name")
        result = newNode(nkColumnRef, tokenSpan(tok))
        result.children.add(newIdent(tok.value, tokenSpan(tok)))
        result.children.add(newIdent(col.value, tokenSpan(col)))
    else:
      result = newIdent(tok.value, tokenSpan(tok))
  of tkNow, tkVector:
    let tok = p.advance()
    if p.check(tkLParen):
      result = p.parseFunctionCall(tok)
    else:
      p.error("expected function call")
  else:
    p.error("unexpected token in expression")

proc parseMulDiv(p: var Parser): SqlNode =
  result = p.parsePrimary()
  while p.current.kind in {tkStar, tkSlash, tkPercent}:
    let op = case p.current.kind
      of tkStar: opMul
      of tkSlash: opDiv
      of tkPercent: opMod
      else: opMul
    discard p.advance()
    result = newBinaryOp(op, result, p.parsePrimary())

proc parseAddSub(p: var Parser): SqlNode =
  result = p.parseMulDiv()
  while p.current.kind in {tkPlus, tkMinus}:
    let op = if p.current.kind == tkPlus: opAdd else: opSub
    discard p.advance()
    result = newBinaryOp(op, result, p.parseMulDiv())

proc parseConcat(p: var Parser): SqlNode =
  result = p.parseAddSub()
  while p.check(tkPipePipe):
    discard p.advance()
    result = newBinaryOp(opStringConcat, result, p.parseAddSub())

proc comparisonOp(kind: TokenKind): BinaryOpKind =
  case kind
  of tkEq: opEq
  of tkNeq: opNeq
  of tkLt: opLt
  of tkLe: opLe
  of tkGt: opGt
  of tkGe: opGe
  else: opEq

proc parseQuantified(p: var Parser; left: SqlNode; op: BinaryOpKind): SqlNode =
  let quantTok = p.advance()
  result = newNode(nkQuantified, tokenSpan(quantTok))
  result.quantifier = if quantTok.kind == tkAll: qkAll else: qkAny
  result.children.add(left)
  result.children.add(newIdent($op, tokenSpan(quantTok)))
  result.children.add(p.parseSubqueryInParens())

proc parseInExpr(p: var Parser; left: SqlNode; negated: bool): SqlNode =
  p.enterNesting()
  defer: p.leaveNesting()
  discard p.expect(tkIn)
  discard p.expect(tkLParen)
  if p.current.kind in {tkSelect, tkValues, tkWith}:
    result = newNode(nkInSubquery)
    result.negated = negated
    result.children.add(left)
    result.children.add(p.parseQueryStmt())
    discard p.expect(tkRParen)
  else:
    let list = newNode(nkExprList)
    if p.check(tkRParen):
      p.error("IN list must contain at least one expression")
    list.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      list.children.add(p.parseExpr())
    discard p.expect(tkRParen)
    result = newBinaryOp(if negated: opNotIn else: opIn, left, list)

proc parsePattern(p: var Parser; left: SqlNode; op: BinaryOpKind; allowEscape: bool): SqlNode =
  let pattern = p.parseConcat()
  if allowEscape and p.check(tkEscape):
    discard p.advance()
    let esc = p.parseConcat()
    let pair = newNode(nkExprList)
    pair.children.add(pattern)
    pair.children.add(esc)
    return newBinaryOp(op, left, pair)
  newBinaryOp(op, left, pattern)

proc parseComparison(p: var Parser): SqlNode =
  result = p.parseConcat()

  if p.check(tkNot):
    discard p.advance()
    if p.check(tkBetween):
      discard p.advance()
      let low = p.parseConcat()
      discard p.expect(tkAnd)
      let high = p.parseConcat()
      let range = newNode(nkExprList)
      range.children.add(low)
      range.children.add(high)
      result = newBinaryOp(opNotBetween, result, range)
    elif p.check(tkLike) or p.check(tkILike) or p.check(tkGlob) or p.check(tkSimilar):
      let opToken = p.advance()
      let op = case opToken.kind
        of tkLike: opNotLike
        of tkILike: opNotILike
        of tkGlob: opNotGlob
        else:
          discard p.expect(tkTo)
          opNotSimilarTo
      result = p.parsePattern(result, op, op in {opNotLike, opNotILike})
    elif p.check(tkIn):
      result = p.parseInExpr(result, true)
    else:
      result = newUnaryOp(opNot, result)
    return

  if p.check(tkIs):
    discard p.advance()
    let negated = p.check(tkNot)
    if negated:
      discard p.advance()
    if p.check(tkNull):
      discard p.advance()
      result = newUnaryOp(if negated: opIsNotNull else: opIsNull, result)
    elif p.current.kind in {tkTrue, tkFalse} or p.checkContextual("unknown"):
      let value = p.advance()
      let predicate = newNode(nkTruthPredicate, tokenSpan(value))
      predicate.negated = negated
      predicate.children.add(result)
      predicate.children.add(newIdent(value.value, tokenSpan(value)))
      result = predicate
    elif p.check(tkDistinct):
      let distinctTok = p.advance()
      discard p.expect(tkFrom)
      let predicate = newNode(nkIsDistinctFrom, tokenSpan(distinctTok))
      predicate.negated = negated
      predicate.children.add(result)
      predicate.children.add(p.parseConcat())
      result = predicate
    else:
      discard p.expect(tkNull)
    return

  if p.current.kind in {tkEq, tkNeq, tkLt, tkLe, tkGt, tkGe}:
    let op = comparisonOp(p.current.kind)
    discard p.advance()
    if p.current.kind in {tkAny, tkSome, tkAll}:
      result = p.parseQuantified(result, op)
    else:
      result = newBinaryOp(op, result, p.parseConcat())
  elif p.check(tkLike) or p.check(tkILike) or p.check(tkGlob) or p.check(tkSimilar):
    let opToken = p.advance()
    let op = case opToken.kind
      of tkLike: opLike
      of tkILike: opILike
      of tkGlob: opGlob
      else:
        discard p.expect(tkTo)
        opSimilarTo
    result = p.parsePattern(result, op, op in {opLike, opILike})
  elif p.check(tkIn):
    result = p.parseInExpr(result, false)
  elif p.check(tkBetween):
    discard p.advance()
    let low = p.parseConcat()
    discard p.expect(tkAnd)
    let high = p.parseConcat()
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

# --- SELECT / FROM parsing ---

proc parseSelectItem(p: var Parser): SqlNode =
  result = p.parseExpr()
  if p.check(tkAs):
    discard p.advance()
    let alias = p.expectExprIdent("alias")
    result = makeAlias(result, alias.value, tokenSpan(alias))
  elif p.current.kind in ImplicitAliasTokens:
    let alias = p.advance()
    result = makeAlias(result, alias.value, tokenSpan(alias))

proc parseSelectList(p: var Parser): seq[SqlNode] =
  result.add(p.parseSelectItem())
  while p.check(tkComma):
    discard p.advance()
    result.add(p.parseSelectItem())

proc parseOptionalAlias(p: var Parser; item: SqlNode): SqlNode =
  result = item
  var aliasToken: Token
  var hasAlias = false
  if p.check(tkAs):
    discard p.advance()
    aliasToken = p.expectIdent("alias")
    hasAlias = true
  elif p.current.kind in ImplicitAliasTokens and
      p.current.kind notin ClauseTerminators:
    aliasToken = p.advance()
    hasAlias = true

  if hasAlias:
    var columns: seq[string] = @[]
    if p.check(tkLParen):
      discard p.advance()
      columns.add(p.expectIdent("table alias column name").value)
      while p.check(tkComma):
        discard p.advance()
        columns.add(p.expectIdent("table alias column name").value)
      discard p.expect(tkRParen)
    # Issue #151: a column-name list is accepted for every relation a FROM item
    # can produce - base tables and table functions as well as derived tables.
    if columns.len > 0 and item.kind notin {nkFromDerived, nkFromFunction, nkIdentifier}:
      p.error("column aliases are only supported for FROM-clause relations")
    result = makeAlias(item, aliasToken.value, tokenSpan(aliasToken), columns)

proc parseTableFunction(p: var Parser; name: Token; lateral: bool): SqlNode =
  ## `name(arg, ...)` in FROM position (issue #151). children[0] is the function
  ## name; the remaining children are the argument expressions.
  p.enterNesting()
  defer: p.leaveNesting()
  discard p.expect(tkLParen)
  result = newNode(nkFromFunction, tokenSpan(name))
  result.lateral = lateral
  result.children.add(newIdent(name.value, tokenSpan(name)))
  if not p.check(tkRParen):
    result.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
  discard p.expect(tkRParen)

proc parseFromItem(p: var Parser): SqlNode =
  # LATERAL is contextual: it only introduces a FROM item when a subquery or a
  # table function follows, so a relation named `lateral` keeps working.
  var lateral = false
  if p.checkContextual("lateral"):
    let next = p.peekNext()
    if (next == tkLParen and p.peekSecond() in {tkSelect, tkValues, tkWith}) or
        (next == tkIdent and p.peekSecond() == tkLParen):
      discard p.advance()
      lateral = true

  if p.check(tkLParen):
    p.enterNesting()
    defer: p.leaveNesting()
    let start = p.advance()
    if p.current.kind in {tkSelect, tkValues, tkWith}:
      let subquery = p.parseQueryStmt()
      discard p.expect(tkRParen)
      result = newNode(nkFromDerived, tokenSpan(start))
      result.lateral = lateral
      result.children.add(subquery)
      result = p.parseOptionalAlias(result)
    else:
      p.error("expected SELECT, VALUES, or WITH in FROM derived table")
  else:
    let name = p.expectIdent("table name")
    if p.check(tkLParen):
      result = p.parseTableFunction(name, lateral)
    else:
      result = newIdent(name.value, tokenSpan(name))
    result = p.parseOptionalAlias(result)

proc parseUsingClause(p: var Parser): seq[string] =
  discard p.expect(tkUsing)
  discard p.expect(tkLParen)
  result.add(p.expectIdent("USING column").value)
  while p.check(tkComma):
    discard p.advance()
    result.add(p.expectIdent("USING column").value)
  discard p.expect(tkRParen)

proc parseFromClause(p: var Parser): SqlNode =
  result = newNode(nkFromClause, p.currentSpan())
  var item = p.parseFromItem()

  while p.current.kind in {tkNatural, tkJoin, tkInner, tkLeft, tkRight, tkFull, tkCross}:
    var natural = false
    if p.check(tkNatural):
      natural = true
      discard p.advance()

    var jk = jkInner
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
      p.error("expected JOIN")

    let right = p.parseFromItem()
    var cond: SqlNode = nil
    var usingCols: seq[string] = @[]
    if p.check(tkOn):
      discard p.advance()
      cond = p.parseExpr()
    elif p.check(tkUsing):
      usingCols = p.parseUsingClause()
    item = newJoin(jk, item, right, cond, usingCols, natural)

  while p.check(tkComma):
    discard p.advance()
    let right = p.parseFromItem()
    item = newJoin(jkCross, item, right)

  result.children.add(item)

proc parseOrderByItem(p: var Parser): SqlNode =
  result = p.parseExpr()
  if p.check(tkAsc):
    let tok = p.advance()
    result = makeAlias(result, "ASC", tokenSpan(tok))
    result.orderAsc = 1
  elif p.check(tkDesc):
    let tok = p.advance()
    result = makeAlias(result, "DESC", tokenSpan(tok))
    result.orderAsc = 0
  if p.check(tkNulls):
    discard p.advance()
    if p.check(tkFirst):
      discard p.advance()
      result.nullsFirst = 1
    elif p.check(tkLast):
      discard p.advance()
      result.nullsFirst = 0
    else:
      p.error("expected FIRST or LAST after NULLS")

proc parseRollupOrCube(p: var Parser; kind: SqlNodeKind): SqlNode =
  ## ROLLUP(e1, ..., en) or CUBE(e1, ..., en) inside GROUP BY (issue #149).
  ## The caller has verified the contextual keyword and the `(` lookahead.
  let start = p.advance()
  discard p.expect(tkLParen)
  result = newNode(kind, tokenSpan(start))
  if p.check(tkRParen):
    p.error("expected expression in " & start.value.toUpperAscii() & " list")
  result.children.add(p.parseExpr())
  while p.check(tkComma):
    discard p.advance()
    result.children.add(p.parseExpr())
  let closing = p.expect(tkRParen)
  result.span = spanThrough(tokenSpan(start), tokenSpan(closing))

proc parseGroupingSetElement(p: var Parser): SqlNode =
  ## One element of GROUPING SETS: `()` | expr | `(e1, ..., en)`.
  ## D1: nested ROLLUP/CUBE/GROUPING SETS is a syntax error in v1.
  if (p.checkContextual("rollup") or p.checkContextual("cube")) and
      p.peekNext() == tkLParen:
    p.error("ROLLUP and CUBE cannot be nested inside GROUPING SETS")
  if p.checkContextual("grouping") and p.peekNextIsContextual("sets"):
    p.error("GROUPING SETS cannot be nested inside GROUPING SETS")
  result = newNode(nkGroupingSet, p.currentSpan())
  if p.check(tkLParen):
    let open = p.advance()
    if p.check(tkRParen):
      let closing = p.advance()
      result.span = spanThrough(tokenSpan(open), tokenSpan(closing))
      return
    result.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseExpr())
    let closing = p.expect(tkRParen)
    result.span = spanThrough(tokenSpan(open), tokenSpan(closing))
  else:
    let expr = p.parseExpr()
    result.children.add(expr)
    result.span = expr.span

proc parseGroupByItem(p: var Parser): SqlNode =
  ## GROUP BY item := expr | ROLLUP(...) | CUBE(...) | GROUPING SETS (...)
  ## | `()` (issue #149, D1). ROLLUP/CUBE/GROUPING stay contextual: a bare
  ## identifier with those names still parses as an ordinary expression.
  if p.checkContextual("rollup") and p.peekNext() == tkLParen:
    result = p.parseRollupOrCube(nkRollup)
  elif p.checkContextual("cube") and p.peekNext() == tkLParen:
    result = p.parseRollupOrCube(nkCube)
  elif p.checkContextual("grouping") and p.peekNextIsContextual("sets"):
    let start = p.advance()          # GROUPING
    discard p.advance()              # SETS
    discard p.expect(tkLParen)
    result = newNode(nkGroupingSets, tokenSpan(start))
    if p.check(tkRParen):
      p.error("expected at least one grouping set in GROUPING SETS")
    result.children.add(p.parseGroupingSetElement())
    while p.check(tkComma):
      discard p.advance()
      result.children.add(p.parseGroupingSetElement())
    let closing = p.expect(tkRParen)
    result.span = spanThrough(tokenSpan(start), tokenSpan(closing))
  elif p.check(tkLParen) and p.peekNext() == tkRParen:
    # GROUP BY () — the single empty grouping set.
    let open = p.advance()
    let closing = p.advance()
    result = newNode(nkGroupingSets,
                     spanThrough(tokenSpan(open), tokenSpan(closing)))
    result.children.add(newNode(nkGroupingSet, result.span))
  else:
    result = p.parseExpr()

proc groupByContainsGroupingSetModifier(query: SqlNode): bool =
  ## Whether a parsed SELECT carries ROLLUP/CUBE/GROUPING SETS in GROUP BY.
  ## Used to keep the staged continuous-aggregate wire byte-compatible.
  for child in query.children:
    if child.kind == nkGroupByClause:
      for item in child.children:
        if item.kind in {nkRollup, nkCube, nkGroupingSets}:
          return true
  false

proc parseSelectCore(p: var Parser): SqlNode =
  let start = p.expect(tkSelect)
  result = newNode(nkSelect, tokenSpan(start))

  if p.check(tkDistinct):
    let distinctTok = p.advance()
    if p.check(tkOn):
      # SELECT DISTINCT ON (expr [, ...]) — PostgreSQL/DuckDB form
      # (issue #150). Parentheses are mandatory and the key list must
      # contain at least one expression.
      discard p.advance()
      discard p.expect(tkLParen)
      if p.check(tkRParen):
        p.error("expected expression in DISTINCT ON list")
      let clause = newNode(nkDistinctOnClause, tokenSpan(distinctTok))
      clause.children.add(p.parseExpr())
      while p.check(tkComma):
        discard p.advance()
        clause.children.add(p.parseExpr())
      let closing = p.expect(tkRParen)
      clause.span = spanThrough(tokenSpan(distinctTok), tokenSpan(closing))
      result.children.add(clause)
    else:
      result.children.add(newIdent("DISTINCT"))

  let selectList = newNode(nkExprList)
  selectList.children = p.parseSelectList()
  result.children.add(selectList)

  if p.check(tkFrom):
    discard p.advance()
    result.children.add(p.parseFromClause())

  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

  if p.check(tkGroup):
    discard p.advance()
    discard p.expect(tkBy)
    let groupBy = newNode(nkGroupByClause)
    groupBy.children.add(p.parseGroupByItem())
    while p.check(tkComma):
      discard p.advance()
      groupBy.children.add(p.parseGroupByItem())
    result.children.add(groupBy)

  if p.check(tkHaving):
    discard p.advance()
    let having = newNode(nkHavingClause)
    having.children.add(p.parseExpr())
    result.children.add(having)

  if p.check(tkWindow):
    let windowTok = p.advance()
    let windowClause = newNode(nkWindowClause, tokenSpan(windowTok))
    while true:
      let name = p.expectIdent("window name")
      let namedWindow = newNode(nkNamedWindow, tokenSpan(name))
      namedWindow.children.add(newIdent(name.value, tokenSpan(name)))
      discard p.expect(tkAs)
      namedWindow.children.add(p.parseWindowSpecBody())
      namedWindow.span = Span(start: tokenSpan(name).start,
                              `end`: namedWindow.children[^1].span.`end`)
      windowClause.children.add(namedWindow)
      if p.check(tkComma):
        discard p.advance()
      else:
        break
    result.children.add(windowClause)

  if p.check(tkQualify):
    let qualifyTok = p.advance()
    let qualify = newNode(nkQualifyClause, tokenSpan(qualifyTok))
    qualify.children.add(p.parseExpr())
    result.children.add(qualify)

proc parseValuesCore(p: var Parser): SqlNode =
  let start = p.expect(tkValues)
  result = newNode(nkValues, tokenSpan(start))
  while true:
    discard p.expect(tkLParen)
    if p.check(tkRParen):
      p.error("expected expression in VALUES row")
    let row = newNode(nkExprList)
    row.children.add(p.parseExpr())
    while p.check(tkComma):
      discard p.advance()
      row.children.add(p.parseExpr())
    let closing = p.expect(tkRParen)
    row.span = spanThrough(row.children[0].span, tokenSpan(closing))
    result.children.add(row)
    if p.check(tkComma):
      discard p.advance()
    else:
      break

proc parseQueryCore(p: var Parser): SqlNode =
  case p.current.kind
  of tkSelect:
    result = p.parseSelectCore()
  of tkValues:
    result = p.parseValuesCore()
  else:
    p.error("expected SELECT or VALUES query body")

proc parseIntersectTerm(p: var Parser): SqlNode =
  result = p.parseQueryCore()
  while p.check(tkIntersect):
    let operatorToken = p.advance()
    let all = if p.check(tkAll):
      discard p.advance()
      true
    else:
      false
    let right = p.parseQueryCore()
    result.children.add(SqlNode(kind: nkSetOperation,
      setOp: soIntersect, setAll: all, setRight: right,
      span: tokenSpan(operatorToken), orderAsc: -1, nullsFirst: -1))

proc parseFetchCount(p: var Parser): SqlNode =
  ## FETCH { FIRST | NEXT } [count] { ROW | ROWS }: the count may be omitted
  ## and defaults to 1 (SQL standard, PostgreSQL, DuckDB).
  if p.current.kind in {tkRow, tkRows}:
    newIntLit(1, p.currentSpan())
  else:
    p.parseExpr()

proc parseLimitOffsetFetch(p: var Parser; target: SqlNode) =
  ## Query tail pagination (issue #152):
  ##   LIMIT (ALL | expr) / OFFSET expr [ROW | ROWS] /
  ##   FETCH (FIRST | NEXT) [expr] (ROW | ROWS) (ONLY | WITH TIES)
  ## Clauses may appear in any order, but at most one limit-setting clause
  ## (LIMIT or FETCH) and one OFFSET are accepted (PostgreSQL semantics).
  var limitNode: SqlNode = nil
  var offsetNode: SqlNode = nil
  var sawLimitAll = false
  while p.current.kind in {tkLimit, tkOffset, tkFetch}:
    case p.current.kind
    of tkLimit:
      if limitNode != nil or sawLimitAll:
        p.error("multiple LIMIT clauses are not allowed")
      discard p.advance()
      if p.check(tkAll):
        # LIMIT ALL means no limit; no nkLimitClause node is produced.
        discard p.advance()
        sawLimitAll = true
      else:
        limitNode = newNode(nkLimitClause)
        limitNode.children.add(p.parseExpr())
    of tkOffset:
      if offsetNode != nil:
        p.error("multiple OFFSET clauses are not allowed")
      discard p.advance()
      offsetNode = newNode(nkOffsetClause)
      offsetNode.children.add(p.parseExpr())
      if p.current.kind in {tkRow, tkRows}:
        discard p.advance()
    of tkFetch:
      if limitNode != nil or sawLimitAll:
        p.error("multiple LIMIT clauses are not allowed")
      discard p.advance()
      if p.current.kind notin {tkFirst, tkNext}:
        p.error("expected FIRST or NEXT after FETCH")
      discard p.advance()
      limitNode = newNode(nkLimitClause)
      limitNode.children.add(p.parseFetchCount())
      if p.checkContextual("percent"):
        p.error("FETCH ... PERCENT is not supported")
      if p.current.kind notin {tkRow, tkRows}:
        p.error("expected ROW or ROWS after FETCH count")
      discard p.advance()
      if p.check(tkOnly):
        discard p.advance()
      elif p.check(tkWith):
        discard p.advance()
        if not p.check(tkTies):
          p.error("expected TIES after WITH in FETCH clause")
        discard p.advance()
        limitNode.limitWithTies = true
      else:
        p.error("expected ONLY or WITH TIES in FETCH clause")
    else:
      discard
  if limitNode != nil:
    target.children.add(limitNode)
  if offsetNode != nil:
    target.children.add(offsetNode)

proc parseSelectStmt(p: var Parser): SqlNode =
  result = p.parseIntersectTerm()

  while p.current.kind in {tkUnion, tkExcept}:
    let operatorToken = p.advance()
    let operator = if operatorToken.kind == tkUnion: soUnion else: soExcept
    let all = if p.check(tkAll):
      discard p.advance()
      true
    else:
      false
    let right = p.parseIntersectTerm()
    result.children.add(SqlNode(kind: nkSetOperation,
      setOp: operator, setAll: all, setRight: right,
      span: tokenSpan(operatorToken), orderAsc: -1, nullsFirst: -1))

  if p.check(tkOrder):
    discard p.advance()
    discard p.expect(tkBy)
    let orderBy = newNode(nkOrderByClause)
    orderBy.children.add(p.parseOrderByItem())
    while p.check(tkComma):
      discard p.advance()
      orderBy.children.add(p.parseOrderByItem())
    result.children.add(orderBy)

  p.parseLimitOffsetFetch(result)

proc parseWithQueryStmt(p: var Parser): SqlNode =
  let start = p.expect(tkWith)
  let withClause = newNode(nkWithClause, tokenSpan(start))
  if p.check(tkRecursive):
    withClause.recursive = true
    discard p.advance()

  while true:
    let name = p.expectIdent("common table expression name")
    let cte = newNode(nkCommonTableExpr, tokenSpan(name))
    cte.children.add(newIdent(name.value, tokenSpan(name)))
    if p.check(tkLParen):
      let columnsStart = p.advance()
      let columns = newNode(nkCteColumnList, tokenSpan(columnsStart))
      let firstColumn = p.expectIdent("common table expression column name")
      columns.children.add(newIdent(firstColumn.value, tokenSpan(firstColumn)))
      while p.check(tkComma):
        discard p.advance()
        let column = p.expectIdent("common table expression column name")
        columns.children.add(newIdent(column.value, tokenSpan(column)))
      discard p.expect(tkRParen)
      cte.children.add(columns)
    discard p.expect(tkAs)
    discard p.expect(tkLParen)
    if p.current.kind notin {tkSelect, tkValues, tkWith}:
      p.error("expected SELECT, VALUES, or WITH in common table expression")
    cte.children.add(p.parseQueryStmt())
    discard p.expect(tkRParen)
    withClause.children.add(cte)
    if p.check(tkComma):
      discard p.advance()
    else:
      break

  if p.current.kind notin {tkSelect, tkValues}:
    p.error("expected SELECT or VALUES after WITH clause")
  result = p.parseSelectStmt()
  result.children.insert(withClause, 0)
  result.span.start = tokenSpan(start).start

proc parseQueryStmt(p: var Parser): SqlNode =
  case p.current.kind
  of tkWith:
    result = p.parseWithQueryStmt()
  of tkSelect, tkValues:
    result = p.parseSelectStmt()
  else:
    p.error("expected SELECT, VALUES, or WITH query")

# --- DML parsing ---

proc parseInsertStmt(p: var Parser): SqlNode =
  let start = p.expect(tkInsert)
  discard p.expect(tkInto)
  result = newNode(nkInsert, tokenSpan(start))
  let table = p.expectIdent("table name")
  result.children.add(newIdent(table.value, tokenSpan(table)))

  if p.check(tkLParen):
    discard p.advance()
    # nkColumnList で VALUES 行 (nkExprList) と区別する。nkExprList を共用すると
    # 「カラムリスト省略 × 多行 VALUES」と「カラムリスト明示 × 単行」が
    # 同じ木構造になり、シリアライズ側で判別できない (issue #40)。
    let cols = newNode(nkColumnList)
    cols.children.add(newIdent(p.expectIdent("column name").value))
    while p.check(tkComma):
      discard p.advance()
      cols.children.add(newIdent(p.expectIdent("column name").value))
    discard p.expect(tkRParen)
    result.children.add(cols)

  if p.check(tkValues):
    discard p.advance()
    while true:
      discard p.expect(tkLParen)
      let row = newNode(nkExprList)
      row.children.add(p.parseExpr())
      while p.check(tkComma):
        discard p.advance()
        row.children.add(p.parseExpr())
      discard p.expect(tkRParen)
      result.children.add(row)
      if p.check(tkComma):
        discard p.advance()
      else:
        break
  elif p.current.kind in {tkSelect, tkWith}:
    p.enterNesting()
    defer: p.leaveNesting()
    result.children.add(p.parseQueryStmt())
  else:
    discard p.expect(tkValues)

proc parseUpdateStmt(p: var Parser): SqlNode =
  let start = p.expect(tkUpdate)
  result = newNode(nkUpdate, tokenSpan(start))
  let table = p.expectIdent("table name")
  result.children.add(newIdent(table.value, tokenSpan(table)))
  discard p.expect(tkSet)
  let setList = newNode(nkExprList)
  while true:
    let col = p.expectIdent("column name")
    discard p.expect(tkEq)
    setList.children.add(newBinaryOp(opEq, newIdent(col.value, tokenSpan(col)), p.parseExpr()))
    if p.check(tkComma):
      discard p.advance()
    else:
      break
  result.children.add(setList)
  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

proc parseDeleteStmt(p: var Parser): SqlNode =
  let start = p.expect(tkDelete)
  discard p.expect(tkFrom)
  result = newNode(nkDelete, tokenSpan(start))
  let table = p.expectIdent("table name")
  result.children.add(newIdent(table.value, tokenSpan(table)))
  if p.check(tkWhere):
    discard p.advance()
    let whereNode = newNode(nkWhereClause)
    whereNode.children.add(p.parseExpr())
    result.children.add(whereNode)

# --- DDL parsing ---

proc parseTypeName(p: var Parser): SqlNode =
  if p.current.kind notin TypeTokens and p.current.kind != tkIdent:
    p.error("expected type name")
  let typeTok = p.advance()
  result = newNode(nkTypeName, tokenSpan(typeTok))
  result.children.add(newIdent(typeTok.value, tokenSpan(typeTok)))
  if p.check(tkLParen):
    p.enterNesting()
    defer: p.leaveNesting()
    discard p.advance()
    result.children.add(p.parseExpr())
    if p.check(tkComma):
      discard p.advance()
      if p.current.kind in {tkCosine, tkL2, tkInner, tkIdent}:
        let metric = p.advance()
        result.children.add(newIdent(metric.value, tokenSpan(metric)))
      else:
        result.children.add(p.parseExpr())
    discard p.expect(tkRParen)

proc parseColumnDef(p: var Parser): SqlNode =
  let name = p.expectIdent("column name")
  let typeName = p.parseTypeName()
  result = SqlNode(kind: nkColumnDef, colName: name.value, colType: typeName,
                   colConstraints: @[], span: tokenSpan(name),
                   orderAsc: -1, nullsFirst: -1)

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

proc parseWithOptions(p: var Parser): SqlNode =
  result = newNode(nkWithOptions)
  discard p.expect(tkWith)
  discard p.expect(tkLParen)
  while true:
    let key = p.expectIdent("option key")
    discard p.expect(tkEq)
    let value = p.expectOptionValue()
    let opt = newNode(nkIndexOption, tokenSpan(key))
    opt.children.add(newIdent(key.value, tokenSpan(key)))
    opt.children.add(newStringLit(value.value, tokenSpan(value)))
    result.children.add(opt)
    if p.check(tkComma):
      discard p.advance()
    else:
      break
  discard p.expect(tkRParen)

proc isSingleMeasurementSelect(query: SqlNode): bool =
  for child in query.children:
    if child.kind == nkFromClause:
      if child.children.len != 1:
        return false
      let source = child.children[0]
      return source.kind == nkIdentifier or
        (source.kind == nkAlias and source.aliasExpr.kind == nkIdentifier)
  false

proc parseContinuousAggregateOptions(p: var Parser): SqlNode =
  const optionNames = ["retention", "refresh_interval"]
  let start = p.expect(tkWith)
  result = newNode(nkWithOptions, tokenSpan(start))
  discard p.expect(tkLParen)

  for index, expectedName in optionNames:
    if p.check(tkRParen):
      p.error("missing continuous aggregate option " & expectedName)
    if p.current.kind != tkIdent:
      p.error("expected continuous aggregate option " & expectedName)
    let key = p.current
    let normalizedKey = key.value.toLowerAscii()
    if normalizedKey notin optionNames:
      p.error("unknown continuous aggregate option " & key.value)
    if normalizedKey != expectedName:
      if index > 0 and normalizedKey == optionNames[index - 1]:
        p.error("duplicate continuous aggregate option " & key.value)
      p.error("expected continuous aggregate option " & expectedName)
    discard p.advance()
    discard p.expect(tkEq)
    if not p.check(tkString):
      p.error("continuous aggregate option " & expectedName &
        " requires a string duration")
    if p.current.value.len == 0:
      p.error("continuous aggregate option " & expectedName &
        " requires a non-empty duration string")
    let value = p.advance()
    let option = newNode(nkIndexOption,
      spanThrough(tokenSpan(key), tokenSpan(value)))
    option.children.add(newIdent(key.value, tokenSpan(key)))
    option.children.add(newStringLit(value.value, tokenSpan(value)))
    result.children.add(option)

    if index < optionNames.high:
      if not p.check(tkComma):
        p.error("missing continuous aggregate option " & optionNames[index + 1])
      discard p.advance()
    elif p.check(tkComma):
      discard p.advance()
      if p.check(tkRParen):
        p.error("unexpected trailing comma in continuous aggregate options")
      if p.current.kind == tkIdent:
        let extra = p.current.value.toLowerAscii()
        if extra in optionNames:
          p.error("duplicate continuous aggregate option " & p.current.value)
        p.error("unknown continuous aggregate option " & p.current.value)
      p.error("expected continuous aggregate option")

  let closing = p.expect(tkRParen)
  result.span = spanThrough(tokenSpan(start), tokenSpan(closing))

proc parseCreateContinuousAggregateAfterCreate(
    p: var Parser; start: Token): SqlNode =
  discard p.expectContextual("CONTINUOUS")
  discard p.expectContextual("AGGREGATE")
  let name = p.expectIdent("continuous aggregate name")
  discard p.expect(tkAs)
  if not p.check(tkSelect):
    p.error("expected SELECT query after AS")
  p.enterNesting()
  defer: p.leaveNesting()
  let query = p.parseSelectStmt()
  query.span = spanThrough(query.span, tokenSpan(p.previous))
  if not query.isSingleMeasurementSelect():
    p.error("continuous aggregate query requires one source measurement")
  # The staged continuous-aggregate wire stays byte-compatible with its
  # historical [Expr] group_by payload (issue #149, D10), so grouping-set
  # modifiers are rejected before they can reach the encoder.
  if query.groupByContainsGroupingSetModifier():
    p.error("continuous aggregate queries do not support GROUPING SETS, ROLLUP, or CUBE")
  if not p.check(tkWith):
    p.error("expected WITH options after continuous aggregate SELECT")
  let options = p.parseContinuousAggregateOptions()
  if p.current.kind notin {tkSemicolon, tkEof}:
    p.error("unexpected token after continuous aggregate options")

  result = newNode(nkCreateContinuousAggregate,
    spanThrough(tokenSpan(start), options.span))
  result.children.add(newIdent(name.value, tokenSpan(name)))
  result.children.add(query)
  result.children.add(options)

proc parseCreateTableAfterCreate(p: var Parser; start: Token): SqlNode =
  discard p.expect(tkTable)
  result = newNode(nkCreateTable, tokenSpan(start))
  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkNot)
    discard p.expect(tkExists)
    result.children.add(newIdent("IF NOT EXISTS"))
  let table = p.expectIdent("table name")
  result.children.add(newIdent(table.value, tokenSpan(table)))
  discard p.expect(tkLParen)
  result.children.add(p.parseColumnDef())
  while p.check(tkComma):
    discard p.advance()
    if p.check(tkPrimary) or p.check(tkUnique) or p.check(tkForeign) or p.check(tkConstraint):
      let c = newNode(nkConstraint)
      c.children.add(newIdent(p.advance().value))
      if p.check(tkKey): discard p.advance()
      if p.check(tkLParen):
        discard p.advance()
        c.children.add(newIdent(p.expectIdent("constraint column").value))
        while p.check(tkComma):
          discard p.advance()
          c.children.add(newIdent(p.expectIdent("constraint column").value))
        discard p.expect(tkRParen)
      result.children.add(c)
    else:
      result.children.add(p.parseColumnDef())
  discard p.expect(tkRParen)
  if p.check(tkWith):
    result.children.add(p.parseWithOptions())

proc parseCreateIndexAfterCreate(p: var Parser; start: Token): SqlNode =
  discard p.expect(tkIndex)
  result = newNode(nkCreateIndex, tokenSpan(start))
  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkNot)
    discard p.expect(tkExists)
    result.children.add(newIdent("IF NOT EXISTS"))
  let name = p.expectIdent("index name")
  discard p.expect(tkOn)
  let table = p.expectIdent("table name")
  discard p.expect(tkLParen)
  let column = p.expectIdent("index column")
  discard p.expect(tkRParen)
  result.children.add(newIdent(name.value, tokenSpan(name)))
  result.children.add(newIdent(table.value, tokenSpan(table)))
  result.children.add(newIdent(column.value, tokenSpan(column)))
  if p.check(tkUsing):
    discard p.advance()
    if p.current.kind notin {tkHnsw, tkBtree}:
      p.error("expected HNSW or BTREE index method")
    let idxMethod = p.advance()
    result.children.add(newIdent(idxMethod.value, tokenSpan(idxMethod)))
  if p.check(tkWith):
    result.children.add(p.parseWithOptions())

proc parseCreateStmt(p: var Parser): SqlNode =
  let start = p.expect(tkCreate)
  if p.check(tkTable):
    result = p.parseCreateTableAfterCreate(start)
  elif p.check(tkIndex):
    result = p.parseCreateIndexAfterCreate(start)
  elif p.checkContextual("CONTINUOUS"):
    result = p.parseCreateContinuousAggregateAfterCreate(start)
  else:
    p.error("expected TABLE, INDEX, or CONTINUOUS AGGREGATE after CREATE")

proc parseDropTableAfterDrop(p: var Parser; start: Token): SqlNode =
  discard p.expect(tkTable)
  result = newNode(nkDropTable, tokenSpan(start))
  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkExists)
    result.children.add(newIdent("IF EXISTS"))
  let table = p.expectIdent("table name")
  result.children.add(newIdent(table.value, tokenSpan(table)))

proc parseDropIndexAfterDrop(p: var Parser; start: Token): SqlNode =
  discard p.expect(tkIndex)
  result = newNode(nkDropIndex, tokenSpan(start))
  if p.check(tkIf):
    discard p.advance()
    discard p.expect(tkExists)
    result.children.add(newIdent("IF EXISTS"))
  let name = p.expectIdent("index name")
  result.children.add(newIdent(name.value, tokenSpan(name)))

proc parseDropStmt(p: var Parser): SqlNode =
  let start = p.expect(tkDrop)
  if p.check(tkTable):
    result = p.parseDropTableAfterDrop(start)
  elif p.check(tkIndex):
    result = p.parseDropIndexAfterDrop(start)
  else:
    p.error("expected TABLE or INDEX after DROP")

proc parsePragmaStmt(p: var Parser): SqlNode =
  let start = p.expect(tkPragma)
  result = newNode(nkPragma, tokenSpan(start))
  let name = p.expectIdent("pragma name")
  result.children.add(newIdent(name.value, tokenSpan(name)))
  if p.check(tkEq):
    discard p.advance()
    if p.check(tkInteger):
      let value = p.advance()
      result.children.add(newIntLit(parseBiggestInt(value.value), tokenSpan(value)))
    elif p.check(tkString):
      let value = p.advance()
      result.children.add(newStringLit(value.value, tokenSpan(value)))
    else:
      p.error("expected integer or string pragma value")

proc parseStatement*(p: var Parser): SqlNode =
  case p.current.kind
  of tkWith, tkSelect, tkValues:
    result = p.parseQueryStmt()
  of tkInsert:
    result = p.parseInsertStmt()
  of tkUpdate:
    result = p.parseUpdateStmt()
  of tkDelete:
    result = p.parseDeleteStmt()
  of tkCreate:
    result = p.parseCreateStmt()
  of tkDrop:
    result = p.parseDropStmt()
  of tkPragma:
    result = p.parsePragmaStmt()
  else:
    p.error("expected SQL statement (WITH, SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, PRAGMA)")

  if p.check(tkSemicolon):
    discard p.advance()

proc parseSqlStatements*(input: string): seq[SqlNode] =
  ## Parse one or more SQL statements separated by semicolons.
  var p = initParser(input)
  while p.check(tkSemicolon):
    discard p.advance()
  while not p.check(tkEof):
    let stmt = p.parseStatement()
    stmt.fillMissingSpans(stmt.span)
    result.add(stmt)
    while p.check(tkSemicolon):
      discard p.advance()

proc parseSql*(input: string): SqlNode =
  ## Parse SQL from string. A single statement returns that statement; multiple
  ## statements return nkStatementList.
  let statements = parseSqlStatements(input)
  if statements.len == 0:
    raise newException(ParseError, "empty SQL input")
  if statements.len == 1:
    result = statements[0]
  else:
    result = newNode(nkStatementList, statements[0].span)
    result.children = statements
    result.fillMissingSpans(result.span)
