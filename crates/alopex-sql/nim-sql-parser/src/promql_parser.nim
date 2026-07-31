## Recursive-descent / precedence-climbing parser for Skulk's PromQL subset.
##
## Operator precedence and selector modifier placement follow
## reference/prometheus/promql/parser/generated_parser.y.

import std/[math, strutils]
import promql_ast, promql_lexer

const
  PromQlMaxDepth* = 64
  AggregateNames = ["sum", "avg", "max", "min", "count"]

type
  PromParser = object
    lexer: PromLexer
    current: PromToken
    depth: int

proc initPromParser(input: string): PromParser =
  result.lexer = initPromLexer(input)
  result.current = result.lexer.nextToken()

proc fail(p: PromParser; message: string) {.noreturn.} =
  let token = if p.current.value.len == 0: "<eof>" else: p.current.value
  raise newException(PromQlParseError,
    "PromQL parse error at line " & $p.current.span.start.line &
    ", col " & $p.current.span.start.column &
    ", offset " & $p.current.span.start.offset &
    " near '" & token & "': " & message)

proc advance(p: var PromParser): PromToken =
  result = p.current
  p.current = p.lexer.nextToken()

proc check(p: PromParser; kind: PromTokenKind): bool =
  p.current.kind == kind

proc expect(p: var PromParser; kind: PromTokenKind;
            message: string): PromToken =
  if not p.check(kind):
    p.fail(message)
  p.advance()

proc isLabelToken(kind: PromTokenKind): bool =
  kind in {ptIdent, ptString, ptOffset, ptBy, ptWithout}

proc expectLabel(p: var PromParser; context: string): PromToken =
  if not p.current.kind.isLabelToken():
    p.fail("expected " & context)
  p.advance()

proc durationUnit(raw: string; index: var int): tuple[factor: float64, rank: int] =
  if index + 1 < raw.len and raw[index] == 'm' and raw[index + 1] == 's':
    index += 2
    return (1.0, 0)
  let unit = raw[index]
  inc index
  case unit
  of 's': (1_000.0, 1)
  of 'm': (60_000.0, 2)
  of 'h': (3_600_000.0, 3)
  of 'd': (86_400_000.0, 4)
  of 'w': (604_800_000.0, 5)
  of 'y': (31_536_000_000.0, 6)
  else: (0.0, -1)

proc decodeDuration(raw: string; span: PromSpan): PromDuration =
  var index = 0
  var total = 0.0
  var previousRank = high(int)
  while index < raw.len:
    let numberStart = index
    var sawDigit = false
    while index < raw.len and raw[index] in {'0'..'9'}:
      sawDigit = true
      inc index
    if index < raw.len and raw[index] == '.':
      inc index
      while index < raw.len and raw[index] in {'0'..'9'}:
        sawDigit = true
        inc index
    if not sawDigit or index >= raw.len:
      raise newException(PromQlParseError,
        "PromQL parse error at line " & $span.start.line &
        ", col " & $span.start.column & ", offset " & $span.start.offset &
        " near '" & raw & "': invalid duration")
    let value = parseFloat(raw[numberStart ..< index])
    let (factor, rank) = durationUnit(raw, index)
    if rank < 0 or rank >= previousRank:
      raise newException(PromQlParseError,
        "PromQL parse error at line " & $span.start.line &
        ", col " & $span.start.column & ", offset " & $span.start.offset &
        " near '" & raw & "': duration units must be unique and descending")
    previousRank = rank
    total += value * factor
  if total <= 0.0 or total > float64(high(int64)):
    raise newException(PromQlParseError,
      "PromQL parse error at line " & $span.start.line &
      ", col " & $span.start.column & ", offset " & $span.start.offset &
      " near '" & raw & "': duration must be positive and in range")
  PromDuration(raw: raw, milliseconds: int64(round(total)))

proc parseDuration(p: var PromParser; allowNegative: bool): PromDuration =
  var sign = 1'i64
  var prefix = ""
  if allowNegative and p.check(ptMinus):
    prefix = "-"
    sign = -1
    discard p.advance()
  elif allowNegative and p.check(ptPlus):
    prefix = "+"
    discard p.advance()
  let token = p.expect(ptDuration, "expected duration")
  result = decodeDuration(token.value, token.span)
  result.raw = prefix & result.raw
  result.milliseconds *= sign

proc parseExpression(p: var PromParser; minPrecedence = 0): PromExpr

proc parseGrouping(p: var PromParser): tuple[kind: PromGroupingKind,
                                             labels: seq[string],
                                             span: PromSpan] =
  let modifier = p.advance()
  result.kind = if modifier.kind == ptBy: pgBy else: pgWithout
  discard p.expect(ptLParen, "expected '(' after grouping modifier")
  if not p.check(ptRParen):
    let first = p.expectLabel("grouping label")
    result.labels.add(first.value)
    while p.check(ptComma):
      discard p.advance()
      if p.check(ptRParen):
        break
      let label = p.expectLabel("grouping label")
      result.labels.add(label.value)
  let close = p.expect(ptRParen, "expected ')' after grouping labels")
  result.span = mergeSpan(modifier.span, close.span)

proc parseMatchers(p: var PromParser): tuple[matchers: seq[PromLabelMatcher],
                                             span: PromSpan] =
  let open = p.expect(ptLBrace, "expected '{'")
  if not p.check(ptRBrace):
    while true:
      let label = p.expectLabel("label name")
      let opToken = p.current
      let op = case opToken.kind
        of ptEqual: pmEqual
        of ptNotEqual: pmNotEqual
        of ptRegex: pmRegex
        of ptNotRegex: pmNotRegex
        else:
          p.fail("expected label matcher operator (=, !=, =~, !~)")
      discard p.advance()
      let value = p.expect(ptString, "expected string literal after matcher operator")
      result.matchers.add(PromLabelMatcher(
        name: label.value,
        op: op,
        value: value.value,
        span: mergeSpan(label.span, value.span),
      ))
      if not p.check(ptComma):
        break
      discard p.advance()
      if p.check(ptRBrace):
        break
  let close = p.expect(ptRBrace, "expected '}' after label matchers")
  result.span = mergeSpan(open.span, close.span)

proc parseVectorSelector(p: var PromParser;
                         metricToken: PromToken = PromToken()): PromExpr =
  let hasMetric =
    metricToken.kind in {ptIdent, ptOffset, ptBy, ptWithout} and
    metricToken.value.len > 0
  var selectorSpan = if hasMetric: metricToken.span else: p.current.span
  result = newPromExpr(peVectorSelector, selectorSpan)
  if hasMetric:
    result.metric = metricToken.value
  if p.check(ptLBrace):
    let parsed = p.parseMatchers()
    result.matchers = parsed.matchers
    if hasMetric:
      result.span = mergeSpan(metricToken.span, parsed.span)
    else:
      result.span = parsed.span
  elif not hasMetric:
    p.fail("selector requires a metric name or label matchers")
  if not hasMetric and result.matchers.len == 0:
    p.fail("selector without a metric must contain at least one matcher")

proc parseFunction(p: var PromParser; nameToken: PromToken): PromExpr =
  discard p.expect(ptLParen, "expected '(' after function name")
  result = newPromExpr(peFunctionCall, nameToken.span)
  result.name = nameToken.value
  if not p.check(ptRParen):
    result.args.add(p.parseExpression())
    while p.check(ptComma):
      discard p.advance()
      result.args.add(p.parseExpression())
  let close = p.expect(ptRParen, "expected ')' after function arguments")
  result.span = mergeSpan(nameToken.span, close.span)

proc parseAggregate(p: var PromParser; nameToken: PromToken): PromExpr =
  result = newPromExpr(peAggregate, nameToken.span)
  result.name = nameToken.value

  if p.current.kind in {ptBy, ptWithout}:
    let grouping = p.parseGrouping()
    result.groupingKind = grouping.kind
    result.groupingLabels = grouping.labels

  discard p.expect(ptLParen, "expected '(' after aggregation operator")
  result.args.add(p.parseExpression())
  if p.check(ptComma):
    p.fail("aggregation operator accepts exactly one expression")
  let close = p.expect(ptRParen, "expected ')' after aggregation expression")
  result.span = mergeSpan(nameToken.span, close.span)

  if p.current.kind in {ptBy, ptWithout}:
    if result.groupingKind != pgNone:
      p.fail("aggregation may contain only one grouping modifier")
    let grouping = p.parseGrouping()
    result.groupingKind = grouping.kind
    result.groupingLabels = grouping.labels
    result.span = mergeSpan(nameToken.span, grouping.span)

proc parsePrimary(p: var PromParser): PromExpr =
  case p.current.kind
  of ptNumber:
    let token = p.advance()
    result = newPromExpr(peNumberLiteral, token.span)
    result.numberRaw = token.value
    try:
      result.numberValue = parseFloat(token.value)
    except ValueError:
      p.fail("invalid numeric literal")
  of ptString:
    let token = p.advance()
    result = newPromExpr(peStringLiteral, token.span)
    result.stringValue = token.value
  of ptLParen:
    let open = p.advance()
    let inner = p.parseExpression()
    let close = p.expect(ptRParen, "expected ')' after expression")
    result = newPromExpr(peParen, mergeSpan(open.span, close.span))
    result.inner = inner
  of ptLBrace:
    result = p.parseVectorSelector()
  of ptIdent, ptOffset, ptBy, ptWithout:
    let nameToken = p.advance()
    if nameToken.value in AggregateNames:
      result = p.parseAggregate(nameToken)
    elif p.check(ptLParen):
      result = p.parseFunction(nameToken)
    else:
      result = p.parseVectorSelector(nameToken)
  else:
    p.fail("expected expression")

proc parsePostfix(p: var PromParser): PromExpr =
  result = p.parsePrimary()
  while true:
    if p.check(ptLBracket):
      if result.kind != peVectorSelector:
        p.fail("ranges are only allowed for vector selectors")
      if result.hasOffset:
        p.fail("offset must follow a range selector")
      discard p.advance()
      let range = p.parseDuration(false)
      let close = p.expect(ptRBracket, "expected ']' after range duration")
      let matrix = newPromExpr(peMatrixSelector, mergeSpan(result.span, close.span))
      matrix.inner = result
      matrix.range = range
      result = matrix
    elif p.check(ptOffset):
      if result.kind notin {peVectorSelector, peMatrixSelector}:
        p.fail("offset is only allowed for vector and range selectors")
      if result.hasOffset:
        p.fail("duplicate offset modifier")
      discard p.advance()
      result.offset = p.parseDuration(true)
      result.hasOffset = true
      result.span.`end` = p.current.span.start
    else:
      break

proc parseUnary(p: var PromParser): PromExpr =
  if p.current.kind in {ptPlus, ptMinus}:
    let operator = p.advance()
    # Prometheus assigns unary signs MUL precedence: power binds inside the
    # operand, while multiplication/addition remain outside the unary node.
    let operand = p.parseExpression(21)
    result = newPromExpr(peUnary, mergeSpan(operator.span, operand.span))
    result.unaryOp = if operator.kind == ptMinus: puMinus else: puPlus
    result.operand = operand
  else:
    result = p.parsePostfix()

proc precedence(kind: PromTokenKind): int =
  case kind
  of ptPlus, ptMinus: 10
  of ptStar, ptSlash, ptPercent: 20
  of ptPower: 30
  else: -1

proc binaryOp(kind: PromTokenKind): PromBinaryOp =
  case kind
  of ptPlus: pbAdd
  of ptMinus: pbSub
  of ptStar: pbMul
  of ptSlash: pbDiv
  of ptPercent: pbMod
  of ptPower: pbPow
  else: pbAdd

proc parseExpression(p: var PromParser; minPrecedence = 0): PromExpr =
  inc p.depth
  if p.depth > PromQlMaxDepth:
    p.fail("expression nesting exceeds limit " & $PromQlMaxDepth)
  defer:
    dec p.depth

  result = p.parseUnary()
  while true:
    let operatorPrecedence = precedence(p.current.kind)
    if operatorPrecedence < minPrecedence:
      break
    let operator = p.advance()
    let nextPrecedence =
      if operator.kind == ptPower: operatorPrecedence
      else: operatorPrecedence + 1
    let right = p.parseExpression(nextPrecedence)
    let combined = newPromExpr(peBinary, mergeSpan(result.span, right.span))
    combined.binaryOp = binaryOp(operator.kind)
    combined.left = result
    combined.right = right
    result = combined

proc parsePromQl*(input: string): PromExpr =
  ## Parse one PromQL expression. The grammar deliberately has an independent
  ## entrypoint and lexer from SQL.
  var parser = initPromParser(input)
  if parser.check(ptEof):
    parser.fail("no expression found in input")
  result = parser.parseExpression()
  if not parser.check(ptEof):
    parser.fail("unexpected token after expression")
