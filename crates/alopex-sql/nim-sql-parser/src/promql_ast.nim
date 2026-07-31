## AST types for the PromQL subset exported to Skulk.
##
## PromQL has a separate tree from SQL so changes in one grammar cannot alter
## the other grammar's accepted language or MessagePack contract.

type
  PromQlParseError* = object of CatchableError

  PromPosition* = object
    line*: int
    column*: int
    offset*: int

  PromSpan* = object
    start*: PromPosition
    `end`*: PromPosition

  PromDuration* = object
    raw*: string
    milliseconds*: int64

  PromMatchOp* = enum
    pmEqual
    pmNotEqual
    pmRegex
    pmNotRegex

  PromLabelMatcher* = object
    name*: string
    op*: PromMatchOp
    value*: string
    span*: PromSpan

  PromBinaryOp* = enum
    pbAdd
    pbSub
    pbMul
    pbDiv
    pbMod
    pbPow

  PromUnaryOp* = enum
    puPlus
    puMinus

  PromGroupingKind* = enum
    pgNone
    pgBy
    pgWithout

  PromExprKind* = enum
    peVectorSelector
    peMatrixSelector
    peNumberLiteral
    peStringLiteral
    peFunctionCall
    peAggregate
    peBinary
    peUnary
    peParen

  PromExpr* = ref object
    kind*: PromExprKind
    span*: PromSpan

    # Selectors.
    metric*: string
    matchers*: seq[PromLabelMatcher]
    range*: PromDuration
    hasOffset*: bool
    offset*: PromDuration

    # Literals and calls.
    numberRaw*: string
    numberValue*: float64
    stringValue*: string
    name*: string
    args*: seq[PromExpr]

    # Aggregation.
    groupingKind*: PromGroupingKind
    groupingLabels*: seq[string]

    # Operators and grouping.
    binaryOp*: PromBinaryOp
    left*, right*: PromExpr
    unaryOp*: PromUnaryOp
    operand*: PromExpr
    inner*: PromExpr

proc mergeSpan*(left, right: PromSpan): PromSpan =
  PromSpan(start: left.start, `end`: right.`end`)

proc newPromExpr*(kind: PromExprKind; span: PromSpan): PromExpr =
  PromExpr(kind: kind, span: span, groupingKind: pgNone)
