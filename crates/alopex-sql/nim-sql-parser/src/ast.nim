## AST definitions for Alopex SQL parser.
##
## The parser still exposes the compact SqlNode tree used by the trial tests,
## while the node/field names are aligned with the target Rust AST contract
## from the nim-sql-parser-migration design.

import std/strutils

type
  Location* = object
    line*: int
    column*: int

  Span* = object
    start*: Location
    `end`*: Location

  SqlNodeKind* = enum
    nkSelect
    nkValues
    nkWithClause
    nkCommonTableExpr
    nkCteColumnList
    nkInsert
    nkUpdate
    nkDelete
    nkCreateTable
    nkDropTable
    nkCreateIndex
    nkCreateContinuousAggregate
    nkDropIndex
    nkPragma
    nkBegin
    nkSetTransaction
    nkCommit
    nkRollback
    nkSavepoint
    nkRollbackToSavepoint
    nkReleaseSavepoint
    nkStatementList
    nkIdentifier
    nkStringLit
    nkIntervalLit
    nkIntLit
    nkFloatLit
    nkBoolLit
    nkNull
    nkParameter
    nkStar
    nkQualifiedStar
    nkColumnRef
    nkBinaryOp
    nkUnaryOp
    nkRowConstructor
    nkTruthPredicate
    nkIsDistinctFrom
    nkCase
    nkCaseWhen
    nkFunctionCall
    nkWindowSpec
    nkWindowClause
    nkNamedWindow
    nkQualifyClause
    nkDistinctOnClause
    nkAggFilterClause    ## aggregate FILTER (WHERE predicate); child[0] = predicate
    nkWithinGroupClause  ## WITHIN GROUP (ORDER BY ...); children = order items
    nkWindowFrame
    nkWindowFrameBound
    nkPartitionByClause
    nkCast
    nkTryCast
    nkAlias
    nkFromClause
    nkFromTable
    nkFromJoin
    nkFromDerived
    nkFromFunction  ## FROM-clause table function; children[0] = name, rest = args
    nkWhereClause
    nkOrderByClause
    nkOrderByExpr
    nkGroupByClause
    nkRollup        ## GROUP BY ROLLUP(e1, ..., en); children = expressions
    nkCube          ## GROUP BY CUBE(e1, ..., en); children = expressions
    nkGroupingSets  ## GROUP BY GROUPING SETS (...); children = nkGroupingSet
    nkGroupingSet   ## one grouping set; children = expressions (may be empty)
    nkHavingClause
    nkLimitClause
    nkOffsetClause
    nkSetOperation
    nkJoin
    nkUsingClause
    nkColumnDef
    nkTypeName
    nkDataTypeVector
    nkConstraint
    nkExprList
    nkColumnList  ## INSERT のカラムリスト。VALUES 行 (nkExprList) と区別する。
    nkWithOptions
    nkIndexOption
    nkVectorLiteral
    nkScalarSubquery
    nkInSubquery
    nkExists
    nkQuantified

  BinaryOpKind* = enum
    opEq, opNeq, opLt, opLe, opGt, opGe
    opAdd, opSub, opMul, opDiv, opMod
    opStringConcat
    opAnd, opOr
    opLike, opNotLike, opILike, opNotILike, opGlob, opNotGlob,
    opSimilarTo, opNotSimilarTo, opIn, opNotIn, opBetween, opNotBetween, opIs,
    opBitAnd, opBitOr, opBitXor, opShiftLeft, opShiftRight

  UnaryOpKind* = enum
    opNot, opNeg, opIsNull, opIsNotNull, opBitNot

  JoinKind* = enum
    jkInner, jkLeft, jkRight, jkFull, jkCross

  QuantifierKind* = enum
    qkAny, qkAll

  SetOperatorKind* = enum
    soUnion, soIntersect, soExcept

  WindowFrameUnitKind* = enum
    wfuRows, wfuRange

  WindowFrameBoundKind* = enum
    wfbUnboundedPreceding, wfbPreceding, wfbCurrentRow,
    wfbFollowing, wfbUnboundedFollowing

  SqlNode* = ref object
    span*: Span
    ## Common optional flags used by Rust-compatible variants:
    ## FunctionCall.distinct/star, EXISTS/IN negated, NATURAL join,
    ## ORDER BY direction/null placement.
    funcDistinct*: bool
    funcStar*: bool
    negated*: bool
    natural*: bool
    lateral*: bool          ## nkFromDerived/nkFromFunction: LATERAL (issue #151)
    withOrdinality*: bool   ## nkFromFunction: WITH ORDINALITY (issue #162)
    recursive*: bool
    parameterIndex*: int
    limitWithTies*: bool    ## nkLimitClause: FETCH ... WITH TIES (issue #152)
    orderAsc*: int          ## -1 = omitted, 0 = DESC, 1 = ASC
    nullsFirst*: int        ## -1 = omitted, 0 = LAST, 1 = FIRST
    quantifier*: QuantifierKind
    case kind*: SqlNodeKind
    of nkBegin, nkSetTransaction:
      isolationLevel*: string
      accessMode*: string
    of nkIdentifier, nkStringLit, nkIntervalLit:
      strVal*: string
    of nkIntLit:
      intVal*: int64
    of nkFloatLit:
      floatVal*: float64
    of nkBoolLit:
      boolVal*: bool
    of nkBinaryOp:
      binOp*: BinaryOpKind
      binLeft*, binRight*: SqlNode
    of nkUnaryOp:
      unOp*: UnaryOpKind
      unOperand*: SqlNode
    of nkCase:
      caseOperand*: SqlNode
      caseBranches*: seq[SqlNode]
      caseElse*: SqlNode
    of nkCaseWhen:
      caseWhen*: SqlNode
      caseThen*: SqlNode
    of nkJoin, nkFromJoin:
      joinKind*: JoinKind
      joinLeft*, joinRight*, joinCond*: SqlNode
      joinUsing*: seq[string]
    of nkAlias:
      aliasExpr*: SqlNode
      aliasName*: string
      aliasColumns*: seq[string]
    of nkColumnDef:
      colName*: string
      colType*: SqlNode
      colConstraints*: seq[SqlNode]
    of nkSetOperation:
      setOp*: SetOperatorKind
      setAll*: bool
      setRight*: SqlNode
    of nkWindowFrame:
      frameUnit*: WindowFrameUnitKind
      frameStart*, frameEnd*: SqlNode
    of nkWindowFrameBound:
      frameBoundKind*: WindowFrameBoundKind
      frameOffset*: uint64
    else:
      children*: seq[SqlNode]

proc emptyLocation*(): Location =
  Location(line: 0, column: 0)

proc emptySpan*(): Span =
  Span(start: emptyLocation(), `end`: emptyLocation())

proc isEmpty*(span: Span): bool =
  span.start.line == 0 and span.start.column == 0

proc newSpan*(line, startCol, endCol: int): Span =
  Span(start: Location(line: line, column: startCol),
       `end`: Location(line: line, column: endCol))

proc newNode*(kind: SqlNodeKind; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: kind, span: span, orderAsc: -1, nullsFirst: -1)

proc newIdent*(name: string; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkIdentifier, strVal: name, span: span, orderAsc: -1, nullsFirst: -1)

proc newStringLit*(val: string; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkStringLit, strVal: val, span: span, orderAsc: -1, nullsFirst: -1)

proc newIntervalLit*(val: string; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkIntervalLit, strVal: val, span: span, orderAsc: -1, nullsFirst: -1)

proc newIntLit*(val: int64; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkIntLit, intVal: val, span: span, orderAsc: -1, nullsFirst: -1)

proc newFloatLit*(val: float64; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkFloatLit, floatVal: val, span: span, orderAsc: -1, nullsFirst: -1)

proc newBoolLit*(val: bool; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkBoolLit, boolVal: val, span: span, orderAsc: -1, nullsFirst: -1)

proc newNull*(span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkNull, span: span, orderAsc: -1, nullsFirst: -1)

proc newStar*(span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkStar, span: span, orderAsc: -1, nullsFirst: -1)

proc newBinaryOp*(op: BinaryOpKind, left, right: SqlNode; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkBinaryOp, binOp: op, binLeft: left, binRight: right,
          span: span, orderAsc: -1, nullsFirst: -1)

proc newUnaryOp*(op: UnaryOpKind, operand: SqlNode; span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkUnaryOp, unOp: op, unOperand: operand,
          span: span, orderAsc: -1, nullsFirst: -1)

proc newJoin*(joinKind: JoinKind, left, right: SqlNode, condition: SqlNode = nil;
              usingCols: seq[string] = @[], natural = false;
              span: Span = emptySpan()): SqlNode =
  SqlNode(kind: nkJoin, joinKind: joinKind, joinLeft: left, joinRight: right,
          joinCond: condition, joinUsing: usingCols, natural: natural,
          span: span, orderAsc: -1, nullsFirst: -1)

proc fillMissingSpans*(node: SqlNode; fallback: Span) =
  if node == nil:
    return
  if node.span.isEmpty:
    node.span = fallback
  case node.kind
  of nkBegin, nkSetTransaction:
    discard
  of nkIdentifier, nkStringLit, nkIntervalLit, nkIntLit, nkFloatLit, nkBoolLit, nkNull, nkStar:
    discard
  of nkBinaryOp:
    node.binLeft.fillMissingSpans(node.span)
    node.binRight.fillMissingSpans(node.span)
  of nkUnaryOp:
    node.unOperand.fillMissingSpans(node.span)
  of nkCase:
    node.caseOperand.fillMissingSpans(node.span)
    for branch in node.caseBranches:
      branch.fillMissingSpans(node.span)
    node.caseElse.fillMissingSpans(node.span)
  of nkCaseWhen:
    node.caseWhen.fillMissingSpans(node.span)
    node.caseThen.fillMissingSpans(node.span)
  of nkJoin, nkFromJoin:
    node.joinLeft.fillMissingSpans(node.span)
    node.joinRight.fillMissingSpans(node.span)
    node.joinCond.fillMissingSpans(node.span)
  of nkAlias:
    node.aliasExpr.fillMissingSpans(node.span)
  of nkColumnDef:
    node.colType.fillMissingSpans(node.span)
    for child in node.colConstraints:
      child.fillMissingSpans(node.span)
  of nkSetOperation:
    node.setRight.fillMissingSpans(node.span)
  of nkWindowFrame:
    node.frameStart.fillMissingSpans(node.span)
    node.frameEnd.fillMissingSpans(node.span)
  of nkWindowFrameBound:
    discard
  else:
    for child in node.children:
      child.fillMissingSpans(node.span)

proc `$`*(node: SqlNode): string =
  ## Debug representation of AST node
  if node == nil:
    return "nil"
  case node.kind
  of nkBegin, nkSetTransaction:
    result = $node.kind & "(" & node.isolationLevel & ", " & node.accessMode & ")"
  of nkIdentifier:
    result = "Ident(" & node.strVal & ")"
  of nkStringLit:
    result = "Str('" & node.strVal & "')"
  of nkIntervalLit:
    result = "Interval('" & node.strVal & "')"
  of nkIntLit:
    result = "Int(" & $node.intVal & ")"
  of nkFloatLit:
    result = "Float(" & $node.floatVal & ")"
  of nkBoolLit:
    result = "Bool(" & $node.boolVal & ")"
  of nkNull:
    result = "NULL"
  of nkStar:
    result = "*"
  of nkBinaryOp:
    result = "BinOp(" & $node.binOp & ", " & $node.binLeft & ", " & $node.binRight & ")"
  of nkUnaryOp:
    result = "UnaryOp(" & $node.unOp & ", " & $node.unOperand & ")"
  of nkCase:
    result = "Case("
    if node.caseOperand != nil:
      result &= $node.caseOperand & ", "
    result &= $node.caseBranches
    if node.caseElse != nil:
      result &= ", ELSE " & $node.caseElse
    result &= ")"
  of nkCaseWhen:
    result = "When(" & $node.caseWhen & ", " & $node.caseThen & ")"
  of nkJoin, nkFromJoin:
    result = "Join(" & $node.joinKind & ", " & $node.joinLeft & ", " & $node.joinRight
    if node.joinCond != nil:
      result &= ", ON " & $node.joinCond
    if node.joinUsing.len > 0:
      result &= ", USING " & $node.joinUsing
    if node.natural:
      result &= ", NATURAL"
    result &= ")"
  of nkAlias:
    result = "Alias(" & $node.aliasExpr & " AS " & node.aliasName
    if node.aliasColumns.len > 0:
      result &= "(" & node.aliasColumns.join(", ") & ")"
    result &= ")"
  of nkColumnDef:
    result = "ColDef(" & node.colName & " " & $node.colType & ")"
  of nkSetOperation:
    result = "SetOperation(" & $node.setOp &
      (if node.setAll: " ALL, " else: ", ") & $node.setRight & ")"
  of nkWindowFrame:
    result = "WindowFrame(" & $node.frameUnit & ", " &
      $node.frameStart & ", " & $node.frameEnd & ")"
  of nkWindowFrameBound:
    result = "WindowFrameBound(" & $node.frameBoundKind
    if node.frameBoundKind in {wfbPreceding, wfbFollowing}:
      result &= ", " & $node.frameOffset
    result &= ")"
  else:
    result = $node.kind & "("
    if node.kind == nkLimitClause and node.limitWithTies:
      result &= "WITH TIES, "
    if node.kind in {nkFromDerived, nkFromFunction} and node.lateral:
      result &= "LATERAL, "
    for i, child in node.children:
      if i > 0:
        result &= ", "
      result &= $child
    result &= ")"
