## Alopex SQL Parser - C ABI entry point
##
## Exports C-compatible functions for FFI with Rust.
## Success payloads are MessagePack bytes containing seq[Statement].
## Build: nim c -d:release --app:lib --mm:orc -o:libalopex_sql_parser.so src/alopex_sql_parser.nim

import std/[strutils]
import msgpack4nim
import ast, parser, promql_ast, promql_parser

# --- C ABI types ---

type
  ParseResultKind* {.exportc.} = enum
    prkOk = 0
    prkError = 1

  CParseResult* {.exportc.} = object
    kind*: ParseResultKind
    buffer_ptr*: pointer  ## MessagePack AST bytes on success.
    buffer_len*: cint
    error_ptr*: cstring   ## Error message if kind == prkError.
    error_len*: cint

# --- MessagePack contract helpers ---

proc writeKey(s: MsgStream; key: string) =
  s.pack_type(key)

proc writeNil(s: MsgStream) =
  s.pack_imp_nil()

proc writeStringOpt(s: MsgStream; value: string) =
  if value.len == 0:
    s.writeNil()
  else:
    s.pack_type(value)

proc writeBoolOpt(s: MsgStream; value: int) =
  case value
  of -1:
    s.writeNil()
  of 0:
    s.pack_type(false)
  else:
    s.pack_type(true)

proc writeLocation(s: MsgStream; loc: Location) =
  s.pack_map(2)
  s.writeKey("line")
  s.pack_type(loc.line)
  s.writeKey("column")
  s.pack_type(loc.column)

proc writeSpan(s: MsgStream; span: Span) =
  s.pack_map(2)
  s.writeKey("start")
  s.writeLocation(span.start)
  s.writeKey("end")
  s.writeLocation(span.`end`)

proc firstIdent(node: SqlNode): string =
  if node == nil:
    return ""
  case node.kind
  of nkIdentifier, nkStringLit:
    node.strVal
  else:
    if node.children.len > 0:
      firstIdent(node.children[0])
    else:
      ""

proc normalizedBinaryOp(op: BinaryOpKind): string =
  case op
  of opAdd: "Add"
  of opSub: "Sub"
  of opMul: "Mul"
  of opDiv: "Div"
  of opMod: "Mod"
  of opEq: "Eq"
  of opNeq: "Neq"
  of opLt: "Lt"
  of opLe: "LtEq"
  of opGt: "Gt"
  of opGe: "GtEq"
  of opAnd: "And"
  of opOr: "Or"
  of opStringConcat: "StringConcat"
  of opLike, opNotLike, opILike, opNotILike, opGlob, opNotGlob,
     opSimilarTo, opNotSimilarTo, opIn, opNotIn, opBetween, opNotBetween, opIs: $op

proc patternKind(op: BinaryOpKind): string =
  case op
  of opILike, opNotILike: "ILike"
  of opGlob, opNotGlob: "Glob"
  of opSimilarTo, opNotSimilarTo: "SimilarTo"
  else: "Like"

proc normalizedBinaryOp(opName: string): string =
  case opName
  of "opEq": "Eq"
  of "opNeq": "Neq"
  of "opLt": "Lt"
  of "opLe": "LtEq"
  of "opGt": "Gt"
  of "opGe": "GtEq"
  of "opAdd": "Add"
  of "opSub": "Sub"
  of "opMul": "Mul"
  of "opDiv": "Div"
  of "opMod": "Mod"
  of "opAnd": "And"
  of "opOr": "Or"
  of "opStringConcat": "StringConcat"
  else: opName

proc normalizedUnaryOp(op: UnaryOpKind): string =
  case op
  of opNot: "Not"
  of opNeg: "Minus"
  of opIsNull, opIsNotNull: $op

proc normalizedJoinKind(kind: JoinKind): string =
  case kind
  of jkInner: "Inner"
  of jkLeft: "Left"
  of jkRight: "Right"
  of jkFull: "Full"
  of jkCross: "Cross"

proc normalizedQuantifier(kind: QuantifierKind): string =
  case kind
  of qkAny: "Any"
  of qkAll: "All"

proc normalizedDataTypeName(name: string): string =
  case name.toUpperAscii()
  of "INTEGER": "Integer"
  of "INT": "Int"
  of "SMALLINT": "Int"
  of "BIGINT": "BigInt"
  of "FLOAT": "Float"
  of "DOUBLE", "DECIMAL": "Double"
  of "TEXT", "VARCHAR", "CHAR": "Text"
  of "BLOB": "Blob"
  of "BOOLEAN": "Boolean"
  of "BOOL": "Bool"
  of "TIMESTAMP", "DATE", "TIME": "Timestamp"
  of "VECTOR": "Vector"
  else: name

proc normalizedMetricName(name: string): string =
  case name.toUpperAscii()
  of "COSINE": "Cosine"
  of "L2": "L2"
  of "INNER": "Inner"
  else: name

proc normalizedIndexMethod(name: string): string =
  case name.toUpperAscii()
  of "BTREE": "BTree"
  of "HNSW": "Hnsw"
  else: name

proc writeStatement(s: MsgStream; node: SqlNode)
proc writeExpr(s: MsgStream; node: SqlNode)
proc writeFromItem(s: MsgStream; node: SqlNode)
proc writeDataType(s: MsgStream; node: SqlNode)

proc writeLiteralKind(s: MsgStream; node: SqlNode) =
  case node.kind
  of nkIntLit:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Number")
    s.writeKey("value")
    s.pack_type($node.intVal)
  of nkFloatLit:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Number")
    s.writeKey("value")
    s.pack_type($node.floatVal)
  of nkStringLit:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("String")
    s.writeKey("value")
    s.pack_type(node.strVal)
  of nkIntervalLit:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Interval")
    s.writeKey("value")
    s.pack_type(node.strVal)
  of nkBoolLit:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Boolean")
    s.writeKey("value")
    s.pack_type(node.boolVal)
  of nkNull:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type("Null")
  else:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type("Null")

proc writeExprOpt(s: MsgStream; node: SqlNode) =
  if node == nil:
    s.writeNil()
  else:
    s.writeExpr(node)

proc writeExprSeq(s: MsgStream; nodes: seq[SqlNode]) =
  s.pack_array(nodes.len)
  for child in nodes:
    s.writeExpr(child)

proc writeStringSeqOpt(s: MsgStream; values: seq[string]) =
  if values.len == 0:
    s.writeNil()
  else:
    s.pack_array(values.len)
    for value in values:
      s.pack_type(value)

proc writeSelectItem(s: MsgStream; node: SqlNode) =
  if node.kind == nkStar:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Wildcard")
    s.writeKey("span")
    s.writeSpan(node.span)
    return

  if node.kind == nkQualifiedStar:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("QualifiedWildcard")
    s.writeKey("table")
    s.pack_type(node.children[0].firstIdent())
    s.writeKey("span")
    s.writeSpan(node.span)
    return

  let exprNode = if node.kind == nkAlias: node.aliasExpr else: node
  s.pack_map(4)
  s.writeKey("variant")
  s.pack_type("Expr")
  s.writeKey("expr")
  s.writeExpr(exprNode)
  s.writeKey("alias")
  if node.kind == nkAlias:
    s.pack_type(node.aliasName)
  else:
    s.writeNil()
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeOrderByExpr(s: MsgStream; node: SqlNode) =
  let exprNode = if node.kind == nkAlias: node.aliasExpr else: node
  s.pack_map(4)
  s.writeKey("expr")
  s.writeExpr(exprNode)
  s.writeKey("asc")
  s.writeBoolOpt(node.orderAsc)
  s.writeKey("nulls_first")
  s.writeBoolOpt(node.nullsFirst)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeIndexOption(s: MsgStream; node: SqlNode) =
  s.pack_map(3)
  s.writeKey("key")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("value")
  s.pack_type(if node.children.len > 1: node.children[1].firstIdent() else: "")
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeIndexOptions(s: MsgStream; node: SqlNode) =
  if node == nil:
    s.pack_array(0)
    return
  s.pack_array(node.children.len)
  for opt in node.children:
    s.writeIndexOption(opt)

proc writeColumnConstraint(s: MsgStream; node: SqlNode) =
  let name = node.firstIdent().toUpperAscii()
  case name
  of "NOT NULL":
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("NotNull")
    s.writeKey("span")
    s.writeSpan(node.span)
  of "PRIMARY KEY":
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("PrimaryKey")
    s.writeKey("span")
    s.writeSpan(node.span)
  of "UNIQUE":
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Unique")
    s.writeKey("span")
    s.writeSpan(node.span)
  of "DEFAULT":
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Default")
    s.writeKey("value")
    if node.children.len > 1:
      s.writeExpr(node.children[1])
    else:
      s.writeNil()
    s.writeKey("span")
    s.writeSpan(node.span)
  else:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type(name)
    s.writeKey("span")
    s.writeSpan(node.span)

proc writeColumnDef(s: MsgStream; node: SqlNode) =
  s.pack_map(4)
  s.writeKey("name")
  s.pack_type(node.colName)
  s.writeKey("data_type")
  s.writeDataType(node.colType)
  s.writeKey("constraints")
  s.pack_array(node.colConstraints.len)
  for constraintNode in node.colConstraints:
    s.writeColumnConstraint(constraintNode)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeTableConstraint(s: MsgStream; node: SqlNode) =
  s.pack_map(3)
  s.writeKey("variant")
  s.pack_type("PrimaryKey")
  s.writeKey("columns")
  var startIdx = 0
  if node.children.len > 0 and node.children[0].kind == nkIdentifier and
      node.children[0].strVal.toUpperAscii() in ["PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT"]:
    startIdx = 1
  s.pack_array(max(node.children.len - startIdx, 0))
  for i in startIdx ..< node.children.len:
    s.pack_type(node.children[i].firstIdent())
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeDataType(s: MsgStream; node: SqlNode) =
  let rawName = node.firstIdent()
  let variant = normalizedDataTypeName(rawName)
  if variant == "Vector":
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Vector")
    s.writeKey("dimension")
    if node.children.len > 1 and node.children[1].kind == nkIntLit:
      s.pack_type(uint32(node.children[1].intVal))
    else:
      s.pack_type(uint32(0))
    s.writeKey("metric")
    if node.children.len > 2:
      s.pack_type(normalizedMetricName(node.children[2].firstIdent()))
    else:
      s.writeNil()
  else:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type(variant)

proc writeFromItem(s: MsgStream; node: SqlNode) =
  if node == nil:
    s.writeNil()
    return

  case node.kind
  of nkAlias:
    if node.aliasExpr.kind == nkFromDerived:
      s.pack_map(4)
      s.writeKey("variant")
      s.pack_type("Derived")
      s.writeKey("subquery")
      s.writeStatement(node.aliasExpr.children[0])
      s.writeKey("alias")
      s.pack_type(node.aliasName)
      s.writeKey("span")
      s.writeSpan(node.span)
    else:
      s.pack_map(4)
      s.writeKey("variant")
      s.pack_type("Table")
      s.writeKey("name")
      s.pack_type(node.aliasExpr.firstIdent())
      s.writeKey("alias")
      s.pack_type(node.aliasName)
      s.writeKey("span")
      s.writeSpan(node.span)
  of nkIdentifier:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("Table")
    s.writeKey("name")
    s.pack_type(node.strVal)
    s.writeKey("alias")
    s.writeNil()
    s.writeKey("span")
    s.writeSpan(node.span)
  of nkFromDerived:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("Derived")
    s.writeKey("subquery")
    s.writeStatement(node.children[0])
    s.writeKey("alias")
    s.writeNil()
    s.writeKey("span")
    s.writeSpan(node.span)
  of nkJoin, nkFromJoin:
    s.pack_map(7)
    s.writeKey("variant")
    s.pack_type("Join")
    s.writeKey("left")
    s.writeFromItem(node.joinLeft)
    s.writeKey("right")
    s.writeFromItem(node.joinRight)
    s.writeKey("join_type")
    s.pack_type(normalizedJoinKind(node.joinKind))
    s.writeKey("condition")
    s.writeExprOpt(node.joinCond)
    s.writeKey("using")
    s.writeStringSeqOpt(node.joinUsing)
    s.writeKey("span")
    s.writeSpan(node.span)
  else:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("Table")
    s.writeKey("name")
    s.pack_type(node.firstIdent())
    s.writeKey("alias")
    s.writeNil()
    s.writeKey("span")
    s.writeSpan(node.span)

proc writeExpr(s: MsgStream; node: SqlNode) =
  if node == nil:
    s.writeNil()
    return

  if node.kind == nkAlias:
    s.writeExpr(node.aliasExpr)
    return

  s.pack_map(2)
  s.writeKey("kind")

  case node.kind
  of nkIntLit, nkFloatLit, nkStringLit, nkIntervalLit, nkBoolLit, nkNull:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Literal")
    s.writeKey("literal")
    s.writeLiteralKind(node)
  of nkIdentifier:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("ColumnRef")
    s.writeKey("table")
    s.writeNil()
    s.writeKey("column")
    s.pack_type(node.strVal)
  of nkColumnRef:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("ColumnRef")
    s.writeKey("table")
    s.pack_type(node.children[0].firstIdent())
    s.writeKey("column")
    s.pack_type(node.children[1].firstIdent())
  of nkStar:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("ColumnRef")
    s.writeKey("table")
    s.writeNil()
    s.writeKey("column")
    s.pack_type("*")
  of nkBinaryOp:
    case node.binOp
    of opBetween, opNotBetween:
      s.pack_map(5)
      s.writeKey("variant")
      s.pack_type("Between")
      s.writeKey("expr")
      s.writeExpr(node.binLeft)
      s.writeKey("low")
      s.writeExpr(node.binRight.children[0])
      s.writeKey("high")
      s.writeExpr(node.binRight.children[1])
      s.writeKey("negated")
      s.pack_type(node.binOp == opNotBetween)
    of opLike, opNotLike, opILike, opNotILike, opGlob, opNotGlob, opSimilarTo, opNotSimilarTo:
      s.pack_map(6)
      s.writeKey("variant")
      s.pack_type("Like")
      s.writeKey("expr")
      s.writeExpr(node.binLeft)
      s.writeKey("pattern")
      if node.binRight.kind == nkExprList:
        s.writeExpr(node.binRight.children[0])
      else:
        s.writeExpr(node.binRight)
      s.writeKey("escape")
      if node.binRight.kind == nkExprList and node.binRight.children.len > 1:
        s.writeExpr(node.binRight.children[1])
      else:
        s.writeNil()
      s.writeKey("negated")
      s.pack_type(node.binOp in {opNotLike, opNotILike, opNotGlob, opNotSimilarTo})
      s.writeKey("kind")
      s.pack_type(patternKind(node.binOp))
    of opIn, opNotIn:
      s.pack_map(4)
      s.writeKey("variant")
      s.pack_type("InList")
      s.writeKey("expr")
      s.writeExpr(node.binLeft)
      s.writeKey("list")
      s.writeExprSeq(node.binRight.children)
      s.writeKey("negated")
      s.pack_type(node.binOp == opNotIn)
    else:
      s.pack_map(4)
      s.writeKey("variant")
      s.pack_type("BinaryOp")
      s.writeKey("left")
      s.writeExpr(node.binLeft)
      s.writeKey("op")
      s.pack_type(normalizedBinaryOp(node.binOp))
      s.writeKey("right")
      s.writeExpr(node.binRight)
  of nkUnaryOp:
    case node.unOp
    of opIsNull, opIsNotNull:
      s.pack_map(3)
      s.writeKey("variant")
      s.pack_type("IsNull")
      s.writeKey("expr")
      s.writeExpr(node.unOperand)
      s.writeKey("negated")
      s.pack_type(node.unOp == opIsNotNull)
    else:
      s.pack_map(3)
      s.writeKey("variant")
      s.pack_type("UnaryOp")
      s.writeKey("op")
      s.pack_type(normalizedUnaryOp(node.unOp))
      s.writeKey("operand")
      s.writeExpr(node.unOperand)
  of nkFunctionCall:
    s.pack_map(5)
    s.writeKey("variant")
    s.pack_type("FunctionCall")
    s.writeKey("name")
    s.pack_type(node.children[0].firstIdent())
    s.writeKey("args")
    var argCount = node.children.len - 1
    if node.funcStar:
      argCount = 0
    s.pack_array(max(argCount, 0))
    if not node.funcStar:
      for i in 1 ..< node.children.len:
        s.writeExpr(node.children[i])
    s.writeKey("distinct")
    s.pack_type(node.funcDistinct)
    s.writeKey("star")
    s.pack_type(node.funcStar)
  of nkCast:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Cast")
    s.writeKey("expr")
    s.writeExpr(node.children[0])
    s.writeKey("target_type")
    s.writeDataType(node.children[1])
  of nkVectorLiteral:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("VectorLiteral")
    s.writeKey("values")
    s.pack_array(node.children.len)
    for valueNode in node.children:
      s.pack_type(valueNode.floatVal)
  of nkScalarSubquery:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("ScalarSubquery")
    s.writeKey("subquery")
    s.writeStatement(node.children[0])
  of nkInSubquery:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("InSubquery")
    s.writeKey("expr")
    s.writeExpr(node.children[0])
    s.writeKey("subquery")
    s.writeStatement(node.children[1])
    s.writeKey("negated")
    s.pack_type(node.negated)
  of nkExists:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Exists")
    s.writeKey("subquery")
    s.writeStatement(node.children[0])
    s.writeKey("negated")
    s.pack_type(node.negated)
  of nkQuantified:
    s.pack_map(5)
    s.writeKey("variant")
    s.pack_type("Quantified")
    s.writeKey("expr")
    s.writeExpr(node.children[0])
    s.writeKey("op")
    s.pack_type(normalizedBinaryOp(node.children[1].firstIdent()))
    s.writeKey("quantifier")
    s.pack_type(normalizedQuantifier(node.quantifier))
    s.writeKey("subquery")
    s.writeStatement(node.children[2])
  else:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("ColumnRef")
    s.writeKey("column")
    s.pack_type(node.firstIdent())

  s.writeKey("span")
  s.writeSpan(node.span)

proc writeSelectKind(s: MsgStream; node: SqlNode) =
  var distinctFlag = false
  var projectionNode: SqlNode = nil
  var fromNode: SqlNode = nil
  var selectionNode: SqlNode = nil
  var groupByNode: SqlNode = nil
  var havingNode: SqlNode = nil
  var orderByNode: SqlNode = nil
  var limitNode: SqlNode = nil

  for child in node.children:
    case child.kind
    of nkIdentifier:
      if child.strVal == "DISTINCT":
        distinctFlag = true
    of nkExprList:
      if projectionNode == nil:
        projectionNode = child
    of nkFromClause:
      fromNode = child
    of nkWhereClause:
      selectionNode = child.children[0]
    of nkGroupByClause:
      groupByNode = child
    of nkHavingClause:
      havingNode = child.children[0]
    of nkOrderByClause:
      orderByNode = child
    of nkLimitClause:
      limitNode = child
    else:
      discard

  s.pack_map(10)
  s.writeKey("variant")
  s.pack_type("Select")
  s.writeKey("distinct")
  s.pack_type(distinctFlag)
  s.writeKey("projection")
  if projectionNode == nil:
    s.pack_array(0)
  else:
    s.pack_array(projectionNode.children.len)
    for item in projectionNode.children:
      s.writeSelectItem(item)
  s.writeKey("from")
  if fromNode == nil:
    s.pack_array(0)
  else:
    s.pack_array(fromNode.children.len)
    for item in fromNode.children:
      s.writeFromItem(item)
  s.writeKey("selection")
  s.writeExprOpt(selectionNode)
  s.writeKey("group_by")
  if groupByNode == nil:
    s.writeNil()
  else:
    s.writeExprSeq(groupByNode.children)
  s.writeKey("having")
  s.writeExprOpt(havingNode)
  s.writeKey("order_by")
  if orderByNode == nil:
    s.pack_array(0)
  else:
    s.pack_array(orderByNode.children.len)
    for item in orderByNode.children:
      s.writeOrderByExpr(item)
  s.writeKey("limit")
  if limitNode != nil and limitNode.children.len > 0:
    s.writeExpr(limitNode.children[0])
  else:
    s.writeNil()
  s.writeKey("offset")
  if limitNode != nil and limitNode.children.len > 1:
    s.writeExpr(limitNode.children[1])
  else:
    s.writeNil()

proc writeInsertKind(s: MsgStream; node: SqlNode) =
  let tableName = node.children[0].firstIdent()
  # カラムリストは nkColumnList、VALUES 行は nkExprList。children 数からの
  # 推測は「カラムリスト省略 × 多行 VALUES」で先頭行を列リストと誤判別する
  # (issue #40) ため、ノード種別で判定する。
  let hasColumns = node.children.len > 1 and node.children[1].kind == nkColumnList

  s.pack_map(5)
  s.writeKey("variant")
  s.pack_type("Insert")
  s.writeKey("table")
  s.pack_type(tableName)
  s.writeKey("columns")
  if hasColumns:
    s.pack_array(node.children[1].children.len)
    for col in node.children[1].children:
      s.pack_type(col.firstIdent())
  else:
    s.writeNil()
  let firstRow = if hasColumns: 2 else: 1
  let source = if node.children.len > firstRow: node.children[firstRow] else: nil
  s.writeKey("source")
  if source != nil and source.kind == nkSelect:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Select")
    s.writeKey("select")
    s.writeSelectKind(source)
  else:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Values")
    s.writeKey("values")
    s.pack_array(max(node.children.len - firstRow, 0))
    for i in firstRow ..< node.children.len:
      s.writeExprSeq(node.children[i].children)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeUpdateKind(s: MsgStream; node: SqlNode) =
  s.pack_map(5)
  s.writeKey("variant")
  s.pack_type("Update")
  s.writeKey("table")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("assignments")
  s.pack_array(node.children[1].children.len)
  for assignmentNode in node.children[1].children:
    s.pack_map(3)
    s.writeKey("column")
    s.pack_type(assignmentNode.binLeft.firstIdent())
    s.writeKey("value")
    s.writeExpr(assignmentNode.binRight)
    s.writeKey("span")
    s.writeSpan(assignmentNode.span)
  s.writeKey("selection")
  if node.children.len > 2:
    s.writeExpr(node.children[2].children[0])
  else:
    s.writeNil()
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeDeleteKind(s: MsgStream; node: SqlNode) =
  s.pack_map(4)
  s.writeKey("variant")
  s.pack_type("Delete")
  s.writeKey("table")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("selection")
  if node.children.len > 1:
    s.writeExpr(node.children[1].children[0])
  else:
    s.writeNil()
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeCreateTableKind(s: MsgStream; node: SqlNode) =
  var ifNotExistsFlag = false
  var tableName = ""
  var columns: seq[SqlNode] = @[]
  var constraints: seq[SqlNode] = @[]
  var optionsNode: SqlNode = nil

  for child in node.children:
    case child.kind
    of nkIdentifier:
      if child.strVal == "IF NOT EXISTS":
        ifNotExistsFlag = true
      elif tableName.len == 0:
        tableName = child.strVal
    of nkColumnDef:
      columns.add(child)
    of nkConstraint:
      constraints.add(child)
    of nkWithOptions:
      optionsNode = child
    else:
      discard

  s.pack_map(7)
  s.writeKey("variant")
  s.pack_type("CreateTable")
  s.writeKey("if_not_exists")
  s.pack_type(ifNotExistsFlag)
  s.writeKey("name")
  s.pack_type(tableName)
  s.writeKey("columns")
  s.pack_array(columns.len)
  for col in columns:
    s.writeColumnDef(col)
  s.writeKey("constraints")
  s.pack_array(constraints.len)
  for constraintNode in constraints:
    s.writeTableConstraint(constraintNode)
  s.writeKey("with_options")
  s.writeIndexOptions(optionsNode)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeDropTableKind(s: MsgStream; node: SqlNode) =
  let ifExistsFlag = node.children.len > 0 and node.children[0].kind == nkIdentifier and
    node.children[0].strVal == "IF EXISTS"
  let tableIdx = if ifExistsFlag: 1 else: 0
  s.pack_map(4)
  s.writeKey("variant")
  s.pack_type("DropTable")
  s.writeKey("if_exists")
  s.pack_type(ifExistsFlag)
  s.writeKey("name")
  s.pack_type(node.children[tableIdx].firstIdent())
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeCreateIndexKind(s: MsgStream; node: SqlNode) =
  var idx = 0
  let ifNotExistsFlag = node.children.len > 0 and node.children[0].kind == nkIdentifier and
    node.children[0].strVal == "IF NOT EXISTS"
  if ifNotExistsFlag:
    idx = 1
  let optionsIdx = node.children.len - 1
  var optionsNode: SqlNode = nil
  if optionsIdx >= idx and node.children[optionsIdx].kind == nkWithOptions:
    optionsNode = node.children[optionsIdx]
  var methodName = ""
  if node.children.len > idx + 3 and node.children[idx + 3].kind == nkIdentifier:
    methodName = normalizedIndexMethod(node.children[idx + 3].strVal)

  s.pack_map(8)
  s.writeKey("variant")
  s.pack_type("CreateIndex")
  s.writeKey("if_not_exists")
  s.pack_type(ifNotExistsFlag)
  s.writeKey("name")
  s.pack_type(node.children[idx].firstIdent())
  s.writeKey("table")
  s.pack_type(node.children[idx + 1].firstIdent())
  s.writeKey("column")
  s.pack_type(node.children[idx + 2].firstIdent())
  s.writeKey("method")
  s.writeStringOpt(methodName)
  s.writeKey("options")
  s.writeIndexOptions(optionsNode)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeDropIndexKind(s: MsgStream; node: SqlNode) =
  let ifExistsFlag = node.children.len > 0 and node.children[0].kind == nkIdentifier and
    node.children[0].strVal == "IF EXISTS"
  let nameIdx = if ifExistsFlag: 1 else: 0
  s.pack_map(4)
  s.writeKey("variant")
  s.pack_type("DropIndex")
  s.writeKey("if_exists")
  s.pack_type(ifExistsFlag)
  s.writeKey("name")
  s.pack_type(node.children[nameIdx].firstIdent())
  s.writeKey("span")
  s.writeSpan(node.span)

proc writePragmaKind(s: MsgStream; node: SqlNode) =
  s.pack_map(4)
  s.writeKey("variant")
  s.pack_type("Pragma")
  s.writeKey("name")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("value")
  if node.children.len < 2:
    s.writeNil()
  else:
    case node.children[1].kind
    of nkIntLit:
      s.pack_type(node.children[1].intVal)
    of nkStringLit:
      s.pack_type(node.children[1].strVal)
    else:
      raise newException(ParseError, "invalid PRAGMA value node")
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeStatementKind(s: MsgStream; node: SqlNode) =
  case node.kind
  of nkSelect:
    s.writeSelectKind(node)
  of nkInsert:
    s.writeInsertKind(node)
  of nkUpdate:
    s.writeUpdateKind(node)
  of nkDelete:
    s.writeDeleteKind(node)
  of nkCreateTable:
    s.writeCreateTableKind(node)
  of nkDropTable:
    s.writeDropTableKind(node)
  of nkCreateIndex:
    s.writeCreateIndexKind(node)
  of nkDropIndex:
    s.writeDropIndexKind(node)
  of nkPragma:
    s.writePragmaKind(node)
  else:
    raise newException(ParseError, "unsupported statement node for MessagePack: " & $node.kind)

proc writeStatement(s: MsgStream; node: SqlNode) =
  s.pack_map(2)
  s.writeKey("kind")
  s.writeStatementKind(node)
  s.writeKey("span")
  s.writeSpan(node.span)

proc astToMsgPack*(statements: seq[SqlNode]): string =
  ## Encode parsed statements into the FFI MessagePack contract.
  ##
  ## Precondition: each `SqlNode` must be a tree produced by this module's
  ## own `parser.nim` (i.e. via `parseSqlStatements`/`parseSql`), where an
  ## INSERT's column list is `nkColumnList` and each VALUES row is
  ## `nkExprList` (see issue #40). A hand-built `nkInsert` tree that still
  ## uses `nkExprList` for the column list — the pre-fix representation —
  ## is structurally indistinguishable from a column-list-omitted,
  ## single-row INSERT (`table, row1` vs. `table, columns`) and
  ## `writeInsertKind` cannot reject it loudly; it will silently
  ## misinterpret the first child as either a values row or a column list.
  ## Callers outside `parseSqlStatements` must ensure this invariant
  ## themselves.
  var s = MsgStream.init()
  s.pack_array(statements.len)
  for statement in statements:
    s.writeStatement(statement)
  result = s.data

proc encodeSqlToMsgPack*(sql: string): string =
  ## Parse SQL and encode it as MessagePack bytes.
  astToMsgPack(parseSqlStatements(sql))

# --- PromQL MessagePack contract ---

proc writePromPosition(s: MsgStream; position: PromPosition) =
  s.pack_map(3)
  s.writeKey("line")
  s.pack_type(position.line)
  s.writeKey("column")
  s.pack_type(position.column)
  s.writeKey("offset")
  s.pack_type(position.offset)

proc writePromSpan(s: MsgStream; span: PromSpan) =
  s.pack_map(2)
  s.writeKey("start")
  s.writePromPosition(span.start)
  s.writeKey("end")
  s.writePromPosition(span.`end`)

proc writePromDuration(s: MsgStream; duration: PromDuration) =
  s.pack_map(2)
  s.writeKey("raw")
  s.pack_type(duration.raw)
  s.writeKey("milliseconds")
  s.pack_type(duration.milliseconds)

proc normalizedMatchOp(op: PromMatchOp): string =
  case op
  of pmEqual: "Equal"
  of pmNotEqual: "NotEqual"
  of pmRegex: "Regex"
  of pmNotRegex: "NotRegex"

proc normalizedPromBinaryOp(op: PromBinaryOp): string =
  case op
  of pbAdd: "Add"
  of pbSub: "Sub"
  of pbMul: "Mul"
  of pbDiv: "Div"
  of pbMod: "Mod"
  of pbPow: "Pow"

proc normalizedPromUnaryOp(op: PromUnaryOp): string =
  case op
  of puPlus: "Plus"
  of puMinus: "Minus"

proc writePromExpr(s: MsgStream; expr: PromExpr)

proc writePromMatcher(s: MsgStream; matcher: PromLabelMatcher) =
  s.pack_map(4)
  s.writeKey("name")
  s.pack_type(matcher.name)
  s.writeKey("op")
  s.pack_type(normalizedMatchOp(matcher.op))
  s.writeKey("value")
  s.pack_type(matcher.value)
  s.writeKey("span")
  s.writePromSpan(matcher.span)

proc writePromExprKind(s: MsgStream; expr: PromExpr) =
  case expr.kind
  of peVectorSelector:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("VectorSelector")
    s.writeKey("metric")
    s.writeStringOpt(expr.metric)
    s.writeKey("matchers")
    s.pack_array(expr.matchers.len)
    for matcher in expr.matchers:
      s.writePromMatcher(matcher)
    s.writeKey("offset")
    if expr.hasOffset:
      s.writePromDuration(expr.offset)
    else:
      s.writeNil()
  of peMatrixSelector:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("MatrixSelector")
    s.writeKey("selector")
    s.writePromExpr(expr.inner)
    s.writeKey("range")
    s.writePromDuration(expr.range)
    s.writeKey("offset")
    if expr.hasOffset:
      s.writePromDuration(expr.offset)
    else:
      s.writeNil()
  of peNumberLiteral:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("NumberLiteral")
    s.writeKey("value")
    s.pack_type(expr.numberRaw)
  of peStringLiteral:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("StringLiteral")
    s.writeKey("value")
    s.pack_type(expr.stringValue)
  of peFunctionCall:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("FunctionCall")
    s.writeKey("name")
    s.pack_type(expr.name)
    s.writeKey("args")
    s.pack_array(expr.args.len)
    for argument in expr.args:
      s.writePromExpr(argument)
  of peAggregate:
    s.pack_map(5)
    s.writeKey("variant")
    s.pack_type("Aggregate")
    s.writeKey("op")
    s.pack_type(expr.name)
    s.writeKey("expr")
    s.writePromExpr(expr.args[0])
    s.writeKey("grouping")
    if expr.groupingKind == pgNone:
      s.writeNil()
    else:
      s.pack_array(expr.groupingLabels.len)
      for label in expr.groupingLabels:
        s.pack_type(label)
    s.writeKey("without")
    s.pack_type(expr.groupingKind == pgWithout)
  of peBinary:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("BinaryOp")
    s.writeKey("left")
    s.writePromExpr(expr.left)
    s.writeKey("op")
    s.pack_type(normalizedPromBinaryOp(expr.binaryOp))
    s.writeKey("right")
    s.writePromExpr(expr.right)
  of peUnary:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("UnaryOp")
    s.writeKey("op")
    s.pack_type(normalizedPromUnaryOp(expr.unaryOp))
    s.writeKey("expr")
    s.writePromExpr(expr.operand)
  of peParen:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Paren")
    s.writeKey("expr")
    s.writePromExpr(expr.inner)

proc writePromExpr(s: MsgStream; expr: PromExpr) =
  s.pack_map(2)
  s.writeKey("kind")
  s.writePromExprKind(expr)
  s.writeKey("span")
  s.writePromSpan(expr.span)

proc encodePromQlToMsgPack*(query: string): string =
  ## Parse one PromQL expression and encode its independent wire AST.
  var stream = MsgStream.init()
  stream.writePromExpr(parsePromQl(query))
  result = stream.data

# --- Nim runtime initialization ---

proc NimMain() {.importc.}

proc alopex_parser_init*() {.exportc, dynlib, cdecl.} =
  ## Initialize Nim runtime. Must be called once before any parse calls.
  NimMain()

proc copyToOwnedBuffer(payload: string): pointer =
  if payload.len == 0:
    return nil
  result = alloc(payload.len)
  copyMem(result, unsafeAddr payload[0], payload.len)

proc errorResult(message: string): CParseResult =
  let copied = cast[cstring](alloc(message.len + 1))
  copyMem(copied, cstring(message), message.len + 1)
  CParseResult(
    kind: prkError,
    buffer_ptr: nil,
    buffer_len: 0,
    error_ptr: copied,
    error_len: cint(message.len),
  )

const internalDefectPrefix = "internal parser defect (this is a parser bug, not invalid SQL): "

proc alopex_parse_sql*(input: cstring, length: cint): CParseResult {.exportc, dynlib, cdecl.} =
  ## Parse SQL and return MessagePack-serialized AST bytes.
  ## Caller must free buffer_ptr with alopex_free_buffer.
  ##
  ## FFI 境界からは決して例外を漏らさない。例外が漏れると
  ## (--exceptions:goto では) スレッドのエラーフラグが立ったまま C 側へ
  ## 戻り、この呼び出しはゼロ初期化の CParseResult (= prkOk + 空バッファ)
  ## を返し、さらに同一スレッドの後続呼び出しも同じ経路で失敗し続ける
  ## ストリーム desync になる (issue #40)。そのため ParseError (通常の
  ## 構文エラー) に限らず CatchableError / Defect (パーサー内部の不変条件
  ## 違反、例: IndexDefect/FieldDefect) も全て prkError へ写像する。
  ##
  ## ただし両者は運用上の意味が異なる (前者はユーザー入力の誤り、後者は
  ## パーサーのバグ) ため、Defect のメッセージには `internalDefectPrefix`
  ## を付与し、Rust 側 (nim_bridge.rs) が機械的に ALOPEX-P007
  ## (InternalParserDefect) として区別できるようにする。MessagePack の
  ## ワイヤ契約 (docs/ffi-ast-contract.md) はエラー経路には及ばないため
  ## 不変。
  try:
    let sql = if length > 0: ($input)[0 ..< length] else: $input
    let payload = encodeSqlToMsgPack(sql)
    result = CParseResult(
      kind: prkOk,
      buffer_ptr: copyToOwnedBuffer(payload),
      buffer_len: cint(payload.len),
      error_ptr: nil,
      error_len: 0,
    )
  except CatchableError:
    result = errorResult(getCurrentExceptionMsg())
  except Defect:
    result = errorResult(internalDefectPrefix & getCurrentExceptionMsg())

proc alopex_parse_promql*(input: cstring, length: cint): CParseResult {.exportc, dynlib, cdecl.} =
  ## Parse one PromQL expression and return MessagePack-serialized AST bytes.
  ## Caller must free buffer_ptr or error_ptr with alopex_free_buffer.
  try:
    let query = if length > 0: ($input)[0 ..< length] else: $input
    let payload = encodePromQlToMsgPack(query)
    result = CParseResult(
      kind: prkOk,
      buffer_ptr: copyToOwnedBuffer(payload),
      buffer_len: cint(payload.len),
      error_ptr: nil,
      error_len: 0,
    )
  except CatchableError:
    result = errorResult(getCurrentExceptionMsg())
  except Defect:
    result = errorResult(internalDefectPrefix & getCurrentExceptionMsg())

proc alopex_free_buffer*(p: pointer) {.exportc, dynlib, cdecl.} =
  ## Free a buffer returned by alopex_parse_sql.
  if p != nil:
    dealloc(p)

proc alopex_parser_version*(): cstring {.exportc, dynlib, cdecl.} =
  ## Return SQL/PromQL wire contract version. Do NOT free this - it is static.
  "0.3.0"
