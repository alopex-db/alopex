## Alopex SQL Parser - C ABI entry point
##
## Exports C-compatible functions for FFI with Rust.
## Success payloads are MessagePack bytes containing seq[Statement].
## Build: nim c -d:release --app:lib --mm:orc -o:libalopex_sql_parser.so src/alopex_sql_parser.nim

import std/[streams, strutils]
import msgpack4nim
import ast, parser, promql_ast, promql_parser

func isExactContractDescriptor(raw, version: string): bool {.compileTime.} =
  raw == version or raw == version & "\n" or raw == version & "\r\n"

const parserContractDescriptor = staticRead("../PARSER_CONTRACT_VERSION")

static:
  doAssert isExactContractDescriptor(parserContractDescriptor, "0.3.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.6.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.7.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.8.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.9.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.10.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.11.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.12.0") or
      isExactContractDescriptor(parserContractDescriptor, "0.13.0") or
  isExactContractDescriptor(parserContractDescriptor, "0.19.0"),
    "PARSER_CONTRACT_VERSION must select an exact supported contract"

const parserContractVersion = parserContractDescriptor.strip()
const continuousAggregateProducerEnabled = parserContractVersion != "0.3.0"

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

proc writeKey(s: Stream; key: string) =
  s.pack_type(key)

proc writeNil(s: Stream) =
  s.pack_imp_nil()

proc writeStringOpt(s: Stream; value: string) =
  if value.len == 0:
    s.writeNil()
  else:
    s.pack_type(value)

proc writeBoolOpt(s: Stream; value: int) =
  case value
  of -1:
    s.writeNil()
  of 0:
    s.pack_type(false)
  else:
    s.pack_type(true)

proc writeLocation(s: Stream; loc: Location) =
  s.pack_map(2)
  s.writeKey("line")
  s.pack_type(loc.line)
  s.writeKey("column")
  s.pack_type(loc.column)

proc writeSpan(s: Stream; span: Span) =
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
  of opBitAnd: "BitAnd"
  of opBitOr: "BitOr"
  of opBitXor: "BitXor"
  of opShiftLeft: "ShiftLeft"
  of opShiftRight: "ShiftRight"
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
  of "opBitAnd": "BitAnd"
  of "opBitOr": "BitOr"
  of "opBitXor": "BitXor"
  of "opShiftLeft": "ShiftLeft"
  of "opShiftRight": "ShiftRight"
  else: opName

proc normalizedUnaryOp(op: UnaryOpKind): string =
  case op
  of opNot: "Not"
  of opNeg: "Minus"
  of opBitNot: "BitNot"
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

proc normalizedSetOperator(kind: SetOperatorKind): string =
  case kind
  of soUnion: "Union"
  of soIntersect: "Intersect"
  of soExcept: "Except"

proc normalizedDataTypeName(name: string): string =
  case name.toUpperAscii()
  of "INTEGER": "Integer"
  of "INT": "Int"
  of "SMALLINT": "Int"
  of "BIGINT": "BigInt"
  of "FLOAT": "Float"
  of "REAL": "Float"
  of "DOUBLE": "Double"
  of "DECIMAL", "NUMERIC": "Decimal"
  of "TEXT", "VARCHAR", "CHAR": "Text"
  of "BLOB": "Blob"
  of "BOOLEAN": "Boolean"
  of "BOOL": "Bool"
  of "TIMESTAMP": "Timestamp"
  of "DATE": "Date"
  of "TIME": "Time"
  of "INTERVAL": "Interval"
  of "JSON", "JSONB": "Json"
  of "ARRAY", "LIST": "Array"
  of "MAP": "Map"
  of "STRUCT": "Struct"
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
  of "FTS": "Fts"
  else: name

proc writeStatement(s: Stream; node: SqlNode)
proc writeExpr(s: Stream; node: SqlNode)
proc writeFromItem(s: Stream; node: SqlNode; publicWire = true)
proc writeDataType(s: Stream; node: SqlNode)
proc writeSelectKind(s: Stream; node: SqlNode)
proc writeQueryBody(s: Stream; node: SqlNode)

proc writeLiteralKind(s: Stream; node: SqlNode) =
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

proc writeExprOpt(s: Stream; node: SqlNode) =
  if node == nil:
    s.writeNil()
  else:
    s.writeExpr(node)

proc writeExprSeq(s: Stream; nodes: seq[SqlNode]) =
  s.pack_array(nodes.len)
  for child in nodes:
    s.writeExpr(child)

proc writeGroupByItem(s: Stream; node: SqlNode) =
  ## Contract 0.13.0 (issue #149): group_by carries tagged GroupByItem values
  ## instead of bare expressions on the public Select wire.
  case node.kind
  of nkRollup:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Rollup")
    s.writeKey("exprs")
    s.writeExprSeq(node.children)
  of nkCube:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Cube")
    s.writeKey("exprs")
    s.writeExprSeq(node.children)
  of nkGroupingSets:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("GroupingSets")
    s.writeKey("sets")
    s.pack_array(node.children.len)
    for groupingSet in node.children:
      s.writeExprSeq(groupingSet.children)
  else:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Expr")
    s.writeKey("expr")
    s.writeExpr(node)

proc writeCaseBranch(s: Stream; node: SqlNode) =
  s.pack_map(2)
  s.writeKey("when")
  s.writeExpr(node.caseWhen)
  s.writeKey("then")
  s.writeExpr(node.caseThen)

proc writeCommonTableExpr(s: Stream; node: SqlNode) =
  let hasColumns = node.children.len > 2 and
    node.children[1].kind == nkCteColumnList
  let queryIndex = if hasColumns: 2 else: 1
  s.pack_map(4)
  s.writeKey("name")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("columns")
  if hasColumns:
    s.pack_array(node.children[1].children.len)
    for column in node.children[1].children:
      s.pack_type(column.firstIdent())
  else:
    s.pack_array(0)
  s.writeKey("query")
  s.writeQueryBody(node.children[queryIndex])
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeWithClause(s: Stream; node: SqlNode) =
  s.pack_map(3)
  s.writeKey("recursive")
  s.pack_type(node.recursive)
  s.writeKey("ctes")
  s.pack_array(node.children.len)
  for cte in node.children:
    s.writeCommonTableExpr(cte)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeStringSeqOpt(s: Stream; values: seq[string]) =
  if values.len == 0:
    s.writeNil()
  else:
    s.pack_array(values.len)
    for value in values:
      s.pack_type(value)

proc writeSelectItem(s: Stream; node: SqlNode) =
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

proc writeOrderByExpr(s: Stream; node: SqlNode) =
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

proc writeWindowFrameBound(s: Stream; node: SqlNode) =
  case node.frameBoundKind
  of wfbUnboundedPreceding:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type("UnboundedPreceding")
  of wfbPreceding:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Preceding")
    s.writeKey("value")
    s.pack_type(node.frameOffset)
  of wfbCurrentRow:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type("CurrentRow")
  of wfbFollowing:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Following")
    s.writeKey("value")
    s.pack_type(node.frameOffset)
  of wfbUnboundedFollowing:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type("UnboundedFollowing")

proc writeWindowFrame(s: Stream; node: SqlNode) =
  s.pack_map(3)
  s.writeKey("units")
  s.pack_type(if node.frameUnit == wfuRows: "Rows" else: "Range")
  s.writeKey("start_bound")
  s.writeWindowFrameBound(node.frameStart)
  s.writeKey("end_bound")
  s.writeWindowFrameBound(node.frameEnd)

proc writeWindowSpec(s: Stream; node: SqlNode) =
  var baseNode: SqlNode = nil
  var partitionByNode: SqlNode = nil
  var orderByNode: SqlNode = nil
  var frameNode: SqlNode = nil
  for child in node.children:
    case child.kind
    of nkIdentifier:
      baseNode = child
    of nkPartitionByClause:
      partitionByNode = child
    of nkOrderByClause:
      orderByNode = child
    of nkWindowFrame:
      frameNode = child
    else:
      discard

  s.pack_map(4)
  s.writeKey("base")
  if baseNode == nil:
    s.writeNil()
  else:
    s.pack_type(baseNode.strVal)
  s.writeKey("partition_by")
  if partitionByNode == nil:
    s.pack_array(0)
  else:
    s.writeExprSeq(partitionByNode.children)
  s.writeKey("order_by")
  if orderByNode == nil:
    s.pack_array(0)
  else:
    s.pack_array(orderByNode.children.len)
    for item in orderByNode.children:
      s.writeOrderByExpr(item)
  s.writeKey("frame")
  if frameNode == nil:
    s.writeNil()
  else:
    s.writeWindowFrame(frameNode)

proc writeIndexOption(s: Stream; node: SqlNode) =
  s.pack_map(3)
  s.writeKey("key")
  s.pack_type(node.children[0].firstIdent())
  s.writeKey("value")
  s.pack_type(if node.children.len > 1: node.children[1].firstIdent() else: "")
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeIndexOptions(s: Stream; node: SqlNode) =
  if node == nil:
    s.pack_array(0)
    return
  s.pack_array(node.children.len)
  for opt in node.children:
    s.writeIndexOption(opt)

proc writeColumnConstraint(s: Stream; node: SqlNode) =
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
  of "CHECK":
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Check")
    s.writeKey("expression")
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

proc writeColumnDef(s: Stream; node: SqlNode) =
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

proc writeTableConstraint(s: Stream; node: SqlNode) =
  var constraintType = "PrimaryKey"
  var refTable = ""
  var refColumns: seq[string] = @[]
  var onDelete: string = ""
  var onUpdate: string = ""
  var startIdx = 0
  
  if node.children.len > 0 and node.children[0].kind == nkIdentifier:
    let typeStr = node.children[0].strVal.toUpperAscii()
    if typeStr == "PRIMARY":
      constraintType = "PrimaryKey"
      startIdx = 1
    elif typeStr == "UNIQUE":
      constraintType = "Unique"
      startIdx = 1
    elif typeStr == "FOREIGN":
      constraintType = "ForeignKey"
      startIdx = 1
      # For FOREIGN KEY, we expect: FOREIGN KEY ( col1, col2, ... ) REFERENCES table ( refcol1, refcol2, ... )
      # For now, we only extract the columns and table name; full parsing of ON DELETE/UPDATE would require
      # more complex state tracking in the parser.
      if node.children.len > 2:
        refTable = node.children[node.children.len - 2].firstIdent()
    elif typeStr == "CONSTRAINT":
      startIdx = 1
      if node.children.len > 1:
        let nextType = node.children[1].strVal.toUpperAscii()
        if nextType == "PRIMARY":
          constraintType = "PrimaryKey"
          startIdx = 2
        elif nextType == "UNIQUE":
          constraintType = "Unique"
          startIdx = 2
        elif nextType == "FOREIGN":
          constraintType = "ForeignKey"
          startIdx = 2
  
  if constraintType == "ForeignKey":
    s.pack_map(6)
    s.writeKey("variant")
    s.pack_type("ForeignKey")
    s.writeKey("columns")
    s.pack_array(max(node.children.len - startIdx - 1, 0))
    for i in startIdx ..< node.children.len - 1:
      s.pack_type(node.children[i].firstIdent())
    s.writeKey("ref_table")
    s.pack_type(refTable)
    s.writeKey("ref_columns")
    s.pack_array(0) # TODO: properly extract ref columns
    s.writeKey("on_delete")
    if onDelete.len > 0:
      s.pack_type(onDelete)
    else:
      s.writeNil()
    s.writeKey("on_update")
    if onUpdate.len > 0:
      s.pack_type(onUpdate)
    else:
      s.writeNil()
  else:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type(constraintType)
    s.writeKey("columns")
    s.pack_array(max(node.children.len - startIdx, 0))
    for i in startIdx ..< node.children.len:
      s.pack_type(node.children[i].firstIdent())
  
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeDataType(s: Stream; node: SqlNode) =
  let rawName = node.firstIdent()
  let variant = normalizedDataTypeName(rawName)
  if variant == "Array":
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Array")
    s.writeKey("element")
    s.writeDataType(node.children[1])
  elif variant == "Map":
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Map")
    s.writeKey("key")
    s.writeDataType(node.children[1])
    s.writeKey("value")
    s.writeDataType(node.children[2])
  elif variant == "Struct":
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Struct")
    s.writeKey("fields")
    s.pack_array((node.children.len - 1) div 2)
    var fieldIndex = 1
    while fieldIndex + 1 < node.children.len:
      s.pack_map(2)
      s.writeKey("name")
      s.pack_type(node.children[fieldIndex].firstIdent())
      s.writeKey("data_type")
      s.writeDataType(node.children[fieldIndex + 1])
      fieldIndex += 2
  elif variant == "Vector":
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
  elif variant == "Decimal":
    var precision = 38
    var scale = 0
    if node.children.len > 1 and node.children[1].kind == nkIntLit:
      precision = node.children[1].intVal.int
    if node.children.len > 2 and node.children[2].kind == nkIntLit:
      scale = node.children[2].intVal.int
    if precision < 1 or precision > 38 or scale < 0 or scale > precision:
      raise newException(ValueError,
        "DECIMAL requires 1 <= precision <= 38 and 0 <= scale <= precision")
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Decimal")
    s.writeKey("precision")
    s.pack_type(uint8(precision))
    s.writeKey("scale")
    s.pack_type(uint8(scale))
  else:
    s.pack_map(1)
    s.writeKey("variant")
    s.pack_type(variant)

proc writeTableFromItem(s: Stream; name: string; alias: SqlNode;
                        columns: seq[string]; span: Span; publicWire: bool) =
  # Contract 0.14.0 (issue #151): the public Table variant carries the alias
  # column-name list. The staged continuous-aggregate encoder intentionally
  # remains byte-for-byte compatible with its historical 4-key payload.
  s.pack_map(if publicWire: 5 else: 4)
  s.writeKey("variant")
  s.pack_type("Table")
  s.writeKey("name")
  s.pack_type(name)
  s.writeKey("alias")
  if alias == nil:
    s.writeNil()
  else:
    s.pack_type(alias.aliasName)
  if publicWire:
    s.writeKey("columns")
    s.pack_array(columns.len)
    for column in columns:
      s.pack_type(column)
  s.writeKey("span")
  s.writeSpan(span)

proc writeDerivedFromItem(s: Stream; derived: SqlNode; alias: SqlNode;
                          columns: seq[string]; span: Span; publicWire: bool) =
  # Contract 0.14.0 (issue #151): the public Derived variant carries `lateral`.
  # The staged encoder keeps its historical 5-key payload; the parser rejects
  # LATERAL inside CREATE CONTINUOUS AGGREGATE before encoding.
  s.pack_map(if publicWire: 6 else: 5)
  s.writeKey("variant")
  s.pack_type("Derived")
  s.writeKey("subquery")
  s.writeQueryBody(derived.children[0])
  s.writeKey("alias")
  if alias == nil:
    s.writeNil()
  else:
    s.pack_type(alias.aliasName)
  s.writeKey("columns")
  s.pack_array(columns.len)
  for column in columns:
    s.pack_type(column)
  if publicWire:
    s.writeKey("lateral")
    s.pack_type(derived.lateral)
  s.writeKey("span")
  s.writeSpan(span)

proc writeFunctionFromItem(s: Stream; function: SqlNode; alias: SqlNode;
                           columns: seq[string]; span: Span) =
  # Contract 0.14.0 (issue #151). Only the public wire carries this variant;
  # the staged continuous-aggregate validator rejects it before encoding.
  s.pack_map(8)
  s.writeKey("variant")
  s.pack_type("Function")
  s.writeKey("name")
  s.pack_type(function.children[0].firstIdent())
  s.writeKey("args")
  s.pack_array(max(function.children.len - 1, 0))
  for i in 1 ..< function.children.len:
    s.writeExpr(function.children[i])
  s.writeKey("alias")
  if alias == nil:
    s.writeNil()
  else:
    s.pack_type(alias.aliasName)
  s.writeKey("columns")
  s.pack_array(columns.len)
  for column in columns:
    s.pack_type(column)
  s.writeKey("lateral")
  s.pack_type(function.lateral)
  s.writeKey("with_ordinality")
  s.pack_type(function.withOrdinality)
  s.writeKey("span")
  s.writeSpan(span)

proc writeFromItem(s: Stream; node: SqlNode; publicWire = true) =
  if node == nil:
    s.writeNil()
    return

  case node.kind
  of nkAlias:
    case node.aliasExpr.kind
    of nkFromDerived:
      s.writeDerivedFromItem(node.aliasExpr, node, node.aliasColumns, node.span,
                             publicWire)
    of nkFromFunction:
      s.writeFunctionFromItem(node.aliasExpr, node, node.aliasColumns, node.span)
    else:
      s.writeTableFromItem(node.aliasExpr.firstIdent(), node, node.aliasColumns,
                           node.span, publicWire)
  of nkIdentifier:
    s.writeTableFromItem(node.strVal, nil, @[], node.span, publicWire)
  of nkFromDerived:
    s.writeDerivedFromItem(node, nil, @[], node.span, publicWire)
  of nkFromFunction:
    s.writeFunctionFromItem(node, nil, @[], node.span)
  of nkJoin, nkFromJoin:
    s.pack_map(7)
    s.writeKey("variant")
    s.pack_type("Join")
    s.writeKey("left")
    s.writeFromItem(node.joinLeft, publicWire)
    s.writeKey("right")
    s.writeFromItem(node.joinRight, publicWire)
    s.writeKey("join_type")
    s.pack_type(normalizedJoinKind(node.joinKind))
    s.writeKey("condition")
    s.writeExprOpt(node.joinCond)
    s.writeKey("using")
    s.writeStringSeqOpt(node.joinUsing)
    s.writeKey("span")
    s.writeSpan(node.span)
  else:
    s.writeTableFromItem(node.firstIdent(), nil, @[], node.span, publicWire)

proc writeExpr(s: Stream; node: SqlNode) =
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
  of nkRowConstructor:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Row")
    s.writeKey("items")
    s.writeExprSeq(node.children)
  of nkTruthPredicate:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("TruthPredicate")
    s.writeKey("expr")
    s.writeExpr(node.children[0])
    s.writeKey("value")
    s.pack_type(node.children[1].strVal.toLowerAscii().capitalizeAscii())
    s.writeKey("negated")
    s.pack_type(node.negated)
  of nkIsDistinctFrom:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("IsDistinctFrom")
    s.writeKey("left")
    s.writeExpr(node.children[0])
    s.writeKey("right")
    s.writeExpr(node.children[1])
    s.writeKey("negated")
    s.pack_type(node.negated)
  of nkCase:
    s.pack_map(4)
    s.writeKey("variant")
    s.pack_type("Case")
    s.writeKey("operand")
    s.writeExprOpt(node.caseOperand)
    s.writeKey("branches")
    s.pack_array(node.caseBranches.len)
    for branch in node.caseBranches:
      s.writeCaseBranch(branch)
    s.writeKey("else_expr")
    s.writeExprOpt(node.caseElse)
  of nkFunctionCall:
    # Trailing clause nodes are appended by the parser after the argument
    # expressions in the fixed order [ORDER BY, WITHIN GROUP, FILTER, OVER].
    var windowNode: SqlNode = nil
    var orderByNode: SqlNode = nil
    var withinGroupNode: SqlNode = nil
    var filterNode: SqlNode = nil
    var argEnd = node.children.len
    while argEnd > 1:
      case node.children[argEnd - 1].kind
      of nkWindowSpec: windowNode = node.children[argEnd - 1]
      of nkAggFilterClause: filterNode = node.children[argEnd - 1]
      of nkWithinGroupClause: withinGroupNode = node.children[argEnd - 1]
      of nkOrderByClause: orderByNode = node.children[argEnd - 1]
      else: break
      dec argEnd
    # Contract 0.12.0 (issue #148): the three aggregate-clause keys are
    # written together whenever any clause is present. Clause-free calls keep
    # the historical 6-key map so the byte-frozen staged continuous-aggregate
    # payload is untouched; the Rust reader takes the absent keys as defaults.
    let hasAggregateClauses =
      orderByNode != nil or withinGroupNode != nil or filterNode != nil
    s.pack_map(if hasAggregateClauses: 9 else: 6)
    s.writeKey("variant")
    s.pack_type("FunctionCall")
    s.writeKey("name")
    s.pack_type(node.children[0].firstIdent())
    s.writeKey("args")
    var argCount = argEnd - 1
    if node.funcStar:
      argCount = 0
    s.pack_array(max(argCount, 0))
    if not node.funcStar:
      for i in 1 ..< argEnd:
        s.writeExpr(node.children[i])
    s.writeKey("distinct")
    s.pack_type(node.funcDistinct)
    s.writeKey("star")
    s.pack_type(node.funcStar)
    if hasAggregateClauses:
      s.writeKey("order_by")
      if orderByNode == nil:
        s.pack_array(0)
      else:
        s.pack_array(orderByNode.children.len)
        for item in orderByNode.children:
          s.writeOrderByExpr(item)
      s.writeKey("within_group")
      if withinGroupNode == nil:
        s.pack_array(0)
      else:
        s.pack_array(withinGroupNode.children.len)
        for item in withinGroupNode.children:
          s.writeOrderByExpr(item)
      s.writeKey("filter")
      if filterNode == nil:
        s.writeNil()
      else:
        s.writeExpr(filterNode.children[0])
    s.writeKey("over")
    if windowNode == nil:
      s.writeNil()
    else:
      s.writeWindowSpec(windowNode)
  of nkCast:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("Cast")
    s.writeKey("expr")
    s.writeExpr(node.children[0])
    s.writeKey("target_type")
    s.writeDataType(node.children[1])
  of nkTryCast:
    s.pack_map(3)
    s.writeKey("variant")
    s.pack_type("TryCast")
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

proc writeSelectFields(s: Stream; node: SqlNode; includeWith = true) =
  var withNode: SqlNode = nil
  var distinctFlag = false
  var distinctOnNode: SqlNode = nil
  var projectionNode: SqlNode = nil
  var fromNode: SqlNode = nil
  var selectionNode: SqlNode = nil
  var groupByNode: SqlNode = nil
  var havingNode: SqlNode = nil
  var windowsNode: SqlNode = nil
  var qualifyNode: SqlNode = nil
  var orderByNode: SqlNode = nil
  var limitNode: SqlNode = nil
  var offsetNode: SqlNode = nil
  var setOperations: seq[SqlNode] = @[]

  for child in node.children:
    case child.kind
    of nkWithClause:
      withNode = child
    of nkIdentifier:
      if child.strVal == "DISTINCT":
        distinctFlag = true
    of nkDistinctOnClause:
      distinctOnNode = child
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
    of nkWindowClause:
      windowsNode = child
    of nkQualifyClause:
      qualifyNode = child.children[0]
    of nkOrderByClause:
      orderByNode = child
    of nkLimitClause:
      limitNode = child
    of nkOffsetClause:
      offsetNode = child
    of nkSetOperation:
      setOperations.add(child)
    else:
      discard

  s.writeKey("variant")
  s.pack_type("Select")
  if includeWith and withNode != nil:
    s.writeKey("with")
    s.writeWithClause(withNode)
  s.writeKey("distinct")
  s.pack_type(distinctFlag)
  # The staged continuous-aggregate encoder intentionally remains byte-for-byte
  # compatible with its historical payload; distinct_on belongs to the current
  # public Select contract only (contract 0.11.0, issue #150).
  if includeWith:
    s.writeKey("distinct_on")
    if distinctOnNode == nil:
      s.pack_array(0)
    else:
      s.writeExprSeq(distinctOnNode.children)
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
      # `includeWith` marks the public encoder; the staged continuous-aggregate
      # encoder keeps the historical FROM-item payload (contract 0.14.0).
      s.writeFromItem(item, includeWith)
  s.writeKey("selection")
  s.writeExprOpt(selectionNode)
  s.writeKey("group_by")
  if groupByNode == nil:
    s.writeNil()
  elif includeWith:
    # Contract 0.13.0 (issue #149): tagged GroupByItem values.
    s.pack_array(groupByNode.children.len)
    for item in groupByNode.children:
      s.writeGroupByItem(item)
  else:
    # The staged continuous-aggregate encoder intentionally remains
    # byte-for-byte compatible with its historical [Expr] payload; the parser
    # rejects grouping-set modifiers inside continuous aggregates (D10).
    s.writeExprSeq(groupByNode.children)
  s.writeKey("having")
  s.writeExprOpt(havingNode)
  # The staged continuous-aggregate encoder intentionally remains byte-for-byte
  # compatible with its historical payload. Named windows and QUALIFY belong to
  # the current public Select contract only.
  if includeWith:
    s.writeKey("windows")
    if windowsNode == nil:
      s.pack_array(0)
    else:
      s.pack_array(windowsNode.children.len)
      for namedWindow in windowsNode.children:
        s.pack_map(3)
        s.writeKey("name")
        s.pack_type(namedWindow.children[0].strVal)
        s.writeKey("spec")
        s.writeWindowSpec(namedWindow.children[1])
        s.writeKey("span")
        s.writeSpan(namedWindow.span)
    s.writeKey("qualify")
    s.writeExprOpt(qualifyNode)
  s.writeKey("set_operations")
  s.pack_array(setOperations.len)
  for setOperation in setOperations:
    s.pack_map(4)
    s.writeKey("operator")
    s.pack_type(normalizedSetOperator(setOperation.setOp))
    s.writeKey("all")
    s.pack_type(setOperation.setAll)
    s.writeKey("right")
    s.writeQueryBody(setOperation.setRight)
    s.writeKey("span")
    s.writeSpan(setOperation.span)
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
  if offsetNode != nil and offsetNode.children.len > 0:
    s.writeExpr(offsetNode.children[0])
  else:
    s.writeNil()
  # The staged continuous-aggregate encoder intentionally remains byte-for-byte
  # compatible with its historical payload; limit_with_ties belongs to the
  # current public Select contract only (contract 0.10.0, issue #152).
  if includeWith:
    s.writeKey("limit_with_ties")
    s.pack_type(limitNode != nil and limitNode.limitWithTies)

proc writeSelectKind(s: Stream; node: SqlNode) =
  # 固定 15 キー(variant/distinct/distinct_on/projection/from/selection/
  # group_by/having/windows/qualify/set_operations/order_by/limit/offset/
  # limit_with_ties)に、WITH 句があれば with を加えて 16 になる。
  var fieldCount = 15
  for child in node.children:
    if child.kind == nkWithClause:
      fieldCount = 16
      break
  s.pack_map(fieldCount)
  s.writeSelectFields(node)

proc writeValuesKind(s: Stream; node: SqlNode) =
  var withNode: SqlNode = nil
  var orderByNode: SqlNode = nil
  var limitNode: SqlNode = nil
  var offsetNode: SqlNode = nil
  var rows: seq[SqlNode] = @[]
  var setOperations: seq[SqlNode] = @[]

  for child in node.children:
    case child.kind
    of nkWithClause:
      withNode = child
    of nkExprList:
      rows.add(child)
    of nkSetOperation:
      setOperations.add(child)
    of nkOrderByClause:
      orderByNode = child
    of nkLimitClause:
      limitNode = child
    of nkOffsetClause:
      offsetNode = child
    else:
      discard

  s.pack_map(if withNode == nil: 8 else: 9)
  s.writeKey("variant")
  s.pack_type("Values")
  if withNode != nil:
    s.writeKey("with")
    s.writeWithClause(withNode)
  s.writeKey("rows")
  s.pack_array(rows.len)
  for row in rows:
    s.writeExprSeq(row.children)
  s.writeKey("set_operations")
  s.pack_array(setOperations.len)
  for setOperation in setOperations:
    s.pack_map(4)
    s.writeKey("operator")
    s.pack_type(normalizedSetOperator(setOperation.setOp))
    s.writeKey("all")
    s.pack_type(setOperation.setAll)
    s.writeKey("right")
    s.writeQueryBody(setOperation.setRight)
    s.writeKey("span")
    s.writeSpan(setOperation.span)
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
  if offsetNode != nil and offsetNode.children.len > 0:
    s.writeExpr(offsetNode.children[0])
  else:
    s.writeNil()
  s.writeKey("limit_with_ties")
  s.pack_type(limitNode != nil and limitNode.limitWithTies)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeQueryBody(s: Stream; node: SqlNode) =
  case node.kind
  of nkSelect:
    s.writeSelectKind(node)
  of nkValues:
    s.writeValuesKind(node)
  else:
    raise newException(ParseError,
      "unsupported query body node for MessagePack: " & $node.kind)

proc writeContinuousAggregateQuery(s: Stream; node: SqlNode) =
  # 継続集約のクエリは WITH を含めない(includeWith = false)ため、
  # Historical staged payload: Select の固定 11 field と statement span の
  # 合計で常に 12。Current-only fields must not alter this byte contract.
  s.pack_map(12)
  s.writeSelectFields(node, false)
  s.writeKey("span")
  s.writeSpan(node.span)

proc writeInsertKind(s: Stream; node: SqlNode) =
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
  elif source != nil and source.kind == nkValues:
    s.pack_map(2)
    s.writeKey("variant")
    s.pack_type("Query")
    s.writeKey("query")
    s.writeQueryBody(source)
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

proc writeUpdateKind(s: Stream; node: SqlNode) =
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

proc writeDeleteKind(s: Stream; node: SqlNode) =
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

proc writeCreateTableKind(s: Stream; node: SqlNode) =
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

proc writeDropTableKind(s: Stream; node: SqlNode) =
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

proc writeCreateIndexKind(s: Stream; node: SqlNode) =
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

proc writeDropIndexKind(s: Stream; node: SqlNode) =
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

proc writePragmaKind(s: Stream; node: SqlNode) =
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

const
  maxStagedPayloadBytes = 1_048_576
  maxStagedPayloadDepth = 128
  stagedSizeError = "staged MessagePack exceeds 1048576-byte limit"
  stagedDepthError = "maximum staged MessagePack nesting depth of 128 exceeded"

type
  StagedLengthKind = enum
    slString, slBinary, slArray, slMap, slExtension

  BoundedMsgPackStream = ref object of StreamObj
    total: int
    data: string
    retainBytes: bool
    expectedBytes: int
    rootValues: int
    frameCount: int
    frames: array[maxStagedPayloadDepth, uint64]
    maximumDepth: int
    pendingPayloadBytes: uint64
    pendingLengthBytes: int
    pendingLength: uint64
    pendingLengthKind: StagedLengthKind

proc stagedValidationError(message: string) {.noreturn.} =
  raise newException(ValueError, message)

proc stagedStreamError(message: string) {.noreturn.} =
  raise newException(IOError, message)

proc finishCompletedFrames(s: BoundedMsgPackStream) =
  while s.frameCount > 0 and s.frames[s.frameCount - 1] == 0:
    dec s.frameCount

proc beginStagedValue(s: BoundedMsgPackStream) =
  if s.frameCount == 0:
    inc s.rootValues
  else:
    if s.frames[s.frameCount - 1] == 0:
      stagedStreamError("malformed staged MessagePack container")
    dec s.frames[s.frameCount - 1]

proc beginStagedScalar(s: BoundedMsgPackStream; payloadBytes: uint64 = 0) =
  s.beginStagedValue()
  s.pendingPayloadBytes = payloadBytes
  s.finishCompletedFrames()

proc beginStagedContainer(s: BoundedMsgPackStream; itemCount: uint64) =
  s.beginStagedValue()
  let depth = s.frameCount + 1
  if depth > maxStagedPayloadDepth:
    stagedStreamError(stagedDepthError)
  if depth > s.maximumDepth:
    s.maximumDepth = depth
  if itemCount == 0:
    s.finishCompletedFrames()
  else:
    s.frames[s.frameCount] = itemCount
    inc s.frameCount

proc startStagedLength(s: BoundedMsgPackStream; kind: StagedLengthKind;
                       byteCount: int) =
  s.pendingLengthKind = kind
  s.pendingLengthBytes = byteCount
  s.pendingLength = 0

proc finishStagedLength(s: BoundedMsgPackStream) =
  case s.pendingLengthKind
  of slString, slBinary:
    s.beginStagedScalar(s.pendingLength)
  of slArray:
    s.beginStagedContainer(s.pendingLength)
  of slMap:
    s.beginStagedContainer(s.pendingLength * 2'u64)
  of slExtension:
    s.beginStagedScalar(s.pendingLength + 1'u64)

proc observeStagedByte(s: BoundedMsgPackStream; value: uint8) =
  if s.pendingPayloadBytes > 0:
    dec s.pendingPayloadBytes
    return
  if s.pendingLengthBytes > 0:
    s.pendingLength = (s.pendingLength shl 8) or uint64(value)
    dec s.pendingLengthBytes
    if s.pendingLengthBytes == 0:
      s.finishStagedLength()
    return

  case value
  of 0x00'u8 .. 0x7f'u8, 0xc0'u8, 0xc2'u8, 0xc3'u8,
      0xe0'u8 .. 0xff'u8:
    s.beginStagedScalar()
  of 0x80'u8 .. 0x8f'u8:
    s.beginStagedContainer(uint64(value and 0x0f'u8) * 2'u64)
  of 0x90'u8 .. 0x9f'u8:
    s.beginStagedContainer(uint64(value and 0x0f'u8))
  of 0xa0'u8 .. 0xbf'u8:
    s.beginStagedScalar(uint64(value and 0x1f'u8))
  of 0xc4'u8: s.startStagedLength(slBinary, 1)
  of 0xc5'u8: s.startStagedLength(slBinary, 2)
  of 0xc6'u8: s.startStagedLength(slBinary, 4)
  of 0xc7'u8: s.startStagedLength(slExtension, 1)
  of 0xc8'u8: s.startStagedLength(slExtension, 2)
  of 0xc9'u8: s.startStagedLength(slExtension, 4)
  of 0xca'u8: s.beginStagedScalar(4)
  of 0xcb'u8: s.beginStagedScalar(8)
  of 0xcc'u8, 0xd0'u8: s.beginStagedScalar(1)
  of 0xcd'u8, 0xd1'u8: s.beginStagedScalar(2)
  of 0xce'u8, 0xd2'u8: s.beginStagedScalar(4)
  of 0xcf'u8, 0xd3'u8: s.beginStagedScalar(8)
  of 0xd4'u8: s.beginStagedScalar(2)
  of 0xd5'u8: s.beginStagedScalar(3)
  of 0xd6'u8: s.beginStagedScalar(5)
  of 0xd7'u8: s.beginStagedScalar(9)
  of 0xd8'u8: s.beginStagedScalar(17)
  of 0xd9'u8: s.startStagedLength(slString, 1)
  of 0xda'u8: s.startStagedLength(slString, 2)
  of 0xdb'u8: s.startStagedLength(slString, 4)
  of 0xdc'u8: s.startStagedLength(slArray, 2)
  of 0xdd'u8: s.startStagedLength(slArray, 4)
  of 0xde'u8: s.startStagedLength(slMap, 2)
  of 0xdf'u8: s.startStagedLength(slMap, 4)
  else:
    stagedStreamError("invalid staged MessagePack marker")

proc writeBoundedMsgPack(s: Stream; buffer: pointer; bufLen: int) =
  let bounded = BoundedMsgPackStream(s)
  if bufLen < 0 or bufLen > maxStagedPayloadBytes - bounded.total:
    stagedStreamError(stagedSizeError)
  if bufLen == 0:
    return

  let bytes = cast[ptr UncheckedArray[uint8]](buffer)
  for i in 0 ..< bufLen:
    bounded.observeStagedByte(bytes[i])

  if bounded.retainBytes:
    let oldLen = bounded.data.len
    bounded.data.setLen(oldLen + bufLen)
    copyMem(addr bounded.data[oldLen], buffer, bufLen)
  bounded.total += bufLen

proc newBoundedMsgPackStream(expectedBytes = -1): BoundedMsgPackStream =
  new(result)
  result.writeDataImpl = writeBoundedMsgPack
  result.expectedBytes = expectedBytes
  result.retainBytes = expectedBytes >= 0
  if result.retainBytes:
    result.data = newStringOfCap(expectedBytes)

proc finishBoundedMsgPack(s: BoundedMsgPackStream) =
  if s.pendingPayloadBytes != 0 or s.pendingLengthBytes != 0 or
      s.frameCount != 0 or s.rootValues != 1:
    stagedValidationError("staged writer produced malformed MessagePack")
  if s.expectedBytes >= 0 and s.total != s.expectedBytes:
    stagedValidationError("staged MessagePack counting pass mismatch")

proc validateStagedSpan(node: SqlNode) =
  let start = node.span.start
  let finish = node.span.`end`
  if start.line < 1 or start.column < 1 or finish.line < 1 or
      finish.column < 1 or finish.line < start.line or
      (finish.line == start.line and finish.column < start.column):
    stagedValidationError("staged MessagePack tree contains an invalid span")

proc validateStagedWriterShape(node: SqlNode) =
  ## Validate only cardinalities dereferenced by the existing MessagePack
  ## writers. SQL grammar and semantic validity remain parser-owned.
  template requireChildren(count: int; label: string) =
    if node.children.len != count:
      stagedValidationError(label & " must have exactly " & $count & " child" &
        (if count == 1: "" else: "ren"))

  case node.kind
  of nkQualifiedStar:
    requireChildren(1, "qualified wildcard")
  of nkColumnRef:
    requireChildren(2, "column reference")
  of nkFunctionCall:
    if node.children.len < 1:
      stagedValidationError("function call must have at least 1 child")
    elif node.children.len > 1:
      for i in 1 ..< node.children.len:
        case node.children[i].kind
        of nkAggFilterClause, nkWithinGroupClause, nkOrderByClause:
          # These clauses have no representation in the byte-frozen staged
          # 6-key FunctionCall payload (issue #148).
          stagedValidationError(
            "staged continuous aggregate query cannot contain aggregate " &
            "FILTER, WITHIN GROUP, or aggregate ORDER BY")
        of nkWindowSpec:
          if i != node.children.len - 1:
            stagedValidationError(
              "window specification must be the last function-call child")
        else:
          discard
  of nkWindowSpec:
    var sawBase = false
    var sawPartitionBy = false
    var sawOrderBy = false
    var sawFrame = false
    for child in node.children:
      case child.kind
      of nkIdentifier:
        if sawBase or sawPartitionBy or sawOrderBy or sawFrame:
          stagedValidationError("invalid base in window specification")
        sawBase = true
      of nkPartitionByClause:
        if sawPartitionBy or sawOrderBy or sawFrame or child.children.len == 0:
          stagedValidationError("invalid PARTITION BY in window specification")
        sawPartitionBy = true
      of nkOrderByClause:
        if sawOrderBy or sawFrame or child.children.len == 0:
          stagedValidationError("invalid ORDER BY in window specification")
        sawOrderBy = true
      of nkWindowFrame:
        if sawFrame or child.frameStart == nil or child.frameEnd == nil:
          stagedValidationError("invalid frame in window specification")
        sawFrame = true
      else:
        stagedValidationError("invalid child in window specification")
  of nkWindowClause:
    if node.children.len == 0:
      stagedValidationError("WINDOW clause must contain a definition")
    for child in node.children:
      if child.kind != nkNamedWindow:
        stagedValidationError("WINDOW clause contains an invalid definition")
  of nkNamedWindow:
    requireChildren(2, "named window")
    if node.children[0].kind != nkIdentifier or
        node.children[1].kind != nkWindowSpec:
      stagedValidationError("named window must contain a name and specification")
  of nkWindowFrame:
    if node.frameStart == nil or node.frameStart.kind != nkWindowFrameBound or
        node.frameEnd == nil or node.frameEnd.kind != nkWindowFrameBound:
      stagedValidationError("window frame must contain two bounds")
  of nkWindowFrameBound:
    if node.frameBoundKind notin {wfbPreceding, wfbFollowing} and
        node.frameOffset != 0:
      stagedValidationError("non-offset window frame bound has an offset")
  of nkCast, nkTryCast:
    requireChildren(2, "cast expression")
  of nkCase:
    if node.caseBranches.len == 0:
      stagedValidationError("CASE expression must have at least 1 branch")
    for branch in node.caseBranches:
      if branch == nil or branch.kind != nkCaseWhen or branch.caseWhen == nil or
          branch.caseThen == nil:
        stagedValidationError("CASE branch must contain WHEN and THEN expressions")
  of nkCaseWhen:
    if node.caseWhen == nil or node.caseThen == nil:
      stagedValidationError("CASE branch must contain WHEN and THEN expressions")
  of nkScalarSubquery, nkExists, nkFromDerived:
    requireChildren(1, "subquery")
    # LATERAL has no representation in the frozen staged FROM-item payload
    # (issue #151, contract 0.14.0).
    if node.kind == nkFromDerived and node.lateral:
      stagedValidationError(
        "staged continuous aggregate query cannot contain LATERAL"
      )
  of nkFromFunction:
    # FROM-clause table functions have no representation in the frozen staged
    # payload (issue #151, contract 0.14.0).
    stagedValidationError(
      "staged continuous aggregate query cannot contain a FROM table function"
    )
  of nkAlias:
    # A relation alias column-name list is only carried by the public Table
    # variant; the staged Table payload has no `columns` key (issue #151).
    if node.aliasColumns.len > 0 and node.aliasExpr != nil and
        node.aliasExpr.kind == nkIdentifier:
      stagedValidationError(
        "staged continuous aggregate query cannot contain a table alias column list"
      )
  of nkInSubquery:
    requireChildren(2, "IN subquery")
  of nkQuantified:
    requireChildren(3, "quantified expression")
  of nkWhereClause:
    requireChildren(1, "WHERE clause")
  of nkHavingClause:
    requireChildren(1, "HAVING clause")
  of nkQualifyClause:
    requireChildren(1, "QUALIFY clause")
  of nkBinaryOp:
    if node.binLeft == nil or node.binRight == nil:
      stagedValidationError("binary expression operands must not be nil")
    elif node.binOp in {opBetween, opNotBetween}:
      if node.binRight.kind != nkExprList or node.binRight.children.len != 2:
        stagedValidationError("BETWEEN bounds must have exactly 2 children")
    elif node.binOp in {opLike, opNotLike, opILike, opNotILike, opGlob,
        opNotGlob, opSimilarTo, opNotSimilarTo} and
        node.binRight.kind == nkExprList and node.binRight.children.len < 1:
      stagedValidationError("pattern expression list must not be empty")
  of nkVectorLiteral:
    for child in node.children:
      if child == nil or child.kind != nkFloatLit:
        stagedValidationError("vector literal children must be nkFloatLit")
  of nkSetOperation:
    if node.setRight == nil or node.setRight.kind != nkSelect:
      stagedValidationError("set-operation right input must be nkSelect")
  else:
    discard

proc validateStagedAst(node: SqlNode; depth: int;
                       ancestors: var array[maxStagedPayloadDepth, SqlNode]) =
  if node == nil:
    stagedValidationError("staged MessagePack tree contains a nil node")
  for i in 0 ..< min(depth - 1, maxStagedPayloadDepth):
    if ancestors[i] == node:
      stagedValidationError("staged MessagePack tree contains a cycle")
  if depth > maxStagedPayloadDepth:
    stagedValidationError("maximum staged AST nesting depth of 128 exceeded")
  node.validateStagedSpan()
  node.validateStagedWriterShape()
  ancestors[depth - 1] = node

  case node.kind
  of nkIdentifier, nkStringLit, nkIntervalLit, nkIntLit, nkFloatLit,
      nkBoolLit, nkNull, nkStar:
    discard
  of nkBinaryOp:
    node.binLeft.validateStagedAst(depth + 1, ancestors)
    node.binRight.validateStagedAst(depth + 1, ancestors)
  of nkUnaryOp:
    node.unOperand.validateStagedAst(depth + 1, ancestors)
  of nkCase:
    if node.caseOperand != nil:
      node.caseOperand.validateStagedAst(depth + 1, ancestors)
    for branch in node.caseBranches:
      branch.validateStagedAst(depth + 1, ancestors)
    if node.caseElse != nil:
      node.caseElse.validateStagedAst(depth + 1, ancestors)
  of nkCaseWhen:
    node.caseWhen.validateStagedAst(depth + 1, ancestors)
    node.caseThen.validateStagedAst(depth + 1, ancestors)
  of nkJoin, nkFromJoin:
    node.joinLeft.validateStagedAst(depth + 1, ancestors)
    node.joinRight.validateStagedAst(depth + 1, ancestors)
    if node.joinCond != nil:
      node.joinCond.validateStagedAst(depth + 1, ancestors)
  of nkAlias:
    node.aliasExpr.validateStagedAst(depth + 1, ancestors)
  of nkSetOperation:
    node.setRight.validateStagedAst(depth + 1, ancestors)
  of nkWindowFrame:
    node.frameStart.validateStagedAst(depth + 1, ancestors)
    node.frameEnd.validateStagedAst(depth + 1, ancestors)
  of nkWindowFrameBound:
    discard
  of nkColumnDef:
    node.colType.validateStagedAst(depth + 1, ancestors)
    for child in node.colConstraints:
      child.validateStagedAst(depth + 1, ancestors)
  else:
    for child in node.children:
      child.validateStagedAst(depth + 1, ancestors)

proc validateContinuousAggregateV040(statement: SqlNode) =
  if statement == nil:
    stagedValidationError("continuous aggregate statement must not be nil")
  if statement.kind != nkCreateContinuousAggregate:
    stagedValidationError("expected nkCreateContinuousAggregate statement")
  if statement.children.len != 3:
    stagedValidationError("continuous aggregate statement must have exactly 3 children")

  var ancestors: array[maxStagedPayloadDepth, SqlNode]
  statement.validateStagedAst(1, ancestors)

  let nameNode = statement.children[0]
  let queryNode = statement.children[1]
  let optionsNode = statement.children[2]
  if nameNode.kind != nkIdentifier or nameNode.strVal.len == 0:
    stagedValidationError("continuous aggregate name must be nkIdentifier")
  if queryNode.kind != nkSelect:
    stagedValidationError("continuous aggregate query must be nkSelect")
  for child in queryNode.children:
    case child.kind
    of nkWindowClause:
      stagedValidationError(
        "staged continuous aggregate query cannot contain WINDOW"
      )
    of nkQualifyClause:
      stagedValidationError(
        "staged continuous aggregate query cannot contain QUALIFY"
      )
    of nkDistinctOnClause:
      # DISTINCT ON has no representation in the staged 12-field payload.
      stagedValidationError(
        "staged continuous aggregate query cannot contain DISTINCT ON"
      )
    of nkLimitClause:
      # Plain LIMIT/FETCH ... ONLY desugars onto the frozen "limit" key;
      # WITH TIES has no representation in the staged 12-field payload.
      if child.limitWithTies:
        stagedValidationError(
          "staged continuous aggregate query cannot contain FETCH ... WITH TIES"
        )
    else:
      discard
  if optionsNode.kind != nkWithOptions:
    stagedValidationError("continuous aggregate options must be nkWithOptions")
  if optionsNode.children.len != 2:
    stagedValidationError("continuous aggregate must have exactly 2 options")

  const expectedOptions = ["retention", "refresh_interval"]
  for i, option in optionsNode.children:
    if option.kind != nkIndexOption:
      stagedValidationError("continuous aggregate option must be nkIndexOption")
    if option.children.len != 2:
      stagedValidationError("continuous aggregate option must have exactly 2 children")
    if option.children[0].kind != nkIdentifier:
      stagedValidationError("continuous aggregate option key must be nkIdentifier")
    if option.children[1].kind != nkStringLit:
      stagedValidationError("continuous aggregate option value must be nkStringLit")
    if option.children[0].strVal != expectedOptions[i]:
      stagedValidationError("expected option " & expectedOptions[i])
    if option.children[1].strVal.len == 0:
      stagedValidationError("continuous aggregate option value must not be empty")

proc writeContinuousAggregateOption(s: Stream; option: SqlNode) =
  s.pack_map(5)
  s.writeKey("key")
  s.pack_type(option.children[0].strVal)
  s.writeKey("key_span")
  s.writeSpan(option.children[0].span)
  s.writeKey("value")
  s.pack_type(option.children[1].strVal)
  s.writeKey("value_span")
  s.writeSpan(option.children[1].span)
  s.writeKey("span")
  s.writeSpan(option.span)

proc writeContinuousAggregateV040Kind(s: Stream; statement: SqlNode) =
  s.pack_map(6)
  s.writeKey("variant")
  s.pack_type("CreateContinuousAggregate")
  s.writeKey("name")
  s.pack_type(statement.children[0].strVal)
  s.writeKey("name_span")
  s.writeSpan(statement.children[0].span)
  s.writeKey("query")
  s.writeContinuousAggregateQuery(statement.children[1])
  s.writeKey("options")
  s.pack_array(statement.children[2].children.len)
  for option in statement.children[2].children:
    s.writeContinuousAggregateOption(option)
  s.writeKey("span")
  s.writeSpan(statement.span)

proc writeStatementKind(s: Stream; node: SqlNode) =
  case node.kind
  of nkSelect:
    s.writeSelectKind(node)
  of nkValues:
    s.writeValuesKind(node)
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
  of nkCreateContinuousAggregate:
    when continuousAggregateProducerEnabled:
      s.writeContinuousAggregateV040Kind(node)
    else:
      raise newException(ParseError,
        "unsupported statement node for MessagePack: " & $node.kind)
  else:
    raise newException(ParseError, "unsupported statement node for MessagePack: " & $node.kind)

proc writeStatement(s: Stream; node: SqlNode) =
  s.pack_map(2)
  s.writeKey("kind")
  s.writeStatementKind(node)
  s.writeKey("span")
  s.writeSpan(node.span)

when defined(alopexSqlParserContractTests):
  proc writeContinuousAggregateStatement(s: Stream; statement: SqlNode) =
    s.pack_map(2)
    s.writeKey("kind")
    s.writeContinuousAggregateV040Kind(statement)
    s.writeKey("span")
    s.writeSpan(statement.span)

type StagedPayloadWriter = proc(s: Stream) {.closure.}

proc encodeBoundedStagedPayload(writePayload: StagedPayloadWriter): string =
  let counting = newBoundedMsgPackStream()
  writePayload(counting)
  counting.finishBoundedMsgPack()

  let output = newBoundedMsgPackStream(counting.total)
  writePayload(output)
  output.finishBoundedMsgPack()
  result = output.data

when continuousAggregateProducerEnabled:
  proc encodeStagedStatements(statements: seq[SqlNode]): string =
    for statement in statements:
      if statement != nil and statement.kind == nkCreateContinuousAggregate:
        statement.validateContinuousAggregateV040()
    result = encodeBoundedStagedPayload(proc(s: Stream) =
      s.pack_array(statements.len)
      for statement in statements:
        s.writeStatement(statement)
    )

when defined(alopexSqlParserContractTests):
  proc validateContinuousAggregateV040ForTest*(statement: SqlNode) =
    statement.validateContinuousAggregateV040()

  proc encodeContinuousAggregateV040ToMsgPack*(statement: SqlNode): string =
    ## Test-only seam for the descriptor-0.3 dormant producer. Production
    ## importers and the C ABI expose no future-contract helper.
    statement.validateContinuousAggregateV040()
    result = encodeBoundedStagedPayload(proc(s: Stream) =
      s.writeContinuousAggregateStatement(statement)
    )

  proc validateStagedMessagePackChunksForTest*(chunks: openArray[string]): int =
    ## Exercise the same incremental depth/size observer with a synthetic
    ## payload, including markers and length prefixes split across writes.
    let counting = newBoundedMsgPackStream()
    for chunk in chunks:
      if chunk.len > 0:
        counting.writeData(unsafeAddr chunk[0], chunk.len)
    counting.finishBoundedMsgPack()
    result = counting.maximumDepth

  proc validateStagedMessagePackDepthForTest*(payload: string): int =
    validateStagedMessagePackChunksForTest([payload])

  proc triggerStagedWriterDefectForTest*() =
    ## Prove that an unexpected implementation Defect is not reclassified as
    ## malformed caller data by the staged encoder.
    discard encodeBoundedStagedPayload(proc(s: Stream) =
      raise newException(IndexDefect, "intentional staged writer defect")
    )

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
  when continuousAggregateProducerEnabled:
    for statement in statements:
      if statement != nil and statement.kind == nkCreateContinuousAggregate:
        return encodeStagedStatements(statements)

  var s = MsgStream.init()
  s.pack_array(statements.len)
  for statement in statements:
    s.writeStatement(statement)
  result = s.data

proc encodeSqlToMsgPack*(sql: string): string =
  ## Parse SQL and encode it as MessagePack bytes.
  astToMsgPack(parseSqlStatements(sql))

# --- PromQL MessagePack contract ---

proc writePromPosition(s: Stream; position: PromPosition) =
  s.pack_map(3)
  s.writeKey("line")
  s.pack_type(position.line)
  s.writeKey("column")
  s.pack_type(position.column)
  s.writeKey("offset")
  s.pack_type(position.offset)

proc writePromSpan(s: Stream; span: PromSpan) =
  s.pack_map(2)
  s.writeKey("start")
  s.writePromPosition(span.start)
  s.writeKey("end")
  s.writePromPosition(span.`end`)

proc writePromDuration(s: Stream; duration: PromDuration) =
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

proc writePromExpr(s: Stream; expr: PromExpr)

proc writePromMatcher(s: Stream; matcher: PromLabelMatcher) =
  s.pack_map(4)
  s.writeKey("name")
  s.pack_type(matcher.name)
  s.writeKey("op")
  s.pack_type(normalizedMatchOp(matcher.op))
  s.writeKey("value")
  s.pack_type(matcher.value)
  s.writeKey("span")
  s.writePromSpan(matcher.span)

proc writePromExprKind(s: Stream; expr: PromExpr) =
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

proc writePromExpr(s: Stream; expr: PromExpr) =
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

const
  maxSqlInputBytes = 1_048_576
  negativeSqlLengthError = "SQL input length must not be negative"
  oversizeSqlInputError = "SQL input exceeds 1048576-byte limit"
  nullSqlInputError = "SQL input pointer must not be null"
  interiorNulSqlInputError = "SQL input contains an interior NUL byte"

proc copySqlInput(input: cstring; length: cint): string =
  ## Validate the C transport metadata before touching `input`, then take one
  ## exact, length-bounded copy. In particular, an oversized hostile pointer
  ## must be rejected without scanning or dereferencing it.
  if length < 0:
    raise newException(ValueError, negativeSqlLengthError)
  if length > cint(maxSqlInputBytes):
    raise newException(ValueError, oversizeSqlInputError)
  if input == nil:
    raise newException(ValueError, nullSqlInputError)

  result = newString(int(length))
  if length > 0:
    copyMem(addr result[0], input, int(length))
  if '\0' in result:
    raise newException(ValueError, interiorNulSqlInputError)

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
    let sql = copySqlInput(input, length)
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
  cstring(parserContractVersion)
