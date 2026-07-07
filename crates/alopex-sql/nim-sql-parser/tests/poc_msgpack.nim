## MessagePack interop PoC for the Rust FFI protocol.
##
## Generates a representative SQL AST payload with msgpack4nim and a JSON
## control payload for size/encoding comparison. The Rust integration test
## decodes the MessagePack bytes with rmp-serde and asserts equality.

import std/[json, os, strformat, times]
import msgpack4nim

type
  PocSpan* = object
    start*: int
    finish*: int

  PocExpr* = object
    kind*: string
    value*: string
    table*: string
    column*: string
    op*: string
    args*: seq[PocExpr]
    span*: PocSpan

  PocFrom* = object
    kind*: string
    leftTable*: string
    rightTable*: string
    joinType*: string
    condition*: PocExpr
    span*: PocSpan

  PocAst* = object
    kind*: string
    isDistinct*: bool
    selectList*: seq[PocExpr]
    fromClause*: PocFrom
    whereExpr*: PocExpr
    span*: PocSpan

proc span(start, finish: int): PocSpan =
  PocSpan(start: start, finish: finish)

proc column(table, name: string, s: PocSpan): PocExpr =
  PocExpr(kind: "ColumnRef", table: table, column: name, span: s)

proc literal(value: string, s: PocSpan): PocExpr =
  PocExpr(kind: "StringLiteral", value: value, span: s)

proc binary(op: string, left, right: PocExpr, s: PocSpan): PocExpr =
  PocExpr(kind: "BinaryOp", op: op, args: @[left, right], span: s)

proc representativeAst(): PocAst =
  let userId = column("users", "id", span(7, 15))
  let orderUserId = column("orders", "user_id", span(40, 54))
  let status = column("orders", "status", span(61, 74))
  PocAst(
    kind: "Select",
    isDistinct: true,
    selectList: @[
      userId,
      column("orders", "total", span(17, 29)),
    ],
    fromClause: PocFrom(
      kind: "Join",
      leftTable: "users",
      rightTable: "orders",
      joinType: "Inner",
      condition: binary("Eq", userId, orderUserId, span(40, 54)),
      span: span(31, 54),
    ),
    whereExpr: binary("Eq", status, literal("paid", span(77, 83)), span(61, 83)),
    span: span(0, 83),
  )

proc toJson(s: PocSpan): JsonNode =
  %* {"start": s.start, "finish": s.finish}

proc toJson(e: PocExpr): JsonNode =
  result = %* {
    "kind": e.kind,
    "value": e.value,
    "table": e.table,
    "column": e.column,
    "op": e.op,
    "args": [],
    "span": toJson(e.span),
  }
  for arg in e.args:
    result["args"].add(toJson(arg))

proc toJson(f: PocFrom): JsonNode =
  %* {
    "kind": f.kind,
    "leftTable": f.leftTable,
    "rightTable": f.rightTable,
    "joinType": f.joinType,
    "condition": toJson(f.condition),
    "span": toJson(f.span),
  }

proc toJson(ast: PocAst): JsonNode =
  result = %* {
    "kind": ast.kind,
    "distinct": ast.isDistinct,
    "selectList": [],
    "fromClause": toJson(ast.fromClause),
    "whereExpr": toJson(ast.whereExpr),
    "span": toJson(ast.span),
  }
  for item in ast.selectList:
    result["selectList"].add(toJson(item))

proc encodeMsgpack(ast: PocAst): string =
  pack(ast)

proc benchmark(ast: PocAst, iterations: int): tuple[msgpackBytes, jsonBytes: int, msgpackMicros, jsonMicros: int64] =
  var msgpackPayload = ""
  var jsonPayload = ""

  let msgpackStart = cpuTime()
  for _ in 0 ..< iterations:
    msgpackPayload = encodeMsgpack(ast)
  let msgpackMicros = int64((cpuTime() - msgpackStart) * 1_000_000)

  let jsonStart = cpuTime()
  for _ in 0 ..< iterations:
    jsonPayload = $toJson(ast)
  let jsonMicros = int64((cpuTime() - jsonStart) * 1_000_000)

  (msgpackPayload.len, jsonPayload.len, msgpackMicros, jsonMicros)

when isMainModule:
  let outDir = getEnv("ALOPEX_POC_OUT_DIR", "tests/fixtures")
  createDir(outDir)

  let ast = representativeAst()
  let msgpackPayload = encodeMsgpack(ast)
  let jsonPayload = $toJson(ast)
  writeFile(outDir / "poc_ast.msgpack", msgpackPayload)
  writeFile(outDir / "poc_ast.json", jsonPayload)

  let iterations = 1000
  let stats = benchmark(ast, iterations)
  let report = &"format,bytes,iterations,encode_micros\n" &
    &"messagepack,{stats.msgpackBytes},{iterations},{stats.msgpackMicros}\n" &
    &"json,{stats.jsonBytes},{iterations},{stats.jsonMicros}\n"
  writeFile(outDir / "poc_bench.csv", report)
  echo report
