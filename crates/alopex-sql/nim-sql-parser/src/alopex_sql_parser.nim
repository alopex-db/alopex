## Alopex SQL Parser — C ABI entry point
##
## Exports C-compatible functions for FFI with Rust.
## Build: nim c -d:release --app:lib --noMain --gc:orc -o:libalopex_sql_parser.so src/alopex_sql_parser.nim

import std/[json, strutils]
import ast, parser

# --- C ABI types ---

type
  ParseResultKind {.exportc.} = enum
    prkOk = 0
    prkError = 1

  CParseResult {.exportc.} = object
    kind: ParseResultKind
    json_ptr: cstring   ## JSON-serialized AST (caller must free with alopex_free_string)
    json_len: cint
    error_ptr: cstring  ## Error message if kind == prkError
    error_len: cint

# --- AST to JSON serialization ---

proc toJson(node: SqlNode): JsonNode =
  if node == nil:
    return newJNull()
  result = newJObject()
  result["kind"] = newJString($node.kind)
  case node.kind
  of nkIdentifier, nkStringLit:
    result["value"] = newJString(node.strVal)
  of nkIntLit:
    result["value"] = newJInt(node.intVal)
  of nkFloatLit:
    result["value"] = newJFloat(node.floatVal)
  of nkBoolLit:
    result["value"] = newJBool(node.boolVal)
  of nkNull:
    result["value"] = newJNull()
  of nkStar:
    result["value"] = newJString("*")
  of nkBinaryOp:
    result["op"] = newJString($node.binOp)
    result["left"] = toJson(node.binLeft)
    result["right"] = toJson(node.binRight)
  of nkUnaryOp:
    result["op"] = newJString($node.unOp)
    result["operand"] = toJson(node.unOperand)
  of nkJoin:
    result["join_kind"] = newJString($node.joinKind)
    result["left"] = toJson(node.joinLeft)
    result["right"] = toJson(node.joinRight)
    result["condition"] = toJson(node.joinCond)
  of nkAlias:
    result["expr"] = toJson(node.aliasExpr)
    result["alias"] = newJString(node.aliasName)
  of nkColumnDef:
    result["name"] = newJString(node.colName)
    result["type"] = toJson(node.colType)
    var constraints = newJArray()
    for c in node.colConstraints:
      constraints.add(toJson(c))
    result["constraints"] = constraints
  else:
    var children = newJArray()
    for child in node.children:
      children.add(toJson(child))
    result["children"] = children

# --- Nim runtime initialization ---

proc NimMain() {.importc.}

proc alopex_parser_init*() {.exportc, dynlib, cdecl.} =
  ## Initialize Nim runtime. Must be called once before any parse calls.
  NimMain()

proc alopex_parse_sql*(input: cstring, length: cint): CParseResult {.exportc, dynlib, cdecl.} =
  ## Parse a SQL string and return JSON-serialized AST.
  ## Caller must free the returned strings with alopex_free_string.
  let sql = if length > 0: ($input)[0 ..< length] else: $input
  try:
    let astNode = parseSql(sql)
    let jsonStr = $toJson(astNode)
    let copied = cast[cstring](alloc(jsonStr.len + 1))
    copyMem(copied, cstring(jsonStr), jsonStr.len + 1)
    result = CParseResult(
      kind: prkOk,
      json_ptr: copied,
      json_len: cint(jsonStr.len),
      error_ptr: nil,
      error_len: 0,
    )
  except ParseError:
    let errMsg = getCurrentExceptionMsg()
    let copied = cast[cstring](alloc(errMsg.len + 1))
    copyMem(copied, cstring(errMsg), errMsg.len + 1)
    result = CParseResult(
      kind: prkError,
      json_ptr: nil,
      json_len: 0,
      error_ptr: copied,
      error_len: cint(errMsg.len),
    )

proc alopex_free_string*(p: cstring) {.exportc, dynlib, cdecl.} =
  ## Free a string returned by alopex_parse_sql.
  if p != nil:
    dealloc(p)

proc alopex_parser_version*(): cstring {.exportc, dynlib, cdecl.} =
  ## Return parser version string. Do NOT free this — it's a static string.
  "0.1.0"
