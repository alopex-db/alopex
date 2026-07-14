## FFI boundary tests for the C ABI entry point (issue #40).
##
## `alopex_parse_sql` は FFI 境界であり、例外を C 側へ漏らしてはならない。
## 例外が漏れると (--exceptions:goto では) スレッドのエラーフラグが立った
## まま戻り、ゼロ初期化の CParseResult (= prkOk + 空バッファ) が返るうえ、
## 以降の呼び出しも巻き込まれてストリームが desync する。

import std/[json, unittest]
import msgpack4nim/msgpack2json
import ../src/alopex_sql_parser

proc callFfi(sql: string): CParseResult =
  alopex_parse_sql(cstring(sql), cint(sql.len))

proc takePayload(res: CParseResult): string =
  result = newString(res.buffer_len)
  if res.buffer_len > 0:
    copyMem(addr result[0], res.buffer_ptr, res.buffer_len)
  alopex_free_buffer(res.buffer_ptr)

proc takeError(res: CParseResult): string =
  result = newString(res.error_len)
  if res.error_len > 0:
    copyMem(addr result[0], res.error_ptr, res.error_len)
  alopex_free_buffer(cast[pointer](res.error_ptr))

suite "FFI boundary (issue #40)":

  test "multi-row INSERT without column list returns Ok payload":
    let res = callFfi("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
    check res.kind == prkOk
    check res.buffer_len > 0
    let doc = toJsonNode(takePayload(res))
    check doc[0]["kind"]["variant"].getStr() == "Insert"
    check doc[0]["kind"]["columns"].kind == JNull
    check doc[0]["kind"]["values"].len == 2

  test "non-ParseError failure maps to prkError instead of leaking":
    # parseBiggestInt は桁あふれの整数リテラルで ValueError (非 ParseError)
    # を送出する。FFI 境界はこれを prkError として返さなければならない。
    let res = callFfi("SELECT 99999999999999999999999999")
    check res.kind == prkError
    check res.error_len > 0
    check takeError(res).len > 0

  test "parse failure does not poison subsequent calls":
    discard callFfi("SELECT 99999999999999999999999999")
    let res = callFfi("SELECT 1")
    check res.kind == prkOk
    check res.buffer_len > 0
    let doc = toJsonNode(takePayload(res))
    check doc[0]["kind"]["variant"].getStr() == "Select"
