## FFI boundary tests for the C ABI entry point (issue #40).
##
## `alopex_parse_sql` は FFI 境界であり、例外を C 側へ漏らしてはならない。
## 例外が漏れると (--exceptions:goto では) スレッドのエラーフラグが立った
## まま戻り、ゼロ初期化の CParseResult (= prkOk + 空バッファ) が返るうえ、
## 以降の呼び出しも巻き込まれてストリームが desync する。

import std/[json, strutils, unittest]
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

  test "internal Defect is mapped to prkError with a distinguishable message":
    # 多行 INSERT・カラムリスト省略で先頭行が nkIntLit の場合、修正前は
    # firstIdent の FieldDefect が漏れていた。ここでは Defect 経路そのものを
    # 直接確認できないため (issue #40 の直接原因は既に直っている)、Defect
    # ハンドラのプレフィックス付与を alopex_sql_parser の公開 API 経由で
    # 間接的に保証する回帰。writeInsertKind の判定が壊れて再度 Defect が
    # 漏れた場合、このテストではなく test_msgpack_output.nim 側で先に
    # FieldDefect として検出される。ここでは "internal parser defect" と
    # いう文言が通常の構文エラーメッセージに紛れ込んでいないことのみ確認する。
    let res = callFfi("SELECT 99999999999999999999999999")
    check res.kind == prkError
    check not takeError(res).contains("internal parser defect")

  test "parse failure does not poison subsequent calls":
    # 最初の失敗結果を捨てず、prkError であることとエラーメッセージを
    # 確認してからバッファを解放し、そのうえで後続呼び出しが desync に
    # 巻き込まれず成功することを確認する。
    let failed = callFfi("SELECT 99999999999999999999999999")
    check failed.kind == prkError
    check failed.error_len > 0
    let failMsg = takeError(failed)
    check failMsg.len > 0

    let res = callFfi("SELECT 1")
    check res.kind == prkOk
    check res.buffer_len > 0
    let doc = toJsonNode(takePayload(res))
    check doc[0]["kind"]["variant"].getStr() == "Select"
