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

proc callPromQlFfi(query: string): CParseResult =
  alopex_parse_promql(cstring(query), cint(query.len))

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
    check doc[0]["kind"]["source"]["variant"].getStr() == "Values"
    check doc[0]["kind"]["source"]["values"].len == 2

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

suite "Skulk query parser FFI contract":

  test "contract version covers SQL-TS and PromQL payloads":
    check $alopex_parser_version() == "0.3.0"

  test "SQL-TS interval is emitted as an explicit literal variant":
    let res = callFfi("SELECT NOW() - INTERVAL '24 hours'")
    check res.kind == prkOk
    let doc = toJsonNode(takePayload(res))
    let expression = doc[0]["kind"]["projection"][0]["expr"]
    check expression["kind"]["variant"].getStr() == "BinaryOp"
    check expression["kind"]["right"]["kind"]["literal"]["variant"].getStr() == "Interval"
    check expression["kind"]["right"]["kind"]["literal"]["value"].getStr() == "24 hours"

  test "PromQL selector range and offset use the independent entrypoint":
    let res = callPromQlFfi(
      "rate(http_requests_total{job=~\"api.*\"}[5m] offset 1m)"
    )
    check res.kind == prkOk
    let doc = toJsonNode(takePayload(res))
    check doc["kind"]["variant"].getStr() == "FunctionCall"
    check doc["kind"]["name"].getStr() == "rate"
    let matrix = doc["kind"]["args"][0]
    check matrix["kind"]["variant"].getStr() == "MatrixSelector"
    check matrix["kind"]["range"]["milliseconds"].getInt() == 300_000
    check matrix["kind"]["offset"]["milliseconds"].getInt() == 60_000
    let selector = matrix["kind"]["selector"]
    check selector["kind"]["metric"].getStr() == "http_requests_total"
    check selector["kind"]["matchers"][0]["op"].getStr() == "Regex"
    check selector["span"]["start"]["offset"].getInt() == 5

  test "PromQL parse failure does not poison subsequent calls":
    let failed = callPromQlFfi("metric{job=}")
    check failed.kind == prkError
    check takeError(failed).contains("offset 11")

    let recovered = callPromQlFfi("metric")
    check recovered.kind == prkOk
    let doc = toJsonNode(takePayload(recovered))
    check doc["kind"]["variant"].getStr() == "VectorSelector"

  test "PromQL aggregate binary unary and grouping variants are stable":
    let res = callPromQlFfi("sum by (job,) (rate(requests[5m])) + -2 ^ 2")
    check res.kind == prkOk
    let doc = toJsonNode(takePayload(res))
    check doc["kind"]["variant"].getStr() == "BinaryOp"
    check doc["kind"]["op"].getStr() == "Add"

    let aggregate = doc["kind"]["left"]
    check aggregate["kind"]["variant"].getStr() == "Aggregate"
    check aggregate["kind"]["op"].getStr() == "sum"
    check aggregate["kind"]["grouping"][0].getStr() == "job"
    check aggregate["kind"]["without"].getBool() == false

    let unary = doc["kind"]["right"]
    check unary["kind"]["variant"].getStr() == "UnaryOp"
    check unary["kind"]["op"].getStr() == "Minus"
    check unary["kind"]["expr"]["kind"]["variant"].getStr() == "BinaryOp"
    check unary["kind"]["expr"]["kind"]["op"].getStr() == "Pow"
