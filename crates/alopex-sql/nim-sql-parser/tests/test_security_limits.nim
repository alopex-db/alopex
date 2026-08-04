## Security boundary tests for SQL parser input size and syntactic nesting.
##
## These tests call the exported SQL FFI directly so invalid transport input
## cannot hide behind the Rust-side preflight. PromQL grammar is intentionally
## out of scope for this SQL entry-point fixture.

import std/[strutils, unittest]
import ../src/alopex_sql_parser

const
  MaxSqlInputBytes = 1_048_576
  MaxSyntacticNesting = 128
  NegativeLengthError = "SQL input length must not be negative"
  OversizeError = "SQL input exceeds 1048576-byte limit"
  NullInputError = "SQL input pointer must not be null"
  InteriorNulError = "SQL input contains an interior NUL byte"
  NestingError = "maximum SQL syntactic nesting depth of 128 exceeded"

type FfiOutcome = object
  kind: ParseResultKind
  payloadLen: int
  error: string

proc consume(res: CParseResult): FfiOutcome =
  result.kind = res.kind
  if res.kind == prkOk:
    result.payloadLen = int(res.buffer_len)
    alopex_free_buffer(res.buffer_ptr)
  else:
    result.error = newString(int(res.error_len))
    if res.error_len > 0:
      copyMem(addr result.error[0], res.error_ptr, int(res.error_len))
    alopex_free_buffer(cast[pointer](res.error_ptr))

proc callSql(sql: string): FfiOutcome =
  consume(alopex_parse_sql(cstring(sql), cint(sql.len)))

proc paddedSql(totalBytes: int): string =
  const statement = "SELECT 1"
  doAssert totalBytes >= statement.len
  repeat(" ", totalBytes - statement.len) & statement

proc parenthesizedSql(depth: int): string =
  "SELECT " & repeat("(", depth) & "1" & repeat(")", depth)

proc mixedExpression(depth: int): string =
  if depth == 0:
    return "1"

  # VECTOR is a non-recursive container but still consumes one syntactic
  # nesting level. The remaining wrappers exercise every implemented recursive
  # expression route without adding unsupported CASE grammar.
  result = "[1]"
  for level in 1 ..< depth:
    case (level - 1) mod 7
    of 0:
      result = "(" & result & ")"
    of 1:
      result = "NOT " & result
    of 2:
      result = "ABS(" & result & ")"
    of 3:
      result = "CAST(" & result & " AS INT)"
    of 4:
      result = "1 IN (" & result & ")"
    of 5:
      result = "EXISTS (SELECT " & result & ")"
    else:
      result = "(SELECT " & result & ")"

proc mixedSql(depth: int): string =
  "SELECT " & mixedExpression(depth)

proc fromSubquerySql(depth: int): string =
  result = "SELECT 1"
  for _ in 0 ..< depth:
    result = "SELECT 1 FROM (" & result & ") nested"

proc checkAccepted(sql: string) =
  let outcome = callSql(sql)
  check outcome.kind == prkOk
  check outcome.payloadLen > 0

proc checkNestingRejected(sql: string) =
  let outcome = callSql(sql)
  check outcome.kind == prkError
  check outcome.error == NestingError
  check outcome.error.len <= 80

suite "SQL FFI input byte boundary":

  test "MAX minus one and MAX bytes are copied exactly and accepted":
    for totalBytes in [MaxSqlInputBytes - 1, MaxSqlInputBytes]:
      let sql = paddedSql(totalBytes)
      check sql.len == totalBytes
      checkAccepted(sql)

  test "MAX plus one rejects a one-byte hostile pointer without reading it":
    let hostileBuffer = alloc(1)
    cast[ptr UncheckedArray[char]](hostileBuffer)[0] = '\0'
    let outcome = consume(alopex_parse_sql(
      cast[cstring](hostileBuffer),
      cint(MaxSqlInputBytes + 1),
    ))
    dealloc(hostileBuffer)
    check outcome.kind == prkError
    check outcome.error == OversizeError
    check outcome.error.len <= 64

  test "negative and null inputs return distinct bounded errors":
    let negative = consume(alopex_parse_sql(nil, cint(-1)))
    check negative.kind == prkError
    check negative.error == NegativeLengthError
    check negative.error.len <= 64

    let nullInput = consume(alopex_parse_sql(nil, cint(0)))
    check nullInput.kind == prkError
    check nullInput.error == NullInputError
    check nullInput.error.len <= 64

  test "interior NUL is rejected from the exact length-bounded buffer":
    let sql = "SELECT 1\0SELECT 2"
    let outcome = callSql(sql)
    check outcome.kind == prkError
    check outcome.error == InteriorNulError
    check outcome.error.len <= 64

suite "SQL syntactic nesting boundary":

  test "parenthesized expression accepts 128 and rejects 129":
    checkAccepted(parenthesizedSql(MaxSyntacticNesting))
    checkNestingRejected(parenthesizedSql(MaxSyntacticNesting + 1))

  test "mixed recursive syntax accepts 128 and rejects 129":
    checkAccepted(mixedSql(MaxSyntacticNesting))
    checkNestingRejected(mixedSql(MaxSyntacticNesting + 1))

  test "FROM derived subquery alternate accepts 128 and rejects 129":
    checkAccepted(fromSubquerySql(MaxSyntacticNesting))
    checkNestingRejected(fromSubquerySql(MaxSyntacticNesting + 1))

  test "long flat expression is not mistaken for nesting":
    let sql = "SELECT " & repeat("1, ", 4096) & "1"
    checkAccepted(sql)
