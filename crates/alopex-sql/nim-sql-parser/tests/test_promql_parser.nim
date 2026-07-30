## PromQL subset conformance tests for the Skulk query frontend contract.

import std/[strutils, unittest]
import ../src/[promql_ast, promql_parser]

suite "PromQL selectors and modifiers":

  test "instant selector preserves metric and all matcher operators":
    let expr = parsePromQl(
      "http_requests_total{job=\"api\",instance!=\"canary\",path=~\"/v1/.*\",zone!~\"dev.*\"}"
    )
    check expr.kind == peVectorSelector
    check expr.metric == "http_requests_total"
    check expr.matchers.len == 4
    check expr.matchers[0].op == pmEqual
    check expr.matchers[1].op == pmNotEqual
    check expr.matchers[2].op == pmRegex
    check expr.matchers[3].op == pmNotRegex
    check expr.matchers[2].value == "/v1/.*"

  test "selector without metric is accepted":
    let expr = parsePromQl("{job=\"api\"}")
    check expr.kind == peVectorSelector
    check expr.metric.len == 0
    check expr.matchers.len == 1

  test "contextual keyword metric and selector trailing comma match Prometheus":
    let expr = parsePromQl("offset{job=\"api\",}")
    check expr.kind == peVectorSelector
    check expr.metric == "offset"
    check expr.matchers.len == 1

  test "range and offset durations are decoded":
    let expr = parsePromQl("rate(http_requests_total[1h30m] offset 2m)")
    check expr.kind == peFunctionCall
    check expr.name == "rate"
    check expr.args.len == 1
    check expr.args[0].kind == peMatrixSelector
    check expr.args[0].range.raw == "1h30m"
    check expr.args[0].range.milliseconds == 5_400_000
    check expr.args[0].hasOffset
    check expr.args[0].offset.raw == "2m"
    check expr.args[0].offset.milliseconds == 120_000

suite "PromQL functions and aggregation":

  test "supported function spellings preserve argument count":
    for name in [
      "rate", "irate", "increase", "avg_over_time", "min_over_time",
      "max_over_time", "sum_over_time", "count_over_time"
    ]:
      let expr = parsePromQl(name & "(metric[5m])")
      check expr.kind == peFunctionCall
      check expr.name == name
      check expr.args.len == 1

    let quantile = parsePromQl("histogram_quantile(0.95, request_bucket)")
    check quantile.kind == peFunctionCall
    check quantile.args.len == 2

  test "aggregation accepts prefix by and postfix without":
    let byExpr = parsePromQl("sum by (job, instance) (rate(requests_total[5m]))")
    check byExpr.kind == peAggregate
    check byExpr.name == "sum"
    check byExpr.groupingKind == pgBy
    check byExpr.groupingLabels == @["job", "instance"]

    let withoutExpr = parsePromQl("avg(cpu_usage) without (instance)")
    check withoutExpr.kind == peAggregate
    check withoutExpr.groupingKind == pgWithout
    check withoutExpr.groupingLabels == @["instance"]

  test "aggregation accepts empty quoted and trailing-comma grouping labels":
    let empty = parsePromQl("sum by () (cpu_usage)")
    check empty.kind == peAggregate
    check empty.groupingKind == pgBy
    check empty.groupingLabels.len == 0

    let quoted = parsePromQl("sum(cpu_usage) without (\"instance\",)")
    check quoted.groupingKind == pgWithout
    check quoted.groupingLabels == @["instance"]

suite "PromQL expression semantics":

  test "binary precedence follows Prometheus grammar":
    let expr = parsePromQl("1 + 2 * 3 ^ 4")
    check expr.kind == peBinary
    check expr.binaryOp == pbAdd
    check expr.right.kind == peBinary
    check expr.right.binaryOp == pbMul
    check expr.right.right.kind == peBinary
    check expr.right.right.binaryOp == pbPow

  test "power is right associative and parentheses are retained":
    let power = parsePromQl("2 ^ 3 ^ 2")
    check power.binaryOp == pbPow
    check power.right.kind == peBinary
    check power.right.binaryOp == pbPow

    let paren = parsePromQl("(1 + 2) * 3")
    check paren.binaryOp == pbMul
    check paren.left.kind == peParen
    check paren.left.inner.binaryOp == pbAdd

  test "power binds more tightly than unary sign":
    let expr = parsePromQl("-2 ^ 2")
    require expr.kind == peUnary
    check expr.unaryOp == puMinus
    require expr.operand.kind == peBinary
    check expr.operand.binaryOp == pbPow

  test "literals unary operators and byte positions are retained":
    let expr = parsePromQl("-1 + \"samples\"")
    check expr.left.kind == peUnary
    check expr.left.unaryOp == puMinus
    check expr.right.kind == peStringLiteral
    check expr.span.start.offset == 0
    check expr.span.`end`.offset == 14

  test "syntax errors include line column offset and nearby token":
    try:
      discard parsePromQl("metric{job=}")
      check false
    except PromQlParseError as exc:
      check exc.msg.contains("line 1")
      check exc.msg.contains("col 12")
      check exc.msg.contains("offset 11")
      check exc.msg.contains("near '}'")

  test "nesting depth is bounded":
    let input = repeat("(", PromQlMaxDepth + 1) & "1" &
      repeat(")", PromQlMaxDepth + 1)
    expect PromQlParseError:
      discard parsePromQl(input)

  test "range selectors reject non-vector operands":
    expect PromQlParseError:
      discard parsePromQl("(1 + 2)[5m]")
