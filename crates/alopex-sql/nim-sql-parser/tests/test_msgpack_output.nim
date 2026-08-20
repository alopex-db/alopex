## MessagePack contract tests for the Nim SQL parser FFI payload.

{.define: alopexSqlParserContractTests.}

import std/[json, strutils, unittest]
import msgpack4nim/msgpack2json
import ../src/[alopex_sql_parser, ast, parser]

const ContractDescriptor = staticRead("../PARSER_CONTRACT_VERSION").strip()
const ContinuousAggregateProducerEnabled = ContractDescriptor != "0.3.0"

const
  MinimalContinuousAggregateSql =
    "CREATE CONTINUOUS AGGREGATE c AS SELECT 1 FROM m " &
    "WITH (retention = '7d', refresh_interval = '1h')"
  CanonicalContinuousAggregateSql = """CREATE CONTINUOUS AGGREGATE cpu_hourly
AS
SELECT
  TIME_BUCKET(INTERVAL '1 hour', time) AS time,
  host,
  AVG(usage_user) AS usage_user_avg
FROM cpu_metrics
GROUP BY TIME_BUCKET(INTERVAL '1 hour', time), host
WITH (
  retention = '30d',
  refresh_interval = '1h'
);"""
  CanonicalContinuousAggregateJson = """{"kind":{"variant":"CreateContinuousAggregate","name":"cpu_hourly","name_span":{"start":{"line":1,"column":29},"end":{"line":1,"column":38}},"query":{"variant":"Select","distinct":false,"projection":[{"variant":"Expr","expr":{"kind":{"variant":"FunctionCall","name":"TIME_BUCKET","args":[{"kind":{"variant":"Literal","literal":{"variant":"Interval","value":"1 hour"}},"span":{"start":{"line":4,"column":15},"end":{"line":4,"column":31}}},{"kind":{"variant":"ColumnRef","table":null,"column":"time"},"span":{"start":{"line":4,"column":34},"end":{"line":4,"column":37}}}],"distinct":false,"star":false,"over":null},"span":{"start":{"line":4,"column":3},"end":{"line":4,"column":13}}},"alias":"time","span":{"start":{"line":4,"column":43},"end":{"line":4,"column":46}}},{"variant":"Expr","expr":{"kind":{"variant":"ColumnRef","table":null,"column":"host"},"span":{"start":{"line":5,"column":3},"end":{"line":5,"column":6}}},"alias":null,"span":{"start":{"line":5,"column":3},"end":{"line":5,"column":6}}},{"variant":"Expr","expr":{"kind":{"variant":"FunctionCall","name":"AVG","args":[{"kind":{"variant":"ColumnRef","table":null,"column":"usage_user"},"span":{"start":{"line":6,"column":7},"end":{"line":6,"column":16}}}],"distinct":false,"star":false,"over":null},"span":{"start":{"line":6,"column":3},"end":{"line":6,"column":5}}},"alias":"usage_user_avg","span":{"start":{"line":6,"column":22},"end":{"line":6,"column":35}}}],"from":[{"variant":"Table","name":"cpu_metrics","alias":null,"span":{"start":{"line":7,"column":6},"end":{"line":7,"column":16}}}],"selection":null,"group_by":[{"kind":{"variant":"FunctionCall","name":"TIME_BUCKET","args":[{"kind":{"variant":"Literal","literal":{"variant":"Interval","value":"1 hour"}},"span":{"start":{"line":8,"column":22},"end":{"line":8,"column":38}}},{"kind":{"variant":"ColumnRef","table":null,"column":"time"},"span":{"start":{"line":8,"column":41},"end":{"line":8,"column":44}}}],"distinct":false,"star":false,"over":null},"span":{"start":{"line":8,"column":10},"end":{"line":8,"column":20}}},{"kind":{"variant":"ColumnRef","table":null,"column":"host"},"span":{"start":{"line":8,"column":48},"end":{"line":8,"column":51}}}],"having":null,"set_operations":[],"order_by":[],"limit":null,"offset":null,"span":{"start":{"line":3,"column":1},"end":{"line":8,"column":51}}},"options":[{"key":"retention","key_span":{"start":{"line":10,"column":3},"end":{"line":10,"column":11}},"value":"30d","value_span":{"start":{"line":10,"column":15},"end":{"line":10,"column":19}},"span":{"start":{"line":10,"column":3},"end":{"line":10,"column":19}}},{"key":"refresh_interval","key_span":{"start":{"line":11,"column":3},"end":{"line":11,"column":18}},"value":"1h","value_span":{"start":{"line":11,"column":22},"end":{"line":11,"column":25}},"span":{"start":{"line":11,"column":3},"end":{"line":11,"column":25}}}],"span":{"start":{"line":1,"column":1},"end":{"line":12,"column":1}}},"span":{"start":{"line":1,"column":1},"end":{"line":12,"column":1}}}"""
  CanonicalContinuousAggregateGolden = "82A46B696E6486A776617269616E74B9437265617465436F6E74696E756F7573416767726567617465A46E616D65AA6370755F686F75726C79A96E616D655F7370616E82A5737461727482A46C696E6501A6636F6C756D6E1DA3656E6482A46C696E6501A6636F6C756D6E26A571756572798CA776617269616E74A653656C656374A864697374696E6374C2AA70726F6A656374696F6E9384A776617269616E74A445787072A46578707282A46B696E6486A776617269616E74AC46756E6374696F6E43616C6CA46E616D65AB54494D455F4255434B4554A4617267739282A46B696E6482A776617269616E74A74C69746572616CA76C69746572616C82A776617269616E74A8496E74657276616CA576616C7565A63120686F7572A47370616E82A5737461727482A46C696E6504A6636F6C756D6E0FA3656E6482A46C696E6504A6636F6C756D6E1F82A46B696E6483A776617269616E74A9436F6C756D6E526566A57461626C65C0A6636F6C756D6EA474696D65A47370616E82A5737461727482A46C696E6504A6636F6C756D6E22A3656E6482A46C696E6504A6636F6C756D6E25A864697374696E6374C2A473746172C2A46F766572C0A47370616E82A5737461727482A46C696E6504A6636F6C756D6E03A3656E6482A46C696E6504A6636F6C756D6E0DA5616C696173A474696D65A47370616E82A5737461727482A46C696E6504A6636F6C756D6E2BA3656E6482A46C696E6504A6636F6C756D6E2E84A776617269616E74A445787072A46578707282A46B696E6483A776617269616E74A9436F6C756D6E526566A57461626C65C0A6636F6C756D6EA4686F7374A47370616E82A5737461727482A46C696E6505A6636F6C756D6E03A3656E6482A46C696E6505A6636F6C756D6E06A5616C696173C0A47370616E82A5737461727482A46C696E6505A6636F6C756D6E03A3656E6482A46C696E6505A6636F6C756D6E0684A776617269616E74A445787072A46578707282A46B696E6486A776617269616E74AC46756E6374696F6E43616C6CA46E616D65A3415647A4617267739182A46B696E6483A776617269616E74A9436F6C756D6E526566A57461626C65C0A6636F6C756D6EAA75736167655F75736572A47370616E82A5737461727482A46C696E6506A6636F6C756D6E07A3656E6482A46C696E6506A6636F6C756D6E10A864697374696E6374C2A473746172C2A46F766572C0A47370616E82A5737461727482A46C696E6506A6636F6C756D6E03A3656E6482A46C696E6506A6636F6C756D6E05A5616C696173AE75736167655F757365725F617667A47370616E82A5737461727482A46C696E6506A6636F6C756D6E16A3656E6482A46C696E6506A6636F6C756D6E23A466726F6D9184A776617269616E74A55461626C65A46E616D65AB6370755F6D657472696373A5616C696173C0A47370616E82A5737461727482A46C696E6507A6636F6C756D6E06A3656E6482A46C696E6507A6636F6C756D6E10A973656C656374696F6EC0A867726F75705F62799282A46B696E6486A776617269616E74AC46756E6374696F6E43616C6CA46E616D65AB54494D455F4255434B4554A4617267739282A46B696E6482A776617269616E74A74C69746572616CA76C69746572616C82A776617269616E74A8496E74657276616CA576616C7565A63120686F7572A47370616E82A5737461727482A46C696E6508A6636F6C756D6E16A3656E6482A46C696E6508A6636F6C756D6E2682A46B696E6483A776617269616E74A9436F6C756D6E526566A57461626C65C0A6636F6C756D6EA474696D65A47370616E82A5737461727482A46C696E6508A6636F6C756D6E29A3656E6482A46C696E6508A6636F6C756D6E2CA864697374696E6374C2A473746172C2A46F766572C0A47370616E82A5737461727482A46C696E6508A6636F6C756D6E0AA3656E6482A46C696E6508A6636F6C756D6E1482A46B696E6483A776617269616E74A9436F6C756D6E526566A57461626C65C0A6636F6C756D6EA4686F7374A47370616E82A5737461727482A46C696E6508A6636F6C756D6E30A3656E6482A46C696E6508A6636F6C756D6E33A6686176696E67C0AE7365745F6F7065726174696F6E7390A86F726465725F627990A56C696D6974C0A66F6666736574C0A47370616E82A5737461727482A46C696E6503A6636F6C756D6E01A3656E6482A46C696E6508A6636F6C756D6E33A76F7074696F6E739285A36B6579A9726574656E74696F6EA86B65795F7370616E82A5737461727482A46C696E650AA6636F6C756D6E03A3656E6482A46C696E650AA6636F6C756D6E0BA576616C7565A3333064AA76616C75655F7370616E82A5737461727482A46C696E650AA6636F6C756D6E0FA3656E6482A46C696E650AA6636F6C756D6E13A47370616E82A5737461727482A46C696E650AA6636F6C756D6E03A3656E6482A46C696E650AA6636F6C756D6E1385A36B6579B0726566726573685F696E74657276616CA86B65795F7370616E82A5737461727482A46C696E650BA6636F6C756D6E03A3656E6482A46C696E650BA6636F6C756D6E12A576616C7565A23168AA76616C75655F7370616E82A5737461727482A46C696E650BA6636F6C756D6E16A3656E6482A46C696E650BA6636F6C756D6E19A47370616E82A5737461727482A46C696E650BA6636F6C756D6E03A3656E6482A46C696E650BA6636F6C756D6E19A47370616E82A5737461727482A46C696E6501A6636F6C756D6E01A3656E6482A46C696E650CA6636F6C756D6E01A47370616E82A5737461727482A46C696E6501A6636F6C756D6E01A3656E6482A46C696E650CA6636F6C756D6E01"

proc canonicalContinuousAggregate(): SqlNode =
  parseSql(MinimalContinuousAggregateSql)

proc stagedError(statement: SqlNode): string =
  try:
    discard encodeContinuousAggregateV040ToMsgPack(statement)
  except CatchableError as exc:
    return exc.msg

proc checkStagedError(statement: SqlNode; fragment: string) =
  let message = stagedError(statement)
  check message.contains(fragment)
  check message.len <= 160

proc messagePackDepthError(payload: string): string =
  try:
    discard validateStagedMessagePackDepthForTest(payload)
  except CatchableError as exc:
    return exc.msg

proc oneByteChunks(payload: string): seq[string] =
  for i in 0 ..< payload.len:
    result.add(payload[i .. i])

proc payloadJson(sql: string): JsonNode =
  toJsonNode(encodeSqlToMsgPack(sql))

proc assertMsgpackRoundtrip(sql: string) =
  let payload = encodeSqlToMsgPack(sql)
  let decoded = toJsonNode(payload)
  check fromJsonNode(decoded) == payload

proc hexPayload(sql: string): string =
  let payload = encodeSqlToMsgPack(sql)
  for ch in payload:
    result.add(toHex(ord(ch), 2))

proc hexBytes(payload: string): string =
  for ch in payload:
    result.add(toHex(ord(ch), 2))

proc firstStmt(doc: JsonNode): JsonNode =
  doc[0]

proc stmtKind(doc: JsonNode): JsonNode =
  doc.firstStmt()["kind"]

proc selectKind(sql: string): JsonNode =
  payloadJson(sql).stmtKind()

suite "MessagePack output - roundtrip":

  test "SELECT JOIN variants round-trip":
    for sql in [
      "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
      "SELECT * FROM users LEFT JOIN orders USING (user_id)",
      "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
      "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id",
      "SELECT * FROM users CROSS JOIN orders",
    ]:
      assertMsgpackRoundtrip(sql)

  test "Subquery variants round-trip":
    for sql in [
      "SELECT (SELECT COUNT(*) FROM orders) AS order_count FROM users",
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)",
      "SELECT * FROM (SELECT id FROM users WHERE active = TRUE) AS active_users",
      "SELECT * FROM scores WHERE score > ANY (SELECT score FROM baseline)",
      "SELECT * FROM scores WHERE score <= ALL (SELECT score FROM baseline)",
    ]:
      assertMsgpackRoundtrip(sql)

  test "CASE variants round-trip":
    for sql in [
      "SELECT CASE WHEN active THEN 'yes' ELSE 'no' END FROM users",
      "SELECT CASE status WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM users",
    ]:
      assertMsgpackRoundtrip(sql)

  test "standard predicate and row variants round-trip":
    for sql in [
      "SELECT flag IS TRUE, flag IS NOT UNKNOWN FROM flags",
      "SELECT value IS DISTINCT FROM fallback FROM flags",
      "SELECT (a, b) = (c, d) FROM pairs",
      "SELECT (a, b) IN ((1, 2), (3, 4)) FROM pairs",
      "SELECT (a, b) BETWEEN (1, 2) AND (3, 4) FROM pairs",
    ]:
      assertMsgpackRoundtrip(sql)

  test "Vector and index DDL round-trip":
    for sql in [
      "CREATE TABLE items (id INT, embedding VECTOR(3, COSINE))",
      "SELECT [1.0, -2.0, 3.5] FROM items",
      "CREATE INDEX idx_doc_embedding ON documents (embedding) USING HNSW WITH (m = 16, ef_construction = 200)",
      "DROP INDEX IF EXISTS idx_doc_embedding",
    ]:
      assertMsgpackRoundtrip(sql)

suite "MessagePack output - contract shape":

  test "standard predicates preserve dedicated expression variants":
    let kind = selectKind(
      "SELECT flag IS NOT UNKNOWN, (a, b) IS DISTINCT FROM (c, d) FROM pairs")
    let truth = kind["projection"][0]["expr"]["kind"]
    check truth["variant"].getStr() == "TruthPredicate"
    check truth["value"].getStr() == "Unknown"
    check truth["negated"].getBool()
    let distinctPredicate = kind["projection"][1]["expr"]["kind"]
    check distinctPredicate["variant"].getStr() == "IsDistinctFrom"
    check distinctPredicate["left"]["kind"]["variant"].getStr() == "Row"
    check distinctPredicate["left"]["kind"]["items"].len == 2
    check distinctPredicate["right"]["kind"]["variant"].getStr() == "Row"

  test "CTE emits its optional ordered column name list":
    let kind = selectKind(
      "WITH c(identifier, label) AS (SELECT 1, 'one') SELECT identifier FROM c")
    let cte = kind["with"]["ctes"][0]

    check cte["name"].getStr() == "c"
    check cte["columns"].len == 2
    check cte["columns"][0].getStr() == "identifier"
    check cte["columns"][1].getStr() == "label"
    check cte["query"]["variant"].getStr() == "Select"

  test "CTE without a column name list emits an empty list":
    let cte = selectKind("WITH c AS (SELECT 1) SELECT * FROM c")["with"]["ctes"][0]
    check cte["columns"].kind == JArray
    check cte["columns"].len == 0

  test "CASE emits operand branches and optional ELSE":
    let searched = selectKind(
      "SELECT CASE WHEN active THEN 'yes' ELSE 'no' END FROM users")
    let searchedCase = searched["projection"][0]["expr"]["kind"]
    check searchedCase["variant"].getStr() == "Case"
    check searchedCase["operand"].kind == JNull
    check searchedCase["branches"].len == 1
    check searchedCase["branches"][0].hasKey("when")
    check searchedCase["branches"][0].hasKey("then")
    check searchedCase["else_expr"].kind != JNull

    let simple = selectKind("SELECT CASE status WHEN 1 THEN 'one' END FROM users")
    let simpleCase = simple["projection"][0]["expr"]["kind"]
    check simpleCase["operand"].kind != JNull
    check simpleCase["else_expr"].kind == JNull

  test "JOIN emits FromItem Join with tag and join_type":
    let kind = selectKind("SELECT * FROM users LEFT JOIN orders USING (user_id)")
    check kind["variant"].getStr() == "Select"
    let joinItem = kind["from"][0]
    check joinItem["variant"].getStr() == "Join"
    check joinItem["join_type"].getStr() == "Left"
    check joinItem["using"][0].getStr() == "user_id"
    check joinItem["left"]["variant"].getStr() == "Table"
    check joinItem["right"]["variant"].getStr() == "Table"

  test "Scalar, IN, EXISTS, and quantified subqueries emit ExprKind tags":
    let scalar = selectKind("SELECT (SELECT COUNT(*) FROM orders) AS order_count FROM users")
    check scalar["projection"][0]["expr"]["kind"]["variant"].getStr() == "ScalarSubquery"

    let inSub = selectKind("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
    check inSub["selection"]["kind"]["variant"].getStr() == "InSubquery"
    check inSub["selection"]["kind"]["negated"].getBool() == false

    let existsSub = selectKind("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)")
    check existsSub["selection"]["kind"]["variant"].getStr() == "Exists"

    let quantified = selectKind("SELECT * FROM scores WHERE score >= ALL (SELECT score FROM baseline)")
    check quantified["selection"]["kind"]["variant"].getStr() == "Quantified"
    check quantified["selection"]["kind"]["op"].getStr() == "GtEq"
    check quantified["selection"]["kind"]["quantifier"].getStr() == "All"

  test "FROM derived table emits Derived FromItem":
    let kind = selectKind("SELECT * FROM (SELECT id FROM users) AS active_users")
    let item = kind["from"][0]
    check item["variant"].getStr() == "Derived"
    check item["alias"].getStr() == "active_users"
    check item["subquery"]["variant"].getStr() == "Select"

  test "Vector DataType and VectorLiteral emit contract variants":
    let table = payloadJson("CREATE TABLE items (id INT, embedding VECTOR(3, COSINE))")
    let create = table.stmtKind()
    let vectorType = create["columns"][1]["data_type"]
    check create["variant"].getStr() == "CreateTable"
    check vectorType["variant"].getStr() == "Vector"
    check vectorType["dimension"].getInt() == 3
    check vectorType["metric"].getStr() == "Cosine"

    let select = selectKind("SELECT [1.0, -2.0, 3.5] FROM items")
    let vectorExpr = select["projection"][0]["expr"]["kind"]
    check vectorExpr["variant"].getStr() == "VectorLiteral"
    check vectorExpr["values"].len == 3

  test "REAL data type emits the Float contract variant":
    for sql in [
      "CREATE TABLE measurements (value REAL)",
      "CREATE TABLE measurements (value real)",
    ]:
      let create = payloadJson(sql).stmtKind()
      check create["columns"][0]["data_type"]["variant"].getStr() == "Float"

  test "SELECT emits limit_with_ties and a detached OFFSET (issue #152)":
    let kind = selectKind(
      "SELECT id FROM t ORDER BY id OFFSET 2 ROWS FETCH FIRST 3 ROWS WITH TIES")
    check kind["limit"]["kind"]["literal"]["value"].getStr() == "3"
    check kind["offset"]["kind"]["literal"]["value"].getStr() == "2"
    check kind["limit_with_ties"].getBool() == true

  test "OFFSET without LIMIT emits nil limit and false limit_with_ties":
    let kind = selectKind("SELECT id FROM t OFFSET 4")
    check kind["limit"].kind == JNull
    check kind["offset"]["kind"]["literal"]["value"].getStr() == "4"
    check kind["limit_with_ties"].getBool() == false

  test "FETCH ... ONLY desugars onto the limit key":
    let kind = selectKind("SELECT id FROM t FETCH NEXT ROW ONLY")
    check kind["limit"]["kind"]["literal"]["value"].getStr() == "1"
    check kind["limit_with_ties"].getBool() == false

  test "VALUES tail emits limit_with_ties":
    let kind = payloadJson(
      "VALUES (1), (2) ORDER BY 1 FETCH FIRST 1 ROW WITH TIES").stmtKind()
    check kind["variant"].getStr() == "Values"
    check kind["limit"]["kind"]["literal"]["value"].getStr() == "1"
    check kind["limit_with_ties"].getBool() == true

  test "CREATE INDEX emits method and WITH options":
    let doc = payloadJson("CREATE INDEX idx_doc_embedding ON documents (embedding) USING HNSW WITH (m = 16, ef_construction = 200)")
    let create = doc.stmtKind()
    check create["variant"].getStr() == "CreateIndex"
    check create["name"].getStr() == "idx_doc_embedding"
    check create["table"].getStr() == "documents"
    check create["column"].getStr() == "embedding"
    check create["method"].getStr() == "Hnsw"
    check create["options"].len == 2

suite "MessagePack output - INSERT (issue #40)":

  test "multi-row INSERT without column list emits nil columns and all rows":
    let kind = payloadJson("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").stmtKind()
    check kind["variant"].getStr() == "Insert"
    check kind["table"].getStr() == "t1"
    check kind["columns"].kind == JNull
    check kind["source"]["variant"].getStr() == "Values"
    check kind["source"]["values"].len == 2
    check kind["source"]["values"][0][0]["kind"]["variant"].getStr() == "Literal"
    check kind["source"]["values"][0][0]["kind"]["literal"]["variant"].getStr() == "Number"
    check kind["source"]["values"][0][0]["kind"]["literal"]["value"].getStr() == "1"
    check kind["source"]["values"][0][1]["kind"]["literal"]["variant"].getStr() == "String"
    check kind["source"]["values"][0][1]["kind"]["literal"]["value"].getStr() == "a"
    check kind["source"]["values"][1][0]["kind"]["literal"]["value"].getStr() == "2"
    check kind["source"]["values"][1][1]["kind"]["literal"]["value"].getStr() == "b"

  test "multi-row all-string INSERT without column list is not misread as columns":
    # 先頭行が全て文字列だと firstIdent が例外を出さず、列リストとして
    # 静かに誤変換される回帰パターン。
    let kind = payloadJson("INSERT INTO t1 VALUES ('a', 'b'), ('c', 'd')").stmtKind()
    check kind["columns"].kind == JNull
    check kind["source"]["values"].len == 2
    check kind["source"]["values"][0][0]["kind"]["literal"]["value"].getStr() == "a"

  test "multi-row INSERT with column list keeps explicit columns":
    let kind = payloadJson("INSERT INTO t1 (id, name) VALUES (1, 'a'), (2, 'b')").stmtKind()
    check kind["columns"].len == 2
    check kind["columns"][0].getStr() == "id"
    check kind["columns"][1].getStr() == "name"
    check kind["source"]["values"].len == 2

  test "INSERT SELECT emits a Select source":
    let kind = payloadJson("INSERT INTO t1 (id, name) SELECT id, name FROM source").stmtKind()
    check kind["source"]["variant"].getStr() == "Select"
    check kind["source"]["select"]["variant"].getStr() == "Select"

  test "INSERT WITH VALUES emits a Query source":
    let kind = payloadJson(
      "INSERT INTO t1 WITH seed(n) AS (VALUES (1)) VALUES (2), (3)").stmtKind()
    check kind["source"]["variant"].getStr() == "Query"
    check kind["source"]["query"]["variant"].getStr() == "Values"
    check kind["source"]["query"]["with"]["ctes"].len == 1
    check kind["source"]["query"]["rows"].len == 2

  test "multi-row INSERT without column list round-trips":
    assertMsgpackRoundtrip("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")

suite "MessagePack output - VALUES query (issue #145)":

  test "top-level VALUES emits rows and query-tail fields":
    let kind = payloadJson(
      "VALUES (3), (1) UNION ALL SELECT 2 ORDER BY column1 LIMIT 2").stmtKind()
    check kind["variant"].getStr() == "Values"
    check kind["rows"].len == 2
    check kind["rows"][0][0]["kind"]["literal"]["value"].getStr() == "3"
    check kind["set_operations"].len == 1
    check kind["set_operations"][0]["right"]["variant"].getStr() == "Select"
    check kind["order_by"].len == 1
    check kind["limit"]["kind"]["literal"]["value"].getStr() == "2"

  test "derived VALUES emits its query body and column aliases":
    let kind = payloadJson(
      "SELECT * FROM (VALUES (1, 'a')) AS t(id, label)").stmtKind()
    let source = kind["from"][0]
    check source["variant"].getStr() == "Derived"
    check source["subquery"]["variant"].getStr() == "Values"
    check source["columns"][0].getStr() == "id"
    check source["columns"][1].getStr() == "label"

  test "CTE VALUES body is a QueryBody value":
    let kind = payloadJson(
      "WITH v(id) AS (VALUES (1), (2)) SELECT id FROM v").stmtKind()
    let cte = kind["with"]["ctes"][0]
    check cte["query"]["variant"].getStr() == "Values"
    check cte["query"]["rows"].len == 2

  test "VALUES query round-trips":
    assertMsgpackRoundtrip(
      "WITH v(id) AS (VALUES (1)) VALUES (2) UNION ALL SELECT id FROM v")

suite "MessagePack output - stability":

  test "SELECT literal payload is stable":
    let v050Payload =
      "9182A46B696E648BA776617269616E74A653656C656374A864697374696E6374C2AA70726F6A656374696F6E9184A776617269616E74A445787072A46578707282A46B696E6482A776617269616E74A74C69746572616CA76C69746572616C82A776617269616E74A64E756D626572A576616C7565A131A47370616E82A5737461727482A46C696E6501A6636F6C756D6E08A3656E6482A46C696E6501A6636F6C756D6E08A5616C696173C0A47370616E82A5737461727482A46C696E6501A6636F6C756D6E08A3656E6482A46C696E6501A6636F6C756D6E08A466726F6D90A973656C656374696F6EC0A867726F75705F6279C0A6686176696E67C0AE7365745F6F7065726174696F6E7390A86F726465725F627990A56C696D6974C0A66F6666736574C0A47370616E82A5737461727482A46C696E6501A6636F6C756D6E01A3656E6482A46C696E6501A6636F6C756D6E06"

    let v060Payload = v050Payload
      .replace("A46B696E648B", "A46B696E648D")
      .replace(
        "A6686176696E67C0AE7365745F6F7065726174696F6E73",
        "A6686176696E67C0A777696E646F777390A77175616C696679C0" &
          "AE7365745F6F7065726174696F6E73"
      )
    # Contract 0.10.0 (issue #152) appends limit_with_ties after offset.
    let v0100Payload = v060Payload
      .replace("A46B696E648D", "A46B696E648E")
      .replace(
        "A56C696D6974C0A66F6666736574C0",
        "A56C696D6974C0A66F6666736574C0" &
          "AF6C696D69745F776974685F74696573C2"
      )
    check hexPayload("SELECT 1") == v0100Payload

suite "MessagePack output - staged continuous aggregate contract":

  test "exported version is sourced from the descriptor":
    check $alopex_parser_version() == ContractDescriptor

  test "requirements canonical SQL owns exact JSON, spans, and MessagePack bytes":
    let payload = encodeContinuousAggregateV040ToMsgPack(
      parseSql(CanonicalContinuousAggregateSql)
    )
    check $toJsonNode(payload) == CanonicalContinuousAggregateJson
    check hexBytes(payload) == CanonicalContinuousAggregateGolden

  test "minimal fixture encodes a whole statement without activating public 0.3":
    let statement = canonicalContinuousAggregate()
    var previous = ""
    for _ in 0 ..< 8:
      let encoded = encodeContinuousAggregateV040ToMsgPack(statement)
      if previous.len > 0:
        check encoded == previous
      previous = encoded

    when ContinuousAggregateProducerEnabled:
      check toJsonNode(astToMsgPack(@[statement]))[0]["kind"]["variant"].getStr() ==
        "CreateContinuousAggregate"
    else:
      expect ParseError:
        discard astToMsgPack(@[statement])

  test "malformed staged trees fail as bounded CatchableError before encoding":
    checkStagedError(nil, "must not be nil")
    checkStagedError(newNode(nkSelect), "expected nkCreateContinuousAggregate")

    var missing = canonicalContinuousAggregate()
    missing.children.setLen(2)
    checkStagedError(missing, "exactly 3 children")

    var extra = canonicalContinuousAggregate()
    extra.children.add(newIdent("extra"))
    checkStagedError(extra, "exactly 3 children")

    var wrongName = canonicalContinuousAggregate()
    wrongName.children[0] = newStringLit("c", wrongName.children[0].span)
    checkStagedError(wrongName, "name must be nkIdentifier")

    var wrongQuery = canonicalContinuousAggregate()
    wrongQuery.children[1] = newNode(nkPragma, wrongQuery.children[1].span)
    checkStagedError(wrongQuery, "query must be nkSelect")

    let namedWindowQuery = parseSql(
      "CREATE CONTINUOUS AGGREGATE c AS " &
      "SELECT ROW_NUMBER() OVER w FROM m WINDOW w AS (ORDER BY id) " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    checkStagedError(namedWindowQuery, "cannot contain WINDOW")

    let qualifyQuery = parseSql(
      "CREATE CONTINUOUS AGGREGATE c AS SELECT 1 FROM m QUALIFY 1 = 1 " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    checkStagedError(qualifyQuery, "cannot contain QUALIFY")

    let withTiesQuery = parseSql(
      "CREATE CONTINUOUS AGGREGATE c AS " &
      "SELECT 1 FROM m ORDER BY 1 FETCH FIRST 1 ROW WITH TIES " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    checkStagedError(withTiesQuery, "cannot contain FETCH ... WITH TIES")

    var malformedWhere = canonicalContinuousAggregate()
    malformedWhere.children[1].children.add(
      newNode(nkWhereClause, malformedWhere.children[1].span)
    )
    checkStagedError(malformedWhere, "WHERE clause must have exactly 1 child")

    var nilBinaryOperand = canonicalContinuousAggregate()
    nilBinaryOperand.children[1].children[0].children[0] = newBinaryOp(
      opAdd, newIntLit(1, nilBinaryOperand.span), nil, nilBinaryOperand.span
    )
    checkStagedError(nilBinaryOperand, "binary expression operands must not be nil")

    var emptyCase = canonicalContinuousAggregate()
    let emptyCaseSpan = emptyCase.children[1].children[0].children[0].span
    emptyCase.children[1].children[0].children[0] = newNode(nkCase, emptyCaseSpan)
    checkStagedError(emptyCase, "CASE expression must have at least 1 branch")

    var wrongCaseBranch = canonicalContinuousAggregate()
    let wrongCaseSpan = wrongCaseBranch.children[1].children[0].children[0].span
    let wrongCase = newNode(nkCase, wrongCaseSpan)
    wrongCase.caseBranches.add(newIntLit(1, wrongCaseSpan))
    wrongCaseBranch.children[1].children[0].children[0] = wrongCase
    checkStagedError(wrongCaseBranch, "CASE branch must contain WHEN and THEN")

    var nilCaseWhen = canonicalContinuousAggregate()
    let nilWhenSpan = nilCaseWhen.children[1].children[0].children[0].span
    let nilWhenCase = newNode(nkCase, nilWhenSpan)
    let nilWhenBranch = newNode(nkCaseWhen, nilWhenSpan)
    nilWhenBranch.caseThen = newIntLit(1, nilWhenSpan)
    nilWhenCase.caseBranches.add(nilWhenBranch)
    nilCaseWhen.children[1].children[0].children[0] = nilWhenCase
    checkStagedError(nilCaseWhen, "CASE branch must contain WHEN and THEN")

    var nilCaseThen = canonicalContinuousAggregate()
    let nilThenSpan = nilCaseThen.children[1].children[0].children[0].span
    let nilThenCase = newNode(nkCase, nilThenSpan)
    let nilThenBranch = newNode(nkCaseWhen, nilThenSpan)
    nilThenBranch.caseWhen = newBoolLit(true, nilThenSpan)
    nilThenCase.caseBranches.add(nilThenBranch)
    nilCaseThen.children[1].children[0].children[0] = nilThenCase
    checkStagedError(nilCaseThen, "CASE branch must contain WHEN and THEN")

    var cyclicCaseTree = canonicalContinuousAggregate()
    let cyclicCaseSpan = cyclicCaseTree.children[1].children[0].children[0].span
    let cyclicCase = newNode(nkCase, cyclicCaseSpan)
    let cyclicBranch = newNode(nkCaseWhen, cyclicCaseSpan)
    cyclicBranch.caseWhen = newBoolLit(true, cyclicCaseSpan)
    cyclicBranch.caseThen = cyclicCase
    cyclicCase.caseBranches.add(cyclicBranch)
    cyclicCaseTree.children[1].children[0].children[0] = cyclicCase
    checkStagedError(cyclicCaseTree, "cycle")

    var malformedVector = canonicalContinuousAggregate()
    let vector = newNode(nkVectorLiteral, malformedVector.span)
    vector.children.add(newIntLit(1, malformedVector.span))
    malformedVector.children[1].children[0].children[0] = vector
    checkStagedError(malformedVector, "vector literal children must be nkFloatLit")

    var wrongOptions = canonicalContinuousAggregate()
    wrongOptions.children[2] = newNode(nkExprList, wrongOptions.children[2].span)
    checkStagedError(wrongOptions, "options must be nkWithOptions")

    var nestedNilOption = canonicalContinuousAggregate()
    nestedNilOption.children[2].children[0] = nil
    checkStagedError(nestedNilOption, "nil node")

    var cyclic = canonicalContinuousAggregate()
    let expressionSpan = cyclic.children[1].children[0].children[0].span
    var cycleExpr = newUnaryOp(opNeg, newIntLit(1, expressionSpan), expressionSpan)
    cycleExpr.unOperand = cycleExpr
    cyclic.children[1].children[0].children[0] = cycleExpr
    checkStagedError(cyclic, "cycle")

    var missingOption = canonicalContinuousAggregate()
    missingOption.children[2].children.setLen(1)
    checkStagedError(missingOption, "exactly 2 options")

    var extraOption = canonicalContinuousAggregate()
    extraOption.children[2].children.add(
      newNode(nkIndexOption, extraOption.children[2].span)
    )
    checkStagedError(extraOption, "exactly 2 options")

    var wrongOptionNode = canonicalContinuousAggregate()
    wrongOptionNode.children[2].children[0] = newNode(
      nkExprList, wrongOptionNode.children[2].children[0].span
    )
    checkStagedError(wrongOptionNode, "option must be nkIndexOption")

    var wrongOptionShape = canonicalContinuousAggregate()
    wrongOptionShape.children[2].children[0].children.setLen(1)
    checkStagedError(wrongOptionShape, "option must have exactly 2 children")

    var wrongOptionKey = canonicalContinuousAggregate()
    wrongOptionKey.children[2].children[0].children[0] = newStringLit(
      "retention", wrongOptionKey.children[2].children[0].children[0].span
    )
    checkStagedError(wrongOptionKey, "option key must be nkIdentifier")

    var wrongOptionValue = canonicalContinuousAggregate()
    wrongOptionValue.children[2].children[0].children[1] = newIntLit(
      7, wrongOptionValue.children[2].children[0].children[1].span
    )
    checkStagedError(wrongOptionValue, "option value must be nkStringLit")

    var unknownOption = canonicalContinuousAggregate()
    unknownOption.children[2].children[1].children[0].strVal = "bogus"
    checkStagedError(unknownOption, "expected option refresh_interval")

    var reordered = canonicalContinuousAggregate()
    swap(reordered.children[2].children[0], reordered.children[2].children[1])
    checkStagedError(reordered, "expected option retention")

    var invalidMultilineSpan = canonicalContinuousAggregate()
    invalidMultilineSpan.children[2].children[0].children[1].span = Span(
      start: Location(line: 1, column: 1),
      `end`: Location(line: 2, column: 0),
    )
    checkStagedError(invalidMultilineSpan, "invalid span")

    var wide = canonicalContinuousAggregate()
    let wideItem = wide.children[1].children[0].children[0]
    wide.children[1].children[0].children.setLen(12_000)
    for i in 0 ..< wide.children[1].children[0].children.len:
      wide.children[1].children[0].children[i] = wideItem
    checkStagedError(wide, "exceeds 1048576-byte limit")

    var atLimit = canonicalContinuousAggregate()
    let deepSpan = atLimit.children[1].children[0].children[0].span
    var expression = newIntLit(1, deepSpan)
    for _ in 0 ..< 124:
      expression = newUnaryOp(opNeg, expression, deepSpan)
    atLimit.children[1].children[0].children[0] = expression
    validateContinuousAggregateV040ForTest(atLimit)

    var tooDeep = canonicalContinuousAggregate()
    tooDeep.children[1].children[0].children[0] =
      newUnaryOp(opNeg, expression, deepSpan)
    checkStagedError(tooDeep, "maximum staged AST nesting depth of 128 exceeded")

  test "unexpected staged writer Defect keeps its internal classification":
    expect Defect:
      triggerStagedWriterDefectForTest()

  test "actual MessagePack depth accepts 128 containers and rejects 129":
    let exact = repeat("\x91", 128) & "\xc0"
    check validateStagedMessagePackDepthForTest(exact) == 128
    let message = messagePackDepthError("\x91" & exact)
    check message.contains("nesting depth of 128 exceeded")
    check message.len <= 160

  test "incremental observer handles split lengths and bin/ext marker families":
    for payload in [
      "\xd9\x00", "\xda\x00\x00", "\xdb\x00\x00\x00\x00",
      "\xdc\x00\x01\xc0", "\xdd\x00\x00\x00\x01\xc0",
      "\xde\x00\x01\xc0\xc0", "\xdf\x00\x00\x00\x01\xc0\xc0",
      "\xc4\x00", "\xc5\x00\x00", "\xc6\x00\x00\x00\x00",
      "\xc7\x00\x00", "\xc8\x00\x00\x00", "\xc9\x00\x00\x00\x00\x00",
      "\xd4\x00\x00", "\xd5\x00\x00\x00", "\xd6\x00" & repeat("\x00", 4),
      "\xd7\x00" & repeat("\x00", 8), "\xd8\x00" & repeat("\x00", 16),
    ]:
      discard validateStagedMessagePackChunksForTest(oneByteChunks(payload))

  test "actual MessagePack byte limit accepts exact-minus and exact, rejects plus":
    const wireLimit = 1_048_576
    var statement = canonicalContinuousAggregate()
    statement.children[0].strVal = repeat("x", 1_000_000)
    let trial = encodeContinuousAggregateV040ToMsgPack(statement)
    let exactNameBytes = statement.children[0].strVal.len + wireLimit - trial.len
    check exactNameBytes > 1_000_000

    statement.children[0].strVal = repeat("x", exactNameBytes - 1)
    check encodeContinuousAggregateV040ToMsgPack(statement).len == wireLimit - 1
    statement.children[0].strVal = repeat("x", exactNameBytes)
    check encodeContinuousAggregateV040ToMsgPack(statement).len == wireLimit
    statement.children[0].strVal = repeat("x", exactNameBytes + 1)
    checkStagedError(statement, "exceeds 1048576-byte limit")

  test "future helper reuses the existing Select writer for full clauses":
    let statement = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS " &
      "SELECT host, AVG(value) FROM samples WHERE value > 0 GROUP BY host " &
      "HAVING AVG(value) > 0 ORDER BY host DESC NULLS LAST LIMIT 1 " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    let query = toJsonNode(
      encodeContinuousAggregateV040ToMsgPack(statement)
    )["kind"]["query"]
    check query["projection"].len == 2
    check query["from"].len == 1
    check query["selection"].kind != JNull
    check query["group_by"].len == 1
    check query["having"].kind != JNull
    check query["order_by"].len == 1
    check query["limit"].kind != JNull

  test "staged payload keeps FETCH ... ONLY on the frozen limit/offset keys":
    let statement = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS " &
      "SELECT host FROM samples ORDER BY host OFFSET 1 ROW " &
      "FETCH FIRST 2 ROWS ONLY " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    let query = toJsonNode(
      encodeContinuousAggregateV040ToMsgPack(statement)
    )["kind"]["query"]
    check query["limit"]["kind"]["literal"]["value"].getStr() == "2"
    check query["offset"]["kind"]["literal"]["value"].getStr() == "1"
    check not query.hasKey("limit_with_ties")

  test "future helper accepts the existing CAST DECIMAL grammar":
    let statement = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS " &
      "SELECT CAST(value AS DECIMAL(10,2)) FROM samples " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    let query = toJsonNode(
      encodeContinuousAggregateV040ToMsgPack(statement)
    )["kind"]["query"]
    let castKind = query["projection"][0]["expr"]["kind"]
    check castKind["variant"].getStr() == "Cast"
    check castKind["target_type"]["variant"].getStr() == "Double"

  test "TRY_CAST has a dedicated MessagePack expression variant":
    let statement = parseSql(
      "CREATE CONTINUOUS AGGREGATE hourly AS " &
      "SELECT TRY_CAST(value AS INTEGER) FROM samples " &
      "WITH (retention = '7d', refresh_interval = '1h')"
    )
    let query = toJsonNode(
      encodeContinuousAggregateV040ToMsgPack(statement)
    )["kind"]["query"]
    let castKind = query["projection"][0]["expr"]["kind"]
    check castKind["variant"].getStr() == "TryCast"
    check castKind["target_type"]["variant"].getStr() == "Integer"

  test "descriptor compile-time selects the public encoder route and C entry recovers":
    let statement = canonicalContinuousAggregate()

    let boundary = alopex_parse_sql(
      cstring(CanonicalContinuousAggregateSql),
      cint(CanonicalContinuousAggregateSql.len),
    )
    when ContinuousAggregateProducerEnabled:
      check toJsonNode(astToMsgPack(@[statement]))[0]["kind"]["variant"].getStr() ==
        "CreateContinuousAggregate"
      check boundary.kind == prkOk
      check boundary.buffer_ptr != nil
      check boundary.buffer_len > 0
      alopex_free_buffer(boundary.buffer_ptr)
    else:
      expect ParseError:
        discard astToMsgPack(@[statement])
      check boundary.kind == prkError
      check boundary.buffer_ptr == nil
      check boundary.error_ptr != nil
      check boundary.error_len > 0
      check boundary.error_len <= 128
      var message = newString(int(boundary.error_len))
      copyMem(addr message[0], boundary.error_ptr, int(boundary.error_len))
      check message.contains("unsupported statement node for MessagePack")
      check message.contains("nkCreateContinuousAggregate")
      alopex_free_buffer(cast[pointer](boundary.error_ptr))

    let normalSql = "SELECT 1"
    let recovered = alopex_parse_sql(cstring(normalSql), cint(normalSql.len))
    check recovered.kind == prkOk
    check recovered.buffer_ptr != nil
    check recovered.buffer_len > 0
    alopex_free_buffer(recovered.buffer_ptr)

  test "window frame is emitted as an additive optional WindowSpec field":
    let payload = toJsonNode(encodeSqlToMsgPack(
      "SELECT SUM(qty) OVER (ORDER BY amount RANGE BETWEEN 50 PRECEDING AND CURRENT ROW) FROM sales"
    ))
    let window = payload[0]["kind"]["projection"][0]["expr"]["kind"]["over"]
    check window["partition_by"].len == 0
    check window["order_by"].len == 1
    check window["frame"]["units"].getStr() == "Range"
    check window["frame"]["start_bound"]["variant"].getStr() == "Preceding"
    check window["frame"]["start_bound"]["value"].getBiggestInt() == 50
    check window["frame"]["end_bound"]["variant"].getStr() == "CurrentRow"

  test "named windows and QUALIFY are emitted in the Select contract":
    let kind = selectKind(
      "SELECT ROW_NUMBER() OVER ranked AS rn FROM sales " &
      "WINDOW base AS (PARTITION BY region), " &
      "ranked AS (base ORDER BY amount DESC) QUALIFY rn = 1"
    )
    check kind["windows"].len == 2
    check kind["windows"][0]["name"].getStr() == "base"
    check kind["windows"][0]["spec"]["base"].kind == JNull
    check kind["windows"][0]["spec"]["partition_by"].len == 1
    check kind["windows"][1]["name"].getStr() == "ranked"
    check kind["windows"][1]["spec"]["base"].getStr() == "base"
    check kind["windows"][1]["spec"]["order_by"].len == 1
    check kind["projection"][0]["expr"]["kind"]["over"]["base"].getStr() ==
      "ranked"
    check kind["qualify"]["kind"]["variant"].getStr() == "BinaryOp"
