## MessagePack contract tests for the Nim SQL parser FFI payload.

import std/[json, strutils, unittest]
import msgpack4nim/msgpack2json
import ../src/alopex_sql_parser

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

  test "Vector and index DDL round-trip":
    for sql in [
      "CREATE TABLE items (id INT, embedding VECTOR(3, COSINE))",
      "SELECT [1.0, -2.0, 3.5] FROM items",
      "CREATE INDEX idx_doc_embedding ON documents (embedding) USING HNSW WITH (m = 16, ef_construction = 200)",
      "DROP INDEX IF EXISTS idx_doc_embedding",
    ]:
      assertMsgpackRoundtrip(sql)

suite "MessagePack output - contract shape":

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
    check item["subquery"]["kind"]["variant"].getStr() == "Select"

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

  test "CASE emits simple operand, ordered branches, and optional ELSE":
    let caseExpr = selectKind("SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM users")["projection"][0]["expr"]["kind"]
    check caseExpr["variant"].getStr() == "Case"
    check caseExpr["operand"]["kind"]["variant"].getStr() == "ColumnRef"
    check caseExpr["branches"].len == 2
    check caseExpr["branches"][0]["when"]["kind"]["literal"]["value"].getStr() == "1"
    check caseExpr["branches"][1]["then"]["kind"]["literal"]["value"].getStr() == "two"
    check caseExpr["else_expr"]["kind"]["literal"]["value"].getStr() == "other"

  test "CASE emits nil for omitted operand and ELSE":
    let caseExpr = selectKind("SELECT CASE WHEN TRUE THEN 1 END")["projection"][0]["expr"]["kind"]
    check caseExpr["variant"].getStr() == "Case"
    check caseExpr["operand"].kind == JNull
    check caseExpr["else_expr"].kind == JNull

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
    check kind["values"].len == 2
    check kind["values"][0][0]["kind"]["variant"].getStr() == "Literal"
    check kind["values"][0][0]["kind"]["literal"]["variant"].getStr() == "Number"
    check kind["values"][0][0]["kind"]["literal"]["value"].getStr() == "1"
    check kind["values"][0][1]["kind"]["literal"]["variant"].getStr() == "String"
    check kind["values"][0][1]["kind"]["literal"]["value"].getStr() == "a"
    check kind["values"][1][0]["kind"]["literal"]["value"].getStr() == "2"
    check kind["values"][1][1]["kind"]["literal"]["value"].getStr() == "b"

  test "multi-row all-string INSERT without column list is not misread as columns":
    # 先頭行が全て文字列だと firstIdent が例外を出さず、列リストとして
    # 静かに誤変換される回帰パターン。
    let kind = payloadJson("INSERT INTO t1 VALUES ('a', 'b'), ('c', 'd')").stmtKind()
    check kind["columns"].kind == JNull
    check kind["values"].len == 2
    check kind["values"][0][0]["kind"]["literal"]["value"].getStr() == "a"

  test "multi-row INSERT with column list keeps explicit columns":
    let kind = payloadJson("INSERT INTO t1 (id, name) VALUES (1, 'a'), (2, 'b')").stmtKind()
    check kind["columns"].len == 2
    check kind["columns"][0].getStr() == "id"
    check kind["columns"][1].getStr() == "name"
    check kind["values"].len == 2

  test "multi-row INSERT without column list round-trips":
    assertMsgpackRoundtrip("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")

suite "MessagePack output - stability":

  test "SELECT literal payload is stable":
    check hexPayload("SELECT 1") ==
      "9182A46B696E648AA776617269616E74A653656C656374A864697374696E6374C2AA70726F6A656374696F6E9184A776617269616E74A445787072A46578707282A46B696E6482A776617269616E74A74C69746572616CA76C69746572616C82A776617269616E74A64E756D626572A576616C7565A131A47370616E82A5737461727482A46C696E6501A6636F6C756D6E08A3656E6482A46C696E6501A6636F6C756D6E08A5616C696173C0A47370616E82A5737461727482A46C696E6501A6636F6C756D6E08A3656E6482A46C696E6501A6636F6C756D6E08A466726F6D90A973656C656374696F6EC0A867726F75705F6279C0A6686176696E67C0A86F726465725F627990A56C696D6974C0A66F6666736574C0A47370616E82A5737461727482A46C696E6501A6636F6C756D6E01A3656E6482A46C696E6501A6636F6C756D6E06"
