use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

struct Harness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl Harness {
    fn new() -> Self {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Self {
            executor: Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog)),
            catalog,
        }
    }

    fn query(&mut self, sql: &str) -> QueryResult {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse");
        let mut result = None;
        for statement in statements {
            let plan = Planner::new(&*self.catalog.read().expect("catalog"))
                .plan(&statement)
                .expect("plan");
            if let ExecutionResult::Query(query) = self.executor.execute(plan).expect("execute") {
                result = Some(query);
            }
        }
        result.expect("query result")
    }

    fn error(&mut self, sql: &str) -> String {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse");
        for statement in statements {
            let plan = match Planner::new(&*self.catalog.read().expect("catalog")).plan(&statement)
            {
                Ok(plan) => plan,
                Err(error) => return error.to_string(),
            };
            if let Err(error) = self.executor.execute(plan) {
                return error.to_string();
            }
        }
        panic!("expected error for {sql}")
    }
}

fn row(sql: &str) -> Vec<SqlValue> {
    Harness::new().query(sql).rows.remove(0)
}

#[test]
fn json_scalar_functions_cover_sqlite_compatible_core() {
    assert_eq!(
        row("SELECT JSON(' { \"a\" : [1,true,null] } '), JSON_VALID('{\"a\":1}'), JSON_VALID('{')"),
        vec![
            SqlValue::Text(r#"{"a":[1,true,null]}"#.into()),
            SqlValue::Boolean(true),
            SqlValue::Boolean(false),
        ]
    );
    assert_eq!(
        row(
            "SELECT JSON_TYPE('{\"a\":[10]}', '$.a'), JSON_EXTRACT('{\"a\":[10]}', '$.a[0]'), JSON_ARRAY_LENGTH('[1,2,3]')"
        ),
        vec![
            SqlValue::Text("array".into()),
            SqlValue::Integer(10),
            SqlValue::Integer(3)
        ]
    );
    assert_eq!(
        row("SELECT JSON_OBJECT('a', 1, 'a', 2), JSON_ARRAY(1, 'x', NULL)"),
        vec![
            SqlValue::Text(r#"{"a":1,"a":2}"#.into()),
            SqlValue::Text(r#"[1,"x",null]"#.into()),
        ]
    );
    assert_eq!(
        row(
            "SELECT JSON_INSERT('{\"a\":1}', '$.a', 9, '$.b', 2), JSON_REPLACE('{\"a\":1}', '$.a', 9, '$.b', 2), JSON_SET('{\"a\":1}', '$.a', 9, '$.b', 2), JSON_REMOVE('{\"a\":1,\"b\":2}', '$.a')"
        ),
        vec![
            SqlValue::Text(r#"{"a":1,"b":2}"#.into()),
            SqlValue::Text(r#"{"a":9}"#.into()),
            SqlValue::Text(r#"{"a":9,"b":2}"#.into()),
            SqlValue::Text(r#"{"b":2}"#.into()),
        ]
    );
}

#[test]
fn json_invalid_input_and_path_are_controlled_errors() {
    let invalid = Harness::new().error("SELECT JSON('{')");
    assert!(invalid.contains("JSON"), "{invalid}");
    let path = Harness::new().error("SELECT JSON_EXTRACT('{}', 'a')");
    assert!(path.contains("path"), "{path}");
}

#[test]
fn json_each_and_tree_use_the_existing_lateral_table_function_path() {
    let each = Harness::new().query(
        "SELECT j.k, j.v, j.t, j.fullkey FROM JSON_EACH('{\"a\":1,\"b\":[2]}') AS j(k, v, t, a, id, parent, fullkey, path)",
    );
    assert_eq!(each.rows.len(), 2);
    assert_eq!(each.rows[0][0], SqlValue::Text("a".into()));
    assert_eq!(each.rows[0][1], SqlValue::Integer(1));
    assert_eq!(each.rows[0][2], SqlValue::Text("integer".into()));
    assert_eq!(each.rows[0][3], SqlValue::Text("$.a".into()));

    let tree = Harness::new().query(
        "SELECT j.fullkey FROM JSON_TREE('[1,{\"x\":2}]') AS j(k, v, t, atom, id, parent, fullkey, path) WHERE j.atom IS NOT NULL",
    );
    assert_eq!(
        tree.rows,
        vec![
            vec![SqlValue::Text("$[0]".into())],
            vec![SqlValue::Text("$[1].x".into())],
        ]
    );

    let array = Harness::new()
        .query("SELECT j.k FROM JSON_EACH('[10]') AS j(k, v, t, atom, id, parent, fullkey, path)");
    assert_eq!(array.rows, vec![vec![SqlValue::Integer(0)]]);
}

#[test]
fn json_aggregates_match_sqlite_empty_and_null_behavior() {
    let mut harness = Harness::new();
    assert_eq!(
        harness.query("SELECT JSON_GROUP_ARRAY(x), JSON_GROUP_OBJECT(k, x) FROM (VALUES (1, 'a'), (NULL, 'b'), (2, NULL)) AS t(x, k)").rows[0],
        vec![
            SqlValue::Text("[1,null,2]".into()),
            SqlValue::Text(r#"{"a":1,"b":null}"#.into()),
        ]
    );
    assert_eq!(
        harness.query("SELECT JSON_GROUP_ARRAY(x), JSON_GROUP_OBJECT(k, x) FROM (SELECT 1 AS x, 'a' AS k WHERE FALSE) AS t").rows[0],
        vec![SqlValue::Text("[]".into()), SqlValue::Text("{}".into())]
    );
}

#[test]
fn json_text_preserves_unicode_and_common_integer_precision() {
    assert_eq!(
        row(
            "SELECT JSON_EXTRACT('{\"text\":\"雪❄️\",\"n\":9007199254740993}', '$.text'), JSON_EXTRACT('{\"n\":9007199254740993}', '$.n')"
        ),
        vec![
            SqlValue::Text("雪❄️".into()),
            SqlValue::BigInt(9_007_199_254_740_993)
        ]
    );
    assert_eq!(
        row("SELECT JSON_EXTRACT('{\"a\":1,\"a\":2}', '$.a')"),
        vec![SqlValue::Integer(2)]
    );
    assert_eq!(
        row("SELECT JSON('{\"b\":1,\"a\":2}')"),
        vec![SqlValue::Text(r#"{"b":1,"a":2}"#.into())]
    );
}

#[test]
fn json_text_resource_limits_fail_closed() {
    let nested = format!("{}0{}", "[".repeat(160), "]".repeat(160));
    let deep = format!("SELECT JSON('{nested}')");
    let error = Harness::new().error(&deep);
    assert!(error.contains("invalid JSON"), "{error}");
}

#[test]
fn json_text_canonical_outputs_match_bundled_sqlite() {
    let sqlite = rusqlite::Connection::open_in_memory().expect("sqlite");
    for sql in [
        "SELECT JSON(' { \"a\" : [1,true,null] } ')",
        "SELECT JSON_ARRAY(1, 'x', NULL)",
        "SELECT JSON_SET('{\"a\":1}', '$.a', 9, '$.b', 2)",
        "SELECT JSON_REMOVE('{\"a\":1,\"b\":2}', '$.a')",
    ] {
        let expected: String = sqlite
            .query_row(sql, [], |row| row.get(0))
            .expect("sqlite JSON1");
        assert_eq!(row(sql), vec![SqlValue::Text(expected)], "{sql}");
    }
}
