use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{JsonValue, RowCodec, SqlValue};

fn run(sql: &str) -> Result<Option<QueryResult>, String> {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, Arc::clone(&catalog));
    let mut last = None;
    for statement in Parser::parse_sql(&AlopexDialect, sql).map_err(|error| error.to_string())? {
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .map_err(|error| error.to_string())?;
        if let ExecutionResult::Query(result) =
            executor.execute(plan).map_err(|error| error.to_string())?
        {
            last = Some(result);
        }
    }
    Ok(last)
}

fn json(value: &str) -> SqlValue {
    SqlValue::Json(JsonValue::parse(value).unwrap())
}

#[test]
fn json_ddl_cast_canonicalization_and_storage_roundtrip() {
    let result = run(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, body JSONB);
         INSERT INTO documents VALUES
             (1, JSONB '{\"b\":1,\"a\":2,\"a\":3}'),
             (2, CAST('{\"n\":90071992547409931234567890}' AS JSON));
         SELECT body FROM documents ORDER BY id",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![json(r#"{"a":3,"b":1}"#)],
            vec![json(r#"{"n":90071992547409931234567890}"#)],
        ]
    );
    let encoded = RowCodec::encode(&result.rows[0]);
    assert_eq!(RowCodec::decode(&encoded).unwrap(), result.rows[0]);
}

#[test]
fn json_operators_preserve_json_null_and_extract_text() {
    let result = run(
        "SELECT JSONB '{\"a\":[{\"b\":null},{\"b\":\"ok\"}]}' -> 'a' -> 0 -> 'b' AS json_null,
                JSONB '{\"a\":[{\"b\":null},{\"b\":\"ok\"}]}' #>> '{a,1,b}' AS text_value,
                JSONB '{\"a\":[1]}' ->> 'missing' AS missing_value,
                JSON '{\"b\":1,\"a\":2}' = JSONB '{\"a\":2,\"b\":1}' AS equal_value",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            json("null"),
            SqlValue::Text("ok".into()),
            SqlValue::Null,
            SqlValue::Boolean(true),
        ]]
    );
}

#[test]
fn jsonb_build_and_update_functions_return_native_json() {
    let result = run(
        "SELECT JSONB_SET(JSONB_BUILD_OBJECT('b', 1, 'a', 2), '$.a', 3),
                JSONB_INSERT(JSONB_BUILD_ARRAY(1, 2), '$[#]', 3),
                JSONB_BUILD_ARRAY(), JSONB_BUILD_OBJECT()",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            json(r#"{"a":3,"b":1}"#),
            json("[1,2,3]"),
            json("[]"),
            json("{}"),
        ]]
    );
}

#[test]
fn jsonb_aggregates_return_native_json() {
    let result = run("SELECT JSONB_AGG(v), JSONB_OBJECT_AGG(k, v)
         FROM (VALUES ('b', 2), ('a', 1)) AS t(k, v)")
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![json("[2,1]"), json(r#"{"a":1,"b":2}"#)]]
    );
}

#[test]
fn json_path_errors_and_btree_indexes_fail_cleanly() {
    let path_error = run("SELECT JSONB '{\"a\":1}' #> 'a'").unwrap_err();
    assert!(
        path_error.contains("JSON path"),
        "unexpected error: {path_error}"
    );

    let index_error = run("CREATE TABLE documents (id INTEGER PRIMARY KEY, body JSON);
         CREATE INDEX documents_body ON documents(body)")
    .unwrap_err();
    assert!(
        index_error.contains("does not define a JSON sort order"),
        "unexpected error: {index_error}"
    );

    let primary_key_error = run("CREATE TABLE invalid (body JSON PRIMARY KEY)").unwrap_err();
    assert!(
        primary_key_error.contains("does not define a JSON sort order"),
        "unexpected error: {primary_key_error}"
    );
}
