use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{RowCodec, SqlValue};

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

fn array(values: Vec<SqlValue>) -> SqlValue {
    SqlValue::Array(values)
}

fn int(value: i32) -> SqlValue {
    SqlValue::Integer(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.into())
}

#[test]
fn array_literal_functions_subscript_and_slice() {
    let result = run("SELECT ARRAY_APPEND(ARRAY[1, 2], 3),
                ARRAY_PREPEND(0, ARRAY[1, 2]),
                ARRAY_CAT(ARRAY[1, 2], ARRAY[3]),
                ARRAY_REMOVE(ARRAY[1, NULL, 1], 1),
                ARRAY_REPLACE(ARRAY[1, NULL, 1], 1, 9),
                ARRAY_LENGTH(ARRAY[1, NULL]),
                ARRAY_POSITION(ARRAY[4, NULL, 5], 5),
                ARRAY_POSITIONS(ARRAY[4, 5, 4], 4),
                STRING_TO_ARRAY('a,b', ','),
                ARRAY_TO_STRING(ARRAY['a', NULL, 'b'], ',', 'NULL'),
                ARRAY[10, 20, 30][2], ARRAY[10, 20, 30][2:3]")
    .unwrap()
    .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            array(vec![int(1), int(2), int(3)]),
            array(vec![int(0), int(1), int(2)]),
            array(vec![int(1), int(2), int(3)]),
            array(vec![SqlValue::Null]),
            array(vec![int(9), SqlValue::Null, int(9)]),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
            array(vec![int(1), int(3)]),
            array(vec![text("a"), text("b")]),
            SqlValue::Text("a,NULL,b".into()),
            SqlValue::Integer(20),
            array(vec![int(20), int(30)]),
        ]]
    );

    let widened = run("SELECT ARRAY[1, 2.5], ARRAY[1, 2.5][1]")
        .unwrap()
        .unwrap();
    assert_eq!(
        widened.rows,
        vec![vec![
            array(vec![SqlValue::Double(1.0), SqlValue::Double(2.5)]),
            SqlValue::Double(1.0),
        ]]
    );
}

#[test]
fn nested_values_roundtrip_and_keep_nulls() {
    let row = vec![
        array(vec![int(1), SqlValue::Null, int(3)]),
        SqlValue::Map(vec![(text("a"), int(1))]),
        SqlValue::Struct(vec![("name".into(), text("Ada"))]),
    ];
    assert_eq!(RowCodec::decode(&RowCodec::encode(&row)).unwrap(), row);
    assert_eq!(
        SqlValue::Map(vec![(int(1), text("one"))])
            .nested_json_text()
            .unwrap(),
        r#"[[1,"one"]]"#
    );
    let invalid_map = vec![SqlValue::Map(vec![(SqlValue::Null, int(1))])];
    assert!(
        RowCodec::decode_with_schema(
            &RowCodec::encode(&invalid_map),
            &[alopex_sql::planner::ResolvedType::Map {
                key: Box::new(alopex_sql::planner::ResolvedType::Text),
                value: Box::new(alopex_sql::planner::ResolvedType::Integer),
            }],
        )
        .is_err()
    );

    let result = run("CREATE TABLE nested (
             id INTEGER PRIMARY KEY,
             items ARRAY<INTEGER>,
             aliases LIST<TEXT>,
             attrs MAP<TEXT, INTEGER>,
             person STRUCT<name TEXT, age INTEGER>
         );
         INSERT INTO nested VALUES (
             1, ARRAY[1, NULL, 3], ARRAY['x'],
             MAP(ARRAY['a'], ARRAY[1]),
             STRUCT_PACK('name', 'Ada', 'age', 37)
         );
         SELECT items, aliases, attrs['a'], person['name'] FROM nested")
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            array(vec![int(1), SqlValue::Null, int(3)]),
            array(vec![text("x")]),
            SqlValue::Integer(1),
            SqlValue::Text("Ada".into()),
        ]]
    );
}

#[test]
fn array_agg_and_unnest_with_ordinality_are_lateral() {
    let aggregate = run("SELECT ARRAY_AGG(v) FROM (VALUES (1), (NULL), (2)) AS t(v)")
        .unwrap()
        .unwrap();
    assert_eq!(
        aggregate.rows,
        vec![vec![array(vec![int(1), SqlValue::Null, int(2)])]]
    );

    let unnested = run("SELECT p.id, u.value, u.ordinality
         FROM (VALUES (1, ARRAY['a', 'b']), (2, ARRAY['c'])) AS p(id, items),
              LATERAL UNNEST(p.items) WITH ORDINALITY AS u(value, ordinality)
         ORDER BY p.id, u.ordinality")
    .unwrap()
    .unwrap();
    assert_eq!(
        unnested.rows,
        vec![
            vec![int(1), text("a"), SqlValue::BigInt(1)],
            vec![int(1), text("b"), SqlValue::BigInt(2)],
            vec![int(2), text("c"), SqlValue::BigInt(1)],
        ]
    );
}

#[test]
fn nested_values_are_not_orderable_index_keys() {
    let primary = run("CREATE TABLE bad (items ARRAY<INTEGER> PRIMARY KEY)").unwrap_err();
    assert!(primary.contains("nested-value sort order"), "{primary}");

    let index = run(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, items ARRAY<INTEGER>); \
         CREATE INDEX bad_items ON t(items)",
    )
    .unwrap_err();
    assert!(index.contains("nested-value sort order"), "{index}");
}
