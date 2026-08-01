use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::join::{hash_join, nested_loop_join};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError, Row};
use alopex_sql::parser::Parser;
use alopex_sql::planner::logical_plan::JoinType;
use alopex_sql::planner::typed_expr::TypedExpr;
use alopex_sql::planner::{Planner, ResolvedType};
use alopex_sql::storage::SqlValue;

fn try_run_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let dialect = AlopexDialect;
    let statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut results = Vec::new();
    for stmt in statements {
        let guard = catalog.read().unwrap();
        let plan = Planner::new(&*guard).plan(&stmt)?;
        drop(guard);
        results.push(executor.execute(plan)?);
    }
    Ok(results)
}

fn run_sql(sql: &str) -> Vec<ExecutionResult> {
    try_run_sql(sql).expect("execute sql")
}

fn last_query(sql: &str) -> alopex_sql::executor::QueryResult {
    run_sql(sql)
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

fn setup_sql(select: &str) -> String {
    format!(
        r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20), (13, 9, 99);
        {select};
        "#
    )
}

#[test]
fn inner_join_uses_equi_condition() {
    let query = last_query(&setup_sql(
        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
    ));
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::Integer(50)],
            vec![SqlValue::Text("alice".into()), SqlValue::Integer(75)],
            vec![SqlValue::Text("bob".into()), SqlValue::Integer(20)],
        ]
    );
}

/// A duplicate range-variable name cannot identify one input of a self join.
/// Resolving it to the first table would silently read the wrong column.
#[test]
fn duplicate_join_qualifiers_are_rejected_as_ambiguous() {
    for (from, reference) in [
        ("t AS x JOIN t AS x ON 1 = 1", "x.id"),
        ("t JOIN t ON 1 = 1", "t.id"),
    ] {
        let err = try_run_sql(&format!(
            "CREATE TABLE t (id INT PRIMARY KEY); SELECT {reference} FROM {from};"
        ))
        .expect_err("duplicate relation names must not bind a qualified column to the first input");

        assert!(
            err.to_string().contains("ALOPEX-C004"),
            "expected C004 for {from}, got: {err}"
        );
    }
}

#[test]
fn outer_and_cross_joins_cover_unmatched_rows() {
    let left = last_query(&setup_sql(
        "SELECT users.id, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id, orders.total",
    ));
    assert!(
        left.rows
            .contains(&vec![SqlValue::Integer(3), SqlValue::Null])
    );

    let right = last_query(&setup_sql(
        "SELECT users.id, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
    ));
    assert!(
        right
            .rows
            .contains(&vec![SqlValue::Null, SqlValue::Integer(99)])
    );

    let full = last_query(&setup_sql(
        "SELECT users.id, orders.total FROM users FULL JOIN orders ON users.id = orders.user_id ORDER BY users.id, orders.id",
    ));
    assert!(
        full.rows
            .contains(&vec![SqlValue::Integer(3), SqlValue::Null])
    );
    assert!(
        full.rows
            .contains(&vec![SqlValue::Null, SqlValue::Integer(99)])
    );

    let cross = last_query(&setup_sql(
        "SELECT users.id, orders.id FROM users CROSS JOIN orders",
    ));
    assert_eq!(cross.rows.len(), 12);
}

#[test]
fn empty_and_skewed_join_inputs_are_handled() {
    let query = last_query(
        r#"
        CREATE TABLE lefts (id INT PRIMARY KEY, k INT);
        CREATE TABLE rights (id INT PRIMARY KEY, k INT);
        INSERT INTO lefts (id, k) VALUES (1, 7), (2, 7), (3, 7), (4, 8);
        SELECT lefts.id, rights.id FROM lefts LEFT JOIN rights ON lefts.k = rights.k ORDER BY lefts.id;
        "#,
    );
    assert_eq!(query.rows.len(), 4);
    assert!(query.rows.iter().all(|row| row[1] == SqlValue::Null));
}

#[test]
fn nested_loop_and_hash_join_match_for_equi_join() {
    let left = vec![
        Row::new(0, vec![SqlValue::Integer(1), SqlValue::Text("a".into())]),
        Row::new(1, vec![SqlValue::Integer(2), SqlValue::Text("b".into())]),
    ];
    let right = vec![
        Row::new(0, vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
        Row::new(1, vec![SqlValue::Integer(3), SqlValue::Integer(30)]),
    ];
    let condition = TypedExpr::binary_op(
        TypedExpr::column_ref(
            "l".into(),
            "id".into(),
            0,
            ResolvedType::Integer,
            Default::default(),
        ),
        alopex_sql::ast::expr::BinaryOp::Eq,
        TypedExpr::column_ref(
            "r".into(),
            "id".into(),
            2,
            ResolvedType::Integer,
            Default::default(),
        ),
        ResolvedType::Boolean,
        Default::default(),
    );

    let nested = nested_loop_join(&left, &right, &condition, JoinType::Inner).unwrap();
    let hashed = hash_join(&left, &right, 0, 0, JoinType::Inner).unwrap();
    assert_eq!(nested, hashed);
}

#[test]
fn natural_join_coalesces_common_columns_and_other_join_forms_remain_available() {
    let natural = last_query(
        r#"
        CREATE TABLE t (s TEXT, left_value INT);
        CREATE TABLE u (s TEXT, right_value INT);
        INSERT INTO t VALUES ('shared', 1), ('left-only', 2);
        INSERT INTO u VALUES ('shared', 10), ('right-only', 20);
        SELECT s, left_value, right_value FROM t NATURAL JOIN u;
        "#,
    );
    assert_eq!(
        natural.rows,
        vec![vec![
            SqlValue::Text("shared".into()),
            SqlValue::Integer(1),
            SqlValue::Integer(10),
        ]]
    );

    let natural_star = last_query(
        r#"
        CREATE TABLE t (s TEXT, left_value INT);
        CREATE TABLE u (s TEXT, right_value INT);
        INSERT INTO t VALUES ('shared', 1);
        INSERT INTO u VALUES ('shared', 10);
        SELECT * FROM t NATURAL JOIN u;
        "#,
    );
    assert_eq!(
        natural_star
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["s", "left_value", "right_value"]
    );
    assert_eq!(
        natural_star.rows,
        vec![vec![
            SqlValue::Text("shared".into()),
            SqlValue::Integer(1),
            SqlValue::Integer(10),
        ]]
    );

    let using = last_query(
        r#"
        CREATE TABLE t (s TEXT, left_value INT);
        CREATE TABLE u (s TEXT, right_value INT);
        INSERT INTO t VALUES ('shared', 1);
        INSERT INTO u VALUES ('shared', 10);
        SELECT s FROM t JOIN u USING (s);
        "#,
    );
    assert_eq!(using.rows, vec![vec![SqlValue::Text("shared".into())]]);

    for sql in [
        "SELECT t.s FROM t INNER JOIN u ON t.s = u.s",
        "SELECT t.s FROM t LEFT JOIN u ON t.s = u.s",
        "SELECT u.s FROM t RIGHT JOIN u ON t.s = u.s",
        "SELECT t.s FROM t FULL JOIN u ON t.s = u.s",
        "SELECT t.s FROM t CROSS JOIN u",
        "SELECT t.s FROM t, u",
    ] {
        let query = last_query(&format!(
            "
            CREATE TABLE t (s TEXT);
            CREATE TABLE u (s TEXT);
            INSERT INTO t VALUES ('shared');
            INSERT INTO u VALUES ('shared');
            {sql};
            "
        ));
        assert_eq!(query.rows.len(), 1, "{sql}");
    }
}

/// USING and NATURAL merge their common columns, so an unqualified reference must
/// resolve to the merged value, not to the left input. Under RIGHT and FULL joins
/// the left side is NULL for right-only rows, so binding to the left silently
/// returns NULL where the key exists.
#[test]
fn using_and_natural_common_columns_merge_under_outer_joins() {
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "
        CREATE TABLE p (k INT PRIMARY KEY, a TEXT);
        CREATE TABLE q (k INT PRIMARY KEY, b TEXT);
        INSERT INTO p (k, a) VALUES (1, 'p1'), (2, 'p2');
        INSERT INTO q (k, b) VALUES (2, 'q2'), (3, 'q3');
        SELECT k FROM p RIGHT JOIN q USING (k) ORDER BY k;
        SELECT k FROM p FULL JOIN q USING (k) ORDER BY k;
        SELECT k FROM p NATURAL RIGHT JOIN q ORDER BY k;
        SELECT k FROM p NATURAL FULL JOIN q ORDER BY k;
        ",
    )
    .expect("parse outer joins");

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut queries = Vec::new();
    let mut names: Vec<Vec<String>> = Vec::new();

    for statement in statements {
        let guard = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*guard)
            .plan(&statement)
            .expect("plan outer joins");
        drop(guard);
        if let ExecutionResult::Query(query) = executor.execute(plan).expect("execute outer joins")
        {
            names.push(
                query
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>(),
            );
            queries.push(query.rows);
        }
    }

    // The merged column keeps its name; it must not degrade to col_0.
    assert!(
        names.iter().all(|columns| columns == &["k"]),
        "merged column lost its name: {names:?}"
    );

    let right = vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(3)]];
    let full = vec![
        vec![SqlValue::Integer(1)],
        vec![SqlValue::Integer(2)],
        vec![SqlValue::Integer(3)],
    ];
    assert_eq!(queries, vec![right.clone(), full.clone(), right, full]);
}
