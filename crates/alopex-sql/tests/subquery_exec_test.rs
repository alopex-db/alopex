use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
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

fn last_query(sql: &str) -> alopex_sql::executor::QueryResult {
    execute_sql(sql)
        .expect("execute sql")
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
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20);
        {select};
        "#
    )
}

#[test]
fn scalar_and_correlated_exists_subqueries_execute() {
    let query = last_query(&setup_sql(
        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id",
    ));
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );

    let exists = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id",
    ));
    assert_eq!(
        exists.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn in_any_all_and_derived_subqueries_execute() {
    let in_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(
        in_query.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );

    let any_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id = ANY (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(any_query.rows, in_query.rows);

    let all_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id < ALL (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert!(all_query.rows.is_empty());

    let derived = last_query(&setup_sql(
        "SELECT active_users.name FROM (SELECT users.id, users.name FROM users WHERE users.id < 3) AS active_users ORDER BY active_users.id",
    ));
    assert_eq!(
        derived.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn scalar_subquery_rejects_multiple_rows() {
    let err = execute_sql(&setup_sql(
        "SELECT (SELECT orders.total FROM orders) AS total FROM users",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("multiple rows"));
}

#[test]
fn local_subquery_columns_shadow_outer_scope_across_all_forms() {
    let setup = r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a (id, x) VALUES (1, 'one'), (2, 'two');
        INSERT INTO b (id, y) VALUES (1, 1), (2, 2);
    "#;

    for (predicate, expected) in [
        ("id > (SELECT MIN(id) FROM b)", 2),
        ("id IN (SELECT id FROM b WHERE b.id = 2)", 2),
        ("id NOT IN (SELECT id FROM b WHERE b.id = 1)", 2),
        ("id = ANY (SELECT id FROM b WHERE b.id = 2)", 2),
        ("id > ALL (SELECT id FROM b WHERE b.id = 1)", 2),
    ] {
        let query = last_query(&format!("{setup} SELECT id FROM a WHERE {predicate};"));
        assert_eq!(query.rows, vec![vec![SqlValue::Integer(expected)]]);
    }

    let self_reference = last_query(
        r#"
        CREATE TABLE t (v INT);
        INSERT INTO t VALUES (1), (3);
        SELECT v FROM t WHERE v > (SELECT AVG(v) FROM t);
        "#,
    );
    assert_eq!(self_reference.rows, vec![vec![SqlValue::Integer(3)]]);
}

#[test]
fn correlated_and_non_overlapping_subquery_names_remain_valid() {
    let exists = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10);
        SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id);
        "#,
    );
    assert_eq!(exists.rows, vec![vec![SqlValue::Integer(1)]]);

    let not_exists = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10);
        SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id);
        "#,
    );
    assert_eq!(not_exists.rows, vec![vec![SqlValue::Integer(2)]]);

    let scalar = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10), (2, 20);
        SELECT a.id, (SELECT y FROM b WHERE b.id = a.id) AS y FROM a ORDER BY a.id;
        "#,
    );
    assert_eq!(
        scalar.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
        ]
    );

    let explicit_self_reference = last_query(
        r#"
        CREATE TABLE t (v INT);
        INSERT INTO t VALUES (1), (3);
        SELECT o.v FROM t AS o WHERE o.v > (SELECT AVG(i.v) FROM t AS i);
        "#,
    );
    assert_eq!(
        explicit_self_reference.rows,
        vec![vec![SqlValue::Integer(3)]]
    );

    let derived = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        SELECT d.id FROM (SELECT id FROM a) AS d ORDER BY d.id;
        "#,
    );
    assert_eq!(
        derived.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );

    let unique_inner_name = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (2);
        INSERT INTO b VALUES (1, 1);
        SELECT id FROM a WHERE id > (SELECT MIN(y) FROM b);
        "#,
    );
    assert_eq!(unique_inner_name.rows, vec![vec![SqlValue::Integer(2)]]);
}

#[test]
fn a_derived_table_does_not_see_the_enclosing_query_without_lateral() {
    // Standard SQL evaluates a derived table independently of the query it sits
    // in, so `o` is not in scope inside it; only LATERAL would make it visible.
    // Resolving `o.v` here builds a correlated reference the user never wrote.
    let error = execute_sql(
        r#"
        CREATE TABLE t (v INT);
        CREATE TABLE u (w INT);
        INSERT INTO t VALUES (1);
        INSERT INTO u VALUES (1);
        SELECT o.v FROM t AS o
        WHERE o.v = (SELECT d.w FROM (SELECT u.w FROM u WHERE u.w = o.v) AS d);
        "#,
    )
    .expect_err("a derived table must not resolve a name from the enclosing query");

    let message = error.to_string();
    assert!(
        message.contains("'o'"),
        "the error should name the unresolvable qualifier, got: {message}"
    );

    // The same shape stays legal one level up: a scalar subquery *is* allowed to
    // correlate, so only the derived-table boundary changed.
    let correlated = last_query(
        r#"
        CREATE TABLE t (v INT);
        CREATE TABLE u (w INT);
        INSERT INTO t VALUES (1), (2);
        INSERT INTO u VALUES (1);
        SELECT o.v FROM t AS o WHERE o.v = (SELECT u.w FROM u WHERE u.w = o.v);
        "#,
    );
    assert_eq!(correlated.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn double_quoted_identifier_resolves_to_the_column_value() {
    let query = last_query(
        r#"
        CREATE TABLE t (s TEXT);
        INSERT INTO t VALUES ('hello world');
        SELECT "s" FROM t;
        "#,
    );

    assert_eq!(query.rows, vec![vec![SqlValue::Text("hello world".into())]]);
}

/// PostgreSQL-style identifiers fold only when they are unquoted: a delimited
/// identifier keeps its case while bare spellings resolve as lowercase.
#[test]
fn quoted_identifiers_preserve_case_while_unquoted_identifiers_fold() {
    let query = last_query(
        r#"
        CREATE TABLE t ("Col" INT, PLAIN INT);
        INSERT INTO t ("Col", PLAIN) VALUES (10, 20);
        SELECT "Col", plain, PLAIN FROM t;
        "#,
    );
    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Integer(10),
            SqlValue::Integer(20),
            SqlValue::Integer(20),
        ]]
    );

    let err = execute_sql(
        r#"
        CREATE TABLE t ("Col" INT);
        INSERT INTO t ("Col") VALUES (10);
        SELECT col FROM t;
        "#,
    )
    .expect_err("unquoted col must not resolve the case-sensitive quoted column");
    assert!(
        err.to_string().contains("ALOPEX-C003"),
        "expected C003 for an unquoted case mismatch, got: {err}"
    );
}

/// Error positions must point into the SQL the caller wrote. Quoted identifiers
/// are normalised before parsing, and dropping the quote characters shifted
/// every later column by two per identifier, so diagnostics pointed at the
/// wrong place in exactly the queries that use quoting.
#[test]
fn error_spans_survive_quoted_identifier_normalisation() {
    let sql = "SELECT \"Quoted\", missing FROM t";
    let column = sql.find("missing").expect("locate the offending column") + 1;

    let statements =
        Parser::parse_sql(&AlopexDialect, "CREATE TABLE t (\"Quoted\" INT)").expect("parse create");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    for statement in statements {
        let guard = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*guard).plan(&statement).expect("plan create");
        drop(guard);
        executor.execute(plan).expect("execute create");
    }

    let statement = Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse select")
        .remove(0);
    let guard = catalog.read().expect("catalog lock");
    let error = Planner::new(&*guard)
        .plan(&statement)
        .expect_err("unknown column must fail");

    let rendered = error.to_string();
    assert!(
        rendered.contains(&format!("column {column}")),
        "error should point at column {column} of the original SQL: {rendered}"
    );
}

/// Scope resolution has to hold at more than one level. A three-level query
/// exercises the inner-first rule twice, and the middle level must not become a
/// candidate for a name the innermost one already defines.
#[test]
fn three_level_nesting_resolves_each_name_in_its_own_scope() {
    let rows = last_query(
        "
        CREATE TABLE outer_t (id INT PRIMARY KEY, v INT);
        CREATE TABLE mid_t (id INT PRIMARY KEY, v INT);
        CREATE TABLE inner_t (id INT PRIMARY KEY, v INT);
        INSERT INTO outer_t (id, v) VALUES (1, 10), (2, 20);
        INSERT INTO mid_t (id, v) VALUES (1, 5);
        INSERT INTO inner_t (id, v) VALUES (1, 1);
        SELECT id FROM outer_t
         WHERE v > (SELECT MAX(v) FROM mid_t
                     WHERE v > (SELECT MAX(v) FROM inner_t))
         ORDER BY id;
        ",
    )
    .rows;
    // MAX(inner_t.v) = 1, so the middle level yields MAX(mid_t.v) = 5 and both
    // outer rows exceed it. Binding v to an enclosing scope would change this.
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
}

/// EXISTS introduces a scope like any other subquery: a name defined inside it
/// shadows the outer one, and only a qualified reference reaches outward.
#[test]
fn exists_subquery_shadows_the_outer_name_it_redefines() {
    let rows = last_query(
        "
        CREATE TABLE lhs (id INT PRIMARY KEY, tag TEXT);
        CREATE TABLE rhs (id INT PRIMARY KEY, tag TEXT);
        INSERT INTO lhs (id, tag) VALUES (1, 'keep'), (2, 'drop');
        INSERT INTO rhs (id, tag) VALUES (9, 'keep');
        SELECT id FROM lhs
         WHERE EXISTS (SELECT 1 FROM rhs WHERE tag = lhs.tag)
         ORDER BY id;
        ",
    )
    .rows;
    // Unqualified tag inside EXISTS is rhs.tag, so only the row whose tag the
    // right side also carries survives. Resolving it to lhs.tag would keep both.
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
}

/// A name that genuinely appears in more than one visible relation must be
/// rejected rather than bound to whichever one comes first.
#[test]
fn a_genuinely_ambiguous_name_is_rejected() {
    let error = execute_sql(
        "
        CREATE TABLE left_t (shared INT PRIMARY KEY, l TEXT);
        CREATE TABLE right_t (shared INT PRIMARY KEY, r TEXT);
        SELECT shared FROM left_t JOIN right_t ON left_t.shared = right_t.shared;
        ",
    )
    .expect_err("shared is ambiguous across both inputs");
    let rendered = error.to_string();
    assert!(
        rendered.contains("ambiguous"),
        "expected an ambiguity error, got: {rendered}"
    );
}
