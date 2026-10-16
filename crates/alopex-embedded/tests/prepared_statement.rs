use std::sync::Arc;
use std::time::Instant;

use alopex_embedded::{Database, Error};
use alopex_sql::{ExecutionResult, SqlValue};

#[test]
fn empty_binding_is_a_noop_for_parameter_free_batches() {
    let sql = "CREATE TABLE t (id INTEGER); SHOW TABLES";
    assert_eq!(alopex_embedded::bind_sql_parameters(sql, &[]).unwrap(), sql);
}

#[test]
fn prepared_statement_supports_null_rebind_reset_and_finalize() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, note) VALUES (?, ?)")
        .unwrap();
    assert_eq!(statement.parameter_count(), 2);

    statement.bind(1, SqlValue::Integer(1)).unwrap();
    statement.bind(2, SqlValue::Text("first".into())).unwrap();
    statement.bind(2, SqlValue::Text("rebound".into())).unwrap();
    statement.execute().unwrap();

    statement.reset().unwrap();
    statement.bind(1, SqlValue::Integer(2)).unwrap();
    statement.bind(2, SqlValue::Null).unwrap();
    statement.execute().unwrap();
    statement.finalize().unwrap();
    assert!(matches!(
        statement.bind(1, SqlValue::Integer(3)),
        Err(Error::PreparedStatementFinalized)
    ));
    assert!(matches!(
        statement.execute(),
        Err(Error::PreparedStatementFinalized)
    ));

    let ExecutionResult::Query(rows) = database
        .execute_sql("SELECT id, note FROM items ORDER BY id")
        .unwrap()
    else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0][1], SqlValue::Text("rebound".into()));
    assert_eq!(rows.rows[1][1], SqlValue::Null);
}

#[test]
fn session_prepared_statement_uses_the_active_transaction() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut session = database.sql_session();
    session.execute_sql("BEGIN").unwrap();
    {
        let mut statement = session
            .prepare("INSERT INTO items (id) VALUES (?)")
            .unwrap();
        statement.bind(1, SqlValue::Integer(1)).unwrap();
        statement.execute().unwrap();
    }
    session.execute_sql("ROLLBACK").unwrap();

    let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap() else {
        panic!("SELECT must return rows");
    };
    assert!(rows.rows.is_empty());
}

#[test]
fn prepared_parameters_work_in_limit_offset_and_fetch_expressions() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (1), (2), (3)")
        .unwrap();
    let mut limit = database
        .prepare("SELECT id FROM items ORDER BY id LIMIT ? OFFSET ?")
        .unwrap();
    limit.bind(1, SqlValue::Integer(1)).unwrap();
    limit.bind(2, SqlValue::Integer(1)).unwrap();
    let ExecutionResult::Query(rows) = limit.execute().unwrap() else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(2)]]);

    let mut fetch = database
        .prepare("SELECT id FROM items ORDER BY id FETCH FIRST ? ROWS ONLY")
        .unwrap();
    fetch.bind(1, SqlValue::Integer(2)).unwrap();
    let ExecutionResult::Query(rows) = fetch.execute().unwrap() else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn prepared_vector_batch_does_not_expand_into_parser_payload() {
    const ROWS: usize = 400;
    const DIMENSIONS: usize = 128;

    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(128, L2))")
        .unwrap();
    let vector_literal = std::iter::repeat_n("0.25", DIMENSIONS)
        .collect::<Vec<_>>()
        .join(", ");
    let literal_sql = format!(
        "INSERT INTO items (id, embedding) VALUES {}",
        (0..ROWS)
            .map(|row| format!("({row}, [{vector_literal}])"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        literal_sql.len() < 1_048_576,
        "literal SQL stays below the input limit"
    );
    let error = database.execute_sql(&literal_sql).unwrap_err();
    assert!(
        matches!(&error, Error::Sql(_))
            && error
                .to_string()
                .contains("MessagePack collection limit of 65536 values exceeded"),
        "literal batch must report its actual MessagePack collection constraint: {error}"
    );

    let sql = format!(
        "INSERT INTO items (id, embedding) VALUES {}",
        std::iter::repeat_n("(?, ?)", ROWS)
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        sql.len() < 1_048_576,
        "prepared SQL stays below the input limit"
    );

    let mut statement = database.prepare(&sql).unwrap();
    for row in 0..ROWS {
        statement
            .bind(row * 2 + 1, SqlValue::Integer(row as i32))
            .unwrap();
        statement
            .bind(row * 2 + 2, SqlValue::Vector(vec![0.25; DIMENSIONS]))
            .unwrap();
    }
    statement.execute().unwrap();

    let ExecutionResult::Query(rows) = database.execute_sql("SELECT COUNT(*) FROM items").unwrap()
    else {
        panic!("COUNT must return rows");
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::BigInt(ROWS as i64)]]);
}

#[test]
fn prepared_native_vector_rejects_non_finite_values() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (embedding VECTOR(1, L2))")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (embedding) VALUES (?)")
        .unwrap();
    statement.bind(1, SqlValue::Vector(vec![f32::NAN])).unwrap();

    assert!(matches!(
        statement.execute(),
        Err(Error::UnsupportedPreparedParameterType)
    ));
}

#[test]
fn prepared_fallback_literals_preserve_float_and_text_contracts() {
    assert_eq!(
        alopex_embedded::render_prepared_parameter(&SqlValue::Double(2.0)).unwrap(),
        "2.0"
    );
    assert_eq!(
        alopex_embedded::render_prepared_parameter(&SqlValue::Text("O'Brien".into())).unwrap(),
        "'O''Brien'"
    );
    assert_eq!(
        alopex_embedded::render_prepared_parameter(&SqlValue::Vector(vec![2.0])).unwrap(),
        "[2.0]"
    );
}

#[test]
fn prepared_execute_many_commits_once_and_rolls_back_on_error() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id) VALUES (?)")
        .unwrap();

    assert!(statement
        .execute_many(vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(1)]])
        .is_err());
    let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap() else {
        panic!("SELECT must return rows");
    };
    assert!(rows.rows.is_empty());

    let results = statement
        .execute_many(vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]])
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn prepared_execute_many_invalidates_catalog_cache_for_parameter_free_ddl() {
    let database = Arc::new(Database::new());
    let epoch = database.table_info_cache_epoch();
    let mut statement = database
        .prepare("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();

    statement
        .execute_many(vec![Vec::<SqlValue>::new()])
        .unwrap();

    assert_eq!(database.table_info_cache_epoch(), epoch + 1);
}

#[test]
fn prepared_execute_many_commits_hnsw_batch_once() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2))")
        .unwrap();
    database
        .execute_sql("CREATE INDEX items_embedding_hnsw ON items (embedding) USING HNSW")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, embedding) VALUES (?, ?)")
        .unwrap();

    let results = statement
        .execute_many(vec![
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![1.0, 0.0])],
            vec![SqlValue::Integer(2), SqlValue::Vector(vec![0.0, 1.0])],
        ])
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_row_count(&database, 2);
    assert_eq!(
        database
            .get_hnsw_stats("items_embedding_hnsw")
            .unwrap()
            .node_count,
        2
    );
}

#[test]
fn prepared_execute_many_rolls_back_hnsw_batch_on_error() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2, L2))")
        .unwrap();
    database
        .execute_sql("CREATE INDEX items_embedding_hnsw ON items (embedding) USING HNSW")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, embedding) VALUES (?, ?)")
        .unwrap();

    assert!(statement
        .execute_many(vec![
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![1.0, 0.0])],
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![0.0, 1.0])],
        ])
        .is_err());
    assert_row_count(&database, 0);
    assert_eq!(
        database
            .get_hnsw_stats("items_embedding_hnsw")
            .unwrap()
            .node_count,
        0
    );

    statement
        .execute_many(vec![
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![1.0, 0.0])],
            vec![SqlValue::Integer(2), SqlValue::Vector(vec![0.0, 1.0])],
        ])
        .unwrap();
    assert_eq!(
        database
            .get_hnsw_stats("items_embedding_hnsw")
            .unwrap()
            .node_count,
        2
    );
}

#[test]
#[ignore = "run by parity-performance to publish issue #463 evidence"]
fn prepared_execute_many_hnsw_throughput_matches_literal_batch() {
    const ROWS: usize = 2_000;
    const DIMENSIONS: usize = 128;
    const LITERAL_BATCH_ROWS: usize = 200;
    const RUNS: usize = 3;

    let rows = (0..ROWS)
        .map(|row| {
            (
                row as i32,
                (0..DIMENSIONS)
                    .map(|dimension| ((row + dimension) % 997) as f32 / 997.0)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let literal_batches = rows
        .chunks(LITERAL_BATCH_ROWS)
        .map(|batch| {
            let values = batch
                .iter()
                .map(|(id, vector)| {
                    let vector = vector
                        .iter()
                        .map(|value| value.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("({id},[{vector}])")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("INSERT INTO items (id, embedding) VALUES {values}")
        })
        .collect::<Vec<_>>();
    let prepared_rows = rows
        .iter()
        .map(|(id, vector)| vec![SqlValue::Integer(*id), SqlValue::Vector(vector.clone())])
        .collect::<Vec<_>>();

    let mut literal = Vec::with_capacity(RUNS);
    let mut prepared = Vec::with_capacity(RUNS);
    for run in 0..RUNS {
        let literal_first = run % 2 == 0;
        if literal_first {
            literal.push(record_literal_run(run, &literal_batches, ROWS));
            prepared.push(record_prepared_run(run, &prepared_rows, ROWS));
        } else {
            prepared.push(record_prepared_run(run, &prepared_rows, ROWS));
            literal.push(record_literal_run(run, &literal_batches, ROWS));
        }
    }
    let literal_median = median(literal);
    let prepared_median = median(prepared);
    let ratio = prepared_median / literal_median;
    eprintln!(
        "prepared_median_rows_per_second={prepared_median:.3} literal_median_rows_per_second={literal_median:.3} ratio={ratio:.3}"
    );

    assert!(
        ratio >= 1.0,
        "prepared execute_many must match literal batch throughput: {ratio:.3}"
    );
}

fn record_literal_run(run: usize, literal_batches: &[String], expected_rows: usize) -> f64 {
    let rows_per_second = measure_literal_hnsw_insert(literal_batches, expected_rows);
    eprintln!("literal run={run} rows_per_second={rows_per_second:.3}");
    rows_per_second
}

fn record_prepared_run(run: usize, rows: &[Vec<SqlValue>], expected_rows: usize) -> f64 {
    let rows_per_second = measure_prepared_hnsw_insert(rows, expected_rows);
    eprintln!("prepared run={run} rows_per_second={rows_per_second:.3}");
    rows_per_second
}

fn measure_literal_hnsw_insert(literal_batches: &[String], expected_rows: usize) -> f64 {
    let database = hnsw_insert_database();
    let start = Instant::now();
    let mut session = database.sql_session();
    session.execute_sql("BEGIN").unwrap();
    for sql in literal_batches {
        session.execute_sql(sql).unwrap();
    }
    session.execute_sql("COMMIT").unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    assert_row_count(&database, expected_rows);
    expected_rows as f64 / elapsed
}

fn measure_prepared_hnsw_insert(rows: &[Vec<SqlValue>], expected_rows: usize) -> f64 {
    let database = hnsw_insert_database();
    let start = Instant::now();
    let mut statement = database
        .prepare("INSERT INTO items (id, embedding) VALUES (?, ?)")
        .unwrap();
    let results = statement
        .execute_many(rows.iter().map(Vec::as_slice))
        .unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(results.len(), expected_rows);
    assert_row_count(&database, expected_rows);
    expected_rows as f64 / elapsed
}

fn hnsw_insert_database() -> Arc<Database> {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(128, L2))")
        .unwrap();
    database
        .execute_sql(
            "CREATE INDEX items_embedding_hnsw ON items (embedding) USING HNSW \
             WITH (m = 16, ef_construction = 200)",
        )
        .unwrap();
    database
}

fn assert_row_count(database: &Database, expected_rows: usize) {
    let ExecutionResult::Query(rows) = database.execute_sql("SELECT COUNT(*) FROM items").unwrap()
    else {
        panic!("COUNT must return rows");
    };
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::BigInt(expected_rows as i64)]]
    );
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[test]
fn prepared_primary_key_query_and_update_preserve_exact_row_semantics() {
    let database = Arc::new(Database::new());
    database
        .execute_sql(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, note TEXT); \
             INSERT INTO items VALUES (0, 'zero'), (1, 'one'), (2, 'two')",
        )
        .unwrap();

    let mut statement = database
        .prepare("SELECT note FROM items WHERE id = ?")
        .unwrap();
    statement.bind(1, SqlValue::Integer(1)).unwrap();
    let ExecutionResult::Query(rows) = statement.execute().unwrap() else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::Text("one".into())]]);

    database
        .execute_sql("UPDATE items SET note = 'updated' WHERE id = 0")
        .unwrap();
    let ExecutionResult::Query(rows) = database
        .execute_sql("SELECT id, note FROM items WHERE id >= 0 ORDER BY id LIMIT 2")
        .unwrap()
    else {
        panic!("SELECT must return rows");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::BigInt(0), SqlValue::Text("updated".into())],
            vec![SqlValue::BigInt(1), SqlValue::Text("one".into())],
        ]
    );

    let ExecutionResult::Query(rows) = database
        .execute_sql("SELECT id, note FROM items ORDER BY id")
        .unwrap()
    else {
        panic!("SELECT must return rows");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::BigInt(0), SqlValue::Text("updated".into())],
            vec![SqlValue::BigInt(1), SqlValue::Text("one".into())],
            vec![SqlValue::BigInt(2), SqlValue::Text("two".into())],
        ]
    );

    database
        .execute_sql("CREATE TABLE narrow (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO narrow VALUES (1, 'one')")
        .unwrap();
    let ExecutionResult::Query(rows) = database
        .execute_sql("SELECT note FROM narrow WHERE id = 2147483648")
        .unwrap()
    else {
        panic!("SELECT must return rows");
    };
    assert!(rows.rows.is_empty());
}

#[test]
fn prepared_statement_rejects_missing_and_non_value_parameters() {
    let database = Arc::new(Database::new());
    assert!(database.execute_sql("SELECT ?").is_err());
    assert_eq!(
        database
            .prepare("SELECT '?' AS literal /* ? */")
            .unwrap()
            .parameter_count(),
        0
    );
    assert_eq!(
        database
            .prepare("SELECT ? -- ?\n")
            .unwrap()
            .parameter_count(),
        1
    );
    let mut statement = database.prepare("SELECT ?").unwrap();
    assert!(matches!(
        statement.execute(),
        Err(Error::PreparedParameterUnbound(1))
    ));
    assert!(matches!(
        statement.bind(0, SqlValue::Integer(1)),
        Err(Error::PreparedParameterOutOfRange { index: 0, count: 1 })
    ));
    assert!(matches!(
        statement.bind(2, SqlValue::Integer(1)),
        Err(Error::PreparedParameterOutOfRange { index: 2, count: 1 })
    ));
    assert!(database.prepare("SELECT $1").is_err());
    assert!(database.prepare("SELECT :named").is_err());
    assert!(database.prepare("SELECT * FROM ?").is_err());
}

#[test]
fn prepared_statement_reparses_after_schema_change_and_can_retry() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id) VALUES (?)")
        .unwrap();
    database.execute_sql("DROP TABLE items").unwrap();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        .unwrap();
    statement.bind(1, SqlValue::Integer(1)).unwrap();
    assert!(statement.execute().is_err());

    database.execute_sql("DROP TABLE items").unwrap();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    statement.execute().unwrap();
}

#[test]
fn prepared_statement_is_send_and_text_binding_cannot_change_sql_structure() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, note) VALUES (?, ?)")
        .unwrap();
    statement.bind(1, SqlValue::Integer(1)).unwrap();
    statement
        .bind(2, SqlValue::Text("x'); DROP TABLE items; --".into()))
        .unwrap();
    std::thread::spawn(move || statement.execute())
        .join()
        .expect("prepared statement thread")
        .unwrap();
    assert!(database.execute_sql("SELECT id FROM items").is_ok());
}
