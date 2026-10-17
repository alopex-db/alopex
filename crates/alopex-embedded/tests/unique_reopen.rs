use alopex_embedded::Database;
use alopex_sql::{storage::SqlValue, ExecutionResult};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn unique_constraints_and_indexes_survive_file_database_reopen() {
    for (kind, definition, index, conflict_target) in [
        ("column", "code TEXT UNIQUE, region INT", "", "code"),
        (
            "composite",
            "code TEXT, region INT, UNIQUE(code, region)",
            "",
            "code, region",
        ),
        (
            "index",
            "code TEXT, region INT",
            "CREATE UNIQUE INDEX items_code_key ON items(code)",
            "code",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unique.alopex");
        let db = Database::open(&path).unwrap();
        db.execute_sql(&format!(
            "CREATE TABLE items (id INT PRIMARY KEY, {definition})"
        ))
        .unwrap();
        db.execute_sql(
            "INSERT INTO items VALUES (1, 'a', 1), (2, 'b', 2), \
             (3, NULL, 1), (4, NULL, 1)",
        )
        .unwrap();
        if !index.is_empty() {
            db.execute_sql(index).unwrap();
        }
        db.close().unwrap();
        drop(db);

        // Reopen the actual disk database twice, including writes made after
        // its first reopen. A MemoryKV catalog reload cannot cover this path.
        for reopen in 0..2 {
            let db = Database::open(&path).unwrap();
            for sql in [
                "INSERT INTO items VALUES (99, 'a', 1)",
                "UPDATE items SET code = 'a', region = 1 WHERE id = 2",
            ] {
                let error = db.execute_sql(sql).expect_err("duplicate must fail");
                assert!(
                    error.to_string().to_lowercase().contains("unique"),
                    "{kind}, reopen {reopen}, {sql}: {error}"
                );
            }
            for target in [String::new(), format!("({conflict_target})")] {
                assert_eq!(
                    db.execute_sql(&format!(
                        "INSERT INTO items VALUES (99, 'a', 1) \
                         ON CONFLICT {target} DO NOTHING"
                    ))
                    .unwrap(),
                    ExecutionResult::RowsAffected(0),
                    "{kind}, reopen {reopen}"
                );
            }
            let id = 5 + reopen;
            assert_eq!(
                db.execute_sql(&format!("INSERT INTO items VALUES ({id}, NULL, 1)"))
                    .unwrap(),
                ExecutionResult::RowsAffected(1)
            );
            let ExecutionResult::Query(result) = db
                .execute_sql("SELECT id, code, region FROM items ORDER BY id")
                .unwrap()
            else {
                panic!("expected rows");
            };
            let mut expected = vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Text("a".into()),
                    SqlValue::Integer(1),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Text("b".into()),
                    SqlValue::Integer(2),
                ],
            ];
            expected.extend(
                (3..=id)
                    .map(|id| vec![SqlValue::Integer(id), SqlValue::Null, SqlValue::Integer(1)]),
            );
            assert_eq!(result.rows, expected, "{kind}, reopen {reopen}");
            db.close().unwrap();
            drop(db);
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn unique_conflict_updates_and_nullable_keys_survive_reopen() {
    for (kind, definition, index, target, composite) in [
        (
            "column",
            "code TEXT UNIQUE, region INT",
            "",
            "(code)",
            false,
        ),
        (
            "composite",
            "code TEXT, region INT, UNIQUE(code, region)",
            "",
            "(code, region)",
            true,
        ),
        (
            "index",
            "code TEXT, region INT",
            "CREATE UNIQUE INDEX items_key ON items(code)",
            "(code)",
            false,
        ),
        (
            "named_unique",
            "code TEXT, region INT, CONSTRAINT item_key UNIQUE(code, region)",
            "",
            "ON CONSTRAINT item_key",
            true,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("upsert.alopex");
        let db = Database::open(&path).unwrap();
        db.execute_sql(&format!(
            "CREATE TABLE items (id INT PRIMARY KEY, qty INT, {definition})"
        ))
        .unwrap();
        db.execute_sql(
            "INSERT INTO items (id, qty, code, region) VALUES (1, 10, 'a', 1), (2, 20, 'b', 2)",
        )
        .unwrap();
        if !index.is_empty() {
            db.execute_sql(index).unwrap();
        }
        db.close().unwrap();
        drop(db);
        let mut null_rows = Vec::new();
        for reopen in 0..2 {
            let db = Database::open(&path).unwrap();
            let ExecutionResult::Query(before) = db
                .execute_sql("SELECT id, qty FROM items WHERE id <= 2 ORDER BY id")
                .unwrap()
            else {
                panic!("expected persisted updates");
            };
            let previous = if reopen == 0 {
                [10, 20]
            } else {
                [29 + reopen; 2]
            };
            assert_eq!(
                before.rows,
                vec![
                    vec![SqlValue::Integer(1), SqlValue::Integer(previous[0])],
                    vec![SqlValue::Integer(2), SqlValue::Integer(previous[1])],
                ],
                "{kind} {reopen}: committed updates must survive reopening"
            );
            let qty = 30 + reopen;
            assert_eq!(db.execute_sql(&format!(
                "INSERT INTO items (id, qty, code, region) VALUES (99, {qty}, 'a', 1) ON CONFLICT {target} DO UPDATE SET qty=EXCLUDED.qty"
            )).unwrap(), ExecutionResult::RowsAffected(1), "{kind} {reopen}");
            // Preserve Alopex's documented targetless DO UPDATE extension:
            // it selects only the primary key, even when other keys exist.
            assert_eq!(db.execute_sql(&format!(
                "INSERT INTO items (id, qty, code, region) VALUES (2, {qty}, 'unused', 8) ON CONFLICT DO UPDATE SET qty=EXCLUDED.qty"
            )).unwrap(), ExecutionResult::RowsAffected(1), "{kind} {reopen}");
            for sql in [
                "INSERT INTO items (id, qty, code, region) VALUES (99, 999, 'a', 1) ON CONFLICT DO UPDATE SET qty=EXCLUDED.qty".to_owned(),
                format!("INSERT INTO items (id, qty, code, region) VALUES (99, 999, 'a', 1) ON CONFLICT {target} DO UPDATE SET code='b', region=2"),
            ] {
                let error = db.execute_sql(&sql).expect_err("non-target or updated UNIQUE conflict must fail");
                assert!(error.to_string().to_lowercase().contains("unique"), "{kind} {reopen}: {error}");
            }
            let first_id = 3 + reopen * 4;
            for offset in 0..if composite { 4 } else { 2 } {
                let id = first_id + offset;
                let (code, region) = if offset < 2 {
                    ("NULL", "1")
                } else {
                    ("'same'", "NULL")
                };
                db.execute_sql(&format!("INSERT INTO items (id, qty, code, region) VALUES ({id}, 0, {code}, {region}) ON CONFLICT {target} DO NOTHING")).unwrap();
                null_rows.push(vec![
                    SqlValue::Integer(id),
                    SqlValue::Integer(0),
                    if offset < 2 {
                        SqlValue::Null
                    } else {
                        SqlValue::Text("same".into())
                    },
                    if offset < 2 {
                        SqlValue::Integer(1)
                    } else {
                        SqlValue::Null
                    },
                ]);
            }
            let ExecutionResult::Query(result) = db
                .execute_sql("SELECT id, qty, code, region FROM items ORDER BY id")
                .unwrap()
            else {
                panic!("expected rows");
            };
            let mut expected = vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(qty),
                    SqlValue::Text("a".into()),
                    SqlValue::Integer(1),
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Integer(qty),
                    SqlValue::Text("b".into()),
                    SqlValue::Integer(2),
                ],
            ];
            expected.extend(null_rows.clone());
            assert_eq!(result.rows, expected, "{kind} {reopen}");
            db.close().unwrap();
            drop(db);
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn failed_unique_index_is_unpublished_after_reopen_and_can_be_rebuilt() {
    let target = "code";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("failed-index.alopex");
    let db = Database::open(&path).unwrap();
    // The public catalog inspection API requires its hierarchy to exist;
    // SQL table creation alone does not initialize these catalog records.
    db.create_catalog(alopex_embedded::CreateCatalogRequest::new("default"))
        .unwrap();
    db.create_namespace(alopex_embedded::CreateNamespaceRequest::new(
        "default", "default",
    ))
    .unwrap();
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT, region INT)")
        .unwrap();
    db.execute_sql("INSERT INTO items VALUES (1, 'a', 1), (2, 'a', 1), (3, NULL, 1), (4, NULL, 1)")
        .unwrap();
    let create = format!("CREATE UNIQUE INDEX items_key ON items({target})");
    let error = db
        .execute_sql(&create)
        .expect_err("existing duplicates must reject the index");
    assert!(
        error.to_string().to_lowercase().contains("unique"),
        "{target}: {error}"
    );
    assert!(!db
        .list_indexes_simple("items")
        .unwrap()
        .iter()
        .any(|i| i.name == "items_key"));
    db.execute_sql("INSERT INTO items VALUES (5, 'a', 1)")
        .unwrap();
    db.close().unwrap();
    drop(db);

    let db = Database::open(&path).unwrap();
    assert!(!db
        .list_indexes_simple("items")
        .unwrap()
        .iter()
        .any(|i| i.name == "items_key"));
    let ExecutionResult::Query(result) =
        db.execute_sql("SELECT id FROM items ORDER BY id").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(
        result.rows,
        (1..=5)
            .map(|id| vec![SqlValue::Integer(id)])
            .collect::<Vec<_>>()
    );
    db.execute_sql("DELETE FROM items WHERE id=2 OR id=5")
        .unwrap();
    db.execute_sql(&create)
        .expect("rolled-back index name and entries must be reusable");
    db.close().unwrap();
    drop(db);

    let db = Database::open(&path).unwrap();
    let indexes = db.list_indexes_simple("items").unwrap();
    let index = indexes.iter().find(|i| i.name == "items_key").unwrap();
    assert!(index.is_unique);
    let error = db
        .execute_sql("INSERT INTO items VALUES (99, 'a', 1)")
        .expect_err("rebuilt index must enforce uniqueness after reopen");
    assert!(
        error.to_string().to_lowercase().contains("unique"),
        "{target}: {error}"
    );
    let ExecutionResult::Query(result) =
        db.execute_sql("SELECT id FROM items ORDER BY id").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(
        result.rows,
        [1, 3, 4]
            .into_iter()
            .map(|id| vec![SqlValue::Integer(id)])
            .collect::<Vec<_>>()
    );
    db.close().unwrap();
    drop(db);
}
