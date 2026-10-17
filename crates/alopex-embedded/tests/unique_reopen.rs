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
