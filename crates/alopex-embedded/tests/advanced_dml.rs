use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};

fn rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = db.execute_sql(sql).unwrap() else {
        panic!("expected rows from {sql}");
    };
    result.rows
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn returning_reports_final_insert_update_and_delete_rows() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "INSERT INTO items VALUES (1, 'a'), (2, 'b') RETURNING id, value"
        ),
        vec![
            vec![SqlValue::BigInt(1), SqlValue::Text("a".into())],
            vec![SqlValue::BigInt(2), SqlValue::Text("b".into())],
        ]
    );
    assert_eq!(
        rows(
            &db,
            "UPDATE items SET value = 'c' WHERE id = 2 RETURNING id, value"
        ),
        vec![vec![SqlValue::BigInt(2), SqlValue::Text("c".into())]]
    );
    assert_eq!(
        rows(&db, "DELETE FROM items WHERE id = 1 RETURNING *"),
        vec![vec![SqlValue::BigInt(1), SqlValue::Text("a".into())]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn on_conflict_and_joined_dml_reuse_existing_row_semantics() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT);
         CREATE TABLE incoming (id BIGINT, value TEXT);
         INSERT INTO items VALUES (1, 'old');
         INSERT INTO incoming VALUES (1, 'new'), (2, 'second')",
    )
    .unwrap();
    assert!(rows(
        &db,
        "INSERT INTO items VALUES (1, 'ignored') ON CONFLICT DO NOTHING RETURNING *"
    )
    .is_empty());
    assert_eq!(
        rows(
            &db,
            "INSERT INTO items VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET value = 'updated' RETURNING value"
        ),
        vec![vec![SqlValue::Text("updated".into())]]
    );
    db.execute_sql(
        "UPDATE items SET value = incoming.value FROM incoming WHERE items.id = incoming.id",
    )
    .unwrap();
    db.execute_sql(
        "DELETE FROM items USING incoming WHERE items.id = incoming.id AND incoming.id = 2",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT id, value FROM items"),
        vec![vec![SqlValue::BigInt(1), SqlValue::Text("new".into())]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_matched_only_updates_through_public_database_api() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE dml_target (id INTEGER PRIMARY KEY, value TEXT);
         CREATE TABLE dml_source (id INTEGER, value TEXT);
         INSERT INTO dml_target VALUES (1, 'old');
         INSERT INTO dml_source VALUES (1, 'updated')",
    )
    .unwrap();

    db.execute_sql(
        "MERGE INTO dml_target USING dml_source ON dml_target.id = dml_source.id
         WHEN MATCHED THEN UPDATE SET value = dml_source.value",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT id, value FROM dml_target ORDER BY id"),
        vec![vec![SqlValue::Integer(1), SqlValue::Text("updated".into()),]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_updates_matches_and_inserts_non_matches() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE target (id BIGINT PRIMARY KEY, value TEXT);
         CREATE TABLE source (id BIGINT, value TEXT);
         INSERT INTO target VALUES (1, 'old');
         INSERT INTO source VALUES (1, 'updated'), (2, 'inserted')",
    )
    .unwrap();

    assert_eq!(
        db.execute_sql(
            "MERGE INTO target USING source ON target.id = source.id
             WHEN MATCHED THEN UPDATE SET value = source.value
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)"
        )
        .unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    assert_eq!(
        rows(&db, "SELECT id, value FROM target ORDER BY id"),
        vec![
            vec![SqlValue::BigInt(1), SqlValue::Text("updated".into())],
            vec![SqlValue::BigInt(2), SqlValue::Text("inserted".into())],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn merge_rejects_multiple_source_matches_atomically() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE target (id BIGINT PRIMARY KEY, value TEXT);
         CREATE TABLE source (id BIGINT, value TEXT);
         INSERT INTO target VALUES (1, 'old');
         INSERT INTO source VALUES (1, 'a'), (1, 'b')",
    )
    .unwrap();
    let error = db
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET value = source.value",
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("target row matched more than once"),
        "unexpected MERGE error: {error}"
    );
    assert_eq!(
        rows(&db, "SELECT value FROM target"),
        vec![vec![SqlValue::Text("old".into())]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn concurrent_advanced_dml_conflict_does_not_publish_stale_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT);
         INSERT INTO items VALUES (1, 'old')",
    )
    .unwrap();
    let mut first = db.begin(TxnMode::ReadWrite).unwrap();
    let mut stale = db.begin(TxnMode::ReadWrite).unwrap();
    first
        .execute_sql("UPDATE items SET value = 'first' WHERE id = 1")
        .unwrap();
    stale
        .execute_sql("UPDATE items SET value = 'stale' WHERE id = 1")
        .unwrap();

    first.commit().unwrap();
    assert!(stale.commit().is_err());
    assert_eq!(
        rows(&db, "SELECT value FROM items"),
        vec![vec![SqlValue::Text("first".into())]]
    );

    db.execute_sql("UPDATE items SET value = 'retry' WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT value FROM items"),
        vec![vec![SqlValue::Text("retry".into())]]
    );
}
