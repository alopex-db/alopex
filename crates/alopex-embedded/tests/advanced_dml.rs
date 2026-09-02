use alopex_embedded::Database;
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
        rows(&db, "INSERT INTO items VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET value = 'updated' RETURNING value"),
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
fn merge_rejects_multiple_source_matches_atomically() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE target (id BIGINT PRIMARY KEY, value TEXT);
         CREATE TABLE source (id BIGINT, value TEXT);
         INSERT INTO target VALUES (1, 'old');
         INSERT INTO source VALUES (1, 'a'), (1, 'b')",
    )
    .unwrap();
    assert!(db
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET value = source.value"
        )
        .is_err());
    assert_eq!(
        rows(&db, "SELECT value FROM target"),
        vec![vec![SqlValue::Text("old".into())]]
    );
}
