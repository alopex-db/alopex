use std::sync::Arc;

use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};

fn rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = db.execute_sql(sql).unwrap() else {
        panic!("expected query result for {sql}");
    };
    result.rows
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn views_are_dynamic_persistent_and_dependency_restricted() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("views.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE items (id BIGINT, label TEXT); \
             CREATE TABLE shadow (id BIGINT); \
             INSERT INTO items VALUES (1, 'one'); \
             CREATE VIEW item_labels AS SELECT label FROM items; \
             CREATE VIEW nested_labels AS SELECT label FROM item_labels; \
             CREATE VIEW cte_labels AS \
                 WITH shadow AS (SELECT label FROM items) SELECT label FROM shadow",
        )
        .unwrap();
        db.execute_sql("DROP TABLE shadow").unwrap();
        db.execute_sql("INSERT INTO items VALUES (2, 'two')")
            .unwrap();
        assert_eq!(
            rows(&db, "SELECT label FROM nested_labels ORDER BY label"),
            vec![
                vec![SqlValue::Text("one".into())],
                vec![SqlValue::Text("two".into())]
            ]
        );
        assert!(db.execute_sql("DROP TABLE items").is_err());
        assert!(db.execute_sql("DROP VIEW IF EXISTS items").is_err());
        for sql in [
            "INSERT INTO item_labels VALUES ('hidden')",
            "UPDATE item_labels SET label = 'hidden'",
            "DELETE FROM item_labels",
            "CREATE INDEX hidden_idx ON item_labels (label)",
        ] {
            assert!(db.execute_sql(sql).is_err(), "views are read-only: {sql}");
        }
    }

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        rows(&reopened, "SELECT label FROM nested_labels ORDER BY label"),
        vec![
            vec![SqlValue::Text("one".into())],
            vec![SqlValue::Text("two".into())]
        ]
    );
    reopened
        .execute_sql(
            "DROP VIEW cte_labels; DROP VIEW nested_labels; DROP VIEW item_labels; DROP TABLE items",
        )
        .unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn alter_table_migrates_existing_rows_and_replans_prepared_sql() {
    let db = Arc::new(Database::new());
    db.execute_sql(
        "CREATE TABLE items (id BIGINT, label TEXT); \
         INSERT INTO items VALUES (1, 'one'), (2, 'two')",
    )
    .unwrap();
    let mut prepared = db.prepare("SELECT label FROM items ORDER BY id").unwrap();

    db.execute_sql(
        "ALTER TABLE items ADD COLUMN quantity BIGINT DEFAULT 3; \
         ALTER TABLE items RENAME COLUMN label TO name; \
         ALTER TABLE items ALTER COLUMN quantity TYPE TEXT",
    )
    .unwrap();

    assert!(
        prepared.execute().is_err(),
        "prepared SQL must be replanned"
    );
    assert_eq!(
        rows(&db, "SELECT id, name, quantity FROM items ORDER BY id"),
        vec![
            vec![
                SqlValue::BigInt(1),
                SqlValue::Text("one".into()),
                SqlValue::Text("3".into())
            ],
            vec![
                SqlValue::BigInt(2),
                SqlValue::Text("two".into()),
                SqlValue::Text("3".into())
            ]
        ]
    );

    db.execute_sql("ALTER TABLE items DROP COLUMN quantity")
        .unwrap();
    assert_eq!(rows(&db, "SELECT id, name FROM items ORDER BY id").len(), 2);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn alter_and_truncate_rollback_atomically() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE items (id BIGINT); INSERT INTO items VALUES (1), (2)")
        .unwrap();

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.execute_sql("ALTER TABLE items ADD COLUMN label TEXT DEFAULT 'pending'")
        .unwrap();
    txn.execute_sql("TRUNCATE TABLE items").unwrap();
    txn.rollback().unwrap();

    assert_eq!(
        rows(&db, "SELECT id FROM items ORDER BY id"),
        vec![vec![SqlValue::BigInt(1)], vec![SqlValue::BigInt(2)]]
    );
    assert!(db.execute_sql("SELECT label FROM items").is_err());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn schema_evolution_survives_reopen_without_reusing_stale_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alter.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE items (id BIGINT, obsolete TEXT); \
             INSERT INTO items VALUES (7, 'remove'); \
             ALTER TABLE items DROP COLUMN obsolete; \
             ALTER TABLE items RENAME TO renamed",
        )
        .unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        rows(&reopened, "SELECT id FROM renamed"),
        vec![vec![SqlValue::BigInt(7)]]
    );
    reopened.execute_sql("TRUNCATE renamed").unwrap();
    assert!(rows(&reopened, "SELECT id FROM renamed").is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn alter_column_constraints_and_defaults_validate_existing_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE items (id BIGINT, label TEXT); \
         INSERT INTO items VALUES (1, NULL)",
    )
    .unwrap();
    assert!(db
        .execute_sql("ALTER TABLE items ALTER COLUMN label SET NOT NULL")
        .is_err());
    db.execute_sql(
        "UPDATE items SET label = 'one'; \
         ALTER TABLE items ALTER COLUMN label SET NOT NULL; \
         ALTER TABLE items ALTER COLUMN label SET DEFAULT 'new'; \
         INSERT INTO items (id) VALUES (2); \
         ALTER TABLE items ALTER COLUMN label DROP DEFAULT; \
         ALTER TABLE items ALTER COLUMN label DROP NOT NULL; \
         INSERT INTO items (id) VALUES (3); \
         ALTER TABLE IF EXISTS missing ADD COLUMN ignored BIGINT",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT id, label FROM items ORDER BY id"),
        vec![
            vec![SqlValue::BigInt(1), SqlValue::Text("one".into())],
            vec![SqlValue::BigInt(2), SqlValue::Text("new".into())],
            vec![SqlValue::BigInt(3), SqlValue::Null],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn concurrent_schema_transactions_publish_only_the_winner() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE items (id BIGINT); INSERT INTO items VALUES (1)")
        .unwrap();
    let mut alter = db.begin(TxnMode::ReadWrite).unwrap();
    let mut truncate = db.begin(TxnMode::ReadWrite).unwrap();
    alter
        .execute_sql("ALTER TABLE items ADD COLUMN label TEXT DEFAULT 'pending'")
        .unwrap();
    truncate.execute_sql("TRUNCATE items").unwrap();
    truncate.commit().unwrap();
    assert!(alter.commit().is_err());
    assert!(rows(&db, "SELECT id FROM items").is_empty());
    assert!(db.execute_sql("SELECT label FROM items").is_err());
}
