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
fn drop_column_preserves_btree_results_and_writes_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-index.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE dc (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER); \
             CREATE TABLE scan_dc (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER); \
             CREATE INDEX dc_b ON dc (b); \
             INSERT INTO dc VALUES (1, 0, 10, 20); \
             INSERT INTO scan_dc VALUES (1, 0, 10, 20); \
             ALTER TABLE dc DROP COLUMN a; \
             ALTER TABLE scan_dc DROP COLUMN a",
        )
        .unwrap();

        let expected = rows(&db, "SELECT * FROM scan_dc WHERE c = 20");
        assert_eq!(
            expected,
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Integer(10),
                SqlValue::Integer(20)
            ]]
        );
        assert_eq!(rows(&db, "SELECT * FROM dc WHERE c = 20"), expected);
        assert_eq!(
            rows(&db, "SELECT * FROM dc WHERE b = 10"),
            rows(&db, "SELECT * FROM scan_dc WHERE b = 10")
        );

        for table in ["dc", "scan_dc"] {
            db.execute_sql(&format!(
                "INSERT INTO {table} VALUES (2, 30, 40); \
                 UPDATE {table} SET b = 11 WHERE id = 1; \
                 DELETE FROM {table} WHERE b = 30"
            ))
            .unwrap();
        }
        assert_eq!(
            rows(&db, "SELECT * FROM dc ORDER BY id"),
            rows(&db, "SELECT * FROM scan_dc ORDER BY id")
        );
    }

    let db = Database::open(&path).unwrap();
    for predicate in ["b = 11", "b = 30", "c = 20", "c = 40"] {
        assert_eq!(
            rows(&db, &format!("SELECT * FROM dc WHERE {predicate}")),
            rows(&db, &format!("SELECT * FROM scan_dc WHERE {predicate}")),
            "reopened index must agree with scan for {predicate}"
        );
    }
    db.execute_sql("INSERT INTO dc VALUES (3, 50, 60)").unwrap();
    assert_eq!(rows(&db, "SELECT id FROM dc WHERE b = 50").len(), 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_column_preserves_primary_key_and_unique_constraints() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-constraints.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE dc2 (a INTEGER, id INTEGER PRIMARY KEY, v TEXT); \
             INSERT INTO dc2 VALUES (5, 1, 'x'), (6, 2, 'y'); \
             ALTER TABLE dc2 DROP COLUMN a",
        )
        .unwrap();
        assert!(
            db.execute_sql("INSERT INTO dc2 VALUES (1, 'z')").is_err(),
            "DROP before the primary key must not permit duplicate keys"
        );
        assert_eq!(rows(&db, "SELECT * FROM dc2 ORDER BY id").len(), 2);

        db.execute_sql(
            "CREATE TABLE uq (a INTEGER, left_key INTEGER, right_key INTEGER UNIQUE); \
             INSERT INTO uq VALUES (0, 10, 20); \
             ALTER TABLE uq DROP COLUMN a; \
             CREATE TABLE composite_uq (a INTEGER, left_key INTEGER, right_key INTEGER, \
                                        UNIQUE (right_key, left_key)); \
             INSERT INTO composite_uq VALUES (0, 10, 20); \
             ALTER TABLE composite_uq DROP COLUMN a",
        )
        .unwrap();
        assert!(db.execute_sql("INSERT INTO uq VALUES (11, 20)").is_err());
        assert!(db.execute_sql("INSERT INTO uq VALUES (10, 20)").is_err());
        db.execute_sql("INSERT INTO uq VALUES (10, 21), (10, NULL), (10, NULL)")
            .unwrap();
        assert!(db
            .execute_sql("INSERT INTO composite_uq VALUES (10, 20)")
            .is_err());
        db.execute_sql("INSERT INTO composite_uq VALUES (11, 20), (10, 21)")
            .unwrap();
    }

    let db = Database::open(&path).unwrap();
    assert!(db.execute_sql("INSERT INTO dc2 VALUES (2, 'z')").is_err());
    assert!(db.execute_sql("INSERT INTO uq VALUES (12, 21)").is_err());
    assert!(db
        .execute_sql("INSERT INTO composite_uq VALUES (11, 20)")
        .is_err());
    assert_eq!(rows(&db, "SELECT * FROM dc2 ORDER BY id").len(), 2);
    assert_eq!(rows(&db, "SELECT * FROM uq").len(), 4);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_column_updates_indexes_inside_transactions_and_rolls_back() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE tx_drop (obsolete INTEGER, id INTEGER PRIMARY KEY, value INTEGER); \
         CREATE INDEX tx_drop_value ON tx_drop (value); \
         INSERT INTO tx_drop VALUES (0, 1, 10)",
    )
    .unwrap();

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.execute_sql("ALTER TABLE tx_drop DROP COLUMN obsolete")
        .unwrap();
    txn.execute_sql("INSERT INTO tx_drop VALUES (2, 20)")
        .unwrap();
    let ExecutionResult::Query(result) = txn
        .execute_sql("SELECT id FROM tx_drop WHERE value = 20")
        .unwrap()
    else {
        panic!("expected query inside schema transaction");
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Integer(2)]]);
    txn.rollback().unwrap();

    assert_eq!(
        rows(&db, "SELECT obsolete, id, value FROM tx_drop").len(),
        1
    );
    assert_eq!(
        rows(&db, "SELECT id FROM tx_drop WHERE value = 10"),
        vec![vec![SqlValue::Integer(1)]]
    );
    db.execute_sql("INSERT INTO tx_drop VALUES (0, 2, 20)")
        .unwrap();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.execute_sql("ALTER TABLE tx_drop DROP COLUMN obsolete")
        .unwrap();
    txn.execute_sql("INSERT INTO tx_drop VALUES (3, 30)")
        .unwrap();
    txn.commit().unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM tx_drop WHERE value = 30"),
        vec![vec![SqlValue::Integer(3)]]
    );
    assert!(db
        .execute_sql("ALTER TABLE tx_drop DROP COLUMN value")
        .is_err());
    assert_eq!(
        rows(&db, "SELECT id FROM tx_drop WHERE value = 30"),
        vec![vec![SqlValue::Integer(3)]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_column_preserves_fts_writes_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-fts.db");
    let query = "SELECT row_id, document FROM FTS_SEARCH('docs', 'body', 'quick') ORDER BY row_id";
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, obsolete INTEGER, body TEXT); \
             CREATE INDEX docs_fts ON docs (body) USING FTS; \
             INSERT INTO docs VALUES (1, 0, 'quick first'), (2, 0, 'quick second'); \
             ALTER TABLE docs DROP COLUMN obsolete; \
             UPDATE docs SET body = 'slow first' WHERE id = 1; \
             DELETE FROM docs WHERE id = 2; \
             INSERT INTO docs VALUES (3, 'quick third')",
        )
        .unwrap();
        assert_eq!(
            rows(&db, query),
            vec![vec![
                SqlValue::BigInt(3),
                SqlValue::Text("quick third".into())
            ]]
        );
    }
    let db = Database::open(&path).unwrap();
    let indexed = rows(&db, query);
    assert_eq!(
        indexed,
        vec![vec![
            SqlValue::BigInt(3),
            SqlValue::Text("quick third".into())
        ]]
    );
    db.execute_sql("DROP INDEX docs_fts").unwrap();
    assert_eq!(rows(&db, query), indexed);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_column_preserves_hnsw_writes_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop-hnsw.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE vectors (id INTEGER PRIMARY KEY, obsolete INTEGER, embedding VECTOR(2, L2)); \
             CREATE INDEX vectors_hnsw ON vectors (embedding) USING HNSW; \
             INSERT INTO vectors VALUES (1, 0, [0.0, 0.0]), (2, 0, [2.0, 0.0]); \
             ALTER TABLE vectors DROP COLUMN obsolete; \
             UPDATE vectors SET embedding = [5.0, 0.0] WHERE id = 1; \
             DELETE FROM vectors WHERE id = 2; \
             INSERT INTO vectors VALUES (3, [1.0, 0.0])",
        )
        .unwrap();
        let (found, _) = db
            .search_hnsw("vectors_hnsw", &[1.0, 0.0], 2, Some(32))
            .unwrap();
        assert_eq!(
            found.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            vec![3u64.to_be_bytes().to_vec(), 1u64.to_be_bytes().to_vec()]
        );
        assert_eq!(found[0].distance, 0.0);
    }
    let db = Database::open(&path).unwrap();
    let (found, _) = db
        .search_hnsw("vectors_hnsw", &[5.0, 0.0], 2, Some(32))
        .unwrap();
    assert_eq!(
        found.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
        vec![1u64.to_be_bytes().to_vec(), 3u64.to_be_bytes().to_vec()]
    );
    assert_eq!(found[0].distance, 0.0);
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
