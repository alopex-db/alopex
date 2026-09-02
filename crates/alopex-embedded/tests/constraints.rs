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
fn check_and_composite_foreign_keys_follow_sql_null_semantics() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE parents (
             tenant BIGINT,
             id BIGINT,
             balance BIGINT CHECK (balance >= 0),
             PRIMARY KEY (tenant, id)
         );
         CREATE TABLE children (
             tenant BIGINT,
             parent_id BIGINT,
             FOREIGN KEY (tenant, parent_id) REFERENCES parents (tenant, id)
         );
         INSERT INTO parents VALUES (1, 10, 0);
         INSERT INTO children VALUES (1, 10), (NULL, 999), (1, NULL)",
    )
    .unwrap();

    assert!(db
        .execute_sql("INSERT INTO parents VALUES (2, 20, -1)")
        .is_err());
    assert!(db
        .execute_sql("INSERT INTO children VALUES (1, 999)")
        .is_err());
    assert!(db
        .execute_sql("UPDATE children SET parent_id = 999 WHERE tenant = 1")
        .is_err());
    assert!(db
        .execute_sql("DELETE FROM parents WHERE tenant = 1 AND id = 10")
        .is_err());
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM children"),
        vec![vec![SqlValue::BigInt(3)]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn foreign_key_actions_cover_cascade_set_null_and_self_reference() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE accounts (
             id BIGINT PRIMARY KEY,
             manager_id BIGINT REFERENCES accounts (id) ON DELETE SET NULL
         );
         CREATE TABLE events (
             id BIGINT PRIMARY KEY,
             account_id BIGINT REFERENCES accounts (id)
                 ON DELETE CASCADE ON UPDATE CASCADE
         );
         INSERT INTO accounts VALUES (1, NULL), (2, 1);
         INSERT INTO events VALUES (10, 1), (20, 2);
         UPDATE accounts SET id = 3 WHERE id = 2;
         DELETE FROM accounts WHERE id = 1",
    )
    .unwrap();

    assert_eq!(
        rows(&db, "SELECT id, manager_id FROM accounts"),
        vec![vec![SqlValue::BigInt(3), SqlValue::Null]]
    );
    assert_eq!(
        rows(&db, "SELECT id, account_id FROM events"),
        vec![vec![SqlValue::BigInt(20), SqlValue::BigInt(3)]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn constraints_persist_and_are_introspectable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("constraints.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            "CREATE TABLE parents (id BIGINT PRIMARY KEY);
             CREATE TABLE children (
                 id BIGINT CHECK (id > 0),
                 parent_id BIGINT REFERENCES parents (id)
             );
             INSERT INTO parents VALUES (1);
             INSERT INTO children VALUES (1, 1)",
        )
        .unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    assert!(reopened
        .execute_sql("INSERT INTO children VALUES (0, 1)")
        .is_err());
    assert!(reopened
        .execute_sql("INSERT INTO children VALUES (2, 9)")
        .is_err());
    assert_eq!(
        rows(
            &reopened,
            "SELECT constraint_type FROM information_schema.table_constraints
             WHERE table_name = 'children' ORDER BY constraint_type",
        ),
        vec![
            vec![SqlValue::Text("CHECK".into())],
            vec![SqlValue::Text("FOREIGN KEY".into())],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn concurrent_parent_delete_and_child_insert_cannot_both_commit() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE parents (id BIGINT PRIMARY KEY);
         CREATE TABLE children (parent_id BIGINT REFERENCES parents (id));
         INSERT INTO parents VALUES (1)",
    )
    .unwrap();

    let mut delete_parent = db.begin(TxnMode::ReadWrite).unwrap();
    let mut insert_child = db.begin(TxnMode::ReadWrite).unwrap();
    delete_parent
        .execute_sql("DELETE FROM parents WHERE id = 1")
        .unwrap();
    insert_child
        .execute_sql("INSERT INTO children VALUES (1)")
        .unwrap();
    insert_child.commit().unwrap();
    assert!(delete_parent.commit().is_err());

    assert_eq!(
        rows(&db, "SELECT parent_id FROM children"),
        vec![vec![SqlValue::BigInt(1)]]
    );
    assert_eq!(
        rows(&db, "SELECT id FROM parents"),
        vec![vec![SqlValue::BigInt(1)]]
    );
}
