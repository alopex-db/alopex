use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};

fn query(db: &Database, sql: &str) -> alopex_sql::QueryResult {
    let ExecutionResult::Query(result) = db.execute_sql(sql).unwrap() else {
        panic!("expected query result for {sql}");
    };
    result
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn portable_metadata_surfaces_have_exact_schemas_and_values() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE \"Order Items\" (id INTEGER PRIMARY KEY, label TEXT DEFAULT 'new'); \
         CREATE INDEX \"Order Label\" ON \"Order Items\" (label);",
    )
    .unwrap();

    let tables = query(&db, "SHOW TABLES");
    assert_eq!(tables.columns[0].name, "table_name");
    assert_eq!(
        tables.rows,
        vec![vec![SqlValue::Text("Order Items".into())]]
    );

    let described = query(&db, "DESCRIBE \"Order Items\"");
    assert_eq!(
        described
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "column_name",
            "column_type",
            "null",
            "key",
            "default",
            "extra"
        ]
    );
    assert_eq!(described.rows[0][0], SqlValue::Text("id".into()));
    assert_eq!(described.rows[0][3], SqlValue::Text("PRI".into()));
    assert_eq!(described.rows[1][4], SqlValue::Text("'new'".into()));

    let indexes = query(&db, "SHOW INDEXES FROM \"Order Items\"");
    assert_eq!(
        indexes
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["table_name", "index_name", "is_unique", "index_type"]
    );
    let label_index = indexes
        .rows
        .iter()
        .find(|row| row[1] == SqlValue::Text("Order Label".into()))
        .unwrap();
    assert_eq!(label_index[2], SqlValue::Boolean(false));

    let columns = query(
        &db,
        "SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type \
         FROM information_schema.columns WHERE table_name = 'Order Items' ORDER BY ordinal_position",
    );
    assert_eq!(columns.rows.len(), 2);
    assert_eq!(columns.rows[0][2], SqlValue::BigInt(1));
    assert_eq!(columns.rows[1][3], SqlValue::Text("'new'".into()));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn transaction_catalog_metadata_observes_ddl_overlay_and_rollback() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.execute_sql("CREATE TABLE pending (id BIGINT)").unwrap();
    let ExecutionResult::Query(inside) = txn.execute_sql("SHOW TABLES").unwrap() else {
        panic!("SHOW TABLES must be a query inside a transaction");
    };
    assert_eq!(inside.rows, vec![vec![SqlValue::Text("pending".into())]]);
    txn.rollback().unwrap();

    assert!(query(&db, "SHOW TABLES").rows.is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn temporary_table_is_visible_for_the_database_handle_but_not_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.db");
    {
        let db = Database::open(&path).unwrap();
        db.execute_sql("CREATE TEMPORARY TABLE scratch (id BIGINT)")
            .unwrap();
        db.execute_sql("INSERT INTO scratch VALUES (7)").unwrap();
        assert_eq!(
            query(&db, "SELECT table_type FROM information_schema.tables").rows,
            vec![vec![SqlValue::Text("LOCAL TEMPORARY".into())]]
        );
        assert_eq!(
            query(&db, "SELECT id FROM scratch").rows[0][0],
            SqlValue::BigInt(7)
        );
    }

    let reopened = Database::open(&path).unwrap();
    assert!(query(&reopened, "SHOW TABLES").rows.is_empty());
    reopened
        .execute_sql("CREATE TABLE durable (id BIGINT)")
        .unwrap();
    assert!(query(&reopened, "SELECT id FROM durable").rows.is_empty());
}
