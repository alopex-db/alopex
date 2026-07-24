use std::sync::Arc;

use alopex_embedded::{CreateCatalogRequest, Database, Metric, SqlStreamingResult, TxnMode};

const I11_REGISTER: [&str; 7] = [
    "Database.local_lifecycle",
    "Transaction.commit",
    "Transaction.rollback",
    "OwnedEmbeddedTransaction.lifecycle",
    "SQL.streaming",
    "Catalog.lifecycle",
    "Vector.lifecycle",
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    passed: bool,
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i11_embedded_lifecycle_register_has_a_semantic_status_row_per_boundary() {
    let database = Arc::new(Database::new());
    let database_lifecycle = database.memory_usage().is_some()
        && database.cluster_status_snapshot().is_ok()
        && database.routing_diagnostics().is_ok()
        && database.flush().is_ok();

    let mut committed = database.begin(TxnMode::ReadWrite).unwrap();
    committed.put(b"committed", b"value").unwrap();
    let committed_ok = committed.commit().is_ok();
    let mut check_commit = database.begin(TxnMode::ReadOnly).unwrap();
    let committed_visible = check_commit.get(b"committed").unwrap() == Some(b"value".to_vec());
    check_commit.commit().unwrap();

    let mut rolled_back = database.begin(TxnMode::ReadWrite).unwrap();
    rolled_back.put(b"rolled-back", b"value").unwrap();
    let rollback_ok = rolled_back.rollback().is_ok();
    let mut check_rollback = database.begin(TxnMode::ReadOnly).unwrap();
    let rollback_hidden = check_rollback.get(b"rolled-back").unwrap().is_none();
    check_rollback.commit().unwrap();

    let mut owned = Arc::clone(&database)
        .begin_owned_embedded_transaction(TxnMode::ReadWrite)
        .unwrap();
    owned.put(b"owned", b"value").unwrap();
    let owned_commit = owned.commit().is_ok();
    let mut check_owned = database.begin(TxnMode::ReadOnly).unwrap();
    let owned_visible = check_owned.get(b"owned").unwrap() == Some(b"value".to_vec());
    check_owned.commit().unwrap();

    database
        .execute_sql("CREATE TABLE stream_rows (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    database
        .execute_sql("INSERT INTO stream_rows (id, value) VALUES (1, 'streamed')")
        .unwrap();
    let streaming = match database
        .execute_sql_streaming("SELECT value FROM stream_rows WHERE id = 1")
        .unwrap()
    {
        SqlStreamingResult::Query(mut rows) => rows.next_row().unwrap().is_some(),
        SqlStreamingResult::Success | SqlStreamingResult::RowsAffected(_) => false,
    };

    database
        .create_catalog(CreateCatalogRequest::new("v09_catalog"))
        .unwrap();
    let catalog_lifecycle = database
        .list_catalogs()
        .unwrap()
        .iter()
        .any(|catalog| catalog.name == "v09_catalog");

    let mut vector_writer = database.begin(TxnMode::ReadWrite).unwrap();
    vector_writer
        .upsert_vector(b"vector", b"metadata", &[1.0, 0.0], Metric::Cosine)
        .unwrap();
    vector_writer.commit().unwrap();
    let mut vector_reader = database.begin(TxnMode::ReadOnly).unwrap();
    let vector_lifecycle = vector_reader
        .search_similar(&[1.0, 0.0], Metric::Cosine, 1, None)
        .unwrap()
        .first()
        .is_some_and(|row| row.key == b"vector");
    vector_reader.commit().unwrap();

    let rows = [
        StatusRow {
            operation: "Database.local_lifecycle",
            passed: database_lifecycle,
        },
        StatusRow {
            operation: "Transaction.commit",
            passed: committed_ok && committed_visible,
        },
        StatusRow {
            operation: "Transaction.rollback",
            passed: rollback_ok && rollback_hidden,
        },
        StatusRow {
            operation: "OwnedEmbeddedTransaction.lifecycle",
            passed: owned_commit && owned_visible,
        },
        StatusRow {
            operation: "SQL.streaming",
            passed: streaming,
        },
        StatusRow {
            operation: "Catalog.lifecycle",
            passed: catalog_lifecycle,
        },
        StatusRow {
            operation: "Vector.lifecycle",
            passed: vector_lifecycle,
        },
    ];
    let names: Vec<_> = rows.iter().map(|row| row.operation).collect();
    assert_eq!(names, I11_REGISTER, "the I-11 lifecycle register drifted");
    for row in rows {
        assert!(row.passed, "{} must retain its lifecycle", row.operation);
    }
}
