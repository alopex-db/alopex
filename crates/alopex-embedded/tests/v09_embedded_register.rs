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

const EMBEDDED_SOURCE: &str = include_str!("../src/lib.rs");
const SQL_SOURCE: &str = include_str!("../src/sql_api.rs");
const OWNED_SESSION_SOURCE: &str = include_str!("../src/owned_session.rs");
const CATALOG_SOURCE: &str = include_str!("../src/catalog_api.rs");
const COLUMNAR_SOURCE: &str = include_str!("../src/columnar_api.rs");

/// Exact I-21 requirements rows. Each row ties one observable public method
/// to the source module and impl block that defines it; names are deliberately
/// not collapsed by subsystem so a removed compatibility method is visible.
const I21_REQUIRED_ROWS: &[(&str, &str, &str)] = &[
    ("Database", "open", EMBEDDED_SOURCE),
    ("Database", "new", EMBEDDED_SOURCE),
    ("Database", "open_in_memory", EMBEDDED_SOURCE),
    ("Database", "open_in_memory_with_options", EMBEDDED_SOURCE),
    ("Database", "open_with_uri", EMBEDDED_SOURCE),
    ("Database", "open_s3", EMBEDDED_SOURCE),
    ("Database", "cluster_status_snapshot", EMBEDDED_SOURCE),
    ("Database", "routing_diagnostics", EMBEDDED_SOURCE),
    ("Database", "table_info_cache_epoch", EMBEDDED_SOURCE),
    ("Database", "get_cached_table_info", EMBEDDED_SOURCE),
    ("Database", "cache_table_info", EMBEDDED_SOURCE),
    ("Database", "invalidate_table_info_cache", EMBEDDED_SOURCE),
    ("Database", "flush", EMBEDDED_SOURCE),
    ("Database", "file_format_version", EMBEDDED_SOURCE),
    ("Database", "memory_usage", EMBEDDED_SOURCE),
    ("Database", "persist_to_disk", EMBEDDED_SOURCE),
    ("Database", "clone_to_memory", EMBEDDED_SOURCE),
    ("Database", "clear", EMBEDDED_SOURCE),
    ("Database", "set_memory_limit", EMBEDDED_SOURCE),
    ("Database", "snapshot", EMBEDDED_SOURCE),
    ("Database", "create_hnsw_index", EMBEDDED_SOURCE),
    ("Database", "drop_hnsw_index", EMBEDDED_SOURCE),
    ("Database", "get_hnsw_stats", EMBEDDED_SOURCE),
    ("Database", "compact_hnsw_index", EMBEDDED_SOURCE),
    ("Database", "search_hnsw", EMBEDDED_SOURCE),
    ("Database", "create_blob_writer", EMBEDDED_SOURCE),
    ("Database", "create_typed_writer", EMBEDDED_SOURCE),
    ("Database", "open_large_value", EMBEDDED_SOURCE),
    ("Database", "begin_read_at_sql", SQL_SOURCE),
    ("Database", "execute_sql", SQL_SOURCE),
    ("Database", "execute_sql_multi", SQL_SOURCE),
    ("Database", "execute_sql_with_rows", SQL_SOURCE),
    ("Database", "execute_sql_streaming", SQL_SOURCE),
    ("Database", "begin_read", OWNED_SESSION_SOURCE),
    ("Database", "begin_transaction", OWNED_SESSION_SOURCE),
    ("Database", "begin_owned_read", OWNED_SESSION_SOURCE),
    ("Database", "begin_owned_transaction", OWNED_SESSION_SOURCE),
    (
        "Database",
        "begin_owned_embedded_transaction",
        OWNED_SESSION_SOURCE,
    ),
    ("OwnedEmbeddedTransaction", "get", OWNED_SESSION_SOURCE),
    ("OwnedEmbeddedTransaction", "put", OWNED_SESSION_SOURCE),
    ("OwnedEmbeddedTransaction", "delete", OWNED_SESSION_SOURCE),
    (
        "OwnedEmbeddedTransaction",
        "execute_sql",
        OWNED_SESSION_SOURCE,
    ),
    (
        "OwnedEmbeddedTransaction",
        "preflight_sql_stream",
        OWNED_SESSION_SOURCE,
    ),
    ("OwnedEmbeddedTransaction", "commit", OWNED_SESSION_SOURCE),
    ("OwnedEmbeddedTransaction", "rollback", OWNED_SESSION_SOURCE),
    ("CreateCatalogRequest", "new", CATALOG_SOURCE),
    ("CreateCatalogRequest", "with_comment", CATALOG_SOURCE),
    ("CreateCatalogRequest", "with_storage_root", CATALOG_SOURCE),
    ("CreateCatalogRequest", "build", CATALOG_SOURCE),
    ("CreateNamespaceRequest", "new", CATALOG_SOURCE),
    ("CreateNamespaceRequest", "with_comment", CATALOG_SOURCE),
    (
        "CreateNamespaceRequest",
        "with_storage_root",
        CATALOG_SOURCE,
    ),
    ("CreateNamespaceRequest", "build", CATALOG_SOURCE),
    ("CreateTableRequest", "new", CATALOG_SOURCE),
    ("CreateTableRequest", "with_catalog_name", CATALOG_SOURCE),
    ("CreateTableRequest", "with_namespace_name", CATALOG_SOURCE),
    ("CreateTableRequest", "with_schema", CATALOG_SOURCE),
    ("CreateTableRequest", "with_table_type", CATALOG_SOURCE),
    (
        "CreateTableRequest",
        "with_data_source_format",
        CATALOG_SOURCE,
    ),
    ("CreateTableRequest", "with_primary_key", CATALOG_SOURCE),
    ("CreateTableRequest", "with_storage_root", CATALOG_SOURCE),
    ("CreateTableRequest", "with_storage_options", CATALOG_SOURCE),
    ("CreateTableRequest", "with_comment", CATALOG_SOURCE),
    ("CreateTableRequest", "with_properties", CATALOG_SOURCE),
    ("CreateTableRequest", "build", CATALOG_SOURCE),
    ("Database", "open_with_config", COLUMNAR_SOURCE),
    ("Database", "storage_mode", COLUMNAR_SOURCE),
    ("Database", "write_columnar_segment", COLUMNAR_SOURCE),
    (
        "Database",
        "write_columnar_segment_with_config",
        COLUMNAR_SOURCE,
    ),
    ("Database", "read_columnar_segment", COLUMNAR_SOURCE),
    ("Database", "in_memory_usage", COLUMNAR_SOURCE),
    ("Database", "open_in_memory_with_limit", COLUMNAR_SOURCE),
    ("Database", "resolve_table_id", COLUMNAR_SOURCE),
    (
        "Database",
        "columnar_segment_streaming_factory_v08",
        COLUMNAR_SOURCE,
    ),
    (
        "Database",
        "columnar_segment_streaming_factory_v08_by_id",
        COLUMNAR_SOURCE,
    ),
    ("Database", "stream_columnar_segment_v08", COLUMNAR_SOURCE),
    (
        "Database",
        "stream_columnar_segment_v08_by_id",
        COLUMNAR_SOURCE,
    ),
    ("Database", "scan_columnar_segment", COLUMNAR_SOURCE),
    ("Database", "scan_columnar_segment_batches", COLUMNAR_SOURCE),
    (
        "Database",
        "scan_columnar_segment_streaming",
        COLUMNAR_SOURCE,
    ),
    ("Database", "list_catalogs", CATALOG_SOURCE),
    ("Database", "get_catalog", CATALOG_SOURCE),
    ("Database", "list_namespaces", CATALOG_SOURCE),
    ("Database", "get_namespace", CATALOG_SOURCE),
    ("Database", "list_tables", CATALOG_SOURCE),
    ("Database", "list_tables_simple", CATALOG_SOURCE),
    ("Database", "get_table_info", CATALOG_SOURCE),
    ("Database", "get_table_info_simple", CATALOG_SOURCE),
    ("Database", "get_table_info_cached", CATALOG_SOURCE),
    ("Database", "list_indexes", CATALOG_SOURCE),
    ("Database", "list_indexes_simple", CATALOG_SOURCE),
    ("Database", "get_index_info", CATALOG_SOURCE),
    ("Database", "get_index_info_simple", CATALOG_SOURCE),
    ("Database", "create_catalog", CATALOG_SOURCE),
    ("Database", "delete_catalog", CATALOG_SOURCE),
    ("Database", "create_namespace", CATALOG_SOURCE),
    ("Database", "delete_namespace", CATALOG_SOURCE),
    ("Database", "create_table", CATALOG_SOURCE),
    ("Database", "create_table_simple", CATALOG_SOURCE),
    ("Database", "delete_table", CATALOG_SOURCE),
];

fn has_public_method(source: &str, method: &str) -> bool {
    let prefix = format!("pub fn {method}");
    source.lines().any(|line| {
        line.trim_start()
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.chars().next())
            .is_some_and(|next| matches!(next, '(' | '<'))
    })
}

#[test]
fn i21_embedded_public_method_register_has_one_source_row_per_requirement() {
    assert_eq!(
        I21_REQUIRED_ROWS.len(),
        100,
        "the approved I-21 requirements register changed"
    );
    for (owner, method, source) in I21_REQUIRED_ROWS {
        assert!(
            source.contains(&format!("impl {owner}")),
            "{owner}.{method} must have a public impl block"
        );
        assert!(
            has_public_method(source, method),
            "missing public I-21 method {owner}.{method}"
        );
    }
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
