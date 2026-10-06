use super::*;
use alopex_core::error::Result as CoreResult;
use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::txn::TxnManager;
use alopex_sql::catalog::persistent::TableFqn;
use alopex_sql::storage::{KeyEncoder, RowCodec};
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "recovery_write_failure.rs"]
mod write_failure;

fn snapshot(store: &MemoryKV, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let entries = txn.scan_prefix(prefix).unwrap().collect();
    txn.rollback_self().unwrap();
    entries
}

// Reproduce the old DROP COLUMN storage transition, without invoking the fixed DDL.
// Index definitions and entries deliberately retain their pre-drop positions.
fn legacy_drop(
    store: &MemoryKV,
    catalog: &RwLock<PersistentCatalog<MemoryKV>>,
    name: &str,
    column: usize,
) -> u32 {
    let mut catalog = catalog.write().unwrap();
    let mut table = catalog.get_table(name).unwrap().clone();
    table.columns.remove(column);
    let prefix = KeyEncoder::table_prefix(table.table_id);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let rows: Vec<_> = txn.scan_prefix(&prefix).unwrap().collect();
    for (key, bytes) in rows {
        let mut row = RowCodec::decode(&bytes).unwrap();
        row.remove(column);
        txn.put(key, RowCodec::encode(&row)).unwrap();
    }
    catalog.persist_create_table(&mut txn, &table).unwrap();
    txn.commit_self().unwrap();
    let id = table.table_id;
    let mut overlay = CatalogOverlay::new();
    overlay.add_table(TableFqn::from(&table), table);
    catalog.apply_overlay(overlay);
    id
}

fn fixture(sql: &str) -> (Arc<MemoryKV>, Arc<RwLock<PersistentCatalog<MemoryKV>>>) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    run_sql_in_txn(store.clone(), catalog.clone(), TxnMode::ReadWrite, sql);
    (store, catalog)
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_recovery_invalid_default_precedes_index_repair() {
    use alopex_core::storage::format::bincode_config;
    use alopex_sql::catalog::persistent::{CatalogError, PersistedTableMeta, TABLES_PREFIX};
    use bincode::Options;

    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, obsolete INTEGER, value INTEGER DEFAULT 7); \
         CREATE INDEX idx_value ON items(value); INSERT INTO items VALUES (1, 0, 10);",
    );
    legacy_drop(&store, &catalog, "items", 1);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let (key, value) = txn.scan_prefix(TABLES_PREFIX).unwrap().next().unwrap();
    let mut persisted: PersistedTableMeta = bincode_config().deserialize(&value).unwrap();
    persisted
        .properties
        .insert("alopex.column.defaults".into(), "not valid JSON".into());
    txn.put(key, bincode_config().serialize(&persisted).unwrap())
        .unwrap();
    txn.commit_self().unwrap();

    let before = snapshot(&store, b"");
    assert!(matches!(
        PersistentCatalog::load(store.clone()),
        Err(CatalogError::InvalidMetadata(_))
    ));
    assert_eq!(
        snapshot(&store, b""),
        before,
        "invalid table metadata must stop load before derived index repair"
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_index_recovery_rebuilds_entries_and_preserves_rows_and_sequence() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, obsolete INTEGER, value INTEGER, tail INTEGER); \
         CREATE INDEX idx_value ON items(value); \
         INSERT INTO items VALUES (1, 0, 10, 90);",
    );
    let table_id = legacy_drop(&store, &catalog, "items", 1);
    // The stale position indexes 80, not 20. Recovery must rebuild, not only fix metadata.
    run_sql_in_txn(
        store.clone(),
        catalog.clone(),
        TxnMode::ReadWrite,
        "INSERT INTO items VALUES (2, 20, 80);",
    );
    let rows = snapshot(&store, &KeyEncoder::table_prefix(table_id));
    let sequence = snapshot(&store, &KeyEncoder::sequence_key(table_id));
    let index_id = catalog
        .read()
        .unwrap()
        .get_index("idx_value")
        .unwrap()
        .index_id;
    let stale_prefix = KeyEncoder::index_value_prefix(index_id, &SqlValue::Integer(80)).unwrap();
    let correct_prefix = KeyEncoder::index_value_prefix(index_id, &SqlValue::Integer(20)).unwrap();
    assert_eq!(snapshot(&store, &stale_prefix).len(), 1);
    assert!(snapshot(&store, &correct_prefix).is_empty());
    let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert!(
        snapshot(&store, &stale_prefix).is_empty(),
        "old derived entry must be removed"
    );
    assert_eq!(
        snapshot(&store, &correct_prefix).len(),
        1,
        "canonical value must be indexed"
    );
    assert_eq!(
        recovered
            .read()
            .unwrap()
            .get_index("idx_value")
            .unwrap()
            .column_indices,
        vec![1]
    );
    for (value, expected) in [(20, vec![vec![SqlValue::Integer(2)]]), (80, vec![])] {
        let result = run_sql_in_txn(
            store.clone(),
            recovered.clone(),
            TxnMode::ReadOnly,
            &format!("SELECT id FROM items WHERE value = {value}"),
        );
        let ExecutionResult::Query(query) = result else {
            panic!("expected query");
        };
        assert_eq!(query.rows, expected);
    }
    assert_eq!(snapshot(&store, &KeyEncoder::table_prefix(table_id)), rows);
    assert_eq!(
        snapshot(&store, &KeyEncoder::sequence_key(table_id)),
        sequence
    );
    let repaired = snapshot(&store, b"");
    PersistentCatalog::load(store.clone()).unwrap();
    assert_eq!(
        snapshot(&store, b""),
        repaired,
        "second open must not rewrite storage"
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_index_recovery_constraint_failure_rolls_back_every_write() {
    for constraint in ["PRIMARY KEY", "UNIQUE"] {
        let (store, catalog) = fixture(&format!(
            "CREATE TABLE items (obsolete INTEGER, id INTEGER {constraint}, tail INTEGER); \
             CREATE INDEX a_tail ON items(tail); INSERT INTO items VALUES (0, 1, 10);"
        ));
        legacy_drop(&store, &catalog, "items", 0);
        // Seed a duplicate canonical value directly: the persisted rows are the truth.
        let table = catalog.read().unwrap().get_table("items").unwrap().clone();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        txn.put(
            KeyEncoder::row_key(table.table_id, 2),
            RowCodec::encode(&[SqlValue::Integer(1), SqlValue::Integer(20)]),
        )
        .unwrap();
        txn.commit_self().unwrap();
        let before = snapshot(&store, b"");
        assert!(
            PersistentCatalog::load(store.clone()).is_err(),
            "{constraint} duplicate must block recovery"
        );
        assert_eq!(
            snapshot(&store, b""),
            before,
            "{constraint}: no partial index or metadata writes"
        );
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_index_recovery_rebuilds_fts_documents() {
    let (store, catalog) = fixture(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, obsolete INTEGER, body TEXT, decoy TEXT); \
         CREATE INDEX docs_fts ON docs(body) USING FTS; \
         INSERT INTO docs VALUES (1, 0, 'quick first', 'slow decoy');",
    );
    let table_id = legacy_drop(&store, &catalog, "docs", 1);
    let index_id = catalog
        .read()
        .unwrap()
        .get_index("docs_fts")
        .unwrap()
        .index_id;
    run_sql_in_txn(
        store.clone(),
        catalog,
        TxnMode::ReadWrite,
        "INSERT INTO docs VALUES (2, 'quick second', 'slow decoy');",
    );
    let rows = snapshot(&store, &KeyEncoder::table_prefix(table_id));
    let sequence = snapshot(&store, &KeyEncoder::sequence_key(table_id));
    let wrong_term =
        KeyEncoder::index_value_prefix(index_id, &SqlValue::Text("decoy".into())).unwrap();
    let correct_term =
        KeyEncoder::index_value_prefix(index_id, &SqlValue::Text("quick".into())).unwrap();
    assert_eq!(snapshot(&store, &wrong_term).len(), 1);
    assert_eq!(snapshot(&store, &correct_term).len(), 1);
    let recovered = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
    assert!(
        snapshot(&store, &wrong_term).is_empty(),
        "old FTS term must be removed"
    );
    assert_eq!(
        snapshot(&store, &correct_term).len(),
        2,
        "both canonical documents must be indexed"
    );
    let result = run_sql_in_txn(
        store.clone(),
        recovered,
        TxnMode::ReadOnly,
        "SELECT row_id, document FROM FTS_SEARCH('docs', 'body', 'quick') ORDER BY row_id",
    );
    let ExecutionResult::Query(query) = result else {
        panic!("expected query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::BigInt(1), SqlValue::Text("quick first".into())],
            vec![SqlValue::BigInt(2), SqlValue::Text("quick second".into())],
        ]
    );
    assert_eq!(snapshot(&store, &KeyEncoder::table_prefix(table_id)), rows);
    assert_eq!(
        snapshot(&store, &KeyEncoder::sequence_key(table_id)),
        sequence
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_hnsw_recovery_rebuilds_graph_from_canonical_vectors() {
    let (store, catalog) = fixture(
        "CREATE TABLE vectors (id INTEGER PRIMARY KEY, obsolete INTEGER, \
         embedding VECTOR(2, L2), decoy VECTOR(2, L2)); \
         CREATE INDEX vectors_hnsw ON vectors(embedding) USING HNSW; \
         INSERT INTO vectors VALUES (1, 0, [10.0, 0.0], [90.0, 0.0]);",
    );
    let table_id = legacy_drop(&store, &catalog, "vectors", 1);
    run_sql_in_txn(
        store.clone(),
        catalog,
        TxnMode::ReadWrite,
        "INSERT INTO vectors VALUES (2, [0.0, 0.0], [80.0, 0.0]);",
    );
    let nearest = || {
        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        let index = alopex_core::HnswIndex::load("vectors_hnsw", &mut txn).unwrap();
        let hits = index.search(&[0.0, 0.0], 2, Some(16)).unwrap().0;
        txn.rollback_self().unwrap();
        hits.into_iter().map(|hit| hit.key).collect::<Vec<_>>()
    };
    assert_eq!(
        nearest(),
        vec![1u64.to_be_bytes().to_vec(), 2u64.to_be_bytes().to_vec()]
    );
    let rows = snapshot(&store, &KeyEncoder::table_prefix(table_id));
    let sequence = snapshot(&store, &KeyEncoder::sequence_key(table_id));
    PersistentCatalog::load(store.clone()).unwrap();
    assert_eq!(
        nearest(),
        vec![2u64.to_be_bytes().to_vec(), 1u64.to_be_bytes().to_vec()]
    );
    assert_eq!(snapshot(&store, &KeyEncoder::table_prefix(table_id)), rows);
    assert_eq!(
        snapshot(&store, &KeyEncoder::sequence_key(table_id)),
        sequence
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_recovery_invalid_fts_metadata_returns_error_without_changes() {
    for invalid in ["empty_columns", "empty_columns_and_positions", "config"] {
        let (store, catalog) = fixture(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, obsolete INTEGER, body TEXT); \
             CREATE INDEX docs_fts ON docs(body) USING FTS; \
             INSERT INTO docs VALUES (1, 0, 'quick first');",
        );
        legacy_drop(&store, &catalog, "docs", 1);
        let mut guard = catalog.write().unwrap();
        let mut index = guard.get_index("docs_fts").unwrap().clone();
        if invalid == "config" {
            index
                .options
                .retain(|(key, _)| !key.eq_ignore_ascii_case("config"));
            index
                .options
                .push(("config".into(), "unknown_config".into()));
        } else {
            index.columns.clear();
            if invalid == "empty_columns_and_positions" {
                index.column_indices.clear();
            }
        }
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        guard.persist_create_index(&mut txn, &index).unwrap();
        txn.commit_self().unwrap();
        drop(guard);
        let before = snapshot(&store, b"");
        let error = match PersistentCatalog::load(store.clone()) {
            Ok(_) => panic!("{invalid}: invalid metadata must block recovery"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("index recovery"), "{invalid}: {error}");
        assert_eq!(snapshot(&store, b""), before, "{invalid}");
    }
}

struct ReadOnlyStore {
    inner: Arc<MemoryKV>,
    write_attempts: AtomicUsize,
}

impl KVStore for ReadOnlyStore {
    type Transaction<'a> = MemoryTransaction<'a>;
    type Manager<'a> = &'a Self;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self
    }

    fn begin(&self, mode: TxnMode) -> CoreResult<Self::Transaction<'_>> {
        if mode == TxnMode::ReadWrite {
            self.write_attempts.fetch_add(1, Ordering::Relaxed);
            return Err(alopex_core::Error::TxnReadOnly);
        }
        self.inner.begin(mode)
    }
}

impl<'a> TxnManager<'a, MemoryTransaction<'a>> for &'a ReadOnlyStore {
    fn begin(&'a self, mode: TxnMode) -> CoreResult<MemoryTransaction<'a>> {
        KVStore::begin(*self, mode)
    }
    fn commit(&'a self, txn: MemoryTransaction<'a>) -> CoreResult<()> {
        txn.commit_self()
    }
    fn rollback(&'a self, txn: MemoryTransaction<'a>) -> CoreResult<()> {
        txn.rollback_self()
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_index_recovery_readonly_open_is_unchanged_or_errors() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (obsolete INTEGER, id INTEGER PRIMARY KEY); INSERT INTO items VALUES (0, 1);",
    );
    let readonly = Arc::new(ReadOnlyStore {
        inner: store.clone(),
        write_attempts: AtomicUsize::new(0),
    });
    let before = snapshot(&store, b"");
    PersistentCatalog::load(readonly.clone()).unwrap();
    assert_eq!(readonly.write_attempts.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), before);
    legacy_drop(&store, &catalog, "items", 0);
    let before = snapshot(&store, b"");
    assert!(PersistentCatalog::load(readonly.clone()).is_err());
    assert_eq!(snapshot(&store, b""), before);
}
