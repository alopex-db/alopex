use super::*;
use alopex_core::storage::format::bincode_config;
use alopex_core::types::{Key, TxnId, Value};
use alopex_sql::catalog::persistent::{CatalogError, INDEXES_PREFIX, META_KEY, PersistedIndexMeta};
use alopex_sql::catalog::persistent::{PersistedTableMeta, TABLES_PREFIX};
use bincode::Options;

enum FailurePoint {
    SecondWrite,
    BeforeCommit,
    MetaBeforeRepair(Vec<u8>),
    IndexBeforeRepair(Key, Value),
    TableBeforeRepair(Key, Value),
}

struct FailingStore {
    inner: Arc<MemoryKV>,
    write_transactions: AtomicUsize,
    attempts: AtomicUsize,
    staged: AtomicUsize,
    commits: AtomicUsize,
    failure: FailurePoint,
}

struct FailingTransaction<'a> {
    inner: MemoryTransaction<'a>,
    store: &'a FailingStore,
}

impl KVStore for FailingStore {
    type Transaction<'a> = FailingTransaction<'a>;
    type Manager<'a> = &'a Self;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self
    }

    fn begin(&self, mode: TxnMode) -> CoreResult<Self::Transaction<'_>> {
        if mode == TxnMode::ReadWrite {
            let previous = self.write_transactions.fetch_add(1, Ordering::Relaxed);
            if previous == 0 {
                let competing_write = match &self.failure {
                    FailurePoint::MetaBeforeRepair(bytes) => {
                        Some((META_KEY.to_vec(), bytes.clone()))
                    }
                    FailurePoint::IndexBeforeRepair(key, bytes) => {
                        Some((key.clone(), bytes.clone()))
                    }
                    FailurePoint::TableBeforeRepair(key, bytes) => {
                        Some((key.clone(), bytes.clone()))
                    }
                    _ => None,
                };
                if let Some((key, bytes)) = competing_write {
                    // Deterministic competing commit after detection, before repair's snapshot.
                    let mut competing = self.inner.begin(TxnMode::ReadWrite)?;
                    competing.put(key, bytes)?;
                    competing.commit_self()?;
                }
            }
        }
        Ok(FailingTransaction {
            inner: self.inner.begin(mode)?,
            store: self,
        })
    }
}

impl<'a> TxnManager<'a, FailingTransaction<'a>> for &'a FailingStore {
    fn begin(&'a self, mode: TxnMode) -> CoreResult<FailingTransaction<'a>> {
        KVStore::begin(*self, mode)
    }
    fn commit(&'a self, txn: FailingTransaction<'a>) -> CoreResult<()> {
        txn.commit_self()
    }
    fn rollback(&'a self, txn: FailingTransaction<'a>) -> CoreResult<()> {
        txn.rollback_self()
    }
}

impl FailingTransaction<'_> {
    fn before_write(&self) -> CoreResult<()> {
        let attempt = self.store.attempts.fetch_add(1, Ordering::Relaxed);
        if matches!(self.store.failure, FailurePoint::SecondWrite) && attempt == 1 {
            return Err(std::io::Error::other("injected recovery write failure").into());
        }
        Ok(())
    }
}

impl<'a> KVTransaction<'a> for FailingTransaction<'a> {
    fn id(&self) -> TxnId {
        self.inner.id()
    }
    fn mode(&self) -> TxnMode {
        self.inner.mode()
    }
    fn get(&mut self, key: &Key) -> CoreResult<Option<Value>> {
        self.inner.get(key)
    }
    fn put(&mut self, key: Key, value: Value) -> CoreResult<()> {
        self.before_write()?;
        self.inner.put(key, value)?;
        self.store.staged.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn delete(&mut self, key: Key) -> CoreResult<()> {
        self.before_write()?;
        self.inner.delete(key)?;
        self.store.staged.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.inner.scan_prefix(prefix)
    }
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.inner.scan_range(start, end)
    }
    fn scan_from(
        &mut self,
        start: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.inner.scan_from(start)
    }
    fn commit_self(self) -> CoreResult<()> {
        if self.inner.mode() == TxnMode::ReadWrite {
            self.store.commits.fetch_add(1, Ordering::Relaxed);
            if matches!(self.store.failure, FailurePoint::BeforeCommit) {
                // This models failure before durable commit, not a lost success response.
                self.inner.rollback_self()?;
                return Err(std::io::Error::other("injected recovery pre-commit failure").into());
            }
        }
        self.inner.commit_self()
    }
    fn rollback_self(self) -> CoreResult<()> {
        self.inner.rollback_self()
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn legacy_recovery_write_failure_rolls_back_staged_changes() {
    let (store, catalog) = fixture(
        "CREATE TABLE items (obsolete INTEGER, id INTEGER PRIMARY KEY); \
         INSERT INTO items VALUES (0, 1), (0, 2);",
    );
    legacy_drop(&store, &catalog, "items", 0);
    let before = snapshot(&store, b"");
    let failing = Arc::new(FailingStore {
        inner: store.clone(),
        write_transactions: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        staged: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        failure: FailurePoint::SecondWrite,
    });
    assert!(PersistentCatalog::load(failing.clone()).is_err());
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 2);
    assert_eq!(failing.staged.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_missing_unique_write_failure_rolls_back_staged_changes() {
    let (store, _) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (1, 10), (2, 20);",
    );
    super::missing_unique::remove_unique_indexes(&store);
    let before = snapshot(&store, b"");
    let failing = Arc::new(FailingStore {
        inner: store.clone(),
        write_transactions: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        staged: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        failure: FailurePoint::SecondWrite,
    });
    assert!(PersistentCatalog::load(failing.clone()).is_err());
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 2);
    assert_eq!(failing.staged.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot(&store, b""), before);
}

fn missing_unique_store() -> Arc<MemoryKV> {
    let (store, _) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (1, 10), (2, 20);",
    );
    super::missing_unique::remove_unique_indexes(&store);
    store
}

fn failing_store(store: Arc<MemoryKV>, failure: FailurePoint) -> Arc<FailingStore> {
    Arc::new(FailingStore {
        inner: store,
        write_transactions: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        staged: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        failure,
    })
}

fn assert_index_recovery(store: Arc<FailingStore>, diagnostic: &str) {
    match PersistentCatalog::load(store) {
        Err(CatalogError::IndexRecovery(message)) => assert!(message.contains(diagnostic)),
        Err(error) => panic!("wrong recovery error: {error}"),
        Ok(_) => panic!("controlled recovery failure must be reported"),
    }
}

fn catalog_counter_bytes(store: &Arc<MemoryKV>, replace: impl FnOnce(u32) -> u32) -> Vec<u8> {
    let bytes = store
        .begin(TxnMode::ReadOnly)
        .unwrap()
        .get(&META_KEY.to_vec())
        .unwrap()
        .unwrap();
    // Persisted CatalogState is three u32 fields; round-trip the actual bytes
    // before changing only the index counter, without a production test API.
    let (version, table, index): (u32, u32, u32) = bincode::deserialize(&bytes).unwrap();
    assert_eq!(bincode::serialize(&(version, table, index)).unwrap(), bytes);
    bincode::serialize(&(version, table, replace(index))).unwrap()
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pre_commit_failure_preserves_all_catalog_and_row_bytes() {
    let store = missing_unique_store();
    let before = snapshot(&store, b"");
    let failing = failing_store(store.clone(), FailurePoint::BeforeCommit);
    assert_index_recovery(failing.clone(), "injected recovery pre-commit failure");
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 1);
    assert!(failing.staged.load(Ordering::Relaxed) > 0);
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_meta_counter_competition_preserves_competing_commit() {
    let store = missing_unique_store();
    let bytes = catalog_counter_bytes(&store, |index| index.checked_add(7).unwrap());
    let mut expected = snapshot(&store, b"");
    let meta = expected
        .iter_mut()
        .find(|(key, _)| key == META_KEY)
        .unwrap();
    meta.1 = bytes.clone();
    let failing = failing_store(store.clone(), FailurePoint::MetaBeforeRepair(bytes));
    assert_index_recovery(
        failing.clone(),
        "catalog counter metadata changed during recovery",
    );
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 0);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), expected);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_index_counter_overflow_preserves_all_bytes() {
    let store = missing_unique_store();
    let bytes = catalog_counter_bytes(&store, |_| u32::MAX);
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    txn.put(META_KEY.to_vec(), bytes).unwrap();
    txn.commit_self().unwrap();
    let before = snapshot(&store, b"");
    let failing = failing_store(store.clone(), FailurePoint::SecondWrite);
    assert_index_recovery(failing.clone(), "index ID counter exhausted");
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 0);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_missing_index_competition_preserves_competing_commit() {
    let (store, _) = fixture(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE); \
         INSERT INTO items VALUES (1, 10), (2, 20);",
    );
    let (key, bytes) = snapshot(&store, INDEXES_PREFIX)
        .into_iter()
        .find(|(_, bytes)| {
            let index: PersistedIndexMeta = bincode::deserialize(bytes).unwrap();
            index.table == "items" && index.unique && index.name != "__pk_items"
        })
        .expect("fixture must contain a real UNIQUE definition");
    super::missing_unique::remove_unique_indexes(&store);
    let mut expected = snapshot(&store, b"");
    assert!(!expected.iter().any(|(existing, _)| existing == &key));
    let original_meta = expected
        .iter()
        .find(|(key, _)| key == META_KEY)
        .unwrap()
        .1
        .clone();
    expected.push((key.clone(), bytes.clone()));
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    // Only the missing index key changes from None to Some; META stays identical,
    // so this must reach the index check rather than the earlier counter check.
    let failing = failing_store(store.clone(), FailurePoint::IndexBeforeRepair(key, bytes));
    assert_index_recovery(failing.clone(), "index metadata changed during recovery");
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 0);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), expected);
    assert_eq!(
        store
            .begin(TxnMode::ReadOnly)
            .unwrap()
            .get(&META_KEY.to_vec())
            .unwrap(),
        Some(original_meta)
    );
}

// Synthetic metadata seam, separate from authenticated v0.8.15 fixture evidence.
fn legacy_pk_store(missing_pk: bool) -> Arc<MemoryKV> {
    let (store, catalog) = fixture(
        "CREATE TABLE items (id INTEGER, value INTEGER, PRIMARY KEY(id)); \
         INSERT INTO items VALUES (1,10),(2,20); \
         CREATE TABLE neighbor (id INTEGER PRIMARY KEY); INSERT INTO neighbor VALUES (7);",
    );
    let mut table = catalog.read().unwrap().get_table("items").unwrap().clone();
    table.columns[0].not_null = false;
    if missing_pk {
        table.primary_key = None;
    }
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    catalog
        .write()
        .unwrap()
        .persist_create_table(&mut txn, &table)
        .unwrap();
    if missing_pk {
        let indexes: Vec<_> = txn.scan_prefix(INDEXES_PREFIX).unwrap().collect();
        for (key, bytes) in indexes {
            let index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
            if index.table == "items" && index.name == "__pk_items" {
                let entries: Vec<_> = txn
                    .scan_prefix(&KeyEncoder::index_prefix(index.index_id))
                    .unwrap()
                    .map(|(key, _)| key)
                    .collect();
                for key in entries {
                    txn.delete(key).unwrap();
                }
                txn.delete(key).unwrap();
            }
        }
    }
    txn.commit_self().unwrap();
    store
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_metadata_only_commit_failure_preserves_all_bytes() {
    let store = legacy_pk_store(false);
    let before = snapshot(&store, b"");
    let failing = failing_store(store.clone(), FailurePoint::BeforeCommit);
    assert_index_recovery(failing.clone(), "injected recovery pre-commit failure");
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 1);
    assert_eq!(
        failing.staged.load(Ordering::Relaxed),
        1,
        "only the table DTO needs a write"
    );
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_metadata_and_index_commit_failure_preserves_all_bytes() {
    let store = legacy_pk_store(true);
    let before = snapshot(&store, b"");
    let failing = failing_store(store.clone(), FailurePoint::BeforeCommit);
    assert_index_recovery(failing.clone(), "injected recovery pre-commit failure");
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 1);
    assert!(failing.staged.load(Ordering::Relaxed) > 1);
    assert_eq!(snapshot(&store, b""), before);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_table_competition_preserves_competing_commit() {
    let store = legacy_pk_store(false);
    let (key, mut table) = snapshot(&store, TABLES_PREFIX)
        .into_iter()
        .find_map(|(key, bytes)| {
            let table: PersistedTableMeta = bincode_config().deserialize(&bytes).unwrap();
            (table.name == "items").then_some((key, table))
        })
        .unwrap();
    table
        .properties
        .insert("app.revision".into(), "concurrent".into());
    let bytes = bincode_config().serialize(&table).unwrap();
    let mut expected = snapshot(&store, b"");
    expected
        .iter_mut()
        .find(|(existing, _)| existing == &key)
        .unwrap()
        .1 = bytes.clone();
    let failing = failing_store(store.clone(), FailurePoint::TableBeforeRepair(key, bytes));
    assert_index_recovery(failing.clone(), "table metadata changed during recovery");
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 0);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), expected);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_supporting_index_competition_preserves_competing_commit() {
    let store = legacy_pk_store(false);
    let (key, mut index) = snapshot(&store, INDEXES_PREFIX)
        .into_iter()
        .find_map(|(key, bytes)| {
            let index: PersistedIndexMeta = bincode::deserialize(&bytes).unwrap();
            (index.name == "__pk_items").then_some((key, index))
        })
        .unwrap();
    index.unique = false;
    let bytes = bincode::serialize(&index).unwrap();
    let mut expected = snapshot(&store, b"");
    expected
        .iter_mut()
        .find(|(existing, _)| existing == &key)
        .unwrap()
        .1 = bytes.clone();
    let failing = failing_store(store.clone(), FailurePoint::IndexBeforeRepair(key, bytes));
    assert_index_recovery(
        failing.clone(),
        "supporting index metadata changed during recovery",
    );
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 0);
    assert_eq!(failing.commits.load(Ordering::Relaxed), 0);
    assert_eq!(snapshot(&store, b""), expected);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_invalid_metadata_is_rejected_without_writes() {
    use alopex_sql::ast::{Span, ddl::TableConstraint};
    for case in [
        "inconsistent",
        "multiple",
        "missing_column",
        "duplicate_column",
        "empty",
    ] {
        let (store, catalog) = fixture(
            "CREATE TABLE items (id INTEGER, value INTEGER, PRIMARY KEY(id)); INSERT INTO items VALUES (1,10)",
        );
        let mut table = catalog.read().unwrap().get_table("items").unwrap().clone();
        let columns = match case {
            "missing_column" => vec!["absent".into()],
            "duplicate_column" => vec!["id".into(), "id".into()],
            "empty" => vec![],
            _ => vec!["id".into()],
        };
        table.primary_key = Some(if case == "inconsistent" {
            vec!["value".into()]
        } else {
            columns.clone()
        });
        table.constraints = vec![TableConstraint::PrimaryKey {
            name: None,
            columns,
            span: Span::default(),
        }];
        if case == "multiple" {
            table.constraints.push(table.constraints[0].clone());
        }
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog
            .write()
            .unwrap()
            .persist_create_table(&mut txn, &table)
            .unwrap();
        txn.commit_self().unwrap();
        let before = snapshot(&store, b"");
        let failing = failing_store(store.clone(), FailurePoint::SecondWrite);
        assert_index_recovery(failing.clone(), "inconsistent primary key metadata");
        assert_eq!(
            failing.write_transactions.load(Ordering::Relaxed),
            0,
            "{case}"
        );
        assert_eq!(snapshot(&store, b""), before, "{case}");
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn issue585_pk_normalized_reload_does_not_write() {
    for definition in ["id INTEGER PRIMARY KEY", "id INTEGER, PRIMARY KEY(id)"] {
        let (store, _) = fixture(&format!(
            "CREATE TABLE items ({definition}); INSERT INTO items VALUES (1)"
        ));
        let before = snapshot(&store, b"");
        let failing = failing_store(store.clone(), FailurePoint::SecondWrite);
        PersistentCatalog::load(failing.clone()).unwrap();
        assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 0);
        assert_eq!(snapshot(&store, b""), before);
    }
}
