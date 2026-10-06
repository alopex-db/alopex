use super::*;
use alopex_core::types::{Key, TxnId, Value};

struct FailingStore {
    inner: Arc<MemoryKV>,
    write_transactions: AtomicUsize,
    attempts: AtomicUsize,
    staged: AtomicUsize,
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
            self.write_transactions.fetch_add(1, Ordering::Relaxed);
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
        if self.store.attempts.fetch_add(1, Ordering::Relaxed) == 1 {
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
    });
    assert!(PersistentCatalog::load(failing.clone()).is_err());
    assert_eq!(failing.write_transactions.load(Ordering::Relaxed), 1);
    assert_eq!(failing.attempts.load(Ordering::Relaxed), 2);
    assert_eq!(failing.staged.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot(&store, b""), before);
}
