use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use pyo3::prelude::*;
use pyo3::types::PyModule;
#[cfg(feature = "numpy")]
use pyo3::PyObject;

use crate::embedded::transaction::{PyTransaction, PyTransactionInner};
use crate::error;
#[cfg(feature = "numpy")]
use crate::types::PySearchResult;
use crate::types::{PyEmbeddedConfig, PyHnswConfig, PyHnswStats, PyMemoryStats, PyTxnMode};
#[cfg(feature = "numpy")]
use crate::vector;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
static ROLLBACK_FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn inject_rollback_failure_once() {
    ROLLBACK_FAIL_COUNT.store(1, Ordering::SeqCst);
}

#[pyclass(name = "Database")]
pub struct PyDatabase {
    inner: Option<Arc<alopex_embedded::Database>>,
    mode: alopex_embedded::StorageMode,
    closed: bool,
    txns: Arc<Mutex<Vec<Weak<PyTransactionInner>>>>,
}

impl PyDatabase {
    fn from_db(db: alopex_embedded::Database, mode: alopex_embedded::StorageMode) -> Self {
        Self {
            inner: Some(Arc::new(db)),
            mode,
            closed: false,
            txns: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn ensure_open(&self) -> PyResult<Arc<alopex_embedded::Database>> {
        if self.closed {
            return Err(error::to_py_err("database is closed"));
        }
        self.inner
            .as_ref()
            .cloned()
            .ok_or_else(|| error::to_py_err("database is closed"))
    }
}

#[pymethods]
impl PyDatabase {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = alopex_embedded::Database::open(Path::new(path)).map_err(error::embedded_err)?;
        Ok(Self::from_db(db, alopex_embedded::StorageMode::Disk))
    }

    #[staticmethod]
    fn new() -> PyResult<Self> {
        Ok(Self::from_db(
            alopex_embedded::Database::new(),
            alopex_embedded::StorageMode::InMemory,
        ))
    }

    #[staticmethod]
    fn open_in_memory() -> PyResult<Self> {
        let db = alopex_embedded::Database::open_in_memory().map_err(error::embedded_err)?;
        Ok(Self::from_db(db, alopex_embedded::StorageMode::InMemory))
    }

    #[staticmethod]
    fn open_with_config(config: PyEmbeddedConfig) -> PyResult<Self> {
        let embedded = config.to_embedded();
        if embedded.storage_mode != alopex_embedded::StorageMode::InMemory {
            return Err(error::to_py_err(
                "open_with_config supports in-memory mode only",
            ));
        }
        let db =
            alopex_embedded::Database::open_with_config(embedded).map_err(error::embedded_err)?;
        Ok(Self::from_db(db, alopex_embedded::StorageMode::InMemory))
    }

    #[pyo3(signature = (mode = None))]
    fn begin(&self, mode: Option<PyTxnMode>) -> PyResult<PyTransaction> {
        let db = self.ensure_open()?;
        let txn_mode = mode.unwrap_or_default().into();
        let mut guard = self
            .txns
            .lock()
            .map_err(|_| error::to_py_err("transaction tracking lock poisoned"))?;
        let txn = db.begin(txn_mode).map_err(error::embedded_err)?;
        let py_txn = PyTransaction::from_txn(Arc::clone(&db), txn);
        guard.push(Arc::downgrade(&py_txn.inner));
        Ok(py_txn)
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.ensure_open()?;
        match self.mode {
            alopex_embedded::StorageMode::Disk => {
                py.allow_threads(|| db.flush()).map_err(error::embedded_err)
            }
            alopex_embedded::StorageMode::InMemory => {
                Err(error::to_py_err("flush is only supported in disk mode"))
            }
        }
    }

    fn memory_usage(&self) -> PyResult<PyMemoryStats> {
        let db = self.ensure_open()?;
        if self.mode == alopex_embedded::StorageMode::Disk {
            return Ok(PyMemoryStats::with_total(0, 0));
        }
        match db.memory_usage() {
            Some(stats) => Ok(PyMemoryStats::from(stats)),
            None => Ok(PyMemoryStats::with_total(0, 0)),
        }
    }

    fn close(&mut self) -> PyResult<()> {
        if self.closed {
            return Err(error::to_py_err("database is closed"));
        }
        let txns = self.txns.clone();
        let mut guard = txns
            .lock()
            .map_err(|_| error::to_py_err("transaction tracking lock poisoned"))?;
        let mut first_err: Option<PyErr> = None;
        guard.retain(|weak| {
            if let Some(handle) = weak.upgrade() {
                let mut txn_guard = match handle.txn.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        if first_err.is_none() {
                            first_err = Some(error::to_py_err("transaction lock poisoned"));
                        }
                        return true;
                    }
                };
                #[cfg(test)]
                if ROLLBACK_FAIL_COUNT
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                        if count > 0 {
                            Some(count - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
                    if first_err.is_none() {
                        first_err = Some(error::to_py_err("ロールバック失敗（テスト注入）"));
                    }
                    return true;
                }
                if let Some(txn) = txn_guard.as_mut() {
                    if let Err(err) = txn.rollback_in_place() {
                        if first_err.is_none() {
                            first_err = Some(error::embedded_err(err));
                        }
                        return true;
                    }
                    *txn_guard = None;
                    false
                } else {
                    false
                }
            } else {
                false
            }
        });
        if let Some(err) = first_err {
            return Err(err);
        }
        self.closed = true;
        self.inner = None;
        Ok(())
    }

    fn create_hnsw_index(&self, name: &str, config: PyHnswConfig) -> PyResult<()> {
        let db = self.ensure_open()?;
        db.create_hnsw_index(name, config.into())
            .map_err(error::embedded_err)
    }

    #[cfg(feature = "numpy")]
    #[pyo3(signature = (name, query, k, ef_search = None))]
    fn search_hnsw(
        &self,
        py: Python<'_>,
        name: &str,
        query: PyObject,
        k: usize,
        ef_search: Option<usize>,
    ) -> PyResult<(Vec<PySearchResult>, PyHnswStats)> {
        let db = self.ensure_open()?;
        vector::require_numpy(py)?;
        let query = query.bind(py);
        vector::with_ndarray_f32(query, |values| {
            let name = name.to_string();
            let values = values.to_vec();
            let (results, _stats) = py
                .allow_threads(|| db.search_hnsw(&name, &values, k, ef_search))
                .map_err(error::embedded_err)?;
            let stats = db.get_hnsw_stats(&name).map_err(error::embedded_err)?;
            let results = results.into_iter().map(PySearchResult::from).collect();
            Ok((results, PyHnswStats::from(stats)))
        })
    }

    fn drop_hnsw_index(&self, name: &str) -> PyResult<()> {
        let db = self.ensure_open()?;
        db.drop_hnsw_index(name).map_err(error::embedded_err)
    }

    fn get_hnsw_stats(&self, name: &str) -> PyResult<PyHnswStats> {
        let db = self.ensure_open()?;
        db.get_hnsw_stats(name)
            .map(PyHnswStats::from)
            .map_err(error::embedded_err)
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::inject_rollback_failure_once;
    use super::PyDatabase;

    #[test]
    fn close_rolls_back_tracked_transactions_and_cleans_up() {
        let mut db = PyDatabase::new().expect("db");
        let txn1 = db.begin(None).expect("txn1");
        let txn2 = db.begin(None).expect("txn2");

        {
            let mut guard = txn1.inner.txn.lock().expect("transaction lock poisoned");
            guard.take();
        }

        db.close().expect("close");

        assert!(db.closed);
        assert!(db.inner.is_none());
        assert!(db
            .txns
            .lock()
            .expect("transaction list lock poisoned")
            .is_empty());

        assert!(txn2
            .inner
            .txn
            .lock()
            .expect("transaction lock poisoned")
            .is_none());
    }

    #[test]
    fn close_retry_keeps_tracked_transactions_on_failure() {
        let mut db = PyDatabase::new().expect("db");
        let _txn = db.begin(None).expect("txn");

        inject_rollback_failure_once();
        db.close().expect_err("close should fail once");

        assert!(!db
            .txns
            .lock()
            .expect("transaction list lock poisoned")
            .is_empty());

        db.close().expect("close retry");
        assert!(db
            .txns
            .lock()
            .expect("transaction list lock poisoned")
            .is_empty());
    }
}
