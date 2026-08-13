use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

use crate::embedded::transaction::{PyTransaction, PyTransactionInner};
use crate::error;
use crate::types::{PyEmbeddedConfig, PyMemoryStats, PyTxnMode};
use crate::types::{PyHnswConfig, PyHnswStats, PySearchResult};
use crate::vector;
use crate::vector::SliceOrOwned;

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

    /// SQL を実行する（auto-commit）。
    ///
    /// Args:
    ///     sql: 実行する SQL 文字列。
    ///     params: `?` プレースホルダへ順番に割り当てる値の list / tuple。
    ///
    /// Returns:
    ///     SELECT: 行の list（各行は列名 -> 値の dict、列順を保持）。
    ///     INSERT/UPDATE/DELETE: 影響行数 (int)。
    ///     DDL: None。
    ///
    /// Raises:
    ///     ValueError: プレースホルダ数とパラメータ数の不一致、不正なパラメータ値。
    ///     TypeError: 未対応のパラメータ型。
    ///     NotImplementedError: bytes パラメータ（BLOB リテラルは SQL パーサー未対応）。
    ///     AlopexError: SQL の解析・実行エラー（`code` に ALOPEX-P/S/C/E### を設定）。
    #[pyo3(signature = (sql, params = None))]
    fn execute_sql(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let db = self.ensure_open()?;
        let bound_sql = crate::embedded::sql::bind_params(sql, params.as_ref())?;
        let result = py
            .detach(move || db.execute_sql(&bound_sql))
            .map_err(error::embedded_err)?;
        crate::embedded::sql::execution_result_to_py(py, result)
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
                py.detach(|| db.flush()).map_err(error::embedded_err)
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

    fn cluster_status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let db = self.ensure_open()?;
        let snapshot = db.cluster_status_snapshot().map_err(error::embedded_err)?;
        crate::types::cluster::cluster_status_to_py(py, &snapshot)
    }

    fn routing_diagnostics(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let db = self.ensure_open()?;
        let diagnostics = db.routing_diagnostics().map_err(error::embedded_err)?;
        crate::types::cluster::routing_diagnostics_to_py(py, &diagnostics)
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

    #[pyo3(signature = (name, query, k, ef_search = None))]
    fn search_hnsw(
        &self,
        py: Python<'_>,
        name: &str,
        query: Py<PyAny>,
        k: usize,
        ef_search: Option<usize>,
    ) -> PyResult<(Vec<PySearchResult>, PyHnswStats)> {
        let db = self.ensure_open()?;
        vector::require_numpy(py)?;
        let name = name.to_string();
        vector::with_ndarray_f32_gil_safe(query.bind(py), |slice_or_owned| {
            let db_clone = Arc::clone(&db);
            let name_clone = name.clone();
            let (results, _stats) = match slice_or_owned {
                SliceOrOwned::Borrowed { ptr, len, _guard } => {
                    let _guard = _guard;
                    let ptr = ptr as usize;
                    py.detach(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        db_clone.search_hnsw(&name_clone, values, k, ef_search)
                    })
                }
                SliceOrOwned::Owned(vec) => {
                    py.detach(move || db_clone.search_hnsw(&name_clone, &vec, k, ef_search))
                }
            }
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
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
    use pyo3::IntoPyObjectExt;

    use super::inject_rollback_failure_once;
    use super::PyDatabase;

    fn with_py<F: FnOnce(Python<'_>)>(f: F) {
        pyo3::Python::initialize();
        Python::attach(f);
    }

    #[test]
    fn execute_sql_ddl_dml_select_end_to_end() {
        with_py(|py| {
            let db = PyDatabase::new().expect("db");

            let ddl = db
                .execute_sql(
                    py,
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
                    None,
                )
                .expect("ddl");
            assert!(ddl.is_none(py));

            let params = PyList::empty(py);
            params.append(1i64).expect("append id");
            params.append("Alice").expect("append name");
            let affected = db
                .execute_sql(
                    py,
                    "INSERT INTO users (id, name) VALUES (?, ?)",
                    Some(params.into_any()),
                )
                .expect("insert");
            assert_eq!(affected.extract::<u64>(py).expect("affected"), 1);

            let params = PyList::empty(py);
            params.append(1i64).expect("append id");
            let rows = db
                .execute_sql(
                    py,
                    "SELECT id, name FROM users WHERE id = ?",
                    Some(params.into_any()),
                )
                .expect("select");
            let rows = rows.bind(py).cast::<PyList>().expect("list").clone();
            assert_eq!(rows.len(), 1);
            let row = rows.get_item(0).expect("row");
            let row = row.cast::<PyDict>().expect("dict");
            let id = row.get_item("id").expect("get id").expect("id present");
            assert_eq!(id.extract::<i64>().expect("id int"), 1);
            let name = row
                .get_item("name")
                .expect("get name")
                .expect("name present");
            assert_eq!(name.extract::<String>().expect("name str"), "Alice");
        });
    }

    #[test]
    fn execute_sql_select_empty_returns_empty_list() {
        with_py(|py| {
            let db = PyDatabase::new().expect("db");
            db.execute_sql(py, "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
                .expect("ddl");
            let rows = db
                .execute_sql(py, "SELECT id FROM t", None)
                .expect("select");
            let rows = rows.bind(py).cast::<PyList>().expect("list").clone();
            assert_eq!(rows.len(), 0);
        });
    }

    #[test]
    fn execute_sql_parse_error_maps_to_alopex_error_with_sql_code() {
        with_py(|py| {
            let db = PyDatabase::new().expect("db");
            let err = db
                .execute_sql(py, "SELEKT 1 FRUM nowhere", None)
                .expect_err("parse error");
            assert!(err.is_instance_of::<crate::error::PyAlopexError>(py));
            let code: String = err
                .value(py)
                .getattr("code")
                .expect("code attr")
                .extract()
                .expect("code str");
            assert!(code.starts_with("ALOPEX-"), "unexpected code: {code}");
        });
    }

    #[test]
    fn execute_sql_on_closed_database_is_error() {
        with_py(|py| {
            let mut db = PyDatabase::new().expect("db");
            db.close().expect("close");
            let err = db.execute_sql(py, "SELECT 1", None).expect_err("closed");
            assert!(err.is_instance_of::<crate::error::PyAlopexError>(py));
        });
    }

    #[test]
    fn execute_sql_vector_param_roundtrip() {
        with_py(|py| {
            let db = PyDatabase::new().expect("db");
            db.execute_sql(
                py,
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
                None,
            )
            .expect("ddl");

            let vector = PyList::new(py, [0.25f64, -1.5, 2.0]).expect("vector");
            let params = PyList::empty(py);
            params.append(1i64).expect("append id");
            params.append(vector).expect("append vector");
            db.execute_sql(
                py,
                "INSERT INTO docs (id, embedding) VALUES (?, ?)",
                Some(params.into_any()),
            )
            .expect("insert");

            let params = PyList::empty(py);
            params.append(1i64).expect("append id");
            let rows = db
                .execute_sql(
                    py,
                    "SELECT embedding FROM docs WHERE id = ?",
                    Some(params.into_any()),
                )
                .expect("select");
            let rows = rows.bind(py).cast::<PyList>().expect("list").clone();
            assert_eq!(rows.len(), 1);
            let row = rows.get_item(0).expect("row");
            let row = row.cast::<PyDict>().expect("dict");
            let embedding = row
                .get_item("embedding")
                .expect("get embedding")
                .expect("embedding present");
            let embedding: Vec<f64> = embedding.extract().expect("vec f64");
            assert_eq!(embedding, vec![0.25, -1.5, 2.0]);
        });
    }

    #[test]
    fn execute_sql_rejects_scalar_params() {
        with_py(|py| {
            let db = PyDatabase::new().expect("db");
            db.execute_sql(py, "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
                .expect("ddl");
            let scalar = 1i64.into_bound_py_any(py).expect("int");
            let err = db
                .execute_sql(py, "SELECT id FROM t WHERE id = ?", Some(scalar))
                .expect_err("scalar params");
            assert!(err.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        });
    }

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
