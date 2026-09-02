use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

use crate::embedded::async_stream::PyNativeAsyncSqlResultStream;
use crate::embedded::local_scan::PyLocalScan;
use crate::embedded::stream::{PySqlResultStream, StreamLeaseRegistry};
use crate::embedded::thread_mode::{DatabaseControl, PyThreadMode, ThreadMode};
use crate::embedded::transaction::{PyTransaction, PyTransactionInner};
use crate::error;
use crate::types::{DataFrameStreamRegistry, PyEmbeddedConfig, PyMemoryStats, PyTxnMode};
use crate::types::{PyHnswConfig, PyHnswStats, PySearchResult, PySearchStats};
use crate::vector;
use crate::vector::SliceOrOwned;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[pyclass(name = "Database")]
pub struct PyDatabase {
    inner: Option<Arc<alopex_embedded::Database>>,
    mode: alopex_embedded::StorageMode,
    control: Arc<DatabaseControl>,
    streams: Arc<StreamLeaseRegistry>,
    dataframe_streams: Arc<DataFrameStreamRegistry>,
    txns: Arc<Mutex<Vec<Weak<PyTransactionInner>>>>,
    #[cfg(test)]
    rollback_fail_count: AtomicUsize,
}

#[pyclass(name = "PreparedStatement")]
pub struct PyPreparedStatement {
    database: Arc<alopex_embedded::Database>,
    sql: String,
    bindings: Vec<Option<String>>,
    finalized: bool,
}

#[pymethods]
impl PyPreparedStatement {
    fn parameter_count(&self) -> usize {
        self.bindings.len()
    }

    fn bind(&mut self, index: usize, value: Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_open()?;
        let count = self.bindings.len();
        let slot = index
            .checked_sub(1)
            .and_then(|index| self.bindings.get_mut(index))
            .ok_or_else(|| {
                error::to_py_err(format!("parameter index {index} is outside 1..={count}"))
            })?;
        *slot = Some(crate::embedded::sql::render_param(&value, index - 1)?);
        Ok(())
    }

    fn reset(&mut self) -> PyResult<()> {
        self.ensure_open()?;
        self.bindings.fill(None);
        Ok(())
    }

    fn finalize(&mut self) -> PyResult<()> {
        self.ensure_open()?;
        self.bindings.clear();
        self.finalized = true;
        Ok(())
    }

    fn execute(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let rendered = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .clone()
                    .ok_or_else(|| error::to_py_err(format!("parameter ?{} is unbound", index + 1)))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let sql = crate::embedded::sql::bind_rendered_params(&self.sql, &rendered)?;
        let database = Arc::clone(&self.database);
        let result = py
            .detach(move || database.execute_sql(&sql))
            .map_err(error::embedded_err)?;
        crate::embedded::sql::execution_result_to_py(py, result)
    }
}

impl PyPreparedStatement {
    fn ensure_open(&self) -> PyResult<()> {
        if self.finalized {
            Err(error::to_py_err("prepared statement is finalized"))
        } else {
            Ok(())
        }
    }
}

impl PyDatabase {
    #[cfg(test)]
    fn inject_rollback_failure_once(&self) {
        self.rollback_fail_count.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn consume_rollback_failure(&self) -> bool {
        let mut count = self.rollback_fail_count.load(Ordering::SeqCst);
        loop {
            if count == 0 {
                return false;
            }
            match self.rollback_fail_count.compare_exchange_weak(
                count,
                count - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(observed) => count = observed,
            }
        }
    }

    fn from_db(
        db: alopex_embedded::Database,
        mode: alopex_embedded::StorageMode,
        thread_mode: ThreadMode,
    ) -> Self {
        Self {
            inner: Some(Arc::new(db)),
            mode,
            control: Arc::new(DatabaseControl::new(thread_mode)),
            streams: Arc::new(StreamLeaseRegistry::default()),
            dataframe_streams: Arc::new(DataFrameStreamRegistry::default()),
            txns: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            rollback_fail_count: AtomicUsize::new(0),
        }
    }

    fn ensure_open(&self) -> PyResult<Arc<alopex_embedded::Database>> {
        self.control.ensure_open()?;
        self.inner
            .as_ref()
            .cloned()
            .ok_or_else(|| error::to_py_err("database is closed"))
    }
}

#[pymethods]
impl PyDatabase {
    #[staticmethod]
    #[pyo3(signature = (path, *, thread_mode = None))]
    fn open(path: &str, thread_mode: Option<&str>) -> PyResult<Self> {
        let thread_mode = ThreadMode::parse(thread_mode)?;
        let db = alopex_embedded::Database::open(Path::new(path)).map_err(error::embedded_err)?;
        Ok(Self::from_db(
            db,
            alopex_embedded::StorageMode::Disk,
            thread_mode,
        ))
    }

    #[staticmethod]
    #[pyo3(signature = (*, thread_mode = None))]
    fn new(thread_mode: Option<&str>) -> PyResult<Self> {
        let thread_mode = ThreadMode::parse(thread_mode)?;
        Ok(Self::from_db(
            alopex_embedded::Database::new(),
            alopex_embedded::StorageMode::InMemory,
            thread_mode,
        ))
    }

    #[staticmethod]
    #[pyo3(signature = (*, thread_mode = None))]
    fn open_in_memory(thread_mode: Option<&str>) -> PyResult<Self> {
        let thread_mode = ThreadMode::parse(thread_mode)?;
        let db = alopex_embedded::Database::open_in_memory().map_err(error::embedded_err)?;
        Ok(Self::from_db(
            db,
            alopex_embedded::StorageMode::InMemory,
            thread_mode,
        ))
    }

    #[staticmethod]
    #[pyo3(signature = (config, *, thread_mode = None))]
    fn open_with_config(config: PyEmbeddedConfig, thread_mode: Option<&str>) -> PyResult<Self> {
        let thread_mode = ThreadMode::parse(thread_mode)?;
        let embedded = config.to_embedded();
        if embedded.storage_mode != alopex_embedded::StorageMode::InMemory {
            return Err(error::to_py_err(
                "open_with_config supports in-memory mode only",
            ));
        }
        let db =
            alopex_embedded::Database::open_with_config(embedded).map_err(error::embedded_err)?;
        Ok(Self::from_db(
            db,
            alopex_embedded::StorageMode::InMemory,
            thread_mode,
        ))
    }

    #[getter]
    fn thread_mode(&self) -> PyResult<PyThreadMode> {
        self.control.ensure_open()?;
        Ok(self.control.thread_mode().into())
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

    /// Prepare one SQL statement with one-based positional `?` parameters.
    fn prepare(&self, sql: &str) -> PyResult<PyPreparedStatement> {
        let database = self.ensure_open()?;
        let parameter_count = database
            .prepare(sql)
            .map_err(error::embedded_err)?
            .parameter_count();
        Ok(PyPreparedStatement {
            database,
            sql: sql.to_owned(),
            bindings: vec![None; parameter_count],
            finalized: false,
        })
    }

    /// Open a local-only, incrementally consumed SQL result stream.
    ///
    /// The stream accepts the documented read-only SELECT subset only. Unsupported SQL is
    /// rejected before an owned session or native cursor is opened.
    #[pyo3(signature = (sql, params = None, *, resource_limit_bytes = None, timeout = None))]
    fn execute_sql_stream(
        &self,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<PySqlResultStream> {
        let db = self.ensure_open()?;
        let bound_sql = crate::embedded::sql::bind_params(sql, params.as_ref())?;
        let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(&db, &bound_sql)
            .map_err(error::embedded_err)?;
        PySqlResultStream::open_database(
            &db,
            self.control.clone(),
            &self.streams,
            plan,
            resource_limit_bytes,
            timeout,
        )
    }

    /// Internal factory for the documented Python asyncio facade.
    ///
    /// This deliberately constructs the native bridge before exposing any Python iterator.  The
    /// async buffer options are validated before the owned SQL lease is opened.
    #[pyo3(
        name = "_open_native_async_sql_stream",
        signature = (sql, params = None, *, resource_limit_bytes = None, timeout = None, prefetch_batches = 1, max_buffered_batches = 1, consumer_idle_timeout = None)
    )]
    #[allow(clippy::too_many_arguments)]
    fn open_native_async_sql_stream(
        &self,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<PyNativeAsyncSqlResultStream> {
        PyNativeAsyncSqlResultStream::validate_options(
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )?;
        let db = self.ensure_open()?;
        let bound_sql = crate::embedded::sql::bind_params(sql, params.as_ref())?;
        let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(&db, &bound_sql)
            .map_err(error::embedded_err)?;
        let stream = PySqlResultStream::open_database(
            &db,
            self.control.clone(),
            &self.streams,
            plan,
            resource_limit_bytes,
            timeout,
        )?;
        PyNativeAsyncSqlResultStream::new(
            stream,
            self.control.thread_mode(),
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    /// Internal async factory for every canonical embedded-local `LocalScan` variant.
    #[pyo3(
        name = "_open_native_async_query_stream",
        signature = (scan, *, resource_limit_bytes = None, timeout = None, prefetch_batches = 1, max_buffered_batches = 1, consumer_idle_timeout = None)
    )]
    fn open_native_async_query_stream(
        &self,
        scan: PyRef<'_, PyLocalScan>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<PyNativeAsyncSqlResultStream> {
        let database = self.ensure_open()?;
        scan.open_native_async_database(
            &database,
            self.control.clone(),
            &self.streams,
            &self.dataframe_streams,
            resource_limit_bytes,
            timeout,
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    /// Open one preflight-validated embedded-local scan stream.
    #[pyo3(signature = (scan, *, resource_limit_bytes = None, timeout = None))]
    fn query_stream(
        &self,
        py: Python<'_>,
        scan: PyRef<'_, PyLocalScan>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        let database = self.ensure_open()?;
        scan.open_database(
            py,
            &database,
            self.control.clone(),
            &self.streams,
            &self.dataframe_streams,
            resource_limit_bytes,
            timeout,
        )
    }

    #[pyo3(signature = (mode = None))]
    fn begin(&self, mode: Option<PyTxnMode>) -> PyResult<PyTransaction> {
        let db = self.ensure_open()?;
        let txn_mode = mode.unwrap_or_default().into();
        let mut guard = self
            .txns
            .lock()
            .map_err(|_| error::to_py_err("transaction tracking lock poisoned"))?;
        let py_txn = PyTransaction::begin_with_control(
            Arc::clone(&db),
            txn_mode,
            self.control.clone(),
            self.streams.clone(),
        )?;
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

    /// Return diagnostics owned by this embedded database instance only.
    ///
    /// This method neither accepts a server target nor creates an HTTP client;
    /// its `cluster_control` result is therefore an unavailable local
    /// diagnostic, never a claim that multi-node management is configured.
    fn cluster_status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let db = self.ensure_open()?;
        let snapshot = db.cluster_status_snapshot().map_err(error::embedded_err)?;
        crate::types::cluster::cluster_status_to_py(py, &snapshot)
    }

    /// Return the latest local routing diagnostics without any remote session.
    fn routing_diagnostics(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let db = self.ensure_open()?;
        let diagnostics = db.routing_diagnostics().map_err(error::embedded_err)?;
        crate::types::cluster::routing_diagnostics_to_py(py, &diagnostics)
    }

    fn close(&mut self) -> PyResult<()> {
        if !self.control.begin_close()? {
            return Ok(());
        }
        if let Err(err) = self.streams.close_all() {
            self.control.reopen_after_close_failure()?;
            return Err(err);
        }
        if let Err(err) = self.dataframe_streams.close_all() {
            self.control.reopen_after_close_failure()?;
            return Err(err);
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
                if self.consume_rollback_failure() {
                    if first_err.is_none() {
                        first_err = Some(error::to_py_err("ロールバック失敗（テスト注入）"));
                    }
                    return true;
                }
                if let Some(txn) = txn_guard.as_mut() {
                    if let Err(err) = txn.rollback() {
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
            self.control.reopen_after_close_failure()?;
            return Err(err);
        }
        drop(guard);
        // Converge into the single `.alopex` file before releasing the handle, so a
        // convergence failure surfaces as a Python exception instead of being
        // swallowed by the best-effort `Drop` path.
        if let Some(db) = self.inner.as_ref() {
            if let Err(err) = db.close() {
                self.control.reopen_after_close_failure()?;
                return Err(error::embedded_err(err));
            }
        }
        self.inner = None;
        self.control.finish_close()?;
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
    ) -> PyResult<(Vec<PySearchResult>, PySearchStats)> {
        let db = self.ensure_open()?;
        vector::require_numpy(py)?;
        let name = name.to_string();
        vector::with_ndarray_f32_gil_safe(query.bind(py), |slice_or_owned| {
            let db_clone = Arc::clone(&db);
            let name_clone = name.clone();
            let (results, stats) = match slice_or_owned {
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
            let results = results.into_iter().map(PySearchResult::from).collect();
            Ok((results, PySearchStats::from(stats)))
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
    m.add_class::<PyThreadMode>()?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyPreparedStatement>()?;
    m.add_class::<PySqlResultStream>()?;
    m.add_class::<PyLocalScan>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
    use pyo3::IntoPyObjectExt;

    use super::PyDatabase;

    fn with_py<F: FnOnce(Python<'_>)>(f: F) {
        pyo3::Python::initialize();
        Python::attach(f);
    }

    #[test]
    fn execute_sql_ddl_dml_select_end_to_end() {
        with_py(|py| {
            let db = PyDatabase::new(None).expect("db");

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
            let db = PyDatabase::new(None).expect("db");
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
    fn prepared_statement_python_surface_rebinds_and_finalizes() {
        with_py(|py| {
            let db = PyDatabase::new(None).expect("db");
            db.execute_sql(py, "CREATE TABLE prepared (id INTEGER PRIMARY KEY)", None)
                .unwrap();
            let mut statement = db.prepare("INSERT INTO prepared (id) VALUES (?)").unwrap();
            assert_eq!(statement.parameter_count(), 1);
            let one = 1i64.into_py_any(py).unwrap();
            statement.bind(1, one.bind(py).clone()).unwrap();
            statement.execute(py).unwrap();
            statement.reset().unwrap();
            let two = 2i64.into_py_any(py).unwrap();
            statement.bind(1, two.bind(py).clone()).unwrap();
            statement.execute(py).unwrap();
            statement.finalize().unwrap();
            assert!(statement.execute(py).is_err());
        });
    }

    #[test]
    fn python_database_sql_stream_reads_rows_written_by_detached_auto_commit_calls() {
        with_py(|py| {
            let db = Py::new(py, PyDatabase::new(None).expect("db")).expect("python database");
            let db = db.bind(py);
            db.call_method1(
                "execute_sql",
                ("CREATE TABLE stream_users (id INTEGER PRIMARY KEY)",),
            )
            .expect("ddl");
            db.call_method1(
                "execute_sql",
                ("INSERT INTO stream_users (id) VALUES (1), (2)",),
            )
            .expect("dml");

            let stream = db
                .call_method1("execute_sql_stream", ("SELECT id FROM stream_users",))
                .expect("stream");
            let first = stream.call_method0("__next__").expect("first streamed row");
            let first = first.cast::<PyDict>().expect("row dictionary");
            assert_eq!(
                first
                    .get_item("id")
                    .expect("dictionary access")
                    .expect("id")
                    .extract::<i64>()
                    .expect("integer id"),
                1
            );
        });
    }

    #[test]
    fn execute_sql_parse_error_maps_to_alopex_error_with_sql_code() {
        with_py(|py| {
            let db = PyDatabase::new(None).expect("db");
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
            let mut db = PyDatabase::new(None).expect("db");
            db.close().expect("close");
            db.close().expect("repeated close is idempotent");
            let err = db.execute_sql(py, "SELECT 1", None).expect_err("closed");
            assert!(err.is_instance_of::<crate::error::PyAlopexError>(py));
        });
    }

    #[test]
    fn single_thread_database_reports_a_classified_cross_thread_error() {
        pyo3::Python::initialize();
        let database = PyDatabase::new(Some("single")).expect("single-thread db");
        assert_eq!(
            database.thread_mode().unwrap().__repr__(),
            "ThreadMode.SINGLE"
        );
        assert!(PyDatabase::new(Some("invalid")).is_err());

        let control = database.control.clone();
        let code = std::thread::spawn(move || {
            let error = control
                .ensure_open()
                .expect_err("other thread must be rejected");
            Python::attach(|py| {
                error
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap()
            })
        })
        .join()
        .expect("thread must finish");
        assert_eq!(code, "thread_mode_violation");
    }

    #[test]
    fn execute_sql_vector_param_roundtrip() {
        with_py(|py| {
            let db = PyDatabase::new(None).expect("db");
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
            let db = PyDatabase::new(None).expect("db");
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
        let mut db = PyDatabase::new(None).expect("db");
        let txn1 = db.begin(None).expect("txn1");
        let txn2 = db.begin(None).expect("txn2");

        {
            let mut guard = txn1.inner.txn.lock().expect("transaction lock poisoned");
            guard.take();
        }

        db.close().expect("close");

        assert!(db.ensure_open().is_err());
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
        pyo3::Python::initialize();
        let mut db = PyDatabase::new(None).expect("db");
        let _txn = db.begin(None).expect("txn");

        db.inject_rollback_failure_once();
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

    #[test]
    fn rollback_failure_injection_is_database_scoped() {
        pyo3::Python::initialize();
        let mut injected = PyDatabase::new(None).expect("injected db");
        let _injected_txn = injected.begin(None).expect("injected txn");
        let mut unaffected = PyDatabase::new(None).expect("unaffected db");
        let _unaffected_txn = unaffected.begin(None).expect("unaffected txn");

        injected.inject_rollback_failure_once();

        unaffected
            .close()
            .expect("another database must not consume the injected failure");
        injected
            .close()
            .expect_err("the owning database must receive the injected failure");
        injected
            .close()
            .expect("the owning database can retry close");
    }

    #[test]
    fn close_terminates_a_transaction_owned_stream_before_rollback() {
        with_py(|py| {
            let mut db = PyDatabase::new(None).expect("db");
            db.execute_sql(
                py,
                "CREATE TABLE stream_close (id INTEGER PRIMARY KEY)",
                None,
            )
            .expect("ddl");
            db.execute_sql(py, "INSERT INTO stream_close (id) VALUES (1)", None)
                .expect("insert");
            let txn = db.begin(None).expect("transaction");
            let stream = txn
                .execute_sql_stream("SELECT id FROM stream_close", None, None, None)
                .expect("stream");

            db.close().expect("close");
            let error = stream.next_row(py).expect_err("closed stream");
            let code: String = error.value(py).getattr("code").unwrap().extract().unwrap();
            assert_eq!(code, "stream_closed");
        });
    }
}
