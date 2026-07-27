use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

use crate::embedded::async_stream::PyNativeAsyncSqlResultStream;
use crate::embedded::local_scan::PyLocalScan;
use crate::embedded::stream::{PySqlResultStream, StreamLeaseRegistry};
use crate::embedded::thread_mode::DatabaseControl;
use crate::error;
#[cfg(feature = "numpy")]
use crate::types::{PyMetric, PySearchResult};
#[cfg(feature = "numpy")]
use crate::vector;
#[cfg(feature = "numpy")]
use crate::vector::SliceOrOwned;
#[cfg(feature = "numpy")]
use alopex_core::Key;
#[cfg(feature = "numpy")]
use pyo3::types::PyAnyMethods;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    RolledBack,
}

pub(crate) struct PyTransactionInner {
    pub(crate) txn: Mutex<Option<alopex_embedded::OwnedEmbeddedTransaction>>,
    state: Mutex<TxnState>,
}

#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    #[allow(dead_code)]
    pub(crate) db: Arc<alopex_embedded::Database>,
    pub(crate) control: Arc<DatabaseControl>,
    pub(crate) streams: Arc<StreamLeaseRegistry>,
    #[allow(dead_code)]
    pub(crate) inner: Arc<PyTransactionInner>,
}

impl PyTransaction {
    /// Construct a transaction facade that owns its embedded session under the database's shared
    /// base control.  No borrowed embedded transaction crosses the PyO3 boundary.
    pub(crate) fn begin_with_control(
        db: Arc<alopex_embedded::Database>,
        mode: alopex_core::TxnMode,
        control: Arc<DatabaseControl>,
        streams: Arc<StreamLeaseRegistry>,
    ) -> PyResult<Self> {
        control.ensure_open()?;
        let txn = Arc::clone(&db)
            .begin_owned_embedded_transaction(mode)
            .map_err(error::embedded_err)?;
        let inner = PyTransactionInner {
            txn: Mutex::new(Some(txn)),
            state: Mutex::new(TxnState::Active),
        };
        Ok(Self {
            db,
            control,
            streams,
            inner: Arc::new(inner),
        })
    }

    fn ensure_active(&self) -> PyResult<()> {
        self.control.ensure_open()?;
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        if *state != TxnState::Active {
            return Err(error::to_py_err("transaction is closed"));
        }
        Ok(())
    }

    fn is_active(&self) -> PyResult<bool> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        Ok(*state == TxnState::Active)
    }

    fn state_name(&self) -> PyResult<&'static str> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        Ok(match *state {
            TxnState::Active => "active",
            TxnState::Committed => "committed",
            TxnState::RolledBack => "rolled_back",
        })
    }

    fn with_txn_mut<F, T>(&self, op: F) -> PyResult<T>
    where
        F: FnOnce(
            &mut alopex_embedded::OwnedEmbeddedTransaction,
        ) -> Result<T, alopex_embedded::Error>,
    {
        self.ensure_active()?;
        let mut guard = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?;
        let txn = guard
            .as_mut()
            .ok_or_else(|| error::to_py_err("transaction is closed"))?;
        op(txn).map_err(error::embedded_err)
    }

    fn finalize_with<F>(&self, op: F, success_state: TxnState) -> PyResult<()>
    where
        F: FnOnce(
            &mut alopex_embedded::OwnedEmbeddedTransaction,
        ) -> Result<(), alopex_embedded::Error>,
    {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        if *state != TxnState::Active {
            return Err(error::to_py_err("transaction is closed"));
        }
        let mut guard = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?;
        let txn = guard
            .as_mut()
            .ok_or_else(|| error::to_py_err("transaction is closed"))?;
        match op(txn) {
            Ok(()) => {
                *guard = None;
                *state = success_state;
                Ok(())
            }
            Err(err) => {
                *guard = None;
                *state = TxnState::RolledBack;
                Err(error::embedded_err(err))
            }
        }
    }
}

#[pymethods]
impl PyTransaction {
    /// Return the transaction's current public lifecycle state.
    ///
    /// `stream_effect` is derived from the owned session state, so callers can distinguish an
    /// active lease from a committable or abort-required transaction.
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.control.check_thread()?;
        let state = self.state_name()?;
        let stream_effect = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?
            .as_ref()
            .map(|txn| match txn.session().status() {
                alopex_core::txn::OwnedTransactionSessionStatus::Open
                | alopex_core::txn::OwnedTransactionSessionStatus::Committable => "committable",
                alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive => "active",
                alopex_core::txn::OwnedTransactionSessionStatus::MustAbort => "must_abort",
                alopex_core::txn::OwnedTransactionSessionStatus::Committed
                | alopex_core::txn::OwnedTransactionSessionStatus::RolledBack
                | alopex_core::txn::OwnedTransactionSessionStatus::Closed => "closed",
            })
            .unwrap_or("closed");
        let status = PyDict::new(py);
        status.set_item("state", state)?;
        status.set_item("stream_effect", stream_effect)?;
        Ok(status.unbind())
    }

    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.with_txn_mut(|txn| txn.get(key))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.put(key, value))
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.delete(key))
    }

    #[cfg(feature = "numpy")]
    fn upsert_vector(
        &self,
        py: Python<'_>,
        key: &[u8],
        metadata: Py<PyAny>,
        vector: Py<PyAny>,
        metric: PyMetric,
    ) -> PyResult<()> {
        vector::require_numpy(py)?;
        let metadata = metadata.bind(py);
        let payload: Vec<u8> = if metadata.is_none() {
            Vec::new()
        } else {
            metadata.extract::<Vec<u8>>()?
        };
        let metric = metric.into();
        let key = key.to_vec();
        vector::with_ndarray_f32_gil_safe(vector.bind(py), |slice_or_owned| {
            match slice_or_owned {
                SliceOrOwned::Borrowed { ptr, len, _guard } => {
                    let _guard = _guard;
                    // `*const f32` is not `Send`; pass it as an integer to `allow_threads`.
                    let ptr = ptr as usize;
                    py.detach(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| txn.upsert_vector(&key, &payload, values, metric))
                    })
                }
                SliceOrOwned::Owned(vec) => py.detach(move || {
                    self.with_txn_mut(|txn| txn.upsert_vector(&key, &payload, &vec, metric))
                }),
            }
        })
    }

    #[cfg(feature = "numpy")]
    #[pyo3(signature = (query, metric, k, filter_keys = None, return_vectors = false, zero_copy_return = true))]
    #[allow(unused_variables, clippy::too_many_arguments)]
    fn search_similar(
        &self,
        py: Python<'_>,
        query: Py<PyAny>,
        metric: PyMetric,
        k: usize,
        filter_keys: Option<Vec<Vec<u8>>>,
        return_vectors: bool,
        zero_copy_return: bool,
    ) -> PyResult<Vec<PySearchResult>> {
        vector::require_numpy(py)?;
        let metric_enum = metric.into();
        vector::with_ndarray_f32_gil_safe(query.bind(py), |slice_or_owned| {
            let results = match slice_or_owned {
                SliceOrOwned::Borrowed { ptr, len, _guard } => {
                    let _guard = _guard;
                    // `*const f32` is not `Send`; pass it as an integer to `allow_threads`.
                    let ptr = ptr as usize;
                    py.detach(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| {
                            txn.search_similar(values, metric_enum, k, filter_keys.as_deref())
                        })
                    })
                }
                SliceOrOwned::Owned(vec) => py.detach(move || {
                    self.with_txn_mut(|txn| {
                        txn.search_similar(&vec, metric_enum, k, filter_keys.as_deref())
                    })
                }),
            }?;

            if !return_vectors {
                return Ok(results.into_iter().map(PySearchResult::from).collect());
            }

            // return_vectors=True の場合、結果キーをまとめて取得し N+1 を避ける
            let mut keys: Vec<Key> = Vec::with_capacity(results.len());
            let mut rows: Vec<(f32, Option<Vec<u8>>)> = Vec::with_capacity(results.len());
            for result in results {
                keys.push(result.key);
                rows.push((
                    result.score,
                    if result.metadata.is_empty() {
                        None
                    } else {
                        Some(result.metadata)
                    },
                ));
            }
            let vectors: Vec<Option<Vec<f32>>> =
                py.detach(|| self.with_txn_mut(|txn| txn.get_vectors(&keys, metric_enum)))?;

            let mut py_results = Vec::with_capacity(rows.len());
            for ((key, (score, metadata)), vector_data) in keys.into_iter().zip(rows).zip(vectors) {
                let Some(vector_data) = vector_data else {
                    return Err(error::to_py_err(
                        "internal error: missing vector for a search result key",
                    ));
                };
                let vector_obj = if zero_copy_return {
                    vector::vec_to_ndarray_opt(py, Some(vector_data))?
                } else {
                    vector::vec_to_ndarray_opt_copy(py, Some(vector_data.as_slice()))?
                };
                py_results.push(PySearchResult::with_vector(
                    key, score, metadata, vector_obj,
                ));
            }
            Ok(py_results)
        })
    }

    #[cfg(feature = "numpy")]
    #[pyo3(signature = (name, key, vector, metadata = None))]
    fn upsert_to_hnsw(
        &self,
        py: Python<'_>,
        name: &str,
        key: &[u8],
        vector: Py<PyAny>,
        metadata: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        vector::require_numpy(py)?;
        let payload: Vec<u8> = if let Some(metadata) = metadata {
            let metadata = metadata.bind(py);
            if metadata.is_none() {
                Vec::new()
            } else {
                metadata.extract::<Vec<u8>>()?
            }
        } else {
            Vec::new()
        };
        let name = name.to_string();
        let key = key.to_vec();
        vector::with_ndarray_f32_gil_safe(vector.bind(py), |slice_or_owned| {
            match slice_or_owned {
                SliceOrOwned::Borrowed { ptr, len, _guard } => {
                    let _guard = _guard;
                    // `*const f32` is not `Send`; pass it as an integer to `allow_threads`.
                    let ptr = ptr as usize;
                    py.detach(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| txn.upsert_to_hnsw(&name, &key, values, &payload))
                    })
                }
                SliceOrOwned::Owned(vec) => py.detach(move || {
                    self.with_txn_mut(|txn| txn.upsert_to_hnsw(&name, &key, &vec, &payload))
                }),
            }
        })
    }

    #[cfg(feature = "numpy")]
    fn delete_from_hnsw(&self, name: &str, key: &[u8]) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.delete_from_hnsw(name, key))
            .map(|_| ())
    }

    /// ベクトルを取得する
    ///
    /// # Arguments
    /// * `key` - ベクトルのキー
    /// * `metric` - メトリック（保存時と一致する必要がある）
    /// * `zero_copy_return` - True の場合、所有権移譲によるゼロコピー返却
    ///
    /// # Returns
    /// NumPy ndarray (float32)
    ///
    /// # Raises
    /// KeyError: キーが存在しない場合
    #[cfg(feature = "numpy")]
    #[pyo3(signature = (key, metric, zero_copy_return = true))]
    fn get_vector(
        &self,
        py: Python<'_>,
        key: &[u8],
        metric: PyMetric,
        zero_copy_return: bool,
    ) -> PyResult<Py<PyAny>> {
        vector::require_numpy(py)?;
        let metric_enum = metric.into();
        let vector_opt = self.with_txn_mut(|txn| txn.get_vector(key, metric_enum))?;
        match vector_opt {
            Some(vec) => {
                if zero_copy_return {
                    vector::owned_vec_to_ndarray(py, vec)
                } else {
                    vector::vec_to_ndarray_copy(py, &vec)
                }
            }
            None => {
                // Format key as hex for readability
                let key_hex: String = key.iter().map(|b| format!("{:02x}", b)).collect();
                Err(pyo3::exceptions::PyKeyError::new_err(format!(
                    "Vector not found for key (len={}): 0x{}",
                    key.len(),
                    key_hex
                )))
            }
        }
    }

    /// SQL をこのトランザクション内で実行する（コミットは行わない）。
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
    ///     AlopexError: SQL の解析・実行エラー、またはトランザクションが完了済みの場合。
    #[pyo3(signature = (sql, params = None))]
    fn execute_sql(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound_sql = crate::embedded::sql::bind_params(sql, params.as_ref())?;
        self.ensure_active()?;

        // NOTE: `allow_threads` 内では PyErr を生成しない（`with_code` が GIL を再取得する）。
        // txn mutex を保持したまま GIL を待つと、GIL 保持スレッドが同じ mutex を
        // 待っている場合にデッドロックするため、エラーは Rust 値のまま持ち出して
        // GIL 復帰後（mutex 解放後）に Python 例外へ変換する。
        enum ExecError {
            LockPoisoned,
            Closed,
            Embedded(alopex_embedded::Error),
        }

        let result = py.detach(|| {
            let mut guard = self.inner.txn.lock().map_err(|_| ExecError::LockPoisoned)?;
            let txn = guard.as_mut().ok_or(ExecError::Closed)?;
            txn.execute_sql(&bound_sql).map_err(ExecError::Embedded)
        });
        let result = result.map_err(|err| match err {
            ExecError::LockPoisoned => error::to_py_err("transaction lock poisoned"),
            ExecError::Closed => error::to_py_err("transaction is closed"),
            ExecError::Embedded(err) => error::embedded_err(err),
        })?;
        crate::embedded::sql::execution_result_to_py(py, result)
    }

    /// Open a local SELECT stream within this explicit transaction.
    ///
    /// Preflight runs before a transaction lease is acquired.  The returned stream shares the
    /// owned transaction session, so normal exhaustion is committable while close/cancel/failure
    /// records the conservative abort requirement.
    #[pyo3(signature = (sql, params = None, *, resource_limit_bytes = None, timeout = None))]
    pub(crate) fn execute_sql_stream(
        &self,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<PySqlResultStream> {
        self.ensure_active()?;
        let bound_sql = crate::embedded::sql::bind_params(sql, params.as_ref())?;
        let (plan, session) = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?
            .as_ref()
            .ok_or_else(|| error::to_py_err("transaction is closed"))
            .and_then(|txn| {
                let plan = txn
                    .preflight_sql_stream(&bound_sql)
                    .map_err(error::embedded_err)?;
                Ok((plan, txn.session()))
            })?;
        PySqlResultStream::open_transaction(
            self.control.clone(),
            &self.streams,
            session,
            plan,
            resource_limit_bytes,
            timeout,
        )
    }

    /// Internal native bridge factory used only by `alopex.asyncio`.
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
        let stream = self.execute_sql_stream(sql, params, resource_limit_bytes, timeout)?;
        PyNativeAsyncSqlResultStream::new(
            stream,
            self.control.thread_mode(),
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    /// Internal native async factory for the transaction-isolated table-scan subset.
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
        PyNativeAsyncSqlResultStream::validate_options(
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )?;
        let stream = self.query_stream(scan, resource_limit_bytes, timeout)?;
        PyNativeAsyncSqlResultStream::new(
            stream,
            self.control.thread_mode(),
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    /// Open a transaction-isolated table scan.  Unsupported `LocalScan` variants are rejected
    /// before a transaction cursor lease opens rather than being detached from the transaction.
    #[pyo3(signature = (scan, *, resource_limit_bytes = None, timeout = None))]
    pub(crate) fn query_stream(
        &self,
        scan: PyRef<'_, PyLocalScan>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<PySqlResultStream> {
        self.ensure_active()?;
        let sql = scan.transaction_sql()?;
        let (plan, session) = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?
            .as_ref()
            .ok_or_else(|| error::to_py_err("transaction is closed"))
            .and_then(|txn| {
                let plan = txn
                    .preflight_sql_stream(&sql)
                    .map_err(error::embedded_err)?;
                Ok((plan, txn.session()))
            })?;
        PySqlResultStream::open_transaction(
            self.control.clone(),
            &self.streams,
            session,
            plan,
            resource_limit_bytes,
            timeout,
        )
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        if *state != TxnState::Active {
            return Err(error::to_py_err("transaction is closed"));
        }
        let mut guard = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?;
        let txn = guard
            .as_mut()
            .ok_or_else(|| error::to_py_err("transaction is closed"))?;
        match txn.session().status() {
            alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive => {
                return Err(error::stream_error(
                    "stream_active",
                    "commit is not allowed while a transaction stream is active",
                ));
            }
            alopex_core::txn::OwnedTransactionSessionStatus::MustAbort => {
                return Err(error::stream_error(
                    "stream_abort_required",
                    "transaction stream requires rollback before commit",
                ));
            }
            _ => {}
        }
        let result = py.detach(|| txn.commit());
        match result {
            Ok(()) => {
                *guard = None;
                *state = TxnState::Committed;
                Ok(())
            }
            Err(err) => {
                *guard = None;
                *state = TxnState::RolledBack;
                Err(error::embedded_err(err))
            }
        }
    }

    #[cfg(not(feature = "numpy"))]
    fn upsert_vector(
        &self,
        py: Python<'_>,
        key: &[u8],
        metadata: Py<PyAny>,
        vector: Py<PyAny>,
        metric: Py<PyAny>,
    ) -> PyResult<()> {
        let _ = (key, metadata, vector, metric);
        crate::vector::require_numpy(py)
    }

    #[cfg(not(feature = "numpy"))]
    #[pyo3(signature = (query, metric, k, filter_keys = None, return_vectors = false, zero_copy_return = true))]
    #[allow(clippy::too_many_arguments)]
    fn search_similar(
        &self,
        py: Python<'_>,
        query: Py<PyAny>,
        metric: Py<PyAny>,
        k: usize,
        filter_keys: Option<Py<PyAny>>,
        return_vectors: bool,
        zero_copy_return: bool,
    ) -> PyResult<()> {
        let _ = (
            query,
            metric,
            k,
            filter_keys,
            return_vectors,
            zero_copy_return,
        );
        crate::vector::require_numpy(py)
    }

    #[cfg(not(feature = "numpy"))]
    #[pyo3(signature = (name, key, vector, metadata = None))]
    fn upsert_to_hnsw(
        &self,
        py: Python<'_>,
        name: &str,
        key: &[u8],
        vector: Py<PyAny>,
        metadata: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let _ = (name, key, vector, metadata);
        crate::vector::require_numpy(py)
    }

    #[cfg(not(feature = "numpy"))]
    fn delete_from_hnsw(&self, py: Python<'_>, name: &str, key: &[u8]) -> PyResult<()> {
        let _ = (name, key);
        crate::vector::require_numpy(py)
    }

    #[cfg(not(feature = "numpy"))]
    #[pyo3(signature = (key, metric, zero_copy_return = true))]
    fn get_vector(
        &self,
        py: Python<'_>,
        key: &[u8],
        metric: Py<PyAny>,
        zero_copy_return: bool,
    ) -> PyResult<()> {
        let _ = (key, metric, zero_copy_return);
        crate::vector::require_numpy(py)
    }

    fn rollback(&self) -> PyResult<()> {
        if let Some(txn) = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))?
            .as_ref()
        {
            if txn.session().status()
                == alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive
            {
                return Err(error::stream_error(
                    "stream_active",
                    "rollback is not allowed while a transaction stream is active",
                ));
            }
        }
        self.finalize_with(|txn| txn.rollback(), TxnState::RolledBack)
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.ensure_active()?;
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type = None, _exc = None, _traceback = None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        if self.is_active()? {
            self.finalize_with(|txn| txn.rollback(), TxnState::RolledBack)?;
        }
        Ok(false)
    }
}

impl Drop for PyTransaction {
    fn drop(&mut self) {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if *state != TxnState::Active {
            return;
        }
        let mut guard = match self.inner.txn.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if let Some(txn) = guard.as_mut() {
            let _ = txn.rollback();
        }
        *guard = None;
        *state = TxnState::RolledBack;
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTransaction>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alopex_core::TxnMode;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};
    use pyo3::Python;
    use std::sync::Arc;

    fn transaction(
        database: Arc<alopex_embedded::Database>,
        mode: TxnMode,
    ) -> super::PyTransaction {
        super::PyTransaction::begin_with_control(
            database,
            mode,
            Arc::new(crate::embedded::thread_mode::DatabaseControl::new(
                crate::embedded::thread_mode::ThreadMode::Multi,
            )),
            Arc::new(crate::embedded::stream::StreamLeaseRegistry::default()),
        )
        .expect("owned transaction")
    }

    fn query_row_count(db: &alopex_embedded::Database, sql: &str) -> usize {
        match db.execute_sql(sql).expect("select") {
            alopex_sql::ExecutionResult::Query(query) => query.rows.len(),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn execute_sql_insert_and_select_within_transaction() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .expect("ddl");
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            let params = PyList::empty(py);
            params.append(7i64).expect("append");
            let affected = txn
                .execute_sql(py, "INSERT INTO t (id) VALUES (?)", Some(params.into_any()))
                .expect("insert");
            assert_eq!(affected.extract::<u64>(py).expect("affected"), 1);

            // 同一トランザクション内で未コミットの行が見える
            let rows = txn
                .execute_sql(py, "SELECT id FROM t", None)
                .expect("select");
            let rows = rows.bind(py).cast::<PyList>().expect("list").clone();
            assert_eq!(rows.len(), 1);

            txn.commit(py).expect("commit");
        });
        assert_eq!(query_row_count(&db, "SELECT id FROM t;"), 1);
    }

    #[test]
    fn execute_sql_rollback_discards_changes() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .expect("ddl");
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            let params = PyList::empty(py);
            params.append(7i64).expect("append");
            txn.execute_sql(py, "INSERT INTO t (id) VALUES (?)", Some(params.into_any()))
                .expect("insert");
        });
        txn.rollback().expect("rollback");
        assert_eq!(query_row_count(&db, "SELECT id FROM t;"), 0);
    }

    #[test]
    fn execute_sql_on_completed_transaction_is_error() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .expect("ddl");
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        txn.rollback().expect("rollback");
        Python::attach(|py| {
            let err = txn
                .execute_sql(py, "SELECT id FROM t", None)
                .expect_err("completed txn");
            assert!(err.is_instance_of::<crate::error::PyAlopexError>(py));
        });
    }

    #[test]
    fn put_get_and_rollback() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        txn.put(b"key", b"value").expect("put");
        txn.rollback().expect("rollback");

        let mut txn2 = db.begin(TxnMode::ReadOnly).expect("txn2");
        let value = txn2.get(b"key").expect("get");
        assert!(value.is_none());
    }

    #[test]
    fn commit_closes_transaction() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            txn.commit(py).expect("commit");
        });
        assert!(txn.get(b"key").is_err());
    }

    #[test]
    fn status_reports_owned_transaction_lifecycle_and_commitability() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            let status = txn.status(py).unwrap();
            let status = status.bind(py);
            assert_eq!(
                status
                    .get_item("state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "active"
            );
            assert_eq!(
                status
                    .get_item("stream_effect")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "committable"
            );
            txn.rollback().unwrap();
            let status = txn.status(py).unwrap();
            assert_eq!(
                status
                    .bind(py)
                    .get_item("state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "rolled_back"
            );
        });
    }

    #[test]
    fn read_only_put_is_error() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadOnly);
        assert!(txn.put(b"key", b"value").is_err());
    }

    #[test]
    fn drop_rolls_back_uncommitted() {
        let db = Arc::new(alopex_embedded::Database::new());
        {
            let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
            txn.put(b"key", b"value").expect("put");
        }
        let mut txn2 = db.begin(TxnMode::ReadOnly).expect("txn2");
        let value = txn2.get(b"key").expect("get");
        assert!(value.is_none());
    }

    #[test]
    fn transaction_stream_blocks_commit_until_exhausted_and_records_abort_on_close() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE stream_t (id INTEGER PRIMARY KEY)")
            .unwrap();
        db.execute_sql("INSERT INTO stream_t (id) VALUES (1)")
            .unwrap();
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);

        Python::attach(|py| {
            let stream = txn
                .execute_sql_stream("SELECT id FROM stream_t", None, None, None)
                .unwrap();
            let row = stream.next_row(py).unwrap();
            let row = row.bind(py).cast::<PyDict>().unwrap();
            assert_eq!(
                row.get_item("id")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
            let error = txn.commit(py).expect_err("active stream blocks commit");
            assert_eq!(
                error
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "stream_active"
            );
            assert!(stream
                .next_row(py)
                .unwrap_err()
                .is_instance_of::<pyo3::exceptions::PyStopIteration>(py));
            txn.commit(py).unwrap();
        });

        let aborted = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            let stream = aborted
                .execute_sql_stream("SELECT id FROM stream_t", None, None, None)
                .unwrap();
            stream.close().unwrap();
            let error = aborted
                .commit(py)
                .expect_err("early stream close requires rollback");
            assert_eq!(
                error
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "stream_abort_required"
            );
            aborted.rollback().unwrap();
        });
    }

    #[test]
    fn transaction_stream_sees_uncommitted_ddl_and_rows() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);

        Python::attach(|py| {
            txn.execute_sql(
                py,
                "CREATE TABLE staged_stream (id INTEGER PRIMARY KEY)",
                None,
            )
            .expect("transactional ddl");
            txn.execute_sql(py, "INSERT INTO staged_stream (id) VALUES (9)", None)
                .expect("transactional dml");

            let stream = txn
                .execute_sql_stream("SELECT id FROM staged_stream", None, None, None)
                .expect("stream planned through the transaction overlay");
            let row = stream.next_row(py).expect("staged row");
            let row = row.bind(py).cast::<PyDict>().unwrap();
            assert_eq!(
                row.get_item("id")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                9
            );
            assert!(stream.next_row(py).is_err(), "stream is finite");
            txn.commit(py).expect("commit after exhaustion");
        });

        assert_eq!(query_row_count(&db, "SELECT id FROM staged_stream"), 1);
    }
}
