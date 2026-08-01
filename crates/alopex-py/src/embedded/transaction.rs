use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods, PyModule};

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

/// The most recent local operation represented by the additive v0.9 status
/// projection.  This is intentionally an in-process compatibility record: an
/// embedded transaction has no remote coordinator or durable idempotency
/// ledger to claim.
#[derive(Clone, Debug)]
struct LocalOperationOutcome {
    request_id: String,
    operation_id: String,
    first_outcome: &'static str,
    state: &'static str,
    failure_class: Option<&'static str>,
    reason_code: String,
}

pub(crate) struct PyTransactionInner {
    pub(crate) txn: Mutex<Option<alopex_embedded::OwnedEmbeddedTransaction>>,
    state: Mutex<TxnState>,
    transaction_id: String,
    latest_outcome: Mutex<LocalOperationOutcome>,
    operation_sequence: AtomicU64,
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
        transaction_id: String,
        request_id: String,
    ) -> PyResult<Self> {
        control.ensure_open()?;
        let txn = Arc::clone(&db)
            .begin_owned_embedded_transaction(mode)
            .map_err(error::embedded_err)?;
        let inner = PyTransactionInner {
            txn: Mutex::new(Some(txn)),
            state: Mutex::new(TxnState::Active),
            latest_outcome: Mutex::new(LocalOperationOutcome {
                request_id,
                operation_id: transaction_id.clone(),
                first_outcome: "begin",
                state: "running",
                failure_class: None,
                reason_code: "local_python_transaction_begin".to_string(),
            }),
            transaction_id,
            operation_sequence: AtomicU64::new(0),
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

    fn transaction_outcome_state(state: TxnState) -> &'static str {
        match state {
            TxnState::Active => "running",
            TxnState::Committed => "committed",
            TxnState::RolledBack => "cancelled",
        }
    }

    fn transaction_outcome_reason(state: TxnState) -> &'static str {
        match state {
            TxnState::Active => "local_python_transaction_active",
            TxnState::Committed => "local_python_transaction_committed",
            TxnState::RolledBack => "local_python_transaction_rolled_back",
        }
    }

    fn new_operation_outcome(
        &self,
        operation: &'static str,
        request_id: Option<String>,
    ) -> PyResult<LocalOperationOutcome> {
        let ordinal = self
            .inner
            .operation_sequence
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let request_id = match request_id {
            Some(request_id) if !request_id.trim().is_empty() => request_id,
            Some(_) => return Err(error::to_py_err("request_id must not be empty")),
            None => format!(
                "{}-{operation}-{ordinal}",
                self.inner.transaction_id.as_str()
            ),
        };
        Ok(LocalOperationOutcome {
            operation_id: format!("{}:{operation}:{ordinal}", self.inner.transaction_id),
            request_id,
            first_outcome: operation,
            state: "running",
            failure_class: None,
            reason_code: format!("local_python_transaction_{operation}"),
        })
    }

    fn record_success(
        &self,
        mut operation: LocalOperationOutcome,
        state: TxnState,
    ) -> PyResult<()> {
        operation.state = Self::transaction_outcome_state(state);
        operation.failure_class = None;
        operation.reason_code = Self::transaction_outcome_reason(state).to_string();
        if state == TxnState::Active {
            operation.reason_code = format!("local_python_transaction_{}", operation.first_outcome);
        }
        *self
            .inner
            .latest_outcome
            .lock()
            .map_err(|_| error::to_py_err("transaction outcome lock poisoned"))? = operation;
        Ok(())
    }

    fn transaction_outcome_from(
        &self,
        py: Python<'_>,
        operation: &LocalOperationOutcome,
    ) -> PyResult<Py<PyDict>> {
        let routing = PyDict::new(py);
        routing.set_item("kind", "local_only")?;
        routing.set_item("range_identity", py.None())?;
        routing.set_item("metadata_version", 0_u64)?;
        routing.set_item("reason_code", &operation.reason_code)?;

        let idempotency = PyDict::new(py);
        idempotency.set_item("operation_id", &operation.operation_id)?;
        idempotency.set_item("request_id", &operation.request_id)?;
        idempotency.set_item("first_outcome", operation.first_outcome)?;
        idempotency.set_item("state", operation.state)?;
        idempotency.set_item("duplicate_count", 0_u64)?;

        let outcome = PyDict::new(py);
        outcome.set_item("outcome_version", "v0.9")?;
        outcome.set_item("transaction_id", &self.inner.transaction_id)?;
        outcome.set_item("request_id", &operation.request_id)?;
        outcome.set_item("participating_ranges", Vec::<String>::new())?;
        outcome.set_item("read_point", py.None())?;
        outcome.set_item("schema_version", py.None())?;
        outcome.set_item("data_epoch", py.None())?;
        outcome.set_item("isolation", "snapshot")?;
        outcome.set_item("state", operation.state)?;
        outcome.set_item("failure_class", operation.failure_class)?;
        outcome.set_item("reason_code", &operation.reason_code)?;
        outcome.set_item("routing", routing)?;
        outcome.set_item("retryable", false)?;
        outcome.set_item("idempotency", idempotency)?;
        Ok(outcome.unbind())
    }

    /// Build the additive local-only v0.9 projection.  The embedded Python
    /// binding has no cluster metadata or coordinator, so range and read-point
    /// fields remain explicitly empty rather than fabricating distributed data.
    fn transaction_outcome(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let operation = self
            .inner
            .latest_outcome
            .lock()
            .map_err(|_| error::to_py_err("transaction outcome lock poisoned"))?
            .clone();
        self.transaction_outcome_from(py, &operation)
    }

    fn transaction_error(
        &self,
        py: Python<'_>,
        mut operation: LocalOperationOutcome,
        err: PyErr,
    ) -> PyErr {
        operation.state = "rejected";
        operation.failure_class = Some("invalid_request");
        operation.reason_code = format!(
            "local_python_transaction_{}_rejected",
            operation.first_outcome
        );
        if let Ok(status) = self.transaction_outcome_from(py, &operation) {
            let _ = err.value(py).setattr("status", status.bind(py));
            let _ = err.value(py).setattr("failure_class", "invalid_request");
        }
        err
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
        status.set_item("transaction", self.transaction_outcome(py)?)?;
        Ok(status.unbind())
    }

    #[pyo3(signature = (key, *, request_id = None))]
    fn get(&self, key: &[u8], request_id: Option<String>) -> PyResult<Option<Vec<u8>>> {
        let operation = self.new_operation_outcome("get", request_id)?;
        match self.with_txn_mut(|txn| txn.get(key)) {
            Ok(value) => {
                self.record_success(operation, TxnState::Active)?;
                Ok(value)
            }
            Err(err) => Python::attach(|py| Err(self.transaction_error(py, operation, err))),
        }
    }

    #[pyo3(signature = (key, value, *, request_id = None))]
    fn put(&self, key: &[u8], value: &[u8], request_id: Option<String>) -> PyResult<()> {
        let operation = self.new_operation_outcome("put", request_id)?;
        match self.with_txn_mut(|txn| txn.put(key, value)) {
            Ok(()) => {
                self.record_success(operation, TxnState::Active)?;
                Ok(())
            }
            Err(err) => Python::attach(|py| Err(self.transaction_error(py, operation, err))),
        }
    }

    #[pyo3(signature = (key, *, request_id = None))]
    fn delete(&self, key: &[u8], request_id: Option<String>) -> PyResult<()> {
        let operation = self.new_operation_outcome("delete", request_id)?;
        match self.with_txn_mut(|txn| txn.delete(key)) {
            Ok(()) => {
                self.record_success(operation, TxnState::Active)?;
                Ok(())
            }
            Err(err) => Python::attach(|py| Err(self.transaction_error(py, operation, err))),
        }
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
    #[pyo3(signature = (sql, params = None, *, request_id = None))]
    fn execute_sql(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
        request_id: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let operation = self.new_operation_outcome("execute_sql", request_id)?;
        let bound_sql = match crate::embedded::sql::bind_params(sql, params.as_ref()) {
            Ok(bound_sql) => bound_sql,
            Err(err) => return Err(self.transaction_error(py, operation, err)),
        };
        if let Err(err) = self.ensure_active() {
            return Err(self.transaction_error(py, operation, err));
        }

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
        });
        let result = match result {
            Ok(result) => result,
            Err(err) => return Err(self.transaction_error(py, operation, err)),
        };
        let value = match crate::embedded::sql::execution_result_to_py(py, result) {
            Ok(value) => value,
            Err(err) => return Err(self.transaction_error(py, operation, err)),
        };
        self.record_success(operation, TxnState::Active)?;
        Ok(value)
    }

    /// Open a local SELECT stream within this explicit transaction.
    ///
    /// Preflight runs before a transaction lease is acquired.  The returned stream shares the
    /// owned transaction session, so normal exhaustion is committable while close/cancel/failure
    /// records the conservative abort requirement.
    #[pyo3(signature = (sql, params = None, *, request_id = None, resource_limit_bytes = None, timeout = None))]
    pub(crate) fn execute_sql_stream(
        &self,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
        request_id: Option<String>,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<PySqlResultStream> {
        let operation = self.new_operation_outcome("execute_sql_stream", request_id)?;
        if let Err(err) = self.ensure_active() {
            return Python::attach(|py| Err(self.transaction_error(py, operation, err)));
        }
        let bound_sql = match crate::embedded::sql::bind_params(sql, params.as_ref()) {
            Ok(bound_sql) => bound_sql,
            Err(err) => {
                return Python::attach(|py| Err(self.transaction_error(py, operation, err)))
            }
        };
        let plan_and_session = self
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
            });
        let (plan, session) = match plan_and_session {
            Ok(plan_and_session) => plan_and_session,
            Err(err) => {
                return Python::attach(|py| Err(self.transaction_error(py, operation, err)))
            }
        };
        let stream = PySqlResultStream::open_transaction(
            self.control.clone(),
            &self.streams,
            session,
            plan,
            resource_limit_bytes,
            timeout,
        );
        match stream {
            Ok(stream) => {
                self.record_success(operation, TxnState::Active)?;
                Ok(stream)
            }
            Err(err) => Python::attach(|py| Err(self.transaction_error(py, operation, err))),
        }
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
        let stream = self.execute_sql_stream(sql, params, None, resource_limit_bytes, timeout)?;
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

    #[pyo3(signature = (*, request_id = None))]
    fn commit(&self, py: Python<'_>, request_id: Option<String>) -> PyResult<()> {
        let operation = self.new_operation_outcome("commit", request_id)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("transaction state lock poisoned"))?;
        if *state != TxnState::Active {
            return Err(self.transaction_error(
                py,
                operation,
                error::to_py_err("transaction is closed"),
            ));
        }
        let mut guard = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))
            .map_err(|err| self.transaction_error(py, operation.clone(), err))?;
        let txn = guard
            .as_mut()
            .ok_or_else(|| error::to_py_err("transaction is closed"))
            .map_err(|err| self.transaction_error(py, operation.clone(), err))?;
        match txn.session().status() {
            alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive => {
                return Err(self.transaction_error(
                    py,
                    operation,
                    error::stream_error(
                        "stream_active",
                        "commit is not allowed while a transaction stream is active",
                    ),
                ));
            }
            alopex_core::txn::OwnedTransactionSessionStatus::MustAbort => {
                return Err(self.transaction_error(
                    py,
                    operation,
                    error::stream_error(
                        "stream_abort_required",
                        "transaction stream requires rollback before commit",
                    ),
                ));
            }
            _ => {}
        }
        let result = py.detach(|| txn.commit());
        match result {
            Ok(()) => {
                *guard = None;
                *state = TxnState::Committed;
                self.record_success(operation, TxnState::Committed)?;
                Ok(())
            }
            Err(err) => {
                *guard = None;
                *state = TxnState::RolledBack;
                Err(self.transaction_error(py, operation, error::embedded_err(err)))
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

    #[pyo3(signature = (*, request_id = None))]
    fn rollback(&self, request_id: Option<String>) -> PyResult<()> {
        let operation = self.new_operation_outcome("rollback", request_id)?;
        if let Some(txn) = self
            .inner
            .txn
            .lock()
            .map_err(|_| error::to_py_err("transaction lock poisoned"))
            .map_err(|err| Python::attach(|py| self.transaction_error(py, operation.clone(), err)))?
            .as_ref()
        {
            if txn.session().status()
                == alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive
            {
                return Python::attach(|py| {
                    Err(self.transaction_error(
                        py,
                        operation,
                        error::stream_error(
                            "stream_active",
                            "rollback is not allowed while a transaction stream is active",
                        ),
                    ))
                });
            }
        }
        match self.finalize_with(|txn| txn.rollback(), TxnState::RolledBack) {
            Ok(()) => {
                self.record_success(operation, TxnState::RolledBack)?;
                Ok(())
            }
            Err(err) => Python::attach(|py| Err(self.transaction_error(py, operation, err))),
        }
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
            let operation = self.new_operation_outcome("rollback", None)?;
            self.finalize_with(|txn| txn.rollback(), TxnState::RolledBack)?;
            self.record_success(operation, TxnState::RolledBack)?;
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
            "test-transaction-1".to_string(),
            "test-request-1".to_string(),
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
                .execute_sql(
                    py,
                    "INSERT INTO t (id) VALUES (?)",
                    Some(params.into_any()),
                    None,
                )
                .expect("insert");
            assert_eq!(affected.extract::<u64>(py).expect("affected"), 1);

            // 同一トランザクション内で未コミットの行が見える
            let rows = txn
                .execute_sql(py, "SELECT id FROM t", None, None)
                .expect("select");
            let rows = rows.bind(py).cast::<PyList>().expect("list").clone();
            assert_eq!(rows.len(), 1);

            txn.commit(py, None).expect("commit");
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
            txn.execute_sql(
                py,
                "INSERT INTO t (id) VALUES (?)",
                Some(params.into_any()),
                None,
            )
            .expect("insert");
        });
        txn.rollback(None).expect("rollback");
        assert_eq!(query_row_count(&db, "SELECT id FROM t;"), 0);
    }

    #[test]
    fn execute_sql_on_completed_transaction_is_error() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .expect("ddl");
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        txn.rollback(None).expect("rollback");
        Python::attach(|py| {
            let err = txn
                .execute_sql(py, "SELECT id FROM t", None, None)
                .expect_err("completed txn");
            assert!(err.is_instance_of::<crate::error::PyAlopexError>(py));
        });
    }

    #[test]
    fn put_get_and_rollback() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        txn.put(b"key", b"value", None).expect("put");
        txn.rollback(None).expect("rollback");

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
            txn.commit(py, None).expect("commit");
        });
        assert!(txn.get(b"key", None).is_err());
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
            let outcome = status
                .get_item("transaction")
                .unwrap()
                .unwrap()
                .cast_into::<PyDict>()
                .unwrap();
            assert_eq!(
                outcome
                    .get_item("outcome_version")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "v0.9"
            );
            assert_eq!(
                outcome
                    .get_item("transaction_id")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "test-transaction-1"
            );
            assert_eq!(
                outcome
                    .get_item("request_id")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "test-request-1"
            );
            assert_eq!(
                outcome
                    .get_item("state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "running"
            );
            assert_eq!(
                outcome
                    .get_item("routing")
                    .unwrap()
                    .unwrap()
                    .cast_into::<PyDict>()
                    .unwrap()
                    .get_item("kind")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "local_only"
            );
            txn.rollback(None).unwrap();
            let status = txn.status(py).unwrap();
            let status = status.bind(py);
            assert_eq!(
                status
                    .get_item("state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "rolled_back"
            );
            assert_eq!(
                status
                    .get_item("transaction")
                    .unwrap()
                    .unwrap()
                    .cast_into::<PyDict>()
                    .unwrap()
                    .get_item("state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "cancelled"
            );
        });
    }

    #[test]
    fn read_only_put_is_error() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = transaction(Arc::clone(&db), TxnMode::ReadOnly);
        assert!(txn.put(b"key", b"value", None).is_err());
    }

    #[test]
    fn drop_rolls_back_uncommitted() {
        let db = Arc::new(alopex_embedded::Database::new());
        {
            let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
            txn.put(b"key", b"value", None).expect("put");
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
                .execute_sql_stream("SELECT id FROM stream_t", None, None, None, None)
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
            let error = txn
                .commit(py, None)
                .expect_err("active stream blocks commit");
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
            txn.commit(py, None).unwrap();
        });

        let aborted = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        Python::attach(|py| {
            let stream = aborted
                .execute_sql_stream("SELECT id FROM stream_t", None, None, None, None)
                .unwrap();
            stream.close().unwrap();
            let error = aborted
                .commit(py, None)
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
            aborted.rollback(None).unwrap();
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
                None,
            )
            .expect("transactional ddl");
            txn.execute_sql(py, "INSERT INTO staged_stream (id) VALUES (9)", None, None)
                .expect("transactional dml");

            let stream = txn
                .execute_sql_stream("SELECT id FROM staged_stream", None, None, None, None)
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
            txn.commit(py, None).expect("commit after exhaustion");
        });

        assert_eq!(query_row_count(&db, "SELECT id FROM staged_stream"), 1);
    }
}
