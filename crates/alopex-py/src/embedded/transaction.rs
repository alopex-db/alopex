use std::sync::{Arc, Mutex, Weak};

use alopex_sql::{AlopexDialect, Parser};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};

use crate::embedded::async_stream::PyNativeAsyncSqlResultStream;
use crate::embedded::local_scan::PyLocalScan;
use crate::embedded::sql::{self, PyPreparedBinding};
use crate::embedded::stream::{PySqlResultStream, StreamLeaseRegistry};
use crate::embedded::thread_mode::DatabaseControl;
use crate::error;
use crate::types::{PyMetric, PySearchResult};
use crate::vector;
use crate::vector::SliceOrOwned;
use alopex_core::Key;
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

/// Borrow the existing owner for one finite call; never create a rollback-on-drop wrapper.
pub(super) fn with_prepared_transaction<T: Send>(
    py: Python<'_>,
    inner: &Arc<PyTransactionInner>,
    executing: bool,
    operation: impl FnOnce(&mut alopex_embedded::OwnedEmbeddedTransaction) -> alopex_embedded::Result<T>
        + Send,
) -> PyResult<T> {
    enum CallError {
        StateLock,
        TransactionLock,
        Closed,
        StreamActive,
        MustAbort,
        Native(alopex_embedded::Error),
    }
    let result = py.detach(|| {
        let state = inner.state.lock().map_err(|_| CallError::StateLock)?;
        if *state != TxnState::Active {
            return Err(CallError::Closed);
        }
        let mut guard = inner.txn.lock().map_err(|_| CallError::TransactionLock)?;
        let txn = guard.as_mut().ok_or(CallError::Closed)?;
        if executing {
            match txn.session().status() {
                alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive => {
                    return Err(CallError::StreamActive);
                }
                alopex_core::txn::OwnedTransactionSessionStatus::MustAbort => {
                    return Err(CallError::MustAbort);
                }
                _ => {}
            }
        }
        operation(txn).map_err(CallError::Native)
    });
    result.map_err(|err| match err {
        CallError::StateLock => error::to_py_err("transaction state lock poisoned"),
        CallError::TransactionLock => error::to_py_err("transaction lock poisoned"),
        CallError::Closed => error::to_py_err("transaction is closed"),
        CallError::StreamActive => {
            error::stream_error("stream_active", "transaction stream is active")
        }
        CallError::MustAbort => error::stream_error(
            "stream_abort_required",
            "transaction stream requires rollback",
        ),
        CallError::Native(err) => error::embedded_err(err),
    })
}

/// Close tracked sessions without exposing lifecycle state or retaining locks across GIL attach.
pub(super) fn rollback_tracked_for_database_close(
    py: Python<'_>,
    tracked: &Mutex<Vec<Weak<PyTransactionInner>>>,
    #[cfg(test)] rollback_fail_count: &std::sync::atomic::AtomicUsize,
) -> PyResult<()> {
    enum CloseError {
        TrackingLock,
        StateLock,
        TransactionLock,
        Native(alopex_embedded::Error),
        #[cfg(test)]
        Injected,
    }

    let result = py.detach(|| {
        let mut tracked = tracked.lock().map_err(|_| CloseError::TrackingLock)?;
        let mut first_error = None;
        tracked.retain(|weak| {
            let Some(handle) = weak.upgrade() else {
                return false;
            };
            // Match commit's state -> transaction order. The tracking guard is also
            // acquired and released in this detached phase, before any Python error.
            let mut state = match handle.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    first_error.get_or_insert(CloseError::StateLock);
                    return true;
                }
            };
            let mut transaction = match handle.txn.lock() {
                Ok(transaction) => transaction,
                Err(_) => {
                    first_error.get_or_insert(CloseError::TransactionLock);
                    return true;
                }
            };
            #[cfg(test)]
            // `try_update` is unstable on the project's Rust 1.90 MSRV.
            #[allow(deprecated)]
            if rollback_fail_count
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                first_error.get_or_insert(CloseError::Injected);
                return true;
            }
            if let Some(txn) = transaction.as_mut() {
                if let Err(error) = txn.rollback() {
                    // Preserve the error and handle, not a promise that the consuming
                    // native rollback can be retried after a backend failure.
                    first_error.get_or_insert(CloseError::Native(error));
                    return true;
                }
            }
            *transaction = None;
            if *state == TxnState::Active {
                *state = TxnState::RolledBack;
            }
            false
        });
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    });
    result.map_err(|error| match error {
        CloseError::TrackingLock => error::to_py_err("transaction tracking lock poisoned"),
        CloseError::StateLock => error::to_py_err("transaction state lock poisoned"),
        CloseError::TransactionLock => error::to_py_err("transaction lock poisoned"),
        CloseError::Native(error) => error::embedded_err(error),
        #[cfg(test)]
        CloseError::Injected => error::to_py_err("ロールバック失敗（テスト注入）"),
    })
}

#[pyclass(name = "Transaction")]
pub struct PyTransaction {
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
        let txn = db
            .begin_owned_embedded_transaction(mode)
            .map_err(error::embedded_err)?;
        let inner = PyTransactionInner {
            txn: Mutex::new(Some(txn)),
            state: Mutex::new(TxnState::Active),
        };
        Ok(Self {
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

    #[pyo3(signature = (prefix, *, limit = 100, cursor = None, scan_budget = 10_000, max_bytes = 16_777_216, include_internal = false))]
    #[allow(clippy::too_many_arguments)]
    fn scan_prefix(
        &self,
        py: Python<'_>,
        prefix: &[u8],
        limit: usize,
        cursor: Option<Vec<u8>>,
        scan_budget: usize,
        max_bytes: usize,
        include_internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.control.ensure_open()?;
        let options = alopex_embedded::KeyScanOptions {
            limit,
            cursor,
            scan_budget,
            max_bytes,
            include_internal,
        };
        let page = with_prepared_transaction(py, &self.inner, true, |txn| {
            txn.scan_prefix_page(prefix, &options)
        })?;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value));
        Ok(PyList::new(py, entries)?.call_method0("__iter__")?.unbind())
    }

    #[pyo3(signature = (start, end, *, limit = 100, cursor = None, scan_budget = 10_000, max_bytes = 16_777_216, include_internal = false))]
    #[allow(clippy::too_many_arguments)]
    fn scan_range(
        &self,
        py: Python<'_>,
        start: &[u8],
        end: &[u8],
        limit: usize,
        cursor: Option<Vec<u8>>,
        scan_budget: usize,
        max_bytes: usize,
        include_internal: bool,
    ) -> PyResult<Py<PyAny>> {
        self.control.ensure_open()?;
        let options = alopex_embedded::KeyScanOptions {
            limit,
            cursor,
            scan_budget,
            max_bytes,
            include_internal,
        };
        let page = with_prepared_transaction(py, &self.inner, true, |txn| {
            txn.scan_range_page(start, end, &options)
        })?;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value));
        Ok(PyList::new(py, entries)?.call_method0("__iter__")?.unbind())
    }

    #[pyo3(signature = (pattern, mode = "glob", limit = 100, cursor = None, scan_budget = 10_000, max_bytes = 16_777_216, *, include_internal = false))]
    #[allow(clippy::too_many_arguments)]
    fn search_keys(
        &self,
        py: Python<'_>,
        pattern: Bound<'_, PyAny>,
        mode: &str,
        limit: usize,
        cursor: Option<Vec<u8>>,
        scan_budget: usize,
        max_bytes: usize,
        include_internal: bool,
    ) -> PyResult<Py<PyDict>> {
        let pattern = match mode {
            "glob" => alopex_embedded::KeyPattern::glob(pattern.extract::<Vec<u8>>()?),
            "regex" => alopex_embedded::KeyPattern::regex(pattern.extract::<String>()?),
            _ => {
                return Err(PyValueError::new_err(
                    "mode must be either 'glob' or 'regex'",
                ));
            }
        };
        let request = alopex_embedded::KeySearchRequest {
            pattern,
            cursor,
            limit,
            scan_budget,
            max_bytes,
        };
        self.control.ensure_open()?;
        let page = with_prepared_transaction(py, &self.inner, true, |txn| {
            txn.search_keys_visible(&request, include_internal)
        })?;
        let alopex_embedded::KeySearchPage {
            entries,
            next_cursor,
            scanned,
        } = page;
        let response = PyDict::new(py);
        response.set_item(
            "entries",
            entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
        )?;
        response.set_item("next_cursor", next_cursor)?;
        response.set_item("scanned", scanned)?;
        Ok(response.unbind())
    }

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

    #[pyo3(signature = (name, keys, vectors, metadata = None))]
    fn upsert_to_hnsw_batch(
        &self,
        py: Python<'_>,
        name: &str,
        keys: Vec<Vec<u8>>,
        vectors: Py<PyAny>,
        metadata: Option<Vec<Option<Vec<u8>>>>,
    ) -> PyResult<usize> {
        vector::require_numpy(py)?;
        let name = name.to_owned();
        vector::with_ndarray_f32_2d_gil_safe(vectors.bind(py), |vectors| match vectors {
            vector::MatrixSliceOrOwned::Borrowed {
                ptr,
                len,
                rows,
                columns,
                _guard,
            } => {
                let _guard = _guard;
                let ptr = ptr as usize;
                py.detach(move || {
                    let values = unsafe { std::slice::from_raw_parts(ptr as *const f32, len) };
                    let vector_refs: Vec<&[f32]> = if columns == 0 {
                        (0..rows).map(|_| &[][..]).collect()
                    } else {
                        values.chunks_exact(columns).collect()
                    };
                    self.with_txn_mut(|txn| {
                        txn.upsert_to_hnsw_batch(&name, &keys, &vector_refs, metadata.as_deref())
                    })
                })
            }
            vector::MatrixSliceOrOwned::Owned {
                values,
                rows,
                columns,
            } => py.detach(move || {
                let vector_refs: Vec<&[f32]> = if columns == 0 {
                    (0..rows).map(|_| &[][..]).collect()
                } else {
                    values.chunks_exact(columns).take(rows).collect()
                };
                self.with_txn_mut(|txn| {
                    txn.upsert_to_hnsw_batch(&name, &keys, &vector_refs, metadata.as_deref())
                })
            }),
        })
    }

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

    #[pyo3(signature = (keys, metric, zero_copy_return = true))]
    fn get_vectors(
        &self,
        py: Python<'_>,
        keys: Vec<Key>,
        metric: PyMetric,
        zero_copy_return: bool,
    ) -> PyResult<Vec<Option<Py<PyAny>>>> {
        vector::require_numpy(py)?;
        let vectors =
            py.detach(|| self.with_txn_mut(|txn| txn.get_vectors(&keys, metric.into())))?;
        vectors
            .into_iter()
            .map(|values| {
                if zero_copy_return {
                    vector::vec_to_ndarray_opt(py, values)
                } else {
                    vector::vec_to_ndarray_opt_copy(py, values.as_deref())
                }
            })
            .collect()
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
        sql::validate_sql_input(sql)?;
        let bindings = sql::prepared_bindings(params.as_ref())?;
        sql::bind_rendered_params(sql, &vec![String::new(); bindings.len()])?;
        self.ensure_active()?;
        if sql::is_transaction_control_statement(sql) {
            return Err(error::to_py_err(
                "Transaction.execute_sql does not accept transaction control SQL; use Transaction.savepoint(), Transaction.rollback_to(), Transaction.release(), Transaction.commit(), or Transaction.rollback()",
            ));
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

        let prepared_statement = (!bindings.is_empty()
            && bindings
                .iter()
                .all(|binding| matches!(binding, PyPreparedBinding::Native(_))))
        .then(|| Parser::parse_sql(&AlopexDialect, sql).ok())
        .flatten()
        .and_then(|mut statements| {
            if statements.len() == 1 {
                statements.pop()
            } else {
                None
            }
        });
        let result = if let Some(statement) = prepared_statement {
            let values = bindings
                .into_iter()
                .map(|binding| match binding {
                    PyPreparedBinding::Native(native) => native,
                    PyPreparedBinding::Rendered(_) => unreachable!("all bindings are native"),
                })
                .collect::<Vec<_>>();
            py.detach(|| {
                let mut guard = self.inner.txn.lock().map_err(|_| ExecError::LockPoisoned)?;
                let txn = guard.as_mut().ok_or(ExecError::Closed)?;
                txn.execute_prepared_statement(&statement, &values)
                    .map_err(ExecError::Embedded)
            })
        } else {
            let rendered = bindings
                .into_iter()
                .map(sql::render_prepared_binding)
                .collect::<alopex_embedded::Result<Vec<_>>>()
                .map_err(error::embedded_err)?;
            let bound_sql = sql::bind_rendered_params(sql, &rendered)?;
            py.detach(|| {
                let mut guard = self.inner.txn.lock().map_err(|_| ExecError::LockPoisoned)?;
                let txn = guard.as_mut().ok_or(ExecError::Closed)?;
                txn.execute_sql(&bound_sql).map_err(ExecError::Embedded)
            })
        };
        let result = result.map_err(|err| match err {
            ExecError::LockPoisoned => error::to_py_err("transaction lock poisoned"),
            ExecError::Closed => error::to_py_err("transaction is closed"),
            ExecError::Embedded(err) => error::embedded_err(err),
        })?;
        sql::execution_result_to_py(py, result)
    }

    /// Prepare one native-bound statement owned by this transaction.
    fn prepare(
        &self,
        py: Python<'_>,
        sql: &str,
    ) -> PyResult<crate::embedded::database::PyPreparedStatement> {
        crate::embedded::database::PyPreparedStatement::for_transaction(
            py,
            sql,
            Arc::clone(&self.inner),
            Arc::clone(&self.control),
        )
    }

    /// Execute native rows without committing. Execution errors require explicit rollback.
    fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        rows: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.prepare(py, sql)?.execute_many(py, rows)
    }

    /// Create a named savepoint within this transaction.
    fn savepoint(&self, name: &str) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.create_savepoint(name))
    }

    /// Roll back changes made after the latest matching savepoint.
    fn rollback_to(&self, name: &str) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.rollback_to_savepoint(name))
    }

    /// Release the latest matching savepoint and its descendants.
    fn release(&self, name: &str) -> PyResult<()> {
        self.with_txn_mut(|txn| txn.release_savepoint(name))
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
        enum CommitError {
            StateLock,
            TransactionLock,
            Closed,
            StreamActive,
            StreamAbortRequired,
            Native(alopex_embedded::Error),
        }

        // No Python error may be constructed while these guards are held: doing
        // so could reacquire the GIL before another Python caller releases it.
        let result = py.detach(|| {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| CommitError::StateLock)?;
            if *state != TxnState::Active {
                return Err(CommitError::Closed);
            }
            let mut guard = self
                .inner
                .txn
                .lock()
                .map_err(|_| CommitError::TransactionLock)?;
            let txn = guard.as_mut().ok_or(CommitError::Closed)?;
            match txn.session().status() {
                alopex_core::txn::OwnedTransactionSessionStatus::LeaseActive => {
                    return Err(CommitError::StreamActive);
                }
                alopex_core::txn::OwnedTransactionSessionStatus::MustAbort => {
                    return Err(CommitError::StreamAbortRequired);
                }
                _ => {}
            }
            match txn.commit() {
                Ok(()) => {
                    *guard = None;
                    *state = TxnState::Committed;
                    Ok(())
                }
                Err(err) => {
                    *guard = None;
                    *state = TxnState::RolledBack;
                    Err(CommitError::Native(err))
                }
            }
        });
        result.map_err(|err| match err {
            CommitError::StateLock => error::to_py_err("transaction state lock poisoned"),
            CommitError::TransactionLock => error::to_py_err("transaction lock poisoned"),
            CommitError::Closed => error::to_py_err("transaction is closed"),
            CommitError::StreamActive => error::stream_error(
                "stream_active",
                "commit is not allowed while a transaction stream is active",
            ),
            CommitError::StreamAbortRequired => error::stream_error(
                "stream_abort_required",
                "transaction stream requires rollback before commit",
            ),
            CommitError::Native(err) => error::embedded_err(err),
        })
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
    use tempfile::tempdir;

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
    fn savepoint_rollback_to_and_release_preserve_prior_sql_work() {
        pyo3::Python::initialize();
        let db = Arc::new(alopex_embedded::Database::new());
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .expect("ddl");
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);

        Python::attach(|py| {
            txn.execute_sql(py, "INSERT INTO t (id) VALUES (1)", None)
                .expect("first insert");
            txn.savepoint("retry").expect("savepoint");
            txn.execute_sql(py, "INSERT INTO t (id) VALUES (2)", None)
                .expect("second insert");
            txn.rollback_to("retry").expect("rollback to savepoint");
            txn.release("retry").expect("release savepoint");
            txn.commit(py).expect("commit");
        });

        assert_eq!(query_row_count(&db, "SELECT id FROM t;"), 1);
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
    fn commit_releases_gil_while_waiting_and_locks_before_reattaching() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        Python::initialize();
        let deadline = Duration::from_secs(3);
        let mut outcomes = Vec::new();
        for lock_state in [true, false] {
            let db = Arc::new(alopex_embedded::Database::new());
            let txn = Arc::new(transaction(Arc::clone(&db), TxnMode::ReadWrite));
            txn.put(b"committed-key", b"committed-value").unwrap();
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let holder_inner = Arc::clone(&txn.inner);
            let holder = thread::spawn(move || {
                if lock_state {
                    let _guard = holder_inner.state.lock().unwrap();
                    let _ = held_tx.send(());
                    // The native coordinator releases this guard even on a failed probe.
                    let _ = release_rx.recv();
                } else {
                    let _guard = holder_inner.txn.lock().unwrap();
                    let _ = held_tx.send(());
                    let _ = release_rx.recv();
                }
            });
            if held_rx.recv_timeout(deadline).is_err() {
                let _ = release_tx.send(());
                holder.join().unwrap();
                panic!("mutex holder did not become ready");
            }

            let (started_tx, started_rx) = mpsc::channel();
            let commit_txn = Arc::clone(&txn);
            let commit = thread::spawn(move || {
                Python::attach(|py| {
                    // Notify only after acquiring the GIL, before entering the real method.
                    let _ = started_tx.send(());
                    commit_txn.commit(py).map_err(|error| error.to_string())
                })
            });
            let started = started_rx.recv_timeout(deadline).is_ok();
            let (ready_tx, ready_rx) = mpsc::channel();
            let (observe_tx, observe_rx) = mpsc::channel();
            let (unlocked_tx, unlocked_rx) = mpsc::channel();
            let (stop_tx, stop_rx) = mpsc::channel();
            let observer_inner = Arc::clone(&txn.inner);
            // Never let a probe which acquired the GIL before commit count as progress.
            let observer = started.then(|| {
                thread::spawn(move || {
                    Python::attach(|_| {
                        let _ = ready_tx.send(());
                        if observe_rx.recv() != Ok(true) {
                            return;
                        }
                        // Keep the GIL: commit can finish only its detached native phase.
                        // All timeouts belong to the GIL-free coordinator, not this worker.
                        loop {
                            if let Ok(state) = observer_inner.state.try_lock() {
                                if *state == super::TxnState::Committed {
                                    if let Ok(transaction) = observer_inner.txn.try_lock() {
                                        if transaction.is_none() {
                                            let _ = unlocked_tx.send(());
                                            break;
                                        }
                                    }
                                }
                            }
                            match stop_rx.try_recv() {
                                Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
                                Err(mpsc::TryRecvError::Empty) => thread::yield_now(),
                            }
                        }
                    });
                })
            });
            let gil_progress = started && ready_rx.recv_timeout(deadline).is_ok();
            // Release the only contended mutex before waiting for or asserting any result.
            let _ = release_tx.send(());
            let _ = observe_tx.send(gil_progress);
            let locks_released = gil_progress && unlocked_rx.recv_timeout(deadline).is_ok();
            let _ = stop_tx.send(());
            // On RED the observer can now acquire the GIL and consume the queued false.
            holder.join().unwrap();
            if let Some(observer) = observer {
                observer.join().unwrap();
            }
            let committed = commit.join().unwrap();
            let mut reader = db.begin(TxnMode::ReadOnly).unwrap();
            assert_eq!(
                reader.get(b"committed-key").unwrap(),
                Some(b"committed-value".to_vec())
            );
            assert!(committed.is_ok(), "commit result: {committed:?}");
            assert_eq!(*txn.inner.state.lock().unwrap(), super::TxnState::Committed);
            assert!(txn.inner.txn.lock().unwrap().is_none());
            outcomes.push((lock_state, started, gil_progress, locks_released));
        }
        // Both state/txn cells run and every worker joins before a regression can fail.
        assert!(
            outcomes
                .iter()
                .all(|(_, started, progress, unlocked)| *started && *progress && *unlocked),
            "(state_mutex, started, GIL_progress, locks_before_GIL_return): {outcomes:?}"
        );
    }

    #[test]
    fn prepared_execution_serializes_with_close_and_commit_without_gil_deadlock() {
        use std::sync::{atomic::AtomicUsize, mpsc, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};

        Python::initialize();
        let deadline = Duration::from_secs(3);
        for close in [true, false] {
            let db = Arc::new(alopex_embedded::Database::new());
            db.execute_sql("CREATE TABLE prepared_race (id INTEGER PRIMARY KEY)")
                .unwrap();
            let txn = Arc::new(transaction(Arc::clone(&db), TxnMode::ReadWrite));
            let tracked = Arc::new(Mutex::new(vec![Arc::downgrade(&txn.inner)]));
            let statement = alopex_sql::Parser::parse_sql(
                &alopex_sql::AlopexDialect,
                "INSERT INTO prepared_race VALUES (?)",
            )
            .unwrap()
            .remove(0);
            let expected = if close {
                super::TxnState::RolledBack
            } else {
                super::TxnState::Committed
            };
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (executed_tx, executed_rx) = mpsc::channel();
            let prepared_inner = Arc::clone(&txn.inner);
            let prepared = thread::spawn(move || {
                let result = Python::attach(|py| {
                    super::with_prepared_transaction(
                        py,
                        &prepared_inner,
                        true,
                        move |transaction| {
                            // Both guards are held: close/commit must first wait for state.
                            let _ = held_tx.send(());
                            if release_rx.recv_timeout(deadline * 4).is_err() {
                                return Ok(None);
                            }
                            transaction
                                .execute_prepared_statement(
                                    &statement,
                                    &[alopex_sql::SqlValue::Integer(1)],
                                )
                                .map(Some)
                        },
                    )
                    .map_err(|error| error.to_string())
                });
                let _ = executed_tx.send(result);
            });
            let held = held_rx.recv_timeout(deadline).is_ok();
            let (started_tx, started_rx) = mpsc::channel();
            let (finished_tx, finished_rx) = mpsc::channel();
            let finish_txn = Arc::clone(&txn);
            let finish_tracked = Arc::clone(&tracked);
            let finisher = thread::spawn(move || {
                let result = Python::attach(|py| {
                    // This notification precedes the method's state-lock acquisition.
                    let _ = started_tx.send(());
                    if close {
                        super::rollback_tracked_for_database_close(
                            py,
                            &finish_tracked,
                            &AtomicUsize::new(0),
                        )
                    } else {
                        finish_txn.commit(py)
                    }
                    .map_err(|error| error.to_string())
                });
                let _ = finished_tx.send(result);
            });
            let started = started_rx.recv_timeout(deadline).is_ok();
            let (ready_tx, ready_rx) = mpsc::channel();
            let (observe_tx, observe_rx) = mpsc::channel();
            let (unlocked_tx, unlocked_rx) = mpsc::channel();
            let (stop_tx, stop_rx) = mpsc::channel();
            let observer_inner = Arc::clone(&txn.inner);
            let observer = thread::spawn(move || {
                Python::attach(|_| {
                    let _ = ready_tx.send(());
                    if observe_rx.recv_timeout(deadline * 4) != Ok(true) {
                        return;
                    }
                    // Keep the GIL until every native guard is observable as released.
                    let until = Instant::now() + deadline;
                    while Instant::now() < until {
                        if let Ok(registry) = tracked.try_lock() {
                            if registry.is_empty() == close {
                                if let Ok(state) = observer_inner.state.try_lock() {
                                    if *state == expected {
                                        if let Ok(transaction) = observer_inner.txn.try_lock() {
                                            if transaction.is_none() {
                                                let _ = unlocked_tx.send(());
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if !matches!(stop_rx.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                            break;
                        }
                        thread::yield_now();
                    }
                });
            });
            let gil_progress = started && ready_rx.recv_timeout(deadline).is_ok();
            // Release both workers even if a GIL probe failed; never assert while blocked.
            let _ = release_tx.send(());
            let _ = observe_tx.send(held && gil_progress);
            let unlocked = held && gil_progress && unlocked_rx.recv_timeout(deadline).is_ok();
            let _ = stop_tx.send(());
            let executed = executed_rx.recv_timeout(deadline);
            let finished = finished_rx.recv_timeout(deadline);
            let mut joined = true;
            for worker in [prepared, finisher, observer] {
                let until = Instant::now() + deadline;
                while !worker.is_finished() && Instant::now() < until {
                    thread::yield_now();
                }
                // Bound join waits; the runner's process timeout still guards lock regressions.
                joined &= worker.is_finished() && worker.join().is_ok();
            }
            assert!(
                held && gil_progress && unlocked && joined,
                "close={close}: held={held}, GIL_progress={gil_progress}, guards_released={unlocked}, joined={joined}"
            );
            assert!(
                matches!(
                    executed,
                    Ok(Ok(Some(alopex_sql::ExecutionResult::RowsAffected(1))))
                ),
                "prepared: {executed:?}"
            );
            assert!(
                matches!(finished, Ok(Ok(()))),
                "close={close}: {finished:?}"
            );
            assert_eq!(*txn.inner.state.lock().unwrap(), expected);
            assert!(txn.inner.txn.lock().unwrap().is_none());
            assert_eq!(
                query_row_count(&db, "SELECT id FROM prepared_race"),
                usize::from(!close)
            );
            let late = Python::attach(|py| {
                super::with_prepared_transaction::<()>(py, &txn.inner, true, |_| {
                    panic!("late prepared execution must not enter a completed transaction")
                })
                .map_err(|error| error.to_string())
            });
            assert!(late.is_err_and(|error| error.contains("transaction is closed")));
        }
    }

    #[test]
    fn committed_transaction_does_not_keep_database_locked() {
        pyo3::Python::initialize();
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("locked.alopex");
        let db = Arc::new(alopex_embedded::Database::open(&path).expect("open"));
        let txn = transaction(Arc::clone(&db), TxnMode::ReadWrite);
        drop(db);

        Python::attach(|py| txn.commit(py).expect("commit"));

        let reopened = alopex_embedded::Database::open(&path)
            .expect("a committed transaction must not keep the database locked");
        drop(reopened);
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
