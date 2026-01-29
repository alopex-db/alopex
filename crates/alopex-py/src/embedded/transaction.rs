use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::PyObject;

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
    pub(crate) txn: Mutex<Option<alopex_embedded::Transaction<'static>>>,
    state: Mutex<TxnState>,
}

#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    #[allow(dead_code)]
    pub(crate) db: Arc<alopex_embedded::Database>,
    #[allow(dead_code)]
    pub(crate) inner: Arc<PyTransactionInner>,
}

impl PyTransaction {
    pub(crate) fn from_txn(
        db: Arc<alopex_embedded::Database>,
        txn: alopex_embedded::Transaction<'_>,
    ) -> Self {
        // SAFETY: We tie the transaction lifetime to the Arc<Database> stored in PyTransaction.
        let txn_static = unsafe {
            std::mem::transmute::<
                alopex_embedded::Transaction<'_>,
                alopex_embedded::Transaction<'static>,
            >(txn)
        };
        let inner = PyTransactionInner {
            txn: Mutex::new(Some(txn_static)),
            state: Mutex::new(TxnState::Active),
        };
        Self {
            db,
            inner: Arc::new(inner),
        }
    }

    fn ensure_active(&self) -> PyResult<()> {
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

    fn with_txn_mut<F, T>(&self, op: F) -> PyResult<T>
    where
        F: FnOnce(&mut alopex_embedded::Transaction<'static>) -> Result<T, alopex_embedded::Error>,
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
        F: FnOnce(alopex_embedded::Transaction<'static>) -> Result<(), alopex_embedded::Error>,
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
            .take()
            .ok_or_else(|| error::to_py_err("transaction is closed"))?;
        match op(txn) {
            Ok(()) => {
                *state = success_state;
                Ok(())
            }
            Err(err) => {
                *state = TxnState::RolledBack;
                Err(error::embedded_err(err))
            }
        }
    }
}

#[pymethods]
impl PyTransaction {
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
        metadata: PyObject,
        vector: PyObject,
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
                    py.allow_threads(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| txn.upsert_vector(&key, &payload, values, metric))
                    })
                }
                SliceOrOwned::Owned(vec) => py.allow_threads(move || {
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
        query: PyObject,
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
                    py.allow_threads(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| {
                            txn.search_similar(values, metric_enum, k, filter_keys.as_deref())
                        })
                    })
                }
                SliceOrOwned::Owned(vec) => py.allow_threads(move || {
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
                py.allow_threads(|| self.with_txn_mut(|txn| txn.get_vectors(&keys, metric_enum)))?;

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
        vector: PyObject,
        metadata: Option<PyObject>,
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
                    py.allow_threads(move || {
                        let ptr = ptr as *const f32;
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        self.with_txn_mut(|txn| txn.upsert_to_hnsw(&name, &key, values, &payload))
                    })
                }
                SliceOrOwned::Owned(vec) => py.allow_threads(move || {
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
    ) -> PyResult<PyObject> {
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
            .take()
            .ok_or_else(|| error::to_py_err("transaction is closed"))?;
        let result = py.allow_threads(|| txn.commit());
        match result {
            Ok(()) => {
                *state = TxnState::Committed;
                Ok(())
            }
            Err(err) => {
                *state = TxnState::RolledBack;
                Err(error::embedded_err(err))
            }
        }
    }

    fn rollback(&self) -> PyResult<()> {
        self.finalize_with(|txn| txn.rollback(), TxnState::RolledBack)
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.ensure_active()?;
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type = None, _exc = None, _traceback = None))]
    fn __exit__(
        &self,
        _exc_type: Option<PyObject>,
        _exc: Option<PyObject>,
        _traceback: Option<PyObject>,
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
        if let Some(txn) = guard.take() {
            let _ = txn.rollback();
        }
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
    use pyo3::Python;
    use std::sync::Arc;

    #[test]
    fn put_get_and_rollback() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = db
            .begin(TxnMode::ReadWrite)
            .map(|txn| super::PyTransaction::from_txn(Arc::clone(&db), txn))
            .expect("txn");
        txn.put(b"key", b"value").expect("put");
        txn.rollback().expect("rollback");

        let mut txn2 = db.begin(TxnMode::ReadOnly).expect("txn2");
        let value = txn2.get(b"key").expect("get");
        assert!(value.is_none());
    }

    #[test]
    fn commit_closes_transaction() {
        pyo3::prepare_freethreaded_python();
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = db
            .begin(TxnMode::ReadWrite)
            .map(|txn| super::PyTransaction::from_txn(Arc::clone(&db), txn))
            .expect("txn");
        Python::with_gil(|py| {
            txn.commit(py).expect("commit");
        });
        assert!(txn.get(b"key").is_err());
    }

    #[test]
    fn read_only_put_is_error() {
        let db = Arc::new(alopex_embedded::Database::new());
        let txn = db
            .begin(TxnMode::ReadOnly)
            .map(|txn| super::PyTransaction::from_txn(Arc::clone(&db), txn))
            .expect("txn");
        assert!(txn.put(b"key", b"value").is_err());
    }

    #[test]
    fn drop_rolls_back_uncommitted() {
        let db = Arc::new(alopex_embedded::Database::new());
        {
            let txn = db
                .begin(TxnMode::ReadWrite)
                .map(|txn| super::PyTransaction::from_txn(Arc::clone(&db), txn))
                .expect("txn");
            txn.put(b"key", b"value").expect("put");
        }
        let mut txn2 = db.begin(TxnMode::ReadOnly).expect("txn2");
        let value = txn2.get(b"key").expect("get");
        assert!(value.is_none());
    }
}
