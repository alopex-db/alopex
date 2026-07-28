use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods, PyList, PyListMethods};
use pyo3::IntoPyObjectExt;

/// Convert the shared Phase 2 outcome into the same nested mapping shape used
/// by HTTP/CLI JSON.  The Python binding must not invent a private result
/// schema or reinterpret CRDT state.
pub(crate) fn crdt_outcome_to_py(
    py: Python<'_>,
    outcome: &alopex_cluster::crdt::CrdtOutcome,
) -> PyResult<Py<PyDict>> {
    let document = serde_json::to_value(outcome)
        .map_err(|error| crate::error::to_py_err(error.to_string()))?;
    let value = json_value_to_py(py, &document)?;
    value
        .into_bound(py)
        .cast_into::<PyDict>()
        .map(Bound::unbind)
        .map_err(|_| crate::error::to_py_err("CRDT outcome must serialize to a Python dict"))
}

fn json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(value) => Ok(value.into_py_any(py)?),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(value.into_py_any(py)?)
            } else if let Some(value) = value.as_u64() {
                Ok(value.into_py_any(py)?)
            } else if let Some(value) = value.as_f64() {
                Ok(value.into_py_any(py)?)
            } else {
                Err(crate::error::to_py_err("unsupported CRDT JSON number"))
            }
        }
        serde_json::Value::String(value) => Ok(value.clone().into_py_any(py)?),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_value_to_py(py, value)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, json_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

#[pyclass(name = "SearchResult", skip_from_py_object)]
pub struct PySearchResult {
    #[pyo3(get, set)]
    pub key: Vec<u8>,
    #[pyo3(get, set)]
    pub score: f32,
    #[pyo3(get, set)]
    pub metadata: Option<Vec<u8>>,
    /// ベクトルデータ（`return_vectors=True` の場合のみ設定）
    /// NumPy ndarray[float32] または None
    #[pyo3(get)]
    pub vector: Option<Py<PyAny>>,
}

impl Clone for PySearchResult {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            score: self.score,
            metadata: self.metadata.clone(),
            vector: self
                .vector
                .as_ref()
                .map(|obj| Python::attach(|py| obj.clone_ref(py))),
        }
    }
}

impl std::fmt::Debug for PySearchResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySearchResult")
            .field("key", &self.key)
            .field("score", &self.score)
            .field("metadata", &self.metadata)
            .field("vector", &self.vector.as_ref().map(|_| "<ndarray>"))
            .finish()
    }
}

#[pymethods]
impl PySearchResult {
    #[new]
    #[pyo3(signature = (key, score, metadata = None, vector = None))]
    fn new(key: Vec<u8>, score: f32, metadata: Option<Vec<u8>>, vector: Option<Py<PyAny>>) -> Self {
        Self {
            key,
            score,
            metadata,
            vector,
        }
    }
}

impl From<alopex_embedded::SearchResult> for PySearchResult {
    fn from(value: alopex_embedded::SearchResult) -> Self {
        Self {
            key: value.key,
            score: value.score,
            metadata: if value.metadata.is_empty() {
                None
            } else {
                Some(value.metadata)
            },
            vector: None, // デフォルトは None（後方互換性）
        }
    }
}

impl From<alopex_core::HnswSearchResult> for PySearchResult {
    fn from(value: alopex_core::HnswSearchResult) -> Self {
        Self {
            key: value.key,
            score: value.distance,
            metadata: if value.metadata.is_empty() {
                None
            } else {
                Some(value.metadata)
            },
            vector: None, // デフォルトは None（後方互換性）
        }
    }
}

impl PySearchResult {
    /// ベクトルデータを含む検索結果を作成
    #[allow(dead_code)]
    pub fn with_vector(
        key: Vec<u8>,
        score: f32,
        metadata: Option<Vec<u8>>,
        vector: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            key,
            score,
            metadata,
            vector,
        }
    }
}

#[pyclass(name = "HnswStats", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyHnswStats {
    #[pyo3(get, set)]
    pub node_count: u64,
    #[pyo3(get, set)]
    pub deleted_count: u64,
    #[pyo3(get, set)]
    pub level_distribution: Vec<u64>,
    #[pyo3(get, set)]
    pub memory_bytes: u64,
    #[pyo3(get, set)]
    pub avg_edges_per_node: f64,
}

impl From<alopex_core::HnswStats> for PyHnswStats {
    fn from(value: alopex_core::HnswStats) -> Self {
        Self {
            node_count: value.node_count,
            deleted_count: value.deleted_count,
            level_distribution: value.level_distribution,
            memory_bytes: value.memory_bytes,
            avg_edges_per_node: value.avg_edges_per_node,
        }
    }
}

#[pymethods]
impl PyHnswStats {
    #[new]
    #[pyo3(signature = (
        node_count = 0,
        deleted_count = 0,
        level_distribution = Vec::new(),
        memory_bytes = 0,
        avg_edges_per_node = 0.0
    ))]
    fn new(
        node_count: u64,
        deleted_count: u64,
        level_distribution: Vec<u64>,
        memory_bytes: u64,
        avg_edges_per_node: f64,
    ) -> Self {
        Self {
            node_count,
            deleted_count,
            level_distribution,
            memory_bytes,
            avg_edges_per_node,
        }
    }
}

#[pyclass(name = "MemoryStats", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyMemoryStats {
    #[pyo3(get, set)]
    pub total_bytes: u64,
    #[pyo3(get, set)]
    pub used_bytes: u64,
    #[pyo3(get, set)]
    pub free_bytes: u64,
}

impl PyMemoryStats {
    pub fn with_total(total_bytes: u64, used_bytes: u64) -> Self {
        let free_bytes = if total_bytes > 0 {
            total_bytes.saturating_sub(used_bytes)
        } else {
            0
        };
        Self {
            total_bytes,
            used_bytes,
            free_bytes,
        }
    }
}

impl From<alopex_core::MemoryStats> for PyMemoryStats {
    fn from(value: alopex_core::MemoryStats) -> Self {
        let used_bytes = value.kv_bytes.saturating_add(value.index_bytes) as u64;
        Self::with_total(value.total_bytes as u64, used_bytes)
    }
}

#[pymethods]
impl PyMemoryStats {
    #[new]
    #[pyo3(signature = (total_bytes, used_bytes))]
    fn new(total_bytes: u64, used_bytes: u64) -> Self {
        Self::with_total(total_bytes, used_bytes)
    }
}
