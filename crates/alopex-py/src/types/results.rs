use pyo3::prelude::*;

#[pyclass(name = "SearchResult")]
#[derive(Clone, Debug)]
pub struct PySearchResult {
    #[pyo3(get, set)]
    pub key: Vec<u8>,
    #[pyo3(get, set)]
    pub score: f32,
    #[pyo3(get, set)]
    pub metadata: Option<Vec<u8>>,
}

#[pymethods]
impl PySearchResult {
    #[new]
    #[pyo3(signature = (key, score, metadata = None))]
    fn new(key: Vec<u8>, score: f32, metadata: Option<Vec<u8>>) -> Self {
        Self {
            key,
            score,
            metadata,
        }
    }
}

#[pyclass(name = "HnswStats")]
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

#[pyclass(name = "MemoryStats")]
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
