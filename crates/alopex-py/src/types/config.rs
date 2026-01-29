use pyo3::basic::CompareOp;
use pyo3::prelude::*;

#[pyclass(name = "TxnMode", frozen)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PyTxnMode {
    pub(crate) inner: alopex_core::TxnMode,
}

impl Default for PyTxnMode {
    fn default() -> Self {
        Self {
            inner: alopex_core::TxnMode::ReadOnly,
        }
    }
}

impl From<alopex_core::TxnMode> for PyTxnMode {
    fn from(value: alopex_core::TxnMode) -> Self {
        Self { inner: value }
    }
}

impl From<PyTxnMode> for alopex_core::TxnMode {
    fn from(value: PyTxnMode) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyTxnMode {
    #[classattr]
    const READ_ONLY: Self = Self {
        inner: alopex_core::TxnMode::ReadOnly,
    };
    #[classattr]
    const READ_WRITE: Self = Self {
        inner: alopex_core::TxnMode::ReadWrite,
    };

    fn __repr__(&self) -> String {
        match self.inner {
            alopex_core::TxnMode::ReadOnly => "TxnMode.READ_ONLY".to_string(),
            alopex_core::TxnMode::ReadWrite => "TxnMode.READ_WRITE".to_string(),
        }
    }

    fn __richcmp__(&self, other: PyRef<'_, PyTxnMode>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner == other.inner),
            CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Ok(false),
        }
    }

    fn __hash__(&self) -> isize {
        match self.inner {
            alopex_core::TxnMode::ReadOnly => 1,
            alopex_core::TxnMode::ReadWrite => 2,
        }
    }
}

#[pyclass(name = "Metric", frozen)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PyMetric {
    pub(crate) inner: alopex_core::Metric,
}

impl Default for PyMetric {
    fn default() -> Self {
        Self {
            inner: alopex_core::Metric::Cosine,
        }
    }
}

impl From<alopex_core::Metric> for PyMetric {
    fn from(value: alopex_core::Metric) -> Self {
        Self { inner: value }
    }
}

impl From<PyMetric> for alopex_core::Metric {
    fn from(value: PyMetric) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyMetric {
    #[classattr]
    const COSINE: Self = Self {
        inner: alopex_core::Metric::Cosine,
    };
    #[classattr]
    const L2: Self = Self {
        inner: alopex_core::Metric::L2,
    };
    #[classattr]
    const INNER_PRODUCT: Self = Self {
        inner: alopex_core::Metric::InnerProduct,
    };

    fn __repr__(&self) -> String {
        match self.inner {
            alopex_core::Metric::Cosine => "Metric.COSINE".to_string(),
            alopex_core::Metric::L2 => "Metric.L2".to_string(),
            alopex_core::Metric::InnerProduct => "Metric.INNER_PRODUCT".to_string(),
        }
    }

    fn __richcmp__(&self, other: PyRef<'_, PyMetric>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner == other.inner),
            CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Ok(false),
        }
    }

    fn __hash__(&self) -> isize {
        match self.inner {
            alopex_core::Metric::Cosine => 1,
            alopex_core::Metric::L2 => 2,
            alopex_core::Metric::InnerProduct => 3,
        }
    }
}

#[pyclass(name = "StorageMode", frozen)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PyStorageMode {
    pub(crate) inner: alopex_embedded::StorageMode,
}

impl Default for PyStorageMode {
    fn default() -> Self {
        Self {
            inner: alopex_embedded::StorageMode::InMemory,
        }
    }
}

impl From<alopex_embedded::StorageMode> for PyStorageMode {
    fn from(value: alopex_embedded::StorageMode) -> Self {
        Self { inner: value }
    }
}

impl From<PyStorageMode> for alopex_embedded::StorageMode {
    fn from(value: PyStorageMode) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyStorageMode {
    #[classattr]
    const DISK: Self = Self {
        inner: alopex_embedded::StorageMode::Disk,
    };
    #[classattr]
    const IN_MEMORY: Self = Self {
        inner: alopex_embedded::StorageMode::InMemory,
    };

    fn __repr__(&self) -> String {
        match self.inner {
            alopex_embedded::StorageMode::Disk => "StorageMode.DISK".to_string(),
            alopex_embedded::StorageMode::InMemory => "StorageMode.IN_MEMORY".to_string(),
        }
    }

    fn __richcmp__(&self, other: PyRef<'_, PyStorageMode>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner == other.inner),
            CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Ok(false),
        }
    }

    fn __hash__(&self) -> isize {
        match self.inner {
            alopex_embedded::StorageMode::Disk => 1,
            alopex_embedded::StorageMode::InMemory => 2,
        }
    }
}

#[pyclass(name = "HnswConfig")]
#[derive(Clone, Debug)]
pub struct PyHnswConfig {
    #[pyo3(get, set)]
    pub dim: usize,
    #[pyo3(get, set)]
    pub m: usize,
    #[pyo3(get, set)]
    pub ef_construction: usize,
    #[pyo3(get, set)]
    pub metric: PyMetric,
}

impl Default for PyHnswConfig {
    fn default() -> Self {
        Self {
            dim: 0,
            m: 16,
            ef_construction: 200,
            metric: PyMetric::default(),
        }
    }
}

impl From<PyHnswConfig> for alopex_core::HnswConfig {
    fn from(value: PyHnswConfig) -> Self {
        Self {
            dimension: value.dim,
            metric: value.metric.into(),
            m: value.m,
            ef_construction: value.ef_construction,
        }
    }
}

#[pymethods]
impl PyHnswConfig {
    #[new]
    #[pyo3(signature = (dim, m = 16, ef_construction = 200, metric = None))]
    fn new(dim: usize, m: usize, ef_construction: usize, metric: Option<PyMetric>) -> Self {
        Self {
            dim,
            m,
            ef_construction,
            metric: metric.unwrap_or_default(),
        }
    }
}

#[pyclass(name = "EmbeddedConfig")]
#[derive(Clone, Debug, Default)]
pub struct PyEmbeddedConfig {
    #[pyo3(get, set)]
    pub memory_limit_bytes: Option<usize>,
}

impl PyEmbeddedConfig {
    pub fn to_embedded(&self) -> alopex_embedded::EmbeddedConfig {
        match self.memory_limit_bytes {
            Some(limit) => alopex_embedded::EmbeddedConfig::in_memory_with_limit(limit),
            None => alopex_embedded::EmbeddedConfig::in_memory(),
        }
    }
}

#[pymethods]
impl PyEmbeddedConfig {
    #[new]
    #[pyo3(signature = (memory_limit_bytes = None))]
    fn new(memory_limit_bytes: Option<usize>) -> Self {
        Self { memory_limit_bytes }
    }
}

#[pyclass(name = "DatabaseOptions")]
#[derive(Clone, Debug, Default)]
pub struct PyDatabaseOptions {
    #[pyo3(get, set)]
    pub path: Option<String>,
    #[pyo3(get, set)]
    pub storage_mode: PyStorageMode,
    #[pyo3(get, set)]
    pub memory_limit_bytes: Option<usize>,
    #[pyo3(get, set)]
    pub enable_metrics: bool,
}

#[pymethods]
impl PyDatabaseOptions {
    #[new]
    #[pyo3(signature = (path = None, storage_mode = None, memory_limit_bytes = None, enable_metrics = false))]
    fn new(
        path: Option<String>,
        storage_mode: Option<PyStorageMode>,
        memory_limit_bytes: Option<usize>,
        enable_metrics: bool,
    ) -> Self {
        Self {
            path,
            storage_mode: storage_mode.unwrap_or_default(),
            memory_limit_bytes,
            enable_metrics,
        }
    }
}
