use alopex_sql::{
    ExecutionStep, ExecutionStepErrorKind, ExecutionStepKind, ExecutionStepOutcome,
    SharedExecutionReport,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

/// One ordered input step for the transport-neutral shared execution model.
#[pyclass(name = "SharedExecutionStep", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PySharedExecutionStep {
    #[pyo3(get)]
    pub step_id: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub sql: Option<String>,
}

#[pymethods]
impl PySharedExecutionStep {
    #[new]
    #[pyo3(signature = (step_id, kind, sql = None))]
    fn new(step_id: String, kind: String, sql: Option<String>) -> PyResult<Self> {
        Self::validate(&kind, sql.as_deref())?;
        Ok(Self { step_id, kind, sql })
    }

    #[staticmethod]
    fn transaction_statement(step_id: String, sql: String) -> Self {
        Self {
            step_id,
            kind: "transaction_statement".to_string(),
            sql: Some(sql),
        }
    }

    #[staticmethod]
    fn commit_barrier(step_id: String) -> Self {
        Self {
            step_id,
            kind: "commit_barrier".to_string(),
            sql: None,
        }
    }

    #[staticmethod]
    fn post_commit_read(step_id: String, sql: String) -> Self {
        Self {
            step_id,
            kind: "post_commit_read".to_string(),
            sql: Some(sql),
        }
    }
}

impl PySharedExecutionStep {
    fn validate(kind: &str, sql: Option<&str>) -> PyResult<()> {
        match (kind, sql) {
            ("transaction_statement" | "post_commit_read", Some(_)) | ("commit_barrier", None) => {
                Ok(())
            }
            ("transaction_statement" | "post_commit_read", None) => Err(PyValueError::new_err(
                format!("shared execution step '{kind}' requires sql"),
            )),
            ("commit_barrier", Some(_)) => Err(PyValueError::new_err(
                "shared execution step 'commit_barrier' does not accept sql",
            )),
            _ => Err(PyValueError::new_err(format!(
                "unknown shared execution step kind: {kind}"
            ))),
        }
    }

    pub(crate) fn to_native(&self) -> PyResult<ExecutionStep> {
        Self::validate(&self.kind, self.sql.as_deref())?;
        let kind = match self.kind.as_str() {
            "transaction_statement" => ExecutionStepKind::TransactionStatement {
                sql: self.sql.clone().expect("validated sql"),
            },
            "commit_barrier" => ExecutionStepKind::CommitBarrier,
            "post_commit_read" => ExecutionStepKind::PostCommitRead {
                sql: self.sql.clone().expect("validated sql"),
            },
            _ => unreachable!("validated shared execution step kind"),
        };
        Ok(ExecutionStep::new(self.step_id.clone(), kind))
    }
}

/// Metadata emitted only after the mutation transaction has committed.
#[pyclass(name = "CommitMetadata", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyCommitMetadata {
    #[pyo3(get)]
    pub transaction_id: String,
}

/// Typed shared-execution failure that preserves the failing phase.
#[pyclass(name = "ExecutionStepError", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyExecutionStepError {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub message: String,
}

/// One correlated outcome in deterministic request order.
#[pyclass(name = "SharedExecutionStepResult", frozen, skip_from_py_object)]
pub struct PySharedExecutionStepResult {
    #[pyo3(get)]
    pub execution_id: String,
    #[pyo3(get)]
    pub transaction_id: String,
    #[pyo3(get)]
    pub step_id: String,
    #[pyo3(get)]
    pub step_index: usize,
    #[pyo3(get)]
    pub outcome_kind: String,
    result: Option<Py<PyAny>>,
    commit_metadata: Option<Py<PyCommitMetadata>>,
    error: Option<Py<PyExecutionStepError>>,
}

#[pymethods]
impl PySharedExecutionStepResult {
    #[getter]
    fn result(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.result.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn commit_metadata(&self, py: Python<'_>) -> Option<Py<PyCommitMetadata>> {
        self.commit_metadata
            .as_ref()
            .map(|value| value.clone_ref(py))
    }

    #[getter]
    fn error(&self, py: Python<'_>) -> Option<Py<PyExecutionStepError>> {
        self.error.as_ref().map(|value| value.clone_ref(py))
    }
}

/// Lossless ordered report for mutation, commit barrier, and post-commit read steps.
#[pyclass(name = "SharedExecutionReport", frozen, skip_from_py_object)]
pub struct PySharedExecutionReport {
    #[pyo3(get)]
    pub execution_id: String,
    #[pyo3(get)]
    pub transaction_id: String,
    steps: Py<PyList>,
}

#[pymethods]
impl PySharedExecutionReport {
    #[getter]
    fn steps(&self, py: Python<'_>) -> Py<PyList> {
        self.steps.clone_ref(py)
    }
}

impl PySharedExecutionReport {
    pub(crate) fn from_native(py: Python<'_>, report: SharedExecutionReport) -> PyResult<Self> {
        let steps = PyList::empty(py);
        for step in report.steps {
            let (outcome_kind, result, commit_metadata, error) = match step.outcome {
                ExecutionStepOutcome::Execution(result) => (
                    "execution".to_string(),
                    Some(crate::embedded::sql::execution_result_to_py(py, result)?),
                    None,
                    None,
                ),
                ExecutionStepOutcome::Commit(metadata) => (
                    "commit".to_string(),
                    None,
                    Some(Py::new(
                        py,
                        PyCommitMetadata {
                            transaction_id: metadata.transaction_id,
                        },
                    )?),
                    None,
                ),
                ExecutionStepOutcome::Error(error) => (
                    "error".to_string(),
                    None,
                    None,
                    Some(Py::new(
                        py,
                        PyExecutionStepError {
                            kind: match error.kind {
                                ExecutionStepErrorKind::Transaction => "transaction",
                                ExecutionStepErrorKind::Commit => "commit",
                                ExecutionStepErrorKind::PostCommitRead => "post_commit_read",
                                ExecutionStepErrorKind::InvalidOrder => "invalid_order",
                            }
                            .to_string(),
                            message: error.message,
                        },
                    )?),
                ),
            };
            steps.append(Py::new(
                py,
                PySharedExecutionStepResult {
                    execution_id: step.execution_id,
                    transaction_id: step.transaction_id,
                    step_id: step.step_id,
                    step_index: step.step_index,
                    outcome_kind,
                    result,
                    commit_metadata,
                    error,
                },
            )?)?;
        }
        Ok(Self {
            execution_id: report.execution_id,
            transaction_id: report.transaction_id,
            steps: steps.unbind(),
        })
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

#[pyclass(name = "SearchStats", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PySearchStats {
    #[pyo3(get, set)]
    pub nodes_visited: u64,
    #[pyo3(get, set)]
    pub distance_computations: u64,
    #[pyo3(get, set)]
    pub search_time_us: u64,
}

impl From<alopex_core::vector::hnsw::SearchStats> for PySearchStats {
    fn from(value: alopex_core::vector::hnsw::SearchStats) -> Self {
        Self {
            nodes_visited: value.nodes_visited,
            distance_computations: value.distance_computations,
            search_time_us: value.search_time_us,
        }
    }
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
