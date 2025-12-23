use pyo3::prelude::*;
use pyo3::types::PyModule;

pub fn register(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
