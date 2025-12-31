use pyo3::prelude::*;
use pyo3::types::PyModule;

mod catalog;
mod embedded;
mod error;
mod types;
#[cfg(feature = "numpy")]
mod vector;

#[pymodule]
fn _alopex(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AlopexError", py.get_type::<error::PyAlopexError>())?;
    let database_module = PyModule::new(py, "database")?;
    embedded::database::register(py, &database_module)?;
    m.add_submodule(&database_module)?;

    let transaction_module = PyModule::new(py, "transaction")?;
    embedded::transaction::register(py, &transaction_module)?;
    m.add_submodule(&transaction_module)?;

    let types_module = PyModule::new(py, "types")?;
    types::register(py, &types_module)?;
    m.add_submodule(&types_module)?;

    let catalog_module = PyModule::new(py, "catalog")?;
    catalog::register(py, &catalog_module)?;
    m.add_submodule(&catalog_module)?;

    Ok(())
}
