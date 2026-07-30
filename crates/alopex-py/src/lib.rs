use pyo3::prelude::*;
use pyo3::types::PyModule;

mod catalog;
mod embedded;
mod error;
mod types;
mod vector;

#[pymodule]
fn _alopex(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AlopexError", py.get_type::<error::PyAlopexError>())?;
    m.add("ALOPEX_ERROR_CODES", error::ERROR_CODES.to_vec())?;
    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyCatalogInfo>()?;
    m.add_class::<catalog::PyNamespaceInfo>()?;
    m.add_class::<catalog::PyTableInfo>()?;
    m.add_class::<catalog::PyColumnInfo>()?;
    let database_module = PyModule::new(py, "database")?;
    embedded::database::register(py, &database_module)?;
    m.add_submodule(&database_module)?;

    m.add_class::<embedded::async_stream::PyNativeAsyncPayload>()?;
    m.add_class::<embedded::async_stream::PyNativeAsyncSqlResultStream>()?;

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

#[cfg(test)]
mod tests {
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};
    use pyo3::Python;

    #[test]
    fn exported_database_sql_stream_reads_auto_committed_rows() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_alopex_runtime_test").unwrap();
            super::_alopex(py, &module).unwrap();
            let database = module
                .getattr("database")
                .unwrap()
                .getattr("Database")
                .unwrap()
                .call_method0("new")
                .unwrap();
            database
                .call_method1(
                    "execute_sql",
                    ("CREATE TABLE runtime_stream (id INTEGER PRIMARY KEY)",),
                )
                .unwrap();
            database
                .call_method1(
                    "execute_sql",
                    ("INSERT INTO runtime_stream (id) VALUES (1)",),
                )
                .unwrap();
            let stream = database
                .call_method1("execute_sql_stream", ("SELECT id FROM runtime_stream",))
                .unwrap();
            let row = stream.call_method0("__next__").unwrap();
            let row = row.cast::<PyDict>().unwrap();
            assert_eq!(
                row.get_item("id")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
        });
    }

    #[test]
    fn exported_database_module_includes_the_sync_changefeed_type() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_alopex_changefeed_export_test").unwrap();
            super::_alopex(py, &module).unwrap();
            let database = module.getattr("database").unwrap();
            assert!(database.getattr("Database").is_ok());
            assert!(database.getattr("Changefeed").is_ok());
            let codes: Vec<String> = module
                .getattr("ALOPEX_ERROR_CODES")
                .unwrap()
                .extract()
                .unwrap();
            for expected in [
                "changefeed_unauthorized",
                "changefeed_conflict",
                "changefeed_unavailable",
                "changefeed_prerequisite_missing",
                "changefeed_timeout",
                "changefeed_invalid_request",
                "changefeed_internal",
                "changefeed_cancelled",
                "changefeed_unsupported",
            ] {
                assert!(
                    codes.iter().any(|code| code == expected),
                    "missing {expected}"
                );
            }
        });
    }
}
