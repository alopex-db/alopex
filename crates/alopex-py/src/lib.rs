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
    fn exported_runtime_types_include_vector_methods_without_feature_flags() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_alopex_runtime_vector_test").unwrap();
            super::_alopex(py, &module).unwrap();
            let database_type = module
                .getattr("database")
                .unwrap()
                .getattr("Database")
                .unwrap();
            for method in [
                "create_hnsw_index",
                "search_hnsw",
                "drop_hnsw_index",
                "get_hnsw_stats",
            ] {
                assert!(database_type.hasattr(method).unwrap(), "Database.{method}");
            }

            let transaction_type = module
                .getattr("transaction")
                .unwrap()
                .getattr("Transaction")
                .unwrap();
            for method in [
                "upsert_vector",
                "search_similar",
                "get_vector",
                "upsert_to_hnsw",
                "delete_from_hnsw",
            ] {
                assert!(
                    transaction_type.hasattr(method).unwrap(),
                    "Transaction.{method}"
                );
            }
        });
    }
}
