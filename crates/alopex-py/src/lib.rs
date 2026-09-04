use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

mod catalog;
mod embedded;
mod error;
mod types;
mod vector;

#[cfg(test)]
mod test_harness {
    /// Every unit test process needs Python before an error path can create a `PyErr`.
    /// cargo-nextest starts one process per test, so initialization cannot depend on
    /// another test running first.
    #[ctor::ctor]
    fn initialize_python() {
        pyo3::Python::initialize();
    }
}

#[pymodule]
fn _alopex(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AlopexError", py.get_type::<error::PyAlopexError>())?;
    m.add("ALOPEX_ERROR_CODES", error::ERROR_CODES.to_vec())?;
    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyCatalogInfo>()?;
    m.add_class::<catalog::PyNamespaceInfo>()?;
    m.add_class::<catalog::PyTableInfo>()?;
    m.add_class::<catalog::PyColumnInfo>()?;
    // pure-Python のサーバークライアント（python/alopex/remote.py）が使う
    // 内部関数。先頭アンダースコアなので __init__.py の _export_public は
    // 再公開しない（公開 API 面は変わらない）。
    m.add_function(wrap_pyfunction!(embedded::sql::bind_sql_params_py, m)?)?;
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

    /// The server client reuses the embedded `?` binder through this private
    /// module-level function; without it the two surfaces would drift (D2).
    #[test]
    fn exported_module_provides_private_sql_param_binder() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_alopex_runtime_binder_test").unwrap();
            super::_alopex(py, &module).unwrap();
            let binder = module.getattr("_bind_sql_params").unwrap();
            assert_eq!(
                binder
                    .call1(("SELECT 1",))
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "SELECT 1"
            );
            let params = pyo3::types::PyList::new(py, [1i64]).unwrap();
            assert_eq!(
                binder
                    .call1(("SELECT ?", params))
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "SELECT 1"
            );
        });
    }

    /// The forwarded server codes and the client-side `ALOPEX-PY2##` block are
    /// part of the published registry, so the Python contract test can assert
    /// an exact set.
    #[test]
    fn error_code_registry_covers_the_server_client_surface() {
        for code in [
            "ALOPEX-PY201",
            "ALOPEX-PY202",
            "ALOPEX-PY203",
            "ALOPEX-PY204",
            "ALOPEX-PY205",
            "UNAUTHORIZED",
            "QUERY_TIMEOUT",
            "SESSION_EXPIRED",
            "SERVER_BACKPRESSURE",
        ] {
            assert!(
                super::error::ERROR_CODES.contains(&code),
                "ERROR_CODES must publish {code}"
            );
        }
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
                "execute_sql_stream",
                "query_stream",
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
                "execute_sql_stream",
                "query_stream",
            ] {
                assert!(
                    transaction_type.hasattr(method).unwrap(),
                    "Transaction.{method}"
                );
            }
        });
    }
}
