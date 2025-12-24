use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyModule;

pub(crate) struct PyTransactionInner {
    pub(crate) txn: Mutex<Option<alopex_embedded::Transaction<'static>>>,
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
        };
        Self {
            db,
            inner: Arc::new(inner),
        }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTransaction>()?;
    Ok(())
}
