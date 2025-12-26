use std::fmt::Display;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::PyErr;

create_exception!(crate::error, AlopexError, PyException);

#[allow(dead_code)]
pub fn to_py_err<E: Display>(err: E) -> PyErr {
    PyErr::new::<AlopexError, _>(err.to_string())
}

#[allow(dead_code)]
pub fn embedded_err(err: alopex_embedded::Error) -> PyErr {
    to_py_err(err)
}

#[allow(dead_code)]
pub fn core_err(err: alopex_core::Error) -> PyErr {
    to_py_err(err)
}
