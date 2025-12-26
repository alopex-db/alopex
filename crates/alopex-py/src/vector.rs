use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyModule;
use pyo3::Bound;
#[allow(deprecated)]
use pyo3::ToPyObject;

use crate::error;

use numpy::{PyArray1, PyReadonlyArray1};

pub fn require_numpy(py: Python<'_>) -> PyResult<()> {
    if PyModule::import(py, "numpy").is_ok() {
        Ok(())
    } else {
        Err(error::to_py_err(
            "NumPy が見つかりません。`pip install alopex[numpy]` を実行してください",
        ))
    }
}

pub fn with_ndarray_f32<'py, F, R>(array: &Bound<'py, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f32]) -> PyResult<R>,
{
    if let Ok(array) = array.extract::<PyReadonlyArray1<'py, f32>>() {
        if let Ok(slice) = array.as_slice() {
            return f(slice);
        }
        let owned: Vec<f32> = array.as_array().iter().copied().collect();
        return f(&owned);
    }

    let py = array.py();
    let numpy = PyModule::import(py, "numpy")?;
    let float32 = numpy.getattr("float32")?;
    let casted = array.call_method1("astype", (float32,))?;
    let casted = casted.extract::<PyReadonlyArray1<'py, f32>>()?;
    if let Ok(slice) = casted.as_slice() {
        return f(slice);
    }
    let owned: Vec<f32> = casted.as_array().iter().copied().collect();
    f(&owned)
}

#[allow(dead_code)]
#[allow(deprecated)]
pub fn vec_to_ndarray<'py>(py: Python<'py>, values: &[f32]) -> PyResult<PyObject> {
    Ok(PyArray1::from_slice(py, values).to_object(py))
}

#[cfg(test)]
mod tests {
    use super::with_ndarray_f32;
    use numpy::PyArray1;
    use pyo3::IntoPyObject;
    use pyo3::Python;

    #[test]
    fn ndarray_to_vec_converts_to_float32() {
        Python::with_gil(|py| {
            let array = PyArray1::from_vec(py, vec![1.25_f64, 2.5_f64]);
            let bound = array.into_pyobject(py).unwrap();
            let values =
                with_ndarray_f32(bound.as_any(), |values| Ok(values.to_vec())).expect("convert");
            assert_eq!(values, vec![1.25_f32, 2.5_f32]);
        });
    }
}
