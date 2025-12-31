use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyModule;
use pyo3::Bound;
#[allow(deprecated)]
use pyo3::ToPyObject;

use crate::error;

use numpy::{PyArray1, PyArrayMethods, PyReadonlyArray1};

// =============================================================================
// SliceOrOwned - GIL 解放対応のゼロコピー/フォールバック判別型
// =============================================================================

/// GIL 解放中に安全に使用できる配列データの表現
///
/// - `Borrowed`: C-contiguous float32 配列の場合、raw pointer + 長さ + PyArray 参照を保持
///   - `_guard` が生存している間、`ptr` は有効
///   - GIL 解放中に `unsafe { std::slice::from_raw_parts(ptr, len) }` でスライス再構築可能
/// - `Owned`: それ以外の場合、`Vec<f32>` として所有データを保持
pub enum SliceOrOwned {
    /// ゼロコピーパス: C-contiguous float32 配列への借用参照
    Borrowed {
        /// 配列データへの raw pointer
        ptr: *const f32,
        /// 配列の長さ
        len: usize,
        /// PyArray への参照（GIL 解放中もオブジェクトを生存させる）
        _guard: Py<PyArray1<f32>>,
    },
    /// フォールバックパス: コピーされた所有データ
    Owned(Vec<f32>),
}

pub fn require_numpy(py: Python<'_>) -> PyResult<()> {
    if PyModule::import(py, "numpy").is_ok() {
        Ok(())
    } else {
        Err(error::to_py_err(
            "NumPy が見つかりません。`pip install numpy` を実行してください",
        ))
    }
}

/// 旧 API: 後方互換性のため維持（新コードは with_ndarray_f32_gil_safe を使用）
#[allow(dead_code)]
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

// =============================================================================
// with_ndarray_f32_gil_safe - GIL 解放対応の配列変換
// =============================================================================

/// NumPy 配列を GIL 解放に対応した形式に変換する
///
/// C-contiguous float32 配列の場合は `SliceOrOwned::Borrowed` を返し、
/// それ以外の場合は `SliceOrOwned::Owned` を返す。
///
/// # Example
///
/// ```ignore
/// vector::with_ndarray_f32_gil_safe(array, |slice_or_owned| {
///     match slice_or_owned {
///         SliceOrOwned::Borrowed { ptr, len, _guard } => {
///             py.allow_threads(|| {
///                 let values = unsafe { std::slice::from_raw_parts(ptr, len) };
///                 // use values...
///             })
///         }
///         SliceOrOwned::Owned(vec) => {
///             py.allow_threads(|| {
///                 // use &vec...
///             })
///         }
///     }
/// })
/// ```
pub fn with_ndarray_f32_gil_safe<'py, F, R>(array: &Bound<'py, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(SliceOrOwned) -> PyResult<R>,
{
    let py = array.py();

    // まず float32 として抽出を試みる
    if let Ok(array_f32) = array.extract::<PyReadonlyArray1<'py, f32>>() {
        // C-contiguous かつスライス取得可能な場合はゼロコピー
        if let Ok(slice) = array_f32.as_slice() {
            let ptr = slice.as_ptr();
            let len = slice.len();
            // PyArray への参照を Py<T> として保持（GIL 解放中もオブジェクトを生存させる）
            let guard: Py<PyArray1<f32>> = array_f32.as_untyped().clone().extract()?;
            return f(SliceOrOwned::Borrowed {
                ptr,
                len,
                _guard: guard,
            });
        }
        // 非連続の場合はコピー
        let owned: Vec<f32> = array_f32.as_array().iter().copied().collect();
        return f(SliceOrOwned::Owned(owned));
    }

    // float32 でない場合は変換
    let numpy = PyModule::import(py, "numpy")?;
    let float32 = numpy.getattr("float32")?;
    let casted = array.call_method1("astype", (float32,))?;
    let casted_f32 = casted.extract::<PyReadonlyArray1<'py, f32>>()?;

    // 変換後の配列が C-contiguous な場合
    if let Ok(slice) = casted_f32.as_slice() {
        let ptr = slice.as_ptr();
        let len = slice.len();
        let guard: Py<PyArray1<f32>> = casted_f32.as_untyped().clone().extract()?;
        return f(SliceOrOwned::Borrowed {
            ptr,
            len,
            _guard: guard,
        });
    }

    // 非連続の場合はコピー
    let owned: Vec<f32> = casted_f32.as_array().iter().copied().collect();
    f(SliceOrOwned::Owned(owned))
}

// =============================================================================
// 出力用変換関数 - Rust → NumPy（所有権移譲）
// =============================================================================

/// Vec<f32> を NumPy 配列に変換（所有権移譲によるゼロコピー）
///
/// `PyArray1::from_vec` を使用して所有権を Python 側に移譲する。
/// 返された配列は Python の GC によって管理される。
#[allow(dead_code)]
pub fn owned_vec_to_ndarray(py: Python<'_>, values: Vec<f32>) -> PyResult<PyObject> {
    Ok(PyArray1::from_vec(py, values)
        .into_pyobject(py)?
        .into_any()
        .unbind())
}

/// Box<[f32]> を NumPy 配列に変換（所有権移譲によるゼロコピー）
///
/// Box を Vec に変換してから `PyArray1::from_vec` を使用する。
#[allow(dead_code)]
pub fn owned_to_ndarray(py: Python<'_>, values: Box<[f32]>) -> PyResult<PyObject> {
    let vec: Vec<f32> = values.into_vec();
    owned_vec_to_ndarray(py, vec)
}

/// Option<Vec<f32>> を Option<ndarray> に変換（所有権移譲）
///
/// 検索結果の `vector` フィールド用。
#[allow(dead_code)]
pub fn vec_to_ndarray_opt(py: Python<'_>, values: Option<Vec<f32>>) -> PyResult<Option<PyObject>> {
    match values {
        Some(v) => Ok(Some(owned_vec_to_ndarray(py, v)?)),
        None => Ok(None),
    }
}

/// Vec<f32> を NumPy 配列に変換（コピー）
///
/// `zero_copy_return=False` の場合に使用。
/// `PyArray1::from_slice` を使用してデータをコピーする。
#[allow(dead_code)]
#[allow(deprecated)]
pub fn vec_to_ndarray_copy(py: Python<'_>, values: &[f32]) -> PyResult<PyObject> {
    Ok(PyArray1::from_slice(py, values).to_object(py))
}

/// Option<Vec<f32>> を Option<ndarray> に変換（コピー）
///
/// `zero_copy_return=False` の場合に使用。
#[allow(dead_code)]
pub fn vec_to_ndarray_opt_copy(
    py: Python<'_>,
    values: Option<&[f32]>,
) -> PyResult<Option<PyObject>> {
    match values {
        Some(v) => Ok(Some(vec_to_ndarray_copy(py, v)?)),
        None => Ok(None),
    }
}

#[allow(dead_code)]
#[allow(deprecated)]
pub fn vec_to_ndarray<'py>(py: Python<'py>, values: &[f32]) -> PyResult<PyObject> {
    Ok(PyArray1::from_slice(py, values).to_object(py))
}

#[cfg(test)]
mod tests {
    use super::vec_to_ndarray_opt_copy;
    use super::with_ndarray_f32;
    use super::with_ndarray_f32_gil_safe;
    use super::{owned_to_ndarray, SliceOrOwned};
    use numpy::PyArray1;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;
    use pyo3::types::PyAnyMethods;
    use pyo3::types::PyDict;
    use pyo3::types::PyDictMethods;
    use pyo3::types::PyModule;
    use pyo3::types::PySlice;
    use pyo3::IntoPyObject;
    use pyo3::PyResult;
    use pyo3::Python;
    use std::env;
    use std::fmt::Display;
    use std::sync::Once;

    static PY_INIT: Once = Once::new();

    fn with_py<F, R>(f: F) -> R
    where
        F: for<'py> FnOnce(Python<'py>) -> R,
    {
        PY_INIT.call_once(pyo3::prepare_freethreaded_python);
        Python::with_gil(f)
    }

    fn tc_fail(msg: impl Into<String>) -> TestCaseError {
        TestCaseError::fail(msg.into())
    }

    fn tc_py<T>(result: PyResult<T>) -> Result<T, TestCaseError> {
        result.map_err(|e| tc_fail(format!("{e}")))
    }

    fn tc_err(msg: impl Display) -> TestCaseError {
        tc_fail(msg.to_string())
    }

    fn add_site_dirs_from_pythonpath(py: Python<'_>, pythonpath: &str) -> PyResult<()> {
        let site = PyModule::import(py, "site")?;
        let sep = if cfg!(windows) { ';' } else { ':' };
        for entry in pythonpath.split(sep).filter(|s| !s.is_empty()) {
            site.call_method1("addsitedir", (entry,))?;
        }
        Ok(())
    }

    fn ensure_numpy<'py>(py: Python<'py>) -> PyResult<pyo3::Bound<'py, PyModule>> {
        let err = match PyModule::import(py, "numpy") {
            Ok(module) => return Ok(module),
            Err(err) => err,
        };
        if let Ok(pythonpath) = env::var("PYTHONPATH") {
            add_site_dirs_from_pythonpath(py, &pythonpath)?;
            return PyModule::import(py, "numpy");
        }
        Err(err)
    }

    fn require_numpy<'py>(py: Python<'py>) -> pyo3::Bound<'py, PyModule> {
        ensure_numpy(py).expect("numpy must be available")
    }

    fn require_numpy_tc<'py>(py: Python<'py>) -> Result<pyo3::Bound<'py, PyModule>, TestCaseError> {
        ensure_numpy(py).map_err(|e| tc_err(format!("numpy must be available: {e}")))
    }

    #[test]
    fn ndarray_to_vec_converts_to_float32() {
        with_py(|py| {
            if ensure_numpy(py).is_err() {
                return;
            }
            let array = PyArray1::from_vec(py, vec![1.25_f64, 2.5_f64]);
            let bound = array.into_pyobject(py).unwrap();
            let values =
                with_ndarray_f32(bound.as_any(), |values| Ok(values.to_vec())).expect("convert");
            assert_eq!(values, vec![1.25_f32, 2.5_f32]);
        });
    }

    #[test]
    fn with_ndarray_f32_gil_safe_borrowed_for_contiguous_f32() {
        with_py(|py| {
            if ensure_numpy(py).is_err() {
                return;
            }
            let array = PyArray1::from_vec(py, vec![1.0_f32, 2.0_f32, 3.0_f32]);
            let bound = array.into_pyobject(py).unwrap();
            let observed =
                with_ndarray_f32_gil_safe(bound.as_any(), |slice_or_owned| match slice_or_owned {
                    SliceOrOwned::Borrowed {
                        ptr,
                        len,
                        _guard: _,
                    } => {
                        let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                        Ok(values.to_vec())
                    }
                    SliceOrOwned::Owned(_) => panic!("expected Borrowed"),
                })
                .expect("convert");
            assert_eq!(observed, vec![1.0_f32, 2.0_f32, 3.0_f32]);
        });
    }

    #[test]
    fn with_ndarray_f32_gil_safe_owned_for_strided_f32() {
        with_py(|py| {
            let numpy = match ensure_numpy(py) {
                Ok(m) => m,
                Err(_) => return,
            };
            let base = numpy.call_method1("arange", (10,)).unwrap();
            let float32 = numpy.getattr("float32").unwrap();
            let base = base.call_method1("astype", (float32,)).unwrap();
            let slice = PySlice::new(py, 0, 10, 2);
            let strided = base.get_item(slice).unwrap();
            let observed =
                with_ndarray_f32_gil_safe(&strided, |slice_or_owned| match slice_or_owned {
                    SliceOrOwned::Borrowed { .. } => panic!("expected Owned"),
                    SliceOrOwned::Owned(vec) => Ok(vec),
                })
                .expect("convert");
            assert_eq!(observed, vec![0.0, 2.0, 4.0, 6.0, 8.0]);
        });
    }

    #[test]
    fn with_ndarray_f32_gil_safe_copies_float64_and_returns_f32_values() {
        with_py(|py| {
            let numpy = require_numpy(py);
            let float64 = numpy.getattr("float64").unwrap();
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", float64).unwrap();
            let arr = numpy
                .call_method("array", (vec![1.25_f64, 2.5_f64],), Some(&kwargs))
                .unwrap();
            let observed = with_ndarray_f32_gil_safe(&arr, |slice_or_owned| match slice_or_owned {
                SliceOrOwned::Borrowed {
                    ptr,
                    len,
                    _guard: _,
                } => {
                    let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                    Ok(values.to_vec())
                }
                SliceOrOwned::Owned(_) => panic!("expected Borrowed after astype(float32)"),
            })
            .expect("convert");
            assert_eq!(observed, vec![1.25_f32, 2.5_f32]);
        });
    }

    proptest! {
        #[test]
        fn owned_to_ndarray_roundtrips(values in proptest::collection::vec(any::<f32>(), 1..256)) {
            with_py(|py| -> Result<(), TestCaseError> {
                require_numpy_tc(py)?;
                let obj = tc_py(owned_to_ndarray(py, values.clone().into_boxed_slice()))?;
                let bound = obj.bind(py);
                let arr = tc_py(bound.extract::<numpy::PyReadonlyArray1<'_, f32>>())?;
                let slice = arr.as_slice().map_err(|e| tc_fail(format!("{e}")))?;
                prop_assert_eq!(slice.len(), values.len());
                for (a, b) in slice.iter().copied().zip(values.iter().copied()) {
                    prop_assert_eq!(a.to_bits(), b.to_bits());
                }
                Ok(())
            })?;
        }

        #[test]
        fn vec_to_ndarray_opt_copy_roundtrips(values in proptest::collection::vec(any::<f32>(), 0..256)) {
            with_py(|py| -> Result<(), TestCaseError> {
                require_numpy_tc(py)?;
                let obj = tc_py(vec_to_ndarray_opt_copy(py, Some(values.as_slice())))?
                    .expect("some");
                let bound = obj.bind(py);
                let arr = tc_py(bound.extract::<numpy::PyReadonlyArray1<'_, f32>>())?;
                let slice = arr.as_slice().map_err(|e| tc_fail(format!("{e}")))?;
                prop_assert_eq!(slice.len(), values.len());
                for (a, b) in slice.iter().copied().zip(values.iter().copied()) {
                    prop_assert_eq!(a.to_bits(), b.to_bits());
                }
                Ok(())
            })?;
        }
    }
}
