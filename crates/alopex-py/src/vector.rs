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

// SAFETY: SliceOrOwned::Borrowed の ptr は _guard が生存している間のみ有効
// _guard は Py<T> であり Send を実装しているため、SliceOrOwned も Send を実装可能
// ただし、ptr のデリファレンスは _guard が生存している間のみ安全
unsafe impl Send for SliceOrOwned {}

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
    values: Option<&Vec<f32>>,
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
    use super::with_ndarray_f32;
    use numpy::PyArray1;
    use pyo3::types::PyModule;
    use pyo3::IntoPyObject;
    use pyo3::Python;

    #[test]
    fn ndarray_to_vec_converts_to_float32() {
        Python::with_gil(|py| {
            if PyModule::import(py, "numpy").is_err() {
                return;
            }
            let array = PyArray1::from_vec(py, vec![1.25_f64, 2.5_f64]);
            let bound = array.into_pyobject(py).unwrap();
            let values =
                with_ndarray_f32(bound.as_any(), |values| Ok(values.to_vec())).expect("convert");
            assert_eq!(values, vec![1.25_f32, 2.5_f32]);
        });
    }
}
