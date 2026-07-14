use std::sync::Arc;

use alopex_dataframe::{col, DataFrame, Series};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, ListBuilder, StringArray,
    StringBuilder, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Utf8,
    Int64,
    TimestampMicros,
    ListUtf8,
}

/// Python wrapper around the Rust alopex-dataframe API.
#[pyclass(name = "DataFrame", skip_from_py_object)]
#[derive(Clone)]
pub struct PyDataFrame {
    inner: DataFrame,
}

impl PyDataFrame {
    fn new(inner: DataFrame) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyDataFrame {
    #[new]
    #[pyo3(signature = (columns, schema = None))]
    fn py_new(columns: &Bound<'_, PyDict>, schema: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        dataframe_from_py(columns, schema).map(Self::new)
    }

    #[staticmethod]
    #[pyo3(signature = (columns, schema = None))]
    fn from_columns(
        columns: &Bound<'_, PyDict>,
        schema: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        dataframe_from_py(columns, schema).map(Self::new)
    }

    fn height(&self) -> usize {
        self.inner.height()
    }

    fn width(&self) -> usize {
        self.inner.width()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        dataframe_to_py_dict(py, &self.inner)
    }

    fn str(&self, column: &str) -> PyStringNamespace {
        PyStringNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
        }
    }

    fn dt(&self, column: &str) -> PyDatetimeNamespace {
        PyDatetimeNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
        }
    }

    fn list(&self, column: &str) -> PyListNamespace {
        PyListNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
        }
    }

    fn explode(&self, column: &str) -> PyResult<Self> {
        self.inner
            .explode(column)
            .map(Self::new)
            .map_err(dataframe_err)
    }

    fn implode(&self) -> PyResult<Self> {
        self.inner.implode().map(Self::new).map_err(dataframe_err)
    }
}

#[pyclass(name = "StringNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyStringNamespace {
    df: DataFrame,
    column: String,
}

#[pymethods]
impl PyStringNamespace {
    #[pyo3(signature = (output = None))]
    fn to_lowercase(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().to_lowercase(), output)
    }

    #[pyo3(signature = (output = None))]
    fn to_uppercase(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().to_uppercase(), output)
    }

    #[pyo3(signature = (pattern, output = None))]
    fn contains(&self, pattern: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().contains(pattern), output)
    }

    #[pyo3(signature = (pattern, replacement, output = None))]
    fn replace(
        &self,
        pattern: &str,
        replacement: &str,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column).str().replace(pattern, replacement),
            output,
        )
    }

    #[pyo3(signature = (chars = None, output = None))]
    fn strip_chars(&self, chars: Option<&str>, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().strip_chars(chars), output)
    }

    #[pyo3(signature = (separator, output = None))]
    fn split(&self, separator: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().split(separator), output)
    }

    #[pyo3(signature = (output = None))]
    fn len_chars(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().len_chars(), output)
    }

    #[pyo3(signature = (pattern, capture_group = 1, output = None))]
    fn extract(
        &self,
        pattern: &str,
        capture_group: usize,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column).str().extract(pattern, capture_group),
            output,
        )
    }
}

impl PyStringNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(PyDataFrame::new)
            .map_err(dataframe_err)
    }
}

#[pyclass(name = "DatetimeNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyDatetimeNamespace {
    df: DataFrame,
    column: String,
}

#[pymethods]
impl PyDatetimeNamespace {
    #[pyo3(signature = (output = None))]
    fn year(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().year(), output)
    }

    #[pyo3(signature = (output = None))]
    fn month(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().month(), output)
    }

    #[pyo3(signature = (output = None))]
    fn day(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().day(), output)
    }

    #[pyo3(signature = (output = None))]
    fn weekday(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().weekday(), output)
    }

    #[pyo3(signature = (output = None))]
    fn to_string(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().to_string(), output)
    }

    #[pyo3(signature = (from_offset, to_offset, output = None))]
    fn convert_time_zone(
        &self,
        from_offset: &str,
        to_offset: &str,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column)
                .dt()
                .convert_time_zone(from_offset, to_offset),
            output,
        )
    }
}

impl PyDatetimeNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(PyDataFrame::new)
            .map_err(dataframe_err)
    }
}

#[pyclass(name = "ListNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyListNamespace {
    df: DataFrame,
    column: String,
}

#[pymethods]
impl PyListNamespace {
    #[pyo3(signature = (separator, null_value = None, output = None))]
    fn join(
        &self,
        separator: &str,
        null_value: Option<&str>,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().join(separator, null_value), output)
    }

    #[pyo3(signature = (output = None))]
    fn len(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().len(), output)
    }

    #[pyo3(signature = (value, output = None))]
    fn contains(&self, value: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().contains(value), output)
    }
}

impl PyListNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(PyDataFrame::new)
            .map_err(dataframe_err)
    }
}

fn dataframe_from_py(
    columns: &Bound<'_, PyDict>,
    schema: Option<&Bound<'_, PyDict>>,
) -> PyResult<DataFrame> {
    let mut series = Vec::with_capacity(columns.len());
    for (name, values) in columns.iter() {
        let name = name.extract::<String>()?;
        let kind = schema_kind(schema, &name)?.unwrap_or_else(|| infer_kind(&values));
        series.push(series_from_py(&name, &values, kind)?);
    }
    DataFrame::new(series).map_err(dataframe_err)
}

fn schema_kind(schema: Option<&Bound<'_, PyDict>>, name: &str) -> PyResult<Option<ColumnKind>> {
    let Some(schema) = schema else {
        return Ok(None);
    };
    let Some(value) = schema.get_item(name)? else {
        return Ok(None);
    };
    let value = value.extract::<String>()?;
    match value.as_str() {
        "utf8" | "str" | "string" => Ok(Some(ColumnKind::Utf8)),
        "int64" | "int" => Ok(Some(ColumnKind::Int64)),
        "timestamp_micros" | "datetime" => Ok(Some(ColumnKind::TimestampMicros)),
        "list_utf8" | "list[str]" | "list" => Ok(Some(ColumnKind::ListUtf8)),
        other => Err(PyValueError::new_err(format!(
            "unsupported DataFrame schema for column '{name}': {other}"
        ))),
    }
}

fn infer_kind(values: &Bound<'_, PyAny>) -> ColumnKind {
    let Ok(iter) = values.try_iter() else {
        return ColumnKind::Utf8;
    };
    for item in iter.flatten() {
        if item.is_none() {
            continue;
        }
        if item.cast::<PyList>().is_ok() || item.cast::<PyTuple>().is_ok() {
            return ColumnKind::ListUtf8;
        }
        if item.extract::<i64>().is_ok() {
            return ColumnKind::Int64;
        }
        return ColumnKind::Utf8;
    }
    ColumnKind::Utf8
}

fn series_from_py(name: &str, values: &Bound<'_, PyAny>, kind: ColumnKind) -> PyResult<Series> {
    let array: ArrayRef = match kind {
        ColumnKind::Utf8 => Arc::new(StringArray::from(py_utf8_values(values)?)),
        ColumnKind::Int64 => Arc::new(Int64Array::from(py_i64_values(values)?)),
        ColumnKind::TimestampMicros => {
            Arc::new(TimestampMicrosecondArray::from(py_i64_values(values)?))
        }
        ColumnKind::ListUtf8 => list_utf8_from_py(values)?,
    };
    Series::from_arrow(name, vec![array]).map_err(dataframe_err)
}

fn py_utf8_values(values: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
    let mut out = Vec::new();
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(None);
        } else {
            out.push(Some(item.extract::<String>()?));
        }
    }
    Ok(out)
}

fn py_i64_values(values: &Bound<'_, PyAny>) -> PyResult<Vec<Option<i64>>> {
    let mut out = Vec::new();
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(None);
        } else {
            out.push(Some(item.extract::<i64>()?));
        }
    }
    Ok(out)
}

fn list_utf8_from_py(values: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            builder.append(false);
            continue;
        }
        for value in item.try_iter()? {
            let value = value?;
            if value.is_none() {
                builder.values().append_null();
            } else {
                builder.values().append_value(value.extract::<String>()?);
            }
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn dataframe_to_py_dict(py: Python<'_>, df: &DataFrame) -> PyResult<Py<PyDict>> {
    let out = PyDict::new(py);
    for series in df.columns() {
        let chunks = series.to_arrow();
        let Some(array) = chunks.first() else {
            out.set_item(series.name(), PyList::empty(py))?;
            continue;
        };
        out.set_item(series.name(), array_to_py_list(py, array)?)?;
    }
    Ok(out.unbind())
}

fn array_to_py_list(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    match array.data_type() {
        DataType::Utf8 => utf8_array_to_py(py, array),
        DataType::Boolean => bool_array_to_py(py, array),
        DataType::Int32 => int32_array_to_py(py, array),
        DataType::Int64 => int64_array_to_py(py, array),
        DataType::UInt64 => uint64_array_to_py(py, array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_array_to_py(py, array),
        DataType::List(field) if field.data_type() == &DataType::Utf8 => {
            list_utf8_array_to_py(py, array)
        }
        other => Err(error::to_py_err(format!(
            "unsupported DataFrame column type for Python conversion: {other}"
        ))),
    }
}

fn utf8_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<StringArray>(array, "StringArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn bool_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<BooleanArray>(array, "BooleanArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn int32_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<Int32Array>(array, "Int32Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn int64_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<Int64Array>(array, "Int64Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn uint64_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<UInt64Array>(array, "UInt64Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn timestamp_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn list_utf8_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<ListArray>(array, "ListArray")?;
    let values = PyList::empty(py);
    for row in 0..array.len() {
        if array.is_null(row) {
            values.append(py.None())?;
            continue;
        }
        let item_array = array.value(row);
        values.append(utf8_array_to_py(py, &item_array)?.bind(py))?;
    }
    Ok(values.unbind())
}

fn downcast_array<'a, T: 'static>(array: &'a ArrayRef, expected: &str) -> PyResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        error::to_py_err(format!(
            "internal DataFrame conversion error: expected {expected}, got {}",
            array.data_type()
        ))
    })
}

fn dataframe_err(err: alopex_dataframe::DataFrameError) -> PyErr {
    error::to_py_err(err)
}
