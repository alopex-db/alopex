//! SQL 実行 API（`execute_sql`）のパラメータバインディングと結果変換。
//!
//! 公開ガイド（docs/guides/python.md）が約束する形状:
//!
//! - `execute_sql(sql, params=None)`（`?` プレースホルダ、list / tuple）
//! - SELECT: 列名でアクセス可能な行（dict）の list
//! - DML: 影響行数（int）/ DDL: `None`
//!
//! Rust 側（alopex-embedded / alopex-sql）はプリペアドステートメントを持たないため、
//! パラメータは SQL リテラルへのエスケープ展開（クライアントサイドバインディング）で
//! 実現する。文字列リテラルのエスケープは Nim lexer の仕様（引用符の二重化）に一致させる。

use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use pyo3::IntoPyObjectExt;

use alopex_sql::storage::SqlValue;
use alopex_sql::ExecutionResult;

use crate::error::AlopexError;

/// `?` プレースホルダへ params をエスケープ展開した SQL を返す。
///
/// - `params` は list / tuple のみ受け付ける（str などのシーケンスは拒否）。
/// - 引用符（`'` / `"`）内およびコメント（`--` / `/* */`）内の `?` は
///   プレースホルダとして扱わない（Nim lexer と同じ規則で読み飛ばす）。
/// - プレースホルダ数とパラメータ数の不一致は `ValueError`。
/// - NUL 文字を含む SQL / 文字列パラメータは `ValueError`（FFI 境界の制約）。
pub(crate) fn bind_params(sql: &str, params: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    // Nim FFI 境界（nim_ffi.rs）は CString を使うため、NUL を含む SQL は panic になる。
    // ここで明示的に ValueError として拒否する。
    if sql.contains('\0') {
        return Err(AlopexError::SqlStatementInvalid(
            "NUL 文字を含む SQL は実行できません".to_string(),
        )
        .into());
    }
    let rendered = render_params(params)?;
    let segments = split_on_placeholders(sql);
    let placeholders = segments.len() - 1;
    if placeholders != rendered.len() {
        return Err(AlopexError::SqlParamCountMismatch {
            expected: placeholders,
            actual: rendered.len(),
        }
        .into());
    }
    if rendered.is_empty() {
        return Ok(sql.to_string());
    }

    let extra: usize = rendered.iter().map(String::len).sum();
    let mut out = String::with_capacity(sql.len() + extra);
    for (i, segment) in segments.iter().enumerate() {
        out.push_str(segment);
        if let Some(value) = rendered.get(i) {
            out.push_str(value);
        }
    }
    Ok(out)
}

/// `ExecutionResult` を Python ネイティブ値へ変換する。
///
/// - DDL（Success）: `None`
/// - DML（RowsAffected）: `int`
/// - SELECT（Query）: `list[dict[str, Any]]`（dict の挿入順 = 列順を保持）
pub(crate) fn execution_result_to_py(
    py: Python<'_>,
    result: ExecutionResult,
) -> PyResult<Py<PyAny>> {
    match result {
        ExecutionResult::Success => Ok(py.None()),
        ExecutionResult::RowsAffected(count) => count.into_py_any(py),
        ExecutionResult::Query(query) => {
            let names: Vec<&str> = query.columns.iter().map(|c| c.name.as_str()).collect();
            let rows = PyList::empty(py);
            for row in query.rows {
                let dict = PyDict::new(py);
                for (name, value) in names.iter().zip(row) {
                    dict.set_item(name, sql_value_to_py(py, value)?)?;
                }
                rows.append(dict)?;
            }
            rows.into_py_any(py)
        }
    }
}

/// プレースホルダ走査の状態。Nim lexer（`readString` / `skipWhitespace`）と同じ規則で
/// 文字列リテラルとコメントを読み飛ばす。
enum ScanState {
    /// 通常のコード領域。
    Normal,
    /// 文字列リテラル内（開始引用符を保持。`''` / `""` の二重化はエスケープ）。
    Quoted(char),
    /// 行コメント（`--` から改行まで）。
    LineComment,
    /// ブロックコメント（`/*` から `*/` まで、ネストなし）。
    BlockComment,
}

/// SQL を「引用符・コメント外の `?`」で分割する。戻り値の長さは `プレースホルダ数 + 1`。
fn split_on_placeholders(sql: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut state = ScanState::Normal;
    let mut iter = sql.char_indices().peekable();
    while let Some((index, c)) = iter.next() {
        let next = iter.peek().map(|&(_, next)| next);
        match state {
            ScanState::Normal => {
                if c == '\'' || c == '"' {
                    state = ScanState::Quoted(c);
                } else if c == '-' && next == Some('-') {
                    iter.next();
                    state = ScanState::LineComment;
                } else if c == '/' && next == Some('*') {
                    iter.next();
                    state = ScanState::BlockComment;
                } else if c == '?' {
                    segments.push(&sql[start..index]);
                    start = index + 1; // '?' は 1 バイト
                }
            }
            ScanState::Quoted(q) => {
                if c == q {
                    if next == Some(q) {
                        iter.next(); // エスケープされた引用符（'' / ""）
                    } else {
                        state = ScanState::Normal;
                    }
                }
            }
            ScanState::LineComment => {
                if c == '\n' {
                    state = ScanState::Normal;
                }
            }
            ScanState::BlockComment => {
                if c == '*' && next == Some('/') {
                    iter.next();
                    state = ScanState::Normal;
                }
            }
        }
    }
    segments.push(&sql[start..]);
    segments
}

/// params（list / tuple）を SQL リテラル文字列の列へ変換する。
fn render_params(params: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };
    let items: Vec<Bound<'_, PyAny>> = if let Ok(list) = params.cast::<PyList>() {
        list.iter().collect()
    } else if let Ok(tuple) = params.cast::<PyTuple>() {
        tuple.iter().collect()
    } else {
        return Err(AlopexError::SqlParamUnsupportedType(format!(
            "params には list または tuple を指定してください（実際: {}）",
            type_name(params)
        ))
        .into());
    };
    items
        .iter()
        .enumerate()
        .map(|(index, value)| render_param(value, index))
        .collect()
}

/// 1 つのパラメータ値を SQL リテラルへ変換する。
fn render_param(value: &Bound<'_, PyAny>, index: usize) -> PyResult<String> {
    if value.is_none() {
        return Ok("NULL".to_string());
    }
    // bool は int のサブクラスなので int より先に判定する。
    if value.is_instance_of::<PyBool>() {
        return Ok(if value.extract::<bool>()? {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        });
    }
    if value.is_instance_of::<PyInt>() {
        return match value.extract::<i64>() {
            Ok(v) => Ok(v.to_string()),
            Err(_) => Err(AlopexError::SqlParamInvalidValue(format!(
                "params[{index}] の整数が 64bit 符号付き整数の範囲外です"
            ))
            .into()),
        };
    }
    if value.is_instance_of::<PyFloat>() {
        let v = value.extract::<f64>()?;
        return render_f64(v).ok_or_else(|| non_finite_error(index, v));
    }
    if value.is_instance_of::<PyString>() {
        let text = value.extract::<String>()?;
        // Nim FFI 境界（CString）は NUL を扱えないため明示的に拒否する。
        if text.contains('\0') {
            return Err(AlopexError::SqlParamInvalidValue(format!(
                "params[{index}]: NUL 文字を含む文字列は使用できません"
            ))
            .into());
        }
        return Ok(escape_text(&text));
    }
    if value.is_instance_of::<PyBytes>() || value.is_instance_of::<PyByteArray>() {
        return Err(AlopexError::SqlParamNotImplemented(format!(
            "params[{index}]: BLOB リテラルは SQL パーサー未対応のため \
             bytes / bytearray パラメータを展開できません"
        ))
        .into());
    }
    // dict などのマッピングはベクトルとして反復するとキーのみが展開されるため明示的に拒否する。
    if value.cast::<PyDict>().is_ok() || value.hasattr("keys")? {
        return Err(unsupported_type_error(value, index));
    }
    // 数値シーケンス（list / tuple / numpy 配列など）はベクトルリテラルへ展開する。
    if let Ok(iter) = value.try_iter() {
        return render_vector(iter, index);
    }
    // numpy スカラーなど、__index__ / __float__ を実装する数値型のフォールバック。
    if let Ok(v) = value.extract::<i64>() {
        return Ok(v.to_string());
    }
    if let Ok(v) = value.extract::<f64>() {
        return render_f64(v).ok_or_else(|| non_finite_error(index, v));
    }
    Err(unsupported_type_error(value, index))
}

/// 数値シーケンスをベクトルリテラル（`[1.0, 2.0]`）へ変換する。
fn render_vector(iter: Bound<'_, pyo3::types::PyIterator>, index: usize) -> PyResult<String> {
    let mut parts: Vec<String> = Vec::new();
    for (position, item) in iter.enumerate() {
        let item = item?;
        let v = item.extract::<f64>().map_err(|_| {
            AlopexError::SqlParamUnsupportedType(format!(
                "params[{index}][{position}] を数値へ変換できません（実際: {}）",
                type_name(&item)
            ))
        })?;
        parts.push(render_f64(v).ok_or_else(|| non_finite_error(index, v))?);
    }
    if parts.is_empty() {
        return Err(AlopexError::SqlParamInvalidValue(format!(
            "params[{index}]: 空のベクトルリテラルは使用できません"
        ))
        .into());
    }
    Ok(format!("[{}]", parts.join(", ")))
}

/// 有限の f64 を SQL リテラルへ変換する（変換できない値は `None`）。
///
/// Nim lexer は指数表記（`1e10`）を受理しないため、Rust の `Display`
/// （位取り表記で整形される）を使い、小数点がなければ `.0` を付与する。
/// 万一 `Display` が指数表記を返した場合は壊れた SQL を生成せず `None` を返す
/// （防御的チェック。現行の Rust 実装では到達しない）。
fn render_f64(value: f64) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    let mut out = format!("{value}");
    if out.contains(['e', 'E']) {
        return None;
    }
    if !out.contains('.') {
        out.push_str(".0");
    }
    Some(out)
}

/// 文字列を SQL 文字列リテラルへ変換する（`'` の二重化、Nim lexer 仕様）。
fn escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('\'');
    for c in text.chars() {
        if c == '\'' {
            out.push_str("''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

fn non_finite_error(index: usize, value: f64) -> PyErr {
    AlopexError::SqlParamInvalidValue(format!(
        "params[{index}]: 有限でない浮動小数点値（{value}）は使用できません"
    ))
    .into()
}

fn unsupported_type_error(value: &Bound<'_, PyAny>, index: usize) -> PyErr {
    AlopexError::SqlParamUnsupportedType(format!(
        "params[{index}] は未対応の型です（実際: {}）。\
         対応型: None / bool / int / float / str / 数値シーケンス（ベクトル）",
        type_name(value)
    ))
    .into()
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string())
}

/// `SqlValue` を Python ネイティブ値へ変換する。
///
/// CLI / Server と同じく値をそのまま返す（Timestamp はエポックマイクロ秒の int、
/// Vector は float の list）。
pub(crate) fn sql_value_to_py(py: Python<'_>, value: SqlValue) -> PyResult<Py<PyAny>> {
    match value {
        SqlValue::Null => Ok(py.None()),
        SqlValue::Integer(v) => v.into_py_any(py),
        SqlValue::BigInt(v) => v.into_py_any(py),
        SqlValue::Float(v) => f64::from(v).into_py_any(py),
        SqlValue::Double(v) => v.into_py_any(py),
        SqlValue::Text(v) => v.into_py_any(py),
        SqlValue::Blob(v) => PyBytes::new(py, &v).into_py_any(py),
        SqlValue::Boolean(v) => v.into_py_any(py),
        SqlValue::Timestamp(v) => v.into_py_any(py),
        SqlValue::Vector(values) => {
            let list = PyList::empty(py);
            for v in values {
                list.append(f64::from(v))?;
            }
            list.into_py_any(py)
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
    use pyo3::IntoPyObjectExt;

    use super::bind_params;

    fn with_py<F: FnOnce(Python<'_>)>(f: F) {
        pyo3::Python::initialize();
        Python::attach(f);
    }

    fn params_list<'py>(py: Python<'py>, values: Vec<Bound<'py, PyAny>>) -> Bound<'py, PyAny> {
        PyList::new(py, values).expect("params list").into_any()
    }

    #[test]
    fn bind_params_none_passes_through() {
        with_py(|_| {
            let sql = "SELECT * FROM users";
            assert_eq!(bind_params(sql, None).expect("bind"), sql);
        });
    }

    #[test]
    fn bind_params_substitutes_int_and_escaped_text() {
        with_py(|py| {
            let params = params_list(
                py,
                vec![
                    1i64.into_bound_py_any(py).expect("int"),
                    "O'Brien".into_bound_py_any(py).expect("str"),
                ],
            );
            let sql = bind_params(
                "SELECT * FROM users WHERE id = ? AND name = ?",
                Some(&params),
            )
            .expect("bind");
            assert_eq!(
                sql,
                "SELECT * FROM users WHERE id = 1 AND name = 'O''Brien'"
            );
        });
    }

    #[test]
    fn bind_params_null_bool_and_floats() {
        with_py(|py| {
            let params = params_list(
                py,
                vec![
                    py.None().into_bound(py),
                    true.into_bound_py_any(py).expect("bool"),
                    2.5f64.into_bound_py_any(py).expect("float"),
                    2.0f64.into_bound_py_any(py).expect("float"),
                ],
            );
            let sql =
                bind_params("INSERT INTO t VALUES (?, ?, ?, ?)", Some(&params)).expect("bind");
            assert_eq!(sql, "INSERT INTO t VALUES (NULL, TRUE, 2.5, 2.0)");
        });
    }

    #[test]
    fn bind_params_tuple_params_accepted() {
        with_py(|py| {
            let params = PyTuple::new(py, [1i64]).expect("tuple").into_any();
            let sql = bind_params("SELECT ? FROM t", Some(&params)).expect("bind");
            assert_eq!(sql, "SELECT 1 FROM t");
        });
    }

    #[test]
    fn bind_params_placeholder_inside_quotes_is_literal() {
        with_py(|py| {
            let params = params_list(py, vec![5i64.into_bound_py_any(py).expect("int")]);
            let sql = bind_params(
                "SELECT * FROM t WHERE name = 'wh?t' AND note = \"double ? quote\" AND id = ?",
                Some(&params),
            )
            .expect("bind");
            assert_eq!(
                sql,
                "SELECT * FROM t WHERE name = 'wh?t' AND note = \"double ? quote\" AND id = 5"
            );
        });
    }

    #[test]
    fn bind_params_escaped_quote_in_literal() {
        with_py(|py| {
            let params = params_list(py, vec![5i64.into_bound_py_any(py).expect("int")]);
            let sql = bind_params(
                "SELECT * FROM t WHERE name = 'it''s ?' AND id = ?",
                Some(&params),
            )
            .expect("bind");
            assert_eq!(sql, "SELECT * FROM t WHERE name = 'it''s ?' AND id = 5");
        });
    }

    #[test]
    fn bind_params_count_mismatch_is_value_error() {
        with_py(|py| {
            let empty = params_list(py, vec![]);
            let err = bind_params("SELECT ? FROM t", Some(&empty)).expect_err("too few");
            assert!(err.is_instance_of::<PyValueError>(py));

            let two = params_list(
                py,
                vec![
                    1i64.into_bound_py_any(py).expect("int"),
                    2i64.into_bound_py_any(py).expect("int"),
                ],
            );
            let err = bind_params("SELECT ? FROM t", Some(&two)).expect_err("too many");
            assert!(err.is_instance_of::<PyValueError>(py));

            // params 省略時にプレースホルダが残っている場合もエラー（黙殺しない）
            let err = bind_params("SELECT ? FROM t", None).expect_err("missing params");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn bind_params_rejects_non_sequence_params() {
        with_py(|py| {
            let scalar = 1i64.into_bound_py_any(py).expect("int");
            let err = bind_params("SELECT ?", Some(&scalar)).expect_err("scalar");
            assert!(err.is_instance_of::<PyTypeError>(py));

            // str はシーケンスだがパラメータ列としては不正
            let text = "abc".into_bound_py_any(py).expect("str");
            let err = bind_params("SELECT ?", Some(&text)).expect_err("str");
            assert!(err.is_instance_of::<PyTypeError>(py));
        });
    }

    #[test]
    fn bind_params_rejects_dict_param() {
        with_py(|py| {
            let params = params_list(py, vec![PyDict::new(py).into_any()]);
            let err = bind_params("SELECT ?", Some(&params)).expect_err("dict");
            assert!(err.is_instance_of::<PyTypeError>(py));
        });
    }

    #[test]
    fn bind_params_bytes_is_not_implemented() {
        with_py(|py| {
            let params = params_list(py, vec![PyBytes::new(py, b"raw").into_any()]);
            let err = bind_params("SELECT ?", Some(&params)).expect_err("bytes");
            assert!(err.is_instance_of::<PyNotImplementedError>(py));
        });
    }

    #[test]
    fn bind_params_non_finite_float_is_value_error() {
        with_py(|py| {
            for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                let params = params_list(py, vec![value.into_bound_py_any(py).expect("float")]);
                let err = bind_params("SELECT ?", Some(&params)).expect_err("non-finite");
                assert!(err.is_instance_of::<PyValueError>(py));
            }
        });
    }

    #[test]
    fn bind_params_int_out_of_i64_range_is_value_error() {
        with_py(|py| {
            let big = (i128::from(i64::MAX) + 1)
                .into_bound_py_any(py)
                .expect("bigint");
            let params = params_list(py, vec![big]);
            let err = bind_params("SELECT ?", Some(&params)).expect_err("overflow");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn bind_params_vector_literal() {
        with_py(|py| {
            let vector = PyList::new(py, [0.25f64, -1.5, 2.0])
                .expect("vector")
                .into_any();
            let params = params_list(py, vec![vector]);
            let sql = bind_params("INSERT INTO docs VALUES (?)", Some(&params)).expect("bind");
            assert_eq!(sql, "INSERT INTO docs VALUES ([0.25, -1.5, 2.0])");
        });
    }

    #[test]
    fn bind_params_placeholder_inside_comments_is_literal() {
        with_py(|py| {
            let params = params_list(py, vec![5i64.into_bound_py_any(py).expect("int")]);
            let sql = bind_params(
                "SELECT * FROM t -- what?\n WHERE id = ? /* really? */",
                Some(&params),
            )
            .expect("bind");
            assert_eq!(sql, "SELECT * FROM t -- what?\n WHERE id = 5 /* really? */");
        });
    }

    #[test]
    fn bind_params_quote_inside_comment_does_not_open_string() {
        with_py(|py| {
            let params = params_list(py, vec![1i64.into_bound_py_any(py).expect("int")]);
            let sql = bind_params("SELECT ? -- it's fine?\n", Some(&params)).expect("bind");
            assert_eq!(sql, "SELECT 1 -- it's fine?\n");
        });
    }

    #[test]
    fn bind_params_comment_start_inside_string_is_literal() {
        with_py(|py| {
            let params = params_list(py, vec![1i64.into_bound_py_any(py).expect("int")]);
            let sql = bind_params("SELECT ? WHERE note = '-- not a comment ?'", Some(&params))
                .expect("bind");
            assert_eq!(sql, "SELECT 1 WHERE note = '-- not a comment ?'");
        });
    }

    #[test]
    fn bind_params_nul_in_text_param_is_value_error() {
        with_py(|py| {
            let params = params_list(py, vec!["a\0b".into_bound_py_any(py).expect("str")]);
            let err = bind_params("SELECT ?", Some(&params)).expect_err("nul param");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn bind_params_nul_in_sql_is_value_error() {
        with_py(|py| {
            let err = bind_params("SELECT 1 \0", None).expect_err("nul in sql");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn bind_params_large_float_renders_without_exponent() {
        with_py(|py| {
            let params = params_list(py, vec![1e300f64.into_bound_py_any(py).expect("float")]);
            // SQL 側に 'e'/'E' を含まない断片を使い、リテラル部分のみを検査する
            let sql = bind_params("v = ?", Some(&params)).expect("bind");
            let literal = sql.strip_prefix("v = ").expect("prefix");
            assert!(
                !literal.contains('e') && !literal.contains('E'),
                "指数表記は lexer 非対応: {literal}"
            );
            assert!(literal.contains('.'), "小数点必須: {literal}");
            assert_eq!(literal.parse::<f64>().expect("round-trip"), 1e300);
        });
    }

    #[test]
    fn bind_params_empty_vector_is_value_error() {
        with_py(|py| {
            let vector = PyList::empty(py).into_any();
            let params = params_list(py, vec![vector]);
            let err = bind_params("SELECT ?", Some(&params)).expect_err("empty vector");
            assert!(err.is_instance_of::<PyValueError>(py));
        });
    }
}
