//! SQLite-style JSON functions over canonical JSON stored as TEXT.

use serde_json::{Number, Value};

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::storage::SqlValue;

use super::registry::EvalFn;

pub(crate) const MAX_JSON_BYTES: usize = 1 << 20;
pub(crate) const MAX_JSON_ROWS: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathPart {
    Key(String),
    Index(usize),
    FromEnd(usize),
    Append,
}

fn invalid(function: &str, reason: impl Into<String>) -> ExecutorError {
    EvaluationError::InvalidArgument {
        function: function.to_ascii_uppercase(),
        reason: reason.into(),
    }
    .into()
}

fn text<'a>(function: &str, value: &'a SqlValue) -> Result<Option<&'a str>> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value) => Ok(Some(value)),
        other => Err(invalid(
            function,
            format!("expected TEXT, found {}", other.type_name()),
        )),
    }
}

pub(crate) fn parse_json(function: &str, input: &str) -> Result<Value> {
    if input.len() > MAX_JSON_BYTES {
        return Err(invalid(function, "JSON input exceeds 1048576 bytes"));
    }
    serde_json::from_str(input).map_err(|error| invalid(function, format!("invalid JSON: {error}")))
}

fn encode(function: &str, value: &Value) -> Result<String> {
    let output = serde_json::to_string(value)
        .map_err(|error| invalid(function, format!("cannot encode JSON: {error}")))?;
    if output.len() > MAX_JSON_BYTES {
        return Err(invalid(function, "JSON output exceeds 1048576 bytes"));
    }
    Ok(output)
}

fn parse_path(function: &str, input: &str) -> Result<Vec<PathPart>> {
    let bytes = input.as_bytes();
    if bytes.first() != Some(&b'$') {
        return Err(invalid(function, "JSON path must begin with '$'"));
    }
    let mut parts = Vec::new();
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'.' => {
                index += 1;
                let start = index;
                while index < bytes.len() && !matches!(bytes[index], b'.' | b'[') {
                    index += 1;
                }
                if start == index {
                    return Err(invalid(
                        function,
                        "JSON path contains an empty object label",
                    ));
                }
                parts.push(PathPart::Key(input[start..index].to_string()));
            }
            b'[' => {
                index += 1;
                let start = index;
                while index < bytes.len() && bytes[index] != b']' {
                    index += 1;
                }
                if index == bytes.len() {
                    return Err(invalid(
                        function,
                        "JSON path has an unterminated array index",
                    ));
                }
                let token = &input[start..index];
                index += 1;
                let part = if token == "#" {
                    PathPart::Append
                } else if let Some(number) = token.strip_prefix("#-") {
                    let value = number
                        .parse::<usize>()
                        .map_err(|_| invalid(function, "invalid JSON array index"))?;
                    if value == 0 {
                        return Err(invalid(
                            function,
                            "JSON '#-N' index requires N greater than zero",
                        ));
                    }
                    PathPart::FromEnd(value)
                } else {
                    PathPart::Index(
                        token
                            .parse::<usize>()
                            .map_err(|_| invalid(function, "invalid JSON array index"))?,
                    )
                };
                parts.push(part);
            }
            _ => return Err(invalid(function, "invalid JSON path syntax")),
        }
    }
    Ok(parts)
}

fn locate<'a>(root: &'a Value, parts: &[PathPart]) -> Option<&'a Value> {
    let mut current = root;
    for part in parts {
        current = match (part, current) {
            (PathPart::Key(key), Value::Object(values)) => values.get(key)?,
            (PathPart::Index(index), Value::Array(values)) => values.get(*index)?,
            (PathPart::FromEnd(offset), Value::Array(values)) => {
                values.get(values.len().checked_sub(*offset)?)?
            }
            (PathPart::Append, _) => return None,
            _ => return None,
        };
    }
    Some(current)
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(true) => "true",
        Value::Bool(false) => "false",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "real",
        Value::String(_) => "text",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(crate) fn json_to_sql(value: &Value) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Boolean(*value),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if let Ok(value) = i32::try_from(value) {
                    SqlValue::Integer(value)
                } else {
                    SqlValue::BigInt(value)
                }
            } else if let Some(value) = number.as_u64().and_then(|value| i64::try_from(value).ok())
            {
                SqlValue::BigInt(value)
            } else {
                SqlValue::Double(
                    number
                        .as_f64()
                        .ok_or_else(|| invalid("JSON", "number is outside supported precision"))?,
                )
            }
        }
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(encode("JSON", value)?),
    })
}

pub(crate) fn sql_to_json(value: &SqlValue) -> Result<Value> {
    Ok(match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(value) => Value::Number(Number::from(*value)),
        SqlValue::BigInt(value) => Value::Number(Number::from(*value)),
        SqlValue::Float(value) => Number::from_f64(f64::from(*value))
            .map(Value::Number)
            .ok_or_else(|| invalid("JSON", "non-finite floating-point value"))?,
        SqlValue::Double(value) => Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| invalid("JSON", "non-finite floating-point value"))?,
        SqlValue::Text(value) => Value::String(value.clone()),
        SqlValue::Boolean(value) => Value::Bool(*value),
        SqlValue::Blob(_) | SqlValue::Timestamp(_) | SqlValue::Vector(_) => {
            return Err(invalid(
                "JSON",
                format!("unsupported SQL type {}", value.type_name()),
            ));
        }
    })
}

fn eval_json(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    Ok(SqlValue::Text(encode("JSON", &parse_json("JSON", input)?)?))
}

fn eval_json_valid(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON_VALID", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    Ok(SqlValue::Boolean(
        input.len() <= MAX_JSON_BYTES && serde_json::from_str::<Value>(input).is_ok(),
    ))
}

fn eval_json_type(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON_TYPE", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let root = parse_json("JSON_TYPE", input)?;
    let selected = if values.len() == 2 {
        let Some(path) = text("JSON_TYPE", &values[1])? else {
            return Ok(SqlValue::Null);
        };
        locate(&root, &parse_path("JSON_TYPE", path)?)
    } else {
        Some(&root)
    };
    Ok(selected
        .map(|value| SqlValue::Text(json_type(value).into()))
        .unwrap_or(SqlValue::Null))
}

fn eval_json_extract(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON_EXTRACT", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let root = parse_json("JSON_EXTRACT", input)?;
    let mut selected = Vec::new();
    for value in &values[1..] {
        let Some(path) = text("JSON_EXTRACT", value)? else {
            selected.push(Value::Null);
            continue;
        };
        selected.push(
            locate(&root, &parse_path("JSON_EXTRACT", path)?)
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    if selected.len() == 1 {
        json_to_sql(&selected[0])
    } else {
        Ok(SqlValue::Text(encode(
            "JSON_EXTRACT",
            &Value::Array(selected),
        )?))
    }
}

fn encode_object_pairs(values: &[SqlValue], function: &str) -> Result<String> {
    let mut output = String::from("{");
    for (index, pair) in values.chunks_exact(2).enumerate() {
        let Some(key) = text(function, &pair[0])? else {
            return Err(invalid(function, "object label must not be NULL"));
        };
        if index > 0 {
            output.push(',');
        }
        output.push_str(&serde_json::to_string(key).expect("string serialization"));
        output.push(':');
        output.push_str(&encode(function, &sql_to_json(&pair[1])?)?);
    }
    if output.len() >= MAX_JSON_BYTES {
        return Err(invalid(function, "JSON output exceeds 1048576 bytes"));
    }
    output.push('}');
    Ok(output)
}

fn eval_json_object(values: &[SqlValue]) -> Result<SqlValue> {
    Ok(SqlValue::Text(encode_object_pairs(values, "JSON_OBJECT")?))
}

fn eval_json_array(values: &[SqlValue]) -> Result<SqlValue> {
    let array = values.iter().map(sql_to_json).collect::<Result<Vec<_>>>()?;
    Ok(SqlValue::Text(encode("JSON_ARRAY", &Value::Array(array))?))
}

#[derive(Clone, Copy)]
enum UpdateMode {
    Insert,
    Replace,
    Set,
}

fn update_at(current: &mut Value, parts: &[PathPart], replacement: Value, mode: UpdateMode) {
    if parts.is_empty() {
        if !matches!(mode, UpdateMode::Insert) {
            *current = replacement;
        }
        return;
    }
    if parts.len() == 1 {
        match (&parts[0], current) {
            (PathPart::Key(key), Value::Object(values)) => {
                let exists = values.contains_key(key);
                if (exists && !matches!(mode, UpdateMode::Insert))
                    || (!exists && !matches!(mode, UpdateMode::Replace))
                {
                    values.insert(key.clone(), replacement);
                }
            }
            (PathPart::Index(index), Value::Array(values)) if *index < values.len() => {
                if !matches!(mode, UpdateMode::Insert) {
                    values[*index] = replacement;
                }
            }
            (PathPart::Append, Value::Array(values)) if !matches!(mode, UpdateMode::Replace) => {
                values.push(replacement)
            }
            (PathPart::FromEnd(offset), Value::Array(values))
                if *offset <= values.len() && !matches!(mode, UpdateMode::Insert) =>
            {
                let index = values.len() - offset;
                values[index] = replacement;
            }
            _ => {}
        }
        return;
    }
    let next = match (&parts[0], current) {
        (PathPart::Key(key), Value::Object(values)) => values.get_mut(key),
        (PathPart::Index(index), Value::Array(values)) => values.get_mut(*index),
        (PathPart::FromEnd(offset), Value::Array(values)) => values
            .len()
            .checked_sub(*offset)
            .and_then(|index| values.get_mut(index)),
        _ => None,
    };
    if let Some(next) = next {
        update_at(next, &parts[1..], replacement, mode);
    }
}

fn eval_update(values: &[SqlValue], function: &str, mode: UpdateMode) -> Result<SqlValue> {
    let Some(input) = text(function, &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let mut root = parse_json(function, input)?;
    for pair in values[1..].chunks_exact(2) {
        let Some(path) = text(function, &pair[0])? else {
            return Err(invalid(function, "JSON path must not be NULL"));
        };
        update_at(
            &mut root,
            &parse_path(function, path)?,
            sql_to_json(&pair[1])?,
            mode,
        );
    }
    Ok(SqlValue::Text(encode(function, &root)?))
}

fn eval_json_insert(values: &[SqlValue]) -> Result<SqlValue> {
    eval_update(values, "JSON_INSERT", UpdateMode::Insert)
}
fn eval_json_replace(values: &[SqlValue]) -> Result<SqlValue> {
    eval_update(values, "JSON_REPLACE", UpdateMode::Replace)
}
fn eval_json_set(values: &[SqlValue]) -> Result<SqlValue> {
    eval_update(values, "JSON_SET", UpdateMode::Set)
}

fn remove_at(current: &mut Value, parts: &[PathPart]) {
    if parts.len() == 1 {
        match (&parts[0], current) {
            (PathPart::Key(key), Value::Object(values)) => {
                values.remove(key);
            }
            (PathPart::Index(index), Value::Array(values)) if *index < values.len() => {
                values.remove(*index);
            }
            (PathPart::FromEnd(offset), Value::Array(values)) if *offset <= values.len() => {
                values.remove(values.len() - offset);
            }
            _ => {}
        }
    } else if let Some((part, rest)) = parts.split_first() {
        let next = match (part, current) {
            (PathPart::Key(key), Value::Object(values)) => values.get_mut(key),
            (PathPart::Index(index), Value::Array(values)) => values.get_mut(*index),
            (PathPart::FromEnd(offset), Value::Array(values)) => values
                .len()
                .checked_sub(*offset)
                .and_then(|index| values.get_mut(index)),
            _ => None,
        };
        if let Some(next) = next {
            remove_at(next, rest);
        }
    }
}

fn eval_json_remove(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON_REMOVE", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let mut root = parse_json("JSON_REMOVE", input)?;
    for path in &values[1..] {
        let Some(path) = text("JSON_REMOVE", path)? else {
            return Ok(SqlValue::Null);
        };
        let parts = parse_path("JSON_REMOVE", path)?;
        if parts.is_empty() {
            return Ok(SqlValue::Null);
        }
        remove_at(&mut root, &parts);
    }
    Ok(SqlValue::Text(encode("JSON_REMOVE", &root)?))
}

fn eval_json_array_length(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = text("JSON_ARRAY_LENGTH", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let root = parse_json("JSON_ARRAY_LENGTH", input)?;
    let selected = if values.len() == 2 {
        let Some(path) = text("JSON_ARRAY_LENGTH", &values[1])? else {
            return Ok(SqlValue::Null);
        };
        locate(&root, &parse_path("JSON_ARRAY_LENGTH", path)?)
    } else {
        Some(&root)
    };
    Ok(match selected {
        None => SqlValue::Null,
        Some(Value::Array(values)) => {
            SqlValue::Integer(i32::try_from(values.len()).unwrap_or(i32::MAX))
        }
        Some(_) => SqlValue::Integer(0),
    })
}

pub(crate) fn eval_for(name: &str) -> Option<EvalFn> {
    Some(match name {
        "json" => eval_json,
        "json_valid" => eval_json_valid,
        "json_type" => eval_json_type,
        "json_extract" => eval_json_extract,
        "json_object" => eval_json_object,
        "json_array" => eval_json_array,
        "json_insert" => eval_json_insert,
        "json_replace" => eval_json_replace,
        "json_set" => eval_json_set,
        "json_remove" => eval_json_remove,
        "json_array_length" => eval_json_array_length,
        _ => return None,
    })
}

pub(crate) fn table_rows(function: &str, values: &[SqlValue]) -> Result<Vec<Vec<SqlValue>>> {
    let Some(input) = text(function, &values[0])? else {
        return Ok(Vec::new());
    };
    let root = parse_json(function, input)?;
    let path = if values.len() == 2 {
        let Some(path) = text(function, &values[1])? else {
            return Ok(Vec::new());
        };
        path
    } else {
        "$"
    };
    let parts = parse_path(function, path)?;
    let Some(selected) = locate(&root, &parts) else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    if function.eq_ignore_ascii_case("JSON_EACH") {
        append_children(selected, path, None, &mut rows)?;
    } else {
        append_tree(selected, path, None, None, &mut rows)?;
    }
    if rows.len() > MAX_JSON_ROWS {
        return Err(invalid(function, "JSON table function exceeds 100000 rows"));
    }
    Ok(rows)
}

fn append_children(
    value: &Value,
    path: &str,
    parent: Option<i64>,
    rows: &mut Vec<Vec<SqlValue>>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                append_row(
                    value,
                    Some(SqlValue::Integer(i32::try_from(index).unwrap_or(i32::MAX))),
                    &format!("{path}[{index}]"),
                    path,
                    parent,
                    rows,
                )?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                append_row(
                    value,
                    Some(SqlValue::Text(key.clone())),
                    &format!("{path}.{key}"),
                    path,
                    parent,
                    rows,
                )?;
            }
        }
        _ => {
            append_row(value, None, path, path, parent, rows)?;
        }
    }
    Ok(())
}

fn append_tree(
    value: &Value,
    fullkey: &str,
    key: Option<SqlValue>,
    parent: Option<i64>,
    rows: &mut Vec<Vec<SqlValue>>,
) -> Result<()> {
    let id = append_row(value, key, fullkey, parent_path(fullkey), parent, rows)?;
    append_children_tree(value, fullkey, Some(id), rows)
}

fn append_children_tree(
    value: &Value,
    path: &str,
    parent: Option<i64>,
    rows: &mut Vec<Vec<SqlValue>>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                append_tree(
                    value,
                    &format!("{path}[{index}]"),
                    Some(SqlValue::Integer(i32::try_from(index).unwrap_or(i32::MAX))),
                    parent,
                    rows,
                )?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                append_tree(
                    value,
                    &format!("{path}.{key}"),
                    Some(SqlValue::Text(key.clone())),
                    parent,
                    rows,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parent_path(fullkey: &str) -> &str {
    fullkey
        .rfind(['.', '['])
        .map_or("$", |index| &fullkey[..index])
}

fn append_row(
    value: &Value,
    key: Option<SqlValue>,
    fullkey: &str,
    path: &str,
    parent: Option<i64>,
    rows: &mut Vec<Vec<SqlValue>>,
) -> Result<i64> {
    if rows.len() >= MAX_JSON_ROWS {
        return Err(invalid(
            "JSON_TREE",
            "JSON table function exceeds 100000 rows",
        ));
    }
    let id = rows.len() as i64;
    let scalar = json_to_sql(value)?;
    let atom = if matches!(value, Value::Array(_) | Value::Object(_)) {
        SqlValue::Null
    } else {
        scalar.clone()
    };
    rows.push(vec![
        key.unwrap_or(SqlValue::Null),
        scalar,
        SqlValue::Text(json_type(value).into()),
        atom,
        SqlValue::BigInt(id),
        parent.map(SqlValue::BigInt).unwrap_or(SqlValue::Null),
        SqlValue::Text(fullkey.into()),
        SqlValue::Text(path.into()),
    ]);
    Ok(id)
}

pub(crate) fn json_group_object(values: &[(String, SqlValue)]) -> Result<String> {
    let flat = values
        .iter()
        .flat_map(|(key, value)| [SqlValue::Text(key.clone()), value.clone()])
        .collect::<Vec<_>>();
    encode_object_pairs(&flat, "JSON_GROUP_OBJECT")
}

pub(crate) fn json_group_array(values: &[SqlValue]) -> Result<String> {
    encode(
        "JSON_GROUP_ARRAY",
        &Value::Array(values.iter().map(sql_to_json).collect::<Result<Vec<_>>>()?),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_json_is_rejected_before_parsing() {
        let input = format!("\"{}\"", "x".repeat(MAX_JSON_BYTES));
        assert_eq!(
            eval_json_valid(&[SqlValue::Text(input.clone())]).unwrap(),
            SqlValue::Boolean(false)
        );
        let error = parse_json("JSON", &input).unwrap_err().to_string();
        assert!(error.contains("1048576 bytes"), "{error}");
    }

    #[test]
    fn table_output_stops_at_the_row_limit() {
        let input = format!("[{}]", vec!["null"; MAX_JSON_ROWS + 1].join(","));
        let error = table_rows("JSON_EACH", &[SqlValue::Text(input)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("100000 rows"), "{error}");
    }
}
