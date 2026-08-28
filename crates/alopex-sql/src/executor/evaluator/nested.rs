use serde_json::Value;

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::ResolvedType;
use crate::storage::SqlValue;

use super::registry::EvalFn;

const MAX_ELEMENTS: usize = 100_000;

fn invalid(function: &str, reason: impl Into<String>) -> ExecutorError {
    EvaluationError::InvalidArgument {
        function: function.into(),
        reason: reason.into(),
    }
    .into()
}

fn array<'a>(function: &str, value: &'a SqlValue) -> Result<Option<&'a [SqlValue]>> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Array(values) => Ok(Some(values)),
        other => Err(invalid(
            function,
            format!("expected ARRAY, found {}", other.type_name()),
        )),
    }
}

fn index(function: &str, value: &SqlValue) -> Result<Option<i64>> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Integer(value) => Ok(Some(i64::from(*value))),
        SqlValue::BigInt(value) => Ok(Some(*value)),
        other => Err(invalid(
            function,
            format!("expected INTEGER, found {}", other.type_name()),
        )),
    }
}

fn bounded(function: &str, values: Vec<SqlValue>) -> Result<SqlValue> {
    if values.len() > MAX_ELEMENTS {
        return Err(invalid(function, "array exceeds 100000 elements"));
    }
    Ok(SqlValue::Array(values))
}

fn eval_array_value(values: &[SqlValue]) -> Result<SqlValue> {
    bounded("ARRAY", values.to_vec())
}

fn eval_array_append(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_APPEND", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let mut output = input.to_vec();
    output.push(values[1].clone());
    bounded("ARRAY_APPEND", output)
}

fn eval_array_prepend(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_PREPEND", &values[1])? else {
        return Ok(SqlValue::Null);
    };
    let mut output = Vec::with_capacity(input.len() + 1);
    output.push(values[0].clone());
    output.extend_from_slice(input);
    bounded("ARRAY_PREPEND", output)
}

fn eval_array_cat(values: &[SqlValue]) -> Result<SqlValue> {
    let (Some(left), Some(right)) = (
        array("ARRAY_CAT", &values[0])?,
        array("ARRAY_CAT", &values[1])?,
    ) else {
        return Ok(SqlValue::Null);
    };
    let mut output = Vec::with_capacity(left.len().saturating_add(right.len()));
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    bounded("ARRAY_CAT", output)
}

fn eval_array_remove(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_REMOVE", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    bounded(
        "ARRAY_REMOVE",
        input
            .iter()
            .filter(|value| *value != &values[1])
            .cloned()
            .collect(),
    )
}

fn eval_array_replace(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_REPLACE", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    bounded(
        "ARRAY_REPLACE",
        input
            .iter()
            .map(|value| {
                if value == &values[1] {
                    values[2].clone()
                } else {
                    value.clone()
                }
            })
            .collect(),
    )
}

fn eval_array_length(values: &[SqlValue]) -> Result<SqlValue> {
    Ok(match array("ARRAY_LENGTH", &values[0])? {
        Some(values) => SqlValue::Integer(values.len() as i32),
        None => SqlValue::Null,
    })
}

fn eval_array_position(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_POSITION", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    Ok(input
        .iter()
        .position(|value| value == &values[1])
        .map(|index| SqlValue::Integer(index as i32 + 1))
        .unwrap_or(SqlValue::Null))
}

fn eval_array_positions(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_POSITIONS", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    bounded(
        "ARRAY_POSITIONS",
        input
            .iter()
            .enumerate()
            .filter(|(_, value)| *value == &values[1])
            .map(|(index, _)| SqlValue::Integer(index as i32 + 1))
            .collect(),
    )
}

fn eval_string_to_array(values: &[SqlValue]) -> Result<SqlValue> {
    let (SqlValue::Text(input), SqlValue::Text(delimiter)) = (&values[0], &values[1]) else {
        if values[0].is_null() || values[1].is_null() {
            return Ok(SqlValue::Null);
        }
        return Err(invalid("STRING_TO_ARRAY", "expected TEXT arguments"));
    };
    let null_text = values.get(2).and_then(|value| match value {
        SqlValue::Text(value) => Some(value.as_str()),
        _ => None,
    });
    let parts: Vec<String> = if delimiter.is_empty() {
        input.chars().map(|value| value.to_string()).collect()
    } else {
        input.split(delimiter).map(str::to_string).collect()
    };
    bounded(
        "STRING_TO_ARRAY",
        parts
            .into_iter()
            .map(|value| {
                if Some(value.as_str()) == null_text {
                    SqlValue::Null
                } else {
                    SqlValue::Text(value)
                }
            })
            .collect(),
    )
}

fn eval_array_to_string(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("ARRAY_TO_STRING", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let SqlValue::Text(delimiter) = &values[1] else {
        return if values[1].is_null() {
            Ok(SqlValue::Null)
        } else {
            Err(invalid("ARRAY_TO_STRING", "delimiter must be TEXT"))
        };
    };
    let null_text = values.get(2).and_then(|value| match value {
        SqlValue::Text(value) => Some(value.as_str()),
        _ => None,
    });
    let mut output = Vec::with_capacity(input.len());
    for value in input {
        match value {
            SqlValue::Null if null_text.is_none() => {}
            SqlValue::Null => output.push(null_text.unwrap().to_string()),
            SqlValue::Text(value) => output.push(value.clone()),
            other => match super::coerce_value(other.clone(), &ResolvedType::Text)? {
                SqlValue::Text(value) => output.push(value),
                _ => return Err(invalid("ARRAY_TO_STRING", "array element is not scalar")),
            },
        }
    }
    Ok(SqlValue::Text(output.join(delimiter)))
}

fn eval_map(values: &[SqlValue]) -> Result<SqlValue> {
    let (Some(keys), Some(map_values)) = (array("MAP", &values[0])?, array("MAP", &values[1])?)
    else {
        return Ok(SqlValue::Null);
    };
    if keys.len() != map_values.len() {
        return Err(invalid(
            "MAP",
            "key and value arrays must have equal length",
        ));
    }
    if keys.iter().any(SqlValue::is_null) {
        return Err(invalid("MAP", "map keys must not be NULL"));
    }
    Ok(SqlValue::Map(
        keys.iter()
            .cloned()
            .zip(map_values.iter().cloned())
            .collect(),
    ))
}

fn eval_struct_pack(values: &[SqlValue]) -> Result<SqlValue> {
    let mut fields = Vec::with_capacity(values.len() / 2);
    for pair in values.as_chunks::<2>().0 {
        let SqlValue::Text(name) = &pair[0] else {
            return Err(invalid("STRUCT_PACK", "field names must be TEXT"));
        };
        if fields.iter().any(|(existing, _)| existing == name) {
            return Err(invalid("STRUCT_PACK", "duplicate struct field name"));
        }
        fields.push((name.clone(), pair[1].clone()));
    }
    Ok(SqlValue::Struct(fields))
}

fn eval_subscript(values: &[SqlValue]) -> Result<SqlValue> {
    match &values[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Array(items) => {
            let Some(index) = index("SUBSCRIPT", &values[1])? else {
                return Ok(SqlValue::Null);
            };
            Ok(index
                .checked_sub(1)
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| items.get(index))
                .cloned()
                .unwrap_or(SqlValue::Null))
        }
        SqlValue::Map(entries) => Ok(entries
            .iter()
            .find(|(key, _)| key == &values[1])
            .map(|(_, value)| value.clone())
            .unwrap_or(SqlValue::Null)),
        SqlValue::Struct(fields) => {
            let SqlValue::Text(name) = &values[1] else {
                return Err(invalid("SUBSCRIPT", "struct field selector must be TEXT"));
            };
            Ok(fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value.clone())
                .unwrap_or(SqlValue::Null))
        }
        other => Err(invalid(
            "SUBSCRIPT",
            format!("cannot subscript {}", other.type_name()),
        )),
    }
}

fn eval_slice(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(input) = array("SLICE", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let start = index("SLICE", &values[1])?.unwrap_or(1).max(1) as usize;
    let end = index("SLICE", &values[2])?
        .unwrap_or(input.len() as i64)
        .max(0) as usize;
    if start > end || start > input.len() {
        return Ok(SqlValue::Array(Vec::new()));
    }
    Ok(SqlValue::Array(
        input[start - 1..end.min(input.len())].to_vec(),
    ))
}

pub(crate) fn parse_typed_json(input: &str, data_type: &ResolvedType) -> Result<SqlValue> {
    let value: Value = serde_json::from_str(input)
        .map_err(|error| invalid("NESTED", format!("invalid JSON input: {error}")))?;
    from_json(&value, data_type, 0)
}

fn from_json(value: &Value, data_type: &ResolvedType, depth: usize) -> Result<SqlValue> {
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    if depth > 16 {
        return Err(invalid("NESTED", "nested value exceeds depth 16"));
    }
    match (value, data_type) {
        (Value::Array(values), ResolvedType::Array(element)) if values.len() <= MAX_ELEMENTS => {
            values
                .iter()
                .map(|value| from_json(value, element, depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(SqlValue::Array)
        }
        (
            Value::Object(values),
            ResolvedType::Map {
                key,
                value: value_type,
            },
        ) if **key == ResolvedType::Text && values.len() <= MAX_ELEMENTS => values
            .iter()
            .map(|(name, value)| {
                Ok((
                    SqlValue::Text(name.clone()),
                    from_json(value, value_type, depth + 1)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(SqlValue::Map),
        (
            Value::Array(values),
            ResolvedType::Map {
                key,
                value: value_type,
            },
        ) if values.len() <= MAX_ELEMENTS => values
            .iter()
            .map(|entry| {
                let Value::Array(entry) = entry else {
                    return Err(invalid("NESTED", "MAP entry must be a key/value pair"));
                };
                if entry.len() != 2 {
                    return Err(invalid("NESTED", "MAP entry must contain two values"));
                }
                Ok((
                    from_json(&entry[0], key, depth + 1)?,
                    from_json(&entry[1], value_type, depth + 1)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(SqlValue::Map),
        (Value::Object(values), ResolvedType::Struct(fields)) => fields
            .iter()
            .map(|(name, data_type)| {
                Ok((
                    name.clone(),
                    values
                        .get(name)
                        .map(|value| from_json(value, data_type, depth + 1))
                        .transpose()?
                        .unwrap_or(SqlValue::Null),
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(SqlValue::Struct),
        (Value::Bool(value), ResolvedType::Boolean) => Ok(SqlValue::Boolean(*value)),
        (Value::String(value), ResolvedType::Text) => Ok(SqlValue::Text(value.clone())),
        (Value::Number(value), ResolvedType::Integer) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(SqlValue::Integer)
            .ok_or_else(|| invalid("NESTED", "number is outside INTEGER range")),
        (Value::Number(value), ResolvedType::BigInt) => value
            .as_i64()
            .map(SqlValue::BigInt)
            .ok_or_else(|| invalid("NESTED", "number is outside BIGINT range")),
        (Value::Number(value), ResolvedType::Float) => value
            .as_f64()
            .map(|value| SqlValue::Float(value as f32))
            .ok_or_else(|| invalid("NESTED", "invalid FLOAT")),
        (Value::Number(value), ResolvedType::Double) => value
            .as_f64()
            .map(SqlValue::Double)
            .ok_or_else(|| invalid("NESTED", "invalid DOUBLE")),
        _ => Err(invalid(
            "NESTED",
            format!("JSON value does not match {data_type}"),
        )),
    }
}

pub(crate) fn eval_for(name: &str) -> Option<EvalFn> {
    Some(match name {
        "array_value" | "list_value" => eval_array_value,
        "array_append" => eval_array_append,
        "array_prepend" => eval_array_prepend,
        "array_cat" => eval_array_cat,
        "array_remove" => eval_array_remove,
        "array_replace" => eval_array_replace,
        "array_length" => eval_array_length,
        "array_position" => eval_array_position,
        "array_positions" => eval_array_positions,
        "string_to_array" => eval_string_to_array,
        "array_to_string" => eval_array_to_string,
        "map" => eval_map,
        "struct_pack" => eval_struct_pack,
        "array_subscript" => eval_subscript,
        "array_slice" => eval_slice,
        _ => return None,
    })
}
