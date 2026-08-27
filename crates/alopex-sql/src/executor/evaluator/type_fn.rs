//! Type inspection and SQL quoting functions.

use crate::executor::Result;
use crate::storage::SqlValue;

pub fn eval_for(name: &str) -> Option<super::registry::EvalFn> {
    match name {
        "typeof" => Some(eval_typeof),
        "pg_typeof" => Some(eval_pg_typeof),
        "quote" => Some(eval_quote),
        _ => None,
    }
}

fn eval_typeof(values: &[SqlValue]) -> Result<SqlValue> {
    Ok(SqlValue::Text(
        values
            .first()
            .unwrap_or(&SqlValue::Null)
            .type_name()
            .to_ascii_lowercase(),
    ))
}

fn eval_pg_typeof(values: &[SqlValue]) -> Result<SqlValue> {
    let name = match values.first().unwrap_or(&SqlValue::Null) {
        SqlValue::Integer(_) => "integer",
        SqlValue::BigInt(_) => "bigint",
        SqlValue::Float(_) => "real",
        SqlValue::Double(_) => "double precision",
        SqlValue::Text(_) => "text",
        SqlValue::Blob(_) => "bytea",
        SqlValue::Boolean(_) => "boolean",
        SqlValue::Timestamp(_) => "timestamp",
        SqlValue::Date(_) => "date",
        SqlValue::Time(_) => "time",
        SqlValue::Interval { .. } => "interval",
        SqlValue::Decimal(_) => "numeric",
        SqlValue::Vector(_) => "vector",
        SqlValue::Null => "unknown",
    };
    Ok(SqlValue::Text(name.into()))
}

fn eval_quote(values: &[SqlValue]) -> Result<SqlValue> {
    let text = match values.first().unwrap_or(&SqlValue::Null) {
        SqlValue::Null => "NULL".into(),
        SqlValue::Text(value) => format!("'{}'", value.replace('\'', "''")),
        SqlValue::Boolean(value) => {
            if *value {
                "1".into()
            } else {
                "0".into()
            }
        }
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::Decimal(value) => value.to_string(),
        SqlValue::BigInt(value) => value.to_string(),
        SqlValue::Float(value) => value.to_string(),
        SqlValue::Double(value) => value.to_string(),
        SqlValue::Blob(value) => format!(
            "X'{}'",
            value.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ),
        other => format!("'{}'", other.type_name()),
    };
    Ok(SqlValue::Text(text))
}
