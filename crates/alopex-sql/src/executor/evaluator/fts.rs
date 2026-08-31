use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::fts;
use crate::storage::SqlValue;

use super::registry::EvalFn;

fn invalid(function: &str, reason: impl Into<String>) -> ExecutorError {
    EvaluationError::InvalidArgument {
        function: function.into(),
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

fn configured<'a>(function: &str, values: &'a [SqlValue]) -> Result<Option<(&'a str, &'a str)>> {
    let (config, input) = if values.len() == 1 {
        ("simple", &values[0])
    } else {
        let Some(config) = text(function, &values[0])? else {
            return Ok(None);
        };
        (config, &values[1])
    };
    Ok(text(function, input)?.map(|input| (config, input)))
}

fn eval_to_tsvector(values: &[SqlValue]) -> Result<SqlValue> {
    let Some((config, input)) = configured("TO_TSVECTOR", values)? else {
        return Ok(SqlValue::Null);
    };
    fts::to_tsvector(config, input)
        .map(SqlValue::Text)
        .map_err(|error| invalid("TO_TSVECTOR", error))
}

fn query_value(function: &str, values: &[SqlValue], web: bool, plain: bool) -> Result<SqlValue> {
    let Some((config, input)) = configured(function, values)? else {
        return Ok(SqlValue::Null);
    };
    let query = if web {
        fts::websearch_to_tsquery(config, input)
    } else if plain {
        fts::plainto_tsquery(config, input)
    } else {
        fts::parse_tsquery(config, input)
    }
    .map_err(|error| invalid(function, error))?;
    Ok(SqlValue::Text(fts::format_query(&query)))
}

fn eval_to_tsquery(values: &[SqlValue]) -> Result<SqlValue> {
    query_value("TO_TSQUERY", values, false, false)
}

fn eval_plainto_tsquery(values: &[SqlValue]) -> Result<SqlValue> {
    query_value("PLAINTO_TSQUERY", values, false, true)
}

fn eval_websearch_to_tsquery(values: &[SqlValue]) -> Result<SqlValue> {
    query_value("WEBSEARCH_TO_TSQUERY", values, true, false)
}

fn eval_ts_rank(values: &[SqlValue]) -> Result<SqlValue> {
    let Some(vector) = text("TS_RANK", &values[0])? else {
        return Ok(SqlValue::Null);
    };
    let Some(query) = text("TS_RANK", &values[1])? else {
        return Ok(SqlValue::Null);
    };
    let tokens = fts::parse_tsvector(vector).map_err(|error| invalid("TS_RANK", error))?;
    let query = fts::parse_tsquery("simple", query).map_err(|error| invalid("TS_RANK", error))?;
    Ok(SqlValue::Double(fts::rank(&tokens, &query)))
}

fn eval_ts_headline(values: &[SqlValue]) -> Result<SqlValue> {
    let (config, document, query) = if values.len() == 2 {
        ("simple", &values[0], &values[1])
    } else {
        let Some(config) = text("TS_HEADLINE", &values[0])? else {
            return Ok(SqlValue::Null);
        };
        (config, &values[1], &values[2])
    };
    let Some(document) = text("TS_HEADLINE", document)? else {
        return Ok(SqlValue::Null);
    };
    let Some(query) = text("TS_HEADLINE", query)? else {
        return Ok(SqlValue::Null);
    };
    let query = fts::parse_tsquery(config, query).map_err(|error| invalid("TS_HEADLINE", error))?;
    fts::headline(config, document, &query)
        .map(SqlValue::Text)
        .map_err(|error| invalid("TS_HEADLINE", error))
}

pub(crate) fn eval_for(name: &str) -> Option<EvalFn> {
    Some(match name {
        "to_tsvector" => eval_to_tsvector,
        "to_tsquery" => eval_to_tsquery,
        "plainto_tsquery" => eval_plainto_tsquery,
        "websearch_to_tsquery" => eval_websearch_to_tsquery,
        "ts_rank" => eval_ts_rank,
        "ts_headline" => eval_ts_headline,
        _ => return None,
    })
}
