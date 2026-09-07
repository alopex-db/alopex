use std::sync::Arc;

use alopex_sql::{AlopexDialect, Parser, SqlValue};

use crate::{Database, Error, Result, SqlResult, SqlSession};

#[derive(Debug)]
struct PreparedState {
    sql: String,
    bindings: Vec<Option<SqlValue>>,
    finalized: bool,
}

impl PreparedState {
    fn new(sql: &str) -> Result<Self> {
        let statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(alopex_sql::SqlError::from)?;
        if statements.len() != 1 {
            return Err(Error::PreparedStatementRequiresSingleStatement);
        }
        Ok(Self {
            sql: sql.to_owned(),
            bindings: vec![None; positional_parameter_count(sql)],
            finalized: false,
        })
    }

    fn parameter_count(&self) -> usize {
        self.bindings.len()
    }

    fn bind(&mut self, index: usize, value: SqlValue) -> Result<()> {
        self.ensure_open()?;
        let count = self.bindings.len();
        let slot = index
            .checked_sub(1)
            .and_then(|index| self.bindings.get_mut(index))
            .ok_or(Error::PreparedParameterOutOfRange { index, count })?;
        *slot = Some(value);
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.ensure_open()?;
        self.bindings.fill(None);
        Ok(())
    }

    fn finalize(&mut self) -> Result<()> {
        self.ensure_open()?;
        self.bindings.clear();
        self.finalized = true;
        Ok(())
    }

    fn render(&self) -> Result<String> {
        self.ensure_open()?;
        for (index, value) in self.bindings.iter().enumerate() {
            if value.is_none() {
                return Err(Error::PreparedParameterUnbound(index + 1));
            }
        }

        let mut rendered = String::with_capacity(self.sql.len() + self.bindings.len() * 8);
        let mut parameter = 0usize;
        scan_sql(&self.sql, |chunk| {
            match chunk {
                SqlChunk::Text(text) => rendered.push_str(text),
                SqlChunk::Parameter => {
                    let value = self.bindings[parameter]
                        .as_ref()
                        .expect("all bindings checked above");
                    parameter += 1;
                    rendered.push_str(&sql_literal(value)?);
                }
            }
            Ok(())
        })?;
        Ok(rendered)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.finalized {
            Err(Error::PreparedStatementFinalized)
        } else {
            Ok(())
        }
    }
}

/// Safely bind positional values and return SQL suitable for any transport.
///
/// Only `?` in expression positions is accepted. Values are emitted as SQL
/// literals, so they cannot become identifiers or SQL syntax.
pub fn bind_sql_parameters(sql: &str, values: &[SqlValue]) -> Result<String> {
    if values.is_empty() && positional_parameter_count(sql) == 0 {
        return Ok(sql.to_owned());
    }
    let mut state = PreparedState::new(sql)?;
    for (index, value) in values.iter().cloned().enumerate() {
        state.bind(index + 1, value)?;
    }
    state.render()
}

/// A reusable positional-parameter statement executed in auto-commit mode.
pub struct PreparedStatement {
    database: Arc<Database>,
    state: PreparedState,
}

impl Database {
    /// Prepare one SQL statement with one-based positional `?` parameters.
    pub fn prepare(self: &Arc<Self>, sql: &str) -> Result<PreparedStatement> {
        Ok(PreparedStatement {
            database: Arc::clone(self),
            state: PreparedState::new(sql)?,
        })
    }
}

impl PreparedStatement {
    /// Return the number of positional parameters.
    pub fn parameter_count(&self) -> usize {
        self.state.parameter_count()
    }

    /// Bind or rebind one one-based positional parameter.
    pub fn bind(&mut self, index: usize, value: SqlValue) -> Result<()> {
        self.state.bind(index, value)
    }

    /// Clear every binding while keeping the prepared SQL reusable.
    pub fn reset(&mut self) -> Result<()> {
        self.state.reset()
    }

    /// Permanently finalize this prepared statement.
    pub fn finalize(&mut self) -> Result<()> {
        self.state.finalize()
    }

    /// Execute with the current bindings in an auto-commit transaction.
    pub fn execute(&mut self) -> Result<SqlResult> {
        self.database.execute_sql(&self.state.render()?)
    }
}

/// A prepared statement borrowing one SQL session and its active transaction.
pub struct PreparedSessionStatement<'a> {
    session: &'a mut SqlSession,
    state: PreparedState,
}

impl SqlSession {
    /// Prepare one statement whose execution uses this session.
    pub fn prepare<'a>(&'a mut self, sql: &str) -> Result<PreparedSessionStatement<'a>> {
        Ok(PreparedSessionStatement {
            session: self,
            state: PreparedState::new(sql)?,
        })
    }
}

impl PreparedSessionStatement<'_> {
    /// Return the number of positional parameters.
    pub fn parameter_count(&self) -> usize {
        self.state.parameter_count()
    }

    /// Bind or rebind one one-based positional parameter.
    pub fn bind(&mut self, index: usize, value: SqlValue) -> Result<()> {
        self.state.bind(index, value)
    }

    /// Clear every binding while keeping the prepared SQL reusable.
    pub fn reset(&mut self) -> Result<()> {
        self.state.reset()
    }

    /// Permanently finalize this prepared statement.
    pub fn finalize(&mut self) -> Result<()> {
        self.state.finalize()
    }

    /// Execute with the current bindings in the borrowed SQL session.
    pub fn execute(&mut self) -> Result<SqlResult> {
        self.session.execute_sql(&self.state.render()?)
    }
}

enum SqlChunk<'a> {
    Text(&'a str),
    Parameter,
}

fn positional_parameter_count(sql: &str) -> usize {
    let mut count = 0;
    let _ = scan_sql(sql, |chunk| {
        if matches!(chunk, SqlChunk::Parameter) {
            count += 1;
        }
        Ok(())
    });
    count
}

fn scan_sql(sql: &str, mut visit: impl FnMut(SqlChunk<'_>) -> Result<()>) -> Result<()> {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut text_start = 0usize;
    let mut quote = None;
    let mut block_comment = false;
    let mut line_comment = false;
    while index < bytes.len() {
        if line_comment {
            if bytes[index] == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if bytes.get(index..index + 2) == Some(b"*/") {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if bytes[index] == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"--") {
            line_comment = true;
            index += 2;
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            block_comment = true;
            index += 2;
        } else if matches!(bytes[index], b'\'' | b'"') {
            quote = Some(bytes[index]);
            index += 1;
        } else if bytes[index] == b'?' {
            visit(SqlChunk::Text(&sql[text_start..index]))?;
            visit(SqlChunk::Parameter)?;
            index += 1;
            text_start = index;
        } else {
            index += 1;
        }
    }
    visit(SqlChunk::Text(&sql[text_start..]))
}

fn sql_literal(value: &SqlValue) -> Result<String> {
    let quote = |value: &str| format!("'{}'", value.replace('\'', "''"));
    Ok(match value {
        SqlValue::Null => "NULL".into(),
        SqlValue::Boolean(value) => value.to_string().to_uppercase(),
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::BigInt(value) => value.to_string(),
        SqlValue::Float(value) if value.is_finite() => value.to_string(),
        SqlValue::Double(value) if value.is_finite() => value.to_string(),
        SqlValue::Text(value) => quote(value),
        SqlValue::Decimal(value) => value.to_string(),
        SqlValue::Json(value) => format!("CAST({} AS JSON)", quote(value.as_str())),
        SqlValue::Vector(values) if values.iter().all(|value| value.is_finite()) => format!(
            "[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => return Err(Error::UnsupportedPreparedParameterType),
    })
}
