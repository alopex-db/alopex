use std::sync::Arc;

use alopex_sql::{AlopexDialect, Parser, SqlValue, Statement};

use crate::{Database, Error, Result, SqlResult, SqlSession};

#[derive(Debug)]
struct PreparedState {
    sql: String,
    statement: Statement,
    bindings: Vec<Option<SqlValue>>,
    finalized: bool,
}

impl PreparedState {
    fn new(sql: &str) -> Result<Self> {
        let mut statements =
            Parser::parse_sql(&AlopexDialect, sql).map_err(alopex_sql::SqlError::from)?;
        if statements.len() != 1 {
            return Err(Error::PreparedStatementRequiresSingleStatement);
        }
        Ok(Self {
            sql: sql.to_owned(),
            statement: statements.pop().expect("prepared statement count checked"),
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
                    rendered.push_str(&render_prepared_parameter(value)?);
                }
            }
            Ok(())
        })?;
        Ok(rendered)
    }

    fn bound_values(&self) -> Result<Vec<SqlValue>> {
        self.ensure_open()?;
        let values = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .clone()
                    .ok_or(Error::PreparedParameterUnbound(index + 1))
            })
            .collect::<Result<Vec<_>>>()?;
        self.validate_values(&values)?;
        Ok(values)
    }

    fn validate_values(&self, values: &[SqlValue]) -> Result<()> {
        self.ensure_open()?;
        let count = self.parameter_count();
        if values.len() > count {
            return Err(Error::PreparedParameterOutOfRange {
                index: count + 1,
                count,
            });
        }
        if values.len() < count {
            return Err(Error::PreparedParameterUnbound(values.len() + 1));
        }
        if values.iter().all(is_supported_prepared_value) {
            Ok(())
        } else {
            Err(Error::UnsupportedPreparedParameterType)
        }
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
        self.database
            .execute_prepared_statement(&self.state.statement, &self.state.bound_values()?)
    }

    /// Execute parameter rows atomically in one transaction.
    pub fn execute_many<I, V>(&mut self, rows: I) -> Result<Vec<SqlResult>>
    where
        I: IntoIterator<Item = V>,
        V: AsRef<[SqlValue]>,
    {
        self.state.ensure_open()?;
        let mut rows = rows.into_iter();
        let Some(first) = rows.next() else {
            return Ok(Vec::new());
        };
        let mut session = self.database.sql_session();
        session.execute_sql("BEGIN")?;
        let state = &self.state;
        let outcome = session.execute_prepared_many(
            &state.statement,
            std::iter::once(first).chain(rows),
            |values| state.validate_values(values),
        );
        let outcome = outcome.and_then(|results| {
            session.execute_sql("COMMIT")?;
            Ok(results)
        });
        if outcome.is_err() {
            let _ = session.execute_sql("ROLLBACK");
        }
        outcome
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
        self.session
            .execute_prepared_statement(&self.state.statement, &self.state.bound_values()?)
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

/// Render a supported prepared value as a SQL literal for a fallback transport path.
pub fn render_prepared_parameter(value: &SqlValue) -> Result<String> {
    let quote = |value: &str| format!("'{}'", value.replace('\'', "''"));
    Ok(match value {
        SqlValue::Null => "NULL".into(),
        SqlValue::Boolean(value) => value.to_string().to_uppercase(),
        SqlValue::Integer(value) => value.to_string(),
        SqlValue::BigInt(value) => value.to_string(),
        SqlValue::Float(value) if value.is_finite() => format_finite_float(value.to_string())?,
        SqlValue::Double(value) if value.is_finite() => format_finite_float(value.to_string())?,
        SqlValue::Text(value) => quote(value),
        SqlValue::Decimal(value) => value.to_string(),
        SqlValue::Json(value) => format!("CAST({} AS JSON)", quote(value.as_str())),
        SqlValue::Vector(values) if values.iter().all(|value| value.is_finite()) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| format_finite_float(value.to_string()))
                .collect::<Result<Vec<_>>>()?
                .join(",")
        ),
        _ => return Err(Error::UnsupportedPreparedParameterType),
    })
}

fn format_finite_float(mut value: String) -> Result<String> {
    if value.contains(['e', 'E']) {
        return Err(Error::UnsupportedPreparedParameterType);
    }
    if !value.contains('.') {
        value.push_str(".0");
    }
    Ok(value)
}

fn is_supported_prepared_value(value: &SqlValue) -> bool {
    match value {
        SqlValue::Null
        | SqlValue::Boolean(_)
        | SqlValue::Integer(_)
        | SqlValue::BigInt(_)
        | SqlValue::Text(_)
        | SqlValue::Decimal(_)
        | SqlValue::Json(_) => true,
        SqlValue::Float(value) => value.is_finite(),
        SqlValue::Double(value) => value.is_finite(),
        SqlValue::Vector(values) => values.iter().all(|value| value.is_finite()),
        _ => false,
    }
}
