use alopex_sql::storage::SqlValue;
use alopex_sql::SqlError;

use crate::Database;
use crate::Result;
use crate::SqlResult;

/// Prepared SQL statement with positional `?` parameters.
///
/// Parameters are 1-based (`bind(1, ...)` binds the first placeholder).
pub struct PreparedStatement<'db> {
    db: &'db Database,
    sql_template: String,
    segments: Vec<String>,
    bindings: Vec<Option<SqlValue>>,
    finalized: bool,
}

impl<'db> PreparedStatement<'db> {
    pub(crate) fn prepare(db: &'db Database, sql: &str) -> Result<Self> {
        if sql.contains('\0') {
            return Err(bind_error(
                "NUL byte in SQL is not supported at the FFI boundary",
            ));
        }
        let segments = split_on_placeholders(sql)?;
        let placeholder_count = segments.len().saturating_sub(1);
        Ok(Self {
            db,
            sql_template: sql.to_string(),
            segments,
            bindings: vec![None; placeholder_count],
            finalized: false,
        })
    }

    /// Returns the original SQL template.
    pub fn sql(&self) -> &str {
        &self.sql_template
    }

    /// Returns the number of positional `?` placeholders in this statement.
    pub fn placeholder_count(&self) -> usize {
        self.bindings.len()
    }

    /// Binds a positional parameter using 1-based indexing.
    pub fn bind<V>(&mut self, index: usize, value: V) -> Result<&mut Self>
    where
        V: Into<SqlValue>,
    {
        self.ensure_open()?;
        let slot = self.slot_mut(index)?;
        *slot = Some(value.into());
        Ok(self)
    }

    /// Binds SQL `NULL` to a positional parameter.
    pub fn bind_null(&mut self, index: usize) -> Result<&mut Self> {
        self.bind(index, SqlValue::Null)
    }

    /// Executes the statement with the currently bound parameters.
    pub fn execute(&self) -> Result<SqlResult> {
        self.ensure_open()?;
        let sql = self.render_sql()?;
        self.db.execute_sql(&sql)
    }

    /// Clears all bound parameters so the statement can be rebound.
    pub fn reset(&mut self) -> Result<()> {
        self.ensure_open()?;
        self.bindings.fill(None);
        Ok(())
    }

    /// Finalizes the statement. A finalized statement cannot be reused.
    pub fn finalize(&mut self) {
        self.bindings.clear();
        self.finalized = true;
    }

    fn ensure_open(&self) -> Result<()> {
        if self.finalized {
            return Err(bind_error("prepared statement is finalized"));
        }
        Ok(())
    }

    fn slot_mut(&mut self, index: usize) -> Result<&mut Option<SqlValue>> {
        if index == 0 || index > self.bindings.len() {
            return Err(bind_error(format!(
                "bind index out of range: {index} (expected 1..={})",
                self.bindings.len()
            )));
        }
        Ok(&mut self.bindings[index - 1])
    }

    fn render_sql(&self) -> Result<String> {
        let mut rendered = Vec::with_capacity(self.bindings.len());
        for (i, value) in self.bindings.iter().enumerate() {
            let Some(value) = value else {
                return Err(bind_error(format!(
                    "missing bind value for parameter {}",
                    i + 1
                )));
            };
            rendered.push(render_value(value)?);
        }
        if rendered.is_empty() {
            return Ok(self.sql_template.clone());
        }

        let extra: usize = rendered.iter().map(String::len).sum();
        let mut out = String::with_capacity(self.sql_template.len() + extra);
        for (i, segment) in self.segments.iter().enumerate() {
            out.push_str(segment);
            if let Some(value) = rendered.get(i) {
                out.push_str(value);
            }
        }
        Ok(out)
    }
}

enum ScanState {
    Normal,
    Quoted(char),
    LineComment,
    BlockComment,
}

fn split_on_placeholders(sql: &str) -> Result<Vec<String>> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut state = ScanState::Normal;
    let mut iter = sql.char_indices().peekable();
    while let Some((index, c)) = iter.next() {
        let next = iter.peek().map(|&(_, ch)| ch);
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
                    segments.push(sql[start..index].to_string());
                    start = index + 1;
                } else if c == '$' && next.is_some_and(|ch| ch.is_ascii_digit()) {
                    return Err(bind_error(
                        "positional '$n' parameters are not supported; use '?' placeholders",
                    ));
                } else if c == ':'
                    && next.is_some_and(is_named_parameter_start)
                    && !sql[..index].ends_with(':')
                {
                    return Err(bind_error(
                        "named parameters are not supported; use '?' placeholders",
                    ));
                }
            }
            ScanState::Quoted(q) => {
                if c == q {
                    if next == Some(q) {
                        iter.next();
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
    segments.push(sql[start..].to_string());
    Ok(segments)
}

fn is_named_parameter_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn render_value(value: &SqlValue) -> Result<String> {
    match value {
        SqlValue::Null => Ok("NULL".to_string()),
        SqlValue::Integer(v) => Ok(v.to_string()),
        SqlValue::BigInt(v) => Ok(v.to_string()),
        SqlValue::Boolean(v) => Ok(if *v { "TRUE" } else { "FALSE" }.to_string()),
        SqlValue::Text(text) => {
            if text.contains('\0') {
                return Err(bind_error(
                    "text parameters containing NUL bytes are not supported",
                ));
            }
            Ok(escape_text(text))
        }
        SqlValue::Float(v) => render_float(f64::from(*v)),
        SqlValue::Double(v) => render_float(*v),
        SqlValue::Vector(values) => {
            if values.is_empty() {
                return Err(bind_error("empty vector literal is not supported"));
            }
            let mut parts = Vec::with_capacity(values.len());
            for value in values {
                parts.push(render_float(f64::from(*value))?);
            }
            Ok(format!("[{}]", parts.join(", ")))
        }
        other => Err(bind_error(format!(
            "unsupported parameter type for binding: {}",
            other.type_name()
        ))),
    }
}

fn render_float(value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(bind_error(
            "non-finite floating-point values are not supported",
        ));
    }
    let mut out = format!("{value}");
    if out.contains(['e', 'E']) {
        return Err(bind_error(
            "scientific notation is not supported for bound floating-point values",
        ));
    }
    if !out.contains('.') {
        out.push_str(".0");
    }
    Ok(out)
}

fn escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('\'');
    for ch in text.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn bind_error(message: impl Into<String>) -> crate::Error {
    crate::Error::Sql(SqlError::Execution {
        message: message.into(),
        code: "ALOPEX-E023",
    })
}
