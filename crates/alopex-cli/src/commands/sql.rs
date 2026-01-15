//! SQL Command - SQL query execution
//!
//! Supports: query execution, file-based queries

use std::fs;
use std::io::{self, Read, Write};

use alopex_embedded::Database;

use crate::batch::BatchMode;
use crate::cli::SqlCommand;
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::Formatter;
use crate::streaming::{StreamingWriter, WriteStatus};

/// Execute a SQL command with dynamic column detection.
///
/// This function creates the StreamingWriter internally based on the query result type,
/// ensuring that SELECT queries use the correct column headers.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The SQL command to execute.
/// * `writer` - The output writer.
/// * `formatter` - The formatter to use.
/// * `limit` - Optional row limit.
/// * `quiet` - Whether to suppress warnings.
pub fn execute_with_formatter<W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let sql = cmd.resolve_query(batch_mode)?;

    execute_sql_with_formatter(db, &sql, writer, formatter, limit, quiet)
}

/// Execute a SQL command against a remote server using HttpClient.
pub async fn execute_remote_with_formatter<W: Write>(
    client: &HttpClient,
    cmd: &SqlCommand,
    batch_mode: &BatchMode,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let sql = cmd.resolve_query(batch_mode)?;
    let request = RemoteSqlRequest {
        sql,
        streaming: false,
    };
    let response: RemoteSqlResponse = client
        .post_json("api/sql/query", &request)
        .await
        .map_err(map_client_error)?;

    if response.columns.is_empty() {
        if quiet {
            return Ok(());
        }
        let message = match response.affected_rows {
            Some(count) => format!("{count} row(s) affected"),
            None => "Operation completed successfully".to_string(),
        };
        let columns = sql_status_columns();
        let mut streaming_writer =
            StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
        streaming_writer.prepare(Some(1))?;
        let row = Row::new(vec![Value::Text("OK".to_string()), Value::Text(message)]);
        streaming_writer.write_row(row)?;
        return streaming_writer.finish();
    }

    let columns: Vec<Column> = response
        .columns
        .iter()
        .map(|col| Column::new(&col.name, data_type_from_string(&col.data_type)))
        .collect();
    let mut streaming_writer =
        StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
    streaming_writer.prepare(Some(response.rows.len()))?;
    for row in response.rows {
        let values = row.into_iter().map(remote_value_to_value).collect();
        match streaming_writer.write_row(Row::new(values))? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }
    streaming_writer.finish()
}

/// Legacy execute function for backward compatibility with tests.
#[allow(dead_code)]
pub fn execute<W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let sql = cmd.resolve_query(batch_mode)?;

    execute_sql(db, &sql, writer)
}

impl SqlCommand {
    /// Resolve the SQL query source (argument, file, or stdin).
    pub fn resolve_query(&self, batch_mode: &BatchMode) -> Result<String> {
        match (&self.query, &self.file) {
            (Some(query), None) => Ok(query.clone()),
            (None, Some(file)) => fs::read_to_string(file).map_err(|e| {
                CliError::InvalidArgument(format!("Failed to read SQL file '{}': {}", file, e))
            }),
            (None, None) if !batch_mode.is_tty => {
                let mut buf = String::new();
                io::stdin().read_to_string(&mut buf)?;
                Ok(buf)
            }
            (None, None) => Err(CliError::NoQueryProvided),
            (Some(_), Some(_)) => Err(CliError::InvalidArgument(
                "Cannot specify both query and file".to_string(),
            )),
        }
    }
}

/// Execute SQL and write results.
fn execute_sql<W: Write>(db: &Database, sql: &str, writer: &mut StreamingWriter<W>) -> Result<()> {
    use alopex_embedded::SqlResult;

    let result = db.execute_sql(sql)?;

    match result {
        SqlResult::Success => {
            // DDL success - output simple status
            writer.prepare(Some(1))?;
            let row = Row::new(vec![
                Value::Text("OK".to_string()),
                Value::Text("Operation completed successfully".to_string()),
            ]);
            writer.write_row(row)?;
            writer.finish()?;
        }
        SqlResult::RowsAffected(count) => {
            // DML success - output affected rows count
            writer.prepare(Some(1))?;
            let row = Row::new(vec![
                Value::Text("OK".to_string()),
                Value::Text(format!("{} row(s) affected", count)),
            ]);
            writer.write_row(row)?;
            writer.finish()?;
        }
        SqlResult::Query(query_result) => {
            // SELECT result - output rows
            let row_count = query_result.rows.len();
            writer.prepare(Some(row_count))?;

            for sql_row in query_result.rows {
                let values: Vec<Value> = sql_row.into_iter().map(sql_value_to_value).collect();
                let row = Row::new(values);

                match writer.write_row(row)? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
                }
            }

            writer.finish()?;
        }
    }

    Ok(())
}

/// Execute SQL with formatter, dynamically determining columns from query result.
///
/// This function executes the SQL using the streaming API for FR-7 compliance,
/// then creates the StreamingWriter with the correct columns based on the result type
/// (status columns for DDL/DML, query result columns for SELECT).
///
/// FR-7 Compliance: Uses SQL parser to detect SELECT queries instead of heuristic.
/// This properly handles:
/// - WITH clauses (CTEs)
/// - Leading comments
/// - Complex query structures
fn execute_sql_with_formatter<W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    use alopex_sql::{AlopexDialect, Parser, StatementKind};

    // FR-7: Use parser to detect SELECT instead of starts_with("SELECT") heuristic
    // This correctly handles WITH clauses, leading comments, and complex query structures
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| CliError::Parse(format!("{}", e)))?;

    let is_select = stmts.len() == 1
        && matches!(
            stmts.first().map(|s| &s.kind),
            Some(StatementKind::Select(_))
        );

    if is_select {
        // SELECT: use streaming path (FR-7)
        execute_sql_select_streaming(db, sql, writer, formatter, limit, quiet)
    } else {
        // DDL/DML: use standard path
        execute_sql_ddl_dml(db, sql, writer, formatter, limit, quiet)
    }
}

/// Execute SELECT query with streaming callback (FR-7).
///
/// This function uses `execute_sql_with_rows` for true streaming output.
/// The callback receives rows one at a time from the iterator, and the
/// transaction is kept alive during streaming.
fn execute_sql_select_streaming<W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    use alopex_embedded::StreamingQueryResult;

    // Helper to convert CliError to alopex_embedded::Error for callback
    fn cli_err_to_embedded(e: crate::error::CliError) -> alopex_embedded::Error {
        alopex_embedded::Error::Sql(alopex_sql::SqlError::Execution {
            message: e.to_string(),
            code: "ALOPEX-C001",
        })
    }

    let result = db.execute_sql_with_rows(sql, |mut rows| {
        // FR-7: SELECT result - stream rows directly from iterator while transaction is alive
        let columns = columns_from_streaming_rows(&rows);
        let mut streaming_writer =
            StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);

        // FR-7: Use None for row count hint to support true streaming output
        streaming_writer
            .prepare(None)
            .map_err(cli_err_to_embedded)?;

        // Consume iterator row by row for true streaming
        while let Ok(Some(sql_row)) = rows.next_row() {
            let values: Vec<Value> = sql_row.into_iter().map(sql_value_to_value).collect();
            let row = Row::new(values);

            match streaming_writer
                .write_row(row)
                .map_err(cli_err_to_embedded)?
            {
                WriteStatus::LimitReached => break,
                WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
            }
        }

        streaming_writer.finish().map_err(cli_err_to_embedded)?;
        Ok(())
    })?;

    match result {
        StreamingQueryResult::QueryProcessed(()) => Ok(()),
        StreamingQueryResult::Success | StreamingQueryResult::RowsAffected(_) => {
            // Unexpected: SELECT should not return these
            Ok(())
        }
    }
}

#[derive(serde::Serialize)]
struct RemoteSqlRequest {
    sql: String,
    #[serde(default)]
    streaming: bool,
}

#[derive(serde::Deserialize)]
struct RemoteColumnInfo {
    name: String,
    data_type: String,
}

#[derive(serde::Deserialize)]
struct RemoteSqlResponse {
    columns: Vec<RemoteColumnInfo>,
    rows: Vec<Vec<alopex_sql::storage::SqlValue>>,
    affected_rows: Option<u64>,
}

fn map_client_error(err: ClientError) -> CliError {
    match err {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) => CliError::InvalidArgument(message),
        ClientError::Build(message) => CliError::InvalidArgument(message),
        ClientError::Auth(err) => CliError::InvalidArgument(err.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::InvalidArgument(format!("Server error: HTTP {} - {}", status.as_u16(), body))
        }
    }
}

/// Execute DDL/DML query (non-SELECT statements).
///
/// This function handles CREATE, DROP, INSERT, UPDATE, DELETE and other
/// non-SELECT statements. It outputs status messages (OK, rows affected).
fn execute_sql_ddl_dml<W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    use alopex_sql::ExecutionResult;

    let result = db.execute_sql(sql)?;

    match result {
        ExecutionResult::Success => {
            // DDL success - suppress status output in quiet mode
            if !quiet {
                let columns = sql_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                streaming_writer.prepare(Some(1))?;
                let row = Row::new(vec![
                    Value::Text("OK".to_string()),
                    Value::Text("Operation completed successfully".to_string()),
                ]);
                streaming_writer.write_row(row)?;
                streaming_writer.finish()?;
            }
        }
        ExecutionResult::RowsAffected(count) => {
            // DML success - suppress status output in quiet mode
            if !quiet {
                let columns = sql_status_columns();
                let mut streaming_writer =
                    StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
                streaming_writer.prepare(Some(1))?;
                let row = Row::new(vec![
                    Value::Text("OK".to_string()),
                    Value::Text(format!("{} row(s) affected", count)),
                ]);
                streaming_writer.write_row(row)?;
                streaming_writer.finish()?;
            }
        }
        ExecutionResult::Query(query_result) => {
            // Unexpected: non-SELECT should not return Query result
            // But handle it gracefully by outputting the result
            let columns = columns_from_query_result(&query_result);
            let mut streaming_writer =
                StreamingWriter::new(writer, formatter, columns, limit).with_quiet(quiet);
            streaming_writer.prepare(Some(query_result.rows.len()))?;
            for sql_row in query_result.rows {
                let values: Vec<Value> = sql_row.into_iter().map(sql_value_to_value).collect();
                let row = Row::new(values);
                match streaming_writer.write_row(row)? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
                }
            }
            streaming_writer.finish()?;
        }
    }

    Ok(())
}

/// Convert alopex_sql::SqlValue to our Value type.
fn sql_value_to_value(sql_value: alopex_sql::SqlValue) -> Value {
    use alopex_sql::SqlValue;

    match sql_value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Int(i as i64),
        SqlValue::BigInt(i) => Value::Int(i),
        SqlValue::Float(f) => Value::Float(f as f64),
        SqlValue::Double(f) => Value::Float(f),
        SqlValue::Text(s) => Value::Text(s),
        SqlValue::Blob(b) => Value::Bytes(b),
        SqlValue::Boolean(b) => Value::Bool(b),
        SqlValue::Timestamp(ts) => {
            // Format timestamp as ISO 8601 string
            Value::Text(format!("{}", ts))
        }
        SqlValue::Vector(v) => Value::Vector(v),
    }
}

fn remote_value_to_value(sql_value: alopex_sql::storage::SqlValue) -> Value {
    use alopex_sql::storage::SqlValue;

    match sql_value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Int(i as i64),
        SqlValue::BigInt(i) => Value::Int(i),
        SqlValue::Float(f) => Value::Float(f as f64),
        SqlValue::Double(f) => Value::Float(f),
        SqlValue::Text(s) => Value::Text(s),
        SqlValue::Blob(b) => Value::Bytes(b),
        SqlValue::Boolean(b) => Value::Bool(b),
        SqlValue::Timestamp(ts) => Value::Text(ts.to_string()),
        SqlValue::Vector(v) => Value::Vector(v),
    }
}

fn data_type_from_string(value: &str) -> DataType {
    let upper = value.to_ascii_uppercase();
    if upper.starts_with("INT") || upper.starts_with("BIGINT") {
        DataType::Int
    } else if upper.starts_with("FLOAT") || upper.starts_with("DOUBLE") {
        DataType::Float
    } else if upper.starts_with("BLOB") {
        DataType::Bytes
    } else if upper.starts_with("BOOLEAN") {
        DataType::Bool
    } else if upper.starts_with("VECTOR") {
        DataType::Vector
    } else {
        DataType::Text
    }
}

/// Convert alopex_sql::executor::ColumnInfo to our Column type.
fn sql_column_to_column(col: &alopex_sql::executor::ColumnInfo) -> Column {
    use alopex_sql::planner::ResolvedType;

    let data_type = match &col.data_type {
        ResolvedType::Integer | ResolvedType::BigInt => DataType::Int,
        ResolvedType::Float | ResolvedType::Double => DataType::Float,
        ResolvedType::Text => DataType::Text,
        ResolvedType::Blob => DataType::Bytes,
        ResolvedType::Boolean => DataType::Bool,
        ResolvedType::Timestamp => DataType::Text, // Display as text
        ResolvedType::Vector { .. } => DataType::Vector,
        ResolvedType::Null => DataType::Text, // Fallback
    };

    Column::new(&col.name, data_type)
}

/// Create columns from SQL query result.
#[allow(dead_code)] // Used by tests with legacy execute_sql function
fn columns_from_query_result(query_result: &alopex_sql::executor::QueryResult) -> Vec<Column> {
    query_result
        .columns
        .iter()
        .map(sql_column_to_column)
        .collect()
}

/// Create columns from streaming query result iterator (FR-7).
#[allow(dead_code)] // Kept for potential future use with old streaming API
fn columns_from_streaming_result(
    query_iter: &alopex_embedded::QueryRowIterator<'_>,
) -> Vec<Column> {
    query_iter
        .columns()
        .iter()
        .map(sql_column_to_column)
        .collect()
}

/// Create columns from callback-based streaming rows (FR-7).
fn columns_from_streaming_rows(rows: &alopex_embedded::StreamingRows<'_>) -> Vec<Column> {
    rows.columns().iter().map(sql_column_to_column).collect()
}

/// Create columns for status output.
pub fn sql_status_columns() -> Vec<Column> {
    vec![
        Column::new("status", DataType::Text),
        Column::new("message", DataType::Text),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchModeSource;
    use crate::output::jsonl::JsonlFormatter;

    fn create_test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn create_status_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = sql_status_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_query_writer(
        output: &mut Vec<u8>,
        columns: Vec<Column>,
    ) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn default_batch_mode() -> BatchMode {
        BatchMode {
            is_batch: false,
            is_tty: true,
            source: BatchModeSource::Default,
        }
    }

    #[test]
    fn test_create_table() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_sql(
                &db,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
                &mut writer,
            )
            .unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("OK"));
    }

    #[test]
    fn test_insert_and_select() {
        let db = create_test_db();

        // Create table
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_sql(
                &db,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
                &mut writer,
            )
            .unwrap();
        }

        // Insert
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_sql(
                &db,
                "INSERT INTO users (id, name) VALUES (1, 'Alice');",
                &mut writer,
            )
            .unwrap();
            let result = String::from_utf8(output).unwrap();
            assert!(result.contains("row(s) affected"));
        }

        // Select - we need columns from the query result
        {
            let mut output = Vec::new();
            let columns = vec![
                Column::new("id", DataType::Int),
                Column::new("name", DataType::Text),
            ];
            let mut writer = create_query_writer(&mut output, columns);
            execute_sql(&db, "SELECT id, name FROM users;", &mut writer).unwrap();
            let result = String::from_utf8(output).unwrap();
            assert!(result.contains("Alice"));
        }
    }

    #[test]
    fn test_syntax_error() {
        let db = create_test_db();

        let mut output = Vec::new();
        let mut writer = create_status_writer(&mut output);
        let result = execute_sql(&db, "CREATE TABEL invalid_syntax;", &mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_statements() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_sql(
                &db,
                "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t (id) VALUES (1);",
                &mut writer,
            )
            .unwrap();
        }

        // Verify the table exists and has data
        {
            let mut output = Vec::new();
            let columns = vec![Column::new("id", DataType::Int)];
            let mut writer = create_query_writer(&mut output, columns);
            execute_sql(&db, "SELECT id FROM t;", &mut writer).unwrap();
            let result = String::from_utf8(output).unwrap();
            assert!(result.contains("1"));
        }
    }

    #[test]
    fn test_sql_value_conversion() {
        use alopex_sql::SqlValue;

        assert!(matches!(sql_value_to_value(SqlValue::Null), Value::Null));
        assert!(matches!(
            sql_value_to_value(SqlValue::Integer(42)),
            Value::Int(42)
        ));
        assert!(matches!(
            sql_value_to_value(SqlValue::BigInt(100)),
            Value::Int(100)
        ));
        assert!(matches!(
            sql_value_to_value(SqlValue::Boolean(true)),
            Value::Bool(true)
        ));
        assert!(
            matches!(sql_value_to_value(SqlValue::Text("hello".to_string())), Value::Text(s) if s == "hello")
        );
    }

    #[test]
    fn resolve_query_from_argument() {
        let cmd = SqlCommand {
            query: Some("SELECT 1".to_string()),
            file: None,
        };

        let sql = cmd.resolve_query(&default_batch_mode()).unwrap();
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn resolve_query_from_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "SELECT * FROM users").unwrap();

        let cmd = SqlCommand {
            query: None,
            file: Some(file.path().display().to_string()),
        };

        let sql = cmd.resolve_query(&default_batch_mode()).unwrap();
        assert_eq!(sql, "SELECT * FROM users\n");
    }

    #[test]
    fn resolve_query_returns_no_query_error() {
        let cmd = SqlCommand {
            query: None,
            file: None,
        };

        let err = cmd.resolve_query(&default_batch_mode()).unwrap_err();
        assert!(matches!(err, CliError::NoQueryProvided));
    }

    #[test]
    fn resolve_query_rejects_query_and_file() {
        let cmd = SqlCommand {
            query: Some("SELECT 1".to_string()),
            file: Some("query.sql".into()),
        };

        let err = cmd.resolve_query(&default_batch_mode()).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidArgument(msg) if msg == "Cannot specify both query and file"
        ));
    }
}
