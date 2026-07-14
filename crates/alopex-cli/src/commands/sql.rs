//! SQL Command - SQL query execution
//!
//! Supports: query execution, file-based queries

use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};

use alopex_embedded::Database;

use crate::batch::BatchMode;
use crate::cli::{OutputFormat, SqlCommand};
use crate::client::http::{ClientError, HttpClient};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};
use crate::streaming::timeout::parse_deadline;
use crate::streaming::{CancelSignal, Deadline, StreamingWriter, WriteStatus};
use crate::tui::{is_tty, TuiApp};
use crate::ui::mode::UiMode;
use futures_util::StreamExt;
use reqwest::Response;

#[doc(hidden)]
pub struct SqlExecutionOptions<'a> {
    pub limit: Option<usize>,
    pub quiet: bool,
    pub cancel: &'a CancelSignal,
    pub deadline: &'a Deadline,
    pub admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
}

/// Source of formatters for SQL result output.
///
/// The `sql` command emits one result block per statement, so a single
/// invocation may need more than one formatter instance.
enum SqlOutput<'a> {
    /// Create formatters from an output format. `OutputFormat::Json` also
    /// wraps the output in an array of per-statement result sets.
    Format(OutputFormat),
    /// Obtain formatters from a caller-supplied factory (e.g. the admin TUI
    /// capture formatter). No statement-array wrapping is applied.
    Custom(&'a mut dyn FnMut() -> Box<dyn Formatter>),
}

impl SqlOutput<'_> {
    /// Create a formatter for the next result block.
    fn create_block_formatter(&mut self) -> Box<dyn Formatter> {
        match self {
            SqlOutput::Format(format) => create_formatter(*format),
            SqlOutput::Custom(factory) => factory(),
        }
    }

    /// Whether output must be wrapped in an array of per-statement result sets.
    fn statement_array(&self) -> bool {
        matches!(self, SqlOutput::Format(OutputFormat::Json))
    }

    /// Whether the produced formatters support streaming output.
    fn supports_streaming(&mut self) -> bool {
        match self {
            SqlOutput::Format(format) => format.supports_streaming(),
            SqlOutput::Custom(factory) => factory().supports_streaming(),
        }
    }
}

/// Write adapter that records whether any bytes have been written.
///
/// Used by the JSON statement-array wrapping to decide, on error paths, how
/// many opened arrays must be closed to keep stdout well-formed JSON.
struct CountingWriter<W> {
    inner: W,
    bytes: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, bytes: 0 }
    }

    fn bytes_written(&self) -> u64 {
        self.bytes
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Execute a SQL command with dynamic column detection.
///
/// This function creates formatters internally based on the query result type,
/// ensuring that SELECT queries use the correct column headers.
///
/// # Output contract
///
/// - `--output json` emits an array of per-statement result sets; a single
///   statement yields a 1-element array. With `--quiet`, DDL/DML status
///   result sets are omitted, so input consisting only of such statements
///   yields an empty array.
/// - Other formats emit one result block per statement, in statement order.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The SQL command to execute.
/// * `writer` - The output writer.
/// * `output_format` - The output format to use.
/// * `limit` - Optional row limit.
/// * `quiet` - Whether to suppress warnings and status result sets.
#[allow(clippy::too_many_arguments)]
pub fn execute_with_formatter<'a, W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output_format: OutputFormat,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let deadline = Deadline::new(parse_deadline(cmd.deadline.as_deref())?);
    let cancel = CancelSignal::new();

    execute_with_formatter_control(
        db,
        cmd,
        batch_mode,
        ui_mode,
        writer,
        output_format,
        SqlExecutionOptions {
            limit,
            quiet,
            cancel: &cancel,
            deadline: &deadline,
            admin_launcher,
        },
    )
}

/// Execute a SQL command using formatters supplied by `make_formatter`.
///
/// One formatter is created per result block (a multi-statement input emits
/// one block per statement). No JSON statement-array wrapping is applied;
/// this entry point is intended for callers that capture results with a
/// custom formatter (e.g. the admin TUI).
#[allow(clippy::too_many_arguments)]
pub fn execute_with_formatter_factory<'a, W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    make_formatter: &mut dyn FnMut() -> Box<dyn Formatter>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let deadline = Deadline::new(parse_deadline(cmd.deadline.as_deref())?);
    let cancel = CancelSignal::new();
    let mut output = SqlOutput::Custom(make_formatter);

    execute_with_output_control(
        db,
        cmd,
        batch_mode,
        ui_mode,
        writer,
        &mut output,
        SqlExecutionOptions {
            limit,
            quiet,
            cancel: &cancel,
            deadline: &deadline,
            admin_launcher,
        },
    )
}

#[doc(hidden)]
pub fn execute_with_formatter_control<W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output_format: OutputFormat,
    options: SqlExecutionOptions<'_>,
) -> Result<()> {
    let mut output = SqlOutput::Format(output_format);
    execute_with_output_control(db, cmd, batch_mode, ui_mode, writer, &mut output, options)
}

fn execute_with_output_control<W: Write>(
    db: &Database,
    cmd: SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    mut options: SqlExecutionOptions<'_>,
) -> Result<()> {
    let sql = cmd.resolve_query(batch_mode)?;
    let effective_limit = merge_limit(options.limit, cmd.max_rows);
    options.limit = effective_limit;

    if ui_mode == UiMode::Tui {
        return execute_tui_local_or_fallback(db, &sql, writer, output, options);
    }

    execute_sql_with_formatter(db, &sql, writer, output, &options)
}

fn is_select_query(sql: &str) -> Result<bool> {
    use alopex_sql::{AlopexDialect, Parser, StatementKind};

    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| CliError::Parse(format!("{}", e)))?;
    Ok(stmts.len() == 1
        && matches!(
            stmts.first().map(|s| &s.kind),
            Some(StatementKind::Select(_))
        ))
}

/// Execute a SQL command against a remote server using HttpClient.
///
/// `--output json` emits an array of result sets (currently a single element,
/// matching the local output contract; server-side per-statement results are
/// tracked as issue #31).
#[allow(clippy::too_many_arguments)]
pub async fn execute_remote_with_formatter<'a, W: Write>(
    client: &HttpClient,
    cmd: &SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output_format: OutputFormat,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let effective_limit = merge_limit(limit, cmd.max_rows);
    let deadline = Deadline::new(parse_deadline(cmd.deadline.as_deref())?);
    let cancel = CancelSignal::new();
    let options = SqlExecutionOptions {
        limit: effective_limit,
        quiet,
        cancel: &cancel,
        deadline: &deadline,
        admin_launcher,
    };

    execute_remote_with_formatter_control(
        client,
        cmd,
        batch_mode,
        ui_mode,
        writer,
        output_format,
        options,
    )
    .await
}

fn sql_context_message(sql: &str) -> String {
    let condensed = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let max_len = 200;
    if condensed.chars().count() > max_len {
        let truncated: String = condensed.chars().take(max_len).collect();
        format!("SQL: {truncated}...")
    } else {
        format!("SQL: {condensed}")
    }
}

/// Execute a remote SQL command using formatters supplied by `make_formatter`.
///
/// See [`execute_with_formatter_factory`] for the output contract.
#[allow(clippy::too_many_arguments)]
pub async fn execute_remote_with_formatter_factory<'a, W: Write>(
    client: &HttpClient,
    cmd: &SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    make_formatter: &mut dyn FnMut() -> Box<dyn Formatter>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let effective_limit = merge_limit(limit, cmd.max_rows);
    let deadline = Deadline::new(parse_deadline(cmd.deadline.as_deref())?);
    let cancel = CancelSignal::new();
    let options = SqlExecutionOptions {
        limit: effective_limit,
        quiet,
        cancel: &cancel,
        deadline: &deadline,
        admin_launcher,
    };
    let mut output = SqlOutput::Custom(make_formatter);
    execute_remote_with_output_control(
        client,
        cmd,
        batch_mode,
        ui_mode,
        writer,
        &mut output,
        options,
    )
    .await
}

#[doc(hidden)]
pub async fn execute_remote_with_formatter_control<W: Write>(
    client: &HttpClient,
    cmd: &SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output_format: OutputFormat,
    options: SqlExecutionOptions<'_>,
) -> Result<()> {
    let mut output = SqlOutput::Format(output_format);
    execute_remote_with_output_control(
        client,
        cmd,
        batch_mode,
        ui_mode,
        writer,
        &mut output,
        options,
    )
    .await
}

async fn execute_remote_with_output_control<W: Write>(
    client: &HttpClient,
    cmd: &SqlCommand,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: SqlExecutionOptions<'_>,
) -> Result<()> {
    let sql = cmd.resolve_query(batch_mode)?;
    if ui_mode == UiMode::Tui {
        return execute_tui_remote_or_fallback(client, &sql, cmd, writer, output, options).await;
    }
    execute_remote_with_formatter_impl(client, &sql, cmd, writer, output, &options).await
}

async fn execute_remote_with_formatter_impl<W: Write>(
    client: &HttpClient,
    sql: &str,
    cmd: &SqlCommand,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    if is_select_query(sql)? && output.supports_streaming() {
        return execute_remote_streaming(
            client,
            sql,
            writer,
            output,
            options,
            cmd.fetch_size,
            cmd.max_rows,
        )
        .await;
    }

    let request = RemoteSqlRequest {
        sql: sql.to_string(),
        streaming: false,
        fetch_size: cmd.fetch_size,
        max_rows: cmd.max_rows,
    };
    let response: RemoteSqlResponse = tokio::select! {
        result = tokio::time::timeout(options.deadline.remaining(), client.post_json("api/sql/query", &request)) => {
            match result {
                Ok(value) => value.map_err(map_client_error)?,
                Err(_) => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Timeout(format!(
                        "deadline exceeded after {}",
                        humantime::format_duration(options.deadline.duration())
                    )));
                }
            }
        }
        _ = options.cancel.wait() => {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
    };

    // JSON output is an array of result sets; the remote API currently
    // returns a single result set, so the array has one element (server-side
    // per-statement results are tracked as issue #31).
    let json_array = output.statement_array();

    if response.columns.is_empty() {
        if options.quiet {
            if json_array {
                writeln!(writer, "[")?;
                writeln!(writer, "]")?;
            }
            return Ok(());
        }
        let message = match response.affected_rows {
            Some(count) => format!("{count} row(s) affected"),
            None => "Operation completed successfully".to_string(),
        };
        if json_array {
            writeln!(writer, "[")?;
        }
        let columns = sql_status_columns();
        {
            let mut streaming_writer = StreamingWriter::new(
                &mut *writer,
                output.create_block_formatter(),
                columns,
                options.limit,
            )
            .with_quiet(options.quiet);
            streaming_writer.prepare(Some(1))?;
            let row = Row::new(vec![Value::Text("OK".to_string()), Value::Text(message)]);
            streaming_writer.write_row(row)?;
            streaming_writer.finish()?;
        }
        if json_array {
            writeln!(writer, "]")?;
        }
        return Ok(());
    }

    let columns: Vec<Column> = response
        .columns
        .iter()
        .map(|col| Column::new(&col.name, data_type_from_string(&col.data_type)))
        .collect();
    if json_array {
        writeln!(writer, "[")?;
    }
    {
        let mut streaming_writer = StreamingWriter::new(
            &mut *writer,
            output.create_block_formatter(),
            columns,
            options.limit,
        )
        .with_quiet(options.quiet);
        streaming_writer.prepare(Some(response.rows.len()))?;
        for row in response.rows {
            if options.cancel.is_cancelled() {
                let _ = send_cancel_request(client).await;
                return Err(CliError::Cancelled);
            }
            options.deadline.check()?;
            let values = row.into_iter().map(remote_value_to_value).collect();
            match streaming_writer.write_row(Row::new(values))? {
                WriteStatus::LimitReached => break,
                WriteStatus::Continue => {}
            }
        }
        streaming_writer.finish()?;
    }
    if json_array {
        writeln!(writer, "]")?;
    }
    Ok(())
}

fn execute_tui_local_or_fallback<'a, W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    mut options: SqlExecutionOptions<'a>,
) -> Result<()> {
    if !is_tty() {
        if !options.quiet {
            eprintln!("Warning: --tui requires a TTY, falling back to batch output.");
        }
        return execute_sql_with_formatter(db, sql, writer, output, &options);
    }

    let admin_launcher = options.admin_launcher.take();
    match execute_tui_local(db, sql, &options, admin_launcher) {
        Ok(()) => Ok(()),
        Err(err) => {
            if !options.quiet {
                eprintln!("Warning: TUI failed ({err}); falling back to batch output.");
            }
            execute_sql_with_formatter(db, sql, writer, output, &options)
        }
    }
}

fn execute_tui_local<'a>(
    db: &Database,
    sql: &str,
    options: &SqlExecutionOptions<'a>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
) -> Result<()> {
    use alopex_sql::ExecutionResult;

    options.deadline.check()?;
    let result = db.execute_sql(sql)?;
    options.deadline.check()?;

    let (columns, rows) = match result {
        ExecutionResult::Success => {
            let columns = sql_status_columns();
            let row = Row::new(vec![
                Value::Text("OK".to_string()),
                Value::Text("Operation completed successfully".to_string()),
            ]);
            (columns, vec![row])
        }
        ExecutionResult::RowsAffected(count) => {
            let columns = sql_status_columns();
            let row = Row::new(vec![
                Value::Text("OK".to_string()),
                Value::Text(format!("{count} row(s) affected")),
            ]);
            (columns, vec![row])
        }
        ExecutionResult::Query(query_result) => {
            let columns = query_result
                .columns
                .iter()
                .map(|col| Column::new(&col.name, DataType::Text))
                .collect::<Vec<_>>();
            let mut rows = Vec::with_capacity(query_result.rows.len());
            for sql_row in query_result.rows {
                let values = sql_row.into_iter().map(sql_value_to_value).collect();
                rows.push(Row::new(values));
            }
            if let Some(limit) = options.limit {
                rows.truncate(limit);
            }
            (columns, rows)
        }
    };

    let app = TuiApp::new(columns, rows, "local", false)
        .with_context_message(Some(sql_context_message(sql)))
        .with_admin_launcher(admin_launcher);
    app.run()
}

async fn execute_tui_remote_or_fallback<'a, W: Write>(
    client: &HttpClient,
    sql: &str,
    cmd: &SqlCommand,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    mut options: SqlExecutionOptions<'a>,
) -> Result<()> {
    if !is_tty() {
        if !options.quiet {
            eprintln!("Warning: --tui requires a TTY, falling back to batch output.");
        }
        return execute_remote_with_formatter_impl(client, sql, cmd, writer, output, &options)
            .await;
    }

    let admin_launcher = options.admin_launcher.take();
    match execute_tui_remote(client, sql, cmd, &options, admin_launcher).await {
        Ok(()) => Ok(()),
        Err(err) => {
            if !options.quiet {
                eprintln!("Warning: TUI failed ({err}); falling back to batch output.");
            }
            execute_remote_with_formatter_impl(client, sql, cmd, writer, output, &options).await
        }
    }
}

async fn execute_tui_remote<'a>(
    client: &HttpClient,
    sql: &str,
    cmd: &SqlCommand,
    options: &SqlExecutionOptions<'a>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
) -> Result<()> {
    if is_select_query(sql)? {
        let (columns, rows) =
            collect_remote_streaming_rows(client, sql, options, cmd.fetch_size, cmd.max_rows)
                .await?;
        let app = TuiApp::new(columns, rows, "server", false)
            .with_context_message(Some(sql_context_message(sql)))
            .with_admin_launcher(admin_launcher);
        return app.run();
    }

    let request = RemoteSqlRequest {
        sql: sql.to_string(),
        streaming: false,
        fetch_size: cmd.fetch_size,
        max_rows: cmd.max_rows,
    };

    let response: RemoteSqlResponse = tokio::select! {
        result = tokio::time::timeout(options.deadline.remaining(), client.post_json("api/sql/query", &request)) => {
            match result {
                Ok(value) => value.map_err(map_client_error)?,
                Err(_) => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Timeout(format!(
                        "deadline exceeded after {}",
                        humantime::format_duration(options.deadline.duration())
                    )));
                }
            }
        }
        _ = options.cancel.wait() => {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
    };

    let (columns, rows) = if response.columns.is_empty() {
        let columns = sql_status_columns();
        let message = match response.affected_rows {
            Some(count) => format!("{count} row(s) affected"),
            None => "Operation completed successfully".to_string(),
        };
        let row = Row::new(vec![Value::Text("OK".to_string()), Value::Text(message)]);
        (columns, vec![row])
    } else {
        let columns: Vec<Column> = response
            .columns
            .iter()
            .map(|col| Column::new(&col.name, data_type_from_string(&col.data_type)))
            .collect();
        let mut rows = response
            .rows
            .into_iter()
            .map(|row| Row::new(row.into_iter().map(remote_value_to_value).collect()))
            .collect::<Vec<_>>();
        if let Some(limit) = options.limit {
            rows.truncate(limit);
        }
        (columns, rows)
    };

    let app = TuiApp::new(columns, rows, "server", false)
        .with_context_message(Some(sql_context_message(sql)))
        .with_admin_launcher(admin_launcher);
    app.run()
}

async fn collect_remote_streaming_rows(
    client: &HttpClient,
    sql: &str,
    options: &SqlExecutionOptions<'_>,
    fetch_size: Option<usize>,
    max_rows: Option<usize>,
) -> Result<(Vec<Column>, Vec<Row>)> {
    let request = RemoteSqlRequest {
        sql: sql.to_string(),
        streaming: true,
        fetch_size,
        max_rows,
    };

    let response = tokio::select! {
        result = tokio::time::timeout(options.deadline.remaining(), client.post_json_stream("api/sql/query", &request)) => {
            match result {
                Ok(value) => value.map_err(map_client_error)?,
                Err(_) => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Timeout(format!(
                        "deadline exceeded after {}",
                        humantime::format_duration(options.deadline.duration())
                    )));
                }
            }
        }
        _ = options.cancel.wait() => {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
    };

    if let Some(content_type) = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        if content_type.starts_with("application/jsonl") {
            return collect_remote_jsonl_rows(client, response, options).await;
        }
    }

    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut pos: usize = 0;
    let mut done = false;
    let mut saw_array_start = false;
    let mut columns: Option<Vec<String>> = None;
    let mut column_set: Option<HashSet<String>> = None;
    let mut rows: Vec<Row> = Vec::new();

    while !done {
        if options.cancel.is_cancelled() {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
        if let Err(err) = options.deadline.check() {
            let _ = send_cancel_request(client).await;
            return Err(err);
        }

        let next = tokio::select! {
            _ = options.cancel.wait() => {
                let _ = send_cancel_request(client).await;
                return Err(CliError::Cancelled);
            }
            result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = send_cancel_request(client).await;
                        return Err(CliError::Timeout(format!(
                            "deadline exceeded after {}",
                            humantime::format_duration(options.deadline.duration())
                        )));
                    }
                }
            }
        };

        let chunk = match next {
            Some(chunk) => chunk,
            None => break,
        };

        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(err) => return Err(CliError::ServerConnection(format!("request failed: {err}"))),
        };

        buffer.extend_from_slice(&bytes);

        loop {
            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }

            if !saw_array_start {
                if buffer[pos] != b'[' {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming response: expected JSON array".into(),
                    ));
                }
                pos += 1;
                saw_array_start = true;
                continue;
            }

            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }

            if buffer[pos] == b']' {
                pos += 1;
                done = true;
                break;
            }

            let slice = &buffer[pos..];
            let mut stream =
                serde_json::Deserializer::from_slice(slice).into_iter::<serde_json::Value>();
            let value = match stream.next() {
                Some(Ok(value)) => value,
                Some(Err(err)) if err.is_eof() => break,
                Some(Err(err)) => return Err(CliError::Json(err)),
                None => break,
            };
            pos = pos.saturating_add(stream.byte_offset());

            let object = value.as_object().ok_or_else(|| {
                CliError::InvalidArgument("Invalid streaming row: expected JSON object".into())
            })?;

            if columns.is_none() {
                let names: Vec<String> = object.keys().cloned().collect();
                let set: HashSet<String> = names.iter().cloned().collect();
                if names.is_empty() {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming row: empty object".into(),
                    ));
                }
                columns = Some(names);
                column_set = Some(set);
            }

            let names = columns
                .as_ref()
                .ok_or_else(|| CliError::InvalidArgument("Missing columns".into()))?;
            let set = column_set
                .as_ref()
                .ok_or_else(|| CliError::InvalidArgument("Missing column set".into()))?;

            if object.len() != names.len() || !object.keys().all(|key| set.contains(key)) {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming row: column mismatch".into(),
                ));
            }

            let values = names
                .iter()
                .map(|name| {
                    object.get(name).ok_or_else(|| {
                        CliError::InvalidArgument(format!(
                            "Invalid streaming row: missing column '{name}'"
                        ))
                    })
                })
                .map(|value| value.and_then(json_value_to_value))
                .collect::<Result<Vec<_>>>()?;
            rows.push(Row::new(values));

            if let Some(limit) = options.limit {
                if rows.len() >= limit {
                    let _ = send_cancel_request(client).await;
                    done = true;
                    break;
                }
            }

            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }
            match buffer[pos] {
                b',' => {
                    pos += 1;
                }
                b']' => {
                    pos += 1;
                    done = true;
                    break;
                }
                _ => {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming response: expected ',' or ']'".into(),
                    ))
                }
            }
        }

        if pos > 0 {
            buffer.drain(..pos);
            pos = 0;
        }
    }

    if done {
        if has_non_whitespace(&buffer) {
            return Err(CliError::InvalidArgument(
                "Invalid streaming response: unexpected trailing data".into(),
            ));
        }
        buffer.clear();
        loop {
            let next = tokio::select! {
                _ = options.cancel.wait() => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Cancelled);
                }
                result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                    match result {
                        Ok(value) => value,
                        Err(_) => {
                            let _ = send_cancel_request(client).await;
                            return Err(CliError::Timeout(format!(
                                "deadline exceeded after {}",
                                humantime::format_duration(options.deadline.duration())
                            )));
                        }
                    }
                }
            };

            let chunk = match next {
                Some(chunk) => chunk,
                None => break,
            };

            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Err(CliError::ServerConnection(format!("request failed: {err}")))
                }
            };

            if has_non_whitespace(&bytes) {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming response: unexpected trailing data".into(),
                ));
            }
        }
    } else {
        skip_whitespace(&buffer, &mut pos);
        if pos < buffer.len() {
            return Err(CliError::InvalidArgument(
                "Invalid streaming response: unexpected trailing data".into(),
            ));
        }
        return Err(CliError::InvalidArgument(
            "Invalid streaming response: unexpected end of stream".into(),
        ));
    }

    let columns = columns
        .unwrap_or_default()
        .into_iter()
        .map(|name| Column::new(name, DataType::Text))
        .collect();
    Ok((columns, rows))
}

#[allow(dead_code)]
async fn collect_remote_non_streaming_rows(
    client: &HttpClient,
    sql: &str,
    options: &SqlExecutionOptions<'_>,
    fetch_size: Option<usize>,
    max_rows: Option<usize>,
) -> Result<(Vec<Column>, Vec<Row>)> {
    let request = RemoteSqlRequest {
        sql: sql.to_string(),
        streaming: false,
        fetch_size,
        max_rows,
    };
    let response: RemoteSqlResponse = tokio::select! {
        result = tokio::time::timeout(options.deadline.remaining(), client.post_json("api/sql/query", &request)) => {
            match result {
                Ok(value) => value.map_err(map_client_error)?,
                Err(_) => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Timeout(format!(
                        "deadline exceeded after {}",
                        humantime::format_duration(options.deadline.duration())
                    )));
                }
            }
        }
        _ = options.cancel.wait() => {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
    };

    if response.columns.is_empty() {
        let message = match response.affected_rows {
            Some(count) => format!("{count} row(s) affected"),
            None => "Operation completed successfully".to_string(),
        };
        let columns = sql_status_columns();
        let row = Row::new(vec![Value::Text("OK".to_string()), Value::Text(message)]);
        return Ok((columns, vec![row]));
    }

    let columns = response
        .columns
        .iter()
        .map(|col| Column::new(&col.name, data_type_from_string(&col.data_type)))
        .collect::<Vec<_>>();
    let mut rows = response
        .rows
        .into_iter()
        .map(|row| Row::new(row.into_iter().map(remote_value_to_value).collect()))
        .collect::<Vec<_>>();
    if let Some(limit) = options.limit {
        rows.truncate(limit);
    }
    Ok((columns, rows))
}

async fn execute_remote_streaming<W: Write>(
    client: &HttpClient,
    sql: &str,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
    fetch_size: Option<usize>,
    max_rows: Option<usize>,
) -> Result<()> {
    let request = RemoteSqlRequest {
        sql: sql.to_string(),
        streaming: true,
        fetch_size,
        max_rows,
    };

    let response = tokio::select! {
        result = tokio::time::timeout(options.deadline.remaining(), client.post_json_stream("api/sql/query", &request)) => {
            match result {
                Ok(value) => value.map_err(map_client_error)?,
                Err(_) => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Timeout(format!(
                        "deadline exceeded after {}",
                        humantime::format_duration(options.deadline.duration())
                    )));
                }
            }
        }
        _ = options.cancel.wait() => {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
    };

    // JSON output is an array of result sets; the remote streaming API
    // currently yields a single result set, so the array has one element
    // (server-side per-statement results are tracked as issue #31).
    if !output.statement_array() {
        return stream_remote_result_set(
            client,
            response,
            writer,
            output.create_block_formatter(),
            options,
        )
        .await;
    }

    writeln!(writer, "[")?;
    let mut result_set_writer = CountingWriter::new(&mut *writer);
    let result = stream_remote_result_set(
        client,
        response,
        &mut result_set_writer,
        output.create_block_formatter(),
        options,
    )
    .await;
    let result_set_started = result_set_writer.bytes_written() > 0;
    match result {
        Ok(()) => {
            writeln!(writer, "]")?;
            Ok(())
        }
        Err(err) => {
            // Keep stdout well-formed JSON on stream errors: rows are always
            // written as complete objects, so closing the result-set array
            // (when it was opened) and the statement array leaves valid JSON
            // alongside the non-zero exit code.
            if result_set_started {
                let _ = writeln!(writer, "]");
            }
            let _ = writeln!(writer, "]");
            Err(err)
        }
    }
}

/// Stream a single remote result set (JSON array or JSONL body) to `writer`.
async fn stream_remote_result_set<W: Write>(
    client: &HttpClient,
    response: Response,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    if let Some(content_type) = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        if content_type.starts_with("application/jsonl") {
            return execute_remote_jsonl_streaming(client, response, writer, formatter, options)
                .await;
        }
    }

    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut pos: usize = 0;
    let mut streaming_writer: Option<StreamingWriter<&mut W>> = None;
    let mut formatter = Some(formatter);
    let mut columns: Option<Vec<String>> = None;
    let mut column_set: Option<HashSet<String>> = None;
    let mut done = false;
    let mut saw_array_start = false;

    while !done {
        if options.cancel.is_cancelled() {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
        if let Err(err) = options.deadline.check() {
            let _ = send_cancel_request(client).await;
            return Err(err);
        }

        let next = tokio::select! {
            _ = options.cancel.wait() => {
                let _ = send_cancel_request(client).await;
                return Err(CliError::Cancelled);
            }
            result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = send_cancel_request(client).await;
                        return Err(CliError::Timeout(format!(
                            "deadline exceeded after {}",
                            humantime::format_duration(options.deadline.duration())
                        )));
                    }
                }
            }
        };

        let chunk = match next {
            Some(chunk) => chunk,
            None => break,
        };

        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(err) => return Err(CliError::ServerConnection(format!("request failed: {err}"))),
        };

        buffer.extend_from_slice(&bytes);

        loop {
            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }

            if !saw_array_start {
                if buffer[pos] != b'[' {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming response: expected JSON array".into(),
                    ));
                }
                pos += 1;
                saw_array_start = true;
                continue;
            }

            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }

            if buffer[pos] == b']' {
                pos += 1;
                done = true;
                break;
            }

            let slice = &buffer[pos..];
            let mut stream =
                serde_json::Deserializer::from_slice(slice).into_iter::<serde_json::Value>();
            let value = match stream.next() {
                Some(Ok(value)) => value,
                Some(Err(err)) if err.is_eof() => break,
                Some(Err(err)) => return Err(CliError::Json(err)),
                None => break,
            };
            pos = pos.saturating_add(stream.byte_offset());

            let object = value.as_object().ok_or_else(|| {
                CliError::InvalidArgument("Invalid streaming row: expected JSON object".into())
            })?;

            if columns.is_none() {
                let names: Vec<String> = object.keys().cloned().collect();
                let set: HashSet<String> = names.iter().cloned().collect();
                if names.is_empty() {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming row: empty object".into(),
                    ));
                }
                let cols = names
                    .iter()
                    .map(|name| Column::new(name, DataType::Text))
                    .collect::<Vec<_>>();
                let formatter = formatter
                    .take()
                    .ok_or_else(|| CliError::InvalidArgument("Missing formatter".into()))?;
                let mut writer = StreamingWriter::new(&mut *writer, formatter, cols, options.limit)
                    .with_quiet(options.quiet);
                writer.prepare(None)?;
                streaming_writer = Some(writer);
                columns = Some(names);
                column_set = Some(set);
            }

            let names = columns
                .as_ref()
                .ok_or_else(|| CliError::InvalidArgument("Missing columns".into()))?;
            let set = column_set
                .as_ref()
                .ok_or_else(|| CliError::InvalidArgument("Missing column set".into()))?;

            if object.len() != names.len() || !object.keys().all(|key| set.contains(key)) {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming row: column mismatch".into(),
                ));
            }

            let values = names
                .iter()
                .map(|name| {
                    object.get(name).ok_or_else(|| {
                        CliError::InvalidArgument(format!(
                            "Invalid streaming row: missing column '{name}'"
                        ))
                    })
                })
                .map(|value| value.and_then(json_value_to_value))
                .collect::<Result<Vec<_>>>()?;

            if let Some(writer) = streaming_writer.as_mut() {
                match writer.write_row(Row::new(values))? {
                    WriteStatus::LimitReached => {
                        let _ = send_cancel_request(client).await;
                        return writer.finish();
                    }
                    WriteStatus::Continue => {}
                }
            }

            skip_whitespace(&buffer, &mut pos);
            if pos >= buffer.len() {
                break;
            }
            match buffer[pos] {
                b',' => {
                    pos += 1;
                }
                b']' => {
                    pos += 1;
                    done = true;
                    break;
                }
                _ => {
                    return Err(CliError::InvalidArgument(
                        "Invalid streaming response: expected ',' or ']'".into(),
                    ))
                }
            }
        }

        if pos > 0 {
            buffer.drain(..pos);
            pos = 0;
        }
    }

    if done {
        if has_non_whitespace(&buffer) {
            return Err(CliError::InvalidArgument(
                "Invalid streaming response: unexpected trailing data".into(),
            ));
        }
        buffer.clear();
        loop {
            let next = tokio::select! {
                _ = options.cancel.wait() => {
                    let _ = send_cancel_request(client).await;
                    return Err(CliError::Cancelled);
                }
                result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                    match result {
                        Ok(value) => value,
                        Err(_) => {
                            let _ = send_cancel_request(client).await;
                            return Err(CliError::Timeout(format!(
                                "deadline exceeded after {}",
                                humantime::format_duration(options.deadline.duration())
                            )));
                        }
                    }
                }
            };

            let chunk = match next {
                Some(chunk) => chunk,
                None => break,
            };

            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Err(CliError::ServerConnection(format!("request failed: {err}")))
                }
            };

            if has_non_whitespace(&bytes) {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming response: unexpected trailing data".into(),
                ));
            }
        }
    } else {
        skip_whitespace(&buffer, &mut pos);
        if pos < buffer.len() {
            return Err(CliError::InvalidArgument(
                "Invalid streaming response: unexpected trailing data".into(),
            ));
        }
        return Err(CliError::InvalidArgument(
            "Invalid streaming response: unexpected end of stream".into(),
        ));
    }

    if let Some(mut writer) = streaming_writer {
        return writer.finish();
    }

    if done && saw_array_start {
        if let Some(formatter) = formatter.take() {
            let mut writer =
                StreamingWriter::new(&mut *writer, formatter, Vec::new(), options.limit)
                    .with_quiet(options.quiet);
            writer.prepare(None)?;
            return writer.finish();
        }
    }

    Ok(())
}

async fn collect_remote_jsonl_rows(
    client: &HttpClient,
    response: Response,
    options: &SqlExecutionOptions<'_>,
) -> Result<(Vec<Column>, Vec<Row>)> {
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut columns: Option<Vec<Column>> = None;
    let mut rows: Vec<Row> = Vec::new();
    let mut done = false;

    while !done {
        if options.cancel.is_cancelled() {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
        if let Err(err) = options.deadline.check() {
            let _ = send_cancel_request(client).await;
            return Err(err);
        }

        let next = tokio::select! {
            _ = options.cancel.wait() => {
                let _ = send_cancel_request(client).await;
                return Err(CliError::Cancelled);
            }
            result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = send_cancel_request(client).await;
                        return Err(CliError::Timeout(format!(
                            "deadline exceeded after {}",
                            humantime::format_duration(options.deadline.duration())
                        )));
                    }
                }
            }
        };

        let chunk = match next {
            Some(chunk) => chunk,
            None => break,
        };

        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(err) => return Err(CliError::ServerConnection(format!("request failed: {err}"))),
        };

        buffer.extend_from_slice(&bytes);

        while let Some(newline) = buffer.iter().position(|&b| b == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<u8>>();
            if let Some(item) = parse_jsonl_line(&line)? {
                if let Some(error) = item.error {
                    return Err(CliError::InvalidArgument(format!(
                        "Server error: {}",
                        error.message
                    )));
                }
                if item.done {
                    done = true;
                    break;
                }
                let row = item.row.ok_or_else(|| {
                    CliError::InvalidArgument("Invalid streaming response: missing row".into())
                })?;
                if columns.is_none() {
                    columns = Some(default_stream_columns(row.len()));
                }
                rows.push(Row::new(
                    row.into_iter().map(remote_value_to_value).collect(),
                ));
                if let Some(limit) = options.limit {
                    if rows.len() >= limit {
                        let _ = send_cancel_request(client).await;
                        done = true;
                        break;
                    }
                }
            }
        }
    }

    if !done {
        if let Some(item) = parse_jsonl_line(&buffer)? {
            if let Some(error) = item.error {
                return Err(CliError::InvalidArgument(format!(
                    "Server error: {}",
                    error.message
                )));
            }
            if item.done {
                done = true;
            } else if let Some(row) = item.row {
                if columns.is_none() {
                    columns = Some(default_stream_columns(row.len()));
                }
                rows.push(Row::new(
                    row.into_iter().map(remote_value_to_value).collect(),
                ));
                if let Some(limit) = options.limit {
                    if rows.len() >= limit {
                        let _ = send_cancel_request(client).await;
                        done = true;
                    }
                }
            } else {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming response: missing row".into(),
                ));
            }
        }
    }

    if !done {
        return Err(CliError::InvalidArgument(
            "Invalid streaming response: unexpected end of stream".into(),
        ));
    }

    Ok((columns.unwrap_or_default(), rows))
}

async fn execute_remote_jsonl_streaming<W: Write>(
    client: &HttpClient,
    response: Response,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut streaming_writer: Option<StreamingWriter<&mut W>> = None;
    let mut formatter = Some(formatter);
    let mut done = false;

    while !done {
        if options.cancel.is_cancelled() {
            let _ = send_cancel_request(client).await;
            return Err(CliError::Cancelled);
        }
        if let Err(err) = options.deadline.check() {
            let _ = send_cancel_request(client).await;
            return Err(err);
        }

        let next = tokio::select! {
            _ = options.cancel.wait() => {
                let _ = send_cancel_request(client).await;
                return Err(CliError::Cancelled);
            }
            result = tokio::time::timeout(options.deadline.remaining(), stream.next()) => {
                match result {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = send_cancel_request(client).await;
                        return Err(CliError::Timeout(format!(
                            "deadline exceeded after {}",
                            humantime::format_duration(options.deadline.duration())
                        )));
                    }
                }
            }
        };

        let chunk = match next {
            Some(chunk) => chunk,
            None => break,
        };

        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(err) => return Err(CliError::ServerConnection(format!("request failed: {err}"))),
        };

        buffer.extend_from_slice(&bytes);

        while let Some(newline) = buffer.iter().position(|&b| b == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<u8>>();
            if let Some(item) = parse_jsonl_line(&line)? {
                if let Some(error) = item.error {
                    return Err(CliError::InvalidArgument(format!(
                        "Server error: {}",
                        error.message
                    )));
                }
                if item.done {
                    done = true;
                    break;
                }
                let row = item.row.ok_or_else(|| {
                    CliError::InvalidArgument("Invalid streaming response: missing row".into())
                })?;
                if streaming_writer.is_none() {
                    let columns = default_stream_columns(row.len());
                    let formatter = formatter
                        .take()
                        .ok_or_else(|| CliError::InvalidArgument("Missing formatter".into()))?;
                    let mut writer =
                        StreamingWriter::new(&mut *writer, formatter, columns, options.limit)
                            .with_quiet(options.quiet);
                    writer.prepare(None)?;
                    streaming_writer = Some(writer);
                }
                let values = row.into_iter().map(remote_value_to_value).collect();
                if let Some(writer) = streaming_writer.as_mut() {
                    match writer.write_row(Row::new(values))? {
                        WriteStatus::LimitReached => {
                            let _ = send_cancel_request(client).await;
                            done = true;
                            break;
                        }
                        WriteStatus::Continue => {}
                    }
                }
            }
        }
    }

    if !done {
        if let Some(item) = parse_jsonl_line(&buffer)? {
            if let Some(error) = item.error {
                return Err(CliError::InvalidArgument(format!(
                    "Server error: {}",
                    error.message
                )));
            }
            if item.done {
                done = true;
            } else if let Some(row) = item.row {
                if streaming_writer.is_none() {
                    let columns = default_stream_columns(row.len());
                    let formatter = formatter
                        .take()
                        .ok_or_else(|| CliError::InvalidArgument("Missing formatter".into()))?;
                    let mut writer =
                        StreamingWriter::new(&mut *writer, formatter, columns, options.limit)
                            .with_quiet(options.quiet);
                    writer.prepare(None)?;
                    streaming_writer = Some(writer);
                }
                let values = row.into_iter().map(remote_value_to_value).collect();
                if let Some(writer) = streaming_writer.as_mut() {
                    match writer.write_row(Row::new(values))? {
                        WriteStatus::LimitReached => {
                            let _ = send_cancel_request(client).await;
                            done = true;
                        }
                        WriteStatus::Continue => {}
                    }
                }
            } else {
                return Err(CliError::InvalidArgument(
                    "Invalid streaming response: missing row".into(),
                ));
            }
        }
    }

    if !done {
        return Err(CliError::InvalidArgument(
            "Invalid streaming response: unexpected end of stream".into(),
        ));
    }

    if let Some(mut writer) = streaming_writer {
        return writer.finish();
    }

    if let Some(formatter) = formatter.take() {
        let mut writer = StreamingWriter::new(&mut *writer, formatter, Vec::new(), options.limit)
            .with_quiet(options.quiet);
        writer.prepare(None)?;
        return writer.finish();
    }

    Ok(())
}

fn default_stream_columns(count: usize) -> Vec<Column> {
    (0..count)
        .map(|idx| Column::new(format!("col{}", idx + 1), DataType::Text))
        .collect()
}

fn parse_jsonl_line(line: &[u8]) -> Result<Option<RemoteStreamItem>> {
    if line
        .iter()
        .all(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
    {
        return Ok(None);
    }
    let text = std::str::from_utf8(line)
        .map_err(|err| CliError::InvalidArgument(format!("Invalid UTF-8: {err}")))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let item = serde_json::from_str::<RemoteStreamItem>(trimmed)?;
    Ok(Some(item))
}

fn skip_whitespace(buffer: &[u8], pos: &mut usize) {
    while *pos < buffer.len() {
        match buffer[*pos] {
            b' ' | b'\n' | b'\r' | b'\t' => *pos += 1,
            _ => break,
        }
    }
}

fn has_non_whitespace(buffer: &[u8]) -> bool {
    buffer
        .iter()
        .any(|byte| !matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
}

fn json_value_to_value(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(CliError::InvalidArgument(
                    "Invalid numeric value in streaming row".into(),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Value::Text(value.clone())),
        serde_json::Value::Array(values) => {
            let mut vector = Vec::with_capacity(values.len());
            for entry in values {
                let number = entry.as_f64().ok_or_else(|| {
                    CliError::InvalidArgument("Invalid vector value in streaming row".into())
                })?;
                vector.push(number as f32);
            }
            Ok(Value::Vector(vector))
        }
        serde_json::Value::Object(_) => Err(CliError::InvalidArgument(
            "Invalid streaming row: nested objects are not supported".into(),
        )),
    }
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
                    WriteStatus::Continue => {}
                }
            }

            writer.finish()?;
        }
    }

    Ok(())
}

/// Execute SQL locally, emitting one result block per statement.
///
/// - A single SELECT statement uses the streaming path (FR-7).
/// - Everything else (DDL/DML and multi-statement input) executes all
///   statements in one transaction via [`Database::execute_sql_multi`] and
///   emits each statement's result in order.
/// - For `--output json` the output is always an array of per-statement
///   result sets (a single statement yields a 1-element array).
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
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    use alopex_sql::{AlopexDialect, Parser, StatementKind};

    // FR-7: Use parser to detect SELECT instead of starts_with("SELECT") heuristic
    // This correctly handles WITH clauses, leading comments, and complex query structures
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| CliError::Parse(format!("{}", e)))?;

    let is_single_select = stmts.len() == 1
        && matches!(
            stmts.first().map(|s| &s.kind),
            Some(StatementKind::Select(_))
        );

    if is_single_select {
        // Single SELECT: use streaming path (FR-7).
        let formatter = output.create_block_formatter();
        if !output.statement_array() {
            return execute_sql_select_streaming(db, sql, writer, formatter, options, false);
        }
        let mut result_set_writer = CountingWriter::new(&mut *writer);
        let result =
            execute_sql_select_streaming(db, sql, &mut result_set_writer, formatter, options, true);
        let output_started = result_set_writer.bytes_written() > 0;
        match result {
            Ok(()) => {
                // Close the statement-result array opened by the streaming callback.
                writeln!(writer, "]")?;
                Ok(())
            }
            Err(err) => {
                // Keep stdout well-formed JSON on mid-stream errors: the
                // callback opens the statement array and the result-set array
                // together before writing rows (always complete objects), so
                // closing both leaves valid JSON alongside the non-zero exit
                // code. If nothing was written (e.g. planning failed), there
                // is nothing to close.
                if output_started {
                    let _ = writeln!(writer, "]");
                    let _ = writeln!(writer, "]");
                }
                Err(err)
            }
        }
    } else {
        // DDL/DML and multi-statement input: emit one result block per statement.
        execute_sql_statements(db, sql, writer, output, options)
    }
}

/// Execute SELECT query with streaming callback (FR-7).
///
/// This function uses `execute_sql_with_rows` for true streaming output.
/// The callback receives rows one at a time from the iterator, and the
/// transaction is kept alive during streaming.
///
/// When `open_statement_array` is true (JSON output), the callback writes the
/// opening `[` of the statement-result array before streaming; the caller is
/// responsible for writing the closing `]` after this function returns.
fn execute_sql_select_streaming<W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
    options: &SqlExecutionOptions<'_>,
    open_statement_array: bool,
) -> Result<()> {
    use alopex_embedded::StreamingQueryResult;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    // Helper to convert CliError to alopex_embedded::Error for callback
    fn cli_err_to_embedded(e: crate::error::CliError) -> alopex_embedded::Error {
        alopex_embedded::Error::Sql(alopex_sql::SqlError::Execution {
            message: e.to_string(),
            code: "ALOPEX-C001",
        })
    }

    let cancelled = Arc::new(AtomicBool::new(false));
    let timed_out = Arc::new(AtomicBool::new(false));
    let cancel_flag = cancelled.clone();
    let timeout_flag = timed_out.clone();

    let result = db.execute_sql_with_rows(sql, |mut rows| {
        // FR-7: SELECT result - stream rows directly from iterator while transaction is alive
        let columns = columns_from_streaming_rows(&rows);
        // The prefix is written here (not by the caller) so that planning
        // errors do not leave a dangling `[` on stdout.
        if open_statement_array {
            writeln!(writer, "[").map_err(|e| cli_err_to_embedded(CliError::Io(e)))?;
        }
        let mut streaming_writer = StreamingWriter::new(writer, formatter, columns, options.limit)
            .with_quiet(options.quiet);

        // FR-7: Use None for row count hint to support true streaming output
        streaming_writer
            .prepare(None)
            .map_err(cli_err_to_embedded)?;

        if let Err(err) = options.deadline.check() {
            timeout_flag.store(true, Ordering::SeqCst);
            return Err(cli_err_to_embedded(err));
        }

        // Consume iterator row by row for true streaming.
        // Errors from `next_row` must propagate: swallowing them here would
        // silently truncate the result set (GitHub issue #23).
        while let Some(sql_row) = rows.next_row()? {
            if options.cancel.is_cancelled() {
                cancel_flag.store(true, Ordering::SeqCst);
                return Err(cli_err_to_embedded(CliError::Cancelled));
            }
            if let Err(err) = options.deadline.check() {
                timeout_flag.store(true, Ordering::SeqCst);
                return Err(cli_err_to_embedded(err));
            }
            let values: Vec<Value> = sql_row.into_iter().map(sql_value_to_value).collect();
            let row = Row::new(values);

            match streaming_writer
                .write_row(row)
                .map_err(cli_err_to_embedded)?
            {
                WriteStatus::LimitReached => break,
                WriteStatus::Continue => {}
            }
        }

        streaming_writer.finish().map_err(cli_err_to_embedded)?;
        Ok(())
    });

    let result = match result {
        Ok(value) => value,
        Err(err) => {
            if cancelled.load(Ordering::SeqCst) {
                return Err(CliError::Cancelled);
            }
            if timed_out.load(Ordering::SeqCst) {
                return Err(CliError::Timeout(format!(
                    "deadline exceeded after {}",
                    humantime::format_duration(options.deadline.duration())
                )));
            }
            return Err(CliError::Database(err));
        }
    };

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
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rows: Option<usize>,
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

#[derive(serde::Deserialize)]
struct RemoteStreamItem {
    row: Option<Vec<alopex_sql::storage::SqlValue>>,
    error: Option<RemoteStreamError>,
    #[serde(default)]
    done: bool,
}

#[derive(serde::Deserialize)]
struct RemoteStreamError {
    message: String,
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

async fn send_cancel_request(client: &HttpClient) -> Result<()> {
    #[derive(serde::Serialize)]
    struct CancelRequest {}

    let request = CancelRequest {};
    let _: serde_json::Value = client
        .post_json("api/sql/cancel", &request)
        .await
        .map_err(map_client_error)?;
    Ok(())
}

fn merge_limit(limit: Option<usize>, max_rows: Option<usize>) -> Option<usize> {
    match (limit, max_rows) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Execute DDL/DML/multi-statement SQL and emit one result block per statement.
///
/// All statements run in a single auto-commit transaction
/// ([`Database::execute_sql_multi`]); if any statement fails, the whole batch
/// is rolled back and an error is returned (non-zero exit code).
///
/// Output contract:
/// - `--output json`: an array of per-statement result sets. DDL/DML
///   statements contribute a status result set (`status`/`message` columns);
///   `--quiet` omits status result sets.
/// - Other formats: one result block per statement, in statement order
///   (status blocks are suppressed by `--quiet`).
fn execute_sql_statements<W: Write>(
    db: &Database,
    sql: &str,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    use alopex_sql::ExecutionResult;

    options.deadline.check()?;
    let results = db.execute_sql_multi(sql)?;
    options.deadline.check()?;

    let json_array = output.statement_array();
    if json_array {
        writeln!(writer, "[")?;
    }
    let mut first = true;
    for result in results {
        let is_status = !matches!(result, ExecutionResult::Query(_));
        if is_status && options.quiet {
            continue;
        }
        if json_array && !first {
            writeln!(writer, ",")?;
        }
        first = false;
        emit_execution_result(result, writer, output, options)?;
    }
    if json_array {
        writeln!(writer, "]")?;
    }
    Ok(())
}

/// Emit a single statement's execution result as one result block.
fn emit_execution_result<W: Write>(
    result: alopex_sql::ExecutionResult,
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
) -> Result<()> {
    use alopex_sql::ExecutionResult;

    match result {
        ExecutionResult::Success => emit_status_result(
            writer,
            output,
            options,
            "Operation completed successfully".to_string(),
        ),
        ExecutionResult::RowsAffected(count) => {
            emit_status_result(writer, output, options, format!("{count} row(s) affected"))
        }
        ExecutionResult::Query(query_result) => {
            let columns = columns_from_query_result(&query_result);
            let mut streaming_writer = StreamingWriter::new(
                &mut *writer,
                output.create_block_formatter(),
                columns,
                options.limit,
            )
            .with_quiet(options.quiet);
            streaming_writer.prepare(Some(query_result.rows.len()))?;
            for sql_row in query_result.rows {
                let values: Vec<Value> = sql_row.into_iter().map(sql_value_to_value).collect();
                match streaming_writer.write_row(Row::new(values))? {
                    WriteStatus::LimitReached => break,
                    WriteStatus::Continue => {}
                }
            }
            streaming_writer.finish()
        }
    }
}

/// Emit an `OK` status result set (`status`/`message` columns).
fn emit_status_result<W: Write>(
    writer: &mut W,
    output: &mut SqlOutput<'_>,
    options: &SqlExecutionOptions<'_>,
    message: String,
) -> Result<()> {
    let columns = sql_status_columns();
    let mut streaming_writer = StreamingWriter::new(
        &mut *writer,
        output.create_block_formatter(),
        columns,
        options.limit,
    )
    .with_quiet(options.quiet);
    streaming_writer.prepare(Some(1))?;
    streaming_writer.write_row(Row::new(vec![
        Value::Text("OK".to_string()),
        Value::Text(message),
    ]))?;
    streaming_writer.finish()
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

    /// Regression test for issue #23: errors surfaced by `next_row` during a
    /// streaming SELECT must propagate as `Err`, not be swallowed into a
    /// silently-empty result set with exit code 0 (the old
    /// `while let Ok(Some(...))` pattern did exactly that).
    #[test]
    fn streaming_select_row_error_propagates() {
        let db = create_test_db();
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        db.execute_sql("INSERT INTO t (id) VALUES (1);").unwrap();

        let deadline = Deadline::new(parse_deadline(None).unwrap());
        let cancel = CancelSignal::new();
        let options = SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel: &cancel,
            deadline: &deadline,
            admin_launcher: None,
        };

        let mut output = Vec::new();
        let formatter = Box::new(JsonlFormatter::new());
        // `id / 0` fails during row evaluation (division by zero), i.e. inside
        // the `next_row` loop of `execute_sql_select_streaming`.
        let result = execute_sql_select_streaming(
            &db,
            "SELECT id / 0 AS x FROM t;",
            &mut output,
            formatter,
            &options,
            false,
        );
        assert!(
            result.is_err(),
            "row evaluation errors must propagate instead of yielding an empty result"
        );
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
            fetch_size: None,
            max_rows: None,
            deadline: None,
            tui: false,
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
            fetch_size: None,
            max_rows: None,
            deadline: None,
            tui: false,
        };

        let sql = cmd.resolve_query(&default_batch_mode()).unwrap();
        assert_eq!(sql, "SELECT * FROM users\n");
    }

    #[test]
    fn resolve_query_returns_no_query_error() {
        let cmd = SqlCommand {
            query: None,
            file: None,
            fetch_size: None,
            max_rows: None,
            deadline: None,
            tui: false,
        };

        let err = cmd.resolve_query(&default_batch_mode()).unwrap_err();
        assert!(matches!(err, CliError::NoQueryProvided));
    }

    #[test]
    fn resolve_query_rejects_query_and_file() {
        let cmd = SqlCommand {
            query: Some("SELECT 1".to_string()),
            file: Some("query.sql".into()),
            fetch_size: None,
            max_rows: None,
            deadline: None,
            tui: false,
        };

        let err = cmd.resolve_query(&default_batch_mode()).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidArgument(msg) if msg == "Cannot specify both query and file"
        ));
    }
}
