//! Alopex CLI - Command-line interface for Alopex DB
//!
//! This binary provides a CLI for interacting with Alopex DB,
//! supporting KV, SQL, Vector, HNSW, and Columnar operations.

mod cli;
mod commands;
mod config;
mod error;
mod models;
mod output;
mod streaming;
mod uri;

use std::io;
use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};
use config::{setup_signal_handler, validate_thread_mode, EXIT_CODE_INTERRUPTED};
use error::{handle_error, CliError, Result};
use models::Column;
use output::create_formatter;
use streaming::StreamingWriter;
use uri::{validate_s3_credentials, StorageUri};

fn main() -> ExitCode {
    // Parse CLI arguments
    let cli = Cli::parse();

    // Save verbose flag for error handling
    let verbose = cli.verbose;

    // Set up logging
    init_logging(cli.verbose, cli.quiet);

    // Set up signal handler
    if let Err(e) = setup_signal_handler() {
        handle_error(e, verbose);
        return ExitCode::from(1);
    }

    // Validate thread mode
    let _thread_mode = validate_thread_mode(cli.thread_mode, cli.quiet);

    // Run the main logic and handle errors
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Check if we were interrupted
            if config::is_interrupted() {
                eprintln!("\nOperation interrupted.");
                return ExitCode::from(EXIT_CODE_INTERRUPTED as u8);
            }

            // Handle the error normally
            handle_error(e, verbose);
            ExitCode::from(1)
        }
    }
}

/// Initialize logging based on CLI options.
fn init_logging(verbose: bool, quiet: bool) {
    if quiet {
        // No logging output
        return;
    }

    let filter = if verbose {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .init();
}

/// Main entry point logic.
fn run(cli: Cli) -> Result<()> {
    if matches!(&cli.command, Command::Profile { .. }) {
        return Err(CliError::InvalidArgument(
            "Profile commands are not implemented yet".to_string(),
        ));
    }

    // Open the database
    let db = open_database(&cli)?;

    // Check if this is a write command before executing
    let is_write = is_write_command(&cli.command);

    // Execute the command
    execute_command(&db, cli.command, cli.output, cli.limit, cli.quiet)?;

    // Flush only for write commands to ensure S3 sync errors are propagated
    // Read-only commands should work even with S3 read-only permissions
    if is_write {
        db.flush()?;
    }

    Ok(())
}

/// Determine if a command modifies the database.
///
/// Returns true for commands that write data (put, delete, insert, create, etc.)
/// Returns false for read-only commands (get, list, select, stats, etc.)
fn is_write_command(command: &Command) -> bool {
    use cli::{ColumnarCommand, HnswCommand, IndexCommand, KvCommand, KvTxnCommand, VectorCommand};

    match command {
        Command::Kv { command: kv_cmd } => matches!(
            kv_cmd,
            KvCommand::Put { .. }
                | KvCommand::Delete { .. }
                | KvCommand::Txn(
                    KvTxnCommand::Begin { .. }
                        | KvTxnCommand::Put { .. }
                        | KvTxnCommand::Delete { .. }
                        | KvTxnCommand::Commit { .. }
                        | KvTxnCommand::Rollback { .. }
                )
        ),
        Command::Sql(sql_cmd) => is_write_sql(sql_cmd),
        Command::Vector { command: vec_cmd } => {
            matches!(
                vec_cmd,
                VectorCommand::Upsert { .. } | VectorCommand::Delete { .. }
            )
        }
        Command::Hnsw { command: hnsw_cmd } => {
            matches!(
                hnsw_cmd,
                HnswCommand::Create { .. } | HnswCommand::Drop { .. }
            )
        }
        Command::Columnar { command: col_cmd } => matches!(
            col_cmd,
            ColumnarCommand::Index(IndexCommand::Create { .. } | IndexCommand::Drop { .. })
        ),
        Command::Profile { .. } => false,
    }
}

/// Check if a SQL command is a write operation.
fn is_write_sql(sql_cmd: &cli::SqlCommand) -> bool {
    // Get the query string (from argument or file)
    let query = if let Some(ref q) = sql_cmd.query {
        q.clone()
    } else if let Some(ref file) = sql_cmd.file {
        // Read file content for analysis
        match std::fs::read_to_string(file) {
            Ok(content) => content,
            Err(_) => return false, // Can't read file, assume read-only to avoid blocking reads
        }
    } else {
        return false;
    };

    // Check for write keywords (case-insensitive)
    let query_upper = query.to_uppercase();
    let trimmed = query_upper.trim_start();

    // Write operations start with these keywords
    trimmed.starts_with("INSERT")
        || trimmed.starts_with("UPDATE")
        || trimmed.starts_with("DELETE")
        || trimmed.starts_with("CREATE")
        || trimmed.starts_with("DROP")
        || trimmed.starts_with("ALTER")
        || trimmed.starts_with("TRUNCATE")
}

/// Open the database based on CLI options.
fn open_database(cli: &Cli) -> Result<alopex_embedded::Database> {
    use alopex_embedded::Database;

    if cli.in_memory {
        // In-memory mode
        tracing::debug!("Opening database in in-memory mode");
        Ok(Database::open_in_memory()?)
    } else if let Some(ref data_dir) = cli.data_dir {
        // Parse URI
        let uri = StorageUri::parse(data_dir)?;

        // Check S3 support and validate credentials
        if uri.is_s3() {
            validate_s3_credentials()?;
        }

        // Open database with URI (supports both local and S3)
        let embedded_uri = uri.to_embedded_uri();
        tracing::debug!("Opening database at: {}", embedded_uri);
        Ok(Database::open_with_uri(&embedded_uri)?)
    } else {
        // Neither in-memory nor data-dir specified
        Err(CliError::InvalidArgument(
            "Either --in-memory or --data-dir must be specified".to_string(),
        ))
    }
}

/// Execute the command and write output.
fn execute_command(
    db: &alopex_embedded::Database,
    command: Command,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Get columns and execute command based on type
    match command {
        Command::Kv { command: kv_cmd } => {
            let columns = get_kv_columns(&kv_cmd);
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::kv::execute(db, kv_cmd, &mut writer)
        }
        Command::Sql(sql_cmd) => {
            let formatter = create_formatter(output_format);
            commands::sql::execute_with_formatter(db, sql_cmd, &mut handle, formatter, limit, quiet)
        }
        Command::Vector { command: vec_cmd } => {
            let columns = get_vector_columns(&vec_cmd);
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::vector::execute(db, vec_cmd, &mut writer)
        }
        Command::Hnsw { command: hnsw_cmd } => {
            let columns = get_hnsw_columns(&hnsw_cmd);
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::hnsw::execute(db, hnsw_cmd, &mut writer)
        }
        Command::Columnar { command: col_cmd } => {
            let formatter = create_formatter(output_format);
            commands::columnar::execute_with_formatter(
                db,
                col_cmd,
                &mut handle,
                formatter,
                limit,
                quiet,
            )
        }
        Command::Profile { .. } => Err(CliError::InvalidArgument(
            "Profile commands are not implemented yet".to_string(),
        )),
    }
}

/// Get columns for KV command output.
fn get_kv_columns(cmd: &cli::KvCommand) -> Vec<Column> {
    use cli::KvCommand;
    match cmd {
        KvCommand::Get { .. } | KvCommand::List { .. } => commands::kv::kv_columns(),
        KvCommand::Put { .. } | KvCommand::Delete { .. } | KvCommand::Txn(_) => {
            commands::kv::kv_status_columns()
        }
    }
}

/// Get columns for Vector command output.
fn get_vector_columns(cmd: &cli::VectorCommand) -> Vec<Column> {
    use cli::VectorCommand;
    match cmd {
        VectorCommand::Search { .. } => commands::vector::vector_search_columns(),
        VectorCommand::Upsert { .. } | VectorCommand::Delete { .. } => {
            commands::vector::vector_status_columns()
        }
    }
}

/// Get columns for HNSW command output.
fn get_hnsw_columns(cmd: &cli::HnswCommand) -> Vec<Column> {
    use cli::HnswCommand;
    match cmd {
        HnswCommand::Stats { .. } => commands::hnsw::hnsw_stats_columns(),
        HnswCommand::Create { .. } | HnswCommand::Drop { .. } => {
            commands::hnsw::hnsw_status_columns()
        }
    }
}
