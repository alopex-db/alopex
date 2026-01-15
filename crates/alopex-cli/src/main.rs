//! Alopex CLI - Command-line interface for Alopex DB
//!
//! This binary provides a CLI for interacting with Alopex DB,
//! supporting KV, SQL, Vector, HNSW, and Columnar operations.

mod batch;
mod cli;
mod client;
mod commands;
mod config;
mod error;
mod models;
mod output;
mod profile;
mod progress;
mod streaming;
mod tui;
mod uri;
mod version;

use std::io;
use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use clap_complete::{generate, Shell};
use tracing_subscriber::EnvFilter;

use batch::BatchMode;
use cli::{Cli, Command};
use client::http::HttpClient;
use config::{setup_signal_handler, validate_thread_mode, EXIT_CODE_INTERRUPTED};
use error::{handle_error, CliError, Result};
use models::Column;
use output::create_formatter;
use profile::config::{ConnectionType, ServerConfig};
use profile::{execute_profile_command, ProfileManager, ResolvedConfig};
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

fn generate_completions(shell: Shell) -> Result<()> {
    let mut command = Cli::command();
    let name = command.get_name().to_string();
    let mut stdout = io::stdout();
    generate(shell, &mut command, name, &mut stdout);
    Ok(())
}

fn resolve_config(cli: &Cli) -> Result<ResolvedConfig> {
    if cli.in_memory {
        return Ok(ResolvedConfig {
            data_dir: None,
            in_memory: true,
            profile_name: None,
            connection_type: ConnectionType::Local,
            server: None,
            fallback_local: None,
        });
    }

    if cli.profile.is_some() && cli.data_dir.is_some() {
        return Err(CliError::ConflictingOptions);
    }

    if let Some(data_dir) = cli.data_dir.as_ref() {
        return Ok(ResolvedConfig {
            data_dir: Some(data_dir.clone()),
            in_memory: false,
            profile_name: None,
            connection_type: ConnectionType::Local,
            server: None,
            fallback_local: None,
        });
    }

    let manager = ProfileManager::load()?;
    manager.resolve(cli)
}

/// Main entry point logic.
fn run(cli: Cli) -> Result<()> {
    match &cli.command {
        Command::Profile { command } => {
            return execute_profile_command(command.clone(), cli.output);
        }
        Command::Completions { shell } => {
            return generate_completions(*shell);
        }
        Command::Version => {
            return commands::version::execute_version(cli.output);
        }
        _ => {}
    }

    // Open the database
    let resolved = resolve_config(&cli)?;
    let batch_mode = BatchMode::detect(&cli);
    let command = cli.command;

    if resolved.connection_type == ConnectionType::Server {
        if let Some(server_config) = resolved.server.as_ref() {
            match execute_server_command(
                &command,
                server_config,
                &batch_mode,
                cli.output,
                cli.limit,
                cli.quiet,
            ) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if matches!(err, CliError::ServerConnection(_)) {
                        if let Some(fallback) = resolved.fallback_local.clone() {
                            eprintln!(
                                "Warning: Failed to connect to server, falling back to local mode"
                            );
                            let mut fallback_resolved = resolved.clone();
                            fallback_resolved.connection_type = ConnectionType::Local;
                            fallback_resolved.server = None;
                            fallback_resolved.data_dir = Some(fallback);
                            fallback_resolved.fallback_local = None;
                            return execute_local_command(
                                &fallback_resolved,
                                command,
                                &batch_mode,
                                cli.output,
                                cli.limit,
                                cli.quiet,
                            );
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    let db = open_database_with_check(&resolved)?;

    // Check if this is a write command before executing
    let is_write = is_write_command(&command);

    // Execute the command
    execute_command(&db, command, &batch_mode, cli.output, cli.limit, cli.quiet)?;

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
            ColumnarCommand::Ingest { .. }
                | ColumnarCommand::Index(IndexCommand::Create { .. } | IndexCommand::Drop { .. })
        ),
        Command::Profile { .. } | Command::Version | Command::Completions { .. } => false,
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

fn open_database_with_check(config: &ResolvedConfig) -> Result<alopex_embedded::Database> {
    let db = open_database(config)?;
    let checker = version::compatibility::VersionChecker::new();
    let file_version = version::Version::from(db.file_format_version());

    match checker.check_compatibility(file_version) {
        version::compatibility::VersionCheckResult::Compatible => {}
        version::compatibility::VersionCheckResult::CliOlderThanFile { cli, file } => {
            eprintln!(
                "Warning: CLI v{} は ファイルフォーマット v{} より古いです。アップグレードを推奨します。",
                cli, file
            );
        }
        version::compatibility::VersionCheckResult::Incompatible { cli, file } => {
            return Err(CliError::IncompatibleVersion {
                cli: cli.to_string(),
                file: file.to_string(),
            });
        }
    }

    Ok(db)
}

/// Open the database based on CLI options.
fn open_database(config: &ResolvedConfig) -> Result<alopex_embedded::Database> {
    use alopex_embedded::Database;

    if config.in_memory {
        // In-memory mode
        tracing::debug!("Opening database in in-memory mode");
        Ok(Database::open_in_memory()?)
    } else if let Some(ref data_dir) = config.data_dir {
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
            "Either --in-memory, --data-dir, or --profile must be specified".to_string(),
        ))
    }
}

fn execute_server_command(
    command: &Command,
    server_config: &ServerConfig,
    batch_mode: &BatchMode,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().map_err(|err| {
        CliError::InvalidArgument(format!("Failed to start async runtime: {err}"))
    })?;
    let client = HttpClient::new(server_config)
        .map_err(|err| CliError::ServerConnection(err.to_string()))?;

    match command {
        Command::Kv { command: kv_cmd } => {
            let formatter = create_formatter(output_format);
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::kv::execute_remote_with_formatter(
                &client,
                kv_cmd,
                &mut handle,
                formatter,
                limit,
                quiet,
            ))
        }
        Command::Sql(sql_cmd) => {
            let formatter = create_formatter(output_format);
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::sql::execute_remote_with_formatter(
                &client,
                sql_cmd,
                batch_mode,
                &mut handle,
                formatter,
                limit,
                quiet,
            ))
        }
        Command::Vector { command: vec_cmd } => {
            let formatter = create_formatter(output_format);
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::vector::execute_remote_with_formatter(
                &client,
                vec_cmd,
                batch_mode,
                &mut handle,
                formatter,
                limit,
                quiet,
            ))
        }
        Command::Hnsw { command: hnsw_cmd } => {
            let formatter = create_formatter(output_format);
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::hnsw::execute_remote_with_formatter(
                &client,
                hnsw_cmd,
                &mut handle,
                formatter,
                limit,
                quiet,
            ))
        }
        Command::Columnar { command: col_cmd } => {
            let formatter = create_formatter(output_format);
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::columnar::execute_remote_with_formatter(
                &client,
                col_cmd,
                batch_mode,
                &mut handle,
                formatter,
                limit,
                quiet,
            ))
        }
        Command::Profile { .. } | Command::Version | Command::Completions { .. } => Err(
            CliError::InvalidArgument("Command is not available in server mode".to_string()),
        ),
    }
}

fn execute_local_command(
    resolved: &ResolvedConfig,
    command: Command,
    batch_mode: &BatchMode,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let db = open_database_with_check(resolved)?;
    execute_command(&db, command, batch_mode, output_format, limit, quiet)
}

/// Execute the command and write output.
fn execute_command(
    db: &alopex_embedded::Database,
    command: Command,
    batch_mode: &BatchMode,
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
            commands::sql::execute_with_formatter(
                db,
                sql_cmd,
                batch_mode,
                &mut handle,
                formatter,
                limit,
                quiet,
            )
        }
        Command::Vector { command: vec_cmd } => {
            let columns = get_vector_columns(&vec_cmd);
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::vector::execute(db, vec_cmd, batch_mode, &mut writer)
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
                batch_mode,
                &mut handle,
                formatter,
                limit,
                quiet,
            )
        }
        Command::Version => commands::version::execute_version(output_format),
        Command::Completions { shell } => generate_completions(shell),
        Command::Profile { command } => execute_profile_command(command, output_format),
    }
}

/// Get columns for KV command output.
fn get_kv_columns(cmd: &cli::KvCommand) -> Vec<Column> {
    use cli::{KvCommand, KvTxnCommand};
    match cmd {
        KvCommand::Get { .. } | KvCommand::List { .. } => commands::kv::kv_columns(),
        KvCommand::Put { .. } | KvCommand::Delete { .. } => commands::kv::kv_status_columns(),
        KvCommand::Txn(txn_cmd) => match txn_cmd {
            KvTxnCommand::Get { .. } | KvTxnCommand::Begin { .. } => commands::kv::kv_columns(),
            KvTxnCommand::Put { .. }
            | KvTxnCommand::Delete { .. }
            | KvTxnCommand::Commit { .. }
            | KvTxnCommand::Rollback { .. } => commands::kv::kv_status_columns(),
        },
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
