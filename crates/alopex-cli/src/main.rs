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
mod ui;
mod uri;
mod version;

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use clap_complete::{generate, Shell};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

use batch::{BatchMode, DistributedReadOutcome};
use cli::{Cli, Command};
use client::http::HttpClient;
use commands::lifecycle::{RemoteLifecycleSupport, SupportLevel};
use config::{setup_signal_handler, validate_thread_mode, EXIT_CODE_INTERRUPTED};
use error::{handle_error, CliError, Result};
use models::{Column, DataType, Row, Value};
use output::create_formatter;
use profile::config::{
    AuthType, ConnectionType, ExecutionScope, ResolvedSqlReadMode, ServerConfig,
};
use profile::{execute_profile_command, execute_profile_tui, ProfileManager, ResolvedConfig};
use streaming::{
    write_distributed_read_routing_report, DistributedReadRoutingReport, StreamingWriter,
};
use tui::admin::actions::AdminAction;
use tui::admin::{AdminBackend, AdminContext, AdminTarget, AuthCapabilities};
use ui::mode::{resolve_ui_mode, UiMode};
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
        handle_error(&e, verbose);
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
            let exit_code = e.exit_code().as_i32() as u8;
            handle_error(&e, verbose);
            ExitCode::from(exit_code)
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
            execution_scope: ExecutionScope::Local,
            cluster_read: None,
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
            execution_scope: ExecutionScope::Local,
            cluster_read: None,
        });
    }

    let manager = ProfileManager::load()?;
    manager.resolve(cli)
}

/// Main entry point logic.
fn run(cli: Cli) -> Result<()> {
    let batch_mode = BatchMode::detect(&cli);
    let ui_resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);
    for warning in &ui_resolution.warnings {
        eprintln!("{}", warning.message());
    }
    let ui_mode = ui_resolution.mode;
    let output_format = cli.output_format();

    match &cli.command {
        Some(Command::Profile { command }) => {
            if ui_mode == UiMode::Tui {
                if let Some(command) = command.clone() {
                    let admin_launcher = resolve_config(&cli).ok().map(|resolved| {
                        let limit = cli.limit;
                        let quiet = cli.quiet;
                        Box::new(move || {
                            let data_dir = resolved.data_dir.as_ref().map(PathBuf::from);
                            match resolved.connection_type {
                                ConnectionType::Server => {
                                    let server_config =
                                        resolved.server.as_ref().ok_or_else(|| {
                                            CliError::InvalidArgument(
                                                "Missing server config".to_string(),
                                            )
                                        })?;
                                    let client = HttpClient::new(server_config).map_err(|err| {
                                        CliError::ServerConnection(err.to_string())
                                    })?;
                                    let runtime =
                                        tokio::runtime::Runtime::new().map_err(|err| {
                                            CliError::InvalidArgument(format!(
                                                "Failed to start async runtime: {err}"
                                            ))
                                        })?;
                                    let auth = auth_capabilities_from_server(
                                        &client,
                                        server_config,
                                        &runtime,
                                    );
                                    tui::admin::run_admin_ui(AdminContext {
                                        connection_label: "server".to_string(),
                                        auth,
                                        backend: AdminBackend::Remote {
                                            client: &client,
                                            batch_mode: &batch_mode,
                                            output_format,
                                            limit,
                                            quiet,
                                            data_dir,
                                        },
                                        initial_target: None,
                                    })
                                }
                                ConnectionType::Local => {
                                    let db = open_database_with_check(&resolved)?;
                                    tui::admin::run_admin_ui(AdminContext {
                                        connection_label: "local".to_string(),
                                        auth: AuthCapabilities::full(),
                                        backend: AdminBackend::Local {
                                            db: &db,
                                            batch_mode: &batch_mode,
                                            output_format,
                                            limit,
                                            quiet,
                                            data_dir,
                                        },
                                        initial_target: None,
                                    })
                                }
                            }
                        }) as Box<dyn FnMut() -> Result<()>>
                    });
                    return execute_profile_tui(command, "local", output_format, admin_launcher);
                }
            }
            if command.is_none() && (ui_mode == UiMode::Tui || !batch_mode.is_tty) {
                let resolved = resolve_config(&cli)?;
                let db = open_database_with_check(&resolved)?;
                let data_dir = resolved.data_dir.as_ref().map(PathBuf::from);
                return tui::admin::run_admin_ui(AdminContext {
                    connection_label: "local".to_string(),
                    auth: AuthCapabilities::full(),
                    backend: AdminBackend::Local {
                        db: &db,
                        batch_mode: &batch_mode,
                        output_format,
                        limit: cli.limit,
                        quiet: cli.quiet,
                        data_dir,
                    },
                    initial_target: None,
                });
            }
            let Some(command) = command.clone() else {
                return Err(CliError::InvalidArgument(
                    "Missing profile subcommand".to_string(),
                ));
            };
            return execute_profile_command(command, output_format);
        }
        Some(Command::Completions { shell }) => {
            return generate_completions(*shell);
        }
        Some(Command::Version) => {
            return commands::version::execute_version(output_format);
        }
        _ => {}
    }

    if cli.command.is_none() {
        let resolved = resolve_config(&cli)?;
        let data_dir = resolved.data_dir.as_ref().map(PathBuf::from);
        return match resolved.connection_type {
            ConnectionType::Server => {
                let server_config = resolved.server.as_ref().ok_or_else(|| {
                    CliError::InvalidArgument("Missing server config".to_string())
                })?;
                let client = HttpClient::new(server_config)
                    .map_err(|err| CliError::ServerConnection(err.to_string()))?;
                let runtime = tokio::runtime::Runtime::new().map_err(|err| {
                    CliError::InvalidArgument(format!("Failed to start async runtime: {err}"))
                })?;
                let auth = auth_capabilities_from_server(&client, server_config, &runtime);
                let context = AdminContext {
                    connection_label: "server".to_string(),
                    auth,
                    backend: AdminBackend::Remote {
                        client: &client,
                        batch_mode: &batch_mode,
                        output_format,
                        limit: cli.limit,
                        quiet: cli.quiet,
                        data_dir,
                    },
                    initial_target: None,
                };
                if ui_mode != UiMode::Tui {
                    if !batch_mode.is_tty {
                        return tui::admin::run_admin_ui(context);
                    }
                    let mut formatter = create_formatter(output_format);
                    let mut writer = io::stdout().lock();
                    let columns = vec![
                        Column::new("Status", DataType::Text),
                        Column::new("Message", DataType::Text),
                    ];
                    let rows = vec![Row::new(vec![
                        Value::Text("Error".to_string()),
                        Value::Text("Admin UI is unavailable in batch mode.".to_string()),
                    ])];
                    formatter.write_header(&mut writer, &columns)?;
                    for row in &rows {
                        formatter.write_row(&mut writer, row)?;
                    }
                    formatter.write_footer(&mut writer)?;
                    return Ok(());
                }
                tui::admin::run_admin_ui(context)
            }
            ConnectionType::Local => {
                let db = open_database_with_check(&resolved)?;
                let context = AdminContext {
                    connection_label: "local".to_string(),
                    auth: AuthCapabilities::full(),
                    backend: AdminBackend::Local {
                        db: &db,
                        batch_mode: &batch_mode,
                        output_format,
                        limit: cli.limit,
                        quiet: cli.quiet,
                        data_dir,
                    },
                    initial_target: None,
                };
                if ui_mode != UiMode::Tui {
                    if !batch_mode.is_tty {
                        return tui::admin::run_admin_ui(context);
                    }
                    let mut formatter = create_formatter(output_format);
                    let mut writer = io::stdout().lock();
                    let columns = vec![
                        Column::new("Status", DataType::Text),
                        Column::new("Message", DataType::Text),
                    ];
                    let rows = vec![Row::new(vec![
                        Value::Text("Error".to_string()),
                        Value::Text("Admin UI is unavailable in batch mode.".to_string()),
                    ])];
                    formatter.write_header(&mut writer, &columns)?;
                    for row in &rows {
                        formatter.write_row(&mut writer, row)?;
                    }
                    formatter.write_footer(&mut writer)?;
                    return Ok(());
                }
                tui::admin::run_admin_ui(context)
            }
        };
    }

    // Open the database
    let resolved = resolve_config(&cli)?;
    let command = cli
        .command
        .ok_or_else(|| CliError::InvalidArgument("Missing subcommand".to_string()))?;
    let sql_read_mode = match &command {
        Command::Sql(sql_cmd) => match resolved.resolve_sql_read_mode(sql_cmd.read_mode) {
            Ok(mode) => Some(mode),
            Err(error) => {
                let reason = error.to_string();
                if let Some(format) = sql_cmd.routing_report {
                    let requested_mode = sql_cmd
                        .read_mode
                        .map(|mode| format!("{mode:?}").to_ascii_lowercase())
                        .unwrap_or_else(|| "local".to_string());
                    let report = DistributedReadRoutingReport::new(
                        requested_mode,
                        None,
                        "pre_execution_rejection",
                        DistributedReadOutcome::Unsupported.as_str(),
                        Some(reason.clone()),
                    );
                    let stderr = io::stderr();
                    let mut stderr = stderr.lock();
                    write_distributed_read_routing_report(&mut stderr, format, &report)?;
                }
                return Err(CliError::DistributedReadOutcome {
                    outcome: DistributedReadOutcome::Unsupported.as_str().to_string(),
                    reason,
                    exit_code: DistributedReadOutcome::Unsupported.exit_code(),
                });
            }
        },
        _ => None,
    };

    if resolved.connection_type == ConnectionType::Server {
        let data_dir = resolved.data_dir.as_deref().map(Path::new);
        if let Some(server_config) = resolved.server.as_ref() {
            match execute_server_command(
                &command,
                server_config,
                data_dir,
                &batch_mode,
                ui_mode,
                output_format,
                cli.limit,
                cli.quiet,
                sql_read_mode,
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
                                ui_mode,
                                output_format,
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
    let data_dir = resolved.data_dir.as_deref().map(Path::new);

    // Check if this is a write command before executing
    let is_write = is_write_command(&command);

    // Execute the command
    execute_command(
        &db,
        command,
        data_dir,
        &batch_mode,
        ui_mode,
        output_format,
        cli.limit,
        cli.quiet,
    )?;

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
            Some(
                KvCommand::Put { .. }
                    | KvCommand::Delete { .. }
                    | KvCommand::Txn(
                        KvTxnCommand::Begin { .. }
                            | KvTxnCommand::Put { .. }
                            | KvTxnCommand::Delete { .. }
                            | KvTxnCommand::Commit { .. }
                            | KvTxnCommand::Rollback { .. }
                    )
            )
        ),
        Command::Sql(sql_cmd) => is_write_sql(sql_cmd),
        Command::Vector { command: vec_cmd } => {
            matches!(
                vec_cmd,
                Some(VectorCommand::Upsert { .. } | VectorCommand::Delete { .. })
            )
        }
        Command::Hnsw { command: hnsw_cmd } => {
            matches!(
                hnsw_cmd,
                Some(HnswCommand::Create { .. } | HnswCommand::Drop { .. })
            )
        }
        Command::Columnar { command: col_cmd } => matches!(
            col_cmd,
            Some(
                ColumnarCommand::Ingest { .. }
                    | ColumnarCommand::Index(
                        IndexCommand::Create { .. } | IndexCommand::Drop { .. }
                    )
            )
        ),
        Command::Server { .. }
        | Command::Lifecycle { .. }
        | Command::Profile { .. }
        | Command::Version
        | Command::Completions { .. } => false,
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

#[allow(clippy::too_many_arguments)]
fn execute_server_command(
    command: &Command,
    server_config: &ServerConfig,
    data_dir: Option<&Path>,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
    sql_read_mode: Option<ResolvedSqlReadMode>,
) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().map_err(|err| {
        CliError::InvalidArgument(format!("Failed to start async runtime: {err}"))
    })?;
    let client = HttpClient::new(server_config)
        .map_err(|err| CliError::ServerConnection(err.to_string()))?;
    let auth_result = runtime.block_on(fetch_auth_capabilities(&client));
    let (auth, lifecycle_support) = match auth_result {
        Ok(auth) => (
            auth.clone(),
            RemoteLifecycleSupport {
                backup: if auth.allows(AdminAction::Backup) {
                    SupportLevel::Supported
                } else {
                    SupportLevel::Unsupported
                },
                restore: if auth.allows(AdminAction::Restore) {
                    SupportLevel::Supported
                } else {
                    SupportLevel::Unsupported
                },
            },
        ),
        Err(_) => (
            auth_capabilities_from_config(server_config),
            RemoteLifecycleSupport::unknown(),
        ),
    };

    match command {
        Command::Kv { command: kv_cmd } => {
            let Some(kv_cmd) = kv_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing KV subcommand".to_string(),
                ));
            };
            let columns = get_kv_columns(kv_cmd);
            if ui_mode == UiMode::Tui {
                let admin_label = "server".to_string();
                let admin_auth = auth.clone();
                let admin_data_dir = data_dir.map(PathBuf::from);
                let client_ref = &client;
                let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Kv),
                        })
                    }));
                return runtime.block_on(commands::kv::execute_remote_tui(
                    &client,
                    kv_cmd,
                    columns,
                    output_format,
                    limit,
                    quiet,
                    "server",
                    admin_launcher,
                ));
            }
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
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                if ui_mode == UiMode::Tui {
                    let client_ref = &client;
                    let admin_label = "server".to_string();
                    let admin_auth = auth.clone();
                    let admin_data_dir = data_dir.map(PathBuf::from);
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Sql),
                        })
                    }))
                } else {
                    None
                };
            let sql_read_mode = sql_read_mode.ok_or_else(|| {
                CliError::InvalidArgument("missing resolved SQL read mode".to_string())
            })?;
            runtime.block_on(commands::sql::execute_remote_with_routing(
                &client,
                sql_cmd,
                sql_read_mode,
                batch_mode,
                ui_mode,
                &mut handle,
                output_format,
                admin_launcher,
                limit,
                quiet,
            ))
        }
        Command::Vector { command: vec_cmd } => {
            let Some(vec_cmd) = vec_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing vector subcommand".to_string(),
                ));
            };
            let columns = get_vector_columns(vec_cmd);
            if ui_mode == UiMode::Tui {
                let admin_label = "server".to_string();
                let admin_auth = auth.clone();
                let admin_data_dir = data_dir.map(PathBuf::from);
                let client_ref = &client;
                let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Vector),
                        })
                    }));
                return runtime.block_on(commands::vector::execute_remote_tui(
                    &client,
                    vec_cmd,
                    batch_mode,
                    columns,
                    output_format,
                    limit,
                    quiet,
                    "server",
                    admin_launcher,
                ));
            }
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
            let Some(hnsw_cmd) = hnsw_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing HNSW subcommand".to_string(),
                ));
            };
            let columns = get_hnsw_columns(hnsw_cmd);
            if ui_mode == UiMode::Tui {
                let admin_label = "server".to_string();
                let admin_auth = auth.clone();
                let admin_data_dir = data_dir.map(PathBuf::from);
                let client_ref = &client;
                let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Hnsw),
                        })
                    }));
                return runtime.block_on(commands::hnsw::execute_remote_tui(
                    &client,
                    hnsw_cmd,
                    columns,
                    output_format,
                    limit,
                    quiet,
                    "server",
                    admin_launcher,
                ));
            }
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
            let Some(col_cmd) = col_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing columnar subcommand".to_string(),
                ));
            };
            if ui_mode == UiMode::Tui {
                let admin_label = "server".to_string();
                let admin_auth = auth.clone();
                let admin_data_dir = data_dir.map(PathBuf::from);
                let client_ref = &client;
                let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Columnar),
                        })
                    }));
                return runtime.block_on(commands::columnar::execute_remote_tui(
                    &client,
                    col_cmd,
                    batch_mode,
                    output_format,
                    limit,
                    quiet,
                    "server",
                    admin_launcher,
                ));
            }
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
        Command::Server {
            command: server_cmd,
        } => {
            let Some(server_cmd) = server_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing server subcommand".to_string(),
                ));
            };
            if ui_mode == UiMode::Tui {
                let admin_label = "server".to_string();
                let admin_auth = auth.clone();
                let admin_data_dir = data_dir.map(PathBuf::from);
                let client_ref = &client;
                let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let auth = admin_auth.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth,
                            backend: AdminBackend::Remote {
                                client: client_ref,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: None,
                        })
                    }));
                return runtime.block_on(commands::server::execute_remote_tui(
                    &client,
                    server_cmd,
                    quiet,
                    "server",
                    output_format,
                    admin_launcher,
                ));
            }
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            runtime.block_on(commands::server::execute_remote_with_format(
                &client,
                server_cmd,
                &mut handle,
                quiet,
                output_format,
            ))
        }
        Command::Lifecycle { command } => {
            let Some(command) = command else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "server".to_string(),
                        auth: auth.clone(),
                        backend: AdminBackend::Remote {
                            client: &client,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing lifecycle subcommand".to_string(),
                ));
            };
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            let formatter = create_formatter(output_format);
            runtime.block_on(commands::lifecycle::execute_remote_with_formatter(
                &client,
                command,
                lifecycle_support,
                &mut handle,
                formatter,
            ))
        }
        Command::Profile { .. } | Command::Version | Command::Completions { .. } => Err(
            CliError::InvalidArgument("Command is not available in server mode".to_string()),
        ),
    }
}

#[derive(Deserialize)]
struct AdminCapabilitiesResponse {
    scope: String,
    allowed_actions: Vec<String>,
}

fn auth_capabilities_from_server(
    client: &HttpClient,
    server_config: &ServerConfig,
    runtime: &tokio::runtime::Runtime,
) -> AuthCapabilities {
    runtime
        .block_on(fetch_auth_capabilities(client))
        .unwrap_or_else(|_| auth_capabilities_from_config(server_config))
}

fn auth_capabilities_from_config(server_config: &ServerConfig) -> AuthCapabilities {
    let auth_type = server_config.auth.unwrap_or(AuthType::None);
    if auth_type == AuthType::None {
        AuthCapabilities::full()
    } else {
        AuthCapabilities::restricted_all()
    }
}

async fn fetch_auth_capabilities(client: &HttpClient) -> Result<AuthCapabilities> {
    let response: AdminCapabilitiesResponse = client
        .get_json("api/admin/capabilities")
        .await
        .map_err(map_client_error)?;

    if response.scope == "full" {
        return Ok(AuthCapabilities::full());
    }

    let mut allowed_actions = HashSet::new();
    for action in response.allowed_actions {
        if let Some(action) = admin_action_from_str(&action) {
            allowed_actions.insert(action);
        }
    }
    if allowed_actions.is_empty() {
        return Ok(AuthCapabilities::restricted_all());
    }
    Ok(AuthCapabilities::restricted(allowed_actions))
}

fn admin_action_from_str(action: &str) -> Option<AdminAction> {
    match action.to_lowercase().as_str() {
        "read" => Some(AdminAction::Read),
        "create" => Some(AdminAction::Create),
        "update" => Some(AdminAction::Update),
        "delete" => Some(AdminAction::Delete),
        "archive" => Some(AdminAction::Archive),
        "restore" => Some(AdminAction::Restore),
        "backup" => Some(AdminAction::Backup),
        "export" => Some(AdminAction::Export),
        _ => None,
    }
}

fn map_client_error(err: client::http::ClientError) -> CliError {
    use client::http::ClientError;
    match err {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) => CliError::InvalidArgument(message),
        ClientError::Build(message) => CliError::InvalidArgument(message),
        ClientError::Auth(err) => CliError::InvalidArgument(err.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::ServerConnection(format!("server error {status}: {body}"))
        }
    }
}

fn execute_local_command(
    resolved: &ResolvedConfig,
    command: Command,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let db = open_database_with_check(resolved)?;
    let data_dir = resolved.data_dir.as_ref().map(PathBuf::from);
    execute_command(
        &db,
        command,
        data_dir.as_deref(),
        batch_mode,
        ui_mode,
        output_format,
        limit,
        quiet,
    )
}

/// Execute the command and write output.
#[allow(clippy::too_many_arguments)]
fn execute_command(
    db: &alopex_embedded::Database,
    command: Command,
    data_dir: Option<&Path>,
    batch_mode: &BatchMode,
    ui_mode: UiMode,
    output_format: cli::OutputFormat,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Get columns and execute command based on type
    match command {
        Command::Kv { command: kv_cmd } => {
            let Some(kv_cmd) = kv_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "local".to_string(),
                        auth: AuthCapabilities::full(),
                        backend: AdminBackend::Local {
                            db,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing KV subcommand".to_string(),
                ));
            };
            let columns = get_kv_columns(&kv_cmd);
            if ui_mode == UiMode::Tui {
                return commands::kv::execute_tui(
                    db,
                    kv_cmd,
                    batch_mode,
                    output_format,
                    columns,
                    limit,
                    quiet,
                    "local",
                    data_dir.map(PathBuf::from),
                );
            }
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::kv::execute(db, kv_cmd, &mut writer)
        }
        Command::Sql(sql_cmd) => {
            let admin_launcher: Option<Box<dyn FnMut() -> Result<()> + '_>> =
                if ui_mode == UiMode::Tui {
                    let admin_label = "local".to_string();
                    let admin_data_dir = data_dir.map(PathBuf::from);
                    Some(Box::new(move || {
                        let connection_label = admin_label.clone();
                        let data_dir = admin_data_dir.clone();
                        tui::admin::run_admin_ui(AdminContext {
                            connection_label,
                            auth: AuthCapabilities::full(),
                            backend: AdminBackend::Local {
                                db,
                                batch_mode,
                                output_format,
                                limit,
                                quiet,
                                data_dir,
                            },
                            initial_target: Some(AdminTarget::Sql),
                        })
                    }))
                } else {
                    None
                };
            commands::sql::execute_with_formatter(
                db,
                sql_cmd,
                batch_mode,
                ui_mode,
                &mut handle,
                output_format,
                admin_launcher,
                limit,
                quiet,
            )
        }
        Command::Vector { command: vec_cmd } => {
            let Some(vec_cmd) = vec_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "local".to_string(),
                        auth: AuthCapabilities::full(),
                        backend: AdminBackend::Local {
                            db,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing vector subcommand".to_string(),
                ));
            };
            let columns = get_vector_columns(&vec_cmd);
            if ui_mode == UiMode::Tui {
                return commands::vector::execute_tui(
                    db,
                    vec_cmd,
                    batch_mode,
                    output_format,
                    columns,
                    limit,
                    quiet,
                    "local",
                    data_dir.map(PathBuf::from),
                );
            }
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::vector::execute(db, vec_cmd, batch_mode, &mut writer)
        }
        Command::Hnsw { command: hnsw_cmd } => {
            let Some(hnsw_cmd) = hnsw_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "local".to_string(),
                        auth: AuthCapabilities::full(),
                        backend: AdminBackend::Local {
                            db,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing HNSW subcommand".to_string(),
                ));
            };
            let columns = get_hnsw_columns(&hnsw_cmd);
            if ui_mode == UiMode::Tui {
                return commands::hnsw::execute_tui(
                    db,
                    hnsw_cmd,
                    batch_mode,
                    output_format,
                    columns,
                    limit,
                    quiet,
                    "local",
                    data_dir.map(PathBuf::from),
                );
            }
            let formatter = create_formatter(output_format);
            let mut writer =
                StreamingWriter::new(&mut handle, formatter, columns, limit).with_quiet(quiet);
            commands::hnsw::execute(db, hnsw_cmd, &mut writer)
        }
        Command::Columnar { command: col_cmd } => {
            let Some(col_cmd) = col_cmd else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "local".to_string(),
                        auth: AuthCapabilities::full(),
                        backend: AdminBackend::Local {
                            db,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing columnar subcommand".to_string(),
                ));
            };
            if ui_mode == UiMode::Tui {
                return commands::columnar::execute_tui(
                    db,
                    col_cmd,
                    batch_mode,
                    output_format,
                    limit,
                    quiet,
                    "local",
                    data_dir.map(PathBuf::from),
                );
            }
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
        Command::Server { .. } => Err(CliError::InvalidArgument(
            "Server commands require a server profile".to_string(),
        )),
        Command::Version => commands::version::execute_version(output_format),
        Command::Completions { shell } => generate_completions(shell),
        Command::Profile { command } => {
            let Some(command) = command else {
                return Err(CliError::InvalidArgument(
                    "Missing profile subcommand".to_string(),
                ));
            };
            execute_profile_command(command, output_format)
        }
        Command::Lifecycle { command } => {
            let Some(command) = command else {
                if ui_mode == UiMode::Tui || !batch_mode.is_tty {
                    return tui::admin::run_admin_ui(AdminContext {
                        connection_label: "local".to_string(),
                        auth: AuthCapabilities::full(),
                        backend: AdminBackend::Local {
                            db,
                            batch_mode,
                            output_format,
                            limit,
                            quiet,
                            data_dir: data_dir.map(PathBuf::from),
                        },
                        initial_target: None,
                    });
                }
                return Err(CliError::InvalidArgument(
                    "Missing lifecycle subcommand".to_string(),
                ));
            };
            commands::lifecycle::execute(&command, data_dir, &mut handle, output_format)
        }
    }
}

/// Get columns for KV command output.
fn get_kv_columns(cmd: &cli::KvCommand) -> Vec<Column> {
    use cli::{KvCommand, KvTxnCommand};
    match cmd {
        KvCommand::Get { .. } | KvCommand::List { .. } => commands::kv::kv_columns(),
        KvCommand::Search { .. } => commands::kv::kv_search_columns(),
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
