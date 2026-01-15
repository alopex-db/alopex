//! CLI Parser - Command-line argument parsing with clap
//!
//! This module defines the CLI structure using clap derive macros.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

fn parse_shell(value: &str) -> Result<Shell, String> {
    match value {
        "bash" => Ok(Shell::Bash),
        "zsh" => Ok(Shell::Zsh),
        "fish" => Ok(Shell::Fish),
        "pwsh" | "powershell" => Ok(Shell::PowerShell),
        _ => Err(format!(
            "Unsupported shell: {}. Use bash, zsh, fish, or pwsh.",
            value
        )),
    }
}

/// Alopex CLI - Command-line interface for Alopex DB
#[derive(Parser, Debug)]
#[command(name = "alopex")]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the database directory (local path or S3 URI)
    #[arg(long)]
    pub data_dir: Option<String>,

    /// Profile name to use for database configuration
    #[arg(long)]
    pub profile: Option<String>,

    /// Run in in-memory mode (no persistence)
    #[arg(long, conflicts_with = "data_dir")]
    pub in_memory: bool,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub output: OutputFormat,

    /// Limit the number of output rows
    #[arg(long)]
    pub limit: Option<usize>,

    /// Suppress informational messages
    #[arg(long)]
    pub quiet: bool,

    /// Enable verbose output (includes stack traces for errors)
    #[arg(long)]
    pub verbose: bool,

    /// Thread mode (multi or single)
    #[arg(long, value_enum, default_value = "multi")]
    pub thread_mode: ThreadMode,

    /// Enable batch mode (non-interactive)
    #[arg(long, short = 'b')]
    pub batch: bool,

    /// Automatically answer yes to prompts
    #[arg(long)]
    pub yes: bool,

    /// Subcommand to execute
    #[command(subcommand)]
    pub command: Command,
}

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table format
    Table,
    /// JSON array format
    Json,
    /// JSON Lines format (one JSON object per line)
    Jsonl,
    /// CSV format (RFC 4180)
    Csv,
    /// TSV format (tab-separated values)
    Tsv,
}

impl OutputFormat {
    /// Returns true if this format supports streaming output.
    #[allow(dead_code)]
    pub fn supports_streaming(&self) -> bool {
        matches!(self, Self::Json | Self::Jsonl | Self::Csv | Self::Tsv)
    }
}

/// Thread mode for database operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ThreadMode {
    /// Multi-threaded mode (default)
    Multi,
    /// Single-threaded mode (not supported in v0.3.2)
    Single,
}

/// Top-level subcommands
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Profile management
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    /// Key-Value operations
    Kv {
        #[command(subcommand)]
        command: KvCommand,
    },
    /// SQL query execution
    Sql(SqlCommand),
    /// Vector operations
    Vector {
        #[command(subcommand)]
        command: VectorCommand,
    },
    /// HNSW index management
    Hnsw {
        #[command(subcommand)]
        command: HnswCommand,
    },
    /// Columnar segment operations
    Columnar {
        #[command(subcommand)]
        command: ColumnarCommand,
    },
    /// Server management commands
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
    /// Show CLI and file format version information
    Version,
    /// Generate shell completion scripts
    Completions {
        /// Shell type (bash, zsh, fish, pwsh)
        #[arg(value_parser = parse_shell, value_name = "SHELL")]
        shell: Shell,
    },
}

/// Profile subcommands
#[derive(Subcommand, Debug, Clone)]
pub enum ProfileCommand {
    /// Create a profile
    Create {
        /// Profile name
        name: String,
        /// Path to the database directory (local path or S3 URI)
        #[arg(long)]
        data_dir: String,
    },
    /// List profiles
    List,
    /// Show profile details
    Show {
        /// Profile name
        name: String,
    },
    /// Delete a profile
    Delete {
        /// Profile name
        name: String,
    },
    /// Set the default profile
    SetDefault {
        /// Profile name
        name: String,
    },
}

/// KV subcommands
#[derive(Subcommand, Debug)]
pub enum KvCommand {
    /// Get a value by key
    Get {
        /// The key to retrieve
        key: String,
    },
    /// Put a key-value pair
    Put {
        /// The key to set
        key: String,
        /// The value to store
        value: String,
    },
    /// Delete a key
    Delete {
        /// The key to delete
        key: String,
    },
    /// List keys with optional prefix
    List {
        /// Filter keys by prefix
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Transaction operations
    #[command(subcommand)]
    Txn(KvTxnCommand),
}

/// KV transaction subcommands
#[derive(Subcommand, Debug)]
pub enum KvTxnCommand {
    /// Begin a transaction
    Begin {
        /// Transaction timeout in seconds (default: 60)
        #[arg(long)]
        timeout_secs: Option<u64>,
    },
    /// Get a value within a transaction
    Get {
        /// The key to retrieve
        key: String,
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
    },
    /// Put a key-value pair within a transaction
    Put {
        /// The key to set
        key: String,
        /// The value to store
        value: String,
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
    },
    /// Delete a key within a transaction
    Delete {
        /// The key to delete
        key: String,
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
    },
    /// Commit a transaction
    Commit {
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
    },
    /// Roll back a transaction
    Rollback {
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
    },
}

/// SQL subcommand
#[derive(Parser, Debug)]
pub struct SqlCommand {
    /// SQL query to execute
    #[arg(conflicts_with = "file")]
    pub query: Option<String>,

    /// File containing SQL query
    #[arg(long, short = 'f')]
    pub file: Option<String>,

    /// Fetch size for server streaming
    #[arg(long)]
    pub fetch_size: Option<usize>,

    /// Max rows to return before stopping
    #[arg(long)]
    pub max_rows: Option<usize>,

    /// Deadline for query execution (e.g. 60s, 5m)
    #[arg(long)]
    pub deadline: Option<String>,

    /// Launch interactive TUI preview
    #[arg(long)]
    pub tui: bool,
}

/// Vector subcommands
#[derive(Subcommand, Debug)]
pub enum VectorCommand {
    /// Search for similar vectors
    Search {
        /// Index name
        #[arg(long)]
        index: String,
        /// Query vector as JSON array
        #[arg(long)]
        query: String,
        /// Number of results to return
        #[arg(long, short = 'k', default_value = "10")]
        k: usize,
        /// Show progress indicator
        #[arg(long)]
        progress: bool,
    },
    /// Upsert a single vector
    Upsert {
        /// Index name
        #[arg(long)]
        index: String,
        /// Vector key/ID
        #[arg(long)]
        key: String,
        /// Vector as JSON array
        #[arg(long)]
        vector: String,
    },
    /// Delete a single vector by key
    Delete {
        /// Index name
        #[arg(long)]
        index: String,
        /// Vector key/ID to delete
        #[arg(long)]
        key: String,
    },
}

/// Distance metric for HNSW index
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum DistanceMetric {
    /// Cosine similarity (default)
    #[default]
    Cosine,
    /// Euclidean distance (L2)
    L2,
    /// Inner product
    Ip,
}

/// HNSW subcommands
#[derive(Subcommand, Debug)]
pub enum HnswCommand {
    /// Create a new HNSW index
    Create {
        /// Index name
        name: String,
        /// Vector dimensions
        #[arg(long)]
        dim: usize,
        /// Distance metric
        #[arg(long, value_enum, default_value = "cosine")]
        metric: DistanceMetric,
    },
    /// Show index statistics
    Stats {
        /// Index name
        name: String,
    },
    /// Drop an index
    Drop {
        /// Index name
        name: String,
    },
}

/// Columnar subcommands
#[derive(Subcommand, Debug)]
pub enum ColumnarCommand {
    /// Scan a columnar segment
    Scan {
        /// Segment ID
        #[arg(long)]
        segment: String,
        /// Show progress indicator
        #[arg(long)]
        progress: bool,
    },
    /// Show segment statistics
    Stats {
        /// Segment ID
        #[arg(long)]
        segment: String,
    },
    /// List all columnar segments
    List,
    /// Ingest a file into columnar storage
    Ingest {
        /// Input file path (CSV or Parquet)
        #[arg(long)]
        file: PathBuf,
        /// Target table name
        #[arg(long)]
        table: String,
        /// CSV delimiter character
        #[arg(long, default_value = ",", value_parser = clap::value_parser!(char))]
        delimiter: char,
        /// Whether the CSV has a header row
        #[arg(
            long,
            default_value = "true",
            value_parser = clap::value_parser!(bool),
            action = clap::ArgAction::Set
        )]
        header: bool,
        /// Compression type (lz4, zstd, none)
        #[arg(long, default_value = "lz4")]
        compression: String,
        /// Row group size (rows per group)
        #[arg(long)]
        row_group_size: Option<usize>,
    },
    /// Index management
    #[command(subcommand)]
    Index(IndexCommand),
}

/// Columnar index subcommands
#[derive(Subcommand, Debug)]
pub enum IndexCommand {
    /// Create an index
    Create {
        /// Segment ID
        #[arg(long)]
        segment: String,
        /// Column name
        #[arg(long)]
        column: String,
        /// Index type (minmax, bloom)
        #[arg(long = "type")]
        index_type: String,
    },
    /// List indexes
    List {
        /// Segment ID
        #[arg(long)]
        segment: String,
    },
    /// Drop an index
    Drop {
        /// Segment ID
        #[arg(long)]
        segment: String,
        /// Column name
        #[arg(long)]
        column: String,
    },
}

/// Server management subcommands
#[derive(Subcommand, Debug)]
pub enum ServerCommand {
    /// Show server status
    Status,
    /// Show server metrics
    Metrics,
    /// Show server health check results
    Health,
    /// Server compaction management
    Compaction {
        #[command(subcommand)]
        command: CompactionCommand,
    },
}

/// Server compaction subcommands
#[derive(Subcommand, Debug)]
pub enum CompactionCommand {
    /// Trigger server compaction
    Trigger,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_in_memory_kv_get() {
        let args = vec!["alopex", "--in-memory", "kv", "get", "mykey"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(cli.in_memory);
        assert!(cli.data_dir.is_none());
        assert_eq!(cli.output, OutputFormat::Table);
        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::Get { key }
            } if key == "mykey"
        ));
    }

    #[test]
    fn test_parse_data_dir_sql() {
        let args = vec![
            "alopex",
            "--data-dir",
            "/path/to/db",
            "sql",
            "SELECT * FROM users",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(!cli.in_memory);
        assert_eq!(cli.data_dir, Some("/path/to/db".to_string()));
        assert!(matches!(
            cli.command,
            Command::Sql(SqlCommand { query: Some(q), file: None, .. }) if q == "SELECT * FROM users"
        ));
    }

    #[test]
    fn test_parse_output_format() {
        let args = vec!["alopex", "--in-memory", "--output", "jsonl", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.output, OutputFormat::Jsonl);
    }

    #[test]
    fn test_parse_limit() {
        let args = vec!["alopex", "--in-memory", "--limit", "100", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.limit, Some(100));
    }

    #[test]
    fn test_parse_sql_streaming_options() {
        let args = vec![
            "alopex",
            "sql",
            "--fetch-size",
            "500",
            "--max-rows",
            "250",
            "--deadline",
            "30s",
            "SELECT 1",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        match cli.command {
            Command::Sql(cmd) => {
                assert_eq!(cmd.fetch_size, Some(500));
                assert_eq!(cmd.max_rows, Some(250));
                assert_eq!(cmd.deadline.as_deref(), Some("30s"));
                assert!(!cmd.tui);
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn test_parse_sql_tui_flag() {
        let args = vec!["alopex", "sql", "--tui", "SELECT 1"];
        let cli = Cli::try_parse_from(args).unwrap();

        match cli.command {
            Command::Sql(cmd) => {
                assert!(cmd.tui);
                assert_eq!(cmd.query.as_deref(), Some("SELECT 1"));
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn test_parse_server_status() {
        let args = vec!["alopex", "server", "status"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Server {
                command: ServerCommand::Status
            }
        ));
    }

    #[test]
    fn test_parse_server_compaction_trigger() {
        let args = vec!["alopex", "server", "compaction", "trigger"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Server {
                command: ServerCommand::Compaction {
                    command: CompactionCommand::Trigger
                }
            }
        ));
    }

    #[test]
    fn test_parse_verbose_quiet() {
        let args = vec!["alopex", "--in-memory", "--verbose", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(cli.verbose);
        assert!(!cli.quiet);
    }

    #[test]
    fn test_parse_thread_mode() {
        let args = vec![
            "alopex",
            "--in-memory",
            "--thread-mode",
            "single",
            "kv",
            "list",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.thread_mode, ThreadMode::Single);
    }

    #[test]
    fn test_parse_profile_option_batch_yes() {
        let args = vec![
            "alopex",
            "--profile",
            "dev",
            "--batch",
            "--yes",
            "--in-memory",
            "kv",
            "list",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.profile.as_deref(), Some("dev"));
        assert!(cli.batch);
        assert!(cli.yes);
    }

    #[test]
    fn test_parse_batch_short_flag() {
        let args = vec!["alopex", "-b", "--in-memory", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(cli.batch);
    }

    #[test]
    fn test_parse_profile_create_subcommand() {
        let args = vec![
            "alopex",
            "profile",
            "create",
            "dev",
            "--data-dir",
            "/path/to/db",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Profile {
                command: ProfileCommand::Create { name, data_dir }
            }
                if name == "dev" && data_dir == "/path/to/db"
        ));
    }

    #[test]
    fn test_parse_completions_bash() {
        let args = vec!["alopex", "completions", "bash"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Completions { shell } if shell == Shell::Bash
        ));
    }

    #[test]
    fn test_parse_completions_pwsh() {
        let args = vec!["alopex", "completions", "pwsh"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Completions { shell } if shell == Shell::PowerShell
        ));
    }

    #[test]
    fn test_parse_kv_put() {
        let args = vec!["alopex", "--in-memory", "kv", "put", "mykey", "myvalue"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::Put { key, value }
            } if key == "mykey" && value == "myvalue"
        ));
    }

    #[test]
    fn test_parse_kv_delete() {
        let args = vec!["alopex", "--in-memory", "kv", "delete", "mykey"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::Delete { key }
            } if key == "mykey"
        ));
    }

    #[test]
    fn test_parse_kv_txn_begin() {
        let args = vec!["alopex", "kv", "txn", "begin", "--timeout-secs", "30"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::Txn(KvTxnCommand::Begin {
                    timeout_secs: Some(30)
                })
            }
        ));
    }

    #[test]
    fn test_parse_kv_txn_get_requires_txn_id() {
        let args = vec!["alopex", "kv", "txn", "get", "mykey"];

        assert!(Cli::try_parse_from(args).is_err());
    }

    #[test]
    fn test_parse_kv_txn_get() {
        let args = vec!["alopex", "kv", "txn", "get", "mykey", "--txn-id", "txn123"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::Txn(KvTxnCommand::Get { key, txn_id })
            } if key == "mykey" && txn_id == "txn123"
        ));
    }

    #[test]
    fn test_parse_kv_list_with_prefix() {
        let args = vec!["alopex", "--in-memory", "kv", "list", "--prefix", "user:"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Kv {
                command: KvCommand::List { prefix: Some(p) }
            } if p == "user:"
        ));
    }

    #[test]
    fn test_parse_sql_from_file() {
        let args = vec!["alopex", "--in-memory", "sql", "-f", "query.sql"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Sql(SqlCommand { query: None, file: Some(f), .. }) if f == "query.sql"
        ));
    }

    #[test]
    fn test_parse_vector_search() {
        let args = vec![
            "alopex",
            "--in-memory",
            "vector",
            "search",
            "--index",
            "my_index",
            "--query",
            "[1.0,2.0,3.0]",
            "-k",
            "5",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Vector {
                command: VectorCommand::Search { index, query, k, progress }
            } if index == "my_index" && query == "[1.0,2.0,3.0]" && k == 5 && !progress
        ));
    }

    #[test]
    fn test_parse_vector_upsert() {
        let args = vec![
            "alopex",
            "--in-memory",
            "vector",
            "upsert",
            "--index",
            "my_index",
            "--key",
            "vec1",
            "--vector",
            "[1.0,2.0,3.0]",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Vector {
                command: VectorCommand::Upsert { index, key, vector }
            } if index == "my_index" && key == "vec1" && vector == "[1.0,2.0,3.0]"
        ));
    }

    #[test]
    fn test_parse_vector_delete() {
        let args = vec![
            "alopex",
            "--in-memory",
            "vector",
            "delete",
            "--index",
            "my_index",
            "--key",
            "vec1",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Vector {
                command: VectorCommand::Delete { index, key }
            } if index == "my_index" && key == "vec1"
        ));
    }

    #[test]
    fn test_parse_hnsw_create() {
        let args = vec![
            "alopex",
            "--in-memory",
            "hnsw",
            "create",
            "my_index",
            "--dim",
            "128",
            "--metric",
            "l2",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Hnsw {
                command: HnswCommand::Create { name, dim, metric }
            } if name == "my_index" && dim == 128 && metric == DistanceMetric::L2
        ));
    }

    #[test]
    fn test_parse_hnsw_create_default_metric() {
        let args = vec![
            "alopex",
            "--in-memory",
            "hnsw",
            "create",
            "my_index",
            "--dim",
            "128",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Hnsw {
                command: HnswCommand::Create { name, dim, metric }
            } if name == "my_index" && dim == 128 && metric == DistanceMetric::Cosine
        ));
    }

    #[test]
    fn test_parse_columnar_scan() {
        let args = vec![
            "alopex",
            "--in-memory",
            "columnar",
            "scan",
            "--segment",
            "seg_001",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::Scan { segment, progress }
            } if segment == "seg_001" && !progress
        ));
    }

    #[test]
    fn test_parse_columnar_stats() {
        let args = vec![
            "alopex",
            "--in-memory",
            "columnar",
            "stats",
            "--segment",
            "seg_001",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::Stats { segment }
            } if segment == "seg_001"
        ));
    }

    #[test]
    fn test_parse_columnar_list() {
        let args = vec!["alopex", "--in-memory", "columnar", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::List
            }
        ));
    }

    #[test]
    fn test_parse_columnar_ingest_defaults() {
        let args = vec![
            "alopex",
            "--in-memory",
            "columnar",
            "ingest",
            "--file",
            "data.csv",
            "--table",
            "events",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::Ingest {
                    file,
                    table,
                    delimiter,
                    header,
                    compression,
                    row_group_size,
                }
            } if file == std::path::Path::new("data.csv")
                && table == "events"
                && delimiter == ','
                && header
                && compression == "lz4"
                && row_group_size.is_none()
        ));
    }

    #[test]
    fn test_parse_columnar_ingest_custom_options() {
        let args = vec![
            "alopex",
            "--in-memory",
            "columnar",
            "ingest",
            "--file",
            "data.csv",
            "--table",
            "events",
            "--delimiter",
            ";",
            "--header",
            "false",
            "--compression",
            "zstd",
            "--row-group-size",
            "500",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::Ingest {
                    file,
                    table,
                    delimiter,
                    header,
                    compression,
                    row_group_size,
                }
            } if file == std::path::Path::new("data.csv")
                && table == "events"
                && delimiter == ';'
                && !header
                && compression == "zstd"
                && row_group_size == Some(500)
        ));
    }

    #[test]
    fn test_parse_columnar_index_create() {
        let args = vec![
            "alopex",
            "--in-memory",
            "columnar",
            "index",
            "create",
            "--segment",
            "123:1",
            "--column",
            "col1",
            "--type",
            "bloom",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Command::Columnar {
                command: ColumnarCommand::Index(IndexCommand::Create {
                    segment,
                    column,
                    index_type,
                })
            } if segment == "123:1"
                && column == "col1"
                && index_type == "bloom"
        ));
    }

    #[test]
    fn test_output_format_supports_streaming() {
        assert!(!OutputFormat::Table.supports_streaming());
        assert!(OutputFormat::Json.supports_streaming());
        assert!(OutputFormat::Jsonl.supports_streaming());
        assert!(OutputFormat::Csv.supports_streaming());
        assert!(OutputFormat::Tsv.supports_streaming());
    }

    #[test]
    fn test_default_values() {
        let args = vec!["alopex", "--in-memory", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.output, OutputFormat::Table);
        assert_eq!(cli.thread_mode, ThreadMode::Multi);
        assert!(cli.limit.is_none());
        assert!(!cli.quiet);
        assert!(!cli.verbose);
    }

    #[test]
    fn test_s3_data_dir() {
        let args = vec![
            "alopex",
            "--data-dir",
            "s3://my-bucket/prefix",
            "kv",
            "list",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.data_dir, Some("s3://my-bucket/prefix".to_string()));
    }
}
