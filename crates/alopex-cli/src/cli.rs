//! CLI Parser - Command-line argument parsing with clap
//!
//! This module defines the CLI structure using clap derive macros.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use serde::{Deserialize, Serialize};

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
    #[arg(long, value_enum)]
    pub output: Option<OutputFormat>,

    /// Limit the number of output rows
    #[arg(long)]
    pub limit: Option<usize>,

    /// Suppress informational messages
    #[arg(long)]
    pub quiet: bool,

    /// Enable verbose output (includes stack traces for errors)
    #[arg(long)]
    pub verbose: bool,

    /// Allow insecure HTTP connections for server profiles
    #[arg(long)]
    pub insecure: bool,

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
    pub command: Option<Command>,
}

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table format
    Table,
    /// JSON array format (sql: array of per-statement result sets)
    Json,
    /// JSON Lines format (one JSON object per line)
    Jsonl,
    /// CSV format (RFC 4180)
    Csv,
    /// TSV format (tab-separated values)
    Tsv,
}

/// Requested SQL read routing mode. `local` remains the compatibility default;
/// all other modes require an explicitly configured cluster profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SqlReadMode {
    #[default]
    Local,
    Inherit,
    Strong,
    Stale,
}

/// Requested format for the distributed-read routing report. The report is
/// emitted by the later output adapter and is intentionally separate from
/// query stdout formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutingReportFormat {
    Human,
    Json,
}

impl OutputFormat {
    /// Returns true if this format supports streaming output.
    #[allow(dead_code)]
    pub fn supports_streaming(&self) -> bool {
        matches!(self, Self::Json | Self::Jsonl | Self::Csv | Self::Tsv)
    }
}

impl Cli {
    pub fn output_format(&self) -> OutputFormat {
        self.output.unwrap_or(OutputFormat::Table)
    }

    pub fn output_is_explicit(&self) -> bool {
        self.output.is_some()
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
        command: Option<ProfileCommand>,
    },
    /// Key-Value operations
    Kv {
        #[command(subcommand)]
        command: Option<KvCommand>,
    },
    /// SQL query execution
    Sql(SqlCommand),
    /// Vector operations
    Vector {
        #[command(subcommand)]
        command: Option<VectorCommand>,
    },
    /// HNSW index management
    Hnsw {
        #[command(subcommand)]
        command: Option<HnswCommand>,
    },
    /// Columnar segment operations
    Columnar {
        #[command(subcommand)]
        command: Option<ColumnarCommand>,
    },
    /// Server management commands
    Server {
        #[command(subcommand)]
        command: Option<ServerCommand>,
    },
    /// Data lifecycle management commands
    Lifecycle {
        #[command(subcommand)]
        command: Option<LifecycleCommand>,
    },
    /// Conflict-free replicated data type operations
    Crdt {
        #[command(subcommand)]
        command: Option<CrdtCommand>,
    },
    /// Durable changefeed lifecycle operations against a server profile
    Changefeed {
        #[command(subcommand)]
        command: ChangefeedCommand,
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

/// CRDT object commands.
#[derive(Subcommand, Debug)]
pub enum CrdtCommand {
    /// Counter operations
    Counter {
        #[command(subcommand)]
        command: Option<CounterCommand>,
    },
    /// Set operations
    Set {
        #[command(subcommand)]
        command: Option<SetCommand>,
    },
}

/// Durable changefeed lifecycle subcommands.
///
/// Changefeeds are only available through an explicitly configured server
/// profile. The server derives the authenticated actor; the CLI deliberately
/// does not expose an actor flag that could disagree with that identity.
#[derive(Subcommand, Debug)]
pub enum ChangefeedCommand {
    /// Create a feed for exactly one table or range target
    Create {
        /// Table target (mutually exclusive with --range)
        #[arg(long, required_unless_present = "range", conflicts_with = "range")]
        table: Option<String>,
        /// Explicit range target (mutually exclusive with --table)
        #[arg(long, required_unless_present = "table", conflicts_with = "table")]
        range: Option<String>,
        /// Tenant context checked by the server-established authorization policy
        #[arg(long)]
        tenant: String,
        /// Stable idempotency identity for this create operation
        #[arg(long)]
        request_id: String,
        /// Optional retention deadline represented in the wire contract epoch
        #[arg(long)]
        deadline: Option<u64>,
        /// Requested result format; rendering is applied by the output adapter
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },
    /// Subscribe using the generation and epoch observed by the consumer
    Subscribe {
        /// Feed identity returned by create
        #[arg(long)]
        feed_id: String,
        /// Stable idempotency identity for this subscribe operation
        #[arg(long)]
        request_id: String,
        /// Expected committed range generation
        #[arg(long)]
        generation: u64,
        /// Expected committed range data epoch
        #[arg(long)]
        epoch: u64,
        /// Requested result format; rendering is applied by the output adapter
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },
    /// Fetch one bounded event delivery batch
    Poll {
        #[command(flatten)]
        request: ChangefeedDeliveryRequest,
    },
    /// Fetch one streaming event delivery batch
    Stream {
        #[command(flatten)]
        request: ChangefeedDeliveryRequest,
        /// Keep the CLI stream presentation open when the output adapter supports it
        #[arg(long)]
        follow: bool,
    },
    /// Acknowledge a feed checkpoint
    Ack {
        #[command(flatten)]
        request: ChangefeedCheckpointRequest,
        /// Stable acknowledgement identity
        #[arg(long)]
        ack_id: String,
    },
    /// Resume strictly after an acknowledged checkpoint
    Resume {
        #[command(flatten)]
        request: ChangefeedCheckpointRequest,
    },
    /// Cancel a feed without silently advancing its checkpoint
    Cancel {
        #[command(flatten)]
        request: ChangefeedLifecycleRequest,
    },
    /// Close a feed through its terminal lifecycle transition
    Close {
        #[command(flatten)]
        request: ChangefeedLifecycleRequest,
    },
}

/// Shared bounded-delivery input for poll and stream.
#[derive(Args, Debug)]
pub struct ChangefeedDeliveryRequest {
    /// Feed identity returned by create
    #[arg(long)]
    pub feed_id: String,
    /// Stable idempotency identity for this delivery operation
    #[arg(long)]
    pub request_id: String,
    /// Maximum number of events to deliver in this response
    #[arg(long)]
    pub max_events: usize,
    /// Caller deadline represented in the wire contract epoch
    #[arg(long)]
    pub deadline: u64,
    /// Requested result format; rendering is applied by the output adapter
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

/// Shared checkpoint input for acknowledgement and resume.
#[derive(Args, Debug)]
pub struct ChangefeedCheckpointRequest {
    /// Feed identity returned by create
    #[arg(long)]
    pub feed_id: String,
    /// Stable idempotency identity for this operation
    #[arg(long)]
    pub request_id: String,
    /// Encoded checkpoint returned by a prior delivery
    #[arg(long)]
    pub checkpoint: String,
    /// Requested result format; rendering is applied by the output adapter
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

/// Shared input for terminal lifecycle operations.
#[derive(Args, Debug)]
pub struct ChangefeedLifecycleRequest {
    /// Feed identity returned by create
    #[arg(long)]
    pub feed_id: String,
    /// Stable idempotency identity for this operation
    #[arg(long)]
    pub request_id: String,
    /// Requested result format; rendering is applied by the output adapter
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

/// Counter CRDT commands.
#[derive(Subcommand, Debug)]
pub enum CounterCommand {
    /// Create a Counter with an explicit durable operation identity
    Create {
        /// Logical Counter identifier
        #[arg(long)]
        object_id: String,
        /// Cluster identity that owns the Counter range
        #[arg(long)]
        cluster_id: String,
        /// Numeric table identity for the Counter range
        #[arg(long)]
        table_id: u32,
        /// Committed range identity
        #[arg(long)]
        range_id: String,
        /// Schema version observed for the range
        #[arg(long)]
        schema_version: u64,
        /// Data epoch observed for the range
        #[arg(long)]
        data_epoch: u64,
        /// Request identity; required for every mutation
        #[arg(long)]
        request_id: String,
        /// Operation identity; required for every mutation
        #[arg(long)]
        operation_id: String,
        /// F1 epoch-scoped update version
        #[arg(long)]
        update_version: u64,
        /// Initial signed Counter value (zero and negative values are valid)
        #[arg(long, allow_hyphen_values = true)]
        initial_value: i64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Read a Counter without adding a mutation to its durable operation ledger
    Read {
        /// Logical Counter identifier
        #[arg(long)]
        object_id: String,
        /// Cluster identity that owns the Counter range
        #[arg(long)]
        cluster_id: String,
        /// Numeric table identity for the Counter range
        #[arg(long)]
        table_id: u32,
        /// Committed range identity
        #[arg(long)]
        range_id: String,
        /// Schema version observed for the range
        #[arg(long)]
        schema_version: u64,
        /// Data epoch observed for the range
        #[arg(long)]
        data_epoch: u64,
        /// Request identity retained for read correlation
        #[arg(long)]
        request_id: String,
        /// Operation identity retained for read correlation
        #[arg(long)]
        operation_id: String,
        /// F1 epoch-scoped update version
        #[arg(long)]
        update_version: u64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Apply a signed delta to an existing Counter with a durable operation identity
    Increment {
        /// Logical Counter identifier
        #[arg(long)]
        object_id: String,
        /// Cluster identity that owns the Counter range
        #[arg(long)]
        cluster_id: String,
        /// Numeric table identity for the Counter range
        #[arg(long)]
        table_id: u32,
        /// Committed range identity
        #[arg(long)]
        range_id: String,
        /// Schema version observed for the range
        #[arg(long)]
        schema_version: u64,
        /// Data epoch observed for the range
        #[arg(long)]
        data_epoch: u64,
        /// Request identity; required for every mutation
        #[arg(long)]
        request_id: String,
        /// Operation identity; required for every mutation
        #[arg(long)]
        operation_id: String,
        /// F1 epoch-scoped update version
        #[arg(long)]
        update_version: u64,
        /// Signed amount to add to the Counter
        #[arg(long, allow_hyphen_values = true)]
        delta: i64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Apply a signed delta to subtract from an existing Counter with a durable operation identity
    Decrement {
        /// Logical Counter identifier
        #[arg(long)]
        object_id: String,
        /// Cluster identity that owns the Counter range
        #[arg(long)]
        cluster_id: String,
        /// Numeric table identity for the Counter range
        #[arg(long)]
        table_id: u32,
        /// Committed range identity
        #[arg(long)]
        range_id: String,
        /// Schema version observed for the range
        #[arg(long)]
        schema_version: u64,
        /// Data epoch observed for the range
        #[arg(long)]
        data_epoch: u64,
        /// Request identity; required for every mutation
        #[arg(long)]
        request_id: String,
        /// Operation identity; required for every mutation
        #[arg(long)]
        operation_id: String,
        /// F1 epoch-scoped update version
        #[arg(long)]
        update_version: u64,
        /// Signed amount to subtract from the Counter
        #[arg(long, allow_hyphen_values = true)]
        delta: i64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
}

/// Set CRDT commands.
#[derive(Subcommand, Debug)]
pub enum SetCommand {
    /// Create an empty Set with an explicit durable operation identity
    Create {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Add one canonical member to a Set with an explicit durable operation identity
    Add {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        #[arg(long)]
        member: String,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Remove one canonical member from a Set with an explicit durable operation identity
    Remove {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        #[arg(long)]
        member: String,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Check one canonical Set member without mutating the durable projection
    Contains {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        #[arg(long)]
        member: String,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// List canonical Set members without mutating the durable projection
    List {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
    },
    /// Read a Set without adding a mutation to its durable operation ledger
    Read {
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        table_id: u32,
        #[arg(long)]
        range_id: String,
        #[arg(long)]
        schema_version: u64,
        #[arg(long)]
        data_epoch: u64,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        update_version: u64,
        /// Local actor identity. Remote requests derive this from transport authentication.
        #[arg(long, default_value = "alopex-cli-local")]
        actor: String,
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
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Get a value within a transaction
    Get {
        /// The key to retrieve
        key: String,
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
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
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Delete a key within a transaction
    Delete {
        /// The key to delete
        key: String,
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Commit a transaction
    Commit {
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Roll back a transaction
    Rollback {
        /// Transaction ID
        #[arg(long)]
        txn_id: String,
        /// Stable request identity for retry-safe transaction execution
        #[arg(long)]
        request_id: Option<String>,
    },
}

/// SQL subcommand
///
/// Multiple `;`-separated statements are executed in a single transaction and
/// each statement emits its own result block. With `--output json` the output
/// is always an array of per-statement result sets (a single statement yields
/// a 1-element array); DDL/DML statements contribute a `status`/`message`
/// result set unless `--quiet` is set.
#[derive(Parser, Debug)]
pub struct SqlCommand {
    /// SQL query to execute (may contain multiple `;`-separated statements)
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

    /// Stable request identity for retry-safe SQL transaction execution
    #[arg(long)]
    pub request_id: Option<String>,

    /// Read routing mode. Non-local modes require an explicit cluster profile.
    #[arg(long, value_enum)]
    pub read_mode: Option<SqlReadMode>,

    /// Emit a distributed-read routing report to stderr without changing SQL
    /// row output on stdout.
    #[arg(long, value_enum)]
    pub routing_report: Option<RoutingReportFormat>,

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
        #[arg(long, default_value = "zstd")]
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
    /// Join the configured cluster membership
    Join,
    /// Leave the configured cluster membership
    Leave,
    /// Server compaction management
    Compaction {
        #[command(subcommand)]
        command: CompactionCommand,
    },
    /// Cluster metadata management commands
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
}

/// Shared operation identity and optimistic-concurrency input for cluster
/// management requests. The request ID is deliberately operator-supplied so a
/// retry can use the same idempotency key.
#[derive(Args, Debug)]
pub struct ClusterOperationRequest {
    /// Stable operation ID used for idempotency and status correlation
    #[arg(long, value_name = "REQUEST_ID")]
    pub request_id: String,
    /// Expected committed metadata version
    #[arg(long)]
    pub expected_version: Option<u64>,
}

/// Read operation which addresses a specific public metadata target.
#[derive(Args, Debug)]
pub struct ClusterTargetedReadRequest {
    #[command(flatten)]
    pub operation: ClusterOperationRequest,
    /// Public target encoded as JSON
    #[arg(long, value_name = "JSON")]
    pub target: String,
}

/// Mutation input. A target and explicit confirmation are kept in the grammar
/// instead of being inferred from a positional argument or an interactive
/// prompt, so automation and the HTTP contract cannot disagree.
#[derive(Args, Debug)]
pub struct ClusterMutationRequest {
    #[command(flatten)]
    pub operation: ClusterOperationRequest,
    /// Public mutation target encoded as JSON
    #[arg(long, value_name = "JSON")]
    pub target: String,
    /// Confirm this cluster metadata mutation
    #[arg(long, required = true)]
    pub confirm: bool,
}

/// Cluster metadata management areas.
#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
    /// Inspect cluster metadata control availability
    Metadata {
        #[command(subcommand)]
        command: ClusterMetadataCommand,
    },
    /// Manage committed members
    #[command(visible_alias = "member")]
    Members {
        #[command(subcommand)]
        command: ClusterMembersCommand,
    },
    /// Inspect and manage registered ranges
    #[command(visible_alias = "range")]
    Ranges {
        #[command(subcommand)]
        command: ClusterRangesCommand,
    },
    /// Inspect and manage range placement
    Placement {
        #[command(subcommand)]
        command: ClusterPlacementCommand,
    },
    /// Inspect and manage the cluster read policy
    ReadPolicy {
        #[command(subcommand)]
        command: ClusterReadPolicyCommand,
    },
    /// Inspect and manage schema ownership and rollout
    Schema {
        #[command(subcommand)]
        command: ClusterSchemaCommand,
    },
    /// Inspect or run recovery management operations
    Recovery {
        #[command(subcommand)]
        command: ClusterRecoveryCommand,
    },
    /// Inspect or start a resumable upgrade
    Upgrade {
        #[command(subcommand)]
        command: ClusterUpgradeCommand,
    },
}

/// Metadata inspection commands.
#[derive(Subcommand, Debug)]
pub enum ClusterMetadataCommand {
    /// Show committed metadata control status
    Show {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
}

/// Member management commands.
#[derive(Subcommand, Debug)]
pub enum ClusterMembersCommand {
    /// List committed members
    List {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Replace a member using an explicit public target
    Replace {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Range management commands.
#[derive(Subcommand, Debug)]
pub enum ClusterRangesCommand {
    /// List committed ranges
    List {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Show one range using an explicit public target
    Show {
        #[command(flatten)]
        request: ClusterTargetedReadRequest,
    },
    /// Register a provisioned range using an explicit public target
    Register {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
    /// Update range metadata using an explicit public target
    Update {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
    /// Retire a range using an explicit public target
    Retire {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Range placement commands.
#[derive(Subcommand, Debug)]
pub enum ClusterPlacementCommand {
    /// Get placement for an explicit range target
    Get {
        #[command(flatten)]
        request: ClusterTargetedReadRequest,
    },
    /// Set placement using an explicit public target
    Set {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
    /// Replace placement using an explicit public target
    Replace {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Cluster read-policy commands.
#[derive(Subcommand, Debug)]
pub enum ClusterReadPolicyCommand {
    /// Get the committed read policy
    Get {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Set the committed read policy using an explicit public target
    Set {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Cluster schema ownership and rollout commands.
#[derive(Subcommand, Debug)]
pub enum ClusterSchemaCommand {
    /// Inspect schema owner
    Owner {
        #[command(subcommand)]
        command: ClusterSchemaOwnerCommand,
    },
    /// Inspect or start schema rollout
    Rollout {
        #[command(subcommand)]
        command: ClusterSchemaRolloutCommand,
    },
}

/// Schema ownership commands.
#[derive(Subcommand, Debug)]
pub enum ClusterSchemaOwnerCommand {
    /// Get the committed schema owner
    Get {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Set the committed schema owner using an explicit public target
    Set {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Schema rollout commands.
#[derive(Subcommand, Debug)]
pub enum ClusterSchemaRolloutCommand {
    /// Start a schema rollout using an explicit public target
    Start {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
    /// Get schema rollout status
    Status {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
}

/// Recovery management commands.
#[derive(Subcommand, Debug)]
pub enum ClusterRecoveryCommand {
    /// Get recovery status
    Status {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Restore from an explicit public target
    Restore {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Upgrade management commands.
#[derive(Subcommand, Debug)]
pub enum ClusterUpgradeCommand {
    /// Get resumable upgrade status
    Status {
        #[command(flatten)]
        request: ClusterOperationRequest,
    },
    /// Start an upgrade from an explicit public target
    Start {
        #[command(flatten)]
        request: ClusterMutationRequest,
    },
}

/// Lifecycle subcommands
#[derive(Subcommand, Debug)]
pub enum LifecycleCommand {
    /// Archive data (placeholder)
    Archive,
    /// Restore archived data (placeholder)
    Restore {
        /// Restore source (server mode only)
        #[arg(long)]
        source: Option<String>,
        /// Restore subcommands
        #[command(subcommand)]
        command: Option<LifecycleRestoreCommand>,
    },
    /// Backup data (placeholder)
    Backup {
        /// Backup subcommands
        #[command(subcommand)]
        command: Option<LifecycleBackupCommand>,
    },
    /// Export data (placeholder)
    Export,
}

/// Backup lifecycle subcommands
#[derive(Subcommand, Debug)]
pub enum LifecycleBackupCommand {
    /// Show backup status for a handle
    Status {
        /// Backup handle
        #[arg(long)]
        handle: String,
    },
}

/// Restore lifecycle subcommands
#[derive(Subcommand, Debug)]
pub enum LifecycleRestoreCommand {
    /// Show restore status for a handle
    Status {
        /// Restore handle
        #[arg(long)]
        handle: String,
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
        assert_eq!(cli.output_format(), OutputFormat::Table);
        assert!(matches!(
            cli.command,
            Some(Command::Kv {
                command: Some(KvCommand::Get { key })
            }) if key == "mykey"
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
            Some(Command::Sql(SqlCommand { query: Some(q), file: None, .. })) if q == "SELECT * FROM users"
        ));
    }

    #[test]
    fn test_parse_output_format() {
        let args = vec!["alopex", "--in-memory", "--output", "jsonl", "kv", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert_eq!(cli.output_format(), OutputFormat::Jsonl);
        assert!(cli.output_is_explicit());
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
            Some(Command::Sql(cmd)) => {
                assert_eq!(cmd.fetch_size, Some(500));
                assert_eq!(cmd.max_rows, Some(250));
                assert_eq!(cmd.deadline.as_deref(), Some("30s"));
                assert!(!cmd.tui);
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn test_parse_sql_distributed_read_options() {
        let args = vec![
            "alopex",
            "sql",
            "--read-mode",
            "stale",
            "--routing-report",
            "json",
            "SELECT 1",
        ];
        let cli = Cli::try_parse_from(args).unwrap();

        match cli.command {
            Some(Command::Sql(cmd)) => {
                assert_eq!(cmd.read_mode, Some(SqlReadMode::Stale));
                assert_eq!(cmd.routing_report, Some(RoutingReportFormat::Json));
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn test_parse_sql_tui_flag() {
        let args = vec!["alopex", "sql", "--tui", "SELECT 1"];
        let cli = Cli::try_parse_from(args).unwrap();

        match cli.command {
            Some(Command::Sql(cmd)) => {
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
            Some(Command::Server {
                command: Some(ServerCommand::Status)
            })
        ));
    }

    #[test]
    fn test_parse_server_compaction_trigger() {
        let args = vec!["alopex", "server", "compaction", "trigger"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Server {
                command: Some(ServerCommand::Compaction {
                    command: CompactionCommand::Trigger
                })
            })
        ));
    }

    #[test]
    fn test_parse_server_join_leave() {
        let join = Cli::try_parse_from(vec!["alopex", "server", "join"]).unwrap();
        assert!(matches!(
            join.command,
            Some(Command::Server {
                command: Some(ServerCommand::Join)
            })
        ));

        let leave = Cli::try_parse_from(vec!["alopex", "server", "leave"]).unwrap();
        assert!(matches!(
            leave.command,
            Some(Command::Server {
                command: Some(ServerCommand::Leave)
            })
        ));
    }

    #[test]
    fn test_parse_changefeed_lifecycle_commands_and_required_options() {
        let create = Cli::try_parse_from([
            "alopex",
            "changefeed",
            "create",
            "--table",
            "orders",
            "--tenant",
            "tenant-a",
            "--request-id",
            "create-1",
            "--deadline",
            "99",
            "--format",
            "json",
        ])
        .expect("create parses");
        assert!(matches!(
            create.command,
            Some(Command::Changefeed {
                command: ChangefeedCommand::Create {
                    table: Some(table),
                    range: None,
                    tenant,
                    request_id,
                    deadline: Some(99),
                    format: Some(OutputFormat::Json),
                }
            }) if table == "orders" && tenant == "tenant-a" && request_id == "create-1"
        ));

        let subscribe = Cli::try_parse_from([
            "alopex",
            "changefeed",
            "subscribe",
            "--feed-id",
            "feed-a",
            "--request-id",
            "sub-1",
            "--generation",
            "7",
            "--epoch",
            "11",
        ])
        .expect("subscribe parses");
        assert!(matches!(
            subscribe.command,
            Some(Command::Changefeed {
                command: ChangefeedCommand::Subscribe {
                    generation: 7,
                    epoch: 11,
                    ..
                }
            })
        ));

        let poll = Cli::try_parse_from([
            "alopex",
            "changefeed",
            "poll",
            "--feed-id",
            "feed-a",
            "--request-id",
            "poll-1",
            "--max-events",
            "2",
            "--deadline",
            "11",
        ])
        .expect("poll parses");
        assert!(matches!(
            poll.command,
            Some(Command::Changefeed {
                command: ChangefeedCommand::Poll { request }
            }) if request.max_events == 2 && request.deadline == 11
        ));

        let stream = Cli::try_parse_from([
            "alopex",
            "changefeed",
            "stream",
            "--feed-id",
            "feed-a",
            "--request-id",
            "stream-1",
            "--max-events",
            "3",
            "--deadline",
            "12",
            "--follow",
            "--format",
            "jsonl",
        ])
        .expect("stream parses");
        assert!(matches!(
            stream.command,
            Some(Command::Changefeed {
                command: ChangefeedCommand::Stream { follow: true, .. }
            })
        ));

        let ack = Cli::try_parse_from([
            "alopex",
            "changefeed",
            "ack",
            "--feed-id",
            "feed-a",
            "--request-id",
            "ack-1",
            "--ack-id",
            "ack-record-1",
            "--checkpoint",
            "checkpoint-a",
        ])
        .expect("ack parses");
        assert!(matches!(
            ack.command,
            Some(Command::Changefeed {
                command: ChangefeedCommand::Ack { ack_id, .. }
            }) if ack_id == "ack-record-1"
        ));

        for args in [
            vec![
                "alopex",
                "changefeed",
                "resume",
                "--feed-id",
                "feed-a",
                "--request-id",
                "resume-1",
                "--checkpoint",
                "checkpoint-a",
            ],
            vec![
                "alopex",
                "changefeed",
                "cancel",
                "--feed-id",
                "feed-a",
                "--request-id",
                "cancel-1",
            ],
            vec![
                "alopex",
                "changefeed",
                "close",
                "--feed-id",
                "feed-a",
                "--request-id",
                "close-1",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_ok());
        }

        assert!(Cli::try_parse_from([
            "alopex",
            "changefeed",
            "create",
            "--tenant",
            "tenant-a",
            "--request-id",
            "create-1",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "alopex",
            "changefeed",
            "create",
            "--table",
            "orders",
            "--range",
            "range-a",
            "--tenant",
            "tenant-a",
            "--request-id",
            "create-1",
        ])
        .is_err());
    }

    #[test]
    fn test_parse_cluster_mutation_requires_explicit_target_and_confirmation() {
        let args = vec![
            "alopex",
            "server",
            "cluster",
            "ranges",
            "register",
            "--request-id",
            "range-register-1",
            "--expected-version",
            "8",
            "--target",
            r#"{"range_id":"primary/0"}"#,
            "--confirm",
        ];
        let cli = Cli::try_parse_from(args).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Server {
                command: Some(ServerCommand::Cluster {
                    command: ClusterCommand::Ranges {
                        command: ClusterRangesCommand::Register { request }
                    }
                })
            }) if request.operation.request_id == "range-register-1"
                && request.operation.expected_version == Some(8)
                && request.target == r#"{"range_id":"primary/0"}"#
                && request.confirm
        ));

        assert!(Cli::try_parse_from([
            "alopex",
            "server",
            "cluster",
            "ranges",
            "register",
            "--request-id",
            "range-register-1",
            "--confirm",
        ])
        .is_err());

        assert!(Cli::try_parse_from([
            "alopex",
            "server",
            "cluster",
            "ranges",
            "register",
            "--request-id",
            "range-register-1",
            "--target",
            r#"{"range_id":"primary/0"}"#,
        ])
        .is_err());
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
            Some(Command::Profile {
                command: Some(ProfileCommand::Create { name, data_dir })
            })
                if name == "dev" && data_dir == "/path/to/db"
        ));
    }

    #[test]
    fn test_parse_completions_bash() {
        let args = vec!["alopex", "completions", "bash"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Completions { shell }) if shell == Shell::Bash
        ));
    }

    #[test]
    fn test_parse_completions_pwsh() {
        let args = vec!["alopex", "completions", "pwsh"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Completions { shell }) if shell == Shell::PowerShell
        ));
    }

    #[test]
    fn test_parse_kv_put() {
        let args = vec!["alopex", "--in-memory", "kv", "put", "mykey", "myvalue"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Kv {
                command: Some(KvCommand::Put { key, value })
            }) if key == "mykey" && value == "myvalue"
        ));
    }

    #[test]
    fn test_parse_kv_delete() {
        let args = vec!["alopex", "--in-memory", "kv", "delete", "mykey"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Kv {
                command: Some(KvCommand::Delete { key })
            }) if key == "mykey"
        ));
    }

    #[test]
    fn test_parse_kv_txn_begin() {
        let args = vec!["alopex", "kv", "txn", "begin", "--timeout-secs", "30"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Kv {
                command: Some(KvCommand::Txn(KvTxnCommand::Begin {
                    timeout_secs: Some(30),
                    ..
                }))
            })
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
            Some(Command::Kv {
                command: Some(KvCommand::Txn(KvTxnCommand::Get { key, txn_id, .. }))
            }) if key == "mykey" && txn_id == "txn123"
        ));
    }

    #[test]
    fn test_parse_kv_list_with_prefix() {
        let args = vec!["alopex", "--in-memory", "kv", "list", "--prefix", "user:"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Kv {
                command: Some(KvCommand::List { prefix: Some(p) })
            }) if p == "user:"
        ));
    }

    #[test]
    fn test_parse_sql_from_file() {
        let args = vec!["alopex", "--in-memory", "sql", "-f", "query.sql"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Sql(SqlCommand { query: None, file: Some(f), .. })) if f == "query.sql"
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
            Some(Command::Vector {
                command: Some(VectorCommand::Search { index, query, k, progress })
            }) if index == "my_index" && query == "[1.0,2.0,3.0]" && k == 5 && !progress
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
            Some(Command::Vector {
                command: Some(VectorCommand::Upsert { index, key, vector })
            }) if index == "my_index" && key == "vec1" && vector == "[1.0,2.0,3.0]"
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
            Some(Command::Vector {
                command: Some(VectorCommand::Delete { index, key })
            }) if index == "my_index" && key == "vec1"
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
            Some(Command::Hnsw {
                command: Some(HnswCommand::Create { name, dim, metric })
            }) if name == "my_index" && dim == 128 && metric == DistanceMetric::L2
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
            Some(Command::Hnsw {
                command: Some(HnswCommand::Create { name, dim, metric })
            }) if name == "my_index" && dim == 128 && metric == DistanceMetric::Cosine
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
            Some(Command::Columnar {
                command: Some(ColumnarCommand::Scan { segment, progress })
            }) if segment == "seg_001" && !progress
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
            Some(Command::Columnar {
                command: Some(ColumnarCommand::Stats { segment })
            }) if segment == "seg_001"
        ));
    }

    #[test]
    fn test_parse_columnar_list() {
        let args = vec!["alopex", "--in-memory", "columnar", "list"];
        let cli = Cli::try_parse_from(args).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Columnar {
                command: Some(ColumnarCommand::List)
            })
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
            Some(Command::Columnar {
                command: Some(ColumnarCommand::Ingest {
                    file,
                    table,
                    delimiter,
                    header,
                    compression,
                    row_group_size,
                })
            }) if file == std::path::Path::new("data.csv")
                && table == "events"
                && delimiter == ','
                && header
                && compression == "zstd"
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
            Some(Command::Columnar {
                command: Some(ColumnarCommand::Ingest {
                    file,
                    table,
                    delimiter,
                    header,
                    compression,
                    row_group_size,
                })
            }) if file == std::path::Path::new("data.csv")
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
            Some(Command::Columnar {
                command: Some(ColumnarCommand::Index(IndexCommand::Create {
                    segment,
                    column,
                    index_type,
                }))
            }) if segment == "123:1"
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

        assert_eq!(cli.output_format(), OutputFormat::Table);
        assert!(!cli.output_is_explicit());
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
