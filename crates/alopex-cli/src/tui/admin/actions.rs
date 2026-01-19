//! Admin action dispatcher for lifecycle operations.
#![allow(dead_code)]

use std::collections::HashSet;
use std::io::Write;

use alopex_embedded::Database;

use crate::batch::BatchMode;
use crate::cli::{
    ColumnarCommand, HnswCommand, IndexCommand, KvCommand, OutputFormat, SqlCommand, VectorCommand,
};
use crate::client::http::HttpClient;
use crate::commands::{columnar, hnsw, kv, sql, vector};
use crate::error::{CliError, Result};
use crate::output::formatter::Formatter;
use crate::streaming::writer::StreamingWriter;
use crate::ui::mode::UiMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdminAction {
    Read,
    Create,
    Update,
    Delete,
    Archive,
    Restore,
    Backup,
    Export,
}

pub fn all_actions() -> HashSet<AdminAction> {
    [
        AdminAction::Read,
        AdminAction::Create,
        AdminAction::Update,
        AdminAction::Delete,
        AdminAction::Archive,
        AdminAction::Restore,
        AdminAction::Backup,
        AdminAction::Export,
    ]
    .into_iter()
    .collect()
}

#[derive(Debug)]
pub enum AdminCommand {
    Sql(SqlCommand),
    Kv(KvCommand),
    Vector(VectorCommand),
    Hnsw(HnswCommand),
    Columnar(ColumnarCommand),
}

pub struct AdminRequest {
    pub action: AdminAction,
    pub command: AdminCommand,
    pub limit: Option<usize>,
    pub quiet: bool,
    pub ui_mode: UiMode,
    pub connection_label: String,
    pub output: OutputFormat,
}

pub fn execute_local_action<W: Write>(
    db: &Database,
    batch_mode: &BatchMode,
    request: AdminRequest,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
) -> Result<()> {
    ensure_action_supported(request.action)?;
    ensure_action_matches_command(request.action, &request.command)?;

    match request.command {
        AdminCommand::Sql(cmd) => {
            if request.ui_mode == UiMode::Tui {
                sql::execute_with_formatter(
                    db,
                    cmd,
                    batch_mode,
                    request.ui_mode,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
            } else {
                sql::execute_with_formatter(
                    db,
                    cmd,
                    batch_mode,
                    UiMode::Batch,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
            }
        }
        AdminCommand::Kv(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = kv_columns_for(&cmd);
                kv::execute_tui(
                    db,
                    cmd,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
            } else {
                let columns = kv_columns_for(&cmd);
                let mut streaming = StreamingWriter::new(writer, formatter, columns, request.limit)
                    .with_quiet(request.quiet);
                kv::execute(db, cmd, &mut streaming)
            }
        }
        AdminCommand::Vector(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = vector_columns_for(&cmd);
                vector::execute_tui(
                    db,
                    cmd,
                    batch_mode,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
            } else {
                let columns = vector_columns_for(&cmd);
                let mut streaming = StreamingWriter::new(writer, formatter, columns, request.limit)
                    .with_quiet(request.quiet);
                vector::execute(db, cmd, batch_mode, &mut streaming)
            }
        }
        AdminCommand::Hnsw(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = hnsw_columns_for(&cmd);
                hnsw::execute_tui(
                    db,
                    cmd,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
            } else {
                let columns = hnsw_columns_for(&cmd);
                let mut streaming = StreamingWriter::new(writer, formatter, columns, request.limit)
                    .with_quiet(request.quiet);
                hnsw::execute(db, cmd, &mut streaming)
            }
        }
        AdminCommand::Columnar(cmd) => {
            if request.ui_mode == UiMode::Tui {
                columnar::execute_tui(
                    db,
                    cmd,
                    batch_mode,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
            } else {
                columnar::execute_with_formatter(
                    db,
                    cmd,
                    batch_mode,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
            }
        }
    }
}

pub async fn execute_remote_action<W: Write>(
    client: &HttpClient,
    batch_mode: &BatchMode,
    request: AdminRequest,
    writer: &mut W,
    formatter: Box<dyn Formatter>,
) -> Result<()> {
    ensure_action_supported(request.action)?;
    ensure_action_matches_command(request.action, &request.command)?;

    match request.command {
        AdminCommand::Sql(cmd) => {
            sql::execute_remote_with_formatter(
                client,
                &cmd,
                batch_mode,
                request.ui_mode,
                writer,
                formatter,
                request.limit,
                request.quiet,
            )
            .await
        }
        AdminCommand::Kv(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = kv_columns_for(&cmd);
                kv::execute_remote_tui(
                    client,
                    &cmd,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
                .await
            } else {
                kv::execute_remote_with_formatter(
                    client,
                    &cmd,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
                .await
            }
        }
        AdminCommand::Vector(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = vector_columns_for(&cmd);
                vector::execute_remote_tui(
                    client,
                    &cmd,
                    batch_mode,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
                .await
            } else {
                vector::execute_remote_with_formatter(
                    client,
                    &cmd,
                    batch_mode,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
                .await
            }
        }
        AdminCommand::Hnsw(cmd) => {
            if request.ui_mode == UiMode::Tui {
                let columns = hnsw_columns_for(&cmd);
                hnsw::execute_remote_tui(
                    client,
                    &cmd,
                    columns,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
                .await
            } else {
                hnsw::execute_remote_with_formatter(
                    client,
                    &cmd,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
                .await
            }
        }
        AdminCommand::Columnar(cmd) => {
            if request.ui_mode == UiMode::Tui {
                columnar::execute_remote_tui(
                    client,
                    &cmd,
                    batch_mode,
                    request.limit,
                    request.quiet,
                    request.connection_label,
                )
                .await
            } else {
                columnar::execute_remote_with_formatter(
                    client,
                    &cmd,
                    batch_mode,
                    writer,
                    formatter,
                    request.limit,
                    request.quiet,
                )
                .await
            }
        }
    }
}

fn ensure_action_supported(action: AdminAction) -> Result<()> {
    if matches!(
        action,
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export
    ) {
        return Err(CliError::InvalidArgument(format!(
            "Admin action '{}' is not implemented yet.",
            action_label(action)
        )));
    }
    Ok(())
}

fn ensure_action_matches_command(action: AdminAction, command: &AdminCommand) -> Result<()> {
    let ok = match command {
        AdminCommand::Sql(_) => matches!(
            action,
            AdminAction::Read | AdminAction::Create | AdminAction::Update | AdminAction::Delete
        ),
        AdminCommand::Kv(cmd) => matches_kv_action(action, cmd),
        AdminCommand::Vector(cmd) => matches_vector_action(action, cmd),
        AdminCommand::Hnsw(cmd) => matches_hnsw_action(action, cmd),
        AdminCommand::Columnar(cmd) => matches_columnar_action(action, cmd),
    };

    if ok {
        Ok(())
    } else {
        Err(CliError::InvalidArgument(format!(
            "Admin action '{}' does not match the selected command.",
            action_label(action)
        )))
    }
}

fn matches_kv_action(action: AdminAction, command: &KvCommand) -> bool {
    match command {
        KvCommand::Get { .. } | KvCommand::List { .. } | KvCommand::Txn(_) => {
            matches!(action, AdminAction::Read)
        }
        KvCommand::Put { .. } => matches!(action, AdminAction::Create | AdminAction::Update),
        KvCommand::Delete { .. } => matches!(action, AdminAction::Delete),
    }
}

fn matches_vector_action(action: AdminAction, command: &VectorCommand) -> bool {
    match command {
        VectorCommand::Search { .. } => matches!(action, AdminAction::Read),
        VectorCommand::Upsert { .. } => matches!(action, AdminAction::Create | AdminAction::Update),
        VectorCommand::Delete { .. } => matches!(action, AdminAction::Delete),
    }
}

fn matches_hnsw_action(action: AdminAction, command: &HnswCommand) -> bool {
    match command {
        HnswCommand::Stats { .. } => matches!(action, AdminAction::Read),
        HnswCommand::Create { .. } => matches!(action, AdminAction::Create),
        HnswCommand::Drop { .. } => matches!(action, AdminAction::Delete),
    }
}

fn matches_columnar_action(action: AdminAction, command: &ColumnarCommand) -> bool {
    match command {
        ColumnarCommand::Scan { .. }
        | ColumnarCommand::Stats { .. }
        | ColumnarCommand::List
        | ColumnarCommand::Index(IndexCommand::List { .. }) => {
            matches!(action, AdminAction::Read)
        }
        ColumnarCommand::Ingest { .. } | ColumnarCommand::Index(IndexCommand::Create { .. }) => {
            matches!(action, AdminAction::Create)
        }
        ColumnarCommand::Index(IndexCommand::Drop { .. }) => {
            matches!(action, AdminAction::Delete)
        }
    }
}

fn kv_columns_for(cmd: &KvCommand) -> Vec<crate::models::Column> {
    match cmd {
        KvCommand::Put { .. } | KvCommand::Delete { .. } => kv::kv_status_columns(),
        KvCommand::Get { .. } | KvCommand::List { .. } => kv::kv_columns(),
        KvCommand::Txn(_) => kv::kv_columns(),
    }
}

fn vector_columns_for(cmd: &VectorCommand) -> Vec<crate::models::Column> {
    match cmd {
        VectorCommand::Search { .. } => vector::vector_search_columns(),
        VectorCommand::Upsert { .. } | VectorCommand::Delete { .. } => {
            vector::vector_status_columns()
        }
    }
}

fn hnsw_columns_for(cmd: &HnswCommand) -> Vec<crate::models::Column> {
    match cmd {
        HnswCommand::Stats { .. } => hnsw::hnsw_stats_columns(),
        HnswCommand::Create { .. } | HnswCommand::Drop { .. } => hnsw::hnsw_status_columns(),
    }
}

fn action_label(action: AdminAction) -> &'static str {
    match action {
        AdminAction::Read => "read",
        AdminAction::Create => "create",
        AdminAction::Update => "update",
        AdminAction::Delete => "delete",
        AdminAction::Archive => "archive",
        AdminAction::Restore => "restore",
        AdminAction::Backup => "backup",
        AdminAction::Export => "export",
    }
}
