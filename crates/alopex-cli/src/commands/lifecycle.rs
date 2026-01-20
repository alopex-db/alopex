//! Lifecycle command handlers.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{LifecycleCommand, OutputFormat};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};

pub fn execute_with_formatter<W: Write>(
    command: &LifecycleCommand,
    data_dir: Option<&Path>,
    writer: &mut W,
    mut formatter: Box<dyn Formatter>,
) -> Result<()> {
    let message = perform_lifecycle_action(command, data_dir)?;
    let columns = vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text("OK".to_string()),
        Value::Text(message),
    ])];
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
}

pub fn execute<W: Write>(
    command: &LifecycleCommand,
    data_dir: Option<&Path>,
    writer: &mut W,
    output: OutputFormat,
) -> Result<()> {
    let formatter = create_formatter(output);
    execute_with_formatter(command, data_dir, writer, formatter)
}

fn perform_lifecycle_action(command: &LifecycleCommand, data_dir: Option<&Path>) -> Result<String> {
    let data_dir = data_dir.ok_or_else(|| {
        CliError::InvalidArgument("Lifecycle actions require a local data directory.".to_string())
    })?;
    if !data_dir.exists() {
        return Err(CliError::InvalidArgument(format!(
            "Data directory does not exist: {}",
            data_dir.display()
        )));
    }
    if !data_dir.is_dir() {
        return Err(CliError::InvalidArgument(format!(
            "Data directory is not a directory: {}",
            data_dir.display()
        )));
    }

    let lifecycle_root = data_dir.join(".lifecycle");
    fs::create_dir_all(&lifecycle_root)?;

    match command {
        LifecycleCommand::Archive => {
            let dest = lifecycle_root.join("archive").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("archive"), &dest)?;
            Ok(format!("Archived data to {}", dest.display()))
        }
        LifecycleCommand::Restore => {
            let archive_root = lifecycle_root.join("archive");
            let latest = read_latest_marker(&archive_root)?;
            let backup_dir = lifecycle_root.join("restore-backup").join(timestamp_dir());
            copy_data_dir(data_dir, &backup_dir)?;
            clear_data_dir(data_dir)?;
            copy_data_dir(&latest, data_dir)?;
            Ok(format!(
                "Restored data from {} (backup at {})",
                latest.display(),
                backup_dir.display()
            ))
        }
        LifecycleCommand::Backup => {
            let dest = lifecycle_root.join("backup").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("backup"), &dest)?;
            Ok(format!("Backup created at {}", dest.display()))
        }
        LifecycleCommand::Export => {
            let dest = lifecycle_root.join("export").join(timestamp_dir());
            copy_data_dir(data_dir, &dest)?;
            write_latest_marker(&lifecycle_root.join("export"), &dest)?;
            Ok(format!("Exported data to {}", dest.display()))
        }
    }
}

fn timestamp_dir() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("ts-{seconds}")
}

fn copy_data_dir(src: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    copy_dir_filtered(src, dest)
}

fn copy_dir_filtered(src: &Path, dest: &Path) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let src_path = entry.path();
        let dest_path = dest.join(&name);
        if file_type.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_dir_filtered(&src_path, &dest_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dest_path)?;
        }
    }
    Ok(())
}

fn clear_data_dir(data_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else if path.is_file() {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn write_latest_marker(root: &Path, latest: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    let marker = root.join("latest");
    fs::write(marker, latest.display().to_string().as_bytes())?;
    Ok(())
}

fn read_latest_marker(root: &Path) -> Result<PathBuf> {
    let marker = root.join("latest");
    if !marker.exists() {
        return Err(CliError::InvalidArgument(
            "No archive snapshot found to restore.".to_string(),
        ));
    }
    let path = fs::read_to_string(&marker)?;
    let path = PathBuf::from(path.trim());
    if !path.exists() {
        return Err(CliError::InvalidArgument(format!(
            "Latest archive path does not exist: {}",
            path.display()
        )));
    }
    Ok(path)
}
