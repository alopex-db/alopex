//! KV Command - Key-Value operations
//!
//! Supports: get, put, delete, list

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use alopex_embedded::{Database, TxnMode};
use serde::{Deserialize, Serialize};

use crate::cli::{KvCommand, KvTxnCommand};
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::streaming::{StreamingWriter, WriteStatus};

const DEFAULT_TXN_TIMEOUT_SECS: u64 = 60;
const TXN_DIR_ENV: &str = "ALOPEX_TXN_DIR";

/// Execute a KV command.
///
/// # Arguments
///
/// * `db` - The database instance.
/// * `cmd` - The KV subcommand to execute.
/// * `writer` - The streaming writer for output.
pub fn execute<W: Write>(
    db: &Database,
    cmd: KvCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        KvCommand::Get { key } => execute_get(db, &key, writer),
        KvCommand::Put { key, value } => execute_put(db, &key, &value, writer),
        KvCommand::Delete { key } => execute_delete(db, &key, writer),
        KvCommand::List { prefix } => execute_list(db, prefix.as_deref(), writer),
        KvCommand::Txn(cmd) => execute_txn_command(db, cmd, writer),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TxnState {
    txn_id: String,
    created_at: u64,
    timeout_secs: u64,
    writes: BTreeMap<String, Option<String>>,
}

impl TxnState {
    fn is_expired(&self) -> bool {
        let now = current_timestamp_secs();
        now.saturating_sub(self.created_at) >= self.timeout_secs
    }
}

fn execute_txn_command<W: Write>(
    db: &Database,
    cmd: KvTxnCommand,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    match cmd {
        KvTxnCommand::Begin { timeout_secs } => execute_txn_begin(timeout_secs, writer),
        KvTxnCommand::Get { key, txn_id } => execute_txn_get(db, &key, &txn_id, writer),
        KvTxnCommand::Put { key, value, txn_id } => execute_txn_put(&key, &value, &txn_id, writer),
        KvTxnCommand::Delete { key, txn_id } => execute_txn_delete(&key, &txn_id, writer),
        KvTxnCommand::Commit { txn_id } => execute_txn_commit(db, &txn_id, writer),
        KvTxnCommand::Rollback { txn_id } => execute_txn_rollback(&txn_id, writer),
    }
}

fn execute_txn_begin<W: Write>(
    timeout_secs: Option<u64>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let txn_id = generate_txn_id();
    let state = TxnState {
        txn_id: txn_id.clone(),
        created_at: current_timestamp_secs(),
        timeout_secs: timeout_secs.unwrap_or(DEFAULT_TXN_TIMEOUT_SECS),
        writes: BTreeMap::new(),
    };
    save_txn_state(&state)?;

    writer.prepare(Some(1))?;
    let row = Row::new(vec![Value::Text("txn_id".to_string()), Value::Text(txn_id)]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

fn execute_txn_get<W: Write>(
    db: &Database,
    key: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let state = load_active_txn_state(txn_id)?;
    if let Some(value) = state.writes.get(key) {
        return write_kv_value(key, value.clone(), writer);
    }

    execute_get(db, key, writer)
}

fn execute_txn_put<W: Write>(
    key: &str,
    value: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut state = load_active_txn_state(txn_id)?;
    state
        .writes
        .insert(key.to_string(), Some(value.to_string()));
    save_txn_state(&state)?;

    write_status_if_needed(writer, &format!("Staged key: {}", key))
}

fn execute_txn_delete<W: Write>(
    key: &str,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut state = load_active_txn_state(txn_id)?;
    state.writes.insert(key.to_string(), None);
    save_txn_state(&state)?;

    write_status_if_needed(writer, &format!("Staged delete: {}", key))
}

fn execute_txn_commit<W: Write>(
    db: &Database,
    txn_id: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let state = load_active_txn_state(txn_id)?;
    let mut txn = db.begin(TxnMode::ReadWrite)?;

    for (key, value) in &state.writes {
        match value {
            Some(value) => txn.put(key.as_bytes(), value.as_bytes())?,
            None => txn.delete(key.as_bytes())?,
        }
    }

    txn.commit()?;
    delete_txn_state(txn_id)?;

    write_status_if_needed(writer, &format!("Committed transaction: {}", txn_id))
}

fn execute_txn_rollback<W: Write>(txn_id: &str, writer: &mut StreamingWriter<W>) -> Result<()> {
    let state = load_active_txn_state(txn_id)?;
    delete_txn_state(&state.txn_id)?;

    write_status_if_needed(writer, &format!("Rolled back transaction: {}", txn_id))
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_txn_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("txn-{}-{}", nanos, std::process::id())
}

fn txn_dir() -> Result<PathBuf> {
    if let Ok(dir) = env::var(TXN_DIR_ENV) {
        if !dir.trim().is_empty() {
            return Ok(PathBuf::from(dir));
        }
    }

    let mut dir = dirs::home_dir().ok_or_else(|| {
        CliError::InvalidArgument("Home directory not found for txn storage".to_string())
    })?;
    dir.push(".alopex");
    dir.push("transactions");
    Ok(dir)
}

fn validate_txn_id(txn_id: &str) -> Result<()> {
    if txn_id.is_empty() || txn_id.contains('/') || txn_id.contains('\\') || txn_id.contains("..") {
        return Err(CliError::InvalidTransactionId(txn_id.to_string()));
    }
    Ok(())
}

fn txn_file_path(txn_id: &str) -> Result<PathBuf> {
    validate_txn_id(txn_id)?;
    let dir = txn_dir()?;
    Ok(dir.join(format!("{}.json", txn_id)))
}

fn save_txn_state(state: &TxnState) -> Result<()> {
    let path = txn_file_path(&state.txn_id)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let payload = serde_json::to_vec(state)?;
    fs::write(&tmp_path, payload)?;
    fs::rename(&tmp_path, &path)?;
    Ok(())
}

fn load_txn_state(txn_id: &str) -> Result<TxnState> {
    let path = txn_file_path(txn_id)?;
    let payload = fs::read(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            CliError::InvalidTransactionId(txn_id.to_string())
        } else {
            CliError::Io(err)
        }
    })?;
    let state: TxnState = serde_json::from_slice(&payload)
        .map_err(|_| CliError::InvalidTransactionId(txn_id.to_string()))?;
    if state.txn_id != txn_id {
        return Err(CliError::InvalidTransactionId(txn_id.to_string()));
    }
    Ok(state)
}

fn load_active_txn_state(txn_id: &str) -> Result<TxnState> {
    let state = load_txn_state(txn_id)?;
    if state.is_expired() {
        let _ = delete_txn_state(txn_id);
        return Err(CliError::TransactionTimeout(txn_id.to_string()));
    }
    Ok(state)
}

fn delete_txn_state(txn_id: &str) -> Result<()> {
    let path = txn_file_path(txn_id)?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Err(CliError::InvalidTransactionId(txn_id.to_string()))
        }
        Err(err) => Err(CliError::Io(err)),
    }
}

fn write_kv_value<W: Write>(
    key: &str,
    value: Option<String>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let Some(value) = value else {
        return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
    };

    writer.prepare(Some(1))?;
    let row = Row::new(vec![Value::Text(key.to_string()), Value::Text(value)]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

fn write_status_if_needed<W: Write>(writer: &mut StreamingWriter<W>, message: &str) -> Result<()> {
    if writer.is_quiet() {
        return Ok(());
    }

    writer.prepare(Some(1))?;
    let row = Row::new(vec![
        Value::Text("OK".to_string()),
        Value::Text(message.to_string()),
    ]);
    writer.write_row(row)?;
    writer.finish()?;
    Ok(())
}

/// Execute a KV get command.
fn execute_get<W: Write>(db: &Database, key: &str, writer: &mut StreamingWriter<W>) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadOnly)?;
    let result = txn.get(key.as_bytes())?;
    txn.commit()?;

    // Prepare writer with hint of 1 row
    writer.prepare(Some(1))?;

    match result {
        Some(value) => {
            // Try to interpret value as UTF-8 text, fallback to hex
            let value_display = match std::str::from_utf8(&value) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Bytes(value),
            };

            let row = Row::new(vec![Value::Text(key.to_string()), value_display]);
            writer.write_row(row)?;
        }
        None => {
            return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
        }
    }

    writer.finish()?;
    Ok(())
}

/// Execute a KV put command.
fn execute_put<W: Write>(
    db: &Database,
    key: &str,
    value: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadWrite)?;
    txn.put(key.as_bytes(), value.as_bytes())?;
    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Stored key: {}", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute a KV delete command.
fn execute_delete<W: Write>(
    db: &Database,
    key: &str,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadWrite)?;

    // Check if key exists first
    let exists = txn.get(key.as_bytes())?.is_some();

    if !exists {
        txn.rollback()?;
        return Err(CliError::InvalidArgument(format!("Key not found: {}", key)));
    }

    txn.delete(key.as_bytes())?;
    txn.commit()?;

    // Suppress status output in quiet mode
    if !writer.is_quiet() {
        writer.prepare(Some(1))?;
        let row = Row::new(vec![
            Value::Text("OK".to_string()),
            Value::Text(format!("Deleted key: {}", key)),
        ]);
        writer.write_row(row)?;
        writer.finish()?;
    }

    Ok(())
}

/// Execute a KV list command.
fn execute_list<W: Write>(
    db: &Database,
    prefix: Option<&str>,
    writer: &mut StreamingWriter<W>,
) -> Result<()> {
    let mut txn = db.begin(TxnMode::ReadOnly)?;

    let prefix_bytes = prefix.map(|p| p.as_bytes()).unwrap_or(b"");
    let iter = txn.scan_prefix(prefix_bytes)?;

    // Prepare writer before streaming (no hint since we don't know count ahead of time)
    writer.prepare(None)?;

    // Stream results directly without collecting to Vec
    for (key, value) in iter {
        // Try to interpret key and value as UTF-8 text
        let key_display = match std::str::from_utf8(&key) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(key),
        };
        let value_display = match std::str::from_utf8(&value) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(value),
        };

        let row = Row::new(vec![key_display, value_display]);
        match writer.write_row(row)? {
            WriteStatus::LimitReached => break,
            WriteStatus::Continue | WriteStatus::FallbackTriggered => {}
        }
    }

    txn.commit()?;
    writer.finish()?;
    Ok(())
}

/// Create columns for KV output.
pub fn kv_columns() -> Vec<Column> {
    vec![
        Column::new("key", DataType::Text),
        Column::new("value", DataType::Text),
    ]
}

/// Create columns for KV status output.
pub fn kv_status_columns() -> Vec<Column> {
    vec![
        Column::new("status", DataType::Text),
        Column::new("message", DataType::Text),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::jsonl::JsonlFormatter;

    fn create_test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn create_test_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = kv_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    fn create_status_writer(output: &mut Vec<u8>) -> StreamingWriter<&mut Vec<u8>> {
        let formatter = Box::new(JsonlFormatter::new());
        let columns = kv_status_columns();
        StreamingWriter::new(output, formatter, columns, None)
    }

    #[test]
    fn test_put_and_get() {
        let db = create_test_db();

        // Put
        let mut output = Vec::new();
        {
            let mut writer = create_status_writer(&mut output);
            execute_put(&db, "test_key", "test_value", &mut writer).unwrap();
        }

        // Get
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_get(&db, "test_key", &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("test_key"));
        assert!(result.contains("test_value"));
    }

    #[test]
    fn test_get_not_found() {
        let db = create_test_db();

        let mut output = Vec::new();
        let mut writer = create_test_writer(&mut output);

        let result = execute_get(&db, "nonexistent", &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_delete() {
        let db = create_test_db();

        // Put first
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_put(&db, "to_delete", "value", &mut writer).unwrap();
        }

        // Delete
        {
            let mut output = Vec::new();
            let mut writer = create_status_writer(&mut output);
            execute_delete(&db, "to_delete", &mut writer).unwrap();
        }

        // Verify deletion
        {
            let mut output = Vec::new();
            let mut writer = create_test_writer(&mut output);
            let result = execute_get(&db, "to_delete", &mut writer);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_delete_not_found() {
        let db = create_test_db();

        let mut output = Vec::new();
        let mut writer = create_status_writer(&mut output);

        let result = execute_delete(&db, "nonexistent", &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CliError::InvalidArgument(_)));
    }

    #[test]
    fn test_list_all() {
        let db = create_test_db();

        // Put some keys
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"key1", b"value1").unwrap();
            txn.put(b"key2", b"value2").unwrap();
            txn.put(b"key3", b"value3").unwrap();
            txn.commit().unwrap();
        }

        // List all
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, None, &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("key1"));
        assert!(result.contains("key2"));
        assert!(result.contains("key3"));
    }

    #[test]
    fn test_list_with_prefix() {
        let db = create_test_db();

        // Put some keys
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"user:1", b"Alice").unwrap();
            txn.put(b"user:2", b"Bob").unwrap();
            txn.put(b"item:1", b"Widget").unwrap();
            txn.commit().unwrap();
        }

        // List with prefix
        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, Some("user:"), &mut writer).unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("user:1"));
        assert!(result.contains("user:2"));
        assert!(!result.contains("item:1"));
    }

    #[test]
    fn test_list_empty() {
        let db = create_test_db();

        let mut output = Vec::new();
        {
            let mut writer = create_test_writer(&mut output);
            execute_list(&db, None, &mut writer).unwrap();
        }

        // Should complete without error even with no results
        let result = String::from_utf8(output).unwrap();
        // Empty output is fine
        assert!(result.is_empty() || result.lines().count() == 0);
    }
}
