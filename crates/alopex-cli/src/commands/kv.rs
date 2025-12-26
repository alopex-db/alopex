//! KV Command - Key-Value operations
//!
//! Supports: get, put, delete, list

use std::io::Write;

use alopex_embedded::{Database, TxnMode};

use crate::cli::KvCommand;
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::streaming::{StreamingWriter, WriteStatus};

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
    }
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
