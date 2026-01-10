use std::fmt;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{Result, ServerError};

/// Audit log sink trait.
pub trait AuditLogSink: Send + Sync + 'static {
    /// Emit a log entry.
    fn log(&self, entry: &AuditLogEntry);
    /// Flush buffered output.
    fn flush(&self) -> Result<()>;
}

/// Output configuration for audit logs.
#[derive(Clone)]
pub enum AuditLogOutput {
    Stdout,
    File { path: PathBuf },
    Custom(Arc<dyn AuditLogSink>),
}

impl fmt::Debug for AuditLogOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditLogOutput::Stdout => f.debug_tuple("Stdout").finish(),
            AuditLogOutput::File { path } => f.debug_struct("File").field("path", path).finish(),
            AuditLogOutput::Custom(_) => f.debug_tuple("Custom").finish(),
        }
    }
}

impl Default for AuditLogOutput {
    fn default() -> Self {
        Self::Stdout
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AuditLogOutputConfig {
    Stdout,
    File { path: PathBuf },
}

impl<'de> Deserialize<'de> for AuditLogOutput {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = AuditLogOutputConfig::deserialize(deserializer)?;
        Ok(match config {
            AuditLogOutputConfig::Stdout => Self::Stdout,
            AuditLogOutputConfig::File { path } => Self::File { path },
        })
    }
}

impl AuditLogOutput {
    fn into_sink(self) -> Result<Arc<dyn AuditLogSink>> {
        match self {
            AuditLogOutput::Stdout => Ok(Arc::new(StdoutAuditSink::default())),
            AuditLogOutput::File { path } => Ok(Arc::new(FileAuditSink::new(path)?)),
            AuditLogOutput::Custom(sink) => Ok(sink),
        }
    }
}

/// Audit log entry structure.
#[derive(Debug, Serialize)]
pub struct AuditLogEntry {
    pub event_type: AuditEventType,
    pub actor: Option<String>,
    pub target: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
    pub details: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    DdlExecute,
    ConfigChange,
    AuthFailure,
}

/// Audit logger with output routing.
#[derive(Clone)]
pub struct AuditLogger {
    sink: Arc<dyn AuditLogSink>,
}

impl AuditLogger {
    /// Create a new audit logger.
    pub fn new(output: AuditLogOutput) -> Result<Self> {
        Ok(Self {
            sink: output.into_sink()?,
        })
    }

    /// Log a raw entry.
    pub fn log(&self, entry: AuditLogEntry) {
        self.sink.log(&entry);
    }

    /// Log DDL execution.
    pub fn log_ddl(&self, sql: &str, actor: Option<&str>, correlation_id: &str) {
        let entry = AuditLogEntry {
            event_type: AuditEventType::DdlExecute,
            actor: actor.map(|v| v.to_string()),
            target: "ddl".to_string(),
            correlation_id: correlation_id.to_string(),
            timestamp: Utc::now(),
            details: serde_json::json!({ "sql": sql }),
        };
        self.log(entry);
    }

    /// Log a config change.
    pub fn log_config_change(&self, key: &str, old: &str, new: &str, correlation_id: &str) {
        let entry = AuditLogEntry {
            event_type: AuditEventType::ConfigChange,
            actor: None,
            target: key.to_string(),
            correlation_id: correlation_id.to_string(),
            timestamp: Utc::now(),
            details: serde_json::json!({ "old": old, "new": new }),
        };
        self.log(entry);
    }

    /// Flush output.
    pub fn flush(&self) -> Result<()> {
        self.sink.flush()
    }
}

#[derive(Default)]
struct StdoutAuditSink {
    buffer: Mutex<()>,
}

impl AuditLogSink for StdoutAuditSink {
    fn log(&self, entry: &AuditLogEntry) {
        let _guard = self.buffer.lock().ok();
        if let Ok(line) = serde_json::to_string(entry) {
            println!("{line}");
        }
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

struct FileAuditSink {
    writer: Mutex<BufWriter<std::fs::File>>,
}

impl FileAuditSink {
    fn new(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(ServerError::Io)?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

impl AuditLogSink for FileAuditSink {
    fn log(&self, entry: &AuditLogEntry) {
        let Ok(mut writer) = self.writer.lock() else {
            return;
        };
        if let Ok(line) = serde_json::to_string(entry) {
            let _ = writeln!(writer, "{line}");
        }
    }

    fn flush(&self) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| ServerError::Internal("audit log lock poisoned".into()))?;
        writer.flush().map_err(ServerError::Io)
    }
}
