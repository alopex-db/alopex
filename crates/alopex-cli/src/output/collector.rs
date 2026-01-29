//! Output collector for capturing rows without formatting.

use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::models::Row;

use super::formatter::Formatter;

const MAX_COLLECTED_BYTES: usize = 10 * 1024 * 1024;

#[derive(Default)]
struct CollectorState {
    rows: Vec<Row>,
    bytes: usize,
    truncated: bool,
}

#[derive(Clone, Default)]
pub struct RowCollector {
    state: Arc<Mutex<CollectorState>>,
}

impl RowCollector {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(CollectorState::default())),
        }
    }

    pub fn formatter(&self) -> CollectingFormatter {
        CollectingFormatter {
            state: Arc::clone(&self.state),
        }
    }

    pub fn rows(&self) -> Vec<Row> {
        self.state.lock().expect("row collector lock").rows.clone()
    }

    pub fn truncation_warning(&self) -> Option<String> {
        let state = self.state.lock().expect("row collector lock");
        if state.truncated {
            Some(format!(
                "Warning: output truncated after {}MB to keep the TUI responsive.",
                MAX_COLLECTED_BYTES / (1024 * 1024)
            ))
        } else {
            None
        }
    }
}

pub struct CollectingFormatter {
    state: Arc<Mutex<CollectorState>>,
}

impl Formatter for CollectingFormatter {
    fn write_header(
        &mut self,
        _writer: &mut dyn std::io::Write,
        _columns: &[crate::models::Column],
    ) -> Result<()> {
        Ok(())
    }

    fn write_row(&mut self, _writer: &mut dyn std::io::Write, row: &Row) -> Result<()> {
        let row_bytes = serde_json::to_vec(row)?.len();
        let mut state = self.state.lock().expect("row collector lock");
        if state.truncated {
            return Ok(());
        }
        if state.bytes + row_bytes > MAX_COLLECTED_BYTES {
            state.truncated = true;
            return Ok(());
        }
        state.bytes += row_bytes;
        state.rows.push(row.clone());
        Ok(())
    }

    fn write_footer(&mut self, _writer: &mut dyn std::io::Write) -> Result<()> {
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}
