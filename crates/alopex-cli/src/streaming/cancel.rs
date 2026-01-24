//! Cancellation utilities for streaming queries.
//!
//! Provides a lightweight cancellation signal that can be triggered
//! by Ctrl-C (via config::is_interrupted) or manually in tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config;

#[derive(Clone, Debug, Default)]
pub struct CancelSignal {
    manual: Arc<AtomicBool>,
}

impl CancelSignal {
    pub fn new() -> Self {
        Self {
            manual: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn cancel(&self) {
        self.manual.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        config::is_interrupted() || self.manual.load(Ordering::SeqCst)
    }

    pub async fn wait(&self) {
        while !self.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
