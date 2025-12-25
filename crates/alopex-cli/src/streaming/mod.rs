//! Streaming output support
//!
//! Provides StreamingWriter for managing streaming output
//! with automatic fallback for non-streaming formats.

pub mod writer;

pub use writer::{StreamingWriter, WriteStatus};

// Re-export for public API configuration
#[allow(unused_imports)]
pub use writer::DEFAULT_BUFFER_LIMIT;
