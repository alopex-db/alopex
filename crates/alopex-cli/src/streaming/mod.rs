//! Streaming output support
//!
//! Provides StreamingWriter for managing streaming output
//! with automatic fallback for non-streaming formats.

pub mod cancel;
pub mod timeout;
pub mod writer;

pub use cancel::CancelSignal;
pub use timeout::Deadline;
pub use writer::{StreamingWriter, WriteStatus};

// Re-export for public API configuration
#[allow(unused_imports)]
pub use writer::DEFAULT_BUFFER_LIMIT;
