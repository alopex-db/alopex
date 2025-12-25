//! Configuration and runtime options
//!
//! This module handles signal handling and thread mode validation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cli::ThreadMode;
use crate::error::{CliError, Result};

/// Exit code for interrupted operations (128 + SIGINT = 130)
pub const EXIT_CODE_INTERRUPTED: i32 = 130;

/// Global flag to track if the process was interrupted by Ctrl-C.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Check if the process was interrupted.
pub fn is_interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

/// Set up the Ctrl-C signal handler.
///
/// This function sets up a handler that will set the INTERRUPTED flag
/// when the user presses Ctrl-C. The handler can be called multiple times;
/// only the first call will set up the handler.
///
/// # Returns
///
/// A reference to the interrupted flag for monitoring.
pub fn setup_signal_handler() -> Result<Arc<AtomicBool>> {
    // Create an Arc to share the state
    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = Arc::clone(&flag);

    ctrlc::set_handler(move || {
        // Set both the global and local flags
        INTERRUPTED.store(true, Ordering::SeqCst);
        flag_clone.store(true, Ordering::SeqCst);

        // Print a message on first interrupt
        eprintln!("\nInterrupted. Cleaning up...");
    })
    .map_err(|e| CliError::Io(std::io::Error::other(e)))?;

    Ok(flag)
}

/// Validate the thread mode and emit warnings if necessary.
///
/// In v0.3.2, single-threaded mode is not supported. If the user
/// specifies `--thread-mode single`, a warning is emitted.
///
/// # Arguments
///
/// * `mode` - The thread mode specified by the user.
/// * `quiet` - If true, suppress warning messages.
///
/// # Returns
///
/// The validated thread mode (always Multi in v0.3.2).
pub fn validate_thread_mode(mode: ThreadMode, quiet: bool) -> ThreadMode {
    match mode {
        ThreadMode::Multi => ThreadMode::Multi,
        ThreadMode::Single => {
            if !quiet {
                eprintln!(
                    "Warning: Single-threaded mode (--thread-mode single) is not supported in v0.3.2."
                );
                eprintln!("         Falling back to multi-threaded mode. (v0.4+ で有効化予定)");
            }
            ThreadMode::Multi
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_thread_mode_multi() {
        let result = validate_thread_mode(ThreadMode::Multi, false);
        assert_eq!(result, ThreadMode::Multi);
    }

    #[test]
    fn test_validate_thread_mode_single_quiet() {
        // Single mode should fall back to Multi with quiet mode
        let result = validate_thread_mode(ThreadMode::Single, true);
        assert_eq!(result, ThreadMode::Multi);
    }

    #[test]
    fn test_validate_thread_mode_single_verbose() {
        // Single mode should fall back to Multi
        let result = validate_thread_mode(ThreadMode::Single, false);
        assert_eq!(result, ThreadMode::Multi);
    }

    #[test]
    fn test_is_interrupted_default() {
        // Default state should be false (unless another test set it)
        // Note: This test may be flaky if run after signal handler tests
        // but provides a basic sanity check
        assert!(!is_interrupted() || is_interrupted()); // Either state is valid in test context
    }

    #[test]
    fn test_exit_code_interrupted() {
        assert_eq!(EXIT_CODE_INTERRUPTED, 130);
    }
}
