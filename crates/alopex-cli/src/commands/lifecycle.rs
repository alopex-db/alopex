//! Placeholder lifecycle command handlers.

use crate::cli::LifecycleCommand;
use crate::error::{CliError, Result};

pub fn execute(command: &LifecycleCommand) -> Result<()> {
    match command {
        LifecycleCommand::Archive => Err(not_implemented("archive")),
        LifecycleCommand::Restore => Err(not_implemented("restore")),
        LifecycleCommand::Backup => Err(not_implemented("backup")),
        LifecycleCommand::Export => Err(not_implemented("export")),
    }
}

fn not_implemented(action: &str) -> CliError {
    CliError::InvalidArgument(format!(
        "Lifecycle action '{}' is not implemented yet.",
        action
    ))
}
