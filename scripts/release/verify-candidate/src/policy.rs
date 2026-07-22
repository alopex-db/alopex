use crate::{Result, VerificationError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Commands are modelled data, never a shell string. The narrow enum prevents
/// shell escapes, registry access, git mutation, and publication tools from
/// entering a candidate verification run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AllowlistedCommand {
    Cargo {
        action: CargoAction,
    },
    PythonCreateEnvironment,
    PythonInstallWheel {
        wheel: String,
    },
    PythonImport {
        package: String,
    },
    CliStartup {
        artifact_id: String,
        argument: CliStartupArgument,
    },
    VerifyWheelContents {
        artifact_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CargoAction {
    Build,
    Test,
    Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CliStartupArgument {
    Help,
    Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateVerificationPolicy {
    pub source_dir: PathBuf,
    pub input_bundle_dir: PathBuf,
    /// Path relative to `/candidate/input` containing the pre-fetched Cargo
    /// home/registry bundle used by `--locked --offline` commands.
    pub cargo_home_relative: String,
    pub output_dir: PathBuf,
}

impl CandidateVerificationPolicy {
    pub fn validate_command(&self, command: &AllowlistedCommand) -> Result<()> {
        match command {
            AllowlistedCommand::Cargo { .. }
            | AllowlistedCommand::PythonCreateEnvironment
            | AllowlistedCommand::PythonInstallWheel { .. }
            | AllowlistedCommand::PythonImport { .. }
            | AllowlistedCommand::CliStartup { .. }
            | AllowlistedCommand::VerifyWheelContents { .. } => Ok(()),
        }
    }

    pub fn reject_program(program: &str) -> Result<()> {
        Err(VerificationError::new(
            "sandbox_command_forbidden",
            format!("{program} is not in the candidate verifier allowlist"),
        ))
    }
}
