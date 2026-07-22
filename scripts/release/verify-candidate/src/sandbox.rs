use crate::policy::{
    AllowlistedCommand, CandidateVerificationPolicy, CargoAction, CliStartupArgument,
};
use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SandboxAudit {
    pub command: AllowlistedCommand,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub backend: String,
}

/// A fail-closed Linux backend. `bwrap` creates a mount namespace with a
/// network namespace and only binds the candidate output directory writable.
/// Other platforms intentionally return `sandbox_unavailable` until an
/// equivalent enforced backend is implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxRunner {
    bwrap: PathBuf,
}

impl SandboxRunner {
    pub fn detect() -> Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(VerificationError::new(
                "sandbox_unavailable",
                "an enforced candidate sandbox backend is required on this platform",
            ));
        }
        let path = find_in_path("bwrap").ok_or_else(|| {
            VerificationError::new(
                "sandbox_unavailable",
                "bubblewrap (bwrap) is required for no-network/no-write verification",
            )
        })?;
        let probe = Command::new(&path)
            .args(["--unshare-net", "--ro-bind", "/", "/", "--", "true"])
            .output()
            .map_err(|error| io_error("probe candidate sandbox", error))?;
        if !probe.status.success() {
            return Err(VerificationError::new(
                "sandbox_unavailable",
                format!(
                    "bubblewrap cannot create the required network namespace: {}",
                    String::from_utf8_lossy(&probe.stderr).trim()
                ),
            ));
        }
        Ok(Self { bwrap: path })
    }

    pub fn execute(
        &self,
        policy: &CandidateVerificationPolicy,
        command: AllowlistedCommand,
        artifacts_dir: &Path,
    ) -> Result<SandboxAudit> {
        policy.validate_command(&command)?;
        let (program, arguments) = materialise_command(&command, artifacts_dir)?;
        let output = Command::new(&self.bwrap)
            .arg("--die-with-parent")
            .arg("--unshare-net")
            .arg("--unshare-ipc")
            .arg("--unshare-pid")
            .arg("--new-session")
            // Start from a read-only host view, then hide ambient home/config
            // locations and re-bind only the verifier's explicit inputs.
            .args(["--ro-bind", "/", "/"])
            .args(["--dir", "/candidate"])
            .args([
                "--ro-bind",
                policy.source_dir.to_string_lossy().as_ref(),
                "/candidate/source",
            ])
            .args([
                "--ro-bind",
                policy.input_bundle_dir.to_string_lossy().as_ref(),
                "/candidate/input",
            ])
            .args([
                "--bind",
                policy.output_dir.to_string_lossy().as_ref(),
                "/candidate/output",
            ])
            .args(["--tmpfs", "/home"])
            .args(["--tmpfs", "/root"])
            .args(["--tmpfs", "/tmp"])
            .args(["--proc", "/proc"])
            .args(["--dev", "/dev"])
            .arg("--clearenv")
            .args(["--setenv", "HOME", "/home/verify"])
            .args(["--setenv", "PATH", "/usr/local/bin:/usr/bin:/bin"])
            .args(["--setenv", "XDG_CACHE_HOME", "/tmp/cache"])
            .args(["--setenv", "CARGO_NET_OFFLINE", "true"])
            .arg("--setenv")
            .arg("CARGO_HOME")
            .arg(format!("/candidate/input/{}", policy.cargo_home_relative))
            .args([
                "--setenv",
                "CARGO_TARGET_DIR",
                "/candidate/output/cargo-target",
            ])
            .args(["--chdir", "/candidate/source"])
            .arg("--")
            .arg(&program)
            .args(&arguments)
            .output()
            .map_err(|error| io_error("start candidate sandbox", error))?;
        Ok(SandboxAudit {
            command,
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            backend: "bubblewrap-unshare-net".to_owned(),
        })
    }
}

fn materialise_command(
    command: &AllowlistedCommand,
    artifacts_dir: &Path,
) -> Result<(String, Vec<String>)> {
    match command {
        AllowlistedCommand::Cargo { action } => {
            let action = match action {
                CargoAction::Build => "build",
                CargoAction::Test => "test",
                CargoAction::Metadata => "metadata",
            };
            Ok((
                "cargo".to_owned(),
                vec![
                    action.to_owned(),
                    "--locked".to_owned(),
                    "--offline".to_owned(),
                ],
            ))
        }
        AllowlistedCommand::PythonCreateEnvironment => Ok((
            "python3".to_owned(),
            vec![
                "-m".to_owned(),
                "venv".to_owned(),
                "/candidate/output/python-venv".to_owned(),
            ],
        )),
        AllowlistedCommand::PythonInstallWheel { wheel } => Ok((
            "/candidate/output/python-venv/bin/python".to_owned(),
            vec![
                "-m".to_owned(),
                "pip".to_owned(),
                "install".to_owned(),
                "--no-index".to_owned(),
                "--no-deps".to_owned(),
                wheel_path(artifacts_dir, wheel)?.display().to_string(),
            ],
        )),
        AllowlistedCommand::PythonImport { package } => {
            if !is_python_package_name(package) {
                return Err(VerificationError::new(
                    "sandbox_command_forbidden",
                    package.clone(),
                ));
            }
            Ok((
                "/candidate/output/python-venv/bin/python".to_owned(),
                vec!["-c".to_owned(), format!("import {package}")],
            ))
        }
        AllowlistedCommand::CliStartup {
            artifact_id,
            argument,
        } => {
            let binary = wheel_path(artifacts_dir, artifact_id)?;
            let argument = match argument {
                CliStartupArgument::Help => "--help",
                CliStartupArgument::Version => "--version",
            };
            Ok((binary.display().to_string(), vec![argument.to_owned()]))
        }
        AllowlistedCommand::VerifyWheelContents { artifact_id } => Ok((
            "python3".to_owned(),
            vec![
                "/candidate/source/scripts/release/verify_wheel_contents.py".to_owned(),
                wheel_path(artifacts_dir, artifact_id)?
                    .display()
                    .to_string(),
            ],
        )),
    }
}

fn wheel_path(artifacts_dir: &Path, file_name: &str) -> Result<PathBuf> {
    let candidate = Path::new(file_name);
    if candidate.is_absolute() || candidate.components().count() != 1 || file_name.contains("..") {
        return Err(VerificationError::new(
            "sandbox_artifact_path_invalid",
            file_name.to_owned(),
        ));
    }
    let path = artifacts_dir.join(candidate);
    if !path.is_file() {
        return Err(VerificationError::new(
            "sandbox_artifact_missing",
            path.display().to_string(),
        ));
    }
    Ok(path)
}

fn is_python_package_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn find_in_path(program: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|directory| directory.join(program))
            .find(|candidate| candidate.is_file())
    })
}
