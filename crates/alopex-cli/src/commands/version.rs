use std::io::{self, Write};

use serde::Serialize;

use crate::cli::OutputFormat;
use crate::error::{CliError, Result};
use crate::version::{cli_version, supported_format_max, supported_format_min};

#[derive(Debug, Serialize)]
pub struct VersionInfo {
    pub cli_version: String,
    pub supported_format_min: String,
    pub supported_format_max: String,
    pub build_date: Option<String>,
    pub git_commit: Option<String>,
}

impl VersionInfo {
    fn new() -> Self {
        let build_date = option_env!("ALOPEX_BUILD_DATE").map(|value| value.to_string());
        let git_commit = option_env!("ALOPEX_GIT_COMMIT").map(|value| value.to_string());

        Self {
            cli_version: cli_version().to_string(),
            supported_format_min: supported_format_min().to_string(),
            supported_format_max: supported_format_max().to_string(),
            build_date,
            git_commit,
        }
    }
}

pub fn execute_version(output: OutputFormat) -> Result<()> {
    let info = VersionInfo::new();
    match output {
        OutputFormat::Table => write_table(&info),
        OutputFormat::Json => write_json(&info),
        other => Err(CliError::InvalidArgument(format!(
            "Unsupported output format for version: {:?}",
            other
        ))),
    }
}

fn write_table(info: &VersionInfo) -> Result<()> {
    let mut stdout = io::stdout();
    writeln!(stdout, "alopex-cli {}", info.cli_version)?;
    writeln!(
        stdout,
        "Supported file format: {} - {}",
        info.supported_format_min, info.supported_format_max
    )?;
    if let Some(build_date) = &info.build_date {
        writeln!(stdout, "Build date: {}", build_date)?;
    }
    if let Some(git_commit) = &info.git_commit {
        writeln!(stdout, "Git commit: {}", git_commit)?;
    }
    Ok(())
}

fn write_json(info: &VersionInfo) -> Result<()> {
    let mut stdout = io::stdout();
    serde_json::to_writer_pretty(&mut stdout, info)?;
    writeln!(stdout)?;
    Ok(())
}
