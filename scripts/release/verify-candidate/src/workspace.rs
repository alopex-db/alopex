use crate::{io_error, Result, VerificationError};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCrate {
    pub name: String,
    pub version: String,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetadata {
    pub members: Vec<WorkspaceCrate>,
    pub development_tools: Vec<WorkspaceCrate>,
}

/// Parses the intentionally small, explicit workspace membership form used by
/// Alopex. The verifier does not invoke `cargo metadata`, which could acquire
/// dependencies or write to the source checkout.
pub fn inspect_workspace(source_root: &Path) -> Result<WorkspaceMetadata> {
    let root_manifest = source_root.join("Cargo.toml");
    let root = std::fs::read_to_string(&root_manifest)
        .map_err(|error| io_error("read root Cargo.toml", error))?;
    let members = parse_workspace_members(&root)?;
    let workspace_version = parse_workspace_version(&root)?;
    let mut crates = Vec::new();
    for member in members {
        let manifest_path = source_root.join(&member).join("Cargo.toml");
        let text = std::fs::read_to_string(&manifest_path)
            .map_err(|error| io_error("read member Cargo.toml", error))?;
        let name = parse_package_value(&text, "name")?.ok_or_else(|| {
            VerificationError::new(
                "workspace_crate_name_missing",
                manifest_path.display().to_string(),
            )
        })?;
        let version = match parse_package_value(&text, "version")? {
            Some(value) => value,
            None if text
                .lines()
                .any(|line| line.trim() == "version.workspace = true") =>
            {
                workspace_version.clone()
            }
            None => {
                return Err(VerificationError::new(
                    "workspace_crate_version_missing",
                    manifest_path.display().to_string(),
                ));
            }
        };
        crates.push(WorkspaceCrate {
            name,
            version,
            manifest_path,
        });
    }
    crates.sort_by(|left, right| left.name.cmp(&right.name));

    let tools_manifest = source_root.join("crates/alopex-tools/Cargo.toml");
    let tools = std::fs::read_to_string(&tools_manifest)
        .map_err(|error| io_error("read alopex-tools manifest", error))?;
    if !tools.lines().any(|line| line.trim() == "publish = false") {
        return Err(VerificationError::new(
            "development_tool_publish_policy_invalid",
            tools_manifest.display().to_string(),
        ));
    }
    let tool_name = parse_package_value(&tools, "name")?.ok_or_else(|| {
        VerificationError::new(
            "workspace_crate_name_missing",
            tools_manifest.display().to_string(),
        )
    })?;
    let tool_version = parse_package_value(&tools, "version")?.ok_or_else(|| {
        VerificationError::new(
            "workspace_crate_version_missing",
            tools_manifest.display().to_string(),
        )
    })?;
    Ok(WorkspaceMetadata {
        members: crates,
        development_tools: vec![WorkspaceCrate {
            name: tool_name,
            version: tool_version,
            manifest_path: tools_manifest,
        }],
    })
}

fn parse_workspace_members(text: &str) -> Result<Vec<String>> {
    let start = text.find("members = [").ok_or_else(|| {
        VerificationError::new(
            "workspace_members_missing",
            "Cargo.toml has no members list",
        )
    })?;
    let after = &text[start + "members = [".len()..];
    let end = after.find(']').ok_or_else(|| {
        VerificationError::new("workspace_members_invalid", "members list is not closed")
    })?;
    let mut members = Vec::new();
    for segment in after[..end].split(',') {
        let value = segment.trim().trim_matches('"');
        if !value.is_empty() && !value.starts_with('#') {
            members.push(value.to_owned());
        }
    }
    if members.is_empty() {
        return Err(VerificationError::new(
            "workspace_members_invalid",
            "members list is empty",
        ));
    }
    Ok(members)
}

fn parse_workspace_version(text: &str) -> Result<String> {
    let position = text.find("[workspace.package]").ok_or_else(|| {
        VerificationError::new("workspace_version_missing", "workspace.package is absent")
    })?;
    let section = &text[position..];
    parse_package_value(section, "version")?.ok_or_else(|| {
        VerificationError::new(
            "workspace_version_missing",
            "workspace.package.version is absent",
        )
    })
}

fn parse_package_value(text: &str, key: &str) -> Result<Option<String>> {
    let mut in_package = false;
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('[') {
            in_package = line == "[package]" || line == "[workspace.package]";
            continue;
        }
        if in_package {
            let prefix = format!("{key} =");
            if let Some(value) = line.strip_prefix(&prefix) {
                let value = value.trim();
                if let Some(value) = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                {
                    return Ok(Some(value.to_owned()));
                }
                return Err(VerificationError::new(
                    "workspace_manifest_invalid",
                    format!("{key} must be a quoted string"),
                ));
            }
        }
    }
    Ok(None)
}
