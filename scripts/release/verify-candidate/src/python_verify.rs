use crate::artifact_verify::ArtifactKind;
use crate::inventory::ArtifactInventory;
use crate::policy::AllowlistedCommand;
use crate::sandbox::SandboxAudit;
use crate::VerificationError;

/// Verifies that Python distribution evidence came from the enforced runner,
/// not an ambient interpreter or an index-backed installation.
pub fn validate_python_artifact_audits(
    inventory: &ArtifactInventory,
    audits: &[SandboxAudit],
) -> Vec<VerificationError> {
    let mut errors = Vec::new();
    let has_wheel = inventory
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::PythonWheel);
    if !has_wheel {
        return errors;
    }
    let install_ok = audits.iter().any(|audit| {
        matches!(audit.command, AllowlistedCommand::PythonInstallWheel { .. })
            && audit.exit_code == Some(0)
    });
    let import_ok = audits.iter().any(|audit| {
        matches!(audit.command, AllowlistedCommand::PythonImport { ref package } if package == "alopex")
            && audit.exit_code == Some(0)
    });
    let contents_ok = audits.iter().any(|audit| {
        matches!(
            audit.command,
            AllowlistedCommand::VerifyWheelContents { .. }
        ) && audit.exit_code == Some(0)
    });
    if !install_ok {
        errors.push(VerificationError::new(
            "python_local_install_missing",
            "no successful --no-index --no-deps wheel install audit",
        ));
    }
    if !import_ok {
        errors.push(VerificationError::new(
            "python_isolated_import_missing",
            "no successful isolated alopex import audit",
        ));
    }
    if !contents_ok {
        errors.push(VerificationError::new(
            "wheel_contents_verification_missing",
            "no successful local wheel content audit",
        ));
    }
    errors
}

pub fn validate_cli_startup_audits(
    inventory: &ArtifactInventory,
    audits: &[SandboxAudit],
) -> Vec<VerificationError> {
    let mut errors = Vec::new();
    if inventory
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::CliBinary)
        && !audits.iter().any(|audit| {
            matches!(audit.command, AllowlistedCommand::CliStartup { .. })
                && audit.exit_code == Some(0)
        })
    {
        errors.push(VerificationError::new(
            "cli_startup_verification_missing",
            "no successful isolated CLI startup audit",
        ));
    }
    errors
}
