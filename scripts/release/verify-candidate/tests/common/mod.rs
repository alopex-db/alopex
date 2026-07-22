#![allow(dead_code)]

use alopex_verify_candidate::artifact_verify::{ArtifactKind, CandidateArtifact};
use alopex_verify_candidate::input_bundle::{hash_path, BundleKind, InputBundle, InputBundleEntry};
use alopex_verify_candidate::inventory::{CrateClassification, CrateScope, InventoryDeclaration};
use alopex_verify_candidate::manifest::{
    CapabilityEvidenceManifest, CapabilityEvidenceRow, DocumentationRef, EvidenceRef, SupportState,
};
use alopex_verify_candidate::policy::{AllowlistedCommand, CliStartupArgument};
use alopex_verify_candidate::sandbox::SandboxAudit;
use alopex_verify_candidate::scope_snapshot::{
    sha256_file, ApprovalEvidence, ApprovedScopeRow, ApprovedScopeSnapshot, ScopeReference,
};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub struct Fixture {
    pub temp: TempDir,
    pub source: PathBuf,
    pub bundle_root: PathBuf,
    pub artifacts_root: PathBuf,
    pub snapshot: ApprovedScopeSnapshot,
    pub manifest: CapabilityEvidenceManifest,
    pub bundle: InputBundle,
    pub inventory: InventoryDeclaration,
    pub audits: Vec<SandboxAudit>,
}

pub fn fixture() -> Fixture {
    let temp = tempfile::tempdir().expect("temporary fixture");
    let source = temp.path().join("source");
    let bundle_root = temp.path().join("bundle");
    let artifacts_root = bundle_root.join("artifacts");
    fs::create_dir_all(&artifacts_root).expect("artifact directory");
    fs::create_dir_all(bundle_root.join("cargo-home")).expect("cargo bundle");
    fs::create_dir_all(bundle_root.join("wheels")).expect("wheel bundle");
    fs::write(bundle_root.join("cargo-home/registry.txt"), "offline").expect("cargo input");
    fs::write(bundle_root.join("wheels/local.txt"), "offline").expect("wheel input");
    write_source(&source);
    let mut snapshot = write_requirements(&source);
    write_approval_evidence(&mut snapshot, &bundle_root);
    let artifacts = write_artifacts(&artifacts_root);
    let evidence_root = bundle_root.join("evidence");
    fs::create_dir_all(&evidence_root).expect("evidence directory");
    let mut rows = Vec::new();
    for phase in 1..=4 {
        let evidence_path = format!("evidence/phase-{phase}.txt");
        let absolute_evidence = bundle_root.join(&evidence_path);
        fs::write(&absolute_evidence, format!("phase-{phase} evidence")).expect("evidence");
        let requirement_path =
            format!(".spec-workflow/specs/phase-{phase}-fixture/requirements.md");
        rows.push(CapabilityEvidenceRow {
            id: format!("phase-{phase}-capability"),
            phase,
            scope: ScopeReference {
                requirement_path,
                requirement_anchor: "Requirement 1".to_owned(),
                requirements_sha256: snapshot.rows[(phase - 1) as usize]
                    .requirements_sha256
                    .clone(),
            },
            public_surface: format!("Phase {phase} fixture surface"),
            support: if phase <= 2 {
                SupportState::Unavailable
            } else {
                SupportState::Supported
            },
            prerequisite: if phase <= 2 {
                Some("external fixture foundation unavailable".to_owned())
            } else {
                None
            },
            normal_outcome: "normal fixture result".to_owned(),
            failure_outcome: "structured fixture failure".to_owned(),
            artifacts: artifacts
                .iter()
                .map(|artifact| artifact.id.clone())
                .collect(),
            evidence: vec![EvidenceRef {
                path: evidence_path,
                sha256: sha256_file(&absolute_evidence).expect("evidence hash"),
                kind: if phase <= 2 {
                    "external_prerequisite_integration"
                } else {
                    "test_report"
                }
                .to_owned(),
            }],
            documentation: DocumentationRef {
                path: "docs/release-v0.8-support.md".to_owned(),
                anchor: "Support".to_owned(),
            },
        });
    }
    let manifest = CapabilityEvidenceManifest {
        schema_version: 1,
        rows,
    };
    let bundle = InputBundle {
        schema_version: 1,
        entries: vec![
            bundle_entry(
                "cargo",
                BundleKind::CargoDependencies,
                &bundle_root,
                "cargo-home",
            ),
            bundle_entry("wheels", BundleKind::LocalWheels, &bundle_root, "wheels"),
            bundle_entry(
                "approvals",
                BundleKind::ApprovalEvidence,
                &bundle_root,
                "approvals",
            ),
            bundle_entry(
                "artifacts",
                BundleKind::CandidateArtifacts,
                &bundle_root,
                "artifacts",
            ),
        ],
    };
    let mut classifications = product_crates()
        .iter()
        .map(|name| CrateClassification {
            crate_name: (*name).to_owned(),
            scope: CrateScope::Product,
        })
        .collect::<Vec<_>>();
    classifications.push(CrateClassification {
        crate_name: "alopex-tools".to_owned(),
        scope: CrateScope::Development,
    });
    let inventory = InventoryDeclaration {
        schema_version: 1,
        classifications,
        artifacts,
    };
    let audits = vec![
        audit(AllowlistedCommand::CliStartup {
            artifact_id: "alopex".to_owned(),
            argument: CliStartupArgument::Version,
        }),
        audit(AllowlistedCommand::PythonCreateEnvironment),
        audit(AllowlistedCommand::PythonInstallWheel {
            wheel: "alopex-0.8.0-cp311-abi3-manylinux.whl".to_owned(),
        }),
        audit(AllowlistedCommand::PythonImport {
            package: "alopex".to_owned(),
        }),
        audit(AllowlistedCommand::VerifyWheelContents {
            artifact_id: "alopex-0.8.0-cp311-abi3-manylinux.whl".to_owned(),
        }),
    ];
    Fixture {
        temp,
        source,
        bundle_root,
        artifacts_root,
        snapshot,
        manifest,
        bundle,
        inventory,
        audits,
    }
}

pub fn product_crates() -> [&'static str; 8] {
    [
        "alopex-core",
        "alopex-dataframe",
        "alopex-sql",
        "alopex-embedded",
        "alopex-server",
        "alopex-cluster",
        "alopex-cli",
        "alopex-py",
    ]
}

fn write_source(source: &Path) {
    let members = product_crates()
        .iter()
        .map(|name| format!("\"crates/{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    fs::create_dir_all(source.join("docs")).expect("docs");
    fs::create_dir_all(source.join("crates/alopex-py")).expect("py crate");
    fs::write(
        source.join("Cargo.toml"),
        format!("[workspace]\nmembers = [{members}]\n\n[workspace.package]\nversion = \"0.8.0\"\n"),
    )
    .expect("root manifest");
    for name in product_crates() {
        let directory = source.join("crates").join(name);
        fs::create_dir_all(&directory).expect("member directory");
        fs::write(
            directory.join("Cargo.toml"),
            format!("[package]\nname = \"{name}\"\nversion.workspace = true\n"),
        )
        .expect("member manifest");
    }
    fs::create_dir_all(source.join("crates/alopex-tools")).expect("tools directory");
    fs::write(
        source.join("crates/alopex-tools/Cargo.toml"),
        "[workspace]\n[package]\nname = \"alopex-tools\"\nversion = \"0.0.0\"\npublish = false\n",
    )
    .expect("tools manifest");
    for path in [
        "docs/cluster-operations.md",
        "docs/distributed-read.md",
        "docs/dataframe-streaming.md",
        "crates/alopex-py/README.md",
        "docs/release-v0.8-support.md",
        "docs/upgrade-v0.7.4-to-v0.8.md",
    ] {
        fs::write(source.join(path), "# Support\nfixture documentation\n").expect("documentation");
    }
}

fn write_requirements(source: &Path) -> ApprovedScopeSnapshot {
    let mut rows = Vec::new();
    for phase in 1..=4 {
        let relative = format!(".spec-workflow/specs/phase-{phase}-fixture/requirements.md");
        let path = source.join(&relative);
        fs::create_dir_all(path.parent().expect("requirement parent"))
            .expect("requirements directory");
        fs::write(
            &path,
            "# Phase fixture\n\n### Requirement 1\n\nApproved capability.\n",
        )
        .expect("requirements");
        rows.push(ApprovedScopeRow {
            phase,
            requirement_path: relative,
            requirements_sha256: sha256_file(&path).expect("requirements hash"),
            approved_revision: "2026-07-22".to_owned(),
            approval: ApprovalEvidence {
                authority: "spec-workflow-dashboard".to_owned(),
                decision_uri: format!("spec-workflow://approval/phase-{phase}"),
                evidence_path: format!("approvals/phase-{phase}.json"),
                evidence_sha256: "a".repeat(64),
            },
            anchors: vec!["Requirement 1".to_owned()],
        });
    }
    ApprovedScopeSnapshot {
        schema_version: 1,
        candidate_commit: "0123456789abcdef".to_owned(),
        rows,
    }
}

fn write_approval_evidence(snapshot: &mut ApprovedScopeSnapshot, bundle_root: &Path) {
    let approvals = bundle_root.join("approvals");
    fs::create_dir_all(&approvals).expect("approval directory");
    for row in &mut snapshot.rows {
        let path = bundle_root.join(&row.approval.evidence_path);
        fs::write(&path, format!("approved phase {}", row.phase)).expect("approval evidence");
        row.approval.evidence_sha256 = sha256_file(&path).expect("approval hash");
    }
}

fn write_artifacts(artifacts_root: &Path) -> Vec<CandidateArtifact> {
    let mut artifacts = Vec::new();
    for name in product_crates() {
        let file_name = format!("{name}-0.8.0.crate");
        let path = artifacts_root.join(&file_name);
        fs::write(&path, format!("{name} crate")).expect("crate artifact");
        artifacts.push(artifact(
            &format!("{name}-crate"),
            name,
            ArtifactKind::CrateArchive,
            &file_name,
            &path,
        ));
    }
    let cli = artifacts_root.join("alopex");
    fs::write(&cli, "binary").expect("CLI artifact");
    artifacts.push(artifact(
        "alopex-cli-binary",
        "alopex-cli",
        ArtifactKind::CliBinary,
        "alopex",
        &cli,
    ));
    let wheel_name = "alopex-0.8.0-cp311-abi3-manylinux.whl";
    let wheel = artifacts_root.join(wheel_name);
    write_stored_zip(&wheel, "alopex/_alopex.abi3.so", b"native");
    artifacts.push(artifact(
        "alopex-wheel",
        "alopex-py",
        ArtifactKind::PythonWheel,
        wheel_name,
        &wheel,
    ));
    artifacts
}

fn artifact(
    id: &str,
    crate_name: &str,
    kind: ArtifactKind,
    relative: &str,
    path: &Path,
) -> CandidateArtifact {
    CandidateArtifact {
        id: id.to_owned(),
        crate_name: crate_name.to_owned(),
        kind,
        path: relative.to_owned(),
        sha256: sha256_file(path).expect("artifact hash"),
        platform: "x86_64-unknown-linux-gnu".to_owned(),
    }
}

fn bundle_entry(id: &str, kind: BundleKind, root: &Path, relative: &str) -> InputBundleEntry {
    InputBundleEntry {
        id: id.to_owned(),
        kind,
        path: relative.to_owned(),
        sha256: hash_path(&root.join(relative)).expect("bundle hash"),
    }
}

fn audit(command: AllowlistedCommand) -> SandboxAudit {
    SandboxAudit {
        command,
        exit_code: Some(0),
        stdout: String::new(),
        stderr: String::new(),
        backend: "fixture".to_owned(),
    }
}

pub fn write_stored_zip(path: &Path, name: &str, contents: &[u8]) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&[0x50, 0x4b, 0x03, 0x04]);
    bytes.extend_from_slice(&20_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&(contents.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(contents.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(name.as_bytes());
    bytes.extend_from_slice(contents);
    let central_offset = bytes.len() as u32;
    bytes.extend_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
    bytes.extend_from_slice(&20_u16.to_le_bytes());
    bytes.extend_from_slice(&20_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&(contents.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(contents.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(name.as_bytes());
    let central_size = bytes.len() as u32 - central_offset;
    bytes.extend_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&central_size.to_le_bytes());
    bytes.extend_from_slice(&central_offset.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    fs::write(path, bytes).expect("wheel fixture");
}
