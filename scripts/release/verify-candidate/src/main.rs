use alopex_verify_candidate::artifact_verify::ArtifactKind;
use alopex_verify_candidate::gate::{
    evaluate, AuthorizationRecord, GateInputs, PublicationRequest,
};
use alopex_verify_candidate::input_bundle::{BundleKind, InputBundle};
use alopex_verify_candidate::inventory::InventoryDeclaration;
use alopex_verify_candidate::manifest::CapabilityEvidenceManifest;
use alopex_verify_candidate::policy::{AllowlistedCommand, CargoAction, CliStartupArgument};
use alopex_verify_candidate::report::write_reports;
use alopex_verify_candidate::sandbox::SandboxRunner;
use alopex_verify_candidate::scope_snapshot::{ApprovedScopeInput, ApprovedScopeSnapshot};
use alopex_verify_candidate::{Result, VerificationError};
use std::path::{Path, PathBuf};

fn main() -> std::process::ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::from(2),
        Err(error) => {
            eprintln!("verify-candidate: {error}");
            std::process::ExitCode::from(3)
        }
    }
}

fn run(arguments: Vec<String>) -> Result<bool> {
    if arguments.first().map(String::as_str) == Some("snapshot") {
        return run_snapshot(arguments);
    }
    let options = Options::parse(arguments)?;
    let source_dir = canonical_directory(&options.source, "source")?;
    let bundle_dir = canonical_directory(&options.bundle, "input bundle")?;
    let output_dir = prepare_output_directory(&options.output, &source_dir, &bundle_dir)?;
    let snapshot = ApprovedScopeSnapshot::read(&options.snapshot)?;
    let manifest = CapabilityEvidenceManifest::read(&options.manifest)?;
    let bundle = InputBundle::read(&options.bundle.join("input-bundle.json"))?;
    let inventory = InventoryDeclaration::read(&options.inventory)?;
    let artifacts_root = bundle
        .entry(BundleKind::CandidateArtifacts)
        .ok_or_else(|| {
            VerificationError::new("input_bundle_required_entry_missing", "candidate_artifacts")
        })
        .and_then(|entry| {
            alopex_verify_candidate::scope_snapshot::resolve_read_only(&bundle_dir, &entry.path)
        })?;
    let cargo_home_relative = bundle
        .entry(BundleKind::CargoDependencies)
        .ok_or_else(|| {
            VerificationError::new("input_bundle_required_entry_missing", "cargo_dependencies")
        })?
        .path
        .clone();

    let (_, inventory_errors) = inventory.verify(&source_dir, &artifacts_root);
    let mut sandbox_errors = inventory_errors;
    let mut audits = Vec::new();
    if sandbox_errors.is_empty() {
        match SandboxRunner::detect() {
            Ok(runner) => {
                let policy = alopex_verify_candidate::policy::CandidateVerificationPolicy {
                    source_dir: source_dir.clone(),
                    input_bundle_dir: bundle_dir.clone(),
                    cargo_home_relative,
                    output_dir: output_dir.clone(),
                };
                for command in planned_commands(&inventory) {
                    match runner.execute(&policy, command, &artifacts_root) {
                        Ok(audit) if audit.exit_code == Some(0) => audits.push(audit),
                        Ok(audit) => {
                            sandbox_errors.push(VerificationError::new(
                                "sandbox_command_failed",
                                format!("{:?}: {}", audit.command, audit.stderr.trim()),
                            ));
                            audits.push(audit);
                        }
                        Err(error) => sandbox_errors.push(error),
                    }
                }
            }
            Err(error) => sandbox_errors.push(error),
        }
    }
    let authorization = options
        .authorization
        .as_deref()
        .map(read_authorization)
        .transpose()?;
    let publication = options
        .requested_publication
        .as_ref()
        .map(|action| PublicationRequest {
            action: action.clone(),
        });
    let report = evaluate(GateInputs {
        requirements_root: &options.requirements_root,
        source_root: &source_dir,
        bundle_root: &bundle_dir,
        artifacts_root: &artifacts_root,
        snapshot: &snapshot,
        manifest: &manifest,
        bundle: &bundle,
        inventory_declaration: &inventory,
        sandbox_audits: &audits,
        sandbox_errors: &sandbox_errors,
        requested_publication: publication.as_ref(),
        authorization: authorization.as_ref(),
    });
    write_reports(&output_dir, &report)?;
    println!("candidate verdict: {:?}", report.verdict);
    Ok(matches!(
        report.verdict,
        alopex_verify_candidate::gate::ReadinessVerdict::Ready
    ))
}

fn run_snapshot(arguments: Vec<String>) -> Result<bool> {
    let mut values = arguments.into_iter();
    debug_assert_eq!(values.next().as_deref(), Some("snapshot"));
    let mut requirements_root = None;
    let mut candidate_commit = None;
    let mut input = None;
    let mut output = None;
    while let Some(flag) = values.next() {
        let value = values.next().ok_or_else(|| {
            VerificationError::new(
                "usage",
                "snapshot options require values; see verify-candidate snapshot --help",
            )
        })?;
        match flag.as_str() {
            "--requirements-root" => requirements_root = Some(PathBuf::from(value)),
            "--candidate-commit" => candidate_commit = Some(value),
            "--input" => input = Some(PathBuf::from(value)),
            "--output" => output = Some(PathBuf::from(value)),
            _ => {
                return Err(VerificationError::new(
                    "usage",
                    format!("unknown snapshot flag {flag}"),
                ))
            }
        }
    }
    let requirements_root = required(requirements_root, "--requirements-root")?;
    let candidate_commit = candidate_commit.ok_or_else(|| {
        VerificationError::new("usage", "--candidate-commit is required for snapshot")
    })?;
    let input = required(input, "--input")?;
    let output = required(output, "--output")?;
    let file = std::fs::File::open(input).map_err(|error| {
        VerificationError::new("io_error", format!("open snapshot input: {error}"))
    })?;
    let rows: Vec<ApprovedScopeInput> = serde_json::from_reader(file).map_err(|error| {
        VerificationError::new("invalid_scope_snapshot_input", error.to_string())
    })?;
    let snapshot = ApprovedScopeSnapshot::create(candidate_commit, &requirements_root, rows)?;
    snapshot.write_append_only(&output)?;
    println!("created immutable scope snapshot: {}", output.display());
    Ok(true)
}

fn planned_commands(inventory: &InventoryDeclaration) -> Vec<AllowlistedCommand> {
    let mut commands = vec![
        AllowlistedCommand::Cargo {
            action: CargoAction::Build,
        },
        AllowlistedCommand::Cargo {
            action: CargoAction::Test,
        },
    ];
    for artifact in &inventory.artifacts {
        let Some(file_name) = Path::new(&artifact.path)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            continue;
        };
        match artifact.kind {
            ArtifactKind::CliBinary => commands.push(AllowlistedCommand::CliStartup {
                artifact_id: file_name.to_owned(),
                argument: CliStartupArgument::Version,
            }),
            ArtifactKind::PythonWheel => {
                commands.push(AllowlistedCommand::PythonCreateEnvironment);
                commands.push(AllowlistedCommand::PythonInstallWheel {
                    wheel: file_name.to_owned(),
                });
                commands.push(AllowlistedCommand::PythonImport {
                    package: "alopex".to_owned(),
                });
                commands.push(AllowlistedCommand::VerifyWheelContents {
                    artifact_id: file_name.to_owned(),
                });
            }
            ArtifactKind::CrateArchive => {}
        }
    }
    commands
}

fn read_authorization(path: &Path) -> Result<AuthorizationRecord> {
    let file = std::fs::File::open(path).map_err(|error| {
        VerificationError::new("io_error", format!("open authorization record: {error}"))
    })?;
    serde_json::from_reader(file)
        .map_err(|error| VerificationError::new("invalid_authorization_record", error.to_string()))
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let path = std::fs::canonicalize(path).map_err(|error| {
        VerificationError::new("io_error", format!("canonicalize {label}: {error}"))
    })?;
    if !path.is_dir() {
        return Err(VerificationError::new(
            "candidate_path_invalid",
            format!("{label} is not a directory"),
        ));
    }
    Ok(path)
}

fn prepare_output_directory(path: &Path, source_dir: &Path, bundle_dir: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = std::fs::canonicalize(parent).map_err(|error| {
        VerificationError::new("io_error", format!("canonicalize output parent: {error}"))
    })?;
    let output = parent.join(path.file_name().ok_or_else(|| {
        VerificationError::new("candidate_output_invalid", "output must name a directory")
    })?);
    if output.starts_with(source_dir) || output.starts_with(bundle_dir) {
        return Err(VerificationError::new(
            "candidate_output_invalid",
            "report output may not be inside the read-only source or input bundle",
        ));
    }
    std::fs::create_dir_all(&output).map_err(|error| {
        VerificationError::new("io_error", format!("create candidate output: {error}"))
    })?;
    let output = canonical_directory(&output, "output")?;
    if output.starts_with(source_dir) || output.starts_with(bundle_dir) {
        return Err(VerificationError::new(
            "candidate_output_invalid",
            "report output resolves inside the read-only source or input bundle",
        ));
    }
    Ok(output)
}

#[derive(Debug)]
struct Options {
    requirements_root: PathBuf,
    source: PathBuf,
    bundle: PathBuf,
    snapshot: PathBuf,
    manifest: PathBuf,
    inventory: PathBuf,
    output: PathBuf,
    authorization: Option<PathBuf>,
    requested_publication: Option<String>,
}

impl Options {
    fn parse(arguments: Vec<String>) -> Result<Self> {
        let mut values = arguments.into_iter();
        if values.next().as_deref() != Some("verify") {
            return Err(VerificationError::new("usage", usage()));
        }
        let mut output = None;
        let mut requirements_root = None;
        let mut source = None;
        let mut bundle = None;
        let mut snapshot = None;
        let mut manifest = None;
        let mut inventory = None;
        let mut authorization = None;
        let mut requested_publication = None;
        while let Some(flag) = values.next() {
            let value = values.next().ok_or_else(|| {
                VerificationError::new("usage", format!("{flag} needs a value\n{}", usage()))
            })?;
            match flag.as_str() {
                "--requirements-root" => requirements_root = Some(PathBuf::from(value)),
                "--source" => source = Some(PathBuf::from(value)),
                "--bundle" => bundle = Some(PathBuf::from(value)),
                "--snapshot" => snapshot = Some(PathBuf::from(value)),
                "--manifest" => manifest = Some(PathBuf::from(value)),
                "--inventory" => inventory = Some(PathBuf::from(value)),
                "--output" => output = Some(PathBuf::from(value)),
                "--authorization" => authorization = Some(PathBuf::from(value)),
                "--requested-publication" => requested_publication = Some(value),
                _ => {
                    return Err(VerificationError::new(
                        "usage",
                        format!("unknown flag {flag}\n{}", usage()),
                    ))
                }
            }
        }
        Ok(Self {
            requirements_root: required(requirements_root, "--requirements-root")?,
            source: required(source, "--source")?,
            bundle: required(bundle, "--bundle")?,
            snapshot: required(snapshot, "--snapshot")?,
            manifest: required(manifest, "--manifest")?,
            inventory: required(inventory, "--inventory")?,
            output: required(output, "--output")?,
            authorization,
            requested_publication,
        })
    }
}

fn required(value: Option<PathBuf>, flag: &str) -> Result<PathBuf> {
    value.ok_or_else(|| VerificationError::new("usage", format!("{flag} is required\n{}", usage())))
}

fn usage() -> &'static str {
    "usage: verify-candidate verify --requirements-root DIR --source DIR --bundle DIR --snapshot FILE --manifest FILE --inventory FILE --output DIR [--authorization FILE --requested-publication ACTION]"
}
