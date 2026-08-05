//! Offline exact-register verifier for the v0.9 Phase 4 transaction surface.
//!
//! `alopex-tools` is deliberately outside the product workspace.  This binary
//! therefore verifies the checked-out candidate source and an explicit evidence
//! manifest instead of linking a stale registry release and mistaking it for a
//! v0.9 result.  It never publishes, tags, or contacts a registry.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde::{Deserialize, Serialize};

const TARGET_VERSION: &str = "0.9.0";
const PHASE: u8 = 4;
const SCHEMA_VERSION: u8 = 1;
const OUTCOME_FIELDS: [&str; 14] = [
    "outcome_version",
    "transaction_id",
    "request_id",
    "participating_ranges",
    "read_point",
    "schema_version",
    "data_epoch",
    "isolation",
    "state",
    "failure_class",
    "reason_code",
    "routing",
    "retryable",
    "idempotency",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u8,
    target_version: String,
    phase: u8,
    source_sha: String,
    entries: Vec<EvidenceEntry>,
    tasks: Vec<TaskEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceEntry {
    id: String,
    source_path: String,
    public_surface: String,
    operation: String,
    matrix_status: String,
    failure_mapping: String,
    isolation: String,
    null_order_contract: String,
    fixture_id: String,
    test_id: String,
    owner: String,
    source_sha: String,
    artifact_identity: String,
    prerequisite_evidence: Vec<String>,
    outcome_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskEvidence {
    id: String,
    owner: String,
    source_path: String,
    test_id: String,
}

#[derive(Debug)]
struct Config {
    repo_root: PathBuf,
    specs_root: Option<PathBuf>,
    candidate_sha: Option<String>,
    manifest: PathBuf,
    generate: bool,
}

fn numbered_ids(prefix: &str, end_inclusive: u8) -> Vec<String> {
    (1..=end_inclusive)
        .map(|number| format!("{prefix}{number:02}"))
        .collect()
}

fn expected_matrix_ids() -> Vec<String> {
    let mut ids = numbered_ids("SQL-TXN-", 51);
    for (prefix, end) in [
        ("SQL-F-V", 4),
        ("SQL-F-N", 26),
        ("SQL-F-H", 10),
        ("SQL-F-S", 25),
        ("SQL-F-C", 9),
        ("SQL-F-SYS", 3),
        ("SQL-A", 8),
        ("API-E", 13),
        ("API-H", 17),
        ("API-G", 20),
        ("API-C", 12),
        ("API-P", 22),
        ("API-S", 8),
        ("API-HI", 43),
        ("API-CI", 55),
    ] {
        ids.extend(numbered_ids(prefix, end));
    }
    ids
}

fn expected_task_ids() -> Vec<String> {
    let mut ids = (1..=21)
        .map(|number| format!("4.{number}"))
        .collect::<Vec<_>>();
    ids.push("4.22".to_owned());
    ids
}

fn status_for(id: &str) -> &'static str {
    if let Some(suffix) = id.strip_prefix("SQL-TXN-") {
        let number = suffix.parse::<u8>().expect("fixed SQL-TXN id");
        return match number {
            10..=14 | 33..=46 => "pre-execution-reject",
            47..=48 => "single-range",
            49..=51 => "local-only",
            _ => "distributed",
        };
    }
    if let Some(suffix) = id.strip_prefix("API-E") {
        return match suffix.parse::<u8>().expect("fixed API-E id") {
            1..=5 => "single-range",
            6..=10 => "local-only",
            _ => "pre-execution-reject",
        };
    }
    if !id.starts_with("API-HI")
        && let Some(suffix) = id.strip_prefix("API-H")
    {
        return match suffix.parse::<u8>().expect("fixed API-H id") {
            1..=3 | 10..=11 => "distributed",
            4..=9 => "single-range",
            _ => "pre-execution-reject",
        };
    }
    if let Some(suffix) = id.strip_prefix("API-G") {
        return match suffix.parse::<u8>().expect("fixed API-G id") {
            1..=5 => "distributed",
            7..=9 => "single-range",
            14..=17 => "local-only",
            _ => "pre-execution-reject",
        };
    }
    if !id.starts_with("API-CI")
        && let Some(suffix) = id.strip_prefix("API-C")
    {
        return match suffix.parse::<u8>().expect("fixed API-C id") {
            1..=6 => "single-range",
            7..=9 => "distributed",
            _ => "pre-execution-reject",
        };
    }
    if let Some(suffix) = id.strip_prefix("API-P") {
        return match suffix.parse::<u8>().expect("fixed API-P id") {
            1..=9 | 12..=19 => "local-only",
            _ => "pre-execution-reject",
        };
    }
    if id.starts_with("API-S") {
        return "distributed";
    }
    if id.starts_with("API-HI") || id.starts_with("API-CI") {
        return "local-only";
    }
    if id.starts_with("SQL-") {
        return "distributed";
    }
    unreachable!("all generated IDs have a fixed status")
}

fn surface_for(id: &str) -> &'static str {
    if id.starts_with("SQL-") || id.starts_with("API-S") {
        "SQL"
    } else if id.starts_with("API-E") {
        "embedded"
    } else if id.starts_with("API-H") {
        "HTTP"
    } else if id.starts_with("API-G") {
        "gRPC"
    } else if id.starts_with("API-C") {
        "CLI"
    } else if id.starts_with("API-P") {
        "Python"
    } else {
        unreachable!("all generated IDs have a public surface")
    }
}

fn source_and_test_for(id: &str) -> (&'static str, &'static str, &'static str) {
    if id.starts_with("SQL-TXN-") {
        return (
            "crates/alopex-sql/src/transaction_classifier.rs",
            "crates/alopex-sql/tests/transaction_classifier.rs",
            "f4_transactions.json",
        );
    }
    if id.starts_with("SQL-F") || id.starts_with("SQL-A") || id.starts_with("API-S") {
        return (
            "crates/alopex-sql/src/scalar/mod.rs",
            "crates/alopex-sql/tests/v09_sql_scalar_aggregate_matrix.rs",
            "f4_transactions.json",
        );
    }
    if id.starts_with("API-E") {
        return (
            "crates/alopex-embedded/src/txn_manager.rs",
            "crates/alopex-embedded/tests/v09_embedded_register.rs",
            "f4_transactions.json",
        );
    }
    if id.starts_with("API-H") {
        return (
            "crates/alopex-server/src/http/mod.rs",
            "crates/alopex-server/tests/v09_http_surface.rs",
            "f4_recovery.json",
        );
    }
    if id.starts_with("API-G") {
        return (
            "crates/alopex-server/src/grpc/mod.rs",
            "crates/alopex-server/tests/v09_grpc_surface.rs",
            "f4_recovery.json",
        );
    }
    if id.starts_with("API-C") {
        return (
            "crates/alopex-cli/src/cli.rs",
            "crates/alopex-cli/tests/v09_cli_transaction_surface.rs",
            "f4_recovery.json",
        );
    }
    if id.starts_with("API-P") {
        return (
            "crates/alopex-py/python/alopex/_alopex.pyi",
            "crates/alopex-py/tests/test_v09_transaction_sync.py",
            "f4_transactions.json",
        );
    }
    unreachable!("all generated IDs have source and test ownership")
}

fn prerequisite_evidence(status: &str) -> Vec<String> {
    if status == "distributed" {
        vec![
            "phase1:routing-ownership-recovery".to_owned(),
            "chirps:version-feature-peer-auth".to_owned(),
        ]
    } else {
        vec!["not-applicable".to_owned()]
    }
}

fn validate_candidate_sha(sha: String) -> Result<String, String> {
    if sha.len() != 40 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("candidate SHA が不正: {sha}"));
    }
    Ok(sha)
}

fn candidate_sha(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|error| format!("candidate SHA を取得できない: {error}"))?;
    if !output.status.success() {
        return Err("candidate SHA を取得できない: git rev-parse HEAD が失敗".to_owned());
    }
    let sha = String::from_utf8(output.stdout)
        .map_err(|_| "candidate SHA が UTF-8 ではない".to_owned())?
        .trim()
        .to_owned();
    validate_candidate_sha(sha)
}

fn build_manifest(_root: &Path, source_sha: String) -> Result<Manifest, String> {
    let entries = expected_matrix_ids()
        .into_iter()
        .map(|id| {
            let status = status_for(&id);
            let (source_path, test_id, fixture_id) = source_and_test_for(&id);
            EvidenceEntry {
                id: id.clone(),
                source_path: source_path.to_owned(),
                public_surface: surface_for(&id).to_owned(),
                operation: id,
                matrix_status: status.to_owned(),
                failure_mapping: if status == "pre-execution-reject" {
                    "rejected/invalid_request/unsupported".to_owned()
                } else {
                    "common Phase 4 outcome mapping".to_owned()
                },
                isolation: "snapshot".to_owned(),
                null_order_contract: "preserve local null/type/order contract".to_owned(),
                fixture_id: fixture_id.to_owned(),
                test_id: test_id.to_owned(),
                owner: "Phase 4 matrix owner".to_owned(),
                source_sha: source_sha.clone(),
                artifact_identity: format!("candidate-source:{source_sha}"),
                prerequisite_evidence: prerequisite_evidence(status),
                outcome_fields: OUTCOME_FIELDS.iter().map(ToString::to_string).collect(),
            }
        })
        .collect();
    let tasks = expected_task_ids()
        .into_iter()
        .map(|id| TaskEvidence {
            source_path: "specs/alopex-v0.9.0-phase-4-distributed-transactions/tasks.md".to_owned(),
            test_id: format!("task-crosswalk:{id}"),
            owner: id.clone(),
            id,
        })
        .collect();
    Ok(Manifest {
        schema_version: SCHEMA_VERSION,
        target_version: TARGET_VERSION.to_owned(),
        phase: PHASE,
        source_sha,
        entries,
        tasks,
    })
}

fn non_empty(value: &str, field: &str, id: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{id}: {field} が空"))
    } else {
        Ok(())
    }
}

fn exact_set(
    actual: impl IntoIterator<Item = String>,
    expected: &[String],
    label: &str,
) -> Result<(), String> {
    let mut counts = BTreeMap::<String, usize>::new();
    for id in actual {
        *counts.entry(id).or_default() += 1;
    }
    let expected = expected.iter().cloned().collect::<BTreeSet<_>>();
    let actual = counts.keys().cloned().collect::<BTreeSet<_>>();
    let missing = expected.difference(&actual).cloned().collect::<Vec<_>>();
    let unknown = actual.difference(&expected).cloned().collect::<Vec<_>>();
    let duplicate = counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    if missing.is_empty() && unknown.is_empty() && duplicate.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{label} が exact register ではない: missing={missing:?}, unknown={unknown:?}, duplicate={duplicate:?}"
        ))
    }
}

fn validate_manifest(manifest: &Manifest) -> Result<(), String> {
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "schema_version が不正: {}",
            manifest.schema_version
        ));
    }
    if manifest.target_version != TARGET_VERSION || manifest.phase != PHASE {
        return Err(format!(
            "target が v{} Phase {} ではない",
            TARGET_VERSION, PHASE
        ));
    }
    non_empty(&manifest.source_sha, "source_sha", "manifest")?;
    let expected_entries = expected_matrix_ids();
    exact_set(
        manifest.entries.iter().map(|entry| entry.id.clone()),
        &expected_entries,
        "matrix entries",
    )?;
    exact_set(
        manifest.tasks.iter().map(|task| task.id.clone()),
        &expected_task_ids(),
        "task entries",
    )?;
    let allowed_statuses = BTreeSet::from([
        "distributed",
        "single-range",
        "local-only",
        "pre-execution-reject",
    ]);
    for entry in &manifest.entries {
        for (field, value) in [
            ("source_path", entry.source_path.as_str()),
            ("public_surface", entry.public_surface.as_str()),
            ("operation", entry.operation.as_str()),
            ("failure_mapping", entry.failure_mapping.as_str()),
            ("isolation", entry.isolation.as_str()),
            ("null_order_contract", entry.null_order_contract.as_str()),
            ("fixture_id", entry.fixture_id.as_str()),
            ("test_id", entry.test_id.as_str()),
            ("owner", entry.owner.as_str()),
            ("source_sha", entry.source_sha.as_str()),
            ("artifact_identity", entry.artifact_identity.as_str()),
        ] {
            non_empty(value, field, &entry.id)?;
        }
        if !allowed_statuses.contains(entry.matrix_status.as_str()) {
            return Err(format!(
                "{}: conditional/unknown matrix_status は許可されない: {}",
                entry.id, entry.matrix_status
            ));
        }
        if entry.matrix_status != status_for(&entry.id) {
            return Err(format!(
                "{}: matrix_status が固定 register と不一致: {}",
                entry.id, entry.matrix_status
            ));
        }
        if entry.isolation != "snapshot" {
            return Err(format!("{}: snapshot isolation ではない", entry.id));
        }
        if entry.outcome_fields != OUTCOME_FIELDS {
            return Err(format!("{}: common outcome schema が不一致", entry.id));
        }
        if entry.prerequisite_evidence.is_empty()
            || entry
                .prerequisite_evidence
                .iter()
                .any(|value| value.trim().is_empty())
        {
            return Err(format!("{}: prerequisite_evidence が欠けている", entry.id));
        }
        if entry.matrix_status == "distributed"
            && !(entry
                .prerequisite_evidence
                .iter()
                .any(|value| value == "phase1:routing-ownership-recovery")
                && entry
                    .prerequisite_evidence
                    .iter()
                    .any(|value| value == "chirps:version-feature-peer-auth"))
        {
            return Err(format!(
                "{}: distributed claim に Phase1/Chirps prerequisite evidence がない",
                entry.id
            ));
        }
    }
    for task in &manifest.tasks {
        non_empty(&task.owner, "owner", &task.id)?;
        non_empty(&task.source_path, "source_path", &task.id)?;
        non_empty(&task.test_id, "test_id", &task.id)?;
    }
    Ok(())
}

fn require_file_with(root: &Path, relative: &str, needle: &str) -> Result<String, String> {
    let path = root.join(relative);
    let content = fs::read_to_string(&path)
        .map_err(|error| format!("required source が読めない {}: {error}", path.display()))?;
    if content.contains(needle) {
        Ok(content)
    } else {
        Err(format!(
            "required source {} に `{needle}` がない",
            path.display()
        ))
    }
}

fn spec_tasks_file(root: &Path, specs_root: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(specs_root) = specs_root {
        let candidate = specs_root
            .join("specs/alopex-v0.9.0-phase-4-distributed-transactions/tasks.md");
        if candidate.is_file() {
            return Ok(candidate);
        }
        return Err(format!(
            "--specs-root に approved Phase 4 tasks.md が見つからない: {}",
            candidate.display()
        ));
    }
    root.ancestors()
        .map(|ancestor| {
            ancestor
                .join(".spec-workflow")
                .join("specs/alopex-v0.9.0-phase-4-distributed-transactions/tasks.md")
        })
        .find(|path| path.is_file())
        .ok_or_else(|| {
            "approved Phase 4 tasks.md が worktree の管理ルートに見つからない".to_owned()
        })
}

fn verify_literal_register(source: &str, prefix: &str, end: u8, label: &str) -> Result<(), String> {
    let expected = numbered_ids(prefix, end);
    for id in &expected {
        if !source.contains(&format!("\"{id}\"")) {
            return Err(format!(
                "{label}: required ID {id} が source register にない"
            ));
        }
    }
    Ok(())
}

fn verify_source_contract(root: &Path, specs_root: Option<&Path>) -> Result<(), String> {
    let transaction = require_file_with(
        root,
        "crates/alopex-sql/src/transaction_classifier.rs",
        "TRANSACTION_SQL_STATEMENT_MATRIX",
    )?;
    verify_literal_register(&transaction, "SQL-TXN-", 51, "SQL transaction")?;
    let scalar = require_file_with(root, "crates/alopex-sql/src/scalar/mod.rs", "signatures()")?;
    for (prefix, end) in [
        ("SQL-F-V", 4),
        ("SQL-F-N", 26),
        ("SQL-F-H", 10),
        ("SQL-F-S", 25),
        ("SQL-F-C", 9),
        ("SQL-F-SYS", 3),
        ("SQL-A", 8),
    ] {
        verify_literal_register(&scalar, prefix, end, "SQL scalar/aggregate")?;
    }
    let matrix = require_file_with(root, "tests/f4_surface_matrix.rs", "COMMON_OUTCOME_FIELDS")?;
    for (prefix, end) in [
        ("API-E", 13),
        ("API-H", 17),
        ("API-G", 20),
        ("API-C", 12),
        ("API-P", 22),
        ("API-S", 8),
    ] {
        let multiline = format!("\"{prefix}\",\n        {end},");
        let inline = format!("rows(\"{prefix}\", {end},");
        if !matrix.contains(&multiline) && !matrix.contains(&inline) {
            return Err(format!("F4 API register marker がない: {prefix} 1..={end}"));
        }
    }
    for marker in [
        "numbered_ids(\"API-HI\", 43)",
        "numbered_ids(\"API-CI\", 55)",
    ] {
        if !matrix.contains(marker) {
            return Err(format!("F4 inherited register marker がない: {marker}"));
        }
    }
    for field in OUTCOME_FIELDS {
        if !matrix.contains(&format!("\"{field}\"")) {
            return Err(format!("F4 common outcome field がない: {field}"));
        }
    }
    for (relative, needle) in [
        ("crates/alopex-cli/src/cli.rs", "pub struct Cli"),
        (
            "crates/alopex-cli/tests/v09_cli_transaction_surface.rs",
            "cli_kv_transaction_preserves_request_identity",
        ),
        ("crates/alopex-server/src/http/mod.rs", "route"),
        (
            "crates/alopex-server/tests/v09_http_surface.rs",
            "I13_REGISTER",
        ),
        ("crates/alopex-server/proto/alopex.proto", "service Alopex"),
        ("crates/alopex-server/src/grpc/mod.rs", "Transaction"),
        (
            "crates/alopex-server/tests/v09_grpc_surface.rs",
            "I14_REGISTER",
        ),
        ("crates/alopex-embedded/src/txn_manager.rs", "Transaction"),
        (
            "crates/alopex-embedded/tests/v09_embedded_register.rs",
            "I21_REQUIRED_ROWS",
        ),
        (
            "crates/alopex-py/python/alopex/_alopex.pyi",
            "class Transaction",
        ),
        (
            "crates/alopex-py/python/alopex/asyncio.pyi",
            "class AsyncTransaction",
        ),
        (
            "crates/alopex-py/tests/test_v09_transaction_sync.py",
            "test_sync_transaction_status_adds_canonical_local_outcome",
        ),
        (
            "crates/alopex-py/tests/test_v09_transaction_async.py",
            "test_async_transaction_forwards_request_ids",
        ),
        (
            "crates/alopex-sql/nim-sql-parser/src/alopex_sql_parser.nim",
            "PRAGMA",
        ),
        (
            "crates/alopex-sql/src/nim_ffi.rs",
            "parser_contract_version",
        ),
        (
            "crates/alopex-core/tests/transaction_compatibility.rs",
            "raw_kv_preserves_bytes_order_rollback",
        ),
        (
            "crates/alopex-cli/tests/transaction_compatibility.rs",
            "inherited_cli_register_stays_local",
        ),
        (
            "crates/alopex-py/tests/test_transaction_compatibility.py",
            "test_inherited_embedded_kv_sql_dataframe_and_lifecycle_remain_local_only",
        ),
        ("tests/fixtures/f4_transactions.json", "transaction_id"),
        ("tests/fixtures/f4_recovery.json", "request_id"),
    ] {
        require_file_with(root, relative, needle)?;
    }
    let tasks_path = spec_tasks_file(root, specs_root)?;
    let tasks = fs::read_to_string(&tasks_path)
        .map_err(|error| format!("approved Phase 4 tasks.md を読めない: {error}"))?;
    if !tasks.contains("4.22") {
        return Err(format!(
            "Phase 4 task register に 4.22 がない: {}",
            tasks_path.display()
        ));
    }
    for task in expected_task_ids() {
        if !tasks.contains(&format!("{task} ")) && !tasks.contains(&format!("{task}\n")) {
            return Err(format!("Phase 4 task register に {task} がない"));
        }
    }
    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut repo_root =
        env::current_dir().map_err(|error| format!("cwd を取得できない: {error}"))?;
    let mut specs_root = None;
    let mut candidate_sha = None;
    let mut manifest = None;
    let mut generate = false;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--repo-root" => {
                repo_root = PathBuf::from(args.next().ok_or("--repo-root の値がない")?);
            }
            "--specs-root" => {
                specs_root = Some(PathBuf::from(
                    args.next().ok_or("--specs-root の値がない")?,
                ));
            }
            "--candidate-sha" => {
                candidate_sha = Some(validate_candidate_sha(
                    args.next().ok_or("--candidate-sha の値がない")?,
                )?);
            }
            "--manifest" => {
                manifest = Some(PathBuf::from(args.next().ok_or("--manifest の値がない")?));
            }
            "--generate" => generate = true,
            "--target-version" => {
                let value = args.next().ok_or("--target-version の値がない")?;
                if value != TARGET_VERSION {
                    return Err(format!(
                        "v{TARGET_VERSION} 以外はこの verifier の対象外: {value}"
                    ));
                }
            }
            "--phase" => {
                let value = args.next().ok_or("--phase の値がない")?;
                if value != PHASE.to_string() {
                    return Err(format!(
                        "Phase {PHASE} 以外はこの verifier の対象外: {value}"
                    ));
                }
            }
            "--help" | "-h" => {
                return Err(
                    "usage: verify-v09-f4 --target-version 0.9.0 --phase 4 --manifest <path> [--repo-root <candidate-root>] [--specs-root <approved-spec-workflow-root>] [--candidate-sha <40-hex-sha>] [--generate]"
                        .to_owned(),
                );
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let manifest = manifest.ok_or("--manifest が必須")?;
    Ok(Config {
        repo_root,
        specs_root,
        candidate_sha,
        manifest,
        generate,
    })
}

fn run(config: Config) -> Result<(), String> {
    let repo_root = config.repo_root.canonicalize().map_err(|error| {
        format!(
            "repo root を解決できない {}: {error}",
            config.repo_root.display()
        )
    })?;
    verify_source_contract(&repo_root, config.specs_root.as_deref())?;
    let source_sha = match config.candidate_sha {
        Some(candidate_sha) => candidate_sha,
        None => candidate_sha(&repo_root)?,
    };
    if config.generate {
        let manifest = build_manifest(&repo_root, source_sha)?;
        validate_manifest(&manifest)?;
        if let Some(parent) = config.manifest.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("manifest directory を作成できない: {error}"))?;
        }
        let body = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| format!("manifest を JSON 化できない: {error}"))?;
        fs::write(&config.manifest, body).map_err(|error| {
            format!("manifest を書けない {}: {error}", config.manifest.display())
        })?;
        println!(
            "generated v{} Phase {} manifest: {} entries, {} tasks -> {}",
            TARGET_VERSION,
            PHASE,
            manifest.entries.len(),
            manifest.tasks.len(),
            config.manifest.display()
        );
        return Ok(());
    }
    let bytes = fs::read(&config.manifest)
        .map_err(|error| format!("manifest を読めない {}: {error}", config.manifest.display()))?;
    let manifest = serde_json::from_slice::<Manifest>(&bytes)
        .map_err(|error| format!("manifest schema が不正: {error}"))?;
    validate_manifest(&manifest)?;
    if manifest.source_sha != source_sha
        || manifest
            .entries
            .iter()
            .any(|entry| entry.source_sha != source_sha)
    {
        return Err("manifest source_sha が candidate HEAD と一致しない".to_owned());
    }
    println!(
        "verified v{} Phase {} manifest: {} entries, {} tasks",
        TARGET_VERSION,
        PHASE,
        manifest.entries.len(),
        manifest.tasks.len()
    );
    Ok(())
}

fn main() -> ExitCode {
    match parse_args().and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("verify-v09-f4: {error}");
            ExitCode::from(2)
        }
    }
}
