mod common;

use alopex_verify_candidate::scope_snapshot::{ApprovedScopeInput, ApprovedScopeSnapshot};

#[test]
fn snapshot_detects_requirement_hash_tampering() {
    let fixture = common::fixture();
    assert!(fixture.snapshot.validate(&fixture.source).is_empty());
    std::fs::write(
        fixture
            .source
            .join(".spec-workflow/specs/phase-2-fixture/requirements.md"),
        "# tampered\n",
    )
    .expect("tamper requirement");
    assert!(fixture
        .snapshot
        .validate(&fixture.source)
        .iter()
        .any(|error| error.code == "scope_hash_mismatch"));
}

#[test]
fn manifest_rejects_unknown_scope_anchor() {
    let fixture = common::fixture();
    let mut manifest = fixture.manifest.clone();
    manifest.rows[0].scope.requirement_anchor = "Requirement 99".to_owned();
    assert!(manifest
        .validate(&fixture.snapshot, &fixture.bundle_root, &fixture.source)
        .iter()
        .any(|error| error.code == "scope_evidence_missing"));
}

#[test]
fn generated_snapshot_is_append_only() {
    let fixture = common::fixture();
    let inputs = fixture
        .snapshot
        .rows
        .iter()
        .map(|row| ApprovedScopeInput {
            phase: row.phase,
            requirement_path: row.requirement_path.clone(),
            approved_revision: row.approved_revision.clone(),
            approval: row.approval.clone(),
            anchors: row.anchors.clone(),
        })
        .collect();
    let snapshot = ApprovedScopeSnapshot::create(
        fixture.snapshot.candidate_commit.clone(),
        &fixture.source,
        inputs,
    )
    .expect("create snapshot");
    let path = fixture.temp.path().join("approved-scope.json");
    snapshot.write_append_only(&path).expect("first write");
    assert_eq!(
        snapshot
            .write_append_only(&path)
            .expect_err("replacement must fail")
            .code,
        "io_error"
    );
}
