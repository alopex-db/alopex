mod common;

use alopex_verify_candidate::input_bundle::validate_snapshot_approval_evidence;

#[test]
fn input_bundle_hashes_are_checked_before_execution() {
    let fixture = common::fixture();
    assert!(fixture.bundle.validate(&fixture.bundle_root).is_empty());
    std::fs::write(
        fixture.bundle_root.join("cargo-home/registry.txt"),
        "tampered",
    )
    .expect("tamper dependency bundle");
    assert!(fixture
        .bundle
        .validate(&fixture.bundle_root)
        .iter()
        .any(|error| error.code == "input_bundle_hash_mismatch"));
}

#[test]
fn dashboard_export_tampering_blocks_scope_evidence() {
    let fixture = common::fixture();
    assert!(validate_snapshot_approval_evidence(
        &fixture.snapshot,
        &fixture.bundle,
        &fixture.bundle_root,
    )
    .is_empty());
    std::fs::write(
        fixture.bundle_root.join("approvals/phase-1.json"),
        "tampered",
    )
    .expect("tamper approval export");
    assert!(validate_snapshot_approval_evidence(
        &fixture.snapshot,
        &fixture.bundle,
        &fixture.bundle_root,
    )
    .iter()
    .any(|error| error.code == "scope_approval_hash_mismatch"));
}
