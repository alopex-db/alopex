mod common;

use alopex_verify_candidate::gate::{evaluate, GateInputs, ReadinessVerdict};
use alopex_verify_candidate::report::write_reports;

#[test]
fn complete_local_candidate_is_ready_and_report_rows_are_parity_preserving() {
    let fixture = common::fixture();
    let report = evaluate(GateInputs {
        requirements_root: &fixture.source,
        source_root: &fixture.source,
        bundle_root: &fixture.bundle_root,
        artifacts_root: &fixture.artifacts_root,
        snapshot: &fixture.snapshot,
        manifest: &fixture.manifest,
        bundle: &fixture.bundle,
        inventory_declaration: &fixture.inventory,
        sandbox_audits: &fixture.audits,
        sandbox_errors: &[],
        requested_publication: None,
        authorization: None,
    });
    assert_eq!(
        report.verdict,
        ReadinessVerdict::Ready,
        "{:?}",
        report.blockers
    );
    let output = fixture.temp.path().join("report");
    write_reports(&output, &report).expect("write local reports");
    let json = std::fs::read_to_string(output.join("readiness-report.json")).expect("JSON report");
    let markdown =
        std::fs::read_to_string(output.join("support-matrix.md")).expect("Markdown report");
    for row in &report.rows {
        assert!(json.contains(&row.id));
        assert!(markdown.contains(&row.id));
    }
}

#[test]
fn missing_upgrade_documentation_blocks_candidate() {
    let fixture = common::fixture();
    std::fs::remove_file(fixture.source.join("docs/upgrade-v0.7.4-to-v0.8.md"))
        .expect("remove required documentation");
    let report = evaluate(GateInputs {
        requirements_root: &fixture.source,
        source_root: &fixture.source,
        bundle_root: &fixture.bundle_root,
        artifacts_root: &fixture.artifacts_root,
        snapshot: &fixture.snapshot,
        manifest: &fixture.manifest,
        bundle: &fixture.bundle,
        inventory_declaration: &fixture.inventory,
        sandbox_audits: &fixture.audits,
        sandbox_errors: &[],
        requested_publication: None,
        authorization: None,
    });
    assert_eq!(report.verdict, ReadinessVerdict::Blocked);
    assert!(report
        .blockers
        .iter()
        .any(|blocker| blocker.code == "required_documentation_missing"));
}
