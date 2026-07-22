use alopex_verify_candidate::policy::CandidateVerificationPolicy;

#[test]
fn unknown_programs_are_not_representable_by_the_sandbox_allowlist() {
    let _policy = CandidateVerificationPolicy {
        source_dir: std::path::PathBuf::from("/candidate/source"),
        input_bundle_dir: std::path::PathBuf::from("/candidate/input"),
        cargo_home_relative: "cargo-home".to_owned(),
        output_dir: std::path::PathBuf::from("/candidate/output"),
    };
    let error = CandidateVerificationPolicy::reject_program("git push")
        .expect_err("push must be forbidden");
    assert_eq!(error.code, "sandbox_command_forbidden");
}
