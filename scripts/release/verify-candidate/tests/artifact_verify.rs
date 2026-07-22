mod common;

use alopex_verify_candidate::artifact_verify::validate_artifact;

#[test]
fn wheel_without_native_extension_is_blocked() {
    let fixture = common::fixture();
    let mut wheel = fixture
        .inventory
        .artifacts
        .iter()
        .find(|artifact| {
            matches!(
                artifact.kind,
                alopex_verify_candidate::artifact_verify::ArtifactKind::PythonWheel
            )
        })
        .expect("wheel artifact")
        .clone();
    let path = fixture.artifacts_root.join(&wheel.path);
    common::write_stored_zip(&path, "alopex/__init__.py", b"python");
    wheel.sha256 = alopex_verify_candidate::scope_snapshot::sha256_file(&path).expect("wheel hash");
    assert!(validate_artifact(&wheel, &fixture.artifacts_root, "0.8.0")
        .iter()
        .any(|error| error.code == "wheel_native_extension_missing"));
}
