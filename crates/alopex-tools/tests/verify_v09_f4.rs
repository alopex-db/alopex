use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repository root")
        .to_path_buf()
}

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "alopex-v09-f4-verifier-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).expect("temporary directory");
    directory
}

fn verify(manifest: &Path, generate: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify-v09-f4"));
    command.args([
        "--repo-root",
        repo_root().to_str().expect("UTF-8 root"),
        "--target-version",
        "0.9.0",
        "--phase",
        "4",
        "--manifest",
        manifest.to_str().expect("UTF-8 manifest"),
    ]);
    if generate {
        command.arg("--generate");
    }
    command.output().expect("verifier process")
}

#[test]
fn generates_and_revalidates_the_complete_exact_manifest() {
    let directory = temp_dir();
    let manifest = directory.join("f4.json");
    let generated = verify(&manifest, true);
    assert!(
        generated.status.success(),
        "generation failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let document: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).expect("manifest")).expect("JSON");
    assert_eq!(document["target_version"], "0.9.0");
    assert_eq!(document["phase"], 4);
    assert_eq!(document["entries"].as_array().expect("entries").len(), 326);
    assert_eq!(document["tasks"].as_array().expect("tasks").len(), 22);
    let checked = verify(&manifest, false);
    assert!(
        checked.status.success(),
        "verification failed: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    fs::remove_dir_all(directory).expect("remove temporary directory");
}

#[test]
fn rejects_duplicate_and_non_fixed_matrix_rows() {
    let directory = temp_dir();
    let manifest = directory.join("f4.json");
    assert!(verify(&manifest, true).status.success());
    let mut document: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).expect("manifest")).expect("JSON");
    let entries = document["entries"].as_array_mut().expect("entries");
    entries[1]["id"] = entries[0]["id"].clone();
    entries[2]["matrix_status"] = serde_json::Value::String("conditional".to_owned());
    fs::write(&manifest, serde_json::to_vec(&document).expect("JSON")).expect("manifest");
    let rejected = verify(&manifest, false);
    assert!(!rejected.status.success(), "invalid manifest was accepted");
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("exact register"),
        "unexpected failure: {stderr}"
    );
    fs::remove_dir_all(directory).expect("remove temporary directory");
}
