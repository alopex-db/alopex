mod build_support;

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use build_support::{LinkBehavior, resolve_native_library};

fn main() {
    println!("cargo:rerun-if-env-changed=NIM_SQL_PARSER_LIB_DIR");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let target = env::var("TARGET").expect("TARGET");
    let explicit_dir = env::var_os("NIM_SQL_PARSER_LIB_DIR");
    let resolved = resolve_native_library(&manifest_dir, &target, explicit_dir.as_deref())
        .unwrap_or_else(|cause| panic!("native parser resolution failed: {cause}"));

    let link_directory = stage_native_library(&resolved, &target);

    let vendor_manifest =
        manifest_dir.join("nim-sql-parser/vendor/parser-vendor-manifest-v0.8.4.json");
    println!("cargo:rerun-if-changed={}", vendor_manifest.display());
    println!(
        "cargo:rerun-if-changed={}",
        resolved.directory.join("CONTRACT_VERSION").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        resolved.directory.join("SHA256SUMS").display()
    );
    println!("cargo:rerun-if-changed={}", resolved.library_path.display());

    println!(
        "cargo:rustc-link-search=native={}",
        link_directory.display()
    );
    if resolved.link_behavior == LinkBehavior::UnixRpath {
        println!("cargo:rustc-link-lib=dylib=alopex_sql_parser");
    }

    // `cargo:rustc-link-arg` emitted by a dependency build script does not
    // propagate to final downstream binaries. The metadata path remains the
    // consumer build-script contract; these arguments cover alopex-sql's own
    // test executables on Unix targets.
    println!("cargo::metadata=libdir={}", link_directory.display());
    if resolved.link_behavior == LinkBehavior::UnixRpath {
        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,{}",
            link_directory.display()
        );
        println!(
            "cargo:rustc-link-arg-tests=-Wl,-rpath,{}",
            link_directory.display()
        );
    }
}

fn stage_native_library(resolved: &build_support::ResolvedNativeLibrary, target: &str) -> PathBuf {
    if !matches!(target, "aarch64-apple-darwin" | "x86_64-apple-darwin") {
        return resolved.directory.clone();
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let stage_directory = out_dir.join("alopex-sql-parser");
    fs::create_dir_all(&stage_directory)
        .unwrap_or_else(|cause| panic!("could not create parser staging directory: {cause}"));
    let staged_library = stage_directory.join(
        resolved
            .library_path
            .file_name()
            .expect("parser library filename"),
    );
    fs::copy(&resolved.library_path, &staged_library).unwrap_or_else(|cause| {
        panic!(
            "could not stage native parser library {}: {cause}",
            resolved.library_path.display()
        )
    });

    let install_name = format!(
        "@rpath/{}",
        staged_library
            .file_name()
            .expect("parser library filename")
            .to_string_lossy()
    );
    let install_name_status = Command::new("install_name_tool")
        .args(["-id", &install_name])
        .arg(&staged_library)
        .status()
        .unwrap_or_else(|cause| panic!("could not execute install_name_tool: {cause}"));
    assert!(
        install_name_status.success(),
        "install_name_tool failed for {}",
        staged_library.display()
    );

    stage_directory
}
