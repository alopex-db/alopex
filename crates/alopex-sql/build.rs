mod build_support;

use std::env;
use std::path::PathBuf;

use build_support::resolve_native_library;

fn main() {
    println!("cargo:rerun-if-env-changed=NIM_SQL_PARSER_LIB_DIR");
    println!(
        "cargo:rerun-if-env-changed={}",
        build_support::ALLOW_LOCAL_BUILD_ENV
    );

    println!(
        "cargo:rerun-if-env-changed={}",
        build_support::SUPPLEMENT_SHA256_ENV
    );

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let target = env::var("TARGET").expect("TARGET");
    let explicit_dir = env::var_os("NIM_SQL_PARSER_LIB_DIR");
    let resolved = resolve_native_library(&manifest_dir, &target, explicit_dir.as_deref())
        .unwrap_or_else(|cause| panic!("native parser resolution failed: {cause}"));

    let vendor_manifest = manifest_dir.join("nim-sql-parser/vendor/parser-vendor-manifest.json");
    println!("cargo:rerun-if-changed={}", vendor_manifest.display());
    println!(
        "cargo:rerun-if-changed={}",
        resolved.directory.join("CONTRACT_VERSION").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        resolved.directory.join("SHA256SUMS").display()
    );
    let supplement = resolved.directory.join(build_support::SUPPLEMENT_FILENAME);
    // Watching a nonexistent optional file makes Cargo rerun this script on
    // every build. Resolution already fails if a required supplement is absent.
    if supplement.is_file() {
        println!("cargo:rerun-if-changed={}", supplement.display());
    }
    println!("cargo:rerun-if-changed={}", resolved.library_path.display());

    println!(
        "cargo:rerun-if-changed={}",
        resolved.static_library_path.display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        resolved.directory.display()
    );
}
