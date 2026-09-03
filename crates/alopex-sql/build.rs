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
    println!("cargo:rerun-if-changed={}", resolved.library_path.display());

    println!(
        "cargo:rerun-if-changed={}",
        resolved.static_library_path.display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        resolved.directory.display()
    );
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,{}",
        resolved.directory.display()
    );
}
