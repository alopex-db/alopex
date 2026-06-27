use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=NIM_SQL_PARSER_LIB_DIR");
    println!("cargo:rerun-if-changed=nim-sql-parser/libalopex_sql_parser.so");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let lib_dir = env::var_os("NIM_SQL_PARSER_LIB_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            let local = manifest_dir.join("nim-sql-parser");
            local
                .join("libalopex_sql_parser.so")
                .exists()
                .then_some(local)
        })
        .unwrap_or_else(|| PathBuf::from("/usr/local/lib"));

    let lib_path = lib_dir.join("libalopex_sql_parser.so");
    if !lib_path.exists() {
        panic!(
            "libalopex_sql_parser.so not found at {}. Run `make nim-parser` or set NIM_SQL_PARSER_LIB_DIR.",
            lib_path.display()
        );
    }

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=alopex_sql_parser");

    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }
}
