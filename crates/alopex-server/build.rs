fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/alopex.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/alopex.proto");

    // alopex-sql の build.rs は Nim 共有ライブラリの link-search/link-lib しか
    // 出せず（依存クレートの rustc-link-arg は最終バイナリに伝播しない cargo
    // の仕様）、rpath はこのバイナリ自身の build.rs で設定する必要がある。
    // alopex-sql の links = "alopex_sql_parser" が cargo::metadata=libdir=...
    // で公開した値を DEP_ALOPEX_SQL_PARSER_LIBDIR として受け取る。
    // (edition 2021 の build.rs のため let chains は使わない)
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR") {
        if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{libdir}");
            // Library unit-test harnesses are not covered by the test-target
            // directive alone, so retain the parser search path for every
            // final link target as well.
            println!("cargo:rustc-link-arg=-Wl,-rpath,{libdir}");
            println!("cargo:rustc-link-arg-tests=-Wl,-rpath,{libdir}");
        }
    }

    Ok(())
}
