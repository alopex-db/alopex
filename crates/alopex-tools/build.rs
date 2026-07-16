fn main() {
    // alopex-tools links the released alopex-sql crate into
    // verify-release-embedded. The Nim parser shared library location is
    // exposed by alopex-sql's build.rs via DEP_ALOPEX_SQL_PARSER_LIBDIR; set
    // rpath here because dependency build scripts cannot set final binary
    // linker args for consumers.
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR") {
        if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            println!("cargo:rustc-link-arg-bin=verify-release-embedded=-Wl,-rpath,{libdir}");
        }
    }
}
