fn main() {
    // Both the published-artifact and v0.8-local binaries link the alopex-sql
    // Nim parser. The shared library location is exposed by alopex-sql's
    // build.rs via DEP_ALOPEX_SQL_PARSER_LIBDIR; set rpath here because
    // dependency build scripts cannot set final binary linker args for
    // consumers.
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR")
        && (cfg!(target_os = "linux") || cfg!(target_os = "macos"))
    {
        println!("cargo:rustc-link-arg-bin=verify-release-embedded=-Wl,-rpath,{libdir}");
    }
}
