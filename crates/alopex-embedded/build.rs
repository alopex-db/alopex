fn main() {
    // The parser's build script exposes its Nim shared-library directory through
    // DEP_ALOPEX_SQL_PARSER_LIBDIR. Runtime-linker settings do not propagate
    // through dependencies, so each final executable needs its own rpath.
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR") {
        if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{libdir}");
            println!("cargo:rustc-link-arg-tests=-Wl,-rpath,{libdir}");
        }
    }
}
