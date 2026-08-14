fn main() {
    // alopex-sql publishes the resolved Nim parser directory as Cargo links
    // metadata. Dependency link arguments do not propagate to a downstream
    // final binary, so each executable consumer must embed its own rpath.
    // The metadata value is target/package-derived and contains no fixed host
    // path in source control.
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR")
        && (cfg!(target_os = "linux") || cfg!(target_os = "macos"))
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{libdir}");
    }
}
