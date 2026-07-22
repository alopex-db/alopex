fn main() {
    if let Ok(libdir) = std::env::var("DEP_ALOPEX_SQL_PARSER_LIBDIR")
        && (cfg!(target_os = "linux") || cfg!(target_os = "macos"))
    {
        println!("cargo:rustc-link-arg-bin=verify-v08-embedded=-Wl,-rpath,{libdir}");
    }
}
