use std::env;
use std::path::PathBuf;

/// OS 別の Nim 共有ライブラリのファイル名。
///
/// Nim `--app:lib` は OS 依存で出力する（Nim Compiler User Guide）:
/// - Linux:   `libalopex_sql_parser.so`（`lib` 接頭辞付き）
/// - macOS:   `libalopex_sql_parser.dylib`（`lib` 接頭辞付き）
/// - Windows: `alopex_sql_parser.dll`（接頭辞なし）
fn nim_lib_filename() -> &'static str {
    if cfg!(target_os = "windows") {
        "alopex_sql_parser.dll"
    } else if cfg!(target_os = "macos") {
        "libalopex_sql_parser.dylib"
    } else {
        "libalopex_sql_parser.so"
    }
}

fn main() {
    let lib_filename = nim_lib_filename();

    println!("cargo:rerun-if-env-changed=NIM_SQL_PARSER_LIB_DIR");
    println!("cargo:rerun-if-changed=nim-sql-parser/{lib_filename}");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let target = env::var("TARGET").expect("TARGET");

    // 解決順:
    //   1. NIM_SQL_PARSER_LIB_DIR（開発時 / CI が make nim-parser 直後に明示指定）
    //   2. クレート内 nim-sql-parser/（開発時のデフォルト。make nim-parser の出力先）
    //   3. クレート同梱 vendor/<target-triple>/（crates.io 公開版。リリース時に
    //      CI が 4 プラットフォーム分の Nim 共有ライブラリを事前ビルドして
    //      同梱したもの。cargo install/build する側は Nim ツールチェーンを
    //      一切必要としない）
    //   4. OS 既定（/usr/local/lib）
    let lib_dir = env::var_os("NIM_SQL_PARSER_LIB_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            let local = manifest_dir.join("nim-sql-parser");
            local.join(lib_filename).exists().then_some(local)
        })
        .or_else(|| {
            let vendored = manifest_dir.join("nim-sql-parser/vendor").join(&target);
            vendored.join(lib_filename).exists().then_some(vendored)
        })
        .unwrap_or_else(|| PathBuf::from("/usr/local/lib"));

    let lib_path = lib_dir.join(lib_filename);
    if !lib_path.exists() {
        panic!(
            "{lib_filename} not found at {} (target={target}). This crate does not build the \
             Nim SQL parser from source — vendored binaries ship in nim-sql-parser/vendor/<target>/ \
             for the platforms alopex publishes releases for. If {target} is not among them, run \
             `make nim-parser` (requires Nim >= 2.2) and set NIM_SQL_PARSER_LIB_DIR, or file an \
             issue requesting vendored support for this target.",
            lib_path.display()
        );
    }

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // Linux/macOS は共有ライブラリ（`.so`/`.dylib`）に動的リンクする。
    // Windows は `nim_ffi.rs` の `#[link(kind = "raw-dylib")]` 属性がリンクを担い、
    // Rust が import library をコンパイル時に生成するため、ここでは指定しない。
    if !cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=dylib=alopex_sql_parser");
    }

    // cargo:rustc-link-arg は依存クレート（alopex-sql は lib 専用）の build.rs
    // から出しても、最終的にリンクされるバイナリ（alopex-cli/alopex-server/
    // alopex-py の cdylib）には伝播しない（cargo の仕様。rustc-link-arg-bins
    // も同様に「その build.rs を持つパッケージ自身の bin」にしか効かない）。
    // そのため rpath はここでは設定せず、Cargo.toml の links = "alopex_sql_parser"
    // を通じて cargo::metadata で libdir を公開し、コンシューマ側の build.rs が
    // DEP_ALOPEX_SQL_PARSER_LIBDIR を読んで自分自身のバイナリに rpath を設定する
    // （*-sys crate の標準パターン）。
    println!("cargo::metadata=libdir={}", lib_dir.display());
}
