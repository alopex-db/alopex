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

    // 解決順: NIM_SQL_PARSER_LIB_DIR → クレート内 nim-sql-parser/ → OS 既定。
    let lib_dir = env::var_os("NIM_SQL_PARSER_LIB_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            let local = manifest_dir.join("nim-sql-parser");
            local.join(lib_filename).exists().then_some(local)
        })
        .unwrap_or_else(|| PathBuf::from("/usr/local/lib"));

    let lib_path = lib_dir.join(lib_filename);
    if !lib_path.exists() {
        panic!(
            "{lib_filename} not found at {}. Run `make nim-parser` or set NIM_SQL_PARSER_LIB_DIR.",
            lib_path.display()
        );
    }

    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // Windows は `raw-dylib` でリンクする。Rust が idata セクションを生成して
    // `.dll` から直接インポートテーブルを構築するため、import library（`.lib`）が不要。
    // 出典: Rust RFC 2627 / Rust Reference「The `link` attribute」。
    // Linux/macOS は従来どおり共有ライブラリ（`.so`/`.dylib`）に動的リンクする。
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=raw-dylib=alopex_sql_parser");
    } else {
        println!("cargo:rustc-link-lib=dylib=alopex_sql_parser");
    }

    // 実行時のライブラリ解決:
    // - Linux:   rpath を共有ライブラリのディレクトリに設定。
    // - macOS:   rpath を設定（Nim 側の install_name と合わせて解決）。
    // - Windows: `.dll` を実行時 PATH / 同梱で解決する（rpath なし）。
    if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }
}
