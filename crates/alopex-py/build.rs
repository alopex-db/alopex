use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn python_config_from_exe(python: &str) -> Option<PathBuf> {
    let exe = Path::new(python);
    let file = exe.file_name()?.to_string_lossy();
    let config_name = format!("{file}-config");
    let candidate = exe.with_file_name(config_name);
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

fn main() {
    if cfg!(target_os = "windows") {
        println!("cargo:warning=Windows では python3-config が利用できないため埋め込みフラグをスキップします");
        return;
    }

    let profile = env::var("PROFILE").unwrap_or_default();
    if profile == "release" {
        return;
    }

    let python = env::var("PYO3_PYTHON").unwrap_or_else(|_| "python3".to_string());
    let config = python_config_from_exe(&python).unwrap_or_else(|| PathBuf::from("python3-config"));
    let output = match Command::new(&config)
        .args(["--ldflags", "--embed"])
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            println!(
                "cargo:warning={} の実行に失敗しました: {err}",
                config.to_string_lossy()
            );
            return;
        }
    };
    if !output.status.success() {
        println!(
            "cargo:warning={} --ldflags --embed が失敗しました (status: {:?})",
            config.to_string_lossy(),
            output.status.code()
        );
        return;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for token in stdout.split_whitespace() {
        if let Some(path) = token.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={path}");
        } else if let Some(lib) = token.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={lib}");
        }
    }
}
