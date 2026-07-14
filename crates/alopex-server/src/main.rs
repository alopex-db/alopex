use std::path::PathBuf;
use std::process::ExitCode;

use alopex_server::{Server, ServerConfig};

enum Action {
    Run(Option<PathBuf>),
    Help,
    Version,
}

fn print_help() {
    println!(
        "\
alopex-server

Usage:
  alopex-server [--config <path>]

Options:
  -c, --config <path>   Path to alopex.toml
  -h, --help            Show this help message
  -V, --version         Show version information
"
    );
}

fn parse_args() -> Result<Action, String> {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Action::Help),
            "-V" | "--version" => return Ok(Action::Version),
            "-c" | "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --config".to_string())?;
                config_path = Some(PathBuf::from(value));
            }
            _ => {
                if let Some(path) = arg.strip_prefix("--config=") {
                    config_path = Some(PathBuf::from(path));
                } else {
                    return Err(format!("unknown argument: {arg}"));
                }
            }
        }
    }

    Ok(Action::Run(config_path))
}

#[tokio::main]
async fn main() -> ExitCode {
    // Install the `ring` CryptoProvider as the process-level default. Our own
    // TLS config construction (see `alopex_server::tls::build_rustls_config`)
    // builds `rustls::ServerConfig` with an explicit provider and does not
    // depend on this, but installing it here keeps behavior consistent with
    // any future rustls-backed client code paths (e.g. tonic Channel TLS) and
    // matches the provider already selected process-wide via `object_store`'s
    // TLS stack.
    let _ = rustls::crypto::ring::default_provider().install_default();

    match parse_args() {
        Ok(Action::Help) => {
            print_help();
            ExitCode::SUCCESS
        }
        Ok(Action::Version) => {
            println!("alopex-server {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Action::Run(config_path)) => {
            let config = match ServerConfig::load(config_path.as_deref()) {
                Ok(config) => config,
                Err(err) => {
                    eprintln!("failed to load config: {err}");
                    return ExitCode::from(1);
                }
            };
            let server = match Server::new(config) {
                Ok(server) => server,
                Err(err) => {
                    eprintln!("failed to initialize server: {err}");
                    return ExitCode::from(1);
                }
            };
            if let Err(err) = server.run().await {
                eprintln!("server terminated with error: {err}");
                return ExitCode::from(1);
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("{err}");
            eprintln!("run with --help for usage");
            ExitCode::from(1)
        }
    }
}
