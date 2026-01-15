use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use clap::Parser;
use tempfile::tempdir;

use alopex_cli::cli::Cli;
use alopex_cli::error::CliError;
use alopex_cli::profile::config::{AuthType, ConnectionType, LocalConfig, Profile, ServerConfig};
use alopex_cli::profile::ProfileManager;

fn parse_cli(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("cli parse")
}

fn config_path(temp: &Path) -> std::path::PathBuf {
    temp.join(".alopex").join("config")
}

fn local_profile(path: &str) -> Profile {
    Profile {
        connection_type: ConnectionType::Local,
        local: Some(LocalConfig {
            path: path.to_string(),
        }),
        server: None,
        data_dir: Some(path.to_string()),
    }
}

fn server_profile(url: &str, auth: AuthType) -> Profile {
    Profile {
        connection_type: ConnectionType::Server,
        local: None,
        server: Some(ServerConfig {
            url: url.to_string(),
            auth: Some(auth),
            token: None,
            username: None,
            password_command: None,
            cert_path: None,
            key_path: None,
        }),
        data_dir: None,
    }
}

#[test]
fn test_profile_create_save_load() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());

    let mut manager = ProfileManager::load_from_path(config_path.clone()).unwrap();
    manager.create("dev", local_profile("/tmp/dev")).unwrap();
    manager.set_default("dev").unwrap();
    manager.save().unwrap();

    let loaded = ProfileManager::load_from_path(config_path).unwrap();
    let profile = loaded.get("dev").unwrap();
    assert_eq!(profile.local_path().as_deref(), Some("/tmp/dev"));
    assert_eq!(loaded.default_profile(), Some("dev"));
}

#[cfg(unix)]
#[test]
fn test_profile_permissions_unix() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());

    let mut manager = ProfileManager::load_from_path(config_path.clone()).unwrap();
    manager.create("dev", local_profile("/tmp/dev")).unwrap();
    manager.save().unwrap();

    let metadata = fs::metadata(&config_path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[test]
fn test_profile_list_and_delete() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager.create("dev", local_profile("/tmp/dev")).unwrap();
    manager.create("prod", local_profile("/tmp/prod")).unwrap();

    let list = manager.list();
    assert_eq!(list, vec!["dev", "prod"]);

    manager.delete("dev").unwrap();
    assert!(manager.get("dev").is_none());
}

#[test]
fn test_profile_delete_missing() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    let result = manager.delete("missing");
    assert!(matches!(result, Err(CliError::ProfileNotFound(_))));
}

#[test]
fn test_profile_set_default_missing() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    let result = manager.set_default("missing");
    assert!(matches!(result, Err(CliError::ProfileNotFound(_))));
}

#[test]
fn test_resolve_conflicting_options() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    let manager = ProfileManager::load_from_path(config_path).unwrap();

    let cli = parse_cli(&[
        "alopex",
        "--profile",
        "dev",
        "--data-dir",
        "/tmp/db",
        "kv",
        "list",
    ]);
    let result = manager.resolve(&cli);
    assert!(matches!(result, Err(CliError::ConflictingOptions)));
}

#[test]
fn test_resolve_profile_and_data_dir() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager.create("dev", local_profile("/tmp/dev")).unwrap();
    manager.set_default("dev").unwrap();

    let cli = parse_cli(&["alopex", "--profile", "dev", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/dev"));
    assert!(!resolved.in_memory);
    assert_eq!(resolved.profile_name.as_deref(), Some("dev"));
    assert_eq!(resolved.connection_type, ConnectionType::Local);

    let cli = parse_cli(&["alopex", "--data-dir", "/tmp/dir", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/dir"));
    assert!(!resolved.in_memory);
    assert!(resolved.profile_name.is_none());
}

#[test]
fn test_resolve_profile_missing_and_default_fallback() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager.create("dev", local_profile("/tmp/dev")).unwrap();
    manager.set_default("dev").unwrap();

    let cli = parse_cli(&["alopex", "--profile", "missing", "kv", "list"]);
    let result = manager.resolve(&cli);
    assert!(matches!(result, Err(CliError::ProfileNotFound(_))));

    let cli = parse_cli(&["alopex", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/dev"));
    assert!(!resolved.in_memory);
    assert_eq!(resolved.profile_name.as_deref(), Some("dev"));
}

#[test]
fn test_resolve_in_memory_fallback() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    let manager = ProfileManager::load_from_path(config_path).unwrap();

    let cli = parse_cli(&["alopex", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert!(resolved.in_memory);
    assert!(resolved.data_dir.is_none());
    assert!(resolved.profile_name.is_none());
    assert_eq!(resolved.connection_type, ConnectionType::Local);
}

#[test]
fn test_profile_load_invalid_toml() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    fs::write(&config_path, "[profiles\ninvalid").unwrap();
    #[cfg(unix)]
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();

    let result = ProfileManager::load_from_path(config_path);
    assert!(matches!(result, Err(CliError::Parse(_))));
}

#[test]
fn test_profile_load_server_auth_configs() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    fs::write(
        &config_path,
        r#"default_profile = "server-token"

[profiles.server-token]
connection_type = "server"

[profiles.server-token.server]
url = "https://example.com"
auth = "token"
token = "abc"

[profiles.server-basic]
connection_type = "server"

[profiles.server-basic.server]
url = "https://example.com"
auth = "basic"
username = "admin"
password_command = "pass show alopex/staging"

[profiles.server-mtls]
connection_type = "server"

[profiles.server-mtls.server]
url = "https://example.com"
auth = "mtls"
cert_path = "/tmp/cert.pem"
key_path = "/tmp/key.pem"
"#,
    )
    .unwrap();
    #[cfg(unix)]
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();

    let manager = ProfileManager::load_from_path(config_path).unwrap();
    let token_profile = manager.get("server-token").unwrap();
    assert_eq!(token_profile.connection_type, ConnectionType::Server);
    assert_eq!(
        token_profile.server.as_ref().unwrap().auth,
        Some(AuthType::Token)
    );
    assert_eq!(
        token_profile.server.as_ref().unwrap().token.as_deref(),
        Some("abc")
    );

    let basic_profile = manager.get("server-basic").unwrap();
    assert_eq!(
        basic_profile.server.as_ref().unwrap().auth,
        Some(AuthType::Basic)
    );
    assert_eq!(
        basic_profile.server.as_ref().unwrap().username.as_deref(),
        Some("admin")
    );
    assert_eq!(
        basic_profile
            .server
            .as_ref()
            .unwrap()
            .password_command
            .as_deref(),
        Some("pass show alopex/staging")
    );

    let mtls_profile = manager.get("server-mtls").unwrap();
    assert_eq!(
        mtls_profile.server.as_ref().unwrap().auth,
        Some(AuthType::MTls)
    );
    assert_eq!(
        mtls_profile
            .server
            .as_ref()
            .unwrap()
            .cert_path
            .as_ref()
            .unwrap(),
        &std::path::PathBuf::from("/tmp/cert.pem")
    );
    assert_eq!(
        mtls_profile
            .server
            .as_ref()
            .unwrap()
            .key_path
            .as_ref()
            .unwrap(),
        &std::path::PathBuf::from("/tmp/key.pem")
    );
}

#[test]
fn test_resolve_server_profile_with_fallback() {
    let temp = tempdir().unwrap();
    let config_path = config_path(temp.path());
    let mut manager = ProfileManager::load_from_path(config_path).unwrap();

    let mut profile = server_profile("https://example.com", AuthType::Token);
    profile.local = Some(LocalConfig {
        path: "/tmp/fallback".to_string(),
    });
    manager.create("server", profile).unwrap();

    let cli = parse_cli(&["alopex", "--profile", "server", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.connection_type, ConnectionType::Server);
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/fallback"));
    assert_eq!(resolved.fallback_local.as_deref(), Some("/tmp/fallback"));
    assert!(resolved.server.is_some());
}
