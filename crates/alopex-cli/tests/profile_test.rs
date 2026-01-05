#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use clap::Parser;
use tempfile::tempdir;

use alopex_cli::cli::Cli;
use alopex_cli::error::CliError;
use alopex_cli::profile::config::Profile;
use alopex_cli::profile::ProfileManager;

fn parse_cli(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("cli parse")
}

#[test]
fn test_profile_create_save_load() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");

    let mut manager = ProfileManager::load_from_path(config_path.clone()).unwrap();
    manager
        .create(
            "dev",
            Profile {
                data_dir: "/tmp/dev".to_string(),
            },
        )
        .unwrap();
    manager.set_default("dev").unwrap();
    manager.save().unwrap();

    let loaded = ProfileManager::load_from_path(config_path).unwrap();
    let profile = loaded.get("dev").unwrap();
    assert_eq!(profile.data_dir, "/tmp/dev");
    assert_eq!(loaded.default_profile(), Some("dev"));
}

#[cfg(unix)]
#[test]
fn test_profile_permissions_unix() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");

    let mut manager = ProfileManager::load_from_path(config_path.clone()).unwrap();
    manager
        .create(
            "dev",
            Profile {
                data_dir: "/tmp/dev".to_string(),
            },
        )
        .unwrap();
    manager.save().unwrap();

    let metadata = fs::metadata(&config_path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[test]
fn test_profile_list_and_delete() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager
        .create(
            "dev",
            Profile {
                data_dir: "/tmp/dev".to_string(),
            },
        )
        .unwrap();
    manager
        .create(
            "prod",
            Profile {
                data_dir: "/tmp/prod".to_string(),
            },
        )
        .unwrap();

    let list = manager.list();
    assert_eq!(list, vec!["dev", "prod"]);

    manager.delete("dev").unwrap();
    assert!(manager.get("dev").is_none());
}

#[test]
fn test_profile_delete_missing() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    let result = manager.delete("missing");
    assert!(matches!(result, Err(CliError::ProfileNotFound(_))));
}

#[test]
fn test_profile_set_default_missing() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");

    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    let result = manager.set_default("missing");
    assert!(matches!(result, Err(CliError::ProfileNotFound(_))));
}

#[test]
fn test_resolve_conflicting_options() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");
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
    let config_path = temp.path().join(".alopex").join("config.toml");
    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager
        .create(
            "dev",
            Profile {
                data_dir: "/tmp/dev".to_string(),
            },
        )
        .unwrap();
    manager.set_default("dev").unwrap();

    let cli = parse_cli(&["alopex", "--profile", "dev", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/dev"));
    assert!(!resolved.in_memory);
    assert_eq!(resolved.profile_name.as_deref(), Some("dev"));

    let cli = parse_cli(&["alopex", "--data-dir", "/tmp/dir", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert_eq!(resolved.data_dir.as_deref(), Some("/tmp/dir"));
    assert!(!resolved.in_memory);
    assert!(resolved.profile_name.is_none());
}

#[test]
fn test_resolve_profile_missing_and_default_fallback() {
    let temp = tempdir().unwrap();
    let config_path = temp.path().join(".alopex").join("config.toml");
    let mut manager = ProfileManager::load_from_path(config_path).unwrap();
    manager
        .create(
            "dev",
            Profile {
                data_dir: "/tmp/dev".to_string(),
            },
        )
        .unwrap();
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
    let config_path = temp.path().join(".alopex").join("config.toml");
    let manager = ProfileManager::load_from_path(config_path).unwrap();

    let cli = parse_cli(&["alopex", "kv", "list"]);
    let resolved = manager.resolve(&cli).unwrap();
    assert!(resolved.in_memory);
    assert!(resolved.data_dir.is_none());
    assert!(resolved.profile_name.is_none());
}
