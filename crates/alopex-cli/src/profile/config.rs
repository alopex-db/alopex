use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::cli::{Cli, SqlReadMode};
use crate::error::{CliError, Result};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const CONFIG_DIR: &str = ".alopex";
const CONFIG_FILE: &str = "config";

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProfileConfig {
    #[serde(alias = "default")]
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionType {
    #[default]
    Local,
    Server,
}

/// Declares whether a profile is allowed to request a distributed read. A
/// server connection alone remains a legacy/local profile until this is set to
/// `cluster`, preventing accidental remote routing after an upgrade.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionScope {
    #[default]
    Local,
    Cluster,
}

/// Cluster-only read-mode policy configured by the profile owner. The server
/// still checks committed cluster policy; this object only determines which
/// client-side overrides are eligible to be sent to that server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReadConfig {
    #[serde(default)]
    pub permitted_read_modes: Vec<SqlReadMode>,
    #[serde(default = "default_cluster_read_mode")]
    pub default_read_mode: SqlReadMode,
}

fn default_cluster_read_mode() -> SqlReadMode {
    SqlReadMode::Inherit
}

impl Default for ClusterReadConfig {
    fn default() -> Self {
        Self {
            permitted_read_modes: Vec::new(),
            default_read_mode: default_cluster_read_mode(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    #[default]
    None,
    Token,
    Basic,
    MTls,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub url: String,
    #[serde(default)]
    pub insecure: bool,
    #[serde(default)]
    pub auth: Option<AuthType>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password_command: Option<String>,
    #[serde(default)]
    pub cert_path: Option<PathBuf>,
    #[serde(default)]
    pub key_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub connection_type: ConnectionType,
    #[serde(default)]
    pub local: Option<LocalConfig>,
    #[serde(default)]
    pub server: Option<ServerConfig>,
    #[serde(default)]
    pub data_dir: Option<String>,
    #[serde(default)]
    pub execution_scope: ExecutionScope,
    #[serde(default)]
    pub cluster_read: Option<ClusterReadConfig>,
}

impl Profile {
    fn normalized(&self) -> Self {
        let mut profile = self.clone();
        if profile.local.is_none() {
            if let Some(data_dir) = profile.data_dir.clone() {
                profile.local = Some(LocalConfig { path: data_dir });
            }
        }
        if profile.connection_type == ConnectionType::Local
            && profile.local.is_none()
            && profile.server.is_some()
        {
            profile.connection_type = ConnectionType::Server;
        }
        profile
    }

    pub fn local_path(&self) -> Option<String> {
        self.local
            .as_ref()
            .map(|local| local.path.clone())
            .or_else(|| self.data_dir.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub data_dir: Option<String>,
    pub in_memory: bool,
    #[allow(dead_code)]
    pub profile_name: Option<String>,
    pub connection_type: ConnectionType,
    #[allow(dead_code)]
    pub server: Option<ServerConfig>,
    #[allow(dead_code)]
    pub fallback_local: Option<String>,
    /// Retained in the resolved configuration so SQL execution can reject an
    /// invalid mode before opening either local storage or a server request.
    pub execution_scope: ExecutionScope,
    pub cluster_read: Option<ClusterReadConfig>,
}

/// The deterministic profile-side result of resolving `SqlCommand` read mode.
/// A cluster candidate must still be accepted by the server's committed read
/// policy; callers must not reinterpret a rejection as permission to run
/// locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedSqlReadMode {
    Local,
    Cluster(SqlReadMode),
}

impl ResolvedConfig {
    pub fn resolve_sql_read_mode(
        &self,
        requested: Option<SqlReadMode>,
    ) -> Result<ResolvedSqlReadMode> {
        match self.execution_scope {
            ExecutionScope::Local => match requested.unwrap_or(SqlReadMode::Local) {
                SqlReadMode::Local => Ok(ResolvedSqlReadMode::Local),
                mode => Err(CliError::InvalidArgument(format!(
                    "read mode '{}' requires an explicit cluster profile",
                    read_mode_name(mode)
                ))),
            },
            ExecutionScope::Cluster => {
                let cluster_read = self.cluster_read.as_ref().ok_or_else(|| {
                    CliError::InvalidArgument(
                        "cluster profile requires a [cluster_read] configuration".into(),
                    )
                })?;
                let requested = requested.unwrap_or(SqlReadMode::Inherit);
                if requested == SqlReadMode::Local {
                    return Err(CliError::InvalidArgument(
                        "local_not_permitted_for_cluster_profile".into(),
                    ));
                }
                let candidate = if requested == SqlReadMode::Inherit {
                    cluster_read.default_read_mode
                } else {
                    if !cluster_read.permitted_read_modes.contains(&requested) {
                        return Err(CliError::InvalidArgument(format!(
                            "read_mode_not_permitted: '{}' is not permitted by the cluster profile",
                            read_mode_name(requested)
                        )));
                    }
                    requested
                };
                if candidate == SqlReadMode::Local {
                    return Err(CliError::InvalidArgument(
                        "cluster profile default_read_mode cannot be local".into(),
                    ));
                }
                Ok(ResolvedSqlReadMode::Cluster(candidate))
            }
        }
    }
}

fn read_mode_name(mode: SqlReadMode) -> &'static str {
    match mode {
        SqlReadMode::Local => "local",
        SqlReadMode::Inherit => "inherit",
        SqlReadMode::Strong => "strong",
        SqlReadMode::Stale => "stale",
    }
}

#[derive(Debug)]
pub struct ProfileManager {
    config_path: PathBuf,
    profiles: HashMap<String, Profile>,
    default_profile: Option<String>,
}

impl ProfileManager {
    pub fn load() -> Result<Self> {
        let config_path = default_config_path()?;
        Self::load_from_path(config_path)
    }

    pub fn load_from_path(config_path: PathBuf) -> Result<Self> {
        if config_path.exists() {
            validate_config_permissions(&config_path)?;
        }

        let config = if config_path.exists() {
            let contents = fs::read_to_string(&config_path)?;
            if contents.trim().is_empty() {
                ProfileConfig::default()
            } else {
                toml::from_str::<ProfileConfig>(&contents)
                    .map_err(|err| CliError::Parse(err.to_string()))?
            }
        } else {
            ProfileConfig::default()
        };

        Ok(Self {
            config_path,
            profiles: config.profiles,
            default_profile: config.default_profile,
        })
    }

    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let config = ProfileConfig {
            default_profile: self.default_profile.clone(),
            profiles: self.profiles.clone(),
        };
        let serialized =
            toml::to_string_pretty(&config).map_err(|err| CliError::Parse(err.to_string()))?;

        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options.open(&self.config_path)?;
        file.write_all(serialized.as_bytes())?;
        file.flush()?;

        #[cfg(unix)]
        fs::set_permissions(&self.config_path, fs::Permissions::from_mode(0o600))?;

        Ok(())
    }

    pub fn create(&mut self, name: &str, profile: Profile) -> Result<()> {
        self.profiles.insert(name.to_string(), profile);
        Ok(())
    }

    pub fn delete(&mut self, name: &str) -> Result<()> {
        if self.profiles.remove(name).is_none() {
            return Err(CliError::ProfileNotFound(name.to_string()));
        }

        if self.default_profile.as_deref() == Some(name) {
            self.default_profile = None;
        }

        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    pub fn list(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.profiles.keys().map(|name| name.as_str()).collect();
        names.sort_unstable();
        names
    }

    pub fn set_default(&mut self, name: &str) -> Result<()> {
        if !self.profiles.contains_key(name) {
            return Err(CliError::ProfileNotFound(name.to_string()));
        }

        self.default_profile = Some(name.to_string());
        Ok(())
    }

    pub fn default_profile(&self) -> Option<&str> {
        self.default_profile.as_deref()
    }

    pub fn resolve(&self, cli: &Cli) -> Result<ResolvedConfig> {
        if cli.profile.is_some() && cli.data_dir.is_some() {
            return Err(CliError::ConflictingOptions);
        }

        if let Some(profile_name) = cli.profile.as_deref() {
            let profile = self
                .profiles
                .get(profile_name)
                .ok_or_else(|| CliError::ProfileNotFound(profile_name.to_string()))?
                .normalized();
            let mut resolved = resolve_profile(profile, Some(profile_name.to_string()))?;
            apply_cli_overrides(cli, &mut resolved);
            return Ok(resolved);
        }

        if let Some(data_dir) = cli.data_dir.as_ref() {
            return Ok(ResolvedConfig {
                data_dir: Some(data_dir.clone()),
                in_memory: false,
                profile_name: None,
                connection_type: ConnectionType::Local,
                server: None,
                fallback_local: None,
                execution_scope: ExecutionScope::Local,
                cluster_read: None,
            });
        }

        if let Some(default_name) = self.default_profile.as_deref() {
            let profile = self
                .profiles
                .get(default_name)
                .ok_or_else(|| CliError::ProfileNotFound(default_name.to_string()))?
                .normalized();
            let mut resolved = resolve_profile(profile, Some(default_name.to_string()))?;
            apply_cli_overrides(cli, &mut resolved);
            return Ok(resolved);
        }

        Ok(ResolvedConfig {
            data_dir: None,
            in_memory: true,
            profile_name: None,
            connection_type: ConnectionType::Local,
            server: None,
            fallback_local: None,
            execution_scope: ExecutionScope::Local,
            cluster_read: None,
        })
    }
}

fn apply_cli_overrides(cli: &Cli, resolved: &mut ResolvedConfig) {
    if cli.insecure {
        if let Some(server) = resolved.server.as_mut() {
            server.insecure = true;
        }
    }
}

fn resolve_profile(profile: Profile, profile_name: Option<String>) -> Result<ResolvedConfig> {
    if profile.execution_scope == ExecutionScope::Cluster {
        if profile.connection_type != ConnectionType::Server {
            return Err(CliError::InvalidArgument(
                "cluster profile requires connection_type = 'server'".into(),
            ));
        }
        if profile.cluster_read.is_none() {
            return Err(CliError::InvalidArgument(
                "cluster profile requires a [cluster_read] configuration".into(),
            ));
        }
    }
    match profile.connection_type {
        ConnectionType::Local => {
            let local_path = profile.local_path().ok_or_else(|| {
                CliError::InvalidArgument("Local profile requires a data directory".to_string())
            })?;
            Ok(ResolvedConfig {
                data_dir: Some(local_path),
                in_memory: false,
                profile_name,
                connection_type: ConnectionType::Local,
                server: None,
                fallback_local: None,
                execution_scope: ExecutionScope::Local,
                cluster_read: None,
            })
        }
        ConnectionType::Server => {
            let execution_scope = profile.execution_scope;
            let cluster_read = profile.cluster_read.clone();
            let fallback_local = profile.local_path();
            let server = profile.server.ok_or_else(|| {
                CliError::InvalidArgument(
                    "Server profile requires a server configuration".to_string(),
                )
            })?;
            let fallback_local = (execution_scope == ExecutionScope::Local)
                .then_some(fallback_local)
                .flatten();
            Ok(ResolvedConfig {
                data_dir: fallback_local.clone(),
                in_memory: false,
                profile_name,
                connection_type: ConnectionType::Server,
                server: Some(server),
                fallback_local,
                execution_scope,
                cluster_read,
            })
        }
    }
}

fn default_config_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| {
        CliError::InvalidArgument("Home directory could not be determined".to_string())
    })?;
    Ok(home.join(CONFIG_DIR).join(CONFIG_FILE))
}

#[cfg(unix)]
fn validate_config_permissions(path: &PathBuf) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let mode = metadata.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(CliError::InvalidArgument(format!(
            "Config file permissions must be 600: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_config_permissions(_path: &PathBuf) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_profile(
        execution_scope: ExecutionScope,
        cluster_read: Option<ClusterReadConfig>,
    ) -> Profile {
        Profile {
            connection_type: ConnectionType::Server,
            local: Some(LocalConfig {
                path: "/tmp/local-fallback".into(),
            }),
            server: Some(ServerConfig {
                url: "https://cluster.example.test".into(),
                insecure: false,
                auth: None,
                token: None,
                username: None,
                password_command: None,
                cert_path: None,
                key_path: None,
            }),
            data_dir: None,
            execution_scope,
            cluster_read,
        }
    }

    #[test]
    fn legacy_server_profile_remains_local_and_allows_legacy_fallback() {
        let resolved = resolve_profile(
            server_profile(ExecutionScope::Local, None),
            Some("legacy".into()),
        )
        .unwrap();
        assert_eq!(resolved.execution_scope, ExecutionScope::Local);
        assert_eq!(
            resolved.fallback_local.as_deref(),
            Some("/tmp/local-fallback")
        );
        assert_eq!(
            resolved.resolve_sql_read_mode(None).unwrap(),
            ResolvedSqlReadMode::Local
        );
        assert!(matches!(
            resolved.resolve_sql_read_mode(Some(SqlReadMode::Strong)),
            Err(CliError::InvalidArgument(message)) if message.contains("explicit cluster profile")
        ));
    }

    #[test]
    fn explicit_cluster_profile_resolves_permitted_overrides_without_local_fallback() {
        let resolved = resolve_profile(
            server_profile(
                ExecutionScope::Cluster,
                Some(ClusterReadConfig {
                    permitted_read_modes: vec![SqlReadMode::Strong, SqlReadMode::Stale],
                    default_read_mode: SqlReadMode::Strong,
                }),
            ),
            Some("cluster".into()),
        )
        .unwrap();
        assert_eq!(resolved.fallback_local, None);
        assert_eq!(
            resolved.resolve_sql_read_mode(None).unwrap(),
            ResolvedSqlReadMode::Cluster(SqlReadMode::Strong)
        );
        assert_eq!(
            resolved
                .resolve_sql_read_mode(Some(SqlReadMode::Stale))
                .unwrap(),
            ResolvedSqlReadMode::Cluster(SqlReadMode::Stale)
        );
        assert!(matches!(
            resolved.resolve_sql_read_mode(Some(SqlReadMode::Local)),
            Err(CliError::InvalidArgument(message)) if message == "local_not_permitted_for_cluster_profile"
        ));
        assert!(matches!(
            resolved.resolve_sql_read_mode(Some(SqlReadMode::Strong)),
            Ok(ResolvedSqlReadMode::Cluster(SqlReadMode::Strong))
        ));
    }

    #[test]
    fn cluster_profile_requires_cluster_read_configuration() {
        assert!(matches!(
            resolve_profile(server_profile(ExecutionScope::Cluster, None), None),
            Err(CliError::InvalidArgument(message)) if message.contains("cluster_read")
        ));
    }
}
