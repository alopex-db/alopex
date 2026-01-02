use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cli::Cli;
use crate::error::{CliError, Result};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const CONFIG_DIR: &str = ".alopex";
const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProfileConfig {
    pub default: Option<String>,
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub data_dir: String,
}

#[derive(Debug)]
pub struct ResolvedConfig {
    pub data_dir: Option<String>,
    pub in_memory: bool,
    pub profile_name: Option<String>,
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
            default_profile: config.default,
        })
    }

    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let config = ProfileConfig {
            default: self.default_profile.clone(),
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

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn resolve(&self, cli: &Cli) -> Result<ResolvedConfig> {
        if cli.profile.is_some() && cli.data_dir.is_some() {
            return Err(CliError::ConflictingOptions);
        }

        if let Some(profile_name) = cli.profile.as_deref() {
            let profile = self
                .profiles
                .get(profile_name)
                .ok_or_else(|| CliError::ProfileNotFound(profile_name.to_string()))?;
            return Ok(ResolvedConfig {
                data_dir: Some(profile.data_dir.clone()),
                in_memory: false,
                profile_name: Some(profile_name.to_string()),
            });
        }

        if let Some(data_dir) = cli.data_dir.as_ref() {
            return Ok(ResolvedConfig {
                data_dir: Some(data_dir.clone()),
                in_memory: false,
                profile_name: None,
            });
        }

        if let Some(default_name) = self.default_profile.as_deref() {
            let profile = self
                .profiles
                .get(default_name)
                .ok_or_else(|| CliError::ProfileNotFound(default_name.to_string()))?;
            return Ok(ResolvedConfig {
                data_dir: Some(profile.data_dir.clone()),
                in_memory: false,
                profile_name: Some(default_name.to_string()),
            });
        }

        Ok(ResolvedConfig {
            data_dir: None,
            in_memory: true,
            profile_name: None,
        })
    }
}

fn default_config_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| {
        CliError::InvalidArgument("Home directory could not be determined".to_string())
    })?;
    Ok(home.join(CONFIG_DIR).join(CONFIG_FILE))
}
