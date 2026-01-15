use std::path::{Path, PathBuf};
use std::process::Command;

use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::{ClientBuilder, RequestBuilder};

use crate::profile::config::{AuthType, ServerConfig};

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("missing authentication configuration: {0}")]
    MissingConfig(String),
    #[error("invalid authentication configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to execute password command: {0}")]
    PasswordCommand(String),
    #[error("failed to load certificate: {0}")]
    Certificate(String),
}

#[derive(Debug, Clone)]
pub enum AuthConfig {
    None,
    Token {
        token: String,
    },
    Basic {
        username: String,
        password_command: String,
    },
    MTls {
        cert_path: PathBuf,
        key_path: PathBuf,
    },
}

impl AuthConfig {
    pub fn from_server_config(config: &ServerConfig) -> Result<Self, AuthError> {
        let auth_type = config.auth.unwrap_or(AuthType::None);
        match auth_type {
            AuthType::None => Ok(Self::None),
            AuthType::Token => {
                let token = config
                    .token
                    .clone()
                    .ok_or_else(|| AuthError::MissingConfig("token is required".to_string()))?;
                Ok(Self::Token { token })
            }
            AuthType::Basic => {
                let username = config
                    .username
                    .clone()
                    .ok_or_else(|| AuthError::MissingConfig("username is required".to_string()))?;
                let password_command = config.password_command.clone().ok_or_else(|| {
                    AuthError::MissingConfig("password_command is required".to_string())
                })?;
                Ok(Self::Basic {
                    username,
                    password_command,
                })
            }
            AuthType::MTls => {
                let cert_path = config
                    .cert_path
                    .clone()
                    .ok_or_else(|| AuthError::MissingConfig("cert_path is required".to_string()))?;
                let key_path = config
                    .key_path
                    .clone()
                    .ok_or_else(|| AuthError::MissingConfig("key_path is required".to_string()))?;
                Ok(Self::MTls {
                    cert_path,
                    key_path,
                })
            }
        }
    }

    pub fn apply_to_builder(&self, builder: ClientBuilder) -> Result<ClientBuilder, AuthError> {
        match self {
            AuthConfig::MTls {
                cert_path,
                key_path,
            } => {
                let identity = load_identity(cert_path, key_path)?;
                Ok(builder.identity(identity))
            }
            _ => Ok(builder),
        }
    }

    pub fn apply_to_request(&self, request: RequestBuilder) -> Result<RequestBuilder, AuthError> {
        match self {
            AuthConfig::None => Ok(request),
            AuthConfig::Token { token } => {
                let value = HeaderValue::from_str(&format!("Bearer {}", token))
                    .map_err(|err| AuthError::InvalidConfig(err.to_string()))?;
                Ok(request.header(AUTHORIZATION, value))
            }
            AuthConfig::Basic {
                username,
                password_command,
            } => {
                let password = execute_password_command(password_command)?;
                Ok(request.basic_auth(username, Some(password)))
            }
            AuthConfig::MTls { .. } => Ok(request),
        }
    }
}

fn execute_password_command(command: &str) -> Result<String, AuthError> {
    let mut parts = command.split_whitespace();
    let Some(program) = parts.next() else {
        return Err(AuthError::InvalidConfig(
            "password_command cannot be empty".to_string(),
        ));
    };
    let output = Command::new(program)
        .args(parts)
        .output()
        .map_err(|err| AuthError::PasswordCommand(err.to_string()))?;
    if !output.status.success() {
        return Err(AuthError::PasswordCommand(format!(
            "command exited with status {}",
            output.status
        )));
    }
    let password = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if password.is_empty() {
        return Err(AuthError::PasswordCommand(
            "password command returned empty output".to_string(),
        ));
    }
    Ok(password)
}

fn load_identity(cert_path: &Path, key_path: &Path) -> Result<reqwest::Identity, AuthError> {
    let cert = std::fs::read(cert_path)
        .map_err(|err| AuthError::Certificate(format!("cert read failed: {err}")))?;
    let key = std::fs::read(key_path)
        .map_err(|err| AuthError::Certificate(format!("key read failed: {err}")))?;
    let mut combined = Vec::with_capacity(cert.len() + key.len() + 1);
    combined.extend_from_slice(&cert);
    if !combined.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(&key);
    reqwest::Identity::from_pem(&combined).map_err(|err| AuthError::Certificate(err.to_string()))
}
