use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;

use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::{ClientBuilder, RequestBuilder};
use time::OffsetDateTime;
use url::Url;
use x509_parser::extensions::GeneralName;
use x509_parser::pem::parse_x509_pem;
use x509_parser::prelude::X509Certificate;

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
        server_host: String,
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
                let server_host = server_host_from_url(&config.url)?;
                Ok(Self::MTls {
                    cert_path,
                    key_path,
                    server_host,
                })
            }
        }
    }

    pub fn apply_to_builder(&self, builder: ClientBuilder) -> Result<ClientBuilder, AuthError> {
        match self {
            AuthConfig::MTls {
                cert_path,
                key_path,
                server_host,
            } => {
                let cert = std::fs::read(cert_path)
                    .map_err(|err| AuthError::Certificate(format!("cert read failed: {err}")))?;
                validate_mtls_certificate(&cert, server_host)?;
                let key = std::fs::read(key_path)
                    .map_err(|err| AuthError::Certificate(format!("key read failed: {err}")))?;
                let identity = load_identity_from_parts(&cert, &key)?;
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

fn load_identity_from_parts(cert: &[u8], key: &[u8]) -> Result<reqwest::Identity, AuthError> {
    let mut combined = Vec::with_capacity(cert.len() + key.len() + 1);
    combined.extend_from_slice(cert);
    if !combined.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(key);
    reqwest::Identity::from_pem(&combined).map_err(|err| AuthError::Certificate(err.to_string()))
}

fn server_host_from_url(url: &str) -> Result<String, AuthError> {
    let parsed = Url::parse(url)
        .map_err(|err| AuthError::InvalidConfig(format!("invalid server url: {err}")))?;
    parsed
        .host_str()
        .map(|host| host.to_string())
        .ok_or_else(|| AuthError::InvalidConfig("server url missing host".to_string()))
}

fn validate_mtls_certificate(cert_pem: &[u8], server_host: &str) -> Result<(), AuthError> {
    let (_, pem) = parse_x509_pem(cert_pem)
        .map_err(|err| AuthError::Certificate(format!("cert parse failed: {err}")))?;
    let cert = pem
        .parse_x509()
        .map_err(|err| AuthError::Certificate(format!("cert parse failed: {err}")))?;
    validate_certificate_dates(&cert)?;
    validate_certificate_host(&cert, server_host)?;
    Ok(())
}

fn validate_certificate_dates(cert: &X509Certificate<'_>) -> Result<(), AuthError> {
    let now = OffsetDateTime::now_utc();
    let not_before = cert.validity().not_before.to_datetime();
    let not_after = cert.validity().not_after.to_datetime();
    if now < not_before {
        return Err(AuthError::Certificate(
            "certificate is not valid yet".to_string(),
        ));
    }
    if now > not_after {
        return Err(AuthError::Certificate(
            "certificate has expired".to_string(),
        ));
    }
    Ok(())
}

fn validate_certificate_host(
    cert: &X509Certificate<'_>,
    server_host: &str,
) -> Result<(), AuthError> {
    let host = normalize_host(server_host);
    let host_ip = host.parse::<IpAddr>().ok();
    let mut matched = false;

    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in san.value.general_names.iter() {
            match name {
                GeneralName::DNSName(dns) => {
                    if host_matches_dns(dns, &host) {
                        matched = true;
                        break;
                    }
                }
                GeneralName::IPAddress(bytes) => {
                    if let Some(ip) = host_ip {
                        if ip_matches_bytes(ip, bytes) {
                            matched = true;
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if !matched {
        if let Some(cn) = cert
            .subject()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
        {
            matched = host_matches_dns(cn, &host);
        }
    }

    if matched {
        Ok(())
    } else {
        Err(AuthError::Certificate(
            "certificate does not match server host".to_string(),
        ))
    }
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_lowercase()
}

fn host_matches_dns(pattern: &str, host: &str) -> bool {
    let pattern = normalize_host(pattern);
    if let Some(suffix) = pattern.strip_prefix("*.") {
        if host == suffix {
            return false;
        }
        return host.ends_with(&format!(".{suffix}"));
    }
    pattern == host
}

fn ip_matches_bytes(ip: IpAddr, bytes: &[u8]) -> bool {
    match ip {
        IpAddr::V4(addr) => bytes == addr.octets(),
        IpAddr::V6(addr) => bytes == addr.octets(),
    }
}
