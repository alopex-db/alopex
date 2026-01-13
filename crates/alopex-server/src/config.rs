use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::audit::AuditLogOutput;
use crate::auth::AuthMode;
use crate::error::{Result, ServerError};
use crate::tls::TlsConfig;

/// Server configuration options.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// HTTP bind address.
    pub http_bind: SocketAddr,
    /// gRPC bind address.
    pub grpc_bind: SocketAddr,
    /// Admin bind address.
    pub admin_bind: SocketAddr,
    /// Allowlist for admin API when non-loopback.
    pub admin_allowlist: Vec<IpAddr>,
    /// Data directory for storage.
    pub data_dir: PathBuf,
    /// API prefix for HTTP routes.
    pub api_prefix: String,
    /// Authentication mode.
    pub auth_mode: AuthMode,
    /// TLS configuration (optional).
    pub tls: Option<TlsConfig>,
    /// Query timeout.
    #[serde(with = "humantime_serde")]
    pub query_timeout: Duration,
    /// Max request size in bytes.
    pub max_request_size: usize,
    /// Max response size in bytes.
    pub max_response_size: usize,
    /// Max concurrent connections.
    pub max_connections: usize,
    /// Session TTL.
    #[serde(with = "humantime_serde")]
    pub session_ttl: Duration,
    /// Enable Prometheus metrics.
    pub metrics_enabled: bool,
    /// Enable tracing.
    pub tracing_enabled: bool,
    /// Enable audit logging.
    pub audit_log_enabled: bool,
    /// Audit log output.
    pub audit_log_output: AuditLogOutput,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_bind: "127.0.0.1:8080".parse().unwrap(),
            grpc_bind: "127.0.0.1:9090".parse().unwrap(),
            admin_bind: "127.0.0.1:8081".parse().unwrap(),
            admin_allowlist: Vec::new(),
            data_dir: PathBuf::from("./data"),
            api_prefix: String::new(),
            auth_mode: AuthMode::None,
            tls: None,
            query_timeout: Duration::from_secs(30),
            max_request_size: 100 * 1024 * 1024,
            max_response_size: 100 * 1024 * 1024,
            max_connections: 1000,
            session_ttl: Duration::from_secs(300),
            metrics_enabled: true,
            tracing_enabled: true,
            audit_log_enabled: true,
            audit_log_output: AuditLogOutput::Stdout,
        }
    }
}

impl ServerConfig {
    /// Load config from TOML and environment variables.
    ///
    /// Environment variables use `ALOPEX__` prefix with `__` separators.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut builder = config::Config::builder();
        if let Some(path) = path {
            builder = builder.add_source(config::File::from(path).required(false));
        } else {
            builder = builder.add_source(config::File::with_name("alopex").required(false));
        }
        builder = builder.add_source(config::Environment::with_prefix("ALOPEX").separator("__"));
        let mut config: ServerConfig = builder
            .build()
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?
            .try_deserialize()
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?;
        config.normalize()?;
        Ok(config)
    }

    /// Validate config invariants.
    pub fn validate(&self) -> Result<()> {
        if !self.admin_bind.ip().is_loopback() && self.admin_allowlist.is_empty() {
            return Err(ServerError::InvalidConfig(
                "admin_allowlist is required for non-loopback admin_bind".into(),
            ));
        }
        if !self.api_prefix.is_empty() && !self.api_prefix.starts_with('/') {
            return Err(ServerError::InvalidConfig(
                "api_prefix must start with '/' or be empty".into(),
            ));
        }
        if self.max_response_size == 0 {
            return Err(ServerError::InvalidConfig(
                "max_response_size must be greater than 0".into(),
            ));
        }
        if self.max_request_size == 0 {
            return Err(ServerError::InvalidConfig(
                "max_request_size must be greater than 0".into(),
            ));
        }
        if self.max_connections == 0 {
            return Err(ServerError::InvalidConfig(
                "max_connections must be greater than 0".into(),
            ));
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.api_prefix == "/" {
            self.api_prefix.clear();
        } else if self.api_prefix.ends_with('/') {
            while self.api_prefix.ends_with('/') {
                self.api_prefix.pop();
            }
        }
        self.validate()
    }
}
