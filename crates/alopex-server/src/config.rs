use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::audit::AuditLogOutput;
use crate::auth::AuthMode;
use crate::error::{Result, ServerError};
use crate::tls::TlsConfig;

const MAX_ADMISSION_LIMIT: usize = 100_000;
const MAX_QUERY_TIMEOUT_MS: u128 = 300_000;

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
    /// Maximum number of concurrent requests admitted.
    #[serde(alias = "max_connections")]
    pub max_concurrency: usize,
    /// Maximum number of queued requests under backpressure.
    pub max_queue_len: usize,
    /// Max request size in bytes.
    pub max_request_size: usize,
    /// Max response size in bytes.
    pub max_response_size: usize,
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
            max_concurrency: 64,
            max_queue_len: 256,
            max_request_size: 100 * 1024 * 1024,
            max_response_size: 100 * 1024 * 1024,
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
        if self.max_concurrency == 0 {
            return Err(ServerError::InvalidConfig(
                "max_concurrency must be greater than 0".into(),
            ));
        }
        if self.max_concurrency > MAX_ADMISSION_LIMIT {
            return Err(ServerError::InvalidConfig(format!(
                "max_concurrency must be <= {MAX_ADMISSION_LIMIT}"
            )));
        }
        if self.max_queue_len == 0 {
            return Err(ServerError::InvalidConfig(
                "max_queue_len must be greater than 0".into(),
            ));
        }
        if self.max_queue_len > MAX_ADMISSION_LIMIT {
            return Err(ServerError::InvalidConfig(format!(
                "max_queue_len must be <= {MAX_ADMISSION_LIMIT}"
            )));
        }
        let query_timeout_ms = self.query_timeout.as_millis();
        if query_timeout_ms == 0 {
            return Err(ServerError::InvalidConfig(
                "query_timeout must be greater than 0ms".into(),
            ));
        }
        if query_timeout_ms > MAX_QUERY_TIMEOUT_MS {
            return Err(ServerError::InvalidConfig(format!(
                "query_timeout must be <= {MAX_QUERY_TIMEOUT_MS}ms"
            )));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_admission_control_values_match_v06_policy() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.max_concurrency, 64);
        assert_eq!(cfg.max_queue_len, 256);
        assert_eq!(cfg.query_timeout.as_millis(), 30_000);
    }

    #[test]
    fn validate_rejects_zero_or_excessive_admission_values() {
        let cfg = ServerConfig {
            max_concurrency: 0,
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ServerConfig {
            max_queue_len: 0,
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ServerConfig {
            max_concurrency: MAX_ADMISSION_LIMIT + 1,
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ServerConfig {
            max_queue_len: MAX_ADMISSION_LIMIT + 1,
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ServerConfig {
            query_timeout: Duration::from_millis(0),
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ServerConfig {
            query_timeout: Duration::from_millis((MAX_QUERY_TIMEOUT_MS + 1) as u64),
            ..ServerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn load_supports_legacy_max_connections_alias() {
        let raw = r#"
http_bind = "127.0.0.1:8080"
grpc_bind = "127.0.0.1:9090"
admin_bind = "127.0.0.1:8081"
data_dir = "./data"
api_prefix = ""
query_timeout = "30s"
max_connections = 77
max_queue_len = 256
max_request_size = 1048576
max_response_size = 1048576
session_ttl = "300s"
metrics_enabled = true
tracing_enabled = true
audit_log_enabled = false

[auth_mode]
type = "none"

[audit_log_output]
type = "stdout"
"#;

        let built = config::Config::builder()
            .add_source(config::File::from_str(raw, config::FileFormat::Toml))
            .build()
            .expect("build config");
        let cfg: ServerConfig = built.try_deserialize().expect("deserialize");
        assert_eq!(cfg.max_concurrency, 77);
    }
}
