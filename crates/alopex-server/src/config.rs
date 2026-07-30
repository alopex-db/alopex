use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use alopex_cluster::{
    ClusterId, ClusterIdentity, ClusterManager, ClusterManagerConfig, ClusterMode, Endpoint,
    MembershipSource, NodeId, NodeRole, NodeState,
};
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
    /// Cluster-aware startup configuration.
    pub cluster: ClusterServerConfig,
    /// Server-established authorization facts for changefeed lifecycle calls.
    /// An empty policy set denies every changefeed request.
    pub changefeed: ChangefeedServerConfig,
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
            cluster: ClusterServerConfig::default(),
            changefeed: ChangefeedServerConfig::default(),
        }
    }
}

/// Changefeed HTTP authorization configuration.
///
/// These facts live in server configuration rather than in a request body so
/// callers cannot grant themselves a tenant, range, or lifecycle scope.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChangefeedServerConfig {
    /// Authorization facts indexed by authenticated actor and tenant.
    pub authorizations: Vec<ChangefeedAuthorizationConfig>,
}

/// One server-established authorization grant for Durable changefeeds.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangefeedAuthorizationConfig {
    /// Authenticated subject as produced by the configured HTTP middleware.
    pub subject: String,
    /// Tenant that this grant covers.
    pub tenant: String,
    /// Range identifiers visible to the subject within the tenant.
    pub allowed_ranges: Vec<String>,
    /// Lifecycle scopes granted to the subject.
    pub allowed_scopes: Vec<ChangefeedScopeConfig>,
}

/// String-stable configuration form of a changefeed authorization scope.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangefeedScopeConfig {
    /// Permits create, subscribe, poll, stream, and resume.
    Read,
    /// Permits acknowledgement, cancellation, and close.
    Ack,
    /// Permits retention-administration operations.
    RetentionAdmin,
}

/// Cluster-aware server configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ClusterServerConfig {
    /// Cluster operating mode.
    pub mode: ClusterMode,
    /// Stable local node identity for cluster-aware mode.
    pub node_id: Option<String>,
    /// Stable cluster identifier shared by configured nodes.
    pub cluster_id: Option<String>,
    /// Endpoint advertised to other cluster members.
    pub advertised_endpoint: Option<String>,
    /// Local node role.
    pub role: NodeRole,
    /// Local lifecycle state used when cluster-aware mode is enabled.
    pub lifecycle_state: NodeState,
    /// Membership metadata source to report and consume.
    pub membership_source: MembershipSource,
    /// Whether the membership source is available at startup.
    pub membership_source_available: bool,
}

impl Default for ClusterServerConfig {
    fn default() -> Self {
        Self {
            mode: ClusterMode::SingleNode,
            node_id: None,
            cluster_id: None,
            advertised_endpoint: None,
            role: NodeRole::Gateway,
            lifecycle_state: NodeState::Active,
            membership_source: MembershipSource::Chirps,
            membership_source_available: true,
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
        let cluster_config = self.cluster_manager_config()?;
        ClusterManager::new(cluster_config)
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?;
        for authorization in &self.changefeed.authorizations {
            if authorization.subject.trim().is_empty()
                || authorization.tenant.trim().is_empty()
                || authorization.allowed_ranges.is_empty()
                || authorization.allowed_scopes.is_empty()
                || authorization
                    .allowed_ranges
                    .iter()
                    .any(|range| range.trim().is_empty())
            {
                return Err(ServerError::InvalidConfig(
                    "changefeed authorization requires subject, tenant, range, and scope".into(),
                ));
            }
        }
        Ok(())
    }

    /// Build cluster manager configuration from server configuration.
    pub fn cluster_manager_config(&self) -> Result<ClusterManagerConfig> {
        self.cluster.to_manager_config()
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

impl ClusterServerConfig {
    fn to_manager_config(&self) -> Result<ClusterManagerConfig> {
        match self.mode {
            ClusterMode::SingleNode => Ok(ClusterManagerConfig::single_node()),
            ClusterMode::ClusterAware => {
                let mut config =
                    ClusterManagerConfig::cluster_aware(self.cluster_aware_identity()?);
                config.membership_source = self.membership_source;
                config.membership_source_available = self.membership_source_available;
                Ok(config)
            }
        }
    }

    fn cluster_aware_identity(&self) -> Result<ClusterIdentity> {
        let node_id = required_cluster_value(self.node_id.as_deref(), "cluster.node_id")?;
        let cluster_id = required_cluster_value(self.cluster_id.as_deref(), "cluster.cluster_id")?;
        let endpoint = required_cluster_value(
            self.advertised_endpoint.as_deref(),
            "cluster.advertised_endpoint",
        )?;

        Ok(ClusterIdentity {
            node_id: NodeId::new(node_id),
            cluster_id: Some(ClusterId::new(cluster_id)),
            advertised_endpoint: Some(Endpoint::new(endpoint)),
            role: self.role,
            lifecycle_state: self.lifecycle_state,
            metadata_schema_version: alopex_cluster::CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch: alopex_cluster::INITIAL_UPDATE_EPOCH,
        })
    }
}

fn required_cluster_value(value: Option<&str>, field: &'static str) -> Result<String> {
    let Some(value) = value else {
        return Err(ServerError::InvalidConfig(format!(
            "{field} is required when cluster.mode is cluster_aware"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerError::InvalidConfig(format!(
            "{field} must be non-empty when cluster.mode is cluster_aware"
        )));
    }
    Ok(trimmed.to_string())
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

    #[test]
    fn default_cluster_config_is_single_node_non_degraded() {
        let cfg = ServerConfig::default();
        let manager = ClusterManager::new(cfg.cluster_manager_config().unwrap()).unwrap();
        let snapshot = manager.status_snapshot();

        assert_eq!(snapshot.mode, ClusterMode::SingleNode);
        assert_eq!(snapshot.identity.node_id.as_str(), "local");
        assert_eq!(snapshot.identity.lifecycle_state, NodeState::Unconfigured);
        assert_eq!(snapshot.membership.source, MembershipSource::LocalDefault);
        assert!(!snapshot.degraded);
        assert!(snapshot.diagnostics.is_empty());
    }

    #[test]
    fn cluster_aware_config_builds_configured_identity() {
        let cfg = ServerConfig {
            cluster: ClusterServerConfig {
                mode: ClusterMode::ClusterAware,
                node_id: Some("node-a".to_string()),
                cluster_id: Some("cluster-a".to_string()),
                advertised_endpoint: Some("127.0.0.1:7001".to_string()),
                role: NodeRole::Worker,
                lifecycle_state: NodeState::Joining,
                membership_source_available: false,
                ..ClusterServerConfig::default()
            },
            ..ServerConfig::default()
        };

        let manager = ClusterManager::new(cfg.cluster_manager_config().unwrap()).unwrap();
        let snapshot = manager.status_snapshot();

        assert_eq!(snapshot.mode, ClusterMode::ClusterAware);
        assert_eq!(snapshot.identity.node_id.as_str(), "node-a");
        assert_eq!(snapshot.identity.cluster_id.unwrap().as_str(), "cluster-a");
        assert_eq!(
            snapshot.identity.advertised_endpoint.unwrap().as_str(),
            "127.0.0.1:7001"
        );
        assert_eq!(snapshot.identity.role, NodeRole::Worker);
        assert_eq!(snapshot.identity.lifecycle_state, NodeState::Joining);
        assert_eq!(snapshot.membership.source, MembershipSource::Chirps);
        assert!(snapshot.degraded);
    }

    #[test]
    fn validate_rejects_invalid_cluster_identity() {
        let cfg = ServerConfig {
            cluster: ClusterServerConfig {
                mode: ClusterMode::ClusterAware,
                node_id: Some("".to_string()),
                cluster_id: Some("cluster-a".to_string()),
                advertised_endpoint: Some("127.0.0.1:7001".to_string()),
                ..ClusterServerConfig::default()
            },
            ..ServerConfig::default()
        };

        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("cluster.node_id"));
    }
}
