use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::{Certificate, PrivateKey};
use serde::Deserialize;

use crate::error::{Result, ServerError};

/// TLS configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: Option<PathBuf>,
    #[serde(default)]
    pub min_version: TlsVersion,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for TlsVersion {
    fn default() -> Self {
        Self::Tls12
    }
}

/// Build a rustls server config from TLS settings.
pub fn build_rustls_config(config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    let certs = load_certs(&config.cert_path)?;
    let key = load_key(&config.key_path)?;
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = match config.min_version {
        TlsVersion::Tls12 => vec![&rustls::version::TLS13, &rustls::version::TLS12],
        TlsVersion::Tls13 => vec![&rustls::version::TLS13],
    };
    let builder = rustls::ServerConfig::builder()
        .with_safe_default_cipher_suites()
        .with_safe_default_kx_groups()
        .with_protocol_versions(&versions)
        .map_err(|err| ServerError::InvalidConfig(err.to_string()))?;
    let server_config = if let Some(ca_path) = &config.ca_path {
        let mut roots = rustls::RootCertStore::empty();
        let ca_certs = load_certs(ca_path)?;
        for cert in ca_certs {
            roots
                .add(&cert)
                .map_err(|_| ServerError::InvalidConfig("invalid CA certificate".into()))?;
        }
        let verifier = rustls::server::AllowAnyAuthenticatedClient::new(roots);
        builder
            .with_client_cert_verifier(Arc::new(verifier))
            .with_single_cert(certs, key)
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?
    };
    Ok(Arc::new(server_config))
}

fn load_certs(path: &PathBuf) -> Result<Vec<Certificate>> {
    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|_| ServerError::InvalidConfig("invalid certificate file".into()))?
        .into_iter()
        .map(Certificate)
        .collect();
    Ok(certs)
}

fn load_key(path: &PathBuf) -> Result<PrivateKey> {
    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .map_err(|_| ServerError::InvalidConfig("invalid private key file".into()))?;
    if let Some(key) = keys.first() {
        return Ok(PrivateKey(key.clone()));
    }

    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    let keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .map_err(|_| ServerError::InvalidConfig("invalid private key file".into()))?;
    keys.first()
        .cloned()
        .map(PrivateKey)
        .ok_or_else(|| ServerError::InvalidConfig("private key not found".into()))
}
