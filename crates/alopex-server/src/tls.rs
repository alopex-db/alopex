use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
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

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsVersion {
    #[default]
    Tls12,
    Tls13,
}

/// Build a rustls server config from TLS settings.
///
/// Uses the `ring` [`rustls::crypto::CryptoProvider`] explicitly (matching the
/// provider already selected process-wide via `object_store`'s TLS stack), so
/// this does not depend on `CryptoProvider::install_default()` having been
/// called elsewhere.
pub fn build_rustls_config(config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    let certs = load_certs(&config.cert_path)?;
    let key = load_key(&config.key_path)?;
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = match config.min_version {
        TlsVersion::Tls12 => vec![&rustls::version::TLS13, &rustls::version::TLS12],
        TlsVersion::Tls13 => vec![&rustls::version::TLS13],
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|err| ServerError::InvalidConfig(err.to_string()))?;
    let mut server_config = if let Some(ca_path) = &config.ca_path {
        let mut roots = rustls::RootCertStore::empty();
        let ca_certs = load_certs(ca_path)?;
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|_| ServerError::InvalidConfig("invalid CA certificate".into()))?;
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|err| ServerError::InvalidConfig(err.to_string()))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| ServerError::InvalidConfig(err.to_string()))?
    };
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(server_config))
}

fn load_certs(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| ServerError::InvalidConfig("invalid certificate file".into()))
}

fn load_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    let keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| ServerError::InvalidConfig("invalid private key file".into()))?;
    if let Some(key) = keys.into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let file = File::open(path).map_err(ServerError::Io)?;
    let mut reader = BufReader::new(file);
    let keys: Vec<_> = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| ServerError::InvalidConfig("invalid private key file".into()))?;
    keys.into_iter()
        .next()
        .map(PrivateKeyDer::Pkcs1)
        .ok_or_else(|| ServerError::InvalidConfig("private key not found".into()))
}
