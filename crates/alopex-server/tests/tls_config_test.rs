//! Integration coverage for `alopex_server::tls::build_rustls_config`.
//!
//! This exercises the rustls 0.23 migration path directly (issue #41 /
//! RUSTSEC-2026-0104, -0099, -0098): explicit `ring` provider selection,
//! `rustls-pemfile` 2.x certificate/key loading, and the
//! `WebPkiClientVerifier` mTLS builder replacing the removed
//! `AllowAnyAuthenticatedClient`.
//!
//! The protocol-version and mTLS-enforcement tests perform real
//! `tokio-rustls` handshakes end-to-end (not just `ServerConfig`
//! construction), so they actually prove the negotiation/rejection
//! behavior rather than merely that the config type-checks.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use alopex_server::tls::{build_rustls_config, TlsConfig, TlsVersion};
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn write_self_signed(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.serialize_pem().expect("cert pem")).expect("write cert");
    std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");
    (cert_path, key_path)
}

/// Generates a self-signed CA certificate (`is_ca = true`, matching how a
/// real trust anchor is constrained) and one leaf certificate signed by that
/// CA, for use as either a server cert or a client cert in an mTLS
/// handshake.
fn generate_ca() -> Certificate {
    let mut params = CertificateParams::new(Vec::<String>::new());
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    Certificate::from_params(params).expect("ca cert")
}

fn write_leaf_signed_by_ca(
    dir: &Path,
    name: &str,
    subject_alt_names: Vec<String>,
    ca: &Certificate,
) -> (PathBuf, PathBuf) {
    let params = CertificateParams::new(subject_alt_names);
    let leaf = Certificate::from_params(params).expect("leaf cert");
    let cert_pem = leaf.serialize_pem_with_signer(ca).expect("leaf pem signed");
    let key_pem = leaf.serialize_private_key_pem();

    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert_pem).expect("write cert");
    std::fs::write(&key_path, key_pem).expect("write key");
    (cert_path, key_path)
}

fn write_ca_cert(dir: &Path, name: &str, ca: &Certificate) -> PathBuf {
    let ca_path = dir.join(format!("{name}-cert.pem"));
    std::fs::write(&ca_path, ca.serialize_pem().expect("ca pem")).expect("write ca cert");
    ca_path
}

fn load_certs(path: &Path) -> Vec<CertificateDer<'static>> {
    let bytes = std::fs::read(path).expect("read cert file");
    rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse certs")
}

fn load_key(path: &Path) -> PrivateKeyDer<'static> {
    let bytes = std::fs::read(path).expect("read key file");
    rustls_pemfile::pkcs8_private_keys(&mut bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse pkcs8 keys")
        .into_iter()
        .next()
        .map(PrivateKeyDer::Pkcs8)
        .expect("at least one private key")
}

/// Runs one real TLS handshake between a `tokio_rustls` server built from
/// `server_config` and a `tokio_rustls` client built from `client_config`,
/// over a loopback TCP socket. Returns `Ok(())` if both sides completed the
/// handshake and exchanged one line of data, or the underlying `io::Error`
/// (which is what a rejected handshake, e.g. protocol-version mismatch or
/// missing/invalid client certificate, surfaces as) otherwise.
async fn try_handshake(
    server_config: Arc<rustls::ServerConfig>,
    client_config: Arc<rustls::ClientConfig>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let mut tls_stream = acceptor.accept(stream).await?;
        let mut buf = [0u8; 5];
        tls_stream.read_exact(&mut buf).await?;
        tls_stream.write_all(b"world").await?;
        tls_stream.shutdown().await?;
        std::io::Result::Ok(())
    });

    let connector = tokio_rustls::TlsConnector::from(client_config);
    let stream = TcpStream::connect(addr).await?;
    let server_name = ServerName::try_from("localhost").expect("server name");
    let mut tls_stream = connector.connect(server_name, stream).await?;
    tls_stream.write_all(b"hello").await?;
    let mut buf = [0u8; 5];
    tls_stream.read_exact(&mut buf).await?;
    assert_eq!(&buf, b"world");
    tls_stream.shutdown().await?;

    server_task.await.expect("server task join")?;
    Ok(())
}

/// Builds a bare-minimum rustls client config that trusts `roots` and,
/// unless `client_identity` is given, presents no client certificate.
/// `versions` restricts which protocol version(s) the client will offer.
fn build_client_config(
    roots: rustls::RootCertStore,
    versions: &[&'static rustls::SupportedProtocolVersion],
    client_identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .expect("supported protocol versions")
        .with_root_certificates(roots);

    let config = match client_identity {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .expect("client auth cert"),
        None => builder.with_no_client_auth(),
    };
    Arc::new(config)
}

#[test]
fn build_rustls_config_without_client_auth() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_path, key_path) = write_self_signed(dir.path(), "server");

    let config = TlsConfig {
        cert_path,
        key_path,
        ca_path: None,
        min_version: TlsVersion::Tls12,
    };

    let server_config = build_rustls_config(&config).expect("server config");
    assert_eq!(
        server_config.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
}

#[test]
fn build_rustls_config_rejects_missing_cert_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = TlsConfig {
        cert_path: dir.path().join("missing-cert.pem"),
        key_path: dir.path().join("missing-key.pem"),
        ca_path: None,
        min_version: TlsVersion::Tls12,
    };

    let err = build_rustls_config(&config).expect_err("missing cert file should error");
    let message = err.to_string();
    assert!(!message.is_empty());
}

/// Real handshake proof that `min_version: Tls13` actually restricts the
/// negotiated protocol: a client offering only TLS 1.2 must be rejected by a
/// server built with `build_rustls_config`, while a client offering TLS 1.3
/// (or both) must succeed.
#[tokio::test]
async fn tls13_only_server_rejects_tls12_client_and_accepts_tls13_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_path, key_path) = write_self_signed(dir.path(), "server");

    let config = TlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        ca_path: None,
        min_version: TlsVersion::Tls13,
    };

    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(&cert_path) {
        roots.add(cert).expect("trust server cert");
    }

    // TLS 1.2-only client must fail to negotiate against a TLS1.3-only
    // server (this is the assertion the previous version of this test did
    // not make).
    let tls12_client = build_client_config(roots.clone(), &[&rustls::version::TLS12], None);
    let server_config = build_rustls_config(&config).expect("server config");
    let result = try_handshake(server_config, tls12_client).await;
    assert!(
        result.is_err(),
        "TLS1.2-only client should be rejected by a TLS1.3-only server, got: {result:?}"
    );

    // TLS 1.3 client must succeed against the same TLS1.3-only server.
    let tls13_client = build_client_config(roots, &[&rustls::version::TLS13], None);
    let server_config = build_rustls_config(&config).expect("server config");
    try_handshake(server_config, tls13_client)
        .await
        .expect("TLS1.3 client should be accepted by a TLS1.3-only server");
}

/// Real handshake proof that `min_version: Tls12` still negotiates TLS 1.3
/// when both sides support it (the "12" here means "TLS 1.2 or newer", per
/// `build_rustls_config`'s version list `[TLS13, TLS12]`), and also accepts
/// a TLS1.2-only client.
#[tokio::test]
async fn tls12_floor_server_accepts_both_tls12_and_tls13_clients() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_path, key_path) = write_self_signed(dir.path(), "server");

    let config = TlsConfig {
        cert_path: cert_path.clone(),
        key_path,
        ca_path: None,
        min_version: TlsVersion::Tls12,
    };

    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(&cert_path) {
        roots.add(cert).expect("trust server cert");
    }

    let tls12_client = build_client_config(roots.clone(), &[&rustls::version::TLS12], None);
    let server_config = build_rustls_config(&config).expect("server config");
    try_handshake(server_config, tls12_client)
        .await
        .expect("TLS1.2 client should be accepted by a TLS1.2-floor server");

    let tls13_client = build_client_config(roots, &[&rustls::version::TLS13], None);
    let server_config = build_rustls_config(&config).expect("server config");
    try_handshake(server_config, tls13_client)
        .await
        .expect("TLS1.3 client should be accepted by a TLS1.2-floor server");
}

/// Real mTLS handshake proof that `WebPkiClientVerifier` actually enforces
/// client-certificate authentication: a client presenting a certificate
/// signed by the configured CA succeeds, a client presenting no certificate
/// is rejected, and a client presenting a certificate from an unrelated
/// (untrusted) CA is rejected.
#[tokio::test]
async fn mtls_server_requires_valid_client_certificate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ca = generate_ca();
    let ca_path = write_ca_cert(dir.path(), "ca", &ca);
    let (server_cert_path, server_key_path) =
        write_leaf_signed_by_ca(dir.path(), "server", vec!["localhost".to_string()], &ca);
    let (client_cert_path, client_key_path) =
        write_leaf_signed_by_ca(dir.path(), "client", vec!["alopex-client".to_string()], &ca);

    let config = TlsConfig {
        cert_path: server_cert_path,
        key_path: server_key_path,
        ca_path: Some(ca_path),
        min_version: TlsVersion::Tls12,
    };

    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(&write_ca_cert(dir.path(), "ca-for-client-roots", &ca)) {
        roots.add(cert).expect("trust ca cert");
    }

    // A client with a valid certificate signed by the trusted CA succeeds.
    let client_certs = load_certs(&client_cert_path);
    let client_key = load_key(&client_key_path);
    let authed_client = build_client_config(
        roots.clone(),
        &[&rustls::version::TLS13, &rustls::version::TLS12],
        Some((client_certs, client_key)),
    );
    let server_config = build_rustls_config(&config).expect("mtls server config");
    try_handshake(server_config, authed_client)
        .await
        .expect("client with a CA-signed certificate should be accepted");

    // A client presenting no certificate at all must be rejected: the
    // server's WebPkiClientVerifier requires authentication.
    let anonymous_client = build_client_config(
        roots.clone(),
        &[&rustls::version::TLS13, &rustls::version::TLS12],
        None,
    );
    let server_config = build_rustls_config(&config).expect("mtls server config");
    let result = try_handshake(server_config, anonymous_client).await;
    assert!(
        result.is_err(),
        "client without a certificate should be rejected by mTLS server, got: {result:?}"
    );

    // A client presenting a certificate from a *different*, untrusted CA
    // must also be rejected.
    let other_ca = generate_ca();
    let (other_client_cert_path, other_client_key_path) = write_leaf_signed_by_ca(
        dir.path(),
        "other-client",
        vec!["alopex-client".to_string()],
        &other_ca,
    );
    let other_client_certs = load_certs(&other_client_cert_path);
    let other_client_key = load_key(&other_client_key_path);
    let untrusted_client = build_client_config(
        roots,
        &[&rustls::version::TLS13, &rustls::version::TLS12],
        Some((other_client_certs, other_client_key)),
    );
    let server_config = build_rustls_config(&config).expect("mtls server config");
    let result = try_handshake(server_config, untrusted_client).await;
    assert!(
        result.is_err(),
        "client with a certificate from an untrusted CA should be rejected, got: {result:?}"
    );
}
