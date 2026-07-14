//! Integration coverage for `alopex_server::tls::build_rustls_config`.
//!
//! This exercises the rustls 0.23 migration path directly (issue #41 /
//! RUSTSEC-2026-0104, -0099, -0098): explicit `ring` provider selection,
//! `rustls-pemfile` 2.x certificate/key loading, and the
//! `WebPkiClientVerifier` mTLS builder replacing the removed
//! `AllowAnyAuthenticatedClient`.

use std::path::PathBuf;

use alopex_server::tls::{build_rustls_config, TlsConfig, TlsVersion};

fn write_self_signed(dir: &std::path::Path, name: &str) -> (PathBuf, PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.serialize_pem().expect("cert pem")).expect("write cert");
    std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");
    (cert_path, key_path)
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
fn build_rustls_config_tls13_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_path, key_path) = write_self_signed(dir.path(), "server");

    let config = TlsConfig {
        cert_path,
        key_path,
        ca_path: None,
        min_version: TlsVersion::Tls13,
    };

    build_rustls_config(&config).expect("server config restricted to TLS 1.3");
}

#[test]
fn build_rustls_config_with_client_auth() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_path, key_path) = write_self_signed(dir.path(), "server");
    let (ca_path, _ca_key_path) = write_self_signed(dir.path(), "ca");

    let config = TlsConfig {
        cert_path,
        key_path,
        ca_path: Some(ca_path),
        min_version: TlsVersion::Tls12,
    };

    build_rustls_config(&config).expect("mTLS server config with WebPkiClientVerifier");
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
