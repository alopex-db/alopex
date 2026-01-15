# Security Audit (alopex-cli v0.4.1)

## Scope
- Password handling (no plaintext storage)
- TLS/SSL configuration (HTTPS enforced)
- Certificate validation (mTLS)
- Config file permissions (600 for ~/.alopex/config)
- Dependency scanning (cargo-audit, cargo-deny)

## Summary
- Password handling: Pass
- TLS enforcement: Pass
- mTLS validation: Pass
- Config permissions: Pass
- Dependency scan: Fail (cargo-audit warnings, cargo-deny license policy)

## Checklist and Evidence

### Password handling (no plaintext storage)
- Result: Pass
- Evidence: Passwords are retrieved via `password_command` and used only in-memory for Basic auth.
- References: `crates/alopex-cli/src/client/auth.rs`, `crates/alopex-cli/src/profile/config.rs`

### TLS/SSL configuration (HTTPS enforced)
- Result: Pass
- Evidence: HTTP client rejects non-HTTPS URLs.
- References: `crates/alopex-cli/src/client/http.rs`

### Certificate validation (mTLS)
- Result: Pass
- Evidence: Client identity is loaded from provided cert/key and applied to the TLS client.
- References: `crates/alopex-cli/src/client/auth.rs`

### Config file permissions (600)
- Result: Pass
- Evidence: Config file creation and validation enforces mode 600.
- References: `crates/alopex-cli/src/profile/config.rs`

### Dependency scanning
- Result: Fail
- Commands:
  - `cargo audit -D warnings --db target/advisory-db`
  - `CARGO_HOME=target/cargo-home cargo deny check`
- Notes:
  - cargo-audit reported warnings for unmaintained/unsound crates: bincode (RUSTSEC-2025-0141), number_prefix (RUSTSEC-2025-0119), paste (RUSTSEC-2024-0436), rustls-pemfile (RUSTSEC-2025-0134), lexical-core (RUSTSEC-2023-0086), lru (RUSTSEC-2026-0002).
  - cargo-deny failed with default license policy (no deny.toml), rejecting common licenses (MIT/Apache-2.0/BSD/Unicode-3.0) and surfacing the same unmaintained advisories.

## Follow-ups
- Address reported RUSTSEC advisories (upgrade/replace where possible or document exceptions).
- Add and maintain a project-level deny.toml to permit approved licenses and configure advisory policy.
