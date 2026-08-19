# alopex-tools

`alopex-tools` is an internal development and release-verification workspace;
it is not a product artifact. It has two intentionally separate modes:

- `verify-release-embedded` uses the pinned registry crates to verify the
  published Embedded/SQL artifacts after a release.
- `crates/alopex-tools/v08/verify-v08-embedded` uses the checked-out v0.8
  Embedded/SQL crates and runs the v0.8 local SQL compatibility corpus.

The nested v08 tool's `--parser-native-smoke` manifest is immutable v0.8.4 /
contract-0.4.0 evidence for the checked-in historical vendor. Current
contract-0.8.0 publication smoke is owned by the four-target release workflow;
do not relabel the nested fixture or its archived digests.

The embedded verifier does not claim to exercise distributed-read routing,
cluster management, CLI output, DataFrame streaming, or Python bindings. Those
surfaces are verified by their owning integration suites and the candidate
readiness verifier. Keeping this boundary explicit prevents a passing embedded
parity run from being mistaken for complete v0.8 coverage.

Example (from the repository root):

```bash
cargo run --manifest-path crates/alopex-tools/v08/Cargo.toml --release --locked --offline
bash crates/alopex-tools/v08/verify-v08-surfaces.sh
```

`verify-v08-surfaces.sh` delegates to the authoritative Phase 1–4 suites:
cluster metadata and SQL catalog internals, server/cluster CLI operations,
DataFrame sources and streaming, Rust binding lifecycle tests, and the full
Python local/async/DataFrame test tree. It fails on any uncovered compatibility
contract; set `ALOPEX_PYTHON` when the Python environment is not
`/tmp/alopex-v08-python`.
