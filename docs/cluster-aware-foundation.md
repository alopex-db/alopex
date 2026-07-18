# Cluster-aware Foundation

This document records the stable v0.7.0 cluster-aware behavior, migration
contracts, and follow-on hooks. It intentionally describes a topic, not a
version-only release page.

## Release Boundary

v0.7.0 is a single-node compatible cluster-aware release. Existing v0.6
Embedded, Server, SQL, DataFrame, and Python behavior remains the default.
Cluster-aware behavior is enabled only when the Server cluster configuration is
explicitly set to cluster-aware mode.

## v0.7.6 Additive Surface

v0.7.6 completes the cluster administration surface across the existing Server
protocols. In addition to the HTTP admin endpoints, `alopex.v0.AlopexService`
provides `ClusterStatus`, `ClusterJoin`, and `ClusterLeave` RPCs.

Each RPC returns the same canonical `ClusterStatusSnapshot` JSON schema used by
the HTTP `cluster` field. This is an additive protocol surface; the default
single-node behavior and the v0.7 status/routing contracts remain unchanged.

Stable in v0.7.0:

- `alopex-cluster` owns cluster identity, node role, node lifecycle, membership
  view, placement metadata, routing capabilities, diagnostics, and the cluster
  status snapshot schema.
- Server starts in `single_node` mode by default. The default identity is local,
  the membership source is local default, no remote members are reported, and
  the status is not degraded.
- Cluster-aware Server mode validates node identity, cluster id, advertised
  endpoint, role, lifecycle state, and membership source availability at
  startup.
- Server admin status, health, metrics, join, and leave surfaces report cluster
  status without bypassing the existing admin/auth boundary.
- CLI and Python status projections are checked against the same Server status
  fixture.
- Query routing for live databases returns local execution when placement is
  absent or resolves to one local target. Queries that require production
  distributed execution return `future_distributed_execution_required` with
  diagnostics.
- Simulated scatter-gather planning is covered by deterministic release-gate
  fixtures. This is a planning and diagnostics contract, not production remote
  execution.
- DataFrame P3 string, datetime, and list namespace primitives are available in
  `alopex-dataframe` and exposed through Python `DataFrame` wrappers.

## Compatibility Contract

The v0.7.0 compatibility contract is:

- Default Embedded and Server operation remains single-node compatible with
  v0.6.
- Embedded direct SQL remains local and does not require QueryRouter or
  cluster-aware routing dependencies.
- v0.7 metadata initialization and upgrade steps are idempotent and safe to
  retry after interruption.
- Server startup on an existing data directory preserves existing SQL data and
  initializes default `single_node` cluster status without adding remote
  placement state.
- Python error codes, default `Database` KV behavior, and default DataFrame
  constructor/from_columns behavior remain stable.
- DataFrame P3 namespace additions are additive and do not alter existing
  default columns unless the caller writes into the same output column.

Release-gate coverage:

- `scripts/release/v07_gate.sh` runs the v0.6 baseline gate.
- Embedded compatibility is covered by
  `cargo test -p alopex-embedded --test v07_compatibility --all-features --locked`.
- Server compatibility is covered by
  `cargo test -p alopex-server --test v07_compatibility --all-features --locked`.
- Python compatibility is covered by
  `crates/alopex-py/tests/test_compatibility_contract.py`.
- DataFrame P3 is covered by Rust and Python namespace tests.
- Server, CLI, and Python status schemas are covered by cross-surface fixture
  checks.

## Stable Status Fields

Cluster status consumers can depend on these v0.7.0 concepts:

- `schema_version`
- `mode`: `single_node` or `cluster_aware`
- `identity`: node id, optional cluster id, optional advertised endpoint, role,
  lifecycle state, metadata schema version, and update epoch
- `membership`: source and known members
- `placement`: placement metadata and update epoch
- `routing_capabilities`: including local-only support
- `metrics_summary`: source and per-member metric summaries
- `degraded`
- `diagnostics`

Additive or experimental fields must remain additive and must not weaken these
stable fields.

## Routing Contract

The live routing contract is intentionally conservative:

- `local_only`: execution proceeds through the existing local engine.
- `future_distributed_execution_required`: the query references targets that
  require future remote execution; v0.7.0 returns a stable diagnostic instead of
  partial results.

Simulated scatter-gather routing exists to validate planning, retry/backoff,
idempotency, cancellation, and diagnostics contracts for future releases.

## Follow-on Hooks

v0.8 hooks:

- Connect Metadata Raft to the v0.7 `alopex-cluster` metadata contract.
- Connect Raft DDL distribution to the stable catalog/placement lifecycle
  boundary.
- Expand remote execution using the v0.7 routing diagnostics and status schema.

v0.9 hooks:

- Connect Multi-Raft range placement to the v0.7 logical shard/range and routing
  target contracts.
- Add distributed transaction coordination over the stable routing target model.
- Connect Changefeed integration to the stable cluster status and placement
  model.

## Explicitly Not Included

v0.7.0 does not include:

- production remote scatter-gather execution
- Raft-backed metadata consensus
- Raft DDL distribution
- distributed transactions
- Multi-Raft placement
- Changefeed execution
- alopex-py Client / Transaction / ConnectionPool APIs

These items must not be described as completed v0.7.0 behavior in release notes
or operational docs.

## Release Checklist

Before tagging `v0.7.0`:

- Run `bash scripts/release/v07_gate.sh`.
- Confirm release notes include stable behavior, migration contracts, and
  v0.8/v0.9 follow-on hooks.
- Merge the release branch into `main`.
- Create the annotated `v0.7.0` tag only from the merged `main` commit.

After tagging:

- Verify the release workflow completed successfully.
- Verify GitHub Release assets exist for Linux, macOS x86_64, macOS aarch64,
  and Windows x86_64.
- Record whether the release branch was deleted or retained. If retained,
  record the reason, owner, and cleanup condition.
