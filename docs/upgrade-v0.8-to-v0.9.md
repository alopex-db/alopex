# v0.8 to v0.9 candidate upgrade and support boundary

This is a candidate upgrade and support document. It does not authorize a tag,
package publication, GitHub Release, notification, or installation into a
production data directory. The v0.9 target-version gate must produce the
candidate-specific support matrix and readiness report before an operator
selects an artifact.

## Candidate identity and release boundary

The checked-out source may retain its previous version while v0.9 development
is in progress. In particular, an artifact is **not** a v0.9 candidate merely
because it was built from this branch. Before a v0.9 candidate can be ready,
the target-version gate must verify all of the following together:

| Item | Required candidate record | Rejection boundary |
| --- | --- | --- |
| Product crates | A hash, platform, and v0.9.0 identity for `alopex-core`, `alopex-sql`, `alopex-dataframe`, `alopex-embedded`, `alopex-cluster`, `alopex-server`, `alopex-cli`, and `alopex-py` | A missing, mismatched, duplicate, or unclassified crate archive blocks readiness. |
| CLI artifact | `alopex`/`alopex.exe`, its platform, hash, and isolated startup result | A binary name/version/startup mismatch blocks readiness. |
| Python artifact | An `alopex-0.9.0` wheel tag, hash, native extension inspection, and isolated install/import result | A missing native extension, wrong wheel version, or unsupported platform blocks readiness. |
| Internal tooling | Exact `alopex-tools` binary/script/fixture identity as development-only evidence | `alopex-tools` is `publish = false` and is never a product artifact. Its v0.8 verifier cannot substitute for the v0.9 target gate. |
| Lockfile and workflows | Candidate commit SHA, `Cargo.lock`, Python build metadata, and the target-version workflow invocation | A legacy-only v0.7/v0.8 gate, a skipped row, or an unpinned input blocks readiness. |

The existing candidate verifier is a no-publication evidence collector: its
only writable path is its report output. A `Ready` report is not release
authorization, and a `Blocked` report must remain blocked until the recorded
cause is fixed and the target-version matrix is run again.

## Supported upgrade path

Before changing a node or local database:

1. Record the running v0.8 artifact identity, platform, storage location, and
   current local/cluster status.
2. Create and verify a restore-capable backup with the documented operations
   workflow. Retain the backup and its manifest until the candidate's local
   read, metadata, and recovery checks have completed.
3. Record the v0.9 candidate SHA and the target-version readiness report. A
   build log, local Markdown state, or approval identifier is not substitute
   evidence.
4. For cluster-aware operation, check the Chirps feature/capability, durable
   Raft storage, recoverable metadata storage, authenticated transport, and
   node identity before advertising cluster control. Missing prerequisites
   must remain `prerequisite_missing`/`unavailable`; do not create an
   in-memory multi-node fallback.
5. Execute range, recovery, or upgrade operations only with their explicit
   request ID, expected version/epoch where required, target, and confirmation.
   Observe the terminal operation status before accepting subsequent writes.

If validation fails or is interrupted, stop the affected upgrade before new
writes are accepted. Restore only from the recorded verified snapshot. A
resume is valid only when the source identity, checkpoint, compatibility
evidence, and requested operation identity match; otherwise the outcome must
remain classified as retryable, rejected, or terminal failure.

## v0.9 capability boundary

| Surface | Candidate support status | Explicit boundary |
| --- | --- | --- |
| Range placement, routing, recovery, and cluster diagnostics | Conditional | The target matrix must record the exact range lifecycle fixture, request/epoch outcome, external foundation state, and operator surface result. Missing Chirps or durable metadata remains unavailable. |
| CRDT Counter/Set | Conditional | It is supported only when its Phase 2 convergence and cross-surface fixtures are present in the same target-version matrix. |
| Durable changefeed | Conditional | It is supported only when its Phase 3 event/checkpoint/ack/resume/retention evidence is present. A missing durable prerequisite is blocked, not a local approximation. |
| Distributed transactions | Conditional | It is supported only when its Phase 4 atomicity/retry/in-doubt evidence is present for the same candidate SHA. |
| DataFrame | Local and streaming scope | Distributed DataFrame execution is not implied by local/streaming DataFrame evidence. |
| Python | Embedded-local synchronous/asynchronous, catalog, and DataFrame bindings | A remote Python client or remote DataFrame surface is not supported unless separately approved and verified. |

Every target-version row must carry its normal result, rejection/blocked
result, prerequisite, test command, platform, and candidate SHA. An unknown,
missing, skipped, or duplicate row is a release-readiness blocker.

## Platform and Nim parser boundary

| Artifact family | Candidate workflow platforms | Nim parser build rule |
| --- | --- | --- |
| CLI/product binary | Linux x86_64, macOS x86_64/aarch64, Windows x86_64 | Linux uses `scripts/build-nim-parser.sh --backend docker`. macOS and Windows use their native Nim setup because their target libraries cannot be produced by the Linux Docker backend. |
| Python wheel | Linux x86_64, macOS x86_64/aarch64, Windows x86_64 | Each wheel is inspected for the required native extension; the Windows wheel additionally carries and imports the Nim DLL without ambient DLL configuration. Linux aarch64 is not a published wheel target until its native Nim-library path is verified. |
| Python sdist | Source distribution job | It is not evidence for a platform wheel and must pass its own content inspection. |

The host-Nim exceptions above are workflow-platform constraints, not a local
test fallback: Linux candidate validation uses the Docker backend. No platform
row may be inferred from another platform's passing artifact.

## Related documentation

- [Cluster operations](cluster-operations.md)
- [Cluster-aware foundation](cluster-aware-foundation.md)
- [Distributed read coverage](distributed-read.md)
- [DataFrame bounded and streaming contract](dataframe-streaming.md)
- [Python embedded-local API](../crates/alopex-py/README.md)
- [v0.8 candidate support and artifact scope](release-v0.8-support.md)
- [v0.7.4 to v0.8 upgrade and recovery](upgrade-v0.7.4-to-v0.8.md)
