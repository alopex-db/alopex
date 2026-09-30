# v0.9 candidate support and release boundary

This document defines the v0.9 candidate support boundary. It does not
authorize a tag, package publication, GitHub Release, notification, or
installation into a production data directory.

## Candidate support

| Surface | Status | Required evidence |
| --- | --- | --- |
| Multi-Raft range placement and routing | Conditional | Phase 1 range lifecycle, recovery, routing, and diagnostic evidence for the exact candidate SHA |
| CRDT Counter/Set | Conditional | Phase 2 convergence and cross-surface fixtures for the exact candidate SHA |
| Durable changefeed | Conditional | Phase 3 event, checkpoint, acknowledgement, resume, and retention evidence |
| Distributed transactions | Conditional | Phase 4 atomicity, retry, in-doubt, and transport-surface evidence |
| DataFrame | Local/streaming scope | Distributed DataFrame execution is not implied by local evidence |
| Python | Embedded-local sync/async, catalog, and DataFrame bindings | Remote client and remote DataFrame surfaces remain outside v0.9 unless separately approved |

Every row in the target-version matrix must include its normal result,
rejection or blocked result, prerequisite, test command, platform, and exact
candidate SHA. Missing, skipped, duplicate, or unknown rows block readiness.

## Chirps boundary

The v0.9 cluster integration requires Chirps v0.7 or newer, durable Raft
storage, authenticated transport, node identity, and recoverable metadata.
Missing prerequisites remain unavailable or blocked; no in-memory multi-node
fallback is accepted as release evidence.

## Release identity

Release qualification is bound to an exact commit object. A candidate is
qualified on an immutable `v0.9.0-rc.N` tag and its candidate manifest records
the artifact inventory and digests. The stable `v0.9.0` tag may promote only
the same peeled SHA and candidate bytes. Python publication uses the independent
`alopex-py-v0.9.0` tag at that same SHA.

The release workflow performs RC qualification and stable promotion separately.
Stable Delivery does not rerun functionality or performance suites; those
checks belong to Development CI and Extended Verification. Public verification
only checks exact-version package reachability and isolated installation/import.

## Upgrade boundary

Before upgrading, record the running v0.8 artifact identity and status, create
a restore-capable backup, and retain its manifest until local read, metadata,
and recovery checks pass. Record the candidate SHA and readiness report before
any range, recovery, or upgrade operation. If a prerequisite is missing or a
validation fails, stop the operation and keep the result classified as blocked,
rejected, retryable, or unavailable.
