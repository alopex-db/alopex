# v0.9 Phase 3 changefeed contract and evidence crosswalk

## Current availability

The public shape of the Phase 3 changefeed is implemented and recorded, but the
feature is **Blocked**.  The current Chirps profile has no authenticated Durable
storage, range-routing, retention, and dispatcher evidence.  `create` and
`subscribe` therefore return the canonical `prerequisite_missing` outcome; they
do not fall back to a local WAL feed and must not be presented as a usable
Durable changefeed.  This document neither approves a release nor authorizes a
tag, publication, Docker image push, or deployment.

The exact machine-readable mapping is
[`evidence/v09-phase3-crosswalk.json`](../evidence/v09-phase3-crosswalk.json).
It records every R1.1–R5.4 criterion with one primary task owner, contributing
tasks, independent unit/integration/cross-surface checks, and evidence paths.
The manifest and inherited ledger remain separate inputs because crosswalking
does not turn their pending common-gate conditions into a pass.

## What a usable deployment will provide

After an authenticated compatible Durable profile passes preflight, an
authorized consumer can use the registered embedded, HTTP, gRPC, CLI, and
Python sync/async lifecycle:

`create` → `subscribe` → `poll` or `stream` → `ack` → `resume` → `cancel` or
`close`.

The canonical result identifies the feed, cluster, table, range, schema/data
epoch, request/operation, ordering scope, event/checkpoint, state, failure
class, retryability, and idempotency result.  Delivery is at least once and is
ordered within a range only.  A duplicate is identifiable by `event_id`; it is
not silently converted into a distinct change.  `ack_state=committed` is the
only durable acknowledgement.  `accepted` and `pending` must never be shown as
a durable checkpoint.

`resume(checkpoint)` returns retained events strictly after that checkpoint in
range order, or an explicit outcome.  A retention-expired checkpoint is a
terminal `stale_metadata` outcome with `reason_code=retention_expired`; it does
not restart silently at the oldest retained event.  Gap, epoch mismatch,
missing prerequisite, and unavailable node/replica are likewise explicit
outcomes.  Repeating the same ack, resume, cancel, or close request returns the
stored first outcome without applying a second side effect.

## Registered surfaces and failures

HTTP, gRPC, CLI, embedded Rust, and Python sync/async project the same
canonical fields.  JSON, JSONL, CSV, TSV, table output, and Python exceptions
retain the status/failure/checkpoint information rather than replacing it with
a successful empty result.  The full lifecycle and failure mapping is the
versioned source of truth in
[`evidence/changefeed-manifest.json`](../evidence/changefeed-manifest.json).
The shared durable-preflight fixture is
[`tests/fixtures/changefeed_surface_parity.json`](../tests/fixtures/changefeed_surface_parity.json).

SQL and DataFrame/DataFrame-source changefeed lifecycle requests are deliberately
pre-execution `unsupported`: no SQL statement, DataFrame source open, lazy plan,
stream, transport, checkpoint mutation, or implicit feed is started.  This does
not change their existing local SQL or DataFrame behavior.

The change-kind matrix also keeps the following outside the Phase 3 feed claim:

- DDL/schema, COPY/bulk, vector/HNSW/columnar mutations, and unclassified
  insert/update records are rejected before publication.
- CRDT event semantics are owned by Phase 2; distributed-transaction events are
  owned by Phase 4.  Neither is implicitly supplied by this changefeed.
- Distributed DataFrame execution and an unapproved Python remote client are
  out of scope.

The current supported journal evidence is confined to a committed local SQL
transaction boundary and classified delete events.  Other kinds may only become
publishable after their own approved journal evidence; no payload or operation
type is inferred from a post-image-only record.

## Requirements-to-evidence summary

| Requirement | Primary design responsibility | Primary task owner | Independent evidence | Gate status |
|---|---|---:|---|---|
| R1.1–R1.4 | Feed coordinator, journal adapter, range order, support matrix | 3.1 / 3.4 / 3.21 / 3.19 | durability, range-movement, and unsupported-boundary fixtures | Durable and Phase 1 evidence block feature availability |
| R2.1–R2.4 | checkpoint, replay, resume, idempotency | 3.7 / 3.23 | checkpoint and three-attempt replay fixtures | Durable evidence blocks feature availability |
| R3.1–R3.3 | retention/backpressure and Durable preflight | 3.8 / 3.6 | backpressure and cross-surface preflight fixtures | intentionally fail closed |
| R4.1–R4.3 | lifecycle register and surface adapters | 3.19 / 3.13 / 3.16 | manifest, SQL/DataFrame boundaries, parity fixture | operational lifecycles await Durable evidence |
| R5.1–R5.4 | ledger, verifier handoff, common gate | 3.29 / 3.28 / 3.27 | ledger, verifier input, common-gate input | common gate and release-wide decision pending |

Each individual criterion, rather than only this summary group, is listed once
in the crosswalk JSON.  It has an `owner_task` (the accountable primary task),
separate `contributing_tasks`, and all three test viewpoints.  This prevents a
representative surface test from being treated as proof for an unregistered
surface or an inherited row.

## Completion and release boundary

Task 3.30 must independently inspect the schema/order, checkpoint/replay,
retention/backpressure, failure/idempotency, surface parity, inherited ledger,
crosswalk, Durable evidence, and v0.9 verifier report.  It may mark Phase 3
complete only when all listed inputs are available.  At this revision the
Durable/common-gate item is still blocked, so Phase 3 is not complete.

Phase 1 range identity/placement is a prerequisite.  Phase 2 CRDT and Phase 4
distributed-transaction work remain release-wide dependencies and do not create
a circular Phase 3 support claim.  A target-version v0.9 release gate is a
separate later operation; v0.7/v0.8 gates cannot substitute for it.
