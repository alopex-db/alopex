# v0.9 changefeed support matrix

`evidence/changefeed-manifest.json` is the machine-readable, exact register for
this document.  It is a release-gate input, not a claim that every prerequisite
is available in every installation.  The common gate must fail on a missing,
unknown, duplicate, stale-snapshot, or invalid-status row.

## Lifecycle surfaces

| Surface | create | subscribe | poll | stream | ack | resume | cancel | close |
|---|---|---|---|---|---|---|---|---|
| Embedded Rust | supported after Durable preflight | supported | supported | supported | supported | supported | supported | supported |
| HTTP | supported after Durable/auth preflight | supported | supported | supported | supported | supported | supported | supported |
| gRPC | supported after Durable/auth preflight | supported | supported | supported | supported | supported | supported | supported |
| CLI | supported after server-profile/Durable preflight | supported | supported | supported | supported | supported | supported | supported |
| Python sync | supported API; fails closed when Durable is absent | supported | supported | supported | supported | supported | supported | supported |
| Python async | supported API; fails closed when Durable is absent | supported | supported | supported | supported | supported | supported | supported |
| SQL | unsupported before parse, plan, or execution | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported |
| DataFrame / source | unsupported before source opening or planning | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported | unsupported |

“supported” always means that the exact preconditions in the manifest have
passed.  A missing authenticated Chirps Durable profile is not a local fallback:
it is `prerequisite_missing`.  SQL and DataFrame have no implicit feed API;
their rejection is `unsupported` before execution and has no checkpoint or
transport side effect.

## Change kinds

| Change kind | v0.9 status | Boundary |
|---|---|---|
| table/range target identity | supported | Phase 1 `RangeIdentity`/`Placement` preflight |
| SQL row delete | supported | journal adapter |
| SQL row insert/update | unsupported | current journal lacks approved operation classification; return `payload_unavailable` |
| local SQL transaction commit | supported | same commit boundary as the journal event group |
| DDL/schema | unsupported | no catalog-delta journal |
| COPY/bulk | unsupported | no bulk-to-event mapping |
| vector/HNSW | unsupported | no vector mutation event evidence |
| columnar segment | unsupported | DataFrame boundary rejects before source work |
| CRDT update | unsupported | Phase 2 semantics are not an implicit feed event |
| distributed transaction commit | unsupported | Phase 4 contract/evidence is required first |

## Exact inventory and ownership

The manifest records, with source, owner, and regression evidence:

- all 77 `alopex-sql::scalar::signatures()` name/arity entries and all nine
  `StatementKind` variants;
- the complete Clap top-level tree, its global options, and the Phase 3
  `changefeed` command paths;
- all literal server routes and all 35 `AlopexService` protobuf methods;
- embedded and sync/async Python changefeed public stubs, plus the eight
  DataFrame targets that must reject a feed request;
- every resolved Chirps package/version and the exact `alopex-cluster/chirps`
  feature members; and
- every existing internal release verifier/binary/script with its scope.

The v0.8 surface script, v0.8 embedded verifier, and legacy v0.7 gate are
listed specifically to prevent their use as a v0.9 success substitute.
`alopex-tools` is internal: it verifies candidate/release evidence and is not a
published product artifact.  Task 3.28 will give the target-version common gate
the verifier handoff; this task neither creates a verifier nor performs a
release.
