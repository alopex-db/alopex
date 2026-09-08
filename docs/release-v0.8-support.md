# v0.8 candidate support and artifact scope

This document describes how a v0.8 **candidate** is evaluated. It is not a
release announcement and does not authorize a tag, package publication, GitHub
Release, or notification. The candidate's generated `support-matrix.md` and
`readiness-report.json` are the authoritative artifact-specific records; they
are produced locally by `scripts/release/verify-candidate` from an approved
requirements snapshot.

## Support scope

| Surface | Candidate support status | Prerequisite and boundary |
| --- | --- | --- |
| Cluster metadata CLI | Conditional | A compatible external cluster foundation and its integration evidence must be present. Otherwise the matrix marks the rows `unavailable`; no in-memory multi-node fallback is supported. |
| Distributed-read SQL and `alopex sql` routing | Conditional | A cluster profile, closed remote-read catalog, retained fenced read point, and compatible foundation evidence are required. Unsupported SQL remains a structured pre-execution result, never a local fallback. |
| DataFrame bounded/streaming execution | Local supported scope | Only the sources, expressions, resource limits, and terminal states in [the DataFrame guide](dataframe-streaming.md) are supported. |
| Python embedded-local API | Local supported scope | Database/Transaction, synchronous/asynchronous streams, and the five documented `LocalScan` variants only. Client, endpoint, remote session, remote DataFrame, and S3 public surfaces are outside v0.8. |

The exact public operation, normal outcome, rejection class, prerequisite, test
evidence, and artifact identity must be present in the generated matrix. A
missing row is a release-readiness blocker, not an implicit support claim.

The [SQL type capability catalog](sql-type-capabilities.json) maps every
v0.8.10 value-type family to its production owners and required evidence. All
required families are complete; the release gate rejects a target version if
any required surface becomes incomplete.
The [JSON-on-TEXT contract](sql-json-text.md) defines issue #160 separately
because those functions reuse `TEXT`; the value-type catalog tracks native JSON
in issue #161. The [nested SQL type contract](sql-nested-types.md) records the
ARRAY/LIST/MAP/STRUCT semantics and public mapping boundaries for issue #162.
The [full-text search contract](sql-full-text-search.md) records the local
tokenizer, query grammar, FTS index lifecycle, and distributed boundary for
issue #163.

## Artifact identity

The candidate verifier inventories every Cargo workspace member separately.
It compares each crate's declared version rather than assuming a shared version,
and requires a local package/archive for each product crate. It also requires:

| Artifact | Candidate verification |
| --- | --- |
| `alopex-cli` / `alopex` binary | Package identity plus isolated `--version` startup. |
| `alopex-py` / `alopex` wheel | Wheel tag/hash/native extension inspection and isolated `--no-index --no-deps` install/import. |
| `alopex-tools` | Development-only (`publish = false`) evidence. `verify-v08-embedded` runs the checked-out v0.8 Embedded/SQL corpus and `verify-v08-surfaces.sh` delegates to the Phase 1–4 cluster/SQL/server/CLI/DataFrame/Rust-Python suites; it is never listed as a v0.8 distribution artifact. |

The verifier records platform and hash next to each artifact. A release candidate
with a missing, mismatched, or unclassified artifact is `Blocked`.

## Related operational documentation

- [Cluster metadata operations](cluster-operations.md)
- [Distributed-read SQL/CLI coverage](distributed-read.md)
- [DataFrame bounded and streaming contract](dataframe-streaming.md)
- [Python embedded-local API](../crates/alopex-py/README.md)
- [JSON-on-TEXT SQL contract](sql-json-text.md)
- [Nested SQL types](sql-nested-types.md)
- [SQL full-text search](sql-full-text-search.md)
- [v0.7.4 to v0.8 upgrade and recovery](upgrade-v0.7.4-to-v0.8.md)

## Verification responsibility and release procedure

The release procedure follows issue #394. A failed check belongs to the layer
whose owner must change it; adding it to Delivery does not transfer that
responsibility.

| Layer | Required evidence | Delivery boundary |
| --- | --- | --- |
| Development CI | unit/integration/API contracts, execution-path contracts, and fast performance regressions for the exact commit | Known functionality must finish here, before an RC tag. |
| Extended Verification | expensive performance, compatibility, stress, durability, and formal evidence for the exact commit | A failure changes the commit or its configuration. |
| RC Qualification | package/archive structure, installability, manifest/digest, and minimum runtime smoke for one produced candidate | It consumes prior evidence; it does not rerun ordinary functionality or performance suites. |
| Stable Delivery | promotion of the approved same-SHA candidate and public registry availability | It never first tests known functionality, execution paths, completeness, or performance. |

Before creating a stable tag, every owning issue must have recorded its required
Development CI / Extended Verification evidence against the target commit. The
existing `v08-release-gate` owns the checked-out implementation surface; the
post-publication workflow must not run its release demos as a second, late
source of truth.

## Cleanup is part of acceptance

Every Development CI, Extended Verification, RC Qualification, and local
release-verification run ends with scoped cleanup. Its owner inventories the
processes, containers, temporary directories, build targets, and artifacts
created by that run; stops owned persistent processes; and removes owned,
reproducible outputs that are no longer needed. Source, fixtures, user data,
active worktrees, and published evidence are never cleanup targets. The owning
issue records retained generated outputs, their owner, and the reason to retain
them alongside the verification evidence.

## Public downstream evidence

The Python publication workflow calls
`.github/workflows/public-release-verification.yml` after publication only to
confirm exact-version PyPI reachability and isolated installation/import. It
does not decide whether known functionality is correct.

The workflow renders and publishes Markdown plus JSON for every run, including
failure and incomplete execution. Each report is stored under a version and
GitHub run/attempt identity, so a later success cannot overwrite or conceal an
earlier failure. The report records the commit, tag, responsibility layer, run
URL, outcome, and diagnostics. The publisher checks out `alopex-db/docs`
directly and writes both files there; it does not rely on an `alopex` branch
being imported elsewhere. Report publication itself is separately observable:
a primary publication failure is appended to the run's JSON/Markdown and saved
by an independent docs-writing job. Only if that independent write also fails
does the workflow create an issue as the remaining visibility signal; an issue
never substitutes for the required docs evidence.

For local review, run the verifier with `--results-file` and `--report-dir`.
`--report-only RESULTS.json` regenerates Markdown without rerunning Docker or
the public package tests.
