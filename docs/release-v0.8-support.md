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

## Artifact identity

The candidate verifier inventories every Cargo workspace member separately.
It compares each crate's declared version rather than assuming a shared version,
and requires a local package/archive for each product crate. It also requires:

| Artifact | Candidate verification |
| --- | --- |
| `alopex-cli` / `alopex` binary | Package identity plus isolated `--version` startup. |
| `alopex-py` / `alopex` wheel | Wheel tag/hash/native extension inspection and isolated `--no-index --no-deps` install/import. |
| `alopex-tools` | Development-only (`publish = false`) evidence. `crates/alopex-tools/v08/verify-v08-embedded` builds the checked-out v0.8 Embedded/SQL sources and runs the local compatibility corpus; it is never listed as a v0.8 distribution artifact. |

The verifier records platform and hash next to each artifact. A release candidate
with a missing, mismatched, or unclassified artifact is `Blocked`.

## Related operational documentation

- [Cluster metadata operations](cluster-operations.md)
- [Distributed-read SQL/CLI coverage](distributed-read.md)
- [DataFrame bounded and streaming contract](dataframe-streaming.md)
- [Python embedded-local API](../crates/alopex-py/README.md)
- [v0.7.4 to v0.8 upgrade and recovery](upgrade-v0.7.4-to-v0.8.md)

Post-release verification is intentionally `not_run` in every candidate report.
It can only be performed after a separately authorized public release action.
