# Immutable public verification reports

The verification job owns the artifact name
`release-verification-<run_id>-<producing_attempt>`. It exports that name as a job
output. Both normal publication and publication-failure handling download that
exact artifact. Retrying only failed publication jobs must not select an artifact
named after the later publishing attempt or silently select the newest artifact.

`scripts/release/report_pair.py select` checks the single downloaded JSON report,
its terminal outcome, version, producing run/attempt and repository URL. Its
Markdown companion must be the deterministic rendering of that JSON. Missing,
ambiguous, mismatched, oversized or symlinked files are rejected. An unresolved
version and a failed verification remain publishable evidence; they do not
become successful verification results.

The primary publisher commits both original files to the Alopex report branch:

```
report/verify-release-v<version>-<run_id>-<producing_attempt>
reports/release-verification/v<version>/run-<run_id>-<producing_attempt>.json
reports/release-verification/v<version>/run-<run_id>-<producing_attempt>.md
```

An existing branch is reusable only when both files are byte-identical. A
Markdown-only legacy branch or either changed file fails without overwriting or
repairing the branch. Docs publication is confirmed only after both public files
match. A docs ingestion timeout retains the Alopex branch for diagnosis and
retry; successful confirmation permits deleting that temporary branch.

The docs receiver on main already imports JSON and Markdown from these public
branches on a configured 15-minute schedule, using its own repository token to
write docs. Actual scheduled execution may be delayed. The
primary publisher relies on this pull route and does not request cross-repository
dispatch. Its bounded wait checks the actual pair, so scheduling delay, receiver
failure, or a mismatch remains a publication failure rather than an assumed
success. Read-only/local contract checks are not evidence of live v0.8.17
publication.

This is the producer-side portion of #431. The remaining acceptance requires a
common primary/failure evidence ingestion path, separate publication-failure
evidence and failure-stage diagnostics, and authenticated/unauthenticated checks
of the real public pair. The current fallback still writes directly to the docs repository;
its credential and retention problems are not resolved by producer validation.
The full automation must not be accepted until those receiver and failure-path
requirements are verified. Existing historical Markdown-only reports are not
rewritten automatically.
