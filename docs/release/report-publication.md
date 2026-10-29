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
match. A dispatch failure or timeout retains the Alopex branch for diagnosis and
retry; successful confirmation permits deleting that temporary branch.

This is the producer-side portion of #431. The remaining acceptance requires a
matching docs consumer for JSON and Markdown, a common primary/failure evidence
ingestion path, scoped dispatch credentials or a receiver that needs no
cross-repository write credential, separate publication-failure evidence and
failure-stage diagnostics, and authenticated/unauthenticated checks of the real
public pair. The current fallback still writes directly to the docs repository;
its credential and retention problems are not resolved by producer validation.
The full automation must not be accepted until those receiver and failure-path
requirements are verified. Existing historical Markdown-only reports are not
rewritten automatically.
