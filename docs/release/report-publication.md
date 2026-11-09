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

## Release-day receiver procedure

After the producer has pushed its `report/verify-release-v0.8.17-*` branch,
dispatch the docs receiver for this version if no receiver run is already
processing it. Do not rely on the scheduled run arriving within the publisher's
bounded wait:

```bash
gh workflow run publish-release-verification.yml \
  --repo alopex-db/docs --ref main -f version=0.8.17
```

Confirm the receiver run succeeded, then fetch the versioned JSON and Markdown
from docs main both through the authenticated GitHub API and without
authentication. Compare the complete bytes of each file with the producer's
run/attempt-bound artifact and, while it remains, its immutable report branch.
A successful dispatch or a matching canonical report alone does not establish
that the versioned pair was published. The producer may remove its branch only
after it confirms both files. If its wait has already timed out, retain the
branch until the same byte checks succeed. Once the
publisher run is terminal, remove only that confirmed branch.

If the producer creates a separate `report/verify-release-failure-v0.8.17-*`
branch, run the same version-scoped receiver again. Check its failure JSON and
Markdown independently from the original verification pair, using both
authenticated and unauthenticated byte checks. When the original pair was
available, confirm all four versioned files before removing the failure branch.
Missing files, rejected bytes, or a receiver failure leave the branches and
Actions artifacts available for diagnosis and retry.

## Independent publication failure evidence

If primary publication fails, `publication_failure.py` creates a separate
`alopex-report-publication/v1` JSON/Markdown pair. It records the failed stage,
publishing attempt and publisher commit. When the original verification pair is
available and valid, it is retained unchanged with both SHA-256 digests and its
original producing attempt. Missing or rejected originals are explicitly marked
unavailable; no successful verification is inferred.

```
report/verify-release-failure-v<version>-<run_id>-<publishing_attempt>
reports/release-verification/v<version>/run-<run_id>-<publishing_attempt>-publication-failure.json
reports/release-verification/v<version>/run-<run_id>-<publishing_attempt>-publication-failure.md
```

The fallback uploads `publication-failure-<run_id>-<publishing_attempt>` before
attempting branch publication, then retains the bundle in Alopex using its own
token. Docs must install the matching reviewed `publication_record.py` contract
and receiver support before this route can complete. The receiver validates the
whole bundle before writing, including the original pair's hashes, identities,
source tag when resolved, and all immutable-path collisions. Publication failure
bundles never promote the canonical full-scenario report. Unknown versions and
unresolved failed verification remain explicitly unqualified evidence.

The fallback confirms every file on docs main before deleting its Alopex branch.
A missing file, permission denial, timeout or byte mismatch leaves the branch
and uploaded artifact available. Neither the primary nor fallback route needs a
cross-repository write token. The separately disabled benchmark job retains its
existing credential interface.

The remaining #431 acceptance is integration of both producer and receiver,
CI, and authenticated/unauthenticated checks of the actual public pair and
failure route. Local contract checks do not prove live delivery. Historical
Markdown-only reports are not rewritten automatically.
