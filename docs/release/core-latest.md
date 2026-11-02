# Core latest after Python publication

Core and Python have independent GitHub Releases. Stable Core publication sets
`make_latest: "true"`; Python publication sets `make_latest: "false"` explicitly.
This prevents the Python-only asset inventory from replacing the CLI download
entry point at the repository's latest endpoint (#688).

After Python publication and the existing Core/Python join, both release
workflows run `scripts/release/verify_latest_release.py`. The public verification
workflow repeats this read-only check and includes its result in the structured
JSON/Markdown report. Each workflow retains the observed release identities and
asset metadata as an Actions artifact, including on verification failure.

The check requires the requested stable Core tag and Release ID at
`releases/latest`. Starting with v0.8.17, Linux x86_64, Windows x86_64, and
Apple Silicon CLI assets must be present with matching IDs, sizes, states,
digests (when supplied by GitHub), and download URLs. Older versions still
require all four CLI assets. The independent primary Python wheel and sdist
remain mandatory. The complete platform inventory is qualified by the primary
release and Core/Python join gates.

The JSON record lists `required_cli_assets` explicitly. It also reports
`intel_cli` as `unavailable`, `pending`, `available`, or `inconsistent`, with its
`required` flag. For v0.8.17 onward, these optional observations do not become a
primary publication dependency. In-progress uploads from #686's separate Intel
bundle are recorded in the asset inventories; their complete identity and
publication are verified by the independent Intel workflow. An `inconsistent`
Intel observation remains unresolved Intel evidence, even when primary Latest
verification succeeds.

Draft/prerelease identities, duplicate assets, unfinished primary assets, and
replaced/missing mandatory CLI assets fail verification. Exact-tag inventories
are paginated. Observation time and release/asset metadata remain in the record.
The immutable candidate and supplemental publication gates retain responsibility
for artifact bytes and native qualification.

```sh
python3 scripts/release/verify_latest_release.py \
  --repository alopex-db/alopex --version 0.8.17 \
  --output latest-release.json
```

The command uses authenticated `gh api` GET requests only. A mismatch in Core
identity or mandatory assets fails delivery/report verification; it does not move tags, alter release metadata,
replace assets, or rebuild them. Diagnose the recorded identity before making
an authorized metadata correction. Verifying an older version after a newer
Core becomes latest will intentionally fail this exact-version check; it must
not demote the newer release automatically.

The required target policy follows #686: a queued, running, failed, or late
Intel delivery cannot block primary publication. A ready Intel CLI is recorded
only when both Core and Latest metadata agree. Missing or replaced primary CLI
assets, Python stealing Latest, and missing independent Python artifacts remain
failures. Published tags, artifacts, and release metadata are never modified by
this check.

Local regression fixtures reproduce Python stealing latest and detect missing
or replaced CLI/Python assets. Final v0.8.17 acceptance still requires actual
Core/Python publication and the recorded live endpoint result in #690.
