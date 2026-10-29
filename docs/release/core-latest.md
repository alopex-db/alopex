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
`releases/latest`, its four currently required CLI names with matching asset
IDs, sizes, states, digests (when supplied by GitHub), and download URLs, plus
the separately published versioned Python wheel and sdist. It rejects draft,
prerelease, unfinished, empty, or duplicate assets. Exact-tag inventories are
paginated. It records observation time and both release inventories. Artifact
byte qualification remains the responsibility of the existing immutable
candidate promotion gates; this metadata check is not a replacement for it.

```sh
python3 scripts/release/verify_latest_release.py \
  --repository alopex-db/alopex --version 0.8.17 \
  --output latest-release.json
```

The command uses authenticated `gh api` GET requests only. A mismatch fails
delivery/report verification; it does not move tags, alter release metadata,
replace assets, or rebuild them. Diagnose the recorded identity before making
an authorized metadata correction. Verifying an older version after a newer
Core becomes latest will intentionally fail this exact-version check; it must
not demote the newer release automatically.

The Intel optional-delivery policy is separately owned by #686. Until that
policy updates the required target contract, this check follows the existing
four-CLI release contract. #686 must update this requirement together with its
manifest, qualification, and post-publication checks; simply disabling this
latest verification would lose #688's acceptance coverage.

Local regression fixtures reproduce Python stealing latest and detect missing
or replaced CLI/Python assets. Final v0.8.17 acceptance still requires actual
Core/Python publication and the recorded live endpoint result in #690.
