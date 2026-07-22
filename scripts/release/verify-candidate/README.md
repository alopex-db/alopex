# verify-candidate

`verify-candidate` is an independent, development-only Cargo workspace for the
v0.8 release-readiness gate. It verifies a local/staging candidate only. It has
no command path for tags, package publication, GitHub Releases, notifications,
branch creation, or pull requests.

The verifier accepts a read-only source tree, requirements root, hash-pinned
offline input bundle, immutable approved-scope snapshot, capability manifest,
and artifact inventory. Its sole writable path is `--output`.

Create the immutable scope snapshot once for a candidate. The input is a JSON
array of `ApprovedScopeInput` records containing Phase 1–4 canonical paths,
approved revisions, dashboard decision URI/export path/hash, and requirement
headings. `--output` uses create-new semantics and refuses replacement.

```text
cargo run --locked --offline -- snapshot \
  --requirements-root /candidate/requirements \
  --candidate-commit COMMIT \
  --input /candidate/input/approved-scope-input.json \
  --output /candidate/input/approved-scope.json
```

```text
cargo run --locked --offline -- verify \
  --requirements-root /candidate/requirements \
  --source /candidate/source \
  --bundle /candidate/input \
  --snapshot /candidate/input/approved-scope.json \
  --manifest /candidate/input/capabilities.json \
  --inventory /candidate/input/inventory.json \
  --output /candidate/report
```

On Linux it requires `bwrap`; the runner uses a network namespace, read-only
source/input mounts, empty home/cache locations, and a fixed command enum. If
that enforced backend is unavailable, or a local artifact check fails, the
verdict is `Blocked`. `Ready` remains evidence only and does not grant release
authority.
