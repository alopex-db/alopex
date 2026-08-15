# S2-c compatibility fixtures

Each version directory contains a deterministic GNU sparse `data.tar.gz`
written by that immutable old source tag, the canonical `99_verify.sql` result,
and content-addressed provenance. Public release verification extracts a fresh
copy for every supported reader; it never rewrites the checked-in fixture.

To reproduce a fixture without a host-specific source path, build the old tag
in disposable directories and generate to a new comparison directory:

```bash
fixture_source="$(mktemp -d)"
fixture_target="$(mktemp -d)"
fixture_output="$(mktemp -d)/v0.8.4"
git worktree add --detach "${fixture_source}" v0.8.4
CARGO_TARGET_DIR="${fixture_target}" cargo +1.90.0 build \
  --manifest-path "${fixture_source}/Cargo.toml" -p alopex-cli
python3 scripts/parity/generate_compat_fixture.py \
  --alopex-binary "${fixture_target}/debug/alopex" \
  --source-version 0.8.4 \
  --source-tag v0.8.4 \
  --source-sha 9a0cea1d24e7672f59cae72d9218b9cc698d9162 \
  --output "${fixture_output}"
cmp scripts/parity/fixtures/compat/v0.8.4/data.tar.gz \
  "${fixture_output}/data.tar.gz"
cmp scripts/parity/fixtures/compat/v0.8.4/expected.json \
  "${fixture_output}/expected.json"
git worktree remove "${fixture_source}"
```

The generator refuses to overwrite an existing fixture and verifies the old
binary's reported version plus the resulting eight canonical queries before it
writes provenance. Source, target, and output paths are disposable and are not
embedded in the fixture data. `source.binary_sha256` identifies the exact local
generator binary used for that run; it can differ between rebuilds because the
binary contains its native parser rpath, so reproducibility is judged by the
peeled source SHA plus corpus, expected, and data digests.
