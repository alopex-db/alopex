# Old UNIQUE fixture generation

The fixture owner is SQL catalog/index recovery. The producer uses exact
v0.8.15 core/SQL and its normal contract-0.25.0 parser. The consumer imports all
committed KV bytes, not reconstructed current metadata. This fixture does not
certify disk-container, WAL, Python-wheel, or CLI compatibility.

The producer generated `legacy_unique_v0815.json` and
`legacy_unique_v0815.manifest.json` in this directory from seven successful
old-engine executions. Each execution checked actual SELECT results before
capturing committed KV bytes. The manifest records the exact source, tools,
dependencies, parser, commands, and artifact digests. Candidate-consumer
acceptance is a separate check. Tests must fail if required fixture evidence is absent;
they must not silently synthesize replacements.

## Reproduction inputs

The operator supplies `OLD_SOURCE` (clean worktree at
`d3917e1fa21d41097742a971e14094c0d6ae397e`), `OWNED_TEMP` (dedicated output),
`THIRD_PARTY` (verified readonly arm64 Rust1.96 dependencies), `PYTHON`,
`BUDGET_WRAPPER` (300MiB/600-second supervisor), and `PRODUCER_SOURCE`
(`crates/alopex-sql/tests/support/legacy_unique_producer.rs` in the candidate).
The old worktree is branch `validate/v0816-585-old-producer`; its product
sources stay unmodified. The operator records private expanded commands only
in local evidence and redacts them before publishing.

The operator builds the old parser from the old worktree with verified
`ALOPEX_NIM_BIN`, `ALOPEX_NIMBLE_BIN`, `ALOPEX_NIMBLE_SEED_DIR`, and
Python 3.11 inputs. The old script invokes `python3` directly, so the operator
places the verified interpreter directory first in PATH and records its version:

```sh
export PATH="$(dirname "$PYTHON"):$PATH"
python3 --version
bash scripts/build-nim-parser.sh --backend host --target aarch64-apple-darwin \
  --output "$OWNED_TEMP/parser/libalopex_sql_parser.dylib" \
  --archive-dir "$OWNED_TEMP/parser-record"
```

The operator runs the candidate-owned build recipe under the same output
budget, then runs one case per process:

```sh
bash crates/alopex-sql/tests/support/build_legacy_unique_producer.sh
"$OWNED_TEMP/native/old-unique-producer" column "$OWNED_TEMP/cases/column.json"
```

The seven case names are `column`, `named`, `composite`, `nullable`, `duplicate`,
`named_duplicate`, and `composite_duplicate`. The producer commits SQL through
PersistentCatalog plus execute_in_txn before reading all KV entries. Duplicate
cases must actually commit with the old engine; raw injection is not a substitute.

The operator records source, compiler, dependency, parser-record, producer
source/binary digests and normalized commands in a public identity JSON. Public
commands use project-relative paths or `<owned-temp>` / `<third-party>` inputs,
never personal absolute paths or hostnames. The operator checks all public
fields before assembling:

```sh
python3 crates/alopex-sql/tests/support/assemble_legacy_unique_fixture.py \
  --cases-dir "$OWNED_TEMP/cases" --identity "$OWNED_TEMP/public-identity.json" \
  --output crates/alopex-sql/tests/fixtures/legacy_unique_v0815.json \
  --manifest crates/alopex-sql/tests/fixtures/legacy_unique_v0815.manifest.json
```

The operator reclaims all old native outputs before building the candidate
consumer. The existing persistence_test target owns filter
`issue585_old_producer_ --nocapture --test-threads=1`. The candidate must retain
old rows/metadata/neighbors, build missing indexes, reject actual duplicates
atomically, allow NULLs and distinct composite values, and remain idempotent.
The fixture is immutable evidence; candidate runs never regenerate it.

## Partial-output recovery

The assembler validates all seven inputs before writing, but its fixture and
manifest are two separate create-new files, not an atomic pair. An I/O error or
process interruption after the first write can leave a fixture without a manifest.
The consumer refuses that incomplete pair. A retry never overwrites either file.
The operator preserves the failed run and uses a fresh output/manifest pair;
the operator may remove only a confirmed run-owned partial output after recording
its digest. The assembler never silently deletes or replaces earlier evidence.

The standard-library behavior tests use synthetic JSON strictly to test the
assembler: seven-case output and digest, missing case, malformed hex, existing
output preservation, and partial-pair refusal/fresh-pair retry. They neither run
an engine nor certify the historical provenance of the synthetic inputs.
