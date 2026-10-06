# Separate legacy table-level PK fixtures

The fixture owner is row-storage catalog recovery. The generated outputs are
`legacy_pk_v0815.json` and `legacy_pk_v0815.manifest.json` beside this document.
The producer completed all six cases with actual SQL and SELECT observations.
The old engine accepted NULL with either PK ordering, rejected a duplicate
under a leading PK, and accepted a duplicate under the later PK. The leading
PK retained table.primary_key but had false column flags; the later PK retained
the normalized declaration but no table.primary_key or PK index.
Candidate-consumer validation is separate: all six independent consumers passed
after the candidate metadata repair, including future NULL/duplicate rejection.
The existing seven-case
`legacy_unique_v0815` evidence and its original producer remain unchanged.

The PK producer uses clean exact commit
`d3917e1fa21d41097742a971e14094c0d6ae397e`, its normal contract-0.25 parser,
and real core/SQL libraries. The operator supplies `OLD_SOURCE`,
`CANDIDATE_SOURCE`, `OWNED_TEMP`, verified read-only `THIRD_PARTY`, Python 3.11
`PYTHON`, and a `BUDGET_WRAPPER` enforcing 300MiB and 600 seconds per process.
The operator additionally supplies the verified Nim/Nimble inputs documented
for the existing UNIQUE producer. The shared build recipe is unchanged:

```sh
export PATH="$(dirname "$PYTHON"):$PATH"
python3 --version
mkdir -p "$OWNED_TEMP/parser" "$OWNED_TEMP/parser-record" "$OWNED_TEMP/cases"
(
  cd "$OLD_SOURCE"
  "$PYTHON" "$BUDGET_WRAPPER" bash scripts/build-nim-parser.sh \
    --backend host --target aarch64-apple-darwin \
    --output "$OWNED_TEMP/parser/libalopex_sql_parser.dylib" \
    --archive-dir "$OWNED_TEMP/parser-record"
)
export PRODUCER_SOURCE="$CANDIDATE_SOURCE/crates/alopex-sql/tests/support/legacy_pk_producer.rs"
bash "$CANDIDATE_SOURCE/crates/alopex-sql/tests/support/build_legacy_unique_producer.sh"
```

The producer source and its sibling `legacy_pk_cases.rs` must be available together.
The shared recipe intentionally retains its executable name. The operator
runs one fresh database per process, stopping on a failed producer assertion:

```sh
for case_name in pk_first_valid pk_later_valid \
  pk_first_null_attempt pk_later_null_attempt \
  pk_first_duplicate_attempt pk_later_duplicate_attempt
do
  "$PYTHON" "$BUDGET_WRAPPER" "$OWNED_TEMP/native/old-unique-producer" \
    "$case_name" "$OWNED_TEMP/cases/$case_name.json" || exit
done
```

The producer records accepted or constraint-rejected attempts, commit/rollback
effects, before/after SELECT rows, neighbor7, observed PK metadata, and all raw
KV bytes. A rejected attempt must preserve all prior bytes. An unrelated error
is not a successful constraint observation. The fixed oracle allows either
observed outcome; it does not presume that the old engine accepts NULLs.

The operator supplies public-safe identity JSON with exact source/tool/dependency,
parser, producer and oracle digests plus normalized commands. The PK wrapper
reuses the existing byte-preserving assembler with only a fixed six-case list:

The identity JSON must contain `source_commit`, `parser_contract`,
`source_manifest_sha256`, `producer_source_sha256`, `producer_binary_sha256`,
`oracle_source_sha256`, `dependencies_sha256`, `parser_record_sha256`,
`compiler`, and `commands`. The operator obtains these digests from the actual
run, not from this preparation plan. The assembler writes each case digest,
its SQL digest and entry count, and the final fixture digest into the manifest.

```sh
python3 crates/alopex-sql/tests/support/assemble_legacy_pk_fixture.py \
  --cases-dir "$OWNED_TEMP/cases" --identity "$OWNED_TEMP/public-identity.json" \
  --output crates/alopex-sql/tests/fixtures/legacy_pk_v0815.json \
  --manifest crates/alopex-sql/tests/fixtures/legacy_pk_v0815.manifest.json
```

The assembler never overwrites either output. The existing partial-pair rule
applies: preserve an interrupted output and retry with a fresh pair; never
silently replace evidence. Public identity must contain no personal absolute
paths or hostnames. This fixture certifies SQL/KV metadata observations only,
not disk-container, WAL, Python-wheel, or CLI compatibility.

The operator removes all old native outputs before building the candidate's
existing `persistence_test` target. Its focused command is
`focused-test issue585_legacy_pk_ --nocapture --test-threads=1`. The consumer
registers six independent tests, each verifying the entire fixture identity
before running one case. A RED in the first valid case does not suppress the
other five registered tests.
The consumer
requires immutable fixture/provenance files and never synthesizes replacements.
It checks valid rows and neighbor preservation, PK/not-null restoration,
actual PK index entries, INSERT/UPDATE/MERGE NULL rejection with unchanged
bytes, normal DML controls, invalid old rows' atomic refusal, and reopen
idempotence. Separate tests in `recovery_write_failure.rs` passed for metadata-only
and metadata-plus-index pre-commit failure, table/supporting-index competition,
invalid PK definitions, and normalized-PK reload without writes. These tests
own the new metadata write boundary; the earlier index-only commit-failure
result is not substituted for them.

The operator budgets at most 400MiB per sequential stage, with a 300MiB stop.
Previous minimal runs measured about 95MiB for the old producer and 99MiB for
the candidate. The operator must recheck total storage and compiler/dependency
identities before execution and reclaim native/parser/temp outputs afterward.
