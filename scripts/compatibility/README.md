# Old writer → candidate reader regression checks

These are the reusable versions of the v0.8.15 → v0.8.16 checks originally run
outside the repository. They test **data written by old software and read by new
software**. They do not require old software to read a later format.

The candidate Python wheel and Rust crates must come from the checkout under test.
The baseline is the published Python wheel and exact Rust crates at **0.8.15**
(source tag `d3917e1fa21d41097742a971e14094c0d6ae397e`). `provenance.json` records
the scope, source, generation command and limitations. These are Alopex local
regressions, not external-product conformance tests.

## Coverage

| Contract | Inputs and assertions |
| --- | --- |
| Inherited Python API behavior | Replay an explicit case-ID allowlist from 16 v0.8.15 files against both isolated packages: SQL/DDL/DML/types/bindings, KV and transaction lifecycle, CSV streams, DataFrame and pinned Polars, catalog/metadata/errors, connection dispatch, async wrappers, credentials and remote-client local HTTP stubs. Every old-passing test must pass in the candidate; skipped/failed baseline cases are listed separately. |
| Public Python surface | Compare all discovered public symbols from `alopex`, `alopex.asyncio`, `alopex.remote`, `alopex.protocols`, including classes/members, callable signatures where metadata exists, defaults and stable error-code preservation. Optional additions are allowed. Presence/signature coverage does not replace behavior assertions. |
| Rust API source and behavior | Compile the same old client against frozen registry crates and candidate local-path crates. Ten groups exercise KV CRUD/scan, read-only/commit/rollback, SQL CRUD/constraints/metadata, prepared bind/reset/finalize, SQL sessions, CSV reader/writer, catalog hierarchy, DataFrame/SQL streaming, memory/config/clone/snapshot/clear, chunked Blob/Typed values and cancel. |
| Direct same-DB upgrade | Old process writes, flushes and closes; candidate checks the original DB file hashes before opening the same path. SQL VECTOR(3/384), raw 3D vector, NULL, empty/NULL/Unicode/quoted/newline TEXT, old HNSW indexes without recreation; second reopen. |
| Legacy columnar | Four TEXT rows (`apple`, `apricot`, `zebra`, `yak`), SQL row-group size 1000 (the minimum accepted by the public API). Candidate must read all rows and reopen. No old-reader self-read diagnostic. This public SQL fixture does not certify a particular on-disk encoding; an independent Rust case writes two 2-row groups with the published old codec, asserts their different encodings, and reads those exact bytes with the candidate. |
| Direct cross-language upgrade | Copies of DBs written by old Python and old Rust, opened by candidate Python and Rust. Typed scalar data, vectors, binary KV, existing HNSW with forced `HnswSearch`, second reopen. |
| Logical export/import | Python→Python, Rust→Rust, Python→Rust, Rust→Python. Explicit schema/index recreation; typed SQL bundle, raw KV hex and columnar SELECT→CSV. Reopen, query, insert and update. Scalar Parquet must preserve values. |
| External PyArrow | All five representations (List/FixedSizeList Float32/Float64 and JSON UTF8), both 3 and 384 dimensions. Exact original float32 bits, NULL and TEXT, reopen, exact nearest-neighbor oracle and forced HNSW. All ten cases must pass. |

Native CSV NULL/empty handling (#691), columnar Parquet export (#692) and VECTOR
Parquet COPY (#693) have separate issue acceptance. Logical reports retain their existing native probes; the external Arrow cases no longer invoke unrelated native COPY diagnostics. A successful alternate route
**does not certify those native APIs**. Their existing defects are not silently
converted into successful compatibility results. No blanket export/reimport
requirement follows from this matrix. The samples do not cover every historical
DB/segment layout or performance.

## Run locally

Python 3.11+ is needed for the harness. Prepare two isolated virtual environments
with `numpy==2.2.1` and `pyarrow==26.0.0`; install `alopex==0.8.15` from a binary wheel
in the old environment and the checkout's built wheel in the candidate environment.
Both environments also need `pytest==9.1.1` and `polars==1.43.2` for the frozen API
corpus. The pinned v0.8.15 Git commit must exist locally (CI fetches it once if missing).
Do not install the published candidate as a substitute for a source-build CI check.

Before building, measure the entire host's Alopex working storage, including other
worktrees, `/tmp`, environments and Cargo caches. Keep one shared Cargo target and
serialize builds. At 40 GB, stop and clean regenerable outputs; 50 GB is the hard
per-host maximum. An inventory failure stops execution. `storage.py` discovers
linked worktrees and usual local roots; pass `--root` for any additional location.
It records the inventory and monitors child commands every 15 seconds.

```bash
export CARGO_TARGET_DIR="$PWD/target"
python scripts/compatibility/storage.py --log /tmp/alopex-compat-storage.jsonl \
  --additional-bytes 20000000000 -- bash scripts/compatibility/build-rust.sh
python -m unittest scripts.compatibility.test_runner scripts.compatibility.test_api
python scripts/compatibility/storage.py --log /tmp/alopex-compat-storage.jsonl \
  --additional-bytes 2000000000 -- \
  python scripts/compatibility/run.py \
    --old-python /path/to/old-env/bin/python \
    --new-python /path/to/candidate-env/bin/python \
    --candidate-wheel /path/to/checkout-built-candidate.whl \
    --old-rust target/debug/alopex-compat-baseline \
    --new-rust target/debug/alopex-compat-candidate \
    --output /tmp/alopex-compat-results
```

`api-cases.json` is the machine-readable behavior corpus inventory, provenance,
required dependency set and explicit exclusions. The archived historical test
files remain unchanged. Only explicit case IDs run, with declared replacements for
the same-process ownership prose assertion and two configurable streaming cases.
The 34,250-row case is excluded; fixed internal Arrow boundary cases retain
1023/1024/1025 rows, while configurable streaming uses five rows and chunk size two. Its replacement preserves the
exception class/code, second-open rejection and reopening after handle release;
`api-cases.json` pins the implementation change and exact old/new case IDs.
A missing family, failed collection or skipped case
that used to pass cannot silently reduce coverage. The driver records the test
source hashes and old/new per-case setup/call/teardown outcomes. It also records
baseline failures and skips without counting them as verified compatibility.
The remote client is exercised against local HTTP stubs. Real old/new server
wire compatibility and the whole public Rust API are not certified by these
curated cases; existing server CI remains separate.

The output directory must be empty. `--python-only` explicitly excludes Rust for a
focused check; CI always uses the full matrix. Missing Rust binaries, a missing
case, an unsuccessful Arrow representation, or a failed child process cannot
produce a full-matrix pass. Reports record native module/binary hashes, dependency
versions, source HEAD, script hashes and each process's exit code. Generated DBs
are removed even on failure. Keep the compact reports/logs/CSV/Parquet evidence;
delete the dedicated venvs and wheels after use, and remove build outputs once no
active check needs them. Never delete another task's files.

## CI ownership

The existing Linux/NumPy `alopex-py` job runs this matrix using its already-built
release wheel and shared Cargo target. It builds the baseline and candidate Rust
harnesses serially, creates an isolated old Python environment, and compares old
writers against the **PR checkout**, not just two published packages. Evidence is
uploaded on failure as well as success. The workflow's ordinary `ci-success`
aggregator includes that job. No extra release tag, release run or local runner is
needed. Tests and workflow changes trigger this lane through its path filters.

## Updating the candidate

The old Rust lockfile is a frozen registry baseline. The candidate Rust manifest
uses local path dependencies; when the workspace version or dependencies change,
regenerate its lockfile (`cargo generate-lockfile --manifest-path
scripts/compatibility/rust-current/Cargo.toml`) in the same change and review the
diff. CI builds both with `--locked` so dependency drift cannot change a passing
result silently. Do not replace the old writer with the candidate writer.

## Known Rust source incompatibility: #696

The runtime cases above do **not** mean that every old Rust source program builds.
The unchanged public-struct client in `legacy_config_literal.rs` builds against
0.8.15 but fails against 0.8.16 with `E0063`: `EmbeddedConfig` gained the public
`hnsw_cache_bytes` and `hnsw_cache_entries` fields. The candidate source file is
identical to the one in the published 0.8.16 crate. This is tracked in
https://github.com/alopex-db/alopex/issues/696 (v0.8.17).

With the same host budget guard and shared target used above, reproduce it with:

```bash
# Historical writer/client: compiles (unset candidate parser overrides first).
env -u NIM_SQL_PARSER_LIB_DIR -u ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD \
  cargo check --locked --manifest-path scripts/compatibility/rust-old/Cargo.toml \
  --features legacy-config-literal
# Candidate: currently fails with E0063; this is not a passing compatibility case.
cargo check --locked --manifest-path scripts/compatibility/rust-current/Cargo.toml \
  --features legacy-config-literal
```

The failing compile witness is opt-in, separate from the passing runtime matrix.
It must not be reported as qualified or silently worked around by replacing the
old client's struct literal with a constructor. Resolving this requires a source
API decision that accounts for clients of both already-published versions; merely
removing the new fields would break clients that adopted them in 0.8.16.

## Case size and execution budgets

These are correctness checks, with one execution per writer/reader and no warmup,
throughput, latency, scale, or load acceptance. Every native and external Arrow
case has its own process, DB path, result, and log. Logical directions already
have independent reader processes and databases. Python API behavior retains
pytest case IDs and per-case outcomes; filenames cannot implicitly expand coverage.

| Case IDs | Fixture budget | Reason |
| --- | --- | --- |
| native/sql-3, native/sql-384 | 5 rows each, 4 non-NULL + 1 NULL | Exact float32 bits, text variants, retained HNSW and NULL contract |
| rust-columnar | 4 rows, 2 groups of 2, <16 KiB | Old codec V2 bytes plus metadata, asserted mixed encodings, candidate read and reopen |
| native/raw-3 | 1 key, 3 floats | Raw vector API persistence |
| native/columnar | 4 rows, SQL row-group size 1000 | TEXT persistence through public SQL, candidate read and reopen |
| arrow/{list_f32,list_f64,fixed_f32,fixed_f64,json_utf8}-{3,384} | 5 rows each | Representation, precision, dimension, NULL and recreated HNSW contract |
| logical/{Python,Rust}→{Python,Rust} | scalar 3, typed 1, vectors 2, columnar 2, KV 2 | Types and directional interoperability, not volume |
| API configurable batches | 5 rows, chunk size 2 | Two full batches plus a tail |
| API fixed Arrow batches | 1023 / 1024 / 1025 rows | Internal 1024-row boundary cannot be configured by this API |

The driver runs children serially, with a 300-second fail-closed timeout per
phase, and no retries. The CI wrapper caps additional storage at 2 GB and stops
at the host's 40 GB threshold. Timeouts, aborts and missing receipts fail the run.
Native logs flush before and after create/write/flush/close/open/read/reopen.
DBs are removed in the driver's finally block; compact logs and receipts remain.
Builds use the existing wheel and one shared Cargo target, not one build per case.

To investigate a single contract after preparing environments, set
`ALOPEX_COMPAT_OUTPUT` to a new owned directory and `ALOPEX_COMPAT_TARGET_VERSION`
to the candidate version, then run `native.py old sql-3` with the old interpreter
and `native.py new sql-3` with the candidate. Arrow accepts
`arrow.py export list_f32 3` / `arrow.py import list_f32 3` similarly. A focused
case is not a full-matrix pass. Remove its owned DB after retaining the receipt.

Normal Python CI explicitly ignores `tests/benchmarks` and `test_benchmark_nfr13.py`.
Performance workflows are manual only, require an owning issue, and must have
separate input/resource/cleanup budgets before dispatch. Their results do not
qualify ordinary correctness CI.
