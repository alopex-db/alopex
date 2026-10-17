#!/bin/bash
set -euo pipefail
src=${OLD_SOURCE:?absolute path to clean exact old worktree}
task=${OWNED_TEMP:?dedicated empty output directory}
out=$task/native
deps=${THIRD_PARTY:?readonly verified release dependency directory}
python=${PYTHON:?verified Python executable for budget supervision}
test "$(git -C "$src" rev-parse HEAD)" = d3917e1fa21d41097742a971e14094c0d6ae397e
test -z "$(git -C "$src" status --porcelain)"
test "$(tr -d '\r\n' < "$src/crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION")" = 0.25.0
mkdir -p "$out" "$task/test-data"
cd "$src"
export CARGO_MANIFEST_DIR="$src/crates/alopex-sql"
R=(rustc -C opt-level=1 -C debuginfo=0 -C embed-bitcode=yes
   -C metadata=issue585old -L "dependency=$out" -L "dependency=$deps" -L "native=$task/parser")
T=(--extern "bincode=$deps/libbincode-28e6283f7225ed8c.rlib"
   --extern "regex=$deps/libregex-3cf8b5a844a093a0.rlib"
   --extern "zstd=$deps/libzstd-6e6d9142821fa43d.rlib"
   --extern "rand=$deps/librand-93d445e122e6b887.rlib"
   --extern "crc32fast=$deps/libcrc32fast-c11278399153afbd.rlib"
   --extern "futures_core=$deps/libfutures_core-5fb07f2fae219509.rlib"
   --extern "memmap2=$deps/libmemmap2-568e674c13f143e0.rlib"
   --extern "serde=$deps/libserde-8f957917eee4a4d8.rlib"
   --extern "tracing=$deps/libtracing-9bd8dc805c23d7b1.rlib"
   --extern "snap=$deps/libsnap-346bb41c337ad273.rlib"
   --extern "hex=$deps/libhex-0372880fdee80842.rlib"
   --extern "uuid=$deps/libuuid-f3828baa1ecfe91a.rlib"
   --extern "csv=$deps/libcsv-2321dcc89595d34b.rlib"
   --extern "chrono=$deps/libchrono-d52e9fe7792979df.rlib"
   --extern "md5=$deps/libmd5-a86c18d7b08a7a5e.rlib"
   --extern "sha2=$deps/libsha2-7efb214e3104708f.rlib"
   --extern "arrow_schema=$deps/libarrow_schema-c1e2018a5e3864e5.rlib"
   --extern "arrow_array=$deps/libarrow_array-56317420238735dc.rlib"
   --extern "base64=$deps/libbase64-504b3cf90ab4f68f.rlib"
   --extern "serde_json=$deps/libserde_json-22e25cc3262a4339.rlib"
   --extern "parquet=$deps/libparquet-e9dffb1501edd449.rlib"
   --extern "rmp_serde=$deps/librmp_serde-95f9bc8f2e71ca9b.rlib")
run() {
    printf 'RUN'; printf ' %q' "$@"; printf '\n'
    "$python" "${BUDGET_WRAPPER:?600 second and 300 MiB supervisor}" "$@"
    du -sk "$task" "$src"
}
if [[ "${1:-all}" == "all" ]]; then
run "${R[@]}" --edition=2021 --crate-type rlib --crate-name alopex_core \
  --cfg 'feature="default"' --cfg 'feature="compression-zstd"' --cfg 'feature="zstd"' \
  "${T[@]}" --extern "thiserror=$deps/libthiserror-9f886331ae89c866.rlib" \
  "$src/crates/alopex-core/src/lib.rs" -o "$out/libalopex_core.rlib"
fi
if [[ "${1:-all}" != "test" ]]; then
run "${R[@]}" --edition=2024 --crate-type rlib --crate-name alopex_sql \
  --cfg 'feature="default"' --cfg 'feature="lane_ci"' -L "native=$task/parser" "${T[@]}" \
  --extern "thiserror=$deps/libthiserror-1292c6e08c73fc30.rlib" \
  --extern "alopex_core=$out/libalopex_core.rlib" \
  "$src/crates/alopex-sql/src/lib.rs" -o "$out/libalopex_sql.rlib"
fi
run "${R[@]}" --edition=2024 --crate-name old_unique_producer -C lto=thin \
  --extern "alopex_core=$out/libalopex_core.rlib" \
  --extern "alopex_sql=$out/libalopex_sql.rlib" \
  --extern "bincode=$deps/libbincode-28e6283f7225ed8c.rlib" \
  --extern "serde_json=$deps/libserde_json-22e25cc3262a4339.rlib" \
  --extern "hex=$deps/libhex-0372880fdee80842.rlib" \
  "${PRODUCER_SOURCE:?candidate-owned standalone producer source}" -o "$out/old-unique-producer"
