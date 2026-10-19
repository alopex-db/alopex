#!/usr/bin/env bash
# Build the alopex Python bindings from a source checkout, including the Nim SQL parser,
# without Docker and without the vendored parser assets.
#
# Usage: scripts/repro/v0816-gates/build_local.sh [WORK_DIR]
#   WORK_DIR defaults to <repo>/target/repro-work (ignored by git).
# Result: $WORK_DIR/venv/bin/python can `import alopex` built from this checkout.
#
# Requirements: git, a C compiler (gcc/clang), Rust toolchain (cargo), python3 (>=3.9).
set -euo pipefail
REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
WORK="${1:-$REPO_ROOT/target/repro-work}"
mkdir -p "$WORK"
WORK="$(cd "$WORK" && pwd)"
NIM_TAG=v2.2.10          # required by crates/alopex-sql/nim-sql-parser/nim_sql_parser.nimble
NPEG_TAG=1.3.0
MSGPACK_TAG=v0.4.4

# 1. Nim compiler from source (nim-lang.org downloads are not required).
if [[ ! -x "$WORK/nim/bin/nim" ]]; then
  git clone -q --depth 1 -b "$NIM_TAG" https://github.com/nim-lang/Nim.git "$WORK/nim"
  (cd "$WORK/nim" && sh build_all.sh)
fi
"$WORK/nim/bin/nim" --version | head -1

# 2. Nim dependencies pinned by the parser's nimble file.
[[ -d "$WORK/deps/npeg" ]] || git clone -q --depth 1 -b "$NPEG_TAG" https://github.com/zevv/npeg.git "$WORK/deps/npeg"
[[ -d "$WORK/deps/msgpack4nim" ]] || git clone -q --depth 1 -b "$MSGPACK_TAG" https://github.com/jangko/msgpack4nim.git "$WORK/deps/msgpack4nim"

# 3. Parser shared + static libraries with the sidecars the Rust build script expects.
PARSER_SRC="$REPO_ROOT/crates/alopex-sql/nim-sql-parser"
OUT="$WORK/parser"
mkdir -p "$OUT"
for app in lib staticlib; do
  ext=$([[ $app == lib ]] && echo so || echo a)
  "$WORK/nim/bin/nim" c -d:release --app:$app --mm:orc --opt:speed --passC:-fPIC --hints:off \
    --nimcache:"$WORK/nimcache-$app" \
    --path:"$WORK/deps/npeg/src" --path:"$WORK/deps/msgpack4nim/src" \
    -o:"$OUT/libalopex_sql_parser.$ext" "$PARSER_SRC/src/alopex_sql_parser.nim"
done
cp "$PARSER_SRC/PARSER_CONTRACT_VERSION" "$OUT/CONTRACT_VERSION"
(cd "$OUT" && sha256sum libalopex_sql_parser.so libalopex_sql_parser.a > SHA256SUMS)
echo "parser contract: $(cat "$OUT/CONTRACT_VERSION")"

# 4. Python bindings (release build) into an isolated venv.
python3 -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q maturin numpy pyarrow
export NIM_SQL_PARSER_LIB_DIR="$OUT" ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD=1 VIRTUAL_ENV="$WORK/venv"
export PATH="$WORK/venv/bin:$PATH"
(cd "$REPO_ROOT/crates/alopex-py" && maturin develop --release)
"$WORK/venv/bin/python" -c "import alopex; print('alopex import OK')"
echo "Python: $WORK/venv/bin/python"
