#!/usr/bin/env bash
set -euo pipefail

# This is the internal v0.8 surface gate.  It delegates to the owning
# integration suites instead of duplicating their assertions in alopex-tools.
# A passing Embedded verifier alone must never be reported as full v0.8
# coverage.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target/alopex-tools-v08-surfaces}"
export CARGO_TARGET_DIR

run_cargo_test() {
    echo "[alopex-tools:v0.8] cargo test $*" >&2
    cargo test --manifest-path "${ROOT}/Cargo.toml" --features lane_ci \
        --locked --offline "$@"
}

echo "[alopex-tools:v0.8] distributed-read and cluster transport" >&2
run_cargo_test -p alopex-server \
    --test distributed_read_http \
    --test http_sql_e2e \
    --test grpc_test

echo "[alopex-tools:v0.8] cluster CLI interface" >&2
run_cargo_test -p alopex-cli --lib commands::server
run_cargo_test -p alopex-cli \
    --test server_test \
    --test profile_test

echo "[alopex-tools:v0.8] DataFrame streaming interface" >&2
run_cargo_test -p alopex-dataframe \
    --test streaming_contract \
    --test streaming_differential

PYTHON_BIN="${ALOPEX_PYTHON:-}"
if [[ -z "${PYTHON_BIN}" && -x /tmp/alopex-v08-python/bin/python ]] \
    && /tmp/alopex-v08-python/bin/python -m pytest --version >/dev/null 2>&1; then
    PYTHON_BIN=/tmp/alopex-v08-python/bin/python
fi
if [[ -z "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON_BIN}" || ! -x "${PYTHON_BIN}" ]]; then
    echo "[alopex-tools:v0.8] Python interpreter not found; set ALOPEX_PYTHON" >&2
    exit 2
fi
if ! "${PYTHON_BIN}" -m pytest --version >/dev/null 2>&1; then
    echo "[alopex-tools:v0.8] ${PYTHON_BIN} has no pytest; set ALOPEX_PYTHON" >&2
    exit 2
fi

echo "[alopex-tools:v0.8] Python local/async/DataFrame interfaces (${PYTHON_BIN})" >&2
PYTHONPATH="${ROOT}/crates/alopex-py/python${PYTHONPATH:+:${PYTHONPATH}}" \
    "${PYTHON_BIN}" -m pytest -q \
    "${ROOT}/crates/alopex-py/tests/test_asyncio.py" \
    "${ROOT}/crates/alopex-py/tests/test_dataframe_p3.py" \
    "${ROOT}/crates/alopex-py/tests/test_surface_consistency.py"

echo "[alopex-tools:v0.8] all requested interfaces verified" >&2
