#!/usr/bin/env bash
set -euo pipefail

# This is the internal v0.8 surface gate.  It delegates to the owning
# integration suites instead of duplicating their assertions in alopex-tools.
# A passing Embedded verifier alone must never be reported as full v0.8
# coverage.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target/alopex-tools-v08-surfaces}"
export CARGO_TARGET_DIR

PYTHON_BIN="${ALOPEX_PYTHON:-}"
if [[ -z "${PYTHON_BIN}" && -x /tmp/alopex-v08-python/bin/python ]] \
    && /tmp/alopex-v08-python/bin/python -m pytest --version >/dev/null 2>&1; then
    PYTHON_BIN=/tmp/alopex-v08-python/bin/python
fi
if [[ -z "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="$(command -v python3 || command -v python || true)"
fi
if [[ -z "${PYTHON_BIN}" || ! -x "${PYTHON_BIN}" ]]; then
    echo "[alopex-tools:v0.8] Python interpreter not found; set ALOPEX_PYTHON" >&2
    exit 2
fi
if ! "${PYTHON_BIN}" -m pytest --version >/dev/null 2>&1; then
    echo "[alopex-tools:v0.8] ${PYTHON_BIN} has no pytest; set ALOPEX_PYTHON" >&2
    exit 2
fi

PYTHON_LIB_DIR="$(${PYTHON_BIN} -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
if [[ -n "${PYTHON_LIB_DIR}" && -d "${PYTHON_LIB_DIR}" ]]; then
    export LD_LIBRARY_PATH="${PYTHON_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
fi
if [[ "${OS:-}" == "Windows_NT" ]]; then
    PYTHONPATH_SEPARATOR=";"
    PYTHON_PACKAGE_DIR="$(cygpath -w "${ROOT}/crates/alopex-py/python")"
else
    PYTHONPATH_SEPARATOR=":"
    PYTHON_PACKAGE_DIR="${ROOT}/crates/alopex-py/python"
fi

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

echo "[alopex-tools:v0.8] cluster metadata and SQL catalog internals" >&2
run_cargo_test -p alopex-cluster --lib --tests
run_cargo_test -p alopex-sql --lib --tests

echo "[alopex-tools:v0.8] cluster CLI interface" >&2
run_cargo_test -p alopex-cli --lib commands::server
run_cargo_test -p alopex-cli \
    --test server_test \
    --test profile_test
run_cargo_test -p alopex-cli \
    --test admin_actions_e2e_test \
    --test lifecycle_e2e_test \
    --test sql_multi_statement \
    --test streaming_test
run_cargo_test -p alopex-server --tests

echo "[alopex-tools:v0.8] DataFrame streaming interface" >&2
run_cargo_test -p alopex-dataframe \
    --test streaming_contract \
    --test streaming_differential
run_cargo_test -p alopex-dataframe --tests
run_cargo_test -p alopex-py --lib

echo "[alopex-tools:v0.8] Python local/async/DataFrame interfaces (${PYTHON_BIN})" >&2
PYTHONPATH="${PYTHON_PACKAGE_DIR}${PYTHONPATH:+${PYTHONPATH_SEPARATOR}${PYTHONPATH}}" \
    "${PYTHON_BIN}" -m pytest -q \
    "${ROOT}/crates/alopex-py/tests"

echo "[alopex-tools:v0.8] all requested interfaces verified" >&2
