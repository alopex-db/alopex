#!/usr/bin/env bash
set -euo pipefail

# This is the internal v0.8 surface gate.  It delegates to the owning
# integration suites instead of duplicating their assertions in alopex-tools.
# A passing Embedded verifier alone must never be reported as full v0.8
# coverage.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target/alopex-tools-v08-surfaces}"
export CARGO_TARGET_DIR

NIM_CONTROLLED_FAILURE_OUTCOME="${ALOPEX_NIM_CONTROLLED_FAILURE_OUTCOME:-}"
if [[ "${NIM_CONTROLLED_FAILURE_OUTCOME}" != "failure" ]]; then
    echo "[alopex-tools:v0.8] controlled Nim parser step outcome must be failure, found ${NIM_CONTROLLED_FAILURE_OUTCOME:-unset}" >&2
    exit 2
fi

NIM_CONTROLLED_FAILURE_PROOF="${ALOPEX_NIM_CONTROLLED_FAILURE_PROOF:-}"
if [[ -z "${NIM_CONTROLLED_FAILURE_PROOF}" \
    || ! -f "${NIM_CONTROLLED_FAILURE_PROOF}" \
    || -L "${NIM_CONTROLLED_FAILURE_PROOF}" ]]; then
    echo "[alopex-tools:v0.8] controlled Nim parser proof is missing or not a regular file" >&2
    exit 2
fi
if [[ "$(wc -l <"${NIM_CONTROLLED_FAILURE_PROOF}")" != "4" ]]; then
    echo "[alopex-tools:v0.8] controlled Nim parser proof has an unexpected shape" >&2
    exit 2
fi
for expected_line in \
    "schema=alopex-nim-controlled-failure-v1" \
    "nim=2.2.10" \
    "nimble=0.22.3" \
    "nimble_sha=42ef70c2102a942c46f13eb76872326edd525cec"; do
    if ! grep -Fqx "${expected_line}" "${NIM_CONTROLLED_FAILURE_PROOF}"; then
        echo "[alopex-tools:v0.8] controlled Nim parser proof is missing: ${expected_line}" >&2
        exit 2
    fi
done
rm -f -- "${NIM_CONTROLLED_FAILURE_PROOF}"
if [[ -e "${NIM_CONTROLLED_FAILURE_PROOF}" ]]; then
    echo "[alopex-tools:v0.8] failed to remove controlled Nim parser proof" >&2
    exit 2
fi

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

"${PYTHON_BIN}" "${ROOT}/scripts/release/type_capability_gate.py"

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

METRICS_DIR="${ALOPEX_BUILD_METRICS_DIR:-${ROOT}/artifacts/build-metrics}"
BUILD_OWNER="${ALOPEX_BUILD_OWNER:-current-implementation-${RUNNER_OS:-local}}"

run_cargo_owner() {
    local owner="$1"
    shift
    echo "[alopex-tools:v0.8] Rust owner ${owner}" >&2
    "${PYTHON_BIN}" "${ROOT}/scripts/ci/run_with_metrics.py" \
        --owner "${owner}" \
        --output "${METRICS_DIR}/${owner}.json" \
        --target-dir "${CARGO_TARGET_DIR}" \
        -- cargo test --manifest-path "${ROOT}/Cargo.toml" \
            --locked --offline --timings "$@"
}

case "${ALOPEX_CURRENT_RUST_SUITE:-full}" in
    full)
        run_cargo_owner "${BUILD_OWNER}" \
            --workspace --features lane_ci
        ;;
    *)
        echo "[alopex-tools:v0.8] unknown Rust suite: ${ALOPEX_CURRENT_RUST_SUITE}" >&2
        exit 2
        ;;
esac

echo "[alopex-tools:v0.8] Python local/async/DataFrame interfaces (${PYTHON_BIN})" >&2
PYTHONPATH="${PYTHON_PACKAGE_DIR}${PYTHONPATH:+${PYTHONPATH_SEPARATOR}${PYTHONPATH}}" \
    "${PYTHON_BIN}" -m pytest -q \
    "${ROOT}/crates/alopex-py/tests"

echo "[alopex-tools:v0.8] all requested interfaces verified" >&2
