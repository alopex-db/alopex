#!/usr/bin/env bash
# v06_gate.sh - v0.6 release gate aggregator

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NIM_SQL_PARSER_DIR="${NIM_SQL_PARSER_DIR:-crates/alopex-sql/nim-sql-parser}"
if [[ "${NIM_SQL_PARSER_DIR}" = /* ]]; then
    NIM_SQL_PARSER_ABS="${NIM_SQL_PARSER_DIR}"
else
    NIM_SQL_PARSER_ABS="${PROJECT_ROOT}/${NIM_SQL_PARSER_DIR}"
fi
KNOWN_P0_P1_FILE="${PROJECT_ROOT}/docs-internal/known-issues.md"
V06_PERF_MARGIN_PCT="25"
V06_GATE_VENV_DIR="${V06_GATE_VENV_DIR:-${TMPDIR:-/tmp}/alopex-v06-gate-venv}"
V06_GATE_CACHE_DIR="${V06_GATE_CACHE_DIR:-${TMPDIR:-/tmp}/alopex-v06-gate-cache}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

quote_command() {
    printf "%q " "$@"
}

run_step() {
    local label="$1"
    shift

    log_info "Running: ${label}"
    log_info "Command: $(quote_command "$@")"
    if "$@"; then
        log_success "${label}"
    else
        local status=$?
        log_error "${label} failed with exit code ${status}"
        log_error "Failed command: $(quote_command "$@")"
        exit "${status}"
    fi
}

run_step_in_dir() {
    local label="$1"
    local dir="$2"
    shift 2

    log_info "Running: ${label}"
    log_info "Directory: ${dir}"
    log_info "Command: $(quote_command "$@")"
    if (cd "${dir}" && "$@"); then
        log_success "${label}"
    else
        local status=$?
        log_error "${label} failed with exit code ${status}"
        log_error "Failed command in ${dir}: $(quote_command "$@")"
        exit "${status}"
    fi
}

require_command() {
    local command_name="$1"
    if ! command -v "${command_name}" > /dev/null 2>&1; then
        log_error "Required command not found: ${command_name}"
        exit 127
    fi
}

check_known_p0_p1_failures() {
    # Future P0/P1 tracking can populate docs-internal/known-issues.md.
    # For v0.6 Task19, no P0/P1 known-failure list exists; absent or empty means pass.
    if [[ ! -s "${KNOWN_P0_P1_FILE}" ]]; then
        log_info "No non-empty P0/P1 known-failure list found at ${KNOWN_P0_P1_FILE}"
        log_success "Known P0/P1 failure gate"
        return
    fi

    log_error "Known P0/P1 failure list is non-empty: ${KNOWN_P0_P1_FILE}"
    log_error "Resolve or explicitly re-triage listed P0/P1 failures before v0.6 release."
    exit 1
}

configure_environment() {
    export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
    export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
    export STRESS_BASELINE_MARGIN_PCT="0.25"
    export UV_CACHE_DIR="${V06_GATE_CACHE_DIR}/uv"
    export PIP_CACHE_DIR="${V06_GATE_CACHE_DIR}/pip"
    export XDG_CACHE_HOME="${V06_GATE_CACHE_DIR}/xdg"
    export NIM_SQL_PARSER_LIB_DIR="${NIM_SQL_PARSER_ABS}"
    export ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD=1
    export LD_LIBRARY_PATH="${NIM_SQL_PARSER_ABS}:${LD_LIBRARY_PATH:-}"
    export DYLD_LIBRARY_PATH="${NIM_SQL_PARSER_ABS}:${DYLD_LIBRARY_PATH:-}"
    export PATH="${NIM_SQL_PARSER_ABS}:${PATH}"

    log_info "Project root: ${PROJECT_ROOT}"
    log_info "Nim SQL parser dir: ${NIM_SQL_PARSER_ABS}"
    log_info "v0.6 representative bench margin: <=${V06_PERF_MARGIN_PCT}%"
    log_info "STRESS_BASELINE_MARGIN_PCT explicitly set to ${STRESS_BASELINE_MARGIN_PCT}"
}

configure_python_environment() {
    require_command python

    if [[ ! -x "${V06_GATE_VENV_DIR}/bin/python" ]]; then
        log_info "Creating Python venv: ${V06_GATE_VENV_DIR}"
        python -m venv --system-site-packages "${V06_GATE_VENV_DIR}"
    fi

    export VIRTUAL_ENV="${V06_GATE_VENV_DIR}"
    export PATH="${VIRTUAL_ENV}/bin:${PATH}"
    export PYO3_PYTHON="${VIRTUAL_ENV}/bin/python"
    export PYTHON_SYS_EXECUTABLE="${VIRTUAL_ENV}/bin/python"
    local python_lib_dir
    python_lib_dir="$("${PYTHON_SYS_EXECUTABLE}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
    if [[ -n "${python_lib_dir}" ]]; then
        export LD_LIBRARY_PATH="${python_lib_dir}:${LD_LIBRARY_PATH:-}"
        export DYLD_LIBRARY_PATH="${python_lib_dir}:${DYLD_LIBRARY_PATH:-}"
    fi

    log_info "Python venv: ${VIRTUAL_ENV}"
    log_info "Python executable: ${PYTHON_SYS_EXECUTABLE}"
}

main() {
    cd "${PROJECT_ROOT}"

    log_info "Alopex v0.6 release gate"
    configure_environment
    configure_python_environment
    require_command cargo
    require_command maturin

    check_known_p0_p1_failures

    run_step "Embedded: open compatibility recovery" \
        cargo test -p alopex-embedded --test open_compat_recovery
    run_step "Core/Txn: snapshot isolation v0.6" \
        cargo test -p alopex-core --test txn_snapshot_isolation_v06
    run_step "SQL: library tests" \
        cargo test -p alopex-core --lib 'sql::'
    run_step "Server: full server package tests" \
        cargo test -p alopex-server --features lane_ci -- --test-threads=1
    run_step_in_dir "Python: maturin develop release build" \
        "${PROJECT_ROOT}/crates/alopex-py" \
        maturin develop --release
    run_step "Python: binding tests" \
        python -m pytest crates/alopex-py/tests
    run_step "DataFrame: stability bench gate" \
        cargo test -p alopex-core --test dataframe_stability_bench
    run_step "Compatibility: format compatibility tests" \
        cargo test -p alopex-core --tests format_compatibility_test
    run_step "Bench: embedded compaction latency <=25%" \
        cargo test -p alopex-core --features lane_perf --test embedded_compaction_latency

    log_success "Alopex v0.6 release gate passed"
}

main "$@"
