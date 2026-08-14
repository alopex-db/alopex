#!/usr/bin/env bash
# v07_gate.sh - v0.7 release gate aggregator

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NIM_SQL_PARSER_DIR="${NIM_SQL_PARSER_DIR:-crates/alopex-sql/nim-sql-parser}"
if [[ "${NIM_SQL_PARSER_DIR}" = /* ]]; then
    NIM_SQL_PARSER_ABS="${NIM_SQL_PARSER_DIR}"
else
    NIM_SQL_PARSER_ABS="${PROJECT_ROOT}/${NIM_SQL_PARSER_DIR}"
fi
V07_GATE_VENV_DIR="${V07_GATE_VENV_DIR:-${TMPDIR:-/tmp}/alopex-v07-gate-venv}"
V07_GATE_CACHE_DIR="${V07_GATE_CACHE_DIR:-${TMPDIR:-/tmp}/alopex-v07-gate-cache}"
V07_GATE_RUN_V06="${V07_GATE_RUN_V06:-1}"
V07_GATE_RUN_MATURIN="${V07_GATE_RUN_MATURIN:-1}"

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

require_file_contains() {
    local file="$1"
    local pattern="$2"
    local description="$3"
    if ! grep -Eq -- "${pattern}" "${file}"; then
        log_error "Release workflow contract check failed: ${description}"
        log_error "Missing pattern '${pattern}' in ${file}"
        exit 1
    fi
    log_success "Workflow contract: ${description}"
}

configure_environment() {
    export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
    export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
    export UV_CACHE_DIR="${V07_GATE_CACHE_DIR}/uv"
    export PIP_CACHE_DIR="${V07_GATE_CACHE_DIR}/pip"
    export XDG_CACHE_HOME="${V07_GATE_CACHE_DIR}/xdg"
    export NIM_SQL_PARSER_LIB_DIR="${NIM_SQL_PARSER_ABS}"
    export LD_LIBRARY_PATH="${NIM_SQL_PARSER_ABS}:${LD_LIBRARY_PATH:-}"
    export DYLD_LIBRARY_PATH="${NIM_SQL_PARSER_ABS}:${DYLD_LIBRARY_PATH:-}"
    export PATH="${NIM_SQL_PARSER_ABS}:${PATH}"

    export V06_GATE_VENV_DIR="${V06_GATE_VENV_DIR:-${V07_GATE_VENV_DIR}}"
    export V06_GATE_CACHE_DIR="${V06_GATE_CACHE_DIR:-${V07_GATE_CACHE_DIR}/v06}"

    log_info "Project root: ${PROJECT_ROOT}"
    log_info "Nim SQL parser dir: ${NIM_SQL_PARSER_ABS}"
    log_info "Python venv dir: ${V07_GATE_VENV_DIR}"
    log_info "Cache dir: ${V07_GATE_CACHE_DIR}"
    log_info "Branch cleanup record must be captured in release docs before tag verification."
}

configure_python_environment() {
    require_command python

    if [[ ! -x "${V07_GATE_VENV_DIR}/bin/python" ]]; then
        log_info "Creating Python venv: ${V07_GATE_VENV_DIR}"
        python -m venv --system-site-packages "${V07_GATE_VENV_DIR}"
    fi

    export VIRTUAL_ENV="${V07_GATE_VENV_DIR}"
    export PATH="${VIRTUAL_ENV}/bin:${PATH}"
    export PYO3_PYTHON="${VIRTUAL_ENV}/bin/python"
    export PYTHON_SYS_EXECUTABLE="${VIRTUAL_ENV}/bin/python"
    local python_lib_dir
    python_lib_dir="$("${PYTHON_SYS_EXECUTABLE}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
    if [[ -n "${python_lib_dir}" ]]; then
        export LD_LIBRARY_PATH="${python_lib_dir}:${LD_LIBRARY_PATH:-}"
        export DYLD_LIBRARY_PATH="${python_lib_dir}:${DYLD_LIBRARY_PATH:-}"
    fi

    log_info "Python executable: ${PYTHON_SYS_EXECUTABLE}"
}

check_release_workflow_contract() {
    local ci_workflow="${PROJECT_ROOT}/.github/workflows/ci.yml"
    local release_workflow="${PROJECT_ROOT}/.github/workflows/release.yml"
    local py_release_workflow="${PROJECT_ROOT}/.github/workflows/alopex-py-release.yml"

    require_file_contains "${ci_workflow}" "scripts/release/v07_gate\\.sh" \
        "CI invokes the v0.7 release gate"
    require_file_contains "${release_workflow}" "scripts/release/v07_gate\\.sh" \
        "release workflow invokes the v0.7 release gate"
    require_file_contains "${release_workflow}" "tags:[[:space:]]*$" \
        "release workflow declares tag trigger block"
    require_file_contains "${release_workflow}" "v\\*" \
        "release workflow accepts repository release tags"
    require_file_contains "${release_workflow}" "maintenance_base:" \
        "Rust release workflow exposes the historical patch dispatch contract"
    require_file_contains "${release_workflow}" "dispatch-python-release:" \
        "Rust maintenance release dispatches matching Python CI/CD"
    require_file_contains "${release_workflow}" "gh run watch" \
        "Rust maintenance release waits for matching Python CI/CD completion"
    require_file_contains "${release_workflow}" "alopex-linux-x86_64" \
        "release workflow publishes Linux CLI artifact"
    require_file_contains "${release_workflow}" "alopex-macos-x86_64" \
        "release workflow publishes macOS x86_64 CLI artifact"
    require_file_contains "${release_workflow}" "alopex-macos-aarch64" \
        "release workflow publishes macOS aarch64 CLI artifact"
    require_file_contains "${release_workflow}" "alopex-windows-x86_64\\.exe" \
        "release workflow publishes Windows CLI artifact"
    require_file_contains "${release_workflow}" "actions/upload-artifact@v4" \
        "release workflow uploads build artifacts"
    require_file_contains "${release_workflow}" "actions/download-artifact@v4" \
        "release workflow downloads build artifacts before release"
    require_file_contains "${release_workflow}" "softprops/action-gh-release@v2" \
        "release workflow creates GitHub release"
    require_file_contains "${py_release_workflow}" "alopex-py-v\\*" \
        "independent alopex-py release tag trigger is preserved"
    require_file_contains "${py_release_workflow}" "workflow_dispatch:" \
        "Python release workflow accepts maintenance CI/CD dispatch"
    require_file_contains "${py_release_workflow}" "verify_python_vector_api\\.py" \
        "Python release workflow executes the installed-wheel Vector/HNSW smoke test"
    require_file_contains "${py_release_workflow}" "verify-release/run\\.sh" \
        "Python release workflow runs demos against the published packages"
    require_file_contains "${py_release_workflow}" "contents: write" \
        "Python release workflow can publish its temporary report branch"
    require_file_contains "${py_release_workflow}" "Wait for docs main publication" \
        "Python release workflow waits for the public demo report to reach docs main"
    require_file_contains "${PROJECT_ROOT}/scripts/release/verify-release/run.sh" \
        '--push-only' \
        "release verification can push a report branch without creating a cross-repository PR"
    require_file_contains "${PROJECT_ROOT}/scripts/release/verify-release/run.sh" \
        'verify_python_vector_api\.py' \
        "public-package demo report includes the installed-wheel Vector/HNSW verification"
    require_file_contains "${PROJECT_ROOT}/crates/alopex-cluster/Cargo.toml" \
        'alopex-chirps-gossip-swim = \{ version = "=0\.5\.1", optional = true \}' \
        "Chirps gossip dependency is pinned to the published crates.io 0.5.1 package"
    require_file_contains "${PROJECT_ROOT}/rust-toolchain.toml" \
        'channel = "1\.90\.0"' \
        "v0.7 release toolchain is pinned to Rust 1.90.0"
    require_file_contains "${release_workflow}" \
        'toolchain: "1\.90\.0"' \
        "Rust release jobs install Rust 1.90.0"
    require_file_contains "${py_release_workflow}" \
        'rust-toolchain: "1\.90\.0"' \
        "Python release jobs install Rust 1.90.0"
}

verify_release_binary() {
    local target_dir="${CARGO_TARGET_DIR:-${PROJECT_ROOT}/target}"
    if [[ "${target_dir}" != /* ]]; then
        target_dir="${PROJECT_ROOT}/${target_dir}"
    fi
    local binary="${target_dir}/release/alopex"
    if [[ "${OS:-}" == "Windows_NT" ]]; then
        binary="${binary}.exe"
    fi
    if [[ ! -x "${binary}" ]]; then
        log_error "Expected release artifact binary is missing or not executable: ${binary}"
        exit 1
    fi
    log_success "Release artifact smoke binary exists: ${binary}"
}

main() {
    cd "${PROJECT_ROOT}"

    log_info "Alopex v0.7 release gate"
    configure_environment
    configure_python_environment
    require_command cargo

    if [[ "${1:-}" == "--workflow-contract-only" ]]; then
        check_release_workflow_contract
        log_success "Alopex v0.7 workflow contract gate passed"
        return
    fi

    if [[ "${V07_GATE_RUN_MATURIN}" == "1" ]]; then
        require_command maturin
    else
        log_warn "Skipping maturin requirement because V07_GATE_RUN_MATURIN=${V07_GATE_RUN_MATURIN}"
    fi

    check_release_workflow_contract
    run_step "Release process: historical patch tag safety" \
        bash scripts/release/tests/safe-tag-maintenance.sh

    if [[ "${V07_GATE_RUN_V06}" == "1" ]]; then
        run_step "Baseline: v0.6 release gate" \
            bash scripts/release/v06_gate.sh
    else
        log_warn "Skipping v0.6 baseline gate because V07_GATE_RUN_V06=${V07_GATE_RUN_V06}"
    fi

    run_step "Formatting: workspace rustfmt" \
        cargo fmt --all -- --check
    run_step "Lint: workspace clippy" \
        cargo clippy --all-targets --all-features --locked -- -D warnings
    # Cover every product crate under the target version without pulling the
    # pseudo-terminal TUI integration suite into a headless release runner.
    # CLI/server/distributed/DataFrame integration viewpoints run below.
    run_step "Target release: all Rust crate library and binary tests" \
        cargo test --workspace --all-features --lib --bins --locked
    run_step "Cluster: metadata, router, and simulated harness tests" \
        cargo test -p alopex-cluster --all-features --locked
    run_step "Embedded: v0.6 compatibility regression tests" \
        cargo test -p alopex-embedded --test v07_compatibility --all-features --locked
    run_step "Server: v0.6 compatibility regression tests" \
        cargo test -p alopex-server --test v07_compatibility --all-features --locked
    run_step "Server: gRPC API, cluster administration, and transport authentication tests" \
        cargo test -p alopex-server --test grpc_test --all-features --locked
    run_step "Server: cluster routing SQL smoke" \
        cargo test -p alopex-server --test http_sql_e2e --all-features --locked
    run_step "Server: cluster status cross-surface fixture" \
        cargo test -p alopex-server --test cross_surface_consistency --all-features --locked
    run_step "CLI: cluster status fixture projection" \
        cargo test -p alopex-cli --test server_test server_status_json_output_matches_cluster_cross_surface_fixture --all-features --locked
    run_step "DataFrame: P3 namespace integration" \
        cargo test -p alopex-dataframe --test p3_namespaces --all-features --locked
    run_step "Python Rust: alopex-py all-features tests" \
        cargo test -p alopex-py --all-features --locked

    if [[ "${V07_GATE_RUN_MATURIN}" == "1" ]]; then
        run_step_in_dir "Python: maturin develop release build" \
            "${PROJECT_ROOT}/crates/alopex-py" \
            maturin develop --release
        run_step "Python: DataFrame P3 and cluster status tests" \
            python -m pytest \
                crates/alopex-py/tests/test_compatibility_contract.py \
                crates/alopex-py/tests/test_dataframe_p3.py \
                crates/alopex-py/tests/test_surface_consistency.py
        run_step "Python: installed Vector/HNSW API smoke" \
            python scripts/release/verify_python_vector_api.py
    else
        log_warn "Skipping Python maturin/pytest checks because V07_GATE_RUN_MATURIN=${V07_GATE_RUN_MATURIN}"
    fi

    # The debug-profile artifacts from the test steps above and maturin's
    # release build are no longer needed once we get here, and CI runners
    # have limited disk (~14GB). Free the debug target before the release
    # build as defense in depth against "No space left on device" on the
    # final compile (the primary fix was unifying reqwest to eliminate a
    # duplicated hyper/rustls/tokio dependency tree; see CHANGELOG).
    run_step "Free debug build artifacts before release build" \
        cargo clean --profile dev
    run_step "Release artifact: CLI binary smoke build" \
        cargo build --release -p alopex-cli --locked
    verify_release_binary

    log_success "Alopex v0.7 release gate passed"
}

main "$@"
