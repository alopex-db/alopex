#!/usr/bin/env bash
# v09_gate.sh - v0.9.0 target-version Phase 4 and common release gate.
#
# This script verifies a source candidate only.  It has no tag, push, publish,
# release, deployment, or notification command path.  Run it through
# verify-release/run.sh --v09-candidate-gate so the candidate source and
# approved spec workflow are mounted read-only in the Docker verifier.

set -euo pipefail

readonly TARGET_VERSION="0.9.0"
readonly TARGET_PHASE="4"
readonly CHIRPS_VERSION="0.5.2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
MANIFEST=""
GATE_TARGET_DIR="${V09_GATE_TARGET_DIR:-}"
GATE_PYTHON_VENV="${V09_GATE_PYTHON_VENV:-}"
GATE_NIM_DIR="${V09_GATE_NIM_DIR:-}"
CANDIDATE_SHA="${V09_CANDIDATE_SHA:-}"
declare -a BLOCKERS=()

usage() {
    cat <<'EOF'
Usage: scripts/release/v09_gate.sh --phase 4 --manifest ABSOLUTE_PATH

Runs the v0.9.0 Phase 4 exact-register verifier, every Phase 4 fixture, the
complete product-workspace suite, Chirps prerequisite checks, and the
documentation/CI/artifact-identity checks.  The manifest and all generated
artifacts must be outside the candidate source tree.  A nonzero exit means the
candidate is not release-ready; this command never performs a release action.
EOF
}

fail_usage() {
    echo "v0.9 gate: $*" >&2
    usage >&2
    exit 64
}

log_info() { printf '[v0.9 gate] %s\n' "$*"; }
log_error() { printf '[v0.9 gate] ERROR: %s\n' "$*" >&2; }

quote_command() {
    printf '%q ' "$@"
}

run_step() {
    local label="$1"
    shift
    log_info "${label}"
    log_info "command: $(quote_command "$@")"
    "$@"
}

add_blocker() {
    BLOCKERS+=("$1")
    log_error "$1"
}

path_is_within_candidate() {
    local path="$1"
    case "${path}" in
        "${PROJECT_ROOT}"|"${PROJECT_ROOT}"/*) return 0 ;;
        *) return 1 ;;
    esac
}

find_specs_root() {
    if [[ -n "${V09_SPECS_DIR:-}" ]]; then
        printf '%s\n' "${V09_SPECS_DIR}"
        return
    fi

    local candidate="${PROJECT_ROOT}"
    while [[ "${candidate}" != "/" ]]; do
        if [[ -d "${candidate}/.spec-workflow" ]]; then
            printf '%s\n' "${candidate}/.spec-workflow"
            return
        fi
        candidate="$(dirname "${candidate}")"
    done
    return 1
}

require_completed_tasks() {
    local phase_name="$1"
    local tasks_file="$2"
    if [[ ! -f "${tasks_file}" ]]; then
        add_blocker "${phase_name}: approved tasks document is missing (${tasks_file})"
        return
    fi
    if grep -Eq '^-[[:space:]]+\[( |-)\][[:space:]]+[0-9]+\.' "${tasks_file}"; then
        add_blocker "${phase_name}: unfinished task exists in approved task register"
    fi
}

require_current_approved_documents() {
    local specs_root="$1"
    local phase_name="$2"
    local phase_dir="$3"
    local approval_root="${specs_root}/approvals/${phase_name}/.snapshots"
    if [[ ! -d "${approval_root}" ]]; then
        add_blocker "${phase_name}: approval snapshot root is missing (${approval_root})"
        return
    fi

    if ! python3 - "${phase_dir}" "${approval_root}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

phase_dir = Path(sys.argv[1])
approval_root = Path(sys.argv[2])
missing = []
for document_name in ("requirements.md", "design.md", "tasks.md"):
    source = phase_dir / document_name
    expected_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    snapshots = approval_root / document_name
    approved_current = False
    for snapshot_path in sorted(snapshots.glob("snapshot-*.json")):
        try:
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        content = snapshot.get("content")
        if (
            snapshot.get("status") == "approved"
            and isinstance(snapshot.get("approvalId"), str)
            and snapshot["approvalId"]
            and isinstance(content, str)
            and hashlib.sha256(content.encode("utf-8")).hexdigest() == expected_hash
        ):
            approved_current = True
            break
    if not approved_current:
        missing.append(document_name)

if missing:
    raise SystemExit("missing current approved snapshot: " + ", ".join(missing))
PY
    then
        add_blocker "${phase_name}: requirements/design/tasks lack an approved snapshot bound to the current document content"
    fi
}

check_phase_dependency_evidence() {
    local specs_root
    if ! specs_root="$(find_specs_root)"; then
        add_blocker "Phase 1-3: approved spec-workflow checkout is unavailable"
        return
    fi
    if [[ ! -d "${specs_root}" ]]; then
        add_blocker "Phase 1-3: approved spec-workflow path is invalid (${specs_root})"
        return
    fi

    local phase1="${specs_root}/specs/alopex-v0.9.0-phase-1-multi-raft-range"
    local phase2="${specs_root}/specs/alopex-v0.9.0-phase-2-crdt-counter-set"
    local phase3="${specs_root}/specs/alopex-v0.9.0-phase-3-durable-changefeed"
    local phase4="${specs_root}/specs/alopex-v0.9.0-phase-4-distributed-transactions"
    local phase
    for phase in "${phase1}" "${phase2}" "${phase3}" "${phase4}"; do
        if [[ ! -f "${phase}/requirements.md" || ! -f "${phase}/design.md" ]]; then
            add_blocker "$(basename "${phase}"): approved requirements/design evidence is missing"
        fi
        require_current_approved_documents "${specs_root}" "$(basename "${phase}")" "${phase}"
    done
    require_completed_tasks "Phase 1" "${phase1}/tasks.md"
    require_completed_tasks "Phase 2" "${phase2}/tasks.md"
    require_completed_tasks "Phase 3" "${phase3}/tasks.md"

    if [[ ! -f "${phase1}/Implementation Logs/phase1-verification.md" ]]; then
        add_blocker "Phase 1: independent verification evidence is missing"
    fi
    if [[ ! -f "${phase2}/evidence/completion.md" ]]; then
        add_blocker "Phase 2: independent completion evidence is missing"
    fi

    # Phase 3's local handoff is source-controlled so that the candidate's
    # recorded hashes and Durable verdict can be inspected in the same checkout.
    local phase3_completion="${PROJECT_ROOT}/evidence/v09-phase3-completion.json"
    if [[ ! -f "${phase3_completion}" ]]; then
        add_blocker "Phase 3: source-controlled completion handoff is missing"
    elif ! python3 - "${phase3_completion}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
if document.get("target_version") != "v0.9.0":
    raise SystemExit("Phase 3 handoff targets a different version")
if document.get("verdict", {}).get("phase3_feature") != "complete":
    raise SystemExit("Phase 3 feature verdict is not complete")
PY
    then
        add_blocker "Phase 3: Durable changefeed evidence is not complete for this candidate"
    fi
}

exit_if_blocked() {
    if (( ${#BLOCKERS[@]} == 0 )); then
        return 0
    fi
    log_error "v${TARGET_VERSION} candidate is BLOCKED (${#BLOCKERS[@]} blocker(s))"
    printf ' - %s\n' "${BLOCKERS[@]}" >&2
    return 2
}

specs_root_for_verifier() {
    local specs_root
    specs_root="$(find_specs_root)" || {
        log_error "approved spec-workflow checkout is unavailable for verifier"
        return 1
    }
    printf '%s\n' "${specs_root}"
}

stage_nim_parser() {
    if [[ -z "${GATE_NIM_DIR}" ]]; then
        GATE_NIM_DIR="$(mktemp -d "${TMPDIR:-/tmp}/alopex-v09-nim.XXXXXX")"
    fi
    mkdir -p "${GATE_NIM_DIR}"
    cp -a "${PROJECT_ROOT}/crates/alopex-sql/nim-sql-parser/." "${GATE_NIM_DIR}/"
    run_step "Nim SQL parser staged build" bash -c \
        'cd "$1" && nimble lib && nimble test' _ "${GATE_NIM_DIR}"
    export NIM_SQL_PARSER_LIB_DIR="${GATE_NIM_DIR}"
    export LD_LIBRARY_PATH="${GATE_NIM_DIR}:${LD_LIBRARY_PATH:-}"
    export DYLD_LIBRARY_PATH="${GATE_NIM_DIR}:${DYLD_LIBRARY_PATH:-}"
}

check_chirps_prerequisites() {
    local cluster_manifest="${PROJECT_ROOT}/crates/alopex-cluster/Cargo.toml"
    if [[ ! -f "${cluster_manifest}" ]]; then
        add_blocker "I-25: alopex-cluster Chirps manifest is missing"
        return
    fi
    if ! grep -Fq "alopex-chirps = { version = \"${CHIRPS_VERSION}\"" "${cluster_manifest}"; then
        add_blocker "I-25: compatible Chirps ${CHIRPS_VERSION} package/version is not declared"
    fi
    if ! grep -Fq 'authenticated_dispatcher' \
        "${PROJECT_ROOT}/crates/alopex-cluster/tests/changefeed_durable_preflight.rs"; then
        add_blocker "I-25: authenticated Chirps dispatcher evidence is missing"
    fi
}

check_docs_ci_and_artifact_identity() {
    local workflow="${PROJECT_ROOT}/.github/workflows/release.yml"
    local workspace_manifest="${PROJECT_ROOT}/Cargo.toml"
    local version
    version="$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "${workspace_manifest}" | head -n 1)"
    if [[ "${version}" != "${TARGET_VERSION}" ]]; then
        add_blocker "I-26: workspace artifact version is ${version:-unknown}, expected ${TARGET_VERSION}"
    fi
    if [[ ! -f "${PROJECT_ROOT}/docs/release-v0.9-support.md" || \
          ! -f "${PROJECT_ROOT}/docs/upgrade-v0.8.1-to-v0.9.md" ]]; then
        add_blocker "I-26: v0.9 support/upgrade documentation is incomplete"
    fi
    if [[ ! -f "${workflow}" ]]; then
        add_blocker "I-26: release workflow is missing"
        return
    fi
    if ! grep -Fq 'v09-candidate-gate' "${workflow}" || \
       ! grep -Fq 'V09_SPECS_DIR' "${workflow}"; then
        add_blocker "I-26: release workflow does not invoke the v0.9 candidate gate with approved specs"
    fi
    if grep -Eq 'v07_gate\.sh|verify-v08-surfaces\.sh' "${workflow}"; then
        add_blocker "I-26: a legacy v0.7/v0.8 gate is present in the v0.9 release workflow"
    fi
}

run_phase4_fixtures() {
    local specs_root
    specs_root="$(specs_root_for_verifier)"
    run_step "Phase 4 exact-register manifest generation" \
        cargo run --offline --locked --manifest-path crates/alopex-tools/Cargo.toml \
        --bin verify-v09-f4 -- --repo-root "${PROJECT_ROOT}" \
        --specs-root "${specs_root}" \
        --candidate-sha "${CANDIDATE_SHA}" \
        --target-version "${TARGET_VERSION}" --phase "${TARGET_PHASE}" \
        --manifest "${MANIFEST}" --generate
    run_step "Phase 4 exact-register manifest revalidation" \
        cargo run --offline --locked --manifest-path crates/alopex-tools/Cargo.toml \
        --bin verify-v09-f4 -- --repo-root "${PROJECT_ROOT}" \
        --specs-root "${specs_root}" \
        --candidate-sha "${CANDIDATE_SHA}" \
        --target-version "${TARGET_VERSION}" --phase "${TARGET_PHASE}" \
        --manifest "${MANIFEST}"

    run_step "F4 core deterministic transaction fixtures" \
        cargo test --offline --locked -p alopex-core --test transaction_fixtures
    run_step "F4 cluster atomicity fixtures" \
        cargo test --offline --locked -p alopex-cluster --test transaction_atomicity
    run_step "F4 cluster recovery fixtures" \
        cargo test --offline --locked -p alopex-cluster --test transaction_recovery_fixtures
    run_step "F4 SQL transaction classifier register" \
        cargo test --offline --locked -p alopex-sql transaction_classifier
    run_step "F4 SQL scalar and aggregate matrix" \
        cargo test --offline --locked -p alopex-sql --test v09_sql_scalar_aggregate_matrix
    run_step "F4 SQL Nim/FFI/COPY/PRAGMA compatibility" \
        cargo test --offline --locked -p alopex-sql --test v09_nim_ffi_copy_pragma_compat
    run_step "F4 embedded transaction register" \
        cargo test --offline --locked -p alopex-embedded --test v09_embedded_register
    run_step "F4 HTTP transaction outcome and complete route register" \
        cargo test --offline --locked -p alopex-server --test v09_http_transaction_outcomes \
        --test v09_http_surface --test transaction_surface_parity --test transaction_recovery_http
    run_step "F4 gRPC transaction outcome and complete RPC register" \
        cargo test --offline --locked -p alopex-server --test v09_grpc_surface
    run_step "F4 CLI transaction and inherited compatibility register" \
        cargo test --offline --locked -p alopex-cli --test v09_cli_transaction_surface \
        --test transaction_compatibility
}

run_product_workspace_suite() {
    run_step "v0.9 all product crates and feature surfaces" \
        cargo test --offline --locked --workspace --all-features

    if [[ -z "${GATE_PYTHON_VENV}" ]]; then
        GATE_PYTHON_VENV="$(mktemp -d "${TMPDIR:-/tmp}/alopex-v09-python.XXXXXX")"
    fi
    python3 -m venv --system-site-packages "${GATE_PYTHON_VENV}"
    run_step "v0.9 Python extension candidate build" env \
        "VIRTUAL_ENV=${GATE_PYTHON_VENV}" \
        "PATH=${GATE_PYTHON_VENV}/bin:${PATH}" \
        maturin develop --locked -m crates/alopex-py/Cargo.toml
    run_step "v0.9 Python sync/async transaction interfaces" \
        "${GATE_PYTHON_VENV}/bin/python" -m pytest \
        crates/alopex-py/tests/test_v09_transaction_sync.py \
        crates/alopex-py/tests/test_v09_transaction_async.py \
        crates/alopex-py/tests/test_transaction_surface_parity.py \
        crates/alopex-py/tests/test_transaction_compatibility.py
}

cleanup() {
    local status=$?
    if [[ -n "${GATE_TARGET_DIR}" && "${GATE_TARGET_DIR}" == "${TMPDIR:-/tmp}"/* ]]; then
        rm -rf "${GATE_TARGET_DIR}"
    fi
    if [[ -n "${GATE_PYTHON_VENV}" && "${GATE_PYTHON_VENV}" == "${TMPDIR:-/tmp}"/* ]]; then
        rm -rf "${GATE_PYTHON_VENV}"
    fi
    if [[ -n "${GATE_NIM_DIR}" && "${GATE_NIM_DIR}" == "${TMPDIR:-/tmp}"/* ]]; then
        rm -rf "${GATE_NIM_DIR}"
    fi
    exit "${status}"
}

main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --phase)
                [[ $# -ge 2 ]] || fail_usage "--phase needs a value"
                [[ "$2" == "${TARGET_PHASE}" ]] || fail_usage "only Phase ${TARGET_PHASE} is supported"
                shift 2
                ;;
            --manifest)
                [[ $# -ge 2 ]] || fail_usage "--manifest needs a value"
                MANIFEST="$2"
                shift 2
                ;;
            --help|-h) usage; return 0 ;;
            *) fail_usage "unknown option: $1" ;;
        esac
    done
    [[ -n "${MANIFEST}" ]] || fail_usage "--manifest is required"
    [[ "${MANIFEST}" == /* ]] || fail_usage "--manifest must be an absolute path"
    path_is_within_candidate "${MANIFEST}" && fail_usage "--manifest must be outside the candidate source tree"

    [[ -n "${GATE_TARGET_DIR}" ]] || GATE_TARGET_DIR="$(mktemp -d "${TMPDIR:-/tmp}/alopex-v09-target.XXXXXX")"
    [[ "${GATE_TARGET_DIR}" == /* ]] || fail_usage "V09_GATE_TARGET_DIR must be an absolute path"
    path_is_within_candidate "${GATE_TARGET_DIR}" && fail_usage "V09_GATE_TARGET_DIR must be outside the candidate source tree"
    export CARGO_TARGET_DIR="${GATE_TARGET_DIR}"
    trap cleanup EXIT

    cd "${PROJECT_ROOT}"
    if [[ -z "${CANDIDATE_SHA}" ]]; then
        CANDIDATE_SHA="$(git -C "${PROJECT_ROOT}" rev-parse HEAD)" || {
            log_error "V09_CANDIDATE_SHA がなく、candidate Git SHA を取得できません"
            return 64
        }
    fi
    if [[ ! "${CANDIDATE_SHA}" =~ ^[[:xdigit:]]{40}$ ]]; then
        log_error "V09_CANDIDATE_SHA が 40 桁の SHA ではありません"
        return 64
    fi

    check_phase_dependency_evidence
    check_chirps_prerequisites
    check_docs_ci_and_artifact_identity
    exit_if_blocked || return $?
    command -v cargo >/dev/null || { log_error "cargo is required"; return 127; }
    command -v python3 >/dev/null || { log_error "python3 is required"; return 127; }
    command -v nimble >/dev/null || { log_error "nimble is required"; return 127; }
    command -v maturin >/dev/null || { log_error "maturin is required"; return 127; }
    stage_nim_parser
    run_phase4_fixtures
    run_product_workspace_suite

    log_info "v${TARGET_VERSION} candidate gate passed for ${CANDIDATE_SHA}; this is evidence only and does not authorize release actions"
}

main "$@"
