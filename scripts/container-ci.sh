#!/usr/bin/env bash
# container-ci.sh - Run GitHub Actions workflows locally using act inside a container
#
# Usage:
#   ./scripts/container-ci.sh                    # List available workflows
#   ./scripts/container-ci.sh alopex-py          # Run alopex-py workflow
#   ./scripts/container-ci.sh alopex-py test     # Run specific job
#   ./scripts/container-ci.sh --custom alopex-py # Use pre-built custom runner image
#
# Requirements:
#   - Docker daemon (act runs inside a container and uses the host Docker socket)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARTIFACTS_DIR="${PROJECT_ROOT}/.act-artifacts"
ACT_IMAGE="${ACT_IMAGE:-ghcr.io/catthehacker/act:latest}"
ACT_WORKDIR="/workspace"

log_info() { echo "[INFO] $1"; }
log_warn() { echo "[WARN] $1"; }
log_error() { echo "[ERROR] $1"; }

check_prerequisites() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not reachable (check permissions or daemon status)"
        exit 1
    fi
}

list_workflows() {
    log_info "Available workflows:"
    echo ""
    for wf in "${PROJECT_ROOT}/.github/workflows"/*.yml; do
        name=$(basename "$wf" .yml)
        echo "  - ${name}"
    done
    echo ""
    log_info "Usage: $0 <workflow-name> [job-name]"
}

run_act() {
    local workflow="$1"
    local job="${2:-}"
    local workflow_file="${PROJECT_ROOT}/.github/workflows/${workflow}.yml"

    if [[ ! -f "${workflow_file}" ]]; then
        log_error "Workflow not found: ${workflow_file}"
        list_workflows
        exit 1
    fi

    mkdir -p "${ARTIFACTS_DIR}"

    local act_args=(
        "-W" "${ACT_WORKDIR}/.github/workflows/${workflow}.yml"
        "--platform" "ubuntu-latest=catthehacker/ubuntu:act-latest"
        "--platform" "macos-latest=catthehacker/ubuntu:act-latest"
        "--platform" "windows-latest=catthehacker/ubuntu:act-latest"
        "--artifact-server-path" "${ACT_WORKDIR}/.act-artifacts"
        "--bind"
        "--reuse"
    )

    if [[ -n "${job}" ]]; then
        act_args+=("-j" "${job}")
        log_info "Running job: ${job}"
    fi

    log_info "Running workflow: ${workflow}"
    docker run --rm \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v "${PROJECT_ROOT}:${ACT_WORKDIR}" \
        -w "${ACT_WORKDIR}" \
        "${ACT_IMAGE}" \
        "${act_args[@]}"
}

run_act_custom() {
    local workflow="$1"
    local job="${2:-}"
    local workflow_file="${PROJECT_ROOT}/.github/workflows/${workflow}.yml"

    if [[ ! -f "${workflow_file}" ]]; then
        log_error "Workflow not found: ${workflow_file}"
        list_workflows
        exit 1
    fi

    mkdir -p "${ARTIFACTS_DIR}"

    local act_args=(
        "-W" "${ACT_WORKDIR}/.github/workflows/${workflow}.yml"
        "--platform" "ubuntu-latest=alopex-py-ci:latest"
        "--platform" "macos-latest=alopex-py-ci:latest"
        "--platform" "windows-latest=alopex-py-ci:latest"
        "--artifact-server-path" "${ACT_WORKDIR}/.act-artifacts"
        "--bind"
        "--reuse"
        "--pull=false"
    )

    if [[ -n "${job}" ]]; then
        act_args+=("-j" "${job}")
        log_info "Running job: ${job}"
    fi

    if ! docker image inspect alopex-py-ci:latest &> /dev/null; then
        log_warn "Custom image not found. Build it with: ./scripts/local-ci.sh --build-image"
        exit 1
    fi

    log_info "Running workflow with custom image: ${workflow}"
    docker run --rm \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v "${PROJECT_ROOT}:${ACT_WORKDIR}" \
        -w "${ACT_WORKDIR}" \
        "${ACT_IMAGE}" \
        "${act_args[@]}"
}

show_help() {
    cat << EOF
container-ci.sh - Run GitHub Actions workflows locally using act (containerized)

Usage:
    $0                           List available workflows
    $0 <workflow> [job]          Run workflow (optionally specific job)
    $0 --custom <workflow> [job] Run with pre-built custom runner image
    $0 --help                    Show this help

Examples:
    $0 alopex-py
    $0 alopex-py rust-check
    $0 --custom alopex-py test

Notes:
    - Uses Docker image: ${ACT_IMAGE}
    - Only ubuntu-latest jobs are supported (macOS/Windows are skipped)
EOF
}

main() {
    check_prerequisites

    case "${1:-}" in
        "")
            list_workflows
            ;;
        --help|-h)
            show_help
            ;;
        --custom)
            shift
            if [[ $# -lt 1 ]]; then
                log_error "Workflow name required"
                exit 1
            fi
            run_act_custom "$@"
            ;;
        *)
            run_act "$@"
            ;;
    esac
}

main "$@"
