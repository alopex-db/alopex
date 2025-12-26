#!/usr/bin/env bash
# local-ci.sh - Run GitHub Actions workflows locally using act
#
# Usage:
#   ./scripts/local-ci.sh                    # List available workflows
#   ./scripts/local-ci.sh alopex-py          # Run alopex-py workflow
#   ./scripts/local-ci.sh alopex-py test     # Run specific job
#   ./scripts/local-ci.sh --build-image      # Build custom Docker image
#   ./scripts/local-ci.sh --clean            # Clean up containers
#
# Requirements:
#   - Docker
#   - act (https://github.com/nektos/act)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ACT_DIR="${PROJECT_ROOT}/.act"
ARTIFACTS_DIR="${PROJECT_ROOT}/.act-artifacts"
ACT_BIN="${HOME}/.local/bin/act"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prerequisites() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi

    if [[ ! -x "${ACT_BIN}" ]]; then
        log_error "act is not installed at ${ACT_BIN}"
        log_info "Install with: curl -sSf https://raw.githubusercontent.com/nektos/act/master/install.sh | bash -s -- -b ~/.local/bin"
        exit 1
    fi
}

# Build custom Docker image for alopex-py CI
build_image() {
    log_info "Building custom CI image: alopex-py-ci"
    docker build \
        -t alopex-py-ci:latest \
        -f "${ACT_DIR}/Dockerfile.alopex-py" \
        "${ACT_DIR}"
    log_success "Image built successfully"
}

# Clean up act containers and artifacts
clean() {
    log_info "Cleaning up act containers..."
    docker ps -a --filter "label=act" -q | xargs -r docker rm -f

    log_info "Cleaning up artifacts directory..."
    rm -rf "${ARTIFACTS_DIR}"

    log_success "Cleanup complete"
}

# List available workflows
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

# Run a specific workflow
run_workflow() {
    local workflow="$1"
    local job="${2:-}"
    local workflow_file="${PROJECT_ROOT}/.github/workflows/${workflow}.yml"

    if [[ ! -f "${workflow_file}" ]]; then
        log_error "Workflow not found: ${workflow_file}"
        list_workflows
        exit 1
    fi

    mkdir -p "${ARTIFACTS_DIR}"

    log_info "Running workflow: ${workflow}"

    local act_args=(
        "-W" "${workflow_file}"
        "--platform" "ubuntu-latest=catthehacker/ubuntu:act-latest"
        "--platform" "macos-latest=catthehacker/ubuntu:act-latest"
        "--platform" "windows-latest=catthehacker/ubuntu:act-latest"
        "--artifact-server-path" "${ARTIFACTS_DIR}"
        "--bind"
        "--reuse"
    )

    # Add job filter if specified
    if [[ -n "${job}" ]]; then
        act_args+=("-j" "${job}")
        log_info "Running job: ${job}"
    fi

    # Run act
    cd "${PROJECT_ROOT}"
    "${ACT_BIN}" "${act_args[@]}"
}

# Run with custom image (pre-built with Rust/Python)
run_with_custom_image() {
    local workflow="$1"
    local job="${2:-}"
    local workflow_file="${PROJECT_ROOT}/.github/workflows/${workflow}.yml"

    if [[ ! -f "${workflow_file}" ]]; then
        log_error "Workflow not found: ${workflow_file}"
        list_workflows
        exit 1
    fi

    # Check if custom image exists
    if ! docker image inspect alopex-py-ci:latest &> /dev/null; then
        log_warn "Custom image not found. Building..."
        build_image
    fi

    mkdir -p "${ARTIFACTS_DIR}"

    log_info "Running workflow with custom image: ${workflow}"

    local act_args=(
        "-W" "${workflow_file}"
        "--platform" "ubuntu-latest=alopex-py-ci:latest"
        "--platform" "macos-latest=alopex-py-ci:latest"
        "--platform" "windows-latest=alopex-py-ci:latest"
        "--artifact-server-path" "${ARTIFACTS_DIR}"
        "--bind"
        "--reuse"
        "--pull=false"
    )

    if [[ -n "${job}" ]]; then
        act_args+=("-j" "${job}")
        log_info "Running job: ${job}"
    fi

    cd "${PROJECT_ROOT}"
    "${ACT_BIN}" "${act_args[@]}"
}

# Show help
show_help() {
    cat << EOF
local-ci.sh - Run GitHub Actions workflows locally using act

Usage:
    $0                           List available workflows
    $0 <workflow> [job]          Run workflow (optionally specific job)
    $0 --custom <workflow> [job] Run with pre-built custom image (faster)
    $0 --build-image             Build custom Docker image
    $0 --clean                   Clean up containers and artifacts
    $0 --help                    Show this help

Examples:
    $0 alopex-py                 Run alopex-py workflow
    $0 alopex-py rust-check      Run only rust-check job
    $0 alopex-py test            Run only test job
    $0 --custom alopex-py        Run with pre-built image (faster)
    $0 ci clippy                 Run clippy job from ci workflow

Note:
    - Only ubuntu-latest jobs are supported (macOS/Windows are skipped)
    - First run may take a while to pull Docker images
    - Use --custom for faster runs after building the image
EOF
}

# Main
main() {
    check_prerequisites

    case "${1:-}" in
        "")
            list_workflows
            ;;
        --help|-h)
            show_help
            ;;
        --build-image)
            build_image
            ;;
        --clean)
            clean
            ;;
        --custom)
            shift
            if [[ $# -lt 1 ]]; then
                log_error "Workflow name required"
                exit 1
            fi
            run_with_custom_image "$@"
            ;;
        *)
            run_workflow "$@"
            ;;
    esac
}

main "$@"
