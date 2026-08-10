#!/usr/bin/env bash
# Verify an immutable release tag target without creating, moving, or deleting tags.
#
# Usage: ./scripts/release/safe-tag.sh <tag-name> <reviewed-main-sha>
#
# The caller must provide the exact 40-hex SHA recorded after the reviewed merge.
# This helper only verifies that identity; tag creation remains an explicit,
# separate operation performed by the release workflow.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1" >&2; }

if [[ $# -ne 2 ]]; then
    log_fail "Usage: $0 <tag-name> <reviewed-main-sha>"
    exit 1
fi

TAG_NAME="$1"
TARGET_SHA="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SAFE_TAG_REPO_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
cd "${REPO_ROOT}"

if [[ ! "${TARGET_SHA}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    log_fail "reviewed-main-sha must be a full 40-hex object id"
    exit 1
fi
if [[ ! "${TAG_NAME}" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]]; then
    log_fail "tag name contains unsupported characters: ${TAG_NAME}"
    exit 1
fi

FAILED=0
fail() { log_fail "$1"; FAILED=1; }

log_info "Verifying immutable release target ${TAG_NAME} -> ${TARGET_SHA}"

if [[ -n "$(git status --porcelain)" ]]; then
    fail "working tree is not clean"
else
    log_ok "working tree is clean"
fi

CURRENT_BRANCH="$(git symbolic-ref --quiet --short HEAD || true)"
if [[ "${CURRENT_BRANCH}" != "main" ]]; then
    fail "release target must be resolved from main (currently ${CURRENT_BRANCH:-detached})"
else
    log_ok "on main branch"
fi

LOCAL_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
if [[ "${LOCAL_HEAD}" != "${TARGET_SHA}" ]]; then
    fail "local main ${LOCAL_HEAD:-missing} does not match reviewed target ${TARGET_SHA}"
else
    log_ok "local main matches reviewed target"
fi

if ! git show-ref --verify --quiet refs/remotes/origin/main; then
    fail "origin/main is not available; fetch and resolve it before tagging"
else
    REMOTE_HEAD="$(git rev-parse refs/remotes/origin/main)"
    if [[ "${REMOTE_HEAD}" != "${TARGET_SHA}" ]]; then
        fail "origin/main ${REMOTE_HEAD} does not match reviewed target ${TARGET_SHA}"
    else
        log_ok "origin/main matches reviewed target"
    fi
fi

if git rev-parse --verify --quiet "refs/tags/${TAG_NAME}" >/dev/null; then
    fail "tag ${TAG_NAME} already exists; use repair-forward or a new patch release"
else
    log_ok "tag name is unused locally"
fi

REMOTE_TAG="$(git ls-remote --tags origin "refs/tags/${TAG_NAME}" "refs/tags/${TAG_NAME}^{}" 2>/dev/null || true)"
if [[ -n "${REMOTE_TAG}" ]]; then
    fail "tag ${TAG_NAME} already exists on origin; use repair-forward or a new patch release"
else
    log_ok "tag name is unused on origin"
fi

if [[ "${FAILED}" -ne 0 ]]; then
    log_fail "immutable target checks failed; no tag operation was performed"
    exit 1
fi

log_ok "all immutable target checks passed"
echo "create the tag only from reviewed target ${TARGET_SHA}: git tag -a ${TAG_NAME} ${TARGET_SHA}"
