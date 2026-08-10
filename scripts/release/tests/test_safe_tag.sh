#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SAFE_TAG="${REPO_ROOT}/scripts/release/safe-tag.sh"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-safe-tag.XXXXXX")"
trap 'rm -rf "${TEMP_ROOT}"' EXIT

assert_fail() {
    if "$@" >"${TEMP_ROOT}/out" 2>&1; then
        echo "expected failure: $*" >&2
        cat "${TEMP_ROOT}/out" >&2
        exit 1
    fi
}

assert_success() {
    if ! "$@" >"${TEMP_ROOT}/out" 2>&1; then
        echo "expected success: $*" >&2
        cat "${TEMP_ROOT}/out" >&2
        exit 1
    fi
}

run_safe_tag() {
    (cd "${TEMP_ROOT}/repo" && SAFE_TAG_REPO_ROOT="${TEMP_ROOT}/repo" \
        bash "${SAFE_TAG}" "$@")
}

git init --bare "${TEMP_ROOT}/origin.git" >/dev/null
git init -b main "${TEMP_ROOT}/repo" >/dev/null
git -C "${TEMP_ROOT}/repo" config user.email test@example.invalid
git -C "${TEMP_ROOT}/repo" config user.name test
printf 'baseline\n' >"${TEMP_ROOT}/repo/file"
git -C "${TEMP_ROOT}/repo" add file
git -C "${TEMP_ROOT}/repo" commit -m baseline >/dev/null
git -C "${TEMP_ROOT}/repo" remote add origin "${TEMP_ROOT}/origin.git"
git -C "${TEMP_ROOT}/repo" push origin main >/dev/null
git -C "${TEMP_ROOT}/repo" fetch origin main >/dev/null
TARGET="$(git -C "${TEMP_ROOT}/repo" rev-parse HEAD)"

assert_success run_safe_tag v0.8.4 "${TARGET}"

assert_fail run_safe_tag v0.8.4
assert_fail run_safe_tag v0.8.4 "$(printf '0%.0s' {1..40})"

git -C "${TEMP_ROOT}/repo" tag v0.8.4
assert_fail run_safe_tag v0.8.4 "${TARGET}"
git -C "${TEMP_ROOT}/repo" tag -d v0.8.4 >/dev/null

git -C "${TEMP_ROOT}/repo" push origin "${TARGET}:refs/tags/v0.8.4" >/dev/null
assert_fail run_safe_tag v0.8.4 "${TARGET}"

echo "safe-tag checks passed"
