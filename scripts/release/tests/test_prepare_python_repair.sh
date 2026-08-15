#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PREPARE="${REPO_ROOT}/scripts/release/prepare-python-repair.sh"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-python-repair.XXXXXX")"
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

run_prepare() {
    (cd "${TEMP_ROOT}/repo" && \
        PATH="${TEMP_ROOT}/bin:${PATH}" GH_TOKEN=test \
        SOURCE_SHA="$1" TARGET_SHA="$2" RELEASE_TAG="$3" \
        bash "${PREPARE}")
}

git init --bare "${TEMP_ROOT}/origin.git" >/dev/null
git init -b main "${TEMP_ROOT}/repo" >/dev/null
git -C "${TEMP_ROOT}/repo" config user.email test@example.invalid
git -C "${TEMP_ROOT}/repo" config user.name test
printf 'release\n' >"${TEMP_ROOT}/repo/file"
git -C "${TEMP_ROOT}/repo" add file
git -C "${TEMP_ROOT}/repo" commit -m release >/dev/null
git -C "${TEMP_ROOT}/repo" remote add origin "${TEMP_ROOT}/origin.git"
git -C "${TEMP_ROOT}/repo" push origin main >/dev/null
SOURCE_SHA="$(git -C "${TEMP_ROOT}/repo" rev-parse HEAD)"
git -C "${TEMP_ROOT}/repo" tag -a v0.8.5 "${SOURCE_SHA}" -m 'Release v0.8.5'
git -C "${TEMP_ROOT}/repo" push origin v0.8.5 >/dev/null

mkdir -p "${TEMP_ROOT}/bin"
cat >"${TEMP_ROOT}/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1 $2 $3" == "release view v0.8.5" ]]
printf '{"tagName":"v0.8.5","isDraft":false,"isPrerelease":false}\n'
EOF
chmod +x "${TEMP_ROOT}/bin/gh"

assert_success run_prepare "${SOURCE_SHA}" "${SOURCE_SHA}" alopex-py-v0.8.5
[[ "$(git -C "${TEMP_ROOT}/repo" cat-file -t refs/tags/alopex-py-v0.8.5)" == tag ]]
[[ "$(git -C "${TEMP_ROOT}/repo" rev-parse 'alopex-py-v0.8.5^{commit}')" == "${SOURCE_SHA}" ]]

# A repeated repair is idempotent and verifies the existing annotated tag.
assert_success run_prepare "${SOURCE_SHA}" "${SOURCE_SHA}" alopex-py-v0.8.5

assert_fail run_prepare "${SOURCE_SHA}" "$(printf '0%.0s' {1..40})" alopex-py-v0.8.5
assert_fail run_prepare "${SOURCE_SHA}" "${SOURCE_SHA}" invalid-tag

# An existing tag on any other commit is rejected rather than moved.
printf 'later\n' >>"${TEMP_ROOT}/repo/file"
git -C "${TEMP_ROOT}/repo" commit -am later >/dev/null
WRONG_SHA="$(git -C "${TEMP_ROOT}/repo" rev-parse HEAD)"
git -C "${TEMP_ROOT}/repo" tag -f -a alopex-py-v0.8.5 "${WRONG_SHA}" -m wrong >/dev/null
git -C "${TEMP_ROOT}/repo" push --force origin alopex-py-v0.8.5 >/dev/null
assert_fail run_prepare "${SOURCE_SHA}" "${SOURCE_SHA}" alopex-py-v0.8.5

echo "prepare-python-repair checks passed"
