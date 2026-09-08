#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PREPARE="${REPO_ROOT}/scripts/release/prepare-python-release.sh"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-python-release.XXXXXX")"
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
        PATH="${TEMP_ROOT}/bin:${PATH}" GH_TOKEN=test CORE_RUN_ID="$1" \
        GITHUB_OUTPUT="${TEMP_ROOT}/output" \
        bash "${PREPARE}")
}

git init --bare "${TEMP_ROOT}/origin.git" >/dev/null
git init -b main "${TEMP_ROOT}/repo" >/dev/null
git -C "${TEMP_ROOT}/repo" config user.email test@example.invalid
git -C "${TEMP_ROOT}/repo" config user.name test
printf '[workspace]\n[workspace.package]\nversion = "0.8.5"\n' >"${TEMP_ROOT}/repo/Cargo.toml"
printf 'release\n' >"${TEMP_ROOT}/repo/file"
git -C "${TEMP_ROOT}/repo" add Cargo.toml file
git -C "${TEMP_ROOT}/repo" commit -m release >/dev/null
git -C "${TEMP_ROOT}/repo" remote add origin "${TEMP_ROOT}/origin.git"
git -C "${TEMP_ROOT}/repo" push origin main >/dev/null
CORE_SHA="$(git -C "${TEMP_ROOT}/repo" rev-parse HEAD)"
git -C "${TEMP_ROOT}/repo" tag -a v0.8.5 "${CORE_SHA}" -m 'Release v0.8.5'
git -C "${TEMP_ROOT}/repo" push origin v0.8.5 >/dev/null
printf 'python release tooling\n' >>"${TEMP_ROOT}/repo/file"
git -C "${TEMP_ROOT}/repo" commit -am 'prepare python release' >/dev/null
git -C "${TEMP_ROOT}/repo" push origin main >/dev/null
CANDIDATE_SHA="$(git -C "${TEMP_ROOT}/repo" rev-parse HEAD)"

mkdir -p "${TEMP_ROOT}/bin"
cat >"${TEMP_ROOT}/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1 $2 $3" == "release view v0.8.5" ]]; then
  printf '{"tagName":"v0.8.5","isDraft":false,"isPrerelease":false}\n'
elif [[ "$1 $2 $3" == "run list --workflow" ]]; then
  printf 'true\n'
else
  exit 1
fi
EOF
chmod +x "${TEMP_ROOT}/bin/gh"

assert_success run_prepare 1001
[[ "$(git -C "${TEMP_ROOT}/repo" cat-file -t refs/tags/alopex-py-v0.8.5)" == tag ]]
[[ "$(git -C "${TEMP_ROOT}/repo" rev-parse 'alopex-py-v0.8.5^{commit}')" == "${CANDIDATE_SHA}" ]]

# A normal tag operation is single-use; retries must not silently publish again.
assert_fail run_prepare 1001

assert_fail run_prepare invalid

echo "prepare-python-release checks passed"
