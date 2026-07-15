#!/usr/bin/env bash
# safe-tag.sh - リリースタグ作成前の必須検証
#
# v0.7.1 のリリースで、ローカル main を fetch はしたが checkout/pull
# し忘れたまま `git tag` を実行し、PR マージ前の古いコミットにタグを
# 打ってしまう事故が2回発生した(うち1回はさらに reqwest 依存の重複を
# 見落としたまま release ビルドがディスク枯渇で失敗)。
#
# このスクリプトは実際にタグを打つ前に、安全な状態であることを
# 機械的に確認する。確認が全て通った場合のみタグ作成コマンドを表示する
# (このスクリプト自身はタグを作成・push しない — 実行は利用者の意思で)。
#
# Usage: ./scripts/release/safe-tag.sh <tag-name>
#   例: ./scripts/release/safe-tag.sh v0.7.2
#       ./scripts/release/safe-tag.sh alopex-py-v0.7.2

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

if [ $# -ne 1 ]; then
    log_fail "Usage: $0 <tag-name>"
    exit 1
fi

TAG_NAME="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

FAILED=0

log_info "Verifying release tag safety for: ${TAG_NAME}"
echo ""

# 1. ワーキングツリーがクリーンか
if [ -n "$(git status --porcelain)" ]; then
    log_fail "Working tree is not clean. Commit or stash changes first."
    git status --short
    FAILED=1
else
    log_ok "Working tree is clean"
fi

# 2. 現在のブランチが main か(タグは main 上のコミットに打つのが前提)
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "${CURRENT_BRANCH}" != "main" ]; then
    log_fail "Not on main branch (currently on: ${CURRENT_BRANCH})"
    echo "  Run: git checkout main"
    FAILED=1
else
    log_ok "On main branch"
fi

# 3. リモートを fetch してから、ローカル main が origin/main と一致するか
#    (fetch だけでは不十分 — checkout/pull まで確認する。これが v0.7.1 で
#    2回事故った直接の原因)
log_info "Fetching origin/main..."
git fetch origin main --quiet
LOCAL_HEAD="$(git rev-parse HEAD)"
REMOTE_HEAD="$(git rev-parse origin/main)"
if [ "${LOCAL_HEAD}" != "${REMOTE_HEAD}" ]; then
    log_fail "Local main (${LOCAL_HEAD:0:7}) != origin/main (${REMOTE_HEAD:0:7})"
    echo "  Run: git pull origin main"
    FAILED=1
else
    log_ok "Local main matches origin/main (${LOCAL_HEAD:0:7})"
fi

# 4. タグ名から期待バージョンを抽出し、Cargo.toml と一致するか確認
#    - v* タグ: workspace.package.version と一致
#    - alopex-py-v* タグ: crates/alopex-py/Cargo.toml の version と一致
EXPECTED_VERSION=""
if [[ "${TAG_NAME}" =~ ^alopex-py-v(.+)$ ]]; then
    EXPECTED_VERSION="${BASH_REMATCH[1]}"
    ACTUAL_VERSION="$(grep -m1 '^version' crates/alopex-py/Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
    VERSION_SOURCE="crates/alopex-py/Cargo.toml"
elif [[ "${TAG_NAME}" =~ ^v(.+)$ ]]; then
    EXPECTED_VERSION="${BASH_REMATCH[1]}"
    ACTUAL_VERSION="$(grep -m1 '^version' Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
    VERSION_SOURCE="Cargo.toml (workspace.package.version)"
else
    log_fail "Tag name does not match expected pattern (v*, alopex-py-v*): ${TAG_NAME}"
    FAILED=1
fi

if [ -n "${EXPECTED_VERSION}" ]; then
    if [ "${EXPECTED_VERSION}" != "${ACTUAL_VERSION}" ]; then
        log_fail "Tag implies version ${EXPECTED_VERSION}, but ${VERSION_SOURCE} has ${ACTUAL_VERSION}"
        FAILED=1
    else
        log_ok "Version matches: ${ACTUAL_VERSION} (${VERSION_SOURCE})"
    fi
fi

# 5. v* タグ(Rust workspace)の場合、alopex-py も同じバージョンに揃って
#    いるか警告する。揃っていて、対応する alopex-py-v* タグがまだ無い
#    場合は、PyPI リリースを忘れていないか注意喚起する(v0.7.1 で実際に
#    忘れた)。
if [[ "${TAG_NAME}" =~ ^v(.+)$ ]] && [[ ! "${TAG_NAME}" =~ ^alopex-py- ]]; then
    PY_VERSION="$(grep -m1 '^version' crates/alopex-py/Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
    PY_TAG="alopex-py-v${PY_VERSION}"
    if git rev-parse "${PY_TAG}" >/dev/null 2>&1; then
        log_ok "Corresponding ${PY_TAG} already exists"
    else
        echo ""
        log_info "NOTE: crates/alopex-py/Cargo.toml is at version ${PY_VERSION}, but tag"
        log_info "      ${PY_TAG} does not exist yet. alopex-py publishes to PyPI on an"
        log_info "      INDEPENDENT tag trigger (alopex-py-release.yml, not release.yml)."
        log_info "      If this Rust release includes alopex-py changes, remember to also"
        log_info "      run: $0 ${PY_TAG}"
    fi
fi

# 6. 既に同名タグが存在しないか(存在する場合は delete が必要なことを明示)
if git rev-parse "${TAG_NAME}" >/dev/null 2>&1; then
    EXISTING_SHA="$(git rev-parse "${TAG_NAME}")"
    log_fail "Tag ${TAG_NAME} already exists, pointing at ${EXISTING_SHA:0:7}"
    echo "  If this is intentional (re-cutting a failed release), delete it first:"
    echo "    git push --delete origin ${TAG_NAME} && git tag -d ${TAG_NAME}"
    echo "  Also delete the associated GitHub Release if one was created:"
    echo "    gh release delete ${TAG_NAME} --yes"
    FAILED=1
fi

echo ""
if [ "${FAILED}" -eq 1 ]; then
    log_fail "Safety checks failed. Fix the issues above before tagging."
    exit 1
fi

log_ok "All safety checks passed."
echo ""
echo "To create and push the tag, run:"
echo "  git tag -a ${TAG_NAME} -m \"Release ${TAG_NAME}\""
echo "  git push origin ${TAG_NAME}"
