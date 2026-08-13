#!/usr/bin/env bash
# safe-tag.sh - リリースタグ作成前の必須検証
#
# v0.7.1 のリリースで、ローカル main を fetch はしたが checkout/pull
# し忘れたまま `git tag` を実行し、PR マージ前の古いコミットにタグを
# 打ってしまう事故が2回発生した(うち1回はさらに reqwest 依存の重複を
# 見落としたまま release ビルドがディスク枯渇で失敗)。
#
# 通常リリースは main、過去系列のパッチリリースは直前タグを起点とする
# release/vX.Y.Z ブランチから行う。このスクリプトは実際にタグを打つ前に、
# remote SHA、版番号、保守対象系列と起点タグを機械的に確認する。確認が全て
# 通った場合のみタグ作成コマンドを表示する(タグ作成・push は行わない)。
#
# Usage: ./scripts/release/safe-tag.sh <tag-name> [--maintenance-base <tag>]
#   例: ./scripts/release/safe-tag.sh v0.7.2
#       ./scripts/release/safe-tag.sh alopex-py-v0.7.2
#       ./scripts/release/safe-tag.sh v0.7.7 --maintenance-base v0.7.6
#       ./scripts/release/safe-tag.sh alopex-py-v0.7.7 --maintenance-base v0.7.6

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

if [ $# -ne 1 ] && [ $# -ne 3 ]; then
    log_fail "Usage: $0 <tag-name> [--maintenance-base <tag>]"
    exit 1
fi

TAG_NAME="$1"
MAINTENANCE_BASE=""
if [ $# -eq 3 ]; then
    if [ "$2" != "--maintenance-base" ]; then
        log_fail "Unknown option: $2"
        exit 1
    fi
    MAINTENANCE_BASE="$3"
fi
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

# 2. タグ名から期待バージョンを抽出し、Cargo.toml と一致するか確認
EXPECTED_VERSION=""
if [[ "${TAG_NAME}" =~ ^alopex-py-v([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    EXPECTED_VERSION="${BASH_REMATCH[1]}"
    ACTUAL_VERSION="$(grep -m1 '^version' crates/alopex-py/Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
    VERSION_SOURCE="crates/alopex-py/Cargo.toml"
elif [[ "${TAG_NAME}" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    EXPECTED_VERSION="${BASH_REMATCH[1]}"
    ACTUAL_VERSION="$(grep -m1 '^version' Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
    VERSION_SOURCE="Cargo.toml (workspace.package.version)"
else
    log_fail "Tag name does not match expected pattern (vX.Y.Z, alopex-py-vX.Y.Z): ${TAG_NAME}"
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

# 3. 通常リリースは main、過去系列のパッチは release/vX.Y.Z に限定する。
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
EXPECTED_MAINTENANCE_BRANCH="release/v${EXPECTED_VERSION}"
RELEASE_MODE=""
if [ "${CURRENT_BRANCH}" = "main" ]; then
    RELEASE_MODE="main"
    if [ -n "${MAINTENANCE_BASE}" ]; then
        log_fail "--maintenance-base is only valid on ${EXPECTED_MAINTENANCE_BRANCH}"
        FAILED=1
    else
        log_ok "Normal release source: main"
    fi
elif [ -n "${EXPECTED_VERSION}" ] && [ "${CURRENT_BRANCH}" = "${EXPECTED_MAINTENANCE_BRANCH}" ]; then
    RELEASE_MODE="maintenance"
    if [ -z "${MAINTENANCE_BASE}" ]; then
        log_fail "Historical patch releases require --maintenance-base <vX.Y.Z>"
        FAILED=1
    else
        log_ok "Maintenance release source: ${CURRENT_BRANCH} (base ${MAINTENANCE_BASE})"
    fi
else
    log_fail "Unsafe release branch: ${CURRENT_BRANCH}"
    echo "  Use main for a normal release, or ${EXPECTED_MAINTENANCE_BRANCH} for a historical patch."
    FAILED=1
fi

# 4. 対象ブランチを fetch して、ローカル HEAD と remote SHA が一致するか確認。
log_info "Fetching origin/${CURRENT_BRANCH} and release tags..."
if ! git fetch origin "${CURRENT_BRANCH}" --tags --quiet; then
    log_fail "Unable to fetch origin/${CURRENT_BRANCH}; push the release branch first"
    FAILED=1
fi
LOCAL_HEAD="$(git rev-parse HEAD)"
if ! REMOTE_HEAD="$(git rev-parse "origin/${CURRENT_BRANCH}" 2>/dev/null)"; then
    log_fail "Remote branch does not exist: origin/${CURRENT_BRANCH}"
    FAILED=1
elif [ "${LOCAL_HEAD}" != "${REMOTE_HEAD}" ]; then
    log_fail "Local ${CURRENT_BRANCH} (${LOCAL_HEAD:0:7}) != origin/${CURRENT_BRANCH} (${REMOTE_HEAD:0:7})"
    echo "  Commit and push the exact release candidate before tagging."
    FAILED=1
else
    log_ok "Local and remote release source match (${LOCAL_HEAD:0:7})"
fi

# 5. 保守リリースは同じ major/minor の古いパッチタグを明示し、そのタグが
#    HEAD の直近リリース祖先かつ remote と同一であることを要求する。
if [ "${RELEASE_MODE}" = "maintenance" ] && [ -n "${MAINTENANCE_BASE}" ]; then
    if [[ ! "${EXPECTED_VERSION}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        log_fail "Expected release version is not semantic X.Y.Z: ${EXPECTED_VERSION}"
        FAILED=1
    else
        TARGET_MAJOR="${BASH_REMATCH[1]}"
        TARGET_MINOR="${BASH_REMATCH[2]}"
        TARGET_PATCH="${BASH_REMATCH[3]}"
    fi
    if [[ ! "${MAINTENANCE_BASE}" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        log_fail "Maintenance base must be a Rust release tag such as v0.7.6: ${MAINTENANCE_BASE}"
        FAILED=1
    else
        BASE_MAJOR="${BASH_REMATCH[1]}"
        BASE_MINOR="${BASH_REMATCH[2]}"
        BASE_PATCH="${BASH_REMATCH[3]}"
        if [ "${BASE_MAJOR}" != "${TARGET_MAJOR:-}" ] || [ "${BASE_MINOR}" != "${TARGET_MINOR:-}" ]; then
            log_fail "Maintenance base ${MAINTENANCE_BASE} is not in target line v${TARGET_MAJOR:-?}.${TARGET_MINOR:-?}.x"
            FAILED=1
        elif [ "${BASE_PATCH}" -ge "${TARGET_PATCH:-0}" ]; then
            log_fail "Maintenance base patch must precede target: ${MAINTENANCE_BASE} -> v${EXPECTED_VERSION}"
            FAILED=1
        fi
    fi

    if ! git show-ref --verify --quiet "refs/tags/${MAINTENANCE_BASE}"; then
        log_fail "Maintenance base tag is missing locally: ${MAINTENANCE_BASE}"
        FAILED=1
    else
        LOCAL_BASE_SHA="$(git rev-list -n1 "${MAINTENANCE_BASE}")"
        REMOTE_BASE_SHA="$(git ls-remote --tags origin "refs/tags/${MAINTENANCE_BASE}^{}" | awk 'NR == 1 {print $1}')"
        if [ -z "${REMOTE_BASE_SHA}" ]; then
            REMOTE_BASE_SHA="$(git ls-remote --tags origin "refs/tags/${MAINTENANCE_BASE}" | awk 'NR == 1 {print $1}')"
        fi
        if [ -z "${REMOTE_BASE_SHA}" ] || [ "${LOCAL_BASE_SHA}" != "${REMOTE_BASE_SHA}" ]; then
            log_fail "Local and remote base tag commits differ: ${MAINTENANCE_BASE}"
            FAILED=1
        elif ! git merge-base --is-ancestor "${MAINTENANCE_BASE}^{commit}" HEAD; then
            log_fail "Maintenance base is not an ancestor of HEAD: ${MAINTENANCE_BASE}"
            FAILED=1
        else
            NEAREST_LINE_TAG="$(git describe --tags --match "v${TARGET_MAJOR}.${TARGET_MINOR}.*" --abbrev=0 HEAD 2>/dev/null || true)"
            ALLOWED_NEAREST_TAG="${MAINTENANCE_BASE}"
            if [[ "${TAG_NAME}" =~ ^alopex-py-v ]] && \
               git show-ref --verify --quiet "refs/tags/v${EXPECTED_VERSION}" && \
               [ "$(git rev-list -n1 "v${EXPECTED_VERSION}")" = "${LOCAL_HEAD}" ]; then
                # The chained Python CI/CD starts after the matching Rust release,
                # so the newly-created Rust tag is now the nearest line tag.
                ALLOWED_NEAREST_TAG="v${EXPECTED_VERSION}"
            fi
            if [ "${NEAREST_LINE_TAG}" != "${ALLOWED_NEAREST_TAG}" ]; then
                log_fail "Nearest v${TARGET_MAJOR}.${TARGET_MINOR}.x ancestor is ${NEAREST_LINE_TAG:-none}, expected ${ALLOWED_NEAREST_TAG}"
                FAILED=1
            else
                log_ok "Maintenance ancestry and remote base tag verified (${MAINTENANCE_BASE} @ ${LOCAL_BASE_SHA:0:7}; nearest ${NEAREST_LINE_TAG})"
            fi
        fi
    fi
fi

# 6. v* タグ(Rust workspace)の場合、alopex-py も同じバージョンに揃って
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

# 7. 既に同名タグが local/remote に存在しないか。
if git show-ref --verify --quiet "refs/tags/${TAG_NAME}"; then
    EXISTING_SHA="$(git rev-list -n1 "${TAG_NAME}")"
    log_fail "Tag ${TAG_NAME} already exists, pointing at ${EXISTING_SHA:0:7}"
    echo "  Published versions are immutable; prepare the next patch version instead of retagging."
    FAILED=1
fi
if git ls-remote --exit-code --tags origin "refs/tags/${TAG_NAME}" >/dev/null 2>&1; then
    log_fail "Remote tag already exists: ${TAG_NAME}"
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
