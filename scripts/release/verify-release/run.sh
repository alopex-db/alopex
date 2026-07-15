#!/usr/bin/env bash
# run.sh - alopex リリース確認コンテナのビルド・実行を一括で行う
#
# 「crates.io / PyPI に公開された配布物」だけを使ってデモスクリプトを
# 完走させる。リポジトリのソースコードは製品クレートとしてはビルドしない
# (crates/alopex-tools の verify-release-embedded だけが、crates.io 公開版
# alopex-embedded への依存としてビルドされる。alopex-tools/Cargo.toml の
# [workspace] 空テーブルにより親ワークスペードから独立しているため、この
# ビルドが通ること自体が「公開クレートが実際に取得・ビルドできる」ことの
# 検証になる)。
#
# Usage:
#   ./scripts/release/verify-release/run.sh [ALOPEX_VERSION]
#   例: ./scripts/release/verify-release/run.sh 0.7.1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
ALOPEX_VERSION="${1:-0.7.1}"
IMAGE_TAG="alopex-verify-release:${ALOPEX_VERSION}"
CHIRPS_DIR="${CHIRPS_DIR:-${REPO_ROOT}/../chirps}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

if [ ! -d "${CHIRPS_DIR}" ]; then
    log_fail "chirps リポジトリが見つからない: ${CHIRPS_DIR}"
    echo "  CHIRPS_DIR=<path> ./scripts/release/verify-release/run.sh ${ALOPEX_VERSION} で指定するか、"
    echo "  ${REPO_ROOT}/../chirps に配置すること。"
    exit 2
fi

log_info "alopex v${ALOPEX_VERSION} リリース確認を開始します"

log_info "コンテナイメージをビルド中: ${IMAGE_TAG}"
docker build \
    -t "${IMAGE_TAG}" \
    --build-arg "ALOPEX_VERSION=${ALOPEX_VERSION}" \
    --build-arg "VERIFY_UID=$(id -u)" \
    --build-arg "VERIFY_GID=$(id -g)" \
    -f "${SCRIPT_DIR}/Dockerfile" \
    "${SCRIPT_DIR}"
log_ok "イメージビルド完了"

# crates/alopex-tools の verify-release-embedded は、EmbeddedSurface
# (released モード)がデモスクリプトから起動する契約(surfaces.py 参照)。
# ここで一度だけビルドし(ホスト側の一時ディレクトリへ出力してコンテナ間
# で使い回す)、以降のデモ実行では PATH に加えるだけにする。
TOOLS_TARGET_DIR="$(mktemp -d)"
trap 'rm -rf "${TOOLS_TARGET_DIR}"' EXIT

# /tools-target/release (verify-release-embedded のビルド出力) を PATH に
# 追加する。イメージの ENV PATH は Dockerfile 側で維持されるので、ここでは
# 追加分だけを渡す(docker run -e PATH=... で丸ごと上書きしない)。
run_in_container() {
    docker run --rm \
        --user "$(id -u):$(id -g)" -e HOME=/tmp/verify-home \
        -v "${REPO_ROOT}":/workspace:ro \
        -v "${CHIRPS_DIR}":/chirps:ro \
        -v "${TOOLS_TARGET_DIR}":/tools-target \
        -w /workspace \
        -e "ALOPEX_BINARY_SOURCE=released" \
        -e "ALOPEX_EXTRA_PATH=/tools-target/release" \
        "${IMAGE_TAG}" \
        "$@"
}

log_info "crates/alopex-tools (verify-release-embedded) をビルド中(公開版 alopex-embedded 依存)"
run_in_container bash -c 'cd crates/alopex-tools && CARGO_TARGET_DIR=/tools-target cargo build --release'
log_ok "verify-release-embedded ビルド完了"

log_info "mode-parity 検証(scripts/parity/verify.py)を実行"
if run_in_container python3 scripts/parity/verify.py \
    --corpus scripts/parity/corpus --expected scripts/parity/expected; then
    log_ok "verify.py 完走(exit 0)"
else
    status=$?
    if [ "${status}" -eq 1 ]; then
        log_fail "verify.py が検証不一致を検出(exit 1)"
    else
        log_fail "verify.py が環境エラー(exit ${status})"
    fi
    exit "${status}"
fi

log_info "mode-parity デモ(scripts/parity/demo.py)を実行"
run_in_container python3 scripts/parity/demo.py
demo_status=$?
if [ "${demo_status}" -ne 0 ]; then
    log_fail "demo.py が失敗(exit ${demo_status})"
    exit "${demo_status}"
fi
log_ok "demo.py 完走(exit 0)"

for script in demo_cluster.py demo_routing.py demo_dataframe_p3.py; do
    log_info "v0.7 機能デモ(scripts/demo/v07/${script})を実行"
    run_in_container python3 "scripts/demo/v07/${script}"
    status=$?
    if [ "${status}" -ne 0 ]; then
        log_fail "${script} が失敗(exit ${status})"
        exit "${status}"
    fi
    log_ok "${script} 完走(exit 0)"
done

echo ""
log_ok "全デモスクリプトが公開版 v${ALOPEX_VERSION} で完走しました。"
