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
# 実行結果は docs-public リポジトリへの PR として自動レポートされる
# (--no-report で無効化可能。CI 以外でのアドホック実行時など、レポート
# 不要な場合に使う)。「実行して終わり」では検証の意味が薄いため、成功時
# だけでなく失敗時も必ずレポートを生成する。
#
# Usage:
#   ./scripts/release/verify-release/run.sh [ALOPEX_VERSION] [--no-report]
#   例: ./scripts/release/verify-release/run.sh 0.7.2

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DOCS_PUBLIC_DIR="${DOCS_PUBLIC_DIR:-${REPO_ROOT}/../docs-public}"

ALOPEX_VERSION="0.7.1"
DO_REPORT=1
for arg in "$@"; do
    case "${arg}" in
        --no-report) DO_REPORT=0 ;;
        *) ALOPEX_VERSION="${arg}" ;;
    esac
done

IMAGE_TAG="alopex-verify-release:${ALOPEX_VERSION}"
CHIRPS_DIR="${CHIRPS_DIR:-${REPO_ROOT}/../chirps}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

# --- レポート用の結果蓄積 ---
# 各ステップの結果を "name|status|detail" 形式で配列に積む。
# status は ok / fail / skip のいずれか。
declare -a REPORT_STEPS=()
OVERALL_STATUS="ok"

record_step() {
    local name="$1" status="$2" detail="${3:-}"
    REPORT_STEPS+=("${name}|${status}|${detail}")
    if [ "${status}" = "fail" ]; then
        OVERALL_STATUS="fail"
    fi
}

write_report_and_maybe_pr() {
    if [ "${DO_REPORT}" -eq 0 ]; then
        log_info "--no-report 指定によりレポート生成をスキップします"
        return 0
    fi
    if [ ! -d "${DOCS_PUBLIC_DIR}" ]; then
        log_fail "docs-public リポジトリが見つからない: ${DOCS_PUBLIC_DIR}"
        log_fail "DOCS_PUBLIC_DIR=<path> で指定するか、${REPO_ROOT}/../docs-public に配置すること。レポートは生成できません。"
        return 1
    fi

    local report_date report_dir report_file
    report_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    report_dir="${DOCS_PUBLIC_DIR}/reports/release-verification"
    report_file="${report_dir}/v${ALOPEX_VERSION}.md"
    mkdir -p "${report_dir}"

    {
        echo "# リリース確認レポート: v${ALOPEX_VERSION}"
        echo ""
        echo "> 生成日時 (UTC): ${report_date}"
        echo "> 検証方法: \`scripts/release/verify-release/run.sh\`(crates.io/PyPI 公開版のみを使用、ソースビルドなし)"
        echo "> 総合結果: **$([ "${OVERALL_STATUS}" = "ok" ] && echo "✅ 全ステップ成功" || echo "❌ 失敗あり")**"
        echo ""
        echo "## ステップ結果"
        echo ""
        echo "| ステップ | 結果 | 詳細 |"
        echo "|---|---|---|"
        local entry name status detail mark
        for entry in "${REPORT_STEPS[@]}"; do
            IFS='|' read -r name status detail <<<"${entry}"
            case "${status}" in
                ok) mark="✅" ;;
                fail) mark="❌" ;;
                skip) mark="⏭️" ;;
                *) mark="?" ;;
            esac
            echo "| ${name} | ${mark} ${status} | ${detail} |"
        done
        echo ""
        echo "## 検証内容"
        echo ""
        echo "- \`crates/alopex-tools\` の \`verify-release-embedded\`: crates.io 公開版 \`alopex-embedded\` への依存としてビルド可能か"
        echo "- mode-parity 検証 (\`scripts/parity/verify.py\`): S2a/S2b/S2c の全組み合わせ一致"
        echo "- mode-parity デモ (\`scripts/parity/demo.py\`): 第1〜5幕(SF-MEM/SF-FILE/SF-HTTP/SF-GRPC/SF-CLUSTER)"
        echo "- v0.7 機能デモ: \`demo_cluster.py\` / \`demo_routing.py\` / \`demo_dataframe_p3.py\`"
        echo ""
        echo "リポジトリのソースコードは一切ビルドせず、\`cargo install\`/\`pip install\` で取得した公開版のみを使用する(Nim ツールチェーンも不要)。"
    } >"${report_file}"

    log_info "レポートを生成しました: ${report_file}"

    if ! command -v gh >/dev/null 2>&1; then
        log_fail "gh コマンドが見つからないため PR 作成をスキップします(レポートファイルは生成済み)"
        return 1
    fi

    local branch="report/verify-release-v${ALOPEX_VERSION}"
    (
        cd "${DOCS_PUBLIC_DIR}"
        if ! git diff --quiet -- "reports/release-verification/v${ALOPEX_VERSION}.md" 2>/dev/null && \
           ! git status --porcelain -- "reports/release-verification/v${ALOPEX_VERSION}.md" | grep -q .; then
            log_info "レポート内容に変更なし。PR 作成をスキップします。"
            exit 0
        fi
        git checkout -B "${branch}"
        git add "reports/release-verification/v${ALOPEX_VERSION}.md"
        git commit -m "docs(release): v${ALOPEX_VERSION} リリース確認レポート ($([ "${OVERALL_STATUS}" = "ok" ] && echo "success" || echo "failure"))"
        git push -u origin "${branch}" --force
        gh pr create \
            --title "docs(release): v${ALOPEX_VERSION} リリース確認レポート" \
            --body "$(cat <<EOF
verify-release/run.sh の自動実行結果。総合結果: $([ "${OVERALL_STATUS}" = "ok" ] && echo "✅ 成功" || echo "❌ 失敗あり")。

詳細は \`reports/release-verification/v${ALOPEX_VERSION}.md\` を参照。
EOF
)" \
            --base main --head "${branch}" 2>&1 || log_info "PR が既に存在するか作成に失敗しました(レポートファイルは push 済み)"
    )
    log_ok "docs-public へレポートを push しました(branch: ${branch})"
}

if [ ! -d "${CHIRPS_DIR}" ]; then
    log_fail "chirps リポジトリが見つからない: ${CHIRPS_DIR}"
    echo "  CHIRPS_DIR=<path> ./scripts/release/verify-release/run.sh ${ALOPEX_VERSION} で指定するか、"
    echo "  ${REPO_ROOT}/../chirps に配置すること。"
    exit 2
fi

log_info "alopex v${ALOPEX_VERSION} リリース確認を開始します"

log_info "コンテナイメージをビルド中: ${IMAGE_TAG}"
if docker build \
    -t "${IMAGE_TAG}" \
    --build-arg "ALOPEX_VERSION=${ALOPEX_VERSION}" \
    --build-arg "VERIFY_UID=$(id -u)" \
    --build-arg "VERIFY_GID=$(id -g)" \
    -f "${SCRIPT_DIR}/Dockerfile" \
    "${SCRIPT_DIR}"; then
    log_ok "イメージビルド完了"
    record_step "コンテナイメージビルド" ok ""
else
    status=$?
    log_fail "イメージビルド失敗(exit ${status})"
    record_step "コンテナイメージビルド" fail "exit ${status}"
    write_report_and_maybe_pr
    exit "${status}"
fi

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
if run_in_container bash -c 'cd crates/alopex-tools && CARGO_TARGET_DIR=/tools-target cargo build --release'; then
    log_ok "verify-release-embedded ビルド完了"
    record_step "verify-release-embedded ビルド" ok ""
else
    status=$?
    log_fail "verify-release-embedded ビルド失敗(exit ${status})"
    record_step "verify-release-embedded ビルド" fail "exit ${status}"
    write_report_and_maybe_pr
    exit "${status}"
fi

log_info "mode-parity 検証(scripts/parity/verify.py)を実行"
if run_in_container python3 scripts/parity/verify.py \
    --corpus scripts/parity/corpus --expected scripts/parity/expected; then
    log_ok "verify.py 完走(exit 0)"
    record_step "mode-parity 検証 (verify.py)" ok ""
else
    status=$?
    if [ "${status}" -eq 1 ]; then
        log_fail "verify.py が検証不一致を検出(exit 1)"
    else
        log_fail "verify.py が環境エラー(exit ${status})"
    fi
    record_step "mode-parity 検証 (verify.py)" fail "exit ${status}"
    write_report_and_maybe_pr
    exit "${status}"
fi

log_info "mode-parity デモ(scripts/parity/demo.py)を実行"
run_in_container python3 scripts/parity/demo.py
demo_status=$?
if [ "${demo_status}" -ne 0 ]; then
    log_fail "demo.py が失敗(exit ${demo_status})"
    record_step "mode-parity デモ (demo.py)" fail "exit ${demo_status}"
    write_report_and_maybe_pr
    exit "${demo_status}"
fi
log_ok "demo.py 完走(exit 0)"
record_step "mode-parity デモ (demo.py)" ok ""

for script in demo_cluster.py demo_routing.py demo_dataframe_p3.py; do
    log_info "v0.7 機能デモ(scripts/demo/v07/${script})を実行"
    run_in_container python3 "scripts/demo/v07/${script}"
    status=$?
    if [ "${status}" -ne 0 ]; then
        log_fail "${script} が失敗(exit ${status})"
        record_step "v0.7 機能デモ (${script})" fail "exit ${status}"
        write_report_and_maybe_pr
        exit "${status}"
    fi
    log_ok "${script} 完走(exit 0)"
    record_step "v0.7 機能デモ (${script})" ok ""
done

echo ""
log_ok "全デモスクリプトが公開版 v${ALOPEX_VERSION} で完走しました。"

write_report_and_maybe_pr
