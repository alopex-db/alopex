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
# だけでなく失敗時も必ずレポートを生成する。レポートは結果の一覧表だけ
# ではなく、各ステップが「何を・なぜ検証するか」の説明文と、実行時の
# 主要な出力(検証コーパスの実行結果サマリー等)を含む。
#
# 新しいステップを追加する場合は run_step 呼び出しに DESCRIPTION も
# 必ず添える(結果一覧だけのステップを増やさない)。
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
LOG_DIR="$(mktemp -d)"
TOOLS_TARGET_DIR=""
cleanup() { rm -rf "${LOG_DIR}" "${TOOLS_TARGET_DIR}"; }
trap cleanup EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

# --- レポート用の結果蓄積 ---
# 各ステップを "name|status|description|logfile" 形式で配列に積む。
# status は ok / fail のいずれか。description はレポート読者(一般公開)
# に「これは何を検証しているか」を伝える1〜2文。logfile はそのステップの
# 標準出力キャプチャ(存在すれば末尾を抜粋してレポートに埋め込む)。
declare -a REPORT_STEPS=()
OVERALL_STATUS="ok"
STEP_INDEX=0

# run_step NAME DESCRIPTION -- CMD...
# CMD の標準出力/標準エラーをログファイルへも複製しつつ画面に表示する
# (tee)。失敗した場合はここで即座にレポートを生成して exit する。
run_step() {
    local name="$1" description="$2"
    shift 2
    if [ "${1:-}" != "--" ]; then
        echo "run_step: expected -- before command" >&2
        exit 64
    fi
    shift
    STEP_INDEX=$((STEP_INDEX + 1))
    local logfile="${LOG_DIR}/step-${STEP_INDEX}.log"

    log_info "${name}"
    "$@" 2>&1 | tee "${logfile}"
    local status="${PIPESTATUS[0]}"

    if [ "${status}" -eq 0 ]; then
        log_ok "${name} 完了(exit 0)"
        REPORT_STEPS+=("${name}|ok|${description}|${logfile}")
    else
        log_fail "${name} 失敗(exit ${status})"
        REPORT_STEPS+=("${name}|fail|${description}|${logfile}")
        OVERALL_STATUS="fail"
        write_report_and_maybe_pr
        exit "${status}"
    fi
}

# ログファイルから末尾 N 行を Markdown コードブロックとして整形する。
render_log_excerpt() {
    local logfile="$1" lines="${2:-40}"
    if [ ! -s "${logfile}" ]; then
        return 0
    fi
    echo '```'
    tail -n "${lines}" "${logfile}"
    echo '```'
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
        echo "> 総合結果: **$([ "${OVERALL_STATUS}" = "ok" ] && echo "✅ 全ステップ成功" || echo "❌ 失敗あり")**"
        echo ""
        echo "## これは何を検証しているか"
        echo ""
        echo "alopex は「ライブラリ(インメモリ)・組み込み(ファイル)・シングルノード"
        echo "サーバー・クラスタが、同一データファイルと同一プロトコルで動作する単一"
        echo "エンジンである」ことを製品価値としている。このレポートは、その価値が"
        echo "**実際にユーザーが手にする配布物**(crates.io の \`cargo install\`、"
        echo "PyPI の \`pip install\`)で成立していることを示す。"
        echo ""
        echo "検証は専用コンテナ(\`scripts/release/verify-release/\`)内で行い、"
        echo "**alopex のソースコードは一切ビルドしない**。Nim ツールチェーンも"
        echo "使わない(v0.7.2 以降、Nim 共有ライブラリは crates.io 公開 crate に"
        echo "事前ビルド済みで同梱されている)。過去に crates.io publish の順序"
        echo "バグ・依存重複によるビルド失敗・実行時のライブラリ解決失敗が"
        echo "リリース後に発覚した経緯があり、この検証はそれらを releaseタグ"
        echo "push 後に即座に検知するためのものである。"
        echo ""
        echo "## ステップ"
        echo ""
        local entry name status description logfile mark i=0
        for entry in "${REPORT_STEPS[@]}"; do
            i=$((i + 1))
            IFS='|' read -r name status description logfile <<<"${entry}"
            mark="✅"
            [ "${status}" = "fail" ] && mark="❌"
            echo "### ${i}. ${name} ${mark}"
            echo ""
            echo "${description}"
            echo ""
            render_log_excerpt "${logfile}" 60
            echo ""
        done
        echo "## 免責"
        echo ""
        echo "- このレポートは \`scripts/release/verify-release/run.sh\` の自動実行結果を"
        echo "  そのまま記録したものである。人手による追記・改変は行わない。"
        echo "- 新しい検証ステップを追加する場合は run.sh 側の \`run_step\` 呼び出しに"
        echo "  \`description\` を含めることが必須(結果一覧だけのステップを増やさない)。"
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

run_step "コンテナイメージビルド" \
    "検証専用の Docker イメージをビルドする。alopex-cli/alopex-server は \`cargo install\`、alopex(Python) は \`pip install\` で crates.io/PyPI から取得する(このイメージには alopex のソースコードを一切 COPY しない)。" \
    -- docker build \
        -t "${IMAGE_TAG}" \
        --build-arg "ALOPEX_VERSION=${ALOPEX_VERSION}" \
        --build-arg "VERIFY_UID=$(id -u)" \
        --build-arg "VERIFY_GID=$(id -g)" \
        -f "${SCRIPT_DIR}/Dockerfile" \
        "${SCRIPT_DIR}"

# crates/alopex-tools の verify-release-embedded は、EmbeddedSurface
# (released モード)がデモスクリプトから起動する契約(surfaces.py 参照)。
# ここで一度だけビルドし(ホスト側の一時ディレクトリへ出力してコンテナ間
# で使い回す)、以降のデモ実行では PATH に加えるだけにする。
TOOLS_TARGET_DIR="$(mktemp -d)"

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

run_step "verify-release-embedded ビルド" \
    "crates/alopex-tools(開発ツール専用の独立ワークスペース)が crates.io 公開版の alopex-embedded/alopex-sql に依存としてビルドできるかを検証する。これが通ること自体が「公開 crate が実際に取得・ビルド可能」であることの証明になる。" \
    -- run_in_container bash -c 'cd crates/alopex-tools && CARGO_TARGET_DIR=/tools-target cargo build --release'

run_step "mode-parity 検証 (verify.py)" \
    "「ライブラリ・組み込み・サーバー・gRPC・クラスタの各サーフェスが同一 SQL コーパスに対して同一結果を返す」ことを機械検証する。S2a(単一プロセス内での全ペア比較)・S2b(writer/reader を分けた永続化データの相互可搬性)の全組み合わせが一致することを確認する。" \
    -- run_in_container python3 scripts/parity/verify.py \
        --corpus scripts/parity/corpus --expected scripts/parity/expected

run_step "mode-parity デモ (demo.py)" \
    "上記の機械検証と同一のコーパスを使い、「One Engine, Four Forms」を人間向けに実演する。第1幕(ライブラリ/インメモリ)で実行した結果が、第2幕(組み込み/ファイル永続化)・第3幕(シングルノードサーバー、HTTP と gRPC の両方)・第4幕(サーバー停止後に CLI で再オープン)・第5幕(cluster-aware 単一メンバー)を通じて一貫することを確認する。" \
    -- run_in_container python3 scripts/parity/demo.py

run_step "v0.7 機能デモ: demo_cluster.py" \
    "cluster status API のクロスサーフェス実証(D2)。single_node/cluster_aware の両モードでの起動、membership の join/leave によるライフサイクル遷移、Chirps 不可時の degraded フォールバックを、HTTP と CLI の両方で観測し、フィールドが一致することを確認する。" \
    -- run_in_container python3 scripts/demo/v07/demo_cluster.py

run_step "v0.7 機能デモ: demo_routing.py" \
    "SQL 実行がすべてルーティング判定を経由し、その決定理由が診断として観測できることを実証する(D3)。v0.7 のライブ実行は local_only であり、分散が必要な操作は明示的に拒否されることを確認する。" \
    -- run_in_container python3 scripts/demo/v07/demo_routing.py

run_step "v0.7 機能デモ: demo_dataframe_p3.py" \
    "DataFrame の string/datetime/list 名前空間関数と explode/implode の往復変換が Rust と Python の両サーフェスで決定的に(同一入力に対して常にバイト単位で同一の出力を)動作することを確認する(D4)。" \
    -- run_in_container python3 scripts/demo/v07/demo_dataframe_p3.py

echo ""
log_ok "全デモスクリプトが公開版 v${ALOPEX_VERSION} で完走しました。"

write_report_and_maybe_pr
