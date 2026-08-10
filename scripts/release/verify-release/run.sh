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
#   ./scripts/release/verify-release/run.sh --verify-join candidate.json
#   例: ./scripts/release/verify-release/run.sh 0.8.4
#
# chirps は既定では隣接 checkout (${REPO_ROOT}/../chirps) を使う。存在しない
# 場合は公開 repo を一時 clone するため、worktree 配置に依存しない。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DEFAULT_DOCS_PUBLIC_DIR="${REPO_ROOT}/../docs-public"
if [ -z "${DOCS_PUBLIC_DIR:-}" ] && [ ! -d "${DEFAULT_DOCS_PUBLIC_DIR}" ] \
    && [ -d "${REPO_ROOT}/../../docs-public" ]; then
    DEFAULT_DOCS_PUBLIC_DIR="${REPO_ROOT}/../../docs-public"
fi
DOCS_PUBLIC_DIR="${DOCS_PUBLIC_DIR:-${DEFAULT_DOCS_PUBLIC_DIR}}"

ALOPEX_VERSION="0.8.4"
DO_REPORT=1
JOIN_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-report) DO_REPORT=0; shift ;;
        --verify-join)
            if [[ $# -lt 2 ]]; then
                echo "--verify-join requires a candidate JSON path" >&2
                exit 64
            fi
            JOIN_FILE="$2"
            shift 2
            ;;
        --*)
            echo "unknown option: $1" >&2
            exit 64
            ;;
        *) ALOPEX_VERSION="$1"; shift ;;
    esac
done

IMAGE_TAG="alopex-verify-release:${ALOPEX_VERSION}"
DEFAULT_CHIRPS_DIR="${REPO_ROOT}/../chirps"
if [ ! -d "${DEFAULT_CHIRPS_DIR}" ] && [ -d "${REPO_ROOT}/../../chirps" ]; then
    DEFAULT_CHIRPS_DIR="${REPO_ROOT}/../../chirps"
fi
CHIRPS_REPO_URL="${CHIRPS_REPO_URL:-https://github.com/alopex-db/alopex-chirps.git}"
CHIRPS_REF="${CHIRPS_REF:-main}"
CHIRPS_DIR_WAS_EXPLICIT=0
if [ -n "${CHIRPS_DIR:-}" ]; then
    CHIRPS_DIR_WAS_EXPLICIT=1
else
    CHIRPS_DIR="${DEFAULT_CHIRPS_DIR}"
fi
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

# Verify the immutable public-surface join without building from repository
# product bytes. The input is a recorded candidate envelope produced from
# public registries/GitHub and is intentionally fixture-friendly for CI tests.
verify_release_join() {
    local candidate="$1"
    if [[ ! -f "${candidate}" || -L "${candidate}" ]]; then
        log_fail "release join candidate is not a regular file: ${candidate}"
        return 2
    fi
    python3 - "${candidate}" "${ALOPEX_VERSION}" <<'PY'
import json
import re
import sys

candidate_path, version = sys.argv[1:]
expected_tag = f"v{version}"
sha40 = re.compile(r"^[0-9a-fA-F]{40}$")
sha64 = re.compile(r"^[0-9a-fA-F]{64}$")
targets = {
    "x86_64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
}

def fail(message):
    print(f"release-join: {message}", file=sys.stderr)
    raise SystemExit(1)

try:
    with open(candidate_path, encoding="utf-8") as stream:
        data = json.load(stream)
except (OSError, json.JSONDecodeError) as exc:
    fail(f"invalid candidate JSON: {exc}")

if data.get("version") != version:
    fail(f"candidate version {data.get('version')!r} != {version!r}")
reviewed = data.get("reviewed_main_sha")
if not isinstance(reviewed, str) or not sha40.fullmatch(reviewed):
    fail("reviewed_main_sha must be a full 40-hex SHA")
tag = data.get("tag")
if not isinstance(tag, dict) or tag.get("name") != expected_tag:
    fail(f"tag must be {expected_tag}")
if tag.get("peeled_sha") != reviewed:
    fail("peeled tag SHA does not match reviewed main SHA")

for surface_name in ("core", "python"):
    surface = data.get(surface_name)
    if not isinstance(surface, dict):
        fail(f"missing {surface_name} public surface")
    if surface.get("status") != "success" or not surface.get("published"):
        fail(f"{surface_name} surface is not successfully published")
    if surface.get("peeled_sha") != reviewed:
        fail(f"{surface_name} surface is bound to a different SHA")
    if not isinstance(surface.get("registry"), str) or not surface["registry"]:
        fail(f"{surface_name} registry identity is missing")

crates = data["core"].get("crates")
if not isinstance(crates, list) or not crates or any(
    not isinstance(item, dict) or item.get("status") != "published"
    for item in crates
):
    fail("core crate publication set is incomplete")
distributions = data["python"].get("distributions")
if not isinstance(distributions, list) or not distributions or any(
    not isinstance(item, dict)
    or item.get("status") != "published"
    or not isinstance(item.get("sha256"), str)
    or not sha64.fullmatch(item["sha256"])
    for item in distributions
):
    fail("Python wheel/sdist publication set is incomplete")

parser = data.get("parser")
if not isinstance(parser, dict):
    fail("parser public surface is missing")
if parser.get("contract") != "0.4.0":
    fail("parser contract must be 0.4.0")
for field in ("manifest_sha256", "envelope_sha256"):
    if not isinstance(parser.get(field), str) or not sha64.fullmatch(parser[field]):
        fail(f"parser {field} is missing or invalid")
assets = parser.get("assets")
if not isinstance(assets, list) or {a.get("target") for a in assets if isinstance(a, dict)} != targets:
    fail("parser assets do not cover exactly the four release targets")
for asset in assets:
    if not isinstance(asset, dict):
        fail("parser asset record is invalid")
    if not sha64.fullmatch(str(asset.get("archive_sha256", ""))) or not sha64.fullmatch(str(asset.get("library_sha256", ""))):
        fail(f"parser asset digest is invalid for {asset.get('target')}")
    if asset.get("native_smoke") is not True:
        fail(f"native smoke evidence is missing for {asset.get('target')}")

if data.get("publication_order", {}).get("core_before_python") is not True:
    fail("publication order does not prove core-before-Python")
if data.get("repair_forward", {}).get("complete") is not True:
    fail("repair-forward closeout is incomplete")

print(f"release-join: complete for {expected_tag} at {reviewed}")
PY
}

if [[ -n "${JOIN_FILE}" ]]; then
    verify_release_join "${JOIN_FILE}"
    exit $?
fi

ensure_chirps_dir() {
    if [ -d "${CHIRPS_DIR}" ]; then
        return 0
    fi
    if [ "${CHIRPS_DIR_WAS_EXPLICIT}" -eq 1 ]; then
        log_fail "CHIRPS_DIR で指定された chirps リポジトリが見つからない: ${CHIRPS_DIR}"
        echo "  パスを修正するか、CHIRPS_DIR を未指定にして公開 repo からの一時取得を使ってください。"
        exit 2
    fi
    if ! command -v git >/dev/null 2>&1; then
        log_fail "git コマンドが見つからないため、公開 chirps repo を取得できません。"
        echo "  CHIRPS_DIR=<path> を指定してください。"
        exit 2
    fi
    local cloned_dir="${LOG_DIR}/chirps"
    log_info "chirps checkout が見つからないため、公開 repo から一時取得します: ${CHIRPS_REPO_URL} (${CHIRPS_REF})"
    git clone --depth 1 --branch "${CHIRPS_REF}" "${CHIRPS_REPO_URL}" "${cloned_dir}"
    CHIRPS_DIR="${cloned_dir}"
}

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

# docs-public 側に前回までの実行が残した「report/verify-release-*」
# ブランチのうち、対応する PR がマージ済みのものをローカル・リモート
# 両方から削除する。run.sh 自身が作ったブランチの後始末を毎回自動で
# 行い、実行のたびに未マージ分だけが残る状態を保つ(マージされていない
# ブランチは残し、確認なしに壊さない)。
cleanup_merged_report_branches() {
    if ! command -v gh >/dev/null 2>&1; then
        return 0
    fi
    (
        cd "${DOCS_PUBLIC_DIR}" || return 0
        if [ -n "$(git status --porcelain)" ]; then
            log_info "docs-public に未コミット変更があるため、古い report ブランチ cleanup をスキップします"
            return 0
        fi
        # 前回実行が report/verify-release-* ブランチにチェックアウトした
        # まま終わっている場合、そのブランチ自身は `git branch -D` できない
        # (カレントブランチは削除不可)。先に main へ戻しておく。
        git checkout main >/dev/null 2>&1
        local merged_branches
        merged_branches="$(gh pr list --repo alopex-db/docs --state merged \
            --search 'head:report/verify-release-' \
            --json headRefName --jq '.[].headRefName' 2>/dev/null | sort -u)"
        [ -z "${merged_branches}" ] && return 0
        local branch
        while IFS= read -r branch; do
            [ -z "${branch}" ] && continue
            if git show-ref --verify --quiet "refs/heads/${branch}"; then
                git branch -D "${branch}" >/dev/null 2>&1 \
                    && log_info "docs-public: マージ済みブランチを削除しました(local): ${branch}"
            fi
            if git ls-remote --exit-code --heads origin "${branch}" >/dev/null 2>&1; then
                git push origin --delete "${branch}" >/dev/null 2>&1 \
                    && log_info "docs-public: マージ済みブランチを削除しました(remote): ${branch}"
            fi
        done <<<"${merged_branches}"
    )
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
    cleanup_merged_report_branches

    local report_date report_dir report_file rust_version nim_image
    report_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    report_dir="${DOCS_PUBLIC_DIR}/reports/release-verification"
    report_file="${report_dir}/v${ALOPEX_VERSION}.md"
    mkdir -p "${report_dir}"
    rust_version="$(grep -oP '^ARG RUST_VERSION=\K.*' "${SCRIPT_DIR}/Dockerfile")"
    nim_image="$(grep -oP '^ARG NIM_IMAGE=\K[^@]*' "${SCRIPT_DIR}/Dockerfile")"

    {
        echo "# リリース確認レポート: v${ALOPEX_VERSION}"
        echo ""
        echo "> 総合結果: **$([ "${OVERALL_STATUS}" = "ok" ] && echo "✅ 全ステップ成功" || echo "❌ 失敗あり")**"
        echo ""
        if [ "${OVERALL_STATUS}" = "ok" ]; then
            echo "v${ALOPEX_VERSION} は、crates.io / PyPI に公開されたパッケージを"
            echo "そのままインストールした状態で、ライブラリ・組み込み(ファイル)・"
            echo "サーバー・クラスタのすべてが同一データに対して同一の結果を返すことを"
            echo "確認済みである。"
        else
            echo "v${ALOPEX_VERSION} の確認中に失敗したステップがある。詳細は下記を参照。"
        fi
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
        echo "---"
        echo ""
        echo "## 検証環境"
        echo ""
        echo "| 項目 | 値 |"
        echo "|---|---|"
        echo "| 対象バージョン | v${ALOPEX_VERSION} |"
        echo "| 生成日時 (UTC) | ${report_date} |"
        echo "| パッケージ取得元 | crates.io(alopex-cli/alopex-server) / PyPI(alopex) |"
        echo "| ソースビルド | なし(公開パッケージのみ使用) |"
        echo "| Rust | \`${rust_version}\` |"
        echo "| Nim(ビルド専用イメージ) | \`${nim_image}\` |"
        echo "| Python | \`3.11\` |"
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

ensure_chirps_dir

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
# で使い回す)、以降のデモ実行では PATH に加えるだけにする。独立crateには
# lockfileを同梱し、read-onlyでマウントした検証対象ソースへCargoが書き込ま
# ないようにする。
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
        -e "ALOPEX_VERSION=${ALOPEX_VERSION}" \
        -e "ALOPEX_EXTRA_PATH=/tools-target/release" \
        "${IMAGE_TAG}" \
        "$@"
}

run_step "verify-release-embedded ビルド" \
    "crates/alopex-tools(開発ツール専用の独立ワークスペース)が crates.io 公開版の alopex-embedded/alopex-sql に依存としてビルドできるかを検証する。これが通ること自体が「公開 crate が実際に取得・ビルド可能」であることの証明になる。" \
    -- run_in_container bash -c 'cd crates/alopex-tools && CARGO_TARGET_DIR=/tools-target cargo build --release --locked'

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

run_step "v${ALOPEX_VERSION} 回帰保証: alopex-sql aggregate state/distinct/parallel" \
    "crates.io 公開版 alopex-sql だけを依存にした一時 Rust crate をコンテナ内で作成し、Accumulator state/merge、COUNT 以外の DISTINCT 集約、単一プロセス partial→final parallel aggregate が v${ALOPEX_VERSION} で実際に動作することを確認する。リポジトリ内の alopex 製品ソースはビルドしないため、公開 artifact の振る舞い保証になる。" \
    -- run_in_container bash -c '
set -euo pipefail
workdir="$(mktemp -d)"
trap "rm -rf \"${workdir}\"" EXIT
cd "${workdir}"
cat > Cargo.toml <<EOF
[package]
name = "alopex-v073-aggregate-guarantee"
version = "0.1.0"
edition = "2024"

[dependencies]
alopex-sql = { version = "=${ALOPEX_VERSION}" }
EOF
mkdir -p src
cat > src/main.rs <<'"'"'RS'"'"'
use alopex_sql::Span;
use alopex_sql::catalog::ColumnMetadata;
use alopex_sql::executor::Row;
use alopex_sql::executor::query::aggregate::{
    Accumulator, AvgAccumulator, CountAccumulator, GroupConcatAccumulator, SumAccumulator,
    build_aggregate_schema, execute_parallel_aggregate_rows,
};
use alopex_sql::executor::query::RowIterator;
use alopex_sql::planner::aggregate_expr::AggregateExpr;
use alopex_sql::planner::typed_expr::{TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

struct LocalVecIterator {
    rows: std::vec::IntoIter<Row>,
    schema: Vec<ColumnMetadata>,
}

impl LocalVecIterator {
    fn new(rows: Vec<Row>, schema: Vec<ColumnMetadata>) -> Self {
        Self {
            rows: rows.into_iter(),
            schema,
        }
    }
}

impl RowIterator for LocalVecIterator {
    fn next_row(&mut self) -> Option<alopex_sql::executor::Result<Row>> {
        self.rows.next().map(Ok)
    }

    fn schema(&self) -> &[ColumnMetadata] {
        &self.schema
    }
}

fn column(index: usize, name: &str, ty: ResolvedType) -> TypedExpr {
    TypedExpr {
        kind: TypedExprKind::ColumnRef {
            table: "t".to_string(),
            column: name.to_string(),
            column_index: index,
        },
        resolved_type: ty,
        span: Span::default(),
    }
}

fn main() -> alopex_sql::executor::Result<()> {
    let mut avg_left = AvgAccumulator::new();
    avg_left.update(Some(SqlValue::Integer(2)))?;
    avg_left.update(Some(SqlValue::Integer(4)))?;
    let mut avg_right = AvgAccumulator::new();
    avg_right.update(Some(SqlValue::Integer(6)))?;
    let mut avg_final = AvgAccumulator::new();
    avg_final.merge(&avg_left.state()?)?;
    avg_final.merge(&avg_right.state()?)?;
    assert_eq!(avg_final.finalize()?, SqlValue::Double(4.0));

    let mut count_distinct = CountAccumulator::new(true);
    count_distinct.update(Some(SqlValue::Integer(1)))?;
    count_distinct.update(Some(SqlValue::Integer(1)))?;
    count_distinct.update(Some(SqlValue::Double(1.0)))?;
    assert_eq!(count_distinct.finalize()?, SqlValue::BigInt(2));

    let mut sum_distinct = SumAccumulator::with_distinct(true);
    for value in [
        SqlValue::Integer(10),
        SqlValue::Integer(10),
        SqlValue::Double(5.0),
        SqlValue::Null,
    ] {
        sum_distinct.update(Some(value))?;
    }
    assert_eq!(sum_distinct.finalize()?, SqlValue::Double(15.0));

    let mut concat_distinct = GroupConcatAccumulator::with_distinct("|".to_string(), true);
    for value in [
        SqlValue::Text("a".to_string()),
        SqlValue::Text("a".to_string()),
        SqlValue::Text("b".to_string()),
    ] {
        concat_distinct.update(Some(value))?;
    }
    assert_eq!(concat_distinct.finalize()?, SqlValue::Text("a|b".to_string()));

    let schema = vec![
        ColumnMetadata::new("category", ResolvedType::Text),
        ColumnMetadata::new("amount", ResolvedType::Double),
    ];
    let rows = vec![
        Row::new(0, vec![SqlValue::Text("book".to_string()), SqlValue::Double(10.0)]),
        Row::new(1, vec![SqlValue::Text("game".to_string()), SqlValue::Double(20.0)]),
        Row::new(2, vec![SqlValue::Text("book".to_string()), SqlValue::Double(15.0)]),
        Row::new(3, vec![SqlValue::Text("game".to_string()), SqlValue::Double(5.0)]),
    ];
    let group_keys = vec![column(0, "category", ResolvedType::Text)];
    let amount = column(1, "amount", ResolvedType::Double);
    let aggregates = vec![
        AggregateExpr::count_star(),
        AggregateExpr::sum(amount),
    ];
    let final_schema = build_aggregate_schema(&group_keys, &aggregates);
    let parallel_rows = execute_parallel_aggregate_rows(
        Box::new(LocalVecIterator::new(rows, schema)),
        group_keys,
        aggregates,
        None,
        final_schema,
        2,
    )?;
    assert_eq!(parallel_rows.len(), 2);
    let mut totals = parallel_rows
        .into_iter()
        .map(|row| (format!("{:?}", row.values[0]), row.values[2].clone()))
        .collect::<Vec<_>>();
    totals.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(totals[0].1, SqlValue::Double(25.0));
    assert_eq!(totals[1].1, SqlValue::Double(25.0));

    println!("aggregate regression guarantee passed");
    Ok(())
}
RS
cargo run --quiet
'

run_step "v${ALOPEX_VERSION} SQL scalar/PRAGMA 動作保証" \
    "crates.io/PyPI から取得した v${ALOPEX_VERSION} の CLI で、ハッシュ・UUID・エンコード・文字列関数と PRAGMA の公開利用経路を確認する。ソースの cargo build は行わず、インストール済みの alopex CLI だけを実行する。" \
    -- run_in_container bash -c 'ALOPEX_CLI=alopex bash scripts/demo/v074/demo_sql_v074.sh'

echo ""
log_ok "全デモスクリプトが公開版 v${ALOPEX_VERSION} で完走しました。"

write_report_and_maybe_pr
