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
# 実行結果は JSON と Markdown に保存するが、このスクリプト自身は push しない。
# 公開は .github/workflows/public-release-verification.yml の明示的な成功時ジョブが
# 担当する。失敗結果は Actions artifact として保持し、一般向け保証書へ自動公開
# しない。--report-only では保存済み JSON から再検証なしで Markdown を再生成する。
#
# 新しいステップを追加する場合は run_step 呼び出しに DESCRIPTION も
# 必ず添える(結果一覧だけのステップを増やさない)。
#
# Usage:
#   ./scripts/release/verify-release/run.sh [ALOPEX_VERSION] [--no-report]
#       [--results-file PATH] [--report-dir DIR]
#   ./scripts/release/verify-release/run.sh --report-only RESULTS.json
#       [--report-dir DIR]
#   ./scripts/release/verify-release/run.sh --verify-join candidate.json
#   例: ./scripts/release/verify-release/run.sh 0.8.5
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
ALOPEX_VERSION="0.8.5"
DO_REPORT=1
JOIN_FILE=""
REPORT_ONLY_FILE=""
RESULTS_FILE=""
REPORT_OUTPUT_DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-report) DO_REPORT=0; shift ;;
        --results-file)
            [[ $# -ge 2 ]] || { echo "--results-file requires a path" >&2; exit 64; }
            RESULTS_FILE="$2"
            shift 2
            ;;
        --report-dir)
            [[ $# -ge 2 ]] || { echo "--report-dir requires a directory" >&2; exit 64; }
            REPORT_OUTPUT_DIR="$2"
            shift 2
            ;;
        --report-only)
            [[ $# -ge 2 ]] || { echo "--report-only requires a result JSON path" >&2; exit 64; }
            REPORT_ONLY_FILE="$2"
            shift 2
            ;;
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

REPORT_OUTPUT_DIR="${REPORT_OUTPUT_DIR:-${REPO_ROOT}/release-verification-output}"
RESULTS_FILE="${RESULTS_FILE:-${REPORT_OUTPUT_DIR}/v${ALOPEX_VERSION}.json}"

if [[ -n "${REPORT_ONLY_FILE}" ]]; then
    python3 "${SCRIPT_DIR}/report.py" render \
        --results "${REPORT_ONLY_FILE}" --output-dir "${REPORT_OUTPUT_DIR}"
    exit 0
fi

IMAGE_TAG="alopex-verify-release:${ALOPEX_VERSION}"
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
    if not isinstance(surface.get("run_id"), str) or not surface["run_id"]:
        fail(f"{surface_name} workflow run identity is missing")
    head_sha = surface.get("head_sha")
    if not isinstance(head_sha, str) or not sha40.fullmatch(head_sha):
        fail(f"{surface_name} workflow head SHA is missing or invalid")
    if surface_name == "core":
        # A repair-forward core run may execute from a CI-fix branch.  The
        # published release envelope still has to bind its source to the
        # reviewed main SHA; keep both identities in the evidence.
        source_sha = surface.get("source_sha", head_sha)
        if source_sha != reviewed:
            fail("core release source SHA does not match reviewed main SHA")
    elif head_sha != reviewed:
        fail("python workflow head SHA does not match reviewed main SHA")
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
        python3 "${SCRIPT_DIR}/report.py" record --results "${RESULTS_FILE}" \
            --name "${name}" --status ok --description "${description}" --log "${logfile}"
    else
        log_fail "${name} 失敗(exit ${status})"
        OVERALL_STATUS="fail"
        python3 "${SCRIPT_DIR}/report.py" record --results "${RESULTS_FILE}" \
            --name "${name}" --status fail --description "${description}" --log "${logfile}"
        write_report
        exit "${status}"
    fi
}

# ログファイルから末尾 N 行を Markdown コードブロックとして整形する。
write_report() {
    if [ "${DO_REPORT}" -eq 0 ]; then
        log_info "--no-report 指定により Markdown 生成をスキップします(JSON は保存済み)"
        return 0
    fi
    python3 "${SCRIPT_DIR}/report.py" render \
        --results "${RESULTS_FILE}" --output-dir "${REPORT_OUTPUT_DIR}"
}

rust_version="$(grep -oP '^ARG RUST_VERSION=\K.*' "${SCRIPT_DIR}/Dockerfile")"
nim_image="$(grep -oP '^ARG NIM_IMAGE=\K[^@]*' "${SCRIPT_DIR}/Dockerfile")"
python3 "${SCRIPT_DIR}/report.py" init --results "${RESULTS_FILE}" \
    --version "${ALOPEX_VERSION}" --rust "${rust_version}" --nim "${nim_image}"

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
        -v "${TOOLS_TARGET_DIR}":/tools-target \
        -w /workspace \
        -e "ALOPEX_BINARY_SOURCE=released" \
        -e "ALOPEX_VERSION=${ALOPEX_VERSION}" \
        -e "ALOPEX_EXTRA_PATH=/tools-target/release" \
        "${IMAGE_TAG}" \
        "$@"
}

run_step "verify-release-embedded ビルド" \
    "公開検証用の2つの bin source を一時 crate へコピーし、ALOPEX_VERSION と完全一致する crates.io 公開版 alopex-embedded/alopex-core/alopex-sql だけを依存としてビルドする。固定 Cargo.toml の追随漏れと repository path 混入の双方を防ぐ。" \
    -- run_in_container bash -c '
set -euo pipefail
tool_source="$(mktemp -d)"
trap "rm -rf \"${tool_source}\"" EXIT
mkdir -p "${tool_source}/src/bin"
cp crates/alopex-tools/src/bin/verify_release_embedded.rs "${tool_source}/src/bin/"
cp crates/alopex-tools/src/bin/demo_v085_embedded.rs "${tool_source}/src/bin/"
cp crates/alopex-tools/build.rs "${tool_source}/"
cat >"${tool_source}/Cargo.toml" <<EOF
[workspace]

[package]
name = "alopex-release-verifier"
version = "0.0.0"
edition = "2024"
publish = false

[[bin]]
name = "verify-release-embedded"
path = "src/bin/verify_release_embedded.rs"

[[bin]]
name = "demo-v085-embedded"
path = "src/bin/demo_v085_embedded.rs"

[dependencies]
serde_json = "1.0"
alopex-embedded = { version = "=${ALOPEX_VERSION}" }
alopex-core = { version = "=${ALOPEX_VERSION}" }
alopex-sql = { version = "=${ALOPEX_VERSION}" }
EOF
CARGO_TARGET_DIR=/tools-target cargo build --manifest-path "${tool_source}/Cargo.toml" --release
'

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

run_step "v${ALOPEX_VERSION} v0.8 SQL correctness (demo_sql_v08.py)" \
    "PyPI 公開版で、v0.8 系の TIMESTAMP 書込み、数値型昇格、SUM(INTEGER)、CAST、IN/BETWEEN、異種数値 JOIN、重複 range-variable 拒否を実行し、値とエラー型を確認する。" \
    -- run_in_container python3 scripts/demo/v08/demo_sql_v08.py

run_step "v${ALOPEX_VERSION} 組み込み API サーフェス (demo_api_surfaces.py)" \
    "PyPI 公開版の Python バインディングから SQL を実行する経路を実演する。Database.new()(SF-MEM)/ Database.open(path)(SF-FILE)でのコーパス実行と再オープン、Transaction の commit/rollback、execute_sql_stream() の反復取得、統計関数と PRAGMA を Python から実行する。最後に CLI/HTTP/gRPC/Rust API/Python API の 5 経路が同一コーパスに対して同一の正規化結果を返すことを表示する。従来の mode-parity(4 経路)に Python API を加えた確認である。" \
    -- run_in_container python3 scripts/demo/v074/demo_api_surfaces.py

run_step "v${ALOPEX_VERSION} ベクトル検索 API (demo_vector_api.py)" \
    "PyPI 公開版の Python バインディングから、SQL 経由とネイティブ API の両方でベクトル検索を実行する。API 不在時だけ issue #82 を明記して SKIP とし、存在時は全メソッドを呼び出して L2 距離と node_count を表示する。" \
    -- run_in_container python3 scripts/demo/v074/demo_vector_api.py

run_step "v${ALOPEX_VERSION} Embedded API 全シナリオ" \
    "crates.io 公開版 alopex-embedded/alopex-core/alopex-sql だけでビルドした専用バイナリを使い、保存・KV/transaction・local SQL 全カテゴリ・catalog/cluster 診断・owned/SQL stream・DataFrame/columnar・Vector/HNSW・large value・fail-closed 境界の10シナリオを Rust Embedded API から自己検証付きで実演する。外部 cluster、Python 専用 API、未 provision の V08 segment、default feature 外 S3 は成功に偽装せず明示的な境界として確認する。" \
    -- run_in_container bash scripts/demo/v08/demo_embedded_v085.sh

echo ""
log_ok "全デモスクリプトが公開版 v${ALOPEX_VERSION} で完走しました。"

write_report
