#!/usr/bin/env python3
"""シナリオ D3: ルーティング透明性。

docs-public/specs/alopex-v07-feature-demo-spec.md「シナリオ D3」:

| 場 | 操作 | 検証 |
|----|------|------|
| 1 | cluster_aware サーバーへ SQL 実行し、応答の routing 診断を表示 | decision が local_only、reason が single_resolved_target または placement_absent |
| 2 | 検証コンテナで cargo test -p alopex-cluster --test simulated_harness | scatter-gather 判定・retry 境界・cancellation 記録・idempotency key 安定性の全テストが green |

- 場 2 は「シミュレーションハーネスの契約検証」であり、分散実行の実証では
  ない(スクリプトはこの区別を明示表示する)。
- 分散必要時の拒否(future_distributed_execution_required)は、本番サーフェス
  に placement を跨がせる操作用 API が存在しない(設計上の意図)ため、ライブ
  では再現しない。該当挙動を担保するサーバー統合テストへの参照を表示する。
- 検証コンテナ(alopex-parity イメージ)が使えない環境では場 2 を SKIP と
  して明示表示する(成功数に含めない)。

exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _v07 import (  # noqa: E402
    ADMIN_STATUS_PATH,
    BINARY_SOURCE_ENV,
    BINARY_SOURCE_RELEASED,
    EXIT_ENV,
    EXIT_MISMATCH,
    EXIT_OK,
    SQL_PATH,
    DemoFailure,
    EnvError,
    banner,
    check,
    check_in,
    find_product_binary,
    http_json,
    install_cleanup_handlers,
    note,
    repo_root,
    start_server,
)

#: v0.7 のルーティング契約(仕様 D3 場 1)。
EXPECTED_DECISION = "local_only"
ALLOWED_REASONS = ("single_resolved_target", "placement_absent")

#: 分散必要時の拒否を担保するサーバー統合テスト(ライブ再現はしない)。
REJECTION_TESTS = (
    "crates/alopex-server/tests/http_sql_e2e.rs::"
    "non_session_sql_rejects_future_distributed_routing",
    "crates/alopex-server/tests/http_sql_e2e.rs::"
    "future_distributed_write_is_rejected_before_local_execution",
)

#: 場 1 で実行する SQL(コーパス)。
SQL_CORPUS = (
    "CREATE TABLE routing_demo (id INTEGER, name TEXT)",
    "INSERT INTO routing_demo VALUES (1, 'ember')",
    "INSERT INTO routing_demo VALUES (2, 'moss')",
    "SELECT id, name FROM routing_demo ORDER BY id",
)

#: 検証コンテナのイメージ名(scripts/parity/Dockerfile)。
PARITY_IMAGE = "alopex-parity"

#: 場 2 の cargo test(検証コンテナ内で実行)。
HARNESS_TEST = ("cargo", "test", "-p", "alopex-cluster", "--test", "simulated_harness")

#: cargo test の結果集計行。
TEST_RESULT_RE = re.compile(
    r"^test result: (?P<status>ok|FAILED)\. (?P<passed>\d+) passed; (?P<failed>\d+) failed;",
    re.MULTILINE,
)


class SceneSkip(Exception):
    """場の明示的 SKIP(理由必須。成功数に含めない)。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# 場 1: ライブサーバーの routing 診断
# ---------------------------------------------------------------------------


def show_diagnostics(diagnostics: List[Dict[str, Any]]) -> None:
    for diagnostic in diagnostics:
        print(
            f"     - decision={diagnostic['decision']}"
            f" reason={diagnostic['reason']}"
            f" plan_id={diagnostic['plan_id']}"
            f" targets={len(diagnostic.get('targets', []))}"
            f" excluded={len(diagnostic.get('excluded_targets', []))}"
        )


def scene1_live_routing(repo: Path, scratch: Path) -> None:
    banner(1, "cluster_aware サーバーへの SQL 実行と routing 診断の実測")
    server_bin = find_product_binary(repo, "alopex-server")
    with start_server(
        server_bin,
        repo=repo,
        work_dir=scratch / "scene1",
        data_dir=scratch / "scene1" / "data",
        cluster_toml=[
            'mode = "cluster_aware"',
            'node_id = "v07-routing-node-1"',
            'cluster_id = "v07-routing-cluster"',
            'advertised_endpoint = "127.0.0.1:0"',
        ],
    ) as server:
        print(f"  server ready: {server.http_base}")

        code, status_body = http_json("GET", server.http_base + ADMIN_STATUS_PATH)
        if code != 200:
            raise DemoFailure(f"GET /api/admin/status が {code} を返した: {status_body}")
        capabilities = status_body["cluster"]["routing_capabilities"]
        print(f"  routing_capabilities = {capabilities}")

        select_rows: Optional[List[Any]] = None
        for sql in SQL_CORPUS:
            print(f"\n  sql> {sql}")
            code, body = http_json(
                "POST",
                server.http_base + SQL_PATH,
                {"sql": sql, "session_id": None, "streaming": False},
            )
            if code != 200:
                raise DemoFailure(f"SQL 実行が HTTP {code} を返した: {body}")
            diagnostics = body.get("routing_diagnostics") or []
            if not diagnostics:
                raise DemoFailure(
                    f"応答に routing_diagnostics が無い(すべての SQL 実行が"
                    f" ルーティング判定を通る契約に反する): {sql!r}"
                )
            show_diagnostics(diagnostics)
            for diagnostic in diagnostics:
                check("  decision", EXPECTED_DECISION, diagnostic["decision"])
                check_in("  reason", ALLOWED_REASONS, diagnostic["reason"])
            if sql.lstrip().upper().startswith("SELECT"):
                select_rows = body.get("rows")

        # ルーティングが local_only で実際にローカル実行された傍証として、
        # SELECT が挿入済みの 2 行を返すことを確認する。
        if select_rows is None:
            raise DemoFailure("SELECT の応答が記録されていない")
        check("SELECT の結果行数(ローカル実行の傍証)", 2, len(select_rows))

    print("  server stopped")
    note(
        "分散必要時の拒否 (future_distributed_execution_required) は、本番"
        "サーフェスに placement を跨がせる操作用 API が存在しない(設計上の"
        "意図)ため、ライブでは再現しない。該当挙動は次のサーバー統合テストが"
        "担保する:"
    )
    for reference in REJECTION_TESTS:
        print(f"      - {reference}")


# ---------------------------------------------------------------------------
# 場 2: 検証コンテナでのシミュレーションハーネス契約検証
# ---------------------------------------------------------------------------


def resolve_chirps_dir(repo: Path) -> Path:
    """chirps リポジトリの場所(alopex-cluster のパス依存 ../../../../chirps)。

    環境変数 ALOPEX_CHIRPS_DIR で上書きできる。
    """
    override = os.environ.get("ALOPEX_CHIRPS_DIR")
    if override:
        return Path(override)
    return repo.parents[1] / "chirps"


def preflight_container(repo: Path) -> Path:
    """場 2 の前提を検査する。満たさなければ SceneSkip(理由付き)。"""
    if shutil.which("docker") is None:
        raise SceneSkip("docker コマンドが無い")
    probe = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if probe.returncode != 0:
        raise SceneSkip(f"docker デーモンに接続できない: {probe.stderr.strip()}")
    image = subprocess.run(
        ["docker", "image", "inspect", PARITY_IMAGE],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if image.returncode != 0:
        raise SceneSkip(
            f"検証コンテナイメージ {PARITY_IMAGE} が無い。"
            " `docker build -t alopex-parity -f scripts/parity/Dockerfile"
            " scripts/parity` でビルドすること"
        )
    chirps = resolve_chirps_dir(repo)
    if not chirps.is_dir():
        raise SceneSkip(
            f"chirps リポジトリが無い: {chirps}"
            "(alopex-cluster のパス依存のため /chirps 読み取り専用マウントが必須。"
            " ALOPEX_CHIRPS_DIR で場所を指定できる)"
        )
    return chirps


def scene2_simulated_harness(repo: Path) -> int:
    """検証コンテナで simulated_harness を実行し、passed 数を返す。"""
    banner(2, "検証コンテナでのシミュレーションハーネス契約検証 (cargo test)")
    note(
        "本場は retry / backoff / idempotency / cancellation / scatter-gather"
        " 判定という「シミュレーションハーネスの契約」の検証であり、分散実行の"
        "実証ではない(v0.7 のライブ実行は local_only)。"
    )
    chirps = preflight_container(repo)

    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "-e",
        "HOME=/home/parity",
        "-v",
        f"{repo}:/workspace",
        "-v",
        f"{chirps}:/chirps:ro",
        "-w",
        "/workspace",
        PARITY_IMAGE,
        *HARNESS_TEST,
    ]
    print(f"  実行: {' '.join(HARNESS_TEST)} (コンテナ {PARITY_IMAGE} 内)")
    print(f"  マウント: {repo} -> /workspace, {chirps} -> /chirps (ro)")

    # cargo の逐次実行保証: parity 実行系(scripts/parity/runner/surfaces.py の
    # run_cargo)と同じロックファイルで、ホスト・コンテナを跨いだ cargo の
    # 多重起動を禁止する。
    lock_dir = repo / "target"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with open(lock_dir / ".parity-cargo.lock", "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=1800,
            )
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)

    combined = result.stdout + result.stderr
    # テスト結果(stdout)を優先して表示する(stderr はコンパイル進行ログ)。
    shown = result.stdout.strip() or result.stderr
    tail = "\n".join(shown.splitlines()[-25:])
    print("  --- cargo test 出力(テスト結果、末尾) ---")
    for line in tail.splitlines():
        print(f"  | {line}")

    if result.returncode != 0:
        raise DemoFailure(
            f"simulated_harness の cargo test が失敗 (exit={result.returncode})"
        )
    summaries = list(TEST_RESULT_RE.finditer(combined))
    if not summaries:
        raise DemoFailure("cargo test の結果集計行 (test result:) が見つからない")
    passed = 0
    for summary in summaries:
        if summary.group("status") != "ok" or summary.group("failed") != "0":
            raise DemoFailure(f"テスト集計が green でない: {summary.group(0)}")
        passed += int(summary.group("passed"))
    if passed < 1:
        raise DemoFailure("simulated_harness の実行テスト数が 0(契約検証になっていない)")
    print(f"  ✔ simulated_harness: 全テスト green ({passed} passed)")
    return passed


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    del argv
    install_cleanup_handlers()
    repo = repo_root()

    print("v0.7 機能デモ D3: ルーティング透明性")

    scene2_result: Optional[str] = None
    try:
        with tempfile.TemporaryDirectory(prefix="v07-demo-routing-") as tmp:
            scene1_live_routing(repo, Path(tmp))
        if os.environ.get(BINARY_SOURCE_ENV) == BINARY_SOURCE_RELEASED:
            note(
                "場 2 は repository source の simulated_harness unit test であり、"
                "公開パッケージのみを対象にする release verification には含めない。"
            )
            scene2_result = "N/A (source-only unit contract)"
        else:
            try:
                passed = scene2_simulated_harness(repo)
                scene2_result = f"PASS ({passed} passed)"
            except SceneSkip as skip:
                print(f"  SKIP: {skip.reason}")
                print("        スキップを完了と偽らない(成功数に含めない)。")
                scene2_result = f"SKIP ({skip.reason})"
    except DemoFailure as exc:
        print(f"\n検証不一致: {exc}", file=sys.stderr)
        return EXIT_MISMATCH
    except (EnvError, subprocess.TimeoutExpired) as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print(f"デモ完了: 場 1 PASS / 場 2 {scene2_result}")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
