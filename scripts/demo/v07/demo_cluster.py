#!/usr/bin/env python3
"""シナリオ D2: cluster status のクロスサーフェス実証。

docs-public/specs/alopex-v07-feature-demo-spec.md「シナリオ D2」:

| 場 | 操作 | 検証 |
|----|------|------|
| 1 | 既定設定(single_node)でサーバー起動 | status の cluster.mode が single_node、degraded=false |
| 2 | [cluster] mode=cluster_aware(identity 明示、単一メンバー)で起動 | mode / identity / membership が設定値と一致。CLI 表示が HTTP と一致 |
| 3 | leave -> status 観測 -> join -> status 観測 | lifecycle_state が leaving -> active、両サーフェスで観測一致 |
| 4 | membership_source_available=false で起動 | mode=cluster_aware のまま degraded=true、診断に chirps 不可 |

- 各場の検証は HTTP レスポンス(JSON)を正とし、CLI 出力はフィールド射影の
  一致で検証する。
- Python の Database.cluster_status() は静的プレースホルダ実装(issue #35)で
  あるため本デモの検証対象に含めない(main 冒頭で注記表示する)。

exit code: 成功 0 / 検証不一致 1 / 環境・起動エラー 2
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _v07 import (  # noqa: E402
    ADMIN_CLUSTER_LEAVE_PATH,
    ADMIN_HEALTH_PATH,
    ADMIN_STATUS_PATH,
    EXIT_ENV,
    EXIT_MISMATCH,
    EXIT_OK,
    DemoFailure,
    EnvError,
    ServerHandle,
    banner,
    bool_text,
    check,
    find_product_binary,
    http_json,
    install_cleanup_handlers,
    note,
    parse_cli_table,
    repo_root,
    run_cli,
    start_server,
    write_cli_server_profile,
)

#: 場 2〜4 で使う固定 identity(仕様は明示設定を要求する。値は任意)。
NODE_ID = "v07-demo-node-1"
CLUSTER_ID = "v07-demo-cluster"

#: CLI `server status` の表示列 -> HTTP レスポンスの対応フィールド射影
#: (crates/alopex-cli/src/output/server.rs status_columns() /
#:  crates/alopex-cli/src/commands/server.rs ClusterDisplayFields)。
#: Uptime / Connections / QPS は呼び出しごとに変わり得るため射影に含めない。


def cluster_projection(status_body: Dict[str, Any]) -> Dict[str, str]:
    """HTTP GET /api/admin/status の応答から CLI 表示への射影を作る。"""
    cluster = status_body["cluster"]
    routing = cluster["routing_capabilities"]
    codes = ",".join(d["code"] for d in cluster.get("diagnostics", []))
    return {
        "Version": status_body.get("version") or "N/A",
        "Cluster Schema": str(cluster["schema_version"]),
        "Cluster Mode": cluster["mode"],
        "Node ID": cluster["identity"]["node_id"],
        "Lifecycle": cluster["identity"]["lifecycle_state"],
        "Degraded": bool_text(cluster["degraded"]),
        "Local Only": bool_text(routing["local_only"]),
        "Future Distributed": bool_text(routing["future_distributed_execution_required"]),
        "Scatter/Gather": bool_text(routing["scatter_gather_simulated"]),
        "Diagnostics": codes or "N/A",
    }


def get_status(server: ServerHandle) -> Dict[str, Any]:
    code, body = http_json("GET", server.http_base + ADMIN_STATUS_PATH)
    if code != 200:
        raise DemoFailure(f"GET /api/admin/status が {code} を返した: {body}")
    return body


def cli_status_row(cli_bin: Path, repo: Path, home: Path) -> Dict[str, str]:
    result = run_cli(cli_bin, repo, home, ["server", "status"])
    if result.returncode != 0:
        raise EnvError(
            f"CLI server status 失敗 (exit={result.returncode}):\n{result.stderr}"
        )
    rows = parse_cli_table(result.stdout)
    if len(rows) != 1:
        raise DemoFailure(f"CLI server status の行数が 1 でない: {rows}")
    return rows[0]


def assert_cli_matches_http(
    label: str,
    status_body: Dict[str, Any],
    cli_row: Dict[str, str],
) -> None:
    """HTTP を正とし、CLI 表示のフィールド射影一致を検証する。"""
    projection = cluster_projection(status_body)
    for column, expected in projection.items():
        actual = cli_row.get(column)
        if actual != expected:
            raise DemoFailure(
                f"{label}: CLI 列 {column!r} が HTTP 射影と不一致\n"
                f"    HTTP 射影: {expected!r}\n    CLI 表示:  {actual!r}"
            )
    print(f"  ✔ {label}: CLI 表示 {len(projection)} フィールドが HTTP 射影と一致")
    print(f"     ({', '.join(projection)})")


def show_membership(cluster: Dict[str, Any]) -> None:
    membership = cluster["membership"]
    print(f"  membership.source = {membership['source']}")
    for member in membership["members"]:
        identity = member["identity"]
        print(
            f"    - node_id={identity['node_id']}"
            f" derived_state={member['derived_state']}"
            f" transition_reason={member.get('transition_reason')}"
        )


# ---------------------------------------------------------------------------
# 各場
# ---------------------------------------------------------------------------


def scene1_single_node(server_bin: Path, repo: Path, scratch: Path) -> None:
    banner(1, "既定設定(single_node)でサーバー起動")
    with start_server(
        server_bin,
        repo=repo,
        work_dir=scratch / "scene1",
        data_dir=scratch / "scene1" / "data",
        cluster_toml=None,  # [cluster] 省略 = 既定(mode = single_node)
    ) as server:
        print(f"  server ready: {server.http_base}")
        body = get_status(server)
        cluster = body["cluster"]
        check("cluster.mode", "single_node", cluster["mode"])
        check("cluster.degraded", False, cluster["degraded"])
        show_membership(cluster)
    print("  server stopped")


def scene2_cluster_aware(
    server_bin: Path,
    cli_bin: Path,
    repo: Path,
    scratch: Path,
) -> ServerHandle:
    """場 2 のサーバーは場 3 でも使うため停止せずに返す。"""
    banner(2, "cluster_aware(identity 明示・単一メンバー)で起動し、両サーフェスで観測")
    advertised = "127.0.0.1:0"  # 起動前にポート未定のため固定表記(値は任意)
    server = start_server(
        server_bin,
        repo=repo,
        work_dir=scratch / "scene2",
        data_dir=scratch / "scene2" / "data",
        cluster_toml=[
            'mode = "cluster_aware"',
            f'node_id = "{NODE_ID}"',
            f'cluster_id = "{CLUSTER_ID}"',
            f'advertised_endpoint = "{advertised}"',
        ],
    )
    try:
        print(f"  server ready: {server.http_base}")
        body = get_status(server)
        cluster = body["cluster"]
        identity = cluster["identity"]

        print("  -- HTTP(正): mode / identity / membership が設定値と一致 --")
        check("cluster.mode", "cluster_aware", cluster["mode"])
        check("identity.node_id", NODE_ID, identity["node_id"])
        check("identity.cluster_id", CLUSTER_ID, identity["cluster_id"])
        check("identity.advertised_endpoint", advertised, identity["advertised_endpoint"])
        check("identity.role(既定 gateway)", "gateway", identity["role"])
        check("identity.lifecycle_state", "active", identity["lifecycle_state"])
        check("cluster.degraded", False, cluster["degraded"])

        members = cluster["membership"]["members"]
        check("membership.members 件数(自ノードのみ)", 1, len(members))
        check("membership.members[0].node_id", NODE_ID, members[0]["identity"]["node_id"])
        check("membership.source(既定 chirps)", "chirps", cluster["membership"]["source"])
        show_membership(cluster)

        print("  -- CLI: `alopex server status` のフィールド射影一致 --")
        home = scratch / "cli-home"
        write_cli_server_profile(home, server.http_base)
        row = cli_status_row(cli_bin, repo, home)
        assert_cli_matches_http("場 2", body, row)
        return server
    except BaseException:
        server.stop()
        raise


def scene3_membership_lifecycle(
    server: ServerHandle,
    cli_bin: Path,
    repo: Path,
    scratch: Path,
) -> None:
    banner(3, "membership lifecycle: leave -> 観測 -> join -> 観測")
    home = scratch / "cli-home"

    print("  -- POST /api/admin/cluster/leave --")
    code, body = http_json("POST", server.http_base + ADMIN_CLUSTER_LEAVE_PATH, payload={})
    if code != 200:
        raise DemoFailure(f"cluster/leave が {code} を返した: {body}")
    check("leave 応答 action", "leave", body["action"])
    check(
        "leave 応答 identity.lifecycle_state",
        "leaving",
        body["cluster"]["identity"]["lifecycle_state"],
    )

    print("  -- leave 後の観測(HTTP 正 + CLI 射影) --")
    status_body = get_status(server)
    check(
        "status identity.lifecycle_state",
        "leaving",
        status_body["cluster"]["identity"]["lifecycle_state"],
    )
    show_membership(status_body["cluster"])
    row = cli_status_row(cli_bin, repo, home)
    check("CLI Lifecycle 列", "leaving", row.get("Lifecycle"))
    assert_cli_matches_http("場 3 (leave 後)", status_body, row)

    print("  -- CLI `alopex server join`(サーフェスを替えて join) --")
    result = run_cli(cli_bin, repo, home, ["server", "join"])
    if result.returncode != 0:
        raise EnvError(f"CLI server join 失敗 (exit={result.returncode}):\n{result.stderr}")
    join_rows = parse_cli_table(result.stdout)
    if len(join_rows) != 1:
        raise DemoFailure(f"CLI server join の行数が 1 でない: {join_rows}")
    check("CLI join 行 Action", "join", join_rows[0].get("Action"))
    check("CLI join 行 Lifecycle", "active", join_rows[0].get("Lifecycle"))

    print("  -- join 後の観測(HTTP 正 + CLI 射影) --")
    status_body = get_status(server)
    check(
        "status identity.lifecycle_state",
        "active",
        status_body["cluster"]["identity"]["lifecycle_state"],
    )
    show_membership(status_body["cluster"])
    row = cli_status_row(cli_bin, repo, home)
    check("CLI Lifecycle 列", "active", row.get("Lifecycle"))
    assert_cli_matches_http("場 3 (join 後)", status_body, row)


def scene4_degraded_fallback(server_bin: Path, repo: Path, scratch: Path) -> None:
    banner(4, "membership_source_available=false で起動(degraded フォールバック)")
    with start_server(
        server_bin,
        repo=repo,
        work_dir=scratch / "scene4",
        data_dir=scratch / "scene4" / "data",
        cluster_toml=[
            'mode = "cluster_aware"',
            f'node_id = "{NODE_ID}"',
            f'cluster_id = "{CLUSTER_ID}"',
            'advertised_endpoint = "127.0.0.1:0"',
            "membership_source_available = false",
        ],
    ) as server:
        print(f"  server ready: {server.http_base}")
        body = get_status(server)
        cluster = body["cluster"]
        check("cluster.mode(cluster_aware のまま)", "cluster_aware", cluster["mode"])
        check("cluster.degraded", True, cluster["degraded"])

        codes = [d["code"] for d in cluster.get("diagnostics", [])]
        if "chirps_unavailable" not in codes:
            raise DemoFailure(
                f"診断に chirps 不可 (chirps_unavailable) が無い: codes={codes}"
            )
        print(f"  ✔ diagnostics codes = {codes}")
        for diagnostic in cluster["diagnostics"]:
            print(f"     - [{diagnostic['code']}] {diagnostic['message']}")
            print(f"       remediation: {diagnostic['remediation']}")

        print("  -- GET /api/admin/health も degraded を報告する --")
        code, health = http_json("GET", server.http_base + ADMIN_HEALTH_PATH)
        if code != 200:
            raise DemoFailure(f"admin/health が {code} を返した: {health}")
        check("health.status", "degraded", health["status"])
        check("health.degraded", True, health["degraded"])
    print("  server stopped")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    del argv
    install_cleanup_handlers()
    repo = repo_root()

    print("v0.7 機能デモ D2: cluster status のクロスサーフェス実証")
    note(
        "Python の Database.cluster_status() は静的プレースホルダ実装"
        "(issue #35)のため、本デモの検証対象に含めない。"
    )

    try:
        server_bin = find_product_binary(repo, "alopex-server")
        cli_bin = find_product_binary(repo, "alopex")

        with tempfile.TemporaryDirectory(prefix="v07-demo-cluster-") as tmp:
            scratch = Path(tmp)
            scene1_single_node(server_bin, repo, scratch)
            server = scene2_cluster_aware(server_bin, cli_bin, repo, scratch)
            try:
                scene3_membership_lifecycle(server, cli_bin, repo, scratch)
            finally:
                server.stop()
                print("  server stopped")
            scene4_degraded_fallback(server_bin, repo, scratch)
    except DemoFailure as exc:
        print(f"\n検証不一致: {exc}", file=sys.stderr)
        return EXIT_MISMATCH
    except EnvError as exc:
        print(f"\n環境エラー: {exc}", file=sys.stderr)
        return EXIT_ENV

    print()
    print("=" * 72)
    print("デモ完了: 場 1〜4 PASS(検証対象外の注記: issue #35 の Python アクセサ)")
    print("=" * 72)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
