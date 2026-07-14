"""v0.7 機能デモの共通実行系。

docs-public/specs/alopex-v07-feature-demo-spec.md「実行系」に従う:

- ランナーはすべて Python スクリプト(標準ライブラリのみで動作する)。
  検証目的のコンパイル済みバイナリは追加しない。
- exit code 規約: 成功 0 / 検証不一致 1 / 環境・起動エラー 2。
- サーバーはヘルスチェック(admin ポート GET /healthz)のポーリングで
  ready を確認してから検証し、終了時(異常終了含む)に確実に停止する。
  ポートは動的割り当てとする。
- SKIP・注記は明示的に表示し、成功数に含めない。

本モジュールは scripts/parity/runner/surfaces.py と同じ規約
(動的ポート、ready ポーリング、SIGTERM→SIGKILL 停止)を採るが、
並行して整備が進む parity 実行系へ依存しないよう自己完結で実装する。
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

EXIT_OK = 0
EXIT_MISMATCH = 1
EXIT_ENV = 2

#: 管理ポートのヘルスチェックパス(crates/alopex-server/src/http/admin.rs)。
HEALTH_PATH = "/healthz"

#: メイン HTTP ポート上の SQL 実行パス(crates/alopex-server/src/http/mod.rs)。
SQL_PATH = "/api/sql/query"

#: メイン HTTP ポート上の管理 API パス(同上)。
ADMIN_STATUS_PATH = "/api/admin/status"
ADMIN_HEALTH_PATH = "/api/admin/health"
ADMIN_CLUSTER_JOIN_PATH = "/api/admin/cluster/join"
ADMIN_CLUSTER_LEAVE_PATH = "/api/admin/cluster/leave"

#: CLI サーバープロファイル名(検証用の一時 HOME 配下にのみ書く)。
CLI_PROFILE = "v07demo"


class DemoFailure(Exception):
    """検証不一致(exit 1)。"""


class EnvError(Exception):
    """環境・起動エラー(exit 2)。"""


# ---------------------------------------------------------------------------
# パス・環境
# ---------------------------------------------------------------------------


def repo_root() -> Path:
    """リポジトリルート(scripts/demo/v07/ から 3 階層上)。"""
    return Path(__file__).resolve().parents[3]


def nim_parser_dir(repo: Path) -> Path:
    return repo / "crates" / "alopex-sql" / "nim-sql-parser"


def product_env(repo: Path) -> Dict[str, str]:
    """製品バイナリ実行用の環境変数。

    Nim 共有ライブラリ(``nimble lib`` の生成物)を解決できるよう、
    CI (.github/workflows/ci.yml) と同じく nim-sql-parser ディレクトリを
    LD_LIBRARY_PATH の先頭へ載せる。
    """
    env = dict(os.environ)
    nim_dir = str(nim_parser_dir(repo))
    current = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{nim_dir}:{current}" if current else nim_dir
    return env


def find_product_binary(repo: Path, name: str) -> Path:
    """製品バイナリ(alopex / alopex-server)を target から探す。

    release を優先し、無ければ debug を使う。どちらにも無ければ環境エラー。
    デモから cargo を起動しない(ビルドの逐次実行はビルド担当者の責務。
    scripts/parity/runner/surfaces.py の run_cargo 規約を参照)。
    """
    for profile in ("release", "debug"):
        path = repo / "target" / profile / name
        if path.is_file():
            return path
    raise EnvError(
        f"製品バイナリ {name} が target/release にも target/debug にも無い。"
        f" 先に `cargo build --release -p alopex-cli -p alopex-server` を"
        f" 逐次(flock 経由)で実行すること。"
    )


def find_free_port() -> int:
    """空きポートを動的に割り当てる。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------------------
# HTTP(標準ライブラリのみ)
# ---------------------------------------------------------------------------


def http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Tuple[int, Any]:
    """JSON リクエストを送り (status, parsed_body) を返す。

    接続不能は環境エラー(EnvError)。HTTP エラー応答は (code, body) として
    返し、判定は呼び出し側で行う。
    """
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except ValueError:
            parsed = {"raw": body}
        return exc.code, parsed
    except (urllib.error.URLError, OSError) as exc:
        raise EnvError(f"HTTP 接続失敗 ({method} {url}): {exc}") from exc


def http_ok(url: str, timeout: float = 1.0) -> bool:
    """GET が 200 を返すかどうか(ヘルスポーリング用)。"""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


# ---------------------------------------------------------------------------
# サーバー起動管理(alopex-server)
# ---------------------------------------------------------------------------

#: 起動中サーバーの登録簿。atexit / シグナルからの一括停止に使う。
_ACTIVE_SERVERS: List["ServerHandle"] = []


def cleanup_active_servers() -> None:
    for handle in list(_ACTIVE_SERVERS):
        try:
            handle.stop()
        except Exception:  # noqa: BLE001 - 掃除は最後まで続行する
            pass


def install_cleanup_handlers() -> None:
    """異常終了(SIGINT / SIGTERM)を含め、確実にサーバーを停止する。"""
    atexit.register(cleanup_active_servers)

    def _handler(signum: int, _frame: Any) -> None:
        cleanup_active_servers()
        sys.exit(EXIT_ENV)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handler)


def _tail(text: str, lines: int = 30) -> str:
    return "\n".join(text.splitlines()[-lines:])


@dataclass(eq=False)
class ServerHandle:
    """起動済み alopex-server のハンドル(確実な停止を担う)。"""

    process: subprocess.Popen
    http_base: str
    admin_base: str
    grpc_target: str
    work_dir: Path
    _log_files: List[Any] = field(default_factory=list)

    def stop(self, timeout: float = 10.0) -> None:
        """SIGTERM -> 待機 -> SIGKILL で確実に停止する(冪等)。"""
        try:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=timeout)
        finally:
            for fh in self._log_files:
                try:
                    fh.close()
                except OSError:
                    pass
            self._log_files.clear()
            if self in _ACTIVE_SERVERS:
                _ACTIVE_SERVERS.remove(self)

    def stderr_tail(self, lines: int = 30) -> str:
        path = self.work_dir / "server-stderr.log"
        if path.is_file():
            return _tail(path.read_text(encoding="utf-8", errors="replace"), lines)
        return ""

    def __enter__(self) -> "ServerHandle":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.stop()


def start_server(
    server_bin: Path,
    *,
    repo: Path,
    work_dir: Path,
    data_dir: Path,
    cluster_toml: Optional[Sequence[str]] = None,
    ready_timeout: float = 60.0,
) -> ServerHandle:
    """alopex-server を動的ポートで起動し、/healthz が ready になるまで待つ。

    - 設定は work_dir/alopex.toml に書き、``--config`` で明示的に渡す。
    - ``cluster_toml`` は ``[cluster]`` セクションの行(キー = 値)。
      None なら既定(mode = single_node)。
    - ready 確認は admin ポートの GET /healthz ポーリング。
    - 停止は ServerHandle.stop()(context manager 推奨)。
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    http_port = find_free_port()
    grpc_port = find_free_port()
    admin_port = find_free_port()

    lines = [
        f'http_bind = "127.0.0.1:{http_port}"',
        f'grpc_bind = "127.0.0.1:{grpc_port}"',
        f'admin_bind = "127.0.0.1:{admin_port}"',
        f'data_dir = "{data_dir}"',
        'auth_mode = { type = "none" }',
    ]
    if cluster_toml is not None:
        lines.append("")
        lines.append("[cluster]")
        lines.extend(cluster_toml)
    config_path = work_dir / "alopex.toml"
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    stdout_log = open(work_dir / "server-stdout.log", "w", encoding="utf-8")
    stderr_log = open(work_dir / "server-stderr.log", "w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            [str(server_bin), "--config", str(config_path)],
            cwd=work_dir,
            env=product_env(repo),
            stdout=stdout_log,
            stderr=stderr_log,
        )
    except OSError as exc:
        stdout_log.close()
        stderr_log.close()
        raise EnvError(f"alopex-server の起動に失敗: {exc}") from exc

    handle = ServerHandle(
        process=process,
        http_base=f"http://127.0.0.1:{http_port}",
        admin_base=f"http://127.0.0.1:{admin_port}",
        grpc_target=f"127.0.0.1:{grpc_port}",
        work_dir=work_dir,
        _log_files=[stdout_log, stderr_log],
    )
    _ACTIVE_SERVERS.append(handle)

    deadline = time.monotonic() + ready_timeout
    health_url = handle.admin_base + HEALTH_PATH
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = handle.stderr_tail()
            handle.stop()
            raise EnvError(
                f"alopex-server が起動直後に終了 (exit={process.returncode}):\n{tail}"
            )
        if http_ok(health_url):
            return handle
        time.sleep(0.2)

    tail = handle.stderr_tail()
    handle.stop()
    raise EnvError(
        f"alopex-server が {ready_timeout}s 以内に ready にならない"
        f" ({health_url}):\n{tail}"
    )


# ---------------------------------------------------------------------------
# CLI(製品バイナリ alopex)のサーバープロファイル実行
# ---------------------------------------------------------------------------


def write_cli_server_profile(home_dir: Path, http_base: str) -> None:
    """一時 HOME 配下に CLI のサーバープロファイルを書く。

    CLI はプロファイル設定を ``$HOME/.alopex/config``(TOML、権限 600 必須)
    から読む(crates/alopex-cli/src/profile/config.rs)。実 HOME を汚さない
    よう、デモ専用の一時 HOME を使う。http スキームのため insecure = true を
    明示する(CLI は警告を stderr に出す)。
    """
    config_dir = home_dir / ".alopex"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config"
    config_path.write_text(
        "\n".join(
            [
                f'default_profile = "{CLI_PROFILE}"',
                "",
                f"[profiles.{CLI_PROFILE}]",
                'connection_type = "server"',
                "",
                f"[profiles.{CLI_PROFILE}.server]",
                f'url = "{http_base}/"',
                "insecure = true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config_path.chmod(0o600)


def run_cli(
    cli_bin: Path,
    repo: Path,
    home_dir: Path,
    args: Sequence[str],
    timeout: float = 60.0,
) -> subprocess.CompletedProcess:
    """CLI をサーバープロファイル + バッチモードで実行する。"""
    env = product_env(repo)
    env["HOME"] = str(home_dir)
    return subprocess.run(
        [str(cli_bin), "--profile", CLI_PROFILE, "--batch", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def parse_cli_table(stdout: str) -> List[Dict[str, str]]:
    """CLI の table 出力(comfy-table UTF8_FULL)を行の辞書列に読む。

    コンテンツ行は ``│``(外枠)で始まり、セルは ``┆``(内側罫線)で
    区切られる。先頭のコンテンツ行がヘッダ。非 TTY 実行では折り返しが
    起きない(列幅は内容で決まる)前提を置く。
    """
    content_rows: List[List[str]] = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("│"):  # │
            cells = [cell.strip() for cell in line.strip("│").split("┆")]
            content_rows.append(cells)
    if len(content_rows) < 2:
        raise EnvError(f"CLI の table 出力を解釈できない:\n{stdout}")
    header = content_rows[0]
    rows = []
    for cells in content_rows[1:]:
        if len(cells) != len(header):
            raise EnvError(
                f"CLI table の列数不一致 header={header} row={cells}:\n{stdout}"
            )
        rows.append(dict(zip(header, cells)))
    return rows


# ---------------------------------------------------------------------------
# 表示・検証ヘルパ
# ---------------------------------------------------------------------------


def banner(scene: int, title: str) -> None:
    print()
    print("=" * 72)
    print(f"場 {scene}: {title}")
    print("=" * 72)


def note(text: str) -> None:
    print(f"  注記: {text}")


def check(label: str, expected: Any, actual: Any) -> None:
    """値の一致検証。合致すれば ✔ 表示、不一致なら DemoFailure。"""
    if expected != actual:
        raise DemoFailure(
            f"{label}: 不一致\n    expected: {expected!r}\n    actual:   {actual!r}"
        )
    print(f"  ✔ {label} = {actual!r}")


def check_in(label: str, allowed: Sequence[Any], actual: Any) -> None:
    """許容集合による検証。"""
    if actual not in allowed:
        raise DemoFailure(
            f"{label}: 許容外\n    allowed: {list(allowed)!r}\n    actual:  {actual!r}"
        )
    print(f"  ✔ {label} = {actual!r} (許容: {list(allowed)!r})")


def bool_text(value: bool) -> str:
    """CLI table のブール表示(true / false)。"""
    return "true" if value else "false"
