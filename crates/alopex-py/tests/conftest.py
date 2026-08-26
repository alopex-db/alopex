from __future__ import annotations

import contextlib
import importlib.util
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

from alopex import Database

#: 実サーバー e2e を必須にする CI 用スイッチ。バイナリ未検出でも skip せず fail
#: させ、レーンが無言で空回りするのを防ぐ（D18）。
REQUIRE_SERVER_ENV = "ALOPEX_REQUIRE_SERVER_E2E"

#: サーバーバイナリの明示指定。
SERVER_BIN_ENV = "ALOPEX_SERVER_BIN"

REPO_ROOT = Path(__file__).resolve().parents[3]


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _has_vector_api() -> bool:
    from alopex import Transaction

    return hasattr(Transaction, "upsert_vector")


def _server_binary() -> Path | None:
    explicit = os.environ.get(SERVER_BIN_ENV)
    if explicit:
        candidate = Path(explicit)
        return candidate if candidate.is_file() else None
    roots = []
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    if target_dir:
        roots.append(Path(target_dir))
    roots.append(REPO_ROOT / "target")
    for root in roots:
        for profile in ("debug", "release"):
            candidate = root / profile / "alopex-server"
            if candidate.is_file():
                return candidate
    return None


def _requires_server_reason() -> str | None:
    if _server_binary() is None:
        return (
            "alopex-server バイナリが見つからない"
            f"（{SERVER_BIN_ENV} か target/{{debug,release}}/alopex-server）"
        )
    return None


def pytest_configure(config):
    config.addinivalue_line("markers", "requires_numpy: numpy が必要なテスト")
    config.addinivalue_line("markers", "requires_polars: polars が必要なテスト")
    config.addinivalue_line(
        "markers", "requires_server: 実 alopex-server バイナリが必要な e2e テスト"
    )


def pytest_runtest_setup(item):
    if "requires_numpy" in item.keywords:
        if not _module_available("numpy") or not _has_vector_api():
            pytest.skip("numpy feature が有効でないためスキップ")
    if "requires_polars" in item.keywords:
        if not _module_available("polars"):
            pytest.skip("polars が未インストールのためスキップ")
    if "requires_server" in item.keywords:
        reason = _requires_server_reason()
        if reason is not None:
            if os.environ.get(REQUIRE_SERVER_ENV) == "1":
                pytest.fail(f"{REQUIRE_SERVER_ENV}=1 だが {reason}")
            pytest.skip(reason)


@pytest.fixture()
def db():
    db = Database.new()
    yield db
    try:
        db.close()
    except Exception:
        pass


@pytest.fixture()
def unique_name():
    return f"test_{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# 実サーバー e2e（scripts/parity/runner/surfaces.py::start_server の移植）
# ---------------------------------------------------------------------------


def _free_ports(count: int) -> list[int]:
    """Reserve `count` distinct loopback ports.

    All sockets are held open at once and released together: binding and closing
    them one at a time lets the kernel hand the same ephemeral port back, and a
    server whose http_bind equals its admin_bind starts serving one listener,
    fails the other with EADDRINUSE, and answers SQL requests with a bare 404.
    A TOCTOU window against other processes remains; _server_fixture retries.
    """
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [int(sock.getsockname()[1]) for sock in sockets]
    finally:
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.close()


def _product_env() -> dict:
    env = os.environ.copy()
    lib_dir = str(REPO_ROOT / "crates" / "alopex-sql" / "nim-sql-parser")
    key = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
    existing = env.get(key, "")
    env[key] = f"{lib_dir}{os.pathsep}{existing}" if existing else lib_dir
    return env


class ServerHandle:
    def __init__(self, process, http_base: str, admin_base: str, work_dir: Path):
        self.process = process
        self.http_base = http_base
        self.admin_base = admin_base
        self.work_dir = work_dir

    def stderr_tail(self, limit: int = 4000) -> str:
        log = self.work_dir / "server-stderr.log"
        if not log.is_file():
            return ""
        return log.read_text(encoding="utf-8", errors="replace")[-limit:]

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                self.process.kill()
                self.process.wait(timeout=10)


def _start_server(binary: Path, work_dir: Path, api_key: str | None) -> ServerHandle:
    http_port, grpc_port, admin_port = _free_ports(3)
    data_dir = work_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    auth = (
        'auth_mode = { type = "none" }'
        if api_key is None
        else f'auth_mode = {{ type = "dev", api_key = "{api_key}" }}'
    )
    config = "\n".join(
        [
            f'http_bind = "127.0.0.1:{http_port}"',
            f'grpc_bind = "127.0.0.1:{grpc_port}"',
            f'admin_bind = "127.0.0.1:{admin_port}"',
            f'data_dir = "{data_dir}"',
            auth,
            "",
        ]
    )
    (work_dir / "alopex.toml").write_text(config, encoding="utf-8")

    stdout_log = (work_dir / "server-stdout.log").open("w", encoding="utf-8")
    stderr_log = (work_dir / "server-stderr.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [str(binary)],
        cwd=str(work_dir),
        env=_product_env(),
        stdout=stdout_log,
        stderr=stderr_log,
    )
    handle = ServerHandle(
        process,
        f"http://127.0.0.1:{http_port}",
        f"http://127.0.0.1:{admin_port}",
        work_dir,
    )

    # 管理ポートだけでなくメインの HTTP ルーターにも到達確認する。admin だけを
    # 見ていると、http_bind の bind に失敗したサーバーを ready と誤認して
    # /sql が素の 404 を返す状態でテストへ渡してしまう。
    # dev auth のときはメインルーターが api_key を要求するので付けて叩く。
    headers = {"x-api-key": api_key} if api_key is not None else {}
    probes = (handle.admin_base + "/healthz", handle.http_base + "/api/admin/health")
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            handle.stop()
            raise RuntimeError(
                f"alopex-server が起動直後に終了 (exit={process.returncode}):\n"
                f"{handle.stderr_tail()}"
            )
        try:
            for probe in probes:
                request = urllib.request.Request(probe, headers=headers)
                with urllib.request.urlopen(request, timeout=1.0) as response:
                    if response.status != 200:
                        raise OSError(f"{probe} -> HTTP {response.status}")
            return handle
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(0.2)

    tail = handle.stderr_tail()
    handle.stop()
    raise RuntimeError(
        f"alopex-server が ready にならない ({', '.join(probes)}):\n{tail}"
    )


def _server_fixture(tmp_path_factory, name: str, api_key: str | None):
    binary = _server_binary()
    assert binary is not None, "requires_server マーカーが binary 不在を弾くはず"
    last_error: BaseException | None = None
    # ポート確保は bind→close→サーバーへ渡す方式なので TOCTOU レースが残る。
    # 起動失敗はポート競合を想定して数回だけ retry する。
    for attempt in range(3):
        work_dir = tmp_path_factory.mktemp(f"{name}-{attempt}")
        try:
            return _start_server(binary, work_dir, api_key)
        except RuntimeError as exc:
            last_error = exc
            time.sleep(0.5)
    raise AssertionError(f"alopex-server を起動できない: {last_error}")


@pytest.fixture(scope="session")
def alopex_server(tmp_path_factory):
    handle = _server_fixture(tmp_path_factory, "alopex-server", None)
    try:
        yield handle
    finally:
        handle.stop()


@pytest.fixture(scope="session")
def alopex_server_api_key():
    return "e2e-secret-key"


@pytest.fixture(scope="session")
def alopex_server_with_auth(tmp_path_factory, alopex_server_api_key):
    handle = _server_fixture(
        tmp_path_factory, "alopex-server-auth", alopex_server_api_key
    )
    try:
        yield handle
    finally:
        handle.stop()


@pytest.fixture()
def remote_db(alopex_server):
    from alopex import RemoteDatabase

    client = RemoteDatabase(alopex_server.http_base)
    try:
        yield client
    finally:
        client.close()
