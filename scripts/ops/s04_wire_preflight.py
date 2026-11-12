#!/usr/bin/env python3
"""Read-only selected-host facts and an offline Docker CLI wire capture for S04.

The capture daemon is a task-owned Unix socket. This module never forwards to
the selected Docker endpoint and never creates a real Docker resource.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time

MAX_HEADERS = 8192
MAX_BODY = 65536
MAX_CONNECTIONS = 8
MAX_SECONDS = 45
MAX_OUTPUT = 1_048_576
SOURCE_HEAD = "69c31a7950743d130ac3f96fb1ffb795a2818528"


class PreflightError(Exception):
    pass


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(argv: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(argv, env=env, capture_output=True, timeout=35, check=False)


def private_write(path: Path, value: dict) -> None:
    parent = path.parent.resolve(strict=True)
    if stat.S_IMODE(parent.stat().st_mode) & 0o077:
        raise PreflightError("PRIVATE_DIRECTORY")
    payload = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if len(payload) > MAX_OUTPUT:
        raise PreflightError("OUTPUT_LIMIT")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def read_request(conn: socket.socket) -> tuple[bytes, list[tuple[str, str]], bytes]:
    conn.settimeout(3)
    wire = bytearray()
    while b"\r\n\r\n" not in wire:
        chunk = conn.recv(min(4096, MAX_HEADERS + 4 - len(wire)))
        if not chunk:
            raise PreflightError("TRUNCATED_HEADERS")
        wire.extend(chunk)
        if len(wire) > MAX_HEADERS:
            raise PreflightError("HEADERS_LIMIT")
    head, body = bytes(wire).split(b"\r\n\r\n", 1)
    lines = head.split(b"\r\n")
    parts = lines[0].split(b" ")
    if len(parts) != 3 or parts[2] != b"HTTP/1.1" or parts[0] not in (b"GET", b"HEAD", b"POST"):
        raise PreflightError("REQUEST_LINE")
    headers = []
    for line in lines[1:]:
        if b":" not in line or line.startswith((b" ", b"\t")):
            raise PreflightError("HEADER_SYNTAX")
        key, value = line.split(b":", 1)
        if not key or any(c <= 32 or c >= 127 for c in key):
            raise PreflightError("HEADER_SYNTAX")
        headers.append((key.decode("ascii").lower(), value.strip().decode("latin1")))
    lengths = [value for key, value in headers if key == "content-length"]
    if len(lengths) > 1 or (lengths and not lengths[0].isascii()) or (lengths and not lengths[0].isdecimal()):
        raise PreflightError("CONTENT_LENGTH")
    if any(key == "transfer-encoding" for key, _ in headers):
        raise PreflightError("TRANSFER_ENCODING")
    size = int(lengths[0]) if lengths else 0
    if size > MAX_BODY:
        raise PreflightError("BODY_LIMIT")
    if len(body) > size:
        raise PreflightError("EXTRA_BYTES")
    while len(body) < size:
        chunk = conn.recv(min(4096, size - len(body)))
        if not chunk:
            raise PreflightError("TRUNCATED_BODY")
        body += chunk
    return lines[0], headers, body


def response(line: bytes, api_version: str = "1.51", server_version: str = "28.0.4") -> tuple[int, bytes]:
    method, raw_path, _ = line.split(b" ", 2)
    path = raw_path.split(b"?", 1)[0]
    if path.startswith(b"/v") and b"/" in path[2:]:
        path = b"/" + path.split(b"/", 2)[2]
    if method == b"HEAD" and path == b"/_ping":
        return 200, b""
    if method == b"GET" and path == b"/_ping":
        return 200, b"OK"
    if method == b"GET" and path == b"/version":
        return 200, json.dumps({"ApiVersion": api_version, "MinAPIVersion": "1.41",
                                "Version": server_version, "Os": "linux", "Arch": "amd64"}).encode()
    if method == b"POST" and path == b"/containers/create":
        return 201, b'{"Id":"offline-only-id","Warnings":[]}'
    return 404, b'{"message":"offline fixture: unsupported"}'


def serve(sock: Path, records: list, errors: list, ready: threading.Event,
          stop: threading.Event, api_version: str = "1.51",
          server_version: str = "28.0.4") -> None:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(sock))
            os.chmod(sock, 0o600)
            server.listen(1)
            server.settimeout(0.2)
            ready.set()
            deadline = time.monotonic() + MAX_SECONDS
            while not stop.is_set() and len(records) + len(errors) < MAX_CONNECTIONS and time.monotonic() < deadline:
                try:
                    client, _ = server.accept()
                except socket.timeout:
                    continue
                with client:
                    try:
                        line, headers, body = read_request(client)
                        code, payload = response(line, api_version, server_version)
                        records.append({"request_line": line.decode("latin1"), "headers": headers,
                                        "body_hex": body.hex(), "body_bytes": len(body),
                                        "body_sha256": sha256(body), "response_status": code})
                        client.sendall((f"HTTP/1.1 {code} Offline\r\nAPI-Version: {api_version}\r\nContent-Type: application/json\r\n"
                                        f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload)
                    except (OSError, PreflightError) as exc:
                        errors.append(exc.args[0] if isinstance(exc, PreflightError) else "SOCKET_ERROR")
    except OSError:
        errors.append("SERVER_ERROR")
        ready.set()


def docker_arguments(profile: dict, run_id: str, token: str, image: str,
                     input_volume: str, work_volume: str) -> list[str]:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test-env"))
    from docker_backend import Docker
    return Docker(profile).arguments(run_id, image, input_volume, work_volume, token)


def source_contract() -> dict[str, str]:
    root = Path(__file__).resolve().parents[2]
    paths = ("scripts/test-env/common.py", "scripts/test-env/docker_backend.py",
             "scripts/ops/hosted_test_env_qualification.py")
    result = {}
    for relative in paths:
        pinned = subprocess.run(["git", "show", SOURCE_HEAD + ":" + relative],
                                cwd=root, capture_output=True, check=False, timeout=10)
        current = (root / relative).read_bytes()
        if pinned.returncode or pinned.stdout != current:
            raise PreflightError("SOURCE_MISMATCH")
        result[relative] = sha256(current)
    return result


def capture(args: argparse.Namespace) -> dict:
    if args.source_head != SOURCE_HEAD:
        raise PreflightError("SOURCE_MISMATCH")
    source_hashes = source_contract()
    profile = json.loads(args.profile.read_text())
    cli = Path(args.cli).resolve(strict=True)
    if not cli.is_file():
        raise PreflightError("CLI_IDENTITY")
    profile_hash = sha256(args.profile.read_bytes())
    api_version = getattr(args, "server_api_version", "1.51")
    server_version = getattr(args, "server_version", "28.0.4")
    if not all(part.isdecimal() for part in api_version.split(".")) or \
            len(api_version.split(".")) != 2 or server_version != "28.0.4":
        raise PreflightError("SERVER_VERSION")
    records: list = []
    errors: list = []
    old_umask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix="s04-offline-", dir=args.output.parent) as directory:
            private = Path(directory)
            os.chmod(private, 0o700)
            config = private / "docker-config"
            config.mkdir(mode=0o700)
            sock = private / "fake.sock"
            if len(os.fsencode(sock)) >= 104:
                raise PreflightError("SOCKET_PATH_LIMIT")
            ready, stop = threading.Event(), threading.Event()
            thread = threading.Thread(target=serve, args=(sock, records, errors, ready, stop,
                                                         api_version, server_version), daemon=True)
            thread.start()
            if not ready.wait(3) or errors:
                raise PreflightError("FAKE_SOCKET")
            env = dict(os.environ, DOCKER_CONFIG=str(config), DOCKER_HOST="", DOCKER_CONTEXT="")
            context = "s04-offline"
            created = run([str(cli), "context", "create", context, "--docker", "host=unix://" + str(sock)], env)
            if created.returncode:
                raise PreflightError("CONTEXT_CREATE")
            command = [str(cli), "--context", context, *docker_arguments(
                profile, args.run_id, args.token, args.image, args.input_volume, args.work_volume)]
            result = run(command, env)
            stop.set()
            thread.join(timeout=4)
            if thread.is_alive():
                raise PreflightError("SERVER_TIMEOUT")
            creates = [r for r in records if r["request_line"].startswith("POST ") and
                       "/containers/create" in r["request_line"]]
            outcome = "CAPTURE_COMPLETE" if result.returncode == 0 and not errors and \
                len(creates) == 1 and creates[0]["response_status"] == 201 and \
                all(r["response_status"] != 404 for r in records) else "CAPTURE_INCOMPLETE"
            return {"source_head": args.source_head, "profile_sha256": profile_hash,
                    "selected_server_api_version": api_version,
                    "selected_server_version": server_version,
                    "source_hashes": source_hashes,
                    "cli_path": str(cli), "cli_sha256": sha256(cli.read_bytes()),
                    "command": command, "exit_code": result.returncode,
                    "stdout_sha256": sha256(result.stdout), "stderr_sha256": sha256(result.stderr),
                    "stderr_excerpt": result.stderr[:8192].decode("utf-8", "replace"),
                    "socket_mode": stat.S_IMODE(sock.stat().st_mode), "records": records,
                    "outcome": outcome,
                    "errors": errors, "fake_only": True}
    finally:
        os.umask(old_umask)


def inspect(args: argparse.Namespace) -> dict:
    """No create/stop/build: only CLI provenance and selected endpoint metadata."""
    if sys.platform != "linux":
        raise PreflightError("HOST_PLATFORM")
    cli = Path(args.cli).resolve(strict=True)
    config = os.environ.copy()
    context = run([str(cli), "context", "inspect", args.context], config)
    version = run([str(cli), "--context", args.context, "version", "--format", "{{json .}}"], config)
    if context.returncode or version.returncode:
        raise PreflightError("READONLY_DOCKER")
    endpoint = json.loads(context.stdout)[0]["Endpoints"]["docker"]["Host"]
    if not endpoint.startswith("unix://"):
        raise PreflightError("ENDPOINT_TYPE")
    path = Path(endpoint.removeprefix("unix://"))
    if not path.is_absolute() or not stat.S_ISSOCK(path.stat().st_mode):
        raise PreflightError("ENDPOINT_TYPE")
    lineage = []
    for item in (path, *path.parents):
        info = item.lstat()
        lineage.append({"path": str(item), "dev": info.st_dev, "ino": info.st_ino,
                        "uid": info.st_uid, "gid": info.st_gid,
                        "mode": stat.S_IMODE(info.st_mode)})
    acl = {}
    if shutil.which("getfacl"):
        for item in (path, *path.parents):
            result = run(["getfacl", "-cp", str(item)])
            acl[str(item)] = {"exit_code": result.returncode,
                              "stdout": result.stdout[:8192].decode("utf-8", "replace"),
                              "truncated": len(result.stdout) > 8192}
    daemon_processes = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            executable = (proc / "exe").resolve(strict=True)
            if executable.name != "dockerd":
                continue
            daemon_processes.append({"pid": int(proc.name), "exe": str(executable),
                                     "exe_sha256": sha256(executable.read_bytes())})
        except (OSError, PermissionError):
            continue
    return {"cli_path": str(cli), "cli_sha256": sha256(cli.read_bytes()),
            "context": args.context, "endpoint": endpoint, "lineage": lineage,
            "acl": acl, "mountinfo": Path("/proc/self/mountinfo").read_text()[:131072],
            "version": json.loads(version.stdout), "daemon_processes": daemon_processes,
            "uid": os.getuid(), "groups": os.getgroups(), "read_only": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cli", default=shutil.which("docker"))
    sub = parser.add_subparsers(dest="mode", required=True)
    selected = sub.add_parser("inspect")
    selected.add_argument("--context", required=True)
    offline = sub.add_parser("capture")
    offline.add_argument("--profile", type=Path, required=True)
    offline.add_argument("--run-id", required=True)
    offline.add_argument("--token", required=True)
    offline.add_argument("--image", required=True)
    offline.add_argument("--input-volume", required=True)
    offline.add_argument("--work-volume", required=True)
    offline.add_argument("--source-head", required=True)
    args = parser.parse_args()
    try:
        if args.cli is None:
            raise PreflightError("CLI_MISSING")
        result = inspect(args) if args.mode == "inspect" else capture(args)
        private_write(args.output, result)
        print(json.dumps({"status": "PREFLIGHT_ONLY", "mode": args.mode,
                          "evidence_sha256": sha256(args.output.read_bytes())}))
        return 0
    except (PreflightError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        code = exc.args[0] if isinstance(exc, PreflightError) else "PREFLIGHT_ERROR"
        print(json.dumps({"status": "FAILED", "code": code}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
