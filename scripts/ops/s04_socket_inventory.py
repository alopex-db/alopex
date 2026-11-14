#!/usr/bin/env python3
"""Observe selected Docker socket metadata without invoking Docker or connecting.

The receipt is private input to the pinned CMS sealer, never an admission rule.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import subprocess
from contextlib import contextmanager

from s04_wire_preflight import PreflightError, private_write

LIMIT = 1_048_576
MAX_ACL = 16_384
DIRECTORY_FLAG = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW


@contextmanager
def pinned_path(path: Path, final_flags: int):
    """Open every absolute component without following a symlink."""
    if not path.is_absolute() or any(
            part in (".", "..") for part in path.parts):
        raise PreflightError("PATH_INVALID")
    fd = os.open("/", DIRECTORY_FLAG)
    try:
        for index, part in enumerate(path.parts[1:]):
            next_fd = os.open(part, final_flags if index == len(path.parts) - 2 else
                              DIRECTORY_FLAG,
                              dir_fd=fd)
            os.close(fd)
            fd = next_fd
        yield fd
    finally:
        os.close(fd)


def bounded_read_fd(fd: int, limit: int = LIMIT) -> bytes:
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        raise PreflightError("METADATA_TYPE")
    with os.fdopen(os.dup(fd), "rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise PreflightError("METADATA_LIMIT")
    return data


def bounded_read(path: Path, limit: int = LIMIT) -> bytes:
    with pinned_path(path, os.O_RDONLY | os.O_NOFOLLOW) as fd:
        return bounded_read_fd(fd, limit)


def selected_endpoint(home: Path, env: dict[str, str]) -> Path:
    if any(env.get(key) for key in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG")):
        raise PreflightError("CONTEXT_OVERRIDE")
    config_path = home / ".docker/config.json"
    meta_root = home / ".docker/contexts/meta"
    config = json.loads(bounded_read(config_path, 65_536))
    name = config.get("currentContext")
    if not isinstance(name, str) or not name or name == "default":
        raise PreflightError("CONTEXT_AMBIGUOUS")
    matches = []
    with pinned_path(meta_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW) as meta_fd:
        for child in os.listdir(meta_fd):
            child_fd = os.open(child, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=meta_fd)
            try:
                metadata_fd = os.open("meta.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=child_fd)
                try:
                    item = json.loads(bounded_read_fd(metadata_fd, 65_536))
                finally:
                    os.close(metadata_fd)
                if item.get("Name") == name:
                    matches.append(item)
            finally:
                os.close(child_fd)
    if len(matches) != 1:
        raise PreflightError("CONTEXT_AMBIGUOUS")
    endpoints = matches[0].get("Endpoints")
    if not isinstance(endpoints, dict) or set(endpoints) != {"docker"}:
        raise PreflightError("ENDPOINT_INVALID")
    host = endpoints["docker"].get("Host")
    if not isinstance(host, str) or not host.startswith("unix://"):
        raise PreflightError("ENDPOINT_INVALID")
    raw = host.removeprefix("unix://")
    if not raw.startswith("/") or "\0" in raw or "\n" in raw or "\r" in raw or ".." in Path(raw).parts:
        raise PreflightError("ENDPOINT_INVALID")
    return Path(raw)


def acl(fd: int) -> str:
    result = subprocess.run(["/usr/bin/getfacl", "-cp", "--", f"/proc/self/fd/{fd}"],
                            stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=5, check=False, pass_fds=(fd,))
    if result.returncode or result.stderr or len(result.stdout) > MAX_ACL:
        raise PreflightError("ACL_INCOMPLETE")
    return result.stdout.decode("utf-8", "strict")


def inspect_path(endpoint: Path) -> list[dict]:
    if not hasattr(os, "O_PATH"):
        raise PreflightError("PATH_UNSUPPORTED")
    paths = [Path("/")]
    paths.extend(Path("/", *endpoint.parts[1:i]) for i in range(2, len(endpoint.parts) + 1))
    if len(paths) > 32:
        raise PreflightError("PATH_LIMIT")
    facts = []
    for index, path in enumerate(paths):
        flags = os.O_PATH | os.O_NOFOLLOW
        if index != len(paths) - 1:
            flags |= os.O_DIRECTORY
        with pinned_path(path, flags) as fd:
            info = os.fstat(fd)
            if not (stat.S_ISSOCK(info.st_mode) if index == len(paths) - 1 else stat.S_ISDIR(info.st_mode)):
                raise PreflightError("PATH_TYPE")
            facts.append({"path": str(path), "dev": info.st_dev, "inode": info.st_ino,
                          "uid": info.st_uid, "gid": info.st_gid,
                          "mode": stat.S_IMODE(info.st_mode), "acl": acl(fd)})
    for fact, path in zip(facts, paths):
        flags = DIRECTORY_FLAG if path != endpoint else os.O_PATH | os.O_NOFOLLOW
        with pinned_path(path, flags) as fd:
            info = os.fstat(fd)
        if (info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode), info.st_uid, info.st_gid) != (
                fact["dev"], fact["inode"], fact["mode"], fact["uid"], fact["gid"]):
            raise PreflightError("PATH_CHANGED")
    return facts


def collect(home: Path, env: dict[str, str]) -> dict:
    expected = env.get("EXPECTED_CONTROLLER_SHA", "")
    if (env.get("GITHUB_EVENT_NAME") != "workflow_dispatch" or
            env.get("ISSUE_NUMBER") != "684" or
            not re.fullmatch(r"[0-9a-f]{40}", expected) or
            expected != env.get("GITHUB_SHA")):
        raise PreflightError("SOURCE_MISMATCH")
    checkout = subprocess.run(["git", "rev-parse", "HEAD"],
                              capture_output=True, timeout=10, check=False)
    if checkout.returncode or checkout.stdout.decode().strip() != expected:
        raise PreflightError("SOURCE_MISMATCH")
    endpoint = selected_endpoint(home, env)
    mountinfo = bounded_read(Path("/proc/self/mountinfo"))
    paths = inspect_path(endpoint)
    return {"schema": "alopex.s04-socket-inventory/v1", "read_only": True,
            "admission": "not_assessed", "source_head": expected,
            "selected_endpoint": str(endpoint), "paths": paths,
            "mountinfo": mountinfo.decode("utf-8", "strict")}


def main() -> int:
    try:
        output = Path(os.environ["RUNNER_TEMP"]) / "alopex-s04-socket-inventory"
        output.mkdir(mode=0o700)
        private_write(output / "inspector.private.json", collect(Path.home(), os.environ))
        print('{"case":"S04_SOCKET_INVENTORY","outcome":"collected"}')
        return 0
    except BaseException:
        print('{"case":"S04_SOCKET_INVENTORY","outcome":"failed"}')
        return 1



if __name__ == "__main__":
    raise SystemExit(main())
