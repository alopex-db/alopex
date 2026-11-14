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

from s04_wire_preflight import PreflightError, private_write

LIMIT = 1_048_576
MAX_ACL = 16_384


def bounded_read(path: Path, limit: int = LIMIT) -> bytes:
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise PreflightError("METADATA_LIMIT")
    return data


def selected_endpoint(home: Path, env: dict[str, str]) -> Path:
    if any(env.get(key) for key in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG")):
        raise PreflightError("CONTEXT_OVERRIDE")
    config_path = home / ".docker/config.json"
    meta_root = home / ".docker/contexts/meta"
    if any(path.is_symlink() for path in (home / ".docker", config_path,
                                          home / ".docker/contexts", meta_root)):
        raise PreflightError("CONTEXT_INVALID")
    config = json.loads(bounded_read(config_path, 65_536))
    name = config.get("currentContext")
    if not isinstance(name, str) or not name or name == "default":
        raise PreflightError("CONTEXT_AMBIGUOUS")
    matches = []
    for child in meta_root.iterdir():
        if child.is_symlink() or not child.is_dir():
            raise PreflightError("CONTEXT_INVALID")
        metadata = child / "meta.json"
        if metadata.is_symlink():
            raise PreflightError("CONTEXT_INVALID")
        item = json.loads(bounded_read(metadata, 65_536))
        if item.get("Name") == name:
            matches.append(item)
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


def acl(path: Path) -> str:
    result = subprocess.run(["/usr/bin/getfacl", "-cp", "--", str(path)],
                            stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=5, check=False)
    if result.returncode or result.stderr or len(result.stdout) > MAX_ACL:
        raise PreflightError("ACL_INCOMPLETE")
    return result.stdout.decode("utf-8", "strict")


def inspect_path(endpoint: Path) -> list[dict]:
    paths = [Path("/")]
    paths.extend(Path("/", *endpoint.parts[1:i]) for i in range(2, len(endpoint.parts) + 1))
    if len(paths) > 32:
        raise PreflightError("PATH_LIMIT")
    facts = []
    for index, path in enumerate(paths):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise PreflightError("SYMLINK")
        if not (stat.S_ISSOCK(info.st_mode) if index == len(paths) - 1 else stat.S_ISDIR(info.st_mode)):
            raise PreflightError("PATH_TYPE")
        facts.append({"path": str(path), "dev": info.st_dev, "inode": info.st_ino,
                      "uid": info.st_uid, "gid": info.st_gid,
                      "mode": stat.S_IMODE(info.st_mode), "acl": acl(path)})
    for fact, path in zip(facts, paths):
        info = path.lstat()
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
