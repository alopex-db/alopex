"""Conservative per-host Alopex inventory and serialized command watchdog.

No failed measurement is treated as zero. Symlink targets are counted through
explicit roots; linked Git worktrees are discovered before each measurement.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[2]
STOP_BYTES = 40_000_000_000


def allocated_bytes(roots):
    roots = sorted(
        {Path(p).resolve() for p in roots if Path(p).exists()},
        key=lambda p: len(p.parts),
    )
    unique = []
    for root in roots:
        if not any(root == p or p in root.parents for p in unique):
            unique.append(root)
    seen = set()
    total = 0
    for root in unique:
        pending = [root]
        while pending:
            path = pending.pop()
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue  # Another process removed this pathname; it no longer consumes space here.
            key = (info.st_dev, info.st_ino)
            if key in seen:
                continue
            seen.add(key)
            total += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(path) as entries:
                    pending.extend(Path(entry.path) for entry in entries)
    return total


def inventory(extra=()):
    home = Path.home()
    roots = {
        REPO.parent,
        home / ".cargo/registry",
        home / ".cargo/git",
        home / ".cache/uv",
        home / ".rustup/downloads",
        home / ".rustup/tmp",
    }
    for parent in [
        home / ".local",
        home / ".cache",
        home / ".cache/nim",
        Path("/tmp"),
        Path("/private/tmp"),
        Path(tempfile.gettempdir()),
    ]:
        if parent.exists():
            roots.update(
                p
                for p in parent.iterdir()
                if "alopex" in p.name.lower() and not p.is_symlink()
            )
    for name in ["CARGO_TARGET_DIR", "RUNNER_TEMP", "ALOPEX_COMPAT_OUTPUT"]:
        if os.environ.get(name):
            roots.add(Path(os.environ[name]))
    roots.update(Path(p) for p in extra)
    # Discover all linked worktrees, including locations outside the workspace.
    for repo in [REPO, *REPO.parent.iterdir()]:
        if not (repo / ".git").exists():
            continue
        result = subprocess.run(
            ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
        )
        roots.update(
            Path(line[9:])
            for line in result.stdout.splitlines()
            if line.startswith("worktree ")
        )
    # Prior cleanup inventories may cover nonstandard Alopex cache/toolchain locations.
    prior = Path("/private/tmp/alopex-cleanup-20261018/local-budget.json")
    if prior.exists():
        roots.update(Path(p) for p in json.loads(prior.read_text())["roots"])
    for root in (home / ".asdf/installs/rust").glob("*"):
        roots.update([root / "registry", root / "git"])
    roots.update(
        (home / ".rustup/toolchains").glob("*/lib/rustlib/wasm32-unknown-unknown")
    )
    return {
        "allocated_bytes": allocated_bytes(roots),
        "time_unix": time.time(),
        "method": "allocated blocks, unique inodes, no symlink traversal",
        "roots": sorted(str(p) for p in roots if p.exists()),
    }


def check(log=None, extra=(), limit=STOP_BYTES):
    result = inventory(extra)
    if log:
        with Path(log).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(result) + "\n")
    if result["allocated_bytes"] >= limit:
        raise RuntimeError(
            f"Alopex host storage {result['allocated_bytes']} reached stop {limit}"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--root", action="append", default=[])
    parser.add_argument("--additional-bytes", type=int, default=20_000_000_000)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    initial = check(args.log, args.root)["allocated_bytes"]
    limit = min(STOP_BYTES, initial + args.additional_bytes)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        print(json.dumps({"allocated_bytes": initial}))
        return
    env = dict(
        os.environ,
        CARGO_INCREMENTAL="0",
        CARGO_PROFILE_DEV_DEBUG="0",
        CARGO_PROFILE_TEST_DEBUG="0",
        CARGO_BUILD_JOBS="2",
        PIP_NO_CACHE_DIR="1",
    )
    child = subprocess.Popen(command, env=env, start_new_session=True)
    try:
        while child.poll() is None:
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                check(args.log, args.root, limit)
        check(args.log, args.root, limit)
    except BaseException:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        raise
    sys.exit(child.returncode)


if __name__ == "__main__":
    main()
