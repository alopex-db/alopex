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
WINDOWS = os.name == "nt"


def worktree_paths(repo):
    result = subprocess.run(
        ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    )
    paths = [
        Path(line[9:]).resolve()
        for line in result.stdout.splitlines()
        if line.startswith("worktree ")
    ]
    if not paths:
        raise RuntimeError(f"Git returned no worktrees for {repo}")
    return paths


def checkout_roots():
    if (REPO / ".git").exists():
        paths = worktree_paths(REPO)
        return paths, paths[0].parent, True  # Git lists the main worktree first.
    runner_temp_value = os.environ.get("RUNNER_TEMP", "")
    workspace_value = os.environ.get("GITHUB_WORKSPACE", "")
    if (os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or not runner_temp_value or not workspace_value):
        raise RuntimeError("missing trusted checkout for storage inventory")
    runner_temp = Path(runner_temp_value)
    workspace = Path(workspace_value)
    bootstrap = runner_temp / "stress-bootstrap"
    if (not runner_temp.is_absolute() or runner_temp.is_symlink()
            or not workspace.is_absolute() or workspace.is_symlink()
            or not workspace.is_dir() or REPO != bootstrap.resolve()):
        raise RuntimeError("untrusted bootstrap inventory location")
    bootstrap_info = bootstrap.lstat()
    workspace_info = workspace.stat()
    if (not stat.S_ISDIR(bootstrap_info.st_mode)
            or bootstrap_info.st_uid != os.getuid()
            or stat.S_IMODE(bootstrap_info.st_mode) != 0o700
            or workspace_info.st_uid != os.getuid()):
        raise RuntimeError("unowned bootstrap inventory location")
    # This pinned bootstrap starts from an empty hosted checkout and never
    # creates a linked worktree. A failed fetch may leave .git unreadable;
    # requiring Git here would hide the failure-path allocation measurement.
    return [workspace], workspace.parent, False


if WINDOWS:
    from storage_windows import allocation_size, native_file_info


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
            try:
                if WINDOWS:
                    key, allocated = native_file_info(path)
                else:
                    key, allocated = (info.st_dev, info.st_ino), info.st_blocks * 512
            except FileNotFoundError:
                continue  # Removed between lstat and native handle open.
            if key in seen:
                continue
            seen.add(key)
            total += allocated
            reparse = getattr(info, "st_file_attributes", 0) & 0x400
            if stat.S_ISDIR(info.st_mode) and not reparse:
                try:
                    with os.scandir(path) as entries:
                        pending.extend(Path(entry.path) for entry in entries)
                except FileNotFoundError:
                    continue  # Concurrent cleanup removed this directory; other IO errors still abort.
    return total


def inventory(extra=()):
    home = Path.home()
    checkout_worktrees, workspace, discover_linked = checkout_roots()
    roots = {
        workspace,
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
    for name in ["CARGO_HOME", "UV_CACHE_DIR", "NIMBLE_DIR"]:
        if os.environ.get(name):
            roots.add(Path(os.environ[name]))
    if os.environ.get("ALOPEX_NIM_BIN"):
        roots.add(Path(os.environ["ALOPEX_NIM_BIN"]).resolve().parent.parent)
    if os.name == "nt" and os.environ.get("LOCALAPPDATA"):
        cache = Path(os.environ["LOCALAPPDATA"])
        roots.add(cache / "uv/cache")
        roots.update(p for p in cache.iterdir() if "alopex" in p.name.lower())
    roots.update(Path(p) for p in extra)
    # Discover linked worktrees for every sibling repository in the main workspace.
    roots.update(checkout_worktrees)
    if discover_linked:
        for repo in workspace.iterdir():
            if not (repo / ".git").exists():
                continue
            roots.update(worktree_paths(repo))
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
        "method": "native allocated bytes, unique file identities, no reparse traversal",
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
    if args.additional_bytes <= 0 or initial + args.additional_bytes > STOP_BYTES:
        raise RuntimeError("declared additional storage does not fit below the host stop budget")
    env = dict(
        os.environ,
        CARGO_INCREMENTAL="0",
        CARGO_PROFILE_DEV_DEBUG="0",
        CARGO_PROFILE_TEST_DEBUG="0",
        CARGO_BUILD_JOBS="2",
        PIP_NO_CACHE_DIR="1",
    )
    child = subprocess.Popen(command, env=env, start_new_session=os.name != "nt")
    try:
        while child.poll() is None:
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                check(args.log, args.root, limit)
        check(args.log, args.root, limit)
    except BaseException:
        if child.poll() is None:
            if os.name == "nt":
                # Stop the owned Cargo/rustc tree, not just its parent process.
                subprocess.run(["taskkill", "/PID", str(child.pid), "/T", "/F"], check=True)
                child.wait(timeout=20)
            else:
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
