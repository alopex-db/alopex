"""Bootstrap exact-source Stage-2 checkout under the host storage watchdog.

This file is fetched with a workflow-pinned digest before repository checkout.
The workflow captures all diagnostics privately and prints only a fixed code.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import stat
import subprocess
import sys


REPOSITORY_URL = "https://github.com/alopex-db/alopex.git"
RAW_BASE = "https://raw.githubusercontent.com/alopex-db/alopex"
SOURCE = re.compile(r"[0-9a-f]{40}\Z")
HELPERS = {
    "scripts/compatibility/storage.py":
        "b4337923f069f388c838bb554f54ad26b479cd27a1e888b75040d6c36783d5b6",
    "scripts/release/release_storage.py":
        "14b10712125f26c195cbb286323ce91f26ba76ed8e35e20e0f1ffcd1eb4e50af",
}
MAX_HELPER_BYTES = 100_000


def _limit_download_file() -> None:
    resource.setrlimit(resource.RLIMIT_FSIZE,
                       (MAX_HELPER_BYTES, MAX_HELPER_BYTES))


def fetch_helper(source: str, relative: str, expected_sha256: str,
                 bootstrap_dir: Path) -> Path:
    destination = bootstrap_dir / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise ValueError("HELPER")
    subprocess.run([
        "curl", "--fail", "--silent", "--show-error", "--location",
        "--proto", "=https", "--proto-redir", "=https",
        "--max-time", "60", "--max-filesize", str(MAX_HELPER_BYTES),
        "--output", str(destination), f"{RAW_BASE}/{source}/{relative}",
    ], check=True, timeout=75, preexec_fn=_limit_download_file)
    body = destination.read_bytes()
    if not 0 < len(body) <= MAX_HELPER_BYTES or hashlib.sha256(body).hexdigest() != expected_sha256:
        raise ValueError("HELPER")
    destination.chmod(0o600)
    return destination


def guarded_checkout_command(*, helper: Path, workspace: Path,
                             runner_temp: Path, home: Path,
                             log: Path) -> list[str]:
    roots = [workspace, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp")]
    command = [sys.executable, str(helper), "--docker", "--log", str(log)]
    for root in roots:
        command.extend(("--root", str(root)))
    command.extend(("--additional-bytes", "20000000000", "--",
                    "bash", "-c",
                    'git -C "$GITHUB_WORKSPACE" init -q && '
                    'git -C "$GITHUB_WORKSPACE" remote add origin '
                    + REPOSITORY_URL + ' && '
                    'git -C "$GITHUB_WORKSPACE" fetch --no-tags --depth=1 origin '
                    '"$GITHUB_SHA" && '
                    'git -C "$GITHUB_WORKSPACE" checkout -q --detach FETCH_HEAD'))
    return command


def failed_checkout_cleanup(*, workspace: Path, runner_temp: Path,
                            bootstrap_dir: Path, helper: Path,
                            original: tuple[int, int, int]) -> None:
    removal_error = False
    removed = False
    try:
        info = workspace.lstat()
        if (not stat.S_ISDIR(info.st_mode)
                or (info.st_dev, info.st_ino, info.st_uid) != original
                or info.st_uid != os.getuid()
                or not shutil.rmtree.avoids_symlink_attacks):
            raise ValueError("OWNERSHIP")
        for entry in workspace.iterdir():
            mode = entry.lstat().st_mode
            if stat.S_ISDIR(mode):
                shutil.rmtree(entry)
            else:
                entry.unlink()
        removed = not any(workspace.iterdir())
        if not removed:
            raise ValueError("OWNERSHIP")
    except (OSError, ValueError):
        removal_error = True

    log = bootstrap_dir / "failed-checkout-final.storage.private.jsonl"
    output = bootstrap_dir / "failed-checkout-final.output.private.log"
    command = [sys.executable, str(helper), "--docker", "--log", str(log)]
    for root in (workspace, runner_temp, Path.home() / ".cargo",
                 Path.home() / ".cache", Path.home() / ".local",
                 Path.home() / ".rustup", Path("/tmp")):
        command.extend(("--root", str(root)))
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        result = subprocess.run(command, stdout=stream,
                                stderr=subprocess.STDOUT, check=False)
    status = bootstrap_dir / "failed-checkout-cleanup.private.json"
    descriptor = os.open(status, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump({"workspace_removed": removed,
                   "inventory_completed": result.returncode == 0,
                   "removal_failed": removal_error}, stream, sort_keys=True)
        stream.write("\n")
    if removal_error or result.returncode:
        raise ValueError("CLEANUP")


def main() -> None:
    os.umask(0o077)
    source = os.environ.get("GITHUB_SHA", "")
    workspace = Path(os.environ.get("GITHUB_WORKSPACE", ""))
    runner_temp = Path(os.environ.get("RUNNER_TEMP", ""))
    if (not SOURCE.fullmatch(source)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or not workspace.is_dir() or not runner_temp.is_dir()
            or workspace.is_symlink() or runner_temp.is_symlink()
            or list(workspace.iterdir())):
        raise ValueError("STATE")
    info = workspace.stat()
    original = (info.st_dev, info.st_ino, info.st_uid)
    if info.st_uid != os.getuid():
        raise ValueError("STATE")
    bootstrap_dir = runner_temp / "stress-bootstrap"
    bootstrap_dir.mkdir(mode=0o700, exist_ok=False)
    helpers = {name: fetch_helper(source, name, digest, bootstrap_dir)
               for name, digest in HELPERS.items()}
    log = bootstrap_dir / "storage.private.jsonl"
    command = guarded_checkout_command(
        helper=helpers["scripts/release/release_storage.py"],
        workspace=workspace, runner_temp=runner_temp, home=Path.home(),
        log=log)
    try:
        subprocess.run(command, check=True)
        actual = subprocess.check_output(
            ["git", "-C", str(workspace), "rev-parse", "HEAD"],
            text=True, timeout=15).strip()
        if actual != source:
            raise ValueError("SOURCE")
        for relative, downloaded in helpers.items():
            if (workspace / relative).read_bytes() != downloaded.read_bytes():
                raise ValueError("SOURCE")
        clean = subprocess.check_output(
            ["git", "-C", str(workspace), "status", "--porcelain"],
            text=True, timeout=15)
        if clean:
            raise ValueError("SOURCE")
    except (OSError, ValueError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        failed_checkout_cleanup(workspace=workspace, runner_temp=runner_temp,
                                bootstrap_dir=bootstrap_dir,
                                helper=helpers["scripts/release/release_storage.py"],
                                original=original)
        raise


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        raise SystemExit("STRESS_MONITORED_CHECKOUT_REJECTED") from None
