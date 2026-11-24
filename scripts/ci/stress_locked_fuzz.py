"""Run one Stage-2 fuzz target with its exact tracked offline resolution."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import platform
import stat
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
CASES = {
    "sql_parser": Path("crates/alopex-sql/fuzz"),
    "dataframe_conversion": Path("crates/alopex-dataframe/fuzz"),
}
MAX_LOCK_BYTES = 1_000_000
MAX_SECONDS = 172_800
HOST_TARGET = "x86_64-unknown-linux-gnu"


class FixedErrorParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ValueError("ARGUMENT")


def read_lock(path: Path) -> bytes:
    if not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("LOCK")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb", buffering=0) as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_LOCK_BYTES):
            raise ValueError("LOCK")
        body = stream.read(MAX_LOCK_BYTES + 1)
        after = os.fstat(stream.fileno())
        if (len(body) != before.st_size or after.st_size != before.st_size
                or after.st_mtime_ns != before.st_mtime_ns):
            raise ValueError("LOCK")
    return body


def tracked_lock(root: Path, relative: Path) -> bytes:
    name = relative.as_posix()
    subprocess.run(["git", "-C", str(root), "ls-files", "--error-unmatch", "--", name],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    expected = subprocess.check_output(
        ["git", "-C", str(root), "show", f"HEAD:{name}"],
        stderr=subprocess.DEVNULL, timeout=15)
    if not 0 < len(expected) <= MAX_LOCK_BYTES:
        raise ValueError("LOCK")
    if read_lock(root / relative) != expected:
        raise ValueError("LOCK")
    return expected


def offline_metadata(workspace: Path, env: dict[str, str]) -> None:
    subprocess.run(["cargo", "metadata", "--offline", "--locked",
                    "--filter-platform", HOST_TARGET,
                    "--format-version", "1", "--manifest-path", str(workspace / "Cargo.toml")],
                   cwd=workspace, env=env, stdout=subprocess.DEVNULL, check=True)


def locked_fetch(workspace: Path) -> None:
    # Only the reviewed hosted Linux x86_64 profile is eligible for this lane.
    if (sys.platform != "linux" or platform.machine() not in {"x86_64", "amd64"}
            or os.environ.get("RUNNER_OS") != "Linux"
            or os.environ.get("RUNNER_ARCH") != "X64"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        raise ValueError("RUNNER")
    subprocess.run(["cargo", "fetch", "--locked", "--target", HOST_TARGET,
                    "--manifest-path", str(workspace / "Cargo.toml")],
                   cwd=workspace, stdout=subprocess.DEVNULL, check=True)


def run(case_id: str, seconds: int, *, root: Path = ROOT) -> None:
    if case_id not in CASES or not 1 <= seconds <= MAX_SECONDS:
        raise ValueError("ARGUMENT")
    if os.environ.get("GITHUB_WORKSPACE") != str(root):
        raise ValueError("SOURCE")
    workspace = root / CASES[case_id]
    relative = CASES[case_id] / "Cargo.lock"
    before = tracked_lock(root, relative)
    digest = hashlib.sha256(before).digest()
    try:
        locked_fetch(workspace)
    finally:
        if read_lock(root / relative) != before:
            raise ValueError("LOCK")
    if hashlib.sha256(tracked_lock(root, relative)).digest() != digest:
        raise ValueError("LOCK")
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    try:
        offline_metadata(workspace, env)
    finally:
        if read_lock(root / relative) != before:
            raise ValueError("LOCK")
    if hashlib.sha256(tracked_lock(root, relative)).digest() != digest:
        raise ValueError("LOCK")

    target = root / "target"
    artifacts = target / "stress-artifacts"
    fuzz = artifacts / "fuzz"
    crash = fuzz / case_id
    for directory in (target, artifacts, fuzz, crash):
        if directory.is_symlink():
            raise ValueError("OUTPUT")
        directory.mkdir(mode=0o700, exist_ok=True)
        if not directory.is_dir():
            raise ValueError("OUTPUT")

    command = ["cargo", "fuzz", "run", case_id, "--",
               f"-max_total_time={seconds}", f"-artifact_prefix={crash}/"]
    failed = False
    try:
        subprocess.run(command, cwd=workspace, env=env, check=True)
    except (OSError, subprocess.CalledProcessError):
        failed = True
    finally:
        # A failed fuzz target may have changed its lock before it exited.
        if read_lock(root / relative) != before:
            raise ValueError("LOCK")
    if failed:
        raise ValueError("FUZZ")


def main() -> None:
    parser = FixedErrorParser()
    parser.add_argument("--case", choices=sorted(CASES), required=True)
    parser.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args()
    run(args.case, args.seconds)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        raise SystemExit("STRESS_LOCKED_FUZZ_REJECTED") from None
