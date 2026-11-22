"""Admit only fixed Stage-2 evidence files to a bounded artifact upload.

The companion composite action pins upload-artifact and invokes this before
and after its single-file streaming upload. Detailed inventory stays private.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys


WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE / "scripts/compatibility"))
sys.path.insert(0, str(WORKSPACE / "scripts/release"))
import release_storage  # noqa: E402
import storage  # noqa: E402
from stress_public_case import (assemble, CASES, MAX_CASE_BYTES, read_json,
                                SHA40)


STOP_BYTES = 40_000_000_000
UPLOAD_RESERVE_BYTES = 1_000_000_000
def verify_output(*, workspace: Path, case_id: str, source: str) -> Path:
    if case_id not in CASES:
        raise ValueError("STATE")
    case_dir = workspace / "target/stress-publish" / case_id
    for parent in (workspace / "target", workspace / "target/stress-publish", case_dir):
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError("OUTPUT")
    info = case_dir.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("OUTPUT")
    status_path = case_dir / f"{case_id}.json"
    if set(os.listdir(case_dir)) != {status_path.name}:
        raise ValueError("OUTPUT")
    status = read_json(status_path, limit=MAX_CASE_BYTES, canonical=True)
    if status.get("outcome") not in {"passed", "failed"}:
        raise ValueError("STATUS")
    expected = assemble(workspace=workspace, case_id=case_id,
                        source=source, outcome=status["outcome"])
    if status != expected:
        raise ValueError("STATUS")
    return status_path


def measure(*, workspace: Path, runner_temp: Path, private: Path,
            case_id: str, stage: str) -> int:
    home = Path.home()
    roots = [workspace, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp"),
             *release_storage.tool_roots()]
    local = storage.inventory(roots)
    external = release_storage.docker_inventory()
    total = local["allocated_bytes"] + external["allocated_bytes"]
    log = private / f"{case_id}-public-{stage}.storage.private.json"
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump({"local": local, "docker": external, "total": total,
                   "stage": stage}, stream, sort_keys=True)
        stream.write("\n")
    if (total >= STOP_BYTES or
            (stage == "before" and total + UPLOAD_RESERVE_BYTES >= STOP_BYTES)):
        raise ValueError("STORAGE")
    return total


def main() -> None:
    os.umask(0o077)
    args = sys.argv[1:]
    if (len(args) != 4 or args[0] != "--case" or args[2] != "--stage"
            or args[3] not in {"before", "after"}):
        raise ValueError("ARGUMENT")
    case_id, stage = args[1], args[3]
    source = os.environ.get("GITHUB_SHA", "")
    if (not SHA40.fullmatch(source)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)):
        raise ValueError("STATE")
    head = subprocess.check_output(
        ["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    private = runner_temp / "stress-private"
    if (runner_temp.is_symlink() or not runner_temp.is_dir()
            or private.is_symlink() or not private.is_dir()
            or private.stat().st_uid != os.getuid()
            or stat.S_IMODE(private.stat().st_mode) != 0o700):
        raise ValueError("STATE")
    verify_output(workspace=WORKSPACE, case_id=case_id, source=source)
    measure(workspace=WORKSPACE, runner_temp=runner_temp, private=private,
            case_id=case_id, stage=stage)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, UnicodeDecodeError,
            json.JSONDecodeError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        raise SystemExit("STRESS_UPLOAD_BOUNDARY_REJECTED") from None
