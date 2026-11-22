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
from stress_public_evidence import build_status, CASE_IDS, SHA40


STOP_BYTES = 40_000_000_000
UPLOAD_RESERVE_BYTES = 1_000_000_000
MAX_STATUS_BYTES = 4_096


def read_status(path: Path) -> dict:
    if not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("STATUS")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or not 0 < info.st_size <= MAX_STATUS_BYTES):
                raise ValueError("STATUS")
            body = stream.read(MAX_STATUS_BYTES + 1)
            after = os.fstat(stream.fileno())
            if (len(body) != info.st_size or after.st_size != info.st_size
                    or after.st_mtime_ns != info.st_mtime_ns):
                raise ValueError("STATUS")
    except OSError:
        raise ValueError("STATUS") from None
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("STATUS") from None
    if not isinstance(value, dict):
        raise ValueError("STATUS")
    return value


def verify_output(*, workspace: Path, case_id: str, kind: str,
                  source: str) -> Path:
    if case_id not in CASE_IDS or kind not in {"public", "private"}:
        raise ValueError("STATE")
    case_dir = workspace / "target/stress-publish" / case_id
    for parent in (workspace / "target", workspace / "target/stress-publish", case_dir):
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError("OUTPUT")
    info = case_dir.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("OUTPUT")
    status_path = case_dir / f"{case_id}.json"
    cms_path = case_dir / f"{case_id}.cms"
    status = read_status(status_path)
    if (not isinstance(status.get("outcome"), str)
            or not isinstance(status.get("error_code"), str)):
        raise ValueError("STATUS")
    cms_exists = cms_path.exists() or cms_path.is_symlink()
    expected = build_status(
        case_id=case_id, source_commit=source,
        outcome=status.get("outcome"), error_code=status.get("error_code"),
        ciphertext=cms_path if cms_exists else None)
    if status != expected or (kind == "private" and not cms_exists):
        raise ValueError("STATUS")
    return status_path if kind == "public" else cms_path


def measure(*, workspace: Path, runner_temp: Path, private: Path,
            case_id: str, kind: str, stage: str) -> int:
    home = Path.home()
    roots = [workspace, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp"),
             *release_storage.tool_roots()]
    local = storage.inventory(roots)
    external = release_storage.docker_inventory()
    total = local["allocated_bytes"] + external["allocated_bytes"]
    log = private / f"{case_id}-{kind}-{stage}.storage.private.json"
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
    if (len(args) != 6 or args[0] != "--case" or args[2] != "--kind"
            or args[4] != "--stage" or args[5] not in {"before", "after"}):
        raise ValueError("ARGUMENT")
    case_id, kind, stage = args[1], args[3], args[5]
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
    verify_output(workspace=WORKSPACE, case_id=case_id, kind=kind,
                  source=source)
    measure(workspace=WORKSPACE, runner_temp=runner_temp, private=private,
            case_id=case_id, kind=kind, stage=stage)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        raise SystemExit("STRESS_UPLOAD_BOUNDARY_REJECTED") from None
