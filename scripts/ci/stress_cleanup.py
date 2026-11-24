"""Remove only Stage-2 owned output, then inventory the complete host budget."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys

WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE / "scripts/compatibility"))
sys.path.insert(0, str(WORKSPACE / "scripts/release"))
import release_storage  # noqa: E402
import storage  # noqa: E402

SOURCE = re.compile(r"[0-9a-f]{40}\Z")
RUN = re.compile(r"[1-9][0-9]{0,19}\Z")
CASE_SETS = frozenset({"stress", "asan", "tsan", "lsan", "msan",
                       "sql_parser,dataframe_conversion", "perf"})
MARKER = "stage2-target-ownership.private.json"


def private_directory(runner_temp: Path) -> Path:
    if runner_temp.is_symlink() or not runner_temp.is_dir():
        raise ValueError("STATE")
    private = runner_temp / "stress-private"
    private.mkdir(mode=0o700, exist_ok=True)
    info = private.stat()
    if (private.is_symlink() or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("STATE")
    return private


def write_private(path: Path, payload: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")


def prepare(*, workspace: Path, private: Path, source: str, run_id: str) -> None:
    target = workspace / "target"
    marker = private / MARKER
    if target.exists() or target.is_symlink() or marker.exists() or marker.is_symlink():
        raise ValueError("OWNERSHIP")
    target.mkdir(mode=0o700)
    info = target.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("OWNERSHIP")
    try:
        write_private(marker, {"schema": "alopex.stage2-target-ownership/v1",
                               "source": source, "run_id": run_id,
                               "device": info.st_dev, "inode": info.st_ino})
    except BaseException:
        if (target.is_dir() and not target.is_symlink()
                and (target.stat().st_dev, target.stat().st_ino) ==
                (info.st_dev, info.st_ino)):
            target.rmdir()
        raise


def owned_target(*, workspace: Path, private: Path,
                 source: str, run_id: str) -> Path:
    marker = private / MARKER
    info = marker.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 1024):
        raise ValueError("OWNERSHIP")
    value = json.loads(marker.read_text(encoding="utf-8"))
    if (not isinstance(value, dict) or set(value) !=
            {"schema", "source", "run_id", "device", "inode"}
            or value["schema"] != "alopex.stage2-target-ownership/v1"
            or value["source"] != source or value["run_id"] != run_id
            or type(value["device"]) is not int or type(value["inode"]) is not int):
        raise ValueError("OWNERSHIP")
    target = workspace / "target"
    current = target.lstat()
    if (not stat.S_ISDIR(current.st_mode) or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o700
            or (current.st_dev, current.st_ino) !=
            (value["device"], value["inode"])):
        raise ValueError("OWNERSHIP")
    return target


def inventory(*, workspace: Path, runner_temp: Path, private: Path,
              source: str, run_id: str, cases: str, deleted: bool) -> int:
    home = Path.home()
    roots = [workspace, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp"),
             *release_storage.tool_roots()]
    local = storage.inventory(roots)
    external = release_storage.docker_inventory()
    total = local["allocated_bytes"] + external["allocated_bytes"]
    write_private(private / "stage2-post-cleanup.storage.private.json",
                  {"schema": "alopex.stage2-post-cleanup/v1",
                   "source": source, "run_id": run_id, "cases": cases,
                   "target_deleted": deleted, "local": local,
                   "docker": external, "total": total})
    if total >= 40_000_000_000:
        raise ValueError("STORAGE")
    return total


def cleanup(*, workspace: Path, runner_temp: Path, private: Path,
            source: str, run_id: str, cases: str) -> None:
    deleted = False
    removal_error = None
    try:
        target = owned_target(workspace=workspace, private=private,
                              source=source, run_id=run_id)
        if not shutil.rmtree.avoids_symlink_attacks:
            raise ValueError("OWNERSHIP")
        shutil.rmtree(target)
        if target.exists() or target.is_symlink():
            raise ValueError("OWNERSHIP")
        deleted = True
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        removal_error = error
    inventory(workspace=workspace, runner_temp=runner_temp, private=private,
              source=source, run_id=run_id, cases=cases, deleted=deleted)
    if removal_error is not None:
        raise ValueError("OWNERSHIP") from None


def main() -> None:
    os.umask(0o077)
    args = sys.argv[1:]
    if (len(args) != 4 or args[0] != "--stage" or
            args[1] not in {"prepare", "cleanup"} or args[2] != "--cases" or
            args[3] not in CASE_SETS):
        raise ValueError("ARGUMENT")
    stage, cases = args[1], args[3]
    source = os.environ.get("GITHUB_SHA", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    if (not SOURCE.fullmatch(source) or not RUN.fullmatch(run_id)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)
            or WORKSPACE.is_symlink()):
        raise ValueError("STATE")
    head = subprocess.check_output(
        ["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")
    private = private_directory(runner_temp)
    if stage == "prepare":
        prepare(workspace=WORKSPACE, private=private, source=source,
                run_id=run_id)
    else:
        cleanup(workspace=WORKSPACE, runner_temp=runner_temp, private=private,
                source=source, run_id=run_id, cases=cases)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, UnicodeDecodeError,
            json.JSONDecodeError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired, RuntimeError):
        raise SystemExit("STRESS_CLEANUP_REJECTED") from None
