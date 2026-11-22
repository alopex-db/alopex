"""Run one Stage-2 growth command under the host storage watchdog.

Keep command output and inventory private on the runner. The workflow receives
only a fixed success or failure code; raw logs are never uploaded directly.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


WORKSPACE = Path(__file__).resolve().parents[2]
SOURCE = re.compile(r"[0-9a-f]{40}\Z")
CASES = frozenset({"stress", "asan", "tsan", "lsan", "msan", "sql_parser",
                   "dataframe_conversion", "perf"})
PHASES = frozenset({"toolchain", "parser", "test", "fuzz_install",
                    "baseline_fetch", "seal", "upload"})
FUZZ_CASES = frozenset({"sql_parser", "dataframe_conversion"})
FUZZ_DONE = re.compile(rb"(?m)^#([0-9]{1,20})[ \t]+DONE(?:[ \t]|$)")


def parse(argv: list[str]) -> tuple[str, str, list[str]]:
    if (len(argv) < 6 or argv[0] != "--case" or argv[2] != "--phase"
            or argv[4] != "--" or argv[1] not in CASES
            or argv[3] not in PHASES or not argv[5:]):
        raise ValueError("ARGUMENT")
    return argv[1], argv[3], argv[5:]


def private_directory() -> Path:
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    if runner_temp.is_symlink() or not runner_temp.is_dir():
        raise ValueError("STATE")
    directory = runner_temp / "stress-private"
    directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.stat()
    if (directory.is_symlink() or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("STATE")
    return directory


def guarded_command(*, private: Path, case_id: str, phase: str,
                    command: list[str]) -> tuple[list[str], Path]:
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    home = Path.home()
    storage_log = private / f"{case_id}-{phase}.storage.private.jsonl"
    roots = [WORKSPACE, runner_temp, home / ".cargo", home / ".cache",
             home / ".local", home / ".rustup", Path("/tmp")]
    args = [sys.executable, str(WORKSPACE / "scripts/release/release_storage.py"),
            "--docker", "--log", str(storage_log)]
    for root in roots:
        args.extend(("--root", str(root)))
    args.extend(("--additional-bytes", "20000000000", "--", *command))
    return args, private / f"{case_id}-{phase}.output.private.log"


def write_public_execution(*, case_id: str, source: str, passed: bool,
                           duration_ms: int,
                           fuzz_executions: int | None = None) -> None:
    if (case_id not in CASES or not SOURCE.fullmatch(source)
            or not 0 <= duration_ms <= 172_800_000
            or (case_id in FUZZ_CASES and fuzz_executions is not None and
                not 0 < fuzz_executions <= 2**64 - 1)
            or (case_id not in FUZZ_CASES and fuzz_executions is not None)
            or (case_id in FUZZ_CASES and passed and fuzz_executions is None)):
        raise ValueError("EVIDENCE")
    target = WORKSPACE / "target"
    public = target / "stress-public-artifacts"
    case = public / case_id
    for directory in (target, public, case):
        if directory.is_symlink():
            raise ValueError("EVIDENCE")
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.stat()
        if info.st_uid != os.getuid() or not stat.S_ISDIR(info.st_mode):
            raise ValueError("EVIDENCE")
    payload = {"schema": "alopex.stress-execution/v1", "case_id": case_id,
               "source_commit": source, "outcome": "passed" if passed else "failed",
               "duration_ms": duration_ms}
    if case_id in FUZZ_CASES:
        payload["fuzz_executions"] = fuzz_executions
    output = case / "execution.json"
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")


def fuzz_execution_count(output_path: Path) -> int | None:
    with output_path.open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(max(0, size - 1_000_000))
        tail = stream.read(1_000_000)
    matches = FUZZ_DONE.findall(tail)
    if not matches:
        return None
    count = int(matches[-1])
    return count if 0 < count <= 2**64 - 1 else None


def main() -> None:
    os.umask(0o077)
    case_id, phase, command = parse(sys.argv[1:])
    source = os.environ.get("GITHUB_SHA", "")
    expected_toolchain = ("stable" if case_id in {"stress", "perf"}
                          else "nightly")
    if (not SOURCE.fullmatch(source)
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("RUSTUP_TOOLCHAIN") != expected_toolchain
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)
            or os.environ.get("CARGO_TARGET_DIR") != str(WORKSPACE / "target")):
        raise ValueError("STATE")
    head = subprocess.check_output(
        ["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")
    private = private_directory()
    args, output_path = guarded_command(private=private, case_id=case_id,
                                        phase=phase, command=command)
    descriptor = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        started = time.monotonic_ns()
        result = subprocess.run(args, stdout=output, stderr=subprocess.STDOUT,
                                check=False)
        duration_ms = (time.monotonic_ns() - started) // 1_000_000
    if phase == "test":
        if os.environ.get("STRESS_EVIDENCE_MODE") != "public":
            raise ValueError("EVIDENCE")
        executions = (fuzz_execution_count(output_path)
                      if case_id in FUZZ_CASES else None)
        passed = result.returncode == 0 and (
            case_id not in FUZZ_CASES or executions is not None)
        write_public_execution(case_id=case_id, source=source,
                               passed=passed, duration_ms=duration_ms,
                               fuzz_executions=executions)
        if not passed:
            raise ValueError("COMMAND")
    elif result.returncode:
        raise ValueError("COMMAND")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        raise SystemExit("STRESS_GUARDED_COMMAND_REJECTED") from None
