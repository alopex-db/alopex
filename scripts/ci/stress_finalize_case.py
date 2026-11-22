"""Split one Stage-2 case into fixed public status and private CMS evidence.

Run this through stress_guarded_command.py. Only the returned case JSON and CMS
may be considered for upload; raw target directories remain private.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys

from stress_private_evidence import seal
from stress_public_evidence import build_status, CASE_IDS, SHA40, write_status


WORKSPACE = Path(__file__).resolve().parents[2]
CERTIFICATE = WORKSPACE / ".github/ops/native-evidence-cert.pem"
CERT_SHA256 = "d33a612dab2f76986e0dc6ba0ba45be15265ceccd2d56b07c14f9f696259f6b2"
FAILURE_CODES = {
    "stress": "TEST", "asan": "SANITIZER", "tsan": "SANITIZER",
    "lsan": "SANITIZER", "msan": "SANITIZER", "sql_parser": "FUZZ",
    "dataframe_conversion": "FUZZ", "perf": "PERF",
}


def _private_dir(parent: Path, name: str) -> Path:
    path = parent / name
    if path.is_symlink():
        raise ValueError("OUTPUT")
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("OUTPUT")
    return path


def finalize(*, workspace: Path, runner_temp: Path, case_id: str,
             outcome: str, source: str, certificate: Path,
             expected_cert_sha256: str) -> tuple[Path, Path | None, bool]:
    if case_id not in CASE_IDS or outcome not in {"passed", "failed"}:
        raise ValueError("STATE")
    if not SHA40.fullmatch(source):
        raise ValueError("SOURCE")
    try:
        head = subprocess.check_output(
            ["git", "-C", str(workspace), "rev-parse", "HEAD"],
            text=True, timeout=15).strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise ValueError("SOURCE") from None
    if head != source:
        raise ValueError("SOURCE")
    if runner_temp.is_symlink() or not runner_temp.is_dir():
        raise ValueError("STATE")
    target = workspace / "target"
    if target.is_symlink():
        raise ValueError("OUTPUT")
    target.mkdir(mode=0o700, exist_ok=True)
    if target.stat().st_uid != os.getuid():
        raise ValueError("OUTPUT")
    publish = _private_dir(target, "stress-publish")
    output = _private_dir(publish, case_id)
    status_path = output / f"{case_id}.json"
    ciphertext = output / f"{case_id}.cms"
    if status_path.exists() or ciphertext.exists():
        raise ValueError("OUTPUT")
    sealed = False
    command_log = (runner_temp / "stress-private" /
                   f"{case_id}-test.output.private.log")
    log_present = command_log.exists() or command_log.is_symlink()
    try:
        if outcome == "passed" and not log_present:
            raise ValueError("EVIDENCE")
        seal(workspace=workspace, case_id=case_id,
             source_commit=source, certificate=certificate,
             expected_cert_sha256=expected_cert_sha256, output=ciphertext,
             command_log=command_log if log_present else None)
        sealed = True
    except (OSError, ValueError, subprocess.TimeoutExpired):
        # A failed test may not have produced any raw files. Preserve the fixed
        # failure status without publishing an unverified ciphertext.
        if outcome == "passed":
            outcome = "failed"
    error_code = ("NONE" if outcome == "passed" and sealed else
                  FAILURE_CODES[case_id] if sealed else "EVIDENCE")
    status = build_status(case_id=case_id, source_commit=source,
                          outcome=outcome, error_code=error_code,
                          ciphertext=ciphertext if sealed else None)
    write_status(status_path, status)
    return status_path, ciphertext if sealed else None, outcome == "passed"


def main() -> None:
    os.umask(0o077)
    args = sys.argv[1:]
    if len(args) != 4 or args[0] != "--case" or args[2] != "--outcome":
        raise ValueError("ARGUMENT")
    source = os.environ["GITHUB_SHA"]
    status, _, passed = finalize(
        workspace=WORKSPACE, runner_temp=Path(os.environ["RUNNER_TEMP"]),
        case_id=args[1], outcome=args[3], source=source,
        certificate=CERTIFICATE, expected_cert_sha256=CERT_SHA256)
    if not status.is_file() or not passed:
        raise ValueError("CASE")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        raise SystemExit("STRESS_CASE_FINALIZATION_REJECTED") from None
