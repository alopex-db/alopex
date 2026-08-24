#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
from typing import NamedTuple


SCHEMA = "alopex-ci-build-owner-result-v1"


class MeasurementRequest(NamedTuple):
    owner: str
    output: Path
    target_dir: Path
    summary: Path | None
    command: tuple[str, ...]


def directory_bytes(path: Path) -> int:
    if path.is_symlink() or not path.exists():
        return 0

    total = 0
    for directory, subdirectories, files in os.walk(path, followlinks=False):
        directory_path = Path(directory)
        subdirectories[:] = [
            name for name in subdirectories if not (directory_path / name).is_symlink()
        ]
        for name in files:
            candidate = directory_path / name
            if candidate.is_symlink():
                continue
            try:
                total += candidate.stat().st_size
            except OSError:
                # Cargo may replace an artifact while the inventory is walking.
                continue
    return total


def _run(command: Sequence[str]) -> int:
    return subprocess.run(command, check=False).returncode


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def measure(
    request: MeasurementRequest,
    *,
    runner: Callable[[Sequence[str]], int] = _run,
    monotonic: Callable[[], float] = time.monotonic,
    utcnow: Callable[[], datetime] = _utcnow,
) -> dict[str, object]:
    started_at = utcnow()
    started = monotonic()
    error: str | None = None
    try:
        returncode = runner(request.command)
    except OSError as exc:
        returncode = 127
        error = str(exc)
    completed_at = utcnow()
    elapsed_seconds = round(monotonic() - started, 3)

    record: dict[str, object] = {
        "schema": SCHEMA,
        "owner": request.owner,
        "command": list(request.command),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "elapsed_seconds": elapsed_seconds,
        "returncode": returncode,
        "target_dir": str(request.target_dir),
        "target_bytes": directory_bytes(request.target_dir),
    }
    if error is not None:
        record["error"] = error

    request.output.parent.mkdir(parents=True, exist_ok=True)
    request.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")

    if request.summary is not None:
        request.summary.parent.mkdir(parents=True, exist_ok=True)
        status = "success" if returncode == 0 else f"failure ({returncode})"
        with request.summary.open("a", encoding="utf-8") as stream:
            stream.write(f"### Build owner: `{request.owner}`\n\n")
            stream.write("| status | wall seconds | target bytes |\n")
            stream.write("|---|---:|---:|\n")
            stream.write(
                f"| {status} | {elapsed_seconds:.3f} | {record['target_bytes']} |\n\n"
            )

    return record


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one CI build owner and persist wall/target metrics."
    )
    parser.add_argument("--owner", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise SystemExit("owner command is required")
    target_dir = args.target_dir or Path(os.environ.get("CARGO_TARGET_DIR", "target"))
    summary = args.summary
    if summary is None and os.environ.get("GITHUB_STEP_SUMMARY"):
        summary = Path(os.environ["GITHUB_STEP_SUMMARY"])

    record = measure(
        MeasurementRequest(
            owner=args.owner,
            output=args.output,
            target_dir=target_dir,
            summary=summary,
            command=tuple(command),
        )
    )
    raise SystemExit(int(record["returncode"]))


if __name__ == "__main__":
    main()
