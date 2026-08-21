#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one experiment owner and record it.")
    parser.add_argument("--owner", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise SystemExit("owner command is required")

    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    result = subprocess.run(command, check=False)
    completed_at = datetime.now(timezone.utc)
    record = {
        "schema": "alopex-issue-196-owner-result-v1",
        "owner": args.owner,
        "command": command,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "returncode": result.returncode,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()

