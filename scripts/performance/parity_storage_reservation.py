"""Reserve a bounded parity run before any container or build operation.

This is one admission predicate. It does not qualify the selected host or its
security policy; the workflow must run those checks before construction too.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test-env"))
from capacity import Budget  # noqa: E402
from common import Failure, require  # noqa: E402


SCHEMA = "alopex.parity-storage-reservation/v1"
STAGES = (
    "setup", "peer", "dependencies", "build", "contracts", "differential",
    "datasets", "hnsw", "paired", "sql_cost", "prepared_batch", "gate",
    "audit", "cleanup",
)
HARD_SECONDS = 360 * 60
STOP_BYTES = 40_000_000_000


def _private_file(path: Path) -> None:
    info = path.lstat()
    require(path.is_file() and not path.is_symlink(), "INPUT")
    require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "INPUT")
    parent = path.parent.stat()
    require(parent.st_uid == os.getuid() and not parent.st_mode & 0o077, "INPUT")


def _roots(values: object) -> list[str]:
    require(isinstance(values, list) and bool(values), "INPUT")
    roots = []
    for value in values:
        require(isinstance(value, str) and bool(value), "INPUT")
        path = Path(value)
        require(path.is_absolute() and path.exists() and not path.is_symlink(), "MEASUREMENT")
        resolved = path.resolve(strict=True)
        require(str(resolved) == value and resolved.is_dir(), "MEASUREMENT")
        require(all(not (resolved.is_relative_to(other) or other.is_relative_to(resolved))
                    for other in roots), "MEASUREMENT")
        roots.append(resolved)
    return [str(root) for root in roots]


def reserve(profile_path: Path, *, source_sha: str, git_head: str,
            run_id: int, run_attempt: int, selected_suite: str) -> dict:
    _private_file(profile_path)
    require(profile_path.stat().st_size <= 64 * 1024, "INPUT")
    try:
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError) as error:
        raise Failure("INPUT") from error
    require(isinstance(profile, dict) and profile.get("schema") == SCHEMA, "INPUT")
    require(re.fullmatch(r"[0-9a-f]{40}", source_sha) is not None, "IDENTITY")
    require(profile.get("source_commit") == source_sha == git_head, "IDENTITY")
    require(type(run_id) is int and run_id > 0 and
            type(run_attempt) is int and run_attempt > 0 and
            profile.get("run_id") == run_id and
            profile.get("run_attempt") == run_attempt, "IDENTITY")
    suite = profile.get("suite")
    require(suite in ("curated", "full") and suite == selected_suite, "INPUT")
    require(profile.get("paired_invocations") == (240 if suite == "curated" else 288), "INPUT")
    stages = profile.get("stages")
    require(isinstance(stages, dict) and tuple(stages) == STAGES, "INPUT")
    conservative = 0
    for name in STAGES:
        row = stages[name]
        require(isinstance(row, dict), "INPUT")
        for key in ("input_units", "observed_units", "observed_seconds",
                    "expected_seconds", "conservative_seconds"):
            require(type(row.get(key)) is int and row[key] > 0, "INPUT")
        require(row["observed_units"] <= row["input_units"], "INPUT")
        pilot_scaled_seconds = ((row["observed_seconds"] * row["input_units"] +
                                 row["observed_units"] - 1) // row["observed_units"])
        require(row["expected_seconds"] >= pilot_scaled_seconds and
                row["conservative_seconds"] >= row["expected_seconds"], "CAPACITY")
        conservative += row["conservative_seconds"]
    margin = profile.get("margin_seconds")
    require(type(margin) is int and margin > 0 and conservative + margin < HARD_SECONDS, "CAPACITY")
    storage = profile.get("storage")
    require(isinstance(storage, dict), "INPUT")
    roots = _roots(storage.get("roots"))
    runtime = storage.get("runtime_storage_roots")
    require(isinstance(runtime, list) and bool(runtime) and
            all(isinstance(root, str) and root in roots for root in runtime), "MEASUREMENT")
    require(storage.get("stop_bytes") == STOP_BYTES, "CAPACITY")
    require(type(storage.get("reserve_bytes")) is int and storage["reserve_bytes"] > 0, "CAPACITY")
    require(type(storage.get("planned_run_bytes")) is int and storage["planned_run_bytes"] > 0, "CAPACITY")
    require(type(storage.get("poll_seconds")) is int and storage["poll_seconds"] > 0, "INPUT")
    config = {
        "roots": roots, "stop_bytes": STOP_BYTES,
        "reserve_bytes": storage["reserve_bytes"],
        "poll_seconds": storage["poll_seconds"],
        "planned_bytes": {"parity_run": storage["planned_run_bytes"]},
    }
    if "readonly_sudo_du_root" in storage:
        require(storage["readonly_sudo_du_root"] in roots, "MEASUREMENT")
        config["readonly_sudo_du_root"] = storage["readonly_sudo_du_root"]
        config["readonly_sudo_du_timeout_seconds"] = storage.get("readonly_sudo_du_timeout_seconds")
    used = Budget(config).before("parity_run")
    return {"schema": SCHEMA, "source_commit": source_sha,
            "run_id": run_id, "run_attempt": run_attempt, "suite": suite,
            "paired_invocations": profile["paired_invocations"],
            "conservative_seconds": conservative, "margin_seconds": margin,
            "allocated_bytes": used, "planned_run_bytes": storage["planned_run_bytes"]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    try:
        source_sha = os.environ["GITHUB_SHA"]
        git_head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                           stderr=subprocess.DEVNULL).strip()
        receipt = reserve(args.profile, source_sha=source_sha, git_head=git_head,
                          run_id=int(os.environ["GITHUB_RUN_ID"]),
                          run_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
                          selected_suite=os.environ["PARITY_SUITE"])
        require(args.receipt.parent.is_dir() and not args.receipt.is_symlink(), "INPUT")
        os.umask(0o077)
        with args.receipt.open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.write("\n")
        print("PARITY_STORAGE_RESERVATION_OK")
        return 0
    except (Failure, OSError, KeyError, ValueError, subprocess.CalledProcessError) as error:
        code = error.code if isinstance(error, Failure) else "INTERNAL"
        print(f"PARITY_STORAGE_RESERVATION_{code}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
