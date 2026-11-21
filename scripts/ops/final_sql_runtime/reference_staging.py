"""Offline, read-only pre-upload admission for the three preserved originals.

The caller must independently supply the reviewed workflow source identity.
Success attests local bytes only; it does not authorize an upload.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat


SUBJECTS = frozenset(("before", "published", "postgresql"))
MAX_ORIGINAL_BYTES = 1_000_000_000
CHUNK = 1024 * 1024
LOCK = Path(__file__).with_name("reference-originals.lock.json")
ORIGINAL_NAME = re.compile(r"[a-z][a-z0-9-]{0,95}\.zip")


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def _hex(value: object, length: int) -> bool:
    return type(value) is str and len(value) == length and all(c in "0123456789abcdef" for c in value)


def _positive(value: object) -> bool:
    return type(value) is int and value > 0


def _pins(lock: dict) -> dict:
    _require(type(lock) is dict and set(lock) == {"schema", "originals"} and
             lock["schema"] == "alopex.reference-original-staging/v1" and
             type(lock["originals"]) is dict and set(lock["originals"]) == SUBJECTS,
             "LOCK_SCHEMA")
    pins = lock["originals"]
    names = set()
    for subject in SUBJECTS:
        pin = pins[subject]
        _require(type(pin) is dict and set(pin) == {"basename", "zip_sha256", "zip_size_bytes"} and
                 type(pin["basename"]) is str and
                 ORIGINAL_NAME.fullmatch(pin["basename"]) and
                 _hex(pin["zip_sha256"], 64) and
                 _positive(pin["zip_size_bytes"]) and
                 pin["zip_size_bytes"] <= MAX_ORIGINAL_BYTES,
                 "LOCK_PIN")
        names.add(pin["basename"])
    _require(len(names) == len(SUBJECTS), "LOCK_PIN")
    return pins


def _identity(run: dict, expected: dict) -> None:
    _require(type(run) is dict and set(run) == {"run_id", "run_attempt", "event", "head_sha",
                                                "repository_id", "workflow_id"} and
             type(expected) is dict and set(expected) == {"head_sha", "repository_id", "workflow_id"} and
             _positive(run["run_id"]) and run["run_attempt"] == 1 and
             type(run["run_attempt"]) is int and run["event"] == "workflow_dispatch" and
             _hex(expected["head_sha"], 40) and
             _positive(expected["repository_id"]) and _positive(expected["workflow_id"]) and
             run["head_sha"] == expected["head_sha"] and
             type(run["repository_id"]) is int and
             run["repository_id"] == expected["repository_id"] and
             type(run["workflow_id"]) is int and
             run["workflow_id"] == expected["workflow_id"], "WORKFLOW_IDENTITY")


def verify_staging(directory: Path, *, run: dict, expected: dict, lock: dict) -> dict:
    """Verify exact private file inventory and bytes; return a safe upload manifest.

    `expected` comes from workflow review, independently of the current run.
    No path or unreviewed metadata is emitted in the returned manifest.
    """
    _identity(run, expected)
    pins = _pins(lock)
    _require(isinstance(directory, Path) and directory.is_absolute(), "STAGING_PATH")
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and
                 stat.S_IMODE(info.st_mode) == 0o700, "STAGING_PRIVACY")
        names = {pin["basename"] for pin in pins.values()}
        _require(set(os.listdir(fd)) == names, "STAGING_INVENTORY")
        for subject in sorted(SUBJECTS):
            pin = pins[subject]
            file_fd = os.open(pin["basename"], os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=fd)
            try:
                before = os.fstat(file_fd)
                _require(stat.S_ISREG(before.st_mode) and before.st_uid == os.getuid() and
                         stat.S_IMODE(before.st_mode) == 0o400, "ORIGINAL_PRIVACY")
                _require(before.st_size == pin["zip_size_bytes"], "ORIGINAL_SIZE")
                digest = hashlib.sha256()
                size = 0
                while block := os.read(file_fd, CHUNK):
                    size += len(block)
                    _require(size <= pin["zip_size_bytes"], "ORIGINAL_BOUND")
                    digest.update(block)
                after = os.fstat(file_fd)
                _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                          before.st_ctime_ns) ==
                         (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                          after.st_ctime_ns) and size == pin["zip_size_bytes"] and
                         digest.hexdigest() == pin["zip_sha256"], "ORIGINAL_BYTES")
                entry = os.stat(pin["basename"], dir_fd=fd, follow_symlinks=False)
                _require((entry.st_dev, entry.st_ino) == (before.st_dev, before.st_ino),
                         "ORIGINAL_REPLACED")
            finally:
                os.close(file_fd)
        _require(set(os.listdir(fd)) == names, "STAGING_INVENTORY")
        return {"status": "verified", "run_id": run["run_id"],
                "head_sha": expected["head_sha"],
                "originals": {subject: dict(pins[subject]) for subject in sorted(SUBJECTS)}}
    finally:
        os.close(fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--expected-head-sha", required=True)
    parser.add_argument("--expected-repository-id", type=int, required=True)
    parser.add_argument("--expected-workflow-id", type=int, required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--event", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--repository-id", type=int, required=True)
    parser.add_argument("--workflow-id", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        lock = json.loads(LOCK.read_text(encoding="utf-8"))
        result = verify_staging(args.directory,
                                run={"run_id": args.run_id, "run_attempt": args.run_attempt,
                                     "event": args.event, "head_sha": args.head_sha,
                                     "repository_id": args.repository_id,
                                     "workflow_id": args.workflow_id},
                                expected={"head_sha": args.expected_head_sha,
                                          "repository_id": args.expected_repository_id,
                                          "workflow_id": args.expected_workflow_id}, lock=lock)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        code = str(exc) if type(exc) is ValueError and str(exc).isupper() else "STAGING_IO"
        print(json.dumps({"status": "rejected", "code": code}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
