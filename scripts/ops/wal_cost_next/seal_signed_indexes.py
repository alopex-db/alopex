"""Encrypt the private signed-index capture before the hosted runner expires."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import stat
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "test-env"))
from capacity import Budget  # noqa: E402
from common import read_json, require  # noqa: E402

LIMIT = 250_000_000
SEAL_RESERVE = 2_000_000


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def audit(root):
    require(root.is_dir() and not root.is_symlink(), "COLLECT")
    size = 0
    for entry in root.rglob("*"):
        mode = entry.lstat().st_mode
        require(stat.S_ISDIR(mode) or stat.S_ISREG(mode), "COLLECT")
        if stat.S_ISREG(mode):
            size += entry.stat().st_size
            require(size <= LIMIT, "CAPACITY")
    return size


def seal(private, certificate, output):
    if output.exists():
        require(output.is_dir() and not output.is_symlink(), "COLLECT")
        public = read_json(output / "public.json")
        ciphertext = output / "private-evidence.cms"
        require(public.get("schema") == "alopex.signed-index-capture/v1" and
                public.get("outcome") in ("passed", "failed") and ciphertext.is_file() and
                not ciphertext.is_symlink() and
                digest(ciphertext) == public.get("evidence_sha256"), "COLLECT")
        return public
    profile = read_json(private / "profile.json", private=True)
    run = private / "runs" / (profile["run_id"] + "-trace-resolve")
    if not run.exists():
        return None
    receipt_path = run / "capture-budget.private.json"
    receipt = read_json(receipt_path, private=True) if receipt_path.exists() else None
    if receipt is not None:
        require(receipt["capture_limit_bytes"] == LIMIT and
                type(receipt["allocated_before_bytes"]) is int, "IDENTITY")
    raw_bytes = audit(run)
    run_record = read_json(run / "run.json", private=True)
    complete = (run_record.get("phase") == "deleted" and
                not (run / "failure.json").exists() and
                (run / "collected" / "capture.status").is_file() and
                (run / "proof" / "proof.sha256").is_file() and
                (run / "collected" / "capture.status").read_text() == "capture-complete\n")
    require(complete or (run / "failure.json").is_file(), "COLLECT")
    require(certificate.is_file() and not certificate.is_symlink(), "PREREQUISITE")
    budget = Budget(profile["storage"])
    baseline = receipt["allocated_before_bytes"] if receipt is not None else None
    if baseline is not None:
        require(budget.used() - baseline <= LIMIT, "CAPACITY")
    files = (["collected", "proof", "bootstrap.private.json", "capture-budget.private.json",
              "run.json", "collection.json"] if complete else
             [name for name in ("failure.json", "pull.log", "stage.log", "container.log",
                                "collect.log", "proof.log", "bootstrap.private.json",
                                "capture-budget.private.json", "run.json", "collection.json",
                                "collected", "collection.pending")
              if (run / name).is_file() or (name in ("collected", "collection.pending") and
                                           (run / name).is_dir())])
    require(bool(files), "COLLECT")
    require(output.parent.is_dir() and not output.parent.is_symlink(), "COLLECT")
    staging = Path(tempfile.mkdtemp(prefix=output.name + ".pending-", dir=output.parent))
    staged_identity = (staging.stat().st_dev, staging.stat().st_ino)
    ciphertext = staging / "private-evidence.cms"
    tar = openssl = None
    try:
        used_before = budget.used()
        remaining = (LIMIT - max(raw_bytes, 0, used_before - baseline) - SEAL_RESERVE
                     if baseline is not None else
                     min(LIMIT - raw_bytes - SEAL_RESERVE,
                         profile["storage"]["stop_bytes"] -
                         profile["storage"]["reserve_bytes"] - used_before - SEAL_RESERVE))
        require(0 < remaining <= LIMIT - SEAL_RESERVE, "CAPACITY")
        tar = subprocess.Popen(["tar", "-C", str(run), "-czf", "-", *files],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               stdin=subprocess.DEVNULL, start_new_session=True)
        assert tar.stdout is not None
        openssl = subprocess.Popen(["openssl", "cms", "-encrypt", "-aes-256-cbc",
                                    "-binary", "-outform", "DER", "-in", "/dev/stdin",
                                    str(certificate)], stdin=tar.stdout,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   start_new_session=True)
        tar.stdout.close()
        assert openssl.stdout is not None
        deadline = time.monotonic() + 300
        next_sample = time.monotonic() + 10
        written = 0
        with ciphertext.open("xb") as stream:
            while True:
                require(time.monotonic() < deadline, "TIMEOUT")
                ready, _, _ = select.select([openssl.stdout], [], [], 1)
                if ready:
                    chunk = os.read(openssl.stdout.fileno(), 1024 * 1024)
                    if not chunk:
                        break
                    require(written + len(chunk) <= remaining, "CAPACITY")
                    stream.write(chunk)
                    written += len(chunk)
                if time.monotonic() >= next_sample:
                    used = budget.used()
                    require((baseline is None or used - baseline <= LIMIT) and
                            used + profile["storage"]["reserve_bytes"] <
                            profile["storage"]["stop_bytes"], "CAPACITY")
                    next_sample = time.monotonic() + 10
        tar.wait(timeout=10)
        openssl.wait(timeout=10)
        require(tar.returncode == 0 and openssl.returncode == 0 and
                ciphertext.is_file() and ciphertext.stat().st_size > 0, "COLLECT")
        require(baseline is None or budget.used() - baseline <= LIMIT, "CAPACITY")
        public = {"schema": "alopex.signed-index-capture/v1", "case": "SIGNED_INDEX_CAPTURE",
                  "outcome": "passed" if complete else "failed",
                  "error_code": None if complete else "CAPTURE_FAILURE",
                  "evidence_sha256": digest(ciphertext)}
        (staging / "public.json").write_text(json.dumps(public, sort_keys=True) + "\n")
        require(not output.exists() and not output.is_symlink(), "COLLECT")
        staging.rename(output)
        return public
    except BaseException:
        for child in (tar, openssl):
            if child is not None and child.poll() is None:
                try:
                    child.terminate()
                    child.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    child.kill()
                    child.wait(timeout=10)
        raise
    finally:
        if staging.exists():
            info = staging.lstat()
            require(stat.S_ISDIR(info.st_mode) and
                    (info.st_dev, info.st_ino) == staged_identity, "COLLECT")
            shutil.rmtree(staging)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = seal(args.private, args.certificate, args.output)
        if result is None:
            return 0  # Wrapper failed before capture was attempted.
        print(json.dumps({"case": result["case"], "outcome": result["outcome"]}))
        return 0
    except BaseException:
        print('{"case":"SIGNED_INDEX_CAPTURE","outcome":"failed","error_code":"EVIDENCE_FAILURE"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
