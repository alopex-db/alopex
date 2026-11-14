"""Encrypt the private signed-index capture before the hosted runner expires."""
from __future__ import annotations

import argparse
import hashlib
import io
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
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "test-env"))
from capacity import Budget  # noqa: E402
from common import read_json, require  # noqa: E402

LIMIT = 250_000_000
SEAL_RESERVE = 2_000_000
FAILURE_FILE_LIMIT = 256_000
FAILURE_PLAINTEXT_LIMIT = 1_000_000
FAILURE_CIPHERTEXT_LIMIT = 2_000_000
FAILURE_FILES = ("failure.json", "run.json", "capture-budget.private.json")
OMITTED_FAILURE_FILES = ("collected", "collection.pending", "proof", "pull.log",
                         "stage.log", "container.log", "collect.log", "proof.log",
                         "bootstrap.private.json", "collection.json")
FAILURE_CODES = frozenset(("CAPACITY", "COLLECT", "COMMAND", "IDENTITY",
                           "MEASUREMENT", "PREREQUISITE", "STATE", "TIMEOUT"))


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


def _seal_full(private, certificate, output):
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
        growth = max(raw_bytes, 0, used_before - baseline) if baseline is not None else raw_bytes
        headroom = (profile["storage"]["stop_bytes"] -
                    profile["storage"]["reserve_bytes"] - used_before)
        remaining = min(LIMIT - growth, headroom) - SEAL_RESERVE
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


def _seal_failure(private, certificate, output, original):
    """Seal only bounded diagnostic metadata when the complete archive cannot fit."""
    profile = read_json(private / "profile.json", private=True)
    run = private / "runs" / (profile["run_id"] + "-trace-resolve")
    require(run.is_dir() and not run.is_symlink() and not output.exists() and
            not output.is_symlink(), "COLLECT")
    require(certificate.is_file() and not certificate.is_symlink(), "PREREQUISITE")
    require(output.parent.is_dir() and not output.parent.is_symlink(), "COLLECT")
    retained = {}
    for name in FAILURE_FILES:
        path = run / name
        if path.exists():
            mode = path.lstat().st_mode
            require(stat.S_ISREG(mode) and path.stat().st_size <= FAILURE_FILE_LIMIT,
                    "COLLECT")
            retained[name] = path.read_bytes()
    require("run.json" in retained, "COLLECT")
    original_code = getattr(original, "code", None)
    failure = {
        "schema": "alopex.signed-index-failure/v1",
        "seal_error_code": original_code if original_code in FAILURE_CODES else "INTERNAL",
        "seal_error_stage": "full_seal",
        "full_evidence_incomplete": True,
        "retained": sorted(retained),
        "omitted_from_failure_receipt": list(OMITTED_FAILURE_FILES),
    }
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for name, data in retained.items():
            info = tarfile.TarInfo("evidence/" + name)
            info.mode = 0o600
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        data = (json.dumps(failure, sort_keys=True) + "\n").encode()
        info = tarfile.TarInfo("evidence/seal-failure.private.json")
        info.mode = 0o600
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    require(payload.tell() <= FAILURE_PLAINTEXT_LIMIT, "CAPACITY")
    encrypted = subprocess.run(
        ["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER",
         "-in", "/dev/stdin", str(certificate)], input=payload.getvalue(),
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30, check=False)
    require(encrypted.returncode == 0 and
            0 < len(encrypted.stdout) <= FAILURE_CIPHERTEXT_LIMIT, "COLLECT")
    budget = Budget(profile["storage"])
    receipt_path = run / "capture-budget.private.json"
    receipt = read_json(receipt_path, private=True) if receipt_path.exists() else None
    if receipt is not None:
        require(receipt.get("capture_limit_bytes") == LIMIT, "IDENTITY")
    baseline = receipt.get("allocated_before_bytes") if receipt is not None else None
    require(baseline is None or type(baseline) is int, "IDENTITY")
    used = budget.used()
    growth = len(encrypted.stdout) + SEAL_RESERVE
    require((baseline is None or used - baseline + growth <= LIMIT) and
            used + profile["storage"]["reserve_bytes"] + growth <
            profile["storage"]["stop_bytes"], "CAPACITY")
    staging = Path(tempfile.mkdtemp(prefix=output.name + ".pending-", dir=output.parent))
    try:
        ciphertext = staging / "private-evidence.cms"
        ciphertext.write_bytes(encrypted.stdout)
        public = {"schema": "alopex.signed-index-capture/v1",
                  "case": "SIGNED_INDEX_CAPTURE", "outcome": "failed",
                  "error_code": "EVIDENCE_FAILURE",
                  "evidence_sha256": digest(ciphertext)}
        (staging / "public.json").write_text(json.dumps(public, sort_keys=True) + "\n")
        used = budget.used()
        require((baseline is None or used - baseline <= LIMIT) and
                used + profile["storage"]["reserve_bytes"] <
                profile["storage"]["stop_bytes"], "CAPACITY")
        require(not output.exists() and not output.is_symlink(), "COLLECT")
        staging.rename(output)
        return public
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def seal(private, certificate, output):
    try:
        return _seal_full(private, certificate, output)
    except Exception as original:
        if output.exists() or output.is_symlink():
            raise
        return _seal_failure(private, certificate, output, original)


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
        print(json.dumps({"case": result["case"], "outcome": result["outcome"],
                          "error_code": result["error_code"]}))
        return 0 if result["outcome"] == "passed" else 1
    except BaseException:
        print('{"case":"SIGNED_INDEX_CAPTURE","outcome":"failed","error_code":"EVIDENCE_FAILURE"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
