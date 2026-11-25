"""Encrypt selected-runner facts; export only CMS and fixed public status."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile

from parity_selected_inventory import CODES, MAX_RECEIPT, PRIVATE_NAME, SCHEMA

REPO = Path(__file__).resolve().parents[2]
CERT = REPO / ".github/ops/native-evidence-cert.pem"
CERT_SHA256 = "d33a612dab2f76986e0dc6ba0ba45be15265ceccd2d56b07c14f9f696259f6b2"
MAX_CIPHERTEXT = 2_097_152


def owner_only(path: Path, directory: bool) -> bool:
    info = path.lstat()
    return (info.st_uid == os.getuid() and not info.st_mode & 0o077 and
            (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)))


def _seal_inner(private: Path, export: Path, collect_outcome: str) -> tuple[dict, int]:
    public = {"schema": "alopex.parity-selected-inventory-public/v1",
              "case": "PARITY_SELECTED_INVENTORY", "outcome": "failed",
              "error_code": "EVIDENCE"}
    try:
        export.mkdir(mode=0o700)
        if not owner_only(private, True) or not owner_only(export, True):
            return public, 1
        receipt = private / "inventory.private.json"
        if not owner_only(receipt, False) or not 0 < receipt.stat().st_size <= MAX_RECEIPT:
            return public, 1
        raw = json.loads(receipt.read_bytes())
        if not isinstance(raw, dict) or raw.get("schema") != SCHEMA or raw.get("read_only") is not True:
            return public, 1
        if (raw.get("source_commit") != os.environ.get("GITHUB_SHA") or
                raw.get("run_id") != os.environ.get("GITHUB_RUN_ID") or
                raw.get("run_attempt") != os.environ.get("GITHUB_RUN_ATTEMPT") or
                raw.get("runner_name") != os.environ.get("RUNNER_NAME") or
                not all(os.environ.get(key) for key in
                        ("GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "RUNNER_NAME"))):
            public["error_code"] = "IDENTITY"
            return public, 1
        code = raw.get("error_code")
        if collect_outcome == "success":
            if code is not None or raw.get("qualification") != "not_assessed":
                return public, 1
            public.update(outcome="collected_not_qualified", error_code=None)
        elif collect_outcome == "failure":
            if raw.get("outcome") != "incomplete" or code not in (CODES | {"INTERNAL"}):
                return public, 1
            public["error_code"] = code
        else:
            return public, 1
        if hashlib.sha256(CERT.read_bytes()).hexdigest() != CERT_SHA256:
            public.update(outcome="failed", error_code="CERT")
            return public, 1
        fd, name = tempfile.mkstemp(prefix=".parity-cms-", dir=export)
        os.close(fd)
        temp = Path(name)
        try:
            result = subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes256",
                                     "-outform", "DER", "-in", str(receipt), "-out",
                                     str(temp), str(CERT)], stdin=subprocess.DEVNULL,
                                    capture_output=True, timeout=30, check=False)
            if result.returncode or not 0 < temp.stat().st_size <= MAX_CIPHERTEXT:
                public.update(outcome="failed", error_code="SEAL")
                return public, 1
            temp.replace(export / "private-evidence.cms")
        finally:
            temp.unlink(missing_ok=True)
        public["ciphertext_sha256"] = hashlib.sha256(
            (export / "private-evidence.cms").read_bytes()).hexdigest()
        # A successfully sealed incomplete receipt is uploadable evidence;
        # the collector step still records the diagnostic failure for the job.
        return public, 0
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        public.update(outcome="failed", error_code="SEAL")
        return public, 1


def seal(private: Path, export: Path, collect_outcome: str) -> tuple[dict, int]:
    public, result = _seal_inner(private, export, collect_outcome)
    try:
        if owner_only(private, True):
            shutil.rmtree(private)
        elif private.exists():
            raise OSError("private owner or mode")
    except OSError:
        # An encrypted copy does not justify uploading while plaintext remains.
        public.update(outcome="failed", error_code="CLEANUP")
        return public, 1
    return public, result


def main() -> int:
    os.umask(0o077)
    private = Path(os.environ["RUNNER_TEMP"]) / PRIVATE_NAME
    export = Path("parity-selected-inventory-export")
    public, result = seal(private, export, os.environ.get("COLLECT_OUTCOME", ""))
    try:
        export.mkdir(mode=0o700, exist_ok=True)
        with (export / "public.json").open("xb") as stream:
            stream.write((json.dumps(public, sort_keys=True) + "\n").encode())
    except OSError:
        print("PARITY_SELECTED_INVENTORY_EVIDENCE", file=sys.stderr)
        return 1
    print("PARITY_SELECTED_INVENTORY_" + (
        "COLLECTED" if result == 0 else public["error_code"] or "EVIDENCE"))
    return result


if __name__ == "__main__":
    raise SystemExit(main())
