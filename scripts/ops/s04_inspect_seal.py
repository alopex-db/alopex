#!/usr/bin/env python3
"""Seal the S04 read-only inspector receipt; export only CMS and fixed status."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile

REPO = Path(__file__).resolve().parents[2]
CERT = REPO / ".github/ops/native-evidence-cert.pem"
CERT_SHA256 = "d33a612dab2f76986e0dc6ba0ba45be15265ceccd2d56b07c14f9f696259f6b2"
MAX_PLAINTEXT = 1_048_576
MAX_CIPHERTEXT = 2_097_152


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def owner_only(path: Path) -> bool:
    info = path.lstat()
    return (stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and
            not stat.S_IMODE(info.st_mode) & 0o077)


def _seal_inner(private: Path, export: Path, inspect_outcome: str) -> tuple[dict, int]:
    """Return allowlisted status and exit code; never return private exception text."""
    public = {"schema": "alopex.s04-readonly-inspect/v1", "case": "S04_INSPECT",
              "outcome": "failed", "error_code": "INCOMPLETE"}
    receipt = private / "inspector.private.json"
    ciphertext = export / "private-evidence.cms"
    try:
        export.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not owner_only(export):
            public["error_code"] = "EXPORT_MODE"
            return public, 1
        if inspect_outcome != "success":
            public["error_code"] = "INSPECT_FAILED"
            return public, 1
        if not owner_only(private):
            public["error_code"] = "PRIVATE_MODE"
            return public, 1
        if not receipt.is_file() or receipt.is_symlink() or receipt.stat().st_size > MAX_PLAINTEXT:
            public["error_code"] = "RECEIPT_INVALID"
            return public, 1
        info = receipt.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            public["error_code"] = "RECEIPT_MODE"
            return public, 1
        raw = json.loads(receipt.read_text())
        if not isinstance(raw, dict) or raw.get("read_only") is not True:
            public["error_code"] = "RECEIPT_INVALID"
            return public, 1
        if digest(CERT) != CERT_SHA256:
            public["error_code"] = "CERT_MISMATCH"
            return public, 1
        fd, temp_name = tempfile.mkstemp(prefix=".s04-cms-", dir=export)
        os.close(fd)
        temporary = Path(temp_name)
        try:
            process = subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes256",
                                      "-outform", "DER", "-in", str(receipt), "-out",
                                      str(temporary), str(CERT)], stdin=subprocess.DEVNULL,
                                     capture_output=True, check=False, timeout=30)
            if process.returncode or not 0 < temporary.stat().st_size <= MAX_CIPHERTEXT:
                public["error_code"] = "SEAL_FAILED"
                return public, 1
            if ciphertext.exists():
                public["error_code"] = "EXPORT_EXISTS"
                return public, 1
            temporary.replace(ciphertext)
        finally:
            temporary.unlink(missing_ok=True)
        public.update(outcome="inspected_read_only", error_code=None,
                      ciphertext_sha256=digest(ciphertext))
        return public, 0
    except (OSError, ValueError, TypeError, KeyError, subprocess.TimeoutExpired):
        public["error_code"] = "SEAL_FAILED"
        return public, 1


def seal(private: Path, export: Path, inspect_outcome: str) -> tuple[dict, int]:
    public, code = _seal_inner(private, export, inspect_outcome)
    # No plaintext inspector receipt enters an artifact, including failures.
    if private.name == "alopex-s04-inspect" and private.is_dir() and not private.is_symlink():
        try:
            if private.lstat().st_uid == os.getuid():
                shutil.rmtree(private)
        except OSError:
            public.update(outcome="failed", error_code="CLEANUP_FAILED")
            return public, 1
    return public, code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--inspect-outcome", required=True)
    args = parser.parse_args()
    public, code = seal(args.private, args.export, args.inspect_outcome)
    try:
        args.export.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not owner_only(args.export):
            raise OSError("export directory is not private")
        payload = (json.dumps(public, sort_keys=True) + "\n").encode()
        fd = os.open(args.export / "public.json", os.O_WRONLY | os.O_CREAT |
                     os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError:
        print('{"case":"S04_INSPECT","outcome":"evidence_failed"}')
        return 1
    print(json.dumps({"case": public["case"], "outcome": public["outcome"],
                      "error_code": public["error_code"]}, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
