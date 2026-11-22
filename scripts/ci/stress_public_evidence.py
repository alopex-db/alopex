"""Write a bounded, allowlisted public status for one Stage-2 stress case.

Raw reports and diagnostics are never parsed or copied here. A CMS ciphertext
may be named only by its digest and byte size; encryption and intake are
separate gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess


CASE_IDS = frozenset({
    "stress", "asan", "tsan", "lsan", "msan", "sql_parser",
    "dataframe_conversion", "perf",
})
OUTCOMES = frozenset({"passed", "failed", "skipped"})
ERROR_CODES = frozenset({
    "NONE", "SOURCE", "STORAGE", "TOOLCHAIN", "PARSER", "BUILD",
    "TEST", "SANITIZER", "FUZZ", "PERF", "EVIDENCE",
})
MAX_CMS_BYTES = 100_000_000
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
ENVELOPED_DATA_OID = bytes.fromhex("06092a864886f70d010703")
BASE_FIELDS = frozenset({"schema", "case_id", "source_commit", "outcome", "error_code"})
CMS_FIELDS = frozenset({"ciphertext_sha256", "ciphertext_size"})


def ciphertext_identity(path: Path) -> tuple[str, int]:
    if path.suffix != ".cms" or not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("EVIDENCE")
    try:
        before = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        # Unbuffered so OpenSSL inherits the same descriptor at offset zero.
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (before.st_dev, before.st_ino)
                    != (info.st_dev, info.st_ino) or not 0 < info.st_size <= MAX_CMS_BYTES):
                raise ValueError("EVIDENCE")
            size = info.st_size
            header = stream.read(32)
            if len(header) < 13 or header[0] != 0x30:
                raise ValueError("EVIDENCE")
            encoded = header[1]
            if encoded < 0x80:
                body_size, offset = encoded, 2
            else:
                width = encoded & 0x7f
                if not 1 <= width <= 4:
                    raise ValueError("EVIDENCE")
                body_size = int.from_bytes(header[2:2 + width], "big")
                offset = 2 + width
            if (offset + body_size != size
                    or header[offset:offset + len(ENVELOPED_DATA_OID)] != ENVELOPED_DATA_OID):
                raise ValueError("EVIDENCE")
            stream.seek(0)
            parsed = subprocess.run(
                ["openssl", "cms", "-cmsout", "-inform", "DER", "-noout"],
                stdin=stream, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=30, check=False,
            )
            if parsed.returncode:
                raise ValueError("EVIDENCE")
            stream.seek(0)
            digest = hashlib.sha256()
            total = 0
            while chunk := stream.read(min(1024 * 1024, MAX_CMS_BYTES + 1 - total)):
                total += len(chunk)
                if total > MAX_CMS_BYTES:
                    raise ValueError("EVIDENCE")
                digest.update(chunk)
            after = os.fstat(stream.fileno())
            if total != size or after.st_size != size or after.st_mtime_ns != info.st_mtime_ns:
                raise ValueError("EVIDENCE")
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("EVIDENCE") from None
    return digest.hexdigest(), size


def build_status(*, case_id: str, source_commit: str, outcome: str,
                 error_code: str, ciphertext: Path | None = None) -> dict:
    if (case_id not in CASE_IDS or not SHA40.fullmatch(source_commit)
            or outcome not in OUTCOMES or error_code not in ERROR_CODES
            or (outcome == "passed") != (error_code == "NONE")):
        raise ValueError("STATUS")
    status = {
        "schema": "alopex.stress-public/v1",
        "case_id": case_id,
        "source_commit": source_commit,
        "outcome": outcome,
        "error_code": error_code,
    }
    if ciphertext is not None:
        digest, size = ciphertext_identity(ciphertext)
        status["ciphertext_sha256"] = digest
        status["ciphertext_size"] = size
    return status


def write_status(destination: Path, status: dict) -> None:
    if (set(status) not in (BASE_FIELDS, BASE_FIELDS | CMS_FIELDS)
            or status.get("schema") != "alopex.stress-public/v1"
            or status.get("case_id") not in CASE_IDS
            or not isinstance(status.get("source_commit"), str)
            or not SHA40.fullmatch(status["source_commit"])
            or status.get("outcome") not in OUTCOMES
            or status.get("error_code") not in ERROR_CODES
            or (status["outcome"] == "passed") != (status["error_code"] == "NONE")):
        raise ValueError("STATUS")
    if CMS_FIELDS <= set(status) and (
            not isinstance(status["ciphertext_sha256"], str)
            or not SHA256.fullmatch(status["ciphertext_sha256"])
            or not isinstance(status["ciphertext_size"], int)
            or not 0 < status["ciphertext_size"] <= MAX_CMS_BYTES):
        raise ValueError("STATUS")
    if (destination.is_symlink() or destination.exists()
            or destination.parent.is_symlink() or not destination.parent.is_dir()):
        raise ValueError("OUTPUT")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    payload = (json.dumps(status, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(destination, flags, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True, choices=sorted(CASE_IDS))
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--outcome", required=True, choices=sorted(OUTCOMES))
    parser.add_argument("--error-code", required=True, choices=sorted(ERROR_CODES))
    parser.add_argument("--ciphertext", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        status = build_status(case_id=args.case_id,
                              source_commit=args.source_commit,
                              outcome=args.outcome,
                              error_code=args.error_code,
                              ciphertext=args.ciphertext)
        write_status(args.output, status)
    except (OSError, ValueError):
        raise SystemExit("STRESS_PUBLIC_EVIDENCE_REJECTED") from None


if __name__ == "__main__":
    main()
