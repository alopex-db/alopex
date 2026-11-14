#!/usr/bin/env python3
"""Encrypt the bounded offline wire receipt; publish fixed status only."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

from s04_inspect_seal import CERT, CERT_SHA256, digest, owner_only


def seal(private: Path, export: Path, capture_outcome: str) -> tuple[dict, int]:
    public = {"schema": "alopex.s04-wire-capture/v1", "case": "S04_WIRE_CAPTURE",
              "outcome": "failed", "error_code": "CAPTURE_FAILED"}
    receipt = private / "s04-wire.private.json"
    temporary = None
    try:
        export.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not owner_only(export) or not owner_only(private):
            public["error_code"] = "PRIVATE_MODE"
            return public, 1
        if not receipt.exists() and capture_outcome != "success":
            return public, 1
        info = receipt.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or \
                stat.S_IMODE(info.st_mode) != 0o600 or not 0 < info.st_size <= 1_048_576:
            public["error_code"] = "RECEIPT_INVALID"
            return public, 1
        data = json.loads(receipt.read_text())
        if data.get("fake_only") is not True or data.get("outcome") not in (
                "CAPTURE_COMPLETE", "CAPTURE_INCOMPLETE"):
            public["error_code"] = "RECEIPT_INVALID"
            return public, 1
        if digest(CERT) != CERT_SHA256:
            public["error_code"] = "CERT_MISMATCH"
            return public, 1
        fd, name = tempfile.mkstemp(prefix=".s04-wire-", dir=export)
        os.close(fd)
        temporary = Path(name)
        result = subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes256",
                                 "-outform", "DER", "-in", str(receipt), "-out",
                                 str(temporary), str(CERT)], stdin=subprocess.DEVNULL,
                                capture_output=True, check=False, timeout=30)
        if result.returncode or not 0 < temporary.stat().st_size <= 2_097_152:
            public["error_code"] = "SEAL_FAILED"
            return public, 1
        ciphertext = export / "private-evidence.cms"
        if ciphertext.exists():
            public["error_code"] = "EXPORT_EXISTS"
            return public, 1
        temporary.replace(ciphertext)
        public["ciphertext_sha256"] = digest(ciphertext)
        if capture_outcome == "success" and data["outcome"] == "CAPTURE_COMPLETE":
            public.update(outcome="captured_offline", error_code=None)
            return public, 0
        public["error_code"] = "CAPTURE_INCOMPLETE"
        return public, 1
    except BaseException:
        public["error_code"] = "SEAL_FAILED"
        return public, 1
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        receipt.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--capture-outcome", required=True)
    args = parser.parse_args()
    public, code = seal(args.private, args.export, args.capture_outcome)
    try:
        args.export.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not owner_only(args.export):
            raise OSError()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        fd = os.open(args.export / "public.json", flags, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write((json.dumps(public, sort_keys=True) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
    except OSError:
        print('{"case":"S04_WIRE_CAPTURE","outcome":"evidence_failed"}')
        return 1
    print(json.dumps({"case": public["case"], "outcome": public["outcome"],
                      "error_code": public["error_code"]}, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
