"""Public-only admission check for NEW CMS encryption, never archive decryption."""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

REPO = Path(__file__).resolve().parents[2]


def reviewed_pin(source):
    tree = ast.parse(Path(source).read_text())
    values = [ast.literal_eval(node.value) for node in tree.body
              if isinstance(node, ast.Assign) and any(
                  isinstance(target, ast.Name) and target.id == "CERT_SHA256"
                  for target in node.targets)]
    if len(values) != 1 or not re.fullmatch(r"[0-9a-f]{64}", values[0]):
        raise ValueError("CERT_PIN_INVALID")
    return values[0]


def check(certificate, expected_sha256, *, minimum_seconds=0, now=None):
    certificate = Path(certificate)
    if (certificate.is_symlink() or not certificate.is_file() or
            certificate.stat().st_size > 16384):
        raise ValueError("CERT_INVALID")
    data = certificate.read_bytes()
    if (data.count(b"-----BEGIN CERTIFICATE-----") != 1 or
            data.count(b"-----END CERTIFICATE-----") != 1 or
            b"PRIVATE KEY" in data):
        raise ValueError("CERT_INVALID")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("CERT_PIN_INVALID")
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ValueError("CERT_MISMATCH")
    if type(minimum_seconds) is not int or minimum_seconds < 0:
        raise ValueError("CERT_WINDOW_INVALID")
    result = subprocess.run(["openssl", "x509", "-noout", "-startdate", "-enddate"],
                            input=data, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            check=False, timeout=10)
    if result.returncode:
        raise ValueError("CERT_INVALID")
    try:
        dates = dict(line.split("=", 1) for line in result.stdout.decode("ascii").splitlines())
        parse = lambda value: datetime.strptime(value, "%b %d %H:%M:%S %Y GMT").replace(tzinfo=timezone.utc)
        start, end = parse(dates["notBefore"]), parse(dates["notAfter"])
    except (KeyError, ValueError, UnicodeError):
        raise ValueError("CERT_INVALID") from None
    current = datetime.now(timezone.utc) if now is None else now
    if current.tzinfo is None or start >= end:
        raise ValueError("CERT_WINDOW_INVALID")
    if current < start:
        raise ValueError("CERT_NOT_YET_VALID")
    if current >= end:
        raise ValueError("CERT_EXPIRED")
    if current + timedelta(seconds=minimum_seconds) >= end:
        raise ValueError("CERT_EXPIRING")
    return {"pem_sha256": expected_sha256, "not_before": start.isoformat(),
            "not_after": end.isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--certificate", type=Path, default=REPO / ".github/ops/native-evidence-cert.pem")
    parser.add_argument("--pin-source", type=Path, default=REPO / "scripts/ops/s04_inspect_seal.py")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--minimum-seconds", type=int, default=86400)
    args = parser.parse_args()
    try:
        result = check(args.certificate, args.expected_sha256 or reviewed_pin(args.pin_source),
                       minimum_seconds=args.minimum_seconds)
    except Exception as error:
        allowed = {"CERT_INVALID", "CERT_PIN_INVALID", "CERT_MISMATCH", "CERT_WINDOW_INVALID",
                   "CERT_NOT_YET_VALID", "CERT_EXPIRED", "CERT_EXPIRING"}
        code = str(error) if isinstance(error, ValueError) and str(error) in allowed else "CERT_CHECK_FAILED"
        print(json.dumps({"outcome": "failed", "error_code": code}))
        return 1
    print(json.dumps({"outcome": "passed", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
