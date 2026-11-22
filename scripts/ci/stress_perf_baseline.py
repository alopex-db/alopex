"""Recover exact-source perf baselines from bounded private CMS evidence.

The caller supplies an authenticated artifact digest, recipient credentials, and
the source commit of a successful scheduled perf run. Keep all inputs private.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import subprocess
import tarfile

from stress_public_evidence import ciphertext_identity, SHA256, SHA40


BASELINES = frozenset({
    "perf_mixed_point_latency.json",
    "perf_sequential_throughput_and_amplification.json",
})
MAX_PLAINTEXT_BYTES = 90_000_000
MAX_ARCHIVE_BYTES = 90_000_000
MAX_BASELINE_BYTES = 1_000_000


def _valid_baseline(body: bytes) -> bool:
    if not 0 < len(body) <= MAX_BASELINE_BYTES:
        return False
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    metrics = value.get("metrics") if isinstance(value, dict) else None
    return (set(value) == {"metrics"} and isinstance(metrics, dict)
            and bool(metrics) and all(
                isinstance(key, str) and key and isinstance(number, (int, float))
                and not isinstance(number, bool) and math.isfinite(number)
                for key, number in metrics.items()))


def recover(*, ciphertext: Path, expected_ciphertext_sha256: str,
            expected_source_commit: str, certificate: Path, private_key: Path,
            destination: Path) -> tuple[str, ...]:
    if (not SHA256.fullmatch(expected_ciphertext_sha256)
            or not SHA40.fullmatch(expected_source_commit)
            or destination.is_symlink() or not destination.is_dir()):
        raise ValueError("STATE")
    info = destination.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("STATE")
    if ciphertext_identity(ciphertext)[0] != expected_ciphertext_sha256:
        raise ValueError("ARTIFACT")
    try:
        decrypted = subprocess.run(
            ["openssl", "cms", "-decrypt", "-binary", "-inform", "DER",
             "-in", str(ciphertext), "-recip", str(certificate),
             "-inkey", str(private_key)],
            capture_output=True, timeout=120, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("DECRYPT") from None
    if decrypted.returncode or not 0 < len(decrypted.stdout) <= MAX_PLAINTEXT_BYTES:
        raise ValueError("DECRYPT")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(decrypted.stdout)) as packed:
            archive_bytes = packed.read(MAX_ARCHIVE_BYTES + 1)
        if len(archive_bytes) > MAX_ARCHIVE_BYTES:
            raise ValueError("ARCHIVE")
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:") as archive:
            members = archive.getmembers()
            if (not 1 <= len(members) <= 501 or len({m.name for m in members}) != len(members)
                    or any(not m.isfile() or not 0 <= m.size <= 20_000_000 for m in members)
                    or members[0].name != "manifest.json"):
                raise ValueError("ARCHIVE")
            manifest_data = archive.extractfile(members[0]).read(1_000_001)
            if len(manifest_data) > 1_000_000:
                raise ValueError("ARCHIVE")
            manifest = json.loads(manifest_data)
            if (manifest.get("schema") != "alopex.stress-private/v1"
                    or manifest.get("case_id") != "perf"
                    or manifest.get("source_commit") != expected_source_commit
                    or not isinstance(manifest.get("files"), list)
                    or len(manifest["files"]) != len(members) - 1):
                raise ValueError("SOURCE")
            names = set()
            baselines: dict[str, bytes] = {}
            for entry, member in zip(manifest["files"], members[1:]):
                name = entry.get("name") if isinstance(entry, dict) else None
                if (name != member.name or not isinstance(name, str)
                        or name.startswith("/") or ".." in Path(name).parts
                        or name in names or entry.get("size") != member.size
                        or not isinstance(entry.get("sha256"), str)
                        or not SHA256.fullmatch(entry["sha256"])):
                    raise ValueError("ARCHIVE")
                names.add(name)
                body = archive.extractfile(member).read(member.size + 1)
                if (len(body) != member.size
                        or hashlib.sha256(body).hexdigest() != entry["sha256"]):
                    raise ValueError("ARCHIVE")
                if name.startswith("baselines/"):
                    filename = name.removeprefix("baselines/")
                    if filename not in BASELINES or not _valid_baseline(body):
                        raise ValueError("BASELINE")
                    baselines[filename] = body
            if set(baselines) != BASELINES:
                raise ValueError("BASELINE")
    except (OSError, EOFError, tarfile.TarError, json.JSONDecodeError,
            AttributeError, TypeError):
        raise ValueError("ARCHIVE") from None
    for filename in BASELINES:
        if (destination / filename).exists() or (destination / filename).is_symlink():
            raise ValueError("OUTPUT")
    created: list[Path] = []
    try:
        for filename in sorted(BASELINES):
            output = destination / filename
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
            descriptor = os.open(output, flags, 0o600)
            created.append(output)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(baselines[filename])
    except OSError:
        for path in created:
            path.unlink(missing_ok=True)
        raise ValueError("OUTPUT") from None
    return tuple(sorted(BASELINES))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ciphertext", required=True, type=Path)
    parser.add_argument("--expected-ciphertext-sha256", required=True)
    parser.add_argument("--expected-source-commit", required=True)
    parser.add_argument("--certificate", required=True, type=Path)
    parser.add_argument("--private-key", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    try:
        recover(ciphertext=args.ciphertext,
                expected_ciphertext_sha256=args.expected_ciphertext_sha256,
                expected_source_commit=args.expected_source_commit,
                certificate=args.certificate, private_key=args.private_key,
                destination=args.destination)
    except (OSError, ValueError):
        raise SystemExit("STRESS_PERF_BASELINE_REJECTED") from None


if __name__ == "__main__":
    main()
