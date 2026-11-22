"""Seal bounded Stage-2 raw evidence to CMS without printing private details.

The caller must run this under the per-host storage watchdog. Upload only the
resulting CMS and an allowlisted public status, never the input directories.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import tempfile

from stress_public_evidence import CASE_IDS


SHA40 = re.compile(r"[0-9a-f]{40}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_FILES = 500
MAX_FILE_BYTES = 20_000_000
MAX_RAW_BYTES = 80_000_000
MAX_ARCHIVE_BYTES = 90_000_000
MAX_CMS_BYTES = 100_000_000


def source_directories(workspace: Path, case_id: str) -> tuple[tuple[str, Path], ...]:
    if case_id not in CASE_IDS:
        raise ValueError("CASE")
    target = workspace / "target"
    if case_id == "stress":
        return (("reports", target / "stress-reports"),)
    if case_id in {"asan", "tsan", "lsan", "msan"}:
        return (("reports", target / "stress-reports" / case_id),
                ("artifacts", target / "stress-artifacts" / case_id))
    if case_id in {"sql_parser", "dataframe_conversion"}:
        return (("reports", target / "stress-reports" / "fuzz" / case_id),
                ("artifacts", target / "stress-artifacts" / "fuzz" / case_id))
    return (("reports", target / "stress-reports" / "perf"),
            ("artifacts", target / "stress-artifacts" / "perf"),
            ("baselines", target / "stress-baselines"))


def read_regular(path: Path) -> bytes:
    if not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("INPUT")
    try:
        before = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (before.st_dev, before.st_ino) != (info.st_dev, info.st_ino)
                    or info.st_size > MAX_FILE_BYTES):
                raise ValueError("INPUT")
            body = stream.read(MAX_FILE_BYTES + 1)
            after = os.fstat(stream.fileno())
            if (len(body) != info.st_size or after.st_size != info.st_size
                    or after.st_mtime_ns != info.st_mtime_ns):
                raise ValueError("INPUT")
            return body
    except OSError:
        raise ValueError("INPUT") from None


def collect_files(workspace: Path, case_id: str) -> list[tuple[str, bytes]]:
    if workspace.is_symlink() or not workspace.is_dir():
        raise ValueError("INPUT")
    files: list[tuple[str, bytes]] = []
    total = 0
    for label, root in source_directories(workspace, case_id):
        ancestor = root
        while ancestor != workspace:
            if ancestor.is_symlink():
                raise ValueError("INPUT")
            ancestor = ancestor.parent
        if not root.exists():
            continue
        if root.is_symlink() or not root.is_dir():
            raise ValueError("INPUT")
        pending = [(root, Path())]
        while pending:
            directory, relative = pending.pop()
            if directory.is_symlink() or not directory.is_dir() or len(relative.parts) > 8:
                raise ValueError("INPUT")
            for child in directory.iterdir():
                info = child.lstat()
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError("INPUT")
                name = relative / child.name
                if stat.S_ISDIR(info.st_mode):
                    pending.append((child, name))
                elif stat.S_ISREG(info.st_mode):
                    body = read_regular(child)
                    files.append((str(Path(label) / name), body))
                    total += len(body)
                    if len(files) > MAX_FILES or total > MAX_RAW_BYTES:
                        raise ValueError("BOUND")
                else:
                    raise ValueError("INPUT")
    if not files:
        raise ValueError("EMPTY")
    return sorted(files, key=lambda item: item[0])


def seal(*, workspace: Path, case_id: str, source_commit: str,
         certificate: Path, expected_cert_sha256: str, output: Path) -> dict:
    if (not hasattr(os, "O_NOFOLLOW") or case_id not in CASE_IDS
            or not SHA40.fullmatch(source_commit)
            or not SHA256.fullmatch(expected_cert_sha256)
            or output.exists() or output.is_symlink()
            or output.parent.is_symlink() or not output.parent.is_dir()):
        raise ValueError("STATE")
    parent_info = output.parent.stat()
    if parent_info.st_uid != os.getuid() or parent_info.st_mode & 0o077:
        raise ValueError("STATE")
    try:
        head = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=15, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("SOURCE") from None
    if head.returncode or head.stdout.strip() != source_commit:
        raise ValueError("SOURCE")
    if certificate.is_symlink() or not certificate.is_file():
        raise ValueError("CERT")
    cert_bytes = read_regular(certificate)
    if hashlib.sha256(cert_bytes).hexdigest() != expected_cert_sha256:
        raise ValueError("CERT")
    valid = subprocess.run(
        ["openssl", "x509", "-checkend", "0", "-noout"],
        input=cert_bytes, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        timeout=15, check=False,
    )
    if valid.returncode:
        raise ValueError("CERT")
    files = collect_files(workspace, case_id)
    manifest = {
        "schema": "alopex.stress-private/v1", "case_id": case_id,
        "source_commit": source_commit,
        "files": [{"name": name, "size": len(body),
                   "sha256": hashlib.sha256(body).hexdigest()}
                  for name, body in files],
    }
    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    temporary = tempfile.NamedTemporaryFile(mode="w+b", suffix=".tar.gz",
                                            prefix="stress-private-", dir=output.parent,
                                            delete=False)
    archive = Path(temporary.name)
    recipient = tempfile.NamedTemporaryFile(mode="w+b", suffix=".pem",
                                            prefix="stress-recipient-", dir=output.parent,
                                            delete=False)
    recipient_path = Path(recipient.name)
    completed = False
    output_created = False
    try:
        with recipient:
            recipient.write(cert_bytes)
        with temporary, tarfile.open(fileobj=temporary, mode="w:gz") as bundle:
            for name, body in [("manifest.json", manifest_bytes), *files]:
                member = tarfile.TarInfo(name)
                member.mode = 0o600
                member.size = len(body)
                bundle.addfile(member, io.BytesIO(body))
        if archive.stat().st_size > MAX_ARCHIVE_BYTES:
            raise ValueError("BOUND")
        encrypted = subprocess.run(
            ["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
             "-outform", "DER", "-in", str(archive), str(recipient_path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=120, check=False,
        )
        if encrypted.returncode or not 0 < len(encrypted.stdout) <= MAX_CMS_BYTES:
            raise ValueError("ENCRYPTION")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        descriptor = os.open(output, flags, 0o600)
        output_created = True
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encrypted.stdout)
        from stress_public_evidence import ciphertext_identity
        digest, size = ciphertext_identity(output)
        completed = True
        return {"ciphertext_sha256": digest, "ciphertext_size": size,
                "file_count": len(files)}
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("SEAL") from None
    finally:
        archive.unlink(missing_ok=True)
        recipient_path.unlink(missing_ok=True)
        # A partial ciphertext is never a publishable artifact.
        if output_created and not completed:
            output.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--certificate", required=True, type=Path)
    parser.add_argument("--expected-cert-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        seal(workspace=args.workspace, case_id=args.case_id,
             source_commit=args.source_commit, certificate=args.certificate,
             expected_cert_sha256=args.expected_cert_sha256, output=args.output)
    except (OSError, ValueError):
        raise SystemExit("STRESS_PRIVATE_EVIDENCE_REJECTED") from None


if __name__ == "__main__":
    main()
