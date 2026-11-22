"""Verify bounded GitHub artifact ZIP bytes before perf-baseline reuse."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import stat
import zipfile

from stress_public_evidence import ciphertext_identity, SHA256


MAX_ZIP_BYTES = 105_000_000
MAX_STATUS_BYTES = 4_096
MAX_CMS_BYTES = 100_000_000


def _read_zip_member(path: Path, expected_zip_digest: str, name: str,
                     max_member_bytes: int) -> bytes:
    if (not isinstance(expected_zip_digest, str)
            or not expected_zip_digest.startswith("sha256:")
            or not SHA256.fullmatch(expected_zip_digest.removeprefix("sha256:"))
            or not hasattr(os, "O_NOFOLLOW")):
        raise ValueError("ARTIFACT")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or not 0 < before.st_size <= MAX_ZIP_BYTES):
                raise ValueError("ARTIFACT")
            body = stream.read(MAX_ZIP_BYTES + 1)
            after = os.fstat(stream.fileno())
            if (len(body) != before.st_size or after.st_size != before.st_size
                    or after.st_mtime_ns != before.st_mtime_ns
                    or hashlib.sha256(body).hexdigest()
                    != expected_zip_digest.removeprefix("sha256:")):
                raise ValueError("ARTIFACT")
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            members = archive.infolist()
            if len(members) != 1 or members[0].filename != name:
                raise ValueError("ARTIFACT")
            member = members[0]
            unix_type = stat.S_IFMT(member.external_attr >> 16)
            if (member.is_dir() or unix_type not in (0, stat.S_IFREG)
                    or member.flag_bits & 1
                    or not 0 < member.file_size <= max_member_bytes):
                raise ValueError("ARTIFACT")
            with archive.open(member) as stream:
                payload = stream.read(max_member_bytes + 1)
            if len(payload) != member.file_size:
                raise ValueError("ARTIFACT")
            return payload
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile,
            EOFError, RuntimeError):
        raise ValueError("ARTIFACT") from None


def read_public_status(zip_path: Path, expected_zip_digest: str) -> dict:
    payload = _read_zip_member(zip_path, expected_zip_digest,
                               "perf.json", MAX_STATUS_BYTES)
    try:
        status = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("STATUS") from None
    if not isinstance(status, dict):
        raise ValueError("STATUS")
    return status


def write_private_ciphertext(zip_path: Path, expected_zip_digest: str,
                             expected_cms_digest: str, destination: Path) -> None:
    if not SHA256.fullmatch(expected_cms_digest):
        raise ValueError("ARTIFACT")
    payload = _read_zip_member(zip_path, expected_zip_digest,
                               "perf.cms", MAX_CMS_BYTES)
    if hashlib.sha256(payload).hexdigest() != expected_cms_digest:
        raise ValueError("ARTIFACT")
    if (destination.suffix != ".cms" or destination.is_symlink()
            or destination.exists() or destination.parent.is_symlink()
            or not destination.parent.is_dir()):
        raise ValueError("OUTPUT")
    parent = destination.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise ValueError("OUTPUT")
    created = False
    try:
        descriptor = os.open(destination,
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600)
        created = True
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
        if ciphertext_identity(destination)[0] != expected_cms_digest:
            raise ValueError("ARTIFACT")
    except (OSError, ValueError):
        if created:
            destination.unlink(missing_ok=True)
        raise ValueError("ARTIFACT") from None
