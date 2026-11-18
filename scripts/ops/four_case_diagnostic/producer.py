"""Produce the strict four-case CMS evidence envelope for independent intake.

The returned ciphertext hash and size are local observations, not an
independently trusted receipt. A separate channel must pin them for intake.
No plaintext archive is written to disk.
"""
from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
import stat
import subprocess

from . import intake


def _json_line(value):
    try:
        return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise ValueError("evidence_format") from error


def _header(name, size):
    encoded = name.encode("ascii")
    intake.require(0 < len(encoded) <= 100 and 0 < size < 8 ** 11,
                   "archive_bound")
    header = bytearray(512)
    header[:100] = encoded.ljust(100, b"\0")
    header[100:124] = b"0000600\0" + b"0000000\0" * 2
    header[124:136] = f"{size:011o}".encode() + b"\0"
    header[136:148] = b"00000000000\0"
    header[156:157] = b"0"
    header[257:265] = b"ustar\0" + b"00"
    checksum = sum(header[:148]) + 8 * 32 + sum(header[156:])
    header[148:156] = f"{checksum:06o}".encode() + b"\0 "
    return bytes(header)


def _archive(files):
    """Encode only the intake's fixed regular-file inventory as ustar+gzip."""
    expected = intake._paths()
    intake.require(type(files) is dict and set(files) == set(expected),
                   "archive_inventory")
    plain = io.BytesIO()
    for name in sorted(expected):
        data = files[name]
        intake.require(type(data) is bytes and 0 < len(data) <= expected[name],
                       "archive_bound")
        plain.write(_header(name, len(data)))
        plain.write(data)
        plain.write(b"\0" * (-len(data) % 512))
    # Two EOF records, then the minimal zero padding to a 10,240-byte block.
    plain.write(b"\0" * 1024)
    plain.write(b"\0" * (-plain.tell() % 10_240))
    raw = plain.getvalue()
    intake.require(len(raw) <= intake.MAX_ARCHIVE, "archive_bound")
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as stream:
        stream.write(raw)
    compressed = output.getvalue()
    intake.require(0 < len(compressed) <= intake.MAX_ARCHIVE,
                   "archive_bound")
    return compressed


def _private_output_parent(path):
    parent = path.parent
    info = parent.lstat()
    intake.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                   stat.S_IMODE(info.st_mode) == 0o700, "output_directory")


def seal_evidence(files, *, binding, expected_fixtures, recipient_certificate,
                  output_path):
    """Validate, encrypt, and save one immutable CMS artifact.

    ``files`` contains exactly the non-manifest allowlisted byte members.
    The caller supplies a recipient certificate explicitly and must arrange an
    independent trusted receipt before passing the result to the intake.
    """
    expected = set(intake._paths()) - {"manifest.json"}
    intake.require(type(files) is dict and set(files) == expected,
                   "archive_inventory")
    members = {}
    for name in expected:
        data = files[name]
        intake.require(type(data) is bytes and 0 < len(data) <= intake._paths()[name],
                       "archive_bound")
        members[name] = data
    manifest = {"schema": intake.SCHEMA, "binding": binding,
                "files": {name: {"sha256": intake.digest(data), "size": len(data)}
                          for name, data in members.items()},
                "cell_count": 24, "client_count": 3720, "server_count": 23919,
                "cleanup_verified": True, "performance_qualification": False}
    members["manifest.json"] = _json_line(manifest)
    intake.require(len(members["manifest.json"]) <= intake.BASE["manifest.json"],
                   "archive_bound")
    # The audit checks all phase, UUID, timer, resource and source joins. It is
    # deliberately performed before any artifact is saved or exported.
    intake._decode(members, binding, expected_fixtures)
    archive = _archive(members)
    certificate = Path(recipient_certificate)
    intake._read_file(certificate, 65_536, "cms_recipient")
    output = Path(output_path)
    _private_output_parent(output)
    try:
        encrypted = subprocess.run(
            ["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
             "-outform", "DER", str(certificate)],
            input=archive, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            check=True, timeout=120).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("cms_encrypt") from error
    intake.require(0 < len(encrypted) <= intake.MAX_CMS, "cms_bound")
    try:
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600)
    except OSError as error:
        raise ValueError("cms_output") from error
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encrypted)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        output.unlink(missing_ok=True)
        raise ValueError("cms_output") from error
    return {"ciphertext_sha256": intake.digest(encrypted),
            "ciphertext_size": len(encrypted)}
