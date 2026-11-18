"""Offline verification of a four-case native-asset acquisition receipt.

The acquisition producer must first verify the GitHub run event, attempt and
conclusion, and artifact API identity, digest and size. Those remote facts cannot
be established by this local validator. The producer must write this v2 receipt
only after hashing the downloaded original ZIP and the extracted assets manifest.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat


SCHEMA = "alopex.sql-four-case-acquisition/v2"
ZIP_LIMIT = 60_000_000
MANIFEST_LIMIT = 2_000_000
RECEIPT_LIMIT = 16_384
FIELDS = frozenset(("schema", "qualification_job_id", "run", "head",
                    "artifact_id", "zip_sha256", "zip_size_bytes",
                    "assets_digest"))
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def _require(value):
    if not value:
        raise ValueError("ACQUISITION_BINDING")


def _digest_file(path, limit, *, read=False):
    """Hash one private regular file through a no-follow descriptor."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(Path(path), flags)
    except OSError as error:
        raise ValueError("ACQUISITION_BINDING") from error
    try:
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                 stat.S_IMODE(info.st_mode) == 0o600 and
                 0 < info.st_size < limit)
        digest = hashlib.sha256()
        chunks = []
        count = 0
        while chunk := os.read(fd, min(1_048_576, limit - count)):
            count += len(chunk)
            _require(count < limit)
            digest.update(chunk)
            if read:
                chunks.append(chunk)
        after = os.fstat(fd)
        _require(count == info.st_size and
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) ==
                 (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))
        return digest.hexdigest(), count, b"".join(chunks) if read else None
    finally:
        os.close(fd)


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def verify(lock, *, receipt_path, archive_path, manifest_path, job_id):
    """Return the exact proof only if private bytes match trusted lock and job.

    ``lock`` is supplied by the caller from an independently reviewed source.
    The receipt itself is an assertion until its files and remote producer are
    authenticated by the live qualification path.
    """
    _require(type(lock) is dict and type(lock.get("pin")) is dict and
             lock.get("schema") == "alopex.sql-four-case-pins/v1" and
             type(job_id) is str and job_id.isdecimal())
    pin = lock["pin"]
    for name in ("asset_run_head",):
        _require(type(pin.get(name)) is str and HEX40.fullmatch(pin[name]))
    for name in ("zip_sha256", "artifact_sha256", "assets_digest"):
        _require(type(pin.get(name)) is str and HEX64.fullmatch(pin[name]))
    _require(type(pin.get("asset_run_id")) is int and pin["asset_run_id"] > 0 and
             type(pin.get("artifact_id")) is int and pin["artifact_id"] > 0 and
             type(pin.get("asset_zip_size_bytes")) is int and
             0 < pin["asset_zip_size_bytes"] < ZIP_LIMIT and
             pin["artifact_sha256"] == pin["zip_sha256"])
    expected = {"schema": SCHEMA, "qualification_job_id": job_id,
                "run": pin["asset_run_id"], "head": pin["asset_run_head"],
                "artifact_id": pin["artifact_id"],
                "zip_sha256": pin["zip_sha256"],
                "zip_size_bytes": pin["asset_zip_size_bytes"],
                "assets_digest": pin["assets_digest"]}
    _, _, data = _digest_file(receipt_path, RECEIPT_LIMIT, read=True)
    try:
        receipt = json.loads(data, object_pairs_hook=_unique_pairs)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("ACQUISITION_BINDING") from error
    _require(type(receipt) is dict and set(receipt) == FIELDS and receipt == expected)
    zip_digest, zip_size, _ = _digest_file(archive_path, ZIP_LIMIT)
    _require(zip_size == pin["asset_zip_size_bytes"] and zip_digest == pin["zip_sha256"])
    manifest_digest, _, _ = _digest_file(manifest_path, MANIFEST_LIMIT)
    _require(manifest_digest == pin["assets_digest"])
    return expected
