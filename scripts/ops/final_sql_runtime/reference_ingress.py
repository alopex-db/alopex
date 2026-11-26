"""Offline admission of three original reference ZIPs in Actions ZIP wrappers.

The caller supplies independently reviewed descriptors, a current successful
staging run, current artifact metadata, and already downloaded wrapper paths.
This module never retrieves artifacts or treats metadata as a source pin.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
import stat
import zipfile


SUBJECTS = frozenset(("before", "published", "postgresql"))
MAX_ORIGINAL_BYTES = 1_000_000_000
MAX_WRAPPER_BYTES = 1_100_000_000
MAX_STAGING_AGE = timedelta(days=1)
CHUNK = 1024 * 1024
ORIGINAL_NAME = re.compile(r"[a-z][a-z0-9-]{0,95}\.zip")


def _require(ok, code):
    if not ok:
        raise ValueError(code)


def _hex(value, length):
    return type(value) is str and len(value) == length and all(c in "0123456789abcdef" for c in value)


def _timestamp(value):
    _require(type(value) is str, "ARTIFACT_EXPIRY")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("ARTIFACT_EXPIRY") from exc
    _require(result.tzinfo is not None, "ARTIFACT_EXPIRY")
    return result.astimezone(timezone.utc)


def _regular_open(path):
    _require(isinstance(path, Path) and path.is_absolute(), "WRAPPER_PATH")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and info.st_size <= MAX_WRAPPER_BYTES,
                 "WRAPPER_TYPE_OR_BOUND")
        return fd, info.st_size
    except BaseException:
        os.close(fd)
        raise


def _hash_file(source, bound):
    digest = hashlib.sha256()
    size = 0
    while block := source.read(CHUNK):
        size += len(block)
        _require(size <= bound, "BYTE_BOUND")
        digest.update(block)
    return digest.hexdigest(), size


def verify_original_zip(path, pin):
    """Verify an original ZIP independently of its transfer format.

    This intentionally checks byte identity only; product manifest and receipt
    checks remain the responsibility of the subsequent subject attestor.
    """
    _require(type(pin) is dict and _hex(pin.get("zip_sha256"), 64) and
             type(pin.get("zip_size_bytes")) is int and
             0 < pin["zip_size_bytes"] <= MAX_ORIGINAL_BYTES, "ORIGINAL_PIN")
    fd, _ = _regular_open(path)
    try:
        with os.fdopen(fd, "rb") as source:
            actual = _hash_file(source, pin["zip_size_bytes"])
    except BaseException:
        raise
    _require(actual == (pin["zip_sha256"], pin["zip_size_bytes"]), "ORIGINAL_BYTES")


def _review_descriptor(subject, descriptor, pin, run, metadata, now):
    _require(type(descriptor) is dict and set(descriptor) == {
        "run_id", "head_sha", "repository_id", "workflow_id",
        "artifact_id", "artifact_name", "artifact_digest",
        "artifact_size_bytes", "inner_name"}, "STAGING_DESCRIPTOR")
    _require(type(descriptor["run_id"]) is int and descriptor["run_id"] > 0 and
             _hex(descriptor["head_sha"], 40) and
             type(descriptor["repository_id"]) is int and descriptor["repository_id"] > 0 and
             type(descriptor["workflow_id"]) is int and descriptor["workflow_id"] > 0 and
             type(descriptor["artifact_id"]) is int and descriptor["artifact_id"] > 0 and
             type(descriptor["artifact_name"]) is str and descriptor["artifact_name"] and
             "/" not in descriptor["artifact_name"] and "\\" not in descriptor["artifact_name"] and
             _hex(descriptor["artifact_digest"], 64) and
             type(descriptor["artifact_size_bytes"]) is int and
             0 < descriptor["artifact_size_bytes"] <= MAX_WRAPPER_BYTES and
             type(descriptor["inner_name"]) is str and
             ORIGINAL_NAME.fullmatch(descriptor["inner_name"]) and
             descriptor["inner_name"] == pin.get("basename"), "STAGING_DESCRIPTOR")
    _require(type(run) is dict and run.get("id") == descriptor["run_id"] and
             run.get("head_sha") == descriptor["head_sha"] and
             type(run.get("repository")) is dict and
             type(run["repository"].get("id")) is int and
             run["repository"]["id"] == descriptor["repository_id"] and
             type(run.get("workflow_id")) is int and
             run["workflow_id"] == descriptor["workflow_id"] and
             type(run.get("run_attempt")) is int and run["run_attempt"] == 1 and
             run.get("event") == "workflow_dispatch" and
             run.get("status") == "completed" and run.get("conclusion") == "success",
             "STAGING_RUN")
    _require(type(metadata) is dict and
             metadata.get("id") == descriptor["artifact_id"] and
             metadata.get("name") == descriptor["artifact_name"] and
             metadata.get("digest") == "sha256:" + descriptor["artifact_digest"] and
             metadata.get("size_in_bytes") == descriptor["artifact_size_bytes"] and
             metadata.get("expired") is False and
             type(metadata.get("workflow_run")) is dict and
             metadata["workflow_run"].get("id") == descriptor["run_id"] and
             metadata["workflow_run"].get("head_sha") == descriptor["head_sha"] and
             _timestamp(metadata.get("expires_at")) > now and
             now - MAX_STAGING_AGE <= _timestamp(metadata.get("created_at")) <= now,
             "ARTIFACT_METADATA")
    _require(type(pin) is dict and _hex(pin.get("zip_sha256"), 64) and
             type(pin.get("zip_size_bytes")) is int and
             0 < pin["zip_size_bytes"] <= MAX_ORIGINAL_BYTES and
             type(pin.get("basename")) is str and
             ORIGINAL_NAME.fullmatch(pin["basename"]), "ORIGINAL_PIN")


def _destination_fd(destination):
    _require(isinstance(destination, Path) and destination.is_absolute(), "DESTINATION")
    fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    info = os.fstat(fd)
    try:
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and
                 stat.S_IMODE(info.st_mode) == 0o700, "DESTINATION_PRIVACY")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _extract_one(wrapper, descriptor, pin, directory_fd):
    fd, wrapper_size = _regular_open(wrapper)
    try:
        with os.fdopen(fd, "rb") as source:
            _require(wrapper_size == descriptor["artifact_size_bytes"], "WRAPPER_SIZE")
            digest, size = _hash_file(source, MAX_WRAPPER_BYTES)
            _require((digest, size) == (descriptor["artifact_digest"], wrapper_size),
                     "WRAPPER_DIGEST")
            source.seek(0)
            with zipfile.ZipFile(source) as archive:
                members = archive.infolist()
                _require(len(members) == 1, "WRAPPER_MEMBERS")
                item = members[0]
                mode = item.external_attr >> 16
                _require(item.filename == descriptor["inner_name"] and
                         not item.is_dir() and
                         (mode == 0 or stat.S_IFMT(mode) in (0, stat.S_IFREG)) and
                         item.file_size == pin["zip_size_bytes"] and
                         0 < item.file_size <= MAX_ORIGINAL_BYTES,
                         "WRAPPER_MEMBER")
                name = descriptor["inner_name"]
                output_fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                    getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=directory_fd)
                try:
                    os.fchmod(output_fd, 0o600)
                    _require(os.fstat(output_fd).st_uid == os.getuid(), "OUTPUT_OWNER")
                    digest = hashlib.sha256()
                    written = 0
                    with os.fdopen(output_fd, "wb") as output, archive.open(item) as inner:
                        while block := inner.read(CHUNK):
                            written += len(block)
                            _require(written <= pin["zip_size_bytes"], "ORIGINAL_BOUND")
                            output.write(block)
                            digest.update(block)
                        output.flush()
                        os.fsync(output.fileno())
                    _require((digest.hexdigest(), written) ==
                             (pin["zip_sha256"], pin["zip_size_bytes"]), "ORIGINAL_BYTES")
                    return name
                except BaseException:
                    os.unlink(name, dir_fd=directory_fd)
                    raise
    except (zipfile.BadZipFile, EOFError) as exc:
        raise ValueError("WRAPPER_ZIP") from exc


def admit_wrapped_originals(*, descriptors, original_pins, staging_run,
                            artifact_metadata, wrappers, destination, now=None):
    """Admit exactly three reviewed originals; return subject-to-path mapping.

    All metadata is validated before the first write. On later failure, this
    invocation removes only outputs that it created. Existing targets fail.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    _require(isinstance(now, datetime) and now.tzinfo is not None, "CLOCK")
    now = now.astimezone(timezone.utc)
    _require(all(type(items) is dict and set(items) == SUBJECTS for items in
                 (descriptors, original_pins, artifact_metadata, wrappers)), "REFERENCE_INVENTORY")
    ids, names = set(), set()
    for subject in sorted(SUBJECTS):
        desc = descriptors[subject]
        _review_descriptor(subject, desc, original_pins[subject], staging_run,
                           artifact_metadata[subject], now)
        _require(desc["artifact_id"] not in ids and desc["artifact_name"] not in names,
                 "DUPLICATE_ARTIFACT")
        ids.add(desc["artifact_id"])
        names.add(desc["artifact_name"])
    directory_fd = _destination_fd(destination)
    created = []
    try:
        for subject in sorted(SUBJECTS):
            name = _extract_one(wrappers[subject], descriptors[subject],
                                original_pins[subject], directory_fd)
            created.append(name)
        return {subject: destination / descriptors[subject]["inner_name"]
                for subject in SUBJECTS}
    except BaseException:
        for name in created:
            os.unlink(name, dir_fd=directory_fd)
        raise
    finally:
        os.close(directory_fd)
