"""Stage only reviewed #712 docs harness files from acquired native assets.

The caller first verifies the acquisition and #804 qualification. This module
rechecks exact local bytes and never executes code from the archive.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import tarfile
import tempfile

from . import qualification

MAX_MANIFEST = 2_000_000
MAX_ARCHIVE = 32_000_000
MAX_EXPANDED = 8_000_000
MAX_MEMBERS = 255
ROLES = {"server", "wheel", "library", "receipt", "harness"}


def require(value, code="MISSING_PIN"):
    if not value:
        raise ValueError(code)


def _owned_directory(path):
    path = Path(path)
    try:
        info = path.lstat()
    except OSError as error:
        raise ValueError("MISSING_PIN") from error
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)
    return path


def _private_bytes(path, limit):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise ValueError("MISSING_PIN") from error
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and
                stat.S_IMODE(before.st_mode) == 0o600 and
                0 < before.st_size <= limit)
        raw = os.read(fd, limit + 1)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns))
        return raw
    finally:
        os.close(fd)


def _name(value, *, directory=False):
    require(type(value) is str and value and value.isascii() and
            "\\" not in value and "\x00" not in value and
            not value.startswith("/") and not value.startswith("./") and
            (not value.endswith("/") or directory), "HARNESS_ARCHIVE")
    clean = value.rstrip("/") if directory else value
    parts = clean.split("/")
    require(all(part not in ("", ".", "..") for part in parts) and
            not any(":" in part for part in parts), "HARNESS_ARCHIVE")
    return clean


def _archive_files(raw, docs_files):
    try:
        archive = tarfile.open(fileobj=io.BytesIO(raw), mode="r:*")
    except (tarfile.TarError, EOFError, OSError) as error:
        raise ValueError("HARNESS_ARCHIVE") from error
    selected = {}
    try:
        members = archive.getmembers()
        require(0 < len(members) <= MAX_MEMBERS, "HARNESS_ARCHIVE")
        seen, total = set(), 0
        for member in members:
            require(member.isfile() or member.isdir(), "HARNESS_ARCHIVE")
            name = _name(member.name, directory=member.isdir())
            allowed = name.startswith("scripts/performance/")
            if member.isdir():
                allowed = (allowed or name in ("scripts", "scripts/performance")) and \
                    not name.endswith(".py")
            require(name not in seen and allowed, "HARNESS_ARCHIVE")
            seen.add(name)
            require(type(member.size) is int and member.size >= 0 and
                    (member.size == 0 if member.isdir() else True),
                    "HARNESS_ARCHIVE")
            total += member.size
            require(total <= MAX_EXPANDED and member.size <= MAX_EXPANDED,
                    "HARNESS_ARCHIVE")
            if not member.isfile():
                continue
            basename = name.removeprefix("scripts/performance/")
            if basename not in docs_files:
                continue
            stream = archive.extractfile(member)
            require(stream is not None, "HARNESS_ARCHIVE")
            data = stream.read(member.size + 1)
            require(len(data) == member.size and
                    hashlib.sha256(data).hexdigest() == docs_files[basename],
                    "HARNESS_IDENTITY")
            selected[basename] = data
        require(set(selected) == set(docs_files), "HARNESS_IDENTITY")
        return selected
    except (tarfile.TarError, EOFError, OSError) as error:
        raise ValueError("HARNESS_ARCHIVE") from error
    finally:
        archive.close()


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "ASSET_IDENTITY")
        result[key] = value
    return result


def stage_docs(assets_root, runner_temp, *, pin, docs_files):
    """Publish fixed private docs tree once; reject any pre-existing stage."""
    require(type(pin) is dict and type(docs_files) is dict and
            0 < len(docs_files) <= 32 and
            {"subject_identity.py", "sql_resources.py"} <= set(docs_files) and
            all(type(name) is str and name.isascii() and name.endswith(".py") and
                Path(name).name == name and
                type(value) is str and qualification.HEX64.fullmatch(value)
                for name, value in docs_files.items()))
    digests = pin.get("asset_file_digests")
    require(type(digests) is dict and set(digests) == ROLES and
            all(type(value) is str and qualification.HEX64.fullmatch(value)
                for value in digests.values()) and
            type(pin.get("assets_digest")) is str and
            qualification.HEX64.fullmatch(pin["assets_digest"]) and
            type(pin.get("source_commit")) is str and
            qualification.HEX40.fullmatch(pin["source_commit"]) and
            type(pin.get("harness_commit")) is str and
            qualification.HEX40.fullmatch(pin["harness_commit"]))
    assets_root, runner_temp = Path(assets_root), _owned_directory(runner_temp)
    _owned_directory(assets_root)
    assets_dir = _owned_directory(assets_root / "assets")
    manifest_raw = _private_bytes(assets_dir / "assets.json", MAX_MANIFEST)
    require(hashlib.sha256(manifest_raw).hexdigest() == pin["assets_digest"],
            "ASSET_IDENTITY")
    try:
        manifest = json.loads(manifest_raw, object_pairs_hook=_unique_pairs)
    except (ValueError, UnicodeError) as error:
        raise ValueError("ASSET_IDENTITY") from error
    rows = manifest.get("files") if type(manifest) is dict else None
    require(type(manifest) is dict and
            manifest.get("schema") == "alopex.test-env-assets/v1" and
            manifest.get("source_commit") == pin["source_commit"] and
            manifest.get("harness_commit") == pin["harness_commit"] and
            type(rows) is list and len(rows) == len(ROLES), "ASSET_IDENTITY")
    by_role = {}
    names = set()
    for row in rows:
        require(type(row) is dict and
                {"role", "path", "size", "sha256"} <= set(row) and
                row["role"] in ROLES and row["role"] not in by_role and
                type(row["path"]) is str and row["path"] not in names and
                type(row["size"]) is int and 0 < row["size"] <= 268_435_456 and
                row["sha256"] == digests[row["role"]], "ASSET_IDENTITY")
        name = _name(row["path"])
        require(len(name.split("/")) <= 4, "ASSET_IDENTITY")
        names.add(name)
        by_role[row["role"]] = row
    require(set(by_role) == ROLES, "ASSET_IDENTITY")
    harness = by_role["harness"]
    archive_path = assets_dir.joinpath(*harness["path"].split("/"))
    parent = assets_dir
    for part in harness["path"].split("/")[:-1]:
        parent = _owned_directory(parent / part)
    raw = _private_bytes(archive_path, MAX_ARCHIVE)
    require(len(raw) == harness["size"] and
            hashlib.sha256(raw).hexdigest() == digests["harness"],
            "HARNESS_IDENTITY")
    selected = _archive_files(raw, docs_files)
    final = runner_temp / "alopex-712-docs"
    require(not final.exists() and not final.is_symlink(), "OUTPUT_STATE")
    lock = runner_temp / ".alopex-712-docs.lock"
    try:
        lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                          getattr(os, "O_NOFOLLOW", 0), 0o600)
    except OSError as error:
        raise ValueError("OUTPUT_STATE") from error
    pending = None
    try:
        pending = Path(tempfile.mkdtemp(prefix=".alopex-712-docs-", dir=runner_temp))
        target = pending / "scripts/performance"
        target.mkdir(mode=0o700, parents=True)
        (pending / "scripts").chmod(0o700)
        target.chmod(0o700)
        for name, data in selected.items():
            fd = os.open(target / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                         getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
        require(not final.exists() and not final.is_symlink(), "OUTPUT_STATE")
        pending.rename(final)
        pending = None
        return final
    finally:
        os.close(lock_fd)
        if pending is not None:
            shutil.rmtree(pending)
        lock.unlink()
