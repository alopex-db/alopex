"""Prepare private local copies of the three reviewed original ZIPs.

This function is offline preparation only. Its caller must separately admit
the storage peak and authorize any copy or external transfer. The original
paths are never renamed, chmodded, or deleted.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shutil
import stat
import tempfile

from . import reference_staging


CHUNK = 1024 * 1024


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def prepare_local_staging(*, originals: dict[str, Path], destination: Path,
                          lock: dict) -> dict:
    """Copy exactly the locked bytes into a new 0700/0400 directory.

    The caller supplies independently reviewed source paths. The destination
    must not exist, and no source inode or mode is changed. A failed copy
    leaves no staged directory; the caller owns deletion after a later handoff.
    """
    pins = reference_staging._pins(lock)
    _require(type(originals) is dict and set(originals) == reference_staging.SUBJECTS and
             isinstance(destination, Path) and destination.is_absolute() and
             not os.path.lexists(destination), "LOCAL_STAGE_INPUT")
    parent = destination.parent
    parent_info = parent.lstat()
    _require(stat.S_ISDIR(parent_info.st_mode) and
             parent_info.st_uid == os.getuid(), "LOCAL_STAGE_PARENT")
    opened = []
    try:
        for subject in sorted(reference_staging.SUBJECTS):
            path = originals[subject]
            _require(isinstance(path, Path) and path.is_absolute(), "LOCAL_STAGE_SOURCE")
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK |
                         getattr(os, "O_NOFOLLOW", 0))
            info = os.fstat(fd)
            opened.append((subject, path, fd, info))
            _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
                     info.st_size == pins[subject]["zip_size_bytes"] and
                     stat.S_IMODE(info.st_mode) & 0o111 == 0,
                     "LOCAL_STAGE_SOURCE")
        _require(len({(info.st_dev, info.st_ino) for _, _, _, info in opened}) == 3,
                 "LOCAL_STAGE_SOURCE")
        temporary = Path(tempfile.mkdtemp(prefix=".reference-stage-", dir=parent))
        try:
            os.chmod(temporary, 0o700)
            for subject, path, source_fd, before in opened:
                pin = pins[subject]
                target = temporary / pin["basename"]
                output_fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
                digest = hashlib.sha256()
                try:
                    remaining = pin["zip_size_bytes"]
                    while remaining:
                        block = os.read(source_fd, min(CHUNK, remaining))
                        _require(bool(block), "LOCAL_STAGE_SOURCE_CHANGED")
                        digest.update(block)
                        view = memoryview(block)
                        while view:
                            written = os.write(output_fd, view)
                            _require(written > 0, "LOCAL_STAGE_IO")
                            view = view[written:]
                        remaining -= len(block)
                    os.fchmod(output_fd, 0o400)
                    os.fsync(output_fd)
                finally:
                    os.close(output_fd)
                after = os.fstat(source_fd)
                entry = path.lstat()
                _require((before.st_dev, before.st_ino, before.st_size,
                          before.st_mtime_ns, before.st_ctime_ns) ==
                         (after.st_dev, after.st_ino, after.st_size,
                          after.st_mtime_ns, after.st_ctime_ns) ==
                         (entry.st_dev, entry.st_ino, entry.st_size,
                          entry.st_mtime_ns, entry.st_ctime_ns) and
                         digest.hexdigest() == pin["zip_sha256"],
                         "LOCAL_STAGE_SOURCE_CHANGED")
            _require(not os.path.lexists(destination), "LOCAL_STAGE_EXISTS")
            os.rename(temporary, destination)
            temporary = None
        finally:
            if temporary is not None:
                shutil.rmtree(temporary)
        return {"status": "staged", "subjects": sorted(reference_staging.SUBJECTS),
                "total_bytes": sum(pin["zip_size_bytes"] for pin in pins.values())}
    finally:
        for _, _, fd, _ in opened:
            os.close(fd)
