"""Reauthenticate the installed public Cargo vendor tree at build time."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time


FILE_COUNT = 35_749
CONTENT_BYTES = 879_770_157
MAX_LOCKED_FILES = 200_000
MAX_LOCKED_BYTES = 2_000_000_000
TREE_SHA256 = "c869f8a6c8e8931dea0a6aa087bc2639be2c9615af452cec5dc2d98c1d291fbc"
CHUNK = 1_048_576
GUARD_BYTES = 64 * CHUNK
GUARD_SECONDS = 15


def require(value):
    if not value:
        raise ValueError("PUBLIC_VENDOR")


def verify_tree(root: Path, guard, expected_mode_sha256, *,
                expected_tree_sha256: str | None = None):
    """Check every installed source byte against the canonical or locked tree."""
    try:
        locked_mode = expected_tree_sha256 is not None
        target_tree_sha256 = expected_tree_sha256 if locked_mode else TREE_SHA256
        max_files = MAX_LOCKED_FILES if locked_mode else FILE_COUNT
        max_bytes = MAX_LOCKED_BYTES if locked_mode else CONTENT_BYTES
        require(isinstance(root, Path) and root.is_absolute() and
                root.is_dir() and not root.is_symlink() and
                root.resolve(strict=True) == root and callable(guard) and
                type(expected_mode_sha256) is str and
                re.fullmatch(r"[0-9a-f]{64}", expected_mode_sha256) and
                type(target_tree_sha256) is str and
                re.fullmatch(r"[0-9a-f]{64}", target_tree_sha256))
        info = root.stat()
        require(info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700)
        guard()
        checked_at = time.monotonic()
        pending = 0
        count = 0
        total = 0
        rows = []
        mode_rows = []
        directories = set()

        def pulse(amount=0):
            nonlocal checked_at, pending
            pending += amount
            now = time.monotonic()
            if pending >= GUARD_BYTES or now - checked_at >= GUARD_SECONDS:
                guard()
                pending = 0
                checked_at = now

        for directory, subdirs, files, directory_fd in os.fwalk(
                root, topdown=True, follow_symlinks=False):
            path = Path(directory)
            require(path == root or root in path.parents)
            directories.add(path.relative_to(root).as_posix())
            require(len(directories) <= max_files + 1)
            info = os.fstat(directory_fd)
            require(stat.S_ISDIR(info.st_mode) and
                    info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o700)
            for name in subdirs:
                entry = os.stat(name, dir_fd=directory_fd,
                                follow_symlinks=False)
                require(stat.S_ISDIR(entry.st_mode) and
                        entry.st_uid == os.geteuid() and
                        stat.S_IMODE(entry.st_mode) == 0o700)
            for name in files:
                entry = os.stat(name, dir_fd=directory_fd,
                                follow_symlinks=False)
                require(stat.S_ISREG(entry.st_mode) and
                        entry.st_uid == os.geteuid() and entry.st_nlink == 1 and
                        stat.S_IMODE(entry.st_mode) in (0o600, 0o700))
                fd = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                             dir_fd=directory_fd)
                with os.fdopen(fd, "rb") as source:
                    opened = os.fstat(source.fileno())
                    require(stat.S_ISREG(opened.st_mode) and
                            (opened.st_dev, opened.st_ino) ==
                            (entry.st_dev, entry.st_ino) and
                            opened.st_uid == os.geteuid() and
                            opened.st_nlink == 1 and
                            stat.S_IMODE(opened.st_mode) in (0o600, 0o700))
                    digest = hashlib.sha256()
                    size = 0
                    while block := source.read(CHUNK):
                        size += len(block)
                        total += len(block)
                        require(total <= max_bytes)
                        digest.update(block)
                        pulse(len(block))
                count += 1
                require(count <= max_files)
                relative = (path / name).relative_to(root).as_posix()
                rows.append((relative, size, digest.hexdigest()))
                mode_rows.append((relative, stat.S_IMODE(opened.st_mode)))
                pulse()
            pulse()
        if locked_mode:
            require(1 <= count <= max_files and 0 < total <= max_bytes)
        else:
            require(count == FILE_COUNT and total == CONTENT_BYTES)
        expected_dirs = {"."}
        for name, _, _ in rows:
            parent = PurePosixPath(name).parent
            while str(parent) != ".":
                expected_dirs.add(str(parent))
                parent = parent.parent
        require(directories == expected_dirs)
        tree = hashlib.sha256()
        for row in sorted(rows):
            tree.update(json.dumps(row, separators=(",", ":")).encode() + b"\n")
        require(tree.hexdigest() == target_tree_sha256)
        mode_tree = hashlib.sha256()
        for row in sorted(mode_rows):
            mode_tree.update(json.dumps(row, separators=(",", ":")).encode() + b"\n")
        require(mode_tree.hexdigest() == expected_mode_sha256)
        guard()
        return target_tree_sha256
    except BaseException:
        raise ValueError("PUBLIC_VENDOR") from None
