"""Fetch one authenticated public Cargo input for the selected hosted job."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import threading
import time

from issue681_hosted_nim_archive import (
    _owned_directory, _promote_noreplace, _redirect_target, _request,
)


NAME = "public-cargo-vendor.zip"
TAG = "issue681-v0817-cargo-input-dd6b618a"
DOWNLOAD_PATH = f"/alopex-db/alopex/releases/download/{TAG}/{NAME}"
EXPECTED_BYTES = 301_598_193
EXPECTED_SHA256 = "dd6b618ad9d45a5703f32bafb30fb936e119c827fed2ac6d649cda22b5489a3c"
FIRST_HOST = "github.com"
ASSET_HOST = "release-assets.githubusercontent.com"
TIME_LIMIT = 600
CHUNK = 1_048_576
ERROR = "PUBLIC_VENDOR"


def require(value):
    if not value:
        raise ValueError(ERROR)


def _private_reason(root, stage, error):
    """Write only one fixed, private category; never expose a signed URL."""
    try:
        _owned_directory(root)
        category = type(error).__name__
        if category not in {"ValueError", "OSError", "TimeoutError",
                            "HTTPException", "RuntimeError"}:
            category = "Other"
        fd = os.open(root / f"vendor-fetch.{secrets.token_hex(8)}.private.log",
                     os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600)
            stream.write(f"{stage}:{category}\n".encode())
    except BaseException:
        pass


def _remove_same_inode(path, identity):
    if identity is None:
        return
    try:
        info = path.lstat()
        if (info.st_dev, info.st_ino) == identity and stat.S_ISREG(info.st_mode):
            path.unlink()
    except FileNotFoundError:
        pass


def _deadline_expired(_signum, _frame):
    raise TimeoutError("vendor fetch deadline")


def fetch_vendor(root: Path, guard):
    """Stream exact public bytes to a new owner-only file; fail closed."""
    first = second = response = None
    partial = output = None
    partial_identity = None
    old_alarm_handler = None
    stage = "admission"
    try:
        _owned_directory(root)
        require(callable(guard) and
                re.fullmatch(r"[0-9a-f]{64}", EXPECTED_SHA256) and
                0 < EXPECTED_BYTES < 2_000_000_000)
        output = root / NAME
        partial = root / (NAME + ".partial")
        require(not output.exists() and not output.is_symlink() and
                not partial.exists() and not partial.is_symlink())
        require(threading.current_thread() is threading.main_thread() and
                signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0))
        old_alarm_handler = signal.signal(signal.SIGALRM, _deadline_expired)
        signal.setitimer(signal.ITIMER_REAL, TIME_LIMIT)
        guard()
        deadline = time.monotonic() + TIME_LIMIT

        stage = "redirect"
        first, response = _request(FIRST_HOST, DOWNLOAD_PATH)
        require(response.status in (302, 307, 308))
        target = _redirect_target(response.getheader("Location"))
        response.close()
        response = None
        first.close()
        first = None
        require(time.monotonic() < deadline)

        stage = "transfer"
        second, response = _request(ASSET_HOST, target)
        require(response.status == 200)
        content_length = response.getheader("Content-Length")
        if content_length is not None:
            require(type(content_length) is str and content_length.isdecimal()
                    and int(content_length) == EXPECTED_BYTES)
        fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        total = 0
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as stream:
            info = os.fstat(stream.fileno())
            partial_identity = (info.st_dev, info.st_ino)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600)
            while block := response.read(CHUNK):
                require(time.monotonic() < deadline and
                        type(block) is bytes and len(block) <= CHUNK)
                total += len(block)
                require(total <= EXPECTED_BYTES)
                digest.update(block)
                stream.write(block)
                guard()
            stream.flush()
            os.fsync(stream.fileno())
        stage = "digest"
        require(time.monotonic() < deadline and total == EXPECTED_BYTES and
                digest.hexdigest() == EXPECTED_SHA256)
        guard()
        response.close()
        response = None
        second.close()
        second = None
        stage = "promotion"
        _promote_noreplace(partial, output)
        return output
    except BaseException as error:
        if old_alarm_handler is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
        if partial is not None:
            try:
                _remove_same_inode(partial, partial_identity)
            except BaseException as cleanup_error:
                stage, error = "cleanup", cleanup_error
        _private_reason(root, stage, error)
        raise ValueError(ERROR) from None
    finally:
        if old_alarm_handler is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_alarm_handler)
        for handle in (response, first, second):
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
