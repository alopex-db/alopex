"""Fetch the exact Nim release archive under the hosted parser-input contract."""

from __future__ import annotations

import hashlib
import http.client
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit


NAME = "nim-2.2.10-linux_x64.tar.xz"
RELEASE_TAG = "2026-04-24-version-2-2-bfeb3146d1638b39f69007a4ae5a23e23ae4e5ef"
DOWNLOAD_PATH = f"/nim-lang/nightlies/releases/download/{RELEASE_TAG}/{NAME}"
EXPECTED_BYTES = 17_574_524
EXPECTED_SHA256 = "0a3a38752e97e9d44aa479b3a7b37336dfe0176daf22ee5b5218ad0991ecd211"
FIRST_HOST = "github.com"
ASSET_HOST = "release-assets.githubusercontent.com"
TIME_LIMIT = 180
CHUNK = 1_048_576


def require(value):
    if not value:
        raise ValueError("PUBLIC_PARSER_INPUT")


def _owned_directory(root):
    require(isinstance(root, Path) and root.is_absolute() and
            root.is_dir() and not root.is_symlink())
    info = root.stat()
    require(info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)


def _redirect_target(location):
    require(type(location) is str and 1 <= len(location) <= 8192 and
            not any(ch in location for ch in "\r\n\\"))
    parsed = urlsplit(location)
    require(parsed.scheme == "https" and parsed.hostname == ASSET_HOST and
            parsed.username is None and parsed.password is None and
            parsed.port in (None, 443) and not parsed.fragment and
            parsed.path.startswith("/github-production-release-asset/") and
            parsed.query)
    return parsed.path + "?" + parsed.query


def _request(host, target):
    connection = http.client.HTTPSConnection(host, timeout=30)
    try:
        connection.request("GET", target, headers={
            "Accept": "application/octet-stream",
            "User-Agent": "alopex-release-input/1",
        })
        return connection, connection.getresponse()
    except BaseException:
        connection.close()
        raise


def _private_reason(root, stage, error):
    """Keep a bounded diagnostic in the owner-only input area."""
    try:
        _owned_directory(root)
        category = type(error).__name__
        if category not in {"ValueError", "OSError", "TimeoutError",
                            "HTTPException", "RuntimeError"}:
            category = "Other"
        path = root / "download.private.log"
        flags = os.O_CREAT | os.O_WRONLY | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        with os.fdopen(fd, "ab") as stream:
            info = os.fstat(stream.fileno())
            require(info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o600 and
                    stat.S_ISREG(info.st_mode))
            stream.write((stage + ":" + category + "\n").encode())
    except BaseException:
        pass


def fetch_archive(root, guard):
    """Save verified archive in one owner-only root; expose fixed failures only."""
    output = partial = None
    first = second = None
    created_partial = False
    stage = "admission"
    try:
        _owned_directory(root)
        output = root / NAME
        partial = root / (NAME + ".partial")
        require(callable(guard) and not output.exists() and
                not output.is_symlink() and not partial.exists() and
                not partial.is_symlink() and
                re.fullmatch(r"[0-9a-f]{64}", EXPECTED_SHA256) and
                0 < EXPECTED_BYTES <= 50_000_000)
        guard()
        deadline = time.monotonic() + TIME_LIMIT
        stage = "redirect"
        first, response = _request(FIRST_HOST, DOWNLOAD_PATH)
        require(response.status in (302, 307, 308))
        target = _redirect_target(response.getheader("Location"))
        response.close()
        first.close()
        first = None
        require(time.monotonic() < deadline)
        stage = "transfer"
        second, response = _request(ASSET_HOST, target)
        require(response.status == 200)
        content_length = response.getheader("Content-Length")
        if content_length is not None:
            require(content_length.isdecimal() and
                    int(content_length) == EXPECTED_BYTES)
        fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        created_partial = True
        total = 0
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as stream:
            while block := response.read(CHUNK):
                require(time.monotonic() < deadline)
                total += len(block)
                require(total <= EXPECTED_BYTES)
                digest.update(block)
                stream.write(block)
                guard()
        stage = "digest"
        require(total == EXPECTED_BYTES and
                digest.hexdigest() == EXPECTED_SHA256)
        guard()
        response.close()
        second.close()
        second = None
        stage = "promotion"
        os.replace(partial, output)
        return output
    except BaseException as error:
        _private_reason(root, stage, error)
        try:
            if created_partial:
                partial.unlink(missing_ok=True)
        except Exception:
            pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
    finally:
        if first is not None:
            try:
                first.close()
            except Exception:
                pass
        if second is not None:
            try:
                second.close()
            except Exception:
                pass
