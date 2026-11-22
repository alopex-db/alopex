"""Fetch the exact Nim release archive under the hosted parser-input contract."""

from __future__ import annotations

import hashlib
import http.client
import ctypes
import io
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
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
MAX_MEMBERS = 20_000
MAX_MEMBER_BYTES = 100_000_000
MAX_EXPANDED_BYTES = 500_000_000
EXTRACTED_NAME = "nim-extracted"
ARCHIVE_ROOTS = {"nim-2.2.10", "nim-2.2.10-linux_x64"}


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


def _tar_name(member):
    name = member.name
    require(type(name) is str and name and "\\" not in name and
            not name.startswith("/"))
    canonical = name[:-1] if member.isdir() and name.endswith("/") else name
    parts = PurePosixPath(canonical).parts
    require(parts and parts[0] in ARCHIVE_ROOTS and
            all(part not in ("", ".", "..") for part in parts) and
            PurePosixPath(*parts).as_posix() == canonical)
    return canonical, parts


def _remove_owned_tree(path, identity):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    require(stat.S_ISDIR(info.st_mode) and
            (info.st_dev, info.st_ino) == identity)
    shutil.rmtree(path)


def _promote_noreplace(source, target):
    """Atomically publish a directory only when the target name is absent."""
    libc = ctypes.CDLL(None, use_errno=True)
    old = os.fsencode(source)
    new = os.fsencode(target)
    if os.uname().sysname == "Linux":
        rename = libc.renameat2
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p,
                           ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        status = rename(-100, old, -100, new, 1)
    elif os.uname().sysname == "Darwin":
        rename = libc.renamex_np
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        status = rename(old, new, 0x00000004)
    else:
        raise ValueError("PUBLIC_PARSER_INPUT")
    if status != 0:
        raise OSError(ctypes.get_errno(), "promotion")


def extract_archive(root, guard):
    """Reauthenticate and safely unpack one complete compiler input tree."""
    partial = None
    owned_partial = None
    stage = "extract-admission"
    try:
        _owned_directory(root)
        require(callable(guard))
        archive_path = root / NAME
        output = root / EXTRACTED_NAME
        partial = root / (EXTRACTED_NAME + ".partial")
        require(archive_path.is_file() and not archive_path.is_symlink() and
                archive_path.stat().st_uid == os.geteuid() and
                stat.S_IMODE(archive_path.stat().st_mode) == 0o600 and
                archive_path.stat().st_size == EXPECTED_BYTES and
                not output.exists() and not output.is_symlink() and
                not partial.exists() and not partial.is_symlink())
        guard()
        digest = hashlib.sha256()
        snapshot = bytearray()
        with archive_path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            require(opened.st_uid == os.geteuid() and
                    stat.S_ISREG(opened.st_mode) and
                    stat.S_IMODE(opened.st_mode) == 0o600 and
                    opened.st_size == EXPECTED_BYTES)
            while block := stream.read(CHUNK):
                require(len(snapshot) + len(block) <= EXPECTED_BYTES)
                snapshot.extend(block)
                digest.update(block)
                guard()
            require(len(snapshot) == EXPECTED_BYTES)
            require(digest.hexdigest() == EXPECTED_SHA256)
            stage = "extract-scan"
            with tarfile.open(fileobj=io.BytesIO(bytes(snapshot)), mode="r:xz") as archive:
                members = archive.getmembers()
                require(1 <= len(members) <= MAX_MEMBERS)
                names = set()
                files = set()
                roots = set()
                total = 0
                for member in members:
                    name, parts = _tar_name(member)
                    require(name not in names and
                            (member.isfile() or member.isdir()) and
                            not (member.mode & ~0o777))
                    names.add(name)
                    roots.add(parts[0])
                    if member.isfile():
                        files.add(name)
                        require(len(parts) >= 2 and
                                0 <= member.size <= MAX_MEMBER_BYTES)
                        total += member.size
                        require(total <= MAX_EXPANDED_BYTES)
                require(len(roots) == 1 and
                        all(next(iter(roots)) + "/bin/" + tool in files
                            for tool in ("nim", "nimble")))
                stage = "extract-write"
                partial.mkdir(mode=0o700)
                info = partial.lstat()
                owned_partial = (info.st_dev, info.st_ino)
                copied = 0
                for member in members:
                    guard()
                    name, parts = _tar_name(member)
                    destination = partial.joinpath(*parts)
                    if member.isdir():
                        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
                        require(destination.is_dir() and not destination.is_symlink())
                        continue
                    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    mode = 0o700 if member.mode & 0o111 else 0o600
                    fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                                 getattr(os, "O_NOFOLLOW", 0), mode)
                    with os.fdopen(fd, "wb") as output_file:
                        source_file = archive.extractfile(member)
                        require(source_file is not None)
                        remaining = member.size
                        while remaining:
                            block = source_file.read(min(CHUNK, remaining))
                            require(block)
                            remaining -= len(block)
                            copied += len(block)
                            output_file.write(block)
                            guard()
                    require(remaining == 0)
                require(copied == total)
        stage = "extract-promotion"
        guard()
        _promote_noreplace(partial, output)
        return output
    except BaseException as error:
        _private_reason(root, stage, error)
        if owned_partial is not None:
            try:
                _remove_owned_tree(partial, owned_partial)
            except Exception:
                pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
