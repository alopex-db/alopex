"""Stage an offline, lockfile-verified Cargo vendor tree via watched cargo vendor --locked.

UC-681-WATCHED-CARGO-FETCH-LOCKED. Eliminates any dependency on an unpublished
GitHub release bundle while preserving Cargo.lock SHA-256 verification, offline
CARGO_NET_OFFLINE=true builds, and 40 GB / 50 GB HostStorageGuard enforcement.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import tempfile
import time

import issue681_hosted_acquire_source as acquire
import issue681_hosted_build_guard as capacity
import issue681_hosted_nim_archive as archive_tools
import issue681_hosted_rust_preflight as rust_preflight
import issue681_hosted_vendor_consume as vendor_consume

ERROR = "PUBLIC_VENDOR"
TIME_LIMIT = 600
MAX_FILES = 200_000
MAX_FILE_BYTES = 256 * 1024 * 1024
GUARD_INTERVAL_SECONDS = 1.0
GUARD_ENTRY_STRIDE = 256
DIAG_CODES = frozenset({
    "FETCH_PRECHECK",
    "FETCH_LOCKS",
    "FETCH_CARGO_BIN",
    "FETCH_VENDOR_TIMEOUT",
    "FETCH_VENDOR_CAPACITY",
    "FETCH_VENDOR_EXIT",
    "FETCH_NORMALIZE_TIMEOUT",
    "FETCH_NORMALIZE_CAPACITY",
    "FETCH_NORMALIZE_TREE",
    "FETCH_PROMOTE",
    "FETCH_CONFIG",
})


def _set_diag(diag, code):
    if isinstance(diag, dict) and code in DIAG_CODES:
        diag["code"] = code


def require(value):
    if not value:
        raise ValueError(ERROR)


def _stop_process(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=20)
    else:
        process.wait()


def _normalize_and_digest(vendor_root: Path, guard, *, deadline=None,
                          guard_interval: float = GUARD_INTERVAL_SECONDS,
                          guard_stride: int = GUARD_ENTRY_STRIDE,
                          diag=None):
    """Reject symlinks/special files, normalize modes, and hash tree/modes."""
    rows = {}
    mode_rows = {}
    files = 0
    visited = 0
    last_guard = time.monotonic()

    def check_progress(*, force=False):
        nonlocal last_guard
        now = time.monotonic()
        if deadline is not None and now >= deadline:
            _set_diag(diag, "FETCH_NORMALIZE_TIMEOUT")
            require(False)
        if force or visited % guard_stride == 0 or (now - last_guard) >= guard_interval:
            try:
                guard()
            except BaseException:
                _set_diag(diag, "FETCH_NORMALIZE_CAPACITY")
                raise
            last_guard = time.monotonic()

    check_progress(force=True)
    pending = [vendor_root]
    while pending:
        current = pending.pop()
        info = current.lstat()
        require(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and
                info.st_uid == os.geteuid())
        os.chmod(current, 0o700)
        visited += 1
        check_progress()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                einfo = path.lstat()
                require(not stat.S_ISLNK(einfo.st_mode) and
                        einfo.st_uid == os.geteuid())
                if stat.S_ISDIR(einfo.st_mode):
                    pending.append(path)
                elif stat.S_ISREG(einfo.st_mode):
                    files += 1
                    visited += 1
                    require(files <= MAX_FILES and 0 <= einfo.st_size <= MAX_FILE_BYTES)
                    mode = 0o700 if (einfo.st_mode & 0o111) else 0o600
                    os.chmod(path, mode)
                    digest = hashlib.sha256()
                    with path.open("rb") as stream:
                        for chunk in iter(lambda: stream.read(1_048_576), b""):
                            digest.update(chunk)
                            check_progress()
                    rel = path.relative_to(vendor_root).as_posix()
                    rows[rel] = (einfo.st_size, digest.hexdigest())
                    mode_rows[rel] = mode
                else:
                    require(False)
    require(files > 0)
    check_progress(force=True)
    tree = hashlib.sha256()
    for name, (size, sha256) in sorted(rows.items()):
        tree.update(json.dumps([name, size, sha256],
                               separators=(",", ":")).encode() + b"\n")
    mode_tree = hashlib.sha256()
    for name, mode in sorted(mode_rows.items()):
        mode_tree.update(json.dumps([name, mode],
                                    separators=(",", ":")).encode() + b"\n")
    return tree.hexdigest(), mode_tree.hexdigest()


def fetch_and_stage_vendor(sources: dict[str, Path], cargo_home: Path,
                           scratch_parent: Path, guard, *, timeout: int = TIME_LIMIT,
                           diag: dict | None = None):
    """Run guarded cargo vendor --locked for both arms and stage offline cargo_home."""
    partial = fetch_home = vendor = config = None
    try:
        _set_diag(diag, "FETCH_PRECHECK")
        require(isinstance(guard, capacity.HostStorageGuard) and
                type(timeout) is int and 1 <= timeout <= 1800)
        archive_tools._owned_directory(cargo_home)
        archive_tools._owned_directory(scratch_parent)
        require(cargo_home.resolve(strict=True) == cargo_home and
                Path.home() in cargo_home.parents and
                not any(cargo_home.iterdir()) and
                scratch_parent.resolve(strict=True) == scratch_parent)
        _set_diag(diag, "FETCH_LOCKS")
        vendor_consume._locks(sources)
        _set_diag(diag, "FETCH_VENDOR_CAPACITY")
        guard()

        _set_diag(diag, "FETCH_CARGO_BIN")
        home = Path.home()
        search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
        cargo_bin = rust_preflight._binary("cargo", search)
        partial = Path(tempfile.mkdtemp(prefix=".vendor.", dir=cargo_home))
        os.chmod(partial, 0o700)
        fetch_home = Path(tempfile.mkdtemp(prefix="cargo-fetch-", dir=scratch_parent))
        os.chmod(fetch_home, 0o700)

        env = {
            "HOME": str(home),
            "PATH": search,
            "LANG": "C.UTF-8",
            "CARGO_HOME": str(fetch_home),
            "RUSTUP_TOOLCHAIN": rust_preflight.VERSION,
            "RUSTUP_AUTO_INSTALL": "0",
        }
        argv = [
            str(cargo_bin), "vendor", "--locked",
            "--manifest-path", str(sources["candidate"] / "Cargo.toml"),
            "--sync", str(sources["before"] / "Cargo.toml"),
            str(partial),
        ]
        log_path = scratch_parent / "cargo-vendor.private.log"
        fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        deadline = time.monotonic() + timeout
        with os.fdopen(fd, "wb") as log:
            _set_diag(diag, "FETCH_VENDOR_CAPACITY")
            guard()
            _set_diag(diag, "FETCH_VENDOR_EXIT")
            process = subprocess.Popen(
                argv, cwd=sources["candidate"], env=env,
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
            try:
                while process.poll() is None:
                    if time.monotonic() >= deadline:
                        _set_diag(diag, "FETCH_VENDOR_TIMEOUT")
                        require(False)
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
                    _set_diag(diag, "FETCH_VENDOR_CAPACITY")
                    guard()
                _set_diag(diag, "FETCH_VENDOR_EXIT")
                require(process.returncode == 0)
                _set_diag(diag, "FETCH_VENDOR_CAPACITY")
                guard()
            except BaseException:
                _stop_process(process)
                raise

        # Release the ephemeral registry download cache before hashing partial so
        # peak storage and filesystem traversal stay bounded during normalization.
        shutil.rmtree(fetch_home)
        fetch_home = None

        _set_diag(diag, "FETCH_NORMALIZE_TREE")
        vendor_tree_sha256, vendor_mode_sha256 = _normalize_and_digest(
            partial, guard, deadline=deadline, diag=diag)
        _set_diag(diag, "FETCH_PROMOTE")
        vendor = cargo_home / "vendor"
        archive_tools._promote_noreplace(partial, vendor)
        partial = None

        _set_diag(diag, "FETCH_CONFIG")
        config = cargo_home / "config.toml"
        content = ("[source.crates-io]\nreplace-with = \"vendored-sources\"\n\n"
                   "[source.vendored-sources]\ndirectory = " +
                   json.dumps(str(vendor)) + "\n").encode()
        cfd = os.open(config, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                      getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(cfd, "wb") as output:
            output.write(content)
        _set_diag(diag, "FETCH_VENDOR_CAPACITY")
        guard()
        if isinstance(diag, dict):
            diag.pop("code", None)
        return {
            "cargo_home": cargo_home,
            "vendor_root": vendor,
            "vendor_tree_sha256": vendor_tree_sha256,
            "vendor_mode_sha256": vendor_mode_sha256,
            "acquisition_mode": "cargo_vendor_locked",
        }
    except BaseException:
        for path in (config,):
            if path is not None and path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass
        for directory in (vendor, partial, fetch_home):
            if directory is not None and directory.exists():
                try:
                    shutil.rmtree(directory)
                except OSError:
                    pass
        raise ValueError(ERROR) from None
