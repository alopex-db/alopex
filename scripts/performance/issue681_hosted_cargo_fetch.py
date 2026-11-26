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


def _normalize_and_digest(vendor_root: Path, guard):
    """Reject symlinks/special files, normalize modes, and hash tree/modes."""
    rows = {}
    mode_rows = {}
    files = 0
    pending = [vendor_root]
    while pending:
        current = pending.pop()
        info = current.lstat()
        require(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and
                info.st_uid == os.geteuid())
        os.chmod(current, 0o700)
        guard()
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
                    require(files <= MAX_FILES and 0 <= einfo.st_size <= MAX_FILE_BYTES)
                    mode = 0o700 if (einfo.st_mode & 0o111) else 0o600
                    os.chmod(path, mode)
                    digest = hashlib.sha256()
                    with path.open("rb") as stream:
                        for chunk in iter(lambda: stream.read(1_048_576), b""):
                            digest.update(chunk)
                            guard()
                    rel = path.relative_to(vendor_root).as_posix()
                    rows[rel] = (einfo.st_size, digest.hexdigest())
                    mode_rows[rel] = mode
                else:
                    require(False)
    require(files > 0)
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
                           scratch_parent: Path, guard, *, timeout: int = TIME_LIMIT):
    """Run guarded cargo vendor --locked for both arms and stage offline cargo_home."""
    partial = fetch_home = vendor = config = None
    try:
        require(isinstance(guard, capacity.HostStorageGuard) and
                type(timeout) is int and 1 <= timeout <= 1800)
        archive_tools._owned_directory(cargo_home)
        archive_tools._owned_directory(scratch_parent)
        require(cargo_home.resolve(strict=True) == cargo_home and
                Path.home() in cargo_home.parents and
                not any(cargo_home.iterdir()) and
                scratch_parent.resolve(strict=True) == scratch_parent)
        vendor_consume._locks(sources)
        guard()

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
        with os.fdopen(fd, "wb") as log:
            guard()
            process = subprocess.Popen(
                argv, cwd=sources["candidate"], env=env,
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
            deadline = time.monotonic() + timeout
            try:
                while process.poll() is None:
                    require(time.monotonic() < deadline)
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
                    guard()
                require(process.returncode == 0)
                guard()
            except BaseException:
                _stop_process(process)
                raise

        vendor_tree_sha256, vendor_mode_sha256 = _normalize_and_digest(partial, guard)
        vendor = cargo_home / "vendor"
        archive_tools._promote_noreplace(partial, vendor)
        partial = None
        shutil.rmtree(fetch_home)
        fetch_home = None

        config = cargo_home / "config.toml"
        content = ("[source.crates-io]\nreplace-with = \"vendored-sources\"\n\n"
                   "[source.vendored-sources]\ndirectory = " +
                   json.dumps(str(vendor)) + "\n").encode()
        cfd = os.open(config, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                      getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(cfd, "wb") as output:
            output.write(content)
        guard()
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
