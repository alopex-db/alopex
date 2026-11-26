"""Build the fixed public docs SQL worker archive from exact source bytes."""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import stat
import subprocess
import tarfile

COMMIT = "5c3d068b9ce4d60fbc1608924882df13b0a27568"
TREE = "ee424fc181960bfcd77111f596115970d4686406"
FILES = {
    "server-sql-isolated-v0816.py": (11397,
        "856a5def988dfa60f0343c6686ebccbe260e67fde644b5451f6063a8faa1b093"),
    "sql_resources.py": (10944,
        "fe084b47aa509cbb520a8ba13266d7886a73559d93386c7318800e5816d29f60"),
    "subject_identity.py": (9738,
        "f1371d261b80b5cab510e699b4d9350aa4a4dd18aa7feb13f8fcada4c2075abd"),
}
MAX_ARCHIVE = 8_000_000


def require(ok):
    if not ok:
        raise ValueError("DOCS_HARNESS")


def _git(checkout, expression):
    result = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "--verify", expression],
        stdin=subprocess.DEVNULL, capture_output=True, timeout=10, check=False)
    require(result.returncode == 0 and not result.stderr)
    return result.stdout.decode("ascii").strip()


def _source(checkout):
    require(isinstance(checkout, Path) and checkout.is_absolute() and
            checkout.is_dir() and not checkout.is_symlink() and
            _git(checkout, "HEAD") == COMMIT and
            _git(checkout, "HEAD^{tree}") == TREE)
    for directory in (checkout / "scripts", checkout / "scripts/performance"):
        require(stat.S_ISDIR(directory.lstat().st_mode))
    files = {}
    for name, (size, digest) in FILES.items():
        path = checkout / "scripts/performance" / name
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_size == size and size < MAX_ARCHIVE)
        data = path.read_bytes()
        require(len(data) == size and hashlib.sha256(data).hexdigest() == digest)
        files[name] = data
    return files


def _verify_archive(path, files):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
            info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
            0 < info.st_size < MAX_ARCHIVE)
    with tarfile.open(path, "r") as archive:
        members = archive.getmembers()
        require(len(members) == len(files) and
                [member.name for member in members] == [
                    "scripts/performance/" + name for name in sorted(files)])
        for member in members:
            name = member.name.removeprefix("scripts/performance/")
            require(member.isfile() and member.mode == 0o644 and
                    member.uid == member.gid == member.mtime == 0 and
                    member.size == len(files[name]) and
                    archive.extractfile(member).read() == files[name])


def build(checkout, destination):
    """Write one canonical private TAR; never copy checkout paths into it."""
    try:
        require(isinstance(destination, Path) and destination.is_absolute() and
                not destination.exists() and not destination.is_symlink())
        parent = destination.parent
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700)
        files = _source(checkout)
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            with os.fdopen(fd, "wb") as output:
                with tarfile.open(fileobj=output, mode="w",
                                  format=tarfile.USTAR_FORMAT) as archive:
                    for name, data in sorted(files.items()):
                        item = tarfile.TarInfo("scripts/performance/" + name)
                        item.size, item.mode = len(data), 0o644
                        item.uid = item.gid = item.mtime = 0
                        item.uname = item.gname = ""
                        archive.addfile(item, io.BytesIO(data))
                output.flush()
                os.fsync(output.fileno())
            _verify_archive(destination, files)
            archive_sha256 = hashlib.sha256(destination.read_bytes()).hexdigest()
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        return {"commit": COMMIT, "tree": TREE,
                "archive_sha256": archive_sha256,
                "files": {name: {"size": size, "sha256": digest}
                          for name, (size, digest) in FILES.items()}}
    except (OSError, KeyError, TypeError, ValueError, UnicodeError,
            subprocess.TimeoutExpired, tarfile.TarError):
        raise ValueError("DOCS_HARNESS") from None
