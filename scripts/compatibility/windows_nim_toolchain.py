"""Bounded, quiet Nim bootstrap for the hosted Windows compatibility suite.

UC #682: the caller runs this process through its private-log storage watchdog.
No archive contents, paths, or subprocess diagnostics are written to Actions.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.request
import zipfile


REPO = Path(__file__).resolve().parents[2]
SETUP = REPO / ".github/actions/setup-nim-parser-toolchain/setup.sh"
URL = "https://nim-lang.org/download/nim-2.2.10_x64.zip"
SHA256 = "fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f"
MAX_DOWNLOAD = 40_000_000
MAX_MEMBER = 128_000_000
MAX_EXTRACTED = 512_000_000
MAX_MEMBERS = 10_000
NIM_ROOT = "nim-2.2.10"
OVERRIDES = ("NIMBLE_SOURCE_OVERRIDE", "NIMBLE_PACKAGE_SEED_OVERRIDE",
             "NIMBLE_BINARY_OVERRIDE", "NIM_BINARY_OVERRIDE")


class BootstrapError(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def download(archive: Path) -> None:
    digest = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(URL, timeout=30) as response, archive.open("xb") as output:
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_DOWNLOAD:
                    raise BootstrapError("NIM_DOWNLOAD_SIZE")
                digest.update(chunk)
                output.write(chunk)
    except BootstrapError:
        raise
    except (OSError, ValueError) as error:
        raise BootstrapError("NIM_DOWNLOAD") from error
    if digest.hexdigest() != SHA256:
        raise BootstrapError("NIM_DOWNLOAD_HASH")


def _parts(info: zipfile.ZipInfo) -> tuple[str, ...]:
    name = info.filename
    orig = getattr(info, "orig_filename", name)
    if (not isinstance(orig, str) or orig != name
            or not name or name.startswith("/") or "\\" in name or ":" in name
            or "\x00" in name):
        raise BootstrapError("NIM_ARCHIVE")
    pieces = name.rstrip("/").split("/")
    if (not pieces or pieces[0] != NIM_ROOT
            or any(part in ("", ".", "..") for part in pieces)):
        raise BootstrapError("NIM_ARCHIVE")
    mode = (info.external_attr >> 16) & 0o170000
    if (info.flag_bits & 1 or mode not in (0, stat.S_IFREG, stat.S_IFDIR)
            or (info.is_dir() and mode == stat.S_IFREG)
            or (not info.is_dir() and mode == stat.S_IFDIR)):
        raise BootstrapError("NIM_ARCHIVE")
    return tuple(pieces)


def extract(archive: Path, destination: Path) -> Path:
    """Validate the entire directory before writing, then extract with size caps."""
    try:
        with zipfile.ZipFile(archive) as source:
            infos = source.infolist()
            if not infos or len(infos) > MAX_MEMBERS:
                raise BootstrapError("NIM_ARCHIVE")
            seen: set[str] = set()
            entries: list[tuple[zipfile.ZipInfo, tuple[str, ...]]] = []
            total = 0
            files: set[str] = set()
            for info in infos:
                parts = _parts(info)
                key = "/".join(parts).casefold()
                if key in seen or info.file_size > MAX_MEMBER:
                    raise BootstrapError("NIM_ARCHIVE")
                seen.add(key)
                if not info.is_dir():
                    files.add(key)
                total += info.file_size
                if total > MAX_EXTRACTED:
                    raise BootstrapError("NIM_ARCHIVE")
                entries.append((info, parts))
            for _, parts in entries:
                if any("/".join(parts[:index]).casefold() in files
                       for index in range(1, len(parts))):
                    raise BootstrapError("NIM_ARCHIVE")
            for info, parts in entries:
                target = destination.joinpath(*parts)
                if info.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                copied = 0
                with source.open(info) as input_stream, target.open("xb") as output:
                    while chunk := input_stream.read(1024 * 1024):
                        copied += len(chunk)
                        if copied > info.file_size or copied > MAX_MEMBER:
                            raise BootstrapError("NIM_ARCHIVE")
                        output.write(chunk)
                if copied != info.file_size:
                    raise BootstrapError("NIM_ARCHIVE")
    except BootstrapError:
        raise
    except (OSError, ValueError, RuntimeError, zipfile.BadZipFile) as error:
        raise BootstrapError("NIM_ARCHIVE") from error
    nim = destination / NIM_ROOT / "bin/nim.exe"
    if not nim.is_file():
        raise BootstrapError("NIM_ARCHIVE")
    return nim


def verify_version(nim: Path) -> None:
    try:
        with tempfile.TemporaryFile() as output:
            result = subprocess.run([str(nim), "--version"], stdin=subprocess.DEVNULL,
                                    stdout=output, stderr=subprocess.STDOUT,
                                    timeout=20, check=False)
            output.seek(0)
            first = output.read(4096).decode("utf-8", errors="replace")
            if result.returncode or not re.search(
                    r"(?m)^Nim Compiler Version 2\.2\.10(?:\s|$)", first):
                raise BootstrapError("NIM_VERSION")
    except BootstrapError:
        raise
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BootstrapError("NIM_VERSION") from error


def bootstrap(work_dir: Path, bash: Path) -> None:
    if not work_dir.is_dir() or not bash.is_file() or not SETUP.is_file():
        raise BootstrapError("NIM_INPUT")
    final = work_dir / NIM_ROOT
    if final.exists() or final.is_symlink():
        raise BootstrapError("NIM_INPUT")
    with tempfile.TemporaryDirectory(prefix="nim-bootstrap-", dir=work_dir) as temporary:
        staging = Path(temporary)
        archive = staging / "nim.zip"
        download(archive)
        nim = extract(archive, staging / "unpack")
        verify_version(nim)
        shutil.move(str(staging / "unpack" / NIM_ROOT), final)
    env = dict(os.environ)
    env.update({name: "" for name in OVERRIDES})
    env["PATH"] = str(final / "bin") + os.pathsep + env.get("PATH", "")
    try:
        result = subprocess.run([str(bash), "--noprofile", "--norc",
                                 SETUP.relative_to(REPO).as_posix()],
                                cwd=REPO, env=env, stdin=subprocess.DEVNULL, check=False)
    except OSError as error:
        raise BootstrapError("NIM_SETUP") from error
    if result.returncode:
        raise BootstrapError("NIM_SETUP")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--bash", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        bootstrap(arguments.work_dir, arguments.bash)
    except BootstrapError as error:
        print(error.code, file=sys.stderr)
        return 1
    except Exception:
        # Untrusted ZIP/decompressor and subprocess failures must not expose
        # exception text or local paths outside the caller's private log.
        print("NIM_IO", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
