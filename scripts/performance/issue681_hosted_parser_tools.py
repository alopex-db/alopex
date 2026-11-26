"""Verify exact offline Nim and Nimble executables before parser construction."""

from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import tarfile
import time

import issue681_hosted_nim_archive as archive


NIM_VERSION = "2.2.10"
NIMBLE_VERSION = "0.22.3"
NIMBLE_TAG = "v0.22.3"
NIMBLE_SHA = "42ef70c2102a942c46f13eb76872326edd525cec"
NIMBLE_ARCHIVE_NAME = "nimble-linux_x64.tar.gz"
NIMBLE_DOWNLOAD_PATH = (
    f"/nim-lang/nimble/releases/download/{NIMBLE_TAG}/{NIMBLE_ARCHIVE_NAME}"
)
NIMBLE_ARCHIVE_BYTES = 1_576_268
NIMBLE_ARCHIVE_SHA256 = (
    "0cf3f7b2b0a1cd3e3c459f73f46936cd84a93c1b98ef07ff43d050a0ef948911"
)
NIMBLE_BIN_BYTES = 4_288_104
NIMBLE_BIN_SHA256 = (
    "674b21cbf1d9de73d34c7477459b5ef62c70c2980eb8db79c909cfb411674bc9"
)
NIMBLE_FETCH_TIME_LIMIT = 120
OUTPUT_LIMIT = 4096
TIME_LIMIT = 10
NIMBLE_DIAG_CODES = frozenset({
    "NIMBLE_ADMISSION",
    "NIMBLE_REDIRECT",
    "NIMBLE_TRANSFER",
    "NIMBLE_ARCHIVE_DIGEST",
    "NIMBLE_EXTRACT",
    "NIMBLE_BIN_DIGEST",
    "NIMBLE_INSTALL",
    "NIMBLE_CAPACITY",
})


def _set_diag(diag, code):
    if isinstance(diag, dict) and code in NIMBLE_DIAG_CODES:
        diag["code"] = code


def require(value):
    if not value:
        raise ValueError("PUBLIC_PARSER_INPUT")


def _tool(path):
    require(path.is_file() and not path.is_symlink())
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)


def _stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def _run_tool(argv, probe, name, guard):
    output = probe / (name + ".private.out")
    environment = {
        "HOME": str(probe), "XDG_CONFIG_HOME": str(probe),
        "XDG_CACHE_HOME": str(probe), "TMPDIR": str(probe),
        "PATH": str(argv[0].parent) + ":/usr/bin:/bin",
        "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
        "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/bin/false",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
    }
    selector = selectors.DefaultSelector()
    process = None
    captured = bytearray()
    closed = False
    stopped = False
    try:
        guard()
        process = subprocess.Popen(
            [str(part) for part in argv], cwd=probe, env=environment,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + TIME_LIMIT
        require(process.stdout is not None)
        selector.register(process.stdout, selectors.EVENT_READ)
        while not closed:
            require(time.monotonic() < deadline)
            guard()
            for key, _ in selector.select(timeout=0.25):
                block = os.read(key.fd, min(1024, OUTPUT_LIMIT + 1 - len(captured)))
                if not block:
                    selector.unregister(process.stdout)
                    closed = True
                    break
                captured.extend(block)
                require(len(captured) <= OUTPUT_LIMIT)
            if process.poll() is not None and not stopped:
                _stop_group(process)
                stopped = True
        require(process.wait(timeout=1) == 0)
        guard()
        fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(captured)
        return captured.decode("utf-8")
    finally:
        if process is not None and not stopped:
            _stop_group(process)
        selector.close()
        if process is not None and process.stdout is not None:
            process.stdout.close()


def verify_tools(extracted, guard):
    """Return fixed verified paths, or fixed PUBLIC_PARSER_INPUT failure."""
    probe = None
    identity = None
    stage = "tools-admission"
    try:
        require(isinstance(extracted, Path) and extracted.is_absolute() and
                extracted.name == archive.EXTRACTED_NAME)
        archive._owned_directory(extracted.parent)
        archive._owned_directory(extracted)
        require(callable(guard))
        roots = [path for path in extracted.iterdir() if path.is_dir()]
        require(len(roots) == 1 and roots[0].name in archive.ARCHIVE_ROOTS and
                not roots[0].is_symlink())
        bin_dir = roots[0] / "bin"
        archive._owned_directory(roots[0])
        archive._owned_directory(bin_dir)
        nim = bin_dir / "nim"
        nimble = bin_dir / "nimble"
        _tool(nim)
        _tool(nimble)
        probe = extracted.parent / "tools-probe.partial"
        require(not probe.exists() and not probe.is_symlink())
        probe.mkdir(mode=0o700)
        info = probe.lstat()
        identity = (info.st_dev, info.st_ino)
        stage = "tools-nim"
        nim_output = _run_tool([nim, "--version"], probe, "nim", guard)
        versions = re.findall(r"(?m)^Nim Compiler Version "
                              r"([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)",
                              nim_output)
        require(versions == [NIM_VERSION])
        stage = "tools-nimble"
        nimble_output = _run_tool([
            nimble, "--nimbleDir:" + str(probe), "--nim:" + str(nim),
            "--useSystemNim", "--offline", "--version",
        ], probe, "nimble", guard)
        nimble_versions = re.findall(r"(?m)^nimble v([0-9]+\.[0-9]+\.[0-9]+)"
                                     r"(?:\s|$)", nimble_output)
        hashes = re.findall(r"(?m)^git hash: ([0-9a-f]{40})$",
                            nimble_output)
        require(nimble_versions == [NIMBLE_VERSION] and hashes == [NIMBLE_SHA])
        stage = "tools-cleanup"
        archive._remove_owned_tree(probe, identity)
        probe = None
        identity = None
        guard()
        return {"nim": nim, "nimble": nimble}
    except BaseException as error:
        if isinstance(extracted, Path):
            archive._private_reason(extracted.parent, stage, error)
        if probe is not None and identity is not None:
            try:
                archive._remove_owned_tree(probe, identity)
            except Exception:
                pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None


def install_pinned_nimble(extracted, guard, *, diag=None):
    """Install cryptographically pinned Nimble 0.22.3 (42ef70c2) release binary."""
    first = second = None
    partial = None
    created_partial = False
    stage = "nimble-install-admission"

    def run_guard():
        try:
            guard()
        except BaseException:
            _set_diag(diag, "NIMBLE_CAPACITY")
            raise

    try:
        _set_diag(diag, "NIMBLE_ADMISSION")
        require(isinstance(extracted, Path) and extracted.is_absolute() and
                extracted.name == archive.EXTRACTED_NAME)
        archive._owned_directory(extracted.parent)
        archive._owned_directory(extracted)
        require(callable(guard) and
                re.fullmatch(r"[0-9a-f]{64}", NIMBLE_ARCHIVE_SHA256) and
                re.fullmatch(r"[0-9a-f]{64}", NIMBLE_BIN_SHA256) and
                0 < NIMBLE_ARCHIVE_BYTES <= 20_000_000 and
                0 < NIMBLE_BIN_BYTES <= 50_000_000)
        roots = [path for path in extracted.iterdir() if path.is_dir()]
        require(len(roots) == 1 and roots[0].name in archive.ARCHIVE_ROOTS and
                not roots[0].is_symlink())
        bin_dir = roots[0] / "bin"
        archive._owned_directory(roots[0])
        archive._owned_directory(bin_dir)
        nim = bin_dir / "nim"
        nimble = bin_dir / "nimble"
        _tool(nim)
        require(not nimble.is_symlink())
        partial = bin_dir / "nimble.partial"
        require(not partial.exists() and not partial.is_symlink())
        run_guard()

        deadline = time.monotonic() + NIMBLE_FETCH_TIME_LIMIT
        stage = "nimble-install-redirect"
        _set_diag(diag, "NIMBLE_REDIRECT")
        first, response = archive._request(archive.FIRST_HOST,
                                           NIMBLE_DOWNLOAD_PATH)
        require(response.status in (302, 307, 308))
        target = archive._redirect_target(response.getheader("Location"))
        response.close()
        first.close()
        first = None
        require(time.monotonic() < deadline)

        stage = "nimble-install-transfer"
        _set_diag(diag, "NIMBLE_TRANSFER")
        second, response = archive._request(archive.ASSET_HOST, target)
        require(response.status == 200)
        content_length = response.getheader("Content-Length")
        if content_length is not None:
            require(content_length.isdecimal() and
                    int(content_length) == NIMBLE_ARCHIVE_BYTES)
        raw = bytearray()
        digest = hashlib.sha256()
        while block := response.read(archive.CHUNK):
            require(time.monotonic() < deadline)
            require(len(raw) + len(block) <= NIMBLE_ARCHIVE_BYTES)
            raw.extend(block)
            digest.update(block)
            run_guard()
        response.close()
        second.close()
        second = None

        stage = "nimble-install-archive-digest"
        _set_diag(diag, "NIMBLE_ARCHIVE_DIGEST")
        require(len(raw) == NIMBLE_ARCHIVE_BYTES and
                digest.hexdigest() == NIMBLE_ARCHIVE_SHA256)

        stage = "nimble-install-extract"
        _set_diag(diag, "NIMBLE_EXTRACT")
        with tarfile.open(fileobj=io.BytesIO(bytes(raw)), mode="r:gz") as tf:
            members = tf.getmembers()
            require(len(members) == 1)
            member = members[0]
            require(member.name == "nimble" and member.isfile() and
                    not (member.mode & ~0o777) and bool(member.mode & 0o111) and
                    member.size == NIMBLE_BIN_BYTES)
            source_file = tf.extractfile(member)
            require(source_file is not None)
            binary = source_file.read(NIMBLE_BIN_BYTES + 1)
            require(len(binary) == NIMBLE_BIN_BYTES)

        stage = "nimble-install-bin-digest"
        _set_diag(diag, "NIMBLE_BIN_DIGEST")
        require(hashlib.sha256(binary).hexdigest() == NIMBLE_BIN_SHA256)

        stage = "nimble-install-replace"
        _set_diag(diag, "NIMBLE_INSTALL")
        fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o700)
        created_partial = True
        with os.fdopen(fd, "wb") as stream:
            stream.write(binary)
        os.chmod(partial, 0o700)
        os.replace(partial, nimble)
        created_partial = False
        _tool(nimble)
        run_guard()
        if isinstance(diag, dict):
            diag.pop("code", None)
        return nimble
    except BaseException as error:
        if isinstance(extracted, Path):
            archive._private_reason(extracted.parent, stage, error)
        if created_partial and partial is not None:
            try:
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
