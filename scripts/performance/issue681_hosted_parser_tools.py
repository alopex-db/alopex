"""Verify exact offline Nim and Nimble executables before parser construction."""

from __future__ import annotations

import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import time

import issue681_hosted_acquire_source as acquire_source
import issue681_hosted_nim_archive as archive


NIM_VERSION = "2.2.10"
NIMBLE_VERSION = "0.22.3"
NIMBLE_TAG = "v0.22.3"
NIMBLE_REPO = "https://github.com/nim-lang/nimble.git"
NIMBLE_SHA = "42ef70c2102a942c46f13eb76872326edd525cec"
OUTPUT_LIMIT = 4096
TIME_LIMIT = 10
BUILD_TIME_LIMIT = 300


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


def build_pinned_nimble(extracted, guard, git_env):
    """Build exact Nimble 0.22.3 (42ef70c2) using verified extracted Nim 2.2.10."""
    scratch = None
    identity = None
    stage = "nimble-build-admission"
    try:
        require(isinstance(extracted, Path) and extracted.is_absolute() and
                extracted.name == archive.EXTRACTED_NAME)
        archive._owned_directory(extracted.parent)
        archive._owned_directory(extracted)
        require(callable(guard))
        acquire_source._trusted_git()
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
        scratch = extracted.parent / "nimble-build.partial"
        require(not scratch.exists() and not scratch.is_symlink())
        scratch.mkdir(mode=0o700)
        info = scratch.lstat()
        identity = (info.st_dev, info.st_ino)

        stage = "nimble-build-nim-check"
        nim_output = _run_tool([nim, "--version"], scratch, "nim", guard)
        versions = re.findall(r"(?m)^Nim Compiler Version "
                              r"([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)", nim_output)
        require(versions == [NIM_VERSION])

        stage = "nimble-build-clone"
        src_dir = scratch / "src"
        clone_home = scratch / "git-home"
        clone_home.mkdir(mode=0o700)
        clone_env = dict(git_env, HOME=str(clone_home),
                         XDG_CONFIG_HOME=str(clone_home))
        log_path = scratch / "nimble-build.private.log"
        fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as log:
            acquire_source._git(
                ["clone", "--quiet", "--branch", NIMBLE_TAG, "--depth", "1",
                 "--recurse-submodules", "--shallow-submodules",
                 NIMBLE_REPO, str(src_dir)],
                scratch, guard, log, clone_env)
            stage = "nimble-build-verify-src"
            head = subprocess.run(
                [str(acquire_source.GIT), "-C", str(src_dir),
                 "-c", "credential.helper=", "-c", "core.hooksPath=/dev/null",
                 "rev-parse", "HEAD"],
                env=clone_env, stdin=subprocess.DEVNULL, capture_output=True,
                timeout=15, check=False)
            require(head.returncode == 0 and
                    head.stdout.decode("ascii", errors="ignore").strip() == NIMBLE_SHA)
            clean = subprocess.run(
                [str(acquire_source.GIT), "-C", str(src_dir),
                 "-c", "credential.helper=", "-c", "core.hooksPath=/dev/null",
                 "status", "--porcelain", "--ignore-submodules=none"],
                env=clone_env, stdin=subprocess.DEVNULL, capture_output=True,
                timeout=15, check=False)
            require(clean.returncode == 0 and clean.stdout == b"")
            sub = subprocess.run(
                [str(acquire_source.GIT), "-C", str(src_dir),
                 "-c", "credential.helper=", "-c", "core.hooksPath=/dev/null",
                 "submodule", "status", "--recursive"],
                env=clone_env, stdin=subprocess.DEVNULL, capture_output=True,
                timeout=15, check=False)
            require(sub.returncode == 0 and not any(
                line[:1] in ("+", "U", "-")
                for line in sub.stdout.decode("utf-8", errors="replace").splitlines()))

            stage = "nimble-build-compile"
            nimcache = scratch / "nimcache"
            nimcache.mkdir(mode=0o700)
            built_bin = scratch / "nimble.built"
            build_env = {
                "HOME": str(scratch), "XDG_CONFIG_HOME": str(scratch),
                "XDG_CACHE_HOME": str(scratch), "TMPDIR": str(scratch),
                "PATH": str(bin_dir) + ":/usr/bin:/bin",
                "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
                "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/bin/false",
                "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
            }
            guard()
            process = subprocess.Popen(
                [str(nim), "c", "-d:release", f"--nimcache:{nimcache}",
                 f"-o:{built_bin}", "src/nimble.nim"],
                cwd=src_dir, env=build_env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                umask=0o077)
            deadline = time.monotonic() + BUILD_TIME_LIMIT
            try:
                while process.poll() is None:
                    require(time.monotonic() < deadline)
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
                    guard()
                require(process.returncode == 0)
            except BaseException:
                if process.poll() is None:
                    _stop_group(process)
                raise

        stage = "nimble-build-install"
        require(built_bin.is_file() and not built_bin.is_symlink())
        os.chmod(built_bin, 0o700)
        os.replace(built_bin, nimble)
        _tool(nimble)
        archive._remove_owned_tree(scratch, identity)
        scratch = None
        identity = None
        guard()
        return nimble
    except BaseException as error:
        if isinstance(extracted, Path):
            archive._private_reason(extracted.parent, stage, error)
        if scratch is not None and identity is not None:
            try:
                archive._remove_owned_tree(scratch, identity)
            except Exception:
                pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
