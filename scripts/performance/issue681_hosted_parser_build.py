"""Build one common offline parser input in the selected hosted job."""

from __future__ import annotations

import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time

import issue681_hosted_acquire_source as acquire
import issue681_hosted_build_guard as capacity
import issue681_hosted_nim_archive as archive
import issue681_hosted_parser_stage as stage
import issue681_hosted_parser_seed as seed_input
import issue681_hosted_parser_tools as tools
import issue681_public_assets_produce as producer


TARGET = "x86_64-unknown-linux-gnu"
LOG_LIMIT = 16 * 1024 * 1024
TIME_LIMIT = 20 * 60
OUTPUT_NAME = "libalopex_sql_parser.so"
SCRATCH = Path("/tmp/alopex-nim-parser-v0.8.17-contract-0.28.0-" + TARGET)
BASH = Path("/usr/bin/bash")
PYTHON = Path("/usr/bin/python3")
READELF = Path("/usr/bin/readelf")


def require(value):
    if not value:
        raise ValueError("PUBLIC_PARSER_INPUT")


def _trusted_executable(path):
    entry = path.lstat()
    require(entry.st_uid == 0)
    resolved = path.resolve(strict=True)
    info = resolved.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
            not (info.st_mode & 0o022) and os.access(path, os.X_OK))


def _stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=10)


def _run(argv, source, environment, log_path, guard):
    process = None
    stopped = False
    closed = False
    count = 0
    selector = selectors.DefaultSelector()
    fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as log:
            info = os.fstat(log.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                    and stat.S_IMODE(info.st_mode) == 0o600)
            guard()
            process = subprocess.Popen(
                argv, cwd=source, env=environment, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                start_new_session=True)
            require(process.stdout is not None)
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + TIME_LIMIT
            while not closed:
                require(time.monotonic() < deadline)
                guard()
                for key, _ in selector.select(timeout=0.25):
                    block = os.read(key.fd, min(65_536, LOG_LIMIT + 1 - count))
                    if not block:
                        selector.unregister(process.stdout)
                        closed = True
                        break
                    count += len(block)
                    require(count <= LOG_LIMIT)
                    log.write(block)
                if process.poll() is not None and not stopped:
                    _stop_group(process)
                    stopped = True
            require(process.wait(timeout=1) == 0)
            guard()
    finally:
        if process is not None and not stopped:
            _stop_group(process)
        selector.close()
        if process is not None and process.stdout is not None:
            process.stdout.close()


def _identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)
    return info.st_dev, info.st_ino


def _verify_seed(seed, guard):
    packages = []
    for name in ("npeg", "msgpack4nim"):
        pin = seed_input.PINS[name]
        package = seed / "pkgs2" / (name + "-" + pin["version"] + "-" +
                                    pin["commit"][:12])
        packages.append(name + "=" + pin["version"] + "=" + str(package))
    metadata = [name + "=" + str(seed / name)
                for name in seed_input.PACKAGE_FILES]
    guard()
    require(len(stage.manifest.package_identities(packages)) == 2)
    guard()
    require(len(stage.manifest.metadata_identities(metadata)) == 2)
    guard()


def _private_failure(root, phase, error):
    try:
        category = type(error).__name__
        if category not in {"ValueError", "OSError", "TimeoutError",
                            "RuntimeError", "ParserAssetError"}:
            category = "Other"
        path = root / "failure.private.log"
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as log:
            info = os.fstat(log.fileno())
            require(stat.S_ISREG(info.st_mode) and
                    info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o600)
            log.write((phase + ":" + category + "\n").encode())
    except BaseException:
        pass


def build_parser(source, nim, nimble, seed, build_parent, stage_parent,
                 guard, git_binary, git_env):
    """Return verified parser inputs for both arms, or fixed failure."""
    owned = []
    admitted = False
    phase = "parser-build-admission"
    try:
        require(sys.platform == "linux" and os.uname().machine == "x86_64")
        require(type(guard) is capacity.HostStorageGuard)
        require(guard.manifest is not None and
                guard.manifest.scratch == SCRATCH)
        for path in (source, seed, build_parent, stage_parent):
            archive._owned_directory(path)
            require(path.resolve(strict=True) == path)
        require(not any(build_parent.iterdir()) and
                not any(stage_parent.iterdir()))
        admitted = True
        roots = (source, seed, build_parent, stage_parent, nim, nimble)
        require(all(left != right and left not in right.parents and
                    right not in left.parents
                    for index, left in enumerate(roots)
                    for right in roots[index + 1:]))
        require(all(" " not in str(path) for path in roots))
        _trusted_executable(BASH)
        _trusted_executable(PYTHON)
        _trusted_executable(READELF)
        require(git_binary == acquire.GIT)
        acquire._trusted_git()
        seed_input._sealed_git_env(git_env)
        tools._tool(nim)
        tools._tool(nimble)
        archive._owned_directory(nim.parent)
        require(nim.parent == nimble.parent and nim.name == "nim" and
                nimble.name == "nimble")
        verified_tools = tools.verify_tools(nim.parents[2], guard)
        require(verified_tools == {"nim": nim, "nimble": nimble})
        _verify_seed(seed, guard)
        require(
                not SCRATCH.exists() and not SCRATCH.is_symlink() and
                not Path(str(SCRATCH) + ".lock").exists() and
                not Path(str(SCRATCH) + ".lock").is_symlink())
        pin = acquire.PINS["candidate"]
        producer.verify_source(source, pin["commit"], pin["tree"],
                               pin["example"], pin["lock"],
                               git_binary=git_binary, git_env=git_env)
        guard()
        output_dir = build_parent / "lib"
        archive_dir = build_parent / "archive"
        output_dir.mkdir(mode=0o700)
        owned.append((output_dir, _identity(output_dir)))
        archive_dir.mkdir(mode=0o700)
        owned.append((archive_dir, _identity(archive_dir)))
        output = output_dir / OUTPUT_NAME
        argv = [str(BASH), str(source / "scripts/build-nim-parser.sh"),
                "--backend", "host", "--target", TARGET,
                "--archive-dir", str(archive_dir), "--output", str(output)]
        environment = {
            "PATH": str(nim.parent) + ":/usr/bin:/bin",
            "HOME": str(build_parent), "XDG_CONFIG_HOME": str(build_parent),
            "XDG_CACHE_HOME": str(build_parent), "TMPDIR": str(build_parent),
            "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
            "PYTHON": str(PYTHON), "ALOPEX_NIM_BIN": str(nim),
            "ALOPEX_NIMBLE_BIN": str(nimble),
            "ALOPEX_NIMBLE_SEED_DIR": str(seed),
            "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/bin/false",
            "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
        }
        phase = "parser-build-process"
        _run(argv, source, environment, build_parent / "build.private.log", guard)
        require(not SCRATCH.exists() and
                not Path(str(SCRATCH) + ".lock").exists())
        guard()
        phase = "parser-build-verification"
        producer.verify_source(source, pin["commit"], pin["tree"],
                               pin["example"], pin["lock"],
                               git_binary=git_binary, git_env=git_env)
        result = stage.stage_parser(archive_dir, stage_parent, guard)
        owned.append((result["parser_root"], _identity(result["parser_root"])))
        guard()
        require(producer.verify_parser(
            result["parser_record"], result["parser_root"],
            result["parser_library"]) == result["parser_record_sha256"])
        return result
    except BaseException as error:
        if admitted:
            _private_failure(build_parent, phase, error)
        for path, identity in reversed(owned):
            try:
                archive._remove_owned_tree(path, identity)
            except Exception:
                pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
