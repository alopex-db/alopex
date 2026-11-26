"""Construct the exact offline Nimble seed from pinned public Git sources."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys

import issue681_hosted_acquire_source as source


PINS = {
    "npeg": {
        "url": "https://github.com/zevv/npeg.git",
        "commit": "409f6796d0e880b3f0222c964d1da7de6e450811",
        "tree": "242bbcca7186f4660de36dc769dc1cf77081ac72",
        "version": "1.3.0",
        "source_files": 15,
        "max_bytes": 100_000_000,
    },
    "msgpack4nim": {
        "url": "https://github.com/jangko/msgpack4nim.git",
        "commit": "f4cc097ca9694f17feced9f82994f583ef7911fe",
        "tree": "da84138fe9e0d858ee8eac8ee74daa0b35ca15b4",
        "version": "0.4.4",
        "source_files": 4,
        "max_bytes": 20_000_000,
    },
    "registry": {
        "url": "https://github.com/nim-lang/packages.git",
        "commit": "b79eaaa3fc65fc473bc9e803445f8f7aef7112a2",
        "tree": "bd71233260120e2f3d3cf7d70512eeab86dd7818",
        "max_bytes": 50_000_000,
    },
}
REGISTRY_BYTES = 977_981
REGISTRY_SHA256 = "e42c394337184b2fe52f65538ea3dc93fb8840efb77ec775261ebd1f7169d329"
PACKAGE_FILES = ("packages_official.json", "packages_temp.json")


def require(value):
    if not value:
        raise ValueError("PUBLIC_PARSER_INPUT")


def _owned_empty(parent):
    require(isinstance(parent, Path) and parent.is_absolute() and
            parent.is_dir() and not parent.is_symlink())
    info = parent.stat()
    require(info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700 and
            not any(parent.iterdir()))


def _directory_identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid())
    return info.st_dev, info.st_ino


def _remove_owned(path, identity):
    if not path.exists() and not path.is_symlink():
        return
    require(_directory_identity(path) == identity)
    shutil.rmtree(path)


def _sealed_git_env(env):
    require(type(env) is dict and type(env.get("HOME")) is str)
    home = Path(env["HOME"])
    require(home.is_absolute() and home.is_dir() and not home.is_symlink() and
            home.stat().st_uid == os.geteuid() and
            stat.S_IMODE(home.stat().st_mode) == 0o700 and
            env == source._git_env(home))


def _git_identity(checkout, env, guard):
    values = []
    for name in ("HEAD", "HEAD^{tree}"):
        guard()
        result = subprocess.run(
            [str(source.GIT), "-c", "credential.helper=", "-c",
             "core.hooksPath=/dev/null", "rev-parse", name],
            cwd=checkout, env=env, stdin=subprocess.DEVNULL,
            capture_output=True, timeout=15)
        require(result.returncode == 0 and
                re.fullmatch(rb"[0-9a-f]{40}\n", result.stdout))
        values.append(result.stdout.decode().strip())
    guard()
    return tuple(values)


def _checkout(checkout, pin, env, guard, log):
    checkout.mkdir(mode=0o700)
    source._git(["init", "-q"], checkout, guard, log, env)
    source._git(["remote", "add", "origin", pin["url"]], checkout,
                guard, log, env)
    source._git(["fetch", "-q", "--no-tags", "--depth=1", "origin",
                 pin["commit"]], checkout, guard, log, env)
    source._git(["checkout", "-q", "--detach", "FETCH_HEAD"], checkout,
                guard, log, env)
    require(_git_identity(checkout, env, guard) ==
            (pin["commit"], pin["tree"]))
    used = 0
    for path in checkout.rglob("*"):
        info = path.lstat()
        require(info.st_uid == os.geteuid() and
                (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)))
        used += info.st_blocks * 512
        require(used <= pin["max_bytes"])
    guard()


def _write_owned(path, data):
    require(type(data) is bytes and not path.exists() and not path.is_symlink())
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)


def _copy_package(checkout, seed, name, pin):
    manifest = checkout / (name + ".nimble")
    src = checkout / "src"
    require(manifest.is_file() and not manifest.is_symlink() and
            src.is_dir() and not src.is_symlink())
    selected = [manifest]
    for path in src.rglob("*"):
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
        if stat.S_ISREG(info.st_mode) and path.suffix == ".nim":
            selected.append(path)
    require(len(selected) == pin["source_files"] + 1)
    package = seed / "pkgs2" / (name + "-" + pin["version"] + "-" +
                               pin["commit"][:12])
    package.mkdir(mode=0o700)
    members = []
    for path in sorted(selected, key=lambda item: item.relative_to(checkout).as_posix()):
        relative = path.relative_to(checkout)
        data = path.read_bytes()
        require(len(data) <= 5_000_000)
        destination = package / relative
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write_owned(destination, data)
        members.append("/" + relative.as_posix())
    metadata = {"version": 1, "metaData": {
        "binaries": [], "downloadMethod": "git", "files": members,
        "specialVersions": [pin["version"]], "url": "",
        "vcsRevision": pin["commit"],
    }}
    _write_owned(package / "nimblemeta.json",
                 (json.dumps(metadata, sort_keys=True,
                             separators=(",", ":")) + "\n").encode())
    return package


def _copy_registry(checkout, seed):
    path = checkout / "packages.json"
    require(path.is_file() and not path.is_symlink())
    data = path.read_bytes()
    require(len(data) == REGISTRY_BYTES and
            hashlib.sha256(data).hexdigest() == REGISTRY_SHA256)
    for name in PACKAGE_FILES:
        _write_owned(seed / name, data)


def _verify_seed(seed, log, guard):
    arguments = [sys.executable,
                 str(Path(__file__).resolve().parents[1] /
                     "release/parser_asset_manifest.py"),
                 "verify-inputs"]
    for name in ("npeg", "msgpack4nim"):
        pin = PINS[name]
        package = seed / "pkgs2" / (name + "-" + pin["version"] + "-" +
                                   pin["commit"][:12])
        arguments += ["--package", name + "=" + pin["version"] + "=" +
                      str(package)]
    for name in PACKAGE_FILES:
        arguments += ["--registry-metadata", name + "=" + str(seed / name)]
    guard()
    result = subprocess.run(arguments, stdin=subprocess.DEVNULL,
                            stdout=log, stderr=subprocess.STDOUT, timeout=30)
    require(result.returncode == 0)
    guard()


def acquire_seed(parent, guard, git_env):
    """Return an authenticated owner-only seed, or only PUBLIC_PARSER_INPUT."""
    owned_dirs = []
    owned_log = None
    checkouts = seed = None
    stage = "admission"
    try:
        _owned_empty(parent)
        require(callable(guard))
        _sealed_git_env(git_env)
        source._trusted_git()
        guard()
        log_path = parent / "seed.private.log"
        fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        log_info = os.fstat(fd)
        owned_log = (log_info.st_dev, log_info.st_ino)
        with os.fdopen(fd, "wb") as log:
            checkouts = parent / "checkouts"
            seed = parent / "seed"
            checkouts.mkdir(mode=0o700)
            owned_dirs.append((checkouts, _directory_identity(checkouts)))
            seed.mkdir(mode=0o700)
            owned_dirs.append((seed, _directory_identity(seed)))
            (seed / "pkgs2").mkdir(mode=0o700)
            stage = "git"
            for name in ("npeg", "msgpack4nim", "registry"):
                _checkout(checkouts / name, PINS[name], git_env, guard, log)
            stage = "assembly"
            for name in ("npeg", "msgpack4nim"):
                _copy_package(checkouts / name, seed, name, PINS[name])
                guard()
            _copy_registry(checkouts / "registry", seed)
            stage = "verification"
            _verify_seed(seed, log, guard)
        stage = "cleanup"
        _remove_owned(checkouts, owned_dirs[0][1])
        owned_dirs = [item for item in owned_dirs if item[0] != checkouts]
        checkouts = None
        guard()
        return seed
    except BaseException as error:
        try:
            path = parent / "seed.private.log"
            if owned_log is not None and path.is_file() and not path.is_symlink():
                fd = os.open(path, os.O_WRONLY | os.O_APPEND |
                             getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(fd, "ab") as log:
                    info = os.fstat(log.fileno())
                    if ((info.st_dev, info.st_ino) == owned_log and
                            info.st_uid == os.geteuid() and
                            stat.S_IMODE(info.st_mode) == 0o600):
                        category = type(error).__name__
                        if category not in {"ValueError", "OSError", "TimeoutExpired",
                                            "RuntimeError", "ParserAssetError"}:
                            category = "Other"
                        log.write((stage + ":" + category + "\n").encode())
        except Exception:
            pass
        for path, identity in reversed(owned_dirs):
            try:
                _remove_owned(path, identity)
            except Exception:
                pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
