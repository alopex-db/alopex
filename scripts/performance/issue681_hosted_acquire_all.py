"""Prepare exact public-build inputs under one selected-job storage guard."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import tempfile
import time

import issue681_hosted_acquire_source as source
import issue681_hosted_build_guard as capacity
import issue681_hosted_cargo_fetch as cargo_fetch
import issue681_hosted_nim_archive as nim_archive
import issue681_hosted_parser_build as parser_build
import issue681_hosted_parser_seed as parser_seed
import issue681_hosted_parser_tools as parser_tools
import issue681_hosted_vendor_consume as vendor_consume
import issue681_hosted_vendor_fetch as vendor_fetch
import issue681_hosted_storage as scoped_storage


ERROR = "PUBLIC_ACQUIRE"
TASK_DIRS = ("sources", "nim", "seed", "parser-build", "parser-stage",
             "vendor-zip")
VENDOR_MODES = ("release_bundle", "cargo_vendor_locked")
ALLOWED_DETAIL_CODES = frozenset({
    "PUBLIC_ACQUIRE",
    "PUBLIC_CAPACITY",
    "PUBLIC_PARSER_INPUT",
    "PUBLIC_VENDOR",
    "TIME_BUDGET",
}) | cargo_fetch.DIAG_CODES


def require(value):
    if not value:
        raise ValueError(ERROR)


def _force_guard(guard):
    if isinstance(guard, capacity.HostStorageGuard) and hasattr(guard, "min_interval_seconds"):
        guard(force=True)
    else:
        guard()


def _detail_code(exc, cargo_diag=None):
    if isinstance(cargo_diag, dict):
        code = cargo_diag.get("code")
        if isinstance(code, str) and code in ALLOWED_DETAIL_CODES:
            return code
    if isinstance(exc, ValueError) and exc.args:
        token = exc.args[0]
        if isinstance(token, str) and token in ALLOWED_DETAIL_CODES:
            return token
    return ERROR


def _owned_empty(path):
    require(isinstance(path, Path) and path.is_absolute() and
            path.is_dir() and not path.is_symlink() and
            path.resolve(strict=True) == path)
    info = path.stat()
    require(info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700 and
            not any(path.iterdir()))


def _identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)
    return info.st_dev, info.st_ino


def _remove_owned(path, identity):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and
            (info.st_dev, info.st_ino) == identity)
    shutil.rmtree(path)


def acquire_all(workspace: Path, task_parent: Path, home_parent: Path, *,
                manifest: scoped_storage.Manifest,
                vendor_mode: str = "release_bundle",
                on_substage=None,
                stage_budget_seconds: int | None = None):
    """Return one complete private input set or a fixed public failure."""
    owned = []
    started = time.monotonic()
    substage = "init"
    substage_started = started
    cargo_diag = {}

    def emit(name, status, detail=None):
        if on_substage is not None:
            elapsed_s = max(0, int(time.monotonic() - substage_started))
            on_substage(name, status, detail, elapsed_s)

    def begin(name):
        nonlocal substage, substage_started
        substage = name
        substage_started = time.monotonic()
        if stage_budget_seconds is not None:
            if (substage_started - started) >= stage_budget_seconds:
                raise ValueError("TIME_BUDGET")

    try:
        require(vendor_mode in VENDOR_MODES and
                isinstance(workspace, Path) and workspace.is_absolute() and
                workspace.is_dir() and not workspace.is_symlink() and
                workspace.resolve(strict=True) == workspace and
                (stage_budget_seconds is None or
                 (type(stage_budget_seconds) is int and stage_budget_seconds > 0)))
        _owned_empty(task_parent)
        _owned_empty(home_parent)
        require(Path.home() in home_parent.parents and
                type(manifest) is scoped_storage.Manifest and
                manifest.workspace == workspace and
                task_parent.is_relative_to(manifest.task_parent) and
                home_parent.is_relative_to(manifest.home_parent) and
                all(left != right and left not in right.parents and
                    right not in left.parents
                    for index, left in enumerate((workspace, task_parent,
                                                  home_parent))
                    for right in (workspace, task_parent, home_parent)[index + 1:]))

        task = Path(tempfile.mkdtemp(prefix="issue681-input-", dir=task_parent))
        owned.append((task, _identity(task)))
        cargo_home = Path(tempfile.mkdtemp(prefix="issue681-cargo-",
                                           dir=home_parent))
        owned.append((cargo_home, _identity(cargo_home)))
        dirs = {}
        for name in TASK_DIRS:
            path = task / name
            path.mkdir(mode=0o700)
            _owned_empty(path)
            dirs[name] = path

        begin("sources")
        acquired = source.acquire_sources(workspace, dirs["sources"],
                                          manifest=manifest)
        guard = acquired["guard"]
        require(type(guard) is capacity.HostStorageGuard)
        _force_guard(guard)
        emit("sources", "passed")

        begin("nim_fetch")
        nim_archive.fetch_archive(dirs["nim"], guard)
        _force_guard(guard)
        emit("nim_fetch", "passed")

        begin("nim_extract")
        extracted = nim_archive.extract_archive(dirs["nim"], guard)
        emit("nim_extract", "passed")

        begin("parser_tools")
        tools = parser_tools.verify_tools(extracted, guard)
        require(type(tools) is dict and set(tools) == {"nim", "nimble"})
        _force_guard(guard)
        emit("parser_tools", "passed")

        begin("parser_seed")
        seed = parser_seed.acquire_seed(dirs["seed"], guard,
                                        acquired["git_env"])
        _force_guard(guard)
        emit("parser_seed", "passed")

        begin("parser_build")
        parser = parser_build.build_parser(
            acquired["sources"]["candidate"], tools["nim"],
            tools["nimble"], seed, dirs["parser-build"],
            dirs["parser-stage"], guard, acquired["git_binary"],
            acquired["git_env"])
        _force_guard(guard)
        emit("parser_build", "passed")

        if vendor_mode == "cargo_vendor_locked":
            begin("cargo_vendor")
            cargo_diag.clear()
            remaining_timeout = cargo_fetch.TIME_LIMIT
            if stage_budget_seconds is not None:
                remaining = int(stage_budget_seconds - (time.monotonic() - started))
                if remaining < 1:
                    raise ValueError("TIME_BUDGET")
                remaining_timeout = min(remaining_timeout, remaining)
            vendor = cargo_fetch.fetch_and_stage_vendor(
                acquired["sources"], cargo_home, dirs["vendor-zip"], guard,
                timeout=remaining_timeout, diag=cargo_diag)
            _force_guard(guard)
            emit("cargo_vendor", "passed")
        else:
            begin("vendor_fetch")
            archive = vendor_fetch.fetch_vendor(dirs["vendor-zip"], guard)
            _force_guard(guard)
            emit("vendor_fetch", "passed")

            begin("vendor_install")
            vendor = vendor_consume.install_vendor(
                archive, acquired["sources"], cargo_home, guard)
            _force_guard(guard)
            emit("vendor_install", "passed")

        return {"guard": guard, "sources": acquired["sources"],
                "parser": parser, "vendor": vendor,
                "git_binary": acquired["git_binary"],
                "git_env": acquired["git_env"], "task": task,
                "cargo_home": cargo_home}
    except BaseException as exc:
        emit(substage, "failed", _detail_code(exc, cargo_diag))
        for path, identity in reversed(owned):
            try:
                _remove_owned(path, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None
