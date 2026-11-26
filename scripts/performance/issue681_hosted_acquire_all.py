"""Prepare exact public-build inputs under one selected-job storage guard."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import tempfile

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


def require(value):
    if not value:
        raise ValueError(ERROR)


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
                vendor_mode: str = "release_bundle"):
    """Return one complete private input set or a fixed public failure."""
    owned = []
    try:
        require(vendor_mode in VENDOR_MODES and
                isinstance(workspace, Path) and workspace.is_absolute() and
                workspace.is_dir() and not workspace.is_symlink() and
                workspace.resolve(strict=True) == workspace)
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

        acquired = source.acquire_sources(workspace, dirs["sources"],
                                          manifest=manifest)
        guard = acquired["guard"]
        require(type(guard) is capacity.HostStorageGuard)
        guard()
        nim_archive.fetch_archive(dirs["nim"], guard)
        guard()
        extracted = nim_archive.extract_archive(dirs["nim"], guard)
        tools = parser_tools.verify_tools(extracted, guard)
        require(type(tools) is dict and set(tools) == {"nim", "nimble"})
        guard()
        seed = parser_seed.acquire_seed(dirs["seed"], guard,
                                        acquired["git_env"])
        guard()
        parser = parser_build.build_parser(
            acquired["sources"]["candidate"], tools["nim"],
            tools["nimble"], seed, dirs["parser-build"],
            dirs["parser-stage"], guard, acquired["git_binary"],
            acquired["git_env"])
        guard()
        if vendor_mode == "cargo_vendor_locked":
            vendor = cargo_fetch.fetch_and_stage_vendor(
                acquired["sources"], cargo_home, dirs["vendor-zip"], guard)
            guard()
        else:
            archive = vendor_fetch.fetch_vendor(dirs["vendor-zip"], guard)
            guard()
            vendor = vendor_consume.install_vendor(
                archive, acquired["sources"], cargo_home, guard)
            guard()
        return {"guard": guard, "sources": acquired["sources"],
                "parser": parser, "vendor": vendor,
                "git_binary": acquired["git_binary"],
                "git_env": acquired["git_env"], "task": task,
                "cargo_home": cargo_home}
    except BaseException:
        for path, identity in reversed(owned):
            try:
                _remove_owned(path, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None
