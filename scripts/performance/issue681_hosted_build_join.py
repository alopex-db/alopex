"""Bind authenticated selected-job inputs to two serial public asset builds."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import stat
import tempfile

import issue681_hosted_acquire_all as acquire_all
import issue681_hosted_acquire_source as acquire_source
import issue681_hosted_build_guard as capacity
import issue681_hosted_build_pair as build_pair
import issue681_hosted_vendor_consume as vendor_consume
import issue681_hosted_vendor_tree as vendor_tree


ERROR = "PUBLIC_BUILD"
RUST_VERSION = "1.90.0"
VENDOR_TREE_SHA256 = vendor_tree.TREE_SHA256
ACQUIRED_KEYS = {"guard", "sources", "parser", "vendor", "git_binary",
                 "git_env", "task", "cargo_home"}
PARSER_KEYS = {"parser_record", "parser_root", "parser_library",
               "parser_record_sha256", "parser_sha256"}
VENDOR_KEYS = {"cargo_home", "vendor_root", "bundle_sha256",
               "vendor_tree_sha256", "vendor_mode_sha256"}
LOCKED_VENDOR_KEYS = {"cargo_home", "vendor_root", "vendor_tree_sha256",
                      "vendor_mode_sha256", "acquisition_mode"}


def require(value):
    if not value:
        raise ValueError(ERROR)


def _private_dir(path):
    require(isinstance(path, Path) and path.is_absolute() and
            path.is_dir() and not path.is_symlink() and
            path.resolve(strict=True) == path)
    info = path.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700)


def _disjoint(paths):
    return all(left != right and left not in right.parents and
               right not in left.parents
               for index, left in enumerate(paths)
               for right in paths[index + 1:])


def _admit(acquired, workspace, build_parent):
    require(type(acquired) is dict and set(acquired) == ACQUIRED_KEYS and
            type(acquired["guard"]) is capacity.HostStorageGuard and
            type(acquired["sources"]) is dict and
            set(acquired["sources"]) == {"before", "candidate"} and
            type(acquired["parser"]) is dict and
            set(acquired["parser"]) == PARSER_KEYS and
            type(acquired["vendor"]) is dict and
            set(acquired["vendor"]) in (VENDOR_KEYS, LOCKED_VENDOR_KEYS))
    task, cargo_home = acquired["task"], acquired["cargo_home"]
    for path in (task, cargo_home, build_parent):
        _private_dir(path)
    vendor = acquired["vendor"]
    if set(vendor) == LOCKED_VENDOR_KEYS:
        vendor_ok = (
            vendor["acquisition_mode"] == "cargo_vendor_locked" and
            isinstance(vendor["vendor_tree_sha256"], str) and
            re.fullmatch(r"[0-9a-f]{64}", vendor["vendor_tree_sha256"])
        )
    else:
        vendor_ok = (
            vendor["bundle_sha256"] == vendor_consume.ZIP_SHA256 and
            vendor["vendor_tree_sha256"] == VENDOR_TREE_SHA256
        )
    require(isinstance(workspace, Path) and workspace.is_absolute() and
            workspace.is_dir() and not workspace.is_symlink() and
            workspace.resolve(strict=True) == workspace and
            _disjoint((workspace, task, cargo_home, build_parent)) and
            not any(build_parent.iterdir()) and
            acquired["sources"] == {
                name: task / "sources" / name
                for name in ("before", "candidate")} and
            acquired["parser"]["parser_root"] ==
            task / "parser-stage" / "parser-input" and
            acquired["parser"]["parser_record"].parent ==
            task / "parser-build" / "archive" and
            vendor["cargo_home"] == cargo_home and
            vendor["vendor_root"] == cargo_home / "vendor" and
            vendor_ok and
            isinstance(vendor["vendor_mode_sha256"], str) and
            re.fullmatch(r"[0-9a-f]{64}",
                         vendor["vendor_mode_sha256"]) and
            acquired["git_binary"] == acquire_source.GIT and
            type(acquired["git_env"]) is dict)
    for path in (*acquired["sources"].values(),
                 acquired["parser"]["parser_root"], cargo_home / "vendor"):
        _private_dir(path)
    require(isinstance(acquired["parser"]["parser_library"], Path) and
            acquired["parser"]["parser_library"].is_relative_to(
                acquired["parser"]["parser_root"]))


def build(acquired, workspace: Path, build_parent: Path, *,
          on_substage=None, diag=None):
    """Return only an independently verified private pair on success."""
    owned = None
    pair_started = False
    try:
        if isinstance(diag, dict):
            diag["code"] = "BUILD_ADMIT"
        _admit(acquired, workspace, build_parent)
        guard = acquired["guard"]
        guard()
        owned = Path(tempfile.mkdtemp(prefix="issue681-build-", dir=build_parent))
        identity = acquire_all._identity(owned)
        for name in ("target", "before-work", "candidate-work", "bundles"):
            (owned / name).mkdir(mode=0o700)
            _private_dir(owned / name)
        parser = acquired["parser"]
        common = {
            "target": owned / "target", "cargo_home": acquired["cargo_home"],
            "vendor_mode_sha256": acquired["vendor"]["vendor_mode_sha256"],
            "parser_record": parser["parser_record"],
            "parser_root": parser["parser_root"],
            "parser_library": parser["parser_library"],
            "parser_record_sha256": parser["parser_record_sha256"],
            "parser_sha256": parser["parser_sha256"],
            "example_sha256": acquire_source.PINS["candidate"]["example"],
            "rust_version": RUST_VERSION,
            "additional_storage_bytes": capacity.qualify.BUILD_BYTES,
            "private_roots": (str(acquired["task"]),
                              str(acquired["cargo_home"]), str(owned)),
            "build_lock": owned / "build.lock",
            "git_binary": acquired["git_binary"],
            "git_env": acquired["git_env"],
        }
        if acquired["vendor"].get("acquisition_mode") == "cargo_vendor_locked":
            common["vendor_tree_sha256"] = acquired["vendor"]["vendor_tree_sha256"]
        require(acquire_source.PINS["before"]["example"] ==
                common["example_sha256"])
        arms = {}
        for name in ("before", "candidate"):
            pin = acquire_source.PINS[name]
            arms[name] = dict(
                common, source=acquired["sources"][name],
                work=owned / (name + "-work"), source_commit=pin["commit"],
                source_tree=pin["tree"], cargo_lock_sha256=pin["lock"])
        guard()
        pair_kwargs = {}
        if on_substage is not None:
            pair_kwargs["on_substage"] = on_substage
        if diag is not None:
            pair_kwargs["diag"] = diag
        pair_started = True
        result = build_pair.build_pair(arms, owned / "bundles", guard,
                                       **pair_kwargs)
        guard()
        return {"pair": result, "build_task": owned, "guard": guard,
                "cargo_home": acquired["cargo_home"]}
    except BaseException:
        if not pair_started and on_substage is not None:
            on_substage("admit", "failed", "BUILD_ADMIT", 0)
        if owned is not None:
            try:
                acquire_all._remove_owned(owned, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None
