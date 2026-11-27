"""Build and reopen two exact public #681 assets in one selected hosted job."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import tempfile
import time

import issue681_hosted_build_guard as capacity
import issue681_public_assets as assets
import issue681_public_assets_produce as producer
import issue681_public_pair as pair

ARM_KEYS = {"source", "target", "work", "parser_record",
            "parser_root", "parser_library", "source_commit", "source_tree",
            "example_sha256", "cargo_lock_sha256", "parser_record_sha256",
            "parser_sha256", "rust_version", "additional_storage_bytes",
            "private_roots", "build_lock", "git_binary", "git_env",
            "cargo_home", "vendor_mode_sha256"}
OPTIONAL_COMMON_KEYS = {"vendor_tree_sha256"}
COMMON = {"target", "parser_record", "parser_root", "parser_library",
          "example_sha256", "parser_record_sha256", "parser_sha256",
          "rust_version", "additional_storage_bytes", "private_roots",
          "build_lock", "git_binary", "git_env", "cargo_home",
          "vendor_mode_sha256"}


def require(value):
    if not value:
        raise ValueError("PUBLIC_BUILD")


def build_pair(inputs, output_parent, guard, *, on_substage=None, diag=None):
    """Return private staged bundles only after both serial arms pass."""
    staging = None
    substage = "admit"
    substage_started = time.monotonic()
    arm_diag = diag if isinstance(diag, dict) else ({} if on_substage is not None else None)

    def emit(name, status, detail=None):
        if on_substage is not None:
            elapsed_s = max(0, int(time.monotonic() - substage_started))
            on_substage(name, status, detail, elapsed_s)

    try:
        if isinstance(arm_diag, dict):
            arm_diag["code"] = "BUILD_ADMIT"
        require(type(inputs) is dict and set(inputs) == {"before", "candidate"})
        before, candidate = inputs["before"], inputs["candidate"]
        require(all(type(arm) is dict and ARM_KEYS <= set(arm) <=
                    (ARM_KEYS | OPTIONAL_COMMON_KEYS)
                    for arm in (before, candidate)) and
                set(before) == set(candidate))
        common_keys = COMMON | (set(before) & OPTIONAL_COMMON_KEYS)
        require(all(before[key] == candidate[key] for key in common_keys) and
                all(before[key] != candidate[key]
                    for key in ("source", "work", "source_commit")) and
                all(isinstance(arm["source"], Path) and
                    isinstance(arm["work"], Path)
                    for arm in (before, candidate)))
        require(isinstance(output_parent, Path) and output_parent.is_absolute() and
                output_parent.is_dir() and not output_parent.is_symlink() and
                output_parent.resolve(strict=True) == output_parent and
                output_parent.stat().st_uid == os.geteuid() and
                stat.S_IMODE(output_parent.stat().st_mode) == 0o700)
        require(type(guard) is capacity.HostStorageGuard)
        roots = [before[key] for key in ("source", "work")]
        roots += [candidate[key] for key in ("source", "work")]
        roots += [before["target"], before["parser_root"],
                  before["cargo_home"], output_parent]
        require(all(isinstance(root, Path) and root.is_absolute() and
                    root.resolve(strict=False) == root for root in roots) and
                all(left != right and left not in right.parents and
                    right not in left.parents
                    for index, left in enumerate(roots)
                    for right in roots[index + 1:]))
        guard()
        staging = Path(tempfile.mkdtemp(prefix="issue681-public-", dir=output_parent))
        require(staging.stat().st_uid == os.geteuid() and
                stat.S_IMODE(staging.stat().st_mode) == 0o700)
        pins = {}
        for name in ("before", "candidate"):
            substage = name
            substage_started = time.monotonic()
            arm = inputs[name]
            guard()
            produce_kwargs = dict(arm, output=staging / name, host_guard=guard)
            if arm_diag is not None:
                arm_diag.clear()
                produce_kwargs["diag"] = arm_diag
            pin = producer.produce(**produce_kwargs)
            guard()
            verify_kwargs = {"private_roots": arm["private_roots"]}
            if arm_diag is not None:
                arm_diag["code"] = "BUILD_ASSETS_VERIFY"
                verify_kwargs["diag"] = arm_diag
            assets.verify(staging / name, pin, **verify_kwargs)
            pins[name] = pin
            emit(name, "passed")
        substage = "pair"
        substage_started = time.monotonic()
        if isinstance(arm_diag, dict):
            arm_diag["code"] = "BUILD_PAIR_VERIFY"
        pair._pins(pins)
        guard()
        if isinstance(arm_diag, dict):
            arm_diag.pop("code", None)
        emit("pair", "passed")
        return {"pins": pins, "staging": staging}
    except BaseException:
        detail = None
        if isinstance(arm_diag, dict):
            code = arm_diag.get("code")
            if isinstance(code, str) and code in producer.BUILD_DIAG_CODES:
                detail = code
        emit(substage, "failed", detail or "PUBLIC_BUILD")
        if staging is not None:
            try:
                shutil.rmtree(staging)
            except Exception:
                pass
        raise ValueError("PUBLIC_BUILD") from None
