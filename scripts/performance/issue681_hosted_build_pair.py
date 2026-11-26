"""Build and reopen two exact public #681 assets in one selected hosted job."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import tempfile

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


def build_pair(inputs, output_parent, guard):
    """Return private staged bundles only after both serial arms pass."""
    staging = None
    try:
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
            arm = inputs[name]
            guard()
            pin = producer.produce(**arm, output=staging / name,
                                   host_guard=guard)
            guard()
            assets.verify(staging / name, pin,
                          private_roots=arm["private_roots"])
            pins[name] = pin
        pair._pins(pins)
        guard()
        return {"pins": pins, "staging": staging}
    except BaseException:
        if staging is not None:
            try:
                shutil.rmtree(staging)
            except Exception:
                pass
        raise ValueError("PUBLIC_BUILD") from None
