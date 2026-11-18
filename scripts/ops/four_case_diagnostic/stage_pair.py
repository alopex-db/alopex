"""Fail-closed staging boundary for a qualified #712 pair.

The #804 image uses a private container network and has no host port binding.
The current pair supervisor needs a host-visible process and loopback endpoint.
Consequently this module verifies the same-job binding but cannot launch a
pair until an owned, source-pinned in-container worker contract is supplied.
"""
from __future__ import annotations

from pathlib import Path
import os
import stat

from . import supervisor


def validate_stage_binding(config, output, proof, receipt):
    """Reject stale or malformed stage inputs before any runtime side effect."""
    if type(config) is not dict or type(proof) is not dict or type(receipt) is not dict:
        raise ValueError("PAIR_STAGE")
    source = config.get("source")
    assets = config.get("assets_digest")
    job_id = config.get("same_job_id")
    try:
        supervisor.validate_config(config, source, assets, job_id)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("PAIR_STAGE") from error
    profile = proof.get("profile")
    product = proof.get("product")
    accepted = proof.get("accepted_public")
    security = profile.get("security") if type(profile) is dict else None
    if (type(profile) is not dict or type(product) is not dict or
            type(accepted) is not dict or type(security) is not dict or
            proof.get("job_id") != job_id or receipt.get("job_id") != job_id or
            receipt.get("qualification_status") != "passed" or
            proof.get("temp_snapshot_verified") is not True or
            proof.get("image_digest") != receipt.get("image_digest") or
            proof.get("image_digest") != accepted.get("image_digest") or
            profile.get("run_id") != "hosted-804-" + job_id or
            profile.get("assets_digest") != assets or
            product.get("assets_digest") != assets or
            receipt.get("assets_digest") != assets or
            product.get("source_commit") != source["commit"] or
            product.get("source_tree") != source["tree"] or
            receipt.get("source_commit") != source["commit"] or
            receipt.get("source_tree") != source["tree"] or
            security.get("lsm_kind") != "apparmor" or
            security.get("lsm_profile") != "docker-default" or
            security.get("seccomp_sha256") !=
            receipt.get("security_profile_digest") or
            profile.get("storage") != {"stop_bytes": 40_000_000_000,
                                       "reserve_bytes": 1_000_000_000,
                                       "poll_seconds": 10} or
            proof.get("runs") != {
                name: {"phase": "deleted", "collection_verified": True,
                       "cleanup_errors": []}
                for name in ("primary", "repeat", "probe")}):
        raise ValueError("PAIR_STAGE")
    try:
        path = Path(output)
        info = path.lstat()
        cells = [(path / name).lstat() for name in ("on", "off")]
    except (OSError, TypeError) as error:
        raise ValueError("PAIR_STAGE") from error
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or
            stat.S_IMODE(info.st_mode) != 0o700 or
            not all(stat.S_ISDIR(cell.st_mode) and
                    cell.st_uid == os.geteuid() and
                    stat.S_IMODE(cell.st_mode) == 0o700 for cell in cells)):
        raise ValueError("PAIR_STAGE")


def stage_pair(config, output, proof, receipt):
    """Fail closed while the qualified image lacks a compatible owned launcher."""
    validate_stage_binding(config, output, proof, receipt)
    raise ValueError("PAIR_STAGE")
