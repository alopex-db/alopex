"""Private 12-cell diagnostic boundary; callable only with a qualified cell runner.

The frozen #804 candidate and native supervisor currently do not match 1951's
diagnostic worker. This module never substitutes a receipt or changes policy.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import hashlib

from evidence import seal_cms
from orchestrate import FILES, DiagnosticFailure, orchestrate, require, sha256

MAX_RECEIPT = 16_384
MAX_CMS = 8_100_000
INPUT_BOUNDS = {"receipt": MAX_RECEIPT, "assets": 65_536,
                "server": 200_000_000, "wheel": 100_000_000}


def bounded_sha256(path, limit):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and
            0 < path.stat().st_size <= limit, "INPUT_BOUND")
    hashed = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1_048_576):
            hashed.update(block)
    require(path.stat().st_size <= limit, "INPUT_BOUND")
    return hashed.hexdigest()


def private_json(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and
            path.stat().st_mode & 0o077 == 0 and path.stat().st_size <= MAX_RECEIPT,
            "QUALIFICATION")
    return json.loads(path.read_text())


def verify_inputs(pin, *, receipt_path, assets_manifest, server, wheel,
                  certificate, expected_receipt_sha256):
    """Read fixed files only; no asset identity is inferred from a filename."""
    for path, expected, code, limit in ((receipt_path, expected_receipt_sha256, "QUALIFICATION", INPUT_BOUNDS["receipt"]),
                                        (assets_manifest, pin.get("assets_digest"), "MISSING_ASSET", INPUT_BOUNDS["assets"]),
                                        (server, pin.get("server_sha256"), "MISSING_ASSET", INPUT_BOUNDS["server"]),
                                        (wheel, pin.get("wheel_sha256"), "MISSING_ASSET", INPUT_BOUNDS["wheel"])):
        path = Path(path)
        require(type(expected) is str and len(expected) == 64, code)
        require(bounded_sha256(path, limit) == expected, code)
    cert = Path(certificate)
    require(cert.is_file() and not cert.is_symlink() and
            0 < cert.stat().st_size <= 65_536, "ENCRYPTION")
    return private_json(receipt_path)


def run_prepared(pin, *, job_id, operator_dir, work, export, receipt_path,
                 expected_receipt_sha256, assets_manifest, server, wheel,
                 certificate, used_bytes, run_pair, cleanup,
                 job_limit_seconds):
    """Wire the existing orchestrator to explicit, independently verified hooks.

    `run_pair` is the private adaptation of the frozen supervisor. Its source
    identity and 300/420-second limits must be verified before a live call.
    """
    work, export = Path(work), Path(export)
    require(work.is_absolute() and export.is_absolute() and
            not work.exists() and not export.exists() and
            work.parent == export.parent and work.parent.is_dir() and
            not work.parent.is_symlink() and work.parent.stat().st_mode & 0o077 == 0,
            "EVIDENCE_STATE")
    old = os.umask(0o077)
    try:
        work.mkdir(mode=0o700)
        evidence = work / "plain"
        evidence.mkdir(mode=0o700)
        sealed_dir = work / "sealed"
        sealed_dir.mkdir(mode=0o700)
        receipt = None

        def qualify(_pin, _job):
            nonlocal receipt
            receipt = verify_inputs(pin, receipt_path=receipt_path,
                                    assets_manifest=assets_manifest, server=server, wheel=wheel,
                                    certificate=certificate,
                                    expected_receipt_sha256=expected_receipt_sha256)
            return receipt

        hooks = {"qualify": qualify, "used_bytes": used_bytes,
                 "run_pair": run_pair, "cleanup": cleanup,
                 "seal": lambda root, hashes: seal_cms(
                     root, hashes, ciphertext=sealed_dir / "evidence.cms",
                     certificate=certificate)}
        result = orchestrate(pin, job_id=job_id, operator_dir=operator_dir,
                             evidence_dir=evidence, hooks=hooks,
                             job_limit_seconds=job_limit_seconds)
        cms = sealed_dir / "evidence.cms"
        require(cms.is_file() and not cms.is_symlink() and
                cms.stat().st_mode & 0o077 == 0 and
                0 < cms.stat().st_size <= MAX_CMS and
                sha256(cms) == result["ciphertext_sha256"] and
                cms.stat().st_size == result["ciphertext_size"] and
                {p.name for p in evidence.iterdir()} == set(FILES), "ENCRYPTION")
        # Export is an exact one-file ciphertext allowlist. Never expose work.
        export.mkdir(mode=0o700)
        shutil.copyfile(cms, export / "evidence.cms")
        (export / "evidence.cms").chmod(0o600)
        require(sha256(export / "evidence.cms") == result["ciphertext_sha256"] and
                {p.name for p in export.iterdir()} == {"evidence.cms"}, "ENCRYPTION")
        for name in FILES:
            (evidence / name).unlink()
        evidence.rmdir()
        cms.unlink()
        sealed_dir.rmdir()
        work.rmdir()  # Refuses unexpected owned contents.
        return result
    finally:
        os.umask(old)
