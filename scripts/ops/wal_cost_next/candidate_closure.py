"""Unapproved diagnostic of one signed Ubuntu package closure.

This returns a distinct candidate schema. It never changes production proof
pins or calls `make_lock`, and it cannot emit a formal builder lock.
"""
from __future__ import annotations

import re

from resolve_builder_lock import (LockError, MAX_DOWNLOAD, MAX_INSTALLED,
                                  capture_precheck, resolve_packages)

SCHEMA = "alopex.trace-builder-closure-candidate/v1"


def candidate_closure(base: dict, authentication: dict, proof_sha256: str,
                      indexes: dict[str, bytes], apt_uris: str,
                      resolver_sha256: str, compare) -> dict:
    """Bind an unapproved result to exact proof, indexes, URIs and base dpkg."""
    if not callable(compare) or re.fullmatch(r"[0-9a-f]{64}", proof_sha256) is None:
        raise LockError("candidate comparator/proof")
    base_versions, actual_proof = capture_precheck(base, authentication,
                                                   resolver_sha256)
    if proof_sha256 != actual_proof:
        raise LockError("candidate proof digest")
    files, installed = resolve_packages(base, authentication, indexes, apt_uris,
                                        base_versions, None, compare)
    proofs = authentication["indexes"]
    return {"schema": SCHEMA, "approval": "unapproved", "formal_lock": False,
            "proof_sha256": proof_sha256,
            "resolver_sha256": resolver_sha256,
            "base_inventory_sha256": authentication["base_inventory_sha256"],
            "signer_fingerprints": sorted({p["signer_fingerprint"] for p in proofs}),
            "signed_indexes": sorted(({"name": p["name"],
                                        "packages_sha256": p["packages_sha256"],
                                        "inrelease_sha256": p["inrelease_sha256"]}
                                       for p in proofs), key=lambda p: p["name"]),
            "packages": sorted(files, key=lambda item: item["name"]),
            "budgets": {"download_bytes": sum(item["size"] for item in files),
                        "installed_size_declared_bytes": installed,
                        "download_cap_bytes": MAX_DOWNLOAD,
                        "installed_size_cap_bytes": MAX_INSTALLED}}
