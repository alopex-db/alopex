"""Parse one captured proof as an unapproved, exact-base dpkg diagnostic.

The caller owns and has already admitted the offline container. This module
does not construct it, approve a signer, or write a formal builder lock.
"""
from __future__ import annotations

import json
from pathlib import Path
import re

from candidate_closure import candidate_closure
from offline_closure_adapter import ExactBaseDpkg
from resolve_builder_lock import LockError, digest

HEX = re.compile(r"[0-9a-f]{64}\Z")


def _regular(path: Path) -> Path:
    if path.is_symlink() or not path.is_file():
        raise LockError("candidate input is not a regular file")
    return path


def captured_candidate_closure(proof_dir: Path, capture_dir: Path,
                               resolver_script: Path,
                               comparator: ExactBaseDpkg) -> dict:
    """Consume verifier output and raw capture with the selected base's dpkg.

    The verifier's indexes.json is a path map into the captured directory.
    Its authentication record is already signed-index evidence, but remains
    unapproved until independently reviewed and pinned by the formal path.
    """
    if type(comparator) is not ExactBaseDpkg:
        raise LockError("exact base dpkg comparator required")
    proof_dir, capture_dir = Path(proof_dir), Path(capture_dir)
    if (proof_dir.is_symlink() or not proof_dir.is_dir() or
            capture_dir.is_symlink() or not capture_dir.is_dir()):
        raise LockError("candidate input directory")
    base = json.loads(_regular(proof_dir / "base.json").read_text())
    authentication = json.loads(_regular(proof_dir / "authentication.json").read_text())
    paths = json.loads(_regular(proof_dir / "indexes.json").read_text())
    if not isinstance(paths, dict) or not paths or not all(
            isinstance(name, str) and isinstance(path, str) for name, path in paths.items()):
        raise LockError("candidate index map")
    capture_root = capture_dir.resolve()
    indexes = {}
    for name, raw_path in paths.items():
        path = Path(raw_path)
        if (not path.is_absolute() or path.is_symlink() or
                not path.resolve().is_relative_to(capture_root)):
            raise LockError("candidate index outside capture")
        indexes[name] = _regular(path).read_bytes()
    apt_uris = _regular(capture_dir / "apt-uris.txt").read_text()
    resolver_sha256 = digest(_regular(Path(resolver_script)).read_bytes())
    proof_sha256 = _regular(proof_dir / "proof.sha256").read_text().strip()
    if HEX.fullmatch(proof_sha256) is None:
        raise LockError("candidate proof digest")

    comparisons = 0

    def compare(actual: str, operator: str, required: str) -> bool:
        nonlocal comparisons
        result = comparator.compare(actual, operator, required)
        if type(result) is not bool:
            raise LockError("malformed exact base dpkg result")
        comparisons += 1
        return result

    result = candidate_closure(base, authentication, proof_sha256, indexes,
                               apt_uris, resolver_sha256, compare)
    result["exact_base_dpkg"] = {"image_id": comparator.identity,
                                 "comparisons": comparisons}
    return result
