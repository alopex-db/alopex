"""Exact private evidence CMS allowlist; plaintext survives any seal failure."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tarfile

from orchestrate import FILES, require, sha256


def seal_cms(root, hashes, *, ciphertext, certificate):
    root, ciphertext, certificate = Path(root), Path(ciphertext), Path(certificate)
    require(root.is_dir() and not root.is_symlink() and
            {path.name for path in root.iterdir()} == set(FILES) and
            set(hashes) == set(FILES), "EVIDENCE_ALLOWLIST")
    total = 0
    for name in FILES:
        path = root / name
        require(path.is_file() and not path.is_symlink() and
                path.stat().st_size == hashes[name]["size"] and
                path.stat().st_size <= FILES[name] and sha256(path) == hashes[name]["sha256"],
                "EVIDENCE_IDENTITY")
        total += path.stat().st_size
    require(total < 8_000_000 and certificate.is_file() and
            not ciphertext.exists(), "EVIDENCE_BOUND")
    archive = root.parent / "private-evidence.tar.gz"
    require(not archive.exists(), "EVIDENCE_STATE")
    previous = os.umask(0o077)
    try:
        with tarfile.open(archive, "x:gz") as bundle:
            for name in FILES:
                bundle.add(root / name, arcname=f"diagnostic/{name}", recursive=False)
        require(archive.stat().st_size <= 8_000_000, "EVIDENCE_BOUND")
        subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
                        "-outform", "DER", "-in", str(archive), "-out", str(ciphertext),
                        str(certificate)], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=True, timeout=120)
        require(ciphertext.is_file() and not ciphertext.is_symlink() and
                0 < ciphertext.stat().st_size <= 8_100_000, "ENCRYPTION")
        ciphertext.chmod(0o600)
        result = {"sha256": sha256(ciphertext), "size": ciphertext.stat().st_size,
                  "verified": True}
        require(len(result["sha256"]) == 64, "ENCRYPTION")
        archive.unlink()  # Only after ciphertext digest and size verification.
        return result
    finally:
        os.umask(previous)
