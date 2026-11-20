"""Offline validation of a separately authenticated Ubuntu APT resolution.

This does not verify InRelease signatures or run APT. The acquisition step must
produce authentication.json from a verified base, signed indexes, and apt run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from pathlib import PurePosixPath
import re
import shlex
import subprocess
from urllib.parse import unquote, urlsplit

BASE = "ubuntu@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852"
PLATFORM = "linux/amd64"
HOSTS = {"archive.ubuntu.com", "security.ubuntu.com"}
TOP = ("gcc", "make", "gawk", "libc6-dev", "linux-libc-dev", "binutils")
MAX_DOWNLOAD = 450_000_000
MAX_INSTALLED = 1_300_000_000
MAX_DIAGNOSTIC = 2_500_000_000
HEX = re.compile(r"[0-9a-f]{64}\Z")
FINGERPRINT_V4 = re.compile(r"[0-9A-F]{40}\Z")
NAME = re.compile(r"[a-z0-9][a-z0-9+.-]*\Z")
DEP = re.compile(r"([a-z0-9][a-z0-9+.-]*)(?::(?:any|native))?(?:\s*\((<<|<=|=|>=|>>)\s*([^() ]+)\))?\Z")

# Set only after a separate authenticated InRelease/Packages verifier emits a
# reviewed canonical proof. Until then, real lock generation fails closed.
APPROVED_PROOF_SHA256: str | None = None
APPROVED_SIGNERS: frozenset[str] = frozenset()


class LockError(ValueError):
    pass


def need(ok: bool, reason: str) -> None:
    if not ok:
        raise LockError(reason)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_relative_path(value: str) -> bool:
    decoded = unquote(value)
    parts = PurePosixPath(decoded).parts
    return (bool(parts) and not decoded.startswith("/") and "\\" not in decoded and
            not re.search(r"%(?:2f|5c)", value, re.I) and
            all(part not in ("", ".", "..") for part in decoded.split("/")))


def paragraphs(raw: str) -> list[dict[str, str]]:
    rows, row, key = [], {}, None
    for line in [*raw.splitlines(), ""]:
        if not line:
            if row:
                rows.append(row)
            row, key = {}, None
        elif line[0].isspace():
            need(key is not None, "bad continuation")
            row[key] += " " + line.strip()
        else:
            need(":" in line, "bad control line")
            key, value = line.split(":", 1)
            need(key not in row, "duplicate control field")
            row[key] = value.strip()
    return rows


def version_satisfies(actual: str, op: str, required: str) -> bool:
    # The selected Linux base supplies dpkg's canonical Debian version ordering.
    try:
        result = subprocess.run(["dpkg", "--compare-versions", actual, op, required],
                                capture_output=True, timeout=3, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LockError("dpkg version comparator unavailable") from exc
    need(result.returncode in (0, 1), "dpkg comparison failed")
    return result.returncode == 0


def dependencies_satisfied(row: dict[str, str], versions: dict[str, str]) -> None:
    for field in ("Pre-Depends", "Depends"):
        if not row.get(field):
            continue
        for group in row[field].split(","):
            options = []
            for part in group.split("|"):
                match = DEP.fullmatch(part.strip())
                need(match is not None, "unsupported dependency expression")
                name, op, required = match.groups()
                actual = versions.get(name)
                options.append(actual is not None and
                               (op is None or version_satisfies(actual, op, required)))
            need(any(options), "unsatisfied " + field + ": " + group.strip())


def make_lock(base: dict, authentication: dict, indexes: dict[str, bytes],
              apt_uris: str, resolver_sha256: str) -> dict:
    need(base.get("base") == BASE and base.get("platform") == PLATFORM,
         "base/platform identity")
    base_versions = base.get("packages")
    need(isinstance(base_versions, dict) and all(NAME.fullmatch(k) and isinstance(v, str) and v
         for k, v in base_versions.items()), "base inventory")
    need(authentication.get("base") == BASE and authentication.get("platform") == PLATFORM and
         authentication.get("signature_verified") is True and
         authentication.get("apt_update_complete") is True and
         authentication.get("apt_no_remove") is True and
         authentication.get("apt_no_recommends") is True and
         authentication.get("resolver_sha256") == resolver_sha256 and
         HEX.fullmatch(resolver_sha256) is not None, "authentication evidence")
    proof_hash = digest(json.dumps(authentication, sort_keys=True,
                                   separators=(",", ":")).encode())
    need(APPROVED_PROOF_SHA256 is not None and proof_hash == APPROVED_PROOF_SHA256 and
         bool(APPROVED_SIGNERS), "unapproved signed-index proof")
    need(authentication.get("base_inventory_sha256") == digest(
         json.dumps(base, sort_keys=True, separators=(",", ":")).encode()), "base inventory digest")
    need(authentication.get("apt_uris_sha256") == digest(apt_uris.encode()) and
         isinstance(authentication.get("apt_uri_count"), int), "APT output proof")
    proofs = authentication.get("indexes")
    need(isinstance(proofs, list) and proofs and len(proofs) == len(indexes), "index set")
    records: dict[str, list[dict[str, str]]] = {}
    for proof in proofs:
        name = proof.get("name")
        url = proof.get("url", "")
        parsed = urlsplit(url)
        repo = urlsplit(proof.get("repo_base_url", ""))
        need(name in indexes and parsed.scheme == "https" and parsed.hostname in HOSTS and
             parsed.port in (None, 443) and not parsed.username and not parsed.password and
             repo.scheme == "https" and repo.hostname == parsed.hostname and
             repo.port in (None, 443) and not repo.username and not repo.password and
             not repo.query and not repo.fragment and repo.path and
             safe_relative_path(repo.path.lstrip("/")) and
             proof.get("packages_sha256") == digest(indexes[name]) and
             HEX.fullmatch(proof.get("inrelease_sha256", "")) is not None and
             FINGERPRINT_V4.fullmatch(proof.get("signer_fingerprint", "")) is not None and
             proof["signer_fingerprint"] in APPROVED_SIGNERS,
             "signed index proof")
        records[name] = paragraphs(indexes[name].decode("utf-8"))
    need(set(records) == set(indexes), "unproven index")

    selected, files, used = {}, [], set()
    for line in apt_uris.splitlines():
        need(line.startswith("'"), "unexpected APT output line")
        fields = shlex.split(line)
        need(len(fields) == 4, "apt URI format")
        url, filename, size_raw, _apt_md5 = fields
        parsed = urlsplit(url)
        need(parsed.scheme == "https" and parsed.hostname in HOSTS and
             parsed.port in (None, 443) and not parsed.username and not parsed.password and
             not parsed.fragment and not parsed.query, "APT URI host/scheme")
        need(re.fullmatch(r"[A-Za-z0-9+.%:_~-]+\.deb", filename) is not None and
             safe_relative_path(filename) and safe_relative_path(parsed.path.lstrip("/")) and
             filename not in used and unquote(filename) == unquote(parsed.path.rsplit("/", 1)[-1]),
             "APT filename")
        used.add(filename)
        name = filename.split("_", 1)[0]
        need(NAME.fullmatch(name) is not None and name not in selected and size_raw.isdigit(),
             "duplicate or malformed package")
        matches = [row for proof in proofs for row in records[proof["name"]]
                   if row.get("Package") == name and row.get("Size") == size_raw and
                   row.get("Filename") and safe_relative_path(row["Filename"]) and
                   parsed.hostname == urlsplit(proof["repo_base_url"]).hostname and
                   unquote(parsed.path) ==
                   unquote(urlsplit(proof["repo_base_url"]).path).rstrip("/") + "/" + row["Filename"]]
        identities = {(r.get("Version"), r.get("Architecture"), r.get("SHA256"),
                       r.get("Installed-Size"), r.get("Depends", ""), r.get("Pre-Depends", ""))
                      for r in matches}
        need(len(identities) == 1, "missing/ambiguous signed package")
        row = matches[0]
        need(row.get("Architecture") in ("amd64", "all") and
             HEX.fullmatch(row.get("SHA256", "")) is not None and
             row.get("Installed-Size", "").isdigit() and
             int(row["Installed-Size"]) > 0 and int(size_raw) > 0 and
             row.get("Version") and NAME.fullmatch(row["Package"]) is not None,
             "package metadata")
        selected[name] = row
        files.append({"name": name, "version": row["Version"],
                      "architecture": row["Architecture"], "filename": row["Filename"],
                      "url": url, "size": int(size_raw), "sha256": row["SHA256"],
                      "installed_size_kib": int(row["Installed-Size"]),
                      "depends": row.get("Depends", ""),
                      "pre_depends": row.get("Pre-Depends", ""),
                      "action": "upgrade" if name in base_versions else "install"})
    need(selected and all(name in selected or name in base_versions for name in TOP),
         "missing top-level package")
    need(len(files) == authentication["apt_uri_count"], "APT URI count")
    versions = {**base_versions, **{name: row["Version"] for name, row in selected.items()}}
    for row in selected.values():
        dependencies_satisfied(row, versions)
    need(sum(f["size"] for f in files) <= MAX_DOWNLOAD, "download budget")
    # KiB is the signed index's Installed-Size unit; allow a separate layer-overhead gate.
    installed = sum(f["installed_size_kib"] * 1024 for f in files)
    need(installed <= MAX_INSTALLED, "installed-size budget")
    return {"schema": "alopex.trace-builder-lock/v1", "platform": PLATFORM,
            "base": BASE, "resolver_sha256": resolver_sha256,
            "base_inventory_sha256": authentication["base_inventory_sha256"],
            "signed_indexes": proofs, "top_level": list(TOP),
            "packages": sorted(files, key=lambda f: f["name"]),
            "budgets": {"download_bytes": sum(f["size"] for f in files),
                        "installed_size_declared_bytes": installed,
                        "installed_layer_cap_bytes": MAX_INSTALLED,
                        "build_tree_upper_bytes": 500_000_000,
                        "diagnostic_peak_bytes": MAX_DIAGNOSTIC}}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base-inventory", type=Path, required=True)
    p.add_argument("--authentication", type=Path, required=True)
    p.add_argument("--apt-uris", type=Path, required=True)
    p.add_argument("--index", action="append", required=True, help="name=Packages-file")
    p.add_argument("--resolver-sha256", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    indexes = dict(item.split("=", 1) for item in args.index)
    need(len(indexes) == len(args.index), "duplicate index name")
    lock = make_lock(json.loads(args.base_inventory.read_text()),
                     json.loads(args.authentication.read_text()),
                     {name: Path(path).read_bytes() for name, path in indexes.items()},
                     args.apt_uris.read_text(), args.resolver_sha256)
    args.output.write_text(json.dumps(lock, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
