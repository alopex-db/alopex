"""Offline, fail-closed proof of captured Ubuntu APT indexes and URI extraction.

Run on the selected runner after collecting the disposable resolver's results.
This does not certify the controller's base digest, security policy or capacity.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from urllib.parse import urlsplit

from resolve_builder_lock import BASE, HOSTS, PLATFORM, paragraphs

HEX = re.compile(r"[0-9a-f]{64}\Z")
FPR = re.compile(r"[0-9A-F]{40}\Z")
NAME = re.compile(r"[a-z0-9][a-z0-9+.-]*\Z")
COMPRESSED = (".xz", ".gz", ".bz2", ".lz4", ".zst")
MAX_CAPTURE_BYTES = 250_000_000


class ProofError(ValueError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ProofError(message)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def audit_capture_tree(directory: Path) -> None:
    total = 0
    require(directory.is_dir() and not directory.is_symlink(), "invalid capture directory")
    for root, dirs, files in os.walk(directory, followlinks=False):
        for name in [*dirs, *files]:
            entry = Path(root) / name
            details = entry.lstat()
            mode = details.st_mode
            require(not stat.S_ISLNK(mode), "capture symlink")
            require(stat.S_ISDIR(mode) or stat.S_ISREG(mode), "capture special file")
            if stat.S_ISREG(mode):
                total += details.st_size
                require(total <= MAX_CAPTURE_BYTES, "capture byte cap")


def safe_url(url: str) -> tuple[str, str]:
    p = urlsplit(url)
    require(p.scheme == "https" and p.hostname in HOSTS and p.port in (None, 443)
            and not p.username and not p.password and not p.query and not p.fragment
            and p.path.startswith("/ubuntu/dists/") and
            not any(part in ("", ".", "..") for part in p.path[1:].split("/")) and
            "%" not in p.path and "\\" not in p.path, "unknown index source")
    return p.hostname, p.path


def signed_hashes(raw: bytes) -> dict[str, tuple[int, str]]:
    text = raw.decode("utf-8")
    require(text.startswith("-----BEGIN PGP SIGNED MESSAGE-----\n"), "unsigned InRelease")
    header, sep, body = text.partition("\n\n")
    require(bool(sep) and "\n-----BEGIN PGP SIGNATURE-----" in body and
            header.startswith("-----BEGIN PGP SIGNED MESSAGE-----"), "invalid InRelease envelope")
    body = body.split("\n-----BEGIN PGP SIGNATURE-----", 1)[0]
    rows, in_sha = {}, False
    for line in body.splitlines():
        if line == "SHA256:":
            require(not in_sha, "duplicate SHA256 stanza")
            in_sha = True
            continue
        if in_sha and line and not line[0].isspace():
            in_sha = False
        if in_sha and line.strip():
            fields = line.split()
            require(len(fields) == 3 and HEX.fullmatch(fields[0]) is not None and
                    fields[1].isdigit() and int(fields[1]) > 0 and
                    fields[2] not in rows and not fields[2].startswith("/") and
                    all(part not in ("", ".", "..") for part in fields[2].split("/")),
                    "invalid signed hash entry")
            rows[fields[2]] = (int(fields[1]), fields[0])
    require(bool(rows), "missing signed SHA256 stanza")
    return rows


def validate_sources(raw: str) -> set[tuple[str, str, str]]:
    stanzas = paragraphs(raw)
    require(bool(stanzas), "missing APT source")
    declared = set()
    for row in stanzas:
        require(row.get("Types") == "deb" and
                row.get("URIs") in {f"https://{host}/ubuntu" for host in HOSTS} and
                row.get("Signed-By") == "/usr/share/keyrings/ubuntu-archive-keyring.gpg" and
                row.get("Enabled", "yes") == "yes" and
                row.get("Architectures", "amd64") == "amd64" and
                set(row) <= {"Types", "URIs", "Signed-By", "Enabled", "Architectures",
                             "Suites", "Components"}, "unexpected APT source")
        suites = row.get("Suites", "").split()
        components = row.get("Components", "").split()
        require(bool(suites) and len(suites) == len(set(suites)) and
                set(suites) <= {"noble", "noble-updates", "noble-security"} and
                bool(components) and len(components) == len(set(components)) and
                set(components) <= {"main", "restricted", "universe", "multiverse"},
                "unexpected APT suite/component")
        host = urlsplit(row["URIs"]).hostname
        declared.update((host, suite, component)
                        for suite in suites for component in components)
    return declared


def signer(inrelease: Path, keyring: Path) -> str:
    result = subprocess.run(["gpgv", "--status-fd", "1", "--keyring", str(keyring),
                             str(inrelease)], capture_output=True, text=True, timeout=30,
                            check=False)
    require(result.returncode == 0, "InRelease signature verification failed")
    fingerprints = {parts[2] for line in result.stdout.splitlines()
                    if (parts := line.split()) and len(parts) >= 3 and
                    parts[0] == "[GNUPG:]" and parts[1] == "VALIDSIG"}
    require(len(fingerprints) == 1 and FPR.fullmatch(next(iter(fingerprints))) is not None,
            "missing or ambiguous signer fingerprint")
    return fingerprints.pop()


def prove(directory: Path, resolver_script: Path) -> tuple[dict, dict, dict[str, Path]]:
    audit_capture_tree(directory)
    require((directory / "capture.status").read_text() == "capture-complete\n",
            "incomplete capture")
    source = (directory / "ubuntu.sources").read_text()
    declared_sources = validate_sources(source)
    script_hash = sha(resolver_script.read_bytes())
    require((directory / "resolver.sha256").read_text().split()[0] == script_hash,
            "resolver script drift")
    keyring = directory / "trusted-keyring.gpg"
    recorded = (directory / "source-keyring.sha256").read_text().splitlines()
    require(any(line.startswith(sha(keyring.read_bytes()) + "  ") and
                line.endswith("/usr/share/keyrings/ubuntu-archive-keyring.gpg")
                for line in recorded), "trusted keyring drift")
    packages: dict[str, str] = {}
    for line in (directory / "base.tsv").read_text().splitlines():
        parts = line.split("\t")
        require(len(parts) == 2 and NAME.fullmatch(parts[0]) is not None and
                parts[1] and parts[0] not in packages, "invalid base inventory")
        packages[parts[0]] = parts[1]
    require(bool(packages), "empty base inventory")
    base = {"base": BASE, "platform": PLATFORM, "packages": packages}
    raw_apt = (directory / "apt-raw.txt").read_text()
    uris = "".join(line + "\n" for line in raw_apt.splitlines() if line.startswith("'"))
    require(uris and uris == (directory / "apt-uris.txt").read_text(),
            "APT URI extraction drift")
    targets = paragraphs((directory / "indextargets.raw").read_text())
    require(bool(targets), "missing index targets")
    proofs, indexes = [], {}
    seen = set()
    used_inrelease = set()
    for target in targets:
        require(target.get("Created-By") == "Packages" and
                target.get("Architecture") == "amd64", "unexpected index target")
        uri = target.get("URI", "")
        host, path = safe_url(uri)
        match = re.fullmatch(r"/ubuntu/dists/([^/]+)/(.+/binary-amd64/Packages(?:\.[a-z0-9]+)?)", path)
        require(match is not None, "invalid Packages target")
        suite, relative = match.groups()
        component = relative.split("/", 1)[0]
        require((host, suite, component) in declared_sources,
                "index absent from declared source")
        if relative.endswith(COMPRESSED):
            relative = relative.rsplit(".", 1)[0]
        require(target.get("MetaKey") == relative, "APT target metadata drift")
        inrelease_url = f"https://{host}/ubuntu/dists/{suite}/InRelease"
        inrelease = directory / "lists" / f"{host}_ubuntu_dists_{suite}_InRelease"
        require(inrelease.is_file(), "missing InRelease")
        used_inrelease.add(inrelease.name)
        filename = Path(target.get("Filename", ""))
        require(filename.is_absolute() and filename.parent == Path("/resolution/results/lists") and
                re.fullmatch(r"[A-Za-z0-9_.-]+", filename.name) is not None,
                "index path outside capture")
        content_path = directory / "packages" / filename.name
        require(content_path.is_file(), "missing Packages bytes")
        content = content_path.read_bytes()
        require((inrelease_url, relative) not in seen, "duplicate index target")
        seen.add((inrelease_url, relative))
        size_hash = signed_hashes(inrelease.read_bytes()).get(relative)
        require(size_hash == (len(content), sha(content)), "Packages signed hash mismatch")
        fingerprint = signer(inrelease, keyring)
        name = "index-" + sha(uri.encode())[:16]
        require(name not in indexes, "duplicate index name")
        indexes[name] = content_path
        proofs.append({"name": name, "url": inrelease_url,
                       "repo_base_url": f"https://{host}/ubuntu",
                       "inrelease_sha256": sha(inrelease.read_bytes()),
                       "signer_fingerprint": fingerprint,
                       "packages_sha256": sha(content)})
    captured_inrelease = {path.name for path in (directory / "lists").glob("*InRelease")}
    require(captured_inrelease == used_inrelease, "unaccounted InRelease")
    auth = {"base": BASE, "platform": PLATFORM,
            "base_inventory_sha256": sha(canonical(base)),
            "resolver_sha256": script_hash,
            "signature_verified": True, "apt_update_complete": True,
            "apt_no_remove": True, "apt_no_recommends": True,
            "apt_uris_sha256": sha(uris.encode()), "apt_uri_count": len(uris.splitlines()),
            "indexes": sorted(proofs, key=lambda row: row["name"])}
    return base, auth, indexes


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--capture", type=Path, required=True)
    p.add_argument("--resolver-script", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    base, authentication, indexes = prove(args.capture, args.resolver_script)
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    (args.output / "base.json").write_text(json.dumps(base, sort_keys=True, indent=2) + "\n")
    (args.output / "authentication.json").write_text(
        json.dumps(authentication, sort_keys=True, indent=2) + "\n")
    (args.output / "indexes.json").write_text(json.dumps(
        {name: str(path) for name, path in indexes.items()}, sort_keys=True, indent=2) + "\n")
    (args.output / "proof.sha256").write_text(sha(canonical(authentication)) + "\n")


if __name__ == "__main__":
    main()
