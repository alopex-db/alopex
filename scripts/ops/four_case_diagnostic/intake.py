"""Pinned CMS intake for the independent four-case auditor.

CMS encryption does not authenticate its sender. The caller must obtain the
ciphertext SHA-256, byte size, and expected binding through an independent
trusted receipt. None of those values is inferred from this archive.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import tempfile

from . import audit

SCHEMA = "alopex.sql-four-case-evidence/v1"
BASE = {"manifest.json": 16_384, "cells.ndjson": 262_144,
        "client.ndjson": 4_000_000, "server.ndjson": 8_000_000,
        "resource-manifests.json": 16_384}
MAX_CMS = 18_000_000
MAX_ARCHIVE = 18_000_000
HEX = re.compile(r"[0-9a-f]{64}\Z")


def require(condition, code):
    if not condition:
        raise ValueError(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def _read_file(path, limit, code):
    path = Path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= limit, code)
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    require(len(data) == info.st_size, code)
    return data


def _json(data, code):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, code)
            result[key] = value
        return result

    def constant(_value):
        raise ValueError(code)

    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                          parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(code) from error


def _records(data, count, code):
    require(data.endswith(b"\n") and len(data.splitlines()) == count, code)
    return [_json(line, code) for line in data.splitlines()]


def _paths():
    files = dict(BASE)
    for case in audit.CASES:
        for trial in range(3):
            files[f"resources/{case}-{trial}.json"] = 65_536
            for mode in ("on", "off"):
                files[f"raw/{case}-{trial}-{mode}.ndjson"] = 1_000_000
    return files


def _archive(data):
    expected = _paths()
    content = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as bundle:
            for member in bundle:
                require(member.name in expected and member.name not in content and
                        member.isfile() and 0 < member.size <= expected[member.name] and
                        not member.pax_headers, "archive_allowlist")
                stream = bundle.extractfile(member)
                require(stream is not None, "archive_allowlist")
                value = stream.read(expected[member.name] + 1)
                require(len(value) == member.size, "archive_bound")
                content[member.name] = value
    except (tarfile.TarError, OSError, EOFError) as error:
        raise ValueError("archive_format") from error
    require(set(content) == set(expected), "archive_inventory")
    return content


def _decode(content, expected_binding, expected_fixtures):
    manifest = _json(content["manifest.json"], "manifest_format")
    require(type(manifest) is dict and set(manifest) ==
            {"schema", "binding", "files", "cell_count", "client_count",
             "server_count", "cleanup_verified", "performance_qualification"} and
            manifest["schema"] == SCHEMA and manifest["binding"] == expected_binding and
            manifest["cell_count"] == 24 and manifest["client_count"] == 3720 and
            manifest["server_count"] == 23994 and
            manifest["cleanup_verified"] is True and
            manifest["performance_qualification"] is False, "manifest_identity")
    files = set(content) - {"manifest.json"}
    entries = manifest["files"]
    require(type(entries) is dict and set(entries) == files, "file_inventory")
    for name in files:
        entry = entries[name]
        require(type(entry) is dict and set(entry) == {"sha256", "size"} and
                type(entry["sha256"]) is str and HEX.fullmatch(entry["sha256"]) and
                type(entry["size"]) is int and entry["size"] == len(content[name]) and
                digest(content[name]) == entry["sha256"], "file_digest")
    cells = _records(content["cells.ndjson"], 24, "cell_inventory")
    clients = _records(content["client.ndjson"], 3720, "client_inventory")
    servers = _records(content["server.ndjson"], 23994, "server_inventory")
    resources = _json(content["resource-manifests.json"], "resource_inventory")
    keys = {f"{case}-{trial}" for case in audit.CASES for trial in range(3)}
    require(type(resources) is dict and set(resources) == keys, "resource_inventory")
    resource_digests = {}
    raw = {}
    for case in audit.CASES:
        for trial in range(3):
            key = f"{case}-{trial}"
            data = content[f"resources/{key}.json"]
            resource = _json(data, "resource_format")
            require(type(resource) is dict and
                    resources[key] == digest(data) and
                    resource.get("schema") == "alopex.sql-operation-resources/v1" and
                    resource.get("status") == "observed" and
                    resource.get("coverage") == "stable_pid_namespace_allocation",
                    "resource_identity")
            resource_digests[(case, trial)] = digest(data)
            for mode in ("on", "off"):
                raw[(case, trial, mode)] = content[f"raw/{key}-{mode}.ndjson"]
    return audit.audit(cells, clients, servers, binding=manifest["binding"],
                       expected_binding=expected_binding,
                       expected_fixtures=expected_fixtures, raw_by_cell=raw,
                       resource_manifests=resource_digests)


def audit_cms(ciphertext_path, *, expected_ciphertext_sha256, expected_ciphertext_size,
              recipient_certificate, recipient_key, expected_binding, expected_fixtures):
    """Decrypt and audit a CMS whose digest and binding came from a trusted receipt.

    The recipient key never appears in the subprocess arguments. OpenSSL reads
    it from its file path. No plaintext file is written by this intake.
    """
    require(type(expected_ciphertext_sha256) is str and
            HEX.fullmatch(expected_ciphertext_sha256) and
            type(expected_ciphertext_size) is int and
            0 < expected_ciphertext_size <= MAX_CMS, "cms_pin")
    ciphertext = _read_file(ciphertext_path, MAX_CMS, "cms_bound")
    require(len(ciphertext) == expected_ciphertext_size and
            digest(ciphertext) == expected_ciphertext_sha256, "cms_digest")
    _read_file(recipient_certificate, 65_536, "cms_recipient")
    _read_file(recipient_key, 65_536, "cms_recipient")
    try:
        with tempfile.TemporaryFile() as source:
            source.write(ciphertext)
            source.seek(0)
            process = subprocess.Popen(
                ["openssl", "cms", "-decrypt", "-binary", "-inform", "DER",
                 "-recip", str(recipient_certificate), "-inkey", str(recipient_key)],
                stdin=source, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                # Cap plaintext before reading the remainder, including a
                # deliberately inflated archive.
                archive = process.stdout.read(MAX_ARCHIVE + 1)
                require(len(archive) <= MAX_ARCHIVE, "archive_bound")
                require(process.wait(timeout=120) == 0, "cms_decrypt")
            finally:
                process.stdout.close()
                if process.poll() is None:
                    process.kill()
                    process.wait()
    except (OSError, subprocess.SubprocessError, BrokenPipeError) as error:
        raise ValueError("cms_decrypt") from error
    require(0 < len(archive), "archive_bound")
    return _decode(_archive(archive), expected_binding, expected_fixtures)
