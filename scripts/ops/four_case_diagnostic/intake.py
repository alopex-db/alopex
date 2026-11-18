"""Pinned CMS intake for the independent four-case auditor.

CMS encryption does not authenticate its sender. The caller must obtain the
ciphertext SHA-256, byte size, and expected binding through an independent
trusted receipt. None of those values is inferred from this archive.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import zlib

from . import audit

SCHEMA = "alopex.sql-four-case-evidence/v1"
BASE = {"manifest.json": 16_384, "cells.ndjson": 262_144,
        "client.ndjson": 4_000_000, "server.ndjson": 8_000_000,
        "resource-manifests.json": 16_384}
MAX_CMS = 18_000_000
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


MAX_ARCHIVE = sum(_paths().values()) + 512 * (len(_paths()) + 2) + 10_240


def _gunzip(data):
    """Bound expansion before inspecting tar headers; require one complete gzip."""
    require(type(data) is bytes and 0 < len(data) <= MAX_ARCHIVE, "archive_bound")
    inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
    output = bytearray()
    try:
        for start in range(0, len(data), 65_536):
            chunk = data[start:start + 65_536]
            output.extend(inflater.decompress(chunk, MAX_ARCHIVE + 1 - len(output)))
            require(len(output) <= MAX_ARCHIVE and not inflater.unconsumed_tail and
                    not inflater.unused_data, "archive_bound")
        require(inflater.eof and not inflater.unconsumed_tail and
                not inflater.unused_data, "archive_format")
    except zlib.error as error:
        raise ValueError("archive_format") from error
    return bytes(output)


def _archive(data):
    """Read only plain ustar regular files, including exact zero-pad EOF."""
    plain = _gunzip(data)
    expected = _paths()
    content = {}
    offset = 0
    while True:
        require(offset + 1024 <= len(plain), "archive_format")
        header = plain[offset:offset + 512]
        if header == b"\0" * 512:
            require(plain[offset + 512:offset + 1024] == b"\0" * 512 and
                    not any(plain[offset + 1024:]) and
                    len(plain) == ((offset + 1024 + 10_239) // 10_240) * 10_240,
                    "archive_trailing")
            break
        require(header[100:124] == b"0000600\0" + b"0000000\0" * 2 and
                header[136:148] == b"00000000000\0" and
                header[156:157] == b"0" and
                header[157:257] == b"\0" * 100 and
                header[257:265] == b"ustar\0" + b"00" and
                header[265:512] == b"\0" * 247, "archive_allowlist")
        checksum = sum(header[:148]) + 8 * 32 + sum(header[156:])
        require(header[148:156] == f"{checksum:06o}".encode() + b"\0 ",
                "archive_format")
        name_bytes = header[:100].split(b"\0", 1)[0]
        require(name_bytes and all(char < 128 for char in name_bytes), "archive_allowlist")
        name = name_bytes.decode("ascii")
        require(name in expected and name not in content and
                header[:100] == name_bytes.ljust(100, b"\0"), "archive_allowlist")
        size_field = header[124:136]
        require(size_field.endswith(b"\0") and
                all(char in b"01234567" for char in size_field[:11]),
                "archive_format")
        size = int(size_field[:11], 8)
        require(0 < size <= expected[name], "archive_bound")
        start = offset + 512
        end = start + size
        next_offset = start + ((size + 511) // 512) * 512
        require(next_offset <= len(plain) and not any(plain[end:next_offset]),
                "archive_format")
        content[name] = plain[start:end]
        offset = next_offset
    require(set(content) == set(expected), "archive_inventory")
    return content


def _decode(content, expected_binding, expected_fixtures):
    manifest = _json(content["manifest.json"], "manifest_format")
    require(type(manifest) is dict and set(manifest) ==
            {"schema", "binding", "files", "cell_count", "client_count",
             "server_count", "cleanup_verified", "performance_qualification"} and
            manifest["schema"] == SCHEMA and manifest["binding"] == expected_binding and
            manifest["cell_count"] == 24 and manifest["client_count"] == 3720 and
            manifest["server_count"] == 23919 and
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
    servers = _records(content["server.ndjson"], 23919, "server_inventory")
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
