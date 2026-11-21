"""Acquire one pinned native asset ZIP through an injected GitHub API fetcher.

The caller supplies the independently trusted four-case lock and a fetcher with
``fetch(path, *, binary=False, timeout=30, max_bytes=None)``. Binary fetchers
must stop reading at ``max_bytes``. This module has no network client.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import types
import zipfile

from . import acquisition, qualification


EXPANDED_LIMIT = 268_435_456
ENTRY_LIMIT = 32


def _require(value, code="ASSET_IDENTITY"):
    if not value:
        raise ValueError(code)


def _private_dir(path):
    info = path.lstat()
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
             stat.S_IMODE(info.st_mode) == 0o700, "ASSET_STATE")


def _write(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _manifest_verifier(expected_digest):
    path = Path(__file__).resolve().parents[2] / "test-env" / "common.py"
    _require(path.is_file() and not path.is_symlink() and path.stat().st_size < 1_000_000,
             "ASSET_MANIFEST")
    source = path.read_bytes()
    _require(hashlib.sha256(source).hexdigest() == expected_digest, "ASSET_MANIFEST")
    module = types.ModuleType("alopex_four_case_asset_common")
    module.__file__ = str(path)
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module.verify_assets


def _extract(archive, root):
    with zipfile.ZipFile(archive) as package:
        entries = package.infolist()
        _require(0 < len(entries) <= ENTRY_LIMIT, "ASSET_ARCHIVE")
        seen, expanded = set(), 0
        for entry in entries:
            raw = entry.filename
            name = raw.rstrip("/")
            parts = name.split("/")
            mode = entry.external_attr >> 16
            _require(name and name not in seen and not raw.startswith("/") and
                     "\\" not in raw and all(part not in ("", ".", "..") for part in parts) and
                     (name in ("result.json", "private-evidence.cms") or
                      (parts[0] == "assets" and (len(parts) > 1 or entry.is_dir()))) and
                     not entry.flag_bits & 1 and
                     (stat.S_IFMT(mode) in (0, stat.S_IFDIR) if entry.is_dir()
                      else stat.S_IFMT(mode) in (0, stat.S_IFREG)),
                     "ASSET_ARCHIVE")
            seen.add(name)
            expanded += entry.file_size
            _require(expanded <= EXPANDED_LIMIT and entry.file_size <= EXPANDED_LIMIT,
                     "ASSET_ARCHIVE")
        _require({"result.json", "private-evidence.cms", "assets/assets.json"} <= seen,
                 "ASSET_ARCHIVE")
        for entry in entries:
            name = PurePosixPath(entry.filename.rstrip("/"))
            target = root.joinpath(*name.parts)
            if entry.is_dir():
                target.mkdir(mode=0o700, parents=True, exist_ok=True)
                _private_dir(target)
                continue
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _private_dir(target.parent)
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            copied = 0
            with package.open(entry) as source, os.fdopen(descriptor, "wb") as output:
                while chunk := source.read(1_048_576):
                    copied += len(chunk)
                    _require(copied <= entry.file_size, "ASSET_ARCHIVE")
                    output.write(chunk)
            _require(copied == entry.file_size, "ASSET_ARCHIVE")


def acquire(lock, *, job_id, directory, fetch):
    """Return verified v2 proof; remove the entire new directory on failure."""
    _require(type(lock) is dict and set(lock) == {"schema", "pin"} and
             lock.get("schema") == "alopex.sql-four-case-pins/v1" and
             type(lock.get("pin")) is dict and
             set(lock["pin"]) == qualification.PIN_FIELDS and
             type(job_id) is str and job_id.isdecimal())
    pin = lock["pin"]
    _require(callable(fetch))
    _require(type(pin.get("asset_run_id")) is int and pin["asset_run_id"] > 0 and
             type(pin.get("artifact_id")) is int and pin["artifact_id"] > 0 and
             type(pin.get("asset_zip_size_bytes")) is int and
             0 < pin["asset_zip_size_bytes"] < acquisition.ZIP_LIMIT and
             pin.get("artifact_sha256") == pin.get("zip_sha256") and
             all(type(pin.get(key)) is str and pattern.fullmatch(pin[key]) for key, pattern in
                 (("asset_run_head", acquisition.HEX40), ("source_commit", acquisition.HEX40),
                  ("source_tree", acquisition.HEX40), ("harness_commit", acquisition.HEX40),
                  ("zip_sha256", acquisition.HEX64), ("assets_digest", acquisition.HEX64),
                  ("server_sha256", acquisition.HEX64), ("wheel_sha256", acquisition.HEX64),
                  ("common_verifier_sha256", acquisition.HEX64))) and
             type(pin.get("asset_file_digests")) is dict and
             set(pin["asset_file_digests"]) == qualification.ASSET_ROLES and
             all(type(value) is str and acquisition.HEX64.fullmatch(value)
                 for value in pin["asset_file_digests"].values()) and
             pin["asset_file_digests"]["server"] == pin["server_sha256"] and
             pin["asset_file_digests"]["wheel"] == pin["wheel_sha256"])
    run_id = pin["asset_run_id"]
    run = fetch(f"/actions/runs/{run_id}", timeout=30)
    _require(type(run) is dict and run.get("id") == run_id and
             run.get("status") == "completed" and run.get("conclusion") == "success" and
             run.get("event") == "workflow_dispatch" and
             type(run.get("run_attempt")) is int and run["run_attempt"] == 1 and
             run.get("head_sha") == pin["asset_run_head"])
    jobs = fetch(f"/actions/runs/{run_id}/jobs?per_page=100", timeout=30)
    _require(type(jobs) is dict and type(jobs.get("jobs")) is list and
             type(jobs.get("total_count")) is int and jobs["total_count"] == len(jobs["jobs"]) == 1)
    job = jobs["jobs"][0]
    _require(type(job) is dict and job.get("run_id") == run_id and
             job.get("head_sha") == pin["asset_run_head"] and
             job.get("status") == "completed" and job.get("conclusion") == "success" and
             type(job.get("steps")) is list and bool(job["steps"]) and
             all(type(step) is dict and step.get("status") == "completed" and
                 step.get("conclusion") == "success" for step in job["steps"]))
    listing = fetch(f"/actions/runs/{run_id}/artifacts?per_page=100", timeout=30)
    _require(type(listing) is dict and type(listing.get("artifacts")) is list and
             type(listing.get("total_count")) is int and
             listing["total_count"] == len(listing["artifacts"]) == 1)
    artifact = listing["artifacts"][0]
    _require(type(artifact) is dict and artifact.get("id") == pin["artifact_id"] and
             artifact.get("name") == "native-product-assets" and
             artifact.get("expired") is False and
             artifact.get("digest") == "sha256:" + pin["zip_sha256"] and
             type(artifact.get("size_in_bytes")) is int and
             artifact["size_in_bytes"] == pin["asset_zip_size_bytes"] and
             type(artifact.get("workflow_run")) is dict and
             artifact["workflow_run"].get("id") == run_id and
             artifact["workflow_run"].get("head_sha") == pin["asset_run_head"])
    raw = fetch(f"/actions/artifacts/{pin['artifact_id']}/zip", binary=True,
                timeout=180, max_bytes=pin["asset_zip_size_bytes"])
    _require(type(raw) is bytes and len(raw) == pin["asset_zip_size_bytes"] and
             hashlib.sha256(raw).hexdigest() == pin["zip_sha256"])
    directory = Path(directory)
    _private_dir(directory.parent)
    _require(not directory.exists() and not directory.is_symlink(), "ASSET_STATE")
    directory.mkdir(mode=0o700)
    try:
        _private_dir(directory)
        archive = directory / "candidate.original.zip"
        assets = directory / "assets-root"
        receipt = directory / "acquisition.private.json"
        _write(archive, raw)
        assets.mkdir(mode=0o700)
        _extract(archive, assets)
        manifest_path = assets / "assets/assets.json"
        _require(0 < manifest_path.stat().st_size < acquisition.MANIFEST_LIMIT,
                 "ASSET_MANIFEST")
        _require(hashlib.sha256(manifest_path.read_bytes()).hexdigest() == pin["assets_digest"],
                 "ASSET_MANIFEST")
        try:
            manifest = _manifest_verifier(pin["common_verifier_sha256"])(manifest_path, "linux/amd64")
        except Exception as error:
            raise ValueError("ASSET_MANIFEST") from error
        _require(manifest["source_commit"] == pin["source_commit"] and
                 manifest["harness_commit"] == pin["harness_commit"], "ASSET_MANIFEST")
        roles = {item["role"]: item["sha256"] for item in manifest["files"]}
        _require(len(manifest["files"]) == len(qualification.ASSET_ROLES) and
                 roles == pin["asset_file_digests"], "ASSET_MANIFEST")
        receipt_entry = next(item for item in manifest["files"] if item["role"] == "receipt")
        try:
            product_receipt = json.loads((manifest_path.parent / receipt_entry["path"]).read_bytes(),
                                         object_pairs_hook=acquisition._unique_pairs)
        except (ValueError, UnicodeError) as error:
            raise ValueError("ASSET_MANIFEST") from error
        source_identity = {"commit": pin["source_commit"],
                           "tree": pin["source_tree"], "profile": "release"}
        _require(type(product_receipt) is dict and
                 product_receipt.get("source") == source_identity, "ASSET_MANIFEST")
        try:
            result_path = assets / "result.json"
            _require(0 < result_path.stat().st_size < acquisition.MANIFEST_LIMIT,
                     "ASSET_MANIFEST")
            result = json.loads(result_path.read_bytes(),
                                object_pairs_hook=acquisition._unique_pairs)
        except (ValueError, UnicodeError) as error:
            raise ValueError("ASSET_MANIFEST") from error
        _require(result == {"schema": "alopex.native-assets/v1", "status": "passed",
                            "source": source_identity,
                            "assets_digest": pin["assets_digest"],
                            "performance_qualification": False}, "ASSET_MANIFEST")
        proof = {"schema": acquisition.SCHEMA, "qualification_job_id": job_id,
                 "run": run_id, "head": pin["asset_run_head"],
                 "artifact_id": pin["artifact_id"], "zip_sha256": pin["zip_sha256"],
                 "zip_size_bytes": pin["asset_zip_size_bytes"],
                 "assets_digest": pin["assets_digest"]}
        _write(receipt, (json.dumps(proof, sort_keys=True, separators=(",", ":")) + "\n").encode())
        return acquisition.verify(lock, receipt_path=receipt, archive_path=archive,
                                  manifest_path=manifest_path, job_id=job_id)
    except BaseException:
        shutil.rmtree(directory)
        raise
