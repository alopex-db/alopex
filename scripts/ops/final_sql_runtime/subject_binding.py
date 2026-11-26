"""Bind admitted public products and one qualified image to four SQL subjects.

The caller supplies independently reviewed pins and an image identity only
after the selected-job #804 lock and live-image checks. Nothing here downloads
or constructs a subject from an unreviewed manifest.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import stat

from . import matrix, reference_public, same_job

PRODUCTS = ("published", "before", "candidate")
VERSIONS = {"published": "0.8.16", "before": "0.8.17", "candidate": "0.8.17"}
SOURCE_COMMITS = {**reference_public.SOURCE_COMMIT,
                  "candidate": "426418099c4337e73f5ced4444215a640898ed5a"}
ROLES = {"server", "library", "wheel", "receipt"}


def require(value):
    if not value:
        raise ValueError("SUBJECT_BINDING")


def _private(path, mode=0o600, max_bytes=268_435_456):
    require(isinstance(path, Path) and path.is_absolute() and not path.is_symlink())
    require(all(stat.S_ISDIR(parent.lstat().st_mode) for parent in path.parents))
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
            info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == mode and
            0 < info.st_size <= max_bytes)
    return path.read_bytes()


def _read(path, limit):
    data = _private(path, max_bytes=limit)
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result)
            result[key] = value
        return result
    value = json.loads(data, object_pairs_hook=unique)
    require(type(value) is dict and data == (json.dumps(
        value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())
    return value, hashlib.sha256(data).hexdigest()


def _product(subject, path, reviewed):
    require(type(reviewed) is dict and set(reviewed) ==
            {"source", "assets_digest", "files", "platform", "version"} and
            reviewed["version"] == VERSIONS[subject] and
            reviewed["platform"] == "linux/amd64" and
            matrix._hex(reviewed["assets_digest"], 64) and
            type(reviewed["source"]) is dict and
            set(reviewed["source"]) == {"commit", "tree", "profile"} and
            matrix._hex(reviewed["source"]["commit"], 40) and
            matrix._hex(reviewed["source"]["tree"], 40) and
            reviewed["source"]["profile"] == "release")
    require(reviewed["source"]["commit"] == SOURCE_COMMITS[subject])
    require(type(reviewed["files"]) is list and len(reviewed["files"]) == 4 and
            {item.get("role") for item in reviewed["files"] if type(item) is dict} == ROLES)
    manifest, digest = _read(path, 16_384)
    require(digest == reviewed["assets_digest"] and
            set(manifest) == {"schema", "platform", "source_commit",
                              "harness_commit", "expanded_limit_bytes", "files"} and
            manifest["schema"] == "alopex.test-env-assets/v1" and
            manifest["platform"] == reviewed["platform"] and
            manifest["source_commit"] == reviewed["source"]["commit"] and
            matrix._hex(manifest["harness_commit"], 40) and
            type(manifest["expanded_limit_bytes"]) is int and
            0 < manifest["expanded_limit_bytes"] <= 268_435_456 and
            type(manifest["files"]) is list and len(manifest["files"]) == 5)
    require(all(type(item) is dict for item in manifest["files"]))
    rows = [item for item in manifest["files"] if item.get("role") != "harness"]
    require(rows == reviewed["files"] and
            {item.get("role") for item in manifest["files"]} == ROLES | {"harness"})
    require(sum(item.get("size", 0) for item in manifest["files"]) <= 268_435_456)
    for item in manifest["files"]:
        require(type(item) is dict and set(item) == {"role", "path", "size", "sha256"} and
                type(item["path"]) is str and type(item["size"]) is int and
                0 < item["size"] <= 268_435_456 and matrix._hex(item["sha256"], 64))
        expected_path = (reference_public.ROLES.get(item["role"]) if
                         item["role"] != "wheel" else
                         f"wheels/alopex-{reviewed['version']}-cp38-abi3-linux_x86_64.whl")
        data = _private(path.parent / item["path"],
                        0o700 if item["role"] == "server" else 0o600,
                        item["size"])
        require(item["path"] == expected_path and len(data) == item["size"] and
                hashlib.sha256(data).hexdigest() == item["sha256"])
    receipt = next(item for item in rows if item["role"] == "receipt")
    body, _ = _read(path.parent / receipt["path"], 16_384)
    require(body.get("schema") == "alopex.performance-build/v1" and
            body.get("source") == reviewed["source"] and
            body.get("version") == reviewed["version"] and
            body.get("server_sha256") == next(item["sha256"] for item in rows
                                               if item["role"] == "server") and
            body.get("python_wheel_sha256") == next(item["sha256"] for item in rows
                                                     if item["role"] == "wheel") and
            body.get("parser_sha256") == next(item["sha256"] for item in rows
                                              if item["role"] == "library"))
    return {"subject": subject, "product_subject": subject,
            "source": copy.deepcopy(reviewed["source"]),
            "assets_digest": reviewed["assets_digest"],
            "platform": reviewed["platform"], "files": copy.deepcopy(rows)}


def assemble(*, manifests, reviewed_pins, qualification_args, selected_image_digest,
             worker_sha256, harness_sha256, controller_digest):
    """Return private controller subject/manifests/audit pins for four subjects."""
    try:
        require(type(manifests) is dict and set(manifests) == set(PRODUCTS) and
                type(reviewed_pins) is dict and set(reviewed_pins) == set(PRODUCTS) and
                type(reviewed_pins["candidate"]) is dict and
                type(reviewed_pins["candidate"].get("source")) is dict and
                type(qualification_args) is dict and
                set(qualification_args) == {"lock_path", "expected", "repo",
                                            "runner_temp", "environment"} and
                all(matrix._hex(value, 64) for value in
                    (selected_image_digest, worker_sha256, harness_sha256,
                     controller_digest)))
        qualification = same_job.attest_live(**qualification_args)
        environment = qualification_args["environment"]
        require(type(environment) is dict and type(qualification) is dict and
                qualification.get("job_id") == environment.get("GITHUB_RUN_ID") and
                qualification.get("run_attempt") == environment.get("GITHUB_RUN_ATTEMPT") and
                qualification.get("qualification_cases") == sorted(same_job.qualification.CASES) and
                qualification.get("image_digest") == selected_image_digest and
                qualification.get("controller_digest") == controller_digest and
                qualification.get("assets_digest") ==
                    reviewed_pins["candidate"].get("assets_digest") and
                qualification.get("source_commit") ==
                    reviewed_pins["candidate"].get("source", {}).get("commit") and
                qualification.get("source_tree") ==
                    reviewed_pins["candidate"].get("source", {}).get("tree"))
        subjects = {name: _product(name, manifests[name], reviewed_pins[name])
                    for name in PRODUCTS}
        published = subjects["published"]
        subjects["postgresql"] = {**copy.deepcopy(published),
                                  "subject": "postgresql",
                                  "product_subject": "published",
                                  "reference_image_digest": selected_image_digest,
                                  "published_client_pin": copy.deepcopy(published)}
        product_manifests = dict(manifests, postgresql=manifests["published"])
        audit_pins = {name: {"source": copy.deepcopy(pin["source"]),
                             "assets_digest": pin["assets_digest"],
                             "files": copy.deepcopy(pin["files"]),
                             "worker_sha256": worker_sha256,
                             "harness_sha256": harness_sha256,
                             "image_digest": selected_image_digest,
                             "controller_digest": controller_digest}
                      for name, pin in subjects.items()}
        return subjects, product_manifests, audit_pins
    except (OSError, KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError("SUBJECT_BINDING") from None
