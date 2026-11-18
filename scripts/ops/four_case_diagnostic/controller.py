"""Source-bound, same-job orchestration for the private #712 SQL diagnostic.

The reviewed manifest digest is supplied independently by the workflow. The
caller supplies only the pinned #804 operations and a pinned owned-server
launcher; this module never constructs or relaxes a security profile.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

from . import adapter, entrypoint, qualification, supervisor

SCHEMA = "alopex.sql-four-case-controller/v1"
MANIFEST_FIELDS = frozenset(("schema", "job_id", "lock_path", "lock_sha256",
                             "source_commit", "source_tree", "assets_digest",
                             "fixture_sha256", "package_files", "package_root",
                             "docs_harness_root", "product_harness_root", "python"))


def require(value, code):
    if not value:
        raise ValueError(code)


def _read_private(path, expected_digest, limit=65_536):
    require(type(expected_digest) is str and
            qualification.HEX64.fullmatch(expected_digest), "MISSING_PIN")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and
                stat.S_IMODE(before.st_mode) == 0o600 and
                0 < before.st_size <= limit, "PRIVATE_MANIFEST")
        raw = os.read(fd, limit + 1)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) and
                hashlib.sha256(raw).hexdigest() == expected_digest, "PRIVATE_MANIFEST")
        return raw
    finally:
        os.close(fd)


def load_manifest(path, expected_digest):
    """Read exact private pins, anchored by an independently reviewed digest."""
    try:
        manifest = json.loads(_read_private(path, expected_digest),
                              object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
    except (ValueError, UnicodeError) as error:
        raise ValueError("PRIVATE_MANIFEST") from error
    require(type(manifest) is dict and set(manifest) == MANIFEST_FIELDS and
            manifest["schema"] == SCHEMA and type(manifest["job_id"]) is str and
            manifest["job_id"].isdecimal() and
            all(type(manifest[key]) is str and qualification.HEX40.fullmatch(manifest[key])
                for key in ("source_commit", "source_tree")) and
            all(type(manifest[key]) is str and qualification.HEX64.fullmatch(manifest[key])
                for key in ("lock_sha256", "assets_digest", "fixture_sha256")) and
            type(manifest["package_files"]) is dict and
            set(manifest["package_files"]) == {
                f"four_case_diagnostic/{name}.py" for name in
                ("__init__", "adapter", "log_parser", "operation", "oracle",
                 "runner", "worker")} and
            all(type(value) is str and qualification.HEX64.fullmatch(value)
                for value in manifest["package_files"].values()) and
            all(type(manifest[key]) is str and Path(manifest[key]).is_absolute()
                for key in ("lock_path", "package_root", "docs_harness_root",
                            "product_harness_root", "python")), "PRIVATE_MANIFEST")
    return manifest


def _owned_directory(path):
    path = Path(path)
    path.mkdir(mode=0o700)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700, "OUTPUT_STATE")
    return path


def _write_json(path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
    return hashlib.sha256(raw).hexdigest()


def _run_current_job(manifest_path, manifest_digest, *, repo, runner_temp,
                     environment, output, operations, stage_pair):
    """Attest #804, then run 12 independently staged owned-server pairs.

    ``stage_pair`` comes from the source-pinned wrapper and returns ``start``,
    ``identity_of`` and ``subject_environment`` for one pair. It must use the
    already qualified image and #804 security profile. Every pair gets a fresh
    output directory. No success receipt is emitted on any incomplete pair.
    """
    manifest = load_manifest(manifest_path, manifest_digest)
    job_id = manifest["job_id"]
    require(environment.get("GITHUB_RUN_ID") == job_id and
            environment.get("GITHUB_ACTIONS") == "true" and
            environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            callable(stage_pair), "SAME_JOB")
    lock = entrypoint.load_reviewed_lock(manifest["lock_path"],
                                         manifest["lock_sha256"])
    pin = lock["pin"]
    require(pin["source_commit"] == manifest["source_commit"] and
            pin["source_tree"] == manifest["source_tree"] and
            pin["assets_digest"] == manifest["assets_digest"], "MISSING_PIN")
    proof, receipt = entrypoint.attest_current_job(
        lock, job_id=job_id, repo=repo, runner_temp=runner_temp,
        environment=environment, operations=operations)
    qualification.validate(lock, proof, receipt, job_id=job_id,
                           expected_source_commit=pin["source_commit"],
                           expected_source_tree=pin["source_tree"],
                           expected_controller_commit=pin["controller_commit"],
                           expected_operator_sha256=pin["operator_sha256"])
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    output = _owned_directory(output)
    results = []
    for case in adapter.CASES:
        for trial in range(3):
            pair_id = f"{case}-{trial}"
            pair_output = _owned_directory(output / pair_id)
            _owned_directory(pair_output / "on")
            _owned_directory(pair_output / "off")
            config = {"schema": "alopex.sql-four-case-pair/v1",
                      "engine": "alopex-server", "case": case, "trial": trial,
                      "modes": ["on", "off"], "source": source,
                      "assets_digest": pin["assets_digest"],
                      "same_job_id": job_id,
                      "fixture_sha256": manifest["fixture_sha256"]}
            staged = stage_pair(config, pair_output, proof, receipt)
            require(type(staged) is dict and set(staged) ==
                    {"start", "identity_of", "subject_environment"} and
                    callable(staged["start"]) and callable(staged["identity_of"]) and
                    type(staged["subject_environment"]) is dict, "PAIR_STAGE")
            report = supervisor.run_owned_pair(
                config, pair_output, source=source, assets_digest=pin["assets_digest"],
                job_id=job_id, start=staged["start"],
                identity_of=staged["identity_of"],
                subject_environment=staged["subject_environment"],
                python=manifest["python"], package_root=manifest["package_root"],
                package_files=manifest["package_files"],
                docs=manifest["docs_harness_root"],
                product=manifest["product_harness_root"])
            require(report["status"] == "passed" and
                    report["primary_error"] is None and
                    report["worker_cleanup_error"] is None and
                    report["stop_error"] is None, "PAIR_FAILED")
            results.append({"case": case, "trial": trial, "status": "passed"})
    private = {"schema": SCHEMA, "status": "passed", "job_id": job_id,
               "source_commit": pin["source_commit"],
               "source_tree": pin["source_tree"],
               "assets_digest": pin["assets_digest"],
               "qualification_binding_sha256": receipt["binding_sha256"],
               "qualification_receipt": receipt, "pairs": results,
               "performance_qualification": False}
    private_digest = _write_json(output / "receipt.private.json", private)
    public = {"schema": SCHEMA, "status": "passed", "job_id": job_id,
              "source_commit": pin["source_commit"],
              "assets_digest": pin["assets_digest"],
              "qualification_binding_sha256": receipt["binding_sha256"],
              "private_receipt_sha256": private_digest, "pair_count": len(results),
              "performance_qualification": False}
    _write_json(output / "receipt.public.json", public)
    return public


def run_current_job(manifest_path, manifest_digest, *, repo, runner_temp,
                    environment, output, operations, stage_pair):
    """Run the matrix and emit a fixed-code public failure when it cannot pass."""
    try:
        return _run_current_job(
            manifest_path, manifest_digest, repo=repo, runner_temp=runner_temp,
            environment=environment, output=output, operations=operations,
            stage_pair=stage_pair)
    except BaseException as error:
        code = error.args[0] if type(error) is ValueError and error.args else "INTERNAL"
        allowed = {"MISSING_PIN", "PRIVATE_MANIFEST", "SAME_JOB", "QUALIFICATION",
                   "QUALIFICATION_BINDING", "POLICY", "CLEANUP", "IDENTITY",
                   "OPERATOR_IDENTITY", "OUTPUT_STATE", "PAIR_STAGE", "PAIR_FAILED"}
        public = {"schema": SCHEMA, "status": "failed",
                  "error_code": code if code in allowed else "INTERNAL",
                  "pair_count": 0, "performance_qualification": False}
        try:
            destination = Path(output)
            if not destination.exists():
                _owned_directory(destination)
            else:
                info = destination.lstat()
                require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                        stat.S_IMODE(info.st_mode) == 0o700, "OUTPUT_STATE")
            failure = {"schema": SCHEMA, "status": "failed",
                       "error_code": public["error_code"],
                       "performance_qualification": False}
            public["private_receipt_sha256"] = _write_json(
                destination / "receipt.private.json", failure)
            _write_json(destination / "receipt.public.json", public)
        except (OSError, ValueError):
            pass
        raise
