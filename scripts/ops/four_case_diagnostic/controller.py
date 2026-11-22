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
MANIFEST_FIELDS = frozenset(("schema", "lock_sha256",
                             "source_commit", "source_tree", "assets_digest",
                             "fixture_sha256", "package_files", "docs_files",
                             "product_files"))


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
            manifest["schema"] == SCHEMA and
            all(type(manifest[key]) is str and qualification.HEX40.fullmatch(manifest[key])
                for key in ("source_commit", "source_tree")) and
            all(type(manifest[key]) is str and qualification.HEX64.fullmatch(manifest[key])
                for key in ("lock_sha256", "assets_digest", "fixture_sha256")) and
            type(manifest["package_files"]) is dict and
            set(manifest["package_files"]) == {
                f"four_case_diagnostic/{name}.py" for name in
                ("__init__", "adapter", "container_pair_entrypoint",
                 "log_parser", "operation", "oracle", "public_attribution", "public_timing",
                 "runner", "supervisor",
                 "worker")} and
            all(type(value) is str and qualification.HEX64.fullmatch(value)
                for value in manifest["package_files"].values()) and
            all(type(manifest[key]) is dict and 0 < len(manifest[key]) <= 32 and
                all(type(name) is str and name.endswith(".py") and
                    Path(name).name == name and
                    type(value) is str and qualification.HEX64.fullmatch(value)
                    for name, value in manifest[key].items())
                for key in ("docs_files", "product_files")) and
            {"subject_identity.py", "sql_resources.py"} <=
            set(manifest["docs_files"]) and
            "parity_v0811_measure.py" in manifest["product_files"],
            "PRIVATE_MANIFEST")
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


def _invalidate_receipts(output):
    """Remove any previous result before attempting this output namespace."""
    output = Path(output)
    if not output.exists() and not output.is_symlink():
        return
    info = output.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700, "OUTPUT_STATE")
    for name in ("receipt.public.json", "receipt.private.json"):
        path = output / name
        if path.exists() or path.is_symlink():
            item = path.lstat()
            require(stat.S_ISREG(item.st_mode) and item.st_uid == os.geteuid() and
                    stat.S_IMODE(item.st_mode) == 0o600, "OUTPUT_STATE")
            path.unlink()


def _read_owned_file(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and
                stat.S_IMODE(before.st_mode) == 0o600 and
                0 < before.st_size <= limit, "RECEIPT_IDENTITY")
        raw = os.read(fd, limit + 1)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                "RECEIPT_IDENTITY")
        return raw
    finally:
        os.close(fd)


def verify_public_receipt(output, *, job_id, run_attempt, binding_sha256):
    """Accept a result only for the caller's exact job, attempt and #804 proof."""
    require(type(job_id) is str and job_id.isdecimal() and
            type(run_attempt) is str and run_attempt.isdecimal() and
            type(binding_sha256) is str and
            qualification.HEX64.fullmatch(binding_sha256), "RECEIPT_IDENTITY")
    output = Path(output)
    public_path = output / "receipt.public.json"
    private_path = output / "receipt.private.json"
    try:
        public = json.loads(_read_owned_file(public_path, 4096),
                            object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
        private_raw = _read_owned_file(private_path, 16384)
        private = json.loads(private_raw,
                             object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
    except (OSError, ValueError, UnicodeError) as error:
        raise ValueError("RECEIPT_IDENTITY") from error
    require(type(public) is dict and type(private) is dict and
            public.get("schema") == private.get("schema") == SCHEMA and
            public.get("status") == private.get("status") == "passed" and
            public.get("job_id") == private.get("job_id") == job_id and
            public.get("run_attempt") == private.get("run_attempt") == run_attempt and
            public.get("qualification_binding_sha256") ==
            private.get("qualification_binding_sha256") == binding_sha256 and
            public.get("private_receipt_sha256") ==
            hashlib.sha256(private_raw).hexdigest() and
            public.get("pair_count") == 12 and
            type(private.get("pairs")) is list and len(private["pairs"]) == 12 and
            {(row.get("case"), row.get("trial"), row.get("status"))
             for row in private["pairs"] if type(row) is dict} ==
            {(case, trial, "passed") for case in adapter.CASES for trial in range(3)} and
            public.get("performance_qualification") is False and
            private.get("performance_qualification") is False, "RECEIPT_IDENTITY")
    return public


def verify_failed_receipt(output, *, run_attempt):
    """Bind a fixed public failure and completed count to its private receipt."""
    require(type(run_attempt) is str and run_attempt.isdecimal() and
            int(run_attempt) > 0, "RECEIPT_IDENTITY")
    output = Path(output)
    try:
        public = json.loads(_read_owned_file(output / "receipt.public.json", 4096),
                            object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
        private_raw = _read_owned_file(output / "receipt.private.json", 4096)
        private = json.loads(private_raw,
                             object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
    except (OSError, ValueError, UnicodeError) as error:
        raise ValueError("RECEIPT_IDENTITY") from error
    require(type(public) is dict and type(private) is dict and
            set(public) == {"schema", "status", "error_code", "pair_count",
                            "performance_qualification", "run_attempt",
                            "private_receipt_sha256"} and
            set(private) == {"schema", "status", "error_code", "pair_count",
                             "performance_qualification"} and
            public["schema"] == private["schema"] == SCHEMA and
            public["status"] == private["status"] == "failed" and
            public["run_attempt"] == run_attempt and
            public["performance_qualification"] is False and
            private["performance_qualification"] is False and
            type(public["pair_count"]) is int and
            0 <= public["pair_count"] <= 12 and
            public["pair_count"] == private["pair_count"] and
            type(public["error_code"]) is str and
            public["error_code"] == private["error_code"] and
            public["private_receipt_sha256"] == hashlib.sha256(private_raw).hexdigest(),
            "RECEIPT_IDENTITY")
    return public


def _run_current_job(manifest_path, manifest_digest, *, repo, runner_temp,
                     environment, output, operations, stage_pair, on_complete=None,
                     completed=None):
    """Attest #804, then run 12 independently staged owned-server pairs.

    ``stage_pair`` comes from the source-pinned wrapper and returns a deleted,
    collected owned Run for one pair. It must use the already qualified image
    and #804 security profile. No success receipt is emitted on an incomplete pair.
    """
    manifest = load_manifest(manifest_path, manifest_digest)
    job_id = environment.get("GITHUB_RUN_ID")
    run_attempt = environment.get("GITHUB_RUN_ATTEMPT")
    require(type(job_id) is str and job_id.isdecimal() and
            type(run_attempt) is str and run_attempt.isdecimal() and
            int(run_attempt) > 0 and
            environment.get("GITHUB_ACTIONS") == "true" and
            environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            environment.get("RUNNER_TEMP") == str(runner_temp) and
            environment.get("ALOPEX_FOUR_CASE_LOCK_SHA256") == manifest["lock_sha256"] and
            callable(stage_pair) and
            (on_complete is None or callable(on_complete)), "SAME_JOB")
    runner_temp = Path(runner_temp)
    lock_dir = runner_temp / "alopex-712-lock"
    lock_path = lock_dir / "lock.json"
    require(runner_temp.is_dir() and not runner_temp.is_symlink() and
            lock_dir.is_dir() and not lock_dir.is_symlink() and
            lock_dir.stat().st_uid == os.geteuid() and
            stat.S_IMODE(lock_dir.stat().st_mode) == 0o700 and
            lock_path.is_file() and not lock_path.is_symlink() and
            lock_path.stat().st_uid == os.geteuid() and
            stat.S_IMODE(lock_path.stat().st_mode) == 0o600, "MISSING_PIN")
    lock = entrypoint.load_reviewed_lock(lock_path,
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
    runs = {}
    run_roots = set()
    for case in adapter.CASES:
        for trial in range(3):
            pair_id = f"{case}-{trial}"
            pair_output = _owned_directory(output / pair_id)
            config = {"schema": "alopex.sql-four-case-pair/v1",
                      "engine": "alopex-server", "case": case, "trial": trial,
                      "modes": ["on", "off"], "source": source,
                      "assets_digest": pin["assets_digest"],
                      "same_job_id": job_id,
                      "fixture_sha256": manifest["fixture_sha256"],
                      "docs_files": manifest["docs_files"],
                      "product_files": manifest["product_files"]}
            run = stage_pair(config, pair_output, proof, receipt)
            require(hasattr(run, "root") and type(getattr(run, "record", None)) is dict and
                    callable(getattr(run, "verify_collection", None)) and
                    Path(run.root).is_absolute() and
                    Path(run.root) not in run_roots, "PAIR_STAGE")
            run_roots.add(Path(run.root))
            report = supervisor.run_collected_pair(config, run, proof, receipt)
            require(report["status"] == "passed" and
                    report["collection_verified"] is True and
                    report["owned_resources_cleaned"] is True, "PAIR_FAILED")
            runs[(case, trial)] = run
            results.append({"case": case, "trial": trial, "status": "passed"})
            if completed is not None:
                completed["pair_count"] = len(results)
    if on_complete is not None:
        on_complete(runs, lock, manifest, proof, receipt, job_id)
    private = {"schema": SCHEMA, "status": "passed", "job_id": job_id,
               "run_attempt": run_attempt,
               "source_commit": pin["source_commit"],
               "source_tree": pin["source_tree"],
               "assets_digest": pin["assets_digest"],
               "qualification_binding_sha256": receipt["binding_sha256"],
               "qualification_receipt": receipt, "pairs": results,
               "performance_qualification": False}
    private_digest = _write_json(output / "receipt.private.json", private)
    public = {"schema": SCHEMA, "status": "passed", "job_id": job_id,
              "run_attempt": run_attempt,
              "source_commit": pin["source_commit"],
              "assets_digest": pin["assets_digest"],
              "qualification_binding_sha256": receipt["binding_sha256"],
              "private_receipt_sha256": private_digest, "pair_count": len(results),
              "performance_qualification": False}
    _write_json(output / "receipt.public.json", public)
    return public


def run_current_job(manifest_path, manifest_digest, *, repo, runner_temp,
                    environment, output, operations, stage_pair, on_complete=None):
    """Run the matrix and emit a fixed-code public failure when it cannot pass."""
    completed = {"pair_count": 0}
    try:
        _invalidate_receipts(output)
        return _run_current_job(
            manifest_path, manifest_digest, repo=repo, runner_temp=runner_temp,
            environment=environment, output=output, operations=operations,
            stage_pair=stage_pair, on_complete=on_complete, completed=completed)
    except BaseException as error:
        code = error.args[0] if type(error) is ValueError and error.args else "INTERNAL"
        allowed = {"MISSING_PIN", "PRIVATE_MANIFEST", "SAME_JOB", "QUALIFICATION",
                   "QUALIFICATION_BINDING", "POLICY", "CLEANUP", "IDENTITY",
                   "OPERATOR_IDENTITY", "OUTPUT_STATE", "PAIR_STAGE", "PAIR_FAILED"}
        public = {"schema": SCHEMA, "status": "failed",
                  "error_code": code if code in allowed else "INTERNAL",
                  "pair_count": completed["pair_count"],
                  "performance_qualification": False}
        attempt = environment.get("GITHUB_RUN_ATTEMPT")
        if type(attempt) is str and attempt.isdecimal() and int(attempt) > 0:
            public["run_attempt"] = attempt
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
                       "pair_count": public["pair_count"],
                       "performance_qualification": False}
            public["private_receipt_sha256"] = _write_json(
                destination / "receipt.private.json", failure)
            _write_json(destination / "receipt.public.json", public)
        except (OSError, ValueError):
            pass
        raise
