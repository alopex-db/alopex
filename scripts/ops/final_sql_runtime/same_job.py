"""Fail-closed #712 gate for a fresh, live #804 qualification in this job.

The caller supplies the reviewed lock hash and expected source/operator pins
independently. ``entrypoint.attest_current_job`` authenticates the live #804
files, image, profile and owned cleanup; this gate additionally checks both
13-case collected reports and the derived R01 evidence before use by SQL work.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

from scripts.ops.four_case_diagnostic import entrypoint, qualification


EXECUTED = qualification.CASES - {"R01"}
PROBE = frozenset(("E01", "E02", "E04"))
RUNS = {"primary": "", "repeat": "-repeat", "probe": "-probe"}


def require(value, code):
    if not value:
        raise ValueError(code)


def _passed(report, cases):
    require(type(report) is dict and report.get("status") == "passed" and
            type(report.get("cases")) is list and len(report["cases"]) == len(cases) and
            all(type(row) is dict and row.get("id") in cases and
                row.get("status") == "passed" and row.get("error_code") is None
                for row in report["cases"]) and
            {row["id"] for row in report["cases"]} == cases, "CASES")


def verify(lock, proof, receipt, observations, *, expected, environment):
    """Check authenticated live observations against independent reviewed pins.

    ``observations`` is supplied by ``attest_live`` from the same private
    run roots. Injecting it permits focused offline contract tests, but an
    arbitrary caller-provided observation is not live evidence.
    """
    require(type(expected) is dict and set(expected) ==
            {"lock_sha256", "source_commit", "source_tree", "controller_commit",
             "operator_sha256"} and
            qualification.HEX64.fullmatch(expected["lock_sha256"]), "MISSING_PIN")
    require(type(environment) is dict and
            environment.get("GITHUB_ACTIONS") == "true" and
            environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            environment.get("GITHUB_REPOSITORY") == "alopex-db/alopex" and
            type(environment.get("GITHUB_RUN_ID")) is str and
            environment["GITHUB_RUN_ID"].isdecimal() and
            int(environment["GITHUB_RUN_ID"]) > 0 and
            type(environment.get("GITHUB_RUN_ATTEMPT")) is str and
            environment["GITHUB_RUN_ATTEMPT"].isdecimal() and
            int(environment["GITHUB_RUN_ATTEMPT"]) > 0 and
            environment.get("GITHUB_SHA") == expected["controller_commit"] ==
            environment.get("EXPECTED_CONTROLLER_SHA"), "SAME_JOB")
    job_id = environment["GITHUB_RUN_ID"]
    qualified = qualification.validate(
        lock, proof, receipt, job_id=job_id,
        expected_source_commit=expected["source_commit"],
        expected_source_tree=expected["source_tree"],
        expected_controller_commit=expected["controller_commit"],
        expected_operator_sha256=expected["operator_sha256"])
    require(type(observations) is dict and set(observations) == set(RUNS), "COLLECTION")
    run_id = "hosted-804-" + job_id
    for name, suffix in RUNS.items():
        item = observations[name]
        require(type(item) is dict and set(item) ==
                {"record", "result", "collection_sha256"} and
                type(item["record"]) is dict and
                item["record"].get("schema") == "alopex.test-env-run/v1" and
                item["record"].get("run_id") == run_id + suffix and
                item["record"].get("phase") == "deleted" and
                type(item["record"].get("resources")) is list and
                item["record"]["resources"] and
                all(type(resource) is dict and resource.get("id")
                    for resource in item["record"]["resources"]) and
                type(item["record"].get("ownership_token")) is str and
                item["record"]["ownership_token"] and
                qualification.HEX64.fullmatch(item["collection_sha256"]), "CLEANUP")
        identities = item["record"].get("identities")
        require(type(identities) is dict and
                identities.get("controller_commit") == expected["controller_commit"] and
                identities.get("controller_digest") == lock["pin"]["controller_digest"] and
                identities.get("assets_digest") == lock["pin"]["assets_digest"] and
                identities.get("image_digest") == proof["image_digest"],
                "QUALIFICATION_BINDING")
        _passed(item["result"], PROBE if name == "probe" else EXECUTED)
        expected_cases = (qualification.CASES if name == "primary" else
                          EXECUTED if name == "repeat" else PROBE)
        recorded_cases = item["record"].get("expected_cases")
        require(type(recorded_cases) is list and len(recorded_cases) ==
                len(expected_cases) and set(recorded_cases) == expected_cases,
                "CASES")
    primary = observations["primary"]["result"]
    repeat = observations["repeat"]["result"]
    signature = lambda report: [(row["id"], row["status"], row.get("error_code"))
                                for row in report["cases"]]
    require(signature(primary) == signature(repeat), "REPEAT")
    acceptance = proof["acceptance"]
    require(type(acceptance) is dict and type(acceptance.get("cases")) is list and
            len(acceptance["cases"]) == 14 and
            signature(acceptance)[0:13] == signature(primary) and
            acceptance["cases"][-1] == {
                "id": "R01", "status": "passed", "error_code": None,
                "evidence": {"first": observations["primary"]["collection_sha256"],
                             "repeat": observations["repeat"]["collection_sha256"]}},
            "R01")
    return {**qualified, "run_attempt": environment["GITHUB_RUN_ATTEMPT"],
            "qualification_cases": sorted(qualification.CASES)}


def _read_private(path, *, max_bytes=1_000_000):
    """Return parsed JSON and its exact checked bytes from one private file."""
    require(type(max_bytes) is int and 0 < max_bytes <= 1_000_000, "COLLECTION")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ValueError("COLLECTION") from error
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and
                before.st_uid == os.geteuid() and
                stat.S_IMODE(before.st_mode) == 0o600 and
                0 < before.st_size <= max_bytes, "COLLECTION")
        raw = os.read(fd, max_bytes + 1)
        after = os.fstat(fd)
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size,
                 before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size,
                 after.st_mtime_ns, after.st_ctime_ns), "COLLECTION")
    finally:
        os.close(fd)
    try:
        return (json.loads(raw, object_pairs_hook=
                           entrypoint.acquire.acquisition._unique_pairs), raw)
    except (ValueError, UnicodeError) as error:
        raise ValueError("COLLECTION") from error


def attest_live(*, lock_path, expected, repo, runner_temp, environment, operations=None):
    """Read only current-job evidence after the existing #804 live attestor.

    No historical receipt path is accepted. The reviewed lock hash and pins
    must come from outside the qualification output.
    """
    require(type(expected) is dict and "lock_sha256" in expected, "MISSING_PIN")
    runner_temp = Path(runner_temp)
    require(type(environment) is dict and
            environment.get("RUNNER_TEMP") == str(runner_temp) and
            Path(lock_path) == runner_temp / "alopex-712-lock/lock.json" and
            runner_temp.is_dir() and not runner_temp.is_symlink(), "SAME_JOB")
    lock = entrypoint.load_reviewed_lock(lock_path, expected["lock_sha256"])
    job_id = environment.get("GITHUB_RUN_ID")
    proof, receipt = entrypoint.attest_current_job(
        lock, job_id=job_id, repo=repo, runner_temp=runner_temp,
        environment=environment, operations=operations)
    private = Path(runner_temp) / "alopex-804-qualification" / "runs"
    observations = {}
    for name, suffix in RUNS.items():
        root = private / ("hosted-804-" + job_id + suffix)
        collection_manifest, collection_raw = _read_private(root / "collection.json")
        result, result_raw = _read_private(root / "collected/result.json")
        record, _ = _read_private(root / "run.json")
        require(type(collection_manifest) is dict and
                collection_manifest.get("result.json") ==
                hashlib.sha256(result_raw).hexdigest(), "COLLECTION")
        observations[name] = {
            "record": record,
            "result": result,
            "collection_sha256": hashlib.sha256(collection_raw).hexdigest()}
    return verify(lock, proof, receipt, observations,
                  expected=expected, environment=environment)
