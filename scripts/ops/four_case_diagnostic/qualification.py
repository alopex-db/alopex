"""Pure contract for a new #804 same-job qualification of four-case assets.

The four-case acquisition proof uses its own v2 schema, extending the 1951
receipt with exact ZIP byte size and current qualification job. A live attestor
must authenticate its private bytes and the container, image, and collection
before supplying ``proof``. This module does not turn self-report into evidence.
"""
from __future__ import annotations

import hashlib
import json
import re


CASES = frozenset(("E01", "E02", "E03", "E04", "E05", "E06", "E07",
                   "S01", "S02", "S03", "F01", "F02", "R01", "P01"))
PIN_FIELDS = frozenset(("source_commit", "source_tree", "artifact_id",
                        "artifact_sha256", "assets_digest", "server_sha256",
                        "wheel_sha256", "controller_commit", "controller_digest",
                        "asset_run_id", "asset_run_head", "zip_sha256",
                        "asset_zip_size_bytes", "asset_file_digests",
                        "common_verifier_sha256",
                        "harness_commit", "image_lock_sha256", "seccomp_sha256",
                        "operator_sha256"))
PROOF_FIELDS = frozenset(("job_id", "qualification_public", "profile",
                          "acquisition", "product", "runs", "acceptance",
                          "accepted_public", "image_digest",
                          "qualification_public_sha256", "acceptance_sha256",
                          "profile_sha256", "temp_snapshot_verified"))
RECEIPT_FIELDS = frozenset(("job_id", "source_commit", "source_tree",
                            "artifact_id", "artifact_sha256", "assets_digest",
                            "server_sha256", "wheel_sha256", "controller_commit",
                            "qualification_status", "acceptance_cases",
                            "image_digest", "binding_sha256",
                            "security_profile_digest"))
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
ASSET_ROLES = frozenset(("server", "wheel", "library", "receipt", "harness"))
REQUIRED_OPERATORS = frozenset({
    "hosted_test_env_qualification_four_case.py",
    "four_case_job.py",
    *(f"four_case_diagnostic/{name}.py" for name in (
        "__init__", "acquire", "acquisition", "adapter", "attestor", "audit",
        "container_pair_entrypoint", "controller", "docs_stage", "evidence_assembly",
        "entrypoint", "intake", "log_parser", "operation", "oracle", "public_attribution", "public_timing",
        "producer", "qualification", "runner", "stage_pair", "supervisor",
        "worker")),
})


def require(value, code):
    if not value:
        raise ValueError(code)


def _hex(value, pattern):
    return type(value) is str and pattern.fullmatch(value) is not None and len(set(value)) > 1


def _cases(rows, *, public):
    require(type(rows) is list and len(rows) == len(CASES), "QUALIFICATION")
    require(all(type(row) is dict and CASES.__contains__(row.get("id")) and
                row.get("status") == "passed" and
                (not public or row.get("error_code") is None)
                for row in rows), "QUALIFICATION")
    require({row["id"] for row in rows} == CASES, "QUALIFICATION")


def validate(lock, proof, receipt, *, job_id, expected_source_commit,
             expected_source_tree, expected_controller_commit,
             expected_operator_sha256):
    """Validate exact #804 proof/receipt fields against independently supplied pins.

    The caller must obtain the expected source, controller, and operator hashes
    from a trusted reviewed lock, and obtain ``proof`` from a fresh live attestor.
    """
    require(type(job_id) is str and job_id.isdecimal() and
            _hex(expected_source_commit, HEX40) and
            _hex(expected_source_tree, HEX40) and
            _hex(expected_controller_commit, HEX40), "MISSING_PIN")
    require(type(expected_operator_sha256) is dict and
            set(expected_operator_sha256) == REQUIRED_OPERATORS and
            all(type(name) is str and name.endswith(".py") and
                _hex(value, HEX64) for name, value in expected_operator_sha256.items()),
            "MISSING_PIN")
    require(type(lock) is dict and set(lock) == {"schema", "pin"} and
            lock["schema"] == "alopex.sql-four-case-pins/v1" and
            type(lock["pin"]) is dict and set(lock["pin"]) == PIN_FIELDS, "MISSING_PIN")
    pin = lock["pin"]
    require(pin["source_commit"] == expected_source_commit and
            pin["source_tree"] == expected_source_tree and
            pin["controller_commit"] == expected_controller_commit and
            pin["operator_sha256"] == expected_operator_sha256 and
            type(pin["artifact_id"]) is int and pin["artifact_id"] > 0 and
            type(pin["asset_run_id"]) is int and pin["asset_run_id"] > 0 and
            type(pin["asset_zip_size_bytes"]) is int and
            0 < pin["asset_zip_size_bytes"] < 60_000_000 and
            all(_hex(pin[key], HEX40) for key in ("asset_run_head", "harness_commit")) and
            all(_hex(pin[key], HEX64) for key in
                ("artifact_sha256", "assets_digest", "server_sha256", "wheel_sha256",
                 "controller_digest", "zip_sha256", "image_lock_sha256",
                 "common_verifier_sha256",
                 "seccomp_sha256")) and
            pin["artifact_sha256"] == pin["zip_sha256"] and
            type(pin["asset_file_digests"]) is dict and
            set(pin["asset_file_digests"]) == ASSET_ROLES and
            all(_hex(value, HEX64) for value in pin["asset_file_digests"].values()) and
            pin["asset_file_digests"]["server"] == pin["server_sha256"] and
            pin["asset_file_digests"]["wheel"] == pin["wheel_sha256"], "MISSING_PIN")
    require(type(proof) is dict and set(proof) == PROOF_FIELDS and
            proof["job_id"] == job_id and proof["temp_snapshot_verified"] is True,
            "QUALIFICATION")
    public = proof["qualification_public"]
    require(type(public) is dict and
            set(public) == {"schema", "case", "outcome", "error_code",
                            "source_commit", "asset_digest", "cases"} and
            public["schema"] == "alopex.hosted-test-env-qualification/v1" and
            public["case"] == "HOST_QUALIFICATION" and public["outcome"] == "passed" and
            public["error_code"] is None and public["source_commit"] == expected_source_commit and
            public["asset_digest"] == pin["assets_digest"], "QUALIFICATION")
    _cases(public["cases"], public=True)
    profile = proof["profile"]
    require(type(profile) is dict and
            set(profile) == {"run_id", "assets_digest", "image_lock_sha256",
                             "security", "storage"} and
            profile["run_id"] == "hosted-804-" + job_id and
            profile["assets_digest"] == pin["assets_digest"] and
            profile["image_lock_sha256"] == pin["image_lock_sha256"] and
            profile["security"] == {"lsm_kind": "apparmor",
                                    "lsm_profile": "docker-default",
                                    "seccomp_sha256": pin["seccomp_sha256"]} and
            profile["storage"] == {"stop_bytes": 40_000_000_000,
                                   "reserve_bytes": 1_000_000_000,
                                   "poll_seconds": 10}, "POLICY")
    require(proof["acquisition"] == {
                "schema": "alopex.sql-four-case-acquisition/v2",
                "qualification_job_id": job_id,
                "run": pin["asset_run_id"], "head": pin["asset_run_head"],
                "artifact_id": pin["artifact_id"],
                "zip_sha256": pin["zip_sha256"],
                "zip_size_bytes": pin["asset_zip_size_bytes"],
                "assets_digest": pin["assets_digest"]} and
            proof["product"] == {"source_commit": expected_source_commit,
                                  "source_tree": expected_source_tree,
                                  "artifact_sha256": pin["artifact_sha256"],
                                  "server_sha256": pin["server_sha256"],
                                  "wheel_sha256": pin["wheel_sha256"],
                                  "assets_digest": pin["assets_digest"]},
            "QUALIFICATION_BINDING")
    require(type(proof["runs"]) is dict and set(proof["runs"]) ==
            {"primary", "repeat", "probe"} and
            all(proof["runs"][name] == {"phase": "deleted",
                                        "collection_verified": True,
                                        "cleanup_errors": []}
                for name in ("primary", "repeat", "probe")), "CLEANUP")
    acceptance = proof["acceptance"]
    require(type(acceptance) is dict and acceptance.get("status") == "passed",
            "QUALIFICATION")
    _cases(acceptance.get("cases"), public=False)
    image = proof["image_digest"]
    accepted = proof["accepted_public"]
    require(_hex(image, HEX64) and type(accepted) is dict and
            accepted.get("status") == "passed" and
            accepted.get("assets_digest") == pin["assets_digest"] and
            accepted.get("image_digest") == image and
            accepted.get("controller_commit") == expected_controller_commit and
            accepted.get("controller_digest") == pin["controller_digest"] and
            all(_hex(proof[key], HEX64) for key in
                ("qualification_public_sha256", "acceptance_sha256", "profile_sha256")),
            "QUALIFICATION_BINDING")
    binding = {"job_id": job_id,
               "qualification_public_sha256": proof["qualification_public_sha256"],
               "acceptance_sha256": proof["acceptance_sha256"],
               "profile_sha256": proof["profile_sha256"],
               "image_digest": image, "assets_digest": pin["assets_digest"]}
    binding_sha256 = hashlib.sha256(json.dumps(binding, sort_keys=True,
                                               separators=(",", ":")).encode()).hexdigest()
    expected_receipt = {"job_id": job_id, "source_commit": expected_source_commit,
                        "source_tree": expected_source_tree,
                        "artifact_id": pin["artifact_id"],
                        "artifact_sha256": pin["artifact_sha256"],
                        "assets_digest": pin["assets_digest"],
                        "server_sha256": pin["server_sha256"],
                        "wheel_sha256": pin["wheel_sha256"],
                        "controller_commit": expected_controller_commit,
                        "qualification_status": "passed",
                        "acceptance_cases": list(("E01", "E02", "E03", "E04", "E05", "E06",
                                                  "E07", "S01", "S02", "S03", "F01", "F02",
                                                  "R01", "P01")),
                        "image_digest": image, "binding_sha256": binding_sha256,
                        "security_profile_digest": pin["seccomp_sha256"]}
    require(type(receipt) is dict and set(receipt) == RECEIPT_FIELDS and
            receipt == expected_receipt, "QUALIFICATION_BINDING")
    return {"source_commit": expected_source_commit, "source_tree": expected_source_tree,
            "assets_digest": pin["assets_digest"], "image_digest": image,
            "security_profile_digest": pin["seccomp_sha256"],
            "binding_sha256": binding_sha256, "job_id": job_id}
