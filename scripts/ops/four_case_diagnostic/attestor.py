"""Live, source-specific adapter for a fresh four-case #804 qualification.

The lock and current job are independent inputs.  This module only reads an
already completed qualification; it never constructs or relaxes a container.
The caller supplies the #804 operations from its reviewed controller checkout.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import acquisition, qualification


OPERATIONS = frozenset(("read_json", "load_profile", "verify_assets",
                        "source_identity", "docker_factory", "verified_image",
                        "run_factory", "verify_temp_snapshot"))


def _digest(path):
    path = Path(path)
    qualification.require(path.is_file() and not path.is_symlink(), "IDENTITY")
    hashed = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1_048_576), b""):
            hashed.update(block)
    return hashed.hexdigest()


def _read(read, path, *, private=False):
    return read(path, private=True) if private else read(path)


def _receipt(lock, proof, job_id):
    pin = lock["pin"]
    binding = {"job_id": job_id,
               "qualification_public_sha256": proof["qualification_public_sha256"],
               "acceptance_sha256": proof["acceptance_sha256"],
               "profile_sha256": proof["profile_sha256"],
               "image_digest": proof["image_digest"],
               "assets_digest": pin["assets_digest"]}
    binding_digest = hashlib.sha256(json.dumps(binding, sort_keys=True,
                                               separators=(",", ":")).encode()).hexdigest()
    return {"job_id": job_id, "source_commit": pin["source_commit"],
            "source_tree": pin["source_tree"], "artifact_id": pin["artifact_id"],
            "artifact_sha256": pin["artifact_sha256"],
            "assets_digest": pin["assets_digest"],
            "server_sha256": pin["server_sha256"],
            "wheel_sha256": pin["wheel_sha256"],
            "controller_commit": pin["controller_commit"],
            "qualification_status": "passed",
            "acceptance_cases": sorted(qualification.CASES,
                                       key=lambda case: ("ESFRP".index(case[0]), case)),
            "image_digest": proof["image_digest"],
            "binding_sha256": binding_digest,
            "security_profile_digest": pin["seccomp_sha256"]}


def attest_current_job(lock, job_id, *, repo, runner_temp, environment, operations):
    """Return (authenticated proof, validated receipt) from current-job files.

    ``operations`` are the unmodified #804 APIs, loaded by the trusted caller.
    In particular, ``source_identity`` and the Docker/Run APIs are live checks,
    not values supplied by the private evidence bundle.
    """
    qualification.require(type(lock) is dict and type(lock.get("pin")) is dict and
                          type(job_id) is str and job_id.isdecimal() and
                          type(operations) is dict and set(operations) == OPERATIONS and
                          all(callable(value) for value in operations.values()),
                          "QUALIFICATION")
    pin = lock["pin"]
    qualification.require(type(environment) is dict and
                          environment.get("GITHUB_RUN_ID") == job_id and
                          environment.get("GITHUB_ACTIONS") == "true" and
                          environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
                          environment.get("GITHUB_SHA") ==
                          environment.get("EXPECTED_CONTROLLER_SHA") ==
                          pin.get("controller_commit"), "QUALIFICATION")
    repo, runner_temp = Path(repo), Path(runner_temp)
    qualification.require(repo.is_dir() and runner_temp.is_dir() and
                          not runner_temp.is_symlink(), "QUALIFICATION")
    private = runner_temp / "alopex-804-qualification"
    candidate = runner_temp / "alopex-804-candidate"
    manifest_path = candidate / "assets/assets.json"
    public_path = repo / "native-qualification-export/public.json"
    profile_path = private / "profile.json"
    acceptance_id = "hosted-804-" + job_id
    read = operations["read_json"]

    identity = operations["source_identity"]()
    qualification.require(identity == {"controller_commit": pin["controller_commit"],
                                     "controller_digest": pin["controller_digest"]},
                          "IDENTITY")
    qualification.require(type(pin["operator_sha256"]) is dict and
                          set(pin["operator_sha256"]) == qualification.REQUIRED_OPERATORS,
                          "OPERATOR_IDENTITY")
    for name, expected in pin["operator_sha256"].items():
        qualification.require(_digest(repo / "scripts/ops" / name) == expected,
                              "OPERATOR_IDENTITY")
    qualification.require(_digest(repo / "scripts/test-env/common.py") ==
                          pin["common_verifier_sha256"], "IDENTITY")
    acquisition_proof = acquisition.verify(
        lock, receipt_path=private / "acquisition.private.json",
        archive_path=private / "candidate.original.zip",
        manifest_path=manifest_path, job_id=job_id)
    exported = _read(read, public_path)
    profile = _read(read, profile_path, private=True)
    qualification.require(profile["run_id"] == acceptance_id and
                          profile["assets"] == str(manifest_path) and
                          profile["private_output"] == str(private / "runs") and
                          profile["image_lock"] == str(private / "image.lock.json") and
                          profile["security"]["seccomp_profile"] == str(private / "seccomp.json") and
                          _digest(profile["image_lock"]) == pin["image_lock_sha256"] and
                          _digest(profile["security"]["seccomp_profile"]) ==
                          pin["seccomp_sha256"], "POLICY")
    operations["load_profile"](profile_path)
    manifest = operations["verify_assets"](manifest_path, profile["platform"])
    by_role = {row["role"]: row for row in manifest["files"]}
    qualification.require(manifest["source_commit"] == pin["source_commit"] and
                          manifest["harness_commit"] == pin["harness_commit"] and
                          len(manifest["files"]) == len(qualification.ASSET_ROLES) and
                          set(by_role) == qualification.ASSET_ROLES and
                          all(by_role[role]["sha256"] == pin["asset_file_digests"][role]
                              for role in qualification.ASSET_ROLES),
                          "QUALIFICATION_BINDING")
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    product_result = _read(read, candidate / "result.json")
    qualification.require(product_result == {
        "schema": "alopex.native-assets/v1", "status": "passed", "source": source,
        "assets_digest": pin["assets_digest"], "performance_qualification": False},
        "QUALIFICATION_BINDING")
    product_receipt = _read(read, manifest_path.parent / by_role["receipt"]["path"])
    qualification.require(product_receipt.get("source") == source,
                          "QUALIFICATION_BINDING")
    docker = operations["docker_factory"](profile)
    docker.prerequisites()
    docker.policy()
    image_id = operations["verified_image"](profile, docker)
    qualification.require(type(image_id) is str and image_id.startswith("sha256:") and
                          len(image_id) == 71, "IDENTITY")
    runs = {}
    primary = None
    for suffix, label in (("", "primary"), ("-repeat", "repeat"), ("-probe", "probe")):
        run = operations["run_factory"](profile["private_output"],
                                        acceptance_id + suffix).load()
        qualification.require(run.record["phase"] == "deleted" and
                              not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
        run.verify_collection()
        runs[label] = {"phase": "deleted", "collection_verified": True,
                       "cleanup_errors": []}
        if label == "primary":
            primary = run
    acceptance_path = primary.root / "acceptance.json"
    acceptance = _read(read, acceptance_path)
    qualification.require(_digest(acceptance_path) == primary.record["acceptance_digest"],
                          "QUALIFICATION_BINDING")
    accepted = _read(read, primary.root / "public.json")
    previous = _read(read, private / "admission.private.json", private=True)
    operations["verify_temp_snapshot"](previous["temp_snapshot"])
    qualification.require(profile["storage"] == previous["storage"], "POLICY")
    proof = {"job_id": job_id, "qualification_public": exported,
             "profile": {"run_id": acceptance_id,
                         "assets_digest": pin["assets_digest"],
                         "image_lock_sha256": pin["image_lock_sha256"],
                         "security": {"lsm_kind": profile["security"]["lsm_kind"],
                                      "lsm_profile": profile["security"]["lsm_profile"],
                                      "seccomp_sha256": profile["security"]["seccomp_sha256"]},
                         "storage": profile["storage"]},
             "acquisition": acquisition_proof,
             "product": {"source_commit": pin["source_commit"],
                         "source_tree": pin["source_tree"],
                         "artifact_sha256": pin["artifact_sha256"],
                         "server_sha256": pin["server_sha256"],
                         "wheel_sha256": pin["wheel_sha256"],
                         "assets_digest": pin["assets_digest"]},
             "runs": runs, "acceptance": acceptance,
             "accepted_public": accepted, "image_digest": image_id[7:],
             "qualification_public_sha256": _digest(public_path),
             "acceptance_sha256": _digest(acceptance_path),
             "profile_sha256": _digest(profile_path),
             "temp_snapshot_verified": True}
    receipt = _receipt(lock, proof, job_id)
    qualification.validate(
        lock, proof, receipt, job_id=job_id,
        expected_source_commit=pin["source_commit"],
        expected_source_tree=pin["source_tree"],
        expected_controller_commit=pin["controller_commit"],
        expected_operator_sha256=pin["operator_sha256"])
    return proof, receipt
