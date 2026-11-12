"""Read-only private adaptation of #831 qualified_same_job for 1951.

The caller must first run a fresh 1951 #804 qualification in this job. This
module does not acquire assets, create Docker resources, or dispatch cells.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys

from orchestrate import QUALIFICATION_CASES, SOURCE_COMMIT, SOURCE_TREE, require, sha256
from same_job_1951 import LOCK_SHA256, SECCOMP_SHA256, validate_pins

ASSET_RUN = 43156778998
ASSET_RUN_HEAD = "39657522a2712f8019880826d28e82567d040b70"
ARTIFACT_ID = 13916314157
ZIP_SHA256 = "c854d24d73b2f0367d59001b6af9c527903db527fcbc5d74d10bf9b258a5e2d4"
ASSETS_SHA256 = "dfd0ec3bd3a5079dcd0be0296891f7a6f624e103de3b23ee6b6a61db39729fc6"
SERVER_SHA256 = "bba07f935fb1c0d564cf7956d60eda0bd1ff577e131905590c252523c8ffd4f5"
WHEEL_SHA256 = "dd9f5787d87542a5698c2de39d1227f4e179700cd206297a68cf88ae4957f012"


def frozen_operations(repo):
    """Load #831 test-env APIs from the checked-out controller at runtime."""
    repo = Path(repo)
    sys.path.insert(0, str(repo / "scripts/test-env"))
    sys.path.insert(0, str(repo / "scripts/ops"))
    common = importlib.import_module("common")
    backend = importlib.import_module("docker_backend")
    envctl = importlib.import_module("envctl")
    lifecycle = importlib.import_module("lifecycle")
    qualification = importlib.import_module("hosted_test_env_qualification_1951")
    return {"read_json": common.read_json, "load_profile": common.load_profile,
            "verify_assets": common.verify_assets,
            "source_identity": envctl.source_identity,
            "docker_factory": backend.Docker,
            "verified_image": envctl.verified_image,
            "run_factory": lifecycle.Run,
            "verify_temp_snapshot": qualification.verify_temp_snapshot}


def attest_current_job(pins, job_id, *, repo, runner_temp, environment, operations):
    """Return only fixed proof fields consumed by derive_receipt()."""
    validate_pins(pins)
    require(pins["asset_run_id"] == ASSET_RUN and
            pins["asset_run_head"] == ASSET_RUN_HEAD and
            pins["artifact_id"] == ARTIFACT_ID and
            pins["artifact_sha256"] == pins["zip_sha256"] == ZIP_SHA256 and
            pins["assets_digest"] == ASSETS_SHA256 and
            pins["server_sha256"] == SERVER_SHA256 and
            pins["wheel_sha256"] == WHEEL_SHA256, "MISSING_ASSET")
    require(type(job_id) is str and job_id.isdecimal() and
            environment.get("GITHUB_RUN_ID") == job_id and
            environment.get("GITHUB_ACTIONS") == "true" and
            environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            environment.get("GITHUB_SHA") == environment.get("EXPECTED_CONTROLLER_SHA") ==
            pins["controller_commit"], "QUALIFICATION")
    require(type(operations) is dict and set(operations) == {
        "read_json", "load_profile", "verify_assets", "source_identity",
        "docker_factory", "verified_image", "run_factory", "verify_temp_snapshot"},
        "QUALIFICATION")
    repo, runner_temp = Path(repo), Path(runner_temp)
    require(repo.is_dir() and runner_temp.is_dir() and not runner_temp.is_symlink(),
            "QUALIFICATION")
    private = runner_temp / "alopex-804-qualification"
    candidate = runner_temp / "alopex-804-candidate"
    manifest_path = candidate / "assets/assets.json"
    public_path = repo / "native-qualification-export/public.json"
    profile_path = private / "profile.json"
    acquisition_path = private / "acquisition.private.json"
    admission_path = private / "admission.private.json"
    read = operations["read_json"]
    identity = operations["source_identity"]()
    require(identity == {"controller_commit": pins["controller_commit"],
                         "controller_digest": pins["controller_digest"]}, "IDENTITY")
    exported = read(public_path)
    require(exported["schema"] == "alopex.hosted-test-env-qualification/v1" and
            exported["outcome"] == "passed" and exported["error_code"] is None and
            exported["source_commit"] == SOURCE_COMMIT and
            exported["asset_digest"] == ASSETS_SHA256 and
            len(exported["cases"]) == len(QUALIFICATION_CASES) and
            {row["id"] for row in exported["cases"]} == set(QUALIFICATION_CASES) and
            all(row["status"] == "passed" and row["error_code"] is None
                for row in exported["cases"]), "QUALIFICATION")
    profile = read(profile_path, private=True)
    require(profile["run_id"] == "hosted-804-" + job_id and
            profile["assets"] == str(manifest_path) and
            profile["private_output"] == str(private / "runs") and
            profile["image_lock"] == str(private / "abi-image.lock.json") and
            profile["security"]["seccomp_profile"] == str(private / "seccomp-default.json") and
            profile["security"]["lsm_kind"] == "apparmor" and
            profile["security"]["lsm_profile"] == "docker-default" and
            profile["security"]["seccomp_sha256"] == SECCOMP_SHA256 and
            sha256(profile["image_lock"]) == LOCK_SHA256 and
            sha256(profile["security"]["seccomp_profile"]) == SECCOMP_SHA256 and
            sha256(manifest_path) == ASSETS_SHA256 and
            profile["storage"]["stop_bytes"] == 40_000_000_000 and
            profile["storage"]["reserve_bytes"] == 1_000_000_000 and
            profile["storage"]["poll_seconds"] == 10, "POLICY")
    operations["load_profile"](profile_path)
    acquisition = read(acquisition_path, private=True)
    require(acquisition == {"run": ASSET_RUN, "head": ASSET_RUN_HEAD,
                            "artifact_id": ARTIFACT_ID, "zip_sha256": ZIP_SHA256,
                            "assets_digest": ASSETS_SHA256} and
            sha256(private / "candidate.original.zip") == ZIP_SHA256, "QUALIFICATION_BINDING")
    manifest = operations["verify_assets"](manifest_path, profile["platform"])
    require(manifest["source_commit"] == SOURCE_COMMIT and
            manifest["harness_commit"] == pins["harness_commit"] and
            {item["role"] for item in manifest["files"]} ==
            {"server", "wheel", "library", "receipt", "harness"} and
            next(item for item in manifest["files"] if item["role"] == "server")["sha256"] == SERVER_SHA256 and
            next(item for item in manifest["files"] if item["role"] == "wheel")["sha256"] == WHEEL_SHA256,
            "QUALIFICATION_BINDING")
    product_source = {"commit": SOURCE_COMMIT, "tree": SOURCE_TREE, "profile": "release"}
    product_result = read(candidate / "result.json")
    require(product_result == {"schema": "alopex.native-assets/v1", "status": "passed",
                               "source": product_source, "assets_digest": ASSETS_SHA256,
                               "performance_qualification": False}, "QUALIFICATION_BINDING")
    receipt_entry = next(item for item in manifest["files"] if item["role"] == "receipt")
    product_receipt = read(manifest_path.parent / receipt_entry["path"])
    require(product_receipt["source"] == product_source, "QUALIFICATION_BINDING")
    docker = operations["docker_factory"](profile)
    docker.prerequisites()
    docker.policy()
    image_id = operations["verified_image"](profile, docker)
    require(type(image_id) is str and image_id.startswith("sha256:") and
            len(image_id) == 71, "IDENTITY")
    runs = {}
    primary = None
    for suffix, label in (("", "primary"), ("-repeat", "repeat"), ("-probe", "probe")):
        run_id = profile["run_id"] + suffix
        run = operations["run_factory"](profile["private_output"], run_id).load()
        require(run.record["phase"] == "deleted" and
                not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
        run.verify_collection()
        runs[label] = {"phase": "deleted", "collection_verified": True, "cleanup_errors": []}
        if label == "primary":
            primary = run
    acceptance_path = primary.root / "acceptance.json"
    acceptance = read(acceptance_path)
    require(sha256(acceptance_path) == primary.record["acceptance_digest"] and
            acceptance["status"] == "passed" and
            len(acceptance["cases"]) == len(QUALIFICATION_CASES) and
            {row["id"] for row in acceptance["cases"]} == set(QUALIFICATION_CASES) and
            all(row["status"] == "passed" for row in acceptance["cases"]), "QUALIFICATION")
    accepted = read(primary.root / "public.json")
    require(accepted["status"] == "passed" and
            accepted["assets_digest"] == ASSETS_SHA256 and
            accepted["image_digest"] == image_id[7:] and
            accepted["controller_commit"] == pins["controller_commit"] and
            accepted["controller_digest"] == pins["controller_digest"], "QUALIFICATION_BINDING")
    previous = read(admission_path, private=True)
    operations["verify_temp_snapshot"](previous["temp_snapshot"])
    require(profile["storage"] == previous["storage"], "POLICY")
    return {"job_id": job_id, "qualification_public": exported,
            "profile": {"run_id": profile["run_id"], "assets_digest": ASSETS_SHA256,
                        "image_lock_sha256": LOCK_SHA256,
                        "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                     "seccomp_sha256": SECCOMP_SHA256},
                        "storage": {"stop_bytes": 40_000_000_000,
                                    "reserve_bytes": 1_000_000_000, "poll_seconds": 10}},
            "acquisition": {"artifact_id": ARTIFACT_ID, "zip_sha256": ZIP_SHA256,
                            "assets_digest": ASSETS_SHA256},
            "product": {"source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
                        "artifact_sha256": ZIP_SHA256, "server_sha256": SERVER_SHA256,
                        "wheel_sha256": WHEEL_SHA256, "assets_digest": ASSETS_SHA256},
            "runs": runs, "acceptance": acceptance, "accepted_public": accepted,
            "image_digest": image_id[7:],
            "qualification_public_sha256": sha256(public_path),
            "acceptance_sha256": sha256(acceptance_path),
            "profile_sha256": sha256(profile_path),
            "temp_snapshot_verified": True}
