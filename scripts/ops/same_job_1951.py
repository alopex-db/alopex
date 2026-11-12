"""Private #831 same-job receipt/cell-preparation adaptation for source 1951.

The frozen qualifier must still produce and verify the live evidence bundle.
There is deliberately no default attestor, workflow or Docker entrypoint.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil

from orchestrate import (QUALIFICATION_CASES, SOURCE_COMMIT, SOURCE_TREE,
                         commit, digest, plan, require, sha256)

LOCK_SHA256 = "37d2499475142c5f5341296c3ce90e5a2d7b4d95a803375ee56dacf7932b50f7"
SECCOMP_SHA256 = "536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74"
PIN_FIELDS = {"source_commit", "source_tree", "artifact_id", "artifact_sha256",
              "assets_digest", "server_sha256", "wheel_sha256", "controller_commit",
              "controller_digest", "asset_run_id", "asset_run_head", "zip_sha256",
              "harness_commit", "image_lock_sha256", "seccomp_sha256", "operator_sha256"}
OPERATORS = {"measurement_controller": ("diagnostic_supervisor.py", "controller.py"),
             "measurement_worker": ("diagnostic_worker.py", "worker.py"),
             "diagnostic_measure": ("diagnostic_measure.py", "diagnostic_measure.py"),
             "diagnostic_adapter": ("diagnostic.py", "diagnostic.py")}


def _real_digest(value):
    return digest(value) and len(set(value)) > 1


def _real_commit(value):
    return commit(value) and len(set(value)) > 1


def validate_pins(pins):
    require(type(pins) is dict and set(pins) == PIN_FIELDS, "MISSING_PIN")
    require(pins["source_commit"] == SOURCE_COMMIT and pins["source_tree"] == SOURCE_TREE and
            type(pins["artifact_id"]) is int and pins["artifact_id"] > 0 and
            type(pins["asset_run_id"]) is int and pins["asset_run_id"] > 0 and
            _real_commit(pins["controller_commit"]) and
            _real_commit(pins["asset_run_head"]) and
            _real_commit(pins["harness_commit"]) and
            all(_real_digest(pins[key]) for key in
                ("artifact_sha256", "assets_digest", "server_sha256", "wheel_sha256",
                 "controller_digest", "zip_sha256")) and
            pins["image_lock_sha256"] == LOCK_SHA256 and
            pins["seccomp_sha256"] == SECCOMP_SHA256 and
            type(pins["operator_sha256"]) is dict and
            all(_real_digest(pins["operator_sha256"].get(name)) for name, _ in OPERATORS.values()),
            "MISSING_PIN")
    return pins


def derive_receipt(pins, *, job_id, attestor):
    """Consume only a current-job bundle from a frozen-qualification attestor.

    The real attestor must verify file hashes, Run.verify_collection, image,
    policy, temp snapshot and deletion. It is intentionally not implemented.
    """
    validate_pins(pins)
    require(type(job_id) is str and job_id.isdecimal() and callable(attestor), "QUALIFICATION")
    proof = attestor(pins, job_id)
    require(type(proof) is dict and set(proof) == {
        "job_id", "qualification_public", "profile", "acquisition", "product",
        "runs", "acceptance", "accepted_public", "image_digest",
        "qualification_public_sha256", "acceptance_sha256", "profile_sha256",
        "temp_snapshot_verified"}, "QUALIFICATION")
    public, profile, acquisition = (proof[key] for key in
                                    ("qualification_public", "profile", "acquisition"))
    require(proof["job_id"] == job_id and public["schema"] ==
            "alopex.hosted-test-env-qualification/v1" and public["outcome"] == "passed" and
            public["error_code"] is None and public["source_commit"] == SOURCE_COMMIT and
            public["asset_digest"] == pins["assets_digest"] and
            {row["id"] for row in public["cases"]} == set(QUALIFICATION_CASES) and
            len(public["cases"]) == len(QUALIFICATION_CASES) and
            all(row["status"] == "passed" and row["error_code"] is None
                for row in public["cases"]), "QUALIFICATION")
    require(profile["run_id"] == "hosted-804-" + job_id and
            profile["assets_digest"] == pins["assets_digest"] and
            profile["image_lock_sha256"] == LOCK_SHA256 and
            profile["security"] == {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                    "seccomp_sha256": SECCOMP_SHA256} and
            profile["storage"]["stop_bytes"] == 40_000_000_000 and
            profile["storage"]["reserve_bytes"] == 1_000_000_000 and
            profile["storage"]["poll_seconds"] == 10 and
            proof["temp_snapshot_verified"] is True, "POLICY")
    require(acquisition == {"artifact_id": pins["artifact_id"],
                            "zip_sha256": pins["zip_sha256"],
                            "assets_digest": pins["assets_digest"]} and
            proof["product"] == {"source_commit": SOURCE_COMMIT,
                                 "source_tree": SOURCE_TREE,
                                 "artifact_sha256": pins["artifact_sha256"],
                                 "server_sha256": pins["server_sha256"],
                                 "wheel_sha256": pins["wheel_sha256"],
                                 "assets_digest": pins["assets_digest"]}, "QUALIFICATION_BINDING")
    require(set(proof["runs"]) == {"primary", "repeat", "probe"} and
            all(proof["runs"][name] == {"phase": "deleted", "collection_verified": True,
                                         "cleanup_errors": []}
                for name in ("primary", "repeat", "probe")), "CLEANUP")
    acceptance = proof["acceptance"]
    require(acceptance["status"] == "passed" and
            len(acceptance["cases"]) == len(QUALIFICATION_CASES) and
            {row["id"] for row in acceptance["cases"]} == set(QUALIFICATION_CASES) and
            all(row["status"] == "passed" for row in acceptance["cases"]), "QUALIFICATION")
    accepted = proof["accepted_public"]
    require(_real_digest(proof["image_digest"]) and
            accepted["status"] == "passed" and
            accepted["assets_digest"] == pins["assets_digest"] and
            accepted["image_digest"] == proof["image_digest"] and
            accepted["controller_commit"] == pins["controller_commit"] and
            accepted["controller_digest"] == pins["controller_digest"] and
            all(_real_digest(proof[key]) for key in
                ("qualification_public_sha256", "acceptance_sha256", "profile_sha256")),
            "QUALIFICATION_BINDING")
    binding = {"job_id": job_id, "qualification_public_sha256": proof["qualification_public_sha256"],
               "acceptance_sha256": proof["acceptance_sha256"],
               "profile_sha256": proof["profile_sha256"],
               "image_digest": proof["image_digest"], "assets_digest": pins["assets_digest"]}
    binding_sha256 = hashlib.sha256(json.dumps(binding, sort_keys=True,
                                               separators=(",", ":")).encode()).hexdigest()
    return {"job_id": job_id, "source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
            "artifact_id": pins["artifact_id"], "artifact_sha256": pins["artifact_sha256"],
            "assets_digest": pins["assets_digest"], "server_sha256": pins["server_sha256"],
            "wheel_sha256": pins["wheel_sha256"],
            "controller_commit": pins["controller_commit"],
            "qualification_status": "passed", "acceptance_cases": list(QUALIFICATION_CASES),
            "image_digest": proof["image_digest"], "binding_sha256": binding_sha256,
            "security_profile_digest": SECCOMP_SHA256}


def _contained(root, name):
    relative = PurePosixPath(name)
    require(not relative.is_absolute() and relative.parts and ".." not in relative.parts,
            "IDENTITY")
    path = root.joinpath(*relative.parts)
    require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve()), "IDENTITY")
    return path


def prepare_diagnostic_cell(product_manifest, destination, harness, harness_record,
                            cell, pins, operators, *, verify_assets):
    """Private candidate-only copy of frozen prepare_cell, with four pinned operators."""
    validate_pins(pins)
    product_manifest, destination, harness = map(Path, (product_manifest, destination, harness))
    require(not destination.exists() and cell in plan() and
            cell["case"] in ("aggregate", "ordered") and
            type(operators) is dict and set(operators) == set(OPERATORS) and
            callable(verify_assets), "INPUT")
    require(product_manifest.is_file() and not product_manifest.is_symlink() and
            product_manifest.stat().st_size <= 65_536 and
            sha256(product_manifest) == pins["assets_digest"], "MISSING_ASSET")
    source = verify_assets(product_manifest, "linux/amd64")
    require(source["source_commit"] == SOURCE_COMMIT and
            source["harness_commit"] == pins["harness_commit"] and
            len(source["files"]) == 5 and
            {item["role"] for item in source["files"]} ==
            {"server", "wheel", "receipt", "library", "harness"}, "IDENTITY")
    by_role = {item["role"]: item for item in source["files"]}
    require(by_role["server"]["sha256"] == pins["server_sha256"] and
            by_role["wheel"]["sha256"] == pins["wheel_sha256"], "MISSING_ASSET")
    original_receipt = json.loads(_contained(product_manifest.parent,
                                            by_role["receipt"]["path"]).read_text())
    require(original_receipt["source"] == {"commit": SOURCE_COMMIT,
                                          "tree": SOURCE_TREE, "profile": "release"}, "IDENTITY")
    require(harness.is_file() and not harness.is_symlink() and
            sha256(harness) == harness_record["sha256"] and
            harness.stat().st_size <= 8_000_000 and
            all(key in harness_record for key in
                ("docs_subdirectory", "product_subdirectory", "docs_files", "product_files")),
            "IDENTITY")
    for role, (source_name, _) in OPERATORS.items():
        record = operators[role]
        path = Path(record["path"])
        expected = pins["operator_sha256"][source_name]
        require(record["sha256"] == expected and path.is_file() and
                not path.is_symlink() and sha256(path) == expected, "OPERATOR_IDENTITY")
    destination.mkdir(mode=0o700, parents=False)
    manifest = copy.deepcopy(source)
    manifest["files"] = []
    for item in source["files"]:
        if item["role"] == "harness":
            continue
        source_path = _contained(product_manifest.parent, item["path"])
        target = _contained(destination, item["path"])
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(source_path.is_file() and not source_path.is_symlink() and
                sha256(source_path) == item["sha256"], "IDENTITY")
        shutil.copyfile(source_path, target)
        manifest["files"].append(item)

    def add(role, name):
        path = destination / name
        manifest["files"].append({"role": role, "path": name,
                                  "size": path.stat().st_size, "sha256": sha256(path)})

    shutil.copyfile(harness, destination / "measurement-harness.tar")
    add("harness", "measurement-harness.tar")
    config = {"schema": "alopex.sql-diagnostic-supervisor/v1",
              "engine": "alopex-server", **cell,
              "source": original_receipt["source"],
              **{key: harness_record[key] for key in
                 ("docs_subdirectory", "product_subdirectory", "docs_files", "product_files")}}
    (destination / "measurement.json").write_text(json.dumps(config, sort_keys=True) + "\n")
    add("measurement_config", "measurement.json")
    for role, (source_name, target_name) in OPERATORS.items():
        record = operators[role]
        path = Path(record["path"])
        shutil.copyfile(path, destination / target_name)
        add(role, target_name)
    output = destination / "assets.json"
    output.write_text(json.dumps(manifest, sort_keys=True) + "\n")
    verify_assets(output, "linux/amd64")
    return output, config
