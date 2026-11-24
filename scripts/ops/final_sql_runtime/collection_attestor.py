"""Read-only, current-job attestation of the 52 owned SQL collections.

Profile hashes, subject pins, and the archived harness worker are reviewed
inputs. Run records and receipts are observations, never independent pins.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile

from . import container, matrix, same_job

HARNESS_MEMBER = "docs/scripts/performance/server-sql-isolated-v0816.py"
FILES = ("measurement", "raw", "resources", "worker", "harness")


def require(value, code):
    if not value:
        raise ValueError(code)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def private_file(path):
    path = Path(path)
    require(path.is_absolute() and not path.is_symlink(), "PRIVATE_FILE")
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
            not info.st_mode & 0o077, "PRIVATE_FILE")
    return path.read_bytes()


def private_json(path):
    data = private_file(path)
    require(len(data) <= 4_000_000, "PRIVATE_JSON")
    try:
        return json.loads(data), data
    except (ValueError, UnicodeError) as error:
        raise ValueError("PRIVATE_JSON") from error


def _collection(root):
    manifest, _ = private_json(root / "collection.json")
    require(type(manifest) is dict and manifest, "COLLECTION_MANIFEST")
    actual = {}
    collected = root / "collected"
    require(collected.is_dir() and not collected.is_symlink(), "COLLECTION_MANIFEST")
    for path in collected.rglob("*"):
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode),
                "COLLECTION_MANIFEST")
        if stat.S_ISREG(info.st_mode):
            name = path.relative_to(collected).as_posix()
            actual[name] = sha(private_file(path))
    require(actual == manifest, "COLLECTION_MANIFEST")
    return actual


def _run(root, run_id, assets_digest, image_digest, controller_commit,
         controller_digest, docker):
    record, _ = private_json(root / "run.json")
    require(type(record) is dict and record.get("schema") == "alopex.test-env-run/v1" and
            record.get("run_id") == run_id and record.get("phase") == "deleted" and
            type(record.get("ownership_token")) is str and
            len(record["ownership_token"]) == 32 and
            type(record.get("resources")) is list and len(record["resources"]) == 3,
            "OWNED_RUN")
    identities = record.get("identities")
    require(type(identities) is dict and
            identities.get("assets_digest") == assets_digest and
            identities.get("image_digest") == image_digest and
            identities.get("controller_commit") == controller_commit and
            identities.get("controller_digest") == controller_digest and
            record.get("expected_cases") == [], "RUN_IDENTITY")
    names = {("container", "alopex-test-" + run_id),
             ("volume", "alopex-test-" + run_id + "-input"),
             ("volume", "alopex-test-" + run_id + "-work")}
    resources = record["resources"]
    require({(item.get("kind"), item.get("name")) for item in resources
             if type(item) is dict} == names and
            all(type(item.get("id")) is str and item["id"] for item in resources),
            "OWNED_RESOURCES")
    # A persisted deleted phase alone is insufficient: query the selected daemon.
    require(all(docker.inspect(item) is None for item in resources),
            "SURVIVING_RESOURCE")
    labels = ["org.alopex.test-env.run=" + run_id,
              "org.alopex.test-env.owner=envctl",
              "org.alopex.test-env.token=" + record["ownership_token"]]
    for resource in ("container", "volume"):
        args = [resource, "ls", "-q"]
        if resource == "container":
            args.append("-a")
        for label in labels:
            args.extend(("--filter", "label=" + label))
        require(docker.call(*args) == "", "SURVIVING_RESOURCE")
    _collection(root)
    return record


def _harness_bytes(path):
    with tarfile.open(path, "r") as archive:
        member = archive.getmember(HARNESS_MEMBER)
        require(member.isfile() and member.size < 4_000_000, "HARNESS_MEMBER")
        return archive.extractfile(member).read()


def expected_runs():
    rows = matrix.plan()
    preflights = [next(row for row in rows if row["case"] == "select-all" and
                  row["trial"] == 0 and row["subject"] == subject)
                  for subject in matrix.SUBJECTS]
    runs = [("preflight", row) for row in preflights] + [("cell", row) for row in rows]
    ids = [container.owned_run_id(kind, row) for kind, row in runs]
    require(len(ids) == len(set(ids)) == 52, "RUN_COVERAGE")
    return runs


def attest_collections(*, qualification_args, profile_for, profile_hashes, pins,
                       harness_path, docker_factory=None):
    """Return audit_bridge inputs after live qualification and owned cleanup checks.

    ``profile_hashes`` must be an independently reviewed map of all 52 profile
    file digests. No caller-supplied receipt or collection path is consumed.
    """
    qualification = same_job.attest_live(**qualification_args)
    environment = qualification_args["environment"]
    runner_temp = Path(qualification_args["runner_temp"]).resolve(strict=True)
    require(qualification.get("job_id") == environment.get("GITHUB_RUN_ID") and
            qualification.get("run_attempt") == environment.get("GITHUB_RUN_ATTEMPT"),
            "CURRENT_JOB")
    runs = expected_runs()
    expected_ids = [container.owned_run_id(kind, row) for kind, row in runs]
    require(type(profile_hashes) is dict and set(profile_hashes) == set(expected_ids) and
            type(pins) is dict and set(pins) == set(matrix.SUBJECTS), "REVIEWED_PINS")
    harness_path = Path(harness_path)
    harness_archive = private_file(harness_path)
    require(len(harness_archive) < 8_000_000, "HARNESS_ARCHIVE")
    harness_data = _harness_bytes(harness_path)
    if docker_factory is None:
        _, _, docker_backend, _, _ = container._runtime()
        docker_factory = docker_backend.Docker
    collections, receipts, preflight_collections, preflight_receipts = [], [], [], []
    sealed = {}
    for kind, row in runs:
        run_id = container.owned_run_id(kind, row)
        profile_path = Path(profile_for(kind, row))
        require(profile_path.is_absolute() and sha(private_file(profile_path)) ==
                profile_hashes[run_id], "PROFILE_PIN")
        common, _, _, _, lifecycle = container._runtime()
        profile = common.load_profile(profile_path)
        require(profile["run_id"] == run_id, "PROFILE_IDENTITY")
        require(Path(profile["private_output"]).resolve(strict=True).is_relative_to(runner_temp) and
                Path(profile["assets"]).resolve(strict=True).is_relative_to(runner_temp),
                "CURRENT_JOB_PATH")
        assets_path = Path(profile["assets"])
        assets_digest = sha(private_file(assets_path))
        pin = pins[row["subject"]]
        require(type(pin.get("assets_digest")) is str and
                len(pin["assets_digest"]) == 64 and
                type(pin.get("source")) is dict and
                type(pin.get("image_digest")) is str and
                len(pin["image_digest"]) == 64 and
                type(pin.get("controller_digest")) is str and
                len(pin["controller_digest"]) == 64, "SUBJECT_PIN")
        assets = common.verify_assets(assets_path, profile["platform"])
        require(assets.get("source_commit") == pin["source"].get("commit") and
                ("source_tree" not in assets or
                 assets["source_tree"] == pin["source"].get("tree")),
                "PRODUCT_SOURCE")
        config_path = assets_path.parent / "measurement.json"
        config, _ = private_json(config_path)
        require(type(config) is dict and config.get("schema") ==
                "alopex.native-sql-supervisor/v1" and
                config.get("phase") == kind and
                all(config.get(key) == value for key, value in row.items()) and
                config.get("source") == pin["source"], "STAGED_CONFIG")
        entries = {item["role"]: item for item in assets["files"]}
        product_files = [item for item in assets["files"] if item["role"] in
                         ("server", "wheel", "receipt", "library")]
        require(type(pin.get("files")) is list and
                sorted(product_files, key=lambda item: item["path"]) ==
                sorted(pin["files"], key=lambda item: item["path"]), "PRODUCT_PIN")
        require(entries.get("measurement_config", {}).get("sha256") == sha(private_file(config_path)) and
                entries.get("measurement_worker", {}).get("path") == "worker.py" and
                entries.get("harness", {}).get("path") == "measurement-harness.tar", "STAGED_ASSETS")
        worker_path = assets_path.parent / "worker.py"
        archive_path = assets_path.parent / "measurement-harness.tar"
        require(sha(private_file(worker_path)) == pin.get("worker_sha256") and
                sha(private_file(archive_path)) == sha(harness_archive) and
                _harness_bytes(archive_path) == harness_data and
                sha(harness_data) == pin.get("harness_sha256"), "OPERATOR_PIN")
        root = Path(profile["private_output"]) / run_id
        require(root.is_absolute() and root.is_dir() and not root.is_symlink(), "OWNED_RUN")
        docker = docker_factory(profile)
        _run(root, run_id, assets_digest, pin["image_digest"],
             environment["GITHUB_SHA"], pin["controller_digest"], docker)
        run = lifecycle.Run(profile["private_output"], run_id).load()
        container.verify_collected(run, config, kind, assets_digest, common=common)
        collected = root / "collected" / "cell"
        report, _ = private_json(root / "collected" / "result.json")
        require(type(report) is dict and report.get("status") == "passed" and
                report.get("phase") == kind and report.get("assets_digest") == assets_digest and
                report.get("source") == pin["source"] and
                all(report.get(key) == row[key] for key in ("case", "engine", "trial")),
                "COLLECTED_REPORT")
        coordinate = {key: row[key] for key in ("case", "trial", "subject")}
        if kind == "preflight":
            path = collected / "exact-result.json"
            digest = sha(private_file(path))
            preflight_collections.append({"subject": row["subject"], "result": path})
            receipt = {"subject": row["subject"], "phase": kind, "run_id": run_id,
                       "source": pin["source"], "assets_digest": pin["assets_digest"],
                       "cleanup": "deleted", "result_sha256": digest}
            preflight_receipts.append(receipt)
        else:
            files = {"measurement": collected / "measurement.json",
                     "raw": collected / "raw.ndjson",
                     "resources": collected / "resources.json",
                     "worker": worker_path,
                     "harness": harness_data}
            collections.append({**coordinate, "phase": kind, "run_id": run_id,
                                "files": files})
            receipt = {**coordinate, "phase": kind, "run_id": run_id,
                       "source": pin["source"], "assets_digest": pin["assets_digest"],
                       "cleanup": "deleted", "sha256":
                       {name: sha(path if name == "harness" else private_file(path))
                        for name, path in files.items()}}
            receipts.append(receipt)
        sealed[id(receipt)] = (sha(json.dumps(receipt, sort_keys=True).encode()),
                               docker, root, run_id, assets_digest,
                               pin["image_digest"], environment["GITHUB_SHA"],
                               pin["controller_digest"])

    def authenticate_receipt(receipt):
        entry = sealed.get(id(receipt))
        if entry is None or entry[0] != sha(json.dumps(receipt, sort_keys=True).encode()):
            return False
        _, docker, root, run_id, assets_digest, image_digest, controller_commit, controller_digest = entry
        try:
            _run(root, run_id, assets_digest, image_digest, controller_commit,
                 controller_digest, docker)
            return True
        except (ValueError, KeyError, TypeError):
            return False

    return {"collections": collections, "receipts": receipts,
            "preflight_collections": preflight_collections,
            "preflight_receipts": preflight_receipts,
            "authenticate_receipt": authenticate_receipt}
