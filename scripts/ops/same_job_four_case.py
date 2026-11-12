"""Run #712 four-case SQL after real #804 acceptance in this same GitHub job.

Integration candidate only: source must be reviewed and committed with its sibling
four-case modules before a workflow dispatch. No security profile is altered.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))

from capacity import Budget  # noqa: E402
from common import CASES, atomic_json, digest, private_dir, read_json, require, verify_assets  # noqa: E402
from docker_backend import Docker  # noqa: E402
from envctl import source_identity, verified_image  # noqa: E402
from lifecycle import Run  # noqa: E402
import hosted_test_env_qualification as qualification  # noqa: E402
from four_case_operation import operation  # noqa: E402

CONFIG = HERE / "same-job-four-case-config.private.json"
EXPORT = REPO / "native-sql-four-case-export"
CERT = REPO / ".github/ops/native-evidence-cert.pem"


def fixed_config():
    config = json.loads(CONFIG.read_text())
    require(config["schema"] == "alopex.same-job-four-case-config/v1" and
            config["platform"] == "linux/amd64" and
            config["native_acceptance"] == {
                "mode": "same-job-804",
                "candidate_assets_digest": qualification.ASSETS_DIGEST,
                "required_cases": len(CASES)}, "IDENTITY")
    require(config["candidate_commit"] == qualification.PRODUCT and
            config["subjects"]["candidate"]["assets_digest"] == qualification.ASSETS_DIGEST and
            config["matrix_seconds"] == 7200 and
            config["limits"] == {"case_seconds": 300, "run_seconds": 420} and
            config["matrix_additional_bound_bytes"] == 4_000_000_000 and
            config["matrix_storage_reserve_bytes"] == 1_000_000_000, "IDENTITY")
    require(digest(REPO / ".github/ops/qualification/abi-image.lock.json") ==
            config["image_lock_sha256"] and
            digest(REPO / ".github/ops/qualification/seccomp-default.json") ==
            config["seccomp_sha256"], "IDENTITY")
    for role, record in config["operators"].items():
        require(role in ("measurement_controller", "measurement_worker") and
                digest(HERE / record["filename"]) == record["sha256"], "IDENTITY")
    return config


def qualified_same_job(config, private):
    """Bind to actual acceptance files produced earlier in this very job."""
    require(os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            os.environ.get("GITHUB_ACTIONS") == "true", "POLICY")
    identity = source_identity()
    require(identity["controller_commit"] == os.environ.get("GITHUB_SHA") ==
            os.environ.get("EXPECTED_CONTROLLER_SHA"), "IDENTITY")
    exported = read_json(REPO / "native-qualification-export/public.json")
    require(exported["schema"] == "alopex.hosted-test-env-qualification/v1" and
            exported["outcome"] == "passed" and exported["error_code"] is None and
            exported["source_commit"] == config["candidate_commit"] and
            exported["asset_digest"] == qualification.ASSETS_DIGEST and
            len(exported["cases"]) == len(CASES) and
            {row["id"] for row in exported["cases"]} == set(CASES) and
            all(row["status"] == "passed" and row["error_code"] is None
                for row in exported["cases"]), "IDENTITY")
    profile_path = private / "profile.json"
    profile = read_json(profile_path, private=True)
    require(profile["run_id"] == "hosted-804-" + os.environ["GITHUB_RUN_ID"] and
            profile["assets"] == str(Path(os.environ["RUNNER_TEMP"]) /
                                     "alopex-804-candidate/assets/assets.json") and
            digest(profile["assets"]) == qualification.ASSETS_DIGEST and
            digest(profile["image_lock"]) == config["image_lock_sha256"] and
            digest(profile["security"]["seccomp_profile"]) == config["seccomp_sha256"] and
            profile["security"]["lsm_kind"] == "apparmor" and
            profile["security"]["lsm_profile"] == "docker-default", "IDENTITY")
    acquisition = read_json(private / "acquisition.private.json", private=True)
    require(acquisition["assets_digest"] == qualification.ASSETS_DIGEST and
            acquisition["artifact_id"] == config["subjects"]["candidate"]["artifact_id"] and
            acquisition["zip_sha256"] ==
            config["subjects"]["candidate"]["artifact_digest"].removeprefix("sha256:") and
            digest(private / "candidate.original.zip") == acquisition["zip_sha256"], "IDENTITY")
    from common import load_profile
    load_profile(profile_path)
    docker = Docker(profile)
    docker.prerequisites()
    docker.policy()
    image_id = verified_image(profile, docker)
    primary = Run(profile["private_output"], profile["run_id"]).load()
    for name in (profile["run_id"], profile["run_id"] + "-repeat",
                 profile["run_id"] + "-probe"):
        run = Run(profile["private_output"], name).load()
        require(run.record["phase"] == "deleted" and
                not (run.root / "cleanup-errors.json").exists(), "CLEANUP")
        run.verify_collection()
    acceptance = read_json(primary.root / "acceptance.json")
    require(digest(primary.root / "acceptance.json") ==
            primary.record["acceptance_digest"] and
            acceptance["status"] == "passed" and
            len(acceptance["cases"]) == len(CASES) and
            {row["id"] for row in acceptance["cases"]} == set(CASES) and
            all(row["status"] == "passed" for row in acceptance["cases"]), "IDENTITY")
    accepted_public = read_json(primary.root / "public.json")
    require(accepted_public["status"] == "passed" and
            accepted_public["assets_digest"] == qualification.ASSETS_DIGEST and
            accepted_public["image_digest"] == image_id.removeprefix("sha256:") and
            accepted_public["controller_commit"] == identity["controller_commit"] and
            accepted_public["controller_digest"] == identity["controller_digest"], "IDENTITY")
    previous = read_json(private / "admission.private.json", private=True)
    qualification.verify_temp_snapshot(previous["temp_snapshot"])
    require(profile["storage"] == previous["storage"], "IDENTITY")
    return profile, image_id


def verify_product(root, expected):
    manifest_path = root / "assets/assets.json"
    require(digest(manifest_path) == expected["assets_digest"], "IDENTITY")
    manifest = verify_assets(manifest_path, expected["platform"])
    require(manifest["source_commit"] == expected["source_commit"] and
            manifest["harness_commit"] == expected["harness_commit"] and
            {item["role"] for item in manifest["files"]} ==
            {"server", "wheel", "library", "receipt", "harness"}, "IDENTITY")
    result = read_json(root / "result.json")
    source = {"commit": expected["source_commit"], "tree": expected["source_tree"],
              "profile": "release"}
    require(result == {"schema": "alopex.native-assets/v1", "status": "passed",
                       "source": source, "assets_digest": expected["assets_digest"],
                       "performance_qualification": False}, "IDENTITY")
    receipt = next(item for item in manifest["files"] if item["role"] == "receipt")
    record = read_json(root / "assets" / receipt["path"])
    require(record["source"] == source and record["version"] == expected["version"],
            "IDENTITY")


def reference_archive(role, expected, destination):
    """Fetch only pinned original before/published ZIPs and reject extra entries."""
    run = qualification.gh_api(f"/actions/runs/{expected['run']}")
    require(run["status"] == "completed" and run["conclusion"] == "success" and
            run["head_sha"] == expected["controller"] and run["run_attempt"] == 1,
            "IDENTITY")
    listed = qualification.gh_api(f"/actions/runs/{expected['run']}/artifacts?per_page=100")
    require(listed["total_count"] == len(listed["artifacts"]) == 1, "IDENTITY")
    artifact = listed["artifacts"][0]
    require(artifact["id"] == expected["artifact_id"] and
            artifact["name"] == expected["artifact_name"] == "native-product-assets" and
            artifact["digest"] == expected["artifact_digest"] and
            artifact["workflow_run"]["head_sha"] == expected["controller"] and
            not artifact["expired"] and 0 < artifact["size_in_bytes"] < 60_000_000,
            "IDENTITY")
    raw = qualification.gh_api(f"/actions/artifacts/{artifact['id']}/zip", binary=True,
                               timeout=180)
    require(len(raw) == artifact["size_in_bytes"] and
            hashlib.sha256(raw).hexdigest() ==
            expected["artifact_digest"].removeprefix("sha256:"), "IDENTITY")
    require(not destination.exists(), "STATE")
    private_dir(destination)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        names = [item.filename for item in entries]
        require(len(entries) == 8 and len(names) == len(set(names)) and
                sum(item.file_size for item in entries) < 100_000_000 and
                {"assets/assets.json", "result.json", "private-evidence.cms"} <= set(names),
                "ARCHIVE")
        for item in entries:
            name = PurePosixPath(item.filename)
            mode = item.external_attr >> 16
            require(not item.is_dir() and not name.is_absolute() and ".." not in name.parts and
                    "\\" not in item.filename and
                    (item.filename in ("result.json", "private-evidence.cms") or
                     item.filename.startswith("assets/")) and
                    stat.S_IFMT(mode) in (0, stat.S_IFREG), "ARCHIVE")
            target = destination.joinpath(*name.parts)
            private_dir(target.parent)
            with archive.open(item) as source, target.open("xb") as output:
                while block := source.read(1_048_576):
                    output.write(block)
            target.chmod(0o600)
    verify_product(destination, expected)
    return {"role": role, "run": expected["run"], "artifact_id": artifact["id"],
            "zip_sha256": expected["artifact_digest"].removeprefix("sha256:"),
            "assets_digest": expected["assets_digest"]}


def preserve_evidence(work, public, config, storage):
    private_dir(EXPORT)
    files = [path for path in sorted(work.rglob("*")) if path.is_file()]
    require(all(not path.is_symlink() for path in files) and
            sum(path.stat().st_size for path in files) <
            config["matrix_evidence_bound_bytes"], "CAPACITY")
    # The work tree remains allocated while both the tar and CMS outputs exist.
    # Reserve both copies, framing overhead, and the host's separate 1 GB reserve
    # before writing either one; the archive budget scans all admitted roots.
    archive_bytes = 2 * config["matrix_evidence_bound_bytes"] + 32_000_000
    archive_storage = dict(storage, planned_bytes=dict(storage["planned_bytes"],
                                                  evidence_archive=archive_bytes))
    archive_budget = Budget(archive_storage)
    archive_budget.before("evidence_archive")
    # A plaintext archive must never enter the artifact upload directory.
    archive = work / "private-evidence.tar.gz"
    encrypted = EXPORT / "private-evidence.cms"
    archive_error = None
    try:
        with tarfile.open(archive, "w:gz") as bundle:
            for index, path in enumerate(files):
                bundle.add(path, arcname=str(path.relative_to(work)), recursive=False)
                if index % 16 == 0:
                    archive_budget.check()
        archive_budget.check()
        require(archive.stat().st_size <= config["matrix_evidence_bound_bytes"] +
                16_000_000, "CAPACITY")
        archive_budget.run(["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
                            "-outform", "DER", "-in", str(archive), "-out", str(encrypted),
                            str(CERT)], timeout=120, log=work / "archive-command.private.log")
        require(encrypted.is_file() and
                encrypted.stat().st_size <= config["matrix_evidence_bound_bytes"] +
                16_000_000, "CAPACITY")
        archive_budget.check()
    except BaseException as error:
        archive_error = error
        raise
    finally:
        try:
            archive.unlink(missing_ok=True)
            if archive_error is None:
                archive_budget.check()
        except BaseException as cleanup_error:
            if archive_error is None:
                raise
            from common import record_cleanup_error
            record_cleanup_error(archive_error, "remove_plaintext_evidence", cleanup_error)
    atomic_json(EXPORT / "result.json", public)


def main():
    os.umask(0o077)
    config = fixed_config()
    private = Path(os.environ["RUNNER_TEMP"]) / "alopex-804-qualification"
    work = private_dir(Path(os.environ["RUNNER_TEMP"]) / "alopex-712-four-case")
    public = {"schema": "alopex.native-sql-four-case-matrix/v1", "status": "failed",
              "performance_qualification": False, "owned_resources_cleaned": False}
    owned = []
    budget = None
    profile = None
    primary = None
    try:
        profile, image_id = qualified_same_job(config, private)
        before = Budget(profile["storage"]).used()
        require(before + config["matrix_additional_bound_bytes"] +
                config["matrix_storage_reserve_bytes"] < 40_000_000_000, "CAPACITY")
        atomic_json(work / "same-job-acceptance.json", {
            "qualification_run": os.environ["GITHUB_RUN_ID"],
            "qualification_public_sha256": digest(REPO / "native-qualification-export/public.json"),
            "acceptance_sha256": digest(Path(profile["private_output"]) /
                                        profile["run_id"] / "acceptance.json"),
            "image_id": image_id, "candidate_assets_digest": qualification.ASSETS_DIGEST})
        binding_digest = digest(work / "same-job-acceptance.json")
        candidate = REPO / "native-candidate-import"
        require(not candidate.exists(), "STATE")
        owned.append(candidate)
        shutil.copytree(Path(os.environ["RUNNER_TEMP"]) / "alopex-804-candidate", candidate)
        verify_product(candidate, config["subjects"]["candidate"])
        references = []
        for role in ("before", "published"):
            destination = REPO / ("native-" + role + "-import")
            owned.append(destination)
            references.append(reference_archive(role, config["subjects"][role], destination))
        atomic_json(work / "reference-imports.json", references)
        config["image"] = {"image_record": {"id": image_id}}
        config["_initial_storage_bytes"] = before
        config["_same_job_qualification_run"] = os.environ["GITHUB_RUN_ID"]
        config["_same_job_binding_sha256"] = binding_digest
        # Same qualified image, CPU allocation and security policy; only the
        # cell timeout and owned output namespace differ from #804 acceptance.
        matrix_profile = dict(profile, limits=dict(profile["limits"], **config["limits"]),
                              private_output=str(private_dir(work / "results")))
        public["image_digest"] = image_id.removeprefix("sha256:")
        public["qualification_run"] = os.environ["GITHUB_RUN_ID"]
        public["qualification_binding_sha256"] = binding_digest
        budget = Budget(profile["storage"])
        operation(REPO, config)(matrix_profile, Docker(matrix_profile), budget, work,
                                private_dir(EXPORT), public)
        require(all(read_json(path / "run.json", private=True)["phase"] == "deleted"
                    for path in (work / "results").iterdir() if path.is_dir()), "CLEANUP")
        qualification.verify_temp_snapshot(
            read_json(private / "admission.private.json", private=True)["temp_snapshot"])
        after = budget.used()
        require(after - before <= config["matrix_additional_bound_bytes"] and
                after + config["matrix_storage_reserve_bytes"] < 40_000_000_000,
                "CAPACITY")
        public.update(status="passed", owned_resources_cleaned=True)
    except BaseException as error:
        primary = error
        atomic_json(work / "failure.private.json", {
            "error_code": getattr(error, "code", "INTERNAL"), "detail": repr(error)})
        public["error_code"] = getattr(error, "code", "INTERNAL")
    finally:
        cleanup_errors = []
        for path in reversed(owned):
            try:
                if path.exists():
                    shutil.rmtree(path)
            except BaseException as error:
                cleanup_errors.append(repr(error))
        if cleanup_errors:
            atomic_json(work / "cleanup-errors.private.json", cleanup_errors)
            public["status"] = "failed"
            public["owned_resources_cleaned"] = False
        try:
            storage = (profile["storage"] if profile is not None else
                       read_json(private / "admission.private.json", private=True)["storage"])
            preserve_evidence(work, public, config, storage)
        except BaseException:
            public["status"] = "failed"
            public["owned_resources_cleaned"] = False
            atomic_json(private_dir(EXPORT) / "result.json", {
                "schema": "alopex.native-sql-four-case-matrix/v1", "status": "evidence_failed",
                "performance_qualification": False, "owned_resources_cleaned": False})
            return 1
    return 0 if public["status"] == "passed" and primary is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
