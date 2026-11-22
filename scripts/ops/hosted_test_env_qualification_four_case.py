"""Source-bound #804 qualification for the final #712 four-case assets.

The workflow supplies an independently reviewed private manifest SHA-256. This
controller reuses the pinned #804 host/security/storage and envctl lifecycle,
while binding acquisition and output to the four-case lock and current job.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import traceback

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
from four_case_diagnostic import acquire, acquisition, entrypoint, qualification  # noqa: E402

BASE_PATH = HERE / "hosted_test_env_qualification.py"
BASE_SHA256 = "bb52019771e059475a3fe8f9db1493849094dd39699d0b3fffb0b5a69cc9051d"
LOCK_RELATIVE = Path("alopex-712-lock/lock.json")


def _require(value, code="IDENTITY"):
    if not value:
        raise ValueError(code)


def _digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _base():
    _require(BASE_PATH.is_file() and not BASE_PATH.is_symlink() and
             _digest(BASE_PATH) == BASE_SHA256, "OPERATOR_IDENTITY")
    spec = importlib.util.spec_from_file_location("alopex_reviewed_804_base", BASE_PATH)
    _require(spec is not None and spec.loader is not None, "OPERATOR_IDENTITY")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def reviewed_lock_path(environment):
    """Derive the sole allowed lock location; no path comes from job data."""
    raw = environment.get("RUNNER_TEMP")
    _require(type(raw) is str and raw and Path(raw).is_absolute(), "PREREQUISITE")
    root = Path(raw)
    _require(root.is_dir() and not root.is_symlink() and
             root.resolve(strict=True) == root, "PREREQUISITE")
    directory = root / LOCK_RELATIVE.parent
    directory_info = directory.lstat()
    _require(stat.S_ISDIR(directory_info.st_mode) and
             directory_info.st_uid == os.geteuid() and
             stat.S_IMODE(directory_info.st_mode) == 0o700 and
             directory.resolve(strict=True) == directory, "PRIVATE_MANIFEST")
    lock_path = directory / LOCK_RELATIVE.name
    info = lock_path.lstat()
    _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
             stat.S_IMODE(info.st_mode) == 0o600 and
             lock_path.resolve(strict=True) == lock_path and
             lock_path.parent == directory, "PRIVATE_MANIFEST")
    return lock_path


def load_inputs(expected_sha256, *, environment=None, repo=REPO):
    """Verify independently pinned lock bytes and operators before growth."""
    environment = os.environ if environment is None else environment
    _require(type(expected_sha256) is str and
             qualification.HEX64.fullmatch(expected_sha256), "MISSING_PIN")
    _require(environment.get("GITHUB_ACTIONS") == "true" and
             environment.get("RUNNER_ENVIRONMENT") == "github-hosted" and
             type(environment.get("GITHUB_RUN_ID")) is str and
             environment["GITHUB_RUN_ID"].isdecimal() and
             int(environment["GITHUB_RUN_ID"]) > 0 and
             type(environment.get("GITHUB_RUN_ATTEMPT")) is str and
             environment["GITHUB_RUN_ATTEMPT"].isdecimal() and
             int(environment["GITHUB_RUN_ATTEMPT"]) > 0 and
             environment.get("GITHUB_REPOSITORY") == "alopex-db/alopex", "SAME_JOB")
    lock = entrypoint.load_reviewed_lock(reviewed_lock_path(environment), expected_sha256)
    pin = lock["pin"]
    _require(set(pin) == qualification.PIN_FIELDS and
             type(pin["operator_sha256"]) is dict and
             set(pin["operator_sha256"]) == qualification.REQUIRED_OPERATORS and
             all(type(value) is str and qualification.HEX64.fullmatch(value)
                 for value in pin["operator_sha256"].values()) and
             type(pin["source_commit"]) is str and
             qualification.HEX40.fullmatch(pin["source_commit"]) and
             type(pin["source_tree"]) is str and
             qualification.HEX40.fullmatch(pin["source_tree"]) and
             type(pin["controller_commit"]) is str and
             qualification.HEX40.fullmatch(pin["controller_commit"]) and
             environment.get("GITHUB_SHA") == environment.get("EXPECTED_CONTROLLER_SHA") ==
             pin["controller_commit"], "MISSING_PIN")
    for name, expected in pin["operator_sha256"].items():
        operator = repo / "scripts/ops" / name
        _require(operator.is_file() and not operator.is_symlink() and
                 _digest(operator) == expected, "OPERATOR_IDENTITY")
    common = repo / "scripts/test-env/common.py"
    _require(common.is_file() and not common.is_symlink() and
             _digest(common) == pin["common_verifier_sha256"], "OPERATOR_IDENTITY")
    base = _base()
    identity = base.source_identity()
    _require(identity == {"controller_commit": pin["controller_commit"],
                          "controller_digest": pin["controller_digest"]}, "IDENTITY")
    image_lock = base.INPUT / "abi-image.lock.json"
    seccomp = base.INPUT / "seccomp-default.json"
    _require(_digest(image_lock) == pin["image_lock_sha256"] and
             _digest(seccomp) == pin["seccomp_sha256"], "POLICY")
    return lock, base


def verify_temp_snapshot(expected):
    """The entrypoint imports this top-level operator only after hashing it."""
    _base().verify_temp_snapshot(expected)


def verified_inputs(base, pin, assets_root):
    manifest_path = assets_root / "assets/assets.json"
    _require(_digest(manifest_path) == pin["assets_digest"], "IDENTITY")
    manifest = base.verify_assets(manifest_path, "linux/amd64")
    by_role = {row["role"]: row for row in manifest["files"]}
    _require(manifest["source_commit"] == pin["source_commit"] and
             manifest["harness_commit"] == pin["harness_commit"] and
             len(manifest["files"]) == len(qualification.ASSET_ROLES) and
             set(by_role) == qualification.ASSET_ROLES and
             all(by_role[role]["sha256"] == pin["asset_file_digests"][role]
                 for role in qualification.ASSET_ROLES), "IDENTITY")
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    _require(json.loads((assets_root / "result.json").read_text(),
                        object_pairs_hook=acquisition._unique_pairs) == {
        "schema": "alopex.native-assets/v1", "status": "passed", "source": source,
        "assets_digest": pin["assets_digest"], "performance_qualification": False},
        "IDENTITY")
    base.require(base.elf_signature(assets_root / "assets" / by_role["server"]["path"]) ==
                 (2, 1, 62) and
                 base.elf_signature(assets_root / "assets" / by_role["library"]["path"]) ==
                 (2, 1, 62) and
                 by_role["wheel"]["path"].endswith("alopex-0.8.17-cp38-abi3-linux_x86_64.whl"),
                 "IDENTITY")
    return manifest_path


def acquire_assets(lock, *, private, assets_root, job_id, fetch=None):
    """Use the bounded v2 acquirer, then install its verified files at #804 paths."""
    pin = lock["pin"]
    _require(not assets_root.exists() and not assets_root.is_symlink() and
             not (private / "candidate.original.zip").exists() and
             not (private / "acquisition.private.json").exists(), "STATE")
    staging = private / "four-case-acquisition"
    proof = acquire.acquire(lock, job_id=job_id, directory=staging,
                            fetch=entrypoint.GitHubFetcher().fetch if fetch is None else fetch)
    _require(proof["assets_digest"] == pin["assets_digest"], "IDENTITY")
    shutil.move(staging / "candidate.original.zip", private / "candidate.original.zip")
    shutil.move(staging / "acquisition.private.json", private / "acquisition.private.json")
    shutil.move(staging / "assets-root", assets_root)
    staging.rmdir()
    acquisition.verify(lock, receipt_path=private / "acquisition.private.json",
                       archive_path=private / "candidate.original.zip",
                       manifest_path=assets_root / "assets/assets.json", job_id=job_id)
    return proof


def _configure_base(base, pin):
    """Set only reviewed identities; keep the base host and security code intact."""
    base.CONTROLLER_DIGEST = pin["controller_digest"]
    base.LOCK_DIGEST = pin["image_lock_sha256"]
    base.POLICY_DIGEST = pin["seccomp_sha256"]
    base.PRODUCT = pin["source_commit"]
    base.ASSETS_DIGEST = pin["assets_digest"]
    base.EXPORT = REPO / "native-qualification-export"
    base.EXPECTED_TEMP_SNAPSHOT = None
    base.PRIVATE_STAGE = "initial"


def export_public_qualification(base, public, budget=None):
    """Publish only fixed qualification status; leave diagnostics local."""
    common = {"schema", "case", "outcome", "error_code"}
    outcome = public.get("outcome") if type(public) is dict else None
    if outcome == "passed":
        expected = common | {"source_commit", "asset_digest", "cases"}
    elif outcome == "assets_verified_not_qualified":
        expected = common | {"asset_digest"}
    else:
        expected = common
    _require(type(public) is dict and set(public) == expected and
             public.get("schema") == "alopex.hosted-test-env-qualification/v1" and
             public.get("case") == "HOST_QUALIFICATION" and
             type(outcome) is str and outcome in {"failed", "admitted_not_qualified",
                         "assets_verified_not_qualified", "passed"} and
             (type(public.get("error_code")) is str and
              public["error_code"] in base.ERRORS if outcome == "failed" else
              public.get("error_code") is None), "PUBLIC_OUTPUT")
    if "asset_digest" in public:
        _require(type(public["asset_digest"]) is str and
                 qualification.HEX64.fullmatch(public["asset_digest"]), "PUBLIC_OUTPUT")
    if outcome == "passed":
        _require(type(public["source_commit"]) is str and
                 qualification.HEX40.fullmatch(public["source_commit"]) and
                 type(public["cases"]) is list and
                 len(public["cases"]) == len(qualification.CASES) and
                 all(type(row) is dict and set(row) == {"id", "status", "error_code"} and
                     type(row["id"]) is str and row["status"] == "passed" and
                     row["error_code"] is None for row in public["cases"]) and
                 {row.get("id") for row in public["cases"] if type(row) is dict} ==
                 qualification.CASES, "PUBLIC_OUTPUT")
    body = (json.dumps(public, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")
    _require(0 < len(body) <= 4096, "PUBLIC_OUTPUT")
    if budget is not None:
        budget.before("collect")
    if base.EXPECTED_TEMP_SNAPSHOT is not None:
        base.verify_temp_snapshot(base.EXPECTED_TEMP_SNAPSHOT)
    export = Path(base.EXPORT)
    _require(export.is_absolute() and not export.is_symlink(), "PUBLIC_OUTPUT")
    export.mkdir(mode=0o700, exist_ok=True)
    info = export.lstat()
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
             stat.S_IMODE(info.st_mode) == 0o700 and
             {path.name for path in export.iterdir()} <= {"public.json"}, "PUBLIC_OUTPUT")
    target = export / "public.json"
    if target.exists() or target.is_symlink():
        info = target.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                 stat.S_IMODE(info.st_mode) == 0o600, "PUBLIC_OUTPUT")
    pending = export / ".public.pending"
    fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        if budget is not None:
            budget.check()
        pending.replace(target)
    finally:
        pending.unlink(missing_ok=True)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv not in (["--admission-only"], ["--acquire-assets"], ["--execute"]):
        print('{"case":"HOST_QUALIFICATION","outcome":"invalid_phase"}')
        return 1
    public = {"schema": "alopex.hosted-test-env-qualification/v1",
              "case": "HOST_QUALIFICATION", "outcome": "failed", "error_code": "INCOMPLETE"}
    base = None
    try:
        lock, base = load_inputs(os.environ["ALOPEX_FOUR_CASE_LOCK_SHA256"])
        pin = lock["pin"]
        job_id = os.environ["GITHUB_RUN_ID"]
        assets_root = Path(os.environ["ALOPEX_804_ASSETS"])
        _require(assets_root == Path(os.environ["RUNNER_TEMP"]) / "alopex-804-candidate" and
                 assets_root.is_absolute(), "IDENTITY")
        _configure_base(base, pin)
        private = base.private_dir(Path(os.environ["RUNNER_TEMP"]) / "alopex-804-qualification")
        base.PRIVATE_STAGE = "controller_identity"
        base.verified_controller()
        if argv == ["--admission-only"]:
            base.checkout_growth(private)
            base.PRIVATE_STAGE = "admission_facts"
            facts = base.admission_facts()
            base.EXPECTED_TEMP_SNAPSHOT = facts["temp_snapshot"]
            base.atomic_json(private / "admission.private.json", facts)
            base.create_profile(private, assets_root / "assets/assets.json", facts)
            public.update(outcome="admitted_not_qualified", error_code=None)
        elif argv == ["--acquire-assets"]:
            previous = base.read_json(private / "admission.private.json", private=True)
            base.require((private / "profile.json").is_file(), "PREREQUISITE")
            facts = base.admission_facts()
            base.same_runner(previous, facts)
            base.EXPECTED_TEMP_SNAPSHOT = previous["temp_snapshot"]
            acquire_assets(lock, private=private, assets_root=assets_root, job_id=job_id)
            verified_inputs(base, pin, assets_root)
            base.verify_temp_snapshot(previous["temp_snapshot"])
            public.update(outcome="assets_verified_not_qualified", error_code=None,
                          asset_digest=pin["assets_digest"])
        else:
            previous = base.read_json(private / "admission.private.json", private=True)
            profile = private / "profile.json"
            base.require(profile.is_file(), "PREREQUISITE")
            acquisition.verify(lock, receipt_path=private / "acquisition.private.json",
                               archive_path=private / "candidate.original.zip",
                               manifest_path=assets_root / "assets/assets.json", job_id=job_id)
            manifest = verified_inputs(base, pin, assets_root)
            facts = base.admission_facts()
            base.same_runner(previous, facts)
            base.EXPECTED_TEMP_SNAPSHOT = previous["temp_snapshot"]
            configured = base.load_profile(profile)
            base.require(configured["assets"] == str(manifest) and
                         configured["storage"] == previous["storage"], "IDENTITY")
            accepted = base.execute(profile)
            base.require(len(accepted["cases"]) == len(qualification.CASES) and
                         {row["id"] for row in accepted["cases"]} == qualification.CASES and
                         all(row["status"] == "passed" and row["error_code"] is None
                             for row in accepted["cases"]), "INCOMPLETE")
            final_bytes = base.Budget(facts["storage"]).used()
            base.verify_temp_snapshot(previous["temp_snapshot"])
            base.atomic_json(private / "storage-after.private.json",
                             {"allocated_bytes": final_bytes})
            base.require(final_bytes < 40_000_000_000, "CAPACITY")
            public.update(outcome="passed", error_code=None,
                          source_commit=pin["source_commit"],
                          asset_digest=pin["assets_digest"],
                          cases=[{"id": row["id"], "status": row["status"],
                                  "error_code": row["error_code"]}
                                 for row in accepted["cases"]])
        return_code = 0
    except BaseException as error:
        code = error.code if base is not None and isinstance(error, base.Failure) else "INTERNAL"
        public["error_code"] = code if base is not None and code in base.ERRORS else "INTERNAL"
        return_code = 1
        if base is not None and "private" in locals():
            base.atomic_json(private / "failure.private.json", {
                "error_code": code, "detail": repr(error),
                "stage": base.PRIVATE_STAGE,
                "traceback": "".join(traceback.format_exception(error))[:8192]})
    if base is not None and "private" in locals():
        try:
            export_public_qualification(
                base, public,
                base.Budget(facts["storage"]) if "facts" in locals() else None)
        except BaseException:
            return 1
    print(json.dumps({"case": "HOST_QUALIFICATION", "outcome": public["outcome"],
                      "error_code": public["error_code"]}, sort_keys=True))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
