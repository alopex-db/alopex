"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import attestor_1951 as lane
import hosted_test_env_qualification_1951 as qualifier
import same_job_1951
from orchestrate import QUALIFICATION_CASES, SOURCE_COMMIT, SOURCE_TREE
from test_same_job_1951 import fixture
from same_job_1951 import derive_receipt


def h(value):
    return hashlib.sha256(value.encode()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


class FakeDocker:
    def __init__(self, profile):
        self.profile = profile
    def prerequisites(self):
        pass
    def policy(self):
        pass


class AttestorTests(unittest.TestCase):
    def test_qualifier_profile_paths_match_attestor_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory)
            manifest = private / "assets.json"
            facts = {"context": "selected", "cpus": [0, 1],
                     "storage": {"stop_bytes": 40_000_000_000,
                                 "reserve_bytes": 1_000_000_000, "poll_seconds": 10}}
            with patch.dict("os.environ", {"GITHUB_RUN_ID": "12345"}), \
                 patch.object(qualifier, "load_profile"), \
                 patch.object(qualifier, "image_lock"), \
                 patch.object(qualifier, "Docker", FakeDocker):
                generated = json.loads(qualifier.create_profile(private, manifest, facts).read_text())
            self.assertEqual(generated["image_lock"], str(private / "image.lock.json"))
            self.assertEqual(generated["security"]["seccomp_profile"],
                             str(private / "seccomp.json"))
            self.assertTrue(Path(generated["image_lock"]).is_file())
            self.assertTrue(Path(generated["security"]["seccomp_profile"]).is_file())

    def setup_fixture(self, root):
        pins, _ = fixture()
        repo, runner = root / "repo", root / "runner"
        repo.mkdir()
        runner.mkdir()
        private = runner / "alopex-804-qualification"
        candidate = runner / "alopex-804-candidate"
        private.mkdir()
        (candidate / "assets").mkdir(parents=True)
        (private / "image.lock.json").write_text("lock")
        (private / "seccomp.json").write_text("policy")
        (private / "candidate.original.zip").write_text("zip")
        pins.update(asset_run_id=654, asset_run_head=hashlib.sha1(b"asset-run").hexdigest(),
                    artifact_id=321, zip_sha256=h("zip"), artifact_sha256=h("zip"),
                    image_lock_sha256=h("lock"), seccomp_sha256=h("policy"))
        source = {"commit": SOURCE_COMMIT, "tree": SOURCE_TREE, "profile": "release"}
        write(candidate / "assets/receipt.json", {"source": source})
        manifest = {"source_commit": SOURCE_COMMIT, "harness_commit": pins["harness_commit"],
                    "files": [{"role": role, "path": role + ".json",
                               "sha256": pins["server_sha256"] if role == "server" else
                                         pins["wheel_sha256"] if role == "wheel" else h(role)}
                              for role in ("server", "wheel", "library", "harness")] +
                             [{"role": "receipt", "path": "receipt.json", "sha256": h("receipt")}]}
        manifest_path = write(candidate / "assets/assets.json", manifest)
        pins["assets_digest"] = h(manifest_path.read_text())
        write(candidate / "result.json", {"schema": "alopex.native-assets/v1",
                                          "status": "passed", "source": source,
                                          "assets_digest": pins["assets_digest"],
                                          "performance_qualification": False})
        rows = [{"id": name, "status": "passed", "error_code": None}
                for name in QUALIFICATION_CASES]
        public_path = write(repo / "native-qualification-export/public.json", {
            "schema": "alopex.hosted-test-env-qualification/v1", "outcome": "passed",
            "error_code": None, "source_commit": SOURCE_COMMIT,
            "asset_digest": pins["assets_digest"], "cases": rows})
        storage = {"stop_bytes": 40_000_000_000, "reserve_bytes": 1_000_000_000,
                   "poll_seconds": 10}
        profile = {"run_id": "hosted-804-12345", "assets": str(manifest_path),
                   "private_output": str(private / "runs"), "platform": "linux/amd64",
                   "image_lock": str(private / "image.lock.json"),
                   "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                "seccomp_profile": str(private / "seccomp.json"),
                                "seccomp_sha256": pins["seccomp_sha256"]},
                   "storage": storage}
        profile_path = write(private / "profile.json", profile)
        write(private / "acquisition.private.json", {"run": pins["asset_run_id"],
                                                    "head": pins["asset_run_head"],
                                                    "artifact_id": pins["artifact_id"],
                                                    "zip_sha256": pins["zip_sha256"],
                                                    "assets_digest": pins["assets_digest"]})
        write(private / "admission.private.json", {"storage": storage,
                                                   "temp_snapshot": ["verified"]})
        primary_root = private / "runs/hosted-804-12345"
        primary_root.mkdir(parents=True)
        acceptance_path = write(primary_root / "acceptance.json",
                                {"status": "passed", "cases": rows})
        write(primary_root / "public.json", {"status": "passed",
                                                  "assets_digest": pins["assets_digest"],
                                                  "image_digest": h("image"),
                                                  "controller_commit": pins["controller_commit"],
                                                  "controller_digest": pins["controller_digest"]})
        class FakeRun:
            def __init__(self, output, run_id):
                self.root = Path(output) / run_id
                self.record = {"phase": "deleted", "acceptance_digest": h(acceptance_path.read_text())}
            def load(self):
                return self
            def verify_collection(self):
                return None
        operations = {"read_json": lambda path, private=False: json.loads(Path(path).read_text()),
                      "load_profile": lambda path: profile,
                      "verify_assets": lambda path, platform: manifest,
                      "source_identity": lambda: {"controller_commit": pins["controller_commit"],
                                                   "controller_digest": pins["controller_digest"]},
                      "docker_factory": FakeDocker,
                      "verified_image": lambda profile, docker: "sha256:" + h("image"),
                      "run_factory": FakeRun,
                      "verify_temp_snapshot": lambda snapshot: self.assertEqual(snapshot, ["verified"])}
        environment = {"GITHUB_RUN_ID": "12345", "GITHUB_ACTIONS": "true",
                       "RUNNER_ENVIRONMENT": "github-hosted",
                       "GITHUB_SHA": pins["controller_commit"],
                       "EXPECTED_CONTROLLER_SHA": pins["controller_commit"]}
        patches = [patch.object(lane, name, value) for name, value in
                   (("ASSET_RUN", pins["asset_run_id"]),
                    ("ASSET_RUN_HEAD", pins["asset_run_head"]),
                    ("ARTIFACT_ID", pins["artifact_id"]),
                    ("ZIP_SHA256", pins["zip_sha256"]),
                    ("ASSETS_SHA256", pins["assets_digest"]),
                    ("SERVER_SHA256", pins["server_sha256"]),
                    ("WHEEL_SHA256", pins["wheel_sha256"]),
                    ("LOCK_SHA256", pins["image_lock_sha256"]),
                    ("SECCOMP_SHA256", pins["seccomp_sha256"]))]
        patches += [patch.object(same_job_1951, "LOCK_SHA256", pins["image_lock_sha256"]),
                    patch.object(same_job_1951, "SECCOMP_SHA256", pins["seccomp_sha256"])]
        return pins, repo, runner, environment, operations, patches, public_path, profile_path

    def invoke(self, fixture):
        pins, repo, runner, environment, operations, patches, _, _ = fixture
        for item in patches:
            item.start()
        try:
            return lane.attest_current_job(pins, "12345", repo=repo, runner_temp=runner,
                                           environment=environment, operations=operations)
        finally:
            for item in reversed(patches):
                item.stop()

    def test_current_job_source_files_and_three_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.setup_fixture(Path(directory))
            proof = self.invoke(fixture)
            self.assertEqual(proof["product"]["source_commit"], SOURCE_COMMIT)
            self.assertEqual(set(proof["runs"]), {"primary", "repeat", "probe"})
            self.assertEqual(proof["profile"]["storage"]["poll_seconds"], 10)
            pins = fixture[0]
            with patch.object(same_job_1951, "LOCK_SHA256", pins["image_lock_sha256"]), \
                    patch.object(same_job_1951, "SECCOMP_SHA256", pins["seccomp_sha256"]):
                receipt = derive_receipt(pins, job_id="12345", attestor=lambda p, j: proof)
            self.assertEqual(receipt["qualification_status"], "passed")

    def test_missing_pin_wrong_run_and_old_source_fail(self):
        for kind in ("missing", "asset_run", "run", "source"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                fixture = self.setup_fixture(Path(directory))
                pins, repo, runner, environment, _, _, public, _ = fixture
                if kind == "missing":
                    pins["artifact_id"] = 0
                elif kind == "asset_run":
                    pins["asset_run_id"] += 1
                elif kind == "run":
                    environment["GITHUB_RUN_ID"] = "other"
                else:
                    value = json.loads(public.read_text())
                    value["source_commit"] = "a4158b873963dd79c7844953d64a8b511ee75fde"
                    public.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    self.invoke(fixture)

    def test_changed_policy_or_profile_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.setup_fixture(Path(directory))
            profile = fixture[-1]
            value = json.loads(profile.read_text())
            value["security"]["lsm_profile"] = "unconfined"
            profile.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "POLICY"):
                self.invoke(fixture)

    def test_repeat_run_must_be_collected_and_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.setup_fixture(Path(directory))
            operations = fixture[4]
            original = operations["run_factory"]
            def stopped(output, run_id):
                result = original(output, run_id)
                if run_id.endswith("-repeat"):
                    result.record["phase"] = "stopped"
                return result
            operations["run_factory"] = stopped
            with self.assertRaisesRegex(ValueError, "CLEANUP"):
                self.invoke(fixture)


if __name__ == "__main__":
    unittest.main()
