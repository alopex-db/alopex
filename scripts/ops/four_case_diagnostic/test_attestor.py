"""File-backed checks for the source-specific #804 evidence adapter."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from . import attestor, qualification
from .test_qualification import fixture, JOB


def sha(data):
    return hashlib.sha256(data).hexdigest()


class AttestorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.repo = root / "repo"
        self.runner = root / "runner"
        self.private = self.runner / "alopex-804-qualification"
        self.candidate = self.runner / "alopex-804-candidate"
        self.assets = self.candidate / "assets"
        for path in (self.repo / "scripts/ops", self.repo / "scripts/test-env",
                     self.repo / "native-qualification-export", self.private / "runs",
                     self.assets):
            path.mkdir(parents=True, exist_ok=True)
        self.lock, self.proof, _ = fixture()
        self.pin = self.lock["pin"]
        self.pin["operator_sha256"] = {}
        for name in qualification.REQUIRED_OPERATORS:
            path = self.repo / "scripts/ops" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            data = (name + " operator bytes").encode()
            self.write(path, data)
            self.pin["operator_sha256"][name] = sha(data)
        self.pin["common_verifier_sha256"] = sha(b"reviewed verifier bytes")
        self.write(self.repo / "scripts/test-env/common.py", b"reviewed verifier bytes")
        self.pin["image_lock_sha256"] = sha(b"image lock")
        self.pin["seccomp_sha256"] = sha(b"seccomp profile")
        self.write(self.private / "image.lock.json", b"image lock")
        self.write(self.private / "seccomp.json", b"seccomp profile")
        original = b"original ZIP fixture"
        self.write(self.private / "candidate.original.zip", original)
        self.pin["artifact_sha256"] = self.pin["zip_sha256"] = sha(original)
        self.pin["asset_zip_size_bytes"] = len(original)
        self.pin["asset_file_digests"] = {}
        source = {"commit": self.pin["source_commit"],
                  "tree": self.pin["source_tree"], "profile": "release"}
        for role in ("server", "wheel", "library", "receipt", "harness"):
            data = (json.dumps({"source": source}).encode() if role == "receipt"
                    else (role + " bytes").encode())
            self.write(self.assets / role, data)
            self.pin["asset_file_digests"][role] = sha(data)
        self.pin["server_sha256"] = self.pin["asset_file_digests"]["server"]
        self.pin["wheel_sha256"] = self.pin["asset_file_digests"]["wheel"]
        self.manifest = {"source_commit": self.pin["source_commit"],
                         "harness_commit": self.pin["harness_commit"],
                         "files": [{"role": role, "path": role, "sha256": digest}
                                   for role, digest in self.pin["asset_file_digests"].items()]}
        manifest_path = self.assets / "assets.json"
        self.write_json(manifest_path, self.manifest)
        self.pin["assets_digest"] = sha(manifest_path.read_bytes())
        self.write_json(self.candidate / "result.json", {
            "schema": "alopex.native-assets/v1", "status": "passed",
            "source": source, "assets_digest": self.pin["assets_digest"],
            "performance_qualification": False})
        self.write_json(self.private / "acquisition.private.json", {
            "schema": "alopex.sql-four-case-acquisition/v2",
            "qualification_job_id": JOB, "run": self.pin["asset_run_id"],
            "head": self.pin["asset_run_head"], "artifact_id": self.pin["artifact_id"],
            "zip_sha256": self.pin["zip_sha256"],
            "zip_size_bytes": self.pin["asset_zip_size_bytes"],
            "assets_digest": self.pin["assets_digest"]})
        self.proof["qualification_public"]["asset_digest"] = self.pin["assets_digest"]
        self.proof["accepted_public"]["assets_digest"] = self.pin["assets_digest"]
        self.proof["accepted_public"]["controller_digest"] = self.pin["controller_digest"]
        self.public_path = self.repo / "native-qualification-export/public.json"
        self.write_json(self.public_path, self.proof["qualification_public"])
        self.profile = {"run_id": "hosted-804-" + JOB,
                        "assets": str(manifest_path),
                        "private_output": str(self.private / "runs"),
                        "image_lock": str(self.private / "image.lock.json"),
                        "platform": "linux/amd64",
                        "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                     "seccomp_profile": str(self.private / "seccomp.json"),
                                     "seccomp_sha256": self.pin["seccomp_sha256"]},
                        "storage": self.proof["profile"]["storage"]}
        self.write_json(self.private / "profile.json", self.profile)
        self.write_json(self.private / "admission.private.json", {
            "storage": self.profile["storage"], "temp_snapshot": {"ok": True}})
        self.run_roots = {}
        for suffix in ("", "-repeat", "-probe"):
            path = self.private / "runs" / ("hosted-804-" + JOB + suffix)
            path.mkdir()
            (path / "collected").mkdir()
            self.write_json(path / "collected/result.json", {"status": "passed"})
            self.write_json(path / "collection.json", {
                "result.json": sha((path / "collected/result.json").read_bytes())})
            self.run_roots[suffix] = path
        primary = self.run_roots[""]
        self.write_json(primary / "acceptance.json", self.proof["acceptance"])
        self.write_json(primary / "public.json", self.proof["accepted_public"])
        self.cleanup_bad = False
        self.env = {"GITHUB_RUN_ID": JOB, "GITHUB_ACTIONS": "true",
                    "RUNNER_ENVIRONMENT": "github-hosted",
                    "GITHUB_SHA": self.pin["controller_commit"],
                    "EXPECTED_CONTROLLER_SHA": self.pin["controller_commit"]}

    def write(self, path, data):
        path.write_bytes(data)
        path.chmod(0o600)

    def write_json(self, path, data):
        self.write(path, json.dumps(data).encode())

    def operations(self):
        case = self

        class Docker:
            def __init__(self, profile):
                self.profile = profile

            def prerequisites(self):
                pass

            def policy(self):
                pass

        class Run:
            def __init__(self, output, run_id):
                self.root = Path(output) / run_id
                self.record = {"phase": "collected" if case.cleanup_bad else "deleted",
                               "acceptance_digest": sha((case.run_roots[""] /
                                                         "acceptance.json").read_bytes())}

            def load(self):
                return self

            def verify_collection(self):
                if case.cleanup_bad:
                    raise ValueError("COLLECT")
                manifest = json.loads((self.root / "collection.json").read_text())
                for name, digest in manifest.items():
                    if sha((self.root / "collected" / name).read_bytes()) != digest:
                        raise ValueError("COLLECT")

        def verify_assets(path, platform):
            self.assertEqual(platform, "linux/amd64")
            for row in self.manifest["files"]:
                self.assertEqual(sha((self.assets / row["path"]).read_bytes()),
                                 row["sha256"])
            return self.manifest

        return {"read_json": lambda path, private=False: json.loads(Path(path).read_text()),
                "load_profile": lambda path: self.assertEqual(Path(path),
                                                                self.private / "profile.json"),
                "verify_assets": verify_assets,
                "source_identity": lambda: {
                    "controller_commit": self.pin["controller_commit"],
                    "controller_digest": self.pin["controller_digest"]},
                "docker_factory": Docker,
                "verified_image": lambda profile, docker: "sha256:" +
                self.proof["image_digest"],
                "run_factory": Run,
                "verify_temp_snapshot": lambda value: self.assertEqual(value, {"ok": True})}

    def attest(self):
        return attestor.attest_current_job(self.lock, JOB, repo=self.repo,
                                           runner_temp=self.runner,
                                           environment=self.env,
                                           operations=self.operations())

    def test_authenticates_private_files_and_binds_receipt(self):
        proof, receipt = self.attest()
        self.assertEqual(proof["acquisition"]["zip_size_bytes"],
                         self.pin["asset_zip_size_bytes"])
        self.assertEqual(receipt["job_id"], JOB)
        self.assertEqual(receipt["source_tree"], self.pin["source_tree"])

    def test_rejects_wrong_job_and_acquisition_bytes(self):
        self.env["GITHUB_RUN_ID"] = "12345677"
        with self.assertRaisesRegex(ValueError, "QUALIFICATION"):
            self.attest()
        self.env["GITHUB_RUN_ID"] = JOB
        self.write(self.private / "candidate.original.zip", b"wrong ZIP bytes")
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.attest()

    def test_rejects_changed_pinned_operator(self):
        self.write(self.repo / "scripts/ops/four_case_diagnostic/entrypoint.py",
                   b"changed operator bytes")
        with self.assertRaisesRegex(ValueError, "OPERATOR_IDENTITY"):
            self.attest()

    def test_rejects_partial_cleanup_and_failed_case(self):
        self.cleanup_bad = True
        with self.assertRaisesRegex(ValueError, "CLEANUP"):
            self.attest()
        self.cleanup_bad = False
        public = self.proof["qualification_public"]
        public["cases"][0]["status"] = "failed"
        self.write_json(self.public_path, public)
        with self.assertRaisesRegex(ValueError, "QUALIFICATION"):
            self.attest()


if __name__ == "__main__":
    unittest.main()
